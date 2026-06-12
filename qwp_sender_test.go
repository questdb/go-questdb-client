/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package questdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// newQwpTestServer creates a mock WebSocket server that accepts QWP
// messages and responds with OK ACKs carrying an incrementing
// cumulative sequence (0-indexed, matches Java and the real server).
func newQwpTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("websocket accept error: %v", err)
			return
		}
		defer conn.CloseNow()

		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
}

// newQwpSenderForTest creates a QWP sender connected to the given
// test server URL.
func newQwpSenderForTest(t *testing.T, serverURL string) *qwpLineSender {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil)
	if err != nil {
		t.Fatalf("newQwpLineSender: %v", err)
	}
	return s
}

// flushAndAwaitAck flushes pending rows and blocks until the server
// has ACKed them. Flush no longer waits for the ACK (Java decision
// #1 — see design/qwp-cursor-durability.md), so tests that assert
// server-side receipt must use this FlushAndGetSequence +
// AwaitAckedFsn barrier instead of relying on Flush alone.
func flushAndAwaitAck(t *testing.T, s *qwpLineSender) {
	t.Helper()
	fsn, err := s.FlushAndGetSequence(context.Background())
	if err != nil {
		t.Fatalf("FlushAndGetSequence: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.AwaitAckedFsn(ctx, fsn); err != nil {
		t.Fatalf("AwaitAckedFsn(fsn=%d): %v", fsn, err)
	}
}

func TestQwpSenderBasicRow(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	err := s.Table("test").
		Symbol("host", "server1").
		Int64Column("cpu", 42).
		Float64Column("mem", 85.5).
		At(context.Background(), time.Unix(0, 1000000000))
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	if s.pendingRowCount != 1 {
		t.Fatalf("pendingRowCount = %d, want 1", s.pendingRowCount)
	}

	err = s.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if s.pendingRowCount != 0 {
		t.Fatalf("pendingRowCount after flush = %d, want 0", s.pendingRowCount)
	}
}

// TestQwpSyncFlushAbsorbsStaleAck verifies that the cursor send
// loop tolerates an ACK whose cumulative sequence is older than the
// most recent published batch and keeps making forward progress.
// engineAcknowledge is monotonic — it clamps to ackedFsn — so stale
// ACKs are absorbed without breaking the engine's drain accounting.
func TestQwpSyncFlushAbsorbsStaleAck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			// For every flush, first emit an ACK with a stale sequence
			// (-1, i.e. "before anything") and then the real ACK.
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(-1))
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	for i := 0; i < 3; i++ {
		if err := s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background()); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if err := s.Flush(context.Background()); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
}

// TestQwpFlushRetainsRowsOnError is a regression test for the
// retain-on-error contract: when flushCursor fails before the rows
// are persisted to a segment (here: ctx cancelled, so
// engineAppendBlocking returns ctx.Err() before assigning an FSN),
// Flush must NOT reset the table buffers. A prior version registered
// `defer resetAfterFlush()` ahead of the flushCursor error check,
// silently destroying rows that were never sent anywhere. The buffer
// must survive so a subsequent flush delivers the data.
func TestQwpFlushRetainsRowsOnError(t *testing.T) {
	var mu sync.Mutex
	framesReceived := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
			mu.Lock()
			framesReceived++
			mu.Unlock()
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	if err := s.Table("t").Int64Column("x", 99).AtNow(context.Background()); err != nil {
		t.Fatalf("AtNow: %v", err)
	}
	if s.pendingRowCount != 1 {
		t.Fatalf("pendingRowCount before flush = %d, want 1", s.pendingRowCount)
	}

	// Cancelled ctx → engineAppendBlocking returns early, nothing
	// persisted. The flush must fail and the row must be retained.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Flush(cancelled); err == nil {
		t.Fatal("Flush with cancelled ctx: want error, got nil")
	}
	if s.pendingRowCount != 1 {
		t.Fatalf("pendingRowCount after failed flush = %d, want 1 "+
			"(row destroyed — retain-on-error contract violated)", s.pendingRowCount)
	}
	mu.Lock()
	got := framesReceived
	mu.Unlock()
	if got != 0 {
		t.Fatalf("server received %d frames from the failed flush, want 0", got)
	}

	// The retained row must be delivered by a subsequent good flush.
	// Flush no longer blocks on the ACK (Java decision #1), so use
	// FlushAndGetSequence + AwaitAckedFsn as the delivery barrier
	// before asserting receipt.
	fsn, err := s.FlushAndGetSequence(context.Background())
	if err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	if s.pendingRowCount != 0 {
		t.Fatalf("pendingRowCount after retry flush = %d, want 0", s.pendingRowCount)
	}
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer awaitCancel()
	if err := s.AwaitAckedFsn(awaitCtx, fsn); err != nil {
		t.Fatalf("AwaitAckedFsn: %v", err)
	}
	mu.Lock()
	got = framesReceived
	mu.Unlock()
	if got != 1 {
		t.Fatalf("server received %d frames total, want exactly 1 "+
			"(retained row not delivered, or duplicated)", got)
	}
}

func TestQwpSenderMultipleRows(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	for i := 0; i < 5; i++ {
		err := s.Table("metrics").
			Int64Column("val", int64(i)).
			AtNow(context.Background())
		if err != nil {
			t.Fatalf("AtNow row %d: %v", i, err)
		}
	}

	if s.pendingRowCount != 5 {
		t.Fatalf("pendingRowCount = %d, want 5", s.pendingRowCount)
	}

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderMultipleTables(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Insert into two different tables.
	err := s.Table("table_a").Int64Column("x", 1).AtNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = s.Table("table_b").Float64Column("y", 2.5).AtNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = s.Table("table_a").Int64Column("x", 3).AtNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(s.tableBuffers) != 2 {
		t.Fatalf("tableBuffers count = %d, want 2", len(s.tableBuffers))
	}

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderSymbolDictionary(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Add symbols.
	s.Table("t").Symbol("sym", "AAPL").Int64Column("v", 1).AtNow(context.Background())
	s.Table("t").Symbol("sym", "MSFT").Int64Column("v", 2).AtNow(context.Background())
	s.Table("t").Symbol("sym", "AAPL").Int64Column("v", 3).AtNow(context.Background())

	// Should have 2 unique symbols.
	if len(s.globalSymbols) != 2 {
		t.Fatalf("globalSymbols count = %d, want 2", len(s.globalSymbols))
	}
	if s.globalSymbols["AAPL"] != 0 {
		t.Fatalf("AAPL ID = %d, want 0", s.globalSymbols["AAPL"])
	}
	if s.globalSymbols["MSFT"] != 1 {
		t.Fatalf("MSFT ID = %d, want 1", s.globalSymbols["MSFT"])
	}
	if s.batchMaxSymbolId != 1 {
		t.Fatalf("batchMaxSymbolId = %d, want 1", s.batchMaxSymbolId)
	}

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// After flush, maxSentSymbolId should be updated.
	if s.maxSentSymbolId != 1 {
		t.Fatalf("maxSentSymbolId = %d, want 1", s.maxSentSymbolId)
	}
}

func TestQwpSenderAllColumnTypes(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	err := s.Table("types").
		Symbol("tag", "a").
		Int64Column("long", 42).
		Float64Column("double", 3.14).
		StringColumn("str", "hello").
		BoolColumn("flag", true).
		TimestampColumn("ts_col", ts).
		At(context.Background(), ts)
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderErrorNoTable(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Column without Table should error.
	s.Int64Column("x", 42)
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for column without table")
	}
}

func TestQwpSenderErrorDoubleTable(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Double Table without At should error.
	s.Table("a").Table("b")
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for double Table without At")
	}
}

func TestQwpSenderErrorInvalidTableName(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("")
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for empty table name")
	}
}

func TestQwpSenderErrorNegativeLong256(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("tbl").Long256Column("h", big.NewInt(-42))
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for negative long256")
	}
	if !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQwpSenderErrorOversizedLong256(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// 2^256: one bit beyond the representable range.
	bigVal := new(big.Int).Lsh(big.NewInt(1), 256)
	s.Table("tbl").Long256Column("h", bigVal)
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for oversized long256")
	}
	if !strings.Contains(err.Error(), "larger than 256-bit") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQwpSenderLong256Encoding(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Distinct-per-limb pattern catches byte-order and limb-order bugs.
	// High-to-low 64-bit words: 0x01..01, 0x02..02, 0x03..03, 0x04..04.
	val, ok := new(big.Int).SetString(
		"0101010101010101020202020202020203030303030303030404040404040404", 16)
	if !ok {
		t.Fatal("SetString failed")
	}
	s.Table("t").Long256Column("h", val)
	if err := s.At(context.Background(), time.Now()); err != nil {
		t.Fatalf("At: %v", err)
	}

	tb := s.tableBuffers["t"]
	var col *qwpColumnBuffer
	for _, c := range tb.columns {
		if c.name == "h" {
			col = c
			break
		}
	}
	if col == nil {
		t.Fatal("column 'h' not found")
	}

	// Wire layout: four LE-encoded uint64 limbs, least-significant first.
	var expected [32]byte
	for i := 0; i < 8; i++ {
		expected[i] = 0x04
		expected[8+i] = 0x03
		expected[16+i] = 0x02
		expected[24+i] = 0x01
	}
	if !bytes.Equal(col.fixedData, expected[:]) {
		t.Fatalf("fixedData = %x, want %x", col.fixedData, expected)
	}
}

func TestQwpSenderFlushEmpty(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Flush with no pending rows should be no-op.
	err := s.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush on empty: %v", err)
	}
}

func TestQwpSenderFlushWithPendingRow(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Start a row but don't finalize.
	s.Table("t").Int64Column("x", 1)

	err := s.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error for flush with pending row")
	}
}

func TestQwpSenderClose(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)

	// Add a row, then close (should auto-flush).
	s.Table("t").Int64Column("x", 1).AtNow(context.Background())

	err := s.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Double close should error.
	err = s.Close(context.Background())
	if err != errDoubleSenderClose {
		t.Fatalf("double close: got %v, want errDoubleSenderClose", err)
	}
}

func TestQwpSenderCloseSurfacesLatchedFluentError(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)

	// Latch a validation error: '?' is an illegal column-name char.
	s.Table("t").Symbol("bad?name", "v")

	err := s.Close(context.Background())
	if err == nil {
		t.Fatalf("Close: nil, expected latched fluent-API error")
	}
	if !strings.Contains(err.Error(), "illegal character") {
		t.Fatalf("Close: %v, want error mentioning illegal character", err)
	}
}

func TestQwpSenderClosedOperations(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)

	s.Close(context.Background())

	// Operations on closed sender should error.
	err := s.Table("t").Int64Column("x", 1).At(context.Background(), time.Now())
	if err != errClosedSenderAt {
		t.Fatalf("At on closed: got %v, want errClosedSenderAt", err)
	}

	err = s.Flush(context.Background())
	if err != errClosedSenderFlush {
		t.Fatalf("Flush on closed: got %v, want errClosedSenderFlush", err)
	}
}

func TestQwpSenderAutoFlushRows(t *testing.T) {
	// Mock server that counts received messages and signals the
	// test goroutine on every receive — cursor mode's auto-flush is
	// asynchronous (send loop transmits in the background), so the
	// test must wait for the server to observe the frame rather
	// than poll on shared memory.
	var mu sync.Mutex
	msgCount := 0
	msgReceived := make(chan struct{}, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			msgCount++
			mu.Unlock()
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
			select {
			case msgReceived <- struct{}{}:
			default:
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 3, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Insert 5 rows with autoFlushRows=3.
	// Should auto-flush at row 3, then 2 remain.
	for i := 0; i < 5; i++ {
		err := s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background())
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	// Auto-flush should have triggered at row 3. Block until the
	// server signals it received that frame.
	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("auto-flush frame did not reach the server within 2s")
	}
	mu.Lock()
	gotMsgCount := msgCount
	mu.Unlock()
	if gotMsgCount != 1 {
		t.Fatalf("auto-flush messages = %d, want 1", gotMsgCount)
	}
	if s.pendingRowCount != 2 {
		t.Fatalf("pendingRowCount = %d, want 2", s.pendingRowCount)
	}
}

func TestQwpSenderAutoFlushTimeInterval(t *testing.T) {
	// Mock server that counts received messages and signals on
	// every receive (see TestQwpSenderAutoFlushRows for rationale).
	var mu sync.Mutex
	msgCount := 0
	readMsgCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return msgCount
	}
	msgReceived := make(chan struct{}, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			msgCount++
			mu.Unlock()
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
			select {
			case msgReceived <- struct{}{}:
			default:
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	// autoFlushRows=0 (disabled), autoFlushInterval=10ms.
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// First row: initializes the deadline but does not flush.
	err = s.Table("t").Int64Column("x", int64(1)).AtNow(context.Background())
	if err != nil {
		t.Fatalf("row 1: %v", err)
	}
	if got := readMsgCount(); got != 0 {
		t.Fatalf("after row 1: msgCount = %d, want 0", got)
	}
	if s.pendingRowCount != 1 {
		t.Fatalf("after row 1: pendingRowCount = %d, want 1", s.pendingRowCount)
	}

	// Wait for the interval to expire.
	time.Sleep(20 * time.Millisecond)

	// Second row: triggers time-based auto-flush. Block until the
	// server signals it received the frame.
	err = s.Table("t").Int64Column("x", int64(2)).AtNow(context.Background())
	if err != nil {
		t.Fatalf("row 2: %v", err)
	}
	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("time-based auto-flush did not reach the server within 2s")
	}
	if got := readMsgCount(); got != 1 {
		t.Fatalf("after row 2: msgCount = %d, want 1 (time-based flush)", got)
	}
	if s.pendingRowCount != 0 {
		t.Fatalf("after row 2: pendingRowCount = %d, want 0", s.pendingRowCount)
	}
}

func TestQwpSenderAutoFlushDisabled(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	// Both autoFlushRows=0 and autoFlushInterval=0 (disabled).
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Insert many rows — no auto-flush should trigger.
	for i := 0; i < 100; i++ {
		err := s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background())
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	if s.pendingRowCount != 100 {
		t.Fatalf("pendingRowCount = %d, want 100", s.pendingRowCount)
	}
}

func TestQwpSenderDecimalColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	d := NewDecimalFromInt64(12345, 2) // 123.45
	err := s.Table("t").
		DecimalColumn("price", d).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderDecimalColumnFromString(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	err := s.Table("t").
		DecimalColumnFromString("price", "123.45").
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderDecimalColumnFromStringVariants(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	cases := []string{
		"0", "1", "-1", "123.456", "-99.99",
		"1e5", "1.23e10", "1.5e-3",
	}

	// Each case uses a different column name since scale must be
	// consistent within a column.
	for i, val := range cases {
		colName := fmt.Sprintf("d%d", i)
		err := s.Table("t").
			DecimalColumnFromString(colName, val).
			AtNow(context.Background())
		if err != nil {
			t.Fatalf("DecimalColumnFromString(%q): %v", val, err)
		}
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderDecimalColumnFromStringInvalid(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// NaN is not representable in binary.
	s.Table("t").DecimalColumnFromString("d", "NaN")
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for NaN decimal")
	}
}

func TestQwpSenderFloat64Array1D(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	err := s.Table("t").
		Float64Array1DColumn("arr", []float64{1.0, 2.0, 3.0}).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderFloat64Array2D(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	err := s.Table("t").
		Float64Array2DColumn("mat", [][]float64{{1.0, 2.0}, {3.0, 4.0}}).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderFloat64Array2DIrregular(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Irregular array should error.
	s.Table("t").Float64Array2DColumn("mat", [][]float64{{1.0, 2.0}, {3.0}})
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for irregular 2D array")
	}
}

func TestQwpSenderFloat64Array3D(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	err := s.Table("t").
		Float64Array3DColumn("tensor", [][][]float64{
			{{1.0, 2.0}, {3.0, 4.0}},
			{{5.0, 6.0}, {7.0, 8.0}},
		}).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderFloat64ArrayND(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	arr, err := NewNDArray[float64](2, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0} {
		_ = i
		arr.Append(v)
	}

	err = s.Table("t").
		Float64ArrayNDColumn("nd", arr).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderFloat64ArrayEmpty(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Empty 1D array.
	err := s.Table("t").
		Float64Array1DColumn("arr", []float64{}).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	// Empty 2D array.
	err = s.Table("t").
		Float64Array2DColumn("mat", [][]float64{}).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestParseDecimalFromString(t *testing.T) {
	tests := []struct {
		input     string
		wantScale uint32
		wantErr   bool
	}{
		{"0", 0, false},
		{"123", 0, false},
		{"-123", 0, false},
		{"123.45", 2, false},
		{"-99.99", 2, false},
		{"1e5", 0, false},
		{"1.5e-3", 4, false},
		{"1.23e2", 0, false},
		{"NaN", 0, true},
		{"Infinity", 0, true},
		{"+Infinity", 0, true},
		{"-Infinity", 0, true},
		{"", 0, true},
		{"+", 0, true},
		{"1.2.3", 0, true},
		{"1.2.", 0, true},
		{".1.", 0, true},
		// Exponent out of range: scale > maxDecimalScale (76).
		{"1e-77", 0, true},
		// Exponent out of range: scale < -maxDecimalScale. Without the
		// guard, buildDecimal would allocate ~1MB of '0' padding before
		// bigIntToTwosComplement rejects the result.
		{"1e1000000", 0, true},
		{"1e-1000000", 0, true},
	}

	for _, tc := range tests {
		d, err := parseDecimalFromString(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseDecimalFromString(%q): expected error, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseDecimalFromString(%q): %v", tc.input, err)
			continue
		}
		if d.scale != tc.wantScale {
			t.Errorf("parseDecimalFromString(%q): scale = %d, want %d", tc.input, d.scale, tc.wantScale)
		}
	}
}

// --- QwpSender extended column type tests ---

func TestQwpSenderByteColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.ByteColumn("b", 42).
		ByteColumn("neg", -128).
		ByteColumn("max", 127).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderShortColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.ShortColumn("s", 1000).
		ShortColumn("neg", -32768).
		ShortColumn("max", 32767).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderInt32Column(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.Int32Column("i", 100000).
		Int32Column("neg", -2147483648).
		Int32Column("max", 2147483647).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderFloat32Column(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.Float32Column("f", 3.14).
		Float32Column("zero", 0).
		Float32Column("neg", -1.5).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderCharColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.CharColumn("c", 'A').
		CharColumn("digit", '9').
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderDateColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	ts := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	s.Table("t")
	err := s.DateColumn("d", ts).
		DateColumn("epoch", time.Unix(0, 0).UTC()).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderTimestampNanosColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456789, time.UTC)
	s.Table("t")
	err := s.TimestampNanosColumn("ts", ts).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderUuidColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// UUID: 550e8400-e29b-41d4-a716-446655440000
	hi := uint64(0x550e8400e29b41d4)
	lo := uint64(0xa716446655440000)
	s.Table("t")
	err := s.UuidColumn("id", hi, lo).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderStringColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.StringColumn("v", "hello world").
		StringColumn("empty", "").
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderGeohashColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.GeohashColumn("geo", 0xABCDE, 20).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderInt64Array1D(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.Int64Array1DColumn("arr", []int64{10, 20, 30}).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderInt64Array2D(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.Int64Array2DColumn("mat", [][]int64{{1, 2}, {3, 4}}).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderInt64Array3D(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.Int64Array3DColumn("tensor", [][][]int64{
		{{1, 2}, {3, 4}},
		{{5, 6}, {7, 8}},
	}).AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderFloat64Array1DOverflowRejected(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Lazy zero pages keep the RAM cost negligible.
	values := make([]float64, MaxArrayElements+1)
	err := s.Table("t").
		Float64Array1DColumn("arr", values).
		At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected overflow error for oversized 1D array")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQwpSenderInt64Array1DOverflowRejected(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	values := make([]int64, MaxArrayElements+1)
	s.Table("t")
	s.Int64Array1DColumn("arr", values)
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected overflow error for oversized 1D array")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQwpSenderFloat64Array2DProductOverflowRejected(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Each dim fits within MaxArrayElements, but their product does not.
	dim0 := 65536
	dim1 := 4097 // 65536 * 4097 = 268 500 992 > MaxArrayElements
	rows := make([][]float64, dim0)
	for i := range rows {
		rows[i] = make([]float64, dim1)
	}
	err := s.Table("t").
		Float64Array2DColumn("mat", rows).
		At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected overflow error for oversized 2D array")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQwpSenderInt64Array2DProductOverflowRejected(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	dim0 := 65536
	dim1 := 4097
	rows := make([][]int64, dim0)
	for i := range rows {
		rows[i] = make([]int64, dim1)
	}
	s.Table("t")
	s.Int64Array2DColumn("mat", rows)
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected overflow error for oversized 2D array")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQwpSenderMixedExtendedTypes(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	s.Table("mixed")
	err := s.ByteColumn("b", 1).
		ShortColumn("sh", 1000).
		Int32Column("i", 50000).
		Float32Column("f", 2.5).
		CharColumn("c", 'X').
		DateColumn("dt", ts).
		UuidColumn("id", 0x1234, 0x5678).
		StringColumn("vc", "test").
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQwpSenderExtendedNoTable(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// ByteColumn without Table should error.
	s.ByteColumn("b", 1)
	err := s.At(context.Background(), time.Now())
	if err == nil {
		t.Fatal("expected error for ByteColumn without Table")
	}
}

func TestQwpSenderMethodChaining(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// QwpSender methods return QwpSender and can be chained.
	// Table() returns LineSender, so call it first then chain
	// QwpSender-specific methods on the concrete sender.
	s.Table("t")
	err := s.ByteColumn("b", 1).
		ShortColumn("sh", 2).
		Int32Column("i", 3).
		Float32Column("f", 4.0).
		At(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("chained At: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// --- Integration test ---

// Renamed from TestQwpSenderIntegration to TestQwpIntegrationSender
// so the qwp-fuzz.yml workflow pattern ^TestQwp(Fuzz|Integration)
// actually catches it. Used to hard-code "ws://localhost:9000" and
// silently skip in CI; now goes through the shared fuzz fixture.
func TestQwpIntegrationSender(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr,
		qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil)
	if err != nil {
		t.Fatalf("sender open against fixture %s: %v", qwpTestAddr, err)
	}
	defer s.Close(ctx)

	ts := time.Now().Truncate(time.Microsecond)

	err = s.Table("qwp_sender_test").
		Symbol("host", "test_host").
		Int64Column("cpu", 42).
		Float64Column("mem", 85.5).
		StringColumn("msg", "hello world").
		BoolColumn("active", true).
		At(ctx, ts)
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	err = s.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Second flush against the same column set — cursor mode always
	// emits FULL schema with schema_id=0 on every frame. The test
	// exercises the steady-state flush path; the wire-format
	// invariant is pinned in encoder tests.
	err = s.Table("qwp_sender_test").
		Symbol("host", "test_host").
		Int64Column("cpu", 99).
		Float64Column("mem", 50.0).
		StringColumn("msg", "second row").
		BoolColumn("active", false).
		At(ctx, ts.Add(time.Microsecond))
	if err != nil {
		t.Fatalf("At (row 2): %v", err)
	}

	err = s.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush (row 2): %v", err)
	}

	t.Log("QWP sender integration test passed")
}

// --- Validation tests ---

func TestQwpSenderSymbolDictAcrossFlushes(t *testing.T) {
	// Track sent messages to verify delta dict content.
	var messages [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			messages = append(messages, data)
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Flush 1: symbols AAPL, MSFT.
	s.Table("t").Symbol("sym", "AAPL").Int64Column("v", 1).AtNow(context.Background())
	s.Table("t").Symbol("sym", "MSFT").Int64Column("v", 2).AtNow(context.Background())
	s.Flush(context.Background())

	if s.maxSentSymbolId != 1 {
		t.Fatalf("after flush 1: maxSentSymbolId = %d, want 1", s.maxSentSymbolId)
	}

	// Flush 2: add symbol GOOG. Await delivery — Flush no longer
	// blocks on the ACK, and the message-bytes assertions below
	// require both frames to have reached the server. Awaiting the
	// second FSN implies the first is delivered too (FSN monotonic).
	s.Table("t").Symbol("sym", "GOOG").Int64Column("v", 3).AtNow(context.Background())
	flushAndAwaitAck(t, s)

	if s.maxSentSymbolId != 2 {
		t.Fatalf("after flush 2: maxSentSymbolId = %d, want 2", s.maxSentSymbolId)
	}

	// Verify we sent 2 messages.
	if len(messages) != 2 {
		t.Fatalf("messages sent = %d, want 2", len(messages))
	}

	// Verify first message has delta dict with symbols 0,1 (AAPL, MSFT).
	msg1 := messages[0]
	if msg1[5]&qwpFlagDeltaSymbolDict == 0 {
		t.Fatal("first message should have delta dict flag")
	}

	// Parse delta dict from first message.
	off := qwpHeaderSize
	deltaStart, n, _ := qwpReadVarint(msg1[off:])
	off += n
	if deltaStart != 0 {
		t.Fatalf("msg1 deltaStart = %d, want 0", deltaStart)
	}
	deltaCount, _, _ := qwpReadVarint(msg1[off:])
	if deltaCount != 2 {
		t.Fatalf("msg1 deltaCount = %d, want 2", deltaCount)
	}

	// Cursor mode emits self-sufficient frames: every batch carries
	// the full symbol dict from id 0. So the second message also
	// has deltaStart=0 (NOT 2), with all three symbols repeated.
	// This is the documented "self-sufficient frames" decision (see
	// design/qwp-cursor-durability.md decision #14).
	msg2 := messages[1]
	off = qwpHeaderSize
	deltaStart2, n, _ := qwpReadVarint(msg2[off:])
	off += n
	if deltaStart2 != 0 {
		t.Fatalf("msg2 deltaStart = %d, want 0 (cursor mode is self-sufficient)", deltaStart2)
	}
	deltaCount2, _, _ := qwpReadVarint(msg2[off:])
	if deltaCount2 != 3 {
		t.Fatalf("msg2 deltaCount = %d, want 3 (full dict re-sent)", deltaCount2)
	}
}

func TestQwpSenderServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			// Return PARSE_ERROR (default Halt). WRITE_ERROR is now
			// default Drop and would not surface a terminal Flush
			// error.
			errMsg := "bad message"
			ack := make([]byte, 11+len(errMsg))
			ack[0] = byte(QwpStatusParseError)
			binary.LittleEndian.PutUint16(ack[9:11], uint16(len(errMsg)))
			copy(ack[11:], errMsg)
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	s.Table("t").Int64Column("x", 1).AtNow(context.Background())
	// Flush no longer waits for the ACK, so the server's PARSE_ERROR
	// surfaces on the ACK-confirmation path (or, racily, already on
	// FlushAndGetSequence). Accept it from either.
	fsn, err := s.FlushAndGetSequence(context.Background())
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = s.AwaitAckedFsn(ctx, fsn)
	}
	if err == nil {
		t.Fatal("expected error from server")
	}

	var senderErr *SenderError
	if !errors.As(err, &senderErr) {
		t.Fatalf("expected *SenderError in chain, got %T: %v", err, err)
	}
	if senderErr.ServerStatusByte != int(QwpStatusParseError) {
		t.Fatalf("status = 0x%02X, want 0x%02X",
			senderErr.ServerStatusByte, byte(QwpStatusParseError))
	}
}

// --- Async sender tests ---

func TestQwpSenderAsyncBasic(t *testing.T) {
	// Mock server that counts messages.
	var mu sync.Mutex
	msgCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			msgCount++
			mu.Unlock()
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the cursor engine is wired (memory-backed, no sf_dir).
	if s.cursorEngine == nil || s.cursorSendLoop == nil {
		t.Fatal("cursor engine and send loop must be wired for QWP sender")
	}

	// Send 5 rows.
	for i := 0; i < 5; i++ {
		err := s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background())
		if err != nil {
			t.Fatalf("AtNow %d: %v", i, err)
		}
	}

	// Flush, then await ACK — Flush itself no longer blocks on the
	// server round-trip; the msgCount assertion needs delivery.
	flushAndAwaitAck(t, s)

	if s.pendingRowCount != 0 {
		t.Fatalf("pendingRowCount = %d, want 0", s.pendingRowCount)
	}

	mu.Lock()
	if msgCount != 1 {
		t.Fatalf("server received %d messages, want 1", msgCount)
	}
	mu.Unlock()

	// Close cleanly.
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestQwpSenderAsyncMultipleFlushes(t *testing.T) {
	var mu sync.Mutex
	msgCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			msgCount++
			mu.Unlock()
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Flush 1: 2 rows.
	for i := 0; i < 2; i++ {
		s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background())
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	// Flush 2: 3 rows. Await the second FSN — that implies the first
	// flush's frame is delivered too (FSN monotonic), so both frames
	// have reached the server before the msgCount assertion.
	for i := 0; i < 3; i++ {
		s.Table("t").Int64Column("x", int64(i+10)).AtNow(context.Background())
	}
	flushAndAwaitAck(t, s)

	mu.Lock()
	if msgCount != 2 {
		t.Fatalf("server received %d messages, want 2", msgCount)
	}
	mu.Unlock()
}

func TestQwpSenderAsyncCloseAutoFlush(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Add rows but don't flush — Close should auto-flush.
	s.Table("t").Int64Column("x", 1).AtNow(context.Background())
	s.Table("t").Int64Column("x", 2).AtNow(context.Background())

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestQwpAsyncSenderTerminalOnFlushFailure(t *testing.T) {
	// The cursor sender matches the Java client's flushPendingRows()
	// semantics: schema and symbol IDs are advanced immediately at
	// enqueue, not after ACK. If a batch later fails, the send loop
	// latches the terminal error (surfaced via sendLoopCheckError) and
	// every subsequent user-facing call returns it — so stale cache
	// state can never reach the wire on a live connection. This test
	// pins that invariant.

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		// Read the first message, then return a PARSE_ERROR
		// (default Halt). WRITE_ERROR is now default Drop and would
		// not poison the sender.
		_, _, err = conn.Read(context.Background())
		if err != nil {
			return
		}
		ack := buildAckError(QwpStatusParseError, 0, "bad message")
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Insert a row with a symbol (to exercise both schema and symbol paths).
	s.Table("t").Symbol("sym", "AAPL").Int64Column("x", 1).AtNow(context.Background())

	// Flush no longer waits for the ACK, so the server's PARSE_ERROR
	// surfaces on the ACK-confirmation path (AwaitAckedFsn) or, racily,
	// already on FlushAndGetSequence. Accept it from either.
	fsn, flushErr := s.FlushAndGetSequence(context.Background())
	if flushErr == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		flushErr = s.AwaitAckedFsn(ctx, fsn)
	}
	if flushErr == nil {
		t.Fatal("expected flush error, got nil")
	}

	// After the failure, the sender must be terminal: the next
	// flush must return the stored I/O error rather than try to
	// reuse the (now stale) schema/symbol state.
	s.Table("t").Int64Column("x", 2).AtNow(context.Background())
	if err := s.Flush(context.Background()); err == nil {
		t.Fatal("expected sender to be terminal after failure, got nil from second Flush")
	}
}

func TestQwpAsyncAutoFlushNonBlocking(t *testing.T) {
	// Verify that auto-flush in async mode enqueues batches without
	// blocking on ACKs. With window=4 and autoFlushRows=10, inserting
	// 30 rows should enqueue 3 batches concurrently (all in-flight),
	// not serialized (one at a time with ACK waits).

	// ackGate blocks the mock server from sending ACKs until closed.
	ackGate := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			// Block until the gate is opened.
			<-ackGate

			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	// window=4, autoFlushRows=10
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 10, 0, nil, 4)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 30 rows → triggers auto-flush at rows 10, 20, 30.
	// With non-blocking auto-flush, all 3 should be enqueued without
	// waiting for ACKs (window=4 has room for all 3).
	for i := 0; i < 30; i++ {
		err := s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background())
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	// All 30 rows have been inserted. The user goroutine returned
	// from AtNow without blocking. Verify that multiple batches are
	// in-flight (published into the engine but not yet ACKed).
	pub := s.cursorEngine.enginePublishedFsn()
	acked := s.cursorEngine.engineAckedFsn()
	inFlight := pub - acked
	if inFlight < 2 {
		t.Fatalf("expected at least 2 batches in-flight concurrently, got %d (published=%d acked=%d)",
			inFlight, pub, acked)
	}

	// Release the gate so the server can ACK all batches.
	close(ackGate)

	// Close the sender — this waits for all in-flight batches.
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestQwpAuthConfigParsing(t *testing.T) {
	t.Run("bearer_token", func(t *testing.T) {
		conf, err := confFromStr("ws::addr=localhost:9000;token=my_secret_token;")
		if err != nil {
			t.Fatal(err)
		}
		if conf.httpToken != "my_secret_token" {
			t.Fatalf("httpToken = %q, want %q", conf.httpToken, "my_secret_token")
		}
	})

	t.Run("basic_auth", func(t *testing.T) {
		conf, err := confFromStr("ws::addr=localhost:9000;username=admin;password=quest;")
		if err != nil {
			t.Fatal(err)
		}
		if conf.httpUser != "admin" {
			t.Fatalf("httpUser = %q, want %q", conf.httpUser, "admin")
		}
		if conf.httpPass != "quest" {
			t.Fatalf("httpPass = %q, want %q", conf.httpPass, "quest")
		}
	})

	t.Run("basic_and_token_conflict", func(t *testing.T) {
		// Validation happens at sender creation, not parsing.
		_, err := LineSenderFromConf(context.Background(),
			"ws::addr=localhost:9000;username=admin;password=quest;token=tok;")
		if err == nil {
			t.Fatal("expected error for conflicting basic + token auth")
		}
		if !strings.Contains(err.Error(), "both basic and token") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestQwpAuthHeaderFormat(t *testing.T) {
	// Verify the exact Authorization header sent during WebSocket upgrade.

	t.Run("bearer", func(t *testing.T) {
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			w.Header().Set(qwpHeaderVersion, "1")
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var seq int64
			for {
				_, _, err := conn.Read(context.Background())
				if err != nil {
					return
				}
				conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
				seq++
			}
		}))
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		opts := qwpTransportOpts{
			authorization: "Bearer my_token",
			endpointPath:  qwpWritePath,
		}
		s, err := newQwpLineSender(context.Background(), wsURL, opts, 0, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.Close(context.Background())

		if gotAuth != "Bearer my_token" {
			t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer my_token")
		}
	})

	t.Run("basic", func(t *testing.T) {
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			w.Header().Set(qwpHeaderVersion, "1")
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var seq int64
			for {
				_, _, err := conn.Read(context.Background())
				if err != nil {
					return
				}
				conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
				seq++
			}
		}))
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		opts := qwpTransportOpts{
			authorization: "Basic YWRtaW46cXVlc3Q=", // base64("admin:quest")
			endpointPath:  qwpWritePath,
		}
		s, err := newQwpLineSender(context.Background(), wsURL, opts, 0, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.Close(context.Background())

		if gotAuth != "Basic YWRtaW46cXVlc3Q=" {
			t.Fatalf("Authorization header = %q, want %q", gotAuth, "Basic YWRtaW46cXVlc3Q=")
		}
	})

	t.Run("from_config_bearer", func(t *testing.T) {
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			w.Header().Set(qwpHeaderVersion, "1")
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var seq int64
			for {
				_, _, err := conn.Read(context.Background())
				if err != nil {
					return
				}
				conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
				seq++
			}
		}))
		defer srv.Close()

		addr := strings.TrimPrefix(srv.URL, "http://")
		confStr := fmt.Sprintf("ws::addr=%s;token=secret123;", addr)
		s, err := LineSenderFromConf(context.Background(), confStr)
		if err != nil {
			t.Fatal(err)
		}
		s.Close(context.Background())

		if gotAuth != "Bearer secret123" {
			t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer secret123")
		}
	})

	t.Run("from_config_basic", func(t *testing.T) {
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			w.Header().Set(qwpHeaderVersion, "1")
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var seq int64
			for {
				_, _, err := conn.Read(context.Background())
				if err != nil {
					return
				}
				conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
				seq++
			}
		}))
		defer srv.Close()

		addr := strings.TrimPrefix(srv.URL, "http://")
		confStr := fmt.Sprintf("ws::addr=%s;username=admin;password=quest;", addr)
		s, err := LineSenderFromConf(context.Background(), confStr)
		if err != nil {
			t.Fatal(err)
		}
		s.Close(context.Background())

		// base64("admin:quest") = "YWRtaW46cXVlc3Q="
		want := "Basic YWRtaW46cXVlc3Q="
		if gotAuth != want {
			t.Fatalf("Authorization header = %q, want %q", gotAuth, want)
		}
	})
}

func TestQwpMaxBufSizeTriggersFlush(t *testing.T) {
	// Verify that when maxBufSize is set and the accumulated buffer
	// size exceeds it, At() triggers an auto-flush.

	var messageCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			messageCount++
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Set a small max buffer size (200 bytes). Each LONG column is 8 bytes,
	// plus null bitmap flag, plus schema overhead per row. With ~25 rows
	// of one LONG column, we should exceed 200 bytes and trigger a flush.
	s.maxBufSize = 200

	// Insert rows without explicit flush. Auto-flush by maxBufSize should
	// trigger before we run out of rows.
	for i := 0; i < 50; i++ {
		err := s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background())
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	// Explicit flush for remaining rows, then await delivery — the
	// messageCount assertion needs the frames on the wire, and Flush
	// no longer blocks on the ACK.
	flushAndAwaitAck(t, s)

	// We should have received at least 2 messages: one from the
	// maxBufSize-triggered flush and one from the explicit Flush.
	if messageCount < 2 {
		t.Fatalf("expected at least 2 messages (maxBufSize flush + final), got %d", messageCount)
	}
}

func TestQwpMaxBufSizeFromConfig(t *testing.T) {
	// Verify that maxBufSize is accepted from config strings.
	srv := newQwpTestServer(t)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	confStr := fmt.Sprintf("ws::addr=%s;max_buf_size=1024;", addr)
	s, err := LineSenderFromConf(context.Background(), confStr)
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer s.Close(context.Background())

	// Verify the sender was created successfully with maxBufSize.
	// Insert some data to verify it works.
	s.Table("t").Int64Column("x", 1).AtNow(context.Background())
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// --- Phase 15: Edge Case & Validation Tests ---

func TestQwpSenderTableNameLengthValidation(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())
	s.fileNameLimit = 127

	t.Run("AtLimit", func(t *testing.T) {
		name := strings.Repeat("a", 127)
		s.Table(name)
		err := s.Int64Column("x", 1).AtNow(context.Background())
		if err != nil {
			t.Fatalf("table name at limit (127) should succeed: %v", err)
		}
		s.Flush(context.Background())
	})

	t.Run("OverLimit", func(t *testing.T) {
		name := strings.Repeat("a", 128)
		s.Table(name)
		err := s.AtNow(context.Background())
		if err == nil {
			t.Fatal("expected error for table name over limit (128)")
		}
		if !strings.Contains(err.Error(), "exceeds limit") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestQwpSenderColumnNameLengthValidation(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())
	s.fileNameLimit = 127

	t.Run("AtLimit", func(t *testing.T) {
		colName := strings.Repeat("c", 127)
		s.Table("t")
		err := s.Int64Column(colName, 42).AtNow(context.Background())
		if err != nil {
			t.Fatalf("column name at limit (127) should succeed: %v", err)
		}
		s.Flush(context.Background())
	})

	t.Run("OverLimit", func(t *testing.T) {
		colName := strings.Repeat("c", 128)
		s.Table("t")
		err := s.Int64Column(colName, 42).AtNow(context.Background())
		if err == nil {
			t.Fatal("expected error for column name over limit (128)")
		}
		if !strings.Contains(err.Error(), "exceeds limit") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestQwpSenderGeohashPrecisionValidation(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	tests := []struct {
		name      string
		precision int
		wantErr   bool
	}{
		{"precision_0", 0, true},
		{"precision_1", 1, false},
		{"precision_30", 30, false},
		{"precision_60", 60, false},
		{"precision_61", 61, true},
		{"precision_negative", -1, true},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use unique table+column names per subtest to avoid precision conflicts.
			tblName := fmt.Sprintf("t%d", i)
			s.Table(tblName)
			err := s.GeohashColumn("geo", 0xABC, tt.precision).AtNow(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for precision %d", tt.precision)
				}
				if !strings.Contains(err.Error(), "out of range") {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("precision %d should be valid: %v", tt.precision, err)
				}
				s.Flush(context.Background())
			}
		})
	}
}

func TestQwpSenderTimestampNanosColumnTypeCode(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456789, time.UTC)
	s.Table("t")
	err := s.TimestampNanosColumn("tsn", ts).
		AtNow(context.Background())
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	// Verify the column uses qwpTypeTimestampNano (0x10) not qwpTypeTimestamp (0x0A).
	tb := s.tableBuffers["t"]
	if tb == nil {
		t.Fatal("table buffer not found")
	}
	var col *qwpColumnBuffer
	for _, c := range tb.columns {
		if c.name == "tsn" {
			col = c
			break
		}
	}
	if col == nil {
		t.Fatal("column 'tsn' not found")
	}
	if col.typeCode != qwpTypeTimestampNano {
		t.Fatalf("typeCode = 0x%02X, want 0x%02X (qwpTypeTimestampNano)",
			col.typeCode, qwpTypeTimestampNano)
	}

	// Verify the stored value is nanoseconds, not microseconds.
	expectedNanos := ts.UnixNano()
	if len(col.fixedData) < 8 {
		t.Fatal("fixedData too short")
	}
	storedVal := int64(binary.LittleEndian.Uint64(col.fixedData[:8]))
	if storedVal != expectedNanos {
		t.Fatalf("stored value = %d, want %d (nanoseconds)", storedVal, expectedNanos)
	}
}

func TestQwpSenderAtNanoDesignatedTimestamp(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456789, time.UTC)
	s.Table("t").Int64Column("v", 1)
	if err := s.AtNano(context.Background(), ts); err != nil {
		t.Fatalf("AtNano: %v", err)
	}

	tb := s.tableBuffers["t"]
	if tb == nil {
		t.Fatal("table buffer not found")
	}
	// Designated timestamp column uses empty name.
	var col *qwpColumnBuffer
	for _, c := range tb.columns {
		if c.name == "" {
			col = c
			break
		}
	}
	if col == nil {
		t.Fatal("designated timestamp column not found")
	}
	if col.typeCode != qwpTypeTimestampNano {
		t.Fatalf("designated ts typeCode = 0x%02X, want 0x%02X (qwpTypeTimestampNano)",
			col.typeCode, qwpTypeTimestampNano)
	}
	if len(col.fixedData) < 8 {
		t.Fatal("fixedData too short")
	}
	storedVal := int64(binary.LittleEndian.Uint64(col.fixedData[:8]))
	if storedVal != ts.UnixNano() {
		t.Fatalf("stored designated ts = %d, want %d (nanoseconds)", storedVal, ts.UnixNano())
	}
}

func TestQwpSenderAtAndAtNanoConflict(t *testing.T) {
	// Mixing At (micros) and AtNano (nanos) on the same table within
	// one flush must fail: the designated timestamp column type is
	// fixed once chosen.
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456789, time.UTC)
	ctx := context.Background()
	if err := s.Table("t").Int64Column("v", 1).At(ctx, ts); err != nil {
		t.Fatalf("At (micros) on first row: %v", err)
	}
	s.Table("t").Int64Column("v", 2)
	err := s.AtNano(ctx, ts.Add(time.Microsecond))
	if err == nil {
		t.Fatal("expected type-conflict error when switching to AtNano, got nil")
	}
	if !strings.Contains(err.Error(), "designated timestamp type conflict") {
		t.Fatalf("unexpected error: %v", err)
	}

	// The reverse direction (AtNano first, then At) should also fail
	// on a fresh table.
	s.Table("t2").Int64Column("v", 1)
	if err := s.AtNano(ctx, ts); err != nil {
		t.Fatalf("AtNano (nanos) on first row: %v", err)
	}
	err = s.Table("t2").Int64Column("v", 2).At(ctx, ts.Add(time.Microsecond))
	if err == nil {
		t.Fatal("expected type-conflict error when switching back to At, got nil")
	}
	if !strings.Contains(err.Error(), "designated timestamp type conflict") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestQwpSenderObservabilityCounters verifies the spec §20 counter
// accessors are wired through the QwpSender interface to the
// underlying send loop / engine / drainer pool. A fresh sender on a
// happy-path test server should report zero on every counter both
// before and after a successful flush, and BackgroundDrainers()
// should be nil on a memory-backed sender (no SF, no orphan
// adoption).
func TestQwpSenderObservabilityCounters(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// Reach the accessors through the interface to lock the public
	// surface in place — a missing method would fail to compile.
	var qs QwpSender = s

	if got := qs.TotalReconnectAttempts(); got != 0 {
		t.Fatalf("TotalReconnectAttempts on fresh sender = %d, want 0", got)
	}
	if got := qs.TotalReconnectsSucceeded(); got != 0 {
		t.Fatalf("TotalReconnectsSucceeded on fresh sender = %d, want 0", got)
	}
	if got := qs.TotalFramesReplayed(); got != 0 {
		t.Fatalf("TotalFramesReplayed on fresh sender = %d, want 0", got)
	}
	if got := qs.TotalBackpressureStalls(); got != 0 {
		t.Fatalf("TotalBackpressureStalls on fresh sender = %d, want 0", got)
	}
	if got := qs.BackgroundDrainers(); got != nil {
		t.Fatalf("BackgroundDrainers on memory-backed sender = %v, want nil", got)
	}

	if err := qs.Table("t").Int64Column("v", 1).AtNow(context.Background()); err != nil {
		t.Fatalf("AtNow: %v", err)
	}
	if err := qs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A clean flush against the happy-path server must not have
	// triggered any reconnects, replays, or backpressure stalls.
	if got := qs.TotalReconnectAttempts(); got != 0 {
		t.Fatalf("TotalReconnectAttempts after clean flush = %d, want 0", got)
	}
	if got := qs.TotalReconnectsSucceeded(); got != 0 {
		t.Fatalf("TotalReconnectsSucceeded after clean flush = %d, want 0", got)
	}
	if got := qs.TotalFramesReplayed(); got != 0 {
		t.Fatalf("TotalFramesReplayed after clean flush = %d, want 0", got)
	}
	if got := qs.TotalBackpressureStalls(); got != 0 {
		t.Fatalf("TotalBackpressureStalls after clean flush = %d, want 0", got)
	}
	if got := qs.BackgroundDrainers(); got != nil {
		t.Fatalf("BackgroundDrainers after clean flush = %v, want nil", got)
	}
}
