/*******************************************************************************
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
	"context"
	"encoding/binary"
	"fmt"
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("newQwpLineSender: %v", err)
	}
	return s
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

// TestQwpSyncFlushAbsorbsStaleAck verifies that sync-mode flushSync
// ignores an ACK whose cumulative sequence is older than the batch it
// just sent and keeps reading until the matching ACK arrives. Matches
// Java's waitForAck, which tolerates stale ACKs on the same connection.
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

	if got := s.syncSequence; got != 3 {
		t.Fatalf("syncSequence = %d, want 3", got)
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
	// Mock server that counts received messages.
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 3, 0, nil)
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

	// Auto-flush should have triggered at row 3.
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
	// Mock server that counts received messages.
	var mu sync.Mutex
	msgCount := 0
	readMsgCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return msgCount
	}
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
	// autoFlushRows=0 (disabled), autoFlushInterval=10ms.
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 10*time.Millisecond, nil)
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

	// Second row: should trigger time-based auto-flush.
	err = s.Table("t").Int64Column("x", int64(2)).AtNow(context.Background())
	if err != nil {
		t.Fatalf("row 2: %v", err)
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil)
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

func TestQwpSenderVarcharColumn(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	s.Table("t")
	err := s.VarcharColumn("v", "hello world").
		VarcharColumn("empty", "").
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
		VarcharColumn("vc", "test").
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

func TestQwpSenderIntegration(t *testing.T) {
	ctx := context.Background()
	s, err := newQwpLineSender(ctx, "ws://localhost:9000", qwpTransportOpts{}, time.Second, 0, 0, nil)
	if err != nil {
		t.Skipf("QuestDB not available: %v", err)
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

	// Second flush with same schema should use reference mode.
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

	// Verify schema was registered (schema ID advanced past -1).
	if s.maxSentSchemaId < 0 {
		t.Fatal("maxSentSchemaId should have advanced after flush")
	}

	t.Log("QWP sender integration test passed")
}

// --- Validation tests ---

func TestQwpSenderSchemaIdCaching(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// First flush: full schema; the table should be assigned
	// schemaId 0 and it should be promoted to maxSentSchemaId.
	s.Table("t").Int64Column("x", 1).AtNow(context.Background())
	s.Flush(context.Background())

	tb := s.tableBuffers["t"]
	if tb == nil || tb.schemaId != 0 {
		t.Fatalf("first flush: table schemaId = %v, want 0", tb)
	}
	if s.maxSentSchemaId != 0 {
		t.Fatalf("first flush: maxSentSchemaId = %d, want 0", s.maxSentSchemaId)
	}
	if s.nextSchemaId != 1 {
		t.Fatalf("first flush: nextSchemaId = %d, want 1", s.nextSchemaId)
	}

	// Second flush: same column set, should reuse schemaId and not
	// allocate a new one.
	s.Table("t").Int64Column("x", 2).AtNow(context.Background())
	s.Flush(context.Background())

	if tb.schemaId != 0 {
		t.Fatalf("second flush: schemaId = %d, want 0 (same column set)", tb.schemaId)
	}
	if s.nextSchemaId != 1 {
		t.Fatalf("second flush: nextSchemaId = %d, want 1 (no new ID allocated)", s.nextSchemaId)
	}
}

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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil)
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

	// Flush 2: add symbol GOOG.
	s.Table("t").Symbol("sym", "GOOG").Int64Column("v", 3).AtNow(context.Background())
	s.Flush(context.Background())

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
	deltaCount, n, _ := qwpReadVarint(msg1[off:])
	off += n
	if deltaCount != 2 {
		t.Fatalf("msg1 deltaCount = %d, want 2", deltaCount)
	}

	// Parse second message: delta should start at 2 with count 1.
	msg2 := messages[1]
	off = qwpHeaderSize
	deltaStart2, n, _ := qwpReadVarint(msg2[off:])
	off += n
	if deltaStart2 != 2 {
		t.Fatalf("msg2 deltaStart = %d, want 2", deltaStart2)
	}
	deltaCount2, n, _ := qwpReadVarint(msg2[off:])
	off += n
	if deltaCount2 != 1 {
		t.Fatalf("msg2 deltaCount = %d, want 1", deltaCount2)
	}

	// Verify the new symbol is "GOOG".
	symLen, n, _ := qwpReadVarint(msg2[off:])
	off += n
	sym := string(msg2[off : off+int(symLen)])
	if sym != "GOOG" {
		t.Fatalf("msg2 delta symbol = %q, want %q", sym, "GOOG")
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
			// Return WRITE_ERROR.
			errMsg := "table error"
			ack := make([]byte, 11+len(errMsg))
			ack[0] = byte(qwpStatusWriteError)
			binary.LittleEndian.PutUint16(ack[9:11], uint16(len(errMsg)))
			copy(ack[11:], errMsg)
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	s.Table("t").Int64Column("x", 1).AtNow(context.Background())
	err = s.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from server")
	}

	qErr, ok := err.(*QwpError)
	if !ok {
		t.Fatalf("expected *QwpError, got %T: %v", err, err)
	}
	if qErr.Status != qwpStatusWriteError {
		t.Fatalf("status = %d, want %d", qErr.Status, qwpStatusWriteError)
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify async mode is enabled.
	if s.asyncState == nil {
		t.Fatal("asyncState should not be nil for window=2")
	}

	// Send 5 rows.
	for i := 0; i < 5; i++ {
		err := s.Table("t").Int64Column("x", int64(i)).AtNow(context.Background())
		if err != nil {
			t.Fatalf("AtNow %d: %v", i, err)
		}
	}

	// Flush — waits for all batches to be ACKed.
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil, 3)
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

	// Flush 2: 3 rows.
	for i := 0; i < 3; i++ {
		s.Table("t").Int64Column("x", int64(i+10)).AtNow(context.Background())
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil, 2)
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

func TestQwpSenderSchemaIdPerTable(t *testing.T) {
	// Verify that two tables with identical columns both get full
	// schema mode on first flush (not schema reference mode).
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
			messages = append(messages, append([]byte(nil), data...))
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Insert one row into each of two tables with identical columns.
	s.Table("alpha").Int64Column("x", 1).AtNow(context.Background())
	s.Table("beta").Int64Column("x", 2).AtNow(context.Background())
	s.Flush(context.Background())

	// With multi-table batching, both tables are in 1 message.
	if len(messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(messages))
	}

	// Both tables in the message must use full schema mode.
	modes := extractAllSchemaModes(t, messages[0])
	if len(modes) != 2 {
		t.Fatalf("tables in message = %d, want 2", len(modes))
	}
	for i, mode := range modes {
		if mode != byte(qwpSchemaModeFull) {
			t.Fatalf("table %d: schemaMode = 0x%02X, want 0x%02X (full)",
				i, mode, qwpSchemaModeFull)
		}
	}

	// After first flush, both tables should have distinct schema IDs
	// and maxSentSchemaId should have advanced to cover both.
	if s.tableBuffers["alpha"].schemaId == s.tableBuffers["beta"].schemaId {
		t.Fatalf("tables must have distinct schema IDs, both = %d",
			s.tableBuffers["alpha"].schemaId)
	}
	if s.maxSentSchemaId != 1 {
		t.Fatalf("maxSentSchemaId = %d, want 1", s.maxSentSchemaId)
	}
	if s.nextSchemaId != 2 {
		t.Fatalf("nextSchemaId = %d, want 2", s.nextSchemaId)
	}

	// Second flush of both tables should now use schema reference.
	messages = messages[:0]
	s.Table("alpha").Int64Column("x", 3).AtNow(context.Background())
	s.Table("beta").Int64Column("x", 4).AtNow(context.Background())
	s.Flush(context.Background())

	if len(messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(messages))
	}
	modes = extractAllSchemaModes(t, messages[0])
	for i, mode := range modes {
		if mode != byte(qwpSchemaModeReference) {
			t.Fatalf("table %d (2nd flush): schemaMode = 0x%02X, want 0x%02X (ref)",
				i, mode, qwpSchemaModeReference)
		}
	}
}

// extractAllSchemaModes parses a multi-table QWP message and returns
// the schema mode byte for each table block. It skips the header,
// delta dict, and then for each table: extracts the schema mode and
// skips the rest of the table block.
//
// Precondition: every table in the message has exactly one non-null
// LONG column. In full mode the helper asserts the type byte; in
// reference mode the caller is responsible for maintaining the same
// shape across flushes. The only caller today is
// TestQwpSenderSchemaIdPerTable, which uses Int64Column("x", ...).
func extractAllSchemaModes(t *testing.T, msg []byte) []byte {
	t.Helper()
	if len(msg) < qwpHeaderSize {
		t.Fatalf("message too short: %d", len(msg))
	}

	tableCount := binary.LittleEndian.Uint16(msg[6:8])
	off := qwpHeaderSize
	flags := msg[qwpHeaderOffsetFlags]

	// Skip delta dict if present.
	if flags&qwpFlagDeltaSymbolDict != 0 {
		_, n, _ := qwpReadVarint(msg[off:])
		off += n
		deltaCount, n, _ := qwpReadVarint(msg[off:])
		off += n
		for i := uint64(0); i < deltaCount; i++ {
			slen, n, _ := qwpReadVarint(msg[off:])
			off += n + int(slen)
		}
	}

	var modes []byte
	for ti := uint16(0); ti < tableCount; ti++ {
		// Skip table name.
		nameLen, n, _ := qwpReadVarint(msg[off:])
		off += n + int(nameLen)
		// Row count — needed to size the column data skip.
		rowCount, n, _ := qwpReadVarint(msg[off:])
		off += n
		// Column count.
		colCount, n, _ := qwpReadVarint(msg[off:])
		off += n
		if colCount != 1 {
			t.Fatalf("table %d: colCount=%d, helper only supports 1 column",
				ti, colCount)
		}
		// Schema mode byte.
		schemaMode := msg[off]
		modes = append(modes, schemaMode)
		off++
		// Schema ID varint (both modes per QWP spec §9).
		_, n, _ = qwpReadVarint(msg[off:])
		off += n

		if schemaMode == byte(qwpSchemaModeFull) {
			// Full schema: name string + type byte.
			slen, n, _ := qwpReadVarint(msg[off:])
			off += n + int(slen)
			if tc := qwpTypeCode(msg[off]); tc != qwpTypeLong {
				t.Fatalf("table %d: column type=0x%02X, helper only supports qwpTypeLong",
					ti, tc)
			}
			off++
		}

		// Column data: null bitmap flag (1 byte, asserted 0x00 = no
		// nulls) followed by rowCount × 8 bytes for the LONG values.
		if msg[off] != 0x00 {
			t.Fatalf("table %d: null bitmap flag=0x%02X, helper requires non-null values",
				ti, msg[off])
		}
		off += 1 + int(rowCount)*8
	}

	return modes
}

func TestQwpAsyncSenderTerminalOnFlushFailure(t *testing.T) {
	// In async mode the sender matches the Java client's
	// flushPendingRows() semantics: schema and symbol IDs are
	// advanced immediately after enqueue, not after ACK. If a batch
	// later fails, the sender is poisoned via asyncState.ioErr and
	// every subsequent user-facing call returns that error — so
	// stale cache state can never reach the wire on a live
	// connection. This test pins that invariant.

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		// Read the first message, then return a WRITE_ERROR.
		_, _, err = conn.Read(context.Background())
		if err != nil {
			return
		}
		ack := make([]byte, 9)
		ack[0] = byte(qwpStatusWriteError)
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// Insert a row with a symbol (to exercise both schema and symbol paths).
	s.Table("t").Symbol("sym", "AAPL").Int64Column("x", 1).AtNow(context.Background())

	// Flush returns the WRITE_ERROR from the server.
	flushErr := s.Flush(context.Background())
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 10, 0, nil, 4)
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
	// in-flight (enqueued but not yet ACKed).
	s.asyncState.mu.Lock()
	count := s.asyncState.inFlightCount
	s.asyncState.mu.Unlock()

	if count < 2 {
		t.Fatalf("expected at least 2 batches in-flight concurrently, got %d", count)
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
		}
		s, err := newQwpLineSender(context.Background(), wsURL, opts, 0, 0, 0, nil)
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
		}
		s, err := newQwpLineSender(context.Background(), wsURL, opts, 0, 0, 0, nil)
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, nil)
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

	// Explicit flush for remaining rows.
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

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
