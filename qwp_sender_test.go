/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// newQwpTestServer creates a mock WebSocket server that accepts
// QWP messages and responds with OK ACKs.
func newQwpTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			t.Logf("websocket accept error: %v", err)
			return
		}
		defer conn.CloseNow()

		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			// Send OK ACK with sequence=0.
			ack := make([]byte, 9)
			ack[0] = qwpWireStatusOK
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
}

// newQwpSenderForTest creates a QWP sender connected to the given
// test server URL.
func newQwpSenderForTest(t *testing.T, serverURL string) *qwpLineSender {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0)
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
	msgCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			return
		}
		defer conn.CloseNow()

		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			msgCount++
			ack := make([]byte, 9)
			ack[0] = qwpWireStatusOK
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 3, 0)
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
	if msgCount != 1 {
		t.Fatalf("auto-flush messages = %d, want 1", msgCount)
	}
	if s.pendingRowCount != 2 {
		t.Fatalf("pendingRowCount = %d, want 2", s.pendingRowCount)
	}
}

// --- Integration test ---

func TestQwpSenderIntegration(t *testing.T) {
	ctx := context.Background()
	s, err := newQwpLineSender(ctx, "ws://localhost:9000", qwpTransportOpts{}, time.Second, 0, 0)
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

	// Verify schema was cached.
	if len(s.sentSchemaHashes) == 0 {
		t.Fatal("sentSchemaHashes should not be empty after flush")
	}

	t.Log("QWP sender integration test passed")
}

// --- Validation tests ---

func TestQwpSenderSchemaHashCaching(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	// First flush: full schema.
	s.Table("t").Int64Column("x", 1).AtNow(context.Background())
	s.Flush(context.Background())

	if len(s.sentSchemaHashes) != 1 {
		t.Fatalf("sentSchemaHashes count = %d, want 1", len(s.sentSchemaHashes))
	}

	// Second flush: should use cached schema.
	s.Table("t").Int64Column("x", 2).AtNow(context.Background())
	s.Flush(context.Background())

	// Hash count should still be 1 (same schema).
	if len(s.sentSchemaHashes) != 1 {
		t.Fatalf("sentSchemaHashes count = %d, want 1", len(s.sentSchemaHashes))
	}
}

func TestQwpSenderSymbolDictAcrossFlushes(t *testing.T) {
	// Track sent messages to verify delta dict content.
	var messages [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			messages = append(messages, data)
			ack := make([]byte, 9)
			ack[0] = qwpWireStatusOK
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0)
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
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
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
			ack[0] = qwpWireStatusWriteError
			binary.LittleEndian.PutUint16(ack[9:11], uint16(len(errMsg)))
			copy(ack[11:], errMsg)
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0)
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
	if qErr.Status != qwpWireStatusWriteError {
		t.Fatalf("status = %d, want %d", qErr.Status, qwpWireStatusWriteError)
	}
}
