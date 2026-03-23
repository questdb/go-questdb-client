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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestQwpSenderAutoFlushTimeInterval(t *testing.T) {
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
	// autoFlushRows=0 (disabled), autoFlushInterval=10ms.
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	// First row: initializes the deadline but does not flush.
	err = s.Table("t").Int64Column("x", int64(1)).AtNow(context.Background())
	if err != nil {
		t.Fatalf("row 1: %v", err)
	}
	if msgCount != 0 {
		t.Fatalf("after row 1: msgCount = %d, want 0", msgCount)
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
	if msgCount != 1 {
		t.Fatalf("after row 2: msgCount = %d, want 1 (time-based flush)", msgCount)
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0)
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

// --- Async sender tests ---

func TestQwpSenderAsyncBasic(t *testing.T) {
	// Mock server that counts messages.
	var mu sync.Mutex
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
			mu.Lock()
			msgCount++
			mu.Unlock()
			ack := make([]byte, 9)
			ack[0] = qwpWireStatusOK
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, 2)
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
			mu.Lock()
			msgCount++
			mu.Unlock()
			ack := make([]byte, 9)
			ack[0] = qwpWireStatusOK
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, 3)
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
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, 2)
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

func TestQwpSchemaKeyDistinguishesTables(t *testing.T) {
	// Two tables with identical column definitions must get different
	// schema cache keys. Without table-name-aware keying, the second
	// table would incorrectly reuse the first table's cached schema
	// and the server would reject it.

	// Unit test for qwpSchemaKey: same schema hash, different tables.
	schemaHash := int64(0x1234567890ABCDEF)
	keyA := qwpSchemaKey("tableA", schemaHash)
	keyB := qwpSchemaKey("tableB", schemaHash)
	if keyA == keyB {
		t.Fatalf("schema keys should differ for different table names, both = %d", keyA)
	}

	// Same table + same schema hash must produce the same key.
	keyA2 := qwpSchemaKey("tableA", schemaHash)
	if keyA != keyA2 {
		t.Fatalf("same table+schema should produce same key: %d != %d", keyA, keyA2)
	}
}

func TestQwpSenderSchemaKeyPerTable(t *testing.T) {
	// Verify that two tables with identical columns both get full
	// schema mode on first flush (not schema reference mode).
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
			messages = append(messages, append([]byte(nil), data...))
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

	// Insert one row into each of two tables with identical columns.
	s.Table("alpha").Int64Column("x", 1).AtNow(context.Background())
	s.Table("beta").Int64Column("x", 2).AtNow(context.Background())
	s.Flush(context.Background())

	// Should have sent 2 messages (one per table in sync mode).
	if len(messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(messages))
	}

	// Both messages must use full schema mode (0x00), not reference (0x01).
	for i, msg := range messages {
		schemaMode := extractSchemaMode(t, msg)
		if schemaMode != byte(qwpSchemaModeFull) {
			t.Fatalf("message %d: schemaMode = 0x%02X, want 0x%02X (full)",
				i, schemaMode, qwpSchemaModeFull)
		}
	}

	// After first flush, both keys should be cached.
	if len(s.sentSchemaHashes) != 2 {
		t.Fatalf("sentSchemaHashes = %d, want 2", len(s.sentSchemaHashes))
	}

	// Second flush of both tables should now use schema reference.
	messages = messages[:0]
	s.Table("alpha").Int64Column("x", 3).AtNow(context.Background())
	s.Table("beta").Int64Column("x", 4).AtNow(context.Background())
	s.Flush(context.Background())

	if len(messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(messages))
	}
	for i, msg := range messages {
		schemaMode := extractSchemaMode(t, msg)
		if schemaMode != byte(qwpSchemaModeReference) {
			t.Fatalf("message %d (2nd flush): schemaMode = 0x%02X, want 0x%02X (ref)",
				i, schemaMode, qwpSchemaModeReference)
		}
	}
}

// extractSchemaMode parses a QWP message and returns the schema mode
// byte. It skips the header, delta dict (if present), table name,
// rowCount, and colCount to reach the schema mode byte.
func extractSchemaMode(t *testing.T, msg []byte) byte {
	t.Helper()
	if len(msg) < qwpHeaderSize {
		t.Fatalf("message too short: %d", len(msg))
	}

	off := qwpHeaderSize
	flags := msg[qwpHeaderOffsetFlags]

	// Skip delta dict if present.
	if flags&qwpFlagDeltaSymbolDict != 0 {
		// deltaStart varint
		_, n, err := qwpReadVarint(msg[off:])
		if err != nil {
			t.Fatalf("read deltaStart: %v", err)
		}
		off += n

		// deltaCount varint
		deltaCount, n, err := qwpReadVarint(msg[off:])
		if err != nil {
			t.Fatalf("read deltaCount: %v", err)
		}
		off += n

		// Skip delta symbol strings.
		for i := uint64(0); i < deltaCount; i++ {
			strLen, n, err := qwpReadVarint(msg[off:])
			if err != nil {
				t.Fatalf("read symbol len: %v", err)
			}
			off += n + int(strLen)
		}
	}

	// Skip table name (varint string).
	nameLen, n, err := qwpReadVarint(msg[off:])
	if err != nil {
		t.Fatalf("read table name len: %v", err)
	}
	off += n + int(nameLen)

	// Skip rowCount varint.
	_, n, err = qwpReadVarint(msg[off:])
	if err != nil {
		t.Fatalf("read rowCount: %v", err)
	}
	off += n

	// Skip colCount varint.
	_, n, err = qwpReadVarint(msg[off:])
	if err != nil {
		t.Fatalf("read colCount: %v", err)
	}
	off += n

	// Schema mode byte.
	return msg[off]
}
