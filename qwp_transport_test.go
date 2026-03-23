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

// --- Unit tests for ACK parsing ---

// buildAckOK builds a minimal OK ACK response (9 bytes).
func buildAckOK(seq int64) []byte {
	data := make([]byte, 9)
	data[0] = byte(qwpStatusOK)
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	return data
}

// buildAckError builds an error ACK response with message.
func buildAckError(status qwpStatusCode, seq int64, errMsg string) []byte {
	data := make([]byte, 11+len(errMsg))
	data[0] = byte(status)
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	binary.LittleEndian.PutUint16(data[9:11], uint16(len(errMsg)))
	copy(data[11:], errMsg)
	return data
}

func TestQwpParseAckError(t *testing.T) {
	t.Run("OK_NoError", func(t *testing.T) {
		data := buildAckOK(0)
		msg := parseAckError(data)
		if msg != "" {
			t.Fatalf("expected empty, got %q", msg)
		}
	})

	t.Run("ErrorWithMessage", func(t *testing.T) {
		errMsg := "bad data"
		data := buildAckError(qwpStatusParseError, 1, errMsg)

		msg := parseAckError(data)
		if msg != errMsg {
			t.Fatalf("parseAckError = %q, want %q", msg, errMsg)
		}
	})

	t.Run("EmptyErrorMessage", func(t *testing.T) {
		data := buildAckError(qwpStatusInternalError, 2, "")
		msg := parseAckError(data)
		if msg != "" {
			t.Fatalf("expected empty, got %q", msg)
		}
	})

	t.Run("TruncatedPayload", func(t *testing.T) {
		// Build valid header then truncate data.
		data := buildAckError(qwpStatusParseError, 3, "long error")
		data = data[:13] // truncate to only 2 of the message bytes
		msg := parseAckError(data)
		if msg != "" {
			t.Fatalf("expected empty for truncated, got %q", msg)
		}
	})

	t.Run("TooShortForLength", func(t *testing.T) {
		// Only status + sequence, no error length field.
		data := buildAckOK(0)
		msg := parseAckError(data)
		if msg != "" {
			t.Fatalf("expected empty, got %q", msg)
		}
	})

	t.Run("AllStatusCodes", func(t *testing.T) {
		codes := []qwpStatusCode{
			qwpStatusPartial,
			qwpStatusSchemaRequired,
			qwpStatusSchemaMismatch,
			qwpStatusTableNotFound,
			qwpStatusParseError,
			qwpStatusInternalError,
			qwpStatusOverloaded,
		}
		for _, code := range codes {
			errMsg := "error for status"
			data := buildAckError(code, 42, errMsg)

			msg := parseAckError(data)
			if msg != errMsg {
				t.Fatalf("status 0x%02X: parseAckError = %q, want %q",
					code, msg, errMsg)
			}
		}
	})
}

func TestQwpParseAckSequence(t *testing.T) {
	data := buildAckOK(12345)
	seq := parseAckSequence(data)
	if seq != 12345 {
		t.Fatalf("sequence = %d, want 12345", seq)
	}

	// Error response should also have sequence.
	dataErr := buildAckError(qwpStatusParseError, 99, "err")
	seq = parseAckSequence(dataErr)
	if seq != 99 {
		t.Fatalf("sequence = %d, want 99", seq)
	}
}

func TestQwpTransportNotConnected(t *testing.T) {
	var tr qwpTransport

	err := tr.sendMessage(context.Background(), []byte{0x00})
	if err == nil {
		t.Fatal("expected error for sendMessage on unconnected transport")
	}

	_, _, err = tr.readAck(context.Background())
	if err == nil {
		t.Fatal("expected error for readAck on unconnected transport")
	}

	// close on unconnected should be no-op.
	if err := tr.close(context.Background()); err != nil {
		t.Fatalf("close on unconnected: %v", err)
	}
}

// --- Mock WebSocket server for transport tests ---

func newTestWSServer(t *testing.T, handler func(*websocket.Conn)) *httptest.Server {
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
		handler(conn)
	}))
}

func TestQwpTransportConnectAndClose(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		// Echo server: just wait for close.
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
		}
	})
	defer srv.Close()

	// Convert http:// to ws://.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	if tr.conn == nil {
		t.Fatal("conn should not be nil after connect")
	}

	err = tr.close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if tr.conn != nil {
		t.Fatal("conn should be nil after close")
	}
}

func TestQwpTransportSendAndReceive(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		// Read a message, reply with ACK OK.
		_, data, err := conn.Read(context.Background())
		if err != nil {
			return
		}

		// Verify we received a QWP message (starts with magic).
		if len(data) >= 4 {
			magic := binary.LittleEndian.Uint32(data[0:4])
			if magic != qwpMagic {
				return
			}
		}

		// Send ACK OK with sequence=0.
		ack := buildAckOK(0)
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close(context.Background())

	// Build a simple QWP message.
	tb := newQwpTableBuffer("test")
	col, _ := tb.getOrCreateColumn("x", qwpTypeLong, false)
	col.addLong(42)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Send.
	if err := tr.sendMessage(context.Background(), msg); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	// Read ACK.
	status, _, err := tr.readAck(context.Background())
	if err != nil {
		t.Fatalf("readAck: %v", err)
	}
	if status != qwpStatusOK {
		t.Fatalf("status = 0x%02X, want 0x00 (OK)", status)
	}
}

func TestQwpTransportAckWithError(t *testing.T) {
	errMsg := "table not found: foo"
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		// Read message, reply with error ACK.
		conn.Read(context.Background())

		ack := buildAckError(qwpStatusTableNotFound, 1, errMsg)
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close(context.Background())

	// Send dummy message.
	if err := tr.sendMessage(context.Background(), []byte{0x00}); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	status, data, err := tr.readAck(context.Background())
	if err != nil {
		t.Fatalf("readAck: %v", err)
	}
	if status != qwpStatusTableNotFound {
		t.Fatalf("status = 0x%02X, want 0x04", status)
	}

	msg := parseAckError(data)
	if msg != errMsg {
		t.Fatalf("error message = %q, want %q", msg, errMsg)
	}
}

// --- Integration test against real QuestDB server ---

func TestQwpIntegrationConnect(t *testing.T) {
	// Skip if QuestDB is not running at localhost:9000.
	ctx := context.Background()

	var tr qwpTransport
	err := tr.connect(ctx, "ws://localhost:9000", qwpTransportOpts{})
	if err != nil {
		t.Skipf("QuestDB not available: %v", err)
	}
	defer tr.close(ctx)

	// Send a simple QWP message with delta symbol dict (required
	// by the server for symbol columns) and verify the ACK.
	tb := newQwpTableBuffer("qwp_transport_test")
	col, _ := tb.getOrCreateColumn("value", qwpTypeLong, false)
	col.addLong(42)
	colTs, _ := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
	colTs.addTimestamp(1000000)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	t.Logf("sending QWP message (%d bytes): %x", len(msg), msg)

	if err := tr.sendMessage(ctx, msg); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	status, data, err := tr.readAck(ctx)
	if err != nil {
		t.Fatalf("readAck: %v", err)
	}

	if status != qwpStatusOK {
		errStr := parseAckError(data)
		t.Logf("raw ACK response (%d bytes): %x", len(data), data)
		t.Fatalf("expected OK, got status 0x%02X: %s", status, errStr)
	}
	t.Logf("ACK OK, sequence=%d", parseAckSequence(data))
}

// --- Retry logic tests ---

// buildWireAckOK builds an OK ACK using the actual wire status code.
func buildWireAckOK(seq int64) []byte {
	data := make([]byte, 9)
	data[0] = qwpWireStatusOK
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	return data
}

// buildWireAckError builds an error ACK using a wire status code.
func buildWireAckError(status byte, seq int64, errMsg string) []byte {
	data := make([]byte, 11+len(errMsg))
	data[0] = status
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	binary.LittleEndian.PutUint16(data[9:11], uint16(len(errMsg)))
	copy(data[11:], errMsg)
	return data
}

func TestQwpTransportSendWithRetrySuccess(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		conn.Write(context.Background(), websocket.MessageBinary, buildWireAckOK(0))
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer tr.close(context.Background())

	msg := []byte{0x51, 0x57, 0x50, 0x31} // dummy
	err := tr.sendWithRetry(context.Background(), time.Second,
		func() []byte { return msg }, nil)
	if err != nil {
		t.Fatalf("sendWithRetry: %v", err)
	}
}

func TestQwpTransportSendWithRetryNonRetriable(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		ack := buildWireAckError(qwpWireStatusParseError, 0, "bad message")
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer tr.close(context.Background())

	err := tr.sendWithRetry(context.Background(), time.Second,
		func() []byte { return []byte{0x00} }, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	qErr, ok := err.(*QwpError)
	if !ok {
		t.Fatalf("expected *QwpError, got %T", err)
	}
	if qErr.Status != qwpWireStatusParseError {
		t.Fatalf("status = %d, want %d", qErr.Status, qwpWireStatusParseError)
	}
}

func TestQwpTransportSendWithRetryRecovery(t *testing.T) {
	// Server returns INTERNAL_ERROR twice, then OK.
	callCount := 0
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			callCount++
			if callCount <= 2 {
				ack := buildWireAckError(qwpWireStatusInternalError, 0, "temporary")
				conn.Write(context.Background(), websocket.MessageBinary, ack)
			} else {
				conn.Write(context.Background(), websocket.MessageBinary, buildWireAckOK(0))
			}
		}
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer tr.close(context.Background())

	err := tr.sendWithRetry(context.Background(), 5*time.Second,
		func() []byte { return []byte{0x00} }, nil)
	if err != nil {
		t.Fatalf("sendWithRetry should succeed after retries: %v", err)
	}
	if callCount != 3 {
		t.Fatalf("expected 3 calls (2 retries + 1 success), got %d", callCount)
	}
}

func TestQwpTransportSendWithRetryTimeout(t *testing.T) {
	// Server always returns INTERNAL_ERROR.
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			ack := buildWireAckError(qwpWireStatusInternalError, 0, "always failing")
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer tr.close(context.Background())

	// Very short timeout to trigger quickly.
	err := tr.sendWithRetry(context.Background(), 50*time.Millisecond,
		func() []byte { return []byte{0x00} }, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	_, ok := err.(*RetryTimeoutError)
	if !ok {
		t.Fatalf("expected *RetryTimeoutError, got %T: %v", err, err)
	}
}

func TestQwpTransportSendWithRetryNoRetry(t *testing.T) {
	// retryTimeout=0 means no retries even for retriable errors.
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		ack := buildWireAckError(qwpWireStatusInternalError, 0, "fail")
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer tr.close(context.Background())

	err := tr.sendWithRetry(context.Background(), 0,
		func() []byte { return []byte{0x00} }, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	qErr, ok := err.(*QwpError)
	if !ok {
		t.Fatalf("expected *QwpError, got %T", err)
	}
	if qErr.Status != qwpWireStatusInternalError {
		t.Fatalf("status = %d, want %d", qErr.Status, qwpWireStatusInternalError)
	}
}

func TestQwpTransportSendWithRetrySchemaError(t *testing.T) {
	// Server returns SCHEMA_ERROR on first call, then OK.
	callCount := 0
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			callCount++
			if callCount == 1 {
				ack := buildWireAckError(qwpWireStatusSchemaError, 0, "unknown hash")
				conn.Write(context.Background(), websocket.MessageBinary, ack)
			} else {
				conn.Write(context.Background(), websocket.MessageBinary, buildWireAckOK(0))
			}
		}
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer tr.close(context.Background())

	schemaErrorCalled := false
	err := tr.sendWithRetry(context.Background(), time.Second,
		func() []byte { return []byte{0x00} },
		func() { schemaErrorCalled = true },
	)
	if err != nil {
		t.Fatalf("sendWithRetry: %v", err)
	}
	if !schemaErrorCalled {
		t.Fatal("onSchemaError callback should have been called")
	}
	if callCount != 2 {
		t.Fatalf("expected 2 calls, got %d", callCount)
	}
}
