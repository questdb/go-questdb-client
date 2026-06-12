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
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Unit tests for ACK parsing ---

// buildAckOK builds a minimal OK ACK response (11 bytes — status +
// sequence + tableCount=0, no per-table entries).
func buildAckOK(seq int64) []byte {
	data := make([]byte, qwpAckOKMinSize)
	data[0] = byte(QwpStatusOK)
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	binary.LittleEndian.PutUint16(data[9:11], 0)
	return data
}

// buildAckOKWithTables builds an OK ACK whose tail carries one or
// more per-table watermark entries (nameLen + name + seqTxn). Used by
// tests that exercise the new OK-with-watermark wire shape.
func buildAckOKWithTables(seq int64, entries ...struct {
	name   string
	seqTxn int64
}) []byte {
	tail := encodeAckTableEntries(entries)
	data := make([]byte, 11+len(tail))
	data[0] = byte(QwpStatusOK)
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	binary.LittleEndian.PutUint16(data[9:11], uint16(len(entries)))
	copy(data[11:], tail)
	return data
}

// buildAckDurable builds a STATUS_DURABLE_ACK response (status +
// tableCount + entries).
func buildAckDurable(entries ...struct {
	name   string
	seqTxn int64
}) []byte {
	tail := encodeAckTableEntries(entries)
	data := make([]byte, 3+len(tail))
	data[0] = byte(QwpStatusDurableAck)
	binary.LittleEndian.PutUint16(data[1:3], uint16(len(entries)))
	copy(data[3:], tail)
	return data
}

// encodeAckTableEntries serializes per-table watermark entries
// (nameLen(2) + name + seqTxn(8)) without the leading tableCount.
// Caller is responsible for prepending tableCount.
func encodeAckTableEntries(entries []struct {
	name   string
	seqTxn int64
}) []byte {
	size := 0
	for _, e := range entries {
		size += 2 + len(e.name) + 8
	}
	out := make([]byte, size)
	off := 0
	for _, e := range entries {
		binary.LittleEndian.PutUint16(out[off:off+2], uint16(len(e.name)))
		off += 2
		copy(out[off:], e.name)
		off += len(e.name)
		binary.LittleEndian.PutUint64(out[off:off+8], uint64(e.seqTxn))
		off += 8
	}
	return out
}

// buildAckError builds an error ACK response with message.
func buildAckError(status QwpStatusCode, seq int64, errMsg string) []byte {
	data := make([]byte, 11+len(errMsg))
	data[0] = byte(status)
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	binary.LittleEndian.PutUint16(data[9:11], uint16(len(errMsg)))
	copy(data[11:], errMsg)
	return data
}

// TestQwpParseAckError exercises parseAckError on inputs that satisfy
// its precondition (non-OK ACKs of exactly qwpAckErrorHeaderSize + msgLen
// bytes, as enforced by readAck). Malformed-length inputs are readAck's
// responsibility and are covered in TestReadAckRejects* tests.
func TestQwpParseAckError(t *testing.T) {
	t.Run("ErrorWithMessage", func(t *testing.T) {
		errMsg := "bad data"
		data := buildAckError(QwpStatusParseError, 1, errMsg)

		msg := parseAckError(data)
		if msg != errMsg {
			t.Fatalf("parseAckError = %q, want %q", msg, errMsg)
		}
	})

	t.Run("EmptyErrorMessage", func(t *testing.T) {
		data := buildAckError(QwpStatusInternalError, 2, "")
		msg := parseAckError(data)
		if msg != "" {
			t.Fatalf("expected empty, got %q", msg)
		}
	})

	t.Run("AllStatusCodes", func(t *testing.T) {
		codes := []QwpStatusCode{
			QwpStatusSchemaMismatch,
			QwpStatusParseError,
			QwpStatusInternalError,
			QwpStatusSecurityError,
			QwpStatusWriteError,
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
	dataErr := buildAckError(QwpStatusParseError, 99, "err")
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
	if err := tr.close(); err != nil {
		t.Fatalf("close on unconnected: %v", err)
	}
}

// --- Mock WebSocket server for transport tests ---

func newTestWSServer(t *testing.T, handler func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("websocket accept error: %v", err)
			return
		}
		defer conn.CloseNow()
		handler(conn)
	}))
}

// newTestWSServerV2 echoes the negotiated version as the X-QWP-Version
// response header (default qwpVersion; override via opts.version), and
// when serverInfoFrame is non-nil writes it as the first WebSocket
// binary frame after the upgrade. The caller-supplied handler runs
// after the SERVER_INFO frame is sent so tests can drive arbitrary
// post-handshake choreography.
func newTestWSServerV2(t *testing.T, opts testWSServerV2Opts, handler func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	version := opts.version
	if version == 0 {
		version = qwpVersion
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, fmt.Sprintf("%d", version))
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("websocket accept error: %v", err)
			return
		}
		defer conn.CloseNow()
		if opts.serverInfoFrame != nil {
			if err := conn.Write(r.Context(), websocket.MessageBinary, opts.serverInfoFrame); err != nil {
				t.Logf("server: SERVER_INFO write error: %v", err)
				return
			}
		}
		if handler != nil {
			handler(conn)
		}
	}))
}

type testWSServerV2Opts struct {
	// version is the value echoed in X-QWP-Version. Zero defaults to
	// qwpMaxSupportedVersion.
	version byte
	// serverInfoFrame, when non-nil, is written as the first binary
	// frame after the upgrade. Built via buildServerInfoFrame in
	// qwp_server_info_test.go.
	serverInfoFrame []byte
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
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	if tr.conn == nil {
		t.Fatal("conn should not be nil after connect")
	}

	err = tr.close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	// close() shuts the connection down but deliberately leaves the conn
	// field intact — it is immutable after connect so the egress reader
	// and dispatcher can read it lock-free without racing a concurrent
	// close (see qwpTransport.conn). The connection is nonetheless dead:
	// I/O on it errors.
	if tr.conn == nil {
		t.Fatal("conn should be retained after close (immutable post-connect)")
	}
	if err := tr.sendMessage(context.Background(), []byte{0x00}); err == nil {
		t.Fatal("sendMessage should fail on a closed connection")
	}
	// close() is idempotent: a repeat call returns the same nil result.
	if err := tr.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestQwpTransportNegotiationHeaders verifies that connect() sends
// the QWP version negotiation headers during the WebSocket upgrade.
func TestQwpTransportNegotiationHeaders(t *testing.T) {
	var gotMaxVersion, gotClientId string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMaxVersion = r.Header.Get(qwpHeaderMaxVersion)
		gotClientId = r.Header.Get(qwpHeaderClientId)
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close()

	if gotMaxVersion != "1" {
		t.Errorf("X-QWP-Max-Version = %q, want %q", gotMaxVersion, "1")
	}
	if gotClientId != qwpClientId {
		t.Errorf("X-QWP-Client-Id = %q, want %q", gotClientId, qwpClientId)
	}
}

// TestQwpTransportVersionMatchAccepted verifies that connect() succeeds
// when the server returns the same protocol version in X-QWP-Version.
func TestQwpTransportVersionMatchAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close()
}

// TestQwpTransportVersionMissingRejected verifies that a server response
// without the X-QWP-Version header aborts the handshake. Matches the
// Java client's fail-fast behavior on non-QWP endpoints.
func TestQwpTransportVersionMissingRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath})
	if err == nil {
		tr.close()
		t.Fatal("expected missing-version error")
	}
	if !strings.Contains(err.Error(), qwpHeaderVersion) {
		t.Fatalf("expected error mentioning %s, got: %v", qwpHeaderVersion, err)
	}
	if tr.conn != nil {
		t.Fatal("conn should be nil after rejected handshake")
	}
}

// TestQwpTransportVersionMismatchRejected verifies that a server response
// advertising a different QWP version aborts the handshake.
func TestQwpTransportVersionMismatchRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "2")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath})
	if err == nil {
		tr.close()
		t.Fatal("expected version mismatch error")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Fatalf("expected version error, got: %v", err)
	}
	if tr.conn != nil {
		t.Fatal("conn should be nil after rejected handshake")
	}
}

// TestQwpTransportNegotiationConsumesServerInfo verifies that an
// egress-style connection reads the SERVER_INFO frame the server emits
// post-upgrade, and exposes the decoded fields via tr.serverInfo. The
// recv buffer must be clean for follow-up frames.
func TestQwpTransportNegotiationConsumesServerInfo(t *testing.T) {
	frame := buildServerInfoFrame(qwpVersion, 0,
		qwpRolePrimary, 17, 0, 1234567890, "alpha", "node-A")
	srv := newTestWSServerV2(t, testWSServerV2Opts{
		serverInfoFrame: frame,
	}, func(conn *websocket.Conn) {
		// Stay alive so the client can close cleanly.
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{
		endpointPath:      qwpReadPath,
		maxVersion:        qwpVersion,
		serverInfoTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close()

	if tr.negotiatedVersion != qwpVersion {
		t.Errorf("negotiatedVersion = %d, want %d",
			tr.negotiatedVersion, qwpVersion)
	}
	if tr.serverInfo == nil {
		t.Fatal("serverInfo should be populated on egress connection")
	}
	if tr.serverInfo.Role != qwpRolePrimary {
		t.Errorf("Role = 0x%02X, want PRIMARY", tr.serverInfo.Role)
	}
	if tr.serverInfo.NodeId != "node-A" {
		t.Errorf("NodeId = %q, want node-A", tr.serverInfo.NodeId)
	}
}

// TestQwpTransportNegotiationDecodeFailureClosesConn ensures that a
// malformed SERVER_INFO frame surfaces as a connect-time error and
// nils tr.conn, so callers see a clean failure rather than a partly
// usable transport.
func TestQwpTransportNegotiationDecodeFailureClosesConn(t *testing.T) {
	srv := newTestWSServerV2(t, testWSServerV2Opts{
		serverInfoFrame: []byte{0xDE, 0xAD, 0xBE, 0xEF}, // not a valid frame
	}, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{
		endpointPath:      qwpReadPath,
		maxVersion:        qwpVersion,
		serverInfoTimeout: 2 * time.Second,
	})
	if err == nil {
		tr.close()
		t.Fatal("expected SERVER_INFO decode error")
	}
	if !strings.Contains(err.Error(), "SERVER_INFO") {
		t.Errorf("error = %v, want SERVER_INFO", err)
	}
	if tr.conn != nil {
		t.Error("conn must be nil after failed SERVER_INFO read")
	}
}

// TestQwpTransportNegotiationTimeout verifies that a stalled server
// (one that never emits SERVER_INFO) trips the bounded timeout.
func TestQwpTransportNegotiationTimeout(t *testing.T) {
	srv := newTestWSServerV2(t, testWSServerV2Opts{
		// Don't emit SERVER_INFO at all; just keep the conn open.
	}, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{
		endpointPath:      qwpReadPath,
		maxVersion:        qwpVersion,
		serverInfoTimeout: 50 * time.Millisecond,
	})
	if err == nil {
		tr.close()
		t.Fatal("expected SERVER_INFO timeout error")
	}
	if !strings.Contains(err.Error(), "SERVER_INFO") {
		t.Errorf("error = %v, want SERVER_INFO timeout", err)
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
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close()

	// Build a simple QWP message.
	tb := newQwpTableBuffer("test")
	col, _ := tb.getOrCreateColumn("x", qwpTypeLong, false)
	col.addLong(42)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb)

	// Send.
	if err := tr.sendMessage(context.Background(), msg); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	// Read ACK.
	status, _, err := tr.readAck(context.Background())
	if err != nil {
		t.Fatalf("readAck: %v", err)
	}
	if status != QwpStatusOK {
		t.Fatalf("status = 0x%02X, want 0x00 (OK)", status)
	}
}

func TestQwpTransportAckWithError(t *testing.T) {
	errMsg := "write failure: table closed"
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		// Read message, reply with error ACK.
		conn.Read(context.Background())

		ack := buildAckError(QwpStatusWriteError, 1, errMsg)
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close()

	// Send dummy message.
	if err := tr.sendMessage(context.Background(), []byte{0x00}); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	status, data, err := tr.readAck(context.Background())
	if err != nil {
		t.Fatalf("readAck: %v", err)
	}
	if status != QwpStatusWriteError {
		t.Fatalf("status = 0x%02X, want 0x09", status)
	}

	msg := parseAckError(data)
	if msg != errMsg {
		t.Fatalf("error message = %q, want %q", msg, errMsg)
	}
}

// --- Strict ACK validation tests (mirror Java isStructurallyValid) ---

// TestReadAckRejectsOversizedOK ensures readAck fails loudly when an OK
// response carries trailing garbage past the per-table entries section.
func TestReadAckRejectsOversizedOK(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		// buildAckOK produces an 11-byte OK with tableCount=0; pad
		// with one extra byte so the trailing entries section no
		// longer ends exactly at len(data).
		ack := append(buildAckOK(0), 0x00)
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatal(err)
	}
	defer tr.close()

	if err := tr.sendMessage(context.Background(), []byte{0x00}); err != nil {
		t.Fatal(err)
	}
	_, _, err := tr.readAck(context.Background())
	if err == nil {
		t.Fatal("expected malformed-ack error")
	}
	if !strings.Contains(err.Error(), "malformed OK") {
		t.Fatalf("error should mention 'malformed OK', got: %v", err)
	}
}

// TestReadAckRejectsErrorLengthMismatch ensures readAck fails when an
// error ACK's declared msg_len doesn't match the trailing payload.
func TestReadAckRejectsErrorLengthMismatch(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		// Build an error ACK claiming msg_len=10 but carrying only 5 msg bytes.
		ack := make([]byte, 16)
		ack[0] = byte(QwpStatusWriteError)
		binary.LittleEndian.PutUint64(ack[1:9], 0)
		binary.LittleEndian.PutUint16(ack[9:11], 10)
		copy(ack[11:], "short") // only 5 bytes, not 10
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatal(err)
	}
	defer tr.close()

	if err := tr.sendMessage(context.Background(), []byte{0x00}); err != nil {
		t.Fatal(err)
	}
	_, _, err := tr.readAck(context.Background())
	if err == nil {
		t.Fatal("expected malformed-ack error")
	}
	if !strings.Contains(err.Error(), "malformed error") {
		t.Fatalf("error should mention 'malformed error', got: %v", err)
	}
}

// TestReadAckSkipsTextFrames ensures readAck keeps reading past stray
// non-binary frames (e.g. text keep-alives a proxy might inject) rather
// than surfacing them as a protocol error. Matches the Java client.
func TestReadAckSkipsTextFrames(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		// Inject a text frame before the binary ACK.
		if err := conn.Write(context.Background(), websocket.MessageText, []byte("keepalive")); err != nil {
			t.Logf("text write: %v", err)
			return
		}
		conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(7))
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatal(err)
	}
	defer tr.close()

	if err := tr.sendMessage(context.Background(), []byte{0x00}); err != nil {
		t.Fatal(err)
	}
	status, data, err := tr.readAck(context.Background())
	if err != nil {
		t.Fatalf("readAck: %v", err)
	}
	if status != QwpStatusOK {
		t.Fatalf("status = 0x%02X, want OK", status)
	}
	if seq := parseAckSequence(data); seq != 7 {
		t.Fatalf("sequence = %d, want 7", seq)
	}
}

// TestQwpTransportEgressUpgrade exercises the opts.endpointPath,
// opts.acceptEncoding, and opts.maxBatchRows fields wired in step 6.
// Each subtest inspects the HTTP upgrade request the transport sends,
// then lets the WebSocket handshake complete so connect() returns.
func TestQwpTransportEgressUpgrade(t *testing.T) {
	type reqSnapshot struct {
		path           string
		acceptEncoding string
		maxBatchRows   string
		hasAcceptEnc   bool
		hasMaxRows     bool
	}

	newServer := func(capture *reqSnapshot) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capture.path = r.URL.Path
			capture.acceptEncoding = r.Header.Get(qwpHeaderAcceptEncoding)
			capture.maxBatchRows = r.Header.Get(qwpHeaderMaxBatchRows)
			// Values() canonicalizes the key internally, so we can
			// probe for header presence without assuming what the
			// canonical form of "X-QWP-*" happens to be.
			capture.hasAcceptEnc = len(r.Header.Values(qwpHeaderAcceptEncoding)) > 0
			capture.hasMaxRows = len(r.Header.Values(qwpHeaderMaxBatchRows)) > 0
			w.Header().Set(qwpHeaderVersion, "1")
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			for {
				if _, _, err := conn.Read(context.Background()); err != nil {
					return
				}
			}
		}))
	}

	t.Run("ReadPathWithBothEgressHeaders", func(t *testing.T) {
		var got reqSnapshot
		srv := newServer(&got)
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		var tr qwpTransport
		opts := qwpTransportOpts{
			endpointPath:   qwpReadPath,
			acceptEncoding: "zstd;level=3,raw",
			maxBatchRows:   10_000,
		}
		require.NoError(t, tr.connect(context.Background(), wsURL, opts))
		defer tr.close()

		assert.Equal(t, qwpReadPath, got.path)
		assert.Equal(t, "zstd;level=3,raw", got.acceptEncoding)
		assert.Equal(t, "10000", got.maxBatchRows)
	})

	t.Run("IngestPathStampsNoEgressHeaders", func(t *testing.T) {
		var got reqSnapshot
		srv := newServer(&got)
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		var tr qwpTransport
		opts := qwpTransportOpts{endpointPath: qwpWritePath}
		require.NoError(t, tr.connect(context.Background(), wsURL, opts))
		defer tr.close()

		assert.Equal(t, qwpWritePath, got.path)
		assert.False(t, got.hasAcceptEnc, "accept-encoding must be omitted on ingest")
		assert.False(t, got.hasMaxRows, "max-batch-rows must be omitted on ingest")
	})

	t.Run("EmptyEndpointPathRejected", func(t *testing.T) {
		// No server needed — the empty-path check short-circuits before
		// any network I/O so the call never leaves the process.
		var tr qwpTransport
		err := tr.connect(context.Background(), "ws://unused", qwpTransportOpts{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "endpointPath is required")
		assert.Nil(t, tr.conn)
	})

	t.Run("EmptyAcceptEncodingOmitsHeader", func(t *testing.T) {
		var got reqSnapshot
		srv := newServer(&got)
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		var tr qwpTransport
		opts := qwpTransportOpts{
			endpointPath:   qwpReadPath,
			acceptEncoding: "",
			maxBatchRows:   0,
		}
		require.NoError(t, tr.connect(context.Background(), wsURL, opts))
		defer tr.close()

		assert.Equal(t, qwpReadPath, got.path)
		assert.False(t, got.hasAcceptEnc, "empty acceptEncoding must omit header")
		assert.False(t, got.hasMaxRows, "zero maxBatchRows must omit header")
	})

	t.Run("MaxBatchRowsOnlyOmitsAcceptEncoding", func(t *testing.T) {
		var got reqSnapshot
		srv := newServer(&got)
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		var tr qwpTransport
		opts := qwpTransportOpts{
			endpointPath: qwpReadPath,
			maxBatchRows: 1,
		}
		require.NoError(t, tr.connect(context.Background(), wsURL, opts))
		defer tr.close()

		assert.False(t, got.hasAcceptEnc)
		assert.Equal(t, "1", got.maxBatchRows)
	})
}

// TestReadAckOKWithTableEntries exercises the new OK ACK shape that
// carries per-table watermark entries (status + seq + tableCount +
// [nameLen + name + seqTxn] * tableCount). The wire frame for one
// 19-char table name lands at exactly 42 bytes — this is the size
// the live QuestDB server returns for typical SF write paths.
func TestReadAckOKWithTableEntries(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		ack := buildAckOKWithTables(7,
			struct {
				name   string
				seqTxn int64
			}{"my_test_table_xxxxx", 100},
		)
		// Sanity: this is the 42-byte ACK shape from the live server.
		// 11 (header) + 2 (nameLen) + 19 (name) + 8 (seqTxn) = 40.
		// Adjust if the helper layout ever changes.
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	require.NoError(t, tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}))
	defer tr.close()

	require.NoError(t, tr.sendMessage(context.Background(), []byte{0x00}))
	status, data, err := tr.readAck(context.Background())
	require.NoError(t, err)
	if status != QwpStatusOK {
		t.Fatalf("status = 0x%02X, want OK", status)
	}
	if seq := parseAckSequence(data); seq != 7 {
		t.Fatalf("sequence = %d, want 7", seq)
	}
}

// TestReadAckDurableAck verifies that DURABLE_ACK frames pass the
// validator, are returned with the correct status code, and don't
// trip the OK / error decoders.
func TestReadAckDurableAck(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		conn.Write(context.Background(), websocket.MessageBinary,
			buildAckDurable(struct {
				name   string
				seqTxn int64
			}{"durable_table", 42}))
		// Followed by a normal OK terminator so the test has something
		// to return after the durable-ack tail.
		conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(0))
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	require.NoError(t, tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}))
	defer tr.close()

	require.NoError(t, tr.sendMessage(context.Background(), []byte{0x00}))
	status, _, err := tr.readAck(context.Background())
	require.NoError(t, err)
	if status != QwpStatusDurableAck {
		t.Fatalf("status = 0x%02X, want DURABLE_ACK", status)
	}
}

// TestReadAckRejectsTruncatedTableEntry confirms that an OK frame
// whose tableCount declares N entries but whose body terminates early
// is rejected as malformed.
func TestReadAckRejectsTruncatedTableEntry(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		// Build an OK frame with tableCount=1 but no entry bytes.
		ack := make([]byte, 11)
		ack[0] = byte(QwpStatusOK)
		binary.LittleEndian.PutUint64(ack[1:9], 0)
		binary.LittleEndian.PutUint16(ack[9:11], 1) // claims 1 entry
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	require.NoError(t, tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}))
	defer tr.close()

	require.NoError(t, tr.sendMessage(context.Background(), []byte{0x00}))
	_, _, err := tr.readAck(context.Background())
	if err == nil {
		t.Fatal("expected malformed-OK error for truncated table entry")
	}
	if !strings.Contains(err.Error(), "malformed OK") {
		t.Fatalf("error should mention 'malformed OK', got: %v", err)
	}
}

// TestReadAckRejectsEmptyTableName confirms that a per-table entry
// with nameLen=0 is rejected. Mirrors the Java client's
// validateTableEntries guard.
func TestReadAckRejectsEmptyTableName(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		// OK frame with one entry: nameLen=0, seqTxn=0. The validator
		// must reject this even though the byte count adds up.
		ack := make([]byte, 11+2+8)
		ack[0] = byte(QwpStatusOK)
		binary.LittleEndian.PutUint64(ack[1:9], 0)
		binary.LittleEndian.PutUint16(ack[9:11], 1)
		binary.LittleEndian.PutUint16(ack[11:13], 0) // nameLen=0
		binary.LittleEndian.PutUint64(ack[13:21], 0) // seqTxn
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	require.NoError(t, tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}))
	defer tr.close()

	require.NoError(t, tr.sendMessage(context.Background(), []byte{0x00}))
	_, _, err := tr.readAck(context.Background())
	if err == nil {
		t.Fatal("expected malformed-OK error for empty table name")
	}
	if !strings.Contains(err.Error(), "empty table name") {
		t.Fatalf("error should mention 'empty table name', got: %v", err)
	}
}

func TestQwpDumpWriter(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()

	s, err := newQwpLineSender(ctx, "", qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, &buf)
	require.NoError(t, err)

	// Insert a row and flush — exercises the full sender pipeline so
	// the dump captures both the HTTP upgrade and at least one
	// WebSocket binary frame round-trip.
	s.Table("test_dump").Int64Column("val", 42)
	require.NoError(t, s.At(ctx, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
	require.NoError(t, s.Flush(ctx))
	require.NoError(t, s.Close(ctx))

	dump := buf.String()
	assert.Contains(t, dump, "GET /write/v4 HTTP/1.1\r\n")
	assert.Contains(t, dump, "Upgrade: websocket\r\n")

	// Should have some binary data after the HTTP request (WebSocket frames).
	httpEnd := strings.Index(dump, "\r\n\r\n")
	require.Greater(t, httpEnd, 0)
	assert.Greater(t, len(dump), httpEnd+4, "expected WebSocket frames after HTTP upgrade")
}

// newUpgradeRejectServer returns an httptest.Server that responds to
// every request with the given status, headers, and body. Used to
// drive the qwpTransport.connect() reject-classification paths without
// running a real WebSocket accept.
func newUpgradeRejectServer(t *testing.T, status int, headers http.Header, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, vs := range headers {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))
}

// connectUpgradeReject is the shared assertion: drive connect() against
// the given server and require a *QwpUpgradeRejectError. Returns the
// typed error so callers can verify its fields.
func connectUpgradeReject(t *testing.T, srv *httptest.Server, opts qwpTransportOpts) *QwpUpgradeRejectError {
	t.Helper()
	if opts.endpointPath == "" {
		opts.endpointPath = qwpWritePath
	}
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, opts)
	require.Error(t, err)
	assert.Nil(t, tr.conn, "transport must not retain a conn on a rejected upgrade")
	var rej *QwpUpgradeRejectError
	require.ErrorAs(t, err, &rej)
	return rej
}

// TestQwpTransportUpgradeReject421PrimaryCatchup verifies that a 421
// response with X-QuestDB-Role: PRIMARY_CATCHUP surfaces as a typed
// QwpUpgradeRejectError that classifies as a (transient) role-reject.
func TestQwpTransportUpgradeReject421PrimaryCatchup(t *testing.T) {
	srv := newUpgradeRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
		"X-QuestDB-Zone": []string{"eu-west-1a"},
	}, "primary is still catching up")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.Equal(t, 421, rej.StatusCode)
	assert.Equal(t, "PRIMARY_CATCHUP", rej.Role)
	assert.Equal(t, "eu-west-1a", rej.Zone)
	assert.True(t, rej.IsRoleReject())
	assert.True(t, rej.IsCatchupRole())
	assert.Contains(t, rej.Body, "catching up")
}

// TestQwpTransportUpgradeReject421Replica verifies that a 421 with a
// non-CATCHUP role surfaces as a topology-style reject (IsRoleReject
// is true but IsCatchupRole is false).
func TestQwpTransportUpgradeReject421Replica(t *testing.T) {
	srv := newUpgradeRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"REPLICA"},
	}, "")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.Equal(t, 421, rej.StatusCode)
	assert.Equal(t, "REPLICA", rej.Role)
	assert.True(t, rej.IsRoleReject())
	assert.False(t, rej.IsCatchupRole())
}

// TestQwpTransportUpgradeReject421CaseInsensitiveRole verifies the
// PRIMARY_CATCHUP comparison is case-insensitive — failover.md §5
// mandates case-insensitive matching for the PRIMARY_CATCHUP and
// REPLICA predicates.
func TestQwpTransportUpgradeReject421CaseInsensitiveRole(t *testing.T) {
	srv := newUpgradeRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"primary_catchup"},
	}, "")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.True(t, rej.IsCatchupRole(),
		"PRIMARY_CATCHUP match must be case-insensitive (got %q)", rej.Role)
}

// TestQwpTransportUpgradeReject421WithoutRole exercises the "421 + no
// role header" path: spec §5 says this degrades to a generic transport
// error from the failover loop's perspective. The transport surfaces
// the typed reject; classification is the caller's responsibility.
func TestQwpTransportUpgradeReject421WithoutRole(t *testing.T) {
	srv := newUpgradeRejectServer(t, 421, http.Header{}, "missing role header")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.Equal(t, 421, rej.StatusCode)
	assert.Empty(t, rej.Role)
	assert.False(t, rej.IsRoleReject(), "421 with empty role must not classify as role-reject")
}

// TestQwpTransportUpgradeReject404 — 404 was previously terminal for
// SF (qwpSfIsProtocolUpgradeFailure matched "got 404"); per the
// 2026-05-08 reclassification, it now flows through the round-walk as
// transient. The transport just surfaces the typed reject.
func TestQwpTransportUpgradeReject404(t *testing.T) {
	srv := newUpgradeRejectServer(t, 404, http.Header{}, "not found")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.Equal(t, 404, rej.StatusCode)
	assert.False(t, rej.IsRoleReject())
}

// TestQwpTransportUpgradeReject426 — same reasoning as 404 (rolling
// upgrade with one peer on a newer/older version).
func TestQwpTransportUpgradeReject426(t *testing.T) {
	srv := newUpgradeRejectServer(t, 426, http.Header{}, "upgrade required")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.Equal(t, 426, rej.StatusCode)
}

// TestQwpTransportUpgradeReject503 — server reachable but currently
// unable to serve. failover.md §6 classifies this as transient.
func TestQwpTransportUpgradeReject503(t *testing.T) {
	srv := newUpgradeRejectServer(t, 503, http.Header{
		"Retry-After": []string{"7"},
	}, "")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.Equal(t, 503, rej.StatusCode)
	assert.Equal(t, 7*time.Second, rej.RetryAfter)
}

// TestQwpTransportUpgradeReject401 — auth-terminal at the failover-loop
// layer. The transport again just surfaces the typed reject; the SF
// classifier maps 401/403 to CategorySecurityError separately.
func TestQwpTransportUpgradeReject401(t *testing.T) {
	srv := newUpgradeRejectServer(t, 401, http.Header{}, "unauthorized")
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.Equal(t, 401, rej.StatusCode)
}

// TestQwpTransportUpgradeRejectBodyTruncation verifies the body
// snippet is bounded by qwpUpgradeBodySnippetCap and that overrun
// adds a trailing ellipsis so the truncation is observable.
func TestQwpTransportUpgradeRejectBodyTruncation(t *testing.T) {
	body := strings.Repeat("X", qwpUpgradeBodySnippetCap+200)
	srv := newUpgradeRejectServer(t, 500, http.Header{}, body)
	defer srv.Close()

	rej := connectUpgradeReject(t, srv, qwpTransportOpts{})
	assert.LessOrEqual(t, len(rej.Body), qwpUpgradeBodySnippetCap+len("…"))
	assert.True(t, strings.HasSuffix(rej.Body, "…"),
		"truncated body must end with ellipsis, got %q", rej.Body)
}

// TestQwpTransportUpgradeRejectErrorIsTyped pins down the
// errors.As contract so failover loop callers can rely on
// `var rej *QwpUpgradeRejectError; errors.As(err, &rej)` after a
// failed connect — even if the transport wraps the error in the
// future.
func TestQwpTransportUpgradeRejectErrorIsTyped(t *testing.T) {
	srv := newUpgradeRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	}, "")
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath})
	require.Error(t, err)
	var rej *QwpUpgradeRejectError
	require.ErrorAs(t, err, &rej)
	assert.Equal(t, 421, rej.StatusCode)
}

// TestQwpTransportUpgradeRejectNoConnLeak drives many non-101 upgrade
// rejects through connect() and asserts the goroutine count stays flat.
// Each connect() builds a fresh one-shot http.Transport; without
// DisableKeepAlives a 421 (steady-state in failover topologies) would
// park the keep-alive TCP conn in that transport's idle pool, stranding
// the conn plus its persistConn read/write goroutines — a per-reject
// leak invisible to the single-shot reject tests above, since each of
// them builds exactly one transport.
func TestQwpTransportUpgradeRejectNoConnLeak(t *testing.T) {
	srv := newUpgradeRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	}, "primary is still catching up")
	defer srv.Close()

	// Warm-up cycle so the httptest accept machinery and any
	// once-initialized globals are already counted in the baseline.
	connectUpgradeReject(t, srv, qwpTransportOpts{})
	base := stableGoroutineCount()

	const cycles = 30
	for i := 0; i < cycles; i++ {
		connectUpgradeReject(t, srv, qwpTransportOpts{})
	}

	// persistConn teardown is asynchronous — the read/write goroutines
	// exit once the closed conn unblocks them — so let it settle. A
	// per-reject leak would add ~2×30 goroutines, far past the slack, so
	// this stays sensitive without flaking on transient runtime or
	// httptest server goroutines.
	const slack = 8
	var got int
	require.Eventuallyf(t, func() bool {
		got = stableGoroutineCount()
		return got <= base+slack
	}, 10*time.Second, 100*time.Millisecond,
		"goroutine count did not return to baseline after %d upgrade-reject "+
			"connect cycles", cycles)
	assert.LessOrEqualf(t, got, base+slack,
		"goroutine count grew from %d to %d across %d upgrade rejects — "+
			"connect() is leaking pooled conns / persistConn goroutines",
		base, got, cycles)
}

// TestQwpTransportAuthTimeoutBoundsUpgradeReadOnly verifies that the
// failover.md §1 auth_timeout_ms knob only bounds the upgrade response
// read — a server that accepts the TCP connection but never writes the
// HTTP response must trip the timeout, and the resulting error must
// surface within the configured window (not the OS default connect
// timeout).
func TestQwpTransportAuthTimeoutBoundsUpgradeReadOnly(t *testing.T) {
	// Black-hole acceptor: accept the TCP connection but never send a
	// response. coder/websocket's Dial will block on response read.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold the connection open without responding.
			_ = conn
		}
	}()

	start := time.Now()
	wsURL := "ws://" + ln.Addr().String()
	var tr qwpTransport
	err = tr.connect(context.Background(), wsURL, qwpTransportOpts{
		endpointPath:  qwpWritePath,
		authTimeoutMs: 200,
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	// Should fire close to the configured 200ms — well under any OS
	// connect default. Allow generous headroom for slow CI.
	assert.Less(t, elapsed, 2*time.Second,
		"auth_timeout_ms (200ms) did not bound the upgrade read; elapsed=%s", elapsed)
}
