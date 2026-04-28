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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Unit tests for ACK parsing ---

// buildAckOK builds a minimal OK ACK response (11 bytes): the
// fixed status + sequence header followed by tableCount=0 and no
// per-table entries.
func buildAckOK(seq int64) []byte {
	data := make([]byte, qwpAckOKMinSize)
	data[0] = byte(qwpStatusOK)
	binary.LittleEndian.PutUint64(data[1:9], uint64(seq))
	// data[9:11] is tableCount, already zero.
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

// TestQwpParseAckError exercises parseAckError on inputs that satisfy
// its precondition (non-OK ACKs of exactly qwpAckErrorHeaderSize + msgLen
// bytes, as enforced by readAck). Malformed-length inputs are readAck's
// responsibility and are covered in TestReadAckRejects* tests.
func TestQwpParseAckError(t *testing.T) {
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

	t.Run("AllStatusCodes", func(t *testing.T) {
		codes := []qwpStatusCode{
			qwpStatusSchemaMismatch,
			qwpStatusParseError,
			qwpStatusInternalError,
			qwpStatusSecurityError,
			qwpStatusWriteError,
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

// newTestWSServerV2 is the v2-aware variant. It echoes the negotiated
// version as the X-QWP-Version response header (default qwpMaxSupportedVersion;
// override via opts.version), and when serverInfoFrame is non-nil
// writes it as the first WebSocket binary frame after the upgrade. The
// caller-supplied handler runs after the SERVER_INFO frame is sent so
// tests can drive arbitrary post-handshake choreography.
func newTestWSServerV2(t *testing.T, opts testWSServerV2Opts, handler func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	version := opts.version
	if version == 0 {
		version = qwpMaxSupportedVersion
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
	if tr.conn != nil {
		t.Fatal("conn should be nil after close")
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

// TestQwpTransportV2NegotiationConsumesServerInfo verifies that an
// egress-style connection that advertises maxVersion=2 reads the
// SERVER_INFO frame the v2 server emits, and exposes the decoded
// fields via tr.serverInfo. The recv buffer must be clean for
// follow-up frames.
func TestQwpTransportV2NegotiationConsumesServerInfo(t *testing.T) {
	frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
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
		maxVersion:        qwpMaxSupportedVersion,
		serverInfoTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close()

	if tr.negotiatedVersion != qwpMaxSupportedVersion {
		t.Errorf("negotiatedVersion = %d, want %d",
			tr.negotiatedVersion, qwpMaxSupportedVersion)
	}
	if tr.serverInfo == nil {
		t.Fatal("serverInfo should be populated on v2 connection")
	}
	if tr.serverInfo.Role != qwpRolePrimary {
		t.Errorf("Role = 0x%02X, want PRIMARY", tr.serverInfo.Role)
	}
	if tr.serverInfo.NodeId != "node-A" {
		t.Errorf("NodeId = %q, want node-A", tr.serverInfo.NodeId)
	}
}

// TestQwpTransportV2NegotiationDecodeFailureClosesConn ensures that a
// malformed SERVER_INFO frame surfaces as a connect-time error and
// nils tr.conn, so callers see a clean failure rather than a partly
// usable transport.
func TestQwpTransportV2NegotiationDecodeFailureClosesConn(t *testing.T) {
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
		maxVersion:        qwpMaxSupportedVersion,
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

// TestQwpTransportV2NegotiationTimeout verifies that a stalled v2
// server (one that never emits SERVER_INFO) trips the bounded timeout.
func TestQwpTransportV2NegotiationTimeout(t *testing.T) {
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
		maxVersion:        qwpMaxSupportedVersion,
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

// TestQwpTransportV1ConnectSkipsServerInfoRead ensures that a server
// that echoes X-QWP-Version=1 does not trigger a SERVER_INFO read,
// even when the client advertises maxVersion=2 with a non-zero
// timeout. Backward-compat path with v1 deployments.
func TestQwpTransportV1ConnectSkipsServerInfoRead(t *testing.T) {
	srv := newTestWSServerV2(t, testWSServerV2Opts{
		version: 1,
		// Even if we somehow set serverInfoFrame, the v1 path should not
		// touch it.
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
		maxVersion:        qwpMaxSupportedVersion,
		serverInfoTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tr.close()

	if tr.negotiatedVersion != 1 {
		t.Errorf("negotiatedVersion = %d, want 1", tr.negotiatedVersion)
	}
	if tr.serverInfo != nil {
		t.Errorf("serverInfo should be nil on v1, got %+v", tr.serverInfo)
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
	errMsg := "write failure: table closed"
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		// Read message, reply with error ACK.
		conn.Read(context.Background())

		ack := buildAckError(qwpStatusWriteError, 1, errMsg)
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
	if status != qwpStatusWriteError {
		t.Fatalf("status = 0x%02X, want 0x09", status)
	}

	msg := parseAckError(data)
	if msg != errMsg {
		t.Fatalf("error message = %q, want %q", msg, errMsg)
	}
}

// --- sendAndAck tests ---

func TestQwpTransportSendAndAckSuccess(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(0))
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatal(err)
	}
	defer tr.close()

	msg := []byte{0x51, 0x57, 0x50, 0x31} // dummy
	if err := tr.sendAndAck(context.Background(), func() []byte { return msg }); err != nil {
		t.Fatalf("sendAndAck: %v", err)
	}
}

func TestQwpTransportSendAndAckServerError(t *testing.T) {
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		conn.Read(context.Background())
		ack := buildAckError(qwpStatusParseError, 0, "bad message")
		conn.Write(context.Background(), websocket.MessageBinary, ack)
	})
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	var tr qwpTransport
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
		t.Fatal(err)
	}
	defer tr.close()

	err := tr.sendAndAck(context.Background(), func() []byte { return []byte{0x00} })
	if err == nil {
		t.Fatal("expected error")
	}
	qErr, ok := err.(*QwpError)
	if !ok {
		t.Fatalf("expected *QwpError, got %T", err)
	}
	if qErr.Status != qwpStatusParseError {
		t.Fatalf("status = %d, want %d", qErr.Status, qwpStatusParseError)
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
		ack[0] = byte(qwpStatusWriteError)
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
	if status != qwpStatusOK {
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

func TestQwpDumpWriter(t *testing.T) {
	// dump mode wires its synthetic server through net.Pipe(); the
	// cursor send loop's separate sender + receiver goroutines on
	// that pipe deadlock the in-process WebSocket reader. The
	// dump-mode pipeline still records the upgrade handshake and
	// outgoing bytes correctly — we just exit before the drain
	// barrier that hangs on net.Pipe — so the test exercises
	// connect + the first sendMessage, then closes.
	var buf bytes.Buffer
	ctx := context.Background()

	var transport qwpTransport
	transport.dumpWriter = &buf
	require.NoError(t, transport.connect(ctx, "", qwpTransportOpts{}))
	require.NoError(t, transport.sendMessage(ctx, []byte{0x00, 0x01, 0x02, 0x03}))
	_ = transport.close(ctx)

	// The dump should start with the HTTP upgrade request.
	dump := buf.String()
	assert.Contains(t, dump, "GET /write/v4 HTTP/1.1\r\n")
	assert.Contains(t, dump, "Upgrade: websocket\r\n")

	// Should have some binary data after the HTTP request (WebSocket frames).
	httpEnd := strings.Index(dump, "\r\n\r\n")
	require.Greater(t, httpEnd, 0)
	assert.Greater(t, len(dump), httpEnd+4, "expected WebSocket frames after HTTP upgrade")
}
