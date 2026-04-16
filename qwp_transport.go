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
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

// qwpWritePath is the WebSocket endpoint for QWP ingestion.
const qwpWritePath = "/write/v4"

// Version-negotiation HTTP headers (QWP spec §3).
const (
	qwpHeaderMaxVersion = "X-QWP-Max-Version"
	qwpHeaderClientId   = "X-QWP-Client-Id"
	qwpHeaderVersion    = "X-QWP-Version"
)

// qwpClientId is sent in X-QWP-Client-Id during the upgrade handshake.
// Follows the lang/version convention used by other QuestDB clients
// (e.g. java/1.0.2).
const qwpClientId = "go/4.1.0"

// QWP ACK response sizes (spec §13). An OK ACK is exactly
// qwpAckOKSize bytes; an error ACK is exactly
// qwpAckErrorHeaderSize + msg_len bytes.
const (
	qwpAckOKSize          = 9  // status(1) + sequence(8)
	qwpAckErrorHeaderSize = 11 // status(1) + sequence(8) + msg_len(2)
)

// qwpTransportOpts configures a WebSocket transport connection.
type qwpTransportOpts struct {
	// tlsMode controls certificate verification.
	// When true, certificate verification is skipped.
	tlsInsecureSkipVerify bool

	// authorization is the value for the Authorization HTTP
	// header, e.g. "Bearer <token>" or "Basic <base64>".
	// Empty string means no auth.
	authorization string
}

// qwpTransport wraps a WebSocket connection for sending QWP
// messages and receiving ACK responses. It is not safe for
// concurrent use; in sync mode the caller goroutine owns it,
// in async mode the I/O goroutine owns it.
type qwpTransport struct {
	conn *websocket.Conn

	// recvBuf is a reusable buffer for reading ACK responses,
	// sized to avoid allocations on the steady-state path.
	recvBuf []byte

	// dumpWriter, when non-nil, records all outgoing TCP bytes
	// (HTTP upgrade + WebSocket frames). Set before connect().
	dumpWriter io.Writer
}

// teeConn wraps a net.Conn, copying all Write calls to a side writer.
// Used by dump mode to capture outgoing bytes.
type teeConn struct {
	net.Conn
	w io.Writer
}

func (c *teeConn) Write(p []byte) (int, error) {
	c.w.Write(p) // best-effort, ignore dump errors
	return c.Conn.Write(p)
}

// connect establishes a WebSocket connection to the QWP endpoint.
// The url should be a ws:// or wss:// URL without the path; the
// /write/v4 path is appended automatically.
//
// If t.dumpWriter is set, outgoing TCP bytes are recorded. When the
// url is empty, an in-process pipe with a fake WebSocket acceptor
// is used so the dump includes full HTTP upgrade + WebSocket framing
// without requiring a real server.
func (t *qwpTransport) connect(ctx context.Context, url string, opts qwpTransportOpts) error {
	wsURL := url + qwpWritePath

	dialOpts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			qwpHeaderMaxVersion: []string{fmt.Sprintf("%d", qwpVersion)},
			qwpHeaderClientId:   []string{qwpClientId},
		},
	}
	if opts.authorization != "" {
		dialOpts.HTTPHeader.Set("Authorization", opts.authorization)
	}

	if t.dumpWriter != nil {
		// Dump mode: use an in-process pipe with a fake server.
		clientConn, serverConn := net.Pipe()
		go qwpFakeServer(serverConn)
		wrapped := &teeConn{Conn: clientConn, w: t.dumpWriter}
		dialOpts.HTTPClient = &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return wrapped, nil
				},
			},
		}
		// Use a dummy URL so the WS library has something to parse.
		wsURL = "ws://dump.local" + qwpWritePath

		// If Dial fails, close the pipe so the fake server goroutine exits.
		defer func() {
			if t.conn == nil {
				clientConn.Close()
			}
		}()
	} else if opts.tlsInsecureSkipVerify {
		// TLS configuration for wss:// connections.
		dialOpts.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	conn, resp, err := websocket.Dial(ctx, wsURL, dialOpts)
	if err != nil {
		return fmt.Errorf("qwp: websocket dial: %w", err)
	}

	// Validate the server-selected QWP version. Require the header to
	// be present and match our version — a missing header signals a
	// non-QWP endpoint or a server that did not run the negotiation
	// path, and a mismatched version means we'd get parse errors on
	// every message. Fail fast in both cases to match the Java client.
	if resp == nil {
		conn.Close(websocket.StatusProtocolError, "no upgrade response")
		return fmt.Errorf("qwp: no HTTP upgrade response available for version negotiation")
	}
	serverVersion := resp.Header.Get(qwpHeaderVersion)
	if serverVersion == "" {
		conn.Close(websocket.StatusProtocolError, "missing version header")
		return fmt.Errorf("qwp: server did not return %s header", qwpHeaderVersion)
	}
	if serverVersion != fmt.Sprintf("%d", qwpVersion) {
		conn.Close(websocket.StatusProtocolError, "version mismatch")
		return fmt.Errorf("qwp: server selected protocol version %q, client supports %d", serverVersion, qwpVersion)
	}

	// Remove the default read limit — QWP ACKs are small but
	// error payloads can vary.
	conn.SetReadLimit(-1)

	t.conn = conn
	if t.recvBuf == nil {
		t.recvBuf = make([]byte, 0, qwpDefaultInitRecvBufSize)
	}
	return nil
}

// sendMessage sends a QWP message as a WebSocket binary frame.
func (t *qwpTransport) sendMessage(ctx context.Context, data []byte) error {
	if t.conn == nil {
		return fmt.Errorf("qwp: not connected")
	}
	return t.conn.Write(ctx, websocket.MessageBinary, data)
}

// readAck reads and parses the server's ACK response. It returns
// the status code and the full response payload (including the
// status byte). The payload is validated against the exact length
// required by §13: OK ACKs must be exactly qwpAckOKSize bytes, error
// ACKs must be exactly qwpAckErrorHeaderSize + msg_len bytes. This
// mirrors the Java client's WebSocketResponse.isStructurallyValid
// and fails loudly on any unrecognized shape (e.g. a legacy PARTIAL
// response) instead of decoding it into garbage fields.
//
// ACK layouts:
//
//	OK:    [status: uint8 (0x00)] [sequence: int64 LE]
//	Error: [status: uint8] [sequence: int64 LE] [msg_len: uint16 LE] [msg: UTF-8]
func (t *qwpTransport) readAck(ctx context.Context) (qwpStatusCode, []byte, error) {
	if t.conn == nil {
		return 0, nil, fmt.Errorf("qwp: not connected")
	}

	// Skip non-binary data frames. coder/websocket handles ping/pong
	// and close control frames internally, so only stray text frames
	// can reach us — e.g. a misbehaving proxy injecting keep-alives.
	// Match the Java client, which ignores them and keeps reading.
	var data []byte
	for {
		msgType, buf, err := t.conn.Read(ctx)
		if err != nil {
			return 0, nil, fmt.Errorf("qwp: read ack: %w", err)
		}
		if msgType == websocket.MessageBinary {
			data = buf
			break
		}
	}
	if len(data) < qwpAckOKSize {
		return 0, nil, fmt.Errorf("qwp: ack too short: %d bytes", len(data))
	}

	statusCode := qwpStatusCode(data[0])
	if statusCode == qwpStatusOK {
		if len(data) != qwpAckOKSize {
			return 0, nil, fmt.Errorf("qwp: malformed OK ack: got %d bytes, want %d", len(data), qwpAckOKSize)
		}
		return statusCode, data, nil
	}
	if len(data) < qwpAckErrorHeaderSize {
		return 0, nil, fmt.Errorf("qwp: malformed error ack: got %d bytes, want at least %d", len(data), qwpAckErrorHeaderSize)
	}
	msgLen := int(binary.LittleEndian.Uint16(data[9:11]))
	if len(data) != qwpAckErrorHeaderSize+msgLen {
		return 0, nil, fmt.Errorf("qwp: malformed error ack: status=0x%02X, got %d bytes, want %d", byte(statusCode), len(data), qwpAckErrorHeaderSize+msgLen)
	}
	return statusCode, data, nil
}

// parseAckError extracts an error message from an ACK response
// payload. The layout is:
//
//	[statusCode: uint8] [sequence: int64 LE] [errorLength: uint16 LE] [errorMessage: UTF-8]
//
// The error length and message are only present for non-OK statuses.
// Returns empty string if no error message is present.
func parseAckError(data []byte) string {
	// data[0] = status, data[1:9] = sequence, data[9:11] = errLen.
	const errLenOffset = 9   // 1 (status) + 8 (sequence)
	const errMsgOffset = 11  // errLenOffset + 2 (uint16)
	if len(data) < errMsgOffset {
		return ""
	}
	errLen := int(binary.LittleEndian.Uint16(data[errLenOffset:errMsgOffset]))
	if len(data) < errMsgOffset+errLen {
		return ""
	}
	return string(data[errMsgOffset : errMsgOffset+errLen])
}

// parseAckSequence extracts the sequence number from an ACK
// response. Returns 0 if the response is too short.
func parseAckSequence(data []byte) int64 {
	if len(data) < 9 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(data[1:9]))
}

// close sends a graceful WebSocket close frame and cleans up.
func (t *qwpTransport) close(ctx context.Context) error {
	if t.conn == nil {
		return nil
	}
	err := t.conn.Close(websocket.StatusNormalClosure, "")
	t.conn = nil
	return err
}

// --- fake server for dump mode ---

// wsAcceptGUID is the magic GUID appended to the client key for the
// Sec-WebSocket-Accept header per RFC 6455 section 4.2.2.
const wsAcceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// qwpFakeServer is a minimal WebSocket acceptor that runs on the
// server end of a net.Pipe(). It performs the HTTP upgrade handshake,
// then reads WebSocket binary frames and responds with QWP OK ACKs.
// It exits when the connection is closed or an error occurs.
func qwpFakeServer(conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReader(conn)

	// --- HTTP upgrade ---
	var wsKey string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break // end of headers
		}
		// Case-insensitive header match — Go's HTTP client
		// may send "Sec-Websocket-Key" (lowercase 'w').
		if len(line) > 20 && strings.EqualFold(line[:19], "Sec-WebSocket-Key: ") {
			wsKey = strings.TrimSpace(line[19:])
		}
	}

	// Compute Sec-WebSocket-Accept.
	h := sha1.New()
	h.Write([]byte(wsKey + wsAcceptGUID))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n" +
		qwpHeaderVersion + ": " + fmt.Sprintf("%d", qwpVersion) + "\r\n" +
		"\r\n"
	if _, err := conn.Write([]byte(resp)); err != nil {
		return
	}

	// --- WebSocket frame loop ---
	var seq uint64
	var hdr [14]byte // max WS header size
	for {
		// Read first 2 bytes: FIN+opcode, MASK+length.
		if _, err := io.ReadFull(br, hdr[:2]); err != nil {
			return
		}
		opcode := hdr[0] & 0x0F
		masked := hdr[1]&0x80 != 0
		payloadLen := uint64(hdr[1] & 0x7F)

		// Extended payload length.
		switch payloadLen {
		case 126:
			if _, err := io.ReadFull(br, hdr[2:4]); err != nil {
				return
			}
			payloadLen = uint64(binary.BigEndian.Uint16(hdr[2:4]))
		case 127:
			if _, err := io.ReadFull(br, hdr[2:10]); err != nil {
				return
			}
			payloadLen = binary.BigEndian.Uint64(hdr[2:10])
		}

		// Masking key (4 bytes if masked).
		var maskKey [4]byte
		if masked {
			if _, err := io.ReadFull(br, maskKey[:]); err != nil {
				return
			}
		}

		// Discard the payload — we don't need to process QWP data.
		if _, err := io.CopyN(io.Discard, br, int64(payloadLen)); err != nil {
			return
		}

		switch opcode {
		case 0x08: // Close frame
			// Echo a close frame with status 1000 (normal closure).
			// FIN + CLOSE = 0x88, length = 2, status = 1000 (big-endian).
			conn.Write([]byte{0x88, 0x02, 0x03, 0xE8})
			return
		case 0x02: // Binary frame — send QWP OK ACK.
			seq++
			var ack [11]byte
			// Unmasked binary frame: FIN+BINARY=0x82, length=9.
			ack[0] = 0x82
			ack[1] = 0x09
			// Payload: status OK (0x00) + sequence (uint64 LE).
			ack[2] = 0x00 // STATUS_OK
			binary.LittleEndian.PutUint64(ack[3:], seq)
			if _, err := conn.Write(ack[:]); err != nil {
				return
			}
		}
		// Ignore other opcodes (ping/pong handled by WS library).
	}
}

// sendAndAck sends a QWP message and reads exactly one ACK.
// Returns nil on OK, a *QwpError for server-side rejections, or a
// transport error on connection failure. No retry: the spec defines
// no retriable status, so any non-OK response is terminal.
func (t *qwpTransport) sendAndAck(ctx context.Context, sendFn func() []byte) error {
	msg := sendFn()
	if err := t.sendMessage(ctx, msg); err != nil {
		return err
	}
	_, data, err := t.readAck(ctx)
	if err != nil {
		return err
	}
	if qErr := newQwpErrorFromAck(data); qErr != nil {
		return qErr
	}
	return nil
}
