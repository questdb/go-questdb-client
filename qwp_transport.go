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
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// QWP WebSocket endpoint paths. Ingest and egress are separate endpoints;
// they share the version-negotiation headers but otherwise do not overlap.
const (
	qwpWritePath = "/write/v4" // ingest (QwpSender)
	qwpReadPath  = "/read/v1"  // egress (QwpQueryClient)
)

// QWP HTTP headers exchanged on the WebSocket upgrade. The version
// negotiation triple is shared by ingest and egress. The accept-encoding
// / max-batch-rows / content-encoding triple is egress-only — ingest
// never sends or reads them.
const (
	qwpHeaderMaxVersion      = "X-QWP-Max-Version"
	qwpHeaderClientId        = "X-QWP-Client-Id"
	qwpHeaderVersion         = "X-QWP-Version"
	qwpHeaderAcceptEncoding  = "X-QWP-Accept-Encoding"
	qwpHeaderMaxBatchRows    = "X-QWP-Max-Batch-Rows"
)

// qwpClientId is sent in X-QWP-Client-Id during the upgrade handshake.
// Follows the lang/version convention used by other QuestDB clients
// (e.g. java/1.0.2).
const qwpClientId = "go/4.1.0"

// QWP ACK response sizes (spec §13). An OK ACK is at least
// qwpAckOKMinSize bytes (status + sequence + tableCount=0); when
// tables committed in the acknowledged batch their per-table entries
// trail the header and the total length grows by 2+name+8 each. An
// error ACK is exactly qwpAckErrorHeaderSize + msg_len bytes.
const (
	qwpAckOKMinSize       = 11 // status(1) + sequence(8) + tableCount(2)
	qwpAckErrorHeaderSize = 11 // status(1) + sequence(8) + msg_len(2)
)

// qwpTransportOpts configures a WebSocket transport connection. The
// same struct drives both ingest (/write/v4) and egress (/read/v1)
// connections; acceptEncoding, maxBatchRows, maxVersion, and
// serverInfoTimeout are egress-only and inert at their zero values.
type qwpTransportOpts struct {
	// tlsMode controls certificate verification.
	// When true, certificate verification is skipped.
	tlsInsecureSkipVerify bool

	// authorization is the value for the Authorization HTTP
	// header, e.g. "Bearer <token>" or "Basic <base64>".
	// Empty string means no auth.
	authorization string

	// endpointPath is the HTTP path used for the WebSocket upgrade.
	// Required: ingest callers set qwpWritePath, egress callers set
	// qwpReadPath. Empty strings are rejected by connect() so mistakes
	// surface loudly instead of dialing the wrong endpoint by default.
	endpointPath string

	// acceptEncoding, when non-empty, is sent verbatim as the
	// X-QWP-Accept-Encoding upgrade header. Egress-only. Matches the
	// Java client's WebSocketClient.setQwpAcceptEncoding contract:
	// the caller builds the value ("zstd;level=3,raw" etc.); the
	// transport just forwards it. Empty string omits the header.
	acceptEncoding string

	// maxBatchRows, when > 0, is sent as the X-QWP-Max-Batch-Rows
	// upgrade header. Egress-only. Zero omits the header and lets
	// the server use its own cap.
	maxBatchRows int

	// maxVersion is the value advertised in the X-QWP-Max-Version
	// handshake header. Zero means qwpVersion (the v1 default), which
	// keeps ingest connections compatible with both v1 and v2
	// QuestDB servers. Egress callers set qwpMaxSupportedVersion to
	// opt the connection into v2-only server features (SERVER_INFO,
	// multi-endpoint failover). The transport accepts any echoed
	// X-QWP-Version that is <= maxVersion.
	maxVersion byte

	// serverInfoTimeout, when > 0, enables synchronous consumption of
	// the SERVER_INFO frame after the upgrade for connections that
	// negotiate version >= 2. Zero leaves the WebSocket recv buffer
	// untouched after the upgrade, suitable for ingest connections
	// where SERVER_INFO is not expected. Must be > 0 on egress
	// connections that advertise maxVersion >= 2 because a v2 server
	// emits the frame unsolicited before any client request.
	serverInfoTimeout time.Duration
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

	// negotiatedVersion is the QWP wire-protocol version selected by
	// the server's X-QWP-Version response header. Populated by
	// connect(); 0 before connect() has succeeded. Egress callers
	// branch on this to decide whether to expect a SERVER_INFO frame.
	negotiatedVersion byte

	// serverInfo holds the SERVER_INFO frame consumed during connect()
	// when the negotiated version is >= 2 and opts.serverInfoTimeout
	// is > 0. Nil on v1 connections and on connections that did not
	// opt into SERVER_INFO consumption (ingest senders).
	serverInfo *QwpServerInfo
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
// The url should be a ws:// or wss:// URL without the path; the path
// comes from opts.endpointPath, which is required.
//
// If t.dumpWriter is set, outgoing TCP bytes are recorded. When the
// url is empty, an in-process pipe with a fake WebSocket acceptor
// is used so the dump includes full HTTP upgrade + WebSocket framing
// without requiring a real server.
func (t *qwpTransport) connect(ctx context.Context, url string, opts qwpTransportOpts) error {
	if opts.endpointPath == "" {
		return fmt.Errorf("qwp: endpointPath is required")
	}
	path := opts.endpointPath
	wsURL := url + path

	advertisedMax := opts.maxVersion
	if advertisedMax == 0 {
		advertisedMax = qwpVersion
	}
	dialOpts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			qwpHeaderMaxVersion: []string{fmt.Sprintf("%d", advertisedMax)},
			qwpHeaderClientId:   []string{qwpClientId},
		},
	}
	if opts.authorization != "" {
		dialOpts.HTTPHeader.Set("Authorization", opts.authorization)
	}
	if opts.acceptEncoding != "" {
		dialOpts.HTTPHeader.Set(qwpHeaderAcceptEncoding, opts.acceptEncoding)
	}
	if opts.maxBatchRows > 0 {
		dialOpts.HTTPHeader.Set(qwpHeaderMaxBatchRows, fmt.Sprintf("%d", opts.maxBatchRows))
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
		wsURL = "ws://dump.local" + path

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
					MinVersion:         tls.VersionTLS12,
				},
			},
		}
	}

	conn, resp, err := websocket.Dial(ctx, wsURL, dialOpts)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
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
	negotiated, err := strconv.Atoi(serverVersion)
	if err != nil || negotiated < 1 || negotiated > int(advertisedMax) {
		conn.Close(websocket.StatusProtocolError, "version mismatch")
		return fmt.Errorf("qwp: server selected protocol version %q, client supports up to %d", serverVersion, advertisedMax)
	}

	// Remove the default read limit — QWP ACKs are small but
	// error payloads can vary.
	conn.SetReadLimit(-1)

	t.conn = conn
	t.negotiatedVersion = byte(negotiated)
	if t.recvBuf == nil {
		t.recvBuf = make([]byte, 0, qwpDefaultInitRecvBufSize)
	}

	// v2 servers emit SERVER_INFO as the first WebSocket frame after
	// the upgrade response, before any client request. Consume it
	// synchronously so the I/O goroutines start with a clean recv
	// queue and the user-visible ServerInfo() accessor is populated
	// before submit. Egress connections opt in via opts.serverInfoTimeout
	// > 0; ingest senders leave it zero so the ACK loop is never
	// fed a SERVER_INFO frame it doesn't know how to parse.
	if t.negotiatedVersion >= 2 && opts.serverInfoTimeout > 0 {
		readCtx, cancel := context.WithTimeout(ctx, opts.serverInfoTimeout)
		defer cancel()
		msgType, payload, err := t.conn.Read(readCtx)
		if err != nil {
			t.conn.Close(websocket.StatusProtocolError, "SERVER_INFO read failed")
			t.conn = nil
			return fmt.Errorf("qwp: SERVER_INFO read failed: %w", err)
		}
		if msgType != websocket.MessageBinary {
			t.conn.Close(websocket.StatusProtocolError, "SERVER_INFO non-binary")
			t.conn = nil
			return fmt.Errorf("qwp: expected SERVER_INFO binary frame, got %v", msgType)
		}
		info, err := decodeServerInfo(payload, t.negotiatedVersion)
		if err != nil {
			t.conn.Close(websocket.StatusProtocolError, "SERVER_INFO decode failed")
			t.conn = nil
			return fmt.Errorf("qwp: SERVER_INFO decode failed: %w", err)
		}
		t.serverInfo = info
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
// status byte). The payload is validated against the shape required
// by §13:
//
//   - OK ACKs are status(1) + sequence(8) + tableCount(2) +
//     tableCount × (nameLen(2) + name + seqTxn(8)). Minimum 11 bytes;
//     the trailing per-table entries section must consume the rest of
//     the payload exactly.
//   - DURABLE_ACK frames are unsolicited per-table watermarks; we
//     skip them and keep reading. Servers only emit them when the
//     client opts in via the X-QWP-Request-Durable-Ack header, which
//     this transport does not, but any well-formed durable-ack frame
//     that arrives is silently consumed.
//   - Error ACKs are exactly qwpAckErrorHeaderSize + msg_len bytes.
//
// Mirrors the Java client's WebSocketResponse.isStructurallyValid;
// unrecognized shapes fail loudly instead of decoding into garbage
// fields.
func (t *qwpTransport) readAck(ctx context.Context) (qwpStatusCode, []byte, error) {
	if t.conn == nil {
		return 0, nil, fmt.Errorf("qwp: not connected")
	}

	// Loop reads until a usable ACK arrives. We skip stray non-binary
	// frames (proxy keep-alives) and unsolicited DURABLE_ACK frames
	// the same way: continue and keep reading.
	for {
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
		if len(data) < 1 {
			return 0, nil, fmt.Errorf("qwp: ack too short: %d bytes", len(data))
		}
		statusCode := qwpStatusCode(data[0])

		switch statusCode {
		case qwpStatusOK:
			if len(data) < qwpAckOKMinSize {
				return 0, nil, fmt.Errorf("qwp: malformed OK ack: got %d bytes, want at least %d", len(data), qwpAckOKMinSize)
			}
			if !validateAckTableEntries(data[9:]) {
				return 0, nil, fmt.Errorf("qwp: malformed OK ack: bad table entries section, got %d bytes", len(data))
			}
			return statusCode, data, nil

		case qwpStatusDurableAck:
			// DURABLE_ACK: status(1) + tableCount(2) + entries. Verify
			// shape and continue reading — we do not surface durable
			// watermarks today.
			if len(data) < 3 {
				return 0, nil, fmt.Errorf("qwp: malformed durable-ack: got %d bytes, want at least 3", len(data))
			}
			if !validateAckTableEntries(data[1:]) {
				return 0, nil, fmt.Errorf("qwp: malformed durable-ack: bad table entries section, got %d bytes", len(data))
			}
			continue

		default:
			if len(data) < qwpAckErrorHeaderSize {
				return 0, nil, fmt.Errorf("qwp: malformed error ack: got %d bytes, want at least %d", len(data), qwpAckErrorHeaderSize)
			}
			msgLen := int(binary.LittleEndian.Uint16(data[9:11]))
			if len(data) != qwpAckErrorHeaderSize+msgLen {
				return 0, nil, fmt.Errorf("qwp: malformed error ack: status=0x%02X, got %d bytes, want %d", byte(statusCode), len(data), qwpAckErrorHeaderSize+msgLen)
			}
			return statusCode, data, nil
		}
	}
}

// validateAckTableEntries walks the per-table entries section that
// trails an OK or DURABLE_ACK header. The buffer must start at the
// 2-byte little-endian table count, contain `tableCount` entries of
// shape (nameLen(2) + name + seqTxn(8)), and end exactly at the last
// entry — no trailing bytes. Mirrors Java's validateTableEntries.
func validateAckTableEntries(buf []byte) bool {
	if len(buf) < 2 {
		return false
	}
	tableCount := int(binary.LittleEndian.Uint16(buf[0:2]))
	off := 2
	for i := 0; i < tableCount; i++ {
		if len(buf) < off+2 {
			return false
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
		off += 2
		// Empty table names are rejected as structurally invalid — a
		// valid table name is never zero bytes, and accepting empty
		// names would let a misbehaving server poison any per-table
		// tracker with "" entries.
		if nameLen == 0 || len(buf) < off+nameLen+8 {
			return false
		}
		off += nameLen + 8
	}
	return off == len(buf)
}

// parseAckError extracts an error message from a non-OK ACK payload.
// The layout is:
//
//	[statusCode: uint8] [sequence: int64 LE] [errorLength: uint16 LE] [errorMessage: UTF-8]
//
// Precondition: data has already been validated by readAck, which
// guarantees at least qwpAckErrorHeaderSize bytes for non-OK statuses
// and that the trailing bytes match the declared errorLength.
func parseAckError(data []byte) string {
	const errLenOffset = 9  // 1 (status) + 8 (sequence)
	const errMsgOffset = 11 // errLenOffset + 2 (uint16)
	errLen := int(binary.LittleEndian.Uint16(data[errLenOffset:errMsgOffset]))
	return string(data[errMsgOffset : errMsgOffset+errLen])
}

// parseAckSequence extracts the cumulative sequence number from an
// ACK payload. The wire field is signed (int64 LE) and uses -1 as
// a sentinel; matches Java's long semantics.
//
// Precondition: data has already been validated by readAck, which
// guarantees at least qwpAckOKMinSize bytes for OK ACKs and the
// header for error ACKs. Not valid for DURABLE_ACK frames, which
// carry no sequence; readAck never returns those.
func parseAckSequence(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data[1:9]))
}

// close sends a graceful WebSocket close frame and cleans up.
func (t *qwpTransport) close() error {
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
			// 2 bytes WS header + 11 bytes payload (status + seq + tableCount=0).
			var ack [13]byte
			ack[0] = 0x82 // FIN+BINARY
			ack[1] = 0x0B // payload length 11
			ack[2] = 0x00 // STATUS_OK
			binary.LittleEndian.PutUint64(ack[3:11], seq)
			// ack[11:13] is tableCount=0 (already zero).
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
