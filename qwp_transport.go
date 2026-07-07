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
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	qwpHeaderMaxVersion     = "X-QWP-Max-Version"
	qwpHeaderClientId       = "X-QWP-Client-Id"
	qwpHeaderVersion        = "X-QWP-Version"
	qwpHeaderAcceptEncoding = "X-QWP-Accept-Encoding"
	qwpHeaderMaxBatchRows   = "X-QWP-Max-Batch-Rows"
	// qwpHeaderRequestDurableAck is the upgrade request header the client
	// sets (value "true") to opt into durable-ack mode; the server echoes
	// qwpHeaderDurableAck: enabled to confirm it will emit DURABLE_ACK
	// frames. Mirrors Java WebSocketClient's X-QWP-Request-Durable-Ack /
	// X-QWP-Durable-Ack pair.
	qwpHeaderRequestDurableAck = "X-QWP-Request-Durable-Ack"
	qwpHeaderDurableAck        = "X-QWP-Durable-Ack"
	// qwpHeaderMaxBatchSize is the server-advertised hard cap on a
	// single DATA_BATCH wire frame (bytes), echoed in the WebSocket
	// upgrade response. Used to clamp the producer's
	// auto_flush_bytes trigger down to 90% of this value so a
	// soft-flush fires before the encoded batch can exceed the cap
	// and trip ws-close[1009]. 0 / absent / unparseable means the
	// server did not advertise a cap (older build) and the
	// configured auto_flush_bytes is kept verbatim. Mirrors Java
	// WebSocketClient.QWP_MAX_BATCH_SIZE_HEADER_NAME.
	qwpHeaderMaxBatchSize = "X-QWP-Max-Batch-Size"
)

// qwpClientId is sent in X-QWP-Client-Id during the upgrade handshake.
// Follows the lang/version convention used by other QuestDB clients
// (e.g. java/1.0.2).
const qwpClientId = "go/4.3.0"

// qwpDurableAckEnabledValue is the X-QWP-Durable-Ack response header value
// a server sends to confirm it will emit DURABLE_ACK frames. Compared
// case-insensitively (matches Java's equalsIgnoreCase("enabled")).
const qwpDurableAckEnabledValue = "enabled"

// QWP ACK response sizes (spec §13). All ACKs share a fixed header
// shape, but their tails vary:
//
//	OK:           [status(1)] [sequence(8)] [tableCount(2)] [entries…]
//	DURABLE_ACK:  [status(1)] [tableCount(2)] [entries…]
//	Error:        [status(1)] [sequence(8)] [msg_len(2)] [msg]
//
// Each table entry is [nameLen(2)] [name(nameLen)] [seqTxn(8)]. The
// minimum frame sizes below correspond to a payload with zero entries.
const (
	qwpAckOKMinSize         = 11 // status(1) + sequence(8) + tableCount(2)
	qwpAckDurableMinSize    = 3  // status(1) + tableCount(2)
	qwpAckErrorHeaderSize   = 11 // status(1) + sequence(8) + msg_len(2)
	qwpAckTableEntryHeader  = 10 // nameLen(2) + seqTxn(8)
	qwpAckSequenceOffset    = 1  // status(1)
	qwpAckOKTablesOffset    = 9  // status(1) + sequence(8)
	qwpAckDurableTablesOff  = 1  // status(1)
	qwpAckErrorMsgLenOffset = 9  // status(1) + sequence(8)
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
	// handshake header. Zero means qwpVersion. QWP currently has a
	// single protocol version, so both ingest and egress callers
	// advertise qwpVersion; the header is retained as the negotiation
	// mechanism for a future version bump. The transport accepts any
	// echoed X-QWP-Version that is <= maxVersion.
	maxVersion byte

	// serverInfoTimeout, when > 0, enables synchronous consumption of
	// the SERVER_INFO frame after the upgrade. The egress endpoint
	// (/read/v1) appends an unsolicited SERVER_INFO frame to the 101
	// response, so egress callers set this. The ingest endpoint
	// (/write/v4) sends no SERVER_INFO and the client never expects
	// one — it sends data right after the upgrade and the first inbound
	// frame is an ACK — so ingest senders leave it zero.
	serverInfoTimeout time.Duration

	// authTimeoutMs is the failover.md §1 per-host upper bound on the
	// HTTP upgrade response read (i.e. the wait between writing the
	// upgrade request and reading the response headers). It does NOT
	// cover TCP connect (OS default), TLS handshake, or the post-
	// upgrade SERVER_INFO frame read. Zero defers to the standard
	// http.Transport default (effectively unbounded), matching the
	// pre-failover-spec behavior; sanitizeQwpConf seeds 15000 for
	// QWP-configured callers.
	authTimeoutMs int

	// requestDurableAck opts into durable-ack mode (ingest-only). When
	// set, connect() sends the X-QWP-Request-Durable-Ack: true upgrade
	// header and REQUIRES the server to echo X-QWP-Durable-Ack: enabled —
	// a completed upgrade without the echo is a terminal
	// *QwpDurableAckMismatchError (the server cannot honour durability).
	requestDurableAck bool
}

// qwpTransport wraps a WebSocket connection for sending QWP
// messages and receiving ACK responses. It is owned by the I/O
// goroutine(s) that drive it — the ingest send loop (qwpSfSendLoop),
// or the egress reader plus dispatcher — and is not safe for
// unrestricted concurrent use.
type qwpTransport struct {
	// conn is the live WebSocket. A successful connect() assigns it once
	// and it is never mutated again for the life of the transport —
	// close() shuts the connection down but leaves the field intact. That
	// immutability is load-bearing: the egress reader and dispatcher read
	// conn lock-free from their own goroutines, and a concurrent close()
	// (e.g. a short-ctx Close that returns before those goroutines join)
	// must not race them. A closed conn already errors every I/O, so
	// nil-ing the field would buy nothing and only reintroduce that race.
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

	// serverDurableAckEnabled records whether the server echoed
	// X-QWP-Durable-Ack: enabled on the upgrade. Only meaningful when the
	// caller set opts.requestDurableAck; connect() has already rejected a
	// requested-but-unconfirmed upgrade, so for a durable-ack sender this
	// is always true on a live transport.
	serverDurableAckEnabled bool

	// serverMaxBatchSize is the server-advertised hard cap on a
	// single DATA_BATCH wire frame (bytes), parsed from the
	// X-QWP-Max-Batch-Size response header during connect(). 0
	// means the server did not advertise a cap (header absent /
	// unparseable / non-positive); callers must treat 0 as "no
	// clamp". Read by the qwpLineSender's transport-swap callback
	// to refresh its effective auto_flush_bytes threshold on every
	// successful connect; a rolling upgrade can leave neighbouring
	// endpoints with different caps.
	serverMaxBatchSize int32

	// serverInfo holds the SERVER_INFO frame consumed during connect()
	// when opts.serverInfoTimeout is > 0. Nil on connections that did
	// not opt into SERVER_INFO consumption (ingest senders).
	serverInfo *QwpServerInfo

	// closeOnce guards close() so the underlying conn is shut down at
	// most once and repeat calls return the same result. It writes no
	// field the I/O goroutines read — conn stays immutable (see above),
	// so close() never races the lock-free reader/dispatcher.
	closeOnce sync.Once
	closeErr  error
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

// asyncWritePipeConn wraps the client end of the dump-mode net.Pipe so
// Write queues the bytes and returns immediately, emulating a kernel
// socket's send buffer. A real socket buffers the client's send, so
// sendMessage returns — and the send loop stores highestFullySent —
// before the server reads the frame and replies. net.Pipe is
// synchronous: Write blocks until the peer reads, which lets the fake
// server's OK ACK reach the receiver before highestFullySent is stored.
// The receiver then clamps that ACK away (its highestFullySent < 0
// guard) and never advances ackedFsn, so Close drains until timeout.
// Queuing the write restores the production ordering. A single pump
// goroutine drains the queue in FIFO order, preserving frame boundaries
// and byte order; Read and all net.Conn metadata pass through to the
// embedded pipe end.
type asyncWritePipeConn struct {
	net.Conn
	mu     sync.Mutex
	cond   *sync.Cond
	queued []byte
	closed bool
}

func newAsyncWritePipeConn(c net.Conn) *asyncWritePipeConn {
	a := &asyncWritePipeConn{Conn: c}
	a.cond = sync.NewCond(&a.mu)
	go a.pump()
	return a
}

func (a *asyncWritePipeConn) Write(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0, net.ErrClosed
	}
	a.queued = append(a.queued, p...)
	a.cond.Signal()
	return len(p), nil
}

// pump owns the only Write to the embedded pipe, so queued chunks reach
// the fake server in order and never interleave. It exits once the conn
// is closed and the queue is drained.
func (a *asyncWritePipeConn) pump() {
	for {
		a.mu.Lock()
		for len(a.queued) == 0 && !a.closed {
			a.cond.Wait()
		}
		if len(a.queued) == 0 && a.closed {
			a.mu.Unlock()
			return
		}
		chunk := a.queued
		a.queued = nil
		a.mu.Unlock()
		if _, err := a.Conn.Write(chunk); err != nil {
			return
		}
	}
}

func (a *asyncWritePipeConn) Close() error {
	a.mu.Lock()
	a.closed = true
	a.cond.Signal()
	a.mu.Unlock()
	return a.Conn.Close()
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
	if opts.requestDurableAck {
		dialOpts.HTTPHeader.Set(qwpHeaderRequestDurableAck, "true")
	}

	// Build the http.Transport so we can install ResponseHeaderTimeout
	// per failover.md §1 (auth_timeout_ms bounds the upgrade response
	// read). The same Transport carries TLS config for wss:// and the
	// pipe-DialContext for dump mode.
	//
	// DisableKeepAlives keeps this one-shot transport from pooling. It is
	// built fresh per connect() and discarded after, so there is no reuse
	// to gain — and on a non-101 upgrade response (421 role-reject, 503
	// proxy, ...) coder/websocket reads the body to EOF and closes it,
	// which would otherwise return the keep-alive TCP conn to this
	// transport's idle pool. Nothing reuses the abandoned transport or
	// calls CloseIdleConnections on it, so the parked conn plus its
	// persistConn read/write goroutines would leak — and role-rejects are
	// steady-state in a failover topology, so the leak accumulates. A
	// successful 101 hijacks the conn out of pool management, so the flag
	// never affects the live WebSocket.
	httpTransport := &http.Transport{
		DisableKeepAlives: true,
	}
	if opts.authTimeoutMs > 0 {
		httpTransport.ResponseHeaderTimeout = time.Duration(opts.authTimeoutMs) * time.Millisecond
	}

	if t.dumpWriter != nil {
		// Dump mode: use an in-process pipe with a fake server. The
		// client write end is buffered (asyncWritePipeConn) so it
		// behaves like a real socket — without it the synchronous pipe
		// lets the fake server's ACK race the send loop's bookkeeping.
		clientConn, serverConn := net.Pipe()
		go qwpFakeServer(serverConn)
		buffered := newAsyncWritePipeConn(clientConn)
		wrapped := &teeConn{Conn: buffered, w: t.dumpWriter}
		httpTransport.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
			return wrapped, nil
		}
		// Use a dummy URL so the WS library has something to parse.
		wsURL = "ws://dump.local" + path

		// If Dial fails, close the buffered conn so the pump and fake
		// server goroutines exit. On success the WebSocket owns wrapped
		// and its Close path tears both down.
		defer func() {
			if t.conn == nil {
				buffered.Close()
			}
		}()
	} else if opts.tlsInsecureSkipVerify {
		// TLS configuration for wss:// connections.
		httpTransport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
	}
	dialOpts.HTTPClient = &http.Client{Transport: httpTransport}

	conn, resp, err := websocket.Dial(ctx, wsURL, dialOpts)
	if err != nil {
		// On a non-101 response, build a typed *QwpUpgradeRejectError
		// from the captured status + headers so the failover loop can
		// classify the host (role-reject / topology / transport) without
		// re-parsing string error messages. resp may be nil for TCP/TLS
		// dial failures or response-header timeouts; in that case fall
		// back to the wrapped dial error.
		if resp != nil {
			rejectErr := buildUpgradeRejectError(resp, err)
			resp.Body.Close()
			return rejectErr
		}
		return fmt.Errorf("qwp: websocket dial: %w", err)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
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

	// Durable-ack negotiation. A durable-ack sender requires the server to
	// echo X-QWP-Durable-Ack: enabled; a completed upgrade without it means
	// the server cannot honour durability, so the OK-driven trim this
	// sender suppresses would never be replaced by a DURABLE_ACK and rows
	// could sit in SF forever. Fail fast with a terminal, typed error
	// (mirrors Java's QwpDurableAckMismatchException) so the failover loop
	// does not burn its budget retrying an incapable cluster.
	t.serverDurableAckEnabled = strings.EqualFold(
		strings.TrimSpace(resp.Header.Get(qwpHeaderDurableAck)), qwpDurableAckEnabledValue)
	if opts.requestDurableAck && !t.serverDurableAckEnabled {
		conn.Close(websocket.StatusProtocolError, "durable-ack not supported")
		return &QwpDurableAckMismatchError{URL: url}
	}

	// Raise — but do not remove — the default read limit. QWP ACKs are
	// small, but egress RESULT_BATCH frames can reach qwpMaxBatchSize,
	// so the 32 KiB default is too low. A finite ceiling (not -1) is
	// load-bearing: this conn is shared by the egress reader and the
	// ingest readAck path, and coder/websocket enforces the limit while
	// streaming the message — a hostile or buggy server emitting a
	// multi-GB frame is cut off mid-read instead of OOMing the host
	// before any downstream size check runs.
	conn.SetReadLimit(qwpMaxFrameReadLimit)

	t.conn = conn
	t.negotiatedVersion = byte(negotiated)
	// Parse the optional X-QWP-Max-Batch-Size advertisement. A
	// non-positive or unparseable value is treated as "no cap":
	// older servers that don't emit the header leave the configured
	// auto_flush_bytes untouched. Mirrors Java
	// WebSocketClient.extractMaxBatchSize.
	if cap := resp.Header.Get(qwpHeaderMaxBatchSize); cap != "" {
		if parsed, perr := strconv.Atoi(cap); perr == nil && parsed > 0 {
			if parsed > math.MaxInt32 {
				parsed = math.MaxInt32
			}
			t.serverMaxBatchSize = int32(parsed)
		}
	}
	if t.recvBuf == nil {
		t.recvBuf = make([]byte, 0, qwpDefaultInitRecvBufSize)
	}

	// The egress endpoint appends a SERVER_INFO frame to the upgrade
	// response (the read endpoint always emits it post-handshake),
	// before any client request. Consume it synchronously so the I/O
	// goroutines start with a clean recv queue and the user-visible
	// ServerInfo() accessor is populated before submit. Egress
	// connections opt in via opts.serverInfoTimeout > 0; the ingest
	// endpoint sends no SERVER_INFO and the client never expects one,
	// so ingest senders leave it zero and read ACKs directly.
	if opts.serverInfoTimeout > 0 {
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

// buildUpgradeRejectError snapshots the relevant fields of a non-101
// upgrade response into a typed QwpUpgradeRejectError. Reads up to
// qwpUpgradeBodySnippetCap bytes of the body so the error message
// surfaces operator-supplied text (e.g. a reverse-proxy maintenance
// page) without unbounded memory cost. The caller is responsible for
// closing resp.Body once this returns. cause is the originating
// websocket.Dial error, retained so it is wrapped (not discarded) —
// notably when StatusCode is 101 but the upgrade still failed.
func buildUpgradeRejectError(resp *http.Response, cause error) *QwpUpgradeRejectError {
	role := strings.TrimSpace(resp.Header.Get("X-QuestDB-Role"))
	zone := strings.TrimSpace(resp.Header.Get("X-QuestDB-Zone"))
	var retryAfter time.Duration
	if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
		// Per RFC 7231 §7.1.3, Retry-After is either an HTTP-date or a
		// non-negative integer of seconds. We only honour the seconds
		// form here — the failover loop's outage budget is the
		// authoritative wait bound, so HTTP-date precision adds little.
		if secs, perr := strconv.Atoi(ra); perr == nil && secs > 0 {
			retryAfter = time.Duration(secs) * time.Second
		}
	}
	var body string
	if resp.Body != nil {
		buf := make([]byte, qwpUpgradeBodySnippetCap+1)
		n, _ := io.ReadFull(resp.Body, buf)
		switch {
		case n <= 0:
			// no body or unreadable; leave empty
		case n > qwpUpgradeBodySnippetCap:
			body = strings.TrimSpace(string(buf[:qwpUpgradeBodySnippetCap])) + "…"
		default:
			body = strings.TrimSpace(string(buf[:n]))
		}
	}
	return &QwpUpgradeRejectError{
		StatusCode: resp.StatusCode,
		Role:       role,
		Zone:       zone,
		RetryAfter: retryAfter,
		Body:       body,
		cause:      cause,
	}
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
// status byte). The payload is validated against the exact shape
// required by spec §13: OK and DURABLE_ACK frames carry per-table
// watermark entries and must consume the frame exactly; error frames
// must end exactly at status + sequence + msg_len + msg. This
// mirrors the Java client's WebSocketResponse.isStructurallyValid
// and fails loudly on any unrecognized shape (e.g. a legacy 9-byte
// OK response) instead of decoding it into garbage fields.
//
//   - OK ACKs are status(1) + sequence(8) + tableCount(2) +
//     tableCount × (nameLen(2) + name + seqTxn(8)). Minimum 11 bytes;
//     the trailing per-table entries section must consume the rest of
//     the payload exactly.
//
//   - DURABLE_ACK frames are unsolicited per-table watermarks. They
//     are validated and returned to the caller with status
//     QwpStatusDurableAck — the caller decides what to do with them
//     (the cursor send loop ignores them and reads on). Servers only
//     emit them when the client opts in via the X-QWP-Request-Durable-
//     Ack header, which this transport does not set.
//
//   - Error ACKs are exactly qwpAckErrorHeaderSize + msg_len bytes.
//
//     OK:           [status (0x00)] [sequence: int64 LE] [tableCount: uint16 LE] [entries…]
//     DURABLE_ACK:  [status (0x02)]                      [tableCount: uint16 LE] [entries…]
//     Error:        [status]        [sequence: int64 LE] [msg_len: uint16 LE]   [msg: UTF-8]
//
// Each table entry is [nameLen: uint16 LE] [name (nameLen bytes UTF-8)]
// [seqTxn: int64 LE]. nameLen must be > 0 — empty names are rejected.
func (t *qwpTransport) readAck(ctx context.Context) (QwpStatusCode, []byte, error) {
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
	if len(data) < 1 {
		return 0, nil, fmt.Errorf("qwp: ack too short: %d bytes", len(data))
	}

	statusCode := QwpStatusCode(data[0])
	switch statusCode {
	case QwpStatusOK:
		if len(data) < qwpAckOKMinSize {
			return 0, nil, fmt.Errorf("qwp: malformed OK ack: got %d bytes, want at least %d", len(data), qwpAckOKMinSize)
		}
		if err := validateAckTableEntries(data[qwpAckOKTablesOffset:]); err != nil {
			return 0, nil, fmt.Errorf("qwp: malformed OK ack: %w", err)
		}
		return statusCode, data, nil
	case QwpStatusDurableAck:
		if len(data) < qwpAckDurableMinSize {
			return 0, nil, fmt.Errorf("qwp: malformed durable ack: got %d bytes, want at least %d", len(data), qwpAckDurableMinSize)
		}
		if err := validateAckTableEntries(data[qwpAckDurableTablesOff:]); err != nil {
			return 0, nil, fmt.Errorf("qwp: malformed durable ack: %w", err)
		}
		return statusCode, data, nil
	}
	// Error frame.
	if len(data) < qwpAckErrorHeaderSize {
		return 0, nil, fmt.Errorf("qwp: malformed error ack: got %d bytes, want at least %d", len(data), qwpAckErrorHeaderSize)
	}
	msgLen := int(binary.LittleEndian.Uint16(data[qwpAckErrorMsgLenOffset : qwpAckErrorMsgLenOffset+2]))
	if len(data) != qwpAckErrorHeaderSize+msgLen {
		return 0, nil, fmt.Errorf("qwp: malformed error ack: status=0x%02X, got %d bytes, want %d", byte(statusCode), len(data), qwpAckErrorHeaderSize+msgLen)
	}
	return statusCode, data, nil
}

// validateAckTableEntries walks the per-table watermark trailer of an
// OK or DURABLE_ACK frame and checks that its declared length consumes
// the buffer exactly. Returns nil on success or a descriptive error
// for any truncation, lying-length entry, empty table name, or
// trailing garbage.
func validateAckTableEntries(tail []byte) error {
	if len(tail) < 2 {
		return fmt.Errorf("missing table count")
	}
	tableCount := int(binary.LittleEndian.Uint16(tail[0:2]))
	off := 2
	for i := 0; i < tableCount; i++ {
		if len(tail) < off+2 {
			return fmt.Errorf("truncated table entry %d (header)", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(tail[off : off+2]))
		off += 2
		// Empty names indicate a corrupt or hostile payload — match
		// the Java client and reject them. A valid table name is
		// never zero bytes.
		if nameLen == 0 {
			return fmt.Errorf("empty table name in entry %d", i)
		}
		if len(tail) < off+nameLen+8 {
			return fmt.Errorf("truncated table entry %d (body)", i)
		}
		off += nameLen + 8
	}
	if off != len(tail) {
		return fmt.Errorf("trailing %d bytes after %d table entries", len(tail)-off, tableCount)
	}
	return nil
}

// parseAckTableEntries parses the per-table (name, seqTxn) watermark
// trailer of an OK or DURABLE_ACK frame into a slice. `tail` must start at
// the tableCount uint16: data[qwpAckOKTablesOffset:] for an OK frame,
// data[qwpAckDurableTablesOff:] for a DURABLE_ACK frame.
//
// Precondition: `tail` has already passed validateAckTableEntries (readAck
// runs it before returning the frame), so every declared length is sound
// and the walk needs no bounds re-checking beyond the loop guard. Returns
// nil when the frame carried no table entries. Used by the durable-ack
// receiver path (qwp_sf_send_loop.go) to feed qwpSfDurableTracker.
func parseAckTableEntries(tail []byte) []qwpAckTableEntry {
	if len(tail) < 2 {
		return nil
	}
	tableCount := int(binary.LittleEndian.Uint16(tail[0:2]))
	if tableCount == 0 {
		return nil
	}
	out := make([]qwpAckTableEntry, 0, tableCount)
	off := 2
	for i := 0; i < tableCount; i++ {
		nameLen := int(binary.LittleEndian.Uint16(tail[off : off+2]))
		off += 2
		name := string(tail[off : off+nameLen])
		off += nameLen
		seqTxn := int64(binary.LittleEndian.Uint64(tail[off : off+8]))
		off += 8
		out = append(out, qwpAckTableEntry{name: name, seqTxn: seqTxn})
	}
	return out
}

// parseAckError extracts an error message from a non-OK, non-durable
// ACK payload. The layout is:
//
//	[statusCode: uint8] [sequence: int64 LE] [errorLength: uint16 LE] [errorMessage: UTF-8]
//
// Precondition: data has already been validated by readAck, which
// guarantees at least qwpAckErrorHeaderSize bytes for error statuses
// and that the trailing bytes match the declared errorLength.
func parseAckError(data []byte) string {
	errLen := int(binary.LittleEndian.Uint16(data[qwpAckErrorMsgLenOffset : qwpAckErrorMsgLenOffset+2]))
	start := qwpAckErrorHeaderSize
	return string(data[start : start+errLen])
}

// parseAckSequence extracts the cumulative sequence number from an
// OK or error ACK payload. The wire field is signed (int64 LE) and
// uses -1 as a sentinel; matches Java's long semantics. DURABLE_ACK
// frames have no sequence — callers must skip them before calling.
//
// Precondition: data has already been validated by readAck.
func parseAckSequence(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data[qwpAckSequenceOffset : qwpAckSequenceOffset+8]))
}

// close shuts the WebSocket down with a graceful close frame. Idempotent
// and safe to call concurrently with the egress reader/dispatcher: it
// closes the conn — which unblocks and errors their in-flight Read/Write
// — but never mutates the conn field, so it cannot race their lock-free
// reads of it. coder/websocket's Conn.Close is itself safe under
// concurrent and repeated calls; closeOnce additionally pins one result.
func (t *qwpTransport) close() error {
	if t.conn == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		t.closeErr = t.conn.Close(websocket.StatusNormalClosure, "")
	})
	return t.closeErr
}

// ping sends a WebSocket PING and waits for the matching PONG (or ctx).
// The durable-ack keepalive uses it to prod an otherwise-idle server into
// flushing pending DURABLE_ACK frames (the OSS server flushes them on
// inbound events). A PONG timeout (ctx deadline exceeded) is benign — the
// PING frame was already written, which is what nudges the server — so the
// caller treats only a non-deadline error as a transport failure. The PONG
// is delivered via the receiver goroutine's Read; coder/websocket makes
// Ping safe to call concurrently with Read/Write.
func (t *qwpTransport) ping(ctx context.Context) error {
	if t.conn == nil {
		return fmt.Errorf("qwp: not connected")
	}
	return t.conn.Ping(ctx)
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
	// seq is the next batch's cumulative ACK sequence, 0-based: the
	// first batch (FSN 0) is acked as sequence 0, matching the real
	// server and the producer's FSN numbering.
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
			var ack [13]byte
			// Unmasked binary frame: FIN+BINARY=0x82, payload length=11.
			ack[0] = 0x82
			ack[1] = 0x0B
			// Payload: status OK (0x00) + cumulative sequence (uint64 LE)
			// + tableCount=0 (uint16 LE). The 2-byte zero-table-count
			// trailer is required by the QWP §13 OK ACK shape. The
			// sequence is 0-based and built before the post-increment so
			// dump mode exercises the same ACK path as production.
			ack[2] = 0x00 // STATUS_OK
			binary.LittleEndian.PutUint64(ack[3:], seq)
			binary.LittleEndian.PutUint16(ack[11:], 0)
			if _, err := conn.Write(ack[:]); err != nil {
				return
			}
			seq++
		}
		// Ignore other opcodes (ping/pong handled by WS library).
	}
}
