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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

// qwpQueryCleanupDrainTimeout bounds the drain that happens on
// close-path cleanup (QwpQuery.Close, iterator break-out, Exec-on-
// SELECT misuse). Deliberately independent of the caller's context so
// the dispatcher returns to idle and the client stays usable for a
// follow-up Query/Exec even when the caller's ctx has already expired
// by the time cleanup runs. 5s matches the Java client's
// shutdownJoinMs default.
const qwpQueryCleanupDrainTimeout = 5 * time.Second

// QwpQueryClient is a QuestDB query-side (egress) client. It opens one
// WebSocket connection to /read/v1, runs a dedicated I/O goroutine
// pair (reader + dispatcher), and streams result batches to the caller
// via Query/Exec. The I/O goroutines read and decode ahead of the
// consumer up to the configured buffer-pool depth.
//
// Thread safety: not safe for concurrent Query or Exec calls on the
// same client. Open one client per query-issuing goroutine. Cancel
// (on the returned *QwpQuery) and Close are safe to call from other
// goroutines.
type QwpQueryClient struct {
	cfg       *qwpQueryClientConfig
	transport qwpTransport
	io        *qwpEgressIO

	// nextRequestId is the monotonic client-assigned request id
	// handed to the I/O goroutine on each submit. Assigned from the
	// user goroutine inside Query/Exec; not accessed from other
	// goroutines (one query at a time).
	nextRequestId int64

	// closed guards Close against double-close and later Query/Exec.
	closed atomic.Bool
	// closeOnce ensures the teardown side effects (I/O shutdown,
	// transport close) run at most once even under concurrent Close
	// callers.
	closeOnce sync.Once
}

// QwpQueryClientOption is a functional option for NewQwpQueryClient.
// Deliberately a distinct type from LineSenderOption — the two clients
// share no transport code above qwpTransport, and using a different
// option type prevents misuse (e.g. passing an ingest option to the
// query constructor).
type QwpQueryClientOption func(*qwpQueryClientConfig)

// WithQwpQueryAddress overrides the default "localhost:9000" server
// address. Form is "host:port".
func WithQwpQueryAddress(addr string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.address = addr }
}

// WithQwpQueryEndpointPath overrides the default "/read/v1" WebSocket
// upgrade path. Rarely needed — present for parity with Java's
// withEndpointPath and to support reverse-proxy rewrites.
func WithQwpQueryEndpointPath(path string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.endpointPath = path }
}

// WithQwpQueryAuth sets the raw Authorization HTTP header value sent
// on the WebSocket upgrade. Mutually exclusive with
// WithQwpQueryBasicAuth and WithQwpQueryBearerToken.
func WithQwpQueryAuth(authHeader string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.authorization = authHeader }
}

// WithQwpQueryBasicAuth enables HTTP Basic authentication. The server
// validates against the same user store that the Postgres wire
// protocol uses — a user created via CREATE USER ... WITH PASSWORD ...
// works unchanged.
func WithQwpQueryBasicAuth(username, password string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) {
		c.httpUser = username
		c.httpPass = password
	}
}

// WithQwpQueryBearerToken enables HTTP Bearer authentication with an
// OIDC access token. The server verifies the token via its configured
// OIDC provider.
func WithQwpQueryBearerToken(token string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.httpToken = token }
}

// WithQwpQueryClientID overrides the default X-QWP-Client-Id header
// sent on the WebSocket upgrade. Empty uses the module default.
func WithQwpQueryClientID(id string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.clientID = id }
}

// WithQwpQueryBufferPoolSize overrides the decode buffer pool depth.
// Larger pools let the dispatcher decode further ahead of a slow
// consumer; smaller pools reduce memory but stall the dispatcher
// sooner. Must be >= 1.
func WithQwpQueryBufferPoolSize(size int) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.bufferPoolSize = size }
}

// WithQwpQueryMaxBatchRows asks the server to cap each RESULT_BATCH
// at the given row count. 0 omits the header and lets the server use
// its own cap. Useful for latency-sensitive streaming consumers that
// want the first rows sooner.
func WithQwpQueryMaxBatchRows(rows int) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.maxBatchRows = rows }
}

// WithQwpQueryInitialCredit opts the next query into credit-based
// egress flow control with the given initial byte budget. The server
// streams at most `bytes` of result payload before pausing; the
// client auto-replenishes by the size of each batch after the
// consumer releases it. 0 (the default) disables flow control.
func WithQwpQueryInitialCredit(bytes int64) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.initialCredit = bytes }
}

// WithQwpQueryCompression selects the compression codec advertised to
// the server on the WebSocket upgrade. Accepted values: "raw" (default,
// no compression, accept-encoding header omitted), "zstd" (demand zstd,
// fall back to raw if the server cannot), "auto" (advertise both and
// let the server pick). Anything else surfaces as an error from the
// constructor. Matches Java QwpQueryClient.withCompression's
// preference argument.
func WithQwpQueryCompression(preference string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.compression = preference }
}

// WithQwpQueryCompressionLevel overrides the zstd compression level
// hint the client sends in the accept-encoding header. Ignored when
// the compression preference is "raw". Accepts [1, 22] matching
// Java; the server clamps down to its own supported range.
func WithQwpQueryCompressionLevel(level int) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.compressionLevel = level }
}

// WithQwpQueryTls enables TLS with full certificate validation against
// the system cert pool.
func WithQwpQueryTls() QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.tlsMode = tlsEnabled }
}

// WithQwpQueryTlsInsecureSkipVerify enables TLS but skips certificate
// validation. Intended for testing only.
func WithQwpQueryTlsInsecureSkipVerify() QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.tlsMode = tlsInsecureSkipVerify }
}

// NewQwpQueryClient constructs a QwpQueryClient from functional options
// and opens the WebSocket connection. Matches Java
// QwpQueryClient.newPlainText + connect(), but bundled into one call
// since Go does not usually separate construction from connection.
func NewQwpQueryClient(ctx context.Context, opts ...QwpQueryClientOption) (*QwpQueryClient, error) {
	cfg := qwpQueryDefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return newQwpQueryClient(ctx, cfg)
}

// QwpQueryClientFromConf constructs a QwpQueryClient from a ws:: /
// wss:: config string and opens the WebSocket connection. See
// parseQwpQueryConf for the full key reference.
func QwpQueryClientFromConf(ctx context.Context, conf string) (*QwpQueryClient, error) {
	cfg, err := parseQwpQueryConf(conf)
	if err != nil {
		return nil, err
	}
	return newQwpQueryClient(ctx, cfg)
}

// newQwpQueryClient is the internal factory shared by both public
// entry points. It performs validation, opens the transport, and
// spawns the I/O goroutines.
func newQwpQueryClient(ctx context.Context, cfg *qwpQueryClientConfig) (*QwpQueryClient, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := &QwpQueryClient{
		cfg:           cfg,
		nextRequestId: 1, // match Java's QwpQueryClient.nextRequestId initial value
	}

	scheme := "ws"
	if cfg.tlsMode != tlsDisabled {
		scheme = "wss"
	}
	wsURL := scheme + "://" + cfg.address

	opts := qwpTransportOpts{
		tlsInsecureSkipVerify: cfg.tlsMode == tlsInsecureSkipVerify,
		endpointPath:          cfg.endpointPath,
		authorization:         cfg.effectiveAuthorization(),
		maxBatchRows:          cfg.maxBatchRows,
		acceptEncoding:        cfg.buildAcceptEncodingHeader(),
	}
	if err := c.transport.connect(ctx, wsURL, opts); err != nil {
		return nil, err
	}
	// Early probe: if we told the server we can accept zstd, round-
	// trip a transient decoder so any klauspost/compress init failure
	// surfaces here on the user goroutine rather than mid-stream on
	// the first compressed batch. Matches Java's probeZstdAvailable
	// in intent; cheaper in pure Go since there is no JNI library to
	// load.
	if cfg.compression != qwpCompressionRaw {
		if err := probeZstdAvailable(); err != nil {
			_ = c.transport.close(ctx)
			return nil, err
		}
	}
	c.io = newQwpEgressIO(&c.transport, cfg.bufferPoolSize)
	c.io.start()
	return c, nil
}

// probeZstdAvailable allocates and immediately closes a zstd decoder
// so init-time failures (allocation pressure, bundled-library issues)
// surface synchronously at construction time. The Go port is simpler
// than Java's because klauspost/compress is pure Go — there is no
// native library to be missing. The probe still serves as a small
// sanity gate and matches Java's ordering (init after upgrade so
// transport errors surface first).
func probeZstdAvailable() error {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("qwp query: zstd decoder init failed: %w", err)
	}
	dec.Close()
	return nil
}

// effectiveAuthorization computes the Authorization header value
// from the config, resolving the three mutually-exclusive auth modes
// into a single header string.
func (c *qwpQueryClientConfig) effectiveAuthorization() string {
	if c.authorization != "" {
		return c.authorization
	}
	if c.httpUser != "" && c.httpPass != "" {
		creds := c.httpUser + ":" + c.httpPass
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))
	}
	if c.httpToken != "" {
		return "Bearer " + c.httpToken
	}
	return ""
}

// Close shuts down the I/O goroutines, sends a WebSocket close frame,
// and releases the underlying connection. Safe to call more than
// once; subsequent calls return nil.
//
// Must be called after every in-flight Query/Exec has returned.
// Calling Close while a *QwpQuery.Batches() loop body is still using
// the batch's aliased []byte slices is undefined: the transport may
// free buffers the caller is still reading.
func (c *QwpQueryClient) Close(ctx context.Context) error {
	var firstErr error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.io != nil {
			if err := c.io.shutdown(ctx); err != nil {
				firstErr = err
			}
		}
		if err := c.transport.close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

// Query submits a SELECT-style statement and returns a cursor over its
// result batches. The server-side execution begins immediately; the
// cursor drains events lazily as the caller ranges over Batches().
//
// Err on a wrong statement kind surfaces through the first Batches()
// yield: if the server sends EXEC_DONE (non-SELECT statement), the
// iterator yields (nil, error) and terminates. Use Exec for
// statements that do not produce a result set.
//
// Breaking out of the range loop early sends a CANCEL frame to the
// server and drains the remaining events until a terminal frame
// arrives. Always defer (*QwpQuery).Close() to guarantee cleanup on
// any path.
func (c *QwpQueryClient) Query(ctx context.Context, sql string) *QwpQuery {
	q := &QwpQuery{
		client: c,
		ctx:    ctx,
		sql:    sql,
	}
	if c.closed.Load() {
		q.pendingErr = errors.New("qwp query: client is closed")
		q.done.Store(true)
		return q
	}
	reqId := c.nextRequestId
	c.nextRequestId++
	q.requestId = reqId
	if err := c.io.submitQuery(ctx, qwpRequest{
		sql:           sql,
		requestId:     reqId,
		initialCredit: c.cfg.initialCredit,
	}); err != nil {
		q.pendingErr = err
		q.done.Store(true)
	}
	return q
}

// Exec runs a non-SELECT statement (DDL / INSERT / UPDATE / ...) and
// blocks until the server returns EXEC_DONE or a terminal error. On
// success returns the ExecResult (op type + rows affected). On a
// QUERY_ERROR frame the returned error is a *QwpQueryError; on a
// transport or decode failure it is a plain error.
//
// Calling Exec on a SELECT statement returns an error — SELECT sends
// RESULT_BATCH + RESULT_END, which Exec does not expect. Use Query
// for SELECTs.
func (c *QwpQueryClient) Exec(ctx context.Context, sql string) (ExecResult, error) {
	if c.closed.Load() {
		return ExecResult{}, errors.New("qwp query: client is closed")
	}
	reqId := c.nextRequestId
	c.nextRequestId++

	if err := c.io.submitQuery(ctx, qwpRequest{
		sql:           sql,
		requestId:     reqId,
		initialCredit: c.cfg.initialCredit,
	}); err != nil {
		return ExecResult{}, err
	}

	for {
		ev, err := c.io.takeEvent(ctx)
		if err != nil {
			// ctx expired or I/O terminated before we saw a terminal
			// event. Cancel + drain on a cleanup ctx so the dispatcher
			// returns to idle; otherwise the next Query/Exec on this
			// client blocks on the single-slot requests channel.
			c.io.requestCancel(reqId)
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(), qwpQueryCleanupDrainTimeout)
			_ = drainUntilTerminal(cleanupCtx, c.io)
			cleanupCancel()
			return ExecResult{}, err
		}
		switch ev.kind {
		case qwpEventKindExecDone:
			return ev.execResult, nil
		case qwpEventKindError:
			return ExecResult{}, eventToError(ev, reqId)
		case qwpEventKindBatch:
			// Server streamed a result batch for what we asked for as
			// an exec. Release the buffer, send a CANCEL so the
			// server stops streaming the rest of the result set, and
			// drain to a terminal frame on a cleanup-bounded context
			// so the dispatcher returns to idle regardless of the
			// caller's ctx. Then surface the type-mismatch.
			c.io.releaseBuffer(ev.batch)
			c.io.requestCancel(reqId)
			cleanupCtx, cancel := context.WithTimeout(
				context.Background(), qwpQueryCleanupDrainTimeout)
			_ = drainUntilTerminal(cleanupCtx, c.io)
			cancel()
			return ExecResult{}, fmt.Errorf(
				"qwp query: Exec called on a SELECT-style statement; use Query instead")
		case qwpEventKindEnd:
			// Bare RESULT_END with no preceding RESULT_BATCH — same
			// misuse as above (user ran a SELECT via Exec).
			return ExecResult{}, fmt.Errorf(
				"qwp query: Exec called on a SELECT-style statement; use Query instead")
		default:
			return ExecResult{}, fmt.Errorf("qwp query: unexpected event kind %d", ev.kind)
		}
	}
}

// drainUntilTerminal reads and discards events until a terminal one
// (End / ExecDone / Error) arrives. Releases any batch buffers along
// the way. Returns a transport/context error if takeEvent fails.
func drainUntilTerminal(ctx context.Context, io *qwpEgressIO) error {
	for {
		ev, err := io.takeEvent(ctx)
		if err != nil {
			return err
		}
		switch ev.kind {
		case qwpEventKindBatch:
			io.releaseBuffer(ev.batch)
		case qwpEventKindEnd, qwpEventKindExecDone, qwpEventKindError:
			return nil
		}
	}
}

// eventToError converts a qwpEventKindError event into the most
// specific Go error type available. Server-sent QUERY_ERROR frames
// (status > 0) become *QwpQueryError; synthesized client-side errors
// (status == 0, set by emitError) stay as plain errors.
func eventToError(ev qwpEvent, reqId int64) error {
	if ev.errStatus != 0 {
		id := ev.requestId
		if id == 0 {
			id = reqId
		}
		return &QwpQueryError{
			RequestId: id,
			Status:    ev.errStatus,
			Message:   ev.errMessage,
		}
	}
	if ev.errMessage != "" {
		return errors.New(ev.errMessage)
	}
	return errors.New("qwp query: unspecified error")
}

// QwpQuery is a streaming cursor over a SELECT result set returned by
// (*QwpQueryClient).Query. It is single-use: once the range over
// Batches() terminates (by End, Error, or break), the cursor is done
// and must not be iterated again.
//
// Thread safety: Batches and the buffers it yields are single-consumer
// — do not share the cursor across goroutines. Cancel and Close are
// safe to call from other goroutines.
type QwpQuery struct {
	client *QwpQueryClient
	ctx    context.Context
	sql    string

	// requestId is the client-assigned id for this query. Captured
	// from the client's nextRequestId counter at Query() time so a
	// concurrent Cancel sends a CANCEL for this query, not whatever
	// is currently in flight.
	requestId int64

	// totalRows is set when a RESULT_END frame arrives. Read via
	// TotalRows(). Default 0 on a query that never reached End
	// (cancelled, errored, or still running).
	totalRows int64

	// pendingErr holds an error surfaced at submit time (closed
	// client, submit blocked on ctx cancel). Yielded on the first
	// iteration of Batches() so callers discover it naturally.
	pendingErr error

	// done is set true after the iterator reaches a terminal event
	// (RESULT_END / EXEC_DONE / QUERY_ERROR / transport failure), a
	// synthesized error from the wrong statement kind, or a caller-
	// driven break-out. Further iterations become no-ops.
	done atomic.Bool

	// cancelled records whether Cancel() has been invoked. Used to
	// avoid emitting a synthesized "cancelled by caller" error on top
	// of the server's QUERY_ERROR(status=CANCELLED) echo.
	cancelled atomic.Bool
}

// Batches returns a range-over-func iterator that yields each
// RESULT_BATCH frame as a *QwpColumnBatch, along with an optional
// error. The iterator terminates on RESULT_END (clean end), a
// QUERY_ERROR from the server (yielded as the last element's error),
// a transport/decode failure (same), or the caller breaking out of
// the range loop (sends CANCEL to the server, drains remaining
// events).
//
// The yielded *QwpColumnBatch is only valid inside the body of the
// current iteration — its slices alias the pool-owned decode buffer
// and will be reused for the next batch. Use batch.CopyAll() to
// retain data across iterations.
func (q *QwpQuery) Batches() iter.Seq2[*QwpColumnBatch, error] {
	return func(yield func(*QwpColumnBatch, error) bool) {
		if q.done.Load() {
			if q.pendingErr != nil {
				yield(nil, q.pendingErr)
				q.pendingErr = nil
			}
			return
		}
		defer q.done.Store(true)

		for {
			ev, err := q.client.io.takeEvent(q.ctx)
			if err != nil {
				// takeEvent returned before a terminal frame (most
				// often q.ctx expired while we were waiting on the
				// server). The dispatcher is still parked in
				// receiveLoop for this query, so cancel + drain on a
				// cleanup ctx before returning — symmetrical to the
				// !keepGoing break-out below. The caller's deferred
				// Close() sees done=true (set by the defer on this
				// function) and becomes a no-op; without this drain
				// the dispatcher would stay stuck and strand the
				// client for follow-up Query/Exec.
				yield(nil, err)
				q.cancelAndDrainOnCleanupCtx()
				return
			}
			switch ev.kind {
			case qwpEventKindBatch:
				keepGoing := false
				func() {
					// Release the buffer even if the caller's yield
					// body panics. Without this, a single panic with
					// bufferPoolSize=1 permanently starves the pool,
					// and the dispatcher — still parked in receiveLoop
					// for this query — blocks the next Query/Exec.
					// On panic we also run the cancel+drain before
					// rethrowing: the outer `defer q.done.Store(true)`
					// has already flipped done=true, so the caller's
					// defer q.Close() would otherwise be a no-op and
					// leave the dispatcher stranded.
					defer func() {
						q.client.io.releaseBuffer(ev.batch)
						if r := recover(); r != nil {
							q.cancelAndDrainOnCleanupCtx()
							panic(r)
						}
					}()
					keepGoing = yield(&ev.batch.batch, nil)
				}()
				if !keepGoing {
					// User broke out — request cancel and drain the
					// remaining events until a terminal frame so the
					// dispatcher returns to idle and the next query
					// can submit cleanly. Drain uses a bounded cleanup
					// ctx independent of q.ctx because a common reason
					// to break out is exactly that q.ctx has expired.
					q.cancelAndDrainOnCleanupCtx()
					return
				}
			case qwpEventKindEnd:
				q.totalRows = ev.totalRows
				return
			case qwpEventKindError:
				// A server-sent cancellation echo (status=Cancelled)
				// in response to our own Cancel call is not an error
				// the caller needs to see — yielding it would make a
				// clean "I broke out of the loop" look like a
				// failure. Swallow that one case.
				if q.cancelled.Load() && ev.errStatus == qwpStatusCancelled {
					return
				}
				yield(nil, eventToError(ev, q.requestId))
				return
			case qwpEventKindExecDone:
				// Wrong statement kind: user ran a non-SELECT via
				// Query. Surface with a typed error so they can
				// switch to Exec.
				yield(nil, fmt.Errorf(
					"qwp query: Query called on a non-SELECT statement; use Exec instead"))
				return
			default:
				yield(nil, fmt.Errorf("qwp query: unexpected event kind %d", ev.kind))
				return
			}
		}
	}
}

// TotalRows returns the server-reported total-row count from the
// RESULT_END frame, or 0 if the query did not reach End (cancelled,
// errored, or still running).
func (q *QwpQuery) TotalRows() int64 {
	return q.totalRows
}

// RequestId returns the client-assigned id for this query. Exposed
// mainly for test instrumentation and cross-correlating logs with
// server-side request ids.
func (q *QwpQuery) RequestId() int64 {
	return q.requestId
}

// Cancel asks the server to abort the current query. Safe to call
// from any goroutine, including before the first Batches() iteration
// or while another goroutine is ranging over Batches(). A no-op if
// the query has already reached a terminal state.
//
// The cancel is asynchronous: Batches() keeps yielding whatever the
// server has already buffered before it reacts to the CANCEL. The
// server eventually responds with QUERY_ERROR(status=CANCELLED),
// which Batches() swallows silently so a caller-initiated Cancel
// produces a clean end of iteration.
func (q *QwpQuery) Cancel() {
	if q.done.Load() {
		return
	}
	q.cancelled.Store(true)
	q.client.io.requestCancel(q.requestId)
}

// Close finalizes the cursor. Drains any pending events to a
// terminal frame so the underlying I/O dispatcher returns to idle —
// required before the next Query or Exec on the same client. Safe
// to defer even on already-finished queries; the second call is a
// no-op.
//
// Does not close the client itself. Call (*QwpQueryClient).Close
// to release the underlying WebSocket connection.
func (q *QwpQuery) Close() {
	if !q.done.CompareAndSwap(false, true) {
		return
	}
	// Reached only on explicit Close-without-draining — when the
	// iterator runs to a terminal event or bails via the takeEvent-
	// error / break-out paths, it sets done=true via its deferred
	// Store and those paths perform their own cancel+drain, so the
	// CAS above would already have returned false.
	q.cancelAndDrainOnCleanupCtx()
}

// cancelAndDrainOnCleanupCtx sends a CANCEL for this query's
// requestId (unless one is already in flight) and drains pending
// events until a terminal frame arrives, so the dispatcher returns
// to idle regardless of q.ctx's state. Uses a fresh bounded context
// because every caller either runs after q.ctx has already been
// observed done (iterator break-out, takeEvent-error) or inside a
// user-driven Close which has no meaningful ctx of its own.
func (q *QwpQuery) cancelAndDrainOnCleanupCtx() {
	if !q.cancelled.Load() {
		q.cancelled.Store(true)
		q.client.io.requestCancel(q.requestId)
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), qwpQueryCleanupDrainTimeout)
	defer cancel()
	_ = drainUntilTerminal(cleanupCtx, q.client.io)
}
