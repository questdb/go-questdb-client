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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
)

// qwpEventKind tags a qwpEvent produced by the egress I/O goroutine.
type qwpEventKind byte

const (
	qwpEventKindBatch    qwpEventKind = iota + 1 // RESULT_BATCH: batch field valid
	qwpEventKindEnd                              // RESULT_END: totalRows valid
	qwpEventKindExecDone                         // EXEC_DONE: execResult valid
	// qwpEventKindError is the server's QUERY_ERROR frame. The
	// connection is still healthy; the next query may submit on the
	// same I/O goroutine. Surfaced to the user as *QwpQueryError.
	qwpEventKindError
	// qwpEventKindTransportError is a synthesized client-side terminal
	// failure: reader closed, server closed, decoder out of sync, send
	// failure, or unknown msg_kind. The connection's per-connection
	// state is no longer trustworthy and the I/O goroutine has
	// poisoned ioErr — every emission of this kind goes through
	// poisonAndEmitError, so a follow-up submitQuery returns the
	// original cause synchronously. Routed by the failover orchestrator
	// to the reconnect-and-replay path; surfaced to the user as a
	// plain error when failover is disabled or exhausted.
	qwpEventKindTransportError
	// qwpEventKindFailoverReset is emitted by the session orchestrator
	// (not the I/O goroutine) on the consumer side after a successful
	// reconnect and resubmit. Carries the new generation's
	// QwpServerInfo so the user can discard accumulated rows from the
	// prior connection. Internal to qwp_query_failover.go;
	// qwp_query_io.go never produces this kind directly.
	qwpEventKindFailoverReset
)

// qwpEvent is the discriminated-union event carried on qwpEgressIO.events
// from the I/O goroutine to the user. Fields are valid only for the
// matching kind — see the constants above.
type qwpEvent struct {
	kind      qwpEventKind
	requestId int64

	// Batch kind
	batch *qwpBatchBuffer

	// End kind
	totalRows int64

	// ExecDone kind
	execResult ExecResult

	// Error kind — carries the server-reported QUERY_ERROR status +
	// message. TransportError kind reuses errMessage; status is
	// always 0 on the synthesized variant since it does not
	// correspond to a server status byte. FailoverReset kind reuses
	// failoverReset.
	errStatus  qwpStatusCode
	errMessage string

	// TransportError kind — optional typed cause. When set, consumers
	// wrap with %w so callers can errors.As against the underlying
	// type (e.g. *QwpRoleMismatchError raised by a failed reconnect).
	// Nil for I/O-goroutine-emitted transport errors that only carry
	// a string message via poisonAndEmitError.
	transportErr error

	// FailoverReset kind — populated by qwp_query_failover.go after a
	// successful reconnect; carries through to the user as
	// *QwpFailoverReset. Nil for any other kind.
	failoverReset *QwpFailoverReset
}

// qwpBatchBuffer is a pool-owned container for one decoded
// RESULT_BATCH. The I/O goroutine borrows a buffer from the pool before
// calling qwpQueryDecoder.decode into buf.batch; the user's consumer
// returns it to the pool via release() after processing.
//
// Lifetime: while the user holds the buffer, io.buffers is missing one
// slot. The I/O goroutine stops reading new frames once the pool is
// empty, providing natural backpressure against slow consumers.
type qwpBatchBuffer struct {
	batch QwpColumnBatch
	// payloadLen is the number of bytes the server spent on this batch
	// (== len(payload)). Captured at decode time so release() can feed
	// it to the credit-replenish counter when flow control is enabled.
	payloadLen int
	// io is the back-reference used by release() to return the buffer
	// to its owning pool.
	io *qwpEgressIO
}

// release hands the buffer back to the I/O goroutine's free pool. Safe
// to call at most once per batch event; further calls have undefined
// buffer-ownership semantics (the decoder may already be writing into
// the batch). Non-blocking — the pool is sized so a live I/O goroutine
// always has exactly one free slot for each buffer currently held
// outside.
func (b *qwpBatchBuffer) release() {
	b.io.releaseBuffer(b)
}

// qwpRequest is a pending query submission handed from the user
// goroutine to the I/O goroutine via qwpEgressIO.requests.
type qwpRequest struct {
	sql string
	// requestId is the client-assigned 64-bit id echoed back on every
	// frame for this query. The user assigns monotonically from a
	// per-client counter (see step 8).
	requestId int64
	// initialCredit is the server's send-ahead byte budget. 0 means
	// "unbounded" — no CREDIT frames exchanged. A positive value
	// opts the query into flow control: the server streams at most
	// initialCredit bytes before parking, and the I/O goroutine
	// replenishes by each batch's byte length after the consumer
	// releases its buffer.
	initialCredit int64
	// bindCount is the number of typed bind parameters encoded in
	// bindPayload, or 0 when the query has no binds.
	bindCount int
	// bindPayload is the pre-encoded typed bind-parameter block for
	// this query, or nil when bindCount == 0. Owned per request —
	// buildRequest copies from QwpQueryClient's reusable bind scratch
	// into this fresh slice before submitQuery, so a follow-up
	// query's reset + re-encode cannot race the dispatcher.
	bindPayload []byte
}

// qwpEgressIO owns the WebSocket transport plus the per-connection
// decoder state, runs the dedicated receive + dispatch goroutines,
// and shuttles events to the consumer.
//
// Goroutine topology (two internal goroutines):
//
//   - reader: blocks in conn.Read and pushes each incoming frame to
//     frameCh. Never sees cancel / credit / user state. Exits when
//     the server closes the connection or shutdown cancels readCtx.
//
//   - dispatcher (aka the "main" I/O goroutine): selects on frameCh /
//     notifyCh / shutdownCh, drains the cancel + credit atomics,
//     dispatches frames to the decoder, and emits events to the user.
//
// This split is deliberate: coder/websocket closes the underlying TCP
// connection when a Read's context is cancelled mid-frame, so we can
// NOT use ctx cancellation as a "kick" signal to drain pending
// cancels. Instead, the dispatcher listens on notifyCh alongside
// frameCh, reacts to user-initiated state changes without touching
// the Read, and only cancels readCtx on final shutdown (when
// destroying the connection is acceptable).
//
// Lifecycle: newQwpEgressIO → start → (submitQuery → takeEvent... →
// release... [+ requestCancel])* → shutdown.
//
// Threading contract:
//   - submitQuery, takeEvent, releaseBuffer: single user goroutine.
//     Concurrent submitQueries / takeEvents are not guaranteed to be
//     safe; Phase-1 supports one query in flight at a time.
//   - requestCancel: any goroutine.
//   - shutdown: any goroutine; idempotent.
//
// Not a public type — wrapped by QwpQueryClient in step 8.
type qwpEgressIO struct {
	transport *qwpTransport
	decoder   qwpQueryDecoder

	// buffers is the free-buffer pool. The dispatcher takes one
	// before decoding a RESULT_BATCH; the user returns it via
	// release() after processing. Capacity == bufferPoolSize.
	buffers chan *qwpBatchBuffer

	// events carries all outbound events to the consumer. Capacity ==
	// bufferPoolSize+2 so a trailing End/Error after every buffered
	// batch fits without blocking the producer. Closed by the
	// dispatcher on exit so a consumer parked on takeEvent wakes with
	// ok=false (rather than on a best-effort sentinel that could be
	// dropped when the channel is full).
	events chan qwpEvent

	// requests is the submission slot. Single-entry: Phase-1 assumes
	// one query at a time.
	requests chan qwpRequest

	// frameCh carries received frames from the reader to the
	// dispatcher. Unbuffered: the reader blocks until the dispatcher
	// is ready, which naturally backpressures the server via the
	// TCP window.
	frameCh chan qwpReaderEvent

	// notifyCh wakes the dispatcher when the user changes state
	// (requestCancel, releaseBuffer). Buffered size 1 with a non-
	// blocking send semantic (concurrent notifies coalesce): the
	// dispatcher drains the atomic on the next loop iteration, so
	// one pending notify always suffices to re-check.
	notifyCh chan struct{}

	// cancelRequestId is the pending-cancel latch. requestCancel
	// stores the to-be-cancelled requestId here; the dispatcher
	// swaps it back to -1 at every loop boundary and sends a CANCEL
	// frame if non-negative.
	cancelRequestId atomic.Int64

	// pendingCredit accumulates bytes to CREDIT-replenish on the next
	// loop iteration. release() Adds; the dispatcher Swaps(0). Only
	// consulted when creditEnabled.
	pendingCredit atomic.Int64

	// ioCtx / ioCancel gate every conn-level I/O this struct owns —
	// the reader's conn.Read and the dispatcher's conn.Write calls
	// (sendQueryRequest / sendCancel / sendCredit). Cancelled on
	// shutdown() to unblock both sides: cancelling tears down the
	// underlying conn via coder/websocket's ctx-driven AfterFunc,
	// which is fine at shutdown.
	//
	// Reusing the same ctx for both directions is deliberate. If only
	// the reader's Read ctx is cancelled and the dispatcher is parked
	// in Write on a peer that has stopped draining, Read's AfterFunc
	// tears down rwc only while Read is active; between Reads (e.g.
	// after the reader has consumed a frame and is parked on
	// frameCh), the AfterFunc is unregistered and shutdown can't
	// reach the dispatcher. The Write ctx closes that gap.
	ioCtx    context.Context
	ioCancel context.CancelFunc

	// shutdownCh closes when shutdown() is called for the first time.
	// doneCh closes when BOTH dispatcher and reader goroutines have
	// exited — shutdown() blocks on doneCh, so once it returns the
	// caller can safely close the transport without racing the
	// still-winding-down reader's conn.Read.
	shutdownCh   chan struct{}
	doneCh       chan struct{}
	shutdownOnce sync.Once
	shutdownWG   sync.WaitGroup
	// closed is set true right before the dispatcher returns so
	// releaseBuffer can early-exit instead of attempting a send on a
	// pool nobody reads from.
	closed atomic.Bool

	// sendBuf is scratch for QUERY_REQUEST / CANCEL / CREDIT frames.
	// Owned by the dispatcher; never aliased outside.
	sendBuf qwpWireBuffer

	// Per-query state, accessed only from the dispatcher.
	currentRequestId int64
	creditEnabled    bool
	currentQueryDone bool

	// ioErrMu guards ioErr. Set on the dispatcher goroutine from any
	// decoder- or framing-level error path; read on the user goroutine
	// from submitQuery.
	ioErrMu sync.Mutex
	// ioErr latches the first transport-class error for the life of
	// this connection: any reader-error / server-close, send failure,
	// decoder/framing desync, or unknown msg_kind. Once set, every
	// subsequent submitQuery returns this error synchronously so a
	// fresh query is never decoded against a desynced
	// qwpConnDict / qwpSchemaRegistry / zstd stream — an undetectable
	// subset of out-of-range reads could leave the dict accidentally
	// in sync with the server (offsets match) while values are wrong,
	// producing silently corrupted results — and never sent on a dead
	// conn either. Mirrors the ingress-side asyncState.ioErr
	// terminal-flag pattern (see CLAUDE.md).
	ioErr error
}

// qwpReaderEvent is what the reader goroutine hands to the dispatcher:
// either a successfully received binary frame (payload != nil, err ==
// nil) or a read error (payload == nil, err != nil). Non-binary frames
// are dropped inside the reader.
type qwpReaderEvent struct {
	payload []byte
	err     error
}

// newQwpEgressIO constructs an I/O controller attached to an already-
// connected transport. bufferPoolSize is the depth of the decode pool;
// must be >= 1.
func newQwpEgressIO(tr *qwpTransport, bufferPoolSize int) *qwpEgressIO {
	if bufferPoolSize < 1 {
		panic("qwp: bufferPoolSize must be >= 1")
	}
	ioCtx, ioCancel := context.WithCancel(context.Background())
	io := &qwpEgressIO{
		transport:  tr,
		buffers:    make(chan *qwpBatchBuffer, bufferPoolSize),
		events:     make(chan qwpEvent, bufferPoolSize+2),
		requests:   make(chan qwpRequest, 1),
		frameCh:    make(chan qwpReaderEvent),
		notifyCh:   make(chan struct{}, 1),
		ioCtx:      ioCtx,
		ioCancel:   ioCancel,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	io.cancelRequestId.Store(-1)
	io.currentRequestId = -1
	for i := 0; i < bufferPoolSize; i++ {
		io.buffers <- &qwpBatchBuffer{io: io}
	}
	return io
}

// start launches the dispatcher + reader goroutines. Must be called
// exactly once, before the first submitQuery.
//
// doneCh is closed by the WaitGroup-tracked wrapper once both
// goroutines have returned — not by the dispatcher alone. This is
// what makes tr.close() safe to call right after shutdown() returns:
// the reader's conn.Read has already unwound before doneCh fires.
func (io *qwpEgressIO) start() {
	io.shutdownWG.Add(2)
	go func() {
		defer io.shutdownWG.Done()
		io.dispatcherRun()
	}()
	go func() {
		defer io.shutdownWG.Done()
		io.readerRun()
	}()
	go func() {
		io.shutdownWG.Wait()
		close(io.doneCh)
	}()
}

// submitQuery hands the request to the I/O goroutine. Blocks if a
// prior query's submission has not yet been picked up (single-slot
// queue). Returns ctx.Err() on user cancellation, a sentinel error
// if the I/O goroutine has shut down, or the latched ioErr if a
// prior decoder/framing failure has poisoned the connection (a fresh
// submit would be decoded against desynced state).
func (io *qwpEgressIO) submitQuery(ctx context.Context, req qwpRequest) error {
	if err := io.loadIoErr(); err != nil {
		return err
	}
	// Non-blocking shutdown check first: if shutdownCh is already
	// closed, Go's select would otherwise non-deterministically pick
	// the buffered requests slot, leaving the request to rot after
	// the dispatcher has already returned.
	select {
	case <-io.shutdownCh:
		return errors.New("qwp: I/O goroutine shut down")
	default:
	}
	select {
	case io.requests <- req:
		return nil
	case <-io.shutdownCh:
		return errors.New("qwp: I/O goroutine shut down")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// setIoErr latches err as the connection's terminal ioErr — first
// writer wins. Called by the dispatcher (always via
// poisonAndEmitError) on any transport-class fault so subsequent
// submitQuery calls fail immediately rather than running a fresh
// query against a dead conn or desynced decoder.
func (io *qwpEgressIO) setIoErr(err error) {
	io.ioErrMu.Lock()
	defer io.ioErrMu.Unlock()
	if io.ioErr == nil {
		io.ioErr = err
	}
}

// loadIoErr returns the latched terminal error, or nil if none.
func (io *qwpEgressIO) loadIoErr() error {
	io.ioErrMu.Lock()
	defer io.ioErrMu.Unlock()
	return io.ioErr
}

// takeEvent pops the next event. Blocks until one arrives or ctx is
// cancelled. Returns a terminal error once the dispatcher has exited
// and its events channel is both drained and closed — so a consumer
// with a long-lived ctx always wakes after shutdown without having to
// rely on a best-effort sentinel.
func (io *qwpEgressIO) takeEvent(ctx context.Context) (qwpEvent, error) {
	select {
	case ev, ok := <-io.events:
		if !ok {
			return qwpEvent{}, errors.New("qwp: I/O goroutine terminated")
		}
		return ev, nil
	case <-ctx.Done():
		return qwpEvent{}, ctx.Err()
	}
}

// requestCancel marks a CANCEL frame as pending for requestId. Safe
// to call from any goroutine; coalesces with prior pending cancels
// (the newer id wins). Wakes the dispatcher so the cancel reaches
// the wire without waiting for the next server frame.
func (io *qwpEgressIO) requestCancel(requestId int64) {
	io.cancelRequestId.Store(requestId)
	io.notify()
}

// releaseBuffer returns a batch buffer to the free pool after the user
// handler is done with it. Must be called exactly once per KIND_BATCH
// event. Non-blocking.
func (io *qwpEgressIO) releaseBuffer(buf *qwpBatchBuffer) {
	if io.closed.Load() {
		// I/O goroutine is gone; the buffer's backing []byte will be
		// reclaimed by Go's GC once the user drops their reference.
		return
	}
	// Queue the bytes for credit replenish before returning the buffer
	// so the next dispatcher loop iteration's drainPendingCredit sees
	// the latest counter. When creditEnabled is false, the dispatcher
	// discards the counter; when true, it sends a CREDIT frame for
	// the accumulated bytes.
	io.pendingCredit.Add(int64(buf.payloadLen))
	select {
	case io.buffers <- buf:
	default:
		// Unreachable in practice: io.buffers has capacity
		// bufferPoolSize and at most bufferPoolSize buffers exist, so
		// a release can never overflow it. Non-blocking defensively —
		// if a double-release or similar accounting bug ever fills the
		// pool, we'd rather drop the extra buffer than deadlock here.
	}
	// Wake the dispatcher so the credit replenish (if flow control is
	// on) reaches the server without waiting for the next server-
	// initiated frame. Harmless when credit is disabled — the
	// dispatcher just re-enters its select.
	io.notify()
}

// shutdown signals both goroutines to exit and blocks until the
// dispatcher returns or ctx expires. Idempotent — repeated calls
// return immediately once the dispatcher has joined.
func (io *qwpEgressIO) shutdown(ctx context.Context) error {
	io.shutdownOnce.Do(func() {
		close(io.shutdownCh)
		// Cancel the shared I/O ctx. coder/websocket tears down the
		// underlying TCP when an active Read or Write's ctx is
		// cancelled — acceptable here because we are destroying the
		// connection anyway. Cancelling both directions matters: if
		// the dispatcher is parked inside conn.Write on a peer that
		// has stopped draining and the reader is not currently inside
		// conn.Read, only the Write's own ctx can unstick it.
		io.ioCancel()
	})
	select {
	case <-io.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// notify signals the dispatcher that user state (cancel atomic,
// credit atomic, pool) has changed. Non-blocking: if a notify is
// already pending, the dispatcher will still re-check both atomics
// in its next iteration, so we coalesce.
func (io *qwpEgressIO) notify() {
	select {
	case io.notifyCh <- struct{}{}:
	default:
	}
}

// readerRun is the reader goroutine's top-level loop. It does nothing
// but pull binary frames off the WebSocket and hand them to the
// dispatcher via frameCh. Never looks at cancel / credit / user state
// — kept minimal so a blocked Read stays out of the dispatch-side
// fast path.
//
// Exits when either (a) conn.Read returns an error (server close,
// malformed frame, or shutdown-cancelled readCtx), or (b) the
// dispatcher is shut down. Closes frameCh on the way out so the
// dispatcher's select sees EOF.
func (io *qwpEgressIO) readerRun() {
	defer close(io.frameCh)
	for {
		msgType, data, err := io.transport.conn.Read(io.ioCtx)
		if err != nil {
			select {
			case io.frameCh <- qwpReaderEvent{err: err}:
			case <-io.shutdownCh:
			}
			return
		}
		if msgType != websocket.MessageBinary {
			// Tolerate stray text frames (keep-alives from misbehaving
			// proxies) — same policy as readAck.
			continue
		}
		select {
		case io.frameCh <- qwpReaderEvent{payload: data}:
		case <-io.shutdownCh:
			return
		}
	}
}

// dispatcherRun is the dispatch goroutine's top-level loop. Exiting
// just decrements the shutdown WaitGroup — doneCh is closed by the
// start() wrapper only after the reader also exits, so that
// tr.close() can run immediately after shutdown() returns without
// racing the reader's in-flight conn.Read.
func (io *qwpEgressIO) dispatcherRun() {
	// Defers run LIFO: close(events) first, then closed.Store(true).
	// Either order is safe because a consumer that wakes on the
	// closed channel and immediately calls releaseBuffer will
	// observe closed=true momentarily — releaseBuffer's fallback
	// path (non-blocking send + coalesced notify) is harmless even
	// on a drained, dead pool. Keeping close first also keeps the
	// reader/dispatcher invariant that events is closed before the
	// waitgroup-gated doneCh fires in start().
	defer io.closed.Store(true)
	defer close(io.events)
	// Release decoder-owned resources (zstd decompression goroutines
	// in particular) before the dispatcher itself exits. Runs LIFO
	// relative to the defers above, which is the order we want: the
	// last consumer that may wake on the closed events channel has
	// already seen its terminal signal by the time decoder.close()
	// tears down zstd state.
	defer io.decoder.close()

	for {
		var req qwpRequest
		select {
		case <-io.shutdownCh:
			return
		case req = <-io.requests:
		}

		io.currentRequestId = req.requestId
		io.creditEnabled = req.initialCredit > 0
		io.currentQueryDone = false
		// Clear a lingering prior-query cancel without clobbering a
		// user-thread Cancel(req.requestId) that raced the dispatcher
		// picking up this request off the single-slot queue. The user
		// can call QwpQuery.Cancel() as soon as Query() returns —
		// submitQuery is non-blocking, so the user's Cancel can reach
		// the atomic before the dispatcher even starts processing.
		// CAS loop: only clear if the stored id is a prior-query id
		// (not -1, not req.requestId). Any user Store that races the
		// CAS either commits first (we see req.requestId and bail) or
		// overwrites our -1 afterwards (drainPendingCancel picks it
		// up on the next loop iteration either way).
		for {
			cur := io.cancelRequestId.Load()
			if cur == -1 || cur == req.requestId {
				break
			}
			if io.cancelRequestId.CompareAndSwap(cur, -1) {
				break
			}
		}
		io.pendingCredit.Store(0)

		if err := io.sendQueryRequest(req); err != nil {
			io.poisonAndEmitError(fmt.Sprintf("qwp: send QUERY_REQUEST: %v", err))
			continue
		}

		io.receiveLoop()
	}
}

// receiveLoop dispatches frames until currentQueryDone or shutdown.
// Drains the cancel + credit latches at every iteration so user-
// initiated signals reach the server at loop boundaries; notifyCh
// wakes the select when those atomics change while we are waiting
// for a server frame.
func (io *qwpEgressIO) receiveLoop() {
	for !io.currentQueryDone {
		select {
		case <-io.shutdownCh:
			return
		default:
		}

		if !io.drainPendingCancel() {
			return
		}
		if !io.drainPendingCredit() {
			return
		}

		select {
		case <-io.shutdownCh:
			return
		case <-io.notifyCh:
			// State change — loop back to drain. This is how a
			// user-initiated cancel or release reaches the wire
			// without waiting for a server frame.
		case ev, ok := <-io.frameCh:
			if !ok {
				// Reader goroutine exited without emitting an error
				// — unusual, but treat as a clean close of an
				// in-flight query.
				io.poisonAndEmitError("qwp: reader closed without error")
				io.currentQueryDone = true
				return
			}
			if ev.err != nil {
				io.poisonAndEmitError(fmt.Sprintf("qwp: server closed connection: %v", ev.err))
				io.currentQueryDone = true
				return
			}
			io.dispatchFrame(ev.payload)
		}
	}
}

// dispatchFrame routes a received frame to the matching decoder method
// and emits the resulting event. Sets currentQueryDone on terminal
// frames (End / ExecDone / Error) so the receive loop exits.
func (io *qwpEgressIO) dispatchFrame(payload []byte) {
	kind, err := qwpPeekMsgKind(payload)
	if err != nil {
		// Header parse failure — we have no trustworthy framing, so
		// poison the connection before emitting.
		io.poisonAndEmitError(fmt.Sprintf("qwp: %v", err))
		io.currentQueryDone = true
		return
	}
	switch kind {
	case qwpMsgKindResultBatch:
		io.handleResultBatch(payload)
	case qwpMsgKindResultEnd:
		io.handleResultEnd(payload)
	case qwpMsgKindQueryError:
		io.handleQueryError(payload)
	case qwpMsgKindExecDone:
		io.handleExecDone(payload)
	case qwpMsgKindCacheReset:
		io.handleCacheReset(payload)
	default:
		// Unknown msg_kind means we are talking to a server whose
		// protocol we do not understand — treat as terminal so we do
		// not parade a desynced stream to the next query.
		io.poisonAndEmitError(fmt.Sprintf("qwp: unknown msg_kind 0x%02X", byte(kind)))
		io.currentQueryDone = true
	}
}

// handleResultBatch takes a buffer from the pool, decodes in place,
// and emits a batch event. Blocks on the pool when full. The select
// also watches shutdown + notify so a user-initiated cancel still
// reaches the wire while we wait for the handler to free up a buffer.
func (io *qwpEgressIO) handleResultBatch(payload []byte) {
	var buf *qwpBatchBuffer
	for buf == nil {
		select {
		case <-io.shutdownCh:
			io.currentQueryDone = true
			return
		case buf = <-io.buffers:
		case <-io.notifyCh:
			// Handler moved the cancel / credit state forward —
			// flush whatever is pending before continuing the wait.
			if !io.drainPendingCancel() {
				return
			}
			if !io.drainPendingCredit() {
				return
			}
		}
	}

	if err := io.decoder.decode(payload, &buf.batch); err != nil {
		// Decoder failed mid-frame: dict/registry state may be out
		// of sync with the server. Return the buffer, poison the
		// connection so the next submitQuery fails immediately
		// (self-correction via the delta-dict sync check is
		// probabilistic — a mis-advanced reader can leave the dict
		// *accidentally* in sync at the offset level while values
		// are wrong, producing silently corrupt rows), surface the
		// error, and stop the query.
		io.buffers <- buf
		io.poisonAndEmitError(fmt.Sprintf("qwp: decode: %v", err))
		io.currentQueryDone = true
		return
	}
	buf.payloadLen = len(payload)

	select {
	case <-io.shutdownCh:
		// Buffer is orphaned to GC here rather than returned to the
		// pool: shutdown is racing the events send, the dispatcher is
		// about to exit, and nobody will drain io.buffers anyway. The
		// always-balanced bookkeeping the pool comment describes
		// applies to the steady state, not to this terminal path.
		io.currentQueryDone = true
		return
	case io.events <- qwpEvent{
		kind:      qwpEventKindBatch,
		requestId: io.currentRequestId,
		batch:     buf,
	}:
	}
}

// handleResultEnd parses RESULT_END, emits an End event, and marks the
// current query done. Parse failure is emitted as a synthesized error.
func (io *qwpEgressIO) handleResultEnd(payload []byte) {
	reqId, total, err := io.decoder.decodeResultEnd(payload)
	if err != nil {
		io.poisonAndEmitError(fmt.Sprintf("qwp: %v", err))
	} else {
		io.emit(qwpEvent{
			kind:      qwpEventKindEnd,
			requestId: reqId,
			totalRows: total,
		})
	}
	io.currentQueryDone = true
}

// handleQueryError parses QUERY_ERROR, emits an Error event with the
// server's status + message, and marks the query done.
func (io *qwpEgressIO) handleQueryError(payload []byte) {
	qe, err := io.decoder.decodeQueryError(payload)
	if err != nil {
		io.poisonAndEmitError(fmt.Sprintf("qwp: %v", err))
	} else {
		io.emit(qwpEvent{
			kind:       qwpEventKindError,
			requestId:  qe.RequestId,
			errStatus:  qe.Status,
			errMessage: qe.Message,
		})
	}
	io.currentQueryDone = true
}

// handleCacheReset parses CACHE_RESET and applies the requested reset
// to the decoder's connection-scoped caches. No user-visible event is
// emitted and the current query is NOT marked done — the server emits
// CACHE_RESET between queries (after the prior query's terminal
// frame, before the next query's RESULT_BATCH), so handling it is
// invisible from the user's perspective. A truncated or otherwise
// malformed frame is terminal: the decoder's per-connection state
// cannot be trusted, so we poison the connection.
func (io *qwpEgressIO) handleCacheReset(payload []byte) {
	mask, err := io.decoder.decodeCacheReset(payload)
	if err != nil {
		io.poisonAndEmitError(fmt.Sprintf("qwp: %v", err))
		io.currentQueryDone = true
		return
	}
	io.decoder.applyCacheReset(mask)
}

// handleExecDone parses EXEC_DONE, emits an ExecDone event, and marks
// the query done.
func (io *qwpEgressIO) handleExecDone(payload []byte) {
	reqId, result, err := io.decoder.decodeExecDone(payload)
	if err != nil {
		io.poisonAndEmitError(fmt.Sprintf("qwp: %v", err))
	} else {
		io.emit(qwpEvent{
			kind:       qwpEventKindExecDone,
			requestId:  reqId,
			execResult: result,
		})
	}
	io.currentQueryDone = true
}

// drainPendingCancel flushes a pending CANCEL to the wire, if any.
// Returns false on send failure (emits the error and marks query
// done so the caller can exit the recv loop).
func (io *qwpEgressIO) drainPendingCancel() bool {
	id := io.cancelRequestId.Swap(-1)
	if id < 0 {
		return true
	}
	if err := io.sendCancel(id); err != nil {
		io.poisonAndEmitError(fmt.Sprintf("qwp: send CANCEL: %v", err))
		io.currentQueryDone = true
		return false
	}
	return true
}

// drainPendingCredit flushes queued credit bytes to the server, if any
// and flow control is enabled. When creditEnabled is false, the counter
// is simply reset — user code may still call release() on an
// unbounded-credit query; the accumulation is harmless but we don't
// want a stale non-zero count to leak into the next (possibly
// flow-controlled) query.
func (io *qwpEgressIO) drainPendingCredit() bool {
	if !io.creditEnabled {
		io.pendingCredit.Store(0)
		return true
	}
	bytes := io.pendingCredit.Swap(0)
	if bytes <= 0 {
		return true
	}
	if err := io.sendCredit(io.currentRequestId, bytes); err != nil {
		io.poisonAndEmitError(fmt.Sprintf("qwp: send CREDIT: %v", err))
		io.currentQueryDone = true
		return false
	}
	return true
}

// sendQueryRequest builds and sends the QUERY_REQUEST frame.
//
// Wire layout: msg_kind(0x10) + request_id(int64 LE) + sql_len(varint)
// + sql(utf8) + initial_credit(varint) + bind_count(varint) +
// bind_payload(bindPayloadLen bytes, pre-encoded by QwpBinds).
func (io *qwpEgressIO) sendQueryRequest(req qwpRequest) error {
	io.sendBuf.reset()
	io.sendBuf.putByte(byte(qwpMsgKindQueryRequest))
	io.sendBuf.putInt64LE(req.requestId)
	io.sendBuf.putString(req.sql)
	io.sendBuf.putVarint(uint64(req.initialCredit))
	io.sendBuf.putVarint(uint64(req.bindCount))
	if req.bindCount > 0 && len(req.bindPayload) > 0 {
		io.sendBuf.putBytes(req.bindPayload)
	}
	return io.transport.sendMessage(io.ioCtx, io.sendBuf.bytes())
}

// sendCancel builds and sends a CANCEL frame. Wire layout:
// msg_kind(0x14) + request_id(int64 LE).
func (io *qwpEgressIO) sendCancel(requestId int64) error {
	io.sendBuf.reset()
	io.sendBuf.putByte(byte(qwpMsgKindCancel))
	io.sendBuf.putInt64LE(requestId)
	return io.transport.sendMessage(io.ioCtx, io.sendBuf.bytes())
}

// sendCredit builds and sends a CREDIT frame. Wire layout:
// msg_kind(0x15) + request_id(int64 LE) + additional_bytes(varint).
func (io *qwpEgressIO) sendCredit(requestId, additionalBytes int64) error {
	io.sendBuf.reset()
	io.sendBuf.putByte(byte(qwpMsgKindCredit))
	io.sendBuf.putInt64LE(requestId)
	io.sendBuf.putVarint(uint64(additionalBytes))
	return io.transport.sendMessage(io.ioCtx, io.sendBuf.bytes())
}

// emit pushes an event to the consumer, aborting on shutdown to avoid
// stranding the I/O goroutine on an unresponsive consumer. The events
// channel's bufferPoolSize+2 capacity guarantees non-batch events always
// fit in the steady state, so the select hits the fast path.
//
// If shutdown wins the race, the event is silently dropped. This is
// acceptable because shutdown is always user-initiated (Close /
// QwpQuery.Close): any QUERY_ERROR or synthesized error that arrives in
// the same instant is for a query the caller is no longer waiting on,
// and after Close returns takeEvent reports "I/O goroutine terminated"
// rather than the lost event. Connection-state poisoning (via
// poisonAndEmitError → setIoErr) is independent of the emit and is
// preserved across the drop, so a follow-up submitQuery on the same
// client still surfaces the underlying failure.
func (io *qwpEgressIO) emit(ev qwpEvent) {
	select {
	case io.events <- ev:
	case <-io.shutdownCh:
	}
}

// poisonAndEmitError latches msg as the connection's terminal ioErr
// AND emits it as the current query's TransportError event. The single
// entry point for every transport-class fault on the dispatcher path:
// reader-error / server-close, send failures (QUERY_REQUEST / CANCEL /
// CREDIT), decoder or framing failures that desync the per-connection
// state (symbol dict, schema registry, zstd stream), and unknown
// msg_kinds. After any of those, the connection is unusable — the
// decoder may be silently out of sync (a mis-advanced reader can leave
// the dict accidentally aligned at the offset level while values are
// wrong, producing silently corrupt rows), or the conn itself is dead.
// The latched ioErr causes every subsequent submitQuery to return
// immediately with the original cause, matching the documented
// "I/O goroutine has poisoned ioErr" contract on
// qwpEventKindTransportError and Java's notifyTerminalFailure pattern.
// Does NOT flip currentQueryDone — callers that also need to terminate
// the current query set it where it belongs.
//
// The transport-error kind makes the failover orchestrator route this
// to the reconnect-and-replay path; without the kind split a server
// QUERY_ERROR would be indistinguishable from a decoder desync and the
// orchestrator would either retry SQL errors (wrong) or never retry
// transport faults (also wrong).
func (io *qwpEgressIO) poisonAndEmitError(msg string) {
	io.setIoErr(errors.New(msg))
	io.emit(qwpEvent{
		kind:       qwpEventKindTransportError,
		requestId:  io.currentRequestId,
		errStatus:  0,
		errMessage: msg,
	})
}

