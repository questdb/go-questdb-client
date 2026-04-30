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
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// qwpSf send-loop tunables. Defaults match the Java
// CursorWebSocketSendLoop spec.
const (
	qwpSfDefaultParkInterval               = 50 * time.Microsecond
	qwpSfDefaultReconnectMaxDuration       = 5 * time.Minute
	qwpSfDefaultReconnectInitialBackoff    = 100 * time.Millisecond
	qwpSfDefaultReconnectMaxBackoff        = 5 * time.Second
	qwpSfReconnectLogThrottleInterval      = 5 * time.Second // throttle "attempt N failed" logs
)

// qwpSfReconnectFactory is invoked by the send loop on a wire
// failure to obtain a fresh connected+upgraded transport. The
// factory encapsulates the dial URL, auth headers, and TLS config —
// the send loop just receives a ready transport.
//
// Implementations should return immediately on terminal errors
// (auth rejection, version mismatch) and let transient errors
// surface as ordinary errors so the caller can apply backoff. The
// "terminal vs transient" classification is delegated to
// qwpSfIsTerminalUpgradeError, which sniffs the error chain for
// the "WebSocket upgrade failed:" sentinel coder/websocket
// produces on non-101 responses.
type qwpSfReconnectFactory func(ctx context.Context) (*qwpTransport, error)

// qwpSfSendLoop owns one I/O goroutine that:
//  1. Polls the engine's publishedFsn and walks newly-published
//     frames from the engine's segments, sending each as one
//     WebSocket binary frame to the server.
//  2. Polls the WebSocket for server ACK frames; on each ACK with
//     cumulative wire sequence N, calls
//     engine.engineAcknowledge(fsnAtZero+N) so the segment
//     manager can trim fully-acked segments.
//  3. On wire failure, runs the configured reconnect policy:
//     backoff with jitter up to reconnectMaxDuration, with
//     auth-style failures (401/403/non-101 upgrade reject)
//     treated as terminal. On reconnect success, repositions the
//     cursor at ackedFsn+1 and replays.
//
// No locks on the steady-state path. The producer goroutine writes
// into the engine; the I/O goroutine reads. publishedFsn is the
// volatile publish barrier.
//
// Errors are reported via lastError(); the I/O goroutine sets it
// and exits. Producers polling checkError() surface the failure.
type qwpSfSendLoop struct {
	engine *qwpSfCursorEngine

	// transport is the active connection. Replaced on reconnect.
	// Loaded by both the send and receive goroutines; the outer
	// loop is the only writer (single-writer pattern).
	transport atomic.Pointer[qwpTransport]

	parkInterval time.Duration

	// reconnectFactory is non-nil when reconnect is enabled. A nil
	// factory makes wire failures immediately terminal (legacy,
	// matches the Java client's "no reconnect" mode).
	reconnectFactory qwpSfReconnectFactory

	reconnectMaxDuration    time.Duration
	reconnectInitialBackoff time.Duration
	reconnectMaxBackoff     time.Duration

	// policyResolver chooses Halt vs DropAndContinue per Category.
	// Non-nil; defaults are baked in via qwpSfDefaultPolicyFor.
	// Atomic pointer because setters can run concurrently with the
	// receiver goroutine that reads it on every classified rejection.
	policyResolver atomic.Pointer[qwpSfPolicyResolver]

	// dispatcher delivers SenderError payloads asynchronously to the
	// user-supplied SenderErrorHandler. Non-nil; uses the default
	// loud-not-silent handler if the user did not configure one.
	// Atomic pointer for the same reason as policyResolver.
	dispatcher atomic.Pointer[qwpSfErrorDispatcher]

	// fsnAtZero is the FSN that wireSeq=0 maps to on the current
	// connection. After a reconnect it's set to engine.ackedFsn()+1
	// so server-side ACK math stays aligned with the disk state.
	// Producer-side state, single-writer (the send loop), read
	// during ACK handling.
	fsnAtZero atomic.Int64
	// nextWireSeq is the next wire sequence the send goroutine will
	// emit. Reset to 0 on every reconnect. Atomic because the
	// receiver goroutine reads it for its sanity check on incoming
	// ACKs — without atomics, an in-process server (e.g. the dump-
	// mode pipe) can deliver an ACK before the producer's plain-int
	// increment is visible to the consumer, and the consumer's
	// "highestSent < 0" guard then drops a real ACK.
	nextWireSeq atomic.Int64
	// sendingSegment / sendOffset track the cursor inside the
	// engine's segment chain. Producer-only state.
	sendingSegment *qwpSfSegment
	sendOffset     int64
	// replayTargetFsn: snapshot of publishedFsn at swapClient time.
	// Frames at FSN ≤ this value are post-reconnect replays; we
	// count them via totalFramesReplayed and reset replayTargetFsn
	// to -1 once we cross the boundary. Producer-only state.
	replayTargetFsn int64

	// running gates the outer reconnect loop. close() flips it to
	// false; inner goroutines observe it via ctx.Done.
	running atomic.Bool

	// ctx is the loop's master context; cancel() forces both
	// inner goroutines out of any blocking transport calls.
	ctx    context.Context
	cancel context.CancelFunc

	// done is closed when run() returns.
	done chan struct{}
	wg   sync.WaitGroup

	// lastError holds the first terminal error. Atomic pointer so
	// the producer can sample it from any goroutine.
	lastError atomic.Pointer[error]

	// lastTerminalServerError is the typed-payload sibling to
	// lastError. Set when recordFatalServerError is called with a
	// fully-populated *SenderError (server-rejection path, WS
	// terminal close, auth-terminal upgrade, reconnect-budget
	// exhaustion). Independent of lastError so QwpSender accessors
	// can return the typed payload without an errors.As walk.
	lastTerminalServerError atomic.Pointer[SenderError]

	// Counters.
	totalFramesSent        atomic.Int64
	totalAcks              atomic.Int64
	totalServerErrors      atomic.Int64
	totalReconnects        atomic.Int64
	totalReconnectAttempts atomic.Int64
	totalFramesReplayed    atomic.Int64

	// Per-connection counters used to detect "server up but doesn't
	// speak our protocol". A WS upgrade that succeeds followed by a
	// drop after we sent ≥1 frame and saw zero ACKs is unrecoverable
	// (likely a server-side version/config mismatch — reconnecting
	// just hammers the server). Reset on every connection swap.
	framesSentOnConn atomic.Int64
	acksRecvOnConn   atomic.Int64
}

// qwpSfNewSendLoop constructs a send loop bound to the given engine
// and (initial) transport. The transport must already be connected
// and WebSocket-upgraded; the send loop takes ownership and will
// close it on shutdown.
//
// Reconnect is opt-in: a nil factory keeps the legacy "single
// failure is terminal" behavior; a non-nil factory enables retry
// with backoff, capped by the *Reconnect* knobs.
func qwpSfNewSendLoop(
	engine *qwpSfCursorEngine,
	transport *qwpTransport,
	factory qwpSfReconnectFactory,
	parkInterval, reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff time.Duration,
) *qwpSfSendLoop {
	if engine == nil || transport == nil {
		panic("qwp/sf: engine and transport must be non-nil")
	}
	if parkInterval <= 0 {
		parkInterval = qwpSfDefaultParkInterval
	}
	if reconnectMaxDuration <= 0 {
		reconnectMaxDuration = qwpSfDefaultReconnectMaxDuration
	}
	if reconnectInitialBackoff <= 0 {
		reconnectInitialBackoff = qwpSfDefaultReconnectInitialBackoff
	}
	if reconnectMaxBackoff <= 0 {
		reconnectMaxBackoff = qwpSfDefaultReconnectMaxBackoff
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &qwpSfSendLoop{
		engine:                  engine,
		parkInterval:            parkInterval,
		reconnectFactory:        factory,
		reconnectMaxDuration:    reconnectMaxDuration,
		reconnectInitialBackoff: reconnectInitialBackoff,
		reconnectMaxBackoff:     reconnectMaxBackoff,
		ctx:                     ctx,
		cancel:                  cancel,
		done:                    make(chan struct{}),
		replayTargetFsn:         -1,
	}
	l.policyResolver.Store(&qwpSfPolicyResolver{})
	l.dispatcher.Store(newQwpSfErrorDispatcher(nil, qwpSfDefaultErrorInboxCapacity))
	l.transport.Store(transport)
	return l
}

// sendLoopSetPolicyResolver replaces the policy resolver used to map
// Categories to Policies. Safe to call any time — the resolver is
// stored atomically and the receiver goroutine picks up the new value
// on its next classified rejection. Pass nil to fall back to spec
// defaults.
func (l *qwpSfSendLoop) sendLoopSetPolicyResolver(r *qwpSfPolicyResolver) {
	if r == nil {
		r = &qwpSfPolicyResolver{}
	}
	l.policyResolver.Store(r)
}

// sendLoopSetErrorHandler replaces the user-supplied SenderErrorHandler
// and the dispatcher's inbox capacity. Safe to call any time — the
// dispatcher is swapped atomically and the previous one is closed
// (its in-flight goroutine drains briefly, then exits). Passing
// handler=nil reverts to the default loud-not-silent handler;
// capacity ≤ 0 keeps the default capacity.
//
// Note: any notifications still queued on the previous dispatcher at
// swap time are subject to its drain timeout — extremely fast swap +
// flood scenarios may lose a notification, matching offer's
// best-effort contract.
func (l *qwpSfSendLoop) sendLoopSetErrorHandler(handler SenderErrorHandler, capacity int) {
	if capacity <= 0 {
		capacity = qwpSfDefaultErrorInboxCapacity
	}
	old := l.dispatcher.Swap(newQwpSfErrorDispatcher(handler, capacity))
	if old != nil {
		old.close()
	}
}

// sendLoopDispatcher exposes the dispatcher for counter accessors on
// the QwpSender public surface. Safe to call concurrently with
// sendLoopSetErrorHandler — returns whatever dispatcher is current
// at the moment of call.
func (l *qwpSfSendLoop) sendLoopDispatcher() *qwpSfErrorDispatcher {
	return l.dispatcher.Load()
}

// sendLoopStart launches the I/O goroutine. Idempotent — a second
// call panics.
func (l *qwpSfSendLoop) sendLoopStart() {
	if !l.running.CompareAndSwap(false, true) {
		panic("qwp/sf: send loop already started")
	}
	// Position cursor at the first unsent FSN before the goroutine
	// observes any state.
	l.positionCursorForStart()
	l.wg.Add(1)
	go l.run()
}

// sendLoopClose stops the I/O goroutine and waits for it to exit.
// Idempotent. Safe to call from any goroutine.
func (l *qwpSfSendLoop) sendLoopClose() error {
	l.running.Store(false)
	l.cancel()
	l.wg.Wait()
	if t := l.transport.Swap(nil); t != nil {
		_ = t.close(context.Background())
	}
	if d := l.dispatcher.Load(); d != nil {
		d.close()
	}
	return l.checkErrorOrNil()
}

// sendLoopCheckError returns the first terminal error the I/O
// goroutine recorded, or nil. Producers should sample this on
// every public API call so wire failures don't stay silent.
func (l *qwpSfSendLoop) sendLoopCheckError() error {
	return l.checkErrorOrNil()
}

func (l *qwpSfSendLoop) checkErrorOrNil() error {
	if p := l.lastError.Load(); p != nil {
		return *p
	}
	return nil
}

func (l *qwpSfSendLoop) recordFatal(err error) {
	if err == nil {
		return
	}
	l.lastError.CompareAndSwap(nil, &err)
	l.running.Store(false)
}

// recordFatalServerError latches a typed *SenderError as the terminal
// error. It populates both lastError (so producer-side errors.As
// continues to work) and lastTerminalServerError (so the QwpSender
// accessor can return the typed payload directly without an unwrap
// walk). Idempotent — only the first failure wins, matching
// recordFatal's semantics.
func (l *qwpSfSendLoop) recordFatalServerError(se *SenderError) {
	if se == nil {
		return
	}
	var err error = se
	l.lastError.CompareAndSwap(nil, &err)
	l.lastTerminalServerError.CompareAndSwap(nil, se)
	l.running.Store(false)
}

// sendLoopLastTerminalServerError returns the typed *SenderError the
// I/O goroutine latched as terminal, or nil if either no terminal
// error has occurred or the terminal error has no typed payload
// (legacy recordFatal path used for transport-only failures).
func (l *qwpSfSendLoop) sendLoopLastTerminalServerError() *SenderError {
	return l.lastTerminalServerError.Load()
}

// sendLoopTotalServerErrors returns the cumulative count of
// SenderError payloads built by the loop (DROP and HALT combined).
func (l *qwpSfSendLoop) sendLoopTotalServerErrors() int64 {
	return l.totalServerErrors.Load()
}

// sendLoopFsnAtZero returns the FSN that wireSeq=0 maps to on the
// current connection. Useful for tests asserting reconnect
// repositioning.
func (l *qwpSfSendLoop) sendLoopFsnAtZero() int64 {
	return l.fsnAtZero.Load()
}

// sendLoopTotalReconnects returns the count of successful
// reconnects since startup.
func (l *qwpSfSendLoop) sendLoopTotalReconnects() int64 {
	return l.totalReconnects.Load()
}

// sendLoopTotalReconnectAttempts returns reconnect attempts
// (succeeded + failed).
func (l *qwpSfSendLoop) sendLoopTotalReconnectAttempts() int64 {
	return l.totalReconnectAttempts.Load()
}

// sendLoopTotalFramesSent returns the cumulative frame count
// transmitted on the wire. Includes replays.
func (l *qwpSfSendLoop) sendLoopTotalFramesSent() int64 {
	return l.totalFramesSent.Load()
}

// sendLoopTotalAcks returns the cumulative ACK count received.
func (l *qwpSfSendLoop) sendLoopTotalAcks() int64 {
	return l.totalAcks.Load()
}

// positionCursorForStart sets fsnAtZero, nextWireSeq, and the
// cursor (sendingSegment + sendOffset) to the first unsent FSN.
// Must be called by the I/O goroutine before it starts sending —
// the producer thread captures the engine's state at that moment.
func (l *qwpSfSendLoop) positionCursorForStart() {
	replayStart := l.engine.engineAckedFsn() + 1
	l.fsnAtZero.Store(replayStart)
	l.nextWireSeq.Store(0)
	l.framesSentOnConn.Store(0)
	l.acksRecvOnConn.Store(0)
	l.positionCursorAt(replayStart)
}

// positionCursorAt walks the engine's segments to find the one
// containing targetFsn and sets sendOffset to the byte offset of
// that frame within it. If targetFsn is past everything published,
// parks at the live active segment's published offset.
func (l *qwpSfSendLoop) positionCursorAt(targetFsn int64) {
	seg := l.engine.engineFindSegmentContaining(targetFsn)
	if seg == nil {
		l.sendingSegment = l.engine.engineActiveSegment()
		if l.sendingSegment != nil {
			l.sendOffset = l.sendingSegment.publishedOffset()
		} else {
			l.sendOffset = qwpSfHeaderSize
		}
		return
	}
	l.sendingSegment = seg
	// Walk frame-by-frame from HEADER_SIZE until we land on targetFsn.
	offset := qwpSfHeaderSize
	fsn := seg.segmentBaseSeq()
	base := seg.address()
	for fsn < targetFsn {
		payloadLen := int64(int32(binary.LittleEndian.Uint32(base[offset+4 : offset+8])))
		offset += qwpSfFrameHeaderSize + payloadLen
		fsn++
	}
	l.sendOffset = offset
}

// run is the outer reconnect loop. Each iteration runs one
// connection's worth of I/O via runOneConnection; on wire failure
// it backs off and reconnects (if a factory is wired) or records
// the failure as terminal and exits.
func (l *qwpSfSendLoop) run() {
	defer l.wg.Done()
	defer close(l.done)

	for l.running.Load() {
		err := l.runOneConnection()
		if !l.running.Load() {
			return
		}
		// Decide: terminal or recoverable?
		if err == nil {
			return
		}
		// Already-terminal SenderErrors come back here from
		// receiverLoop's classify branch — route them through
		// recordFatalServerError (idempotent) so the typed payload is
		// preserved end-to-end.
		var alreadyTyped *SenderError
		if errors.As(err, &alreadyTyped) {
			l.recordFatalServerError(alreadyTyped)
			return
		}
		// WebSocket close-frame violations (PROTOCOL_ERROR 1002,
		// UNSUPPORTED_DATA 1003, MESSAGE_TOO_BIG 1009, etc.) come up
		// from either inner goroutine via runOneConnection's first-
		// error aggregation. They map to ProtocolViolation+Halt; do
		// not retry — replaying the same bytes will produce the same
		// close frame.
		if code := websocket.CloseStatus(err); qwpSfIsTerminalCloseCode(code) {
			se := l.qwpSfBuildProtocolViolationSE(code, err.Error())
			l.totalServerErrors.Add(1)
			l.dispatcher.Load().offer(se)
			l.recordFatalServerError(se)
			return
		}
		if l.reconnectFactory == nil {
			l.recordFatal(err)
			return
		}
		if qwpSfIsTerminalUpgradeError(err) {
			se := l.qwpSfBuildUpgradeFailureSE(err)
			l.totalServerErrors.Add(1)
			l.dispatcher.Load().offer(se)
			l.recordFatalServerError(se)
			return
		}
		// Detect "server up, accepts the WS upgrade, but doesn't speak
		// our QWP protocol" — the dial succeeds every time, so plain
		// reconnect-with-backoff would hammer the server in a hot
		// loop until reconnectMaxDuration expires (5 min default),
		// burning thousands of ephemeral ports per second. The
		// signature: this connection sent ≥1 frame and saw zero ACKs
		// before dropping. A healthy server either ACKs OK or sends a
		// non-OK status ACK (which is already classified terminal in
		// receiverLoop) — silent disconnect after a frame is a
		// version/config mismatch, and reconnecting can't fix it.
		if l.framesSentOnConn.Load() > 0 && l.acksRecvOnConn.Load() == 0 {
			// The connection finished the WS upgrade and the X-QWP-
			// Version negotiation, then closed without ACKing any of
			// the frames we sent. Reconnect can't fix this — the
			// server isn't speaking the same wire-format dialect we
			// are (most often: server build is older than this
			// client's branch, even if both sides declared the same
			// X-QWP-Version). Fail terminally to avoid hammering the
			// server with thousands of dial attempts per second.
			reason := fmt.Sprintf(
				"server accepted the WebSocket upgrade but disconnected "+
					"without ACKing any of the %d frame(s) we sent — server is "+
					"likely running an incompatible build (won't retry): %s",
				l.framesSentOnConn.Load(), err.Error())
			se := l.qwpSfBuildBudgetExhaustedSE(reason)
			l.totalServerErrors.Add(1)
			l.dispatcher.Load().offer(se)
			l.recordFatalServerError(se)
			return
		}
		// Reconnect with backoff.
		ok := l.reconnectWithBackoff(err)
		if !ok {
			return
		}
	}
}

// runOneConnection runs the send + receive goroutines for the
// currently-installed transport until one of them returns. Returns
// the first error seen, or nil for a clean exit (running=false).
//
// On a successful reconnect, the outer loop calls
// repositionForReconnect to reset wire state and replay window
// before this method runs again.
func (l *qwpSfSendLoop) runOneConnection() error {
	connCtx, connCancel := context.WithCancel(l.ctx)
	defer connCancel()

	type loopErr struct{ err error }
	errCh := make(chan loopErr, 2)

	var inner sync.WaitGroup
	inner.Add(2)
	go func() {
		defer inner.Done()
		err := l.senderLoop(connCtx)
		errCh <- loopErr{err}
		connCancel()
	}()
	go func() {
		defer inner.Done()
		err := l.receiverLoop(connCtx)
		errCh <- loopErr{err}
		connCancel()
	}()
	inner.Wait()
	close(errCh)
	var first error
	for e := range errCh {
		if e.err != nil && first == nil {
			first = e.err
		}
	}
	return first
}

// senderLoop walks the engine's frames and sends each as one
// WebSocket binary message. Returns ctx.Err() on shutdown or the
// transport's send error on wire failure.
func (l *qwpSfSendLoop) senderLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil // clean shutdown
		}
		if !l.running.Load() {
			return nil
		}
		didWork, err := l.trySendOne(ctx)
		if err != nil {
			return err
		}
		if !didWork {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(l.parkInterval):
			}
		}
	}
}

// trySendOne sends at most one frame. Returns (true, nil) if it
// sent a frame, (false, nil) if there's nothing ready, or (false,
// err) on wire failure.
//
// Bounded: at most one frame per call so the receiver goroutine
// gets scheduling fairness.
func (l *qwpSfSendLoop) trySendOne(ctx context.Context) (bool, error) {
	if l.sendingSegment == nil {
		l.sendingSegment = l.engine.engineActiveSegment()
		if l.sendingSegment == nil {
			return false, nil
		}
		l.sendOffset = qwpSfHeaderSize
	}
	pub := l.sendingSegment.publishedOffset()
	if l.sendOffset >= pub {
		// Nothing more in the current segment. If it's a sealed
		// segment (no longer the live active), advance to the next.
		if l.sendingSegment != l.engine.engineActiveSegment() {
			next := l.advanceSegment()
			if next != l.sendingSegment {
				l.sendingSegment = next
				l.sendOffset = qwpSfHeaderSize
				return true, nil
			}
		}
		return false, nil
	}
	if l.sendOffset+qwpSfFrameHeaderSize > pub {
		return false, nil
	}
	base := l.sendingSegment.address()
	payloadLen := int64(int32(binary.LittleEndian.Uint32(base[l.sendOffset+4 : l.sendOffset+8])))
	if payloadLen < 0 {
		return false, fmt.Errorf("qwp/sf: negative payloadLen at offset %d in segment baseSeq=%d",
			l.sendOffset, l.sendingSegment.segmentBaseSeq())
	}
	frameEnd := l.sendOffset + qwpSfFrameHeaderSize + payloadLen
	if frameEnd > pub {
		return false, nil // payload not fully published yet
	}
	transport := l.transport.Load()
	if transport == nil {
		return false, errors.New("qwp/sf: transport gone mid-loop")
	}
	payload := base[l.sendOffset+qwpSfFrameHeaderSize : frameEnd]
	// Bump nextWireSeq BEFORE the wire write. The receiver
	// goroutine uses nextWireSeq to validate incoming ACK
	// sequence numbers; if we incremented after sendMessage, a
	// fast in-process server could deliver an ACK before the
	// store became visible and the receiver's sanity check would
	// reject a legitimate ACK. The trade-off — a wire failure
	// leaves nextWireSeq advanced for a frame that never made it
	// — is harmless because every reconnect path resets it via
	// swapClient/positionCursorForStart.
	wireSeq := l.nextWireSeq.Load()
	fsnSent := l.fsnAtZero.Load() + wireSeq
	l.nextWireSeq.Store(wireSeq + 1)
	if err := transport.sendMessage(ctx, payload); err != nil {
		// Treat ctx-cancelled as a clean shutdown rather than a
		// wire failure — runOneConnection will return nil and the
		// outer loop sees running=false and exits.
		if ctx.Err() != nil {
			return false, nil
		}
		return false, err
	}
	l.sendOffset = frameEnd
	l.totalFramesSent.Add(1)
	l.framesSentOnConn.Add(1)
	if l.replayTargetFsn >= 0 {
		l.totalFramesReplayed.Add(1)
		if fsnSent >= l.replayTargetFsn {
			l.replayTargetFsn = -1
		}
	}
	return true, nil
}

// advanceSegment walks to the next segment when the current one is
// sealed and fully drained. Mirrors Java's CursorWebSocketSendLoop
// state machine: prefer the next sealed-by-baseSeq segment; fall
// back to the active if no later sealed exists; fall back to the
// oldest remaining sealed if our current was trimmed out from
// under us.
func (l *qwpSfSendLoop) advanceSegment() *qwpSfSegment {
	current := l.sendingSegment
	liveActive := l.engine.engineActiveSegment()
	if current == liveActive {
		return current
	}
	next := l.engine.engineNextSealedAfter(current)
	if next != nil {
		return next
	}
	first := l.engine.engineFirstSealed()
	if first != nil && first.segmentBaseSeq() > current.segmentBaseSeq() {
		return first
	}
	return liveActive
}

// receiverLoop reads ACKs from the WebSocket and routes them to
// the engine. Returns ctx.Err() on shutdown or the transport's
// read error on wire failure.
func (l *qwpSfSendLoop) receiverLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if !l.running.Load() {
			return nil
		}
		transport := l.transport.Load()
		if transport == nil {
			return errors.New("qwp/sf: transport gone mid-loop")
		}
		status, data, err := transport.readAck(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if status == QwpStatusDurableAck {
			// Per-table fsync confirmation. Cursor SF doesn't
			// currently surface durable-ack progress to the
			// producer, but receiving one is not an error — match
			// the Java client and silently ignore.
			continue
		}
		seq := parseAckSequence(data)
		if status != QwpStatusOK {
			// Application-layer rejection by the server. Classify the
			// status byte, resolve the policy, surface a typed
			// SenderError. Halt latches and exits the receiver loop;
			// DropAndContinue advances ackedFsn past the rejected
			// span and keeps draining (the bytes on disk are the
			// bytes the server rejected — reconnect/replay cannot
			// fix them; only dropping moves us past them).
			//
			// Sanity clamp: do not trust a rejection wireSeq beyond
			// what we have actually sent. Without this clamp the DROP
			// path can advance ackedFsn past publishedFsn, which makes
			// the segment manager trim sealed segments the I/O thread
			// is still reading. Mirrors handleServerRejection in the
			// Java client. The clamp only feeds the FSN math; the
			// reported MessageSequence is the raw server-sent seq so
			// it round-trips verbatim against server-side logs.
			highestSent := l.nextWireSeq.Load() - 1
			cappedSeq := seq
			if highestSent < 0 {
				cappedSeq = 0
			} else if cappedSeq > highestSent {
				cappedSeq = highestSent
			}
			_, _, msg := parseAckErrorPayload(data)
			fsn := l.fsnAtZero.Load() + cappedSeq
			cat := qwpSfClassify(status)
			pol := l.policyResolver.Load().resolve(cat)
			se := &SenderError{
				Category:         cat,
				AppliedPolicy:    pol,
				ServerStatusByte: int(status),
				ServerMessage:    msg,
				MessageSequence:  seq,
				FromFsn:          fsn,
				ToFsn:            fsn,
				DetectedAt:       time.Now(),
			}
			l.totalServerErrors.Add(1)
			l.dispatcher.Load().offer(se)
			if pol == PolicyHalt {
				l.recordFatalServerError(se)
				return se
			}
			// PolicyDropAndContinue: advance past the rejected span
			// via the same engine entry the success branch uses. The
			// segment manager will trim the now-acked range on its
			// next maintenance pass. Bump totalAcks for parity with
			// the success path so producer-visible counters reflect
			// "the server has resolved this batch".
			l.engine.engineAcknowledge(fsn)
			l.totalAcks.Add(1)
			l.acksRecvOnConn.Add(1)
			continue
		}
		// Sanity: don't trust an ACK beyond what we've actually
		// sent. A malformed/replayed server response could
		// otherwise force trim of segments the new server hasn't
		// seen.
		highestSent := l.nextWireSeq.Load() - 1
		if highestSent < 0 {
			continue
		}
		capped := seq
		if capped > highestSent {
			capped = highestSent
		}
		l.engine.engineAcknowledge(l.fsnAtZero.Load() + capped)
		l.totalAcks.Add(1)
		l.acksRecvOnConn.Add(1)
	}
}

// reconnectWithBackoff loops on factory.reconnect until success,
// terminal error, budget exhaustion, or running=false. On success,
// installs the new transport and resets wire state. Returns true
// to continue the outer loop, false to exit.
func (l *qwpSfSendLoop) reconnectWithBackoff(initial error) bool {
	outageStart := time.Now()
	deadline := outageStart.Add(l.reconnectMaxDuration)
	backoff := l.reconnectInitialBackoff
	attempts := 0
	lastErr := initial
	for l.running.Load() && time.Now().Before(deadline) {
		attempts++
		l.totalReconnectAttempts.Add(1)
		newTransport, err := l.reconnectFactory(l.ctx)
		if err == nil && newTransport != nil {
			l.swapClient(newTransport)
			l.totalReconnects.Add(1)
			return true
		}
		if err != nil {
			if qwpSfIsTerminalUpgradeError(err) {
				se := l.qwpSfBuildUpgradeFailureSE(err)
				l.totalServerErrors.Add(1)
				l.dispatcher.Load().offer(se)
				l.recordFatalServerError(se)
				return false
			}
			lastErr = err
		}
		// Backoff with jitter: sleep [backoff, 2*backoff). Cap at
		// remaining budget so we don't oversleep past the deadline.
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		sleep := backoff + jitter
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if sleep > remaining {
			sleep = remaining
		}
		select {
		case <-l.ctx.Done():
			return false
		case <-time.After(sleep):
		}
		backoff *= 2
		if backoff > l.reconnectMaxBackoff {
			backoff = l.reconnectMaxBackoff
		}
	}
	if !l.running.Load() {
		return false
	}
	elapsed := time.Since(outageStart)
	reason := fmt.Sprintf("reconnect failed after %s / %d attempts: %v",
		elapsed, attempts, lastErr)
	se := l.qwpSfBuildBudgetExhaustedSE(reason)
	l.totalServerErrors.Add(1)
	l.dispatcher.Load().offer(se)
	l.recordFatalServerError(se)
	return false
}

// swapClient replaces the active transport, realigns fsnAtZero to
// the next unacked FSN, restarts wire sequencing from 0, and
// repositions the cursor so the next trySendOne call replays the
// first unacked frame.
func (l *qwpSfSendLoop) swapClient(newTransport *qwpTransport) {
	old := l.transport.Swap(newTransport)
	if old != nil {
		_ = old.close(context.Background())
	}
	replayStart := l.engine.engineAckedFsn() + 1
	l.fsnAtZero.Store(replayStart)
	l.nextWireSeq.Store(0)
	l.framesSentOnConn.Store(0)
	l.acksRecvOnConn.Store(0)
	pubAtSwap := l.engine.enginePublishedFsn()
	if pubAtSwap >= replayStart {
		l.replayTargetFsn = pubAtSwap
	} else {
		l.replayTargetFsn = -1
	}
	l.positionCursorAt(replayStart)
}

// qwpSfIsTerminalUpgradeError reports whether err indicates any
// server-side WebSocket-upgrade reject that won't fix itself on
// retry — auth or protocol-mismatch alike. Kept for backwards
// compatibility; callers that need the auth-vs-protocol split
// should use qwpSfIsAuthFailure / qwpSfIsProtocolUpgradeFailure
// instead.
func qwpSfIsTerminalUpgradeError(err error) bool {
	return qwpSfIsAuthFailure(err) || qwpSfIsProtocolUpgradeFailure(err)
}

// qwpSfIsAuthFailure reports whether err indicates the server
// rejected the WebSocket upgrade with an auth-related HTTP status
// (401 unauthorized, 403 forbidden). These map to
// CategorySecurityError on the SenderError surface.
//
// coder/websocket reports upgrade failures with messages like
// "failed to WebSocket dial: expected handshake response status
// code 101 but got 401" — we match on the status-code substring
// plus the textual "unauthorized" / "forbidden" hints servers
// commonly emit alongside.
func qwpSfIsAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"got 401", "got 403",
		"unauthorized", "forbidden",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// qwpSfIsProtocolUpgradeFailure reports whether err indicates the
// server rejected the WebSocket upgrade with a protocol-related
// HTTP status (404 not found — wrong endpoint; 426 upgrade required
// — wrong protocol version). These map to
// CategoryProtocolViolation on the SenderError surface.
func qwpSfIsProtocolUpgradeFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"got 404", "got 426",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// qwpSfBuildUpgradeFailureSE constructs a typed *SenderError for an
// upgrade-failure terminal: SecurityError for auth (401/403),
// ProtocolViolation for protocol (404/426). Callers must have
// already determined the err is one of those two via the helpers
// above.
func (l *qwpSfSendLoop) qwpSfBuildUpgradeFailureSE(err error) *SenderError {
	cat := CategoryProtocolViolation
	if qwpSfIsAuthFailure(err) {
		cat = CategorySecurityError
	}
	from := l.engine.engineAckedFsn() + 1
	to := l.engine.enginePublishedFsn()
	if to < from {
		to = from
	}
	return &SenderError{
		Category:         cat,
		AppliedPolicy:    PolicyHalt,
		ServerStatusByte: NoStatusByte,
		ServerMessage:    "ws-upgrade-failed: " + err.Error(),
		MessageSequence:  NoMessageSequence,
		FromFsn:          from,
		ToFsn:            to,
		DetectedAt:       time.Now(),
	}
}

// qwpSfBuildProtocolViolationSE constructs a typed *SenderError for
// a terminal WebSocket close frame (PROTOCOL_ERROR /
// UNSUPPORTED_DATA / etc.). The FSN span is the unacked window at
// close time.
func (l *qwpSfSendLoop) qwpSfBuildProtocolViolationSE(code websocket.StatusCode, reason string) *SenderError {
	from := l.engine.engineAckedFsn() + 1
	to := l.engine.enginePublishedFsn()
	if to < from {
		to = from
	}
	return &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyHalt,
		ServerStatusByte: NoStatusByte,
		ServerMessage:    fmt.Sprintf("ws-close[%d]: %s", code, reason),
		MessageSequence:  NoMessageSequence,
		FromFsn:          from,
		ToFsn:            to,
		DetectedAt:       time.Now(),
	}
}

// qwpSfBuildBudgetExhaustedSE constructs a typed *SenderError for
// reconnect-budget exhaustion. Treated as a ProtocolViolation since
// the wire is gone — the FSN span is the unacked window.
func (l *qwpSfSendLoop) qwpSfBuildBudgetExhaustedSE(reason string) *SenderError {
	from := l.engine.engineAckedFsn() + 1
	to := l.engine.enginePublishedFsn()
	if to < from {
		to = from
	}
	return &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyHalt,
		ServerStatusByte: NoStatusByte,
		ServerMessage:    reason,
		MessageSequence:  NoMessageSequence,
		FromFsn:          from,
		ToFsn:            to,
		DetectedAt:       time.Now(),
	}
}

// qwpSfConnectWithRetry runs the same exponential-backoff-with-jitter
// loop as the reconnect path, but is reusable from the sender's
// "ensureConnected" entry point to implement initialConnectRetry.
// Returns the connected transport on success; an error on terminal
// upgrade failure (won't retry) or budget exhaustion.
//
// factory is invoked once per attempt and should produce a fresh,
// connected, upgraded transport (or return an error). The lambda
// is intentionally shaped like qwpSfReconnectFactory so the same
// implementation in the sender can serve both startup and reconnect
// paths verbatim.
func qwpSfConnectWithRetry(
	ctx context.Context,
	factory qwpSfReconnectFactory,
	maxDuration, initialBackoff, maxBackoff time.Duration,
) (*qwpTransport, error) {
	if maxDuration <= 0 {
		maxDuration = qwpSfDefaultReconnectMaxDuration
	}
	if initialBackoff <= 0 {
		initialBackoff = qwpSfDefaultReconnectInitialBackoff
	}
	if maxBackoff <= 0 {
		maxBackoff = qwpSfDefaultReconnectMaxBackoff
	}
	start := time.Now()
	deadline := start.Add(maxDuration)
	backoff := initialBackoff
	attempts := 0
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attempts++
		t, err := factory(ctx)
		if err == nil && t != nil {
			return t, nil
		}
		if err != nil {
			if qwpSfIsTerminalUpgradeError(err) {
				return nil, fmt.Errorf("qwp/sf: WebSocket upgrade failed (won't retry): %w", err)
			}
			lastErr = err
		}
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		sleep := backoff + jitter
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if sleep > remaining {
			sleep = remaining
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	elapsed := time.Since(start)
	if lastErr == nil {
		lastErr = errors.New("no attempts made")
	}
	return nil, fmt.Errorf("qwp/sf: connect failed after %s / %d attempts: %w",
		elapsed, attempts, lastErr)
}
