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
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// qwpSf send-loop tunables. The reconnect and backoff defaults match
// the Java CursorWebSocketSendLoop spec.
const (
	// qwpSfDefaultParkInterval caps how long senderLoop sleeps when the
	// engine has no new frame and the producer doorbell (wakeSender)
	// has not fired. The doorbell drives every steady-state send, so
	// this timer only bounds the recovery time of a missed wakeup and
	// never gates send latency. The Java spec parks 50µs via
	// LockSupport.parkNanos, a cheap futex-style park; re-arming a Go
	// time.Timer that often costs a sizable fraction of a core per idle
	// sender, so the Go port parks 1ms. Parity of the constant is not
	// parity of cost.
	qwpSfDefaultParkInterval            = 1 * time.Millisecond
	qwpSfDefaultReconnectMaxDuration    = 5 * time.Minute
	qwpSfDefaultReconnectInitialBackoff = 100 * time.Millisecond
	qwpSfDefaultReconnectMaxBackoff     = 5 * time.Second
	qwpSfReconnectLogThrottleInterval   = 5 * time.Second // throttle "attempt N failed" logs
)

// qwpSfMaxSilentConnStrikes is the number of consecutive ACK-less
// connections the never-ACKed terminal heuristic in run() tolerates
// before declaring the server incompatible and stopping retries. A
// single connection that sends frames and is met with silence is
// indistinguishable from a routine server restart or LB RST landing
// in the window between a fresh sender's first frame and its first
// ACK, so that strike triggers an ordinary reconnect+replay. Reaching
// this many strikes means at least one full reconnect+replay cycle
// has also met nothing but silence — strong evidence the server isn't
// speaking our wire-format dialect. Go-only: there is no Java
// counterpart.
const qwpSfMaxSilentConnStrikes = 2

// qwpSfReconnectFactory is invoked by the send loop on a wire
// failure to obtain a fresh connected+upgraded transport. idx is
// the host index PickNext returned (see failover.md §2); the
// factory owns the mapping idx → URL, auth headers, and TLS config.
// Single-host factories may ignore idx — they always dial the same
// address.
//
// Implementations should return immediately on terminal errors
// (auth rejection, version mismatch) and let transient errors
// surface as ordinary errors so the caller can apply backoff. The
// "terminal vs transient" classification is delegated to
// qwpSfIsTerminalUpgradeError, which sniffs the error chain for
// the "WebSocket upgrade failed:" sentinel coder/websocket
// produces on non-101 responses.
type qwpSfReconnectFactory func(ctx context.Context, idx int) (*qwpTransport, error)

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

	// parkInterval bounds how long senderLoop sleeps when the engine
	// has no new frame. The common case is event-driven via the wakeup
	// doorbell; this is the defense-in-depth fallback poll. See
	// qwpSfDefaultParkInterval for why it need not be tight.
	parkInterval time.Duration

	// wakeup is a single-slot doorbell rung by the producer (through
	// the ring's sendLoopWakeup callback) after each publish so an
	// idle senderLoop reacts immediately instead of spinning at
	// parkInterval. Mirrors qwpSfSegmentManager.wakeup. Buffered so a
	// publish never blocks on a busy/parked loop; extra rings
	// coalesce into the one slot (senderLoop drains all ready frames
	// per wake, so one token suffices for any backlog).
	wakeup chan struct{}

	// reconnectFactory is non-nil when reconnect is enabled. A nil
	// factory makes wire failures immediately terminal (legacy,
	// matches the Java client's "no reconnect" mode).
	reconnectFactory qwpSfReconnectFactory

	reconnectMaxDuration    time.Duration
	reconnectInitialBackoff time.Duration
	reconnectMaxBackoff     time.Duration

	// tracker drives the failover.md §13.6 round-walk. Constructed at
	// sendLoopSetHostTracker time with the host count, client zone, and
	// target filter — both inert on this ingress path, which does not
	// route by server role or zone (see qwp_sender_cursor.go). When
	// tracker is nil (legacy single-host tests), connectWithBackoff
	// falls back to a synthetic 1-host tracker on first need so the
	// round-walk machinery is the only code path.
	tracker *qwpHostTracker

	// previousIdx is this loop's private slot for the §2.3
	// per-caller mid-stream-demote pattern. After a successful
	// connect it holds the bound endpoint index; on pump exit the
	// outer run() loop leaves it as-is so the next connectWithBackoff
	// can invoke RecordMidStreamFailure(previousIdx) before PickNext.
	// connectWithBackoff resets it to the new bound idx on success
	// and to -1 after consuming the mid-stream slot. Single-writer
	// (the I/O goroutine).
	previousIdx int

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

	// connDispatcher delivers SenderConnectionEvent payloads to the
	// user-supplied SenderConnectionListener, off the I/O goroutine.
	// Non-nil; uses the loud default when no listener is configured.
	connDispatcher atomic.Pointer[qwpDispatcher[*SenderConnectionEvent]]

	// connectedOnce flips true on the first successful connect so the
	// connect-event emitter can tell CONNECTED from RECONNECTED/FAILED_OVER.
	connectedOnce atomic.Bool

	// endpoints is the addr= list, indexed by the bound idx the round-walk
	// returns, used to fill SenderConnectionEvent host:port. Set once before
	// the I/O goroutine starts; read-only thereafter.
	endpoints []qwpEndpoint

	// fsnAtZero is the FSN that wireSeq=0 maps to on the current
	// connection. After a reconnect it's set to engine.ackedFsn()+1
	// so server-side ACK math stays aligned with the disk state.
	// Producer-side state, single-writer (the send loop), read
	// during ACK handling.
	fsnAtZero atomic.Int64
	// nextWireSeq is the next wire sequence the send goroutine will
	// emit; each frame's wireSeq/fsnSent derive from it. Reset to 0 on
	// every reconnect. The send path and the reset paths
	// (positionCursorForStart at startup, swapClient on reconnect) are
	// serialized — never concurrent — but run on different goroutines,
	// so it is atomic for safe publication across those handoffs.
	nextWireSeq atomic.Int64
	// highestFullySent is the highest wire sequence whose sendMessage
	// has fully returned, or -1 when no frame has finished sending on
	// the current connection. Reset to -1 on every reconnect. The
	// receiver clamps every incoming ACK's sequence to this ceiling,
	// so a non-compliant server's early or forged ACK cannot advance
	// ackedFsn over a frame the send goroutine is still reading out of
	// the mmap'd segment — which would let the segment manager munmap
	// that buffer mid-read (SIGSEGV) — nor over a frame a wire failure
	// dropped before delivery (silent loss). nextWireSeq is bumped
	// BEFORE the wire write, so it sits one frame too high to serve as
	// this ceiling; highestFullySent advances only AFTER the write
	// completes. The send goroutine writes it concurrently with the
	// receiver goroutine's reads (and the reset paths re-seed it
	// between connections), so it must be atomic.
	highestFullySent atomic.Int64
	// serverAckedSeq is the highest cumulative wire sequence the server
	// has OK-ACK'd on the current connection, or -1 before the first
	// ACK. Reset to -1 on every (re)connect alongside highestFullySent.
	// Written by the receiver goroutine; read in applyAckWatermark.
	// Paired with highestFullySent: the engine's ACK cursor advances to
	// the lesser of the two (see applyAckWatermark), reconciling the
	// receiver's ACK against the sender's send-completion no matter which
	// of the two — written on separate goroutines — lands last.
	serverAckedSeq atomic.Int64
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

	// framesSentOnConn counts frames written to the wire on the
	// current connection (reset on every connection swap). Paired
	// with the lifetime totalAcks counter in the silent-drop guard
	// in run(): a connection that sends frames yet sees no ACK while
	// totalAcks == 0 is a candidate for the "server up but doesn't
	// speak our protocol" classification.
	framesSentOnConn atomic.Int64

	// silentConnStrikes counts consecutive connections that sent at
	// least one frame and ended while totalAcks was still 0 — i.e.
	// ACK-less drops on a sender that has never once been ACK'd. The
	// silent-drop guard in run() declares the server incompatible
	// (and stops retrying) once this reaches qwpSfMaxSilentConnStrikes;
	// a lone restart/RST in the first-frame→first-ACK window stays
	// below the threshold and reconnects+replays. No reset is needed:
	// the guard's totalAcks == 0 precondition makes this counter
	// unreachable — and thus frozen — the moment any ACK lands.
	silentConnStrikes atomic.Int64

	// Reconnect-loop status, exposed so engineAppendBlocking can
	// distinguish "wire publishing but slow" from "wire is in the
	// retry loop" when the backpressure deadline fires (spec §16).
	// outageStartUnixNano is non-zero iff connectWithBackoff is
	// currently running; reconnectAttempts is the per-outage counter
	// (resets at the start of each connectWithBackoff call).
	outageStartUnixNano atomic.Int64
	reconnectAttempts   atomic.Int64

	// onTransportSwap, when non-nil, is invoked from swapClient with
	// the freshly bound transport so the sender can refresh
	// connection-derived state (currently: the auto_flush_bytes
	// clamp derived from X-QWP-Max-Batch-Size). Atomic pointer so
	// the producer-side install in the sender constructor cannot
	// race the I/O goroutine's reconnect-time read. nil = no
	// callback installed (legacy bench harness / drainers).
	onTransportSwap atomic.Pointer[func(*qwpTransport)]
}

// qwpSfNewSendLoop constructs a send loop bound to the given engine
// and (optional) initial transport.
//
//   - When transport is non-nil it must already be connected and
//     WebSocket-upgraded; the send loop takes ownership and will
//     close it on shutdown.
//   - When transport is nil, the loop drives the initial dial on
//     its I/O goroutine before serving frames — this is the
//     `initial_connect_retry=async` path. A nil transport is only
//     valid together with a non-nil factory (otherwise there's no
//     way for the loop to obtain a connection).
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
	if engine == nil {
		panic("qwp/sf: engine must be non-nil")
	}
	if transport == nil && factory == nil {
		panic("qwp/sf: nil transport requires a non-nil reconnect factory")
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
		wakeup:                  make(chan struct{}, 1),
		replayTargetFsn:         -1,
		previousIdx:             -1,
	}
	l.policyResolver.Store(&qwpSfPolicyResolver{})
	l.dispatcher.Store(newQwpSfErrorDispatcher(nil, qwpSfDefaultErrorInboxCapacity))
	l.connDispatcher.Store(newQwpConnDispatcher(nil, qwpSfDefaultErrorInboxCapacity))
	l.transport.Store(transport)
	// Seed the "nothing fully sent yet" / "nothing ACK'd yet" sentinels;
	// positionCursorForStart and swapClient re-establish both on every
	// (re)connect.
	l.highestFullySent.Store(-1)
	l.serverAckedSeq.Store(-1)
	// Wire the producer's per-publish doorbell. Set here (before
	// sendLoopStart and before any producer append) so it satisfies
	// the ring's "set once before producing starts" contract, and so
	// every construction path — memory and SF — gets it for free.
	engine.engineSetSendLoopWakeup(l.wakeSender)
	return l
}

// wakeSender pushes a non-blocking token so a parked senderLoop wakes
// on the very next iteration. Cheap; safe to call from any goroutine;
// idempotent (multiple publishes coalesce into the single slot).
// No-op when a token is already pending. Mirrors
// qwpSfSegmentManager.wakeWorker.
func (l *qwpSfSendLoop) wakeSender() {
	select {
	case l.wakeup <- struct{}{}:
	default:
	}
}

// sendLoopSetOnTransportSwap installs a callback fired by swapClient
// after each successful transport bind (initial sync connect on the
// memory-mode path, and every reconnect on either path). The
// sender uses it to refresh state derived from the upgrade
// response — currently the X-QWP-Max-Batch-Size-derived
// auto_flush_bytes clamp. Idempotent: a later call replaces the
// previous callback. Pass nil to clear. Safe to call before
// sendLoopStart or while the loop is running (atomic install).
//
// The callback runs on whichever goroutine triggered the swap: the
// producer goroutine for the constructor's seed call, the I/O
// goroutine for every reconnect. Implementations must be cheap and
// non-blocking — the swap path is on the wire's critical path.
func (l *qwpSfSendLoop) sendLoopSetOnTransportSwap(cb func(*qwpTransport)) {
	if cb == nil {
		l.onTransportSwap.Store(nil)
		return
	}
	l.onTransportSwap.Store(&cb)
}

// sendLoopSetHostTracker installs the failover.md §2 host-health
// tracker. Optional — when not called, the loop builds a 1-host
// implicit tracker on first connectWithBackoff entry so all paths
// converge on the round-walk machinery. initialBoundIdx is the
// host index the caller already bound (e.g. from
// qwpSfConnectWithRetry's initial-sync path); pass -1 when no host
// has been bound yet (initial-async path) or for legacy single-host
// tests. MUST be called before sendLoopStart; not safe to call
// concurrently.
func (l *qwpSfSendLoop) sendLoopSetHostTracker(tracker *qwpHostTracker, initialBoundIdx int) {
	l.tracker = tracker
	l.previousIdx = initialBoundIdx
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
//
// Safe to call from within a SenderErrorHandler: old.close() detects
// that it is running on the old dispatcher's own loop goroutine and
// returns without joining itself (see qwpSfErrorDispatcher.close).
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

// sendLoopSetConnectionListener replaces the connection-event listener and the
// dispatcher inbox capacity. Mirrors sendLoopSetErrorHandler.
func (l *qwpSfSendLoop) sendLoopSetConnectionListener(listener SenderConnectionListener, capacity int) {
	if capacity <= 0 {
		capacity = qwpSfDefaultErrorInboxCapacity
	}
	old := l.connDispatcher.Swap(newQwpConnDispatcher(listener, capacity))
	if old != nil {
		old.close()
	}
}

// sendLoopSetEndpoints records the addr= list so connection events can carry
// host:port for a bound index. Called once before sendLoopStart.
func (l *qwpSfSendLoop) sendLoopSetEndpoints(endpoints []qwpEndpoint) {
	l.endpoints = endpoints
}

// sendLoopConnDispatcher returns the current connection-event dispatcher.
func (l *qwpSfSendLoop) sendLoopConnDispatcher() *qwpDispatcher[*SenderConnectionEvent] {
	return l.connDispatcher.Load()
}

// emitConn builds and offers a connection event. idx/previousIdx are endpoint
// indices (-1 when not applicable); host:port are filled from l.endpoints.
func (l *qwpSfSendLoop) emitConn(kind SenderConnectionEventKind, idx, previousIdx int, attempt int64, cause error) {
	e := &SenderConnectionEvent{
		Kind:            kind,
		AttemptNumber:   attempt,
		Cause:           cause,
		TimestampMillis: time.Now().UnixMilli(),
	}
	if idx >= 0 && idx < len(l.endpoints) {
		e.Host = l.endpoints[idx].host
		e.Port = l.endpoints[idx].port
	}
	if previousIdx >= 0 && previousIdx < len(l.endpoints) {
		e.PreviousHost = l.endpoints[previousIdx].host
		e.PreviousPort = l.endpoints[previousIdx].port
	}
	l.connDispatcher.Load().offer(e)
}

// emitInitialConnected fires the one-shot CONNECTED event for a sender that
// connected synchronously at construction (InitialConnectOff/Sync). The async
// path emits CONNECTED from connectWithBackoff instead.
func (l *qwpSfSendLoop) emitInitialConnected(idx int) {
	if l.connectedOnce.CompareAndSwap(false, true) {
		l.emitConn(SenderConnected, idx, -1, 0, nil)
	}
}

// sendLoopStart launches the I/O goroutine. Idempotent — a second
// call panics.
func (l *qwpSfSendLoop) sendLoopStart() {
	if !l.running.CompareAndSwap(false, true) {
		panic("qwp/sf: send loop already started")
	}
	// Position cursor at the first unsent FSN before the goroutine
	// observes any state. If the walk hits a corrupt frame header,
	// latch the error and still spin up the goroutine — its first
	// iteration sees running=false and exits cleanly, releasing
	// wg/done. Producer-side calls then surface the latched error.
	if err := l.positionCursorForStart(); err != nil {
		l.recordFatal(err)
	}
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
		_ = t.close()
	}
	if d := l.dispatcher.Load(); d != nil {
		d.close()
	}
	if d := l.connDispatcher.Load(); d != nil {
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

// sendLoopDone returns a channel closed when the I/O goroutine exits —
// on graceful sendLoopClose and on every terminal HALT path alike
// (run() closes it via defer). A blocked AwaitAckedFsn selects on it so
// it stops waiting once ackedFsn can no longer advance, rather than
// hanging until its ctx fires.
func (l *qwpSfSendLoop) sendLoopDone() <-chan struct{} {
	return l.done
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
//
// Invariant: callers MUST invoke this before dispatcher.offer(se) on
// any HALT path. The dispatcher delivers asynchronously to user
// handlers that may synchronously probe sendLoopCheckError() or call
// Flush; if the latch is written after offer, those probes race and
// can see nil. See qwp-cursor-error-api.md §120 and the Java
// CursorWebSocketSendLoop comments around recordFatal/dispatchError.
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

// sendLoopReconnectStatus reports whether the I/O loop is currently
// inside connectWithBackoff. When reconnecting is true, attempts is
// the per-outage attempt counter (≥ 1) and outageStart is the wall-
// clock time the current outage began. When reconnecting is false,
// attempts is 0 and outageStart is the zero time.Time.
//
// Used by engineAppendBlocking to enrich the backpressure timeout
// error per spec §16: distinguish "publishing but slow" from
// "reconnecting" with attempt count + outage start.
func (l *qwpSfSendLoop) sendLoopReconnectStatus() (reconnecting bool, attempts int64, outageStart time.Time) {
	startNanos := l.outageStartUnixNano.Load()
	if startNanos == 0 {
		return false, 0, time.Time{}
	}
	return true, l.reconnectAttempts.Load(), time.Unix(0, startNanos)
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

// sendLoopTotalFramesReplayed returns the cumulative count of
// frames re-emitted on the post-reconnect catch-up window — i.e.
// frames whose FSN was already on the wire before the drop.
func (l *qwpSfSendLoop) sendLoopTotalFramesReplayed() int64 {
	return l.totalFramesReplayed.Load()
}

// positionCursorForStart sets fsnAtZero, nextWireSeq,
// highestFullySent, and the cursor (sendingSegment + sendOffset) to
// the first unsent FSN. Must be called by the I/O goroutine before it
// starts sending — the producer thread captures the engine's state at
// that moment. Returns a non-nil error if the cursor walk hits a
// corrupt frame header; see positionCursorAt.
func (l *qwpSfSendLoop) positionCursorForStart() error {
	replayStart := l.engine.engineAckedFsn() + 1
	l.fsnAtZero.Store(replayStart)
	l.nextWireSeq.Store(0)
	l.highestFullySent.Store(-1)
	l.serverAckedSeq.Store(-1)
	l.framesSentOnConn.Store(0)
	return l.positionCursorAt(replayStart)
}

// positionCursorAt points the cursor (sendingSegment + sendOffset) at
// the frame for targetFsn. It is called at startup and after every
// reconnect, once fsnAtZero has been reset to targetFsn and nextWireSeq
// to 0.
//
// If targetFsn is already published, the cursor lands exactly on that
// frame. If targetFsn is not published yet, the cursor parks at the
// active segment's current tip and the normal send loop waits for the
// producer to publish more bytes.
//
// Returns a non-nil error if the frame walk hits a corrupt header; see
// positionCursorInSegment.
func (l *qwpSfSendLoop) positionCursorAt(targetFsn int64) error {
	seg := l.engine.engineFindSegmentContaining(targetFsn)
	if seg == nil {
		// No segment currently advertises targetFsn. That normally
		// means targetFsn is just past publishedFsn and there is
		// nothing to replay yet, so the cursor resumes from the active
		// tip.
		//
		// The producer runs concurrently with this I/O goroutine,
		// though: it can publish targetFsn after the lookup above
		// returns nil but before (or during) the active-tip snapshot
		// below. publishedOffset() reads publishedCursor, which
		// tryAppend stores AFTER it increments frameCount — so if this
		// read observes the new frame's bytes, the frameCount bump that
		// makes targetFsn discoverable is necessarily visible too, and
		// the re-check below finds it and lands the cursor exactly on
		// targetFsn (keeping wireSeq=0 mapped to targetFsn). Without the
		// re-check we would park at the post-publish tip — one frame
		// past targetFsn — dropping targetFsn and misnumbering every
		// following frame by one, i.e. silent row loss on
		// reconnect-under-load (see Java PR #40). If the producer
		// publishes only later, both lookups miss, sendOffset stays at
		// the old tip, and trySendOne sends the frame normally.
		l.sendingSegment = l.engine.engineActiveSegment()
		if l.sendingSegment == nil {
			l.sendOffset = qwpSfHeaderSize
			return nil
		}
		l.sendOffset = l.sendingSegment.publishedOffset()
		if seg = l.engine.engineFindSegmentContaining(targetFsn); seg != nil {
			return l.positionCursorInSegment(seg, targetFsn)
		}
		return nil
	}
	return l.positionCursorInSegment(seg, targetFsn)
}

// positionCursorInSegment points sendingSegment/sendOffset at targetFsn
// inside seg, which the caller has already established contains it.
// Segment frame boundaries are not indexed, so it walks payload strides
// from the segment's baseSeq until it reaches targetFsn.
//
// Returns a non-nil error if a frame header along the walk has a
// payloadLen that is negative or that would push the walk past the
// end of the segment buffer — defense-in-depth against a corrupt
// segment that escaped CRC recovery. Without these bounds a
// corrupt-but-positive length (e.g. 0x7FFFFFFF) would overrun offset
// and panic on the next slice index; the panic fires on the
// unrecovered I/O goroutine and crashes the process, bypassing
// recordFatal. Mirrors the bound in qwpSfScanFrames. tryAppend
// validates payloadLen on write and recovery's CRC scan validates it
// on startup, so this is not expected to fire in practice; the callers
// route the returned error through recordFatal.
func (l *qwpSfSendLoop) positionCursorInSegment(seg *qwpSfSegment, targetFsn int64) error {
	l.sendingSegment = seg
	// Walk frame-by-frame from HEADER_SIZE until we land on targetFsn.
	offset := qwpSfHeaderSize
	fsn := seg.segmentBaseSeq()
	base := seg.address()
	segLen := int64(len(base))
	for fsn < targetFsn {
		// Bound the header read itself: a prior corrupt stride could
		// have left offset within the buffer but with fewer than
		// qwpSfFrameHeaderSize bytes remaining.
		if offset < qwpSfHeaderSize || offset+qwpSfFrameHeaderSize > segLen {
			return fmt.Errorf("qwp/sf: frame header at offset %d overruns segment size %d baseSeq=%d (corrupt segment)",
				offset, segLen, seg.segmentBaseSeq())
		}
		payloadLen := int64(int32(binary.LittleEndian.Uint32(base[offset+4 : offset+8])))
		// Reject negative and corrupt-but-positive lengths: a stride
		// that runs past the buffer would panic the next iteration's
		// slice index on the unrecovered I/O goroutine.
		if payloadLen < 0 || offset+qwpSfFrameHeaderSize+payloadLen > segLen {
			return fmt.Errorf("qwp/sf: invalid payloadLen %d at offset %d in segment baseSeq=%d size=%d (corrupt segment)",
				payloadLen, offset, seg.segmentBaseSeq(), segLen)
		}
		offset += qwpSfFrameHeaderSize + payloadLen
		fsn++
	}
	l.sendOffset = offset
	return nil
}

// run is the outer reconnect loop. Each iteration runs one
// connection's worth of I/O via runOneConnection; on wire failure
// it backs off and reconnects (if a factory is wired) or records
// the failure as terminal and exits.
//
// When the loop is constructed with a nil transport (the
// `initial_connect_retry=async` path) the very first iteration
// performs the initial dial in-band on this goroutine using the
// same backoff loop as reconnect. Producers that publish before
// the wire is up experience backpressure via engineAppendBlocking;
// terminal initial-connect failures are surfaced via the dispatcher
// and latched as the loop's terminal error.
func (l *qwpSfSendLoop) run() {
	defer l.wg.Done()
	defer close(l.done)
	// Release the active transport on every exit from this loop,
	// including a terminal HALT (recordFatal* + offer, then return)
	// where no reconnect or Close has swapped it out yet. Without this
	// the dead WebSocket — and its server-side connection — would
	// linger until the user eventually calls Close(). Idempotent and
	// nil-safe: on a clean shutdown sendLoopClose has not yet swapped
	// the transport (it does so after wg.Wait), so the swap here wins
	// and its later swap sees nil; close() guards a nil conn and pins
	// one result via closeOnce.
	defer func() {
		if t := l.transport.Swap(nil); t != nil {
			_ = t.close()
		}
	}()
	// Convert a panic on this wire-driving goroutine into the same
	// latched terminal error an explicit HALT produces, so untrusted
	// server bytes can never crash the host process. Registered last so
	// it runs FIRST on unwind (defers are LIFO): recordFatal latches the
	// error before close(l.done) and wg.Done() release any waiter,
	// preserving the "error latched before the loop exits" ordering the
	// inline terminal paths below rely on. Every slice/index read on this
	// path is bounds-checked, so this is defense-in-depth, not a known
	// reachable panic.
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("qwp/sf: send loop panicked: %v\n%s", r, debug.Stack())
			log.Printf("[ERROR] %v", err)
			l.recordFatal(err)
		}
	}()

	if l.transport.Load() == nil && l.running.Load() {
		initial := errors.New("async initial connect deferred to I/O goroutine")
		if !l.connectWithBackoff(initial, "initial connect") {
			return
		}
	}

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
			// Latch BEFORE dispatching: a handler that synchronously
			// calls Flush / sendLoopCheckError must observe the typed
			// terminal error. See qwp-cursor-error-api.md §120.
			l.recordFatalServerError(se)
			l.dispatcher.Load().offer(se)
			return
		}
		if l.reconnectFactory == nil {
			l.recordFatal(err)
			return
		}
		if qwpSfIsTerminalUpgradeError(err) {
			se := l.qwpSfBuildUpgradeFailureSE(err)
			l.totalServerErrors.Add(1)
			l.recordFatalServerError(se)
			l.dispatcher.Load().offer(se)
			return
		}
		// Detect "server up, accepts the WS upgrade, but doesn't speak
		// our QWP protocol" — the dial succeeds every time, so plain
		// reconnect-with-backoff would hammer the server in a hot
		// loop until reconnectMaxDuration expires (5 min default),
		// burning thousands of ephemeral ports per second.
		//
		// Gate on *lifetime* ACK history (totalAcks), not the per-
		// connection counter: once any ACK has been observed across
		// this sender's life, we have proof the server speaks our
		// wire-format dialect, so a later silent disconnect is a
		// transient outage (LB drain emitting WS 1001 GoingAway, TCP
		// RST surfacing as 1006, proxy reset, graceful 1011/1012/
		// 1013 — none of which are flagged terminal by
		// qwpSfIsTerminalCloseCode) and reconnect is the right
		// reaction. The never-ACK'd case is the terminal candidate
		// here: the port-hammering signature is a fresh sender whose
		// every dial succeeds and every frame is met with silence,
		// repeatedly. The strike-count gate below decides when that
		// pattern has repeated enough to be conclusive.
		if l.framesSentOnConn.Load() > 0 && l.totalAcks.Load() == 0 {
			// This connection finished the WS upgrade and the X-QWP-
			// Version negotiation, sent frames, then closed without
			// ACKing any of them — and no prior connection on this
			// sender has ACK'd anything either.
			//
			// A single such strike is ambiguous: a routine server
			// restart or LB RST landing in the window between a fresh
			// sender's first frame and its first ACK produces the
			// identical signature, so it counts as a strike and falls
			// through to an ordinary reconnect+replay. Reaching
			// qwpSfMaxSilentConnStrikes consecutive ACK-less
			// connections — at least one full reconnect+replay cycle
			// that still met nothing but silence — is conclusive
			// evidence the server isn't speaking our wire-format
			// dialect (most often: a server build older than this
			// client's branch, even if both sides declared the same
			// X-QWP-Version). At that point we fail terminally to
			// avoid hammering the server with thousands of dial
			// attempts per second until reconnectMaxDuration expires.
			if l.silentConnStrikes.Add(1) >= qwpSfMaxSilentConnStrikes {
				reason := fmt.Sprintf(
					"server accepted the WebSocket upgrade but %d consecutive "+
						"connection(s) disconnected without ACKing any of the "+
						"frames we sent — server is likely running an incompatible "+
						"build (won't retry): %s",
					l.silentConnStrikes.Load(), err.Error())
				se := l.qwpSfBuildBudgetExhaustedSE(reason)
				l.totalServerErrors.Add(1)
				l.emitConn(SenderReconnectBudgetExhausted, -1, -1, 0, err)
				l.recordFatalServerError(se)
				l.dispatcher.Load().offer(se)
				return
			}
			// Fall through to reconnect+replay. If the next connection
			// also sends frames and meets silence the strike count
			// crosses the threshold and we HALT then.
		}
		// The wire dropped; announce the outage before the reconnect walk.
		l.emitConn(SenderDisconnected, l.previousIdx, -1, 0, err)
		// Reconnect with backoff.
		ok := l.connectWithBackoff(err, "reconnect")
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
	// runGuarded executes one inner loop and reports its outcome on errCh.
	// senderLoop and receiverLoop run on their own goroutines, so run()'s
	// recover defer does not cover them; this converts a panic on either —
	// e.g. parsing untrusted server ACK bytes in receiverLoop, or walking
	// segment frames in senderLoop — into an ordinary error so run()'s
	// terminal-error path latches it via recordFatal instead of crashing
	// the host. Both loops' slice/index reads are bounds-checked, so this
	// is defense-in-depth, not a known reachable panic.
	runGuarded := func(name string, fn func(context.Context) error) {
		defer inner.Done()
		defer connCancel()
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("qwp/sf: %s panicked: %v\n%s", name, r, debug.Stack())
				log.Printf("[ERROR] %v", err)
				errCh <- loopErr{err}
			}
		}()
		errCh <- loopErr{fn(connCtx)}
	}
	go runGuarded("sender loop", l.senderLoop)
	go runGuarded("receiver loop", l.receiverLoop)
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
	// A single reusable timer backs the fallback poll, re-armed each
	// idle iteration. The doorbell (wakeup) drives the common case, so
	// the timer only bounds how long a missed wakeup can stall a ready
	// frame; it never gates steady-state latency.
	timer := time.NewTimer(l.parkInterval)
	defer timer.Stop()
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
			// Drain a possibly-fired timer before Reset (same
			// dance as qwpSfSegmentManager.workerLoop). Wake on
			// shutdown, a producer doorbell, or the fallback tick.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(l.parkInterval)
			select {
			case <-ctx.Done():
				return nil
			case <-l.wakeup:
			case <-timer.C:
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
	// wireSeq/fsnSent for this frame derive from nextWireSeq, which
	// the send goroutine advances here before the wire write. A wire
	// failure thus leaves nextWireSeq advanced for a frame that never
	// made it out; that is harmless because every reconnect path
	// resets it via swapClient/positionCursorForStart. The receiver's
	// ACK clamp keys off highestFullySent — advanced only after the
	// write below returns — not off nextWireSeq, so a server's
	// early/forged ACK cannot ride this pre-bump to cover the
	// in-flight frame.
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
	// The frame is fully on the wire. Publish highestFullySent only
	// now, after sendMessage returns: this is what lets the receiver
	// safely let an ACK advance ackedFsn over this frame. Until this
	// store the receiver clamps any ACK naming this sequence down to
	// the previous frame, so the segment manager cannot trim (munmap)
	// the segment while the payload slice we handed sendMessage still
	// points into it.
	l.highestFullySent.Store(wireSeq)
	// An ACK for this frame may already have landed and been held back
	// while highestFullySent still trailed it; reconcile now that the
	// watermark is published so a quiescent last frame — whose ACK has
	// no later ACK to re-drive it — does not strand its acknowledgement.
	l.applyAckWatermark()
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

// applyAckWatermark advances the engine's ACK cursor to the lesser of
// the server's cumulative ACK sequence (serverAckedSeq, owned by the
// receiver) and the highest wire sequence whose send has fully returned
// (highestFullySent, owned by the sender), mapped through fsnAtZero.
// Both inputs are monotonic within a connection but written on separate
// goroutines, so it is called from both: by the receiver as each ACK
// lands, and by the sender right after it publishes a fresh
// highestFullySent. Whichever store completes last observes both values
// and drives the advance — closing the race where the ACK for the only
// in-flight frame arrives before the send completes and would otherwise
// be stranded (no later ACK to re-drive it, leaving engineAckedFsn
// below publishedFsn forever).
//
// The min is the munmap-safety clamp: capping at highestFullySent keeps
// ackedFsn off any frame the send goroutine is still reading out of the
// mmap'd segment, and off a frame a wire failure dropped before
// delivery — so a non-compliant server's early or forged ACK cannot
// move the watermark past what we have actually put on the wire.
// engineAcknowledge is monotonic, idempotent, and clamps to
// publishedFsn internally, so the concurrent calls from the two
// goroutines are safe and a stale-lower min is ignored.
func (l *qwpSfSendLoop) applyAckWatermark() {
	sent := l.highestFullySent.Load()
	acked := l.serverAckedSeq.Load()
	if sent < 0 || acked < 0 {
		return
	}
	if acked > sent {
		acked = sent
	}
	l.engine.engineAcknowledge(l.fsnAtZero.Load() + acked)
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
			// Sanity clamp: do not trust a rejection wireSeq beyond the
			// frames whose sendMessage has fully returned. Without this
			// clamp the DROP path can advance ackedFsn over an in-flight
			// or never-delivered frame, which makes the segment manager
			// trim (munmap) a segment the I/O thread is still reading.
			// Mirrors handleServerRejection in the Java client. The
			// clamp only feeds the FSN math; the reported MessageSequence
			// is the raw server-sent seq so it round-trips verbatim
			// against server-side logs.
			highestSent := l.highestFullySent.Load()
			_, _, msg := parseAckErrorPayload(data)
			cat := qwpSfClassify(status)
			pol := l.policyResolver.Load().resolve(cat)
			if highestSent < 0 {
				// Pre-send rejection: no frame has finished sending on
				// this connection yet, so the server emitted the error
				// frame before it could have received one of ours
				// (typical right after a fresh swapClient — auth failure,
				// server-initiated halt, etc.). The server-named
				// wireSeq does not correspond to any frame we delivered,
				// so clamping to 0 and acknowledging fsnAtZero would
				// silently advance ackedFsn past a real unsent batch
				// (fsnAtZero == ackedFsn + 1 right after a swap).
				// Attribute the failure to the unacked
				// [ackedFsn+1, publishedFsn] window — the same span
				// the protocol-violation close path uses — and skip
				// the watermark advance entirely; there is nothing
				// on this connection to drop. Still surface the
				// typed error so HALT latches and the handler fires.
				// Mirrors handlePreSendRejection in the Java client.
				from := l.engine.engineAckedFsn() + 1
				to := l.engine.enginePublishedFsn()
				if to < from {
					to = from
				}
				se := &SenderError{
					Category:         cat,
					AppliedPolicy:    pol,
					ServerStatusByte: int(status),
					ServerMessage:    msg,
					MessageSequence:  seq,
					FromFsn:          from,
					ToFsn:            to,
					DetectedAt:       time.Now(),
				}
				l.totalServerErrors.Add(1)
				if pol == PolicyHalt {
					l.recordFatalServerError(se)
					l.dispatcher.Load().offer(se)
					return se
				}
				l.dispatcher.Load().offer(se)
				continue
			}
			cappedSeq := seq
			if cappedSeq > highestSent {
				cappedSeq = highestSent
			}
			fsn := l.fsnAtZero.Load() + cappedSeq
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
			if pol == PolicyHalt {
				// Latch BEFORE dispatching: a handler that
				// synchronously calls Flush / sendLoopCheckError
				// must observe the typed terminal error. See
				// qwp-cursor-error-api.md §120.
				l.recordFatalServerError(se)
				l.dispatcher.Load().offer(se)
				return se
			}
			l.dispatcher.Load().offer(se)
			// PolicyDropAndContinue: advance past the rejected span
			// via the same engine entry the success branch uses. The
			// segment manager will trim the now-acked range on its
			// next maintenance pass. Bump totalAcks for parity with
			// the success path so producer-visible counters reflect
			// "the server has resolved this batch".
			l.engine.engineAcknowledge(fsn)
			l.totalAcks.Add(1)
			continue
		}
		// Record the server's cumulative ACK sequence, then reconcile it
		// against highestFullySent. applyAckWatermark caps the advance at
		// the last fully-sent frame, so a malformed, early, or forged
		// server response can never move ackedFsn over an in-flight frame
		// (a trim would munmap a segment the I/O thread is still reading)
		// nor over a frame a wire failure dropped before delivery. The
		// matching call on the send side re-drives the same reconciliation
		// so an ACK that arrives before its frame's send completes is not
		// stranded when no later ACK follows it.
		l.serverAckedSeq.Store(seq)
		l.totalAcks.Add(1)
		l.applyAckWatermark()
	}
}

// connectWithBackoff runs the failover.md §13.6 round-walk through
// qwpSfRunRoundWalk: each iteration demotes a just-failed host
// (previousIdx), picks the highest-priority unattempted endpoint,
// dials it, and classifies the outcome. Round-boundary sleep pays
// equal-jitter exponential backoff for transport rounds and a
// non-doubling InitialBackoff for role-reject rounds. Returns true
// on a successful bind (caller resumes the pump loop), false on
// terminal failure / budget exhaustion / shutdown.
//
// Shared between the reconnect path (phase="reconnect") and the
// async-initial-connect path (phase="initial connect"); the phase
// string only flavors the log/error message — control flow is
// identical.
func (l *qwpSfSendLoop) connectWithBackoff(initial error, phase string) bool {
	if l.tracker == nil {
		// Legacy single-host path (tests that didn't call
		// sendLoopSetHostTracker). Synthesize an implicit 1-host
		// tracker so the round-walk machinery handles every code
		// path uniformly.
		l.tracker = newQwpHostTracker(1, "", qwpTargetAny)
	}
	outageStart := time.Now()
	l.outageStartUnixNano.Store(outageStart.UnixNano())
	l.reconnectAttempts.Store(0)
	defer func() {
		l.outageStartUnixNano.Store(0)
		l.reconnectAttempts.Store(0)
	}()

	// Snapshot the entering previousIdx and consume it for this
	// connect cycle. The round-walk calls RecordMidStreamFailure
	// internally; we reset our slot so a subsequent successful
	// bind starts clean.
	enteringPreviousIdx := l.previousIdx
	l.previousIdx = -1

	var round int64
	params := qwpSfRoundWalkParams{
		Factory:        l.reconnectFactory,
		Tracker:        l.tracker,
		MaxDuration:    l.reconnectMaxDuration,
		InitialBackoff: l.reconnectInitialBackoff,
		MaxBackoff:     l.reconnectMaxBackoff,
		OnAttempt: func() {
			l.reconnectAttempts.Add(1)
			l.totalReconnectAttempts.Add(1)
		},
		OnEndpointFailed: func(idx int, derr error) {
			l.emitConn(SenderEndpointAttemptFailed, idx, -1, l.reconnectAttempts.Load(), derr)
		},
		OnRoundExhausted: func() {
			round++
			l.connDispatcher.Load().offer(&SenderConnectionEvent{
				Kind:            SenderAllEndpointsUnreachable,
				RoundNumber:     round,
				TimestampMillis: time.Now().UnixMilli(),
			})
		},
	}
	result := qwpSfRunRoundWalk(l.ctx, nil, params, enteringPreviousIdx)

	if result.Transport != nil {
		// Successful bind. Remember the idx so a subsequent
		// pump-exit can mid-stream-demote.
		l.previousIdx = result.Idx
		if swapErr := l.swapClient(result.Transport); swapErr != nil {
			// Cursor positioning detected segment corruption —
			// not retryable; reconnecting won't fix bad bytes
			// in the on-disk segment.
			l.recordFatal(swapErr)
			return false
		}
		l.totalReconnects.Add(1)
		attempt := l.reconnectAttempts.Load()
		switch {
		case !l.connectedOnce.Swap(true):
			// First successful connect in the sender's lifetime — not a
			// reconnect, so AttemptNumber is 0 (matches the sync/off
			// emitInitialConnected path and the SenderConnectionEvent
			// contract: 0 = initial / not-a-reconnect).
			l.emitConn(SenderConnected, result.Idx, -1, 0, nil)
		case result.Idx == enteringPreviousIdx:
			l.emitConn(SenderReconnected, result.Idx, -1, attempt, nil)
		default:
			l.emitConn(SenderFailedOver, result.Idx, enteringPreviousIdx, attempt, nil)
		}
		return true
	}
	if result.Terminal != nil {
		se := l.qwpSfBuildUpgradeFailureSE(result.Terminal)
		l.totalServerErrors.Add(1)
		// Fire the terminal connection event before latching the producer-
		// observable error, so a listener is notified no later than the
		// producer learns via its next API call (matches Java's order).
		l.emitConn(SenderAuthFailed, -1, -1, l.reconnectAttempts.Load(), result.Terminal)
		l.recordFatalServerError(se)
		l.dispatcher.Load().offer(se)
		return false
	}
	if result.Cancelled != nil {
		// ctx cancelled (close), or the round-walk reported a
		// configuration error. The latter is rare and benign at
		// shutdown; sample running to distinguish.
		if !l.running.Load() {
			return false
		}
		l.recordFatal(fmt.Errorf("%s aborted: %w", phase, result.Cancelled))
		return false
	}
	// Budget exhausted. Surface the underlying error chain to the
	// dispatcher; reach into qwpSfBuildBudgetExhaustedSE so the
	// SenderError carries the per-host snapshot. `initial` is the
	// caller-supplied entry error (the mid-stream failure that
	// triggered this connectWithBackoff); attach it as context.
	reason := fmt.Sprintf("%s failed: %v (after entry error: %v)",
		phase, result.Exhausted, initial)
	se := l.qwpSfBuildBudgetExhaustedSE(reason)
	l.totalServerErrors.Add(1)
	l.emitConn(SenderReconnectBudgetExhausted, -1, -1, l.reconnectAttempts.Load(), result.Exhausted)
	l.recordFatalServerError(se)
	l.dispatcher.Load().offer(se)
	return false
}

// swapClient replaces the active transport, realigns fsnAtZero to
// the next unacked FSN, restarts wire sequencing from 0 (clearing the
// fully-sent and server-ACK'd watermarks), and repositions the cursor
// so the next trySendOne call replays the first unacked frame. Returns
// a non-nil error if the cursor walk hits a corrupt frame header; see
// positionCursorAt.
//
// On success, fires onTransportSwap (if installed) with the new
// transport so the sender can refresh connection-derived state
// (the auto_flush_bytes clamp). The callback runs after the
// transport is published via atomic.Swap and after the cursor is
// repositioned, so any sender side effect (e.g. an updated
// effective threshold) is in place before the next trySendOne can
// publish a frame on the new connection.
func (l *qwpSfSendLoop) swapClient(newTransport *qwpTransport) error {
	old := l.transport.Swap(newTransport)
	if old != nil {
		_ = old.close()
	}
	replayStart := l.engine.engineAckedFsn() + 1
	l.fsnAtZero.Store(replayStart)
	l.nextWireSeq.Store(0)
	l.highestFullySent.Store(-1)
	l.serverAckedSeq.Store(-1)
	l.framesSentOnConn.Store(0)
	pubAtSwap := l.engine.enginePublishedFsn()
	if pubAtSwap >= replayStart {
		l.replayTargetFsn = pubAtSwap
	} else {
		l.replayTargetFsn = -1
	}
	if err := l.positionCursorAt(replayStart); err != nil {
		return err
	}
	if cb := l.onTransportSwap.Load(); cb != nil {
		(*cb)(newTransport)
	}
	return nil
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
// Preferred path: the transport surfaces a typed *QwpUpgradeRejectError
// with the parsed status code. Falls back to substring matching on
// coder/websocket's free-form text so any code path that bypasses the
// typed reject (e.g. a future change in the dial library) still
// classifies cleanly.
func qwpSfIsAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	var rej *QwpUpgradeRejectError
	if errors.As(err, &rej) {
		return rej.StatusCode == 401 || rej.StatusCode == 403
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
//
// The round-walk (failover.md §6) treats 404/426 as transient and
// routes them through RecordTransportError so a misconfig on one
// peer does not lock the client out of healthy siblings. This
// helper remains as a defensive fallback for the run()-level outer
// branch; typed `*QwpUpgradeRejectError`s originate from the factory
// and are consumed by the round-walk, so they do not reach this
// branch in normal operation.
func qwpSfIsProtocolUpgradeFailure(err error) bool {
	if err == nil {
		return false
	}
	var rej *QwpUpgradeRejectError
	if errors.As(err, &rej) {
		return rej.StatusCode == 404 || rej.StatusCode == 426
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

// qwpSfConnectWithRetry runs the failover.md §13.6 round-walk on
// the calling goroutine for the InitialConnectSync path. The walk
// retries with backoff against every host in the tracker until
// success, terminal AuthError (401/403), or budget exhaustion.
// Returns the connected transport plus the bound endpoint index so
// the caller can seed qwpSfSendLoop's previousIdx.
//
// tracker may be nil — the function synthesizes a 1-host implicit
// tracker so legacy single-host tests don't need to construct one.
// In that mode the returned idx is always 0.
//
// factory is invoked once per dial attempt; idx is the host index
// PickNext returned. Single-host callers may ignore idx.
func qwpSfConnectWithRetry(
	ctx context.Context,
	factory qwpSfReconnectFactory,
	tracker *qwpHostTracker,
	maxDuration, initialBackoff, maxBackoff time.Duration,
) (*qwpTransport, int, error) {
	if maxDuration <= 0 {
		maxDuration = qwpSfDefaultReconnectMaxDuration
	}
	if initialBackoff <= 0 {
		initialBackoff = qwpSfDefaultReconnectInitialBackoff
	}
	if maxBackoff <= 0 {
		maxBackoff = qwpSfDefaultReconnectMaxBackoff
	}
	if tracker == nil {
		tracker = newQwpHostTracker(1, "", qwpTargetAny)
	}
	params := qwpSfRoundWalkParams{
		Factory:        factory,
		Tracker:        tracker,
		MaxDuration:    maxDuration,
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
	}
	result := qwpSfRunRoundWalk(ctx, nil, params, -1)
	if result.Transport != nil {
		return result.Transport, result.Idx, nil
	}
	if result.Terminal != nil {
		return nil, -1, fmt.Errorf("qwp/sf: WebSocket upgrade failed (won't retry): %w", result.Terminal)
	}
	if result.Cancelled != nil {
		return nil, -1, result.Cancelled
	}
	if result.Exhausted == nil {
		return nil, -1, errors.New("qwp/sf: round-walk returned no result")
	}
	return nil, -1, fmt.Errorf("qwp/sf: connect failed after %s / %d attempts: %w",
		result.Exhausted.Elapsed, result.Exhausted.Attempts, result.Exhausted.LastError)
}
