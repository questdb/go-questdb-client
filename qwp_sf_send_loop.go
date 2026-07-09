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
	"log/slog"
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
	qwpSfDefaultParkInterval = 1 * time.Millisecond
	// qwpSfDefaultReconnectMaxDuration bounds ONLY the blocking sync
	// initial connect (qwpSfConnectWithRetry). The running loop, async
	// initial connect, and drainers retry unbounded (Invariant B). It
	// doubles as the settle budget wherever a "give the condition time
	// to resolve" knob is needed: the poison-frame detector's episode
	// floor (recordRejectionStrike) and the drainer's no-progress
	// wedge budget.
	qwpSfDefaultReconnectMaxDuration    = 5 * time.Minute
	qwpSfDefaultReconnectInitialBackoff = 100 * time.Millisecond
	qwpSfDefaultReconnectMaxBackoff     = 5 * time.Second
	qwpDurableAckKeepaliveDefault       = 200 * time.Millisecond
	// qwpSfReconnectWarnThrottle rate-limits the running loop's outage
	// WARN lines: a failover window can last minutes and is retried
	// indefinitely, so per-attempt logging would flood.
	qwpSfReconnectWarnThrottle = 5 * time.Second
)

// qwpSfDefaultMaxFrameRejections is the poison-frame detector strike
// threshold: consecutive server-active rejections (retriable NACK, or
// non-orderly close after at least one send on the connection) of the
// SAME frame, with no covering ack in between. Strikes alone do not
// escalate: declaring the frame poisoned (a typed PROTOCOL_VIOLATION
// terminal) additionally requires the rejection episode to have lasted
// at least reconnectMaxDuration, so a transient server-side rejection
// window cannot burn every strike in a second and kill the sender —
// below either bar the loop keeps recycling the connection with paced
// backoff and replays from ackedFsn+1. No data is dropped either way.
// Configurable via max_frame_rejections / WithMaxFrameRejections.
const qwpSfDefaultMaxFrameRejections = 4

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
//     capped equal-jitter exponential backoff with no wall-clock
//     limit (Invariant B), with auth-style failures (401/403/non-101
//     upgrade reject) treated as terminal. On reconnect success,
//     repositions the cursor at ackedFsn+1 and replays.
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

	// reconnectMaxDuration never bounds the running loop's reconnect
	// walk: reconnect_max_duration_millis limits only the blocking sync
	// initial connect (qwpSfConnectWithRetry), never a running loop or
	// drainer (Invariant B). The running loop reuses it as the
	// poison-frame episode budget: a rejection streak must last at
	// least this long before strike-count overflow may escalate to the
	// poisoned-frame terminal (see recordRejectionStrike). Defaulted to
	// qwpSfDefaultReconnectMaxDuration when non-positive, not floored: a
	// smaller reconnect_max_duration_millis deliberately shortens this
	// episode floor too (the knob does double duty with the sync-connect
	// bound, documented on WithReconnectPolicy).
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

	// policyResolver chooses Terminal vs Retriable / RetriableOther per
	// Category. Non-nil; defaults are baked in via qwpSfDefaultPolicyFor.
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

	// deltaDictEnabled mirrors the engine's setting. When true the loop
	// maintains the sent-dictionary mirror below and, on every (re)connect,
	// re-registers the whole dictionary via a catch-up frame before replay so
	// the fresh server can resolve the non-self-sufficient delta frames.
	deltaDictEnabled bool
	// sentDictBytes mirrors — as concatenated [len varint][utf8] in global-id
	// order — every symbol the loop has sent; sentDictCount is how many. It is
	// the source for the reconnect catch-up frame. Written by the send
	// goroutine (accumulateSentDict) and read by the catch-up sender; the two
	// never run concurrently (catch-up runs between connections, when no send
	// goroutine is alive), and it is seeded once at construction from a
	// recovered slot's persisted dictionary.
	sentDictBytes []byte
	sentDictCount int
	// catchUpBuf is a reusable scratch buffer for building catch-up frames;
	// sendMessage copies synchronously, so it is safe to reuse across chunks.
	catchUpBuf []byte

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
	// terminal close, auth-terminal upgrade). Independent of
	// lastError so QwpSender accessors can return the typed payload
	// without an errors.As walk.
	lastTerminalServerError atomic.Pointer[SenderError]

	// Counters.
	totalFramesSent          atomic.Int64
	totalAcks                atomic.Int64
	totalServerErrors        atomic.Int64
	totalReconnects          atomic.Int64
	totalReconnectAttempts   atomic.Int64
	totalFramesReplayed      atomic.Int64
	totalDurableAcks         atomic.Int64
	totalDurableTrimAdvances atomic.Int64

	// Durable-ack (request_durable_ack). Set once before sendLoopStart, then
	// read by the loop goroutines. durable is owned by the receiver goroutine;
	// its pendingLen is atomic so the sender can gate the keepalive ping on it.
	durableAckMode           bool
	durable                  *qwpDurableTracker
	durableKeepaliveInterval time.Duration
	warnedStrayDurable       atomic.Bool
	// durableMismatchTerminal classifies a durable-ack mismatch: true (default,
	// foreground) fails loud; false (drainer) retries within the reconnect budget.
	durableMismatchTerminal bool
	// onRoundExhausted, when set (by a drainer), is invoked by
	// connectWithBackoff once per exhausted reconnect sweep with the
	// sweep's aggregate outcome, so the drainer's per-sweep durable-ack
	// settle accounting covers mid-drain reconnect windows exactly like
	// its initial connect walk. Set before sendLoopStart.
	onRoundExhausted func(qwpSfSweepOutcome)
	// backgroundWalks marks this loop's reconnect walks as background
	// (drainers): attempted hosts land in a walk-local cursor, never in
	// the shared tracker's round slots. Set before sendLoopStart.
	backgroundWalks bool

	// progressDispatcher delivers ackedFsn advances to the user's
	// SenderProgressHandler; nil when none is registered. progressDispatchMu
	// guards lastProgressFsn's compare-store and the offer (both goroutines call
	// dispatchProgress) so the delivered FSN stream stays strictly monotonic.
	progressDispatcher atomic.Pointer[qwpDispatcher[int64]]
	lastProgressFsn    int64
	progressDispatchMu sync.Mutex

	// framesSentOnConn counts frames written to the wire on the
	// current connection (reset on every connection swap). Gates the
	// poison-strike accounting on connection failures in run(): a
	// non-orderly close can only implicate the head-of-line frame if
	// this connection actually sent it.
	framesSentOnConn atomic.Int64

	// acksOnConn counts server acks received on the current connection
	// (reset on every connection swap). run() paces a recycle whose
	// connection sent frames but got nothing back, so a server that
	// upgrades and then closes without acking cannot hot-loop
	// dial→replay→close with no backoff.
	acksOnConn atomic.Int64

	// Poison-frame detector state. poisonFsn is the FSN implicated by
	// the most recent server-active rejection (retriable NACK, or
	// non-orderly close after at least one send on the connection): the
	// NACKed frame itself when the server named a wire sequence, the
	// head-of-line frame (ackedFsn+1) otherwise. poisonStrikes counts
	// consecutive rejections of that same FSN; poisonFirstStrikeAt
	// anchors the episode. Declaring the frame poisoned — the server
	// (or an intermediary) deterministically rejects these exact bytes,
	// so replay cannot succeed and the loop halts with a typed
	// PROTOCOL_VIOLATION terminal — requires BOTH maxFrameRejections
	// consecutive strikes AND an episode at least reconnectMaxDuration
	// long; a strike burst inside a transient rejection window keeps
	// recycling with paced backoff instead of killing the sender. An
	// ack covering poisonFsn resets the detector (in durable mode
	// replay re-delivers already-OK'd predecessors, whose fresh OKs
	// must not shield the rejected frame). rejectionRecycles counts
	// consecutive rejection-driven connection recycles and paces them
	// with capped equal-jitter backoff; only an ack that advances past
	// the retried frame resets it — a durable-mode replay re-OKs
	// predecessors ahead of the rejected frame and must not reset the
	// pacing (see noteAckProgress). Written by
	// the receiver goroutine and by run() — never concurrently: run()
	// touches them only after runOneConnection has joined both inner
	// goroutines, and the next connection's goroutines start after
	// run()'s writes.
	poisonFsn           int64
	poisonStrikes       int
	poisonFirstStrikeAt time.Time
	rejectionRecycles   int
	// maxFrameRejections is the poison-strike threshold
	// (max_frame_rejections; default qwpSfDefaultMaxFrameRejections).
	// Set before sendLoopStart, read-only afterwards.
	maxFrameRejections int

	// logger sinks the loop's own diagnostics and is propagated to the
	// error/connection dispatchers it owns. nil -> slog.Default() via
	// qwpEffectiveLogger; set from the config before sendLoopStart.
	logger *slog.Logger

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
		poisonFsn:               -1,
		lastProgressFsn:         -1,
		maxFrameRejections:      qwpSfDefaultMaxFrameRejections,
		deltaDictEnabled:        engine.engineDeltaDictEnabled(),
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
	// Recovery / orphan-drain: seed the sent-dict mirror from the slot's
	// persisted dictionary so the first connection re-registers the whole
	// dictionary (a catch-up frame) before replaying the recovered delta
	// frames. Empty for a fresh memory-mode sender.
	if l.deltaDictEnabled {
		l.seedSentDictFromPersisted(engine.enginePersistedSymbolDict())
	}
	l.durableMismatchTerminal = true
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
	d := newQwpSfErrorDispatcher(handler, capacity)
	d.logger = l.logger
	old := l.dispatcher.Swap(d)
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
	d := newQwpConnDispatcher(listener, capacity)
	d.logger = l.logger
	old := l.connDispatcher.Swap(d)
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

// eventAttempt is the AttemptNumber to stamp on a connection event: 0 while the
// sender has not connected once (contract: 0 = initial connect), the reconnect
// counter afterward. Keeps failure/terminal events on the initial async connect
// from reporting a spurious attempt >= 1.
func (l *qwpSfSendLoop) eventAttempt() int64 {
	if !l.connectedOnce.Load() {
		return 0
	}
	return l.reconnectAttempts.Load()
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

// qwpSfSendLoopCloseGrace bounds how long sendLoopClose waits for the I/O
// goroutine to exit after cancelling its context. cancel() unwinds every
// ctx-aware blocking op at once, so a goroutine still alive past this grace is
// wedged in un-cancellable I/O — a disk-backed segment mmap page-fault on hung
// storage. var (not const) so package tests can dial it down.
var qwpSfSendLoopCloseGrace = 5 * time.Second

// sendLoopClose stops the I/O goroutine and waits for it to exit, bounded by
// qwpSfSendLoopCloseGrace so a goroutine wedged in un-cancellable disk I/O
// cannot hang Close forever. Idempotent. Safe to call from any goroutine.
func (l *qwpSfSendLoop) sendLoopClose() error {
	l.running.Store(false)
	l.cancel()
	joined := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(joined)
	}()
	timer := time.NewTimer(qwpSfSendLoopCloseGrace)
	defer timer.Stop()
	select {
	case <-joined:
		// run() exited: its own defers already released the transport, and both
		// inner goroutines were joined before it returned, so reclaiming the
		// remaining resources below cannot race the wire loop.
	case <-timer.C:
		// Wedged in I/O the ctx cannot reach (a disk-backed segment mmap
		// page-fault on hung storage). Abandon rather than hang Close: the
		// goroutine lives until its syscall returns and releases the transport
		// via its own defer, so we must NOT swap the transport out. The
		// dispatchers are still safe to close — offer() on a closed dispatcher is
		// a no-op — which bounds the leak to the single wedged goroutine.
		qwpEffectiveLogger(l.logger).Warn("qwp/sf: send loop still running after close; "+
			"abandoning (wedged in un-cancellable disk I/O)", "grace", qwpSfSendLoopCloseGrace)
		l.closeDispatchers()
		return l.checkErrorOrNil()
	}
	if t := l.transport.Swap(nil); t != nil {
		_ = t.close()
	}
	l.closeDispatchers()
	return l.checkErrorOrNil()
}

// closeDispatchers stops the error, connection, and progress dispatcher
// goroutines. Safe even on the wedged-I/O abandon path: offer() on a closed
// dispatcher is a no-op, so a still-running send loop cannot fault on them.
func (l *qwpSfSendLoop) closeDispatchers() {
	if d := l.dispatcher.Load(); d != nil {
		d.close()
	}
	if d := l.connDispatcher.Load(); d != nil {
		d.close()
	}
	if d := l.progressDispatcher.Load(); d != nil {
		d.close()
	}
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
// SenderError payloads built by the loop (retriable and terminal
// combined).
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

func (l *qwpSfSendLoop) sendLoopTotalDurableAcks() int64 {
	return l.totalDurableAcks.Load()
}

func (l *qwpSfSendLoop) sendLoopTotalDurableTrimAdvances() int64 {
	return l.totalDurableTrimAdvances.Load()
}

// positionCursorForStart sets fsnAtZero, nextWireSeq,
// highestFullySent, and the cursor (sendingSegment + sendOffset) to
// the first unsent FSN. Must be called by the I/O goroutine before it
// starts sending — the producer thread captures the engine's state at
// that moment. Returns a non-nil error if the cursor walk hits a
// corrupt frame header; see positionCursorAt.
func (l *qwpSfSendLoop) positionCursorForStart() error {
	replayStart := l.engine.engineAckedFsn() + 1
	l.highestFullySent.Store(-1)
	l.serverAckedSeq.Store(-1)
	l.framesSentOnConn.Store(0)
	l.acksOnConn.Store(0)
	if l.durableAckMode {
		// A (re)connection re-emits cumulative durable watermarks from scratch,
		// and replay restarts from engineAckedFsn()+1 (the durable watermark), so
		// drop stale per-table watermarks and pending entries.
		l.durable.reset()
	}
	l.setWireBaselineWithCatchUp(replayStart)
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
		// This defer runs after the top-level recover (LIFO), so a panic
		// here would escape it; contain it locally. No close() panic is
		// known — defense in depth on the unwind path.
		defer func() { _ = recover() }()
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
			qwpEffectiveLogger(l.logger).Error("qwp/sf: send loop panicked", "error", err)
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
		if l.reconnectFactory == nil {
			l.recordFatal(err)
			return
		}
		// Classify the retriable-rejection sentinel BEFORE any upgrade
		// sniffing: the sentinel embeds free-form server NACK text and
		// the upgrade helpers fall back to substring matching
		// ("forbidden", "got 401", ...), so a NACK message containing
		// such text would otherwise latch a spurious terminal auth
		// error. The sentinel's strike was already counted at the
		// rejection site in receiverLoop, so its recycle skips the
		// accounting below.
		var retriable *qwpSfRetriableRejection
		rejectionRecycle := errors.As(err, &retriable)
		if !rejectionRecycle {
			if qwpSfIsTerminalUpgradeError(err) {
				se := l.qwpSfBuildUpgradeFailureSE(err)
				l.totalServerErrors.Add(1)
				l.recordFatalServerError(se)
				l.dispatcher.Load().offer(se)
				return
			}
			// WS close codes carry no policy semantics: policy travels only
			// in QWP NACK frames (a server rejecting bytes NACKs before it
			// closes). Every close is therefore a transport event and
			// reconnect-eligible. The one guarded case is a frame that
			// deterministically kills the connection without a NACK (e.g.
			// an intermediary's frame-size limit, or a server build that
			// silently drops our dialect). That is caught behaviorally, not
			// by code list: a non-orderly close arriving after this
			// connection already sent the head frame counts a poison
			// strike against the head FSN; strike-count overflow plus a
			// full episode budget escalate to a typed terminal (see
			// recordRejectionStrike). Orderly closes (NORMAL_CLOSURE
			// role-change handoff, GOING_AWAY restart drain) never count
			// strikes — they are the server asking us to go elsewhere,
			// not a verdict on the bytes.
			code := websocket.CloseStatus(err)
			orderly := code == websocket.StatusNormalClosure ||
				code == websocket.StatusGoingAway
			sentSomething := l.framesSentOnConn.Load() > 0
			if !orderly && sentSomething {
				if l.recordRejectionStrike(l.highestOkAckedFsn() + 1) {
					se := l.buildPoisonedFrameSE(err.Error())
					l.totalServerErrors.Add(1)
					l.emitConn(SenderDisconnected, l.previousIdx, -1, 0, err)
					l.recordFatalServerError(se)
					l.dispatcher.Load().offer(se)
					return
				}
				rejectionRecycle = true
			} else if sentSomething && l.acksOnConn.Load() == 0 {
				// Orderly close (or any close before the first ack) after we
				// sent frames but got nothing back: pace the recycle without a
				// strike, so a server that upgrades and then closes without
				// acking cannot hot-loop dial→replay→close at full rate.
				rejectionRecycle = true
			}
		}
		// The wire dropped; announce the outage before the reconnect walk.
		l.emitConn(SenderDisconnected, l.previousIdx, -1, 0, err)
		// A rejection-driven recycle pays escalating capped equal-jitter
		// backoff before redialing. connectWithBackoff alone cannot pace
		// it: against a reachable server its first dial binds instantly,
		// so a persistent rejection window (e.g. a WRITE_ERROR burst)
		// would otherwise hot-loop send→NACK→reconnect with no growth.
		// An ack covering the retried frame resets the counter.
		if rejectionRecycle {
			pause := qwpSfComputeBackoff(l.rejectionRecycles,
				l.reconnectInitialBackoff, l.reconnectMaxBackoff)
			if !qwpSfSleepInterruptible(l.ctx, nil, pause) {
				return
			}
			l.rejectionRecycles++
		}
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
				qwpEffectiveLogger(l.logger).Error("qwp/sf: connection goroutine panicked", "goroutine", name, "error", err)
				errCh <- loopErr{err}
			}
		}()
		errCh <- loopErr{fn(connCtx)}
	}
	go runGuarded("sender loop", l.senderLoop)
	go runGuarded("receiver loop", l.receiverLoop)
	inner.Wait()
	close(errCh)
	// Both goroutines report exactly one result each. When the server
	// NACKs and then closes, the sender's wire error can land on errCh
	// ahead of the receiver's classified result — surfacing the raw
	// transport error would make run() count a second poison strike for
	// the same rejection (and lose a typed terminal). Prefer the
	// receiver's classification: the retriable-rejection sentinel first,
	// then a typed *SenderError; otherwise keep first-error semantics.
	var first, retriableErr, typedErr error
	for e := range errCh {
		if e.err == nil {
			continue
		}
		if first == nil {
			first = e.err
		}
		var retriable *qwpSfRetriableRejection
		if retriableErr == nil && errors.As(e.err, &retriable) {
			retriableErr = e.err
		}
		var se *SenderError
		if typedErr == nil && errors.As(e.err, &se) {
			typedErr = e.err
		}
	}
	if retriableErr != nil {
		return retriableErr
	}
	if typedErr != nil {
		return typedErr
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
	// Durable keepalive: an idle opted-in client must prod the server, which only
	// flushes pending STATUS_DURABLE_ACK frames on inbound events. A nil channel
	// (disabled) blocks forever, so the case never fires.
	var keepaliveC <-chan time.Time
	var keepalive *time.Timer
	if l.durableAckMode && l.durableKeepaliveInterval > 0 {
		keepalive = time.NewTimer(l.durableKeepaliveInterval)
		defer keepalive.Stop()
		keepaliveC = keepalive.C
	}
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
		if didWork {
			// A real frame already prods the server, so defer the next ping.
			if keepalive != nil {
				drainResetTimer(keepalive, l.durableKeepaliveInterval)
			}
			continue
		}
		// Wake on shutdown, a producer doorbell, the fallback tick, or a
		// keepalive tick.
		drainResetTimer(timer, l.parkInterval)
		select {
		case <-ctx.Done():
			return nil
		case <-l.wakeup:
		case <-timer.C:
		case <-keepaliveC:
			l.sendDurableKeepalive(ctx)
			drainResetTimer(keepalive, l.durableKeepaliveInterval)
		}
	}
}

// drainResetTimer stops t, drains a possibly-fired tick, and re-arms it to d.
func drainResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// sendDurableKeepalive pings the server iff there are pending durable batches, to
// elicit their STATUS_DURABLE_ACK frames. The outbound ping frame is what
// matters (it is the server's recv event); the pong wait is truncated to a
// quarter interval so an idle→active transition doesn't stall the send
// goroutine behind a slow echo.
func (l *qwpSfSendLoop) sendDurableKeepalive(ctx context.Context) {
	if !l.durable.hasPending() {
		return
	}
	tr := l.transport.Load()
	if tr == nil {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, l.durableKeepaliveInterval/4)
	defer cancel()
	_ = tr.ping(pctx)
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
	if l.deltaDictEnabled {
		// Torn-dictionary guard. A delta frame's start id normally never
		// exceeds the dictionary coverage established so far (replayed frames
		// overlap the catch-up dict; fresh frames extend it contiguously). A
		// gap means the persisted dictionary was torn — almost always by a
		// host/power crash that kept the segment frames but lost recently
		// written dictionary entries. Sending would corrupt the table (the
		// server null-pads the missing ids), so fail terminally; the
		// unreplayable data must be resent.
		if start := qwpFrameDeltaStart(payload); start > l.sentDictCount {
			return false, l.qwpSfBuildTornDictSE(start, l.sentDictCount)
		}
	}
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
	if l.deltaDictEnabled {
		// Mirror the symbols this frame introduced so a later reconnect can
		// re-register the whole dictionary. Idempotent on replay: a frame
		// whose delta we already hold advances nothing.
		l.accumulateSentDict(payload)
	}
	// An ACK for this frame may already have landed and been held back
	// while highestFullySent still trailed it; reconcile now that the
	// watermark is published so a quiescent last frame — whose ACK has
	// no later ACK to re-drive it — does not strand its acknowledgement.
	// Durable mode has the same strand (durableDrain is clamped by
	// highestFullySent too, for the same munmap safety), so it gets the
	// same send-side re-drive.
	if l.durableAckMode {
		l.durableDrain()
	} else {
		l.applyAckWatermark()
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
	l.dispatchProgress()
}

// sendLoopSetProgressHandler installs the ack-progress dispatcher. Call once
// before sendLoopStart. A nil handler leaves progress dispatch off. Swap + close
// (like sendLoopSetErrorHandler) so a repeated registration can't leak the old
// dispatcher's goroutine.
func (l *qwpSfSendLoop) sendLoopSetProgressHandler(handler SenderProgressHandler, capacity int) {
	old := l.progressDispatcher.Swap(newQwpProgressDispatcher(handler, capacity))
	if old != nil {
		old.close()
	}
}

// dispatchProgress delivers the engine's acked FSN to the progress handler when
// it has advanced. The lock spans read-compare-store-offer so concurrent
// dispatches from the send and receive goroutines can't reorder two advances
// into a non-monotonic stream (offering outside it could).
func (l *qwpSfSendLoop) dispatchProgress() {
	d := l.progressDispatcher.Load()
	if d == nil {
		return
	}
	l.progressDispatchMu.Lock()
	defer l.progressDispatchMu.Unlock()
	fsn := l.engine.engineAckedFsn()
	if fsn <= l.lastProgressFsn {
		return
	}
	l.lastProgressFsn = fsn
	d.offer(fsn)
}

// sendLoopSetMaxFrameRejections overrides the poison-frame detector's
// strike threshold (max_frame_rejections). Non-positive keeps the
// default. Must be called before sendLoopStart.
func (l *qwpSfSendLoop) sendLoopSetMaxFrameRejections(n int) {
	if n > 0 {
		l.maxFrameRejections = n
	}
}

// sendLoopSetOnRoundExhausted installs a hook fired once per exhausted
// reconnect sweep (a full address-list walk that bound no host) with
// the sweep's aggregate outcome, in addition to the loop's own
// SenderAllEndpointsUnreachable event and throttled WARN. A drainer
// uses it to run its per-sweep durable-ack settle accounting during
// mid-drain reconnect windows. Must be called before sendLoopStart;
// pass nil to clear.
func (l *qwpSfSendLoop) sendLoopSetOnRoundExhausted(fn func(qwpSfSweepOutcome)) {
	l.onRoundExhausted = fn
}

// sendLoopSetBackgroundWalks marks this loop's reconnect walks as
// background: attempted hosts are tracked in a walk-local cursor
// instead of the shared tracker's round slots (see qwpRoundCursor).
// Set by drainers, whose walks run concurrently with the foreground
// sender's. Must be called before sendLoopStart.
func (l *qwpSfSendLoop) sendLoopSetBackgroundWalks(background bool) {
	l.backgroundWalks = background
}

// sendLoopSetDurableAck enables durable-ack mode and installs the tracker. Call
// once before sendLoopStart. keepalive <= 0 disables the durable keepalive ping.
// mismatchTerminal is true for a foreground sender (a durable-ack mismatch is
// terminal) and false for a drainer (it retries within the reconnect budget).
func (l *qwpSfSendLoop) sendLoopSetDurableAck(enabled bool, keepalive time.Duration, mismatchTerminal bool) {
	l.durableAckMode = enabled
	l.durableKeepaliveInterval = keepalive
	l.durableMismatchTerminal = mismatchTerminal
	if enabled {
		l.durable = newQwpDurableTracker()
	}
}

// durableOnOk stashes an OK ACK as a pending entry (durable mode) instead of
// advancing the engine.
//
// Load-bearing invariant: the server sends exactly one OK ACK per frame,
// carrying that frame's per-table seqTxn trailer. Per-entry trim in durableDrain
// relies on it — a server that coalesced OK ACKs (a cumulative seq skipping
// frames, with a trailer omitting the skipped tables) would leave the skipped
// frames untracked and let the drain advance past not-yet-durable data. Rather
// than trust the contract silently, seqGap detects the forward jump and this
// returns a terminal PROTOCOL_VIOLATION so the sender HALTs (fail-closed) before
// any un-durable frame is trimmed; close+rebuild then replays from the durable
// watermark. Matches the server's per-frame ACK contract; the non-durable
// path is immune (it ignores the trailer entirely). Returns nil when the ACK is
// well-formed.
func (l *qwpSfSendLoop) durableOnOk(seq int64, tail []byte) *SenderError {
	assigned := l.nextWireSeq.Load() - 1
	if assigned < 0 {
		return nil // ACK before any send
	}
	if seq < 0 {
		return nil // OK acks name a real wire sequence; -1 is the NACK-only no-seq sentinel
	}
	// An OK ack naming a wire sequence beyond the highest frame we sent is a
	// protocol violation: the server cannot commit a frame it never received.
	// Checked before seqGap so a forged value neither raises the gap-detection
	// floor nor gets enqueued with this frame's trailer under a clamped wireSeq
	// (which could trim a real, not-yet-durable frame against the wrong table set).
	if seq > assigned {
		return l.qwpSfBuildDurableOverrunSE(seq, assigned)
	}
	if expected, gap := l.durable.seqGap(seq); gap {
		return l.qwpSfBuildDurableGapSE(seq, expected)
	}
	if !l.durable.enqueueOk(seq, tail) {
		return l.qwpSfBuildDurableCapSE(seq)
	}
	l.durableDrain()
	return nil
}

// durableUnackedSpan is the [ackedFsn+1, publishedFsn] window a terminal
// durable-ack fault attributes as lost.
func (l *qwpSfSendLoop) durableUnackedSpan() (from, to int64) {
	from = l.engine.engineAckedFsn() + 1
	to = l.engine.enginePublishedFsn()
	if to < from {
		to = from
	}
	return from, to
}

// qwpSfBuildDurableGapSE builds the terminal PROTOCOL_VIOLATION surfaced when
// the durable OK-ack sequence jumps forward (durableOnOk).
func (l *qwpSfSendLoop) qwpSfBuildDurableGapSE(gotSeq, expectedSeq int64) *SenderError {
	from, to := l.durableUnackedSpan()
	return &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyTerminal,
		ServerStatusByte: NoStatusByte,
		ServerMessage: fmt.Sprintf("durable-ack OK sequence gap: got %d, expected %d — server "+
			"coalesced or dropped an OK ack, leaving frames untracked for durable trimming", gotSeq, expectedSeq),
		MessageSequence: gotSeq,
		FromFsn:         from,
		ToFsn:           to,
		DetectedAt:      time.Now(),
	}
}

// qwpSfBuildDurableOverrunSE builds the terminal PROTOCOL_VIOLATION surfaced when
// an OK ack names a wire sequence beyond the highest frame sent — the server
// acknowledged a frame it could not have received.
func (l *qwpSfSendLoop) qwpSfBuildDurableOverrunSE(gotSeq, assigned int64) *SenderError {
	from, to := l.durableUnackedSpan()
	return &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyTerminal,
		ServerStatusByte: NoStatusByte,
		ServerMessage: fmt.Sprintf("durable-ack OK sequence %d exceeds highest sent %d — server "+
			"acked a frame that was never delivered", gotSeq, assigned),
		MessageSequence: gotSeq,
		FromFsn:         from,
		ToFsn:           to,
		DetectedAt:      time.Now(),
	}
}

// qwpSfBuildDurableCapSE builds the terminal PROTOCOL_VIOLATION surfaced when a
// server streams more distinct table names than qwpDurableMaxTrackedTables, so
// the tracker can no longer intern them and would otherwise trim on a truncated
// table set.
func (l *qwpSfSendLoop) qwpSfBuildDurableCapSE(seq int64) *SenderError {
	from, to := l.durableUnackedSpan()
	return &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyTerminal,
		ServerStatusByte: NoStatusByte,
		ServerMessage: fmt.Sprintf("durable-ack tracked-table cap (%d) exceeded at wire seq %d — "+
			"server streamed more distinct table names than any real workload", qwpDurableMaxTrackedTables, seq),
		MessageSequence: seq,
		FromFsn:         from,
		ToFsn:           to,
		DetectedAt:      time.Now(),
	}
}

// durableDrain releases every pending batch now covered by the durable
// watermarks and advances the engine to the highest such wire sequence. The
// drain is capped at highestFullySent — the same munmap-safety clamp the
// non-durable path applies in applyAckWatermark — so a forged/early durable ack
// cannot advance the trim watermark onto a frame the send goroutine is still
// reading out of an mmap'd segment.
func (l *qwpSfSendLoop) durableDrain() {
	if wireSeq := l.durable.drainUpTo(l.highestFullySent.Load()); wireSeq >= 0 {
		l.engine.engineAcknowledge(l.fsnAtZero.Load() + wireSeq)
		l.totalDurableTrimAdvances.Add(1)
		l.dispatchProgress()
	}
}

// qwpSfRetriableRejection is the sentinel error a retriable server NACK
// returns from receiverLoop: run() must recycle the connection (reconnect +
// replay from ackedFsn+1) WITHOUT counting a second poison strike — the
// receiver already recorded one at the rejection site.
type qwpSfRetriableRejection struct{ msg string }

func (e *qwpSfRetriableRejection) Error() string { return e.msg }

// highestOkAckedFsn is the FSN of the last frame the server OK-acked on the
// current connection (the baseline when none yet). The poison detector keys its
// head-of-line anchor on this + 1. In durable mode the engine watermark tracks
// durable uploads, not OK acks, so a reconnect replays OK'd-but-not-yet-durable
// frames; anchoring on the durable watermark would let those replay-OKs reset
// the detector — and the recycle backoff — every cycle, defeating both. The
// durable tracker's OK high-water is used instead, clamped to what was actually
// sent so a forged ahead-of-send ack cannot push the anchor past a real frame.
func (l *qwpSfSendLoop) highestOkAckedFsn() int64 {
	if !l.durableAckMode {
		return l.engine.engineAckedFsn()
	}
	last := l.durable.lastOkSeq()
	if assigned := l.nextWireSeq.Load() - 1; last > assigned {
		last = assigned
	}
	return l.fsnAtZero.Load() + last
}

// recordRejectionStrike records a server-active rejection (retriable NACK, or
// non-orderly close after at least one send on this connection) against fsn —
// the NACKed frame when the server named a wire sequence, the head-of-line
// frame otherwise. Returns true when the frame is deemed poisoned: the same
// FSN has been rejected maxFrameRejections consecutive times with no covering
// ack, AND the episode has lasted at least reconnectMaxDuration. The duration
// floor keeps a transient server-side rejection window (e.g. a WRITE_ERROR
// burst during recovery) from burning every strike in a second and killing
// the sender; below it the loop keeps recycling with paced backoff — a loud
// stall, never a terminal, never a drop.
func (l *qwpSfSendLoop) recordRejectionStrike(fsn int64) bool {
	if fsn != l.poisonFsn {
		l.poisonFsn = fsn
		l.poisonStrikes = 1
		l.poisonFirstStrikeAt = time.Now()
	} else {
		l.poisonStrikes++
	}
	return l.poisonStrikes >= l.maxFrameRejections &&
		time.Since(l.poisonFirstStrikeAt) >= l.reconnectMaxDuration
}

// clearPoisonUpTo resets the poison detector once an ack covers the
// suspected frame. A lower ack (durable-mode replay re-OK-ing frames
// behind the poisoned one) leaves the episode intact so fresh OKs for
// predecessors cannot indefinitely defer escalation. Receiver-goroutine
// only.
func (l *qwpSfSendLoop) clearPoisonUpTo(ackFsn int64) {
	if l.poisonFsn >= 0 && ackFsn >= l.poisonFsn {
		l.poisonFsn = -1
		l.poisonStrikes = 0
	}
}

// noteAckProgress applies an OK ack's forward progress to both the poison
// detector and the rejection-recycle pacing. It resets the recycle backoff
// only when the ack advances to or past the frame the loop has been
// retrying: a durable-mode reconnect replays OK'd-but-not-durable
// predecessors (ackFsn < poisonFsn) ahead of the rejected frame, and letting
// those replay-OKs reset the pacing would pin the backoff at its initial
// value and hot-loop the recycle. Receiver-goroutine only.
func (l *qwpSfSendLoop) noteAckProgress(ackFsn int64) {
	if l.poisonFsn < 0 || ackFsn >= l.poisonFsn {
		l.rejectionRecycles = 0
	}
	l.clearPoisonUpTo(ackFsn)
}

// buildPoisonedFrameSE builds the typed terminal for a poisoned frame: the
// server (or an intermediary) has deterministically rejected the same frame
// maxFrameRejections consecutive times across a full episode budget with no
// covering ack, so replaying it cannot succeed and retrying would loop
// forever. The rejected bytes remain in the SF log on disk — nothing is
// silently discarded.
func (l *qwpSfSendLoop) buildPoisonedFrameSE(lastRejection string) *SenderError {
	from := l.engine.engineAckedFsn() + 1
	to := l.engine.enginePublishedFsn()
	if to < from {
		to = from
	}
	return &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyTerminal,
		ServerStatusByte: NoStatusByte,
		ServerMessage: fmt.Sprintf("frame at fsn=%d rejected %d consecutive times with "+
			"no ack progress — poisoned frame, replay cannot succeed (last: %s)",
			l.poisonFsn, l.poisonStrikes, lastRejection),
		MessageSequence: NoMessageSequence,
		FromFsn:         from,
		ToFsn:           to,
		DetectedAt:      time.Now(),
	}
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
			// Per-table durable-upload watermark. In durable-ack mode it
			// advances the trim/replay/await watermark; otherwise it is an
			// informational frame the server must not have sent — ignore.
			if l.durableAckMode {
				l.totalDurableAcks.Add(1)
				l.durable.applyDurable(data[qwpAckTablesOffset(status):])
				l.durableDrain()
			} else if l.warnedStrayDurable.CompareAndSwap(false, true) {
				qwpEffectiveLogger(l.logger).Warn("qwp/sf: received STATUS_DURABLE_ACK frame without opt-in — ignoring")
			}
			continue
		}
		seq := parseAckSequence(data)
		if status != QwpStatusOK {
			// Application-layer rejection by the server. Classify the
			// status byte, resolve the policy, surface a typed
			// SenderError. TERMINAL latches and exits the receiver
			// loop; RETRIABLE / RETRIABLE_OTHER recycle the connection
			// — the bytes stay in the SF log and the reconnect path
			// replays from ackedFsn+1. A rejection NEVER advances the
			// watermark: there is no drop policy.
			//
			// FSN attribution only: the wireSeq clamp bounds the FSN
			// named in the error report; the reported MessageSequence
			// is the raw server-sent seq so it round-trips verbatim
			// against server-side logs.
			highestSent := l.highestFullySent.Load()
			_, _, msg := parseAckErrorPayload(data)
			cat := qwpSfClassify(status)
			pol := l.policyResolver.Load().resolve(cat)
			// from/to bound the reported (informational) span; anchor is the
			// frame a poison strike keys on. They coincide for a server-named
			// wireSeq but differ head-of-line: the report spans the whole
			// unacked replay window while the strike keys on the first
			// not-yet-OK'd frame so replay re-OKs of predecessors cannot
			// restart the episode.
			var from, to, anchor int64
			if highestSent < 0 || seq < 0 {
				// Pre-send rejection (no frame has finished sending on
				// this connection, so the server-named wireSeq does not
				// correspond to any frame we delivered), or a NACK
				// carrying the legal -1 no-sequence sentinel.
				from = l.engine.engineAckedFsn() + 1
				to = l.engine.enginePublishedFsn()
				if to < from {
					to = from
				}
				anchor = l.highestOkAckedFsn() + 1
			} else {
				cappedSeq := seq
				if cappedSeq > highestSent {
					cappedSeq = highestSent
				}
				fsn := l.fsnAtZero.Load() + cappedSeq
				from, to, anchor = fsn, fsn, fsn
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
			if pol == PolicyTerminal {
				// Latch BEFORE dispatching: a handler that
				// synchronously calls Flush / sendLoopCheckError
				// must observe the typed terminal error. See
				// qwp-cursor-error-api.md §120.
				l.recordFatalServerError(se)
				l.dispatcher.Load().offer(se)
				return se
			}
			// RETRIABLE / RETRIABLE_OTHER: informational dispatch, then
			// recycle the wire through the same reconnect machinery a
			// transport failure uses. A pre-send rejection cannot
			// implicate the head frame (nothing was sent on this
			// connection), so it never counts a poison strike. Strikes
			// key on `from`: the NACKed frame when attributable, the
			// head-of-line frame otherwise — in durable mode replay
			// re-sends already-OK'd predecessors ahead of the rejected
			// frame, so head-of-line keying would restart the episode on
			// every recycle.
			l.dispatcher.Load().offer(se)
			if highestSent >= 0 && l.recordRejectionStrike(anchor) {
				poison := l.buildPoisonedFrameSE(fmt.Sprintf(
					"server NACK status=0x%02X (%s): %s", byte(status), cat, msg))
				l.totalServerErrors.Add(1)
				l.recordFatalServerError(poison)
				l.dispatcher.Load().offer(poison)
				return poison
			}
			// The rejection is already dispatched as a *SenderError above (and
			// logged by the default handler when the user registered none), so
			// this recycle trace is Debug: hidden by default, available for
			// deep debugging without double-logging every retriable NACK.
			qwpEffectiveLogger(l.logger).Debug("qwp/sf: server rejected frame — recycling connection",
				"seq", seq, "category", cat, "policy", pol,
				"status", byte(status), "replayFromFsn", l.engine.engineAckedFsn()+1)
			return &qwpSfRetriableRejection{msg: fmt.Sprintf(
				"server NACK (%s, %s): %s", cat, pol, msg)}
		}
		// Clear poison suspicion and reset the rejection-recycle pacing
		// only once the acked FSN covers the frame the loop is retrying
		// (noteAckProgress) — in durable mode replay re-delivers already-
		// OK'd predecessors ahead of the rejected frame, whose fresh OKs
		// must neither shield a later poisoned frame nor reset the recycle
		// backoff into a ~initial-backoff hot loop.
		l.totalAcks.Add(1)
		l.acksOnConn.Add(1)
		if l.durableAckMode {
			// Durable mode: stash the OK and wait for STATUS_DURABLE_ACK to
			// trim. serverAckedSeq is deliberately not recorded here — the
			// durable tracker owns advancement and reconnect targets
			// engineAckedFsn()+1, so the non-durable watermark (whose only
			// reader, applyAckWatermark, is guarded off below) would be dead
			// state. A sequence gap HALTs fail-closed (latch before dispatch so a
			// handler that synchronously calls Flush observes the typed error).
			if se := l.durableOnOk(seq, data[qwpAckTablesOffset(QwpStatusOK):]); se != nil {
				l.totalServerErrors.Add(1)
				l.recordFatalServerError(se)
				l.dispatcher.Load().offer(se)
				return se
			}
			// Key progress on the highest OK-acked frame (clamped to what was
			// sent), not the raw server seq: a forged ahead-of-send ack must
			// not clear the poison detector for frames never delivered.
			l.noteAckProgress(l.highestOkAckedFsn())
		} else {
			// Record the server's cumulative ACK sequence, then reconcile it
			// against highestFullySent. applyAckWatermark caps the advance at
			// the last fully-sent frame, so a malformed, early, or forged
			// server response can never move ackedFsn over an in-flight frame
			// (a trim would munmap a segment the I/O thread is still reading)
			// nor over a frame a wire failure dropped before delivery. The
			// matching call on the send side re-drives the same reconciliation
			// so an ACK that arrives before its frame's send completes is not
			// stranded when no later ACK follows it.
			//
			// Cap the recorded sequence at the highest assigned wire seq: the raw
			// value persists and is re-applied on the send side after every send,
			// so an unclamped ahead-of-send ack would advance ackedFsn as each
			// later frame is put on the wire — before the server ever acks it —
			// and a drop in that window would replay past the lost frames. A
			// compliant server only acks what it received (seq <= assigned), so
			// this is inert for it and defends against a forged/buggy one, mirroring
			// durableOnOk's clamp.
			if assigned := l.nextWireSeq.Load() - 1; seq > assigned {
				seq = assigned
			}
			l.serverAckedSeq.Store(seq)
			l.applyAckWatermark()
			l.noteAckProgress(l.engine.engineAckedFsn())
		}
	}
}

// connectWithBackoff runs the failover.md §13.6 round-walk through
// qwpSfRunRoundWalk in unbounded mode: each iteration demotes a
// just-failed host (previousIdx), picks the highest-priority
// unattempted endpoint, dials it, and classifies the outcome.
// Round-boundary sleep pays capped equal-jitter exponential backoff
// for every round shape — role-reject rounds included. The walk
// retries until success, a genuine terminal (auth, non-421 upgrade
// reject, durable-ack mismatch), or shutdown; it never gives up on a
// wall clock (Invariant B: reconnect_max_duration_millis bounds only
// the blocking sync initial connect). Returns true on a successful
// bind (caller resumes the pump loop), false on terminal failure /
// shutdown.
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
	var lastWarnAt time.Time
	params := qwpSfRoundWalkParams{
		Factory: l.reconnectFactory,
		Tracker: l.tracker,
		// Unbounded (Invariant B): the running loop and async initial
		// connect retry until success, terminal, or shutdown.
		MaxDuration:    0,
		InitialBackoff: l.reconnectInitialBackoff,
		MaxBackoff:     l.reconnectMaxBackoff,
		OnAttempt: func() {
			l.reconnectAttempts.Add(1)
			l.totalReconnectAttempts.Add(1)
		},
		OnEndpointFailed: func(idx int, derr error) {
			l.emitConn(SenderEndpointAttemptFailed, idx, -1, l.eventAttempt(), derr)
		},
		OnRoundExhausted: func(outcome qwpSfSweepOutcome) {
			round++
			l.connDispatcher.Load().offer(&SenderConnectionEvent{
				Kind:            SenderAllEndpointsUnreachable,
				AttemptNumber:   l.eventAttempt(),
				RoundNumber:     round,
				TimestampMillis: time.Now().UnixMilli(),
			})
			// The all-endpoints-unreachable transition is already dispatched as a
			// SenderConnectionEvent above (and logged by the default listener when
			// the user registered none), so this is a throttled Debug trace that
			// adds only the replica-only-vs-no-endpoint reason — hidden by default,
			// available for deep debugging without shadowing the event.
			if now := time.Now(); now.Sub(lastWarnAt) >= qwpSfReconnectWarnThrottle {
				lastWarnAt = now
				reason := "no endpoint reachable"
				if outcome.allReplica() {
					reason = "every reachable endpoint is a replica (transient failover window)"
				}
				qwpEffectiveLogger(l.logger).Debug("qwp/sf: reconnect round exhausted; retrying with capped backoff",
					"phase", phase, "round", round, "reason", reason)
			}
			if fn := l.onRoundExhausted; fn != nil {
				fn(outcome)
			}
		},
		DurableMismatchTerminal: l.durableMismatchTerminal,
		Background:              l.backgroundWalks,
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
		// producer learns via its next API call. Only an
		// actual auth reject maps to SenderAuthFailed; other terminal upgrade
		// failures (e.g. a durable-ack mismatch) surface via the typed
		// PROTOCOL_VIOLATION SenderError only.
		if qwpSfIsAuthFailure(result.Terminal) {
			l.emitConn(SenderAuthFailed, -1, -1, l.eventAttempt(), result.Terminal)
		}
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
	// Unreachable: the walk runs unbounded, so it exits only via the
	// three branches above. `initial` is retained in the message for
	// the defensive latch.
	l.recordFatal(fmt.Errorf("%s: round-walk returned no result (entry error: %v)",
		phase, initial))
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
	l.highestFullySent.Store(-1)
	l.serverAckedSeq.Store(-1)
	l.framesSentOnConn.Store(0)
	l.acksOnConn.Store(0)
	if l.durableAckMode {
		// A (re)connection re-emits cumulative durable watermarks from scratch,
		// and replay restarts from engineAckedFsn()+1 (the durable watermark), so
		// drop stale per-table watermarks and pending entries.
		l.durable.reset()
	}
	l.setWireBaselineWithCatchUp(replayStart)
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

// setWireBaselineWithCatchUp sets the wire-sequence baseline for a fresh
// connection. When the sent-dict mirror is non-empty it first emits a
// full-dictionary catch-up frame (or several, under a small batch cap) so the
// fresh server — whose dictionary starts empty — can resolve the
// non-self-sufficient delta frames that replay next. The catch-up frames
// occupy wire seqs 0..k-1, which map to already-acked FSNs (harmless
// re-acks), so the first real replay frame still lands on replayStart.
//
// Caller must have reset highestFullySent / serverAckedSeq / framesSentOnConn
// / acksOnConn and the durable tracker first. Runs on the goroutine that owns
// the (re)connect (no send goroutine alive), so it is the sole socket writer.
// A catch-up send failure means the just-bound transport is already broken;
// that is not fatal — reset to the plain baseline so the first real send
// surfaces the wire failure and run()'s reconnect redoes the catch-up.
func (l *qwpSfSendLoop) setWireBaselineWithCatchUp(replayStart int64) {
	transport := l.transport.Load()
	if transport != nil && l.deltaDictEnabled && l.sentDictCount > 0 {
		l.nextWireSeq.Store(0)
		if k, err := l.sendDictCatchUp(l.ctx, transport); err == nil {
			l.fsnAtZero.Store(replayStart - int64(k))
			return
		}
		l.highestFullySent.Store(-1)
	}
	l.fsnAtZero.Store(replayStart)
	l.nextWireSeq.Store(0)
}

// sendDictCatchUp re-registers the whole sent dictionary on a freshly
// connected server, split into as many table-less frames as the server's
// batch cap requires so no single frame exceeds it. Each chunk carries a
// contiguous id range, in order, so the server accumulates them exactly as it
// would the original per-frame deltas. Returns the number of frames sent (each
// consumed a wire sequence). A too-large single entry goes in its own frame
// and, if truly over the wire cap, surfaces as a server close handled by the
// reconnect/poison machinery.
func (l *qwpSfSendLoop) sendDictCatchUp(ctx context.Context, transport *qwpTransport) (int, error) {
	budget := l.catchUpBudget(transport)
	framesSent := 0
	chunkStartId := 0
	chunkStart := 0 // byte offset of the chunk's first entry in sentDictBytes
	chunkSymbols := 0
	chunkBytes := 0
	for p := 0; p < len(l.sentDictBytes); {
		entryStart := p
		el, n, err := qwpReadVarint(l.sentDictBytes[p:])
		if err != nil {
			break // malformed mirror — never happens for frames we built
		}
		entryEnd := p + n + int(el)
		if entryEnd > len(l.sentDictBytes) {
			break
		}
		entryBytes := entryEnd - entryStart
		if chunkSymbols > 0 && chunkBytes+entryBytes > budget {
			if err := l.sendCatchUpChunk(ctx, transport, chunkStartId, chunkSymbols,
				l.sentDictBytes[chunkStart:chunkStart+chunkBytes]); err != nil {
				return framesSent, err
			}
			framesSent++
			chunkStartId += chunkSymbols
			chunkStart, chunkSymbols, chunkBytes = entryStart, 0, 0
		}
		chunkSymbols++
		chunkBytes += entryBytes
		p = entryEnd
	}
	if chunkSymbols > 0 {
		if err := l.sendCatchUpChunk(ctx, transport, chunkStartId, chunkSymbols,
			l.sentDictBytes[chunkStart:chunkStart+chunkBytes]); err != nil {
			return framesSent, err
		}
		framesSent++
	}
	return framesSent, nil
}

// sendCatchUpChunk sends one table-less frame carrying dictionary ids
// [deltaStart .. deltaStart+deltaCount). It consumes a wire sequence and
// advances the fully-sent ceiling — safe because the catch-up FSNs are
// already acked and non-segment-backed — but does NOT count toward the
// poison-strike gate (framesSentOnConn), so a drop right after catch-up,
// before any real frame, strikes nothing.
func (l *qwpSfSendLoop) sendCatchUpChunk(ctx context.Context, transport *qwpTransport, deltaStart, deltaCount int, symbols []byte) error {
	frame := l.buildCatchUpFrame(deltaStart, deltaCount, symbols)
	wireSeq := l.nextWireSeq.Load()
	if err := transport.sendMessage(ctx, frame); err != nil {
		return err
	}
	l.nextWireSeq.Store(wireSeq + 1)
	l.highestFullySent.Store(wireSeq)
	l.totalFramesSent.Add(1)
	return nil
}

// buildCatchUpFrame assembles a table-less QWP message into the reusable
// catchUpBuf: the 12-byte header (tableCount=0), the delta start/count
// varints, then the symbol bytes verbatim.
func (l *qwpSfSendLoop) buildCatchUpFrame(deltaStart, deltaCount int, symbols []byte) []byte {
	buf := l.catchUpBuf[:0]
	var hdr [qwpHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], qwpMagic)
	hdr[4] = qwpVersion
	hdr[qwpHeaderOffsetFlags] = qwpFlagDeltaSymbolDict | qwpFlagGorilla
	buf = append(buf, hdr[:]...)
	var vb [qwpMaxVarintLen]byte
	buf = append(buf, vb[:qwpPutVarint(vb[:], uint64(deltaStart))]...)
	buf = append(buf, vb[:qwpPutVarint(vb[:], uint64(deltaCount))]...)
	buf = append(buf, symbols...)
	binary.LittleEndian.PutUint32(buf[qwpHeaderOffsetPayloadLen:qwpHeaderOffsetPayloadLen+4],
		uint32(len(buf)-qwpHeaderSize))
	l.catchUpBuf = buf
	return buf
}

// catchUpBudget is the symbol-bytes budget per catch-up frame: the server's
// advertised batch cap (or the protocol hard cap when none is advertised),
// less the header and the two delta-section varints.
func (l *qwpSfSendLoop) catchUpBudget(transport *qwpTransport) int {
	capBytes := int(transport.serverMaxBatchSize)
	if capBytes <= 0 {
		capBytes = qwpMaxBatchSize
	}
	if b := capBytes - qwpHeaderSize - 16; b > 1 {
		return b
	}
	return 1
}

// accumulateSentDict extends the sent-dict mirror with the symbols a just-sent
// frame introduced. Only a delta that extends exactly from the current tip
// adds symbols; a replayed or empty-delta frame is skipped (idempotent on
// replay).
func (l *qwpSfSendLoop) accumulateSentDict(payload []byte) {
	deltaStart, deltaCount, symbols, ok := qwpParseDeltaDict(payload)
	if !ok || deltaCount <= 0 || deltaStart != l.sentDictCount {
		return
	}
	l.sentDictBytes = append(l.sentDictBytes, symbols...)
	l.sentDictCount += deltaCount
}

// seedSentDictFromPersisted seeds the mirror from a recovered slot's persisted
// dictionary so the first connection re-registers the whole dictionary before
// replaying the recovered delta frames. No-op in memory mode.
func (l *qwpSfSendLoop) seedSentDictFromPersisted(pd *qwpSfSymbolDict) {
	if pd == nil {
		return
	}
	symbols := pd.loadedSymbols()
	for _, name := range symbols {
		var vb [qwpMaxVarintLen]byte
		l.sentDictBytes = append(l.sentDictBytes, vb[:qwpPutVarint(vb[:], uint64(len(name)))]...)
		l.sentDictBytes = append(l.sentDictBytes, name...)
	}
	l.sentDictCount = len(symbols)
}

// qwpSfBuildTornDictSE builds the terminal PROTOCOL_VIOLATION surfaced when a
// recovered delta frame references ids beyond the recovered dictionary — a
// host-crash tear caught before it can corrupt the target table.
func (l *qwpSfSendLoop) qwpSfBuildTornDictSE(deltaStart, dictSize int) *SenderError {
	from := l.engine.engineAckedFsn() + 1
	to := l.engine.enginePublishedFsn()
	if to < from {
		to = from
	}
	return &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyTerminal,
		ServerStatusByte: NoStatusByte,
		ServerMessage: fmt.Sprintf("recovered store-and-forward symbol dictionary is incomplete "+
			"(likely a host crash): frame delta start %d exceeds recovered dictionary size %d; "+
			"cannot replay without corrupting data — resend required", deltaStart, dictSize),
		FromFsn:    from,
		ToFsn:      to,
		DetectedAt: time.Now(),
	}
}

// qwpIsDeltaFrame reports whether msg is a well-formed QWP message carrying a
// delta symbol-dict section.
func qwpIsDeltaFrame(msg []byte) bool {
	return len(msg) >= qwpHeaderSize &&
		binary.LittleEndian.Uint32(msg[:4]) == qwpMagic &&
		msg[qwpHeaderOffsetFlags]&qwpFlagDeltaSymbolDict != 0
}

// qwpFrameDeltaStart returns a QWP message's symbol-dict delta start id, or -1
// when it carries no delta section (used by the torn-dict guard).
func qwpFrameDeltaStart(msg []byte) int {
	if !qwpIsDeltaFrame(msg) {
		return -1
	}
	start, _, err := qwpReadVarint(msg[qwpHeaderSize:])
	if err != nil {
		return -1
	}
	return int(start)
}

// qwpParseDeltaDict returns a QWP message's delta section — the start id, the
// symbol count, and the raw [len varint][utf8] symbol bytes (excluding any
// table blocks that follow). ok is false for a non-delta or malformed message.
func qwpParseDeltaDict(msg []byte) (deltaStart, deltaCount int, symbols []byte, ok bool) {
	if !qwpIsDeltaFrame(msg) {
		return 0, 0, nil, false
	}
	p := qwpHeaderSize
	start, n, err := qwpReadVarint(msg[p:])
	if err != nil {
		return 0, 0, nil, false
	}
	p += n
	count, n, err := qwpReadVarint(msg[p:])
	if err != nil {
		return 0, 0, nil, false
	}
	p += n
	region := p
	for i := uint64(0); i < count; i++ {
		el, m, verr := qwpReadVarint(msg[p:])
		if verr != nil {
			return 0, 0, nil, false
		}
		p += m + int(el)
		if p > len(msg) {
			return 0, 0, nil, false
		}
	}
	return int(start), int(count), msg[region:p], true
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
	return qwpSfUpgradeFailureSE(l.engine.engineAckedFsn()+1, l.engine.enginePublishedFsn(), err)
}

// qwpSfUpgradeFailureSE is the engine-independent core of the above so the
// initial-connect (InitialConnectOff / InitialConnectSync) paths — which fail
// before a send loop exists — can synthesize the SAME typed *SenderError the
// async reconnect path latches, keeping the WithRequestDurableAck /
// QwpDurableAckMismatchError PROTOCOL_VIOLATION contract uniform across all
// three connect modes. err is preserved as the SenderError cause (Unwrap) so
// errors.As still reaches the typed transport error. [from,to] is the unacked
// FSN span (empty at initial connect).
func qwpSfUpgradeFailureSE(from, to int64, err error) *SenderError {
	cat := CategoryProtocolViolation
	if qwpSfIsAuthFailure(err) {
		cat = CategorySecurityError
	}
	if to < from {
		to = from
	}
	return &SenderError{
		Category:         cat,
		AppliedPolicy:    PolicyTerminal,
		ServerStatusByte: NoStatusByte,
		ServerMessage:    "ws-upgrade-failed: " + err.Error(),
		MessageSequence:  NoMessageSequence,
		FromFsn:          from,
		ToFsn:            to,
		DetectedAt:       time.Now(),
		cause:            err,
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
	durableMismatchTerminal bool,
	onEndpointFailed func(idx int, err error),
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
		Factory:                 factory,
		Tracker:                 tracker,
		MaxDuration:             maxDuration,
		InitialBackoff:          initialBackoff,
		MaxBackoff:              maxBackoff,
		OnEndpointFailed:        onEndpointFailed,
		DurableMismatchTerminal: durableMismatchTerminal,
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
