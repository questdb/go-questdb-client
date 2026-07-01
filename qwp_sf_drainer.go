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
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// qwpSfDrainOutcome is the terminal state of a drainer's run.
type qwpSfDrainOutcome int32

const (
	qwpSfDrainOutcomePending qwpSfDrainOutcome = iota
	qwpSfDrainOutcomeLockedByOther
	qwpSfDrainOutcomeSuccess
	qwpSfDrainOutcomeFailed
	qwpSfDrainOutcomeStopped
)

// qwpMaxDurableAckMismatchAttempts bounds how many times a durable-ack drainer
// retries an endpoint that does not advertise durable-ack before quarantining
// the slot with a .failed sentinel. Matches Java's
// DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS.
const qwpMaxDurableAckMismatchAttempts = 16

// QwpBackgroundDrainerListener receives durable-ack drain outcomes for a crashed
// sibling's store-and-forward slot. Both callbacks run on the drainer goroutine
// and must not block. Either may be nil. Applied to every drainer via
// WithBackgroundDrainerListener.
type QwpBackgroundDrainerListener struct {
	// OnDurableAckUnavailable fires each time a durable-ack drainer dials an
	// endpoint that does not advertise durable-ack. attempt is the cumulative
	// mismatch count; the drainer keeps retrying (its source is pinned on disk).
	OnDurableAckUnavailable func(dir string, attempt int)
	// OnDurableAckPersistentFailure fires once when a drainer gives up after
	// repeated durable-ack mismatches and quarantines the slot (.failed).
	OnDurableAckPersistentFailure func(dir string, attempts int, elapsed time.Duration)
}

// qwpSfDrainerPollInterval is how often the drainer wakes to
// re-check whether the slot is fully drained.
const qwpSfDrainerPollInterval = 50 * time.Millisecond

// qwpSfDrainerPoolCloseGrace bounds how long the pool's close()
// waits for active drainers to exit cleanly before cancelling the
// pool's master ctx to forcibly unwind blocking dials. Mirrors the
// Java 3-second grace. var (not const) so package tests can dial
// it down without paying the full 3 s.
var qwpSfDrainerPoolCloseGrace = 3 * time.Second

// qwpSfDrainerPoolHardCloseGrace bounds how long the pool's close()
// waits AFTER cancelling the master ctx. Cancellation unwinds
// ctx-aware blocking (TCP dials, the drainer poll loop); a drainer
// still alive past this second grace is wedged in I/O the ctx cannot
// reach — drainerRun's engine-open phase (flock, mmap, full CRC scan
// of a possibly-huge slot, hung NFS) makes no ctx checks. Such a
// drainer is abandoned rather than blocking close() on un-cancellable
// I/O; the slot it holds stays a valid orphan for a future sender to
// re-adopt. var (not const) so package tests can dial it down.
var qwpSfDrainerPoolHardCloseGrace = 1 * time.Second

// qwpSfOrphanDrainer empties one orphan slot and exits. Owned by
// qwpSfDrainerPool; one instance per slot.
//
// Lifecycle:
//  1. Open a cursor engine on the slot — recovery picks up every
//     .sfa file already on disk. The engine itself acquires the
//     slot lock; if it's held by someone else we exit silently.
//  2. Open a fresh transport via the supplied factory (separate
//     connection from the foreground sender).
//  3. Run a send loop until ackedFsn catches up to the snapshot of
//     publishedFsn taken at startup.
//  4. Close everything in reverse order; release the lock.
//
// On terminal failure (auth-rejection, reconnect-budget exhaustion,
// recovery error), the drainer drops a .failed sentinel into the
// slot before exiting. Future scans skip the slot until an operator
// clears the sentinel — bounded automatic retry, then human-in-
// the-loop.
type qwpSfOrphanDrainer struct {
	slotPath        string
	segmentSize     int64
	sfMaxTotalBytes int64
	clientFactory   qwpSfReconnectFactory
	// tracker is the shared host-health tracker. When non-nil, the
	// drainer participates in the same failover.md §2 model the
	// foreground SF loop uses: PickNext observations from one loop
	// inform the next. Each drainer's send loop owns a private
	// previousIdx slot on the shared tracker per §2.3, so mid-stream
	// demotions don't corrupt foreground's bookkeeping (or each
	// other's). nil = synthesized 1-host implicit tracker (legacy
	// single-host tests).
	tracker                 *qwpHostTracker
	reconnectMaxDuration    time.Duration
	reconnectInitialBackoff time.Duration
	reconnectMaxBackoff     time.Duration
	durableAckMode          bool          // trim only on STATUS_DURABLE_ACK
	durableKeepalive        time.Duration // durable keepalive-ping cadence
	listener                QwpBackgroundDrainerListener
	mismatchAttempts        atomic.Int64
	durableMismatchGaveUp   atomic.Bool
	connectCancel           atomic.Pointer[context.CancelFunc]
	startedAt               time.Time
	stopRequested           atomic.Bool
	targetFsn               atomic.Int64 // -1 until startup observes publishedFsn
	ackedFsn                atomic.Int64 // mirrors engine.ackedFsn for visibility
	outcome                 atomic.Int32
	lastErrorMessage        atomic.Pointer[string]
}

// qwpSfNewOrphanDrainer constructs a drainer for the given slot.
// All knobs are required; pool defaults are not applied here so
// the caller (the drainer pool) can pass through user-configured
// values verbatim.
//
// tracker is the shared foreground host-health tracker (failover.md
// §2). Pass nil for legacy single-host tests; the drainer
// synthesizes a 1-host implicit tracker internally in that case.
func qwpSfNewOrphanDrainer(
	slotPath string,
	segmentSize, sfMaxTotalBytes int64,
	clientFactory qwpSfReconnectFactory,
	tracker *qwpHostTracker,
	reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff time.Duration,
) *qwpSfOrphanDrainer {
	d := &qwpSfOrphanDrainer{
		slotPath:                slotPath,
		segmentSize:             segmentSize,
		sfMaxTotalBytes:         sfMaxTotalBytes,
		clientFactory:           clientFactory,
		tracker:                 tracker,
		reconnectMaxDuration:    reconnectMaxDuration,
		reconnectInitialBackoff: reconnectInitialBackoff,
		reconnectMaxBackoff:     reconnectMaxBackoff,
	}
	d.targetFsn.Store(-1)
	d.ackedFsn.Store(-1)
	d.outcome.Store(int32(qwpSfDrainOutcomePending))
	return d
}

// drainerOutcome returns the terminal state of the drainer's run,
// or qwpSfDrainOutcomePending while it's still running.
func (d *qwpSfOrphanDrainer) drainerOutcome() qwpSfDrainOutcome {
	return qwpSfDrainOutcome(d.outcome.Load())
}

// drainerSlotPath returns the absolute path of the orphan slot
// the drainer adopted.
func (d *qwpSfOrphanDrainer) drainerSlotPath() string {
	return d.slotPath
}

// drainerLastError returns the latest error string the drainer
// recorded, or "" if no error has been recorded.
func (d *qwpSfOrphanDrainer) drainerLastError() string {
	if p := d.lastErrorMessage.Load(); p != nil {
		return *p
	}
	return ""
}

// drainerTargetFsn returns the publishedFsn snapshot taken at
// startup, or -1 if the drainer hasn't started yet.
func (d *qwpSfOrphanDrainer) drainerTargetFsn() int64 {
	return d.targetFsn.Load()
}

// drainerAckedFsn returns the latest known ackedFsn for the slot.
func (d *qwpSfOrphanDrainer) drainerAckedFsn() int64 {
	return d.ackedFsn.Load()
}

// drainerRequestStop politely asks the drainer to exit at its next
// poll. Used by the pool's close path; drainers ALSO exit on their
// own when the slot fully drains.
func (d *qwpSfOrphanDrainer) drainerRequestStop() {
	d.stopRequested.Store(true)
}

func (d *qwpSfOrphanDrainer) recordFailure(reason string) {
	d.lastErrorMessage.Store(&reason)
	qwpSfMarkSlotFailed(d.slotPath, reason)
	d.outcome.Store(int32(qwpSfDrainOutcomeFailed))
}

// recordDurableGiveUp quarantines the slot after the durable-ack mismatch cap was
// hit, firing OnDurableAckPersistentFailure. Scoped to a genuine mismatch-driven
// give-up (durableMismatchGaveUp) — a non-mismatch terminal failure that merely
// followed an earlier mismatch must NOT fire it (Java parity: the callback fires
// only from connectWithDurableAckRetry's exhausted branch).
func (d *qwpSfOrphanDrainer) recordDurableGiveUp() {
	attempts := int(d.mismatchAttempts.Load())
	if d.listener.OnDurableAckPersistentFailure != nil {
		d.listener.OnDurableAckPersistentFailure(d.slotPath, attempts, time.Since(d.startedAt))
	}
	d.recordFailure(fmt.Sprintf(
		"durable-ack unavailable: no reachable endpoint advertised durable-ack after %d attempts", attempts))
}

// onDurableMismatch is the drainer's endpoint-failure hook: on a durable-ack
// mismatch it counts the attempt, notifies the listener, and — once the cap is
// hit — cancels the in-flight connect and asks the drainer to quarantine the slot.
func (d *qwpSfOrphanDrainer) onDurableMismatch(_ int, err error) {
	var mismatch *QwpDurableAckMismatchError
	if !errors.As(err, &mismatch) {
		return
	}
	attempt := int(d.mismatchAttempts.Add(1))
	if d.listener.OnDurableAckUnavailable != nil {
		d.listener.OnDurableAckUnavailable(d.slotPath, attempt)
	}
	if attempt >= qwpMaxDurableAckMismatchAttempts {
		d.durableMismatchGaveUp.Store(true)
		if c := d.connectCancel.Load(); c != nil {
			(*c)()
		}
		d.drainerRequestStop()
	}
}

// drainerRun is the drainer goroutine entry point. Runs to
// completion (or terminal failure), then sets outcome and exits.
func (d *qwpSfOrphanDrainer) drainerRun(ctx context.Context) {
	d.startedAt = time.Now()
	// Convert a panic on this goroutine into the same terminal Failed
	// outcome an explicit fault produces, so neither untrusted on-disk
	// .sfa bytes (the CRC/recovery scan in qwpSfNewCursorEngine) nor
	// user-supplied clientFactory code (invoked via qwpSfConnectWithRetry)
	// can crash the host process — which would take down the foreground
	// sender and every sibling drainer with it. Both run on this
	// goroutine before the send loop's own recover is in play, so this
	// is the only guard covering them. Registered first so it runs LAST
	// on unwind (defers are LIFO): the engine-close (slot-lock release)
	// and send-loop teardown defers run ahead of it, so recordFailure
	// drops its .failed sentinel onto a quiesced slot. Quarantining is
	// deliberate — a panic driven by corrupt on-disk bytes would re-panic
	// on every future re-adoption, so the sentinel breaks that loop
	// (bounded automatic retry, then human-in-the-loop), exactly as the
	// no-progress watchdog does for a wedged connection. The scan's
	// slice/index reads are bounds-checked, so this is defense-in-depth,
	// not a known reachable panic.
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("qwp/sf: orphan drainer panicked: %v\n%s", r, debug.Stack())
			log.Printf("[ERROR] %s", msg)
			d.recordFailure(msg)
		}
	}()

	engine, err := qwpSfNewCursorEngine(d.slotPath, d.segmentSize, d.sfMaxTotalBytes, qwpSfEngineDefaultAppendDeadline)
	if err != nil {
		// Lock contention is expected (a sibling drainer or the
		// foreground sender holds it) — exit silently, no .failed.
		if errors.Is(err, qwpSfErrLockBusy) || strings.Contains(err.Error(), "slot already in use") {
			d.outcome.Store(int32(qwpSfDrainOutcomeLockedByOther))
			return
		}
		// Recovery / disk error — surface as failure with sentinel.
		msg := err.Error()
		d.lastErrorMessage.Store(&msg)
		qwpSfMarkSlotFailed(d.slotPath, "engine open: "+msg)
		d.outcome.Store(int32(qwpSfDrainOutcomeFailed))
		return
	}
	defer func() { _ = engine.engineClose() }()

	target := engine.enginePublishedFsn()
	d.targetFsn.Store(target)
	if engine.engineAckedFsn() >= target {
		// Slot is already drained — engineClose will unlink residual
		// .sfa files in its own logic.
		d.outcome.Store(int32(qwpSfDrainOutcomeSuccess))
		return
	}
	// Initial connect via the round-walk so the drainer immediately
	// honours classifications the foreground tracker has already
	// observed (e.g. host 0 is currently TopologyReject — start at
	// host 1 instead). When d.tracker is nil, qwpSfConnectWithRetry
	// synthesises a 1-host implicit tracker, matching the legacy
	// behaviour single-host tests rely on.
	connectCtx, cancelConnect := context.WithCancel(ctx)
	// Panic-safety net: on a panic in qwpSfConnectWithRetry (recovered above) the
	// inline cancelConnect() is skipped, orphaning the child ctx in the pool ctx
	// until close. cancel is idempotent; the eager call still releases it normally.
	defer cancelConnect()
	d.connectCancel.Store(&cancelConnect)
	transport, boundIdx, err := qwpSfConnectWithRetry(connectCtx, d.clientFactory, d.tracker,
		d.reconnectMaxDuration, d.reconnectInitialBackoff, d.reconnectMaxBackoff,
		!d.durableAckMode, d.onDurableMismatch)
	d.connectCancel.Store(nil)
	cancelConnect()
	if err != nil {
		// Gave up after repeated durable-ack mismatches: quarantine the slot.
		if d.durableMismatchGaveUp.Load() {
			d.recordDurableGiveUp()
			return
		}
		// Pool close (or caller cancellation) during the dial:
		// don't drop a .failed sentinel — the slot is still
		// drainable on a future sender start.
		if ctx.Err() != nil || d.stopRequested.Load() {
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		msg := err.Error()
		d.recordFailure("initial connect: " + msg)
		return
	}
	loop := qwpSfNewSendLoop(engine, transport, d.clientFactory,
		qwpSfDefaultParkInterval,
		d.reconnectMaxDuration, d.reconnectInitialBackoff, d.reconnectMaxBackoff)
	// A durable-ack drainer trims the orphan slot only on STATUS_DURABLE_ACK, so
	// recovered data is not deleted before it is durably uploaded. A mismatch is
	// transient (not terminal): the drainer retries, since its data is pinned.
	loop.sendLoopSetDurableAck(d.durableAckMode, d.durableKeepalive, false)
	loop.onEndpointFailed = d.onDurableMismatch
	// Share the foreground tracker; the loop carries its OWN
	// previousIdx slot (failover.md §2.3 "per-caller previousIdx,
	// not shared") so a mid-stream demote here doesn't corrupt
	// foreground's bookkeeping.
	loop.sendLoopSetHostTracker(d.tracker, boundIdx)
	engine.engineSetReconnectStatusGetter(loop.sendLoopReconnectStatus)
	// Wired for parity with the foreground loop; the drainer replays an
	// adopted slot and never appends, so the getter is never consulted
	// here (no engineAppendBlocking caller to park).
	engine.engineSetTerminalErrorGetter(loop.sendLoopCheckError)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	timer := time.NewTicker(qwpSfDrainerPollInterval)
	defer timer.Stop()
	// No-progress watchdog. A server that completes the WS upgrade
	// and accepts our frames but never ACKs and never drops the
	// connection (wedged server, black-hole proxy, or a silently
	// incompatible build that holds the socket open) keeps acked
	// below target forever while sendLoopCheckError stays nil — the
	// run()-level "frames sent, zero acks → terminal" heuristic only
	// fires after the connection drops, which by definition never
	// happens here. Without a bound the drainer spins on the poll
	// interval forever and, on Close, exits Stopped (no .failed
	// sentinel), so every future process start re-adopts the same
	// wedged slot in full — an unbounded re-adoption livelock.
	//
	// Bound it with the same reconnectMaxDuration budget that bounds
	// the connect round-walk (this mirrors the Java drainer's
	// connect-phase deadline semantics — "give the cluster a budget
	// to settle before quarantining the slot"): if acked makes no
	// forward progress for that long while we are NOT inside a
	// (separately bounded) reconnect, drop a .failed sentinel so the
	// design's "bounded automatic retry, then human-in-the-loop"
	// promise holds. A reconnect exhausting its own budget still
	// surfaces ahead of this via sendLoopCheckError.
	noProgressBudget := d.reconnectMaxDuration
	if noProgressBudget <= 0 {
		noProgressBudget = qwpSfDefaultReconnectMaxDuration
	}
	lastProgressAcked := engine.engineAckedFsn()
	lastProgressAt := time.Now()
	for {
		acked := engine.engineAckedFsn()
		d.ackedFsn.Store(acked)
		if acked >= target {
			d.outcome.Store(int32(qwpSfDrainOutcomeSuccess))
			return
		}
		// Check the mismatch cap before the generic wire error so a
		// mismatch-driven give-up surfaces as such (and fires the listener),
		// rather than being masked by a coincident reconnect-budget error.
		if d.durableMismatchGaveUp.Load() {
			d.recordDurableGiveUp()
			return
		}
		if err := loop.sendLoopCheckError(); err != nil {
			d.recordFailure("wire: " + err.Error())
			return
		}
		if d.stopRequested.Load() {
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		// Forward ACK progress, or being inside the separately
		// bounded reconnect loop, resets the watchdog clock. A fresh
		// connection thus always gets a full budget to produce its
		// first ACK.
		now := time.Now()
		reconnecting, _, _ := loop.sendLoopReconnectStatus()
		switch {
		case acked > lastProgressAcked || reconnecting:
			lastProgressAcked = acked
			lastProgressAt = now
		case now.Sub(lastProgressAt) >= noProgressBudget:
			d.recordFailure(fmt.Sprintf(
				"no drain progress: ackedFsn stuck at %d (target %d) for %s "+
					"on a live connection — server accepted frames but is not "+
					"ACKing (wedged server or incompatible build)",
				acked, target, now.Sub(lastProgressAt)))
			return
		}
		select {
		case <-ctx.Done():
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		case <-timer.C:
		}
	}
}

// qwpSfDrainerPool is a bounded thread pool that runs orphan
// drainer tasks. One pool per foreground sender; size capped by
// max_background_drainers.
//
// Each drainer gets its own goroutine, throttled by a buffered
// semaphore channel. Idle pool (no orphans submitted) costs zero
// goroutines. Closing the pool requests every still-running
// drainer to stop and waits up to qwpSfDrainerPoolCloseGrace for
// them to exit cleanly; if any drainer is still alive after the
// grace (typically blocked in a TCP dial / WS upgrade), the pool
// cancels its master context so blocking I/O unwinds, then waits a
// further qwpSfDrainerPoolHardCloseGrace. A drainer wedged in
// un-cancellable I/O past that bound is abandoned (with a logged
// count) so close() never hangs.
type qwpSfDrainerPool struct {
	maxConcurrent int
	sem           chan struct{}
	closed        atomic.Bool
	wg            sync.WaitGroup

	// ctx is the master context handed to every drainerRun call.
	// Cancelled in drainerPoolClose so dials and other ctx-aware
	// blocking calls unwind. Independent of the caller's setup
	// ctx — drainers are long-lived and must outlive whatever
	// transient ctx was used to construct the parent sender.
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	active []*qwpSfOrphanDrainer
}

// qwpSfNewDrainerPool constructs a pool with the given concurrency
// cap. Panics on a non-positive cap.
func qwpSfNewDrainerPool(maxConcurrent int) *qwpSfDrainerPool {
	if maxConcurrent <= 0 {
		panic("qwp/sf: maxConcurrent must be > 0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &qwpSfDrainerPool{
		maxConcurrent: maxConcurrent,
		sem:           make(chan struct{}, maxConcurrent),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// drainerPoolSubmit launches the drainer in a managed goroutine.
// Returns an error if the pool has been closed.
//
// Drainers queue when the concurrency cap is reached: the
// goroutine takes a slot on the semaphore and proceeds. The
// caller's ctx only gates the semaphore wait — once the drainer
// is running, it observes the pool's master ctx instead, so
// drainers outlive the caller's (typically setup-only) ctx.
func (p *qwpSfDrainerPool) drainerPoolSubmit(ctx context.Context, d *qwpSfOrphanDrainer) error {
	if p.closed.Load() {
		return errors.New("qwp/sf: drainer pool closed")
	}
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return errors.New("qwp/sf: drainer pool closed")
	}
	p.active = append(p.active, d)
	p.wg.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.wg.Done()
		defer p.removeActive(d)
		// Wait for a slot. The caller's ctx unblocks if the user
		// gives up on setup; the pool's ctx unblocks on close.
		select {
		case p.sem <- struct{}{}:
		case <-ctx.Done():
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		case <-p.ctx.Done():
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		defer func() { <-p.sem }()
		if p.closed.Load() {
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		// Use the pool's ctx so the drainer is detached from the
		// caller's setup ctx (its expected lifetime is far longer)
		// but is forcibly cancellable when the pool is closing.
		d.drainerRun(p.ctx)
	}()
	return nil
}

// removeActive unlinks d from the active list when its goroutine
// exits. Called from a defer in drainerPoolSubmit's worker.
func (p *qwpSfDrainerPool) removeActive(d *qwpSfOrphanDrainer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, x := range p.active {
		if x == d {
			n := len(p.active)
			p.active[i] = p.active[n-1]
			p.active[n-1] = nil
			p.active = p.active[:n-1]
			return
		}
	}
}

// drainerPoolSnapshot returns a copy of the drainers currently
// running (or queued on the semaphore). Drainers that have run
// to completion are pruned. Useful for status accessors.
func (p *qwpSfDrainerPool) drainerPoolSnapshot() []*qwpSfOrphanDrainer {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*qwpSfOrphanDrainer, len(p.active))
	copy(out, p.active)
	return out
}

// activeCount returns the number of drainers still tracked as
// running or queued. drainerPoolClose reports it as the count of
// drainers abandoned at the hard-grace boundary.
func (p *qwpSfDrainerPool) activeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.active)
}

// drainerPoolClose stops the pool. Sets closed=true so new submits
// fail; requests a polite stop on every tracked drainer; waits up
// to qwpSfDrainerPoolCloseGrace. If any drainer is still alive at
// the grace boundary it is most likely parked in a TCP dial / WS
// upgrade — cancel the master ctx to unwind those blocking calls,
// then wait a further qwpSfDrainerPoolHardCloseGrace. A drainer
// still running past that bound is wedged in I/O the ctx cannot
// reach (engine-open flock / mmap / CRC scan / hung NFS); it is
// abandoned with a logged count rather than hanging close() — its
// slot stays a valid orphan for a future sender. Idempotent.
func (p *qwpSfDrainerPool) drainerPoolClose() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	p.mu.Lock()
	for _, d := range p.active {
		d.drainerRequestStop()
	}
	p.mu.Unlock()
	doneCh := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(doneCh)
	}()
	graceTimer := time.NewTimer(qwpSfDrainerPoolCloseGrace)
	defer graceTimer.Stop()
	select {
	case <-doneCh:
		// Every drainer exited within the polite grace.
	case <-graceTimer.C:
		// A drainer outlived the polite grace — most likely parked in
		// a TCP dial / WS upgrade. Cancel the master ctx to unwind
		// those ctx-aware blocking calls, then wait a bounded second
		// grace.
		p.cancel()
		hardTimer := time.NewTimer(qwpSfDrainerPoolHardCloseGrace)
		defer hardTimer.Stop()
		select {
		case <-doneCh:
			// Cancellation unwound the straggler(s).
		case <-hardTimer.C:
			// A drainer is wedged in I/O the ctx cannot reach
			// (engine-open flock / mmap / CRC scan / hung NFS).
			// Abandon it: its goroutine lives until the syscall
			// returns, but close() must not block on un-cancellable
			// I/O. The slot it holds stays a valid orphan a future
			// sender re-adopts. Surface the abandoned count for ops.
			log.Printf("[WARN] qwp/sf: %d orphan drainer(s) still running %s "+
				"after close; abandoning (wedged in un-cancellable disk I/O). "+
				"Their slots remain adoptable on a future sender start.",
				p.activeCount(), qwpSfDrainerPoolCloseGrace+qwpSfDrainerPoolHardCloseGrace)
		}
	}
	// Release the master ctx even on the clean-exit path so the
	// underlying timer goroutine doesn't linger.
	p.cancel()
}
