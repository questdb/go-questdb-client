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
	"log/slog"
	"runtime/debug"
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

// qwpMaxDurableAckMismatchAttempts is the durable-ack settle budget: how
// many exhausted connect sweeps that met a capability gap (an endpoint
// not advertising durable-ack) a drainer tolerates before quarantining
// the slot with a .failed sentinel. Counted per sweep, not per dial, so
// a wide address list cannot burn the budget in one walk; an
// all-replica sweep resets it (see onRoundExhausted).
const qwpMaxDurableAckMismatchAttempts = 16

// qwpSfDrainerDefaultConnectTimeoutMs is the finite default TCP-connect
// deadline for background-drainer dials when connect_timeout is unset.
// An explicit connect_timeout overrides it for drainers too; the
// foreground sender keeps the untimed OS default when unset.
const qwpSfDrainerDefaultConnectTimeoutMs = 15_000

// QwpBackgroundDrainerListener receives durable-ack drain outcomes for a crashed
// sibling's store-and-forward slot. Callbacks must not block.
//
// Each drainer uses a limited-size queue to deliver callbacks in the background.
// Drainers share the listener, so callbacks for different slots may run at the
// same time. Callbacks must be safe to call concurrently. A callback panic is
// caught and logged; it does not quarantine the slot or crash the process.
// Any callback may be nil. WithBackgroundDrainerListener sets the listener for
// every drainer.
//
// Use a channel or context cancellation to signal the application code that
// uses the sender. Do not change or close a sender, or call QuestDB.Close
// directly from a callback. Documented methods returning read-only snapshots
// of state are allowed. Return promptly. Shutdown may drop queued
// notifications, and a callback already running may finish after Close returns.
// QuestDB.Close cannot succeed while an internal drainer can still access a
// slot. See [QuestDB.Close] and README's "QWP shutdown and ownership".
type QwpBackgroundDrainerListener struct {
	// OnDurableAckUnavailable fires once per exhausted connect sweep in which
	// a durable-ack drainer met an endpoint that does not advertise
	// durable-ack. attempt is the cumulative count of such sweeps; the drainer
	// keeps retrying (its source is pinned on disk) until the settle budget
	// (qwpMaxDurableAckMismatchAttempts) is exhausted.
	OnDurableAckUnavailable func(dir string, attempt int)
	// OnDurableAckPersistentFailure fires once when a drainer gives up after
	// repeated durable-ack mismatches and quarantines the slot (.failed).
	OnDurableAckPersistentFailure func(dir string, attempts int, elapsed time.Duration)
	// OnPrimaryUnavailable fires each time a full connect sweep found only
	// role-rejecting endpoints (every reachable node is a replica or a
	// catching-up primary). attempt is the cumulative all-replica sweep count.
	// The window is transient — a replica gets promoted, a primary reappears —
	// so the drainer keeps retrying indefinitely; this callback is
	// observability only and never escalates.
	OnPrimaryUnavailable func(dir string, attempt int)
}

// qwpDrainerListenerCall catches a panic from the callback. Notifications run
// on a separate worker and are not part of resource cleanup. A callback panic
// must not mark a slot as failed or change who cleans it up. Nil does nothing.
func qwpDrainerListenerCall(logger *slog.Logger, fn func()) {
	if fn == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			// The report goes through the guarded logger too. The user's slog
			// handler is user code exactly like the callback, and a panic
			// while reporting the first panic would produce the outcome this
			// guard exists to prevent.
			qwpEffectiveLogger(logger).Error("qwp/sf drainer listener callback panicked", "panic", r)
		}
	}()
	fn()
}

// qwpSfDrainerPollInterval is how often the drainer wakes to
// re-check whether the slot is fully drained.
const qwpSfDrainerPollInterval = 50 * time.Millisecond

// qwpSfDurableStallFactor scales the no-progress budget into the durable-stall
// bound: how long a durable-ack drainer tolerates OK-ack/reconnect activity
// with zero durable trim advance before quarantining. Generous (uploads are
// slow) but finite, so a flapping never-durable endpoint cannot livelock the
// drainer.
const qwpSfDurableStallFactor = 4

// qwpSfMinNoProgressBudget floors the drainer's live-connection no-progress
// watchdog so a small reconnect_max_duration_millis (set to fail the blocking
// initial connect fast) cannot also shrink the watchdog and quarantine a
// healthy-but-slow adopted slot. Durable mode scales it by qwpSfDurableStallFactor.
// Tests can shorten this timeout. Reads and writes are atomic because a drainer
// may still read it while a test restores the original value.
var qwpSfMinNoProgressBudget = qwpSfSwappable(30 * time.Second)

// qwpSfDrainerPoolCloseGrace is how long close() waits for active drainers to
// stop before cancelling their shared context to interrupt connection attempts.
// The default is three seconds, matching Java. Tests can shorten it; atomic
// reads and writes let them change it safely while drainers are running.
var qwpSfDrainerPoolCloseGrace = qwpSfSwappable(3 * time.Second)

// qwpSfDrainerPoolHardCloseGrace limits the second wait, after the pool cancels
// the drainers' shared context. Cancellation can stop network connection attempts
// and the sending loop, but not all file operations or recovery scans. The pool
// keeps track of drainers that outlast this wait. Their slots cannot be reused
// until cleanup releases them. Tests can change this timeout.
var qwpSfDrainerPoolHardCloseGrace = qwpSfSwappable(1 * time.Second)

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
// On terminal failure (auth-rejection, durable-ack settle-budget
// exhaustion, recovery error, a wedged no-progress connection), the
// drainer drops a .failed sentinel into the slot before exiting.
// Future scans skip the slot until an operator clears the sentinel.
// Transport outages and all-replica failover windows are NOT terminal:
// the drainer retries them indefinitely with capped backoff
// (Invariant B) — its source data is pinned on disk.
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
	maxFrameRejections      int           // poison-frame threshold; 0 -> default
	logger                  *slog.Logger  // nil -> slog.Default() via qwpEffectiveLogger
	listener                QwpBackgroundDrainerListener
	mismatchAttempts        atomic.Int64
	roleRejectRounds        atomic.Int64
	lastReplicaWarnUnixNano atomic.Int64
	durableMismatchGaveUp   atomic.Bool
	connectCancel           atomic.Pointer[context.CancelFunc]
	startedAt               time.Time
	stopRequested           atomic.Bool
	stopOnce                sync.Once
	// stopCh is closed by drainerRequestStop so a polite stop unwinds an
	// in-flight connect walk promptly (as the walk's cancelCh) instead of
	// waiting out the pool-close hard-cancel grace.
	stopCh            chan struct{}
	targetFsn         atomic.Int64 // -1 until startup observes publishedFsn
	ackedFsn          atomic.Int64 // mirrors engine.ackedFsn for visibility
	outcome           atomic.Int32
	lastErrorMessage  atomic.Pointer[string]
	cleanupMu         sync.Mutex
	cleanup           *qwpSfCursorEngine
	cleanupErr        error
	notificationsOnce sync.Once
	notifications     *qwpDispatcher[func()]
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
	d.stopCh = make(chan struct{})
	return d
}

// noProgressBudget is the live-connection no-progress watchdog budget: how long
// a bound-but-not-advancing drain is tolerated before quarantine. It derives
// from reconnectMaxDuration (the default when unset) but never drops below
// qwpSfMinNoProgressBudget, so bounding the blocking initial connect with a
// small reconnect_max_duration_millis cannot also wrongly quarantine a
// slow-but-healthy slot. Durable mode scales the result by qwpSfDurableStallFactor.
func (d *qwpSfOrphanDrainer) noProgressBudget() time.Duration {
	budget := d.reconnectMaxDuration
	if budget <= 0 {
		budget = qwpSfDefaultReconnectMaxDuration
	}
	if floor := qwpSfMinNoProgressBudget.load(); budget < floor {
		budget = floor
	}
	return budget
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
	d.stopOnce.Do(func() { close(d.stopCh) })
}

func (d *qwpSfOrphanDrainer) recordFailure(reason string) {
	d.cleanupMu.Lock()
	d.cleanupErr = errors.Join(d.cleanupErr, errors.New(reason))
	d.cleanupMu.Unlock()
	d.lastErrorMessage.Store(&reason)
	qwpSfMarkSlotFailed(d.slotPath, reason)
	d.outcome.Store(int32(qwpSfDrainOutcomeFailed))
}

// recordDurableGiveUp quarantines the slot after the durable-ack mismatch cap was
// hit, firing OnDurableAckPersistentFailure. Scoped to a genuine mismatch-driven
// give-up (durableMismatchGaveUp) — a non-mismatch terminal failure that merely
// followed an earlier mismatch must NOT fire it: the callback fires only from
// the durable-ack retry's exhausted branch.
func (d *qwpSfOrphanDrainer) recordDurableGiveUp() {
	attempts := int(d.mismatchAttempts.Load())
	if fn := d.listener.OnDurableAckPersistentFailure; fn != nil {
		elapsed := time.Since(d.startedAt)
		d.notifyListener(func() { fn(d.slotPath, attempts, elapsed) })
	}
	d.recordFailure(fmt.Sprintf(
		"durable-ack unavailable: no reachable endpoint advertised durable-ack after %d attempts", attempts))
}

// onRoundExhausted is the drainer's per-sweep hook, fired both by the initial
// connect walk and by the send loop's mid-drain reconnect walks. The
// durable-ack settle budget charges exhausted sweeps, not dials: a
// capability-gap sweep (any dial met an endpoint without durable-ack) counts
// one attempt, notifies the listener with the cumulative count, and — at the
// cap — cancels any in-flight connect and asks the drainer to quarantine the
// slot. An all-replica sweep proves topology churn: whatever node produced
// earlier mismatches is no longer the primary the next sweep hits, so the
// capability-gap episode restarts and the next gap gets the full settle
// budget. Sweeps that saw a transport failure neither charge nor reset — the
// budget pauses with the outage (Invariant B) — and the walk keeps retrying
// with capped backoff throughout.
func (d *qwpSfOrphanDrainer) onRoundExhausted(outcome qwpSfSweepOutcome) {
	if outcome.SawDurableMismatch && !outcome.SawTransportError {
		attempt := int(d.mismatchAttempts.Add(1))
		if fn := d.listener.OnDurableAckUnavailable; fn != nil {
			d.notifyListener(func() { fn(d.slotPath, attempt) })
		}
		if attempt >= qwpMaxDurableAckMismatchAttempts {
			d.durableMismatchGaveUp.Store(true)
			if c := d.connectCancel.Load(); c != nil {
				(*c)()
			}
			d.drainerRequestStop()
		}
		return
	}
	if !outcome.allReplica() {
		return
	}
	d.mismatchAttempts.Store(0)
	attempt := int(d.roleRejectRounds.Add(1))
	if fn := d.listener.OnPrimaryUnavailable; fn != nil {
		d.notifyListener(func() { fn(d.slotPath, attempt) })
	}
	now := time.Now().UnixNano()
	if last := d.lastReplicaWarnUnixNano.Load(); now-last >= int64(qwpSfReconnectWarnThrottle) &&
		d.lastReplicaWarnUnixNano.CompareAndSwap(last, now) {
		// Shadows the OnPrimaryUnavailable listener callback (dispatched
		// alongside this); throttled Debug so it adds a trace for deep
		// debugging without duplicating the callback for the default case.
		qwpEffectiveLogger(d.logger).Debug("qwp/sf: drainer sweep found only replicas "+
			"(transient failover window), retrying with capped backoff",
			"slot", d.slotPath, "sweep", attempt)
	}
}

// drainerRun is the drainer goroutine entry point. Runs to
// completion (or terminal failure), then sets outcome and exits.
func (d *qwpSfOrphanDrainer) drainerRun(ctx context.Context) {
	d.startedAt = time.Now()
	// An internal panic does not prove the stored data is corrupt. Keep the
	// affected resources and report the error without writing a .failed file.
	defer func() {
		if r := recover(); r != nil {
			d.cleanupMu.Lock()
			if build, ok := r.(qwpSfBuildPanic); ok {
				if reporter, ok := build.reporter.(*qwpSfBuildCleanupError); ok {
					d.cleanup = reporter.engine
				}
			}
			d.cleanupErr = &qwpCleanupPanicError{phase: "orphan drainer", cause: r, stack: debug.Stack()}
			d.cleanupMu.Unlock()
			d.outcome.Store(int32(qwpSfDrainOutcomeFailed))
			qwpFailedDrainers.Lock()
			qwpFailedDrainers.items = append(qwpFailedDrainers.items, d)
			qwpFailedDrainers.Unlock()
		}
		// Resource cleanup need not wait for user callbacks to finish.
		go d.listenerDispatcher().close()
	}()

	engine, err := qwpSfNewCursorEngineWithOptions(d.slotPath, d.segmentSize, d.sfMaxTotalBytes, qwpSfEngineDefaultAppendDeadline, qwpSfEngineOpenOptions{
		logger: d.logger,
		// The scan that queued this drainer happened earlier, possibly before a
		// foreground sender preserved the slot aside or marked it failed. Under
		// the logical slot lock, the complete candidate decision is taken again
		// against what is on disk now. A slot that was preserved, marked failed,
		// removed, or fully drained is abandoned without opening it.
		revalidate: qwpSfRequireCandidateForAdoption,
	})
	if err != nil {
		if errors.Is(err, qwpSfErrSlotNotAdoptable) && !errors.Is(err, ErrSfDurability) {
			// Not a fault and not evidence about the bytes: the slot stopped
			// being adoptable between the scan and this lock. Leave it alone. A
			// fully drained candidate is successful; other lifecycle races use
			// the existing skipped/locked outcome.
			qwpEffectiveLogger(d.logger).Debug("qwp/sf: orphan drainer skipped a slot that is no longer adoptable",
				"slot", d.slotPath, "reason", err)
			outcome := qwpSfDrainOutcomeLockedByOther
			if errors.Is(err, qwpSfErrSlotAlreadyDrained) {
				outcome = qwpSfDrainOutcomeSuccess
			}
			d.outcome.Store(int32(outcome))
			return
		}
		var buildErr *qwpSfBuildCleanupError
		if errors.As(err, &buildErr) {
			d.cleanupMu.Lock()
			d.cleanup = buildErr.engine
			d.cleanupMu.Unlock()
		}
		// Lock contention is expected (a sibling drainer or the
		// foreground sender holds it) — exit silently, no .failed.
		if errors.Is(err, qwpSfErrLockBusy) {
			d.outcome.Store(int32(qwpSfDrainOutcomeLockedByOther))
			return
		}
		// Recovery / disk error. The .failed sentinel is permanent — nothing in
		// the client ever removes it — so it is reserved for a slot recovery
		// proved inconsistent, where every future adoption would fail the same
		// way. A local I/O fault (a full disk, an exhausted fd table, a mount
		// that went away) says nothing about the bytes: the slot keeps its data
		// and its eligibility, and the next foreground scan adopts it again
		// once the fault clears.
		d.cleanupMu.Lock()
		d.cleanupErr = errors.Join(d.cleanupErr, err)
		d.cleanupMu.Unlock()
		msg := err.Error()
		d.lastErrorMessage.Store(&msg)
		if errors.Is(err, qwpSfErrRecoveryFailClosed) {
			qwpSfMarkSlotFailed(d.slotPath, "engine open: "+msg)
		} else {
			qwpEffectiveLogger(d.logger).Error("qwp/sf: orphan drainer could not open the slot; leaving it eligible for a later scan",
				"slot", d.slotPath, "error", err)
		}
		d.outcome.Store(int32(qwpSfDrainOutcomeFailed))
		return
	}
	// Save the engine before doing setup that could fail. The pool must still
	// track its cleanup after this goroutine exits. Only the engine's cleanup
	// worker may release its resources.
	d.cleanupMu.Lock()
	d.cleanup = engine
	d.cleanupMu.Unlock()
	var loop *qwpSfSendLoop
	defer func() {
		r := recover()
		var cause error
		if r != nil {
			cause = &qwpCleanupPanicError{phase: "orphan work", cause: r, stack: debug.Stack()}
		}
		_ = engine.engineCloseWithCause(cause)
		if r != nil {
			panic(r)
		}
	}()
	if hook := qwpSfTestAfterDrainerEngineOpenHook.Load(); hook != nil {
		(*hook)(engine)
	}

	target := engine.enginePublishedFsn()
	d.targetFsn.Store(target)
	if engine.engineAckedFsn() >= target {
		// Slot is already drained — engineClose will unlink residual
		// .sfa files in its own logic.
		d.outcome.Store(int32(qwpSfDrainOutcomeSuccess))
		return
	}
	// Initial connect via the round-walk, unbounded (Invariant B): a
	// down server or an all-replica window is transient, so the walk
	// retries with capped backoff until success, a genuine terminal
	// (auth reject; durable settle-budget exhaustion via the
	// onRoundExhausted cancel), or pool close. The walk also honours
	// classifications the foreground tracker has already observed
	// (e.g. host 0 is currently TopologyReject — start at host 1
	// instead). When d.tracker is nil, a synthesized 1-host implicit
	// tracker matches the legacy behaviour single-host tests rely on.
	connectCtx, cancelConnect := context.WithCancel(ctx)
	// Panic-safety net: on a panic in the walk (recovered above) the
	// inline cancelConnect() is skipped, orphaning the child ctx in the pool ctx
	// until close. cancel is idempotent; the eager call still releases it normally.
	defer cancelConnect()
	d.connectCancel.Store(&cancelConnect)
	initialBackoff := d.reconnectInitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = qwpSfDefaultReconnectInitialBackoff
	}
	maxBackoff := d.reconnectMaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = qwpSfDefaultReconnectMaxBackoff
	}
	tracker := d.tracker
	if tracker == nil {
		tracker = newQwpHostTracker(1, "", qwpTargetAny)
	}
	result := qwpSfRunRoundWalk(connectCtx, d.stopCh, qwpSfRoundWalkParams{
		Factory:                 engine.trackConnectCleanup(d.clientFactory),
		Tracker:                 tracker,
		MaxDuration:             0,
		InitialBackoff:          initialBackoff,
		MaxBackoff:              maxBackoff,
		OnRoundExhausted:        d.onRoundExhausted,
		DurableMismatchTerminal: !d.durableAckMode,
		Background:              true,
	}, -1)
	d.connectCancel.Store(nil)
	cancelConnect()
	if result.Transport == nil {
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
		// Terminal (auth / non-retriable upgrade) — the only failure
		// class the unbounded walk surfaces.
		if result.Terminal != nil {
			d.recordFailure("initial connect: " + result.Terminal.Error())
			return
		}
		if result.Cancelled != nil {
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		d.recordFailure("initial connect: round-walk returned no result")
		return
	}
	transport, boundIdx := result.Transport, result.Idx
	loop = qwpSfNewSendLoop(engine, transport, d.clientFactory,
		qwpSfDefaultParkInterval,
		d.reconnectMaxDuration, d.reconnectInitialBackoff, d.reconnectMaxBackoff)
	loop.logger = qwpEffectiveLogger(d.logger)
	// A durable-ack drainer trims the orphan slot only on STATUS_DURABLE_ACK, so
	// recovered data is not deleted before it is durably uploaded. A mismatch is
	// transient (not terminal): the drainer retries, since its data is pinned.
	loop.sendLoopSetDurableAck(d.durableAckMode, d.durableKeepalive, false)
	loop.sendLoopSetMaxFrameRejections(d.maxFrameRejections)
	loop.sendLoopSetOnRoundExhausted(d.onRoundExhausted)
	loop.sendLoopSetConnectionListener(silentSenderConnectionListener, 0)
	// Share the foreground tracker; the loop carries its OWN
	// previousIdx slot (failover.md §2.3 "per-caller previousIdx,
	// not shared") so a mid-stream demote here doesn't corrupt
	// foreground's bookkeeping, and its reconnect walks run as
	// background (walk-local round cursor) so they can't consume or
	// clear the foreground walker's round slots.
	loop.sendLoopSetBackgroundWalks(true)
	loop.sendLoopSetHostTracker(d.tracker, boundIdx)
	engine.engineSetReconnectStatusGetter(loop.sendLoopReconnectStatus)
	// Wired for parity with the foreground loop; the drainer replays an
	// adopted slot and never appends, so the getter is never consulted
	// here (no engineAppendBlocking caller to park).
	engine.engineSetTerminalErrorGetter(loop.sendLoopCheckError)
	loop.sendLoopStart()

	timer := time.NewTicker(qwpSfDrainerPollInterval)
	defer timer.Stop()
	// No-progress watchdog. A server that completes the WS upgrade
	// and accepts our frames but never ACKs and never drops the
	// connection (wedged server, black-hole proxy, or a silently
	// incompatible build that holds the socket open) keeps acked
	// below target forever while sendLoopCheckError stays nil.
	// Without a bound the drainer spins on the poll interval forever
	// and, on Close, exits Stopped (no .failed sentinel), so every
	// future process start re-adopts the same wedged slot in full —
	// an unbounded re-adoption livelock.
	//
	// This bounds only a LIVE-but-not-acking connection: transport
	// outages never charge it (a reconnect window resets/pauses the
	// clocks below), so a long server outage cannot quarantine the
	// slot (Invariant B). The budget derives from reconnectMaxDuration but is
	// floored (see noProgressBudget), so a small reconnect_max_duration_millis
	// set to bound the blocking initial connect cannot also quarantine a
	// slow-but-healthy slot here.
	noProgressBudget := d.noProgressBudget()
	lastProgressAcked := engine.engineAckedFsn()
	lastProgressAcks := loop.sendLoopTotalAcks()
	lastProgressAt := time.Now()
	// Durable-stall clock: accumulates only live-connection time (a
	// reconnect window pauses it without resetting what has already
	// accumulated — anti-evasion: a flapping endpoint that accepts +
	// OK-acks + drops without ever issuing STATUS_DURABLE_ACK cannot
	// evade the bound by reconnect-cycling) and zeroes only on a
	// genuine durable trim advance. Without it, okAcks climbs on every
	// cycle and the drainer never quarantines.
	durableStallElapsed := time.Duration(0)
	lastStallSampleAt := lastProgressAt
	for {
		acked := engine.engineAckedFsn()
		d.ackedFsn.Store(acked)
		if acked >= target {
			d.outcome.Store(int32(qwpSfDrainOutcomeSuccess))
			return
		}
		// Check the mismatch cap before the generic wire error so a
		// mismatch-driven give-up surfaces as such (and fires the listener),
		// rather than being masked by a coincident terminal wire error.
		if d.durableMismatchGaveUp.Load() {
			d.recordDurableGiveUp()
			return
		}
		// The running loop latches only genuine terminals (auth, a
		// poisoned frame, corrupt segment, durable-ack mismatch) —
		// transport outages reconnect indefinitely and never reach here.
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
		//
		// In durable mode `acked` only advances on STATUS_DURABLE_ACK, so a
		// healthy-but-slow durable pipeline (the server OK-acking frames while
		// an object-storage upload lags) would otherwise trip the watchdog.
		// Treat forward movement of the OK-ack counter as progress too, so only
		// a slot with NO ack activity at all — the genuine wedge — is quarantined.
		now := time.Now()
		reconnecting, _, _ := loop.sendLoopReconnectStatus()
		okAcks := loop.sendLoopTotalAcks()
		// A transport window pauses the durable-stall clock: only
		// live-connection time is charged, so a long outage cannot burn
		// the settle budget (Invariant B).
		if !reconnecting {
			durableStallElapsed += now.Sub(lastStallSampleAt)
		}
		lastStallSampleAt = now
		if acked > lastProgressAcked || reconnecting ||
			(d.durableAckMode && okAcks > lastProgressAcks) {
			// Forward progress of either the durable trim watermark or the OK-ack
			// counter proves the sender bound a durable-advertising primary and is
			// draining this slot — an OK ack cannot arrive unless the durable-ack
			// handshake succeeded — so forget accumulated capability-gap mismatches.
			// Otherwise the lifetime counter climbs one per primary-flap reconnect
			// (replica picked first in the sweep, then the primary rebinds and OK-acks
			// but drops before uploading) and eventually trips
			// qwpMaxDurableAckMismatchAttempts, falsely quarantining a slot whose
			// durable primary was reached. A durable primary reached but slow to
			// upload is caught by the separate durable-stall watchdog, not this
			// counter.
			if d.durableAckMode && (acked > lastProgressAcked || okAcks > lastProgressAcks) {
				d.mismatchAttempts.Store(0)
			}
			if acked > lastProgressAcked {
				durableStallElapsed = 0
			}
			lastProgressAcked = acked
			lastProgressAcks = okAcks
			lastProgressAt = now
		}
		// In durable mode the trim watermark moves only on STATUS_DURABLE_ACK,
		// which can lag long after every frame is OK-acked and okAcks has
		// plateaued, so gate quarantine on the wider durable-stall clock,
		// not the no-progress budget on lastProgressAt.
		if d.durableAckMode {
			if durableStallElapsed >= qwpSfDurableStallFactor*noProgressBudget {
				d.recordFailure(d.noProgressReason(acked, target, okAcks, durableStallElapsed))
				return
			}
		} else if now.Sub(lastProgressAt) >= noProgressBudget {
			d.recordFailure(d.noProgressReason(acked, target, okAcks, now.Sub(lastProgressAt)))
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

// noProgressReason builds the .failed sentinel reason for a stalled drain,
// distinguishing a genuinely wedged connection (no ACKs at all) from a durable
// pipeline that OK-acks frames but lags on STATUS_DURABLE_ACK — the two call for
// very different operator remediation, and the latter is not a wedged/incompatible
// server.
func (d *qwpSfOrphanDrainer) noProgressReason(acked, target, okAcks int64, stuckFor time.Duration) string {
	if d.durableAckMode && okAcks > 0 {
		return fmt.Sprintf(
			"no durable-ack progress: ackedFsn stuck at %d (target %d) for %s "+
				"on a live connection — the server OK-acked frames but issued no "+
				"STATUS_DURABLE_ACK (a stalled durable pipeline, e.g. a wedged "+
				"object-storage upload, or an incompatible build)",
			acked, target, stuckFor)
	}
	return fmt.Sprintf(
		"no drain progress: ackedFsn stuck at %d (target %d) for %s on a live "+
			"connection — server accepted frames but is not ACKing (wedged "+
			"server or incompatible build)",
		acked, target, stuckFor)
}

// qwpSfDrainerPool runs background tasks that send data left in orphan slots.
// Each foreground sender has one pool. At most max_background_drainers tasks
// run at once, each in its own goroutine; an unused pool starts no goroutines.
//
// Closing first asks the drainers to stop and waits up to
// qwpSfDrainerPoolCloseGrace. If work remains, it cancels their shared context
// and waits up to qwpSfDrainerPoolHardCloseGrace. These limits only end the wait.
// The pool keeps tracking unfinished engine cleanup, and QuestDB.Close checks
// that cleanup separately.
type qwpSfDrainerPool struct {
	maxConcurrent int
	sem           chan struct{}
	closed        atomic.Bool
	wg            sync.WaitGroup
	logger        *slog.Logger // nil -> slog.Default() via qwpEffectiveLogger

	// ctx is the master context handed to every drainerRun call.
	// Cancelled in drainerPoolClose so dials and other ctx-aware
	// blocking calls unwind. Independent of the caller's setup
	// ctx — drainers are long-lived and must outlive whatever
	// transient ctx was used to construct the parent sender.
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	active     []*qwpSfOrphanDrainer
	cleanupErr error
	failed     []*qwpSfOrphanDrainer
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
// Returns an error if the pool has been closed or ctx is already cancelled
// when the submission is accepted under the pool lock.
//
// Drainers queue when the concurrency cap is reached. Once accepted, both
// queued and running drainers belong to the pool's master context, so they
// outlive the caller's setup context and are stopped by drainerPoolClose.
func (p *qwpSfDrainerPool) drainerPoolSubmit(ctx context.Context, d *qwpSfOrphanDrainer) error {
	if p.closed.Load() {
		return errors.New("qwp/sf: drainer pool closed")
	}
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return errors.New("qwp/sf: drainer pool closed")
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return err
	}
	p.active = append(p.active, d)
	p.wg.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				p.failTask(d, r)
				return
			}
			if err := qwpRunCleanupPhaseGuarded("drainer result", func() error { p.removeActive(d); return nil }); err != nil {
				p.failTask(d, err)
			}
		}()
		// Accepted work stays queued until capacity is available or the pool
		// closes, even if the caller cancels its setup context in the meantime.
		select {
		case p.sem <- struct{}{}:
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
		// Finishing the send loop does not mean the slot's resources are freed.
		// Keep tracking this drainer until engine cleanup finishes or fails
		// permanently with its resources still held.
		d.waitCleanup()
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
			err := d.cleanupResult()
			failed := errors.Is(err, ErrCleanupFailed)
			p.cleanupErr = errors.Join(p.cleanupErr, err)
			if failed {
				p.failed = append(p.failed, d)
				p.retainFailureLocked()
			}
			n := len(p.active)
			p.active[i] = p.active[n-1]
			p.active[n-1] = nil
			p.active = p.active[:n-1]
			return
		}
	}
}

// drainerPoolSnapshot includes drainers waiting to start, still running, or
// waiting for engine cleanup. Keep tracking them after sending stops if their
// engine still has resources to release.
func (p *qwpSfDrainerPool) drainerPoolSnapshot() []*qwpSfOrphanDrainer {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*qwpSfOrphanDrainer, len(p.active))
	copy(out, p.active)
	return out
}

// activeCount includes drainers still waiting for their engine's cleanup.
func (p *qwpSfDrainerPool) activeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.active)
}

// drainerPoolClose refuses new tasks and asks each drainer to stop. It waits
// up to qwpSfDrainerPoolCloseGrace, then cancels their shared context and waits
// up to qwpSfDrainerPoolHardCloseGrace. Drainers that still have work or cleanup
// left remain tracked after this method returns. Later calls do not restart
// shutdown; cleanupResult reports whether work remains and any errors.
func (p *qwpSfDrainerPool) drainerPoolClose() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	// Install cancellation before any operation that can fault. Once closed is
	// published no later caller may enter this body, so normal fallthrough is
	// not a safe owner for the only signal that unwinds blocking drainer I/O.
	defer p.cancel()
	defer func() {
		if r := recover(); r != nil {
			p.mu.Lock()
			p.cleanupErr = errors.Join(p.cleanupErr, &qwpCleanupPanicError{phase: "drainer pool close", cause: r, stack: debug.Stack()})
			p.retainFailureLocked()
			p.mu.Unlock()
		}
	}()
	if hook := qwpTestDrainerPoolCloseHook.Load(); hook != nil {
		(*hook)()
	}
	p.mu.Lock()
	active := append([]*qwpSfOrphanDrainer(nil), p.active...)
	p.mu.Unlock()
	for _, d := range active {
		d.drainerRequestStop()
	}
	doneCh := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(doneCh)
	}()
	graceTimer := time.NewTimer(qwpSfDrainerPoolCloseGrace.load())
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
		hardTimer := time.NewTimer(qwpSfDrainerPoolHardCloseGrace.load())
		defer hardTimer.Stop()
		select {
		case <-doneCh:
			// Cancellation unwound the straggler(s).
		case <-hardTimer.C:
			// Stop waiting, but keep the drainer and engine alive until cleanup
			// finishes, even if they are stuck in a system call.
			go qwpEffectiveLogger(p.logger).Warn("qwp/sf: orphan cleanup still pending; resources remain owned until actual release",
				"count", p.activeCount(),
				"grace", qwpSfDrainerPoolCloseGrace.load()+qwpSfDrainerPoolHardCloseGrace.load())
		}
	}
}

// qwpTestDrainerPoolCloseHook fires after the pool has published closed and
// installed its cancellation obligation. Test seam only: it proves a panic in
// the remaining close body still cancels the master context. Nil in production.
var qwpTestDrainerPoolCloseHook atomic.Pointer[func()]

// qwpSfTestAfterDrainerEngineOpenHook gives tests access to the engine so they
// can wait for its cleanup worker to stop before removing the test directory.
var qwpSfTestAfterDrainerEngineOpenHook atomic.Pointer[func(*qwpSfCursorEngine)]
