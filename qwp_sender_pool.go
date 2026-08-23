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
	"math/big"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// qwpSenderPool is the elastic QWP ingest pool behind the QuestDB facade. It
// pools connected QWP LineSenders over one cluster config: borrow leases one
// for the caller's lifetime; Close returns it. It keeps minSize warm, grows to
// maxSize on demand, and reaps idle/over-age slots back to minSize.
//
// Store-and-forward (sf_dir set) is supported with one twist: each slot
// gets a distinct sender_id <base>-<index> so concurrent senders never
// collide on a slot dir (Hazard A), and every pooled sender fences the
// pool's whole in-range slot set out of orphan adoption so a live sibling is
// never drained (Hazard G). Crash-stranded in-range slots are recovered by
// binding a normal async sender to each at construction (the Go sender
// self-recovers its dir) — no dedicated recoverer, build never blocks.
//
// Leases are generation-stamped: a stale handle from a returned-then-reborrowed
// slot cannot write into or double-return a different borrow (Hazard B).
type qwpSenderPool struct {
	mu sync.Mutex
	// notify is closed to wake every acquire waiter; recreated under mu after
	// each broadcast. The cond-via-channel idiom gives acquire a timed wait
	// (sync.Cond has none). waiters counts goroutines parked on notify so a
	// broadcast with nobody listening skips the close+realloc.
	notify  chan struct{}
	waiters int

	all       []*qwpSenderSlot
	available []*qwpSenderSlot

	minSize, maxSize int
	acquireTimeout   time.Duration
	idleTimeout      time.Duration
	maxLifetime      time.Duration

	inFlightCreations int // memory-mode metric and close-wait optimization
	closed            bool
	closeStarted      bool
	closeDone         chan struct{}
	// closing is set before the housekeeper is stopped so an about-to-start reap
	// bails immediately instead of racing teardown. Distinct from closed, which
	// close() sets after the housekeeper has joined.
	closing atomic.Bool

	baseConf           string
	errorHandler       SenderErrorHandler
	connectionListener SenderConnectionListener
	drainerListener    QwpBackgroundDrainerListener
	logger             *slog.Logger

	// Store-and-forward coordination (storeAndForward true iff sfDir != "").
	storeAndForward bool
	sfDir           string
	slotBase        string
	sfSlots         []qwpSfSlotLifecycle
	retiredSlots    []*qwpSenderSlot

	// pendingLeaseTeardowns counts delegate teardowns currently running on
	// returning borrowers' goroutines (giveBack's closed branch). close()
	// counts these as outstanding so it does not return while a delegate is
	// still being torn down on another goroutine. Guarded by mu.
	pendingLeaseTeardowns int

	// closeTeardownErr is the first close() pass's teardown error, kept so a
	// repeat close() — which only re-probes the retired slots — reports it
	// again instead of silently downgrading a real failure to nil. Guarded by
	// mu, written once.
	closeTeardownErr error

	// poisonedErr is the terminal pool error recorded by the first panic in a
	// slot-classification pass (see poisonLocked). Once set, borrow refuses,
	// giveBack and close report it, and the reap/reprobe passes stand down.
	// Guarded by mu, written once.
	poisonedErr error
}

// qwpSfSlotState is the sole authority for both SF pool capacity and shutdown
// obligations. Every in-range slot index owns exactly one record, and every
// transition happens while p.mu is held.
type qwpSfSlotState uint8

const (
	qwpSfSlotFree qwpSfSlotState = iota
	qwpSfSlotCreating
	qwpSfSlotAvailable
	qwpSfSlotLeased
	qwpSfSlotClosing
	qwpSfSlotRetired
)

func (s qwpSfSlotState) String() string {
	switch s {
	case qwpSfSlotFree:
		return "free"
	case qwpSfSlotCreating:
		return "creating"
	case qwpSfSlotAvailable:
		return "available"
	case qwpSfSlotLeased:
		return "leased"
	case qwpSfSlotClosing:
		return "closing"
	case qwpSfSlotRetired:
		return "retired"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

type qwpSfSlotLifecycle struct {
	state qwpSfSlotState
}

type qwpSfSlotObligation struct {
	index int
	state qwpSfSlotState
}

// ErrSfCleanupPending is the sentinel a store-and-forward pool close wraps when
// one or more slot lifecycle records are not free. The obligation may be an
// outstanding lease, an in-flight construction that could still acquire a
// flock, an active teardown, or cleanup owned by a per-engine retry goroutine.
// Return leases and call Close again: each call re-probes and takes a fresh
// snapshot. Once Close returns nil, all pool-managed flocks are released and
// later calls remain nil. Stable teardown errors are reported alongside this
// sentinel and remain reportable. Match it with errors.Is.
//
// A failed Connect / NewQuestDB / NewLineSender can also wrap it, from the
// teardown of what the build had already created. There is no handle to call
// Close on in that case: the retry owner still releases the lock on its own,
// but an immediate rebuild on the same sf_dir and sender_id may fail naming
// this process as the holder, so retry the build rather than treating the
// sentinel as fatal.
var ErrSfCleanupPending = errors.New("qwp pool: SF slot cleanup still pending; lifecycle obligations remain")

// qwpPoolCloseResult combines the stable teardown/poison errors with one live
// lifecycle snapshot. The snapshot carries both the indices and states of all
// obligations that still could acquire or retain a slot flock.
func qwpPoolCloseResult(teardownErr, poisonErr error, pending []qwpSfSlotObligation) error {
	if poisonErr != nil {
		if teardownErr != nil {
			teardownErr = errors.Join(poisonErr, teardownErr)
		} else {
			teardownErr = poisonErr
		}
	}
	if len(pending) == 0 {
		return teardownErr
	}
	pendingErr := fmt.Errorf("%w (%d slot(s): %v)", ErrSfCleanupPending, len(pending), pending)
	if teardownErr != nil {
		return errors.Join(teardownErr, pendingErr)
	}
	return pendingErr
}

// qwpPoolMaxCloseLeaseWait hard-caps close()'s outstanding-lease wait. The
// acquire timeout is a BORROW policy and must never unbound SHUTDOWN: without
// this cap a forgotten lease would hang close() for the whole acquire budget.
const qwpPoolMaxCloseLeaseWait = 5 * time.Second

// qwpSenderSlot is one reusable pool entry. generation is bumped under the pool
// lock on every hand-out and every return; a lease carries the value it was
// issued, so a stale lease (slot already re-borrowed) is detectable.
type qwpSenderSlot struct {
	delegate   QwpSender
	cleanup    closeLifecycleReporter
	generation atomic.Uint64
	slotIndex  int // SF slot index, or -1 in memory mode
	createdAt  time.Time
	idleSince  time.Time
}

// qwpMaxPoolSize caps sender_pool_max. A larger value is a misconfiguration: in
// SF mode the pool allocates a maxSize-entry slot table and stats every slot
// dir in [0, maxSize) at construction, so an outsized value (e.g. 100000000)
// would allocate a huge slice and hang the launch.
const qwpMaxPoolSize = 10000

// newQwpSenderPool parses the cluster config, prewarms minSize senders, and (in
// SF mode) recovers any crash-stranded in-range slots on async senders. Returns
// an error only on a malformed config or a failed eager prewarm; SF recovery
// senders connect async so a down server never fails construction.
func newQwpSenderPool(
	ctx context.Context,
	conf string,
	minSize, maxSize int,
	acquireTimeout, idleTimeout, maxLifetime time.Duration,
	errorHandler SenderErrorHandler,
	connectionListener SenderConnectionListener,
	drainerListener QwpBackgroundDrainerListener,
	logger *slog.Logger,
) (*qwpSenderPool, error) {
	if minSize < 0 || maxSize < 1 || minSize > maxSize {
		return nil, fmt.Errorf("qwp pool: invalid sizes min=%d max=%d (max defaults to %d when unset — raise sender_pool_max alongside min)", minSize, maxSize, qwpDefaultPoolMax)
	}
	if maxSize > qwpMaxPoolSize {
		return nil, fmt.Errorf("qwp pool: sender_pool_max=%d exceeds the maximum %d; a pool this large is a misconfiguration (SF mode would allocate a %d-entry slot table and stat that many slot dirs at startup)", maxSize, qwpMaxPoolSize, maxSize)
	}
	template, err := confFromStr(conf)
	if err != nil {
		return nil, err
	}
	if template.senderType != qwpSenderType {
		return nil, errors.New("qwp pool: only ws/wss schemas are supported")
	}

	p := &qwpSenderPool{
		notify:             make(chan struct{}),
		minSize:            minSize,
		maxSize:            maxSize,
		acquireTimeout:     acquireTimeout,
		idleTimeout:        idleTimeout,
		maxLifetime:        maxLifetime,
		baseConf:           conf,
		errorHandler:       errorHandler,
		connectionListener: connectionListener,
		drainerListener:    drainerListener,
		logger:             logger,
		storeAndForward:    template.sfDir != "",
		sfDir:              template.sfDir,
	}
	if p.storeAndForward {
		p.slotBase = template.senderId
		if p.slotBase == "" {
			p.slotBase = qwpSfDefaultSenderId
		}
		p.sfSlots = make([]qwpSfSlotLifecycle, maxSize)
	}

	// Eagerly prewarm minSize slots. A prewarm failure tears down whatever was
	// built and propagates (a misconfig or down server with a fail-fast
	// connect mode should surface at build()).
	for i := 0; i < minSize; i++ {
		slot, err := p.createSlot(ctx, false)
		if err != nil {
			// Join the unwind close's error rather than drop it: it can be
			// ErrSfCleanupPending, and this pool is about to become
			// unreachable, so this is the only place the caller can learn
			// that a slot lock survives the failed build and that an
			// immediate retry on the same sf_dir may name this process as
			// the holder. The engine's own retry owner keeps releasing it.
			if closeErr := p.close(ctx); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return nil, err
		}
		p.all = append(p.all, slot)
		p.available = append(p.available, slot)
	}

	// SF crash recovery: bind an async self-recovering sender to every in-range
	// slot that still holds unacked data and isn't already owned by a prewarmed
	// slot. Async so build never blocks on a down server (Hazard D/H).
	if p.storeAndForward {
		p.recoverStrandedSlots(ctx)
	}
	return p, nil
}

// recoverStrandedSlots scans <sfDir>/<base>-<i> for i in [0,maxSize) and binds
// an async sender to each that holds unacked data but no live owner.
//
// The pool is not published while this scan runs, but it still uses the normal
// locked lifecycle transitions so construction does not have a second source
// of truth for reservations.
func (p *qwpSenderPool) recoverStrandedSlots(ctx context.Context) {
	for i := 0; i < p.maxSize; i++ {
		p.mu.Lock()
		free := p.sfSlots[i].state == qwpSfSlotFree
		p.mu.Unlock()
		if !free {
			continue // already owned by a prewarmed slot
		}
		dir := filepath.Join(p.sfDir, p.slotBase+"-"+strconv.Itoa(i))
		if !qwpSfIsCandidateOrphan(dir) {
			continue
		}
		p.mu.Lock()
		if p.sfSlots[i].state != qwpSfSlotFree {
			p.mu.Unlock()
			continue
		}
		p.transitionSfSlotLocked(i, qwpSfSlotFree, qwpSfSlotCreating)
		p.mu.Unlock()
		slot, err := p.createSlotAt(ctx, i, true)
		p.mu.Lock()
		if err != nil {
			p.reclaimFailedBuildLocked(slot, i, err)
			p.mu.Unlock()
			continue // best-effort; the dir's data stays on disk for next start
		}
		p.transitionSfSlotLocked(i, qwpSfSlotCreating, qwpSfSlotAvailable)
		p.all = append(p.all, slot)
		p.available = append(p.available, slot)
		p.mu.Unlock()
	}
}

// borrow leases a sender, blocking up to acquireTimeout (or ctx) when the pool
// is at capacity. The returned LineSender must be Close()d to return it; the
// real disconnect happens only at pool close.
func (p *qwpSenderPool) borrow(ctx context.Context) (LineSender, error) {
	deadline := time.Now().Add(p.acquireTimeout)
	p.mu.Lock()
	for {
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		if err := p.poisonedErr; err != nil {
			p.mu.Unlock()
			return nil, err
		}
		// A disabled housekeeper must not make successfully deferred cleanup a
		// permanent capacity loss. Retry/reprobe retired slots outside p.mu before
		// deciding the pool is full; if cleanup is still owned, the helper is a
		// cheap observation only.
		if p.sfSlotStateCountLocked(qwpSfSlotRetired) > 0 {
			p.mu.Unlock()
			p.reprobeRetiredSlots()
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				return nil, errPoolClosed
			}
			if err := p.poisonedErr; err != nil {
				p.mu.Unlock()
				return nil, err
			}
		}
		if n := len(p.available); n > 0 {
			slot := p.available[n-1]
			p.available = p.available[:n-1]
			// A slot's background send loop keeps running while it sits idle and
			// can terminally HALT with no lease watching (reconnect-budget
			// exhaustion after a long outage, PROTOCOL_VIOLATION close, AUTH_FAILED
			// on reconnect, incompatible-server strike). Handing such a poisoned
			// slot to the next borrower would fail it through no fault of its own
			// and break borrower isolation precisely during incident recovery, so
			// discard it and look for another.
			if slotTerminallyFailed(slot.delegate) {
				p.discardLocked(slot) // releases p.mu
				p.mu.Lock()
				continue
			}
			p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotAvailable, qwpSfSlotLeased)
			gen := slot.generation.Add(1)
			p.mu.Unlock()
			return &qwpPooledSender{pool: p, slot: slot, gen: gen}, nil
		}
		if p.capUsedLocked() < p.maxSize {
			// A waiter woken by the acquire timer can reach here with the
			// deadline already past; report pool exhaustion rather than
			// starting a dial doomed by an expired context.
			if time.Until(deadline) <= 0 {
				p.mu.Unlock()
				return nil, fmt.Errorf("%w after %s", errPoolExhausted, p.acquireTimeout)
			}
			// Honor a cancelled borrow ctx explicitly: the async growth build no
			// longer surfaces it via a synchronous dial.
			if err := ctx.Err(); err != nil {
				p.mu.Unlock()
				return nil, err
			}
			p.inFlightCreations++
			slotIndex := -1
			if p.storeAndForward {
				slotIndex = p.allocateSlotIndexLocked()
			}
			p.mu.Unlock()
			// Grow asynchronously: the pool is already running, so a growth
			// borrow during a transient outage must not hard-fail — it buffers
			// via store-and-forward and connects in the background (Invariant B).
			// Only the min-prewarm at build follows the configured connect mode
			// (a down server there is a startup error).
			//
			// createSlotAt's heavy work (SF segment open/recovery) takes no ctx
			// and can block indefinitely on a wedged sf_dir mount, so the deadline
			// cannot bound it in-line — run it on a helper goroutine and abandon it
			// at the deadline. A late-completing build settles itself through
			// settleGrowthBuild: its slot lands in `available` (or is torn down if
			// the pool closed meanwhile), and inFlightCreations stays counted until
			// then so close()'s wait and the cap check both see it.
			bctx, cancel := context.WithDeadline(ctx, deadline)
			resultCh := make(chan *qwpSenderSlot, 1)
			errCh := make(chan error, 1)
			go func() {
				slot, err := p.createSlotAt(bctx, slotIndex, true)
				resultCh <- slot
				errCh <- err
			}()
			timer := time.NewTimer(time.Until(deadline))
			select {
			case slot := <-resultCh:
				timer.Stop()
				cancel()
				err := <-errCh
				p.mu.Lock()
				p.inFlightCreations--
				if err != nil {
					p.reclaimFailedBuildLocked(slot, slotIndex, err)
					p.broadcastLocked()
					p.mu.Unlock()
					return nil, err
				}
				if p.closed {
					// close() left this just-built slot for us. Track the teardown and
					// keep the SF slot counted as closing so close()'s outstanding-count
					// wait cannot return while the delegate — and its SF flock — is still
					// closing, then free the index only after the close completes. Mirrors
					// giveBack's closed branch; freeing the index before the off-lock close
					// briefly stranded the flock on an index a reopen could reuse.
					p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotCreating, qwpSfSlotClosing)
					p.pendingLeaseTeardowns++
					p.mu.Unlock()
					// Disconnect off-lock with a panic guard and a background ctx — a
					// cancelled caller ctx must not cut this close short, matching every
					// other close site in the pool.
					_ = closeSlotGuarded(context.Background(), slot.delegate)
					p.mu.Lock()
					p.pendingLeaseTeardowns--
					p.reclaimSlotLocked(slot, nil)
					p.broadcastLocked()
					p.mu.Unlock()
					return nil, errPoolClosed
				}
				p.all = append(p.all, slot)
				p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotCreating, qwpSfSlotLeased)
				gen := slot.generation.Add(1)
				p.mu.Unlock()
				return &qwpPooledSender{pool: p, slot: slot, gen: gen}, nil
			case <-timer.C:
				go p.settleGrowthBuild(resultCh, errCh, cancel, slotIndex)
				return nil, fmt.Errorf("%w after %s", errPoolExhausted, p.acquireTimeout)
			case <-ctx.Done():
				timer.Stop()
				go p.settleGrowthBuild(resultCh, errCh, cancel, slotIndex)
				return nil, ctx.Err()
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			p.mu.Unlock()
			return nil, fmt.Errorf("%w after %s", errPoolExhausted, p.acquireTimeout)
		}
		p.waiters++
		ch := p.notify
		// Engine completion has no pool-lock callback. Poll only while retired
		// capacity exists so a disabled housekeeper still notices it promptly;
		// the ordinary pool wait remains notification-driven.
		waitFor := remaining
		if p.sfSlotStateCountLocked(qwpSfSlotRetired) > 0 && waitFor > 10*time.Millisecond {
			waitFor = 10 * time.Millisecond
		}
		p.mu.Unlock()
		timer := time.NewTimer(waitFor)
		select {
		case <-ch:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			p.mu.Lock()
			p.waiters--
			p.mu.Unlock()
			return nil, ctx.Err()
		}
		timer.Stop()
		p.mu.Lock()
		p.waiters--
	}
}

// settleGrowthBuild resolves a growth build that outran its borrow's acquire
// deadline (borrow abandoned it and already returned a timeout). It waits for
// the build to complete, decrements inFlightCreations (kept counted until now so
// close()'s wait and the cap check saw the pending build), and disposes of the
// result: a build error frees the reserved index; a successful slot is added to
// `available` for the next borrower unless the pool closed meanwhile, in which
// case it is torn down through the same tracked path borrow's closed-race uses.
// Runs on its own goroutine so a wedged sf_dir open never pins the borrower.
func (p *qwpSenderPool) settleGrowthBuild(resultCh <-chan *qwpSenderSlot, errCh <-chan error, cancel context.CancelFunc, slotIndex int) {
	slot := <-resultCh
	err := <-errCh
	cancel()
	p.mu.Lock()
	p.inFlightCreations--
	if err != nil {
		p.reclaimFailedBuildLocked(slot, slotIndex, err)
		p.broadcastLocked()
		p.mu.Unlock()
		return
	}
	if p.closed {
		p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotCreating, qwpSfSlotClosing)
		p.pendingLeaseTeardowns++
		p.mu.Unlock()
		_ = closeSlotGuarded(context.Background(), slot.delegate)
		p.mu.Lock()
		p.pendingLeaseTeardowns--
		p.reclaimSlotLocked(slot, nil)
		p.broadcastLocked()
		p.mu.Unlock()
		return
	}
	slot.idleSince = time.Now()
	p.all = append(p.all, slot)
	p.available = append(p.available, slot)
	p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotCreating, qwpSfSlotAvailable)
	p.broadcastLocked()
	p.mu.Unlock()
}

// giveBack returns a slot to the available set, dropping the return if the
// lease is stale (already returned and possibly re-borrowed) or the pool is
// closed — this is what makes lease Close idempotent under a concurrent
// re-borrow (Hazard B). Broken slots are discarded instead of recycled.
// giveBack returns a lease's slot to the pool. Its error is the pool's
// poisoned verdict (see poisonLocked), surfaced so a producer returning a
// lease learns the pool is dead; the return itself still completes — the slot
// is recycled, discarded or torn down exactly as when the pool is healthy,
// since the poisoned state left the pool's bookkeeping intact.
func (p *qwpSenderPool) giveBack(ctx context.Context, ps *qwpPooledSender, broken bool) error {
	p.mu.Lock()
	poisonErr := p.poisonedErr
	if ps.slot.generation.Load() != ps.gen {
		p.mu.Unlock()
		return poisonErr // stale lease (already returned / re-borrowed) — never double-act
	}
	// Invalidate this lease so a duplicate Close is dropped above.
	ps.slot.generation.Add(1)
	if p.closed {
		// The pool was torn down while this slot was on loan, so close() left it
		// for us: close() never tears down a borrowed delegate (a producer
		// goroutine may be inside it mid-append). The producer is done now,
		// so closing the delegate here cannot race a writer. Track the teardown
		// so a concurrent close() does not return while it is in flight.
		p.removeFromAllLocked(ps.slot)
		p.transitionSfSlotLocked(ps.slot.slotIndex, qwpSfSlotLeased, qwpSfSlotClosing)
		p.pendingLeaseTeardowns++
		p.mu.Unlock()
		_ = closeSlotGuarded(ctx, ps.slot.delegate)
		p.mu.Lock()
		p.pendingLeaseTeardowns--
		p.reclaimSlotLocked(ps.slot, nil)
		p.broadcastLocked()
		p.mu.Unlock()
		return poisonErr
	}
	if broken {
		p.discardLocked(ps.slot)
		return poisonErr // discardLocked unlocks
	}
	ps.slot.idleSince = time.Now()
	p.available = append(p.available, ps.slot)
	p.transitionSfSlotLocked(ps.slot.slotIndex, qwpSfSlotLeased, qwpSfSlotAvailable)
	p.broadcastLocked()
	p.mu.Unlock()
	return poisonErr
}

// terminalReporter lets the pool detect a delegate whose background send loop
// has terminally HALTed without growing the public QwpSender interface. Every
// pooled delegate is a *qwpLineSender, which implements it.
type terminalReporter interface {
	terminallyFailed() bool
}

// slotTerminallyFailed reports whether a slot's delegate has latched a terminal
// error (a background HALT). A poisoned slot must never be handed to a borrower
// nor recycled after a producer error — it is discarded and rebuilt
// instead. Mirrors the Query lease's client.terminalError() check.
func slotTerminallyFailed(delegate QwpSender) bool {
	tr, ok := delegate.(terminalReporter)
	return ok && tr.terminallyFailed()
}

// unackedReporter reports whether a delegate still holds published-but-unacked
// rows. Every pooled delegate is a *qwpLineSender, which implements it.
type unackedReporter interface {
	hasUnackedRows() bool
}

// returnFlusher lets the pool flush a delegate for return without growing the
// public QwpSender interface: it drops the in-progress (un-At'd) row, flushes
// committed rows, and surfaces (without discarding those rows) a latched
// fluent-API error — the correct lease-return path, distinct from Flush.
// The returned retained bool reports whether committed rows were left
// un-enqueued (a backpressure-deadline / engine-closed failure, not a terminal
// HALT); such a slot is dirty and must be discarded, not recycled.
// Every pooled delegate is a *qwpLineSender, which implements it.
type returnFlusher interface {
	flushForReturn(ctx context.Context) (retained bool, err error)
}

// closeCompletionReporter is implemented by the concrete QWP sender. Close
// may safely return before its manager worker exits; in that case the delegate
// and slot index must remain retired until the worker publishes completion.
type closeCompletionReporter interface {
	closeCompleted() bool
}

type closeRetryReporter interface {
	retryCloseIfNeeded() error
}

type closeRetryOwner interface {
	ensureCloseRetryOwner(logger *slog.Logger)
}

type closeLifecycleReporter interface {
	closeCompletionReporter
	closeRetryReporter
	closeRetryOwner
}

func slotCloseCompleted(slot *qwpSenderSlot) bool {
	return slot == nil || slot.cleanup == nil || slot.cleanup.closeCompleted()
}

// slotHasUnackedRows reports whether an idle slot's delegate still has in-flight
// rows the server has not acknowledged. Reaping such a memory-mode slot would
// destroy those rows and cut short the reconnect/replay window that could still
// deliver them, so the reaper spares it.
func slotHasUnackedRows(delegate QwpSender) bool {
	ur, ok := delegate.(unackedReporter)
	return ok && ur.hasUnackedRows()
}

// closeSlotGuarded closes a delegate, converting a panic into an error so a
// faulting Close cannot strand the pool's SF slot lifecycle or skip sibling
// teardowns. qwpLineSender.Close is itself panic-guarded on its I/O goroutines;
// this is defense-in-depth for lifecycle transitions that bracket off-lock
// Close calls.
func closeSlotGuarded(ctx context.Context, delegate LineSender) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp pool: delegate close panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return delegate.Close(ctx)
}

// discardLocked evicts a broken slot from `all` and closes its delegate outside
// the lock. Caller holds mu; discardLocked releases it. The delegate is closed
// with context.Background(): discarding a broken slot is incidental teardown
// that a cancelled caller ctx must not cut short, matching reapIdle and the
// borrow closed-race close site (the discarded slot is terminally failed, so
// its drain returns immediately regardless).
//
// The off-lock close is counted in pendingLeaseTeardowns: the slot is already
// out of `all`, so without it close()'s outstanding count would not see this
// teardown and could return while the delegate still holds its SF flock.
func (p *qwpSenderPool) discardLocked(slot *qwpSenderSlot) {
	p.removeFromAllLocked(slot)
	p.transitionSfSlotToClosingLocked(slot.slotIndex)
	p.pendingLeaseTeardowns++
	p.mu.Unlock()
	closeErr := closeSlotGuarded(context.Background(), slot.delegate)
	p.mu.Lock()
	p.pendingLeaseTeardowns--
	p.reclaimSlotLocked(slot, closeErr)
	p.broadcastLocked()
	p.mu.Unlock()
}

// markClosing signals reapIdle to bail. Called before the housekeeper is
// stopped so a not-yet-started reap does not run during the stop-and-join window.
func (p *qwpSenderPool) markClosing() { p.closing.Store(true) }

// reapCloseHook, when non-nil, is invoked at the start of each reap-victim
// close goroutine. Test seam only: it lets a test hold a reap teardown in
// flight to assert close() waits for it. Nil in production.
var reapCloseHook atomic.Pointer[func()]

// createSlotHook, when non-nil, is invoked at the start of createSlotAt's build.
// Test seam only: it lets a test wedge a growth build past the acquire deadline
// to assert the borrow abandons it and settleGrowthBuild reclaims the slot. Nil
// in production.
var createSlotHook atomic.Pointer[func()]

// reapIdle closes idle-expired / over-age slots from the available set, never
// shrinking below minSize. Called by the housekeeper.
func (p *qwpSenderPool) reapIdle() {
	if p.closing.Load() {
		return
	}
	p.reprobeRetiredSlots()
	toClose := p.selectReapVictims(time.Now())
	if len(toClose) == 0 {
		return
	}
	// Close concurrently so an N-slot sweep during an outage is bounded by one
	// close_flush_timeout, not N — otherwise it overruns the housekeeper join
	// budget and the reap outlives QuestDB.Close, holding SF flocks past return.
	var wg sync.WaitGroup
	errs := make([]error, len(toClose))
	for i, slot := range toClose {
		wg.Add(1)
		go func(i int, delegate QwpSender) {
			defer wg.Done()
			if hook := reapCloseHook.Load(); hook != nil {
				(*hook)()
			}
			errs[i] = closeSlotGuarded(context.Background(), delegate)
		}(i, slot.delegate)
	}
	wg.Wait()
	p.mu.Lock()
	var reapErrs []error
	for i, slot := range toClose {
		if errs[i] != nil {
			// Collect under the lock, log after Unlock: the logger is the user's
			// slog handler, so running it while holding p.mu would let a slow or
			// panicking handler stall (or, behind the housekeeper's log-only
			// recover, strand) the pool lock.
			reapErrs = append(reapErrs, errs[i])
		}
		// Balance the per-victim increment in selectReapVictims now that the
		// teardown has completed, so close()'s outstanding wait no longer counts it.
		p.pendingLeaseTeardowns--
		p.reclaimSlotLocked(slot, errs[i])
	}
	p.broadcastLocked()
	p.mu.Unlock()
	for _, err := range reapErrs {
		qwpEffectiveLogger(p.logger).Warn("qwp pool: reaping a slot failed to drain cleanly", "error", err)
	}
}

// qwpSlotPartition is the outcome of classifying one of the pool's slot lists:
// the slots that stay, the slots that leave, and the counter deltas the
// departure implies. Classification — which calls into each delegate and can
// fault — builds the whole partition before any pool state is written;
// applySlotPartitionLocked then publishes it in one uninterruptible step.
type qwpSlotPartition struct {
	kept                       []*qwpSenderSlot
	removed                    []*qwpSenderSlot
	pendingLeaseTeardownsDelta int
	removeFromAll              bool
	transitionSf               bool
	sfFrom                     qwpSfSlotState
	sfTo                       qwpSfSlotState
}

// applySlotPartitionLocked publishes a classified partition: it installs the
// kept list into *target, drops or releases the removed slots as the partition
// directs, and applies the counter deltas. Both classify-then-apply sites (the
// idle reap and the retired-slot reprobe) go through here, so the
// all-or-nothing update discipline lives in one function instead of being
// hand-copied per site. The body is slice-header and integer assignments plus
// bounds-checked bitmap writes — nothing in it can panic — so once
// classification has finished, the pool's state takes the whole partition or,
// if classification faulted before reaching here, none of it. Caller holds mu.
func (p *qwpSenderPool) applySlotPartitionLocked(target *[]*qwpSenderSlot, part qwpSlotPartition) {
	if p.storeAndForward && part.transitionSf {
		// Validate the entire lifecycle batch before publishing any slice or
		// state change, preserving classify/apply's all-or-nothing contract.
		for _, slot := range part.removed {
			p.requireSfSlotStateLocked(slot.slotIndex, part.sfFrom)
		}
	}
	for _, slot := range part.removed {
		if part.removeFromAll {
			p.removeFromAllLocked(slot)
		}
		if part.transitionSf {
			p.transitionSfSlotLocked(slot.slotIndex, part.sfFrom, part.sfTo)
		}
	}
	p.pendingLeaseTeardowns += part.pendingLeaseTeardownsDelta
	*target = part.kept
}

// poisonLocked records the first classification panic as the pool's terminal
// error. A faulting classification predicate is a client bug, and the pool
// cannot tell which of its slots the fault has made untrustworthy — a pool
// that kept lending after one could hand a single delegate to two goroutines.
// Poisoning converts that residual risk from silent corruption into a loud
// outage: borrow refuses, giveBack and close report the error, and the
// reap/reprobe passes stand down. Caller holds mu.
func (p *qwpSenderPool) poisonLocked(op string, panicValue any) {
	if p.poisonedErr == nil {
		p.poisonedErr = fmt.Errorf("%w: %s panicked: %v", ErrPoolPoisoned, op, panicValue)
	}
}

// selectReapVictims removes the idle-expired / over-age / poisoned slots from
// the available set under the lock and returns them for off-lock closing.
// Classification and apply are split: classifyReapVictimsLocked builds the
// whole partition without writing any pool state, and applySlotPartitionLocked
// publishes it in one step that cannot fault. A panic during classification
// therefore leaves the pool byte-for-byte unchanged; it is recovered there and
// poisons the pool (see poisonLocked). The lock is released via defer so no
// exit can strand it.
func (p *qwpSenderPool) selectReapVictims(now time.Time) []*qwpSenderSlot {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.poisonedErr != nil {
		return nil
	}
	part, ok := p.classifyReapVictimsLocked(now)
	if !ok {
		return nil
	}
	p.applySlotPartitionLocked(&p.available, part)
	return part.removed
}

// classifyReapVictimsLocked partitions the available set into keepers and reap
// victims. It calls into each delegate and writes no pool state: a panic in a
// predicate is recovered here, poisons the pool, and returns ok=false with the
// half-built partition discarded. The apply step transitions every SF victim
// from available to closing and increments pendingLeaseTeardowns so memory-mode
// close also sees the off-lock reap. reapIdle settles both after Close lands.
// Caller holds mu.
func (p *qwpSenderPool) classifyReapVictimsLocked(now time.Time) (part qwpSlotPartition, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			p.poisonLocked("reap-victim classification", r)
		}
	}()
	// The survivors go into a fresh slice rather than an in-place filter, so
	// p.available stays untouched until the apply step publishes the whole
	// partition. One allocation per housekeeper tick buys the all-or-nothing
	// property.
	part.kept = make([]*qwpSenderSlot, 0, len(p.available))
	part.removeFromAll = true
	part.transitionSf = true
	part.sfFrom = qwpSfSlotAvailable
	part.sfTo = qwpSfSlotClosing
	for _, slot := range p.available {
		idleExpired := p.idleTimeout > 0 && now.Sub(slot.idleSince) >= p.idleTimeout
		overAge := p.maxLifetime > 0 && now.Sub(slot.createdAt) >= p.maxLifetime
		// A slot poisoned by a background HALT is useless to a borrower, so reap
		// it even at minSize (borrow re-creates a fresh slot on demand). This
		// proactively clears poisoned slots so a wave of borrowers does not have
		// to discard them one by one during incident recovery.
		poisoned := slotTerminallyFailed(slot.delegate)
		// Idle/age recycling is floored at minSize, so max_lifetime_ms is inert
		// for min slots: no evict-and-replace (recreating mid-outage could
		// orphan unacked SF data); QWP self-reconnects anyway. See README.
		// Spare an idle/over-age slot that still has unacked rows: reaping it in
		// memory mode destroys them and cuts short the reconnect/replay window. A
		// poisoned slot is exempt — its HALT is terminal, so the rows are already
		// lost and holding the slot only wastes it.
		hasUnacked := !poisoned && slotHasUnackedRows(slot.delegate)
		// The apply step shrinks len(p.all) only after classification, so the
		// minSize floor subtracts the victims chosen so far to match reaping
		// them one at a time.
		if poisoned || ((idleExpired || overAge) && len(p.all)-len(part.removed) > p.minSize && !hasUnacked) {
			part.removed = append(part.removed, slot)
			part.pendingLeaseTeardownsDelta++
			continue
		}
		part.kept = append(part.kept, slot)
	}
	return part, true
}

// close shuts the pool down and disconnects its slots. Idempotent.
//
// It closes only the returned (available) slots here. A slot still on loan has a
// live producer goroutine, and closing its delegate concurrently would race that
// writer (its row buffer and table maps) — a data race the panic guards cannot
// catch. An outstanding lease instead closes its own delegate when it is returned
// (giveBack sees p.closed). A lease that is never returned leaks its connection:
// the caller must return every lease before QuestDB.Close.
//
// A BorrowSender whose slot build is still in flight is likewise closed by the
// creating goroutine after it observes p.closed. The lifecycle record remains
// creating until that happens, so even a build that has not acquired its flock
// keeps Close pending. A teardown that cannot prove manager quiescence moves to
// retired with a retry owner. Every Close call re-probes and reports a fresh
// snapshot; nil means all lifecycle records are free. A teardown error from the
// first pass is remembered and reported by every later call as well.
//
// Slots close concurrently on context.Background(): each drain is bounded by
// close_flush_timeout, so the caller's ctx must neither serialize the drains
// (an N-slot outage would stall shutdown for N × the timeout) nor cancel them
// (dropping undelivered memory-mode rows). Matches the lease-return and reap
// paths. The ctx argument is accepted for interface symmetry but unused.
func (p *qwpSenderPool) close(_ context.Context) error {
	p.mu.Lock()
	if p.closeStarted {
		done := p.closeDone
		p.mu.Unlock()
		<-done
		return p.currentCloseResult()
	}
	p.closeStarted = true
	p.closeDone = make(chan struct{})
	p.closed = true
	p.closing.Store(true)
	// Wake parked borrowers so they observe the shutdown and error out.
	p.broadcastLocked()

	// Bounded graceful wait for outstanding leases: close() NEVER tears down
	// a borrowed delegate — a producer goroutine may be inside it mid-append,
	// and closing it here would flush and free buffers under that
	// goroutine. giveBack observing `closed` tears each delegate down on the
	// returning borrower's goroutine instead (tracked via
	// pendingLeaseTeardowns so this method does not return while a teardown
	// is still in flight). The budget is the acquire timeout hard-capped at
	// qwpPoolMaxCloseLeaseWait: a huge acquire timeout is a borrow policy,
	// not a licence for close() to hang on a lease that never comes home.
	waitBudget := p.acquireTimeout
	if waitBudget > qwpPoolMaxCloseLeaseWait {
		waitBudget = qwpPoolMaxCloseLeaseWait
	}
	deadline := time.Now().Add(waitBudget)
	for {
		outstanding := p.closeWaitOutstandingLocked()
		if outstanding <= 0 {
			break
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		p.waiters++
		ch := p.notify
		p.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
		case <-timer.C:
		}
		timer.Stop()
		p.mu.Lock()
		p.waiters--
	}
	// A logged leak is recoverable; a freed buffer under a live producer is
	// not. The delegate is torn down whenever its lease finally returns
	// (giveBack's closed branch). Count under the lock, log after the unlock
	// below: the logger is the user's slog handler, and while the guarded
	// handler absorbs a panicking one, a merely slow one here would hold p.mu
	// for the whole call -- every later borrow, return, reprobe and repeat
	// close would wait on that lock, including the repeat-Close retry. The
	// off-lock placement matters for the same reason the site does: the slots
	// below are already out of p.all and in the closing lifecycle state, so
	// skipping the teardown loop would retain every flock with no retry owner
	// and no retiredSlots entry to re-probe.
	leaked := p.closeLeasedCountLocked()
	toClose := append([]*qwpSenderSlot(nil), p.available...)
	for _, slot := range toClose {
		p.removeFromAllLocked(slot)
		p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotAvailable, qwpSfSlotClosing)
	}
	p.available = nil
	p.broadcastLocked()
	p.mu.Unlock()
	if leaked > 0 {
		qwpEffectiveLogger(p.logger).Warn("qwp pool: close() leaving borrowed sender(s) alive; "+
			"each is torn down when its lease is closed", "leaked", leaked)
	}

	var wg sync.WaitGroup
	errs := make([]error, len(toClose))
	for i, slot := range toClose {
		wg.Add(1)
		go func(i int, delegate QwpSender) {
			defer wg.Done()
			errs[i] = closeSlotGuarded(context.Background(), delegate)
		}(i, slot.delegate)
	}
	wg.Wait()
	var teardownErr error
	for _, closeErr := range errs {
		teardownErr = qwpAppendCloseError(teardownErr, closeErr)
	}
	p.mu.Lock()
	for i, slot := range toClose {
		p.reclaimSlotLocked(slot, errs[i])
	}
	p.closeTeardownErr = teardownErr
	close(p.closeDone)
	p.broadcastLocked()
	p.mu.Unlock()
	return p.currentCloseResult()
}

// currentCloseResult re-probes retired delegates and combines the immutable
// first-pass result with a fresh lifecycle snapshot. It is the only pool-close
// status path, so no caller can accidentally report from cached counters.
func (p *qwpSenderPool) currentCloseResult() error {
	p.reprobeRetiredSlots()
	p.mu.Lock()
	pending := p.sfCloseSnapshotLocked()
	teardownErr := p.closeTeardownErr
	poisonErr := p.poisonedErr
	p.mu.Unlock()
	return qwpPoolCloseResult(teardownErr, poisonErr, pending)
}

// stableCloseResult returns only errors from the one-time teardown and pool
// poison. It deliberately excludes the live lifecycle snapshot.
func (p *qwpSenderPool) stableCloseResult() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return qwpPoolCloseResult(p.closeTeardownErr, p.poisonedErr, nil)
}

// createSlot allocates an SF slot index when needed and builds a slot.
//
// The sole caller is newQwpSenderPool's prewarm loop. It still takes p.mu for
// lifecycle changes so construction follows the same state rules as growth.
func (p *qwpSenderPool) createSlot(ctx context.Context, async bool) (*qwpSenderSlot, error) {
	slotIndex := -1
	p.mu.Lock()
	if p.storeAndForward {
		slotIndex = p.allocateSlotIndexLocked()
	}
	p.mu.Unlock()
	slot, err := p.createSlotAt(ctx, slotIndex, async)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.reclaimFailedBuildLocked(slot, slotIndex, err)
		return nil, err
	}
	p.transitionSfSlotLocked(slotIndex, qwpSfSlotCreating, qwpSfSlotAvailable)
	return slot, nil
}

// reclaimFailedBuild releases the capacity reservation left by a failed slot
// build. Memory-mode senders own no slot index or flock and therefore do not
// consume SF lifecycle capacity while cleanup completes.
func (p *qwpSenderPool) reclaimFailedBuild(slot *qwpSenderSlot, slotIndex int, buildErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reclaimFailedBuildLocked(slot, slotIndex, buildErr)
}

func (p *qwpSenderPool) reclaimFailedBuildLocked(slot *qwpSenderSlot, slotIndex int, buildErr error) {
	if p.storeAndForward && slot != nil && slot.slotIndex >= 0 && slot.cleanup != nil {
		p.transitionSfSlotLocked(slotIndex, qwpSfSlotCreating, qwpSfSlotClosing)
		p.reclaimSlotLocked(slot, buildErr)
		return
	}
	p.transitionSfSlotLocked(slotIndex, qwpSfSlotCreating, qwpSfSlotFree)
}

// createSlotAt builds a sender bound to slotIndex (-1 in memory mode). It parses
// a fresh config from the base string and overrides the per-slot identity, the
// in-range orphan fence, the ingest callbacks, and (for recovery) async connect.
// Guarded against a panic in the heavy build path (Hazard I).
func (p *qwpSenderPool) createSlotAt(ctx context.Context, slotIndex int, async bool) (slot *qwpSenderSlot, err error) {
	defer func() {
		if r := recover(); r != nil {
			// A panic that left an engine behind carries its cleanup reporter,
			// so the slot keeps its index reserved and is retired rather than
			// freed while that engine's retry owner still holds the flock.
			if bp, ok := r.(qwpSfBuildPanic); ok {
				slot = &qwpSenderSlot{cleanup: bp.reporter, slotIndex: slotIndex}
				err = fmt.Errorf("qwp pool: sender build panicked: %v\n%s", bp.cause, bp.stack)
				return
			}
			err = fmt.Errorf("qwp pool: sender build panicked: %v", r)
		}
	}()
	if hook := createSlotHook.Load(); hook != nil {
		(*hook)()
	}
	cfg, perr := confFromStr(p.baseConf)
	if perr != nil {
		return nil, perr
	}
	// The facade owns the callbacks (funcs aren't connect-string-expressible);
	// the inbox capacities parsed from the cluster string are left intact for
	// both, so error_inbox_capacity / connection_listener_inbox_capacity in the
	// config still apply to every pooled sender.
	cfg.errorHandler = p.errorHandler
	cfg.connectionListener = p.connectionListener
	cfg.backgroundDrainerListener = p.drainerListener
	cfg.logger = p.logger
	if p.storeAndForward {
		cfg.senderId = p.slotBase + "-" + strconv.Itoa(slotIndex)
		cfg.orphanDrainExclude = p.inRangeFence
	}
	if async {
		cfg.initialConnectMode = InitialConnectAsync
		cfg.initialConnectModeSet = true
	}
	delegate, err := newLineSender(ctx, cfg)
	if err != nil {
		var cleanup closeLifecycleReporter
		if errors.As(err, &cleanup) {
			return &qwpSenderSlot{cleanup: cleanup, slotIndex: slotIndex}, err
		}
		return nil, err
	}
	// The facade is ws/wss-only, so newLineSender always yields a QWP sender;
	// asserting here lets a pooled lease forward the full QwpSender surface.
	qwpDelegate, ok := delegate.(QwpSender)
	if !ok {
		_ = closeSlotGuarded(ctx, delegate)
		return nil, fmt.Errorf("qwp pool: delegate is %T, not a QwpSender", delegate)
	}
	now := time.Now()
	cleanup, _ := qwpDelegate.(closeLifecycleReporter)
	return &qwpSenderSlot{delegate: qwpDelegate, cleanup: cleanup, slotIndex: slotIndex, createdAt: now, idleSince: now}, nil
}

// inRangeFence reports whether name is one of the pool's managed in-range slot
// dirs <base>-<i>, i in [0,maxSize). Pooled senders exclude these from orphan
// adoption so a live sibling is never drained (Hazard G). Out-of-range
// same-base slots (a previous larger run) and foreign dirs stay drainable.
func (p *qwpSenderPool) inRangeFence(name string) bool {
	suffix, ok := strings.CutPrefix(name, p.slotBase+"-")
	if !ok {
		return false
	}
	i, err := strconv.Atoi(suffix)
	// Only the pool ever mints these dirs, always with a canonical
	// strconv.Itoa suffix, so require a canonical round-trip: a foreign dir
	// like "<base>-007" or "<base>-+7" must stay drainable (Hazard G), not be
	// fenced by Atoi's tolerance of leading zeros / sign.
	return err == nil && suffix == strconv.Itoa(i) && i >= 0 && i < p.maxSize
}

// capUsedLocked derives SF capacity from the per-index lifecycle ledger. The
// memory-mode pool has no flock identities and retains its ordinary list-based
// accounting. Caller holds mu.
func (p *qwpSenderPool) capUsedLocked() int {
	if !p.storeAndForward {
		return len(p.all) + p.inFlightCreations
	}
	return p.sfSlotObligationCountLocked()
}

func (p *qwpSenderPool) allocateSlotIndexLocked() int {
	for i := range p.sfSlots {
		if p.sfSlots[i].state == qwpSfSlotFree {
			p.transitionSfSlotLocked(i, qwpSfSlotFree, qwpSfSlotCreating)
			return i
		}
	}
	// The cap check admits a creation only below maxSize, so a free index
	// always exists here; -1 is a defensive sentinel (createSlotAt tolerates it
	// as memory mode, but storeAndForward callers never reach this).
	return -1
}

func qwpSfSlotTransitionAllowed(from, to qwpSfSlotState) bool {
	switch from {
	case qwpSfSlotFree:
		return to == qwpSfSlotCreating
	case qwpSfSlotCreating:
		return to == qwpSfSlotAvailable || to == qwpSfSlotLeased ||
			to == qwpSfSlotClosing || to == qwpSfSlotFree
	case qwpSfSlotAvailable:
		return to == qwpSfSlotLeased || to == qwpSfSlotClosing
	case qwpSfSlotLeased:
		return to == qwpSfSlotAvailable || to == qwpSfSlotClosing
	case qwpSfSlotClosing:
		return to == qwpSfSlotRetired || to == qwpSfSlotFree
	case qwpSfSlotRetired:
		return to == qwpSfSlotFree
	default:
		return false
	}
}

func (p *qwpSenderPool) requireSfSlotStateLocked(index int, want qwpSfSlotState) {
	if !p.storeAndForward || index < 0 {
		return
	}
	if index >= len(p.sfSlots) {
		panic(fmt.Sprintf("qwp pool: SF slot index %d outside lifecycle ledger of size %d", index, len(p.sfSlots)))
	}
	if got := p.sfSlots[index].state; got != want {
		panic(fmt.Sprintf("qwp pool: SF slot %d state is %s, want %s", index, got, want))
	}
}

func (p *qwpSenderPool) transitionSfSlotLocked(index int, from, to qwpSfSlotState) {
	if !p.storeAndForward || index < 0 {
		return
	}
	p.requireSfSlotStateLocked(index, from)
	if !qwpSfSlotTransitionAllowed(from, to) {
		panic(fmt.Sprintf("qwp pool: illegal SF slot %d transition %s -> %s", index, from, to))
	}
	p.sfSlots[index].state = to
}

func (p *qwpSenderPool) transitionSfSlotToClosingLocked(index int) {
	if !p.storeAndForward || index < 0 {
		return
	}
	if index >= len(p.sfSlots) {
		panic(fmt.Sprintf("qwp pool: SF slot index %d outside lifecycle ledger of size %d", index, len(p.sfSlots)))
	}
	from := p.sfSlots[index].state
	p.transitionSfSlotLocked(index, from, qwpSfSlotClosing)
}

func (p *qwpSenderPool) sfSlotStateCountLocked(states ...qwpSfSlotState) int {
	count := 0
	for i := range p.sfSlots {
		for _, state := range states {
			if p.sfSlots[i].state == state {
				count++
				break
			}
		}
	}
	return count
}

func (p *qwpSenderPool) sfSlotObligationCountLocked() int {
	count := 0
	for i := range p.sfSlots {
		if p.sfSlots[i].state != qwpSfSlotFree {
			count++
		}
	}
	return count
}

func (p *qwpSenderPool) sfCloseSnapshotLocked() []qwpSfSlotObligation {
	if !p.storeAndForward {
		return nil
	}
	pending := make([]qwpSfSlotObligation, 0, len(p.sfSlots))
	for i := range p.sfSlots {
		if state := p.sfSlots[i].state; state != qwpSfSlotFree {
			pending = append(pending, qwpSfSlotObligation{index: i, state: state})
		}
	}
	return pending
}

func (p *qwpSenderPool) closeWaitOutstandingLocked() int {
	if !p.storeAndForward {
		return len(p.all) - len(p.available) + p.inFlightCreations + p.pendingLeaseTeardowns
	}
	return p.sfSlotStateCountLocked(qwpSfSlotCreating, qwpSfSlotLeased, qwpSfSlotClosing)
}

func (p *qwpSenderPool) closeLeasedCountLocked() int {
	if !p.storeAndForward {
		return len(p.all) - len(p.available)
	}
	return p.sfSlotStateCountLocked(qwpSfSlotLeased)
}

// reclaimSlotLocked returns an SF slot index only after the delegate confirms
// terminal engine cleanup released its flock. A close timeout may have handed
// cleanup to the manager worker; such a slot is retired -- still reserved, and
// still counted against capacity -- until a reprobe observes completion.
// reprobeRetiredSlots runs on housekeeper ticks AND on the borrow-at-capacity
// path, so housekeeper_interval_ms=0 does not leak the capacity.
func (p *qwpSenderPool) reclaimSlotLocked(slot *qwpSenderSlot, closeErr error) {
	_ = closeErr
	if !p.storeAndForward || slot.slotIndex < 0 {
		return
	}
	p.requireSfSlotStateLocked(slot.slotIndex, qwpSfSlotClosing)
	if !p.slotCloseCompletedGuardedLocked(slot) {
		p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotClosing, qwpSfSlotRetired)
		p.retiredSlots = append(p.retiredSlots, slot)
		p.ensureCloseRetryOwnerGuardedLocked(slot)
		return
	}
	p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotClosing, qwpSfSlotFree)
}

func (p *qwpSenderPool) slotCloseCompletedGuardedLocked(slot *qwpSenderSlot) (completed bool) {
	defer func() {
		if r := recover(); r != nil {
			p.poisonLocked("slot close-completion probe", r)
			completed = false
		}
	}()
	return slotCloseCompleted(slot)
}

func (p *qwpSenderPool) ensureCloseRetryOwnerGuardedLocked(slot *qwpSenderSlot) {
	if slot.cleanup == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			p.poisonLocked("slot close-retry ownership", r)
		}
	}()
	slot.cleanup.ensureCloseRetryOwner(p.logger)
}

// reprobeRetiredSlots observes completion and restores capacity. Cleanup itself
// has an engine-owned retry goroutine, so this function never blocks a borrower
// on manager quiescence or filesystem I/O. It runs on housekeeper ticks and the
// borrow-at-capacity path, including when the housekeeper is disabled.
func (p *qwpSenderPool) reprobeRetiredSlots() {
	restored := p.reprobeRetiredSlotsLocked()
	if restored > 0 {
		qwpEffectiveLogger(p.logger).Info("qwp pool: restored SF capacity after deferred slot cleanup", "slots", restored)
	}
}

// reprobeRetiredSlotsLocked observes the retired slots under p.mu and returns
// how many gave their capacity back. The lock is released via defer, so a
// fault cannot strand the pool mutex -- which would deadlock every later
// borrow, return, reap and repeat close, including the re-probe the
// ErrSfCleanupPending contract tells callers to keep making. Classification
// and apply are split exactly as in selectReapVictims: the partition is built
// whole, then published by applySlotPartitionLocked in one step that cannot
// fault, so a panic in a predicate leaves the retired list and lifecycle
// records untouched (and poisons the pool).
func (p *qwpSenderPool) reprobeRetiredSlotsLocked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poisonedErr != nil || len(p.retiredSlots) == 0 {
		return 0
	}
	part, ok := p.classifyRetiredSlotsLocked()
	if !ok {
		return 0
	}
	p.applySlotPartitionLocked(&p.retiredSlots, part)
	if len(part.removed) > 0 {
		p.broadcastLocked()
	}
	return len(part.removed)
}

// classifyRetiredSlotsLocked partitions the retired list into slots whose
// deferred cleanup has completed (removed: lifecycle returns to free) and
// slots still owed (kept). slotCloseCompleted
// calls into the delegate, so this writes no pool state: a panic is recovered
// here, poisons the pool, and returns ok=false with the half-built partition
// discarded. The retired-to-free transitions are applied only once the whole
// list has classified, so a fault on a later slot cannot free an earlier
// lifecycle record while preserving that slot in the retired list. Caller
// holds mu.
func (p *qwpSenderPool) classifyRetiredSlotsLocked() (part qwpSlotPartition, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			p.poisonLocked("retired-slot reprobe classification", r)
		}
	}()
	part.kept = make([]*qwpSenderSlot, 0, len(p.retiredSlots))
	part.transitionSf = true
	part.sfFrom = qwpSfSlotRetired
	part.sfTo = qwpSfSlotFree
	for _, slot := range p.retiredSlots {
		if slotCloseCompleted(slot) {
			part.removed = append(part.removed, slot)
			continue
		}
		part.kept = append(part.kept, slot)
	}
	return part, true
}

func (p *qwpSenderPool) removeFromAllLocked(slot *qwpSenderSlot) {
	for i, s := range p.all {
		if s == slot {
			p.all[i] = p.all[len(p.all)-1]
			p.all = p.all[:len(p.all)-1]
			return
		}
	}
}

// broadcastLocked wakes every parked waiter (borrow at capacity, close's
// outstanding-count wait). No-op when nobody is parked — a waiter registers in
// p.waiters and captures p.notify under the same mu hold, so it can never miss
// a broadcast this skips. Caller holds mu.
func (p *qwpSenderPool) broadcastLocked() {
	if p.waiters == 0 {
		return
	}
	close(p.notify)
	p.notify = make(chan struct{})
}

// poolSnapshot reports live/idle/leaked counts for tests and introspection.
func (p *qwpSenderPool) poolSnapshot() (total, available, leaked int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.all), len(p.available), p.sfSlotStateCountLocked(qwpSfSlotRetired)
}

// ErrPoolClosed is returned by BorrowSender / BorrowQuery (and the underlying
// pools) once QuestDB.Close has run. Match it with errors.Is.
var ErrPoolClosed = errors.New("qwp pool: handle is closed")

// ErrPoolPoisoned is the sentinel wrapped by BorrowSender, a lease's Close and
// QuestDB.Close after a panic inside the ingest pool's slot classification (the
// idle reap or the retired-slot reprobe). Such a panic is a client bug: the
// classification left the pool's state unchanged, but the predicate that
// faulted once would fault again, and a pool that kept lending after one could
// hand a single sender to two goroutines. The pool records the first panic as
// a terminal error and stops lending — a loud outage instead of silent
// corruption. There is no recovery short of rebuilding the QuestDB handle.
// Match it with errors.Is; the wrapping error carries the panic value.
var ErrPoolPoisoned = errors.New("qwp pool: poisoned by a panic during slot classification; the pool no longer lends senders")

// ErrSenderPoolExhausted is returned by BorrowSender when the ingest pool is at
// sender_pool_max and no sender frees up within acquire_timeout_ms. The returned
// error wraps it with the elapsed budget; match the sentinel with errors.Is.
var ErrSenderPoolExhausted = errors.New("qwp pool: timed out waiting for a sender")

// ErrStaleLease is returned by a leased sender's row/flush methods (At, AtNow,
// AtNano, Flush, FlushAndGetSequence) once the lease has been returned via Close
// (and possibly re-borrowed by another caller) or its slot was evicted. A stale
// lease can no longer write: hold each lease only between BorrowSender and its
// Close, and never use a handle after Close. Match it with errors.Is.
var ErrStaleLease = errors.New("qwp pool: lease is no longer bound to its slot (returned, or the slot was evicted)")

// Internal aliases so existing call sites keep compiling; errors.Is works
// against either name since they are the same error value.
var (
	errPoolClosed    = ErrPoolClosed
	errPoolExhausted = ErrSenderPoolExhausted
	errStaleLease    = ErrStaleLease
)

// qwpPooledSender is the per-borrow lease handed to the caller. It forwards
// LineSender calls to the slot's delegate while the lease is live, and returns
// the slot to the pool on Close. A stale lease (slot returned then re-borrowed)
// no-ops fluent calls and errors on At/AtNow/Flush so it can never corrupt the
// borrow that now owns the slot (Hazard B).
type qwpPooledSender struct {
	pool   *qwpSenderPool
	slot   *qwpSenderSlot
	gen    uint64
	broken bool
}

// A borrowed lease must expose the full QWP surface so callers can type-assert it
// to QwpSender. BorrowSender returns the LineSender view; this keeps the
// QwpSender forwarders below exhaustive at compile time.
var _ QwpSender = (*qwpPooledSender)(nil)

func (ps *qwpPooledSender) live() bool {
	return ps.slot.generation.Load() == ps.gen
}

func (ps *qwpPooledSender) Table(name string) LineSender {
	if ps.live() {
		ps.slot.delegate.Table(name)
	}
	return ps
}

func (ps *qwpPooledSender) Symbol(name, val string) LineSender {
	if ps.live() {
		ps.slot.delegate.Symbol(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) Int64Column(name string, val int64) LineSender {
	if ps.live() {
		ps.slot.delegate.Int64Column(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) Long256Column(name string, val *big.Int) LineSender {
	if ps.live() {
		ps.slot.delegate.Long256Column(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) TimestampColumn(name string, ts time.Time) LineSender {
	if ps.live() {
		ps.slot.delegate.TimestampColumn(name, ts)
	}
	return ps
}

func (ps *qwpPooledSender) Float64Column(name string, val float64) LineSender {
	if ps.live() {
		ps.slot.delegate.Float64Column(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) DecimalColumnFromString(name string, val string) LineSender {
	if ps.live() {
		ps.slot.delegate.DecimalColumnFromString(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) DecimalColumn(name string, val Decimal) LineSender {
	if ps.live() {
		ps.slot.delegate.DecimalColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) DecimalColumnShopspring(name string, val ShopspringDecimal) LineSender {
	if ps.live() {
		ps.slot.delegate.DecimalColumnShopspring(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) StringColumn(name, val string) LineSender {
	if ps.live() {
		ps.slot.delegate.StringColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) BoolColumn(name string, val bool) LineSender {
	if ps.live() {
		ps.slot.delegate.BoolColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) Float64Array1DColumn(name string, values []float64) LineSender {
	if ps.live() {
		ps.slot.delegate.Float64Array1DColumn(name, values)
	}
	return ps
}

func (ps *qwpPooledSender) Float64Array2DColumn(name string, values [][]float64) LineSender {
	if ps.live() {
		ps.slot.delegate.Float64Array2DColumn(name, values)
	}
	return ps
}

func (ps *qwpPooledSender) Float64Array3DColumn(name string, values [][][]float64) LineSender {
	if ps.live() {
		ps.slot.delegate.Float64Array3DColumn(name, values)
	}
	return ps
}

func (ps *qwpPooledSender) Float64ArrayNDColumn(name string, values *NdArray[float64]) LineSender {
	if ps.live() {
		ps.slot.delegate.Float64ArrayNDColumn(name, values)
	}
	return ps
}

// markBrokenIfTerminal evicts the slot only when the delegate's background send
// loop has terminally HALTed, mirroring the Query lease (which evicts on
// client.terminalError(), not on every producer error). A benign fluent-API
// latch (illegal column/table name, At without a preceding Table) or a
// ctx-bounded flush timeout leaves a perfectly healthy connection, so it must be
// recycled, not discarded — discarding would tear down (and, in SF mode,
// re-flock/re-mmap) a warm slot on ordinary malformed input, churning the pool
// on a stream with sporadic bad records.
func (ps *qwpPooledSender) markBrokenIfTerminal() {
	if slotTerminallyFailed(ps.slot.delegate) {
		ps.broken = true
	}
}

func (ps *qwpPooledSender) At(ctx context.Context, ts time.Time) error {
	if !ps.live() {
		return errStaleLease
	}
	if err := ps.slot.delegate.At(ctx, ts); err != nil {
		ps.markBrokenIfTerminal()
		return err
	}
	return nil
}

func (ps *qwpPooledSender) AtNow(ctx context.Context) error {
	if !ps.live() {
		return errStaleLease
	}
	if err := ps.slot.delegate.AtNow(ctx); err != nil {
		ps.markBrokenIfTerminal()
		return err
	}
	return nil
}

func (ps *qwpPooledSender) Flush(ctx context.Context) error {
	if !ps.live() {
		return errStaleLease
	}
	if err := ps.slot.delegate.Flush(ctx); err != nil {
		ps.markBrokenIfTerminal()
		return err
	}
	return nil
}

// --- QwpSender interface: extended column types and accessors ---
//
// A borrowed sender is handed back as a LineSender, but the delegate is always a
// QWP sender (the facade is ws/wss-only), so callers can type-assert the lease to
// QwpSender and reach the binary-protocol-only column types — exactly as they do
// with a standalone QWP sender. These forwarders mirror the LineSender ones: the
// fluent setters no-op on a stale lease and return the lease; the row/flush
// methods error with errStaleLease; the read-only accessors report a dead lease's
// zero value rather than leaking the re-borrowed slot's state.

func (ps *qwpPooledSender) ByteColumn(name string, val int8) QwpSender {
	if ps.live() {
		ps.slot.delegate.ByteColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) ShortColumn(name string, val int16) QwpSender {
	if ps.live() {
		ps.slot.delegate.ShortColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) Int32Column(name string, val int32) QwpSender {
	if ps.live() {
		ps.slot.delegate.Int32Column(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) Float32Column(name string, val float32) QwpSender {
	if ps.live() {
		ps.slot.delegate.Float32Column(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) CharColumn(name string, val rune) QwpSender {
	if ps.live() {
		ps.slot.delegate.CharColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) DateColumn(name string, val time.Time) QwpSender {
	if ps.live() {
		ps.slot.delegate.DateColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) TimestampNanosColumn(name string, val time.Time) QwpSender {
	if ps.live() {
		ps.slot.delegate.TimestampNanosColumn(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) UuidColumn(name string, hi, lo uint64) QwpSender {
	if ps.live() {
		ps.slot.delegate.UuidColumn(name, hi, lo)
	}
	return ps
}

func (ps *qwpPooledSender) GeohashColumn(name string, hash uint64, precision int) QwpSender {
	if ps.live() {
		ps.slot.delegate.GeohashColumn(name, hash, precision)
	}
	return ps
}

func (ps *qwpPooledSender) Int64Array1DColumn(name string, values []int64) QwpSender {
	if ps.live() {
		ps.slot.delegate.Int64Array1DColumn(name, values)
	}
	return ps
}

func (ps *qwpPooledSender) Int64Array2DColumn(name string, values [][]int64) QwpSender {
	if ps.live() {
		ps.slot.delegate.Int64Array2DColumn(name, values)
	}
	return ps
}

func (ps *qwpPooledSender) Int64Array3DColumn(name string, values [][][]int64) QwpSender {
	if ps.live() {
		ps.slot.delegate.Int64Array3DColumn(name, values)
	}
	return ps
}

func (ps *qwpPooledSender) Decimal64Column(name string, val Decimal) QwpSender {
	if ps.live() {
		ps.slot.delegate.Decimal64Column(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) Decimal128Column(name string, val Decimal) QwpSender {
	if ps.live() {
		ps.slot.delegate.Decimal128Column(name, val)
	}
	return ps
}

func (ps *qwpPooledSender) Decimal256Column(name string, val Decimal) QwpSender {
	if ps.live() {
		ps.slot.delegate.Decimal256Column(name, val)
	}
	return ps
}

// AtNano closes the current row with a nanosecond designated timestamp. Like At,
// a terminal delegate error marks the slot broken so Close discards rather than
// recycles it; a benign latch leaves the slot reusable.
func (ps *qwpPooledSender) AtNano(ctx context.Context, ts time.Time) error {
	if !ps.live() {
		return errStaleLease
	}
	if err := ps.slot.delegate.AtNano(ctx, ts); err != nil {
		ps.markBrokenIfTerminal()
		return err
	}
	return nil
}

// FlushAndGetSequence mirrors Flush, additionally returning the published FSN.
func (ps *qwpPooledSender) FlushAndGetSequence(ctx context.Context) (int64, error) {
	if !ps.live() {
		// -1 is the no-FSN sentinel the live sender returns on a failed flush and
		// AckedFsn reports on a dead lease; keep the stale path consistent.
		return -1, errStaleLease
	}
	fsn, err := ps.slot.delegate.FlushAndGetSequence(ctx)
	if err != nil {
		ps.markBrokenIfTerminal()
		return fsn, err
	}
	return fsn, nil
}

// AwaitAckedFsn blocks until the delegate has acknowledged target. It does not
// touch broken: a terminal error here also surfaces on the next producer call (or
// the Close flush), which is where the slot is marked for discard.
func (ps *qwpPooledSender) AwaitAckedFsn(ctx context.Context, target int64) error {
	if !ps.live() {
		return errStaleLease
	}
	return ps.slot.delegate.AwaitAckedFsn(ctx, target)
}

func (ps *qwpPooledSender) AckedFsn() int64 {
	if !ps.live() {
		return -1
	}
	return ps.slot.delegate.AckedFsn()
}

func (ps *qwpPooledSender) LastTerminalError() *SenderError {
	if !ps.live() {
		return nil
	}
	return ps.slot.delegate.LastTerminalError()
}

func (ps *qwpPooledSender) TotalServerErrors() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalServerErrors()
}

func (ps *qwpPooledSender) DroppedErrorNotifications() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.DroppedErrorNotifications()
}

func (ps *qwpPooledSender) DroppedConnectionNotifications() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.DroppedConnectionNotifications()
}

func (ps *qwpPooledSender) TotalErrorNotificationsDelivered() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalErrorNotificationsDelivered()
}

func (ps *qwpPooledSender) TotalReconnectAttempts() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalReconnectAttempts()
}

func (ps *qwpPooledSender) TotalReconnectsSucceeded() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalReconnectsSucceeded()
}

func (ps *qwpPooledSender) TotalFramesReplayed() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalFramesReplayed()
}

func (ps *qwpPooledSender) TotalDurableAcks() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalDurableAcks()
}

func (ps *qwpPooledSender) TotalDurableTrimAdvances() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalDurableTrimAdvances()
}

func (ps *qwpPooledSender) TotalBackpressureStalls() int64 {
	if !ps.live() {
		return 0
	}
	return ps.slot.delegate.TotalBackpressureStalls()
}

func (ps *qwpPooledSender) QuarantinedSlotPath() string {
	if !ps.live() {
		return ""
	}
	return ps.slot.delegate.QuarantinedSlotPath()
}

func (ps *qwpPooledSender) BackgroundDrainers() []QwpBackgroundDrainer {
	if !ps.live() {
		return nil
	}
	return ps.slot.delegate.BackgroundDrainers()
}

// Close returns the leased sender to the pool. It flushes committed rows first
// (surfacing but not swallowing a latched fluent-API error) so the next
// borrower starts clean; the slot is marked broken (discarded rather than
// recycled) on a terminal fault OR when the return flush left committed rows
// un-enqueued (a backpressure-deadline / engine-closed failure — a dirty slot
// that would leak this borrower's rows into the next). A benign fluent-API
// latch, whose committed rows still flushed cleanly, leaves a healthy slot to be
// reused. Idempotent — a stale lease no-ops.
//
// The flush and return run on context.Background, not the caller's ctx: the
// return flush only publishes into the cursor engine (it never waits for the
// server ACK), so
// a request-scoped ctx must not (a) leave pending rows un-published in a slot
// the next borrower reuses, or (b) mark a healthy slot broken via a
// context.Canceled / DeadlineExceeded. Both would thrash the pool under the
// standard `ctx, cancel := ...; defer sender.Close(ctx)` pattern. The publish is
// bounded by the engine's append deadline, not the caller's ctx.
func (ps *qwpPooledSender) Close(_ context.Context) (retErr error) {
	if !ps.live() {
		return nil
	}
	var flushErr error
	ctx1 := context.Background()
	// Panic-guard the return flush the way closeSlotGuarded guards every
	// pool-internal delegate close: a fault in flushForReturn (a producer left
	// mid-mutation by a recovered panic in At/*Column) must not skip giveBack
	// and strand the slot on-loan forever. On panic, discard the slot (its
	// producer state is untrusted), still give it back, and surface the panic
	// as an error consistent with closeSlotGuarded's wording rather than
	// re-panicking through the caller's defer.
	defer func() {
		if r := recover(); r != nil {
			ps.broken = true
			_ = ps.pool.giveBack(ctx1, ps, true)
			retErr = fmt.Errorf("qwp pool: delegate close panicked: %v", r)
		}
	}()
	if !ps.broken {
		// Route the return through flushForReturn, NOT Flush. Flush both
		// early-returns errFlushWithPendingMessage while a row is open and
		// early-returns a latched fluent-API error ahead of its pending-rows
		// branch — either way it leaves committed rows unflushed in a
		// non-terminal (kept) slot, poisoning the next borrower.
		// flushForReturn mirrors closeCursor: drop the open row, surface the
		// latch, but still flush committed rows so the slot returns clean.
		if fr, ok := ps.slot.delegate.(returnFlusher); ok {
			var retained bool
			retained, flushErr = fr.flushForReturn(ctx1)
			if retained {
				// flushForReturn could not enqueue the committed rows — the
				// cursor ring saturated and the append deadline elapsed (an
				// outage, the exact condition that makes backpressure bite),
				// or the engine was closed. Neither is a terminal send-loop
				// HALT, so markBrokenIfTerminal below leaves the slot healthy;
				// but the rows are RETAINED in the delegate's producer buffers.
				// Recycling this dirty slot would encode borrower A's rows into
				// borrower B's next flush, shipping them under B's FSN
				// (borrower-isolation / row misattribution). Discard it so B is never
				// poisoned; the discard-close still best-effort-drains A's own rows,
				// and A's retry of the surfaced error is idempotent under server
				// dedup (at-least-once).
				ps.broken = true
			}
		} else {
			flushErr = ps.slot.delegate.Flush(ctx1)
		}
		if flushErr != nil {
			// A benign latched fluent-API error (already cleared, committed
			// rows already flushed) leaves a healthy connection; only a
			// terminal fault forfeits the slot. Matches the in-use paths'
			// markBrokenIfTerminal.
			ps.markBrokenIfTerminal()
		}
	}
	poolErr := ps.pool.giveBack(ctx1, ps, ps.broken)
	if flushErr != nil {
		return flushErr
	}
	return poolErr
}
