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
// With store-and-forward enabled (sf_dir set), each slot gets a separate
// sender_id, <base>-<index>, so senders cannot use the same directory (Hazard A).
// Background drainers must skip all of the pool's reserved directories, since
// another pooled sender may be using them (Hazard G). At startup, each reserved
// directory left by a crash gets its own sender, which recovers it and connects
// in the background. Opening local files and creating the pool's initial
// connections can still make construction wait.
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
	failureDone       chan struct{}
	// Set closing before stopping the housekeeper, so it cannot start removing
	// idle senders during shutdown. close() sets closed when pool shutdown
	// starts. QuestDB.Close waits separately for the housekeeper to exit.
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

	// pendingLeaseTeardowns counts senders being closed outside the pool lock,
	// including returned senders and those removed by periodic maintenance.
	// Read and update it with mu held.
	pendingLeaseTeardowns int

	// closeTeardownErr saves errors from closing senders, including those that
	// finish after the first pool Close returns. Read and update it with mu held.
	closeTeardownErr error

	// poisonedErr saves the first internal pool failure. After that, the pool
	// refuses new borrows and stops trying to reuse failed slots. Cleanup of
	// other senders may still finish if it is safe. Set once, with mu held.
	poisonedErr error
	// Keep these senders alive after an internal failure. This list does not
	// track available space; sfSlots records the store-and-forward reservations.
	failedSlots []*qwpSenderSlot
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

// ErrSfCleanupPending means some work can still use or acquire a
// store-and-forward slot. This includes borrowed senders, senders still being
// created, and unfinished cleanup. Match it with errors.Is. Return borrowed
// senders and check storage before waiting again with a fresh deadline.
// The error does not promise that the slot will eventually be released.
//
// Unfinished store-and-forward cleanup also produces [ErrCleanupPending].
// Check [ErrCleanupFailed] first: it means an internal failure has left
// resources held that cannot safely be released. One result can include this
// failure and other cleanup still in progress. See [QuestDB.Close] for what
// it waits for and which errors remain in later results.
//
// Connect, NewQuestDB, or NewLineSender can return an error wrapping this
// value without returning a handle. The client still tracks resources it has
// not released, but there is no handle for you to Close. Creating another
// client for the same slot may fail because this process still holds its
// lock. Limit retries and inspect logs and storage; an internal cleanup
// failure may require a process restart before the slot can be reused.
var ErrSfCleanupPending = errors.New("qwp pool: SF slot cleanup still pending; lifecycle obligations remain")

// qwpPoolCloseResult combines the stable teardown/poison errors with one live
// lifecycle snapshot. The snapshot carries both the indices and states of all
// obligations that still could acquire or retain a slot flock.
func qwpPoolCloseResult(teardownErr, poisonErr error, pending []qwpSfSlotObligation) error {
	if poisonErr != nil {
		if teardownErr != nil {
			teardownErr = errors.Join(teardownErr, poisonErr)
		} else {
			teardownErr = poisonErr
		}
	}
	if len(pending) == 0 {
		return teardownErr
	}
	pendingErr := fmt.Errorf("%w: %w (%d slot(s): %v)", ErrCleanupPending, ErrSfCleanupPending, len(pending), pending)
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
	// Read and update with pool.mu held. If a panic leaves the slot in an
	// unknown state, do not retry its cleanup or reuse it.
	cleanupFailed bool
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
			// the holder. The engine's cleanup worker keeps the lock until it
			// can safely release it.
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
	if err := p.stableCloseResult(); err != nil {
		return nil, errors.Join(err, p.close(ctx))
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
		var free bool
		if err := p.withLock("check recovery slot", nil, func() { free = p.sfSlots[i].state == qwpSfSlotFree }); err != nil {
			return
		}
		if !free {
			continue
		}
		dir := filepath.Join(p.sfDir, p.slotBase+"-"+strconv.Itoa(i))
		if !qwpSfIsCandidateOrphan(dir) {
			continue
		}
		if err := p.withLock("reserve recovery slot", nil, func() {
			p.transitionSfSlotLocked(i, qwpSfSlotFree, qwpSfSlotCreating)
		}); err != nil {
			return
		}
		slot, err := p.createSlotAt(ctx, i, true)
		if lockErr := p.withLock("finish recovery slot", []*qwpSenderSlot{slot}, func() {
			if err != nil {
				p.reclaimFailedBuildLocked(slot, i, err)
				return
			}
			p.transitionSfSlotLocked(i, qwpSfSlotCreating, qwpSfSlotAvailable)
			p.all = append(p.all, slot)
			p.available = append(p.available, slot)
		}); lockErr != nil {
			return
		}
	}
}

// borrow waits up to acquireTimeout, or until ctx ends, if the pool is full.
// Call Close on the returned sender to give it back. The pool handles resource
// cleanup when it removes a sender or shuts down.
func (p *qwpSenderPool) borrow(ctx context.Context) (LineSender, error) {
	deadline := time.Now().Add(p.acquireTimeout)
	for {
		var lease *qwpPooledSender
		var discard *qwpSenderSlot
		var build bool
		index := -1
		var ch, failed chan struct{}
		var waitFor time.Duration
		var result error
		lockErr := p.withLock("borrow sender", nil, func() {
			if p.poisonedErr != nil {
				result = p.poisonedErr
				return
			}
			if p.closed || p.closing.Load() {
				result = errPoolClosed
				return
			}
			p.reprobeRetiredSlotsLocked()
			if p.poisonedErr != nil {
				result = p.poisonedErr
				return
			}
			if n := len(p.available); n > 0 {
				slot := p.available[n-1]
				// Keep the slot in the list until its checks and state update
				// succeed, so a panic cannot make the pool lose the sender.
				defer func() {
					if r := recover(); r != nil {
						p.failSlotsLocked("borrow slot", r, []*qwpSenderSlot{slot})
						result = p.poisonedErr
					}
				}()
				if slotTerminallyFailed(slot.delegate) {
					if p.prepareSlotCloseLocked(slot, qwpSfSlotAvailable) {
						discard = slot
					}
				} else {
					p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotAvailable, qwpSfSlotLeased)
					lease = &qwpPooledSender{pool: p, slot: slot, gen: slot.generation.Add(1)}
				}
				p.available = p.available[:n-1]
				return
			}
			if time.Until(deadline) <= 0 {
				result = fmt.Errorf("%w after %s", errPoolExhausted, p.acquireTimeout)
				return
			}
			if ctx.Err() != nil {
				result = ctx.Err()
				return
			}
			if p.capUsedLocked() < p.maxSize {
				if p.storeAndForward {
					index = p.allocateSlotIndexLocked()
				}
				p.inFlightCreations++
				build = true
				return
			}
			p.waiters++
			ch, failed = p.notify, p.failureDone
			waitFor = time.Until(deadline)
			// Cleanup does not notify the pool when it finishes. Check regularly
			// only while some slots are waiting for cleanup.
			if p.sfSlotStateCountLocked(qwpSfSlotRetired) > 0 {
				waitFor = min(waitFor, 10*time.Millisecond)
			}
		})
		if lockErr != nil {
			return nil, lockErr
		}
		if result != nil {
			return nil, result
		}
		if lease != nil {
			return lease, nil
		}
		if discard != nil {
			go p.closeReturnedSlot(discard)
			continue
		}
		if build {
			// Opening files can take longer than the caller is willing to wait.
			// Keep track of the new sender even if the caller leaves: pass it to
			// settleGrowthBuild when it is ready.
			bctx, cancel := context.WithDeadline(ctx, deadline)
			resultCh := make(chan *qwpSenderSlot, 1)
			errCh := make(chan error, 1)
			go func() { slot, err := p.createSlotAt(bctx, index, true); resultCh <- slot; errCh <- err }()
			timer := time.NewTimer(time.Until(deadline))
			select {
			case slot := <-resultCh:
				timer.Stop()
				cancel()
				return p.settleBuiltSlot(slot, index, <-errCh, true)
			case <-timer.C:
				go p.settleGrowthBuild(resultCh, errCh, cancel, index)
				return nil, fmt.Errorf("%w after %s", errPoolExhausted, p.acquireTimeout)
			case <-ctx.Done():
				timer.Stop()
				go p.settleGrowthBuild(resultCh, errCh, cancel, index)
				return nil, ctx.Err()
			}
		}
		timer := time.NewTimer(waitFor)
		select {
		case <-ch:
		case <-failed:
		case <-timer.C:
		case <-ctx.Done():
		}
		timer.Stop()
		p.withLock("finish borrow wait", nil, func() { p.waiters-- })
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

// settleGrowthBuild handles a sender that was still being created when its
// borrower stopped waiting. The pool keeps counting that work until creation
// finishes. settleBuiltSlot then saves the sender for another borrower or
// arranges cleanup; failed cleanup keeps its slot reserved. This runs in a
// separate goroutine so slow file operations do not hold up the borrower.
func (p *qwpSenderPool) settleGrowthBuild(resultCh <-chan *qwpSenderSlot, errCh <-chan error, cancel context.CancelFunc, slotIndex int) {
	slot := <-resultCh
	err := <-errCh
	cancel()
	_, _ = p.settleBuiltSlot(slot, slotIndex, err, false)
}

// settleBuiltSlot keeps the new sender's slot reserved until it is handed to
// a borrower, saved for a later borrow, or passed to a cleanup worker.
func (p *qwpSenderPool) settleBuiltSlot(slot *qwpSenderSlot, index int, buildErr error, borrow bool) (LineSender, error) {
	var lease *qwpPooledSender
	var closeSlot bool
	result := buildErr
	err := p.withLock("finish sender construction", []*qwpSenderSlot{slot}, func() {
		p.inFlightCreations--
		defer p.broadcastLocked()
		if buildErr != nil {
			p.reclaimFailedBuildLocked(slot, index, buildErr)
			return
		}
		if p.closed || p.closing.Load() || p.poisonedErr != nil {
			closeSlot = p.prepareSlotCloseLocked(slot, qwpSfSlotCreating)
			result = errPoolClosed
			if p.poisonedErr != nil {
				result = p.poisonedErr
			}
			return
		}
		if borrow {
			p.transitionSfSlotLocked(index, qwpSfSlotCreating, qwpSfSlotLeased)
			lease = &qwpPooledSender{pool: p, slot: slot, gen: slot.generation.Add(1)}
		} else {
			p.transitionSfSlotLocked(index, qwpSfSlotCreating, qwpSfSlotAvailable)
			slot.idleSince = time.Now()
			p.available = append(p.available, slot)
		}
		p.all = append(p.all, slot)
	})
	if closeSlot && err == nil {
		go p.closeReturnedSlot(slot)
	}
	if err != nil {
		return nil, errors.Join(result, err)
	}
	if lease == nil {
		return nil, result
	}
	return lease, result
}

// giveBack accepts a borrowed sender only once. A healthy sender can be reused;
// a failed sender or one returned after shutdown is closed in the background.
// If a panic left a slot's state unknown, keep it reserved without trying to
// repair it.
func (p *qwpSenderPool) giveBack(_ context.Context, ps *qwpPooledSender, broken bool) error {
	var closeSlot bool
	var result error
	err := p.withLock("return sender", []*qwpSenderSlot{ps.slot}, func() {
		result = p.poisonedErr
		if ps.slot.generation.Load() != ps.gen {
			return
		}
		ps.slot.generation.Add(1)
		if ps.slot.cleanupFailed {
			return
		}
		if p.closed || p.closing.Load() || p.poisonedErr != nil || broken {
			closeSlot = p.prepareSlotCloseLocked(ps.slot, qwpSfSlotLeased)
		} else {
			p.transitionSfSlotLocked(ps.slot.slotIndex, qwpSfSlotLeased, qwpSfSlotAvailable)
			ps.slot.idleSince = time.Now()
			p.available = append(p.available, ps.slot)
		}
		p.broadcastLocked()
	})
	if closeSlot && err == nil {
		go p.closeReturnedSlot(ps.slot)
	}
	return errors.Join(result, err)
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

type closeLifecycleReporter interface {
	closeCompletionReporter
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
			err = fmt.Errorf("%w: qwp pool: delegate close panicked: %v\n%s", ErrCleanupFailed, r, debug.Stack())
		}
	}()
	return delegate.Close(ctx)
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
	// Start closing all these slots without waiting for one to finish first.
	// QuestDB.Close tracks each slot's cleanup and waits for the housekeeper
	// to exit. The timeout for server confirmation does not limit total cleanup.
	var wg sync.WaitGroup
	errs := make([]error, len(toClose))
	for i, slot := range toClose {
		wg.Add(1)
		go func(i int, slot *qwpSenderSlot) {
			defer wg.Done()
			var before func()
			if hook := reapCloseHook.Load(); hook != nil {
				before = *hook
			}
			errs[i] = p.closeSlotTask(slot, before)
		}(i, slot)
	}
	wg.Wait()
	for i, slot := range toClose {
		p.finishSlotClose(slot, errs[i])
	}
	for _, err := range errs {
		if err != nil {
			qwpEffectiveLogger(p.logger).Warn("qwp pool: reaping a slot failed to drain cleanly", "error", err)
		}
	}
}

// qwpSlotPartition lists the slots to keep and the slots to remove. Check all
// senders before changing the pool. If making the changes fails, keep references
// to the affected senders; do not try to finish or undo the partial changes.
type qwpSlotPartition struct {
	kept                       []*qwpSenderSlot
	removed                    []*qwpSenderSlot
	pendingLeaseTeardownsDelta int
	removeFromAll              bool
	transitionSf               bool
	sfFrom                     qwpSfSlotState
	sfTo                       qwpSfSlotState
}

// applySlotPartitionLocked updates the lists, slot states, and counts together.
// Check for problems first, but still handle a panic during the updates. If that
// happens, keep every affected sender. The caller must hold mu.
func (p *qwpSenderPool) applySlotPartitionLocked(target *[]*qwpSenderSlot, part qwpSlotPartition) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			p.failSlotsLocked("apply slot changes", r, part.removed)
		}
	}()
	if p.storeAndForward && part.transitionSf {
		seen := make(map[int]bool, len(part.removed))
		for _, slot := range part.removed {
			if seen[slot.slotIndex] {
				panic("duplicate SF slot in batch")
			}
			seen[slot.slotIndex] = true
		}
		// Check expected states before changing any of them.
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
	return true
}

// poisonLocked refuses future borrows and wakes callers waiting for shutdown.
// Keep the failed pool alive even if the application drops its reference, so
// garbage collection cannot close files or release locks that may still be in
// use. This does not retry cleanup or repair the pool. The caller must hold mu.
func (p *qwpSenderPool) poisonLocked(op string, panicValue any) {
	if p.poisonedErr == nil {
		p.poisonedErr = fmt.Errorf("%w: %w: %s panicked: %v", ErrCleanupFailed, ErrPoolPoisoned, op, panicValue)
		qwpFailedSenderPools.Lock()
		qwpFailedSenderPools.pools = append(qwpFailedSenderPools.pools, p)
		qwpFailedSenderPools.Unlock()
		if p.failureDone == nil {
			p.failureDone = make(chan struct{})
		}
		close(p.failureDone)
		// Logging must not hold the mutex or make callers wait for the error.
		logger, err := p.logger, p.poisonedErr
		go func() {
			qwpEffectiveLogger(logger).Error("qwp pool: internal failure; uncertain senders remain held", "operation", op, "error", err)
		}()
	}
}

// selectReapVictims chooses idle, old, or failed senders to close outside the
// lock. Check them before changing the lists. If updating the pool fails partway
// through, keep the affected senders and refuse future borrows.
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
	if !p.applySlotPartitionLocked(&p.available, part) {
		return nil
	}
	return part.removed
}

// classifyReapVictimsLocked checks which available senders should be closed,
// without changing the lists or slot states. If checking a sender panics, keep
// that sender, record the pool failure, and return ok=false. On success, the
// caller marks the selected slots as closing and counts their cleanup work.
// The caller must hold mu.
func (p *qwpSenderPool) classifyReapVictimsLocked(now time.Time) (part qwpSlotPartition, ok bool) {
	var checking *qwpSenderSlot
	defer func() {
		if r := recover(); r != nil {
			p.failSlotsLocked("reap-victim classification", r, []*qwpSenderSlot{checking})
		}
	}()
	// Build a separate list so a panic during the checks cannot partly
	// overwrite p.available.
	part.kept = make([]*qwpSenderSlot, 0, len(p.available))
	part.removeFromAll = true
	part.transitionSf = true
	part.sfFrom = qwpSfSlotAvailable
	part.sfTo = qwpSfSlotClosing
	for _, slot := range p.available {
		checking = slot
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

// close starts pool shutdown once. Later calls wait for the same attempt and
// check its current result.
//
// Close only senders that have been returned to the pool. A borrower may still
// be writing rows; closing its sender could free buffers it is using. Catching
// panics cannot make that safe. The application must stop using each borrowed
// sender and return it, which starts cleanup if the pool has already closed.
// A sender that is never returned keeps its resources.
//
// If a sender is still being created, keep its slot reserved, even before it
// takes the file lock. Once created, pass it to background cleanup if the pool
// has closed. Slots waiting for release remain reserved and cannot be reused;
// slots whose cleanup fails permanently stay reserved. Later Close calls also
// report saved errors.
//
// One background worker closes the available senders in parallel. ctx limits
// how long the caller waits, not the worker or each sender's time to queue rows
// and wait for server acknowledgements. Report internal failure without waiting
// for other senders that are still closing.
func (p *qwpSenderPool) close(ctx context.Context) error {
	var start bool
	var done, failed chan struct{}
	err := p.withLock("start pool close", nil, func() {
		if !p.closeStarted {
			p.closeStarted = true
			p.closeDone = make(chan struct{})
			if p.failureDone == nil {
				p.failureDone = make(chan struct{})
			}
			start = true
			p.closed = true
			p.closing.Store(true)
			p.broadcastLocked()
		}
		done, failed = p.closeDone, p.failureDone
	})
	if start {
		go p.closeWorker()
	}
	if err != nil {
		return err
	}
	return p.cleanupWaitResult(ctx, done, failed)
}

// closeWorker runs the pool's one shutdown attempt. It closes closeDone when
// the attempt ends, even after a panic. That signal alone does not mean all
// resources have been released.
func (p *qwpSenderPool) closeWorker() {
	var candidates []*qwpSenderSlot
	var leaked int
	defer func() {
		// This runs after closeDone is closed. A slow logger must not delay
		// cleanup or keep callers waiting after the work has finished.
		if leaked > 0 {
			qwpEffectiveLogger(p.logger).Warn("qwp pool: close() leaving borrowed sender(s) alive; "+
				"each is torn down when its lease is closed", "leaked", leaked)
		}
	}()
	defer func() {
		r := recover()
		p.withLock("finish pool close", candidates, func() {
			if r != nil {
				p.failSlotsLocked("pool close", r, candidates)
			}
			close(p.closeDone)
			p.broadcastLocked()
		})
	}()
	waitBudget := min(p.acquireTimeout, qwpPoolMaxCloseLeaseWait)
	deadline := time.Now().Add(waitBudget)
	for {
		var ch, failed chan struct{}
		p.withLock("wait for returned senders", nil, func() {
			if p.poisonedErr == nil && p.closeWaitOutstandingLocked() > 0 && time.Now().Before(deadline) {
				p.waiters++
				ch, failed = p.notify, p.failureDone
			}
		})
		if ch == nil {
			break
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ch:
		case <-failed:
		case <-timer.C:
		}
		timer.Stop()
		p.withLock("finish lease wait", nil, func() { p.waiters-- })
	}
	p.withLock("select senders to close", nil, func() {
		leaked = p.closeLeasedCountLocked()
		candidates = append([]*qwpSenderSlot(nil), p.available...)
	})
	toClose := make([]*qwpSenderSlot, 0, len(candidates))
	for _, slot := range candidates {
		var ready bool
		err := p.withLock("prepare sender close", []*qwpSenderSlot{slot}, func() {
			ready = p.prepareSlotCloseLocked(slot, qwpSfSlotAvailable)
			if ready {
				p.removeAvailableLocked(slot)
			}
		})
		if ready && err == nil {
			toClose = append(toClose, slot)
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, len(toClose))
	for i, slot := range toClose {
		wg.Add(1)
		go func(i int, slot *qwpSenderSlot) {
			defer wg.Done()
			errs[i] = p.closeSlotTask(slot, nil)
		}(i, slot)
	}
	wg.Wait()
	for i, slot := range toClose {
		p.finishSlotClose(slot, errs[i])
	}
}

// currentCloseResult combines saved errors with the current slot states.
// If checking the result panics, report the failure and still unlock the mutex.
func (p *qwpSenderPool) currentCloseResult() error {
	p.reprobeRetiredSlots()
	var result error
	err := p.withLock("read pool close result", nil, func() {
		pending := p.sfCloseSnapshotLocked()
		teardownErr := p.closeTeardownErr
		if !p.storeAndForward && (p.closeWaitOutstandingLocked() > 0 || len(p.retiredSlots) > 0) {
			teardownErr = errors.Join(teardownErr, ErrCleanupPending)
		}
		for _, slot := range p.retiredSlots {
			if slot.cleanupFailed {
				continue
			}
			if reporter, ok := slot.cleanup.(interface{ cleanupResult() error }); ok {
				teardownErr = errors.Join(teardownErr, reporter.cleanupResult())
			} else {
				teardownErr = errors.Join(teardownErr, p.slotCleanupFailureGuardedLocked(slot))
			}
		}
		// A failed slot may still have other resources being released. Read
		// results only from these known types, whose methods do not perform
		// cleanup. Do not repeat a check that panicked or call Close again on
		// a sender whose state may be partly changed.
		for _, slot := range p.failedSlots {
			switch reporter := slot.cleanup.(type) {
			case *qwpLineSender:
				teardownErr = errors.Join(teardownErr, reporter.cleanupResult())
			case *qwpSfBuildCleanupError:
				teardownErr = errors.Join(teardownErr, reporter.cleanupResult())
			case *qwpSenderBuildCleanupError:
				teardownErr = errors.Join(teardownErr, reporter.cleanupResult())
			}
		}
		result = qwpPoolCloseResult(teardownErr, p.poisonedErr, pending)
	})
	return errors.Join(result, err)
}

// stableCloseResult returns saved errors without checking for unfinished work.
func (p *qwpSenderPool) stableCloseResult() error {
	var result error
	err := p.withLock("read saved pool errors", nil, func() {
		result = qwpPoolCloseResult(p.closeTeardownErr, p.poisonedErr, nil)
	})
	return errors.Join(result, err)
}

// createSlot allocates an SF slot index when needed and builds a slot.
//
// The sole caller is newQwpSenderPool's prewarm loop. It still takes p.mu for
// lifecycle changes so construction follows the same state rules as growth.
func (p *qwpSenderPool) createSlot(ctx context.Context, async bool) (*qwpSenderSlot, error) {
	slotIndex := -1
	if err := p.withLock("reserve initial sender", nil, func() {
		if p.storeAndForward {
			slotIndex = p.allocateSlotIndexLocked()
		}
	}); err != nil {
		return nil, err
	}
	slot, err := p.createSlotAt(ctx, slotIndex, async)
	lockErr := p.withLock("finish initial sender", []*qwpSenderSlot{slot}, func() {
		if err != nil {
			p.reclaimFailedBuildLocked(slot, slotIndex, err)
			return
		}
		p.transitionSfSlotLocked(slotIndex, qwpSfSlotCreating, qwpSfSlotAvailable)
	})
	if err != nil || lockErr != nil {
		return nil, errors.Join(err, lockErr)
	}
	return slot, nil
}

func (p *qwpSenderPool) reclaimFailedBuildLocked(slot *qwpSenderSlot, slotIndex int, buildErr error) {
	if slot != nil && slot.cleanup != nil {
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
			// freed while that engine's cleanup worker still holds the file lock.
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
	if hook := qwpTestPoolTransitionHook.Load(); hook != nil {
		(*hook)(p, index, from, to)
	}
	p.sfSlots[index].state = to
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

// reclaimSlotLocked makes a store-and-forward slot available only after the
// sender confirms that cleanup released its file lock. A timeout leaves cleanup
// running and the slot reserved, still counting against pool capacity.
// reprobeRetiredSlots checks for completion both during periodic maintenance
// and when a borrow finds the pool full, so capacity can be recovered even with
// housekeeper_interval_ms=0.
func (p *qwpSenderPool) reclaimSlotLocked(slot *qwpSenderSlot, closeErr error) {
	if errors.Is(closeErr, ErrCleanupFailed) {
		p.failSlotsLocked("sender close", closeErr, []*qwpSenderSlot{slot})
	}
	if slot.cleanupFailed {
		return
	}
	p.requireSfSlotStateLocked(slot.slotIndex, qwpSfSlotClosing)
	if !p.slotCloseCompletedGuardedLocked(slot) {
		if slot.cleanupFailed {
			return
		}
		p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotClosing, qwpSfSlotRetired)
		p.retiredSlots = append(p.retiredSlots, slot)
		return
	}
	p.harvestSlotResultLocked(slot)
	p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotClosing, qwpSfSlotFree)
}

// Save the sender's final cleanup error before the pool stops tracking it.
// This only reads the result; it does not try to close the sender again.
func (p *qwpSenderPool) harvestSlotResultLocked(slot *qwpSenderSlot) {
	if reporter, ok := slot.cleanup.(interface{ cleanupResult() error }); ok {
		p.closeTeardownErr = qwpAppendCloseError(p.closeTeardownErr, reporter.cleanupResult())
	}
}

// Check for cleanup failure without running cleanup from the pool.
// Catch a panic from this check so the caller can still unlock p.mu.
func (p *qwpSenderPool) slotCleanupFailureGuardedLocked(slot *qwpSenderSlot) (err error) {
	if slot.cleanupFailed {
		return p.poisonedErr
	}
	defer func() {
		if r := recover(); r != nil {
			p.failSlotsLocked("slot cleanup-error probe", r, []*qwpSenderSlot{slot})
			err = errors.Join(ErrCleanupFailed, p.poisonedErr)
		}
	}()
	if reporter, ok := slot.cleanup.(interface{ cleanupFailure() error }); ok {
		return reporter.cleanupFailure()
	}
	return nil
}

func (p *qwpSenderPool) slotCloseCompletedGuardedLocked(slot *qwpSenderSlot) (completed bool) {
	defer func() {
		if r := recover(); r != nil {
			p.failSlotsLocked("slot close-completion probe", r, []*qwpSenderSlot{slot})
			completed = false
		}
	}()
	return slotCloseCompleted(slot)
}

// reprobeRetiredSlots checks whether cleanup has finished for retired slots.
// It unlocks the pool before logging restored capacity.
func (p *qwpSenderPool) reprobeRetiredSlots() {
	p.mu.Lock()
	restored := func() int {
		defer p.mu.Unlock()
		return p.reprobeRetiredSlotsLocked()
	}()
	if restored > 0 {
		go qwpEffectiveLogger(p.logger).Info("qwp pool: restored capacity after deferred cleanup", "slots", restored)
	}
}

// reprobeRetiredSlotsLocked checks slots waiting for cleanup and returns how
// many can now be reused. The caller must hold p.mu; this method does not lock
// or unlock it. Check completion before changing slot states. If a check panics,
// leave the lists unchanged. If an update panics, keep the affected senders and
// stop trying to make slots available again.
func (p *qwpSenderPool) reprobeRetiredSlotsLocked() int {
	if p.poisonedErr != nil || len(p.retiredSlots) == 0 {
		return 0
	}
	part, ok := p.classifyRetiredSlotsLocked()
	if !ok {
		return 0
	}
	if !p.applySlotPartitionLocked(&p.retiredSlots, part) {
		return 0
	}
	if len(part.removed) > 0 {
		p.broadcastLocked()
	}
	return len(part.removed)
}

// classifyRetiredSlotsLocked separates slots whose cleanup has finished from
// those still waiting. It asks each sender whether cleanup is complete without
// changing the lists or slot states. If a check panics, keep the sender, record
// the pool failure, and return ok=false. The caller updates slot states only
// after all checks succeed. The caller must hold mu.
func (p *qwpSenderPool) classifyRetiredSlotsLocked() (part qwpSlotPartition, ok bool) {
	var checking *qwpSenderSlot
	defer func() {
		if r := recover(); r != nil {
			p.failSlotsLocked("retired-slot reprobe classification", r, []*qwpSenderSlot{checking})
		}
	}()
	part.kept = make([]*qwpSenderSlot, 0, len(p.retiredSlots))
	part.transitionSf = p.storeAndForward
	part.sfFrom = qwpSfSlotRetired
	part.sfTo = qwpSfSlotFree
	for _, slot := range p.retiredSlots {
		checking = slot
		if !slot.cleanupFailed && slotCloseCompleted(slot) {
			p.harvestSlotResultLocked(slot)
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

// ErrPoolPoisoned means an internal failure left a pool unable to safely lend
// senders or query clients. BorrowSender, BorrowQuery, a borrowed handle's Close,
// and QuestDB.Close may return an error wrapping it. Use errors.Is to check for
// it and inspect the underlying error. Stop using the affected pool, and stop
// using each borrowed handle before returning it.
//
// Corrupt pool state also produces [ErrCleanupFailed]. Calling Close again
// reports the failure; it does not repair the pool's records. Slots that
// cannot safely be reused stay reserved. Creating another QuestDB does not
// free the old pool's locks; reusing the same slots may require a process
// restart. Other cleanup that is known to be safe may finish, but the
// internal failure remains in the result. Unlike [ErrSfCleanupPending] alone,
// this error cannot be resolved just by waiting longer.
var ErrPoolPoisoned = errors.New("qwp pool: internal pool failure; the pool no longer lends handles")

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

func (ps *qwpPooledSender) SlotLockReleased() bool {
	if !ps.live() {
		return true
	}
	return ps.slot.delegate.SlotLockReleased()
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

// Close returns the borrowed sender; see [LineSender.Close] for the QWP rules.
// It discards an unfinished row and queues completed rows for sending. This
// uses the append timeout, not the caller's context, so a cancelled request
// cannot leave buffered rows for the next borrower. An earlier row-building
// error need not remove the sender from the pool if completed rows were queued
// successfully. Rows that could not be queued, or a terminal sender error,
// require removal instead of reuse. Calls after return do nothing.
func (ps *qwpPooledSender) Close(_ context.Context) (retErr error) {
	if !ps.live() {
		return nil
	}
	var flushErr error
	ctx1 := context.Background()
	// A panic can leave the row buffers partly changed. Keep the sender rather
	// than call Close and risk flushing those rows again.
	defer func() {
		if r := recover(); r != nil {
			ps.pool.withLock("return flush", []*qwpSenderSlot{ps.slot}, func() {
				if ps.slot.generation.Load() == ps.gen {
					ps.slot.generation.Add(1)
				}
				ps.pool.failSlotsLocked("return flush", r, []*qwpSenderSlot{ps.slot})
				retErr = errors.Join(flushErr, ps.pool.poisonedErr)
			})
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
	return errors.Join(flushErr, poolErr)
}
