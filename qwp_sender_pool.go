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
	"math/big"
	"path/filepath"
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
// Store-and-forward (sf_dir set) is supported with one Java-faithful twist
// (design §4.4): each slot gets a distinct sender_id <base>-<index> so
// concurrent senders never collide on a slot dir (Hazard A), and every pooled
// sender fences the pool's whole in-range slot set out of orphan adoption so a
// live sibling is never drained (Hazard G). Crash-stranded in-range slots are
// recovered by binding a normal async sender to each at construction (the Go
// sender self-recovers its dir) — no dedicated recoverer, build never blocks.
//
// Leases are generation-stamped: a stale handle from a returned-then-reborrowed
// slot cannot write into or double-return a different borrow (Hazard B).
type qwpSenderPool struct {
	mu sync.Mutex
	// notify is closed to wake every acquire waiter; recreated under mu after
	// each broadcast. The cond-via-channel idiom gives acquire a timed wait
	// (sync.Cond has none).
	notify chan struct{}

	all       []*qwpSenderSlot
	available []*qwpSenderSlot

	minSize, maxSize int
	acquireTimeout   time.Duration
	idleTimeout      time.Duration
	maxLifetime      time.Duration

	inFlightCreations int
	closed            bool
	// closing is set before the housekeeper is stopped so an about-to-start reap
	// bails immediately instead of racing teardown. Distinct from closed, which
	// close() sets after the housekeeper has joined.
	closing atomic.Bool

	baseConf           string
	errorHandler       SenderErrorHandler
	connectionListener SenderConnectionListener

	// Store-and-forward coordination (storeAndForward true iff sfDir != "").
	storeAndForward bool
	sfDir           string
	slotBase        string
	slotInUse       []bool // reservation bitmap for SF slot indices [0, maxSize)
	closingSlots    int    // SF slots removed from `all` but still releasing their flock
	leakedSlots     int    // SF slots permanently retired (close left the flock held)
}

// qwpSenderSlot is one reusable pool entry. generation is bumped under the pool
// lock on every hand-out and every return; a lease carries the value it was
// issued, so a stale lease (slot already re-borrowed) is detectable.
type qwpSenderSlot struct {
	delegate   QwpSender
	generation atomic.Uint64
	slotIndex  int // SF slot index, or -1 in memory mode
	createdAt  time.Time
	idleSince  time.Time
}

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
) (*qwpSenderPool, error) {
	if minSize < 0 || maxSize < 1 || minSize > maxSize {
		return nil, fmt.Errorf("qwp pool: invalid sizes min=%d max=%d", minSize, maxSize)
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
		storeAndForward:    template.sfDir != "",
		sfDir:              template.sfDir,
	}
	if p.storeAndForward {
		p.slotBase = template.senderId
		if p.slotBase == "" {
			p.slotBase = qwpSfDefaultSenderId
		}
		p.slotInUse = make([]bool, maxSize)
	}

	// Eagerly prewarm minSize slots. A prewarm failure tears down whatever was
	// built and propagates (a misconfig or down server with a fail-fast
	// connect mode should surface at build()).
	for i := 0; i < minSize; i++ {
		slot, err := p.createSlot(ctx, false)
		if err != nil {
			_ = p.close(ctx)
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
// Construction-only: like createSlot, the sole caller is newQwpSenderPool,
// single-threaded before the pool is published, so the direct slotInUse writes
// and the createSlotAt index bookkeeping here run WITHOUT p.mu. Do not call this
// off the construction path.
func (p *qwpSenderPool) recoverStrandedSlots(ctx context.Context) {
	for i := 0; i < p.maxSize; i++ {
		if p.slotInUse[i] {
			continue // already owned by a prewarmed slot
		}
		dir := filepath.Join(p.sfDir, p.slotBase+"-"+strconv.Itoa(i))
		if !qwpSfIsCandidateOrphan(dir) {
			continue
		}
		slot, err := p.createSlotAt(ctx, i, true)
		if err != nil {
			continue // best-effort; the dir's data stays on disk for next start
		}
		// Reserve the index: this live recovered sender owns dir <base>-i and
		// holds its flock, so a later borrow's allocateSlotIndexLocked must not
		// re-pick i and collide (Hazard A — the reservation bitmap is the
		// primary defense, not just the flock backstop).
		p.slotInUse[i] = true
		p.all = append(p.all, slot)
		p.available = append(p.available, slot)
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
		if n := len(p.available); n > 0 {
			slot := p.available[n-1]
			p.available = p.available[:n-1]
			// A slot's background send loop keeps running while it sits idle and
			// can terminally HALT with no lease watching (reconnect-budget
			// exhaustion after a long outage, PROTOCOL_VIOLATION close, AUTH_FAILED
			// on reconnect, incompatible-server strike). Handing such a poisoned
			// slot to the next borrower would fail it through no fault of its own
			// and break borrower isolation precisely during incident recovery, so
			// discard it and look for another (M1).
			if slotTerminallyFailed(slot.delegate) {
				p.discardLocked(slot) // releases p.mu
				p.mu.Lock()
				continue
			}
			gen := slot.generation.Add(1)
			p.mu.Unlock()
			return &qwpPooledSender{pool: p, slot: slot, gen: gen}, nil
		}
		if p.capUsedLocked() < p.maxSize {
			p.inFlightCreations++
			slotIndex := -1
			if p.storeAndForward {
				slotIndex = p.allocateSlotIndexLocked()
			}
			p.mu.Unlock()
			slot, err := p.createSlotAt(ctx, slotIndex, false)
			p.mu.Lock()
			p.inFlightCreations--
			if err != nil {
				p.freeSlotIndexLocked(slotIndex)
				p.broadcastLocked()
				p.mu.Unlock()
				return nil, err
			}
			if p.closed {
				p.freeSlotIndexLocked(slotIndex)
				p.mu.Unlock()
				// Disconnect off-lock with a panic guard and a background ctx — a
				// cancelled caller ctx must not cut this close short, matching every
				// other close site in the pool.
				_ = closeSlotGuarded(context.Background(), slot.delegate)
				return nil, errPoolClosed
			}
			p.all = append(p.all, slot)
			gen := slot.generation.Add(1)
			p.mu.Unlock()
			return &qwpPooledSender{pool: p, slot: slot, gen: gen}, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			p.mu.Unlock()
			return nil, fmt.Errorf("%w after %s", errPoolExhausted, p.acquireTimeout)
		}
		ch := p.notify
		p.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
		timer.Stop()
		p.mu.Lock()
	}
}

// giveBack returns a slot to the available set, dropping the return if the
// lease is stale (already returned and possibly re-borrowed) or the pool is
// closed — this is what makes lease Close idempotent under a concurrent
// re-borrow (Hazard B). Broken slots are discarded instead of recycled.
func (p *qwpSenderPool) giveBack(ctx context.Context, ps *qwpPooledSender, broken bool) {
	p.mu.Lock()
	if ps.slot.generation.Load() != ps.gen {
		p.mu.Unlock()
		return // stale lease (already returned / re-borrowed) — never double-act
	}
	// Invalidate this lease so a duplicate Close is dropped above.
	ps.slot.generation.Add(1)
	if p.closed {
		// The pool was torn down while this slot was on loan, so close() left it
		// for us. The producer is done now, so closing the delegate here cannot
		// race a writer.
		p.mu.Unlock()
		_ = closeSlotGuarded(ctx, ps.slot.delegate)
		return
	}
	if broken {
		p.discardLocked(ps.slot)
		return // discardLocked unlocks
	}
	ps.slot.idleSince = time.Now()
	p.available = append(p.available, ps.slot)
	p.broadcastLocked()
	p.mu.Unlock()
}

// terminalReporter lets the pool detect a delegate whose background send loop
// has terminally HALTed without growing the public QwpSender interface. Every
// pooled delegate is a *qwpLineSender, which implements it.
type terminalReporter interface {
	terminallyFailed() bool
}

// slotTerminallyFailed reports whether a slot's delegate has latched a terminal
// error (a background HALT). A poisoned slot must never be handed to a borrower
// (M1) nor recycled after a producer error (M4) — it is discarded and rebuilt
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
// fluent-API error — the C1-correct lease-return path, distinct from Flush.
// Every pooled delegate is a *qwpLineSender, which implements it.
type returnFlusher interface {
	flushForReturn(ctx context.Context) error
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
// faulting Close cannot strand the pool's SF slot accounting (closingSlots /
// slotInUse) or skip sibling teardowns. qwpLineSender.Close is itself panic-
// guarded on its I/O goroutines; this is defense-in-depth for the pool's own
// invariants on the paths that bump closingSlots before the out-of-lock Close.
func closeSlotGuarded(ctx context.Context, delegate LineSender) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp pool: delegate close panicked: %v", r)
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
func (p *qwpSenderPool) discardLocked(slot *qwpSenderSlot) {
	p.removeFromAllLocked(slot)
	if p.storeAndForward && slot.slotIndex >= 0 {
		p.closingSlots++
	}
	p.mu.Unlock()
	closeErr := closeSlotGuarded(context.Background(), slot.delegate)
	p.mu.Lock()
	p.reclaimSlotLocked(slot, closeErr)
	p.broadcastLocked()
	p.mu.Unlock()
}

// markClosing signals reapIdle to bail. Called before the housekeeper is
// stopped so a not-yet-started reap does not run during the stop-and-join window.
func (p *qwpSenderPool) markClosing() { p.closing.Store(true) }

// reapIdle closes idle-expired / over-age slots from the available set, never
// shrinking below minSize. Called by the housekeeper.
func (p *qwpSenderPool) reapIdle() {
	if p.closing.Load() {
		return
	}
	now := time.Now()
	var toClose []*qwpSenderSlot
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	kept := p.available[:0]
	for _, slot := range p.available {
		idleExpired := p.idleTimeout > 0 && now.Sub(slot.idleSince) >= p.idleTimeout
		overAge := p.maxLifetime > 0 && now.Sub(slot.createdAt) >= p.maxLifetime
		// A slot poisoned by a background HALT is useless to a borrower, so reap
		// it even at minSize (borrow re-creates a fresh slot on demand). This
		// proactively clears poisoned slots so a wave of borrowers does not have
		// to discard them one by one during incident recovery (M1).
		poisoned := slotTerminallyFailed(slot.delegate)
		// removeFromAllLocked already shrinks p.all, so test it directly — do
		// not also subtract len(toClose) or the reaped count is counted twice
		// and the pool under-reaps to ~min+excess/2 per tick.
		// Idle/age recycling is floored at minSize, so max_lifetime_ms is inert
		// for min slots (M2): no evict-and-replace (recreating mid-outage could
		// orphan unacked SF data); QWP self-reconnects anyway. See README.
		// Spare an idle/over-age slot that still has unacked rows: reaping it in
		// memory mode destroys them and cuts short the reconnect/replay window. A
		// poisoned slot is exempt — its HALT is terminal, so the rows are already
		// lost and holding the slot only wastes it.
		hasUnacked := !poisoned && slotHasUnackedRows(slot.delegate)
		if poisoned || ((idleExpired || overAge) && len(p.all) > p.minSize && !hasUnacked) {
			p.removeFromAllLocked(slot)
			if p.storeAndForward && slot.slotIndex >= 0 {
				p.closingSlots++
			}
			toClose = append(toClose, slot)
			continue
		}
		kept = append(kept, slot)
	}
	p.available = kept
	p.mu.Unlock()

	for _, slot := range toClose {
		closeErr := closeSlotGuarded(context.Background(), slot.delegate)
		if closeErr != nil {
			// Autonomous reap has no caller to return this to (the producer long
			// since returned its lease), so surface it here — it may be the
			// "data may be lost" drain timeout.
			log.Printf("[WARN] qwp pool: reaping a slot failed to drain cleanly: %v", closeErr)
		}
		p.mu.Lock()
		p.reclaimSlotLocked(slot, closeErr)
		p.mu.Unlock()
	}
	if len(toClose) > 0 {
		p.mu.Lock()
		p.broadcastLocked()
		p.mu.Unlock()
	}
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
// A BorrowSender whose slot build is still in flight is likewise not closed here
// — the creating goroutine closes its just-built delegate itself once it acquires
// p.mu and observes p.closed (see borrow). close() does not block for that, so the
// "all resources released" postcondition is reached a beat after close() returns:
// in SF mode the just-built slot's flock can linger briefly, and an immediate
// reopen on the same sf_dir may momentarily observe it. This is covered by the
// same contract — quiesce borrows and return every lease before Close.
//
// Slots close concurrently on context.Background(): each drain is bounded by
// close_flush_timeout, so the caller's ctx must neither serialize the drains
// (an N-slot outage would stall shutdown for N × the timeout) nor cancel them
// (dropping undelivered memory-mode rows). Matches the lease-return and reap
// paths (M2). The ctx argument is accepted for interface symmetry but unused.
func (p *qwpSenderPool) close(_ context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	toClose := append([]*qwpSenderSlot(nil), p.available...)
	p.all = nil
	p.available = nil
	p.broadcastLocked()
	p.mu.Unlock()

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for _, slot := range toClose {
		wg.Add(1)
		go func(delegate QwpSender) {
			defer wg.Done()
			if err := closeSlotGuarded(context.Background(), delegate); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(slot.delegate)
	}
	wg.Wait()
	return firstErr
}

// createSlot allocates an SF slot index when needed and builds a slot.
//
// Construction-only: the sole caller is newQwpSenderPool's prewarm loop, which
// runs single-threaded before the pool is published, so the *Locked index
// helpers below are invoked WITHOUT holding p.mu. Do not call createSlot off the
// construction path — once the pool is shared, allocateSlotIndexLocked /
// freeSlotIndexLocked must run under p.mu (as borrow does) or they race the
// slotInUse bitmap.
func (p *qwpSenderPool) createSlot(ctx context.Context, async bool) (*qwpSenderSlot, error) {
	slotIndex := -1
	if p.storeAndForward {
		slotIndex = p.allocateSlotIndexLocked()
	}
	slot, err := p.createSlotAt(ctx, slotIndex, async)
	if err != nil {
		p.freeSlotIndexLocked(slotIndex)
		return nil, err
	}
	return slot, nil
}

// createSlotAt builds a sender bound to slotIndex (-1 in memory mode). It parses
// a fresh config from the base string and overrides the per-slot identity, the
// in-range orphan fence, the ingest callbacks, and (for recovery) async connect.
// Guarded against a panic in the heavy build path (Hazard I).
func (p *qwpSenderPool) createSlotAt(ctx context.Context, slotIndex int, async bool) (slot *qwpSenderSlot, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp pool: sender build panicked: %v", r)
		}
	}()
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
		return nil, err
	}
	// The facade is ws/wss-only, so newLineSender always yields a QWP sender;
	// asserting here lets a pooled lease forward the full QwpSender surface.
	qwpDelegate, ok := delegate.(QwpSender)
	if !ok {
		_ = delegate.Close(ctx)
		return nil, fmt.Errorf("qwp pool: delegate is %T, not a QwpSender", delegate)
	}
	now := time.Now()
	return &qwpSenderSlot{delegate: qwpDelegate, slotIndex: slotIndex, createdAt: now, idleSince: now}, nil
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
	return err == nil && i >= 0 && i < p.maxSize
}

// capUsedLocked is the capacity-accounting sum; a new creation is admitted only
// while it is below maxSize so closing/leaked/in-flight slots never let the
// pool over-allocate (Hazard F). Caller holds mu.
func (p *qwpSenderPool) capUsedLocked() int {
	return len(p.all) + p.inFlightCreations + p.closingSlots + p.leakedSlots
}

func (p *qwpSenderPool) allocateSlotIndexLocked() int {
	for i := range p.slotInUse {
		if !p.slotInUse[i] {
			p.slotInUse[i] = true
			return i
		}
	}
	// The cap check admits a creation only below maxSize, so a free index
	// always exists here; -1 is a defensive sentinel (createSlotAt tolerates it
	// as memory mode, but storeAndForward callers never reach this).
	return -1
}

func (p *qwpSenderPool) freeSlotIndexLocked(idx int) {
	if idx >= 0 && idx < len(p.slotInUse) {
		p.slotInUse[idx] = false
	}
}

// reclaimSlotLocked returns an SF slot index to the free set after its delegate
// close completed. Caller holds mu.
//
// Hazard F (a retired, never-reused index) does NOT manifest in Go's model the
// way it does in Java: the Go sender's Close synchronously joins the I/O
// goroutine (sendLoopClose's wg.Wait) and then releases the slot flock in
// engineClose, so by the time Close returns — error or not — the flock is
// released. A close error here is benign (e.g. a drain-timeout on a slot reaped
// while the server is down) and must NOT permanently erode capacity, so the
// index is always freed. leakedSlots therefore stays 0; it is retained in the
// cap math only as a defensive accountant.
func (p *qwpSenderPool) reclaimSlotLocked(slot *qwpSenderSlot, closeErr error) {
	_ = closeErr
	if !p.storeAndForward || slot.slotIndex < 0 {
		return
	}
	p.closingSlots--
	p.freeSlotIndexLocked(slot.slotIndex)
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

// broadcastLocked wakes every acquire waiter. Caller holds mu.
func (p *qwpSenderPool) broadcastLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

// poolSnapshot reports live/idle/leaked counts for tests and introspection.
func (p *qwpSenderPool) poolSnapshot() (total, available, leaked int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.all), len(p.available), p.leakedSlots
}

var (
	errPoolClosed    = errors.New("qwp pool: handle is closed")
	errPoolExhausted = errors.New("qwp pool: timed out waiting for a sender")
	errStaleLease    = errors.New("qwp pool: sender used after it was returned to the pool")
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
// to QwpSender — matching the Java client, whose borrowSender() returns the
// complete Sender. BorrowSender returns the LineSender view; this keeps the
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
// on a stream with sporadic bad records (M4).
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

func (ps *qwpPooledSender) BackgroundDrainers() []QwpBackgroundDrainer {
	if !ps.live() {
		return nil
	}
	return ps.slot.delegate.BackgroundDrainers()
}

// Close returns the leased sender to the pool. It flushes committed rows first
// (surfacing but not swallowing a latched fluent-API error) so the next
// borrower starts clean; only a terminal fault marks the slot broken so it is
// discarded rather than recycled — a benign latch or a ctx-bounded timeout
// leaves a healthy slot to be reused. Idempotent — a stale lease no-ops.
//
// The flush and return run on context.Background, not the caller's ctx: the
// return flush only publishes into the cursor engine (it never waits for the
// server ACK), so
// a request-scoped ctx must not (a) leave pending rows un-published in a slot
// the next borrower reuses, or (b) mark a healthy slot broken via a
// context.Canceled / DeadlineExceeded. Both would thrash the pool under the
// standard `ctx, cancel := ...; defer sender.Close(ctx)` pattern. The publish is
// bounded by the engine's append deadline, not the caller's ctx.
func (ps *qwpPooledSender) Close(_ context.Context) error {
	if !ps.live() {
		return nil
	}
	var flushErr error
	ctx1 := context.Background()
	if !ps.broken {
		// Route the return through flushForReturn, NOT Flush. Flush both
		// early-returns errFlushWithPendingMessage while a row is open and
		// early-returns a latched fluent-API error ahead of its pending-rows
		// branch — either way it leaves committed rows unflushed in a
		// non-terminal (kept) slot, poisoning the next borrower (C1).
		// flushForReturn mirrors closeCursor: drop the open row, surface the
		// latch, but still flush committed rows so the slot returns clean.
		if fr, ok := ps.slot.delegate.(returnFlusher); ok {
			flushErr = fr.flushForReturn(ctx1)
		} else {
			flushErr = ps.slot.delegate.Flush(ctx1)
		}
		if flushErr != nil {
			// A benign latched fluent-API error (already cleared, committed
			// rows already flushed) leaves a healthy connection; only a
			// terminal fault forfeits the slot. Matches the in-use paths'
			// markBrokenIfTerminal (M4).
			ps.markBrokenIfTerminal()
		}
	}
	ps.pool.giveBack(ctx1, ps, ps.broken)
	return flushErr
}
