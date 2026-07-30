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
	"sync"
	"sync/atomic"
	"time"
)

// qwpQueryPool is the elastic QWP egress pool behind the QuestDB facade. It
// mirrors qwpSenderPool's borrow/return/reap shape (generation-stamped leases,
// cond-via-channel acquire, prewarm + lazy growth) but pools QwpQueryClients
// and has no store-and-forward concerns.
//
// minSize == 0 is the lazy read pool used by lazy_connect: build prewarms no
// clients (so a down server never fails the facade build), and the first
// borrow create+connects on demand — the Go QwpQueryClient connects in its
// constructor: create + connect on first borrow.
type qwpQueryPool struct {
	mu     sync.Mutex
	notify chan struct{}

	all       []*qwpQueryWorker
	available []*qwpQueryWorker

	minSize, maxSize int
	acquireTimeout   time.Duration
	idleTimeout      time.Duration
	maxLifetime      time.Duration

	inFlightCreations int
	closed            bool
	closing           atomic.Bool // set before the housekeeper stops; reapIdle bails

	// pendingTeardowns counts client teardowns running off-lock after the
	// worker has already left `all` (giveBack's broken branch, discardWorkerLocked,
	// reap victims). close() adds it to the outstanding count so it does not
	// return while a query connection is still winding down. Guarded by mu.
	// Mirrors qwpSenderPool.pendingLeaseTeardowns.
	pendingTeardowns int

	baseConf string
	logger   *slog.Logger
}

type qwpQueryWorker struct {
	client     *QwpQueryClient
	generation atomic.Uint64
	createdAt  time.Time
	idleSince  time.Time
}

func newQwpQueryPool(
	ctx context.Context,
	conf string,
	minSize, maxSize int,
	acquireTimeout, idleTimeout, maxLifetime time.Duration,
	logger *slog.Logger,
) (*qwpQueryPool, error) {
	if minSize < 0 || maxSize < 1 || minSize > maxSize {
		return nil, fmt.Errorf("qwp query pool: invalid sizes min=%d max=%d (max defaults to %d when unset — raise query_pool_max alongside min)", minSize, maxSize, qwpDefaultPoolMax)
	}
	p := &qwpQueryPool{
		notify:         make(chan struct{}),
		minSize:        minSize,
		maxSize:        maxSize,
		acquireTimeout: acquireTimeout,
		idleTimeout:    idleTimeout,
		maxLifetime:    maxLifetime,
		baseConf:       conf,
		logger:         logger,
	}
	for i := 0; i < minSize; i++ {
		w, err := p.createWorker(ctx)
		if err != nil {
			p.close(ctx)
			return nil, err
		}
		p.all = append(p.all, w)
		p.available = append(p.available, w)
	}
	return p, nil
}

// borrow leases a query client, blocking up to acquireTimeout (or ctx) when the
// pool is at capacity. The returned *Query must be Close()d to return it.
func (p *qwpQueryPool) borrow(ctx context.Context) (*Query, error) {
	deadline := time.Now().Add(p.acquireTimeout)
	p.mu.Lock()
	for {
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		if n := len(p.available); n > 0 {
			w := p.available[n-1]
			p.available = p.available[:n-1]
			// A worker's client can latch a transport-terminal error (failover
			// exhaustion on a background reconnect) while it sits idle, with no
			// lease watching. Handing such a poisoned worker to the next borrower
			// would fail their first query through no fault of their own, so
			// discard it and look for another (mirrors the sender pool).
			if w.client.terminalError() != nil {
				// Background: this worker is already terminally failed, so its
				// close is incidental teardown a cancelled borrow ctx must not
				// cut short (matches the sender pool).
				p.discardWorkerLocked(context.Background(), w) // releases p.mu
				p.mu.Lock()
				continue
			}
			gen := w.generation.Add(1)
			p.mu.Unlock()
			return &Query{pool: p, worker: w, gen: gen}, nil
		}
		// Unlike the sender pool, discards remove from p.all immediately with no
		// closingSlots equivalent, so a concurrent discard's out-of-lock close can
		// briefly put live clients at maxSize+K. Intentional: query workers hold no
		// flock/mmap (unlike SF sender slots), so a transient overshoot is harmless.
		if len(p.all)+p.inFlightCreations < p.maxSize {
			// A waiter woken by the acquire timer can reach here with the
			// deadline already past; report pool exhaustion rather than
			// starting a dial doomed by an expired context.
			if time.Until(deadline) <= 0 {
				p.mu.Unlock()
				return nil, fmt.Errorf("%w after %s", errQueryPoolExhausted, p.acquireTimeout)
			}
			p.inFlightCreations++
			p.mu.Unlock()
			// Bound the dial by the acquire deadline (sender-pool / HikariCP parity)
			// so a black-holed server is abandoned within acquire_timeout_ms.
			bctx, cancel := context.WithDeadline(ctx, deadline)
			w, err := p.createWorker(bctx)
			cancel()
			p.mu.Lock()
			p.inFlightCreations--
			if err != nil {
				p.broadcastLocked()
				p.mu.Unlock()
				return nil, err
			}
			if p.closed {
				// inFlightCreations dropped just above; wake close()'s
				// outstanding-lease wait so it re-checks. The just-built worker was
				// never handed out, so closing it off-lock races nothing.
				p.broadcastLocked()
				p.mu.Unlock()
				_ = closeQueryClientGuarded(context.Background(), w.client)
				return nil, errPoolClosed
			}
			p.all = append(p.all, w)
			gen := w.generation.Add(1)
			p.mu.Unlock()
			return &Query{pool: p, worker: w, gen: gen}, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			p.mu.Unlock()
			return nil, fmt.Errorf("%w after %s", errQueryPoolExhausted, p.acquireTimeout)
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

// giveBack returns a lease's worker to the pool (or closes it, when the pool is
// already shut down or the worker is broken). The close paths run on
// context.Background(): they are incidental teardown of a worker nobody will
// reuse, which a caller deadline must not cut short. Returns the close error,
// if any — recycling a healthy worker never fails.
func (p *qwpQueryPool) giveBack(q *Query, broken bool) error {
	p.mu.Lock()
	if q.worker.generation.Load() != q.gen {
		p.mu.Unlock()
		return nil // stale lease (already returned / re-borrowed) — never double-act
	}
	q.worker.generation.Add(1)
	if p.closed {
		// On loan at close (close() skips on-loan workers), so self-close now.
		// Query.Close drained the active cursor first, so this can't race a read.
		// Drop the worker from p.all and wake close()'s outstanding-lease wait so
		// it does not return while this teardown is still in flight.
		p.mu.Unlock()
		err := closeQueryClientGuarded(context.Background(), q.worker.client)
		p.mu.Lock()
		p.removeFromAllLocked(q.worker)
		p.broadcastLocked()
		p.mu.Unlock()
		return err
	}
	if broken {
		// Drop from `all` before the off-lock close but count the teardown so
		// close()'s outstanding wait still sees it (the closed branch above keeps
		// the worker in `all` across its close, which the formula already covers;
		// this branch removes first, so it needs the explicit counter).
		p.removeFromAllLocked(q.worker)
		p.pendingTeardowns++
		p.mu.Unlock()
		err := closeQueryClientGuarded(context.Background(), q.worker.client)
		p.mu.Lock()
		p.pendingTeardowns--
		p.broadcastLocked()
		p.mu.Unlock()
		return err
	}
	q.worker.idleSince = time.Now()
	p.available = append(p.available, q.worker)
	p.broadcastLocked()
	p.mu.Unlock()
	return nil
}

// discardWorkerLocked evicts a worker from `all` and closes its client outside
// the lock. Caller holds mu; discardWorkerLocked releases it. Used on borrow
// when a pooled worker is found terminally failed (mirrors the sender pool's
// discardLocked).
func (p *qwpQueryPool) discardWorkerLocked(ctx context.Context, w *qwpQueryWorker) {
	p.removeFromAllLocked(w)
	// The worker is out of `all`, so count the off-lock close in the outstanding
	// wait or close() could return while this teardown is still in flight.
	p.pendingTeardowns++
	p.mu.Unlock()
	_ = closeQueryClientGuarded(ctx, w.client)
	p.mu.Lock()
	p.pendingTeardowns--
	p.broadcastLocked()
	p.mu.Unlock()
}

// markClosing signals reapIdle to bail before the housekeeper is stopped.
func (p *qwpQueryPool) markClosing() { p.closing.Store(true) }

// queryReapCloseHook, when non-nil, is invoked at the start of each reap-victim
// close goroutine. Test seam only (mirrors reapCloseHook): it lets a test hold a
// reap teardown in flight to assert close() waits for it. Nil in production.
var queryReapCloseHook func()

func (p *qwpQueryPool) reapIdle() {
	if p.closing.Load() {
		return
	}
	toClose := p.selectReapVictims(time.Now())
	if len(toClose) == 0 {
		return
	}
	// Close concurrently so an N-worker sweep is bounded by one close budget,
	// not N — a sequential sweep of wedged connections would overrun the
	// housekeeper join budget and outlive QuestDB.Close (matches the sender
	// pool's reap).
	var wg sync.WaitGroup
	for _, w := range toClose {
		wg.Add(1)
		go func(client *QwpQueryClient) {
			defer wg.Done()
			if queryReapCloseHook != nil {
				queryReapCloseHook()
			}
			_ = closeQueryClientGuarded(context.Background(), client)
		}(w.client)
	}
	wg.Wait()
	p.mu.Lock()
	// Balance the per-victim increments in selectReapVictims now the teardowns
	// have completed, so close()'s outstanding wait no longer counts them.
	p.pendingTeardowns -= len(toClose)
	p.broadcastLocked()
	p.mu.Unlock()
}

// selectReapVictims removes the idle-expired / over-age / poisoned workers from
// the available set under the lock and returns them for off-lock closing. The
// lock is released via defer so a panic in the selection can never strand it (the
// reap runs behind a recover in the housekeeper).
func (p *qwpQueryPool) selectReapVictims(now time.Time) []*qwpQueryWorker {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	var toClose []*qwpQueryWorker
	kept := p.available[:0]
	for _, w := range p.available {
		idleExpired := p.idleTimeout > 0 && now.Sub(w.idleSince) >= p.idleTimeout
		overAge := p.maxLifetime > 0 && now.Sub(w.createdAt) >= p.maxLifetime
		// A worker poisoned by a background transport-terminal failure is useless
		// to a borrower, so reap it even at minSize (borrow re-creates a fresh
		// worker on demand), mirroring the sender pool's reap.
		poisoned := w.client.terminalError() != nil
		// removeFromAllLocked already shrinks p.all; test it directly (don't
		// also subtract len(toClose) — that double-counts and under-reaps).
		// Idle/age recycling is floored at minSize, so max_lifetime_ms is inert
		// for min workers (see sender pool + README); poisoned reaps anyway.
		if poisoned || ((idleExpired || overAge) && len(p.all) > p.minSize) {
			p.removeFromAllLocked(w)
			// Count the off-lock reap teardown so close()'s outstanding wait
			// cannot return while a reaped client is still winding down (the
			// victim is already out of `all`). Decremented in reapIdle after close.
			p.pendingTeardowns++
			toClose = append(toClose, w)
			continue
		}
		kept = append(kept, w)
	}
	p.available = kept
	return toClose
}

// closeQueryClientGuarded closes a worker's client, converting a panic into an
// error so a faulting Close cannot unwind through the pool's teardown. Mirrors
// the sender pool's closeSlotGuarded.
func closeQueryClientGuarded(ctx context.Context, client *QwpQueryClient) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp query pool: client close panicked: %v", r)
		}
	}()
	return client.Close(ctx)
}

// close shuts the pool down. It bounded-waits for outstanding leases to return
// (mirroring the sender pool) then closes the available workers concurrently on
// context.Background(), so a caller ctx neither serializes the closes nor cancels
// them mid-drain. The ctx argument is accepted for interface symmetry but unused.
func (p *qwpQueryPool) close(_ context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.broadcastLocked()

	// Close only available workers: an on-loan one may have a live Batches()
	// reading its aliased buffers, which a concurrent Close would free —
	// undefined behaviour. On-loan leases self-close via giveBack, which drops
	// them from p.all and wakes this wait. Bounded-wait for those returns
	// (mirrors qwpSenderPool.close) so QuestDB.Close does not return while a query
	// connection is still live, capped so a never-returned lease cannot hang
	// shutdown forever.
	waitBudget := p.acquireTimeout
	if waitBudget > qwpPoolMaxCloseLeaseWait {
		waitBudget = qwpPoolMaxCloseLeaseWait
	}
	deadline := time.Now().Add(waitBudget)
	for {
		outstanding := len(p.all) - len(p.available) + p.inFlightCreations + p.pendingTeardowns
		if outstanding <= 0 {
			break
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ch := p.notify
		p.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
		case <-timer.C:
		}
		timer.Stop()
		p.mu.Lock()
	}
	if leaked := len(p.all) - len(p.available); leaked > 0 {
		qwpEffectiveLogger(p.logger).Warn("qwp query pool: close() leaving borrowed query client(s) alive; "+
			"each is closed when its lease is returned", "leaked", leaked)
	}
	toClose := append([]*qwpQueryWorker(nil), p.available...)
	p.all = nil
	p.available = nil
	p.broadcastLocked()
	p.mu.Unlock()

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for _, w := range toClose {
		wg.Add(1)
		go func(client *QwpQueryClient) {
			defer wg.Done()
			if err := closeQueryClientGuarded(context.Background(), client); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(w.client)
	}
	wg.Wait()
	return firstErr
}

func (p *qwpQueryPool) createWorker(ctx context.Context) (w *qwpQueryWorker, err error) {
	// Panic-guard the build path so a fault converts to an error the caller can
	// clean up after, rather than unwinding through the pool (Hazard I).
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp query pool: client build panicked: %v", r)
		}
	}()
	// Parse and inject the logger (funcs/handlers aren't connect-string-
	// expressible) before building, so every pooled query client emits
	// through the facade's configured sink.
	cfg, cerr := parseQwpQueryConf(p.baseConf)
	if cerr != nil {
		return nil, cerr
	}
	cfg.logger = p.logger
	client, cerr := newQwpQueryClient(ctx, cfg)
	if cerr != nil {
		return nil, cerr
	}
	now := time.Now()
	return &qwpQueryWorker{client: client, createdAt: now, idleSince: now}, nil
}

func (p *qwpQueryPool) removeFromAllLocked(w *qwpQueryWorker) {
	for i, x := range p.all {
		if x == w {
			p.all[i] = p.all[len(p.all)-1]
			p.all = p.all[:len(p.all)-1]
			return
		}
	}
}

func (p *qwpQueryPool) broadcastLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

func (p *qwpQueryPool) poolSnapshot() (total, available int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.all), len(p.available)
}

// ErrQueryPoolExhausted is returned by BorrowQuery when the query pool is at
// query_pool_max and no connection frees up within acquire_timeout_ms. The
// query-pool counterpart of ErrSenderPoolExhausted, so a borrow timeout names
// the pool the caller actually exhausted. Match it with errors.Is.
var ErrQueryPoolExhausted = errors.New("qwp pool: timed out waiting for a query connection")

// ErrQueryLeaseUnusable is returned by a live Query lease's Query / Exec once
// its worker can no longer serve statements: the connection latched a
// transport-terminal error, or an abandoned drain left the single-stream wire
// desynced. Distinct from ErrStaleLease (the handle was already returned or its
// slot evicted) so callers can tell "you gave this handle back" from "close this
// lease and borrow a fresh one". Match it with errors.Is.
var ErrQueryLeaseUnusable = errors.New(
	"qwp pool: query lease's connection is no longer usable (terminally failed or desynced); close the lease and borrow a fresh one")

// Internal aliases so existing call sites keep compiling; errors.Is works
// against either name since they are the same error value.
var (
	errQueryPoolExhausted = ErrQueryPoolExhausted
	errQueryLeaseUnusable = ErrQueryLeaseUnusable
)

// Query is a query session leased from the QuestDB facade via BorrowQuery. It
// delegates to the leased QwpQueryClient's cursor/iterator API; Close returns
// the client to the pool (draining any in-flight cursor first). The real
// disconnect happens only at QuestDB.Close. Not safe for concurrent use; borrow
// one handle per concurrent query.
type Query struct {
	pool   *qwpQueryPool
	worker *qwpQueryWorker
	gen    uint64
	active *QwpQuery // last cursor opened via Query, drained on Close
	closed bool
	broken bool // set on a terminal client failure so Close evicts the worker
}

func (q *Query) live() bool {
	return !q.closed && q.worker.generation.Load() == q.gen
}

// leaseErr reports why the lease must not submit another statement:
// errStaleLease when the handle is dead (returned, or the slot was evicted),
// errQueryLeaseUnusable when the lease is live but its worker is broken or
// latched a transport-terminal error while idle (which marks it broken so Close
// evicts the worker rather than submitting once on the dead wire first). Nil
// when the lease may submit.
func (q *Query) leaseErr() error {
	if !q.live() {
		return errStaleLease
	}
	if !q.broken && q.worker.client.terminalError() != nil {
		q.broken = true
	}
	if q.broken {
		return errQueryLeaseUnusable
	}
	return nil
}

// Query submits a SELECT-style statement and returns a cursor over its result
// batches. See QwpQueryClient.Query. Iterate Batches() and Close the returned
// cursor (or rely on this handle's Close to drain it).
func (q *Query) Query(ctx context.Context, sql string, opts ...QwpQueryOption) *QwpQuery {
	if err := q.leaseErr(); err != nil {
		// Use-after-close, a broken worker, or an idle worker that latched a
		// transport-terminal error: return a cursor that surfaces the error
		// from its first Batches() yield rather than a nil that panics the
		// caller, and never submit on the dead/desynced worker.
		return failedQueryCursor(err)
	}
	if q.active != nil {
		// Drain the cursor left open by a prior Query first, then mirror
		// Exec/Close: a drain that abandoned before the terminal frame leaves
		// leftover events on the single-stream wire that a fresh query would
		// misread as its own (the egress path does not demux by requestId).
		// Mark the worker broken so Close evicts it, and refuse to submit on
		// the desynced wire — surface the error from the cursor's first
		// Batches() yield rather than serving wrong rows first.
		q.active.Close()
		q.active = nil
		if q.worker.client.execDesynced() {
			q.broken = true
			return failedQueryCursor(errQueryLeaseUnusable)
		}
	}
	q.active = q.worker.client.Query(ctx, sql, opts...)
	return q.active
}

// failedQueryCursor returns an already-done cursor whose first Batches() yield
// surfaces err, used when the lease is dead or the worker's wire is desynced
// and must not serve another query.
func failedQueryCursor(err error) *QwpQuery {
	failed := &QwpQuery{pendingErr: err}
	failed.state.Store(qwpQueryStateDone)
	return failed
}

// Exec runs a non-SELECT statement and blocks until completion. See
// QwpQueryClient.Exec.
func (q *Query) Exec(ctx context.Context, sql string, opts ...QwpQueryOption) (ExecResult, error) {
	if err := q.leaseErr(); err != nil {
		// Use-after-close, a broken worker, or an idle worker that latched a
		// transport-terminal error: refuse to submit (mirrors the Query
		// top-level gate).
		return ExecResult{}, err
	}
	// Drain any cursor left open by Query first: the leased client's
	// dispatcher is single-stream, so an in-flight cursor would otherwise
	// race the Exec submission for the next terminal frame.
	if q.active != nil {
		q.active.Close()
		q.active = nil
	}
	if q.worker.client.execDesynced() {
		// The drain abandoned before its terminal frame, leaving leftover
		// RESULT_BATCH events on the single-stream wire that this Exec would
		// misread as its own (the egress path does not demux by requestId).
		// Mirror Query: mark the worker broken so Close evicts it, and refuse
		// to submit on the desynced wire — submitting anyway would both
		// misreport the Exec result and leave the statement queued to execute
		// server-side unobserved, so a caller retry could double-execute it.
		q.broken = true
		return ExecResult{}, errQueryLeaseUnusable
	}
	res, err := q.worker.client.Exec(ctx, sql, opts...)
	// A failover-exhausted failure means the client could not re-establish a
	// connection within budget; evict the worker on return rather than recycle
	// a likely-dead client (a QUERY_ERROR is the server rejecting the SQL — the
	// client is fine, so it is NOT treated as broken).
	if err != nil {
		var fe *QwpFailoverExhaustedError
		if errors.As(err, &fe) {
			q.broken = true
		}
	}
	// Exec's internal cleanup drain (ctx-error / SELECT-via-Exec path) can
	// abandon before its terminal frame on an otherwise-healthy transport,
	// leaving the single-stream wire desynced. That surfaces neither a
	// *QwpFailoverExhaustedError nor a terminalError() — the transport never
	// faulted — so check the client-level latch explicitly and evict, else the
	// next borrower would misread the leftover frames as its own (the Exec-path
	// analogue of the cursor desync handled above and in Close).
	if q.worker.client.execDesynced() {
		q.broken = true
	}
	return res, err
}

// Close returns the leased client to the pool, draining any cursor left open by
// Query first. Idempotent; the underlying client normally stays connected for
// reuse and Close returns nil — the real disconnect happens at QuestDB.Close.
//
// Close deliberately takes no context, unlike the sender lease's Close(ctx):
// the pool return itself never blocks, and the only wait — draining an open
// cursor — runs on an internal budget bounded by query_close_timeout_ms, which
// a caller deadline must not cut short (an interrupted drain would desync the
// worker's wire and force an eviction). A non-nil error is possible only when
// the worker is not recycled — it is broken and evicted, or the pool has
// already shut down — and its client's own Close fails.
func (q *Query) Close() error {
	if !q.live() {
		return nil
	}
	if q.active != nil {
		q.active.Close()
		q.active = nil
	}
	// A cursor that ended in failover-exhaustion (or any transport-terminal
	// fault) latches the client's terminal error; detect it here so the
	// poisoned worker is evicted, not recycled. The Query cursor path surfaces
	// its terminal error to the caller via Batches(), not to this lease, so
	// Exec's eager *QwpFailoverExhaustedError check does not cover it.
	if !q.broken && q.worker.client.terminalError() != nil {
		q.broken = true
	}
	// A cleanup drain that abandoned on a healthy transport — the cursor drain
	// just above, or an earlier Exec-internal one — leaves the wire desynced
	// without latching terminalError(); evict so the leftover frames cannot
	// leak into the next borrower (see execDesynced).
	if !q.broken && q.worker.client.execDesynced() {
		q.broken = true
	}
	q.closed = true
	return q.pool.giveBack(q, q.broken)
}
