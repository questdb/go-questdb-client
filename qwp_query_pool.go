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
// constructor, which is exactly Java's create+connect on first borrow.
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

	baseConf string
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
) (*qwpQueryPool, error) {
	if minSize < 0 || maxSize < 1 || minSize > maxSize {
		return nil, fmt.Errorf("qwp query pool: invalid sizes min=%d max=%d", minSize, maxSize)
	}
	p := &qwpQueryPool{
		notify:         make(chan struct{}),
		minSize:        minSize,
		maxSize:        maxSize,
		acquireTimeout: acquireTimeout,
		idleTimeout:    idleTimeout,
		maxLifetime:    maxLifetime,
		baseConf:       conf,
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
			// discard it and look for another (mirrors the sender pool's M1).
			if w.client.terminalError() != nil {
				p.discardWorkerLocked(ctx, w) // releases p.mu
				p.mu.Lock()
				continue
			}
			gen := w.generation.Add(1)
			p.mu.Unlock()
			return &Query{pool: p, worker: w, gen: gen}, nil
		}
		if len(p.all)+p.inFlightCreations < p.maxSize {
			p.inFlightCreations++
			p.mu.Unlock()
			w, err := p.createWorker(ctx)
			p.mu.Lock()
			p.inFlightCreations--
			if err != nil {
				p.broadcastLocked()
				p.mu.Unlock()
				return nil, err
			}
			if p.closed {
				p.mu.Unlock()
				_ = closeQueryClientGuarded(ctx, w.client)
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

func (p *qwpQueryPool) giveBack(ctx context.Context, q *Query, broken bool) {
	p.mu.Lock()
	if q.worker.generation.Load() != q.gen {
		p.mu.Unlock()
		return // stale lease (already returned / re-borrowed) — never double-act
	}
	q.worker.generation.Add(1)
	if p.closed {
		// On loan at close (close() skips on-loan workers), so self-close now.
		// Query.Close drained the active cursor first, so this can't race a read.
		p.mu.Unlock()
		_ = closeQueryClientGuarded(ctx, q.worker.client)
		return
	}
	if broken {
		p.removeFromAllLocked(q.worker)
		p.mu.Unlock()
		_ = closeQueryClientGuarded(ctx, q.worker.client)
		p.mu.Lock()
		p.broadcastLocked()
		p.mu.Unlock()
		return
	}
	q.worker.idleSince = time.Now()
	p.available = append(p.available, q.worker)
	p.broadcastLocked()
	p.mu.Unlock()
}

// discardWorkerLocked evicts a worker from `all` and closes its client outside
// the lock. Caller holds mu; discardWorkerLocked releases it. Used on borrow
// when a pooled worker is found terminally failed (mirrors the sender pool's
// discardLocked).
func (p *qwpQueryPool) discardWorkerLocked(ctx context.Context, w *qwpQueryWorker) {
	p.removeFromAllLocked(w)
	p.mu.Unlock()
	_ = closeQueryClientGuarded(ctx, w.client)
	p.mu.Lock()
	p.broadcastLocked()
	p.mu.Unlock()
}

func (p *qwpQueryPool) reapIdle() {
	now := time.Now()
	var toClose []*qwpQueryWorker
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	kept := p.available[:0]
	for _, w := range p.available {
		idleExpired := p.idleTimeout > 0 && now.Sub(w.idleSince) >= p.idleTimeout
		overAge := p.maxLifetime > 0 && now.Sub(w.createdAt) >= p.maxLifetime
		// A worker poisoned by a background transport-terminal failure is useless
		// to a borrower, so reap it even at minSize (borrow re-creates a fresh
		// worker on demand), mirroring the sender pool's M1 reap.
		poisoned := w.client.terminalError() != nil
		// removeFromAllLocked already shrinks p.all; test it directly (don't
		// also subtract len(toClose) — that double-counts and under-reaps).
		// Idle/age recycling is floored at minSize, so max_lifetime_ms is inert
		// for min workers (M2, see sender pool + README); poisoned reaps anyway.
		if poisoned || ((idleExpired || overAge) && len(p.all) > p.minSize) {
			p.removeFromAllLocked(w)
			toClose = append(toClose, w)
			continue
		}
		kept = append(kept, w)
	}
	p.available = kept
	p.mu.Unlock()

	for _, w := range toClose {
		_ = closeQueryClientGuarded(context.Background(), w.client)
	}
	if len(toClose) > 0 {
		p.mu.Lock()
		p.broadcastLocked()
		p.mu.Unlock()
	}
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

func (p *qwpQueryPool) close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	// Close only available workers: an on-loan one may have a live Batches()
	// reading its aliased buffers, which a concurrent Close would free (M1,
	// QwpQueryClient.Close UB). On-loan leases self-close via giveBack; a never-
	// returned lease leaks, as in qwpSenderPool.close.
	toClose := append([]*qwpQueryWorker(nil), p.available...)
	p.all = nil
	p.available = nil
	p.broadcastLocked()
	p.mu.Unlock()

	var firstErr error
	for _, w := range toClose {
		if err := closeQueryClientGuarded(ctx, w.client); err != nil && firstErr == nil {
			firstErr = err
		}
	}
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
	client, cerr := QwpQueryClientFromConf(ctx, p.baseConf)
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

// Query submits a SELECT-style statement and returns a cursor over its result
// batches. See QwpQueryClient.Query. Iterate Batches() and Close the returned
// cursor (or rely on this handle's Close to drain it).
func (q *Query) Query(ctx context.Context, sql string, opts ...QwpQueryOption) *QwpQuery {
	if !q.live() || q.broken {
		// Use-after-close, or a worker desynced by a prior abandoned drain
		// (q.broken — live() does not cover it, since a broken lease must still
		// be Close()d to evict the worker): return a cursor that surfaces the
		// error from its first Batches() yield rather than a nil that panics the
		// caller, and never submit on the (possibly re-borrowed or desynced)
		// worker.
		return staleQueryCursor()
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
		desynced := q.active.drainFailed.Load()
		q.active = nil
		if desynced {
			q.broken = true
			return staleQueryCursor()
		}
	}
	q.active = q.worker.client.Query(ctx, sql, opts...)
	return q.active
}

// staleQueryCursor returns an already-done cursor whose first Batches() yield
// surfaces errStaleLease, used when the lease is dead or the worker's wire is
// desynced and must not serve another query.
func staleQueryCursor() *QwpQuery {
	stale := &QwpQuery{pendingErr: errStaleLease}
	stale.state.Store(qwpQueryStateDone)
	return stale
}

// Exec runs a non-SELECT statement and blocks until completion. See
// QwpQueryClient.Exec.
func (q *Query) Exec(ctx context.Context, sql string, opts ...QwpQueryOption) (ExecResult, error) {
	if !q.live() || q.broken {
		// Use-after-close, or a worker already desynced by a prior abandoned
		// drain: refuse to submit (mirrors the Query top-level gate).
		return ExecResult{}, errStaleLease
	}
	// Drain any cursor left open by Query first: the leased client's
	// dispatcher is single-stream, so an in-flight cursor would otherwise
	// race the Exec submission for the next terminal frame.
	if q.active != nil {
		q.active.Close()
		if q.active.drainFailed.Load() {
			q.broken = true
		}
		q.active = nil
	}
	if q.broken {
		// The drain abandoned before its terminal frame, leaving leftover
		// RESULT_BATCH events on the single-stream wire that this Exec would
		// misread as its own (the egress path does not demux by requestId).
		// Mirror Query: refuse to submit on the desynced wire and surface
		// errStaleLease — submitting anyway would both misreport the Exec
		// result and leave the statement queued to execute server-side
		// unobserved, so a caller retry could double-execute it. Close evicts
		// the broken worker rather than recycling it.
		return ExecResult{}, errStaleLease
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
	return res, err
}

// Close returns the leased client to the pool, draining any cursor left open by
// Query first. Idempotent; the underlying client stays connected for reuse.
func (q *Query) Close() error {
	if !q.live() {
		return nil
	}
	if q.active != nil {
		q.active.Close()
		// A drain that abandoned before the terminal frame leaves leftover
		// events on the wire that the next borrower would consume as its own
		// (the egress path does not demux by requestId), so evict rather than
		// recycle.
		if q.active.drainFailed.Load() {
			q.broken = true
		}
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
	q.closed = true
	q.pool.giveBack(context.Background(), q, q.broken)
	return nil
}
