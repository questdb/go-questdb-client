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

	// Keep clients in this map until their cleanup finishes. If cleanup fails
	// permanently, keep them here; another Close reports the failure but does
	// not try to close them again.
	teardowns     map[*qwpQueryWorker]struct{}
	closeErr      error
	failedErr     error
	failedWorkers map[*qwpQueryWorker]struct{}

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
			if w != nil {
				lockErr := p.withLock([]*qwpQueryWorker{w}, func() { p.startTeardownLocked(w, nil) })
				err = errors.Join(err, lockErr)
			}
			stopCtx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = p.close(stopCtx)
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
	for {
		var lease *Query
		var result error
		var changed <-chan struct{}
		var build bool
		err := p.withLock(nil, func() {
			if p.closed || p.closing.Load() || p.failedErr != nil {
				result = errors.Join(errPoolClosed, p.failedErr)
				return
			}
			for len(p.available) > 0 {
				n := len(p.available)
				w := p.available[n-1]
				if w.client.terminalError() != nil {
					p.startTeardownLocked(w, nil)
					p.available = p.available[:n-1]
					continue
				}
				lease = &Query{pool: p, worker: w, gen: w.generation.Add(1)}
				p.available = p.available[:n-1]
				return
			}
			if time.Until(deadline) <= 0 {
				result = fmt.Errorf("%w after %s", errQueryPoolExhausted, p.acquireTimeout)
				return
			}
			if len(p.all)+p.inFlightCreations+len(p.teardowns) < p.maxSize {
				p.inFlightCreations++
				build = true
			}
			changed = p.notify
		})
		if err != nil || result != nil {
			return nil, errors.Join(result, err)
		}
		if lease != nil {
			return lease, nil
		}
		if build {
			bctx, cancel := context.WithDeadline(ctx, deadline)
			built := make(chan error, 1)
			go func() { built <- p.buildAvailable(bctx) }()
			select {
			case err := <-built:
				cancel()
				if err != nil {
					return nil, err
				}
			case <-bctx.Done():
				cancel()
				return nil, bctx.Err()
			}
			continue
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-changed:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
		timer.Stop()
	}
}

// giveBack makes a healthy client available to borrow again, or records it for
// background cleanup before returning. The caller must already have stopped
// using the query's response buffers.
func (p *qwpQueryPool) giveBack(q *Query, broken bool) error {
	var result error
	err := p.withLock([]*qwpQueryWorker{q.worker}, func() {
		if q.worker.generation.Load() != q.gen {
			return
		}
		q.worker.generation.Add(1)
		if p.closed || p.closing.Load() || p.failedErr != nil || broken {
			p.startTeardownLocked(q.worker, nil)
		} else {
			q.worker.idleSince = time.Now()
			p.available = append(p.available, q.worker)
		}
		p.broadcastLocked()
		result = p.failedErr
	})
	return errors.Join(result, err)
}

// markClosing signals reapIdle to bail before the housekeeper is stopped.
func (p *qwpQueryPool) markClosing() { p.closing.Store(true) }

// queryReapCloseHook, when non-nil, is invoked at the start of each reap-victim
// close goroutine. Test seam only (mirrors reapCloseHook): it lets a test hold a
// reap teardown in flight to assert close() waits for it. Nil in production.
var queryReapCloseHook atomic.Pointer[func()]

func (p *qwpQueryPool) reapIdle() {
	if p.closing.Load() {
		return
	}
	now := time.Now()
	p.withLock(nil, func() {
		if p.closed || p.closing.Load() || p.failedErr != nil {
			return
		}
		kept := p.available[:0]
		for _, w := range p.available {
			idleExpired := p.idleTimeout > 0 && now.Sub(w.idleSince) >= p.idleTimeout
			overAge := p.maxLifetime > 0 && now.Sub(w.createdAt) >= p.maxLifetime
			poisoned := w.client.terminalError() != nil
			// Keep at least minSize clients when removing old or idle ones, but
			// always remove failed clients. Starting cleanup removes each client
			// from p.all immediately, so its length already reflects that change.
			if poisoned || ((idleExpired || overAge) && len(p.all) > p.minSize) {
				p.startTeardownLocked(w, func() {
					if hook := queryReapCloseHook.Load(); hook != nil {
						(*hook)()
					}
				})
				continue
			}
			kept = append(kept, w)
		}
		p.available = kept
	})
}

// queryClientCloseHook holds off-lock client teardown in lifecycle tests.
// Invoked inside the close panic guard; nil in production.
var queryClientCloseHook atomic.Pointer[func(*QwpQueryClient)]

// closeQueryClientGuarded closes a worker's client, converting a panic into an
// error so a faulting Close cannot unwind through the pool's teardown. Mirrors
// the sender pool's closeSlotGuarded.
func closeQueryClientGuarded(ctx context.Context, client *QwpQueryClient) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: query client close panicked: %v", ErrCleanupFailed, r)
		}
	}()
	if hook := queryClientCloseHook.Load(); hook != nil {
		(*hook)(client)
	}
	return client.Close(ctx)
}

// close starts cleanup once for each client that is not borrowed. Borrowed
// clients stay in p.all until their callers return them. Even after a timeout,
// we must not close a client whose response buffers may still be in use.
func (p *qwpQueryPool) close(ctx context.Context) error {
	p.withLock(nil, func() {
		if !p.closed {
			p.closed = true
			for _, w := range p.available {
				p.startTeardownLocked(w, nil)
			}
			p.available = nil
			p.broadcastLocked()
		}
	})
	for {
		changed, result := p.closeResult()
		if !errors.Is(result, ErrCleanupPending) || errors.Is(result, ErrCleanupFailed) {
			return result
		}
		select {
		case <-changed:
		case <-ctx.Done():
			_, result = p.closeResult()
			if errors.Is(result, ErrCleanupPending) && !errors.Is(result, ErrCleanupFailed) {
				return errors.Join(result, ctx.Err())
			}
			return result
		}
	}
}

func (p *qwpQueryPool) createWorker(ctx context.Context) (w *qwpQueryWorker, err error) {
	// Panic-guard the build path so a fault converts to an error the caller can
	// clean up after, rather than unwinding through the pool (Hazard I).
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: query client build panicked: %v", ErrCleanupFailed, r)
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
		var buildErr *qwpQueryBuildError
		if errors.As(cerr, &buildErr) {
			return &qwpQueryWorker{client: buildErr.client}, cerr
		}
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
	if p.notify != nil {
		close(p.notify)
	}
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
// disconnect happens when the pool removes the client or shuts down. Do not
// use a handle concurrently, including Close while running or reading a query; borrow
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

// Close returns the borrowed query client to the pool. First stop Query/Exec,
// result iteration, and use of slices backed by result-batch memory. Another
// goroutine may request cancellation, but must not call Close concurrently.
// Calls after the client has been returned do nothing and return nil; they
// do not report errors from later cleanup. Returning a client does not mean
// its connection has closed.
//
// Close finishes reading any open query response, with a time limit set by
// query_close_timeout_ms. It then either makes a healthy client available
// for reuse or asks the pool to close it in the background. Close takes no
// context: the response wait uses that configured limit, and Close does not
// wait for background resource cleanup. It reports errors detected while
// returning the client, including failure to safely hand it back to the pool.
// Later cleanup errors are logged and included in [QuestDB.Close]'s result,
// not in repeated calls to this Close. Return all borrowed handles, then use
// QuestDB.Close to wait for all pool resources to be released.
func (q *Query) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			q.closed = true
			err = fmt.Errorf("%w: query return panicked: %v", ErrCleanupFailed, r)
			if q.pool != nil {
				q.pool.withLock([]*qwpQueryWorker{q.worker}, func() {
					q.pool.failLocked(err, []*qwpQueryWorker{q.worker})
					if q.worker != nil {
						q.worker.generation.Add(1)
					}
				})
			}
		}
	}()
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
