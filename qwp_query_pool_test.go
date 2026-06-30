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
	"strings"
	"testing"
	"time"
)

func newQwpQueryPoolForTest(t *testing.T, min, max int) *qwpQueryPool {
	t.Helper()
	// Keep each upgraded connection open until the client closes it, so
	// borrow/return mechanics aren't disturbed by an early server close.
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		for {
			if _, _, err := m.conn.Read(context.Background()); err != nil {
				return
			}
		}
	})
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpQueryPool(context.Background(), conf, min, max,
		500*time.Millisecond, 0, 0)
	if err != nil {
		t.Fatalf("newQwpQueryPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })
	return p
}

func TestQwpQueryPoolBorrowReuse(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 1, 4)
	if total, _ := p.poolSnapshot(); total != 1 {
		t.Fatalf("prewarm: total=%d, want 1", total)
	}
	q, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	q2, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("re-borrow: %v", err)
	}
	defer q2.Close()
	if total, _ := p.poolSnapshot(); total != 1 {
		t.Errorf("after reuse: total=%d, want 1 (no growth)", total)
	}
}

func TestQwpQueryPoolLazyMinZero(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 0, 2)
	// min=0 (the lazy read pool): build prewarms nothing.
	if total, _ := p.poolSnapshot(); total != 0 {
		t.Fatalf("lazy pool prewarmed total=%d, want 0", total)
	}
	q, err := p.borrow(context.Background()) // connects on first borrow
	if err != nil {
		t.Fatalf("first borrow: %v", err)
	}
	defer q.Close()
	if total, _ := p.poolSnapshot(); total != 1 {
		t.Errorf("after first borrow: total=%d, want 1", total)
	}
}

func TestQwpQueryPoolStaleLeaseExec(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 1, 2)
	q, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	q.Close()
	if _, err := q.Exec(context.Background(), "select 1"); !errors.Is(err, errStaleLease) {
		t.Fatalf("stale Exec err=%v, want errStaleLease", err)
	}
}

func TestQwpQueryPoolExhausted(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 0, 1)
	q, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer q.Close()
	if _, err := p.borrow(context.Background()); !errors.Is(err, errPoolExhausted) {
		t.Fatalf("second borrow err=%v, want errPoolExhausted", err)
	}
}

// TestQwpQueryPoolGiveBackBroken covers the broken-worker discard branch.
func TestQwpQueryPoolGiveBackBroken(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	p.giveBack(ctx, q, true) // broken → worker evicted, not recycled
	if total, avail := p.poolSnapshot(); avail != 0 || total != 0 {
		t.Errorf("broken worker recycled: total=%d avail=%d, want 0/0", total, avail)
	}
}

// TestQwpQueryLeaseStaleQuery covers the stale-lease cursor (Query on a closed
// handle yields errStaleLease from Batches rather than a nil panic).
func TestQwpQueryLeaseStaleQuery(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	_ = q.Close()
	cur := q.Query(ctx, "select 1")
	sawStale := false
	for _, err := range cur.Batches() {
		if errors.Is(err, errStaleLease) {
			sawStale = true
		}
	}
	if !sawStale {
		t.Error("stale Query cursor did not yield errStaleLease")
	}
}

func TestQwpQueryPoolPrewarmFailure(t *testing.T) {
	_, err := newQwpQueryPool(context.Background(), "ws::addr=127.0.0.1:1;", 1, 2, 200*time.Millisecond, 0, 0)
	if err == nil {
		t.Error("query prewarm against a down server should fail the build")
	}
}

func TestQwpQueryPoolBorrowCreateError(t *testing.T) {
	ctx := context.Background()
	p, err := newQwpQueryPool(ctx, "ws::addr=127.0.0.1:1;", 0, 1, 200*time.Millisecond, 0, 0)
	if err != nil {
		t.Fatalf("build (min=0 must not connect): %v", err)
	}
	defer p.close(ctx)
	if _, err := p.borrow(ctx); err == nil {
		t.Error("query borrow against a down server should fail")
	}
}

func TestQwpQueryPoolConstructorErrors(t *testing.T) {
	if _, err := newQwpQueryPool(context.Background(), "ws::addr=a:9000;", 3, 1, time.Second, 0, 0); err == nil {
		t.Error("query pool min>max should error")
	}
}

func TestQwpQueryPoolClosedOps(t *testing.T) {
	ctx := context.Background()
	p := queryPoolWithIdle(t, 1, 2, time.Second)
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	_ = p.close(ctx)
	if _, err := p.borrow(ctx); !errors.Is(err, errPoolClosed) {
		t.Errorf("borrow after close=%v, want errPoolClosed", err)
	}
	p.reapIdle()
	// On loan at close, so close() left it open; returning it self-closes (M1).
	_ = q.Close()
}

// TestQwpQueryPoolCloseLeavesOnLoanWorker (M1): close() must not close a worker
// still on loan (its client may have a live Batches() read); the lease self-
// closes on return instead.
func TestQwpQueryPoolCloseLeavesOnLoanWorker(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 1, 2)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	w := q.worker
	if err := p.close(ctx); err != nil {
		t.Fatalf("pool close: %v", err)
	}
	if w.client.closed.Load() {
		t.Fatal("close() force-closed an on-loan worker (would race an in-flight Batches() read)")
	}
	if err := q.Close(); err != nil {
		t.Fatalf("lease close after pool close: %v", err)
	}
	if !w.client.closed.Load() {
		t.Error("returning the on-loan lease after pool close did not self-close its client")
	}
}

func TestQwpQueryPoolDoubleClose(t *testing.T) {
	ctx := context.Background()
	p := queryPoolWithIdle(t, 1, 2, 0)
	if err := p.close(ctx); err != nil {
		t.Errorf("query close: %v", err)
	}
	if err := p.close(ctx); err != nil {
		t.Errorf("query double close: %v", err)
	}
}

func TestQwpQueryLeaseCloseIdempotent(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Errorf("second (stale) close: %v", err)
	}
}

func TestQwpQueryLeaseReopenClosesPrevious(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer q.Close()
	_ = q.Query(ctx, "select 1")
	_ = q.Query(ctx, "select 2") // active != nil → previous cursor closed
}

// TestQwpQueryLeaseReopenOverAbandonedDrain covers the desync hazard: reopening
// a lease whose previous cursor's cleanup drain abandoned before its terminal
// frame must mark the worker broken (so Close evicts it instead of recycling a
// wire-desynced worker) and refuse to submit the new query on the desynced wire
// (so the caller gets errStaleLease, not silently the prior cursor's leftover
// frames).
func TestQwpQueryLeaseReopenOverAbandonedDrain(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer q.Close()

	c1 := q.Query(ctx, "select 1")
	// Simulate a caller that broke out of Batches() early and whose cursor
	// cleanup drain abandoned before the terminal frame: the cursor is Done
	// with drainFailed latched, leaving leftover events on the single-stream
	// wire.
	c1.state.Store(qwpQueryStateDone)
	c1.drainFailed.Store(true)

	c2 := q.Query(ctx, "select 2")
	if !q.broken {
		t.Error("reopening over an abandoned drain must mark the worker broken")
	}
	if q.active != nil {
		t.Error("Query must refuse to submit on the desynced wire (active stays nil)")
	}
	sawStale := false
	for _, err := range c2.Batches() {
		if errors.Is(err, errStaleLease) {
			sawStale = true
		}
	}
	if !sawStale {
		t.Error("desynced reopen cursor did not yield errStaleLease")
	}
}

// TestQwpQueryLeaseExecOverAbandonedDrain is the Exec sibling of
// TestQwpQueryLeaseReopenOverAbandonedDrain (item C1): Exec must NOT submit on a
// wire it has just flagged desynced. A prior Query cursor whose cleanup drain
// abandoned before its terminal frame leaves leftover events on the
// single-stream wire; Exec drains it, detects drainFailed, marks the worker
// broken, and must refuse to submit (errStaleLease) rather than misread the
// leftover frames as its own result while the statement executes server-side
// unobserved.
func TestQwpQueryLeaseExecOverAbandonedDrain(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer q.Close()

	c1 := q.Query(ctx, "select 1")
	// Simulate a caller that broke out of Batches() early and whose cursor
	// cleanup drain abandoned before the terminal frame.
	c1.state.Store(qwpQueryStateDone)
	c1.drainFailed.Store(true)

	_, err = q.Exec(ctx, "insert into t values (1)")
	if !errors.Is(err, errStaleLease) {
		t.Errorf("Exec over an abandoned drain must return errStaleLease, got %v", err)
	}
	if !q.broken {
		t.Error("Exec over an abandoned drain must mark the worker broken")
	}
	if q.active != nil {
		t.Error("Exec must not leave a cursor active after refusing to submit")
	}

	// A second Query/Exec on the now-broken lease must also refuse via the
	// top-level broken gate, not submit on the still-desynced wire.
	c2 := q.Query(ctx, "select 2")
	sawStale := false
	for _, e := range c2.Batches() {
		if errors.Is(e, errStaleLease) {
			sawStale = true
		}
	}
	if !sawStale {
		t.Error("Query on a broken lease did not yield errStaleLease")
	}
	if _, e := q.Exec(ctx, "insert into t values (2)"); !errors.Is(e, errStaleLease) {
		t.Errorf("Exec on a broken lease must return errStaleLease, got %v", e)
	}
}

// TestQwpQueryLeaseEvictsOnExecInternalDrainAbandoned (item C1, Exec-path
// analogue) covers the gap where Exec's OWN internal cleanup drain (its
// ctx-error path or SELECT-via-Exec path) abandons before a terminal frame on
// an otherwise-healthy transport. That latches QwpQueryClient.execDrainAbandoned
// but surfaces neither a *QwpFailoverExhaustedError nor a terminalError() (the
// transport never faulted), so before this fix the desynced worker was recycled
// and the next borrower misread the leftover frames as its own result. The lease
// must instead evict the worker on return. The abandoned drain is simulated by
// latching the client flag directly (as the sibling tests simulate drainFailed),
// since reproducing the real 5s qwpQueryCleanupDrainTimeout would be slow/flaky.
func TestQwpQueryLeaseEvictsOnExecInternalDrainAbandoned(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}

	// Simulate an Exec whose internal cleanup drain abandoned: the wire is
	// desynced but the transport stayed healthy (terminalError() == nil).
	q.worker.client.execDrainAbandoned.Store(true)
	if q.worker.client.terminalError() != nil {
		t.Fatal("precondition: transport must look healthy (terminalError nil)")
	}
	if !q.worker.client.execDesynced() {
		t.Fatal("execDesynced must report the latched abandoned drain")
	}

	// Returning the lease must evict the desynced worker, not recycle it.
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !q.broken {
		t.Error("Close over an abandoned Exec drain must mark the worker broken")
	}
	if total, avail := p.poolSnapshot(); total != 0 || avail != 0 {
		t.Errorf("desynced worker recycled: total=%d avail=%d, want 0/0", total, avail)
	}
}

func TestQwpQueryLeaseExecError(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 2, 0)
	q, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer q.Close()
	ectx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := q.Exec(ectx, "drop table nope"); err == nil {
		t.Error("Exec against a non-responding server should error")
	}
}

// poisonQueryWorker latches a terminal transport error on w's bound I/O, the
// same state a background failover-exhaustion would leave behind.
func poisonQueryWorker(t *testing.T, w *qwpQueryWorker) {
	t.Helper()
	io := w.client.io()
	if io == nil {
		t.Fatal("worker client has no bound io to poison")
	}
	io.setIoErr(errors.New("test: terminal transport failure"))
	if w.client.terminalError() == nil {
		t.Fatal("worker not poisoned after setIoErr")
	}
}

// TestQwpQueryPoolBorrowDiscardsTerminalWorker (item #5): a worker that latched
// a terminal transport error while idle is discarded on borrow and replaced,
// never handed to the borrower (mirrors the sender pool's M1).
func TestQwpQueryPoolBorrowDiscardsTerminalWorker(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 1, 2)
	ctx := context.Background()
	p.mu.Lock()
	poisoned := p.all[0]
	p.mu.Unlock()
	poisonQueryWorker(t, poisoned)

	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer q.Close()
	if q.worker == poisoned {
		t.Fatal("borrow handed out the poisoned worker")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.all {
		if w == poisoned {
			t.Fatal("poisoned worker still in pool after borrow")
		}
	}
}

// TestQwpQueryPoolReapsTerminalWorker (item #5): reapIdle evicts a
// terminally-failed worker even at minSize (idle/maxLifetime are off here, so
// only the poisoned branch can reap).
func TestQwpQueryPoolReapsTerminalWorker(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 1, 2)
	p.mu.Lock()
	poisoned := p.all[0]
	p.mu.Unlock()
	poisonQueryWorker(t, poisoned)

	p.reapIdle()

	if total, avail := p.poolSnapshot(); total != 0 || avail != 0 {
		t.Errorf("poisoned worker not reaped: total=%d avail=%d, want 0/0", total, avail)
	}
}
