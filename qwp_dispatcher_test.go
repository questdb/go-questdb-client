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
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// panicOnHandleSlog is an slog.Handler whose Handle always panics — a stand-in
// for a buggy user-supplied logger. It proves deliver() contains a panic from
// the recovery-path logger, not only from the handler itself.
type panicOnHandleSlog struct{}

func (panicOnHandleSlog) Enabled(context.Context, slog.Level) bool  { return true }
func (panicOnHandleSlog) Handle(context.Context, slog.Record) error { panic("logger boom") }
func (h panicOnHandleSlog) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h panicOnHandleSlog) WithGroup(string) slog.Handler           { return h }

// TestQwpDispatcherUnit covers the generic dispatcher: delivery, panic-recovery,
// counters, nil-guards, and offer on a nil/closed dispatcher.
func TestQwpDispatcherUnit(t *testing.T) {
	var delivered atomic.Int64
	d := newQwpConnDispatcher(func(e SenderConnectionEvent) {
		delivered.Add(1)
		if e.Kind == SenderAuthFailed {
			panic("listener boom") // exercise deliver's panic-recovery
		}
	}, 8)
	d.offer(&SenderConnectionEvent{Kind: SenderConnected})
	d.offer(&SenderConnectionEvent{Kind: SenderAuthFailed})
	deadline := time.Now().Add(2 * time.Second)
	for d.totalDelivered() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if d.totalDelivered() < 2 {
		t.Errorf("totalDelivered=%d, want >= 2", d.totalDelivered())
	}
	d.close()
	d.close() // idempotent
	if d.offer(nil) {
		t.Error("offer(nil) should return false")
	}
	if d.offer(&SenderConnectionEvent{Kind: SenderConnected}) {
		t.Error("offer after close should return false")
	}

	var nilD *qwpDispatcher[*SenderConnectionEvent]
	if nilD.droppedNotifications() != 0 || nilD.totalDelivered() != 0 {
		t.Error("nil dispatcher accessors should return 0")
	}
	nilD.close()
	if nilD.offer(&SenderConnectionEvent{}) {
		t.Error("offer on nil dispatcher should return false")
	}
}

// TestQwpDispatcherHandlerAndLoggerPanicDoesNotCrash pins the guarantee that a
// user handler panic AND a panicking recovery-path logger together do not
// escape deliver()/loop() and crash the host.
func TestQwpDispatcherHandlerAndLoggerPanicDoesNotCrash(t *testing.T) {
	var delivered atomic.Int64
	d := newQwpConnDispatcher(func(SenderConnectionEvent) {
		delivered.Add(1)
		panic("handler boom")
	}, 4)
	d.logger = slog.New(panicOnHandleSlog{})
	for i := 0; i < 3; i++ {
		d.offer(&SenderConnectionEvent{Kind: SenderConnected})
	}
	deadline := time.Now().Add(2 * time.Second)
	for d.totalDelivered() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if d.totalDelivered() < 3 {
		t.Fatalf("dispatcher stopped after logger panic: delivered=%d", d.totalDelivered())
	}
	d.close()
}

// TestQwpDispatcherDeliversInOrder pins the single-consumer FIFO contract on the
// generic dispatcher (the sibling qwpSfErrorDispatcher has this; the generic one
// did not). AttemptNumber carries the ordering token.
func TestQwpDispatcherDeliversInOrder(t *testing.T) {
	var mu sync.Mutex
	var got []int64
	done := make(chan struct{}, 3)
	d := newQwpConnDispatcher(func(e SenderConnectionEvent) {
		mu.Lock()
		got = append(got, e.AttemptNumber)
		mu.Unlock()
		done <- struct{}{}
	}, 8)
	defer d.close()

	for i := int64(1); i <= 3; i++ {
		if !d.offer(&SenderConnectionEvent{Kind: SenderReconnected, AttemptNumber: i}) {
			t.Fatalf("offer %d dropped on a non-full inbox", i)
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handler not invoked in time")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("delivered %d events, want 3", len(got))
	}
	for i, v := range got {
		if v != int64(i+1) {
			t.Errorf("delivery order got[%d]=%d, want %d", i, v, i+1)
		}
	}
	if d.droppedNotifications() != 0 {
		t.Errorf("dropped = %d, want 0", d.droppedNotifications())
	}
}

// TestQwpDispatcherCloseDrainsLeftover asserts close() synchronously delivers an
// item queued before the loop goroutine ever started — the not-started branch of
// close() must drain rather than discard.
func TestQwpDispatcherCloseDrainsLeftover(t *testing.T) {
	var mu sync.Mutex
	var got int64 = -1
	d := newQwpConnDispatcher(func(e SenderConnectionEvent) {
		mu.Lock()
		got = e.AttemptNumber
		mu.Unlock()
	}, 4)

	d.inbox <- &SenderConnectionEvent{Kind: SenderConnected, AttemptNumber: 42}
	if d.started.Load() {
		t.Fatal("test setup: dispatcher unexpectedly started")
	}

	d.close()

	mu.Lock()
	defer mu.Unlock()
	if got != 42 {
		t.Fatalf("got = %d, want 42 — close did not synchronously drain the leftover", got)
	}
	if d.totalDelivered() != 1 {
		t.Errorf("delivered = %d, want 1", d.totalDelivered())
	}
}

// TestQwpDispatcherOfferCloseRaceNoLoss stresses the offer/close serialization
// (mu-guarded closed-check + send): every offer that returns true must be
// delivered, even when close races concurrent offers from many goroutines. The
// inbox is sized above the offerer count so nothing is drop-oldest displaced, so
// accepted == delivered exactly. Run under -race.
func TestQwpDispatcherOfferCloseRaceNoLoss(t *testing.T) {
	const iterations = 200
	const offerers = 16
	for iter := 0; iter < iterations; iter++ {
		var delivered atomic.Int64
		d := newQwpConnDispatcher(func(SenderConnectionEvent) {
			delivered.Add(1)
		}, offerers*2)

		var accepted atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for k := 0; k < offerers; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if d.offer(&SenderConnectionEvent{Kind: SenderConnected}) {
					accepted.Add(1)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d.close()
		}()
		close(start)
		wg.Wait()

		if got, want := delivered.Load(), accepted.Load(); got != want {
			t.Fatalf("iter %d: delivered=%d, accepted=%d (lost %d)", iter, got, want, want-got)
		}
	}
}

func TestQwpDispatcherDefaultCapacity(t *testing.T) {
	d := newQwpConnDispatcher(func(SenderConnectionEvent) {}, 0) // capacity < 1 → default
	defer d.close()
	if cap(d.inbox) != qwpSfDefaultErrorInboxCapacity {
		t.Errorf("inbox cap=%d, want default %d", cap(d.inbox), qwpSfDefaultErrorInboxCapacity)
	}
}

// TestQwpDispatcherCloseAbandonsWedgedHandler exercises the bounded-close
// contract: when the handler is still running past the join timeout, close()
// gives up joining, sets abandon, returns within the budget, and counts every
// queued-but-undelivered item as dropped — it must not block forever on the
// wedged goroutine. (TestConnectionListenerDispatcherDrops unblocks the handler
// before close, so it never reaches the abandon path.)
func TestQwpDispatcherCloseAbandonsWedgedHandler(t *testing.T) {
	block := make(chan struct{})
	var delivered atomic.Int64
	d := newQwpConnDispatcher(func(SenderConnectionEvent) {
		delivered.Add(1)
		<-block // wedge the loop goroutine inside the handler
	}, 2)

	// Wedge the handler on the first event, then pile on more so the inbox holds
	// undelivered items when close runs.
	d.offer(&SenderConnectionEvent{Kind: SenderConnected})
	wedge := time.Now().Add(2 * time.Second)
	for delivered.Load() == 0 && time.Now().Before(wedge) {
		time.Sleep(time.Millisecond)
	}
	if delivered.Load() == 0 {
		close(block)
		t.Fatal("handler never started; cannot reach the abandon path")
	}
	for i := 0; i < 10; i++ {
		d.offer(&SenderConnectionEvent{Kind: SenderConnected})
	}

	start := time.Now()
	d.close() // join times out → abandon → drains queued as dropped → returns
	elapsed := time.Since(start)
	if elapsed < qwpSfDispatcherCloseJoinTimeout {
		t.Errorf("close returned in %v, want >= join timeout %v (did it really wait then abandon?)",
			elapsed, qwpSfDispatcherCloseJoinTimeout)
	}
	if elapsed > 3*time.Second {
		t.Errorf("close took %v; the bounded-close budget did not fire", elapsed)
	}
	if d.droppedNotifications() == 0 {
		t.Error("queued-but-undelivered items should be counted as dropped on abandon")
	}

	// Releasing the wedged handler after abandon must not resurrect deliveries:
	// only the first (in-flight) event was ever delivered.
	close(block)
	time.Sleep(50 * time.Millisecond)
	if got := d.totalDelivered(); got != 1 {
		t.Errorf("totalDelivered=%d after abandon, want 1 (no post-abandon delivery)", got)
	}
}

// TestQwpDispatcherReentrantCloseCountsAbandonedAsDropped pins the accounting
// on the re-entrant close path: close() called by the handler returns before
// close()'s own leftovers sweep (the handler owns the loop goroutine), so when
// loop() unwinds into drain() and the drain deadline fires, whatever is still
// queued must be counted as dropped rather than stranded uncounted. The hard
// invariant is delivered + dropped == offered.
func TestQwpDispatcherReentrantCloseCountsAbandonedAsDropped(t *testing.T) {
	const extra = 12
	var d *qwpDispatcher[*SenderConnectionEvent]
	queued := make(chan struct{})
	var once sync.Once
	d = newQwpConnDispatcher(func(SenderConnectionEvent) {
		once.Do(func() {
			<-queued
			d.close() // re-entrant: returns without the close-side sweep
		})
		// Slow enough that drain()'s deadline fires with items still queued.
		time.Sleep(qwpSfDispatcherDrainTimeout + 20*time.Millisecond)
	}, extra+4)

	if !d.offer(&SenderConnectionEvent{Kind: SenderConnected}) {
		t.Fatal("first offer rejected")
	}
	for i := 0; i < extra; i++ {
		if !d.offer(&SenderConnectionEvent{Kind: SenderConnected}) {
			t.Fatalf("offer %d rejected before close", i)
		}
	}
	close(queued)

	joined := make(chan struct{})
	go func() { d.wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(10 * time.Second):
		t.Fatal("dispatcher loop never exited after re-entrant close")
	}

	delivered, dropped := d.totalDelivered(), d.droppedNotifications()
	if got, want := delivered+dropped, int64(extra+1); got != want {
		t.Fatalf("delivered(%d) + dropped(%d) = %d, want %d — items abandoned on the "+
			"re-entrant close path went uncounted", delivered, dropped, got, want)
	}
}

// TestQwpDispatcherReentrantClose exercises the loopGoid guard: a handler that
// calls close() on the loop goroutine must return immediately (recognising it is
// the loop goroutine) instead of self-joining via wg.Wait — which would stall
// until the join timeout, or deadlock if the budget were unbounded.
func TestQwpDispatcherReentrantClose(t *testing.T) {
	var d *qwpDispatcher[*SenderConnectionEvent]
	var innerClose atomic.Int64 // nanoseconds the re-entrant close() took
	reentered := make(chan struct{})
	d = newQwpConnDispatcher(func(SenderConnectionEvent) {
		start := time.Now()
		d.close() // re-entrant close from the loop goroutine
		innerClose.Store(int64(time.Since(start)))
		close(reentered)
	}, 4)

	d.offer(&SenderConnectionEvent{Kind: SenderConnected})
	select {
	case <-reentered:
	case <-time.After(3 * time.Second):
		t.Fatal("re-entrant close deadlocked the loop goroutine")
	}
	// The guard short-circuits before the join budget, so the inner close is near
	// instant — well under the join timeout it would otherwise have waited out.
	if got := time.Duration(innerClose.Load()); got >= qwpSfDispatcherCloseJoinTimeout {
		t.Errorf("re-entrant close took %v, want << join timeout %v (loopGoid guard not taken)",
			got, qwpSfDispatcherCloseJoinTimeout)
	}
	// The dispatcher is closed; a subsequent offer/close stays safe and idempotent.
	if d.offer(&SenderConnectionEvent{Kind: SenderConnected}) {
		t.Error("offer after re-entrant close should return false")
	}
	d.close()
}
