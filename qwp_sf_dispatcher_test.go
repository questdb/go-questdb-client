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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQwpSfDispatcherDeliversInOrder asserts the dispatcher delivers
// queued errors to the handler FIFO and counts each delivery.
func TestQwpSfDispatcherDeliversInOrder(t *testing.T) {
	var got []*SenderError
	var mu sync.Mutex
	done := make(chan struct{}, 3)
	d := newQwpSfErrorDispatcher(func(e *SenderError) {
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
		done <- struct{}{}
	}, 8)
	defer d.close()

	es := []*SenderError{
		{Category: CategoryParseError},
		{Category: CategoryWriteError},
		{Category: CategorySchemaMismatch},
	}
	for _, e := range es {
		if !d.offer(e) {
			t.Fatalf("offer dropped a non-full inbox")
		}
	}
	for range es {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handler not invoked in time")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(es) {
		t.Fatalf("got %d, want %d", len(got), len(es))
	}
	for i := range es {
		if got[i] != es[i] {
			t.Errorf("got[%d]=%v, want %v", i, got[i], es[i])
		}
	}
	if d.totalDelivered() != int64(len(es)) {
		t.Errorf("delivered = %d, want %d", d.totalDelivered(), len(es))
	}
	if d.droppedNotifications() != 0 {
		t.Errorf("dropped = %d, want 0", d.droppedNotifications())
	}
}

// TestQwpSfDispatcherSlowHandlerDropsOldest asserts that when a slow
// handler causes the inbox to fill, the OLDEST queued entry is
// displaced to admit the new one (sf-client.md §14.6). Every offer
// must be admitted; only previously queued entries are displaced;
// the inbox at end-of-flood must contain the most recent items.
func TestQwpSfDispatcherSlowHandlerDropsOldest(t *testing.T) {
	release := make(chan struct{})
	handlerStarted := make(chan struct{})
	var mu sync.Mutex
	var delivered []*SenderError
	var firstOnce sync.Once
	d := newQwpSfErrorDispatcher(func(e *SenderError) {
		firstOnce.Do(func() { close(handlerStarted) })
		mu.Lock()
		delivered = append(delivered, e)
		mu.Unlock()
		<-release
	}, 4)

	items := make([]*SenderError, 9)
	for i := range items {
		items[i] = &SenderError{Category: CategoryParseError, ToFsn: int64(i)}
	}

	// First offer lazy-starts the dispatcher. Wait until the handler
	// has actually pulled item 0 so the inbox is verifiably empty
	// before we fill it.
	if !d.offer(items[0]) {
		t.Fatal("first offer rejected on empty inbox")
	}
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start within timeout")
	}

	// Fill the inbox to capacity (4) without overflowing.
	for i := 1; i <= 4; i++ {
		if !d.offer(items[i]) {
			t.Fatalf("offer %d rejected on non-full inbox", i)
		}
	}
	if got := d.droppedNotifications(); got != 0 {
		t.Fatalf("dropped = %d before overflow, want 0", got)
	}

	// Offer 4 more. Drop-oldest must admit each one and displace the
	// oldest entry that was queued.
	for i := 5; i <= 8; i++ {
		if !d.offer(items[i]) {
			t.Fatalf("offer %d rejected (drop-oldest must admit every offer)", i)
		}
	}
	if got, want := d.droppedNotifications(), int64(4); got != want {
		t.Errorf("dropped = %d, want %d (one per overflow offer)", got, want)
	}

	// Release the handler and drain. Item 0 was already in the handler
	// when the flood started; items 1-4 should have been displaced;
	// items 5-8 should still be queued. Total delivered: 5.
	close(release)
	d.close()

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 5 {
		t.Fatalf("delivered = %d, want 5 (item 0 + 4 newest)", len(delivered))
	}
	wantFsns := []int64{0, 5, 6, 7, 8}
	for i, want := range wantFsns {
		if delivered[i].ToFsn != want {
			t.Errorf("delivered[%d] ToFsn = %d, want %d (drop-oldest must preserve newest)",
				i, delivered[i].ToFsn, want)
		}
	}
}

// TestQwpSfDispatcherCloseIsIdempotent asserts close() can be called
// multiple times without panicking or leaking goroutines.
func TestQwpSfDispatcherCloseIsIdempotent(t *testing.T) {
	d := newQwpSfErrorDispatcher(func(e *SenderError) {}, 4)
	d.close()
	d.close() // must not panic
	if d.offer(&SenderError{}) {
		t.Fatal("offer succeeded on closed dispatcher")
	}
}

// TestQwpSfDispatcherCloseDrainsLeftover asserts that an item in the
// inbox at close time is delivered even when the dispatcher goroutine
// never started. Reproduces the never-started race: in production
// offer's send-to-inbox can complete before its startIfNeeded call,
// and a close() that wins the closed flag between those two steps
// would otherwise strand the queued payload.
func TestQwpSfDispatcherCloseDrainsLeftover(t *testing.T) {
	var got *SenderError
	var mu sync.Mutex
	d := newQwpSfErrorDispatcher(func(e *SenderError) {
		mu.Lock()
		got = e
		mu.Unlock()
	}, 4)

	want := &SenderError{Category: CategoryParseError, AppliedPolicy: PolicyHalt}
	d.inbox <- want
	if d.started.Load() {
		t.Fatal("test setup: dispatcher unexpectedly started")
	}

	d.close()

	mu.Lock()
	defer mu.Unlock()
	if got != want {
		t.Fatalf("got = %v, want %v — close did not synchronously drain", got, want)
	}
	if d.totalDelivered() != 1 {
		t.Errorf("delivered = %d, want 1", d.totalDelivered())
	}
}

// TestQwpSfDispatcherOfferCloseRaceNoLoss stresses the offer/close
// serialization: every offer that returns true must result in a
// delivered handler invocation, even when close races with offers
// from many goroutines. Verifies mu prevents a producer's send from
// landing in an abandoned inbox after close has drained.
func TestQwpSfDispatcherOfferCloseRaceNoLoss(t *testing.T) {
	const iterations = 200
	const offerers = 16
	for iter := 0; iter < iterations; iter++ {
		var delivered atomic.Int64
		d := newQwpSfErrorDispatcher(func(e *SenderError) {
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
				if d.offer(&SenderError{Category: CategoryParseError}) {
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
			t.Fatalf("iter %d: delivered=%d, accepted=%d (lost %d)",
				iter, got, want, want-got)
		}
	}
}

// TestQwpSfDispatcherPanicCaught asserts a panicking handler is
// recovered and does not stop the dispatcher.
func TestQwpSfDispatcherPanicCaught(t *testing.T) {
	var calls atomic.Int64
	d := newQwpSfErrorDispatcher(func(e *SenderError) {
		calls.Add(1)
		if calls.Load() == 1 {
			panic("boom")
		}
	}, 4)
	defer d.close()

	d.offer(&SenderError{Category: CategoryParseError})
	d.offer(&SenderError{Category: CategoryWriteError})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatalf("dispatcher stopped after panic: calls=%d", calls.Load())
	}
	if d.totalDelivered() < 2 {
		t.Errorf("delivered = %d, want ≥ 2 (panic counts as delivery)",
			d.totalDelivered())
	}
}

// TestQwpSfDispatcherLazyStart asserts no goroutine is spawned until
// the first successful offer.
func TestQwpSfDispatcherLazyStart(t *testing.T) {
	d := newQwpSfErrorDispatcher(func(e *SenderError) {}, 4)
	if d.started.Load() {
		t.Fatal("dispatcher started before any offer")
	}
	d.offer(&SenderError{Category: CategoryParseError})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if d.started.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !d.started.Load() {
		t.Fatal("dispatcher did not start after offer")
	}
	d.close()
}

// TestQwpSfDispatcherNilHandlerUsesDefault asserts a nil handler
// falls through to the loud-not-silent default rather than panicking.
func TestQwpSfDispatcherNilHandlerUsesDefault(t *testing.T) {
	d := newQwpSfErrorDispatcher(nil, 4)
	defer d.close()
	d.offer(&SenderError{
		Category:      CategoryParseError,
		AppliedPolicy: PolicyHalt,
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if d.totalDelivered() >= 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("default handler not invoked: delivered=%d", d.totalDelivered())
}

// TestQwpSfDispatcherNilOfferIsNoop asserts that offer(nil) returns
// false without affecting counters.
func TestQwpSfDispatcherNilOfferIsNoop(t *testing.T) {
	d := newQwpSfErrorDispatcher(func(e *SenderError) {}, 4)
	defer d.close()
	if d.offer(nil) {
		t.Fatal("offer(nil) returned true")
	}
	if d.droppedNotifications() != 0 {
		t.Errorf("nil offer should not bump dropped: %d", d.droppedNotifications())
	}
}
