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

// TestQwpSfDispatcherSlowHandlerDrops asserts that a slow handler
// causes inbox-overflow drops instead of stalling the producer side.
func TestQwpSfDispatcherSlowHandlerDrops(t *testing.T) {
	release := make(chan struct{})
	d := newQwpSfErrorDispatcher(func(e *SenderError) {
		<-release
	}, 4)
	defer func() {
		close(release)
		d.close()
	}()

	const offers = 64
	accepted := 0
	for i := 0; i < offers; i++ {
		if d.offer(&SenderError{Category: CategoryParseError}) {
			accepted++
		}
	}
	dropped := d.droppedNotifications()
	if dropped == 0 {
		t.Fatalf("expected drops, got 0 (accepted=%d)", accepted)
	}
	// The first one might've fired the goroutine and the inbox cap
	// is 4, so accepted should be at most cap+1 (one in flight).
	if accepted > 5 {
		t.Errorf("accepted = %d, want ≤ 5 (inbox cap 4 + 1 in flight)", accepted)
	}
	if int64(accepted)+dropped != int64(offers) {
		t.Errorf("accepted (%d) + dropped (%d) = %d, want %d",
			accepted, dropped, int64(accepted)+dropped, offers)
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
