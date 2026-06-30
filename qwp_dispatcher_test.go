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
	"sync/atomic"
	"testing"
	"time"
)

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
