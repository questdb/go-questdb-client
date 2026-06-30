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
