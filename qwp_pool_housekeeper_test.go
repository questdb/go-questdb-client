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
	"testing"
	"time"
)

// TestQwpPoolHousekeeperDefaultsAndGuard covers the negative-interval default
// and the panic-guarded reap step. End-to-end reaping via the running goroutine
// is covered by TestQuestDBHousekeeperReaps.
func TestQwpPoolHousekeeperDefaultsAndGuard(t *testing.T) {
	h := newQwpPoolHousekeeper(nil, nil, -1, 0)
	if h.interval != qwpDefaultHousekeeperInterval {
		t.Errorf("interval=%v, want default %v", h.interval, qwpDefaultHousekeeperInterval)
	}
	if want := qwpSfDefaultCloseFlushTimeout + time.Second; h.joinBudget != want {
		t.Errorf("joinBudget=%v, want default %v", h.joinBudget, want)
	}
	h.reapGuarded(func() { panic("reap boom") }) // recovered, must not crash
}

// TestQwpPoolHousekeeperDisabled pins interval 0 = disabled: start spawns no
// goroutine and stopAndJoin returns immediately (not after the join budget)
// and stays idempotent.
func TestQwpPoolHousekeeperDisabled(t *testing.T) {
	h := newQwpPoolHousekeeper(nil, nil, 0, 10*time.Second)
	h.start()
	if h.started.Load() {
		t.Fatal("disabled housekeeper must not start its goroutine")
	}
	begin := time.Now()
	h.stopAndJoin()
	h.stopAndJoin() // idempotent
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("stopAndJoin on a disabled housekeeper took %v; want immediate return", elapsed)
	}
}
