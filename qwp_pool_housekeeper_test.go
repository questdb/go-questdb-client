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
	"log/slog"
	"testing"
	"time"
)

// Check the default interval and failure reporting if pool cleanup panics.
// TestQuestDBHousekeeperReaps covers the running background worker.
func TestQwpPoolHousekeeperDefaultsAndGuard(t *testing.T) {
	h := newQwpPoolHousekeeper(nil, nil, -1)
	if h.interval != qwpDefaultHousekeeperInterval {
		t.Errorf("interval=%v, want default %v", h.interval, qwpDefaultHousekeeperInterval)
	}
	if h.reapGuarded(func() { panic("reap boom") }) {
		t.Fatal("reap panic must stop the worker")
	}
	if err := h.close(context.Background()); !errors.Is(err, ErrCleanupFailed) {
		t.Fatalf("lost cleanup failure: %v", err)
	}
}

// With interval 0, no background worker starts. Closing the housekeeper returns
// immediately, including on later calls.
func TestQwpPoolHousekeeperDisabled(t *testing.T) {
	h := newQwpPoolHousekeeper(nil, nil, 0)
	h.start()
	if h.started.Load() {
		t.Fatal("disabled housekeeper must not start its goroutine")
	}
	begin := time.Now()
	_ = h.close(context.Background())
	_ = h.close(context.Background()) // closing again must also return immediately
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("close on a disabled housekeeper took %v; want immediate return", elapsed)
	}
}

// TestQwpPoolHousekeeperReapPanicSurvivesPanickingLogger pins that reporting a
// reap panic cannot itself kill the process. The daemon loop has no recover of
// its own, and a recover() cannot catch a second panic raised while the first
// one is still unwinding — so the log call inside the recover has to be
// guarded, and a user-supplied slog handler is exactly what can panic there.
func TestQwpPoolHousekeeperReapPanicSurvivesPanickingLogger(t *testing.T) {
	h := &qwpPoolHousekeeper{logger: slog.New(panicOnHandleSlog{})}
	h.reapGuarded(func() { panic("reap boom") })
}
