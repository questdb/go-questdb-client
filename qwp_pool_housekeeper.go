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
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// qwpPoolHousekeeper periodically reaps idle / over-age slots from both pools.
// One per QuestDB handle. It drives no SF recovery (the Go sender
// self-recovers; the pool binds recovery senders at construction, §4.4) — it
// only reaps. interval 0 disables it: start() spawns nothing and stopAndJoin
// returns immediately.
type qwpPoolHousekeeper struct {
	interval   time.Duration
	joinBudget time.Duration
	senderPool *qwpSenderPool
	queryPool  *qwpQueryPool
	stop       chan struct{}
	done       chan struct{}
	started    atomic.Bool
	stopOnce   sync.Once
}

// newQwpPoolHousekeeper builds the reaper. interval 0 means disabled; a
// negative interval falls back to the default. joinBudget bounds stopAndJoin
// and must cover a reaped slot's worst-case Close (the close-flush drain), so
// a reap in flight can never outlive QuestDB.Close.
func newQwpPoolHousekeeper(sp *qwpSenderPool, qp *qwpQueryPool, interval, joinBudget time.Duration) *qwpPoolHousekeeper {
	if interval < 0 {
		interval = qwpDefaultHousekeeperInterval
	}
	if joinBudget <= 0 {
		joinBudget = qwpSfDefaultCloseFlushTimeout + time.Second
	}
	return &qwpPoolHousekeeper{
		interval:   interval,
		joinBudget: joinBudget,
		senderPool: sp,
		queryPool:  qp,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (h *qwpPoolHousekeeper) start() {
	if h.interval == 0 {
		return
	}
	if h.started.CompareAndSwap(false, true) {
		go h.run()
	}
}

func (h *qwpPoolHousekeeper) run() {
	defer close(h.done)
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			h.reapGuarded(func() { h.senderPool.reapIdle() })
			h.reapGuarded(func() { h.queryPool.reapIdle() })
		}
	}
}

// reapGuarded runs a reap step under a panic guard so a fault in one pool's
// teardown can never kill the daemon and stop all future reaping.
func (h *qwpPoolHousekeeper) reapGuarded(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] qwp pool housekeeper: reap step panicked: %v", r)
		}
	}()
	fn()
}

// stopAndJoin signals the daemon and waits for it to exit, bounded so a stuck
// reap can't block Close forever. Idempotent, and an immediate no-op when the
// housekeeper is disabled (never started) — waiting on done there would sleep
// out the whole join budget for a goroutine that does not exist.
func (h *qwpPoolHousekeeper) stopAndJoin() {
	h.stopOnce.Do(func() { close(h.stop) })
	if !h.started.Load() {
		return
	}
	t := time.NewTimer(h.joinBudget)
	defer t.Stop()
	select {
	case <-h.done:
	case <-t.C:
	}
}
