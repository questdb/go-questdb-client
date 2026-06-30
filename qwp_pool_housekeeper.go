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
	"time"
)

// qwpPoolHousekeeper periodically reaps idle / over-age slots from both pools.
// One per QuestDB handle. Unlike Java's it drives no SF recovery (the Go sender
// self-recovers; the pool binds recovery senders at construction, §4.4) — it
// only reaps.
type qwpPoolHousekeeper struct {
	interval   time.Duration
	senderPool *qwpSenderPool
	queryPool  *qwpQueryPool
	stop       chan struct{}
	done       chan struct{}
}

func newQwpPoolHousekeeper(sp *qwpSenderPool, qp *qwpQueryPool, interval time.Duration) *qwpPoolHousekeeper {
	if interval <= 0 {
		interval = qwpDefaultHousekeeperInterval
	}
	return &qwpPoolHousekeeper{
		interval:   interval,
		senderPool: sp,
		queryPool:  qp,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (h *qwpPoolHousekeeper) start() {
	go h.run()
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
// reap can't block Close forever.
func (h *qwpPoolHousekeeper) stopAndJoin() {
	close(h.stop)
	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
	}
}
