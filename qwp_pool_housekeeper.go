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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// qwpPoolHousekeeper periodically removes unused or old clients from both pools.
// Each QuestDB handle has one. It does not recover stored data; senders handle
// that themselves. With interval 0, no worker starts and close returns immediately.
type qwpPoolHousekeeper struct {
	interval   time.Duration
	senderPool *qwpSenderPool
	queryPool  *qwpQueryPool
	logger     *slog.Logger // nil -> slog.Default() via qwpEffectiveLogger
	stop       chan struct{}
	done       chan struct{}
	started    atomic.Bool
	stopOnce   sync.Once
	mu         sync.Mutex
	err        error
}

// newQwpPoolHousekeeper sets up periodic pool cleanup. An interval of 0 disables
// it; a negative interval uses the default.
func newQwpPoolHousekeeper(sp *qwpSenderPool, qp *qwpQueryPool, interval time.Duration) *qwpPoolHousekeeper {
	if interval < 0 {
		interval = qwpDefaultHousekeeperInterval
	}
	var logger *slog.Logger
	if sp != nil {
		logger = sp.logger
	}
	return &qwpPoolHousekeeper{
		interval:   interval,
		senderPool: sp,
		queryPool:  qp,
		logger:     logger,
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
			if !h.reapGuarded(func() { h.senderPool.reapIdle() }) {
				return
			}
			if !h.reapGuarded(func() { h.queryPool.reapIdle() }) {
				return
			}
		}
	}
}

// If removing unused clients panics, keep the pools alive and report the failure
// rather than retrying a partly completed operation. Shutdown can still close
// other clients where that is known to be safe.
func (h *qwpPoolHousekeeper) reapGuarded(fn func()) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			h.mu.Lock()
			h.err = fmt.Errorf("%w: housekeeper reap panicked: %v", ErrCleanupFailed, r)
			h.mu.Unlock()
			qwpFailedHousekeepers.Lock()
			qwpFailedHousekeepers.items = append(qwpFailedHousekeepers.items, h)
			qwpFailedHousekeepers.Unlock()
			go qwpEffectiveLogger(h.logger).Error("qwp: housekeeper cleanup failed", "error", h.err)
		}
	}()
	fn()
	return true
}

var qwpFailedHousekeepers struct {
	sync.Mutex
	items []*qwpPoolHousekeeper
}

func (h *qwpPoolHousekeeper) close(ctx context.Context) error {
	h.stopOnce.Do(func() { close(h.stop) })
	if h.started.Load() {
		select {
		case <-h.done:
		default:
			select {
			case <-h.done:
			case <-ctx.Done():
				select {
				case <-h.done:
				default:
					return errors.Join(ErrCleanupPending, ctx.Err())
				}
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}
