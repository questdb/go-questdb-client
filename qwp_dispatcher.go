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

// qwpDispatcher is the off-I/O delivery channel for the QWP sender's async
// user callbacks. The I/O goroutine offers items non-blockingly into a bounded
// inbox; a dedicated goroutine drains it and invokes the handler, so a slow
// handler never stalls publishing or reconnect. Overflow displaces the oldest
// queued item and bumps the dropped counter. The goroutine starts lazily on
// the first offer, so a sender that never notifies pays no goroutine cost.
//
// It generalises the proven qwpSfErrorDispatcher machinery (drop-oldest,
// lazy start, re-entrant-safe bounded close) over the payload type so the
// connection-event listener reuses it without copying it. The error
// dispatcher predates this type and is left as-is; it could migrate here.
type qwpDispatcher[T any] struct {
	handler   func(T)
	describe  func(T) string // used only in the panic-recovery log line
	valid     func(T) bool   // nil → every item is valid
	logPrefix string         // log tag for the panic-recovery line (e.g. "qwp/conn")

	inbox chan T
	done  chan struct{}

	mu      sync.Mutex
	startMu sync.Mutex
	started atomic.Bool
	closed  atomic.Bool
	// abandon: set when close() gives up joining a wedged handler. loop()/drain()
	// then drop instead of deliver, so a handler returning past the close timeout
	// can't fire callbacks after close() returns (bounded-close contract).
	abandon atomic.Bool

	dropped   atomic.Int64
	delivered atomic.Int64
	loopGoid  atomic.Int64
	wg        sync.WaitGroup
}

func newQwpDispatcher[T any](handler func(T), describe func(T) string, valid func(T) bool, logPrefix string, capacity int) *qwpDispatcher[T] {
	if capacity < 1 {
		capacity = qwpSfDefaultErrorInboxCapacity
	}
	if capacity > qwpSfMaxErrorInboxCapacity {
		// Guard against an unsanitized caller: the current instantiations pre-clamp
		// (conf_parse), but the type invites reuse, and makechan panics on a huge size.
		capacity = qwpSfMaxErrorInboxCapacity
	}
	if logPrefix == "" {
		logPrefix = "qwp"
	}
	return &qwpDispatcher[T]{
		handler:   handler,
		describe:  describe,
		valid:     valid,
		logPrefix: logPrefix,
		inbox:     make(chan T, capacity),
		done:      make(chan struct{}),
	}
}

// offer enqueues an item for async delivery. Holds mu across the closed-check
// and the send so close cannot interleave; lazy-starts the goroutine. When the
// inbox is full the oldest queued item is displaced (the newest event is the
// most informative). Returns false only when the dispatcher is closed or the
// item is rejected by valid.
func (d *qwpDispatcher[T]) offer(e T) bool {
	if d == nil {
		return false
	}
	if d.valid != nil && !d.valid(e) {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return false
	}
	if !d.started.Load() {
		d.startIfNeeded()
	}
	// Drop-oldest, converging in ≤2 iterations: either our receive frees a slot
	// and the retry send lands, or the consumer drained concurrently and the
	// retry lands without counting a drop.
	for {
		select {
		case d.inbox <- e:
			return true
		default:
		}
		select {
		case <-d.inbox:
			d.dropped.Add(1)
		default:
		}
	}
}

func (d *qwpDispatcher[T]) startIfNeeded() {
	d.startMu.Lock()
	defer d.startMu.Unlock()
	if d.started.Load() || d.closed.Load() {
		return
	}
	d.wg.Add(1)
	d.started.Store(true)
	go d.loop()
}

func (d *qwpDispatcher[T]) loop() {
	defer d.wg.Done()
	d.loopGoid.Store(qwpGoid())
	defer d.loopGoid.Store(0)
	for {
		select {
		case e := <-d.inbox:
			if d.abandon.Load() {
				d.dropped.Add(1)
				continue
			}
			d.deliver(e)
		case <-d.done:
			d.drain()
			return
		}
	}
}

func (d *qwpDispatcher[T]) drain() {
	deadline := time.NewTimer(qwpSfDispatcherDrainTimeout)
	defer deadline.Stop()
	for {
		select {
		case e := <-d.inbox:
			if d.abandon.Load() {
				d.dropped.Add(1)
				continue
			}
			d.deliver(e)
		case <-deadline.C:
			return
		default:
			return
		}
	}
}

func (d *qwpDispatcher[T]) deliver(e T) {
	d.delivered.Add(1)
	defer func() {
		if r := recover(); r != nil {
			msg := ""
			if d.describe != nil {
				msg = d.describe(e)
			}
			log.Printf("[ERROR] %s: handler panicked on %s: %v", d.logPrefix, msg, r)
		}
	}()
	d.handler(e)
}

// close signals the goroutine to drain and exit, joining it within a bounded
// budget. Idempotent. A handler that re-enters close on the loop goroutine
// (e.g. a listener that calls Close) is recognised via loopGoid and returns
// without a self-join; loop() then unwinds, observes done, and drains.
func (d *qwpDispatcher[T]) close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if !d.closed.CompareAndSwap(false, true) {
		d.mu.Unlock()
		return
	}
	close(d.done)
	started := d.started.Load()
	d.mu.Unlock()

	if g := qwpGoid(); g != 0 && d.loopGoid.Load() == g {
		return
	}
	if !started {
		d.drain()
		return
	}

	joined := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(joined)
	}()
	timer := time.NewTimer(qwpSfDispatcherCloseJoinTimeout)
	defer timer.Stop()
	select {
	case <-joined:
	case <-timer.C:
		d.abandon.Store(true)
		log.Printf("[WARN] %s: handler still running after %s on close; "+
			"abandoning dispatcher goroutine and dropping queued notifications",
			d.logPrefix, qwpSfDispatcherCloseJoinTimeout)
	}
	// Whatever is still queued (abandoned by drain's timeout or unreached by a
	// wedged handler) counts as dropped; re-delivering would defeat the bound.
	for {
		select {
		case <-d.inbox:
			d.dropped.Add(1)
		default:
			return
		}
	}
}

func (d *qwpDispatcher[T]) droppedNotifications() int64 {
	if d == nil {
		return 0
	}
	return d.dropped.Load()
}

func (d *qwpDispatcher[T]) totalDelivered() int64 {
	if d == nil {
		return 0
	}
	return d.delivered.Load()
}
