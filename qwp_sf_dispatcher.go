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

// qwpSfDefaultErrorInboxCapacity is the default size of the bounded
// inbox connecting the I/O goroutine to the user-handler dispatcher
// goroutine. Java spec § "Configuration knobs" sets the same value.
const qwpSfDefaultErrorInboxCapacity = 256

// qwpSfMinErrorInboxCapacity is the floor enforced on user-supplied
// capacities by the connect-string sanitizer per the spec.
const qwpSfMinErrorInboxCapacity = 16

// qwpSfDispatcherDrainTimeout is the maximum time close() waits for
// the dispatcher loop to finish draining queued errors before giving
// up and abandoning anything still in the inbox.
const qwpSfDispatcherDrainTimeout = 100 * time.Millisecond

// qwpSfErrorDispatcher is the off-I/O delivery channel for SenderError
// notifications. The I/O goroutine offers errors non-blockingly into a
// bounded channel; a dedicated goroutine drains the channel and
// invokes the user-supplied SenderErrorHandler. A slow handler does
// not stall publishing — surplus offers drop and bump a counter.
//
// The dispatcher goroutine is started lazily on the first successful
// offer, so workloads that never see a server error pay zero
// goroutine cost.
type qwpSfErrorDispatcher struct {
	handler SenderErrorHandler

	// inbox is the bounded delivery channel. Capacity is set at
	// construction; never resized.
	inbox chan *SenderError

	// done is closed by close() to signal the loop should drain and
	// exit. Closing the inbox would race with offer; instead the
	// loop polls done.
	done chan struct{}

	// startMu serializes lazy-start. Combined with started.Load(),
	// it ensures the goroutine spawns exactly once.
	startMu sync.Mutex

	// started flips true after the dispatch goroutine is launched.
	started atomic.Bool

	// closed flips true on close(). offer() short-circuits to drop
	// when closed.
	closed atomic.Bool

	dropped   atomic.Int64
	delivered atomic.Int64

	// wg waits for the dispatch goroutine to exit during close().
	wg sync.WaitGroup
}

// newQwpSfErrorDispatcher constructs a dispatcher with the given
// handler and inbox capacity. handler must be non-nil; capacity must
// be ≥ 1 (the connect-string sanitizer separately enforces ≥ 16 for
// user-supplied values, but internal callers like tests and the
// silent-default constructor are allowed smaller buffers).
func newQwpSfErrorDispatcher(handler SenderErrorHandler, capacity int) *qwpSfErrorDispatcher {
	if handler == nil {
		handler = defaultSenderErrorHandler
	}
	if capacity < 1 {
		capacity = qwpSfDefaultErrorInboxCapacity
	}
	return &qwpSfErrorDispatcher{
		handler: handler,
		inbox:   make(chan *SenderError, capacity),
		done:    make(chan struct{}),
	}
}

// offer enqueues a SenderError for asynchronous delivery to the
// handler. Non-blocking: returns true if the error was queued, false
// if the inbox was full or the dispatcher has been closed (the drop
// counter is bumped in both cases for ops visibility — except when
// closed, in which case the counter stays put because the sender is
// shutting down and queueing more would be misleading).
//
// Lazy-starts the dispatch goroutine on the first successful offer.
func (d *qwpSfErrorDispatcher) offer(e *SenderError) bool {
	if d == nil || e == nil {
		return false
	}
	if d.closed.Load() {
		return false
	}
	select {
	case d.inbox <- e:
		// Common case after the first offer: goroutine is already
		// running; this is a single channel send and a volatile read.
		if !d.started.Load() {
			d.startIfNeeded()
		}
		return true
	default:
		d.dropped.Add(1)
		return false
	}
}

// startIfNeeded launches the dispatch goroutine if it hasn't been
// already. Idempotent under contention.
func (d *qwpSfErrorDispatcher) startIfNeeded() {
	d.startMu.Lock()
	defer d.startMu.Unlock()
	if d.started.Load() || d.closed.Load() {
		return
	}
	d.wg.Add(1)
	d.started.Store(true)
	go d.loop()
}

// loop is the dispatch goroutine body. It ranges over the inbox
// until close() signals via done; on shutdown it drains any
// remaining queued errors with a short deadline before returning.
//
// Handler panics are recovered and logged; the dispatcher and
// sender continue running.
func (d *qwpSfErrorDispatcher) loop() {
	defer d.wg.Done()
	for {
		select {
		case e := <-d.inbox:
			if e == nil {
				continue
			}
			d.deliver(e)
		case <-d.done:
			d.drain()
			return
		}
	}
}

// drain delivers any errors still in the inbox after close. Two
// exit paths: the inbox is empty (the common case — by the time
// drain runs, closed.Load() is true and producers stop offering),
// or qwpSfDispatcherDrainTimeout fires (a slow handler is still
// chewing through queued items). A producer that races the close
// (read closed=false then was preempted before the channel send)
// may lose its notification — best-effort, matching offer's contract.
func (d *qwpSfErrorDispatcher) drain() {
	deadline := time.NewTimer(qwpSfDispatcherDrainTimeout)
	defer deadline.Stop()
	for {
		select {
		case e := <-d.inbox:
			if e == nil {
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

// deliver invokes the handler under a panic guard, bumping the
// delivered counter unconditionally — a handler panic still counts
// as "we attempted delivery" for ops visibility.
func (d *qwpSfErrorDispatcher) deliver(e *SenderError) {
	d.delivered.Add(1)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] qwp/sf: error handler panicked on %s: %v", e, r)
		}
	}()
	d.handler(e)
}

// close stops the dispatch goroutine and waits for it to finish
// draining (up to qwpSfDispatcherDrainTimeout). Idempotent — second
// and subsequent calls are no-ops.
func (d *qwpSfErrorDispatcher) close() {
	if d == nil {
		return
	}
	if !d.closed.CompareAndSwap(false, true) {
		return
	}
	close(d.done)
	d.wg.Wait()
}

// droppedNotifications returns the cumulative count of inbox-overflow
// drops. Non-zero means the user's handler is slower than the error
// rate.
func (d *qwpSfErrorDispatcher) droppedNotifications() int64 {
	if d == nil {
		return 0
	}
	return d.dropped.Load()
}

// totalDelivered returns the cumulative count of errors delivered to
// the handler (including those where the handler panicked).
func (d *qwpSfErrorDispatcher) totalDelivered() int64 {
	if d == nil {
		return 0
	}
	return d.delivered.Load()
}

// defaultSenderErrorHandler is the loud-not-silent fallback used when
// the user has not registered a handler. ERROR for HALT, WARN for
// DROP — both with the full structured payload. Per Java spec
// § "Loud defaults — silence is forbidden".
func defaultSenderErrorHandler(e *SenderError) {
	if e == nil {
		return
	}
	level := "[ERROR]"
	if e.AppliedPolicy == PolicyDropAndContinue {
		level = "[WARN]"
	}
	log.Printf("%s qwp/sf: %s", level, e)
}
