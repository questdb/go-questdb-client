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
	"runtime"
	"strconv"
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
// not stall publishing — overflow displaces the oldest queued entry
// (sf-client.md §14.6) and bumps droppedNotifications.
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

	// mu serializes offer vs close. offer holds it from the closed
	// check through the channel send; close holds it across the
	// CAS that flips closed=true and the close(done) call. This
	// makes the closed-flag check and the channel send atomic with
	// respect to close — a producer that read closed=false cannot
	// then have its send land after close has already drained.
	mu sync.Mutex

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

	// loopGoid is the goroutine ID of loop(), stored when it starts
	// and cleared (back to 0) when it exits. close() compares the
	// caller's goid against it to detect a re-entrant shutdown: a
	// SenderErrorHandler that calls Close() — or swaps the handler,
	// routing through sendLoopSetErrorHandler -> old.close() — runs
	// inside deliver() *on this goroutine*. A wg.Wait() from there
	// would join the loop goroutine to itself and hang forever. 0
	// never matches a real goid, so a close() before loop() starts
	// (or after it exits) takes the normal waiting path.
	loopGoid atomic.Int64

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
// handler. Always admits the new entry unless the dispatcher is
// closed or e is nil. When the inbox is full, the oldest queued
// entry is displaced to make room (drop-oldest per sf-client.md
// §14.6 — watermarks are monotonic, so the newest entry is always
// the most informative). Each displacement bumps droppedNotifications.
//
// Holds mu across the closed-check, send, and any drop step so close
// cannot interleave. Lazy-starts the dispatch goroutine on the first
// call. Returns true when the new entry is queued, false only when
// the dispatcher is closed or e is nil.
func (d *qwpSfErrorDispatcher) offer(e *SenderError) bool {
	if d == nil || e == nil {
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
	// Drop-oldest overflow. We hold mu so no concurrent producer can
	// run; only the consumer goroutine races with our receive step,
	// and it can only remove items. The loop converges in ≤2 iters:
	// either our receive drops the head and the retry send succeeds,
	// or the consumer drained between the failed send and our receive
	// (default fires) and the retry succeeds without counting a drop.
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
	// Publish our goroutine identity before the first deliver() so a
	// handler that re-enters close() on this goroutine is recognized.
	// Cleared on exit so a later close() never matches a stale id.
	d.loopGoid.Store(qwpGoid())
	defer d.loopGoid.Store(0)
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
// chewing through queued items). With offer/close serialized
// through mu, no new sends can land here once close has run, so
// the inbox is guaranteed to go quiet.
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
//
// Acquires mu before flipping closed and closing done, so any
// in-flight offer either commits its send first (and gets handled
// below) or sees closed=true and returns false.
//
// Two post-wait paths:
//
//   - Goroutine never started (no offer ever succeeded, or only
//     direct inbox injection in tests): no loop/drain ran, so call
//     drain() here to deliver any queued items within the same
//     bounded budget.
//
//   - Goroutine ran: drain() already had its budget. Anything still
//     in the inbox is what drain() deliberately abandoned via its
//     timeout (slow handler). Re-delivering on the way out would
//     defeat the cap, so count those as dropped and exit. This is
//     what makes qwpSfDispatcherDrainTimeout a hard ceiling on
//     close() blocking time.
func (d *qwpSfErrorDispatcher) close() {
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

	// Re-entrant shutdown guard. A SenderErrorHandler invoked by
	// deliver() on the loop goroutine is allowed to call Close()
	// (or swap the handler, which routes through
	// sendLoopSetErrorHandler -> old.close()). Both land here on
	// this very goroutine. wg.Wait() would block until loop() calls
	// wg.Done(), but loop() is the current goroutine, suspended in
	// the handler frame below this call — a permanent self-join that
	// no timeout escapes. done is already closed above, so once the
	// handler stack unwinds, loop() observes done, runs its own
	// bounded drain(), and exits cleanly. Skip the wait (and the
	// post-wait inbox sweep, which would race loop()'s drain) and
	// return. Non-loop callers fall through to the normal path. The
	// g != 0 check keeps a goid parse failure (returns 0) from
	// matching the loopGoid==0 "not running" sentinel.
	if g := qwpGoid(); g != 0 && d.loopGoid.Load() == g {
		return
	}

	d.wg.Wait()
	if !started {
		d.drain()
		return
	}
	for {
		select {
		case e := <-d.inbox:
			if e != nil {
				d.dropped.Add(1)
			}
		default:
			return
		}
	}
}

// droppedNotifications returns the cumulative count of inbox-overflow
// displacements (drop-oldest) plus any items abandoned at close().
// Non-zero means the user's handler is slower than the error rate.
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

// qwpGoid returns the numeric ID of the calling goroutine, or 0 if it
// cannot be parsed. Go exposes goroutine identity only through the
// runtime.Stack header ("goroutine <id> [<status>]:"); there is no
// public accessor. This is used solely by the dispatcher's re-entrant
// close() guard — a SenderErrorHandler that calls Close() runs on the
// dispatcher loop goroutine and a blocking join from there would
// self-deadlock. The cost (one fixed-size runtime.Stack of the current
// goroutine only) is paid once at loop() start and on close(), never
// on the publish/encode hot path.
func qwpGoid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	b := buf[:n]
	if len(b) < len(prefix) {
		return 0
	}
	b = b[len(prefix):]
	i := 0
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		i++
	}
	id, err := strconv.ParseInt(string(b[:i]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
