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
	"fmt"
	"sync"
	"time"
)

// qwpAsyncState manages the in-flight window and I/O goroutines for
// async QWP mode (in-flight window > 1). It coordinates between the
// user goroutine (which encodes and enqueues batches) and two I/O
// goroutines: senderLoop transmits batches, receiverLoop processes
// ACKs in parallel so multiple batches can be in flight on the wire
// at once (matches the Java client's sliding-window design).
//
// qwpAsyncBatch carries an encoded batch payload and a signal channel
// to mark the encoder's buffer as reusable after the data is written
// to the socket.
type qwpAsyncBatch struct {
	data        []byte
	readySignal chan<- struct{} // signaled after sendMessage completes
}

type qwpAsyncState struct {
	// sendCh carries encoded batch payloads from the user goroutine
	// to senderLoop. Buffered to decouple encoding from sending.
	sendCh chan qwpAsyncBatch

	// mu protects inFlightCount, nextSequence, ackedSequence,
	// senderDone, lastSentSequence, ioErr, and stopped.
	mu   sync.Mutex
	cond *sync.Cond

	// inFlightCount is the number of batches enqueued on sendCh or
	// sent but not yet ACKed. Incremented in acquireSlot; decremented
	// by releaseSlot (enqueue-cancelled or send-failed batches) and
	// by releaseSlotsUpTo (ACK-based cumulative release).
	inFlightCount int
	inFlightMax   int

	// nextSequence is the sequence number that will be assigned to
	// the next successfully-sent batch. First batch is 0. Incremented
	// only by senderLoop after a successful sendMessage.
	nextSequence int64
	// ackedSequence is the highest cumulative sequence acknowledged
	// by the server, or -1 if none. Updated only by receiverLoop.
	// The -1 sentinel matches Java's InFlightWindow.highestAcked and
	// disambiguates "no ACK yet" from "sequence 0 ACKed" — without it,
	// a server that starts its sequence counter at 0 would look like
	// a stale ACK and never release the first slot.
	ackedSequence int64

	// lastSentSequence is the sequence of the last batch actually
	// transmitted, or -1 if none. Set by senderLoop as it exits
	// (sendCh closed and drained). Once senderDone is true,
	// receiverLoop exits when ackedSequence >= lastSentSequence.
	lastSentSequence int64
	senderDone       bool

	// ioErr is the first error from either I/O goroutine. Once set,
	// all blocking operations return this error.
	ioErr error

	// stopped is set to true after both I/O goroutines have exited.
	stopped bool

	// doneSender is closed when senderLoop exits; doneReceiver is
	// closed when receiverLoop exits.
	doneSender   chan struct{}
	doneReceiver chan struct{}

	// wg tracks both I/O goroutines for clean shutdown.
	wg sync.WaitGroup

	// ctx is a cancellable context used by both goroutines for all
	// WebSocket operations. Cancelled by stop() or senderLoop (on
	// clean drain) to unblock sendMessage/readAck if the server
	// becomes unresponsive.
	ctx    context.Context
	cancel context.CancelFunc

	// transport is the WebSocket connection shared by both goroutines.
	// senderLoop and receiverLoop are single-writer / single-reader
	// on the connection respectively.
	transport *qwpTransport
}

// newQwpAsyncState creates async state with the given in-flight window
// size. The send channel is buffered to the window size so the user
// goroutine can enqueue without blocking until the window is full.
func newQwpAsyncState(maxWindow int, transport *qwpTransport) *qwpAsyncState {
	ctx, cancel := context.WithCancel(context.Background())
	a := &qwpAsyncState{
		sendCh:           make(chan qwpAsyncBatch, maxWindow),
		inFlightMax:      maxWindow,
		ackedSequence:    -1,
		lastSentSequence: -1,
		doneSender:       make(chan struct{}),
		doneReceiver:     make(chan struct{}),
		ctx:              ctx,
		cancel:           cancel,
		transport:        transport,
	}
	a.cond = sync.NewCond(&a.mu)
	return a
}

// acquireSlot blocks until there is space in the in-flight window.
// Returns ctx.Err() if ctx is cancelled during the wait, or the I/O
// goroutine's error if it has failed. Mirrors the Java client's
// InFlightWindow.addInFlight, which checks Thread.interrupt() during
// its park-spin.
func (a *qwpAsyncState) acquireSlot(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Watcher goroutine is spawned lazily on the first cond.Wait so
	// the fast path (slot immediately available) pays no overhead.
	var watchCancel chan struct{}
	defer func() {
		if watchCancel != nil {
			close(watchCancel)
		}
	}()

	for a.inFlightCount >= a.inFlightMax {
		if a.ioErr != nil {
			return a.ioErr
		}
		if a.stopped {
			return fmt.Errorf("qwp: async I/O goroutine stopped")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if watchCancel == nil {
			watchCancel = a.startCtxWatcher(ctx)
		}
		a.cond.Wait()
	}

	if a.ioErr != nil {
		return a.ioErr
	}

	a.inFlightCount++
	return nil
}

// startCtxWatcher launches a goroutine that Broadcasts on the cond
// when ctx is cancelled, so a caller in cond.Wait() wakes up and
// can return ctx.Err(). The returned channel stops the watcher —
// close it after exiting the wait loop.
func (a *qwpAsyncState) startCtxWatcher(ctx context.Context) chan struct{} {
	cancelWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			a.mu.Lock()
			a.cond.Broadcast()
			a.mu.Unlock()
		case <-cancelWatch:
		}
	}()
	return cancelWatch
}

// releaseSlot decrements inFlightCount by one and wakes a waiter.
// Used when a batch never reaches the wire: either the user goroutine
// cancelled its enqueue after acquireSlot, or senderLoop drained a
// batch without sending it (send failed or shutting down).
func (a *qwpAsyncState) releaseSlot() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.inFlightCount > 0 {
		a.inFlightCount--
	}
	a.cond.Signal()
}

// releaseSlotsUpTo processes a cumulative ACK: advances ackedSequence
// to the given sequence and releases (delta) slots, where delta counts
// the batches newly acknowledged. Returns a protocol error if the
// server acknowledged more batches than were sent. Called only by
// receiverLoop.
func (a *qwpAsyncState) releaseSlotsUpTo(seq int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if seq <= a.ackedSequence {
		// Stale or duplicate ACK — Java absorbs and keeps reading.
		return nil
	}
	if seq >= a.nextSequence {
		return fmt.Errorf(
			"qwp: server acknowledged sequence %d but only %d batches sent",
			seq, a.nextSequence,
		)
	}
	delta := int(seq - a.ackedSequence)
	a.ackedSequence = seq
	a.inFlightCount -= delta
	a.cond.Broadcast()
	return nil
}

// setError records the first I/O error and wakes all waiters.
// Subsequent calls are no-ops (first error wins).
func (a *qwpAsyncState) setError(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.ioErr == nil {
		a.ioErr = err
	}
	a.cond.Broadcast()
}

// checkError returns the I/O error if one has been set.
func (a *qwpAsyncState) checkError() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ioErr
}

// waitEmpty blocks until all in-flight batches have been ACKed.
// Returns ctx.Err() if ctx is cancelled during the wait, or the I/O
// goroutine's error if it fails before draining.
func (a *qwpAsyncState) waitEmpty(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var watchCancel chan struct{}
	defer func() {
		if watchCancel != nil {
			close(watchCancel)
		}
	}()

	for a.inFlightCount > 0 {
		if a.ioErr != nil {
			return a.ioErr
		}
		if a.stopped {
			return fmt.Errorf("qwp: async I/O goroutine stopped with %d batches in flight", a.inFlightCount)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if watchCancel == nil {
			watchCancel = a.startCtxWatcher(ctx)
		}
		a.cond.Wait()
	}

	return a.ioErr
}

// markStopped signals that both I/O goroutines have exited.
func (a *qwpAsyncState) markStopped() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.stopped = true
	a.cond.Broadcast()
}

// senderLoop consumes batches from sendCh, transmits them over the
// WebSocket, and assigns sequence numbers. It never blocks on ACKs;
// that is receiverLoop's job. Exits when sendCh is closed.
func (a *qwpAsyncState) senderLoop() {
	defer a.wg.Done()
	defer close(a.doneSender)

	for batch := range a.sendCh {
		a.mu.Lock()
		drop := a.ioErr != nil || a.ctx.Err() != nil
		a.mu.Unlock()

		if drop {
			// Already failing or shutting down — drain without sending.
			if batch.readySignal != nil {
				batch.readySignal <- struct{}{}
			}
			a.releaseSlot()
			continue
		}

		if err := a.transport.sendMessage(a.ctx, batch.data); err != nil {
			// Signal encoder buffer as reusable so the user goroutine
			// does not deadlock on encoder handoff.
			if batch.readySignal != nil {
				batch.readySignal <- struct{}{}
			}
			a.releaseSlot()
			a.setError(fmt.Errorf("qwp: async send failed: %w", err))
			continue
		}

		// Send succeeded — the encoder buffer is safe to reuse.
		if batch.readySignal != nil {
			batch.readySignal <- struct{}{}
		}
		a.mu.Lock()
		a.nextSequence++
		a.mu.Unlock()
	}

	// sendCh has been closed and drained. Record the highest
	// sequence actually sent so receiverLoop can decide when to
	// exit, and wake it if it is currently blocked in readAck but
	// has no more ACKs to process. If nothing was sent at all,
	// lastSentSequence stays -1 and the receiver exits immediately.
	a.mu.Lock()
	a.lastSentSequence = a.nextSequence - 1
	a.senderDone = true
	caughtUp := a.ackedSequence >= a.lastSentSequence
	a.cond.Broadcast()
	a.mu.Unlock()

	if caughtUp {
		a.cancel()
	}
}

// receiverLoop reads ACKs from the WebSocket and releases in-flight
// slots. Matches Java's cumulative-ACK semantics: a single ACK with
// sequence N releases (N - ackedSequence) slots.
//
// Exits when (a) senderLoop has finished AND ackedSequence has caught
// up to lastSentSequence, (b) ioErr has been set by either loop, or
// (c) readAck returns an error because ctx was cancelled.
func (a *qwpAsyncState) receiverLoop() {
	defer a.wg.Done()
	defer close(a.doneReceiver)

	for {
		a.mu.Lock()
		if a.ioErr != nil {
			a.mu.Unlock()
			return
		}
		if a.senderDone && a.ackedSequence >= a.lastSentSequence {
			a.mu.Unlock()
			return
		}
		a.mu.Unlock()

		status, data, err := a.transport.readAck(a.ctx)
		if err != nil {
			// Distinguish a clean shutdown (ctx cancelled once the
			// sender has drained and the receiver has nothing more
			// to wait for) from a real I/O failure.
			a.mu.Lock()
			draining := a.senderDone && a.ackedSequence >= a.lastSentSequence
			a.mu.Unlock()
			if !draining {
				a.setError(fmt.Errorf("qwp: async ack read failed: %w", err))
			}
			return
		}

		seq := parseAckSequence(data)

		if status != qwpStatusOK {
			qErr := newQwpErrorFromAck(data)
			if qErr == nil {
				qErr = &QwpError{Status: status, Sequence: seq, Message: "unknown error"}
			}
			a.setError(qErr)
			return
		}

		if err := a.releaseSlotsUpTo(seq); err != nil {
			a.setError(err)
			return
		}
	}
}

// start launches the sender and receiver goroutines.
func (a *qwpAsyncState) start() {
	a.wg.Add(2)
	go a.senderLoop()
	go a.receiverLoop()
	go func() {
		a.wg.Wait()
		a.markStopped()
	}()
}

// stop closes the send channel and waits for both I/O goroutines to
// exit. If they do not finish within the grace period (e.g., stuck
// on an unresponsive server), the I/O context is cancelled to force
// them out. Must be called exactly once.
func (a *qwpAsyncState) stop(gracePeriod time.Duration) {
	close(a.sendCh)

	// Wait for senderLoop to drain and exit, then for receiverLoop
	// to catch up and exit. senderLoop self-cancels the I/O context
	// if it observes the receiver already caught up, so in the
	// normal case we do not have to force anything.
	timer := time.NewTimer(gracePeriod)
	defer timer.Stop()

	select {
	case <-a.doneSender:
	case <-timer.C:
		a.cancel()
		<-a.doneSender
		<-a.doneReceiver
		a.wg.Wait()
		return
	}

	select {
	case <-a.doneReceiver:
	case <-timer.C:
		a.cancel()
		<-a.doneReceiver
	}

	a.wg.Wait()
	a.cancel() // idempotent; ensures context is always cleaned up
}
