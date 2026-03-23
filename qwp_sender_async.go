/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
)

// qwpAsyncState manages the in-flight window and I/O goroutine for
// async QWP mode (in-flight window > 1). It coordinates between the
// user goroutine (which encodes and enqueues batches) and the I/O
// goroutine (which sends over WebSocket and processes ACKs).
type qwpAsyncState struct {
	// sendCh carries encoded batch payloads from the user goroutine
	// to the I/O goroutine. Buffered to decouple encoding from sending.
	sendCh chan []byte

	// mu protects inFlightCount, ioErr, and stopped.
	mu   sync.Mutex
	cond *sync.Cond

	// inFlightCount is the number of batches sent but not yet ACKed.
	inFlightCount int
	// inFlightMax is the maximum concurrent in-flight batches.
	inFlightMax int

	// nextSequence is the sequence number to assign to the next batch.
	nextSequence uint64
	// ackedSequence is the highest cumulative sequence ACKed by the server.
	ackedSequence uint64

	// ioErr is the first error from the I/O goroutine. Once set, all
	// blocking operations return this error.
	ioErr error

	// stopped is set to true after the I/O goroutine exits.
	stopped bool

	// done is closed when the I/O goroutine exits.
	done chan struct{}

	// wg tracks the I/O goroutine for clean shutdown.
	wg sync.WaitGroup

	// transport is the WebSocket connection used by the I/O goroutine.
	transport *qwpTransport
}

// newQwpAsyncState creates async state with the given in-flight window size.
// The send channel is buffered to the window size so the user goroutine
// can enqueue without blocking until the window is full.
func newQwpAsyncState(maxWindow int, transport *qwpTransport) *qwpAsyncState {
	a := &qwpAsyncState{
		sendCh:      make(chan []byte, maxWindow),
		inFlightMax: maxWindow,
		done:        make(chan struct{}),
		transport:   transport,
	}
	a.cond = sync.NewCond(&a.mu)
	return a
}

// acquireSlot blocks until there is space in the in-flight window.
// Returns an error if the I/O goroutine has failed.
func (a *qwpAsyncState) acquireSlot() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for a.inFlightCount >= a.inFlightMax {
		if a.ioErr != nil {
			return a.ioErr
		}
		if a.stopped {
			return fmt.Errorf("qwp: async I/O goroutine stopped")
		}
		a.cond.Wait()
	}

	if a.ioErr != nil {
		return a.ioErr
	}

	a.inFlightCount++
	return nil
}

// releaseSlot decrements the in-flight count and wakes waiters.
// Called by the I/O goroutine after receiving an ACK.
func (a *qwpAsyncState) releaseSlot() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.inFlightCount > 0 {
		a.inFlightCount--
	}
	a.cond.Broadcast()
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
// Returns an error if the I/O goroutine fails before draining.
func (a *qwpAsyncState) waitEmpty() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for a.inFlightCount > 0 {
		if a.ioErr != nil {
			return a.ioErr
		}
		if a.stopped {
			return fmt.Errorf("qwp: async I/O goroutine stopped with %d batches in flight", a.inFlightCount)
		}
		a.cond.Wait()
	}

	return a.ioErr
}

// markStopped signals that the I/O goroutine has exited.
func (a *qwpAsyncState) markStopped() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.stopped = true
	a.cond.Broadcast()
}

// ioLoop is the I/O goroutine. It reads encoded batches from sendCh,
// sends them over the WebSocket, reads ACKs, and updates flow control.
func (a *qwpAsyncState) ioLoop() {
	defer a.wg.Done()
	defer a.markStopped()
	defer close(a.done)

	for batch := range a.sendCh {
		// Send the batch over the WebSocket.
		if err := a.transport.sendMessage(context.Background(), batch); err != nil {
			a.setError(fmt.Errorf("qwp: async send failed: %w", err))
			// Drain remaining batches from channel to unblock senders.
			for range a.sendCh {
				a.releaseSlot()
			}
			return
		}

		// Read the ACK.
		status, ackData, err := a.transport.readAck(context.Background())
		if err != nil {
			a.setError(fmt.Errorf("qwp: async ACK read failed: %w", err))
			for range a.sendCh {
				a.releaseSlot()
			}
			return
		}

		if byte(status) != qwpWireStatusOK {
			qErr := newQwpErrorFromAck(ackData)
			if qErr == nil {
				qErr = &QwpError{Status: byte(status), Message: "unknown error"}
			}
			a.setError(qErr)
			for range a.sendCh {
				a.releaseSlot()
			}
			return
		}

		// ACK success — release the slot.
		a.releaseSlot()
	}
}

// start launches the I/O goroutine.
func (a *qwpAsyncState) start() {
	a.wg.Add(1)
	go a.ioLoop()
}

// stop closes the send channel and waits for the I/O goroutine to exit.
// Must be called exactly once.
func (a *qwpAsyncState) stop() {
	close(a.sendCh)
	a.wg.Wait()
}
