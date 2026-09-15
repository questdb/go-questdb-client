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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQwpSfEngineCloseDuringBackpressuredAppendNoCrash is the
// regression for the engine-level crash (Hazard A).
//
// A SenderErrorHandler is documented as allowed to call Close(). When a
// HALT stalls the wire, the send loop stops draining, the cursor ring
// fills, and the producer parks in engineAppendBlocking's backpressure
// spin — calling appendOrFsn every park interval. Close() then tears
// the engine down on a different goroutine: segmentRingClose swaps the
// active segment to nil and munmaps it while the parked producer is
// still calling appendOrFsn. Pre-fix the producer dereferences the
// just-nil'd active segment.
//
// Memory mode is used deliberately: there the dangling access is a
// recoverable nil-pointer panic, so the failure is assertable rather
// than a process-killing SIGBUS (which is what the equivalent SF-mode
// race produces against the munmapped pages). The same engine-level
// append/close serialization fixes both.
func TestQwpSfEngineCloseDuringBackpressuredAppendNoCrash(t *testing.T) {
	const segSize int64 = 96 // 24-byte header + 72-byte payload region
	// Cap total bytes at one segment so the manager never provisions a
	// hot spare: once the active fills, every further append
	// backpressures forever (nothing acks, so no trim frees space). Long
	// append deadline so the producer stays parked until we close it.
	e, err := qwpSfNewCursorEngine("", segSize, segSize, 30*time.Second)
	require.NoError(t, err)

	// Fill the active segment: capacity 72, each frame is 8-byte envelope
	// + 16-byte payload = 24, so exactly 3 frames fit.
	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "fill frame %d", i)
	}

	// Park a producer on the 4th append. It spins in the backpressure
	// loop until either the (30s) deadline or the engine is closed under
	// it. Any panic is recovered so the test binary survives to assert.
	var prodErr error
	var prodPanic atomic.Value
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				prodPanic.Store(fmt.Sprintf("%v", r))
			}
		}()
		_, prodErr = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	}()

	// Wait until the producer is genuinely in the backpressure spin
	// (stall counter bumps once on the first miss, before the spin).
	require.Eventually(t, func() bool {
		return e.engineTotalBackpressureStalls() >= 1
	}, 2*time.Second, 50*time.Microsecond,
		"producer never entered the backpressure spin")

	// Close the engine out from under the parked producer — exactly what
	// a SenderErrorHandler's Close() does on the dispatcher goroutine.
	require.NoError(t, e.engineClose())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("parked producer never returned after engineClose")
	}

	require.Nil(t, prodPanic.Load(),
		"producer crashed dereferencing a torn-down segment: %v", prodPanic.Load())
	require.ErrorIs(t, prodErr, qwpSfErrEngineClosed,
		"a producer parked in backpressure must observe a clean closed-engine "+
			"error once the engine is closed, got: %v", prodErr)
}

// Error callbacks notify the owner. Only the owner flushes the staged rows
// and closes the sender; a notification itself never touches producer state.
func TestQwpSenderErrorHandlerNotifiesOwnerToFlushAndClose(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	ctx := context.Background()
	producerReady := make(chan struct{})
	handlerDone := make(chan struct{})
	var once sync.Once
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		once.Do(func() {
			// Wait until the owner has staged the rows, then notify it.
			<-producerReady
			close(handlerDone)
		})
	}, 16)

	// Publish the first batch before staging the rows used by the assertion.
	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))

	// Stage two rows before allowing the notification.
	require.NoError(t, s.Table("t").Int64Column("v", 2).AtNow(ctx))
	require.NoError(t, s.Table("t").Int64Column("v", 3).AtNow(ctx))
	require.Equal(t, 2, s.pendingRowCount)
	fsnBefore := engine.enginePublishedFsn()

	loop.sendLoopDispatcher().offer(&SenderError{Category: CategoryWriteError, AppliedPolicy: PolicyRetriable})
	close(producerReady) // release the notification; only this owner may flush

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("error notification was not delivered")
	}

	// The callback only signalled: rows remain staged for the owner.
	assert.Equal(t, 2, s.pendingRowCount,
		"notification must not publish producer-buffered rows")
	assert.Equal(t, fsnBefore, engine.enginePublishedFsn())
	require.NoError(t, s.Flush(ctx))
	require.Zero(t, s.pendingRowCount)
	require.Greater(t, engine.enginePublishedFsn(), fsnBefore)
	s.closeTimeout = 0
	require.NoError(t, s.Close(ctx))
}

// The callback asks a running producer to stop. The producer itself closes
// its handle after its last use, without racing mutable sender state.
func TestQwpSenderErrorHandlerStopsOwner(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusSchemaMismatch})
	defer srv.Close()

	// autoFlushRows=1 triggers the server rejection and its notification.
	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 1)
	defer cleanup()

	closed := make(chan struct{})
	var once sync.Once
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		once.Do(func() {
			close(closed)
		})
	}, 16)

	var prodPanic atomic.Value
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		defer func() { _ = s.Close(context.Background()) }()
		defer func() {
			if r := recover(); r != nil {
				prodPanic.Store(fmt.Sprintf("%v", r))
			}
		}()
		ctx := context.Background()
		for i := 0; i < 100000; i++ {
			select {
			case <-closed:
				return
			default:
			}
			// A fresh table per row keeps the tableBuffers map churning,
			// maximizing overlap with closeCursor's map range.
			tbl := fmt.Sprintf("t%d", i)
			if err := s.Table(tbl).Int64Column("v", int64(i)).AtNow(ctx); err != nil {
				return // closed-sender or terminal error: producer stops cleanly
			}
		}
	}()

	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never signalled the owner")
	}

	select {
	case <-prodDone:
	case <-time.After(10 * time.Second):
		t.Fatal("producer goroutine did not stop after Close()")
	}
	require.Nil(t, prodPanic.Load(),
		"producer panicked while handling its stop notification: %v", prodPanic.Load())
}
