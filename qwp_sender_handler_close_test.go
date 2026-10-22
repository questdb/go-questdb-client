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
// Callbacks must not call Close while the application is using the sender. This
// test bypasses that rule and closes the engine while an append is waiting. The
// unsupported overlap must return an error instead of crashing. Before the fix,
// Close could clear the active segment just before the waiting append used it.
//
// The test uses memory-backed storage so an invalid access produces a panic that
// the test can catch. With disk-backed storage, the same bug could terminate the
// process. The same engine lock protects both modes.
func TestQwpSfEngineCloseDuringBackpressuredAppendNoCrash(t *testing.T) {
	const segSize int64 = 96 // 24-byte header + 72-byte payload region
	// Limit storage to one segment. Once it is full, another append must wait
	// because no frames are acknowledged or removed. Use a long timeout so the
	// append is still waiting when the engine closes.
	e, err := qwpSfNewCursorEngine("", segSize, segSize, 30*time.Second)
	require.NoError(t, err)

	// Fill the active segment: capacity 72, each frame is 8-byte envelope
	// + 16-byte payload = 24, so exactly 3 frames fit.
	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "fill frame %d", i)
	}

	// Start a fourth append, which must wait for space or for the engine to
	// close. Catch any panic so the test can report it as a failure.
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

	// Wait until the fourth append has tried and failed to find space.
	require.Eventually(t, func() bool {
		return e.engineTotalBackpressureStalls() >= 1
	}, 2*time.Second, 50*time.Microsecond,
		"append never started waiting for space")

	// Close the engine while the append is waiting. The append must return an
	// error instead of panicking.
	require.NoError(t, e.engineClose())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("waiting append did not return after the engine closed")
	}

	require.Nil(t, prodPanic.Load(),
		"append crashed after the active segment was cleared: %v", prodPanic.Load())
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
