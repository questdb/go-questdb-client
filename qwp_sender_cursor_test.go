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
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCursorSenderForTest builds a memory-mode cursor sender pointed
// at the given fake server. Returns the sender plus the engine + loop
// (so tests can inspect them) plus a cleanup that closes the sender.
func newCursorSenderForTest(t *testing.T, srv *qwpSfTestServer, autoFlushRows int) (*qwpLineSender, *qwpSfCursorEngine, *qwpSfSendLoop, func()) {
	t.Helper()
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 5*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	// 5s closeFlushTimeout matches the Java default; long enough
	// that drain-waits in tests don't flake under heavy parallel
	// test load.
	s, err := newQwpCursorLineSender(autoFlushRows, 0, 0, 0, 0, engine, loop, 5*time.Second)
	require.NoError(t, err)
	cleanup := func() {
		_ = s.Close(context.Background())
	}
	return s, engine, loop, cleanup
}

func TestQwpCursorSenderHappyPath(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.True(t, s.qwpCursorMode())

	for i := 0; i < 5; i++ {
		err := s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background())
		require.NoError(t, err, "row %d", i)
	}
	require.Equal(t, 5, s.pendingRowCount)
	require.NoError(t, s.Flush(context.Background()))
	// After Flush, pending rows are drained into the engine.
	assert.Equal(t, 0, s.pendingRowCount)
	// Wait for ackedFsn to catch up — Flush in cursor mode does NOT
	// wait for ACKs, so we wait here explicitly.
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 2*time.Second, 1*time.Millisecond)
	// Five frames should have been sent.
	assert.Equal(t, int64(1), loop.sendLoopTotalFramesSent(),
		"expected 1 multi-row frame, got %d", loop.sendLoopTotalFramesSent())
	assert.Equal(t, int64(1), srv.totalFramesReceived.Load())
}

func TestQwpCursorSenderFlushNoRowsIsCheap(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// Flush with no pending rows is a no-op. Crucially, it does NOT
	// block waiting for in-flight ACKs (Java spec: cursor flush
	// never waits for ACK). Should return immediately.
	start := time.Now()
	require.NoError(t, s.Flush(context.Background()))
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 50*time.Millisecond,
		"Flush(no rows) should return immediately, took %s", elapsed)
}

func TestQwpCursorSenderAutoFlushOnRowCount(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 3)
	defer cleanup()

	// 7 rows → autoFlushRows=3 should flush twice (after rows 3 and
	// 6); 7th row stays pending.
	for i := 0; i < 7; i++ {
		err := s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background())
		require.NoError(t, err, "row %d", i)
	}
	assert.Equal(t, 1, s.pendingRowCount)
	require.NoError(t, s.Flush(context.Background()))

	// Wait for drain.
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 2*time.Second, 1*time.Millisecond)
	// Three batches: row 3, row 6, and the explicit Flush.
	assert.Equal(t, int64(3), loop.sendLoopTotalFramesSent())
}

func TestQwpCursorSenderCloseDrainsEngine(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 5*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	s, err := newQwpCursorLineSender(0, 0, 0, 0, 0, engine, loop, 5*time.Second)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
	}
	// Don't call Flush — Close should encode pending rows and drain.
	require.NoError(t, s.Close(context.Background()))
	// After close, the engine must be fully drained.
	assert.Equal(t, engine.enginePublishedFsn(), engine.engineAckedFsn())
	assert.GreaterOrEqual(t, srv.totalFramesReceived.Load(), int64(1))
}

func TestQwpCursorSenderCloseDrainTimeoutReturnsError(t *testing.T) {
	// Server accepts frames but never ACKs. Close's drain wait must
	// time out within closeFlushTimeout AND return a non-nil error
	// that names publishedFsn / ackedFsn — silently swallowing it
	// would hide data loss from users who never call Flush.
	srv := newSilentAckServer(t)
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 5*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	s, err := newQwpCursorLineSender(0, 0, 0, 0, 0, engine, loop, 100*time.Millisecond)
	require.NoError(t, err)

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	start := time.Now()
	closeErr := s.Close(context.Background())
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 5*time.Second, "Close should not block on un-ACK'd data forever")
	require.Error(t, closeErr, "Close must surface the drain timeout, not swallow it")
	assert.Contains(t, closeErr.Error(), "drain timed out")
	assert.Contains(t, closeErr.Error(), "publishedFsn")
	assert.Contains(t, closeErr.Error(), "ackedFsn")
}

func TestQwpCursorSenderFlushAfterTerminalError(t *testing.T) {
	// ParseError defaults to Halt; SchemaMismatch is now Drop and
	// would not produce a terminal error.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	// First Flush enqueues; the loop hits the rejection and goes
	// terminal. Subsequent Flush calls must surface the error.
	_ = s.Flush(context.Background())

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)

	// Empty Flush after the loop is dead surfaces the terminal error.
	err := s.Flush(context.Background())
	require.Error(t, err)
}

// TestQwpCursorSenderTableEntrySurfacesTerminalError verifies that
// once the I/O loop has latched a terminal error, the next Table()
// call latches it into s.lastErr so the user observes it at the
// following At/AtNow instead of having to call Flush first. This
// matches the spec contract that the producer's next API call sees
// the latched HALT (sf-client.md §14.5).
func TestQwpCursorSenderTableEntrySurfacesTerminalError(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// Push one row and Flush so the loop hits the HALT and latches.
	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = s.Flush(context.Background())
	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)

	// New row: Table() must observe the latched terminal error and
	// arrange for it to surface at AtNow, without the user having
	// to Flush first.
	err := s.Table("t").Int64Column("v", 2).AtNow(context.Background())
	require.Error(t, err, "AtNow must surface the latched terminal error from Table()")
}

// newSilentAckServer creates a fake QWP server that accepts the
// upgrade and reads frames forever, but never sends any ACK. Used
// by close-drain-timeout and AwaitAckedFsn tests where we need an
// ACK gap to materialize.
func newSilentAckServer(t *testing.T) *qwpSfTestServer {
	t.Helper()
	return newQwpSfTestServer(t, qwpSfTestServerOpts{silentAcks: true})
}

func TestQwpCursorSenderAckedFsnTracksEngine(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// Before any publish, both producer-visible accessor and engine
	// agree at -1.
	assert.Equal(t, int64(-1), s.AckedFsn())

	for i := 0; i < 3; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
	}
	require.NoError(t, s.Flush(context.Background()))

	require.Eventually(t, func() bool {
		return s.AckedFsn() == engine.enginePublishedFsn()
	}, 2*time.Second, 1*time.Millisecond)
	assert.GreaterOrEqual(t, s.AckedFsn(), int64(0))
}

func TestQwpCursorSenderAwaitAckedFsnHappyPath(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	// autoFlushRows=2 → enqueue happens without blocking on ACK,
	// so AwaitAckedFsn does meaningful waiting work.
	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 2)
	defer cleanup()

	for i := 0; i < 4; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
	}
	target := engine.enginePublishedFsn()
	require.GreaterOrEqual(t, target, int64(0), "auto-flush should have published at least one frame")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, s.AwaitAckedFsn(ctx, target))
	assert.GreaterOrEqual(t, s.AckedFsn(), target)
}

func TestQwpCursorSenderAwaitAckedFsnTimeout(t *testing.T) {
	srv := newSilentAckServer(t)
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 5*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	// autoFlushRows=1 enqueues the row into the engine on AtNow,
	// without blocking on ACK — exactly the auto-flush path users
	// pair with AwaitAckedFsn. closeTimeout=100ms keeps the deferred
	// Close fast (the server never ACKs).
	s, err := newQwpCursorLineSender(1, 0, 0, 0, 0, engine, loop, 100*time.Millisecond)
	require.NoError(t, err)
	defer func() { _ = s.Close(context.Background()) }()

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.Eventually(t, func() bool {
		return engine.enginePublishedFsn() >= 0
	}, time.Second, time.Millisecond, "auto-flush should have published the frame")
	target := engine.enginePublishedFsn()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = s.AwaitAckedFsn(ctx, target)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded, "no ACK was ever sent — must time out")
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
}

func TestQwpSenderAwaitAckedFsnAlreadyAcked(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))

	// Flush already waited for ACK — AwaitAckedFsn for the same
	// target returns immediately without consuming the deadline.
	target := engine.enginePublishedFsn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, s.AwaitAckedFsn(ctx, target))
	assert.Less(t, time.Since(start), 50*time.Millisecond,
		"AwaitAckedFsn must short-circuit when target is already met")

	// A negative target is trivially reached, even with an
	// already-cancelled context (the pre-loop check returns first).
	cancelled, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	require.NoError(t, s.AwaitAckedFsn(cancelled, -1))
}

// stableGoroutineCount returns runtime.NumGoroutine() once it has
// settled: it GCs and samples until two successive reads agree (or a
// bounded number of attempts elapse), so a transient teardown
// goroutine doesn't poison the sample.
func stableGoroutineCount() int {
	prev := -1
	for i := 0; i < 50; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == prev {
			return n
		}
		prev = n
	}
	return prev
}

// TestQwpCursorNoGoroutineLeakOnClose re-creates the goroutine-leak
// coverage that the removed TestQwpAsyncGoroutineLeakOnClose provided
// for the old async state. The cursor model spawns *more* goroutines
// than the async one did — per sender: run(), plus a senderLoop and a
// receiverLoop per connection — all of which Close()/sendLoopClose()
// must join. A leak of even one of them per sender would be invisible
// to every other cursor test (they each build exactly one sender),
// so this drives many open/send/flush/close cycles and asserts the
// goroutine count does not grow with the cycle count.
func TestQwpCursorNoGoroutineLeakOnClose(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	runCycle := func() {
		s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
		require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
		require.NoError(t, s.Flush(context.Background()))
		require.Eventually(t, func() bool {
			return engine.engineAckedFsn() >= engine.enginePublishedFsn()
		}, 2*time.Second, 1*time.Millisecond, "frame never ACKed")
		cleanup() // Close(): joins run() + sender/receiver goroutines.
	}

	// Warm-up cycle so the httptest accept machinery and any
	// once-initialized globals are already counted in the baseline.
	runCycle()
	base := stableGoroutineCount()

	const cycles = 25
	for i := 0; i < cycles; i++ {
		runCycle()
	}

	// Teardown is partly asynchronous (server-side WS conn goroutines
	// unwind once the client drops the transport), so give it time to
	// settle. A per-cycle leak across run()/senderLoop/receiverLoop
	// would add ~3×25 goroutines — far past the constant slack — so
	// this stays sensitive without flaking on transient runtime/server
	// goroutines.
	const slack = 8
	var got int
	require.Eventuallyf(t, func() bool {
		got = stableGoroutineCount()
		return got <= base+slack
	}, 10*time.Second, 100*time.Millisecond,
		"goroutine count did not return to baseline after %d cursor "+
			"open/send/flush/close cycles", cycles)
	assert.LessOrEqualf(t, got, base+slack,
		"goroutine count grew from %d to %d across %d cycles — Close "+
			"is leaking cursor send-loop goroutines", base, got, cycles)
}
