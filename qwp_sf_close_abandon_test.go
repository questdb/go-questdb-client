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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestQwpSenderCloseRetainsOwnerForStorageRetry(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, nil,
		func(context.Context, int) (*qwpTransport, error) {
			return nil, errors.New("unexpected reconnect")
		}, time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)

	injected := errors.New("injected first flock release failure")
	calls := atomic.Int32{}
	flockHook := func() error {
		if calls.Add(1) == 1 {
			return injected
		}
		return nil
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&flockHook)
	t.Cleanup(func() {
		qwpSfTestBeforeFlockReleaseHook.Store(nil)
		_ = engine.engineClose()
	})

	require.ErrorIs(t, sender.Close(context.Background()), injected)
	require.Eventually(t, engine.engineCloseCompleted, qwpTestWaitTimeout, 5*time.Millisecond,
		"the original cleanup owner must retry the storage error")
	require.GreaterOrEqual(t, calls.Load(), int32(2))

	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpSenderDrainerPoolPanicKeepsEarlierErrorAndCancels(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, nil,
		func(context.Context, int) (*qwpTransport, error) {
			return nil, errors.New("unexpected reconnect")
		}, time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)
	pool := qwpSfNewDrainerPool(1)
	sender.drainerPool = pool
	earlierErr := errors.New("earlier close failure")
	sender.lastErr = earlierErr

	orphanDir := t.TempDir()
	orphanEngine, err := qwpSfNewCursorEngine(orphanDir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = orphanEngine.engineAppendBlocking(context.Background(), []byte("orphan frame"))
	require.NoError(t, err)
	require.NoError(t, orphanEngine.engineClose())
	dialEntered := make(chan struct{})
	blockingFactory := func(ctx context.Context, _ int) (*qwpTransport, error) {
		close(dialEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	drainer := qwpSfNewOrphanDrainer(
		orphanDir, 4096, qwpSfUnlimitedTotalBytes,
		blockingFactory, nil,
		time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	require.NoError(t, pool.drainerPoolSubmit(context.Background(), drainer))
	select {
	case <-dialEntered:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("orphan drainer did not acquire its slot and enter the blocking dial")
	}

	boom := func() { panic("injected drainer pool panic") }
	qwpTestDrainerPoolCloseHook.Store(&boom)
	t.Cleanup(func() {
		qwpTestDrainerPoolCloseHook.Store(nil)
		pool.drainerPoolClose()
		_ = engine.engineClose()
	})

	closeErr := sender.Close(context.Background())
	require.ErrorIs(t, closeErr, earlierErr,
		"a later drainer failure must not replace the first user-facing error")
	require.ErrorIs(t, closeErr, ErrCleanupFailed)
	select {
	case <-pool.ctx.Done():
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("a drainer-pool panic skipped its installed cancellation")
	}
	drainersDone := make(chan struct{})
	go func() {
		pool.wg.Wait()
		close(drainersDone)
	}()
	select {
	case <-drainersDone:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("cancelled orphan drainer did not exit after the pool-close panic")
	}
	require.Empty(t, pool.drainerPoolSnapshot())
	require.Equal(t, qwpSfDrainOutcomeStopped, drainer.drainerOutcome())
	orphanLock, orphanLockErr := qwpSfAcquireSlotLock(orphanDir)
	require.NoError(t, orphanLockErr, "joined drainer must release its orphan flock")
	require.NoError(t, orphanLock.close())
	require.True(t, engine.engineCloseCompleted(), "the earlier close phases must still complete")
}

func TestQwpSenderClosePhaseOrderIsExplicit(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, nil,
		func(context.Context, int) (*qwpTransport, error) {
			return nil, errors.New("unexpected reconnect")
		}, time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)
	loop.sendLoopStart()
	sender.drainerPool = qwpSfNewDrainerPool(1)

	order := make([]string, 0, 4)
	drainHook := func() { order = append(order, "drain") }
	loopHook := func() { order = append(order, "send-loop") }
	engineHook := func(point qwpSfCleanupTestPoint) {
		if point == qwpSfCleanupTestAfterQuiescence {
			order = append(order, "engine")
		}
	}
	drainerHook := func() { order = append(order, "drainer-pool") }
	qwpTestCloseDrainHook.Store(&drainHook)
	qwpTestCloseSendLoopHook.Store(&loopHook)
	qwpSfTestCleanupHook.Store(&engineHook)
	qwpTestDrainerPoolCloseHook.Store(&drainerHook)
	t.Cleanup(func() {
		qwpTestCloseDrainHook.Store(nil)
		qwpTestCloseSendLoopHook.Store(nil)
		qwpSfTestCleanupHook.Store(nil)
		qwpTestDrainerPoolCloseHook.Store(nil)
	})

	require.NoError(t, sender.Close(context.Background()))
	require.Equal(t, []string{"drain", "send-loop", "engine", "drainer-pool"}, order)
	select {
	case <-loop.done:
	default:
		t.Fatal("send-loop phase returned without joining its goroutine")
	}
}

// A drainer must save its engine as soon as it opens it. If the next setup step
// panics, cleanup must still report failure and keep the slot locked rather
// than lose track of the engine or falsely report that its resources are freed.
func TestQwpOrphanDrainerOwnsEngineBeforePostOpenFault(t *testing.T) {
	dir, child := terminalDrainerTestDir(t)
	if !child {
		return
	}
	seed, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = seed.engineAppendBlocking(context.Background(), []byte("data"))
	require.NoError(t, err)
	require.NoError(t, seed.engineClose())
	var drainerEngine *qwpSfCursorEngine
	openedHook := func(engine *qwpSfCursorEngine) {
		drainerEngine = engine
		panic("injected orphan post-open panic")
	}
	qwpSfTestAfterDrainerEngineOpenHook.Store(&openedHook)
	t.Cleanup(func() {
		qwpSfTestAfterDrainerEngineOpenHook.Store(nil)
	})

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		func(context.Context, int) (*qwpTransport, error) {
			return nil, errors.New("unexpected dial")
		}, nil,
		time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	require.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	require.NotNil(t, drainerEngine)
	waitQwpSfEngineCleanup(t, drainerEngine)
	require.False(t, drainerEngine.engineCloseCompleted())
	require.ErrorIs(t, drainer.cleanupResult(), ErrCleanupFailed)
	drainer, drainerEngine = nil, nil
	assertTerminalDrainerRetained(t, dir)
}

func TestQwpSenderForegroundCloseRetriesDrainedFileCleanup(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, nil,
		func(context.Context, int) (*qwpTransport, error) {
			return nil, errors.New("unexpected reconnect")
		}, time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)

	<-engine.manager.segmentManagerStop()
	var syncCalls atomic.Int32
	injected := errors.New("injected directory sync failure")
	syncHook := func(string) error {
		if syncCalls.Add(1) == 1 {
			return injected
		}
		return nil
	}
	qwpSfTestDirSyncHook.Store(&syncHook)
	t.Cleanup(func() {
		qwpSfTestDirSyncHook.Store(nil)
		waitQwpSfEngineCleanup(t, engine)
	})

	err = sender.Close(context.Background())
	require.ErrorIs(t, err, injected)
	waitQwpSfEngineCleanup(t, engine)
	require.True(t, engine.engineCloseCompleted(), "ordinary storage failure must be retried")
	require.GreaterOrEqual(t, syncCalls.Load(), int32(2))

	_, err = os.Stat(filepath.Join(dir, "sf-initial.sfa"))
	require.True(t, os.IsNotExist(err), "retry must remove the residual segment")
	_, err = os.Stat(filepath.Join(dir, qwpSfManifestFileName))
	require.True(t, os.IsNotExist(err), "retry must remove the residual manifest")
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpEngineCleanupRetriesFlockError(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	calls := atomic.Int32{}
	flockHook := func() error {
		if calls.Add(1) == 1 {
			return errors.New("injected first release failure")
		}
		return nil
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&flockHook)
	t.Cleanup(func() { qwpSfTestBeforeFlockReleaseHook.Store(nil) })

	require.Error(t, engine.engineClose())
	// The same cleanup worker retries without another call to Close.
	require.Eventually(t, engine.engineCloseCompleted, qwpTestWaitTimeout, 10*time.Millisecond)
	require.GreaterOrEqual(t, calls.Load(), int32(2))
}

func TestQwpEngineCleanupRetrySurvivesPanickingLogger(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)

	flockCalls := atomic.Int32{}
	flockHook := func() error {
		if flockCalls.Add(1) <= 2 {
			return errors.New("injected release failure")
		}
		return nil
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&flockHook)
	originalInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(10 * time.Millisecond)
	t.Cleanup(func() {
		qwpSfTestBeforeFlockReleaseHook.Store(nil)
		qwpSfCloseRetryInterval.store(originalInterval)
	})

	require.Error(t, engine.engineClose())
	engine.manager.logger.Store(qwpGuardLogger(slog.New(panicOnHandleSlog{})))
	require.Eventually(t, engine.engineCloseCompleted, qwpTestWaitTimeout, 5*time.Millisecond)
	require.GreaterOrEqual(t, flockCalls.Load(), int32(3))

	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpSfCloseRetryLogThrottle(t *testing.T) {
	now := time.Unix(1_000, 0)
	require.True(t, qwpSfShouldLogCloseRetry(time.Time{}, now))
	require.False(t, qwpSfShouldLogCloseRetry(now, now.Add(qwpSfCloseRetryLogThrottle-time.Nanosecond)))
	require.True(t, qwpSfShouldLogCloseRetry(now, now.Add(qwpSfCloseRetryLogThrottle)))
}

// TestQwpEngineCloseRetainsSlotUntilManagerWorkerExits reproduces the close
// race at the real worker I/O boundary. A manager blocked in spare creation
// must retain every worker-reachable resource and the flock; its exit path
// performs terminal cleanup after the operation is released.
func TestQwpEngineCloseRetainsSlotUntilManagerWorkerExits(t *testing.T) {
	dir := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("manager did not enter spare creation")
	}

	require.NoError(t, engine.engineClose())
	require.False(t, engine.engineCloseCompleted())
	// Another Close call must not start a second cleanup worker or release
	// the slot while the first worker is still using it.
	require.NoError(t, engine.engineClose())
	_, err = qwpSfAcquireSlotLock(dir)
	require.ErrorIs(t, err, qwpSfErrLockBusy)
	_, err = os.Stat(filepath.Join(dir, "sf-initial.sfa"))
	require.NoError(t, err, "close must not remove segment files while the worker can touch the slot")

	close(release)
	require.Eventually(t, engine.engineCloseCompleted, qwpTestWaitTimeout, time.Millisecond)
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
	// A retry after deferred cleanup converges without a second teardown.
	require.NoError(t, engine.engineClose())
}

func TestQwpEngineNonFirstCloseComputesDrainStateBeforeCleanup(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.engineClose() })
	segmentPath := filepath.Join(dir, "sf-initial.sfa")
	manifestPath := filepath.Join(dir, qwpSfManifestFileName)
	require.FileExists(t, segmentPath)
	require.FileExists(t, manifestPath)

	// Model the exact concurrent-close window: another goroutine won the
	// closed CAS, then was preempted before appendMu. This non-first caller is
	// therefore the first one to reach the serialized teardown section.
	engine.closed.Store(true)
	require.NoError(t, engine.engineClose())
	require.True(t, engine.engineCloseCompleted())

	_, err = os.Stat(segmentPath)
	require.True(t, os.IsNotExist(err), "drained close must remove the segment")
	_, err = os.Stat(manifestPath)
	require.True(t, os.IsNotExist(err), "drained close must remove the manifest")
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpEngineDoubleCloseDuringUnlinkRunsOneCleanup(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	var finishCalls atomic.Int32
	finishHook := func() { finishCalls.Add(1) }
	qwpSfTestEngineFinishCloseHook.Store(&finishHook)
	unlinkHook := func(path string) {
		if filepath.Base(path) != "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	qwpSfTestBeforeSegmentUnlinkHook.Store(&unlinkHook)
	t.Cleanup(func() {
		qwpSfTestBeforeSegmentUnlinkHook.Store(nil)
		qwpSfTestEngineFinishCloseHook.Store(nil)
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() { done <- engine.engineClose() }()
	select {
	case <-entered:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("close did not reach segment unlink")
	}
	require.NoError(t, engine.engineClose(), "concurrent close must only observe the existing owner")
	close(release)
	require.NoError(t, <-done)
	waitQwpSfEngineCleanup(t, engine)
	require.True(t, engine.engineCloseCompleted())
	require.Equal(t, int32(1), finishCalls.Load(), "terminal cleanup ran more than once")
}

// TestQwpSfSegmentCloseUnmaps pins the normal close still unmaps and nils buf.
func TestQwpSfSegmentCloseUnmaps(t *testing.T) {
	dir := t.TempDir()
	seg, err := qwpSfCreateSegment(filepath.Join(dir, "s.sfa"), 0, 4096)
	require.NoError(t, err)
	require.NoError(t, seg.close())
	require.Nil(t, seg.buf)
}

// TestQwpEngineSurfacesManagerWorkerPanic pins that a latched segment-manager
// worker panic is surfaced to producers as a terminal via engineTerminalError,
// rather than leaving them to stall in backpressure forever with no signal.
func TestQwpEngineSurfacesManagerWorkerPanic(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	require.NoError(t, engine.engineTerminalError(), "healthy manager: no terminal")

	detail := errors.New("qwp/sf: segment manager worker stopped: boom\nstack")
	engine.manager.workerErr.Store(&detail)

	got := engine.engineTerminalError()
	require.Error(t, got)
	require.Contains(t, got.Error(), "segment manager worker stopped")
}

// Stopping a loop requests transport teardown but does not pretend a reader
// has exited. The owned join completes only after the reader is released.
func TestQwpSendLoopCloseWaitsForReaderExit(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	loop.wg.Add(1)
	var release sync.Once
	t.Cleanup(func() { release.Do(loop.wg.Done) })
	closed := make(chan error, 1)
	go func() { closed <- loop.sendLoopClose() }()
	select {
	case <-loop.ctx.Done():
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("stop was not requested")
	}
	select {
	case <-closed:
		t.Fatal("close finished before reader exit")
	case <-time.After(30 * time.Millisecond):
	}
	release.Do(loop.wg.Done)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("close did not finish after reader exit")
	}
	require.Nil(t, loop.transport.Load())
}

// TestQwpSenderConstructionPanicReleasesSlotLock checks that a construction
// panic starts engine cleanup. Construction can open files, map memory, and
// take the slot lock before returning a sender. On failure, cleanup cannot
// depend on the caller closing a sender it never received.
func TestQwpSenderConstructionPanicReleasesSlotLock(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	sfDir := t.TempDir()
	slot := filepath.Join(sfDir, "boom")

	boom := func() error { panic("construction boom") }
	qwpSfTestAfterEngineCreateHook.Store(&boom)
	t.Cleanup(func() { qwpSfTestAfterEngineCreateHook.Store(nil) })

	conf := strings.Join([]string{
		"ws::addr=" + strings.TrimPrefix(srv.URL, "http://"),
		"sf_dir=" + sfDir,
		"sender_id=boom",
		"close_flush_timeout_millis=50;",
	}, ";")
	func() {
		defer func() { require.NotNil(t, recover(), "the injected fault must unwind") }()
		_, _ = LineSenderFromConf(context.Background(), conf)
	}()
	qwpSfTestAfterEngineCreateHook.Store(nil)

	require.Eventually(t, func() bool {
		lock, err := qwpSfAcquireSlotLock(slot)
		if err != nil {
			return false
		}
		require.NoError(t, lock.close())
		return true
	}, 5*time.Second, 10*time.Millisecond,
		"a construction fault must not strand the slot flock")
}

// TestQwpSenderConstructionPanicClosesTheSendLoop pins that a fault after the
// send loop is built also releases it. By then the loop owns a bound WebSocket
// and three dispatcher goroutines; closing only the engine leaves the socket
// fd, the server-side connection and those goroutines alive for the process
// lifetime.
func TestQwpSenderConstructionPanicClosesTheSendLoop(t *testing.T) {
	sfDir := t.TempDir()

	// The server's read loop returns when the client's socket closes, so a
	// leaked transport keeps this handler alive.
	var handlersLive atomic.Int64
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		handlersLive.Add(1)
		defer handlersLive.Add(-1)
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer srv2.Close()

	// Fault in the window where the loop exists but the sender does not.
	boom := func() { panic("post-loop boom") }
	qwpTestAfterSendLoopBuiltHook.Store(&boom)
	t.Cleanup(func() { qwpTestAfterSendLoopBuiltHook.Store(nil) })

	conf := strings.Join([]string{
		"ws::addr=" + strings.TrimPrefix(srv2.URL, "http://"),
		"sf_dir=" + sfDir,
		"sender_id=loop-boom",
		"close_flush_timeout_millis=50;",
	}, ";")
	func() {
		defer func() { require.NotNil(t, recover(), "the injected fault must unwind") }()
		_, _ = LineSenderFromConf(context.Background(), conf)
	}()
	qwpTestAfterSendLoopBuiltHook.Store(nil)

	require.Eventually(t, func() bool {
		return handlersLive.Load() == 0
	}, 5*time.Second, 20*time.Millisecond,
		"the send loop's WebSocket must not outlive a failed construction")
}

// TestQwpSenderConcurrentCloseKeepsSegmentsMappedUnderLiveSendLoop pins the
// rule that a repeated public Close cannot take ownership of resource cleanup.
//
// While the first Close is still draining, the send loop is still reading the
// segment mappings, and terminal cleanup unmaps them. The send loop works from
// slice headers it took earlier, so the lengths still look right, the bounds
// checks pass, and the reads land on memory the process has given back. The
// biggest window is the payload slice passed to the WebSocket write, which
// faults inside memmove for the length of a whole frame. The fault arrives as a
// runtime throw, which the send loop's recover cannot catch.
//
// Only a disk-backed slot can fault. A memory-backed segment skips the unmap
// and just drops its buffer reference, which comes out as a panic the send loop
// recovers. That is why this uses a temporary directory.
//
// The check is on the mapping rather than on the fault, because the fault kills
// the test binary instead of failing the test.
func TestQwpSenderConcurrentCloseKeepsSegmentsMappedUnderLiveSendLoop(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 5*time.Second)
	require.NoError(t, err)

	// Hold the first Close inside the drain phase. It gets there after winning
	// the closed CAS, and before it stops the send loop or touches the engine.
	// That is the window a second caller must not act in.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hook := func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	qwpTestCloseDrainHook.Store(&hook)
	t.Cleanup(func() { qwpTestCloseDrainHook.Store(nil) })

	firstDone := make(chan error, 1)
	go func() { firstDone <- sender.Close(context.Background()) }()
	<-entered
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		<-firstDone
	})

	active := engine.engineActiveSegment()
	require.NotNil(t, active)
	require.NotEmpty(t, active.address(), "precondition: the active segment is mapped")

	secondErr := sender.Close(context.Background())

	require.NotEmpty(t, active.address(),
		"a second Close unmapped the active segment while the send loop was still reading it")
	require.False(t, engine.engineCloseCompleted(),
		"a second Close released the slot lock while the first Close was still draining")
	require.ErrorIs(t, secondErr, errDoubleSenderClose,
		"a second Close arriving before the first returned must report the double close")

}

// TestQwpSenderSlotLockReleasedTracksTheSlot checks the accessor a caller polls
// to learn when the slot directory is free. Close on its own cannot answer that
// question: it returns as soon as its own teardown is done, which can be before
// the lock is gone.
func TestQwpSenderSlotLockReleasedTracksTheSlot(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 5*time.Second)
	require.NoError(t, err)

	require.False(t, sender.SlotLockReleased(), "an open sender is still using its slot")

	// Fail every release while this is set, so Close returns with the lock still
	// held. That is the state the accessor exists for. A switch rather than a
	// call count, because the hook is package-wide and retry owners left running
	// by earlier tests can reach it too; blocking them for a moment is harmless,
	// whereas letting one consume a single scheduled failure is not.
	var failing atomic.Bool
	failing.Store(true)
	hook := func() error {
		if failing.Load() {
			return os.ErrPermission
		}
		return nil
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestBeforeFlockReleaseHook.Store(nil) })

	// Keep the retry owner brisk so the wait below is short, and put the
	// package value back for whatever runs next.
	previousInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(5 * time.Millisecond)
	t.Cleanup(func() { qwpSfCloseRetryInterval.store(previousInterval) })

	require.Error(t, sender.Close(context.Background()))
	require.False(t, sender.SlotLockReleased(),
		"Close returned while the slot lock was still held, and the accessor must say so")

	// The retry owner finishes the release once the fault clears.
	failing.Store(false)
	require.Eventually(t, sender.SlotLockReleased, 10*time.Second, time.Millisecond,
		"the accessor must turn true once the background retry releases the lock")
	lock, lockErr := qwpSfAcquireSlotLock(engine.engineSfDir())
	require.NoError(t, lockErr, "the slot must be acquirable once the accessor reports released")
	require.NoError(t, lock.close())
}

// TestQwpSenderSlotLockReleasedIsTrueInMemoryMode pins the other half: with no
// sf_dir there is no slot, so the answer is true throughout.
func TestQwpSenderSlotLockReleasedIsTrueInMemoryMode(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	sender, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.True(t, sender.SlotLockReleased(), "memory mode holds no slot")
	require.NoError(t, sender.Close(context.Background()))
	require.True(t, sender.SlotLockReleased())
}
