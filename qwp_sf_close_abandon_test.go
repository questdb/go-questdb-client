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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQwpSfSegmentCloseLeakMappingKeepsBuf pins that closeInternal(true) leaves
// a disk-backed segment's mmap mapped and its buf reference intact, so a
// goroutine wedged mid-dereference keeps a valid address instead of faulting.
func TestQwpSfSegmentCloseLeakMappingKeepsBuf(t *testing.T) {
	dir := t.TempDir()
	seg, err := qwpSfCreateSegment(filepath.Join(dir, "s.sfa"), 0, 4096)
	require.NoError(t, err)
	require.False(t, seg.memoryBacked)
	require.NotNil(t, seg.address())

	require.NoError(t, seg.closeInternal(true))
	require.NotNil(t, seg.buf, "leaked mapping keeps buf")
	_ = seg.address()[0] // must not fault

	// Release the deliberately-leaked mapping so the test does not leak it.
	require.NoError(t, qwpSfMunmap(seg.buf))
}

func TestQwpSenderRepeatedCloseRetriesFlockRelease(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	injected := errors.New("injected flock release failure")
	calls := 0
	flockHook := func() error {
		calls++
		if calls == 1 {
			return injected
		}
		return nil
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&flockHook)
	t.Cleanup(func() { qwpSfTestBeforeFlockReleaseHook.Store(nil) })

	require.ErrorIs(t, engine.engineClose(), injected)
	require.False(t, engine.engineCloseCompleted())
	_, err = qwpSfAcquireSlotLock(dir)
	require.Error(t, err, "failed release must retain the flock")

	// Model the public sender after its first Close has already run: the repeat
	// call must reach ownerless engine cleanup instead of returning the ordinary
	// double-close error.
	sender := &qwpLineSender{cursorEngine: engine}
	sender.closed.Store(true)
	require.NoError(t, sender.Close(context.Background()))
	require.True(t, engine.engineCloseCompleted())
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpEngineTerminalRetryOwnerCompletesFailedDeferredCleanup(t *testing.T) {
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
	engine.engineStartCloseRetryOwner(nil)
	require.Eventually(t, engine.engineCloseCompleted, 2*time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, calls.Load(), int32(2))
}

func TestQwpEngineTerminalRetryOwnerRecoversPanicAndCompletes(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)

	flockCalls := atomic.Int32{}
	flockHook := func() error {
		if flockCalls.Add(1) == 1 {
			return errors.New("injected first release failure")
		}
		return nil
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&flockHook)
	require.Error(t, engine.engineClose())

	finishCalls := atomic.Int32{}
	finishHook := func() {
		if finishCalls.Add(1) == 1 {
			panic("injected terminal cleanup panic")
		}
	}
	qwpSfTestEngineFinishCloseHook.Store(&finishHook)
	originalInterval := qwpSfCloseRetryInterval
	qwpSfCloseRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		qwpSfTestBeforeFlockReleaseHook.Store(nil)
		qwpSfTestEngineFinishCloseHook.Store(nil)
		qwpSfCloseRetryInterval = originalInterval
	})

	engine.engineStartCloseRetryOwner(nil)
	require.Eventually(t, engine.engineCloseCompleted, time.Second, 5*time.Millisecond)
	require.GreaterOrEqual(t, finishCalls.Load(), int32(2))
	require.GreaterOrEqual(t, flockCalls.Load(), int32(2))

	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpEngineTerminalRetryOwnerSurvivesPanickingLogger(t *testing.T) {
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
	originalInterval := qwpSfCloseRetryInterval
	qwpSfCloseRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		qwpSfTestBeforeFlockReleaseHook.Store(nil)
		qwpSfCloseRetryInterval = originalInterval
	})

	require.Error(t, engine.engineClose())
	engine.engineStartCloseRetryOwner(slog.New(panicOnHandleSlog{}))
	require.Eventually(t, engine.engineCloseCompleted, time.Second, 5*time.Millisecond)
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
	oldGrace := qwpSfManagerCloseGrace
	qwpSfManagerCloseGrace = 20 * time.Millisecond
	t.Cleanup(func() {
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace = oldGrace
	})

	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("manager did not enter spare creation")
	}

	require.NoError(t, engine.engineClose())
	require.False(t, engine.engineCloseCompleted())
	// Duplicate close while the owned-manager handoff is pending must be an
	// idempotent observation, not a second registration or a panic.
	require.NoError(t, engine.engineClose())
	_, err = qwpSfAcquireSlotLock(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "slot already in use")
	_, err = os.Stat(filepath.Join(dir, "sf-initial.sfa"))
	require.NoError(t, err, "close must not remove segment files while the worker can touch the slot")

	close(release)
	require.Eventually(t, engine.engineCloseCompleted, time.Second, time.Millisecond)
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
	// A retry after deferred cleanup converges without a second teardown.
	require.NoError(t, engine.engineClose())
}

func TestQwpEngineDeferredCleanupPanicTransfersToRetryOwner(t *testing.T) {
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
	finishCalls := atomic.Int32{}
	finishHook := func() {
		if finishCalls.Add(1) == 1 {
			panic("injected deferred cleanup panic")
		}
	}
	qwpSfTestEngineFinishCloseHook.Store(&finishHook)
	oldGrace := qwpSfManagerCloseGrace
	qwpSfManagerCloseGrace = 20 * time.Millisecond
	oldInterval := qwpSfCloseRetryInterval
	qwpSfCloseRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfTestEngineFinishCloseHook.Store(nil)
		qwpSfManagerCloseGrace = oldGrace
		qwpSfCloseRetryInterval = oldInterval
		select {
		case <-release:
		default:
			close(release)
		}
	})

	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("manager did not enter spare creation")
	}

	require.NoError(t, engine.engineClose())
	require.False(t, engine.engineCloseCompleted())
	close(release)
	require.Eventually(t, engine.engineCloseCompleted, time.Second, time.Millisecond)
	require.GreaterOrEqual(t, finishCalls.Load(), int32(2))

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
	case <-time.After(time.Second):
		t.Fatal("close did not reach segment unlink")
	}
	require.NoError(t, engine.engineClose(), "concurrent close must observe claimed cleanup")
	close(release)
	require.NoError(t, <-done)
	require.True(t, engine.engineCloseCompleted())
	require.Equal(t, int32(1), finishCalls.Load(), "terminal cleanup ran more than once")
}

// TestQwpSharedManagerDefersOnlyTheBusyRing pins the test-only shared-manager
// path: one ring hands cleanup to its current service pass while the manager
// remains alive to service its sibling.
func TestQwpSharedManagerDefersOnlyTheBusyRing(t *testing.T) {
	const segSize int64 = 4096
	mgr, err := qwpSfNewSegmentManager(segSize, 100*time.Microsecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	defer mgr.segmentManagerClose()

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
	oldGrace := qwpSfManagerCloseGrace
	qwpSfManagerCloseGrace = 20 * time.Millisecond
	t.Cleanup(func() {
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace = oldGrace
	})

	busy, err := qwpSfNewCursorEngineWithManager(dir, segSize, mgr, time.Second)
	require.NoError(t, err)
	sibling, err := qwpSfNewCursorEngineWithManager("", segSize, mgr, time.Second)
	require.NoError(t, err)
	defer func() { _ = sibling.engineClose() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("manager did not enter busy ring service")
	}

	require.NoError(t, busy.engineClose())
	require.False(t, busy.engineCloseCompleted())
	close(release)
	require.Eventually(t, busy.engineCloseCompleted, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return !sibling.ring.needsHotSpare() }, time.Second, time.Millisecond)
}

// TestQwpSfSegmentCloseUnmaps pins the normal close still unmaps and nils buf.
func TestQwpSfSegmentCloseUnmaps(t *testing.T) {
	dir := t.TempDir()
	seg, err := qwpSfCreateSegment(filepath.Join(dir, "s.sfa"), 0, 4096)
	require.NoError(t, err)
	require.NoError(t, seg.close())
	require.Nil(t, seg.buf)
}

// TestQwpEngineCloseLeakSegmentsKeepsMappings pins that engineCloseLeakSegments
// tears the engine down (fds, watermark, slot lock) but leaves the segment
// mmaps valid for a still-live wedged send-loop goroutine.
func TestQwpEngineCloseLeakSegmentsKeepsMappings(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)
	seg := engine.engineActiveSegment()
	require.NotNil(t, seg)
	require.False(t, seg.memoryBacked)

	require.NoError(t, engine.engineCloseLeakSegments())
	require.NotNil(t, seg.buf, "leaked mapping stays valid after engine close")
	_ = seg.address()[0] // must not fault
	require.NoError(t, qwpSfMunmap(seg.buf))
}

// TestQwpEngineSurfacesManagerWorkerPanic pins that a latched segment-manager
// worker panic is surfaced to producers as a terminal via engineTerminalError,
// rather than leaving them to stall in backpressure forever with no signal.
func TestQwpEngineSurfacesManagerWorkerPanic(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	require.NoError(t, engine.engineTerminalError(), "healthy manager: no terminal")

	detail := "boom\nstack"
	engine.manager.workerPanic.Store(&detail)

	got := engine.engineTerminalError()
	require.Error(t, got)
	require.Contains(t, got.Error(), "segment manager worker stopped")
}

// A panicked worker has exited its ring loop, so engine close must clean up
// inline rather than treating the five-second join timeout as uncertainty.
func TestQwpEngineCloseAfterManagerWorkerPanicReleasesSlot(t *testing.T) {
	dir := t.TempDir()
	createHook := func(path string) {
		if filepath.Base(path) != "sf-initial.sfa" {
			panic("injected spare-create panic")
		}
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	t.Cleanup(func() { qwpSfTestSegmentCreateHook.Store(nil) })
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	select {
	case <-engine.manager.done:
	case <-time.After(time.Second):
		t.Fatal("manager worker did not exit after injected panic")
	}
	require.Error(t, engine.engineTerminalError())
	require.NoError(t, engine.engineClose())
	require.True(t, engine.engineCloseCompleted())
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

// TestQwpSendLoopCloseAbandonSignalsAndReleasesTransport pins that a send loop
// whose I/O goroutine never joins abandons after the grace, flags itself so the
// engine teardown leaks the mappings, and still releases the WebSocket.
func TestQwpSendLoopCloseAbandonSignalsAndReleasesTransport(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	// Simulate a wedged I/O goroutine: a wg count that is never matched by a
	// Done, so the join never completes and sendLoopClose must abandon.
	loop.wg.Add(1)

	old := qwpSfSendLoopCloseGrace
	qwpSfSendLoopCloseGrace = 20 * time.Millisecond
	defer func() { qwpSfSendLoopCloseGrace = old }()

	require.NoError(t, loop.sendLoopClose())
	require.True(t, loop.sendLoopAbandoned(), "wedged join must abandon")
	require.Nil(t, loop.transport.Load(), "transport released on abandon")

	loop.wg.Done() // let the internal join goroutine finish
}
