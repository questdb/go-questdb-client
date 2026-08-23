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
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func waitQwpSfCloseRetryOwner(t *testing.T, engine *qwpSfCursorEngine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		engine.closeRetryWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal cleanup retry owner did not exit")
	}
}

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

func TestQwpSenderForegroundCloseStartsTerminalCleanupRetryOwner(t *testing.T) {
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
	require.Eventually(t, engine.engineCloseCompleted, time.Second, 5*time.Millisecond,
		"foreground Close must leave an owner retrying terminal cleanup")
	require.GreaterOrEqual(t, calls.Load(), int32(2))

	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

// TestQwpSenderEnginePreClaimPanicStillClosesDrainerPoolAndRetries pins the
// owning close stack: once the foreground sender owns an orphan-drainer pool,
// an engine fault before manager quiescence is published must not bypass pool
// cancellation. The same fault leaves cleanup at open, so Close must also
// install the standalone retry owner that re-drives manager teardown and
// eventually releases the foreground slot flock.
func TestQwpSenderEnginePreClaimPanicStillClosesDrainerPoolAndRetries(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, nil,
		func(context.Context, int) (*qwpTransport, error) {
			return nil, errors.New("unexpected reconnect")
		}, time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)
	pool := qwpSfNewDrainerPool(1)
	sender.drainerPool = pool

	oldInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(10 * time.Millisecond)
	var injected atomic.Bool
	hook := func(point qwpSfCleanupTestPoint) {
		if point == qwpSfCleanupTestBeforeManagerStop && injected.CompareAndSwap(false, true) {
			panic("injected engine pre-claim panic")
		}
	}
	qwpSfTestCleanupHook.Store(&hook)
	t.Cleanup(func() {
		qwpSfTestCleanupHook.Store(nil)
		qwpSfCloseRetryInterval.store(oldInterval)
		pool.drainerPoolClose()
		_ = engine.engineClose()
	})

	var (
		closeErr   error
		closePanic any
	)
	func() {
		defer func() { closePanic = recover() }()
		closeErr = sender.Close(context.Background())
	}()
	require.Nil(t, closePanic, "an engine-close panic must be converted at its phase boundary")
	require.ErrorContains(t, closeErr, "engine close panicked")
	select {
	case <-pool.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("engine-close panic skipped drainer-pool cancellation")
	}
	require.Eventually(t, engine.engineCloseCompleted, time.Second, 5*time.Millisecond,
		"engine pre-claim panic must leave one owner re-driving cleanup")
	waitQwpSfCloseRetryOwner(t, engine)

	lock, lockErr := qwpSfAcquireSlotLock(dir)
	require.NoError(t, lockErr, "the retry owner must eventually release the foreground slot flock")
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
	case <-time.After(time.Second):
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
	require.ErrorContains(t, closeErr, "drainer pool close panicked")
	select {
	case <-pool.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("a drainer-pool panic skipped its installed cancellation")
	}
	drainersDone := make(chan struct{})
	go func() {
		pool.wg.Wait()
		close(drainersDone)
	}()
	select {
	case <-drainersDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled orphan drainer did not exit after the pool-close panic")
	}
	require.Empty(t, pool.drainerPoolSnapshot())
	require.Equal(t, qwpSfDrainOutcomeStopped, drainer.drainerOutcome())
	orphanLock, orphanLockErr := qwpSfAcquireSlotLock(orphanDir)
	require.NoError(t, orphanLockErr, "joined drainer must release its orphan flock")
	require.NoError(t, orphanLock.close())
	require.True(t, engine.engineCloseCompleted(), "the earlier close phases must still complete")
}

func TestQwpPooledEnginePreClaimPanicRetiresUntilRetryCompletes(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, nil,
		func(context.Context, int) (*qwpTransport, error) {
			return nil, errors.New("unexpected reconnect")
		}, time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)
	slot := &qwpSenderSlot{delegate: sender, cleanup: sender, slotIndex: 0}
	p := &qwpSenderPool{
		notify:          make(chan struct{}),
		maxSize:         1,
		acquireTimeout:  time.Second,
		storeAndForward: true,
		sfSlots:         []qwpSfSlotLifecycle{{state: qwpSfSlotAvailable}},
		all:             []*qwpSenderSlot{slot},
		available:       []*qwpSenderSlot{slot},
	}

	oldInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(time.Millisecond)
	enteredRetry := make(chan struct{})
	releaseRetry := make(chan struct{})
	var calls atomic.Int32
	hook := func(point qwpSfCleanupTestPoint) {
		if point != qwpSfCleanupTestBeforeManagerStop {
			return
		}
		switch calls.Add(1) {
		case 1:
			panic("injected pooled engine pre-claim panic")
		case 2:
			close(enteredRetry)
			<-releaseRetry
		}
	}
	qwpSfTestCleanupHook.Store(&hook)
	t.Cleanup(func() {
		qwpSfTestCleanupHook.Store(nil)
		qwpSfCloseRetryInterval.store(oldInterval)
		select {
		case <-releaseRetry:
		default:
			close(releaseRetry)
		}
	})

	closeErr := p.close(context.Background())
	require.ErrorContains(t, closeErr, "engine close panicked")
	require.ErrorIs(t, closeErr, ErrSfCleanupPending)
	select {
	case <-enteredRetry:
	case <-time.After(time.Second):
		t.Fatal("pooled engine retry owner did not re-drive manager cleanup")
	}
	p.mu.Lock()
	require.Equal(t, qwpSfSlotRetired, p.sfSlots[0].state,
		"the pool must reserve the slot index while retry owns its flock")
	p.mu.Unlock()

	close(releaseRetry)
	require.Eventually(t, engine.engineCloseCompleted, time.Second, 5*time.Millisecond)
	waitQwpSfCloseRetryOwner(t, engine)
	finalErr := p.currentCloseResult()
	require.NotErrorIs(t, finalErr, ErrSfCleanupPending)
	require.ErrorContains(t, finalErr, "engine close panicked",
		"the stable first-pass diagnostic must survive after the live obligation clears")
	p.mu.Lock()
	require.Equal(t, qwpSfSlotFree, p.sfSlots[0].state)
	p.mu.Unlock()
	lock, lockErr := qwpSfAcquireSlotLock(dir)
	require.NoError(t, lockErr)
	require.NoError(t, lock.close())
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
		if point == qwpSfCleanupTestBeforeManagerStop {
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

// TestQwpOrphanDrainerEnginePreClaimPanicStartsRetryOwner covers the second
// real owner of an SF engine. A drainer close fault before manager quiescence
// must be contained by the engine phase, must not condemn sound slot bytes,
// and must leave a retry owner that eventually releases the orphan flock.
func TestQwpOrphanDrainerEnginePreClaimPanicStartsRetryOwner(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	oldInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(10 * time.Millisecond)
	var injected atomic.Bool
	hook := func(point qwpSfCleanupTestPoint) {
		if point == qwpSfCleanupTestBeforeManagerStop && injected.CompareAndSwap(false, true) {
			panic("injected orphan engine pre-claim panic")
		}
	}
	qwpSfTestCleanupHook.Store(&hook)
	var drainerEngine *qwpSfCursorEngine
	openedHook := func(engine *qwpSfCursorEngine) { drainerEngine = engine }
	qwpSfTestAfterDrainerEngineOpenHook.Store(&openedHook)
	t.Cleanup(func() {
		qwpSfTestCleanupHook.Store(nil)
		qwpSfTestAfterDrainerEngineOpenHook.Store(nil)
		qwpSfCloseRetryInterval.store(oldInterval)
	})

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv), nil,
		time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.drainerRun(context.Background())
	require.Equal(t, qwpSfDrainOutcomeSuccess, drainer.drainerOutcome(),
		"a close-path panic must not quarantine an otherwise successful drain")
	require.NoFileExists(t, filepath.Join(dir, qwpSfFailedSentinelName))
	require.NotNil(t, drainerEngine)
	require.Eventually(t, drainerEngine.engineCloseCompleted, time.Second, 5*time.Millisecond,
		"the orphan engine retry owner must complete")
	waitQwpSfCloseRetryOwner(t, drainerEngine)
	lock, lockErr := qwpSfAcquireSlotLock(dir)
	require.NoError(t, lockErr, "the orphan engine retry owner must release its slot flock")
	require.NoError(t, lock.close())
}

// TestQwpOrphanDrainerOwnsEngineBeforePostOpenFault proves the ownership
// obligation is installed at the acquisition boundary, before logger setup or
// any other faultable post-open work can strand the manager and slot flock.
func TestQwpOrphanDrainerOwnsEngineBeforePostOpenFault(t *testing.T) {
	dir := t.TempDir()
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
	require.True(t, drainerEngine.engineCloseCompleted(),
		"the post-open panic must unwind through the installed engine owner")
	waitQwpSfCloseRetryOwner(t, drainerEngine)
	lock, lockErr := qwpSfAcquireSlotLock(dir)
	require.NoError(t, lockErr, "the post-open panic must not strand the orphan flock")
	require.NoError(t, lock.close())
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

	unlinkCalls := atomic.Int32{}
	unlinkHook := func(string) {
		if unlinkCalls.Add(1) == 1 {
			panic("injected first segment unlink failure")
		}
	}
	qwpSfTestBeforeSegmentUnlinkHook.Store(&unlinkHook)
	t.Cleanup(func() {
		qwpSfTestBeforeSegmentUnlinkHook.Store(nil)
		_ = engine.engineClose()
	})

	err = sender.Close(context.Background())
	require.ErrorContains(t, err, "terminal cleanup panicked")
	require.Eventually(t, engine.engineCloseCompleted, time.Second, 5*time.Millisecond,
		"foreground Close must retry partially completed drained-file cleanup")
	require.GreaterOrEqual(t, unlinkCalls.Load(), int32(2))

	_, err = os.Stat(filepath.Join(dir, "sf-initial.sfa"))
	require.True(t, os.IsNotExist(err), "retry must remove the residual segment")
	_, err = os.Stat(filepath.Join(dir, qwpSfManifestFileName))
	require.True(t, os.IsNotExist(err), "retry must remove the residual manifest")
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
	// Every hook is restored by a t.Cleanup registered before the next
	// assertion. An assertion between the two calls Goexit on failure, which
	// would leave a fault-injecting hook installed for every later test in the
	// package and fail them for a reason none of them can see.
	qwpSfTestBeforeFlockReleaseHook.Store(&flockHook)
	t.Cleanup(func() { qwpSfTestBeforeFlockReleaseHook.Store(nil) })
	require.Error(t, engine.engineClose())

	finishCalls := atomic.Int32{}
	finishHook := func() {
		if finishCalls.Add(1) == 1 {
			panic("injected terminal cleanup panic")
		}
	}
	qwpSfTestEngineFinishCloseHook.Store(&finishHook)
	t.Cleanup(func() { qwpSfTestEngineFinishCloseHook.Store(nil) })
	originalInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(10 * time.Millisecond)
	t.Cleanup(func() { qwpSfCloseRetryInterval.store(originalInterval) })

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
	originalInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(10 * time.Millisecond)
	t.Cleanup(func() {
		qwpSfTestBeforeFlockReleaseHook.Store(nil)
		qwpSfCloseRetryInterval.store(originalInterval)
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
	case <-time.After(time.Second):
		t.Fatal("manager did not enter spare creation")
	}

	require.NoError(t, engine.engineClose())
	require.False(t, engine.engineCloseCompleted())
	// Duplicate close while the owned-manager handoff is pending must be an
	// idempotent observation, not a second registration or a panic.
	require.NoError(t, engine.engineClose())
	_, err = qwpSfAcquireSlotLock(dir)
	require.ErrorIs(t, err, qwpSfErrLockBusy)
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

// A second Close decides whether cleanup is ownerless from two markers:
// managerTornDown and deferredCleanupOwned. Between them lies the narrowest
// window in the close path, and a claim taken there releases the slot lock
// while the manager worker is still writing in the slot directory. The hook
// below stands in for that second Close, at the exact instant the teardown
// marker is published.
func TestQwpEngineCleanupCannotBeClaimedWhileTheWorkerRuns(t *testing.T) {
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
	case <-time.After(time.Second):
		t.Fatal("manager did not enter spare creation")
	}

	var rivalClaimed atomic.Bool
	teardownHook := func() { rivalClaimed.Store(engine.engineTryClaimTerminalCleanup()) }
	qwpSfTestAfterManagerTeardownHook.Store(&teardownHook)
	t.Cleanup(func() { qwpSfTestAfterManagerTeardownHook.Store(nil) })

	require.NoError(t, engine.engineClose())
	require.False(t, rivalClaimed.Load(),
		"cleanup must never look ownerless while the manager worker is still running")
	require.False(t, engine.engineCloseCompleted())

	close(release)
	require.Eventually(t, engine.engineCloseCompleted, time.Second, time.Millisecond)
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
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
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	oldInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(10 * time.Millisecond)
	t.Cleanup(func() {
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfTestEngineFinishCloseHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
		qwpSfCloseRetryInterval.store(oldInterval)
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
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
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

	old := qwpSfSendLoopCloseGrace.load()
	qwpSfSendLoopCloseGrace.store(20 * time.Millisecond)
	defer func() { qwpSfSendLoopCloseGrace.store(old) }()

	require.NoError(t, loop.sendLoopClose())
	require.True(t, loop.sendLoopAbandoned(), "wedged join must abandon")
	require.Nil(t, loop.transport.Load(), "transport released on abandon")

	loop.wg.Done() // let the internal join goroutine finish
}

// TestQwpCloseDrainPanicLeaksMappingsAndStopsSendLoop pins the contract that
// decides between unmapping the segment ring and leaking it. A fault in the
// drain phase must still stop the send loop, so the I/O goroutine is provably
// done with the mappings and the WebSocket is released; a fault in the
// send-loop stop leaves that unproven, so the teardown must leak the mappings
// rather than unmap them under a goroutine that may still be dereferencing
// them. Unmapping there faults the host process, which no recover can catch.
func TestQwpCloseDrainPanicLeaksMappingsAndStopsSendLoop(t *testing.T) {
	newSender := func(t *testing.T, sfDir string) (*qwpLineSender, func()) {
		t.Helper()
		srv := newQwpTestServer(t)
		conf := strings.Join([]string{
			"ws::addr=" + strings.TrimPrefix(srv.URL, "http://"),
			"sf_dir=" + sfDir,
			"sender_id=drain-panic",
			"close_flush_timeout_millis=100;",
		}, ";")
		ls, err := LineSenderFromConf(context.Background(), conf)
		require.NoError(t, err)
		require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
		return ls.(*qwpLineSender), srv.Close
	}

	t.Run("drain panic stops the loop and unmaps", func(t *testing.T) {
		sfDir := t.TempDir()
		s, stopSrv := newSender(t, sfDir)
		defer stopSrv()

		boom := func() { panic("drain boom") }
		qwpTestCloseDrainHook.Store(&boom)
		t.Cleanup(func() { qwpTestCloseDrainHook.Store(nil) })

		err := s.Close(context.Background())
		require.ErrorContains(t, err, "close drain panicked")

		// The send loop is stopped, so leaking the mappings is unnecessary.
		require.False(t, s.cursorSendLoop.running.Load(),
			"a drain fault must not leave the I/O goroutine running")
		require.True(t, s.cursorEngine.engineCloseCompleted(),
			"the teardown must still complete and release the slot lock")
		lock, lockErr := qwpSfAcquireSlotLock(filepath.Join(sfDir, "drain-panic"))
		require.NoError(t, lockErr, "the slot flock must be released")
		require.NoError(t, lock.close())
	})

	t.Run("send-loop stop panic leaks the mappings", func(t *testing.T) {
		sfDir := t.TempDir()
		s, stopSrv := newSender(t, sfDir)
		defer stopSrv()
		seg := s.cursorEngine.engineActiveSegment()
		require.NotNil(t, seg)

		boom := func() { panic("send loop close boom") }
		qwpTestCloseSendLoopHook.Store(&boom)
		t.Cleanup(func() { qwpTestCloseSendLoopHook.Store(nil) })

		err := s.Close(context.Background())
		require.ErrorContains(t, err, "send loop close panicked")

		// The I/O goroutine was never joined, so every segment stays mapped.
		require.True(t, s.cursorEngine.cleanup.snapshot().leakMappings,
			"an unjoined send loop must leave the segment mappings in place")
		select {
		case <-s.cursorSendLoop.done:
		case <-time.After(time.Second):
			t.Fatal("the panic boundary left the cancelled send-loop goroutine running")
		}
		require.True(t, s.cursorEngine.engineCloseCompleted(),
			"the rest of the teardown still runs and releases the slot lock")
		lock, lockErr := qwpSfAcquireSlotLock(filepath.Join(sfDir, "drain-panic"))
		require.NoError(t, lockErr, "the slot flock must be released")
		require.NoError(t, lock.close())
		require.NotNil(t, seg.buf, "the conservative close must retain the mapping until the loop joins")
		require.NoError(t, qwpSfMunmap(seg.buf))
	})
}

// TestQwpSenderConstructionPanicReleasesSlotLock pins that a fault during
// sender construction hands the engine to a cleanup owner. The engine takes the
// slot flock, the segment mappings and the side-file descriptors before the
// sender exists, and a panic between the two returns no slot at all to the
// pool's recover — so nothing downstream can install a retry owner, and the
// flock would be held until the process exits while close() reported success.
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

func TestQwpSenderConstructionPanicKeepsCauseAndSurvivesCleanupPanic(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	sfDir := t.TempDir()
	slot := filepath.Join(sfDir, "build-cleanup-boom")

	buildHook := func() error { panic("original construction panic") }
	qwpSfTestAfterEngineCreateHook.Store(&buildHook)
	var cleanupInjected atomic.Bool
	cleanupHook := func(point qwpSfCleanupTestPoint) {
		if point == qwpSfCleanupTestBeforeManagerStop && cleanupInjected.CompareAndSwap(false, true) {
			panic("construction cleanup panic")
		}
	}
	qwpSfTestCleanupHook.Store(&cleanupHook)
	oldInterval := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(10 * time.Millisecond)
	t.Cleanup(func() {
		qwpSfTestAfterEngineCreateHook.Store(nil)
		qwpSfTestCleanupHook.Store(nil)
		qwpSfCloseRetryInterval.store(oldInterval)
	})

	conf := strings.Join([]string{
		"ws::addr=" + strings.TrimPrefix(srv.URL, "http://"),
		"sf_dir=" + sfDir,
		"sender_id=build-cleanup-boom",
		"close_flush_timeout_millis=50;",
	}, ";")
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = LineSenderFromConf(context.Background(), conf)
	}()
	bp, ok := recovered.(qwpSfBuildPanic)
	require.True(t, ok, "disk construction panic must carry its cleanup reporter, got %T", recovered)
	detail := bp.String()
	require.Contains(t, detail, "original construction panic")
	require.Contains(t, detail, "engine close panicked")
	require.Less(t, strings.Index(detail, "original construction panic"), strings.Index(detail, "engine close panicked"),
		"the original construction cause must remain first")
	reporter, ok := bp.reporter.(*qwpSfBuildCleanupError)
	require.True(t, ok, "pre-sender cleanup should report the retained engine")
	require.Eventually(t, reporter.engine.engineCloseCompleted, time.Second, 5*time.Millisecond)
	waitQwpSfCloseRetryOwner(t, reporter.engine)
	lock, lockErr := qwpSfAcquireSlotLock(slot)
	require.NoError(t, lockErr)
	require.NoError(t, lock.close())
}

func TestQwpEngineConstructionCleanupContinuesAfterPhasePanic(t *testing.T) {
	buildErr := errors.New("injected construction failure")
	tests := []struct {
		name   string
		target qwpSfConstructionCleanupPoint
		fresh  bool
		order  []qwpSfConstructionCleanupPoint
	}{
		{
			name:   "ring",
			target: qwpSfConstructionCleanupRing,
			order: []qwpSfConstructionCleanupPoint{
				qwpSfConstructionCleanupRing,
				qwpSfConstructionCleanupWatermark,
				qwpSfConstructionCleanupSymbolDict,
				qwpSfConstructionCleanupSlotLock,
				qwpSfConstructionCleanupManager,
			},
		},
		{
			name:   "watermark",
			target: qwpSfConstructionCleanupWatermark,
			order: []qwpSfConstructionCleanupPoint{
				qwpSfConstructionCleanupRing,
				qwpSfConstructionCleanupWatermark,
				qwpSfConstructionCleanupSymbolDict,
				qwpSfConstructionCleanupSlotLock,
				qwpSfConstructionCleanupManager,
			},
		},
		{
			name:   "symbol-dict",
			target: qwpSfConstructionCleanupSymbolDict,
			order: []qwpSfConstructionCleanupPoint{
				qwpSfConstructionCleanupRing,
				qwpSfConstructionCleanupWatermark,
				qwpSfConstructionCleanupSymbolDict,
				qwpSfConstructionCleanupSlotLock,
				qwpSfConstructionCleanupManager,
			},
		},
		{
			name:   "initial",
			target: qwpSfConstructionCleanupInitial,
			fresh:  true,
			order: []qwpSfConstructionCleanupPoint{
				qwpSfConstructionCleanupInitial,
				qwpSfConstructionCleanupManifest,
				qwpSfConstructionCleanupWatermark,
				qwpSfConstructionCleanupSymbolDict,
				qwpSfConstructionCleanupSlotLock,
				qwpSfConstructionCleanupManager,
			},
		},
		{
			name:   "manifest",
			target: qwpSfConstructionCleanupManifest,
			fresh:  true,
			order: []qwpSfConstructionCleanupPoint{
				qwpSfConstructionCleanupInitial,
				qwpSfConstructionCleanupManifest,
				qwpSfConstructionCleanupWatermark,
				qwpSfConstructionCleanupSymbolDict,
				qwpSfConstructionCleanupSlotLock,
				qwpSfConstructionCleanupManager,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fail := func() error { return buildErr }
			if tc.fresh {
				qwpSfTestBeforeFreshRingAdoptHook.Store(&fail)
				t.Cleanup(func() { qwpSfTestBeforeFreshRingAdoptHook.Store(nil) })
			} else {
				qwpSfTestBeforeEngineRegisterHook.Store(&fail)
				t.Cleanup(func() { qwpSfTestBeforeEngineRegisterHook.Store(nil) })
			}

			seen := make([]qwpSfConstructionCleanupPoint, 0, len(tc.order))
			cleanupHook := func(point qwpSfConstructionCleanupPoint) {
				seen = append(seen, point)
				if point == tc.target {
					panic("injected construction cleanup panic")
				}
			}
			qwpSfTestConstructionCleanupHook.Store(&cleanupHook)
			t.Cleanup(func() { qwpSfTestConstructionCleanupHook.Store(nil) })

			_, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.ErrorIs(t, err, buildErr)
			require.ErrorContains(t, err, "cleanup panicked")
			require.Equal(t, tc.order, seen,
				"a cleanup phase panic must not skip any later installed obligation")

			lock, lockErr := qwpSfAcquireSlotLock(dir)
			require.NoError(t, lockErr, "constructor unwind must release the slot flock")
			require.NoError(t, lock.close())
		})
	}
}

// TestQwpEngineTerminalPhasePanicRetriesToCompletion pins the retry contract
// for a fully drained close. The durability barriers are checkpointed before
// the resource-close phases run, so a panic in one phase leaves a retryable
// generation whose next pass skips the barriers, re-attempts each idempotent
// close, removes the drained files, and releases the slot flock. The
// watermark and symbol-dict cases fault after this pass has closed side files
// the barriers write through, which is exactly the state a retry must survive.
func TestQwpEngineTerminalPhasePanicRetriesToCompletion(t *testing.T) {
	for _, tc := range []struct {
		name  string
		point qwpSfCleanupTestPoint
	}{
		{"ring", qwpSfCleanupTestRingClosePhase},
		{"watermark", qwpSfCleanupTestWatermarkClosePhase},
		{"symbol-dict", qwpSfCleanupTestSymbolDictClosePhase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.NoError(t, err)
			oldInterval := qwpSfCloseRetryInterval.load()
			qwpSfCloseRetryInterval.store(10 * time.Millisecond)
			var injected atomic.Bool
			hook := func(point qwpSfCleanupTestPoint) {
				if point == tc.point && injected.CompareAndSwap(false, true) {
					panic("injected terminal phase panic")
				}
			}
			qwpSfTestCleanupHook.Store(&hook)
			t.Cleanup(func() {
				qwpSfTestCleanupHook.Store(nil)
				qwpSfCloseRetryInterval.store(oldInterval)
			})

			closeErr := closeEngineGuarded(engine, false, nil)
			require.ErrorContains(t, closeErr, "cleanup panicked")
			require.False(t, engine.engineCloseCompleted(),
				"the faulted pass must leave a retryable generation, not report completion")
			require.Eventually(t, engine.engineCloseCompleted, time.Second, 5*time.Millisecond,
				"retry after a terminal-phase panic must complete once the fault clears")
			waitQwpSfCloseRetryOwner(t, engine)
			lock, lockErr := qwpSfAcquireSlotLock(dir)
			require.NoError(t, lockErr, "the completed retry must release the slot flock")
			require.NoError(t, lock.close())
		})
	}
}

// TestQwpSenderRepeatedCloseRedrivesAnAbortedClose pins the escape hatch
// LineSender.Close documents. A close that faults before the manager teardown
// leaves no terminal cleanup to claim and, on a standalone sender, no pool
// guard to install a retry owner — so the slot lock is held and a repeated
// Close is the caller's only way to get it back.
func TestQwpSenderRepeatedCloseRedrivesAnAbortedClose(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	sfDir := t.TempDir()
	conf := strings.Join([]string{
		"ws::addr=" + strings.TrimPrefix(srv.URL, "http://"),
		"sf_dir=" + sfDir,
		"sender_id=abort",
		"close_flush_timeout_millis=50;",
	}, ";")
	ls, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	s := ls.(*qwpLineSender)
	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))

	// The state a close leaves behind when it faults inside the manager
	// teardown: the engine is marked closed, nothing is in flight, and the
	// teardown marker was never published — so there is no claim to take and
	// no retry owner was installed.
	require.NoError(t, s.cursorSendLoop.sendLoopClose())
	s.cursorEngine.closed.Store(true)
	s.closed.Store(true)
	require.False(t, cleanupManagerTornDown(s.cursorEngine))
	require.False(t, s.cursorEngine.cleanup.snapshot().retryOwnerStarted)

	require.True(t, s.cursorEngine.engineCloseNeedsRedrive(),
		"an aborted close must be recognised as needing a re-drive")
	require.NoError(t, ls.Close(context.Background()),
		"a repeated Close must finish the cleanup, not report a double close")
	require.True(t, s.cursorEngine.engineCloseCompleted())

	lock, lockErr := qwpSfAcquireSlotLock(filepath.Join(sfDir, "abort"))
	require.NoError(t, lockErr, "the slot flock must be released")
	require.NoError(t, lock.close())
}
