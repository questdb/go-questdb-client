/******************************************************************************
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
 *****************************************************************************/

package questdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This test sender uses real memory-mapped files and a slot lock. When hold is
// true, Close returns without releasing them. The test can then cause a panic
// while the pool updates the slot's state.
type poolFaultSender struct {
	QwpSender
	engine      *qwpSfCursorEngine
	hold        bool
	panicReturn bool
	closes      atomic.Int32
}

func (s *poolFaultSender) Close(context.Context) error {
	s.closes.Add(1)
	if s.hold {
		return nil
	}
	return s.engine.engineClose()
}
func (s *poolFaultSender) closeCompleted() bool { return s.engine.engineCloseCompleted() }
func (s *poolFaultSender) flushForReturn(context.Context) (bool, error) {
	if s.panicReturn {
		panic("injected return-flush failure")
	}
	return false, nil
}

// Run in a child process because the failed slots must stay locked until that
// process exits. The test must not retry or repair their failed cleanup.
func TestQwpSenderPoolFailureRetainsResources(t *testing.T) {
	if mode := os.Getenv("QWP_POOL_FAILURE_CHILD"); mode != "" {
		dir := os.Getenv("QWP_POOL_FAILURE_DIR")
		buffers, indices := poolFailureChild(t, dir, mode)
		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
		}
		for _, index := range indices {
			lock, err := qwpSfAcquireSlotLock(filepath.Join(dir, fmt.Sprintf("slot-%d", index)))
			if lock != nil {
				_ = lock.close()
			}
			require.ErrorIs(t, err, qwpSfErrLockBusy, "GC released an uncertain slot")
		}
		for _, buf := range buffers {
			require.NotZero(t, buf[0], "retained mapping is not readable")
		}
		runtime.KeepAlive(buffers)
		return
	}
	for _, mode := range []string{"close-transition", "reap-apply", "reclaim", "late-return", "late-build", "return-flush"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQwpSenderPoolFailureRetainsResources$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "QWP_POOL_FAILURE_CHILD="+mode, "QWP_POOL_FAILURE_DIR="+dir)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
			for i := 0; i < 4; i++ {
				path := filepath.Join(dir, fmt.Sprintf("slot-%d", i))
				lock, err := qwpSfAcquireSlotLock(path)
				require.NoError(t, err, "process exit must release slot %d", i)
				require.NoError(t, lock.close())
				e, err := qwpSfNewCursorEngineForDrainer(path, 4096, qwpSfUnlimitedTotalBytes, time.Second)
				require.NoError(t, err)
				require.Equal(t, int64(0), e.enginePublishedFsn(), "published frame lost from slot %d", i)
				require.Equal(t, int64(-1), e.engineAckedFsn())
				require.NoError(t, e.engineClose())
				waitQwpSfEngineCleanup(t, e)
			}
		})
	}
}

func poolFailureChild(t *testing.T, dir, mode string) ([][]byte, []int) {
	t.Helper()
	p := &qwpSenderPool{storeAndForward: true, maxSize: 4, notify: make(chan struct{}), acquireTimeout: time.Millisecond, idleTimeout: time.Hour, sfSlots: make([]qwpSfSlotLifecycle, 4)}
	senders := make([]*poolFaultSender, 4)
	slots := make([]*qwpSenderSlot, 4)
	for i := range slots {
		e, err := qwpSfNewCursorEngine(filepath.Join(dir, fmt.Sprintf("slot-%d", i)), 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = e.engineAppendBlocking(context.Background(), make([]byte, 128))
		require.NoError(t, err)
		<-e.manager.segmentManagerStop()
		senders[i] = &poolFaultSender{engine: e, hold: mode == "reclaim" && i == 1}
		slots[i] = &qwpSenderSlot{delegate: senders[i], cleanup: senders[i], slotIndex: i, idleSince: time.Now()}
		p.sfSlots[i].state = qwpSfSlotAvailable
	}
	p.all = append([]*qwpSenderSlot(nil), slots...)
	p.available = append([]*qwpSenderSlot(nil), slots[:3]...)
	p.sfSlots[3].state = qwpSfSlotLeased
	lease := &qwpPooledSender{pool: p, slot: slots[3], gen: slots[3].generation.Add(1), broken: true}
	if mode == "late-build" {
		p.all = p.all[:3]
		p.sfSlots[3].state = qwpSfSlotCreating
		p.inFlightCreations = 1
	}
	if mode == "return-flush" {
		lease.broken = false
		senders[3].panicReturn = true
	}
	var hits atomic.Int32
	var earlierMoved atomic.Bool
	hook := func(pool *qwpSenderPool, index int, from, to qwpSfSlotState) {
		if pool != p {
			return
		}
		fail := index == 1 && to == qwpSfSlotClosing
		switch mode {
		case "reclaim":
			fail = index == 1 && from == qwpSfSlotClosing && to == qwpSfSlotRetired
		case "late-return":
			fail = index == 3 && from == qwpSfSlotLeased
		case "late-build":
			fail = index == 3 && from == qwpSfSlotCreating
		case "return-flush":
			fail = false
		}
		if fail {
			if mode == "reclaim" {
				earlierMoved.Store(p.sfSlots[0].state == qwpSfSlotFree)
			} else {
				earlierMoved.Store(p.sfSlots[0].state == qwpSfSlotClosing)
			}
			hits.Add(1)
			panic("injected pool transition failure")
		}
	}
	qwpTestPoolTransitionHook.Store(&hook)
	t.Cleanup(func() { qwpTestPoolTransitionHook.Store(nil) })
	retained := []int{1}
	if mode == "reap-apply" {
		slots[0].idleSince = time.Now().Add(-2 * time.Hour)
		slots[1].idleSince = slots[0].idleSince
		require.Empty(t, p.selectReapVictims(time.Now()))
		require.Equal(t, qwpSfSlotClosing, p.sfSlots[0].state, "first slot must move before the fault")
		retained = []int{0, 1}
	}
	if mode == "late-return" || mode == "late-build" || mode == "return-flush" {
		require.ErrorIs(t, p.close(context.Background()), ErrSfCleanupPending)
		if mode != "late-build" {
			require.ErrorIs(t, lease.Close(context.Background()), ErrCleanupFailed)
		} else {
			_, err := p.settleBuiltSlot(slots[3], 3, nil, true)
			require.ErrorIs(t, err, ErrCleanupFailed)
		}
		retained = []int{3}
	} else {
		require.ErrorIs(t, p.close(context.Background()), ErrCleanupFailed)
	}
	waitQwpCleanupSignal(t, p.closeDone, "failed pool close attempt")
	// Removing the injected fault must not make cleanup start again.
	qwpTestPoolTransitionHook.Store(nil)
	if mode == "return-flush" {
		require.Zero(t, hits.Load())
	} else {
		require.Equal(t, int32(1), hits.Load())
	}
	if mode == "close-transition" || mode == "reap-apply" || mode == "reclaim" {
		require.True(t, earlierMoved.Load(), "fault must follow an earlier slot transition")
	}
	if mode != "late-return" && mode != "late-build" && mode != "return-flush" {
		require.Zero(t, senders[3].closes.Load(), "pool must not close a borrowed sender")
		require.ErrorIs(t, lease.Close(context.Background()), ErrPoolPoisoned)
		require.Eventually(t, senders[3].engine.engineCloseCompleted, time.Second, time.Millisecond)
	}
	// Call QuestDB.Close itself to check that concurrent calls keep reporting
	// the failure, including calls after the first shutdown attempt.
	db := &QuestDB{senderPool: p, queryPool: &qwpQueryPool{notify: make(chan struct{})}}
	db.housekeeper = newQwpPoolHousekeeper(p, db.queryPool, 0, time.Millisecond)
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results <- db.Close(ctx)
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.ErrorIs(t, err, ErrCleanupFailed)
		require.ErrorIs(t, err, ErrPoolPoisoned)
	}
	_, err := p.borrow(context.Background())
	require.ErrorIs(t, err, ErrPoolPoisoned)
	require.True(t, p.mu.TryLock(), "failure stranded pool mutex")
	require.NotNil(t, p.poisonedErr)
	require.GreaterOrEqual(t, p.capUsedLocked(), len(retained))
	p.mu.Unlock()
	for i, s := range senders {
		held := false
		for _, index := range retained {
			held = held || i == index
		}
		if !held {
			require.True(t, s.engine.engineCloseCompleted(), "healthy sibling %d was not released", i)
		}
	}
	var buffers [][]byte
	for _, index := range retained {
		buffers = append(buffers, senders[index].engine.ring.getActiveSegment().buf)
	}
	return buffers, retained
}

type poolBlockedLog struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (w *poolBlockedLog) Write(b []byte) (int, error) {
	close(w.entered)
	<-w.release
	close(w.done)
	return len(b), nil
}

func TestQwpSenderPoolFailureDoesNotWaitForBlockedSibling(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var closes atomic.Int32
	log := &poolBlockedLog{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	p := &qwpSenderPool{maxSize: 2, notify: make(chan struct{}), logger: slog.New(slog.NewTextHandler(log, nil))}
	t.Cleanup(func() { close(log.release); waitQwpCleanupSignal(t, log.done, "failure logger") })
	p.all = []*qwpSenderSlot{
		{slotIndex: -1, delegate: resultPoolCloseSender{panic: "close panic", closed: &closes}},
		{slotIndex: -1, delegate: blockingPoolCloseSender{entered: entered, release: release}},
	}
	p.available = append([]*qwpSenderSlot(nil), p.all...)
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		if p.closeDone != nil {
			waitQwpCleanupSignal(t, p.closeDone, "unblocked sibling close")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, p.close(ctx), ErrCleanupFailed)
	waitQwpCleanupSignal(t, log.entered, "blocked failure logger")
	waitQwpCleanupSignal(t, entered, "blocked sibling close")
	select {
	case <-p.closeDone:
		t.Fatal("close attempt finished before sibling release")
	default:
	}
	require.ErrorIs(t, p.close(ctx), ErrCleanupFailed)
	once.Do(func() { close(release) })
	waitQwpCleanupSignal(t, p.closeDone, "pool close attempt")
	require.Equal(t, int32(1), closes.Load())
	require.ErrorIs(t, p.close(ctx), ErrCleanupFailed)
}

func TestQwpSenderPoolCloseWaitIsCallerBounded(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	slot := &qwpSenderSlot{slotIndex: -1, delegate: blockingPoolCloseSender{entered: entered, release: release}}
	p := &qwpSenderPool{maxSize: 1, notify: make(chan struct{}), all: []*qwpSenderSlot{slot}, available: []*qwpSenderSlot{slot}}
	t.Cleanup(func() { once.Do(func() { close(release) }); waitQwpCleanupSignal(t, p.closeDone, "pool close worker") })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.close(ctx)
	require.ErrorIs(t, err, ErrCleanupPending)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrSfCleanupPending)
	waitQwpCleanupSignal(t, entered, "close started despite cancelled caller")
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	err = p.close(deadline)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, ErrCleanupPending)
	once.Do(func() { close(release) })
	waitQwpCleanupSignal(t, p.closeDone, "pool close worker")
	require.NoError(t, p.close(ctx), "completed result wins over cancellation")
}

type poolLateCloseSender struct {
	QwpSender
	entered, release chan struct{}
	err              error
}

func (s poolLateCloseSender) Close(context.Context) error {
	close(s.entered)
	<-s.release
	return s.err
}

func TestQwpSenderPoolLateReturnTransfersCleanupAndPreservesError(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	injected := errors.New("late return close failed")
	slot := &qwpSenderSlot{slotIndex: -1, delegate: poolLateCloseSender{entered: entered, release: release, err: injected}}
	p := &qwpSenderPool{maxSize: 1, notify: make(chan struct{}), all: []*qwpSenderSlot{slot}}
	lease := &qwpPooledSender{pool: p, slot: slot, gen: slot.generation.Add(1), broken: true}
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		require.Eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.pendingLeaseTeardowns == 0 }, time.Second, time.Millisecond)
	})
	require.ErrorIs(t, p.close(context.Background()), ErrCleanupPending)
	returned := make(chan error, 1)
	go func() { returned <- lease.Close(context.Background()) }()
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("lease return waited for physical cleanup")
	}
	waitQwpCleanupSignal(t, entered, "late return cleanup")
	require.ErrorIs(t, p.close(context.Background()), ErrCleanupPending)
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool { return errors.Is(p.stableCloseResult(), injected) }, time.Second, time.Millisecond)
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, p.close(context.Background()), injected)
	}
	require.NoError(t, lease.Close(context.Background()), "repeated lease Close is not a cleanup retry")
}

// Simulate shutdown finishing just after currentCloseResult reads the old
// result. This lets us test a cancelled caller without relying on timing.
// The pool holds its mutex when it calls cleanupFailure.
type poolCompleteDuringSnapshot struct {
	neverDoneSlot
	once    sync.Once
	publish func()
}

func (s *poolCompleteDuringSnapshot) cleanupFailure() error {
	s.once.Do(s.publish)
	return nil
}

func TestQwpSenderPoolCompletionDuringCancelledSnapshot(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("initially-pending=%t", pending), func(t *testing.T) {
			p := &qwpSenderPool{}
			done := make(chan struct{})
			var finalErr error
			if pending {
				p.pendingLeaseTeardowns = 1
			} else {
				finalErr = errors.New("completed close failed")
			}
			probe := &poolCompleteDuringSnapshot{publish: func() {
				p.pendingLeaseTeardowns = 0
				p.closeTeardownErr = finalErr
				close(done)
			}}
			p.retiredSlots = []*qwpSenderSlot{{slotIndex: -1, cleanup: probe}}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := p.cleanupWaitResult(ctx, done, make(chan struct{}))
			if finalErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, finalErr)
			}
			require.NotErrorIs(t, err, context.Canceled)
		})
	}
}

type poolFaultingCloseError struct{ checks *atomic.Int32 }

func (e poolFaultingCloseError) Error() string { return "close result" }
func (e poolFaultingCloseError) Unwrap() error {
	e.checks.Add(1)
	panic("error inspection panic")
}

func TestQwpSenderPoolContainsFailureAfterDelegateClose(t *testing.T) {
	var closes, checks atomic.Int32
	slot := &qwpSenderSlot{slotIndex: -1, delegate: resultPoolCloseSender{closed: &closes, err: poolFaultingCloseError{checks: &checks}}}
	p := &qwpSenderPool{notify: make(chan struct{}), maxSize: 1, all: []*qwpSenderSlot{slot}, available: []*qwpSenderSlot{slot}}
	require.ErrorIs(t, p.close(context.Background()), ErrCleanupFailed)
	waitQwpCleanupSignal(t, p.closeDone, "post-close failure")
	for i := 0; i < 3; i++ {
		err := p.close(context.Background())
		require.ErrorIs(t, err, ErrPoolPoisoned)
		require.ErrorContains(t, err, "error inspection panic")
	}
	require.Equal(t, int32(1), closes.Load())
	require.Equal(t, int32(1), checks.Load(), "do not retry the failed result check")
	require.True(t, p.mu.TryLock())
	p.mu.Unlock()
}

func TestQwpSenderPoolPartialReprobeNeverRepairs(t *testing.T) {
	p := &qwpSenderPool{storeAndForward: true, maxSize: 2, notify: make(chan struct{}), sfSlots: []qwpSfSlotLifecycle{{state: qwpSfSlotRetired}, {state: qwpSfSlotRetired}}}
	p.retiredSlots = []*qwpSenderSlot{{slotIndex: 0}, {slotIndex: 1}}
	var hits int
	hook := func(pool *qwpSenderPool, index int, from, to qwpSfSlotState) {
		if pool == p && index == 1 && to == qwpSfSlotFree {
			hits++
			panic("partial reprobe")
		}
	}
	qwpTestPoolTransitionHook.Store(&hook)
	defer qwpTestPoolTransitionHook.Store(nil)
	p.reprobeRetiredSlots()
	require.Equal(t, qwpSfSlotFree, p.sfSlots[0].state)
	require.Equal(t, qwpSfSlotRetired, p.sfSlots[1].state)
	qwpTestPoolTransitionHook.Store(nil)
	for i := 0; i < 4; i++ {
		require.ErrorIs(t, p.currentCloseResult(), ErrCleanupFailed)
	}
	require.Equal(t, 1, hits)
	require.Equal(t, qwpSfSlotRetired, p.sfSlots[1].state)
	require.Len(t, p.failedSlots, 2)
	require.True(t, p.mu.TryLock())
	p.mu.Unlock()
}
