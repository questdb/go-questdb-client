// Copyright (c) 2014-2019 Appsicle
// Copyright (c) 2019-2026 QuestDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package questdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func terminalDrainerTestDir(t *testing.T) (string, bool) {
	t.Helper()
	if os.Getenv("QWP_D1_TERMINAL_CHILD") == t.Name() {
		return os.Getenv("QWP_D1_TERMINAL_DIR"), true
	}
	dir := t.TempDir()
	cmd := qwpTestSubprocess(t, t.Name())
	cmd.Env = append(os.Environ(), "QWP_D1_TERMINAL_CHILD="+t.Name(), "QWP_D1_TERMINAL_DIR="+dir)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err, "process exit must release retained flock")
	require.NoError(t, lock.close())
	return dir, false
}

func assertTerminalDrainerRetained(t *testing.T, dir string) {
	t.Helper()
	for i := 0; i < 3; i++ {
		runtime.GC()
	}
	lock, err := qwpSfAcquireSlotLock(dir)
	if lock != nil {
		_ = lock.close()
	}
	require.ErrorIs(t, err, qwpSfErrLockBusy)
	_, err = os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	require.ErrorIs(t, err, os.ErrNotExist, "cleanup failure is not corruption evidence")
}

func cancelledCleanupWait() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func cleanupTestFacade(sp *qwpSenderPool, qp *qwpQueryPool) *QuestDB {
	if sp == nil {
		sp = &qwpSenderPool{notify: make(chan struct{})}
	}
	if qp == nil {
		qp = &qwpQueryPool{notify: make(chan struct{})}
	}
	return &QuestDB{senderPool: sp, queryPool: qp, housekeeper: newQwpPoolHousekeeper(sp, qp, 0)}
}

func TestQwpPoolHarvestsLateCompletedEngineError(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	<-e.manager.segmentManagerStop()
	release := make(chan struct{})
	finish := func() { <-release }
	injected := errors.New("late completed cleanup error")
	fileClose := func(f *os.File) error {
		if filepath.Base(f.Name()) == qwpSfManifestFileName {
			return injected
		}
		return nil
	}
	old := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(5 * time.Millisecond)
	qwpSfTestEngineFinishCloseHook.Store(&finish)
	qwpSfTestAfterFileCloseHook.Store(&fileClose)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = e.engineClose()
		waitQwpSfEngineCleanup(t, e)
		qwpSfTestEngineFinishCloseHook.Store(nil)
		qwpSfTestAfterFileCloseHook.Store(nil)
		qwpSfManagerCloseGrace.store(old)
	})
	require.NoError(t, e.engineClose())
	p := closedSfPoolWithRetiredSlot(&qwpSfBuildCleanupError{engine: e})
	require.ErrorIs(t, p.currentCloseResult(), ErrSfCleanupPending)
	close(release)
	waitQwpSfEngineCleanup(t, e)
	require.ErrorIs(t, p.currentCloseResult(), injected)
	require.ErrorIs(t, p.currentCloseResult(), injected, "removing the retired slot must not lose its saved error")
}

type lateCleanupReporter struct {
	flippableDoneSlot
	err error
}

func (r *lateCleanupReporter) cleanupResult() error {
	if r.done.Load() {
		return r.err
	}
	return nil
}

func TestQwpMemoryPoolRetainsUnfinishedCleanup(t *testing.T) {
	injected := errors.New("memory cleanup completed with error")
	r := &lateCleanupReporter{err: injected}
	slot := &qwpSenderSlot{slotIndex: -1, cleanup: r}
	p := &qwpSenderPool{closed: true}
	p.mu.Lock()
	p.reclaimSlotLocked(slot, nil)
	p.mu.Unlock()
	err := p.currentCloseResult()
	require.ErrorIs(t, err, ErrCleanupPending)
	require.NotErrorIs(t, err, ErrSfCleanupPending)
	r.done.Store(true)
	require.ErrorIs(t, p.currentCloseResult(), injected)
	require.ErrorIs(t, p.currentCloseResult(), injected)
}

func TestQwpPoolClearsRecoveredStorageError(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	<-e.manager.segmentManagerStop()
	var fail atomic.Bool
	fail.Store(true)
	injected := errors.New("temporary unmap failure")
	hook := func([]byte) error {
		if fail.Load() {
			return injected
		}
		return nil
	}
	qwpSfTestMunmapHook.Store(&hook)
	old := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(time.Millisecond)
	t.Cleanup(func() {
		fail.Store(false)
		_ = e.engineClose()
		waitQwpSfEngineCleanup(t, e)
		qwpSfTestMunmapHook.Store(nil)
		qwpSfCloseRetryInterval.store(old)
	})
	s := &qwpLineSender{cursorEngine: e}
	slot := &qwpSenderSlot{slotIndex: 0, delegate: s, cleanup: s}
	p := &qwpSenderPool{notify: make(chan struct{}), storeAndForward: true, all: []*qwpSenderSlot{slot}, available: []*qwpSenderSlot{slot}, sfSlots: []qwpSfSlotLifecycle{{state: qwpSfSlotAvailable}}}
	require.ErrorIs(t, p.close(context.Background()), injected)
	require.ErrorIs(t, p.currentCloseResult(), ErrSfCleanupPending)
	fail.Store(false)
	waitQwpSfEngineCleanup(t, e)
	require.NoError(t, p.currentCloseResult(), "a successful retry must clear only its recovered cleanup error")
}

func TestQwpQueryAcquisitionFailureRetainsGeneration(t *testing.T) {
	if os.Getenv("QWP_QUERY_ACQUISITION_CHILD") == "" {
		cmd := qwpTestSubprocess(t, "TestQwpQueryAcquisitionFailureRetainsGeneration")
		cmd.Env = append(os.Environ(), "QWP_QUERY_ACQUISITION_CHILD=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return
	}
	peerClosed, finalized := makeRetainedQueryGeneration(t)
	for i := 0; i < 3; i++ {
		runtime.GC()
	}
	select {
	case <-finalized:
		t.Fatal("failed query resource bundle became unreachable")
	case <-peerClosed:
		t.Fatal("uncertain query connection was released after dropping application references")
	case <-time.After(50 * time.Millisecond):
	}
}

// Return only channels so the calling test keeps no reference to the client,
// pool, or connection state. The finalizer tells us if that state is garbage
// collected. It does not close the connection or try to repair failed cleanup.
func makeRetainedQueryGeneration(t *testing.T) (<-chan struct{}, <-chan struct{}) {
	peerClosed, finalized := make(chan struct{}), make(chan struct{})
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) { <-m.conn.CloseRead(context.Background()).Done(); close(peerClosed) })
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	var acquisitions atomic.Int32
	hook := func(*qwpTransport) { acquisitions.Add(1); panic("query acquisition fault") }
	qwpTestQueryAfterTransportAcquired.Store(&hook)
	p, err := newQwpQueryPool(context.Background(), conf, 0, 1, qwpPoolTestUnhurriedAcquire, 0, 0, nil)
	require.NoError(t, err)
	_, err = p.borrow(context.Background())
	require.ErrorIs(t, err, ErrCleanupFailed)
	qwpTestQueryAfterTransportAcquired.Store(nil)
	var build *qwpQueryBuildError
	require.ErrorAs(t, err, &build)
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, p.close(context.Background()), ErrCleanupFailed)
	}
	waitQwpCleanupSignal(t, build.client.closeDone, "query cleanup worker exit")
	g := build.client.generations[0]
	waitQwpCleanupSignal(t, g.closeDone, "failed generation")
	runtime.SetFinalizer(g, func(*qwpConnectResult) { close(finalized) })
	require.EqualValues(t, 1, acquisitions.Load())
	return peerClosed, finalized
}

func TestQwpQueryReturnDoesNotWaitForTransportRelease(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 1, 0)
	q, err := p.borrow(context.Background())
	require.NoError(t, err)
	entered, release, calls := qwpBlockTransportRelease(t, q.worker.client.transport())
	defer release()
	q.broken = true
	returned := make(chan error, 1)
	go func() { returned <- q.Close() }()
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("query lease return waited for physical release")
	}
	waitQwpCleanupSignal(t, entered, "query release")
	require.NoError(t, q.Close(), "returned lease is not a cleanup observer")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = p.close(ctx)
	require.ErrorIs(t, err, ErrCleanupPending)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrSfCleanupPending)
	release()
	require.NoError(t, p.close(context.Background()))
	require.NoError(t, p.close(cancelledCleanupWait()))
	require.EqualValues(t, 1, calls.Load())
}

func TestQwpQueryFailedBuildRetainsDiscardedConnection(t *testing.T) {
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) { <-m.conn.CloseRead(context.Background()).Done() })
	defer srv.Close()
	p, err := newQwpQueryPool(context.Background(), "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";target=replica;", 0, 1, qwpPoolTestUnhurriedAcquire, 0, 0, nil)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func(*qwpTransport) { once.Do(func() { close(entered) }); <-release }
	qwpTestBeforeTransportClose.Store(&hook)
	t.Cleanup(func() { qwpTestBeforeTransportClose.Store(nil) })
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	_, err = p.borrow(context.Background())
	require.Error(t, err)
	waitQwpCleanupSignal(t, entered, "failed-build transport release")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.close(ctx), ErrCleanupPending)
	p.mu.Lock()
	require.Len(t, p.teardowns, 1)
	p.mu.Unlock()
	unblock.Do(func() { close(release) })
	require.NoError(t, p.close(context.Background()))
}

func TestQwpQueryCloseIncludesDiscardedGeneration(t *testing.T) {
	injected := errors.New("old generation close error")
	done := make(chan struct{})
	tr := &qwpTransport{closeDone: done, closeErr: injected, dumpConn: &asyncWritePipeConn{}}
	tr.closeOnce.Do(func() {})
	old := &qwpConnectResult{transport: tr}
	c := &QwpQueryClient{}
	c.keepGeneration(old)
	old.startClose()
	c.keepGeneration(&qwpConnectResult{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.Close(ctx), ErrCleanupPending)
	close(done)
	require.ErrorIs(t, c.Close(context.Background()), injected)
	require.ErrorIs(t, c.Close(cancelledCleanupWait()), injected)
}

func TestQwpQueryPoolFailureDoesNotWaitForSibling(t *testing.T) {
	bad, slow := &QwpQueryClient{}, &QwpQueryClient{}
	p := &qwpQueryPool{notify: make(chan struct{}), all: []*qwpQueryWorker{{client: bad}, {client: slow}}}
	p.available = append(p.available, p.all...)
	entered, release := make(chan struct{}), make(chan struct{})
	hook := func(c *QwpQueryClient) {
		if c == bad {
			panic("query cleanup fault")
		}
		close(entered)
		<-release
	}
	queryClientCloseHook.Store(&hook)
	defer queryClientCloseHook.Store(nil)
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
	defer cancel()
	err := p.close(ctx)
	require.ErrorIs(t, err, ErrCleanupFailed)
	require.ErrorIs(t, err, ErrPoolPoisoned)
	waitQwpCleanupSignal(t, entered, "safe sibling cleanup")
	queryClientCloseHook.Store(nil)
	require.ErrorIs(t, p.close(cancelledCleanupWait()), ErrCleanupFailed)
	_, err = p.borrow(context.Background())
	require.ErrorIs(t, err, ErrPoolPoisoned)
	p.mu.Lock()
	require.Len(t, p.failedWorkers, 1)
	p.mu.Unlock()
}

func TestQwpFacadeKeepsLateSiblingErrorAfterPermanentFailure(t *testing.T) {
	injected := errors.New("safe sibling completed with error")
	done := make(chan struct{})
	close(done)
	tr := &qwpTransport{closeDone: done, closeErr: injected, dumpConn: &asyncWritePipeConn{}}
	tr.closeOnce.Do(func() {})
	bad, slow := &QwpQueryClient{}, &QwpQueryClient{generations: []*qwpConnectResult{{transport: tr}}}
	p := &qwpQueryPool{notify: make(chan struct{}), all: []*qwpQueryWorker{{client: bad}, {client: slow}}}
	p.available = append(p.available, p.all...)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	hook := func(c *QwpQueryClient) {
		if c == bad {
			panic("failed sibling")
		}
		close(entered)
		<-release
	}
	queryClientCloseHook.Store(&hook)
	defer queryClientCloseHook.Store(nil)
	db := cleanupTestFacade(nil, p)
	require.ErrorIs(t, db.Close(context.Background()), ErrCleanupFailed)
	waitQwpCleanupSignal(t, entered, "safe sibling release")
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool { return errors.Is(db.Close(cancelledCleanupWait()), injected) }, qwpTestWaitTimeout, time.Millisecond,
		"an early terminal report must not freeze other cleanup results")
	require.ErrorIs(t, db.Close(cancelledCleanupWait()), ErrCleanupFailed)
}

func TestQwpQueryPoolPartialShutdownDoesNotReplay(t *testing.T) {
	first, second := &qwpQueryWorker{client: &QwpQueryClient{}}, &qwpQueryWorker{client: &QwpQueryClient{}}
	p := &qwpQueryPool{notify: make(chan struct{}), all: []*qwpQueryWorker{first, second}, available: []*qwpQueryWorker{first, second}}
	var closes atomic.Int32
	closeHook := func(c *QwpQueryClient) {
		if c != first.client {
			t.Error("damaged worker was closed again")
		}
		closes.Add(1)
	}
	queryClientCloseHook.Store(&closeHook)
	defer queryClientCloseHook.Store(nil)
	registered := func(w *qwpQueryWorker) {
		if w == second {
			panic("partly changed query pool")
		}
	}
	qwpTestQueryTeardownRegistered.Store(&registered)
	defer qwpTestQueryTeardownRegistered.Store(nil)
	require.ErrorIs(t, p.close(context.Background()), ErrPoolPoisoned)
	qwpTestQueryTeardownRegistered.Store(nil)
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, p.close(cancelledCleanupWait()), ErrCleanupFailed)
	}
	require.Eventually(t, func() bool { return closes.Load() == 1 }, qwpTestWaitTimeout, time.Millisecond)
	require.Eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); _, exists := p.teardowns[first]; return !exists }, qwpTestWaitTimeout, time.Millisecond)
	p.mu.Lock()
	_, held := p.teardowns[second]
	p.mu.Unlock()
	require.True(t, held, "the partial handoff must retain the affected worker")
	require.False(t, second.client.closed.Load(), "removing the hook must not restart damaged cleanup")
}

func TestQwpFacadeWaitsForActualHousekeeperExit(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	slot := &qwpSenderSlot{slotIndex: -1, delegate: poolLateCloseSender{entered: entered, release: release}, idleSince: time.Now().Add(-time.Hour)}
	sp := &qwpSenderPool{notify: make(chan struct{}), idleTimeout: time.Millisecond, all: []*qwpSenderSlot{slot}, available: []*qwpSenderSlot{slot}}
	db := cleanupTestFacade(sp, nil)
	db.housekeeper.interval = time.Millisecond
	db.housekeeper.start()
	waitQwpCleanupSignal(t, entered, "housekeeper reap")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, db.Close(ctx), ErrCleanupPending)
	select {
	case <-db.housekeeper.done:
		t.Fatal("housekeeper has not actually exited")
	default:
	}
	once.Do(func() { close(release) })
	require.NoError(t, db.Close(context.Background()))
	waitQwpCleanupSignal(t, db.housekeeper.done, "housekeeper exit")
}

func TestQwpFacadeTracksOrphanEngineAfterDrainerExit(t *testing.T) {
	dir := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	finish := func() { close(entered); <-release }
	qwpSfTestEngineFinishCloseHook.Store(&finish)
	old := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(5 * time.Millisecond)
	defer func() { qwpSfTestEngineFinishCloseHook.Store(nil); qwpSfManagerCloseGrace.store(old) }()
	injected := errors.New("orphan manifest close result")
	fileClose := func(f *os.File) error {
		if filepath.Base(f.Name()) == qwpSfManifestFileName {
			return injected
		}
		return nil
	}
	qwpSfTestAfterFileCloseHook.Store(&fileClose)
	defer qwpSfTestAfterFileCloseHook.Store(nil)
	pool := qwpSfNewDrainerPool(1)
	d := qwpSfNewOrphanDrainer(dir, 4096, qwpSfUnlimitedTotalBytes, nil, nil, time.Second, time.Millisecond, time.Millisecond)
	require.NoError(t, pool.drainerPoolSubmit(context.Background(), d))
	waitQwpCleanupSignal(t, entered, "orphan engine release")
	// Stop accepting new drainers. This slot is empty, so no connection attempt
	// needs to be cancelled.
	pool.closed.Store(true)
	defer pool.cancel()
	db := cleanupTestFacade(closedSfPoolWithRetiredSlot(&qwpLineSender{drainerPool: pool}), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := db.Close(ctx)
	require.ErrorIs(t, err, ErrSfCleanupPending)
	lock, err := qwpSfAcquireSlotLock(dir)
	if lock != nil {
		_ = lock.close()
	}
	require.ErrorIs(t, err, qwpSfErrLockBusy)
	once.Do(func() { close(release) })
	require.ErrorIs(t, db.Close(context.Background()), injected)
	require.ErrorIs(t, db.Close(cancelledCleanupWait()), injected)
	pool.wg.Wait()
}

func TestQwpDrainerCallbackIsOutsideCleanupBarrier(t *testing.T) {
	d := qwpSfNewOrphanDrainer(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, nil, nil, time.Second, time.Millisecond, time.Millisecond)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	d.listener.OnDurableAckUnavailable = func(string, int) { close(entered); <-release }
	d.onRoundExhausted(qwpSfSweepOutcome{SawDurableMismatch: true})
	waitQwpCleanupSignal(t, entered, "drainer callback")
	pool := qwpSfNewDrainerPool(1)
	require.NoError(t, pool.drainerPoolSubmit(context.Background(), d))
	require.Eventually(t, func() bool { return d.drainerOutcome() == qwpSfDrainOutcomeSuccess }, qwpTestWaitTimeout, time.Millisecond)
	pool.drainerPoolClose()
	require.NoError(t, pool.cleanupResult(), "a blocked user notification does not retain slot access")
}

func TestQwpFacadeQueryOnlyPendingAndConcurrentWaiters(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 1, 0)
	q, err := p.borrow(context.Background())
	require.NoError(t, err)
	db := cleanupTestFacade(nil, p)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = db.Close(ctx)
	require.ErrorIs(t, err, ErrCleanupPending)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrSfCleanupPending)
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		go func() { results <- db.Close(context.Background()) }()
	}
	require.NoError(t, q.Close())
	for i := 0; i < 12; i++ {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(qwpTestWaitTimeout):
			t.Fatal("facade waiter stuck")
		}
	}
	require.NoError(t, db.Close(cancelledCleanupWait()))
}
