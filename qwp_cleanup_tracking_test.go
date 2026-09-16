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
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestQwpRejectedIngestConnectionRemainsPending(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Complete the WebSocket upgrade, but omit the mandatory QWP version.
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		<-conn.CloseRead(r.Context()).Done()
	}))
	defer srv.Close()
	db, err := NewQuestDB(context.Background(), "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";", WithSenderPoolMin(0), WithQueryPoolMin(0))
	require.NoError(t, err)
	release := make(chan struct{})
	entered := make(chan *qwpTransport, 1)
	hook := func(tr *qwpTransport) { entered <- tr; <-release }
	qwpTestBeforeTransportClose.Store(&hook)
	var tr *qwpTransport
	defer func() {
		close(release)
		if tr != nil {
			select {
			case <-tr.closeDone:
			case <-time.After(3 * time.Second):
				t.Error("transport cleanup did not finish")
			}
		}
		qwpTestBeforeTransportClose.Store(nil)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = db.Close(ctx)
	}()
	sender, err := db.BorrowSender(context.Background())
	require.NoError(t, err)
	select {
	case tr = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("rejected connection did not start cleanup")
	}
	require.NoError(t, sender.Close(context.Background()))
	waitCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	err = db.Close(waitCtx)
	select {
	case <-tr.closeDone:
		t.Fatal("test failed to keep transport cleanup pending")
	default:
	}
	if !errors.Is(err, ErrCleanupPending) {
		t.Errorf("facade Close = %v; want ErrCleanupPending while a rejected ingest connection still has a live transport", err)
	}
}

// Check that close errors are reported. Close the real socket before forcing
// a panic, so this test does not leave it open; it is not a retention test.
func TestQwpReconnectPreservesOldTransportCloseError(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	tr, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	fresh, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, tr, qwpSfDialFor(srv), time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)
	hook := func(got *qwpTransport) {
		if got == tr {
			_ = tr.conn.CloseNow()
			panic("injected: retired transport release failed")
		}
		if got == fresh {
			_ = fresh.conn.CloseNow()
			panic("injected: final transport release failed")
		}
	}
	qwpTestBeforeTransportClose.Store(&hook)
	defer qwpTestBeforeTransportClose.Store(nil)
	defer func() {
		_ = sender.Close(context.Background())
		if op := sender.shutdown.Load(); op != nil {
			<-op.done
		}
	}()
	require.NoError(t, loop.swapClient(fresh))
	require.ErrorIs(t, tr.closeErr, ErrCleanupFailed, "fault must actually occur in the retired connection")
	slot := &qwpSenderSlot{slotIndex: -1, delegate: sender, cleanup: sender}
	pool := &qwpSenderPool{notify: make(chan struct{}), all: []*qwpSenderSlot{slot}, available: []*qwpSenderSlot{slot}}
	db := cleanupTestFacade(pool, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = db.Close(ctx)
	require.ErrorIs(t, err, ErrCleanupFailed)
	require.ErrorContains(t, err, "retired transport release failed")
	require.ErrorContains(t, err, "final transport release failed")
}

// Empty test pipes let the close hook run without opening real sockets or files.
// Do not retry either failed close. Wait for both workers before ending the test.
func TestQwpQueryPoolKeepsLateGenerationError(t *testing.T) {
	early := &qwpTransport{dumpConn: &asyncWritePipeConn{}}
	late := &qwpTransport{dumpConn: &asyncWritePipeConn{}}
	client := &QwpQueryClient{generations: []*qwpConnectResult{{transport: early}, {transport: late}}}
	worker := &qwpQueryWorker{client: client}
	pool := &qwpQueryPool{notify: make(chan struct{}), all: []*qwpQueryWorker{worker}, available: []*qwpQueryWorker{worker}}
	db := cleanupTestFacade(nil, pool)
	entered, release := make(chan struct{}), make(chan struct{})
	hook := func(tr *qwpTransport) {
		if tr == early {
			panic("injected early generation failure")
		}
		if tr == late {
			close(entered)
			<-release
			panic("injected late generation failure")
		}
	}
	qwpTestBeforeTransportClose.Store(&hook)
	released := false
	defer func() {
		if !released {
			close(release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = client.Close(ctx)
		select {
		case <-client.closeDone:
		case <-ctx.Done():
			t.Error("query cleanup workers did not finish")
		}
		qwpTestBeforeTransportClose.Store(nil)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.ErrorIs(t, db.Close(ctx), ErrCleanupFailed)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("late generation did not enter cleanup")
	}
	close(release)
	released = true
	select {
	case <-client.closeDone:
	case <-ctx.Done():
		t.Fatal("late cleanup did not finish")
	}
	require.ErrorContains(t, client.closeResult(), "injected late generation failure")
	result := db.Close(ctx)
	if result == nil || !strings.Contains(result.Error(), "injected late generation failure") {
		t.Errorf("facade omitted a completed generation's late error: %v", result)
	}
}

// Even for a memory-only sender, the pool must keep a way to check cleanup
// after construction fails. A timeout does not mean memory and connections
// have been released.
func TestQwpMemoryBuildPanicKeepsCleanupReporter(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	finish := func() { close(entered); <-release }
	boom := func() { panic("post-sender build failure") }
	old := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(10 * time.Millisecond)
	qwpSfTestEngineFinishCloseHook.Store(&finish)
	qwpTestAfterSenderBuiltHook.Store(&boom)
	var p *qwpSenderPool
	defer func() {
		close(release)
		if p != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = p.close(ctx)
			require.Eventually(t, func() bool { return !errors.Is(p.currentCloseResult(), ErrCleanupPending) }, 3*time.Second, time.Millisecond)
		}
		qwpSfTestEngineFinishCloseHook.Store(nil)
		qwpTestAfterSenderBuiltHook.Store(nil)
		qwpSfManagerCloseGrace.store(old)
	}()
	var err error
	p, err = newQwpSenderPool(context.Background(), "ws::addr=127.0.0.1:1;initial_connect_retry=async;close_flush_timeout_millis=0;",
		0, 1, qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	_, err = p.borrow(context.Background())
	require.ErrorContains(t, err, "sender build panicked")
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup never reached the blocked finish")
	}
	require.ErrorIs(t, p.close(cancelledCleanupWait()), ErrCleanupPending)
	p.mu.Lock()
	count := len(p.retiredSlots)
	p.mu.Unlock()
	require.Equal(t, 1, count, "memory-mode failed construction still owns cleanup")
}

// If construction is cancelled after accepting background work on another
// sender's saved data, the error must let callers check that work's cleanup too.
// This sender's own directory may be unlocked while the other is still locked.
func TestQwpFailedBuildKeepsAcceptedOrphanCleanup(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"orphan-a", "orphan-b"} {
		e, err := qwpSfNewCursorEngine(filepath.Join(root, name), 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = e.engineAppendBlocking(context.Background(), []byte("saved row"))
		require.NoError(t, err)
		_ = e.engineClose()
		waitQwpSfEngineCleanup(t, e)
		require.True(t, e.engineCloseCompleted())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var sender *qwpLineSender
	var drainer *qwpSfCursorEngine
	open := func(e *qwpSfCursorEngine) { drainer = e; close(entered); <-release }
	submitted := func(s *qwpLineSender) { sender = s; <-entered; cancel() }
	qwpSfTestAfterDrainerEngineOpenHook.Store(&open)
	qwpTestAfterOrphanSubmitHook.Store(&submitted)
	defer func() {
		close(release)
		if sender != nil {
			require.Eventually(t, sender.closeCompleted, 5*time.Second, time.Millisecond)
		}
		qwpSfTestAfterDrainerEngineOpenHook.Store(nil)
		qwpTestAfterOrphanSubmitHook.Store(nil)
	}()
	result, err := LineSenderFromConf(ctx, "ws::addr=127.0.0.1:1;sf_dir="+root+
		";sender_id=foreground;initial_connect_retry=async;drain_orphans=on;close_flush_timeout_millis=0;")
	require.Nil(t, result)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, sender)
	<-entered
	require.Eventually(t, sender.cursorEngine.engineCloseCompleted, 3*time.Second, time.Millisecond)
	var reporter closeLifecycleReporter
	require.ErrorAs(t, err, &reporter)
	require.False(t, reporter.closeCompleted(), "foreground release must not hide the accepted orphan")
	require.False(t, drainer.engineCloseCompleted())
	lock, lockErr := qwpSfAcquireSlotLock(drainer.engineSfDir())
	if lockErr == nil {
		_ = lock.close()
	}
	require.ErrorIs(t, lockErr, qwpSfErrLockBusy)
}

func TestQwpRejectedSynchronousIngestBuildRetainsCleanup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		<-conn.CloseRead(r.Context()).Done()
	}))
	defer srv.Close()
	entered, release := make(chan *qwpTransport, 1), make(chan struct{})
	hook := func(tr *qwpTransport) { entered <- tr; <-release }
	qwpTestBeforeTransportClose.Store(&hook)
	old := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(10 * time.Millisecond)
	var reporter closeLifecycleReporter
	defer func() {
		close(release)
		if reporter != nil {
			require.Eventually(t, reporter.closeCompleted, 3*time.Second, time.Millisecond)
		}
		qwpTestBeforeTransportClose.Store(nil)
		qwpSfManagerCloseGrace.store(old)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	sender, err := LineSenderFromConf(ctx, "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";initial_connect_retry=off;")
	require.Nil(t, sender)
	require.Error(t, err)
	require.ErrorAs(t, err, &reporter)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("rejected transport did not enter cleanup")
	}
	require.False(t, reporter.closeCompleted())
}

func TestQwpIngestAcquisitionPanicRetainsTransport(t *testing.T) {
	if os.Getenv("QWP_INGEST_ACQUISITION_CHILD") == "1" {
		finalized := make(chan struct{}, 1)
		entered := make(chan struct{})
		var closes atomic.Int32
		hook := func(tr *qwpTransport) {
			runtime.SetFinalizer(tr, func(*qwpTransport) { finalized <- struct{}{} })
			close(entered)
			panic("injected after ingest acquisition")
		}
		closing := func(*qwpTransport) { closes.Add(1) }
		qwpTestAfterIngestTransportConnect.Store(&hook)
		qwpTestBeforeTransportClose.Store(&closing)
		runRetainedIngestAcquisition(t, entered)
		for i := 0; i < 4; i++ {
			runtime.GC()
			runtime.Gosched()
		}
		select {
		case <-finalized:
			t.Fatal("uncertain acquired transport became unreachable")
		case <-time.After(150 * time.Millisecond):
		}
		require.Zero(t, closes.Load(), "do not attempt release after a setup panic")
		return // The parent checks release after the process exits.
	}
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestQwpIngestAcquisitionPanicRetainsTransport$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "QWP_INGEST_ACQUISITION_CHILD=1", "QWP_INGEST_ACQUISITION_ADDR="+strings.TrimPrefix(srv.URL, "http://"), "QWP_INGEST_ACQUISITION_DIR="+root)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	slotDir := filepath.Join(root, "acquisition-0")
	require.DirExists(t, slotDir)
	lock, err := qwpSfAcquireSlotLock(slotDir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func runRetainedIngestAcquisition(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := NewQuestDB(ctx, "ws::addr="+os.Getenv("QWP_INGEST_ACQUISITION_ADDR")+
		";sf_dir="+os.Getenv("QWP_INGEST_ACQUISITION_DIR")+";sender_id=acquisition;initial_connect_retry=async;close_flush_timeout_millis=0;",
		WithSenderPoolMin(0), WithQueryPoolMin(0))
	require.NoError(t, err)
	sender, err := db.BorrowSender(ctx)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("ingest connection was not acquired")
	}
	e := sender.(*qwpPooledSender).slot.delegate.(*qwpLineSender).cursorEngine
	// Return the borrowed sender before closing the pool.
	_ = sender.Close(ctx)
	err = db.Close(ctx)
	require.ErrorIs(t, err, ErrCleanupFailed)
	require.ErrorIs(t, err, ErrSfCleanupPending)
	waitQwpSfEngineCleanup(t, e)
	lock, lockErr := qwpSfAcquireSlotLock(e.engineSfDir())
	if lockErr == nil {
		_ = lock.close()
	}
	require.ErrorIs(t, lockErr, qwpSfErrLockBusy)
}

// The engine can report one close failure while another connection is still
// closing. Check that the sender and pool also report the later close error.
func TestQwpSenderPoolKeepsLateTransportError(t *testing.T) {
	early := &qwpTransport{dumpConn: &asyncWritePipeConn{}}
	late := &qwpTransport{dumpConn: &asyncWritePipeConn{}}
	entered, release := make(chan struct{}), make(chan struct{})
	hook := func(tr *qwpTransport) {
		if tr == early {
			panic("early ingest transport failure")
		}
		if tr == late {
			close(entered)
			<-release
			panic("late ingest transport failure")
		}
	}
	qwpTestBeforeTransportClose.Store(&hook)
	e, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	released := false
	defer func() {
		if !released {
			close(release)
		}
		_ = e.engineClose()
		waitQwpSfEngineCleanup(t, e)
		qwpTestBeforeTransportClose.Store(nil)
	}()
	_ = early.closeContext(cancelledCleanupWait())
	_ = late.closeContext(cancelledCleanupWait())
	e.rejectedTransports = []*qwpTransport{early, late}
	s := &qwpLineSender{cursorEngine: e}
	slot := &qwpSenderSlot{slotIndex: -1, delegate: s, cleanup: s}
	pool := &qwpSenderPool{notify: make(chan struct{}), all: []*qwpSenderSlot{slot}, available: []*qwpSenderSlot{slot}}
	db := cleanupTestFacade(pool, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.ErrorIs(t, db.Close(ctx), ErrCleanupFailed)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("late transport never entered cleanup")
	}
	close(release)
	released = true
	waitQwpSfEngineCleanup(t, e)
	require.ErrorContains(t, e.cleanupResult(), "late ingest transport failure")
	require.ErrorContains(t, db.Close(ctx), "late ingest transport failure")
}
