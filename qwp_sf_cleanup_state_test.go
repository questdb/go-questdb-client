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
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQwpSfCleanupObserversNeverDriveRetries(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("preserved"))
	require.NoError(t, err)
	old := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(time.Millisecond)
	injected := errors.New("release temporarily unavailable")
	var fail atomic.Bool
	fail.Store(true)
	flock := func() error {
		if fail.Load() {
			return injected
		}
		return nil
	}
	var calls atomic.Int32
	retryEntered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	finish := func() {
		if calls.Add(1) == 2 {
			close(retryEntered)
			<-release
		}
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&flock)
	qwpSfTestEngineFinishCloseHook.Store(&finish)
	t.Cleanup(func() {
		fail.Store(false)
		releaseOnce.Do(func() { close(release) })
		waitQwpSfEngineCleanup(t, e)
		qwpSfTestBeforeFlockReleaseHook.Store(nil)
		qwpSfTestEngineFinishCloseHook.Store(nil)
		qwpSfCloseRetryInterval.store(old)
	})
	require.ErrorIs(t, e.engineClose(), injected)
	select {
	case <-retryEntered:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("owned retry did not start")
	}
	var wg sync.WaitGroup
	results := make([]error, 32)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = e.engineClose()
		}(i)
	}
	wg.Wait()
	for _, result := range results {
		require.ErrorIs(t, result, injected, "observer lost pending error")
	}
	require.Equal(t, int32(2), calls.Load(), "observers must not enter physical cleanup")
	require.False(t, e.engineCloseCompleted())
	lock, err := qwpSfAcquireSlotLock(e.sfDir)
	if lock != nil {
		_ = lock.close()
	}
	require.ErrorIs(t, err, qwpSfErrLockBusy)
	fail.Store(false)
	releaseOnce.Do(func() { close(release) })
	waitQwpSfEngineCleanup(t, e)
	require.True(t, e.engineCloseCompleted())
	require.NoError(t, e.engineClose(), "recovered storage errors must clear")
	require.Equal(t, int32(2), calls.Load())
}

func TestQwpSfReleaseRetriesKeepMappingReferences(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("unacknowledged"))
	require.NoError(t, err)
	<-e.manager.segmentManagerStop()
	s := e.ring.getActiveSegment()
	w := e.watermark
	old := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(time.Millisecond)
	var fail atomic.Bool
	fail.Store(true)
	injected := errors.New("unmap temporarily unavailable")
	hook := func([]byte) error {
		if fail.Load() {
			return injected
		}
		return nil
	}
	qwpSfTestMunmapHook.Store(&hook)
	t.Cleanup(func() {
		fail.Store(false)
		waitQwpSfEngineCleanup(t, e)
		qwpSfTestMunmapHook.Store(nil)
		qwpSfCloseRetryInterval.store(old)
	})
	require.ErrorIs(t, e.engineClose(), injected)
	// The injected failure leaves both the mapping and file handle open.
	// Check them before allowing a retry to release them.
	require.NotNil(t, s.buf)
	require.NotNil(t, s.file)
	w.mu.Lock()
	mapped := w.buf != nil && w.file != nil
	w.mu.Unlock()
	require.True(t, mapped)
	require.False(t, e.engineCloseCompleted())
	fail.Store(false)
	waitQwpSfEngineCleanup(t, e)
	require.Nil(t, s.buf)
	require.Nil(t, s.file)
	require.NoError(t, e.engineClose())
}

func waitQwpSfEngineCleanup(t *testing.T, engine *qwpSfCursorEngine) {
	t.Helper()
	engine.cleanup.mu.Lock()
	done := engine.cleanup.done
	engine.cleanup.mu.Unlock()
	require.NotNil(t, done, "cleanup must already have an owner")
	select {
	case <-done:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("engine cleanup did not finish")
	}
}

func waitQwpCleanupSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("timed out waiting for " + what)
	}
}
