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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQwpSfAcquiredReleaseFailureTransfersToEngine(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	<-e.manager.segmentManagerStop()
	segment, err := qwpSfCreateSegment(filepath.Join(e.sfDir, "sf-extra.sfa"), 77, 4096)
	require.NoError(t, err)
	var fail atomic.Bool
	fail.Store(true)
	var attempts atomic.Int32
	injected := errors.New("acquired segment unmap unavailable")
	hook := func(b []byte) error {
		if len(b) >= 16 && binary.LittleEndian.Uint64(b[8:16]) == 77 {
			attempts.Add(1)
			if fail.Load() {
				return injected
			}
		}
		return nil
	}
	qwpSfTestMunmapHook.Store(&hook)
	t.Cleanup(func() {
		fail.Store(false)
		_ = e.engineClose()
		waitQwpSfEngineCleanup(t, e)
		qwpSfTestMunmapHook.Store(nil)
	})
	err = qwpSfFailedAcquisition(errors.New("construction failed"), &qwpSfAcquiredResources{segments: []*qwpSfSegment{segment}})
	require.ErrorIs(t, err, injected)
	var held *qwpSfAcquisitionError
	require.ErrorAs(t, err, &held)
	e.acquired = held.resources
	require.Equal(t, int32(1), attempts.Load(), "unwinding must not replay the failed release")
	require.False(t, segment.resourcesReleased())
	require.ErrorIs(t, e.engineClose(), injected)
	require.False(t, e.engineCloseCompleted())
	fail.Store(false)
	waitQwpSfEngineCleanup(t, e)
	require.True(t, e.engineCloseCompleted())
	require.NoError(t, e.engineClose())
	require.True(t, segment.resourcesReleased())
}

func TestQwpSfConsumedFileCloseErrorIsNotRetried(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("retained frame"))
	require.NoError(t, err)
	<-e.manager.segmentManagerStop()
	injected := errors.New("file close reported an error after consuming the handle")
	var calls atomic.Int32
	hook := func(*os.File) error { calls.Add(1); return injected }
	qwpSfTestAfterFileCloseHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestAfterFileCloseHook.Store(nil) })
	require.ErrorIs(t, e.engineClose(), injected)
	waitQwpSfEngineCleanup(t, e)
	require.True(t, e.engineCloseCompleted(), "all handles, including flock, are actually released")
	before := calls.Load()
	require.Positive(t, before)
	for i := 0; i < 10; i++ {
		require.ErrorIs(t, e.engineClose(), injected)
	}
	require.Equal(t, before, calls.Load(), "an error is not permission to close a consumed fd again")
	lock, err := qwpSfAcquireSlotLock(e.sfDir)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpSfFileCloseErrorSurvivesOtherResourceRetry(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)
	<-e.manager.segmentManagerStop()
	var fail atomic.Bool
	fail.Store(true)
	unmapErr, fileErr := errors.New("unmap unavailable"), errors.New("manifest close failed after consuming fd")
	unmap := func([]byte) error {
		if fail.Load() {
			return unmapErr
		}
		return nil
	}
	var closes atomic.Int32
	fileClose := func(f *os.File) error {
		if filepath.Base(f.Name()) == qwpSfManifestFileName {
			closes.Add(1)
			return fileErr
		}
		return nil
	}
	qwpSfTestMunmapHook.Store(&unmap)
	qwpSfTestAfterFileCloseHook.Store(&fileClose)
	t.Cleanup(func() {
		fail.Store(false)
		_ = e.engineClose()
		waitQwpSfEngineCleanup(t, e)
		qwpSfTestMunmapHook.Store(nil)
		qwpSfTestAfterFileCloseHook.Store(nil)
	})
	err = e.engineClose()
	require.ErrorIs(t, err, unmapErr)
	require.ErrorIs(t, err, fileErr)
	fail.Store(false)
	waitQwpSfEngineCleanup(t, e)
	require.True(t, e.engineCloseCompleted())
	err = e.engineClose()
	require.ErrorIs(t, err, fileErr, "unrecovered file-close error must survive a later successful unmap")
	require.NotErrorIs(t, err, unmapErr)
	require.Equal(t, int32(1), closes.Load())
}

func TestQwpSfLateCompletedCloseErrorIsLogged(t *testing.T) {
	e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	<-e.manager.segmentManagerStop()
	var logs bytes.Buffer
	e.manager.logger.Store(qwpGuardLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { close(entered); <-release }
	injected := errors.New("late consumed-file close error")
	fileClose := func(f *os.File) error {
		if filepath.Base(f.Name()) == qwpSfManifestFileName {
			return injected
		}
		return nil
	}
	previous := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(5 * time.Millisecond)
	qwpSfTestEngineFinishCloseHook.Store(&finish)
	qwpSfTestAfterFileCloseHook.Store(&fileClose)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = e.engineClose()
		waitQwpSfEngineCleanup(t, e)
		qwpSfTestEngineFinishCloseHook.Store(nil)
		qwpSfTestAfterFileCloseHook.Store(nil)
		qwpSfManagerCloseGrace.store(previous)
	})
	require.NoError(t, e.engineClose(), "the bounded observer returns while cleanup is still pending")
	waitQwpCleanupSignal(t, entered, "blocked engine cleanup")
	require.False(t, e.engineCloseCompleted())
	releaseOnce.Do(func() { close(release) })
	waitQwpSfEngineCleanup(t, e)
	require.True(t, e.engineCloseCompleted())
	require.ErrorIs(t, e.engineClose(), injected)
	require.Contains(t, logs.String(), "cleanup completed with error")
	require.Contains(t, logs.String(), injected.Error())
}

func TestQwpSfRecoveryReleaseFaultRetainsAcquisitions(t *testing.T) {
	dir := t.TempDir()
	old, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = old.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)
	require.NoError(t, old.engineClose())
	file, err := os.OpenFile(filepath.Join(dir, "sf-initial.sfa"), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{0, 0, 0, 0}, 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	var fail atomic.Bool
	fail.Store(true)
	injected := errors.New("unmap unavailable during recovery")
	hook := func([]byte) error {
		if fail.Load() {
			return injected
		}
		return nil
	}
	qwpSfTestMunmapHook.Store(&hook)
	previous := qwpSfCloseRetryInterval.load()
	qwpSfCloseRetryInterval.store(time.Millisecond)
	t.Cleanup(func() { fail.Store(false); qwpSfTestMunmapHook.Store(nil); qwpSfCloseRetryInterval.store(previous) })
	_, err = qwpSfNewCursorEngineWithOptions(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second, qwpSfEngineOpenOptions{recoverForeground: true})
	require.ErrorIs(t, err, ErrSfDurability)
	require.ErrorIs(t, err, injected)
	require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed, "cleanup failure must not license quarantine")
	var held *qwpSfBuildCleanupError
	require.ErrorAs(t, err, &held)
	t.Cleanup(func() { fail.Store(false); waitQwpSfEngineCleanup(t, held.engine) })
	require.False(t, held.closeCompleted())
	require.NotNil(t, held.engine.acquired)
	require.FileExists(t, filepath.Join(dir, "sf-initial.sfa"))
	lock, lockErr := qwpSfAcquireSlotLock(dir)
	if lock != nil {
		_ = lock.close()
	}
	require.ErrorIs(t, lockErr, qwpSfErrLockBusy)
	fail.Store(false)
	waitQwpSfEngineCleanup(t, held.engine)
	require.True(t, held.closeCompleted())
	require.NoError(t, held.engine.engineClose())
}
