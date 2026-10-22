//go:build windows

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
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestQwpSfWindowsReleaseErrorsRetainViews(t *testing.T) {
	for _, stage := range []string{"close-handle", "unmap-view"} {
		t.Run(stage, func(t *testing.T) {
			e, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.NoError(t, err)
			_, err = e.engineAppendBlocking(context.Background(), []byte("preserved"))
			require.NoError(t, err)
			<-e.manager.segmentManagerStop()
			s := e.ring.getActiveSegment()
			addr := uintptr(unsafe.Pointer(&s.buf[0]))
			qwpSfWindowsMappingMu.Lock()
			mapping := qwpSfWindowsMappings[addr]
			qwpSfWindowsMappingMu.Unlock()
			require.NotNil(t, mapping)
			handle := mapping.handle
			var fail atomic.Bool
			fail.Store(true)
			var successfulCloses atomic.Int32
			injected := errors.New("injected native release failure")
			ops := &qwpSfWindowsReleaseOps{
				closeHandle: func(h windows.Handle) error {
					if h == handle && stage == "close-handle" && fail.Load() {
						return injected
					}
					err := windows.CloseHandle(h)
					if h == handle && err == nil {
						successfulCloses.Add(1)
					}
					return err
				},
				unmapView: func(a uintptr) error {
					if a == addr && stage == "unmap-view" && fail.Load() {
						return injected
					}
					return windows.UnmapViewOfFile(a)
				},
			}
			qwpSfTestWindowsReleaseOps.Store(ops)
			prior := qwpSfCloseRetryInterval.load()
			qwpSfCloseRetryInterval.store(time.Millisecond)
			t.Cleanup(func() {
				fail.Store(false)
				_ = e.engineClose()
				waitQwpSfEngineCleanup(t, e)
				qwpSfTestWindowsReleaseOps.Store(nil)
				qwpSfCloseRetryInterval.store(prior)
			})
			require.ErrorIs(t, e.engineClose(), injected)
			require.False(t, e.engineCloseCompleted())
			require.NotNil(t, s.file)
			require.NotZero(t, s.buf[0], "view must remain valid on either release error")
			qwpSfWindowsMappingMu.Lock()
			retained := qwpSfWindowsMappings[addr]
			qwpSfWindowsMappingMu.Unlock()
			require.Same(t, mapping, retained)
			lock, lockErr := qwpSfAcquireSlotLock(e.sfDir)
			if lock != nil {
				_ = lock.close()
			}
			require.ErrorIs(t, lockErr, qwpSfErrLockBusy)
			fail.Store(false)
			waitQwpSfEngineCleanup(t, e)
			require.True(t, e.engineCloseCompleted())
			require.NoError(t, e.engineClose())
			require.Equal(t, int32(1), successfulCloses.Load(), "unmap retry must not close an already consumed mapping handle")
			qwpSfWindowsMappingMu.Lock()
			after := qwpSfWindowsMappings[addr]
			qwpSfWindowsMappingMu.Unlock()
			require.NotSame(t, mapping, after)
		})
	}
}

func TestQwpSfWindowsFailedMapViewCleanupRetries(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	injected := errors.New("mapping handle close unavailable")
	ops := &qwpSfWindowsReleaseOps{
		mapView: func(windows.Handle) (uintptr, error) { return 0, errors.New("view creation rejected") },
		closeHandle: func(h windows.Handle) error {
			if fail.Load() {
				return injected
			}
			return windows.CloseHandle(h)
		},
	}
	qwpSfTestWindowsReleaseOps.Store(ops)
	t.Cleanup(func() { fail.Store(false); qwpSfTestWindowsReleaseOps.Store(nil) })
	_, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.ErrorIs(t, err, ErrSfDurability)
	require.ErrorIs(t, err, injected)
	require.NotErrorIs(t, err, ErrCleanupFailed)
	var held *qwpSfBuildCleanupError
	require.ErrorAs(t, err, &held)
	t.Cleanup(func() { fail.Store(false); waitQwpSfEngineCleanup(t, held.engine) })
	require.NotEmpty(t, held.engine.acquired.mappingObjects)
	require.False(t, held.closeCompleted())
	fail.Store(false)
	waitQwpSfEngineCleanup(t, held.engine)
	require.True(t, held.closeCompleted())
	require.NoError(t, held.engine.engineClose())
}

func TestQwpSfWindowsAcquisitionPanicRetainsOwnership(t *testing.T) {
	if dir := os.Getenv("QWP_WINDOWS_ACQUISITION_CHILD"); dir != "" {
		handle := qwpWindowsAcquisitionPanicChild(t, dir)
		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
		}
		var probe windows.Handle
		require.NoError(t, windows.DuplicateHandle(windows.CurrentProcess(), handle, windows.CurrentProcess(), &probe, 0, false, windows.DUPLICATE_SAME_ACCESS))
		// Close only the copy made for this check. The original handle must
		// stay open until the child process exits.
		require.NoError(t, windows.CloseHandle(probe))
		lock, err := qwpSfAcquireSlotLock(dir)
		if lock != nil {
			_ = lock.close()
		}
		require.ErrorIs(t, err, qwpSfErrLockBusy)
		return
	}
	cmd := qwpTestSubprocess(t, "TestQwpSfWindowsAcquisitionPanicRetainsOwnership")
	cmd.Env = append(os.Environ(), "QWP_WINDOWS_ACQUISITION_CHILD="+t.TempDir())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

func qwpWindowsAcquisitionPanicChild(t *testing.T, dir string) windows.Handle {
	var calls atomic.Int32
	ops := &qwpSfWindowsReleaseOps{
		mapView:     func(windows.Handle) (uintptr, error) { return 0, errors.New("view creation rejected") },
		closeHandle: func(windows.Handle) error { calls.Add(1); panic("mapping handle cleanup interrupted") },
	}
	qwpSfTestWindowsReleaseOps.Store(ops)
	_, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.ErrorIs(t, err, ErrCleanupFailed)
	var held *qwpSfBuildCleanupError
	require.ErrorAs(t, err, &held)
	waitQwpSfEngineCleanup(t, held.engine)
	require.False(t, held.closeCompleted())
	require.ErrorIs(t, held.engine.engineClose(), ErrCleanupFailed)
	require.Equal(t, int32(1), calls.Load(), "terminal release must not be replayed")
	return windows.Handle(held.engine.acquired.mappingObjects[0].handle)
}
