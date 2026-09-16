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
	"encoding/binary"
	"errors"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// Failed cleanup must keep resources alive even after its worker exits and
// the application drops its references. Run this in a child process because
// the mappings and file locks must remain until process exit. The parent then
// removes the directory. Do not release resources just to make the test pass.
func TestQwpSfTerminalCleanupRetainsOwnership(t *testing.T) {
	if mode := os.Getenv("QWP_TERMINAL_CLEANUP_CHILD"); mode != "" {
		dir := os.Getenv("QWP_TERMINAL_CLEANUP_DIR")
		buf := qwpTerminalCleanupChild(t, dir, mode)
		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
		}
		lock, err := qwpSfAcquireSlotLock(dir)
		if lock != nil {
			_ = lock.close()
		}
		require.ErrorIs(t, err, qwpSfErrLockBusy, "worker exit and GC must not release retained flock")
		if len(buf) > 0 {
			require.NotZero(t, buf[0], "the retained mapping must remain readable")
		}
		runtime.KeepAlive(buf)
		return
	}
	for _, mode := range []string{"ring", "middle-segment", "segment-file-close", "watermark", "symbol-dict", "construction-initial", "construction-manifest", "construction-ring", "manager-spare", "replacement", "unlink"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQwpSfTerminalCleanupRetainsOwnership$", "-test.timeout=12s")
			dir := t.TempDir()
			cmd.Env = append(os.Environ(), "QWP_TERMINAL_CLEANUP_CHILD="+mode, "QWP_TERMINAL_CLEANUP_DIR="+dir)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
			lock, err := qwpSfAcquireSlotLock(dir)
			require.NoError(t, err, "process exit must release the retained flock")
			require.NoError(t, lock.close())
			recovered, err := qwpSfNewCursorEngineForDrainer(dir, 256, qwpSfUnlimitedTotalBytes, time.Second)
			require.NoError(t, err)
			defer func() { require.NoError(t, recovered.engineClose()) }()
			if mode == "ring" || mode == "middle-segment" || mode == "segment-file-close" || mode == "watermark" || mode == "symbol-dict" || mode == "replacement" {
				require.Equal(t, int64(2), recovered.ring.segmentRingPublishedFsn())
				require.Equal(t, int64(-1), recovered.engineAckedFsn())
			}
		})
	}
}

func qwpTerminalCleanupChild(t *testing.T, dir, mode string) []byte {
	t.Helper()
	var hits atomic.Int32
	boom := func() { hits.Add(1); panic("injected cleanup fault") }
	if mode == "manager-spare" {
		hook := func(*qwpSfManagerRingEntry) { boom() }
		qwpSfTestAfterSpareCreateHook.Store(&hook)
		e, err := qwpSfNewCursorEngine(dir, 256, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		select {
		case <-e.manager.done:
		case <-time.After(time.Second):
			t.Fatal("manager did not exit")
		}
		qwpSfTestAfterSpareCreateHook.Store(nil)
		require.ErrorIs(t, e.engineClose(), ErrCleanupFailed)
		waitQwpSfEngineCleanup(t, e)
		require.False(t, e.engineCloseCompleted())
		require.Equal(t, int32(1), hits.Load())
		return e.managerEntry.spareInProgress.buf
	}
	if mode == "construction-initial" || mode == "construction-manifest" || mode == "construction-ring" {
		buildErr := errors.New("construction failed")
		fail := func() error { return buildErr }
		if mode == "construction-ring" {
			qwpSfTestBeforeEngineRegisterHook.Store(&fail)
		} else {
			qwpSfTestBeforeFreshRingAdoptHook.Store(&fail)
		}
		if mode == "construction-initial" {
			hook := func(*qwpSfSegment) { boom() }
			qwpSfTestBeforeSegmentCloseHook.Store(&hook)
		} else {
			point := qwpSfCleanupTestManifestClosePhase
			if mode == "construction-ring" {
				point = qwpSfCleanupTestRingClosePhase
			}
			hook := func(p qwpSfCleanupTestPoint) {
				if p == point {
					boom()
				}
			}
			qwpSfTestCleanupHook.Store(&hook)
		}
		_, err := qwpSfNewCursorEngine(dir, 256, qwpSfUnlimitedTotalBytes, time.Second)
		require.ErrorIs(t, err, buildErr)
		require.ErrorIs(t, err, ErrCleanupFailed)
		var held *qwpSfBuildCleanupError
		require.ErrorAs(t, err, &held)
		waitQwpSfEngineCleanup(t, held.engine)
		require.False(t, held.closeCompleted())
		if mode == "construction-ring" {
			p := &qwpSenderPool{storeAndForward: true, maxSize: 1, sfSlots: []qwpSfSlotLifecycle{{state: qwpSfSlotCreating}}}
			slot := &qwpSenderSlot{slotIndex: 0, cleanup: held}
			p.mu.Lock()
			p.reclaimFailedBuildLocked(slot, 0, err)
			p.mu.Unlock()
			for i := 0; i < 4; i++ {
				result := p.currentCloseResult()
				require.ErrorIs(t, result, ErrCleanupFailed)
				require.ErrorIs(t, result, ErrSfCleanupPending)
			}
			p.mu.Lock()
			used, available := p.capUsedLocked(), p.allocateSlotIndexLocked()
			p.mu.Unlock()
			require.Equal(t, 1, used)
			require.Equal(t, -1, available, "terminal failure must never return the reserved index")
		}
		require.ErrorIs(t, held.engine.engineClose(), ErrCleanupFailed)
		require.Equal(t, int32(1), hits.Load())
		if mode == "construction-initial" {
			return held.engine.looseSegments[0].buf
		}
		if mode == "construction-ring" {
			return held.engine.ring.getActiveSegment().buf
		}
		return nil
	}
	e, err := qwpSfNewCursorEngine(dir, 256, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	// Each frame fills a separate mapped segment. The middle segment is no
	// longer being written, so we can test closing it separately.
	for i := 0; i < 3; i++ {
		_, err = e.engineAppendBlocking(context.Background(), make([]byte, 128))
		require.NoError(t, err)
	}
	<-e.manager.segmentManagerStop()
	middle := e.ring.findSegmentContaining(1)
	require.NotNil(t, middle)
	var buf []byte
	switch mode {
	case "replacement":
		hook := func(b []byte) error {
			if len(b) >= 16 && binary.LittleEndian.Uint64(b[8:16]) == 77 {
				boom()
			}
			return nil
		}
		qwpSfTestMunmapHook.Store(&hook)
		_, err := qwpSfReplaceTornActive(e.ring.getActiveSegment().path, 77, 256)
		require.ErrorIs(t, err, ErrCleanupFailed)
		var held *qwpSfAcquisitionError
		require.ErrorAs(t, err, &held)
		e.acquired = held.resources
		require.ErrorIs(t, e.engineCloseWithCause(err), ErrCleanupFailed)
		waitQwpSfEngineCleanup(t, e)
		require.Equal(t, int32(1), hits.Load())
		return held.resources.segments[0].buf
	case "segment-file-close":
		path := middle.path
		hook := func(f *os.File) error {
			if f.Name() == path {
				boom()
			}
			return nil
		}
		qwpSfTestAfterFileCloseHook.Store(&hook)
	case "middle-segment":
		hook := func(s *qwpSfSegment) {
			if s == middle {
				boom()
			}
		}
		qwpSfTestBeforeSegmentCloseHook.Store(&hook)
		buf = middle.buf
	case "unlink":
		e.engineAcknowledge(e.ring.segmentRingPublishedFsn())
		hook := func(string) { boom() }
		qwpSfTestBeforeSegmentUnlinkHook.Store(&hook)
	default:
		point := qwpSfCleanupTestRingClosePhase
		if mode == "watermark" {
			point = qwpSfCleanupTestWatermarkClosePhase
		}
		if mode == "symbol-dict" {
			point = qwpSfCleanupTestSymbolDictClosePhase
		}
		hook := func(p qwpSfCleanupTestPoint) {
			if p == point {
				boom()
			}
		}
		qwpSfTestCleanupHook.Store(&hook)
		if mode == "ring" {
			buf = middle.buf
		}
	}
	require.ErrorIs(t, e.engineClose(), ErrCleanupFailed)
	waitQwpSfEngineCleanup(t, e)
	require.False(t, e.engineCloseCompleted())
	for i := 0; i < 8; i++ {
		require.ErrorIs(t, e.engineClose(), ErrCleanupFailed)
	}
	require.Equal(t, int32(1), hits.Load(), "terminal cleanup must never replay")
	if mode == "middle-segment" || mode == "segment-file-close" {
		for _, s := range e.ring.closingSegments {
			if s != nil && s != middle {
				require.True(t, s.resourcesReleased(), "safe sibling close was skipped")
			}
		}
	}
	return buf
}
