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
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createRecoverySegment(t *testing.T, dir, name string, base int64, payloads ...string) *qwpSfSegment {
	t.Helper()
	seg, err := qwpSfCreateSegment(filepath.Join(dir, name), base, 4096)
	require.NoError(t, err)
	for _, payload := range payloads {
		_, err := seg.tryAppend([]byte(payload))
		require.NoError(t, err)
	}
	return seg
}

func createRecoveryManifest(t *testing.T, dir string, head, active int64, segments ...*qwpSfSegment) {
	t.Helper()
	m, err := qwpSfManifestCreate(dir, head, active)
	require.NoError(t, err)
	require.NoError(t, m.close())
	for _, seg := range segments {
		require.NoError(t, seg.markManifestRequired())
	}
}

func closeRecoverySegments(t *testing.T, segments ...*qwpSfSegment) {
	t.Helper()
	for _, seg := range segments {
		require.NoError(t, seg.close())
	}
}

func TestQwpSfRecoveryFailsClosedWhenManifestNewestIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, dir, 0, 1, s0, s1)
	closeRecoverySegments(t, s0, s1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-0001.sfa"), make([]byte, 4096), 0o644))

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "active")
}

func TestQwpSfRecoveryFailsClosedWhenManifestOldestIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, dir, 0, 1, s0, s1)
	closeRecoverySegments(t, s0, s1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-initial.sfa"), make([]byte, 4096), 0o644))

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "head")
}

func TestQwpSfRecoveryRejectsFlaggedSegmentWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	require.NoError(t, seg.markManifestRequired())
	require.NoError(t, seg.close())

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "manifest.bin is missing")
}

func TestQwpSfRecoveryManifestOnlyBoundaries(t *testing.T) {
	t.Run("data-boundaries-fail", func(t *testing.T) {
		dir := t.TempDir()
		m, err := qwpSfManifestCreate(dir, 0, 1)
		require.NoError(t, err)
		require.NoError(t, m.close())
		_, _, err = qwpSfRecoverRing(dir, 4096)
		require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	})
	t.Run("collapsed-is-empty", func(t *testing.T) {
		dir := t.TempDir()
		m, err := qwpSfManifestCreate(dir, 4, 4)
		require.NoError(t, err)
		require.NoError(t, m.close())
		ring, manifest, err := qwpSfRecoverRing(dir, 4096)
		require.NoError(t, err)
		assert.Nil(t, ring)
		assert.Nil(t, manifest)
		_, err = os.Stat(filepath.Join(dir, qwpSfManifestFileName))
		assert.True(t, os.IsNotExist(err))
	})
}

func TestQwpSfRecoveryCollapsedManifestWithCorruptActiveFailsClosed(t *testing.T) {
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", 4, "unacked")
	createRecoveryManifest(t, dir, 4, 4, active)
	closeRecoverySegments(t, active)
	path := filepath.Join(dir, "sf-active.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0}, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.ErrorContains(t, err, "references durable data")
	_, statErr := os.Stat(path)
	require.NoError(t, statErr, "fail-closed recovery must preserve the corrupt active")
}

func TestQwpSfRecoveryMigratesLegacyAndStampsManifestFlag(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	require.NoError(t, seg.close())

	ring, manifest, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	require.NotNil(t, manifest)
	assert.True(t, ring.getActiveSegment().segmentManifestRequired())
	require.NoError(t, ring.segmentRingClose())

	ring, _, err = qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	assert.Equal(t, int64(0), ring.getActiveSegment().segmentBaseSeq())
}

// TestQwpSfRecoveryDoesNotReflushMigratedHeaders pins that the manifest-required
// stamp costs a flush only on the restart that actually migrates the slot.
// Recovery re-runs it over the whole chain every time, so a slot of thousands
// of segments would otherwise pay thousands of flushes before it can send its
// first row — in exactly the situation where a fast restart matters most.
func TestQwpSfRecoveryDoesNotReflushMigratedHeaders(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	require.NoError(t, s0.close())
	require.NoError(t, s1.close())

	// The first recovery migrates the legacy slot and stamps every segment.
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NoError(t, ring.segmentRingClose())

	var flushes atomic.Int64
	hook := func(string) { flushes.Add(1) }
	qwpSfTestSegmentSyncHeaderHook.Store(&hook)
	ring, _, err = qwpSfRecoverRing(dir, 4096)
	qwpSfTestSegmentSyncHeaderHook.Store(nil)
	require.NoError(t, err)
	require.NoError(t, ring.segmentRingClose())

	assert.Zero(t, flushes.Load(), "a chain that already carries the flag must not be re-flushed")
}

func TestQwpSfRecoverySanitizesSealedResidueThenRetries(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, dir, 0, 1, s0, s1)
	off := s0.publishedOffset()
	s0.buf[off+20] = 0x7f // all-zero bad header with non-zero payload farther on
	closeRecoverySegments(t, s0, s1)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrSanitizedResidue)

	b, err := os.ReadFile(filepath.Join(dir, "sf-initial.sfa"))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, len(b)-int(off)), b[off:])

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	assert.Equal(t, int64(1), ring.segmentRingPublishedFsn())
}

func TestQwpSfRecoveryLegacyPositiveHeadWithCorruptUnknownFailsClosed(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-0002.sfa", 2, "a")
	require.NoError(t, seg.close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-unknown.sfa"), []byte("bad"), 0o644))

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.True(t, errors.Is(err, qwpSfErrRecoveryFailClosed))
	assert.Contains(t, err.Error(), "cannot migrate")
}

func TestQwpSfRecoveryManifestBoundaryExtras(t *testing.T) {
	t.Run("stale-below-head-is-removed", func(t *testing.T) {
		dir := t.TempDir()
		stale := createRecoverySegment(t, dir, "sf-stale.sfa", 0, "old")
		active := createRecoverySegment(t, dir, "sf-active.sfa", 1, "live")
		createRecoveryManifest(t, dir, 1, 1, stale, active)
		closeRecoverySegments(t, stale, active)

		ring, _, err := qwpSfRecoverRing(dir, 4096)
		require.NoError(t, err)
		require.NotNil(t, ring)
		defer ring.segmentRingClose()
		_, err = os.Stat(filepath.Join(dir, "sf-stale.sfa"))
		assert.True(t, os.IsNotExist(err))
		assert.Equal(t, int64(1), ring.getActiveSegment().segmentBaseSeq())
	})

	t.Run("overlap-head-fails", func(t *testing.T) {
		dir := t.TempDir()
		overlap := createRecoverySegment(t, dir, "sf-overlap.sfa", 0, "a", "b")
		active := createRecoverySegment(t, dir, "sf-active.sfa", 1, "live")
		createRecoveryManifest(t, dir, 1, 1, overlap, active)
		closeRecoverySegments(t, overlap, active)
		_, _, err := qwpSfRecoverRing(dir, 4096)
		require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
		assert.Contains(t, err.Error(), "overlaps")
	})

	t.Run("beyond-active-fails", func(t *testing.T) {
		dir := t.TempDir()
		active := createRecoverySegment(t, dir, "sf-active.sfa", 0, "a")
		beyond := createRecoverySegment(t, dir, "sf-beyond.sfa", 1, "b")
		createRecoveryManifest(t, dir, 0, 0, active, beyond)
		closeRecoverySegments(t, active, beyond)
		_, _, err := qwpSfRecoverRing(dir, 4096)
		require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
		assert.Contains(t, err.Error(), "beyond")
	})
}

func TestQwpSfRecoveryRejectsMissingManifestHead(t *testing.T) {
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", 1, "live")
	createRecoveryManifest(t, dir, 0, 1, active)
	closeRecoverySegments(t, active)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "missing expected SF head segment")
}

func TestQwpSfRecoveryCleanDrainCrashWindowRemovesStaleFiles(t *testing.T) {
	dir := t.TempDir()
	stale := createRecoverySegment(t, dir, "sf-stale.sfa", 0, "old")
	createRecoveryManifest(t, dir, 2, 2, stale)
	closeRecoverySegments(t, stale)

	ring, manifest, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	assert.Nil(t, ring)
	assert.Nil(t, manifest)
	_, err = os.Stat(filepath.Join(dir, "sf-stale.sfa"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, qwpSfManifestFileName))
	assert.True(t, os.IsNotExist(err))
}

func TestQwpSfRecoveryRotationCrashWindow(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "clean"
		if corrupt {
			name = "corrupt-unknown"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
			active := createRecoverySegment(t, dir, "sf-active.sfa", 1)
			createRecoveryManifest(t, dir, 0, 1, sealed, active)
			closeRecoverySegments(t, sealed, active)
			if corrupt {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-unknown.sfa"), []byte("bad"), 0o644))
			}
			ring, _, err := qwpSfRecoverRing(dir, 4096)
			if corrupt {
				require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ring)
			defer ring.segmentRingClose()
			assert.Equal(t, int64(1), ring.getActiveSegment().segmentBaseSeq())
			assert.Equal(t, 1, ring.sealedSegmentCount())
			assert.Equal(t, int64(0), ring.segmentRingPublishedFsn())
		})
	}
}

func TestQwpSfRecoveryFullyTrimmedEmptyActiveKeepsSequenceDomain(t *testing.T) {
	const (
		baseSeq     int64 = 7
		segmentSize int64 = 4096
	)
	dir := t.TempDir()
	activePath := filepath.Join(dir, "sf-active.sfa")
	active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
	createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
	closeRecoverySegments(t, active)

	engine, err := qwpSfNewCursorEngine(dir, segmentSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()
	require.Equal(t, baseSeq-1, engine.enginePublishedFsn())
	require.Equal(t, baseSeq-1, engine.engineAckedFsn())
	require.Equal(t, baseSeq, engine.ring.nextSeqHint())

	require.Eventually(t, func() bool {
		return engine.ring.hotSpare.Load() != nil
	}, time.Second, time.Millisecond)

	// Fill the recovered active almost to its end, then append once more to
	// rotate it into the sealed chain.
	filler := make([]byte, segmentSize-qwpSfHeaderSize-qwpSfFrameHeaderSize-4)
	fsn, err := engine.engineAppendBlocking(context.Background(), filler)
	require.NoError(t, err)
	require.Equal(t, baseSeq, fsn)
	fsn, err = engine.engineAppendBlocking(context.Background(), []byte("next"))
	require.NoError(t, err)
	require.Equal(t, baseSeq+1, fsn)

	engine.engineAcknowledge(baseSeq)
	require.Equal(t, baseSeq, engine.engineAckedFsn())
	awaitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sender := &qwpLineSender{cursorEngine: engine, cursorSendLoop: &qwpSfSendLoop{}}
	require.NoError(t, sender.AwaitAckedFsn(awaitCtx, baseSeq))
	require.Eventually(t, func() bool {
		_, err := os.Stat(activePath)
		return os.IsNotExist(err)
	}, time.Second, time.Millisecond, "the acknowledged segment at the recovered base must trim")
}

func TestQwpSfRecoveryLegacyBaseZeroRefusesCorruptStray(t *testing.T) {
	// A legacy slot has no committed boundaries, so nothing shows the corrupt
	// file's frames already delivered — not even when the surviving chain
	// starts at base 0 and therefore owns the head. Recovery refuses to migrate
	// and leaves the bytes exactly where they are.
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	require.NoError(t, seg.close())
	stray := filepath.Join(dir, "sf-stray.sfa")
	require.NoError(t, os.WriteFile(stray, []byte("bad"), 0o644))

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "corrupt segment of unknown identity")
	_, statErr := os.Stat(stray)
	require.NoError(t, statErr)
	_, statErr = os.Stat(stray + ".corrupt")
	require.True(t, os.IsNotExist(statErr))
}

func TestQwpSfRecoveryRejectsLegacyTornEmptyBelowPositiveHead(t *testing.T) {
	dir := t.TempDir()
	torn := createRecoverySegment(t, dir, "sf-torn.sfa", 0)
	torn.buf[qwpSfHeaderSize+20] = 1
	data := createRecoverySegment(t, dir, "sf-data.sfa", 2, "a")
	closeRecoverySegments(t, torn, data)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "lost its frames")
}

func TestQwpSfRecoverySanitizesActiveTail(t *testing.T) {
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	createRecoveryManifest(t, dir, 0, 0, active)
	off := active.publishedOffset()
	active.buf[off+20] = 1
	require.NoError(t, active.close())

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	assert.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
	b, err := os.ReadFile(filepath.Join(dir, "sf-initial.sfa"))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, len(b)-int(off)), b[off:])
}

func TestQwpSfRecoveryQuarantinesTornEmptyActiveBeforeReplacement(t *testing.T) {
	const baseSeq int64 = 7
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
	createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
	closeRecoverySegments(t, active)
	path := filepath.Join(dir, "sf-active.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+20)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	require.Equal(t, baseSeq, ring.getActiveSegment().segmentBaseSeq())
	require.Zero(t, ring.getActiveSegment().segmentFrameCount())
	require.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
	require.Equal(t, baseSeq-1, ring.segmentRingPublishedFsn())

	corrupt, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, byte(1), corrupt[qwpSfHeaderSize+20], "quarantine must preserve the torn bytes")
}

// TestQwpSfRecoveryKeepsTornActiveWhenReplacementCannotBeCreated pins that a
// full disk during the replacement costs nothing permanent. Quarantining the
// torn file first would leave the slot with no segment at its committed active
// base, which the next startup refuses — so a disk that fills up and empties
// again would cost the whole slot.
func TestQwpSfRecoveryKeepsTornActiveWhenReplacementCannotBeCreated(t *testing.T) {
	const baseSeq int64 = 7
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
	createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
	closeRecoverySegments(t, active)
	path := filepath.Join(dir, "sf-active.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+20)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	originalReserve := qwpSfReserveNewBlocksFn
	qwpSfReserveNewBlocksFn = func(*os.File, int64, int64) error { return syscall.ENOSPC }
	_, _, err = qwpSfRecoverRing(dir, 4096)
	qwpSfReserveNewBlocksFn = originalReserve
	require.ErrorIs(t, err, syscall.ENOSPC)

	// The torn file is still where the manifest says the active segment is, and
	// nothing was filed away as evidence yet.
	torn, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte(1), torn[qwpSfHeaderSize+20])
	_, err = os.Stat(path + ".corrupt")
	require.True(t, os.IsNotExist(err), "nothing may be quarantined until the replacement exists")
	_, err = os.Stat(path + qwpSfTornActiveTempSuffix)
	require.True(t, os.IsNotExist(err), "the half-built replacement must not be left behind")

	// With space back, the same slot recovers.
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	require.Equal(t, baseSeq, ring.getActiveSegment().segmentBaseSeq())
	require.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
	corrupt, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, byte(1), corrupt[qwpSfHeaderSize+20], "quarantine must preserve the torn bytes")
}

func TestQwpSfQuarantinePathPreservesEarlierEvidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-active.sfa")
	require.NoError(t, os.WriteFile(path, []byte("new evidence"), 0o644))
	require.NoError(t, os.WriteFile(path+".corrupt", []byte("old evidence"), 0o644))

	target, err := qwpSfQuarantinePath(path)
	require.NoError(t, err)
	require.Equal(t, path+".corrupt-1", target)
	oldEvidence, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, []byte("old evidence"), oldEvidence)
	newEvidence, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, []byte("new evidence"), newEvidence)
}

func TestQwpSfForegroundFailClosedQuarantinesAndStartsFresh(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	require.NoError(t, os.MkdirAll(slot, 0o755))
	s0 := createRecoverySegment(t, slot, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, slot, "sf-active.sfa", 1, "b")
	createRecoveryManifest(t, slot, 0, 1, s0, s1)
	closeRecoverySegments(t, s0, s1)
	require.NoError(t, os.WriteFile(filepath.Join(slot, "sf-active.sfa"), make([]byte, 4096), 0o644))

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	require.NotNil(t, engine)
	assert.False(t, engine.engineWasRecoveredFromDisk())
	require.NoError(t, engine.engineClose())

	quarantineRoot := filepath.Join(root, "quarantined")
	entries, err := os.ReadDir(quarantineRoot)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.False(t, qwpSfIsCandidateOrphan(quarantineRoot))
}
