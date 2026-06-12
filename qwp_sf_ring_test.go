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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfRingFreshHasNoPublishedFsn(t *testing.T) {
	seg, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(seg, 4096)
	defer func() { _ = r.segmentRingClose() }()

	assert.Equal(t, int64(-1), r.segmentRingPublishedFsn())
	assert.Equal(t, int64(-1), r.segmentRingAckedFsn())
	assert.Equal(t, int64(0), r.nextSeqHint())
	assert.True(t, r.needsHotSpare())
}

func TestQwpSfRingAppendAdvancesPublishedFsn(t *testing.T) {
	seg, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(seg, 4096)
	defer func() { _ = r.segmentRingClose() }()

	for i := int64(0); i < 5; i++ {
		fsn := r.appendOrFsn([]byte("frame"))
		assert.Equal(t, i, fsn, "iteration %d", i)
	}
	assert.Equal(t, int64(4), r.segmentRingPublishedFsn())
	assert.Equal(t, int64(5), r.nextSeqHint())
}

func TestQwpSfRingBackpressureWhenNoSpare(t *testing.T) {
	const segSize int64 = 64
	seg, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(seg, segSize)
	defer func() { _ = r.segmentRingClose() }()

	payload := []byte("12345678") // 8 bytes payload, 16 byte total framing
	// Fill the active until tryAppend refuses.
	for {
		fsn := r.appendOrFsn(payload)
		if fsn == qwpSfBackpressureNoSpare {
			return
		}
		require.GreaterOrEqual(t, fsn, int64(0))
	}
}

func TestQwpSfRingRotatesIntoHotSpare(t *testing.T) {
	const segSize int64 = 64
	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()

	// Pre-install a spare.
	spare, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare))
	assert.False(t, r.needsHotSpare())

	// Fill the first segment until the next append rotates.
	payload := make([]byte, 16) // 24 bytes total framing
	rotated := false
	expectedNextFsn := int64(0)
	for !rotated {
		fsn := r.appendOrFsn(payload)
		require.NotEqual(t, qwpSfBackpressureNoSpare, fsn, "needed multiple rotations")
		require.NotEqual(t, qwpSfPayloadTooLarge, fsn)
		assert.Equal(t, expectedNextFsn, fsn)
		expectedNextFsn++
		// Check whether rotation has happened: getActiveSegment now
		// returns the spare and sealed list contains the original.
		if r.getActiveSegment() == spare {
			rotated = true
		}
	}
	// First segment should be in sealed list.
	sealed := r.getSealedSegments()
	require.Len(t, sealed, 1)
	assert.Equal(t, first, sealed[0])
	// Hot spare should be cleared.
	assert.True(t, r.needsHotSpare())
}

// TestQwpSfRingBackupWakeupRearmsPerActiveSegment pins the contract
// that the high-water-mark backup wakeup nudges the segment manager
// once per active segment: every freshly promoted active must re-arm
// it so a stalled spare provision on the new segment can still be
// rescued. A latch that survived rotation would fire the backup only
// once over the ring's whole lifetime.
func TestQwpSfRingBackupWakeupRearmsPerActiveSegment(t *testing.T) {
	// 512-byte segments put the 75% mark at 384, leaving several
	// 32-byte frames of room before the segment fills, so the active
	// crosses its HWM well before it rotates.
	const segSize int64 = 512
	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()

	var wakeups int
	r.managerWakeup = func() { wakeups++ }

	payload := make([]byte, 24) // 32 bytes on the wire with the frame header

	// drivePastHwm appends until the active is past its high-water mark
	// (plus one more to prove repeated crossings coalesce), asserting no
	// rotation or backpressure happens along the way.
	drivePastHwm := func() {
		for r.getActiveSegment().publishedOffset() < r.signalAtBytes {
			fsn := r.appendOrFsn(payload)
			require.GreaterOrEqual(t, fsn, int64(0), "unexpected backpressure/oversize before HWM")
		}
		require.GreaterOrEqual(t, r.appendOrFsn(payload), int64(0))
	}

	// First active segment, no spare staged: the backup fires exactly
	// once however many frames land past the mark.
	drivePastHwm()
	require.Equal(t, 1, wakeups, "first active should fire one backup wakeup")
	require.True(t, r.wakeupRequestedForActive)

	// Stage a spare and fill the rest so the next full-segment append
	// rotates into it. Rotation must re-arm the per-segment backup.
	spare, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare))
	for r.getActiveSegment() != spare {
		fsn := r.appendOrFsn(payload)
		require.NotEqual(t, qwpSfBackpressureNoSpare, fsn)
		require.NotEqual(t, qwpSfPayloadTooLarge, fsn)
	}
	require.False(t, r.wakeupRequestedForActive, "rotation must re-arm the backup wakeup")

	// Isolate the second active segment, then drive it past its own HWM
	// (still no spare). The backup must fire again — the latched-flag bug
	// suppressed this entirely.
	wakeups = 0
	drivePastHwm()
	require.Equal(t, 1, wakeups, "freshly promoted active must re-fire its backup wakeup")
}

func TestQwpSfRingTrimsAckedSegments(t *testing.T) {
	// Each segment fits exactly two minimal frames (16-byte payloads,
	// 8-byte envelopes). 24 (header) + 2*(8+16) = 72.
	const segSize int64 = 72
	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()

	spare, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare))

	payload := make([]byte, 16)
	// Three appends: two land in the first active, the third forces
	// rotation into the spare.
	for i := 0; i < 3; i++ {
		fsn := r.appendOrFsn(payload)
		require.GreaterOrEqual(t, fsn, int64(0), "iteration %d", i)
	}
	sealed := r.getSealedSegments()
	require.Len(t, sealed, 1)
	lastSeqInFirst := sealed[0].segmentBaseSeq() + sealed[0].segmentFrameCount() - 1
	r.acknowledge(lastSeqInFirst)

	trim := r.drainTrimmable()
	require.Len(t, trim, 1)
	assert.Equal(t, sealed[0], trim[0])
	assert.Len(t, r.getSealedSegments(), 0)
	for _, s := range trim {
		_ = s.close()
	}
}

func TestQwpSfRingSnapshotSealedSegments(t *testing.T) {
	const segSize int64 = 72
	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()

	spare, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare))

	// Three appends → one segment sealed, one active.
	for i := 0; i < 3; i++ {
		_ = r.appendOrFsn(make([]byte, 16))
	}
	target := make([]*qwpSfSegment, 4)
	n := r.snapshotSealedSegments(target)
	assert.Equal(t, 1, n)
	assert.NotNil(t, target[0])

	// Too-small target returns -1 to signal "buffer too small".
	tiny := make([]*qwpSfSegment, 0)
	assert.Equal(t, -1, r.snapshotSealedSegments(tiny))
}

func TestQwpSfRingFindSegmentContaining(t *testing.T) {
	const segSize int64 = 72 // exactly two minimal frames
	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()

	spare, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare))

	payload := make([]byte, 16)
	var fsns []int64
	for i := 0; i < 3; i++ {
		fsns = append(fsns, r.appendOrFsn(payload))
	}
	seg := r.findSegmentContaining(fsns[0])
	require.NotNil(t, seg)
	assert.Equal(t, first, seg)
	seg = r.findSegmentContaining(fsns[len(fsns)-1])
	require.NotNil(t, seg)
	assert.Equal(t, spare, seg)
	assert.Nil(t, r.findSegmentContaining(999))
}

func TestQwpSfRingTotalSegmentBytes(t *testing.T) {
	const segSize int64 = 64
	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()

	assert.Equal(t, segSize, r.totalSegmentBytes())
	spare, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare))
	assert.Equal(t, segSize*2, r.totalSegmentBytes())
}

func TestQwpSfRingInstallHotSpareRejectsDouble(t *testing.T) {
	first, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, 4096)
	defer func() { _ = r.segmentRingClose() }()

	spare1, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare1))

	spare2, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	err = r.installHotSpare(spare2)
	require.Error(t, err)
	_ = spare2.close()
}

func TestQwpSfRingInstallHotSpareRejectsAfterClose(t *testing.T) {
	first, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, 4096)
	require.NoError(t, r.segmentRingClose())

	spare, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	err = r.installHotSpare(spare)
	assert.ErrorIs(t, err, qwpSfErrRingClosed)
	_ = spare.close()
}

func TestQwpSfRingOpenExistingNilOnEmpty(t *testing.T) {
	dir := t.TempDir()
	r, err := qwpSfOpenRing(dir, 4096)
	require.NoError(t, err)
	assert.Nil(t, r)
}

func TestQwpSfRingOpenExistingRecoversInOrder(t *testing.T) {
	dir := t.TempDir()

	// Create three segments with frames.
	for _, base := range []int64{0, 5, 10} {
		path := filepath.Join(dir, "sf-"+formatHex16(uint64(base))+".sfa")
		seg, err := qwpSfCreateSegment(path, base, 4096)
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			_, err := seg.tryAppend([]byte{byte(base), byte(i)})
			require.NoError(t, err)
		}
		require.NoError(t, seg.close())
	}

	r, err := qwpSfOpenRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, r)
	defer func() { _ = r.segmentRingClose() }()

	// Highest baseSeq becomes active; other two go into sealed.
	active := r.getActiveSegment()
	require.NotNil(t, active)
	assert.Equal(t, int64(10), active.segmentBaseSeq())
	sealed := r.getSealedSegments()
	require.Len(t, sealed, 2)
	assert.Equal(t, int64(0), sealed[0].segmentBaseSeq())
	assert.Equal(t, int64(5), sealed[1].segmentBaseSeq())
	// Counters should reflect 3 segments × 5 frames = 15 frames total
	// = next FSN 15.
	assert.Equal(t, int64(15), r.nextSeqHint())
	assert.Equal(t, int64(14), r.segmentRingPublishedFsn())
}

func TestQwpSfRingOpenExistingRejectsFsnGap(t *testing.T) {
	dir := t.TempDir()
	// Create two segments with non-contiguous FSN ranges.
	for _, c := range []struct {
		base   int64
		frames int
	}{
		{base: 0, frames: 5},
		{base: 100, frames: 5},
	} {
		path := filepath.Join(dir, "sf-"+formatHex16(uint64(c.base))+".sfa")
		seg, err := qwpSfCreateSegment(path, c.base, 4096)
		require.NoError(t, err)
		for i := 0; i < c.frames; i++ {
			_, err := seg.tryAppend([]byte{byte(i)})
			require.NoError(t, err)
		}
		require.NoError(t, seg.close())
	}
	r, err := qwpSfOpenRing(dir, 4096)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FSN gap")
	assert.Nil(t, r)
}

func TestQwpSfRingOpenExistingQuarantinesCorruptFirstFrame(t *testing.T) {
	// A bit-flip in the first frame's CRC makes scanFrames bail out at
	// HEADER_SIZE with frameCount=0 — but valid frames may follow. The
	// pre-fix recovery path would silently unlink the file as an "empty
	// hot spare", destroying every surviving frame. The fix quarantines
	// torn-tail-bearing files to <path>.corrupt instead so a postmortem
	// can recover what's left.
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-corrupt.sfa")
	{
		seg, err := qwpSfCreateSegment(path, 0, 4096)
		require.NoError(t, err)
		_, err = seg.tryAppend([]byte("frame-zero"))
		require.NoError(t, err)
		_, err = seg.tryAppend([]byte("frame-one"))
		require.NoError(t, err)
		// Flip a byte in frame[0]'s CRC. The frame is at HEADER_SIZE;
		// CRC is the first 4 bytes of the frame.
		buf := seg.address()
		buf[qwpSfHeaderSize] ^= 0xFF
		require.NoError(t, seg.close())
	}

	r, err := qwpSfOpenRing(dir, 4096)
	require.NoError(t, err)
	assert.Nil(t, r)

	// Original file is gone; quarantine sentinel is in its place.
	_, statErr := os.Stat(path)
	assert.True(t, os.IsNotExist(statErr), "original .sfa should have been renamed")
	_, statErr = os.Stat(path + ".corrupt")
	assert.NoError(t, statErr, "<path>.corrupt should exist after quarantine")
}

func TestQwpSfRingAcknowledgeClampsAtPublishedFsn(t *testing.T) {
	// Defense-in-depth: a malformed/poisoned ACK with a wireSeq beyond
	// publishedFsn must NOT advance ackedFsn past what the producer has
	// actually written, otherwise the segment manager could trim
	// segments the I/O thread is still iterating.
	seg, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(seg, 4096)
	defer func() { _ = r.segmentRingClose() }()

	r.appendOrFsn([]byte("a"))
	r.appendOrFsn([]byte("b"))
	require.Equal(t, int64(1), r.segmentRingPublishedFsn())

	r.acknowledge(1 << 30)
	assert.Equal(t, int64(1), r.segmentRingAckedFsn())
}

// formatHex16 mirrors the segment-manager filename format.
func formatHex16(v uint64) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		out[i] = hex[v&0xF]
		v >>= 4
	}
	return string(out)
}
