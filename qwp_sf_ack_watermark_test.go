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
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeForeignAckWatermark hand-writes the normative dual-slot bytes a
// different client (e.g. the Java reference's AckWatermark.java) would
// leave on disk: magic 'AKW1' little-endian at offset 0, reserved 0
// at offset 4, the FSN little-endian at offset 8. Used to prove the Go
// client honours a watermark it did not itself write (sf-client.md
// §19 interop).
func writeForeignAckWatermark(t *testing.T, slotDir string, fsn int64) {
	t.Helper()
	buf := make([]byte, qwpSfAckWatermarkFileSize)
	qwpSfEncodeDualRecord(buf[qwpSfDualRecordSlotSize:qwpSfDualRecordSlotSize+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, 1, fsn, 0)
	path := filepath.Join(slotDir, qwpSfAckWatermarkFileName)
	require.NoError(t, os.WriteFile(path, buf, 0o644))
}

func readAckWatermarkFileBytes(t *testing.T, slotDir string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(slotDir, qwpSfAckWatermarkFileName))
	require.NoError(t, err)
	return b
}

func TestQwpSfAckWatermarkFreshFileIsInvalid(t *testing.T) {
	dir := t.TempDir()
	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()

	assert.Equal(t, qwpSfAckWatermarkInvalid, w.read(),
		"a freshly created (zero-filled) watermark must read INVALID")

	b := readAckWatermarkFileBytes(t, dir)
	require.Len(t, b, int(qwpSfAckWatermarkFileSize))
	assert.Equal(t, make([]byte, qwpSfAckWatermarkFileSize), b,
		"open() must not stamp anything until the first persist")
}

func TestQwpSfAckWatermarkPersistGateAndFormat(t *testing.T) {
	dir := t.TempDir()
	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)

	advanced, err := w.persistIfAdvanced(7)
	require.NoError(t, err)
	assert.True(t, advanced, "first advance writes")
	advanced, err = w.persistIfAdvanced(7)
	require.NoError(t, err)
	assert.False(t, advanced, "same value does not re-write")
	advanced, err = w.persistIfAdvanced(3)
	require.NoError(t, err)
	assert.False(t, advanced, "a regression never writes")
	advanced, err = w.persistIfAdvanced(9)
	require.NoError(t, err)
	assert.True(t, advanced, "a higher value writes")
	assert.Equal(t, int64(9), w.read())
	require.NoError(t, w.close())

	// Generation 2 is in slot 0; generation 1 remains intact in slot 1.
	b := readAckWatermarkFileBytes(t, dir)
	require.Len(t, b, int(qwpSfAckWatermarkFileSize))
	assert.Equal(t, qwpSfAckWatermarkMagic, binary.LittleEndian.Uint32(b[0:4]))
	assert.Equal(t, qwpSfDualRecordVersion, binary.LittleEndian.Uint32(b[4:8]))
	assert.Equal(t, int64(2), int64(binary.LittleEndian.Uint64(b[8:16])))
	assert.Equal(t, int64(9), int64(binary.LittleEndian.Uint64(b[16:24])))
	assert.Equal(t, qwpSfAckWatermarkMagic, binary.LittleEndian.Uint32(b[qwpSfDualRecordSlotSize:qwpSfDualRecordSlotSize+4]))

	// Reopen preserves the value (magic already stamped).
	w2 := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w2)
	defer func() { _ = w2.close() }()
	assert.Equal(t, int64(9), w2.read())
	// The write gate is seeded from the selected durable record, so an equal or
	// regressing value remains a no-op after reopen.
	advanced, err = w2.persistIfAdvanced(-1)
	require.NoError(t, err)
	assert.False(t, advanced)
	advanced, err = w2.persistIfAdvanced(10)
	require.NoError(t, err)
	assert.True(t, advanced)
	assert.Equal(t, int64(10), w2.read())
}

func TestQwpSfAckWatermarkRejectsGenerationWraparound(t *testing.T) {
	dir := t.TempDir()
	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()

	w.generation = math.MaxInt64
	advanced, err := w.persistIfAdvanced(0)
	require.ErrorContains(t, err, "ack watermark generation overflow")
	require.ErrorIs(t, err, qwpSfErrGenerationOverflow)
	assert.False(t, advanced)
	assert.Equal(t, qwpSfAckWatermarkInvalid, w.read())
}

func TestQwpSfAckWatermarkMaxGenerationAllowsReopenedNoOps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfAckWatermarkFileName)
	image := make([]byte, qwpSfAckWatermarkFileSize)
	off := int(qwpSfDualRecordSlotSize) // MaxInt64 is odd, so it belongs in slot 1.
	qwpSfEncodeDualRecord(image[off:off+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, math.MaxInt64, 41, 0)
	require.NoError(t, os.WriteFile(path, image, 0o644))

	w, err := qwpSfAckWatermarkOpenRequired(dir)
	require.NoError(t, err)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()

	advanced, err := w.persistIfAdvanced(41)
	require.NoError(t, err, "equal durable FSN is a no-op, not an overflow")
	assert.False(t, advanced)
	advanced, err = w.persistIfAdvanced(40)
	require.NoError(t, err, "regressing durable FSN is a no-op, not an overflow")
	assert.False(t, advanced)
	advanced, err = w.persistIfAdvanced(42)
	require.ErrorIs(t, err, qwpSfErrGenerationOverflow, "only a true advance overflows")
	assert.False(t, advanced)
}

func TestQwpSfAckWatermarkFallsBackFromCorruptNewestRecordOnMmapOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfAckWatermarkFileName)
	image := make([]byte, qwpSfAckWatermarkFileSize)
	// Generation 1 belongs in slot 1 and is the older committed value.
	oldOff := int(qwpSfDualRecordSlotSize)
	qwpSfEncodeDualRecord(image[oldOff:oldOff+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, 1, 41, 0)
	// Generation 2 belongs in slot 0. Corrupt only its CRC so the real open
	// path must reject it and select generation 1 from the mapped file.
	qwpSfEncodeDualRecord(image[:qwpSfDualRecordSize], qwpSfAckWatermarkMagic, 2, 42, 0)
	image[qwpSfDualRecordSize-1] ^= 0xff
	require.NoError(t, os.WriteFile(path, image, 0o644))

	w, err := qwpSfAckWatermarkOpenRequired(dir)
	require.NoError(t, err)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()
	require.NotEmpty(t, w.buf, "open must exercise the mmap-backed path")
	assert.Equal(t, int64(1), w.generation)
	assert.Equal(t, int64(41), w.read())
}

func TestQwpSfAckWatermarkHonoursForeignBytes(t *testing.T) {
	dir := t.TempDir()
	writeForeignAckWatermark(t, dir, 42)

	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()
	assert.Equal(t, int64(42), w.read(),
		"a watermark written by another client must be read byte-for-byte")
}

func TestQwpSfAckWatermarkValidityPredicate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		first  int64
		second int64
		valid  bool
	}{
		{name: "empty-prefix", first: -1, second: 0, valid: true},
		{name: "acked-prefix", first: 42, second: 0, valid: true},
		{name: "reserved-field-is-not-a-boundary", first: 42, second: math.MinInt64, valid: true},
		{name: "below-empty-prefix", first: -2, second: 0, valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, qwpSfAckWatermarkRecordValid(qwpSfDualRecord{
				first:  tc.first,
				second: tc.second,
			}))
		})
	}
}

func TestQwpSfAckWatermarkExistingBlockReservationFailureStopsOpen(t *testing.T) {
	dir := t.TempDir()
	writeForeignAckWatermark(t, dir, 42)

	originalWriteAt := qwpSfAckWatermarkWriteAt.load()
	writeCalls := 0
	qwpSfAckWatermarkWriteAt.store(func(*os.File, []byte, int64) (int, error) {
		writeCalls++
		return 0, syscall.ENOSPC
	})
	t.Cleanup(func() { qwpSfAckWatermarkWriteAt.store(originalWriteAt) })

	w, err := qwpSfAckWatermarkOpenRequired(dir)
	if w != nil {
		_ = w.close()
	}
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.Nil(t, w)
	require.Equal(t, 1, writeCalls)
}

// TestQwpSfAckWatermarkNewBlockReservationFailureStopsOpen pins that a file
// this call creates gets the same block-forcing write as one it finds. On a
// filesystem with no reservation primitive -- NFS, SMB, overlayfs, or the
// generic-unix build -- qwpSfAllocate reports success having reserved nothing,
// so without the write the fresh file is sparse. The manager goroutine stores
// through that mapping, and on a full disk that is a SIGBUS with no error to
// report and no goroutine to report it on.
func TestQwpSfAckWatermarkNewBlockReservationFailureStopsOpen(t *testing.T) {
	dir := t.TempDir()

	originalWriteAt := qwpSfAckWatermarkWriteAt.load()
	writeCalls := 0
	qwpSfAckWatermarkWriteAt.store(func(*os.File, []byte, int64) (int, error) {
		writeCalls++
		return 0, syscall.ENOSPC
	})
	t.Cleanup(func() { qwpSfAckWatermarkWriteAt.store(originalWriteAt) })

	w, err := qwpSfAckWatermarkOpenRequired(dir)
	if w != nil {
		_ = w.close()
	}
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.Nil(t, w)
	require.Equal(t, 1, writeCalls)
}

// TestQwpSfAckWatermarkResetBlockReservationFailureStopsOpen covers the third
// way in: a correctly sized file whose records are both unreadable is
// truncated and rebuilt, which leaves it as freshly allocated as the create
// path and needing the same write.
func TestQwpSfAckWatermarkResetBlockReservationFailureStopsOpen(t *testing.T) {
	dir := t.TempDir()
	buf := make([]byte, qwpSfAckWatermarkFileSize)
	binary.LittleEndian.PutUint32(buf[0:4], 0xDEADBEEF)
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfAckWatermarkFileName), buf, 0o644))

	originalWriteAt := qwpSfAckWatermarkWriteAt.load()
	writeCalls := 0
	qwpSfAckWatermarkWriteAt.store(func(f *os.File, p []byte, off int64) (int, error) {
		writeCalls++
		if writeCalls == 1 {
			// The pre-mmap write for the file as found must still succeed, or
			// the reset below is never reached.
			return originalWriteAt(f, p, off)
		}
		return 0, syscall.ENOSPC
	})
	t.Cleanup(func() { qwpSfAckWatermarkWriteAt.store(originalWriteAt) })

	w, err := qwpSfAckWatermarkOpenRequired(dir)
	if w != nil {
		_ = w.close()
	}
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.Nil(t, w)
	require.Equal(t, 2, writeCalls)
}

func TestQwpSfAckWatermarkBadMagicIsInvalid(t *testing.T) {
	dir := t.TempDir()
	buf := make([]byte, qwpSfAckWatermarkFileSize)
	binary.LittleEndian.PutUint32(buf[0:4], 0xDEADBEEF)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(123))
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfAckWatermarkFileName), buf, 0o644))

	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()
	assert.Equal(t, qwpSfAckWatermarkInvalid, w.read(),
		"a wrong-magic file must read INVALID so recovery falls back")
}

func TestQwpSfAckWatermarkWrongSizeRecreated(t *testing.T) {
	dir := t.TempDir()
	// A truncated/garbage 4-byte file: mmapping its full 8192 bytes would
	// SIGBUS, so open() must recreate it at FILE_SIZE.
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfAckWatermarkFileName),
		[]byte{1, 2, 3, 4}, 0o644))

	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()
	assert.Equal(t, qwpSfAckWatermarkInvalid, w.read())

	st, err := os.Stat(filepath.Join(dir, qwpSfAckWatermarkFileName))
	require.NoError(t, err)
	assert.Equal(t, qwpSfAckWatermarkFileSize, st.Size())
}

func TestQwpSfAckWatermarkLegacy16ByteFileIsReset(t *testing.T) {
	dir := t.TempDir()
	legacy := make([]byte, 16)
	binary.LittleEndian.PutUint32(legacy[0:4], qwpSfAckWatermarkMagic)
	binary.LittleEndian.PutUint64(legacy[8:16], 42)
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfAckWatermarkFileName), legacy, 0o644))

	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	defer func() { _ = w.close() }()
	assert.Equal(t, qwpSfAckWatermarkInvalid, w.read())
	st, err := os.Stat(filepath.Join(dir, qwpSfAckWatermarkFileName))
	require.NoError(t, err)
	assert.Equal(t, qwpSfDualRecordFileSize, st.Size())
}

func TestQwpSfEngineRecoveryRequiresAckWatermark(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	createRecoveryManifest(t, dir, 0, 0, seg)
	require.NoError(t, seg.close())
	require.NoError(t, os.Mkdir(filepath.Join(dir, qwpSfAckWatermarkFileName), 0o755))

	engine, err := qwpSfNewCursorEngineForDrainer(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.Error(t, err)
	assert.Nil(t, engine)
	assert.Contains(t, err.Error(), "could not open required ack watermark")
}

func TestQwpSfAckWatermarkClosedAndNilSafe(t *testing.T) {
	dir := t.TempDir()
	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	advanced, err := w.persistIfAdvanced(5)
	require.NoError(t, err)
	require.True(t, advanced)
	require.NoError(t, w.close())

	assert.Equal(t, qwpSfAckWatermarkInvalid, w.read(), "read after close is INVALID")
	advanced, err = w.persistIfAdvanced(99)
	require.NoError(t, err)
	assert.False(t, advanced, "persist after close is a no-op")
	assert.NoError(t, w.close(), "close is idempotent")

	var nilW *qwpSfAckWatermark
	assert.Equal(t, qwpSfAckWatermarkInvalid, nilW.read())
	advanced, err = nilW.persistIfAdvanced(1)
	require.NoError(t, err)
	assert.False(t, advanced)
	assert.NoError(t, nilW.close())

	assert.Nil(t, qwpSfAckWatermarkOpen(""), "empty slot dir yields no watermark")
}

func TestQwpSfAckWatermarkRemoveOrphan(t *testing.T) {
	dir := t.TempDir()
	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	advanced, err := w.persistIfAdvanced(1)
	require.NoError(t, err)
	require.True(t, advanced)
	require.NoError(t, w.close())

	path := filepath.Join(dir, qwpSfAckWatermarkFileName)
	_, err = os.Stat(path)
	require.NoError(t, err)

	qwpSfAckWatermarkRemoveOrphan(dir)
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "removeOrphan must unlink the file")

	// Best-effort: must not panic on a missing file or empty dir.
	qwpSfAckWatermarkRemoveOrphan(dir)
	qwpSfAckWatermarkRemoveOrphan("")
}

// TestQwpSfEngineRecoveryHonoursForeignWatermark is the regression
// test for the review: a Go engine (the same path a drainer uses to
// adopt an orphan slot) recovering a slot whose .ack-watermark was
// written by another client MUST seed ackedFsn from it, so replay
// resumes past the already-durable prefix instead of re-sending every
// frame in the lowest surviving segment (row-level duplicates against
// a still-alive server).
func TestQwpSfEngineRecoveryHonoursForeignWatermark(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096

	// Session 1: write 6 frames, close with no acks. Files survive;
	// the manager never advanced the watermark (no acks), so it is
	// present but zero-magic.
	{
		e, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		for i := 0; i < 6; i++ {
			_, err := e.engineAppendBlocking(context.Background(), []byte{byte(i)})
			require.NoError(t, err)
		}
		require.Equal(t, int64(5), e.enginePublishedFsn())
		require.NoError(t, e.engineClose())
	}

	// A prior client (e.g. the Java reference) received cumulative
	// durable acks through FSN 3 and persisted that watermark.
	writeForeignAckWatermark(t, dir, 3)

	// Session 2 (== the drainer-adoption code path): the seed must be
	// the watermark, not lowestBase-1 (= -1 here).
	e2, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e2.engineClose() }()
	assert.True(t, e2.engineWasRecoveredFromDisk())
	assert.Equal(t, int64(5), e2.enginePublishedFsn())
	assert.Equal(t, int64(3), e2.engineAckedFsn(),
		"recovery must honour the foreign .ack-watermark; replay resumes at FSN 4")
}

// TestQwpSfEngineRecoveryRejectsCorruptWatermark covers the
// sf-client.md §5.4 / §18.1 bound: a watermark above publishedFsn is
// corruption and MUST be ignored, falling back to the segment-derived
// seed so the un-acked tail still replays (no silent data loss).
func TestQwpSfEngineRecoveryRejectsCorruptWatermark(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096
	{
		e, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		for i := 0; i < 4; i++ {
			_, err := e.engineAppendBlocking(context.Background(), []byte{byte(i)})
			require.NoError(t, err)
		}
		require.Equal(t, int64(3), e.enginePublishedFsn())
		require.NoError(t, e.engineClose())
	}
	// Watermark FSN 99 >> publishedFsn 3 — bit-rot / torn write.
	writeForeignAckWatermark(t, dir, 99)

	e2, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e2.engineClose() }()
	assert.Equal(t, int64(3), e2.enginePublishedFsn())
	assert.Equal(t, int64(-1), e2.engineAckedFsn(),
		"a watermark past publishedFsn must be rejected; tail still replays")
}

// TestQwpSfEngineWatermarkPersistedByManager proves the write half:
// the segment manager persists ackedFsn so a later Go session (or a
// Go→Go drainer adoption) resumes past the durable prefix too.
func TestQwpSfEngineWatermarkPersistedByManager(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096
	{
		e, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		for i := 0; i < 8; i++ {
			_, err := e.engineAppendBlocking(context.Background(), []byte{byte(i)})
			require.NoError(t, err)
		}
		// Ack a prefix only — the slot is NOT fully drained, so the
		// files + watermark survive engineClose.
		e.engineAcknowledge(4)

		// The manager polls on a ~1ms tick; wait for it to flush the
		// watermark through to disk in the normative format.
		require.Eventually(t, func() bool {
			b, err := os.ReadFile(filepath.Join(dir, qwpSfAckWatermarkFileName))
			if err != nil || int64(len(b)) != qwpSfAckWatermarkFileSize {
				return false
			}
			r0, ok0 := qwpSfDecodeDualRecord(b[:qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
			off := int(qwpSfDualRecordSlotSize)
			r1, ok1 := qwpSfDecodeDualRecord(b[off:off+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
			rec, ok := qwpSfSelectDualRecord(r0, ok0, r1, ok1)
			return ok && rec.first == 4
		}, 2*time.Second, 5*time.Millisecond)

		require.NoError(t, e.engineClose())
	}

	e2, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e2.engineClose() }()
	assert.Equal(t, int64(7), e2.enginePublishedFsn())
	assert.Equal(t, int64(4), e2.engineAckedFsn(),
		"the manager-persisted watermark must seed the next session's ackedFsn")
}
