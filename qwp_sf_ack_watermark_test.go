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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeForeignAckWatermark hand-writes the 16 normative bytes a
// different client (e.g. the Java reference's AckWatermark.java) would
// leave on disk: magic 'AKW1' little-endian at offset 0, reserved 0
// at offset 4, the FSN little-endian at offset 8. Used to prove the Go
// client honours a watermark it did not itself write (sf-client.md
// §19 interop).
func writeForeignAckWatermark(t *testing.T, slotDir string, fsn int64) {
	t.Helper()
	buf := make([]byte, qwpSfAckWatermarkFileSize)
	binary.LittleEndian.PutUint32(buf[0:4], qwpSfAckWatermarkMagic)
	// bytes[4:8] reserved == 0
	binary.LittleEndian.PutUint64(buf[8:16], uint64(fsn))
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

	assert.True(t, w.persistIfAdvanced(7), "first advance writes")
	assert.False(t, w.persistIfAdvanced(7), "same value does not re-write")
	assert.False(t, w.persistIfAdvanced(3), "a regression never writes")
	assert.True(t, w.persistIfAdvanced(9), "a higher value writes")
	assert.Equal(t, int64(9), w.read())
	require.NoError(t, w.close())

	// On-disk bytes must match the normative little-endian layout so a
	// Java drainer can read them.
	b := readAckWatermarkFileBytes(t, dir)
	require.Len(t, b, 16)
	assert.Equal(t, qwpSfAckWatermarkMagic, binary.LittleEndian.Uint32(b[0:4]))
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(b[4:8]), "reserved must be zero")
	assert.Equal(t, int64(9), int64(binary.LittleEndian.Uint64(b[8:16])))

	// Reopen preserves the value (magic already stamped).
	w2 := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w2)
	defer func() { _ = w2.close() }()
	assert.Equal(t, int64(9), w2.read())
	// lastPersistedAck resets per session, but the gate still honours
	// the on-disk value's monotonicity once we advance past it.
	assert.False(t, w2.persistIfAdvanced(-1))
	assert.True(t, w2.persistIfAdvanced(10))
	assert.Equal(t, int64(10), w2.read())
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
	// A truncated/garbage 4-byte file: mmapping its full 16 bytes would
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

func TestQwpSfAckWatermarkClosedAndNilSafe(t *testing.T) {
	dir := t.TempDir()
	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	require.True(t, w.persistIfAdvanced(5))
	require.NoError(t, w.close())

	assert.Equal(t, qwpSfAckWatermarkInvalid, w.read(), "read after close is INVALID")
	assert.False(t, w.persistIfAdvanced(99), "persist after close is a no-op")
	assert.NoError(t, w.close(), "close is idempotent")

	var nilW *qwpSfAckWatermark
	assert.Equal(t, qwpSfAckWatermarkInvalid, nilW.read())
	assert.False(t, nilW.persistIfAdvanced(1))
	assert.NoError(t, nilW.close())

	assert.Nil(t, qwpSfAckWatermarkOpen(""), "empty slot dir yields no watermark")
}

func TestQwpSfAckWatermarkRemoveOrphan(t *testing.T) {
	dir := t.TempDir()
	w := qwpSfAckWatermarkOpen(dir)
	require.NotNil(t, w)
	require.True(t, w.persistIfAdvanced(1))
	require.NoError(t, w.close())

	path := filepath.Join(dir, qwpSfAckWatermarkFileName)
	_, err := os.Stat(path)
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
			if err != nil || len(b) != 16 {
				return false
			}
			return binary.LittleEndian.Uint32(b[0:4]) == qwpSfAckWatermarkMagic &&
				int64(binary.LittleEndian.Uint64(b[8:16])) == 4
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
