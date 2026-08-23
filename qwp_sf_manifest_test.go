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
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfDualRecordJavaGoldenBytes(t *testing.T) {
	tests := []struct {
		name                      string
		magic                     uint32
		generation, first, second int64
		golden                    string
	}{
		{
			name:       "manifest",
			magic:      qwpSfManifestMagic,
			generation: 1,
			first:      2,
			second:     7,
			golden:     "53464d3101000000010000000000000002000000000000000700000000000000000000000000000000000000000000000000000000000000000000007a4d6d90",
		},
		{
			name:       "ack-watermark",
			magic:      qwpSfAckWatermarkMagic,
			generation: 1,
			first:      42,
			second:     0,
			golden:     "414b57310100000001000000000000002a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000ba81507e",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want, err := hex.DecodeString(tc.golden)
			require.NoError(t, err)
			got := make([]byte, qwpSfDualRecordSize)
			qwpSfEncodeDualRecord(got, tc.magic, tc.generation, tc.first, tc.second)
			assert.Equal(t, want, got)

			rec, ok := qwpSfDecodeDualRecord(want, tc.magic, func(qwpSfDualRecord) bool { return true })
			require.True(t, ok, "Java golden must decode")
			assert.Equal(t, tc.generation, rec.generation)
			assert.Equal(t, tc.first, rec.first)
			assert.Equal(t, tc.second, rec.second)
		})
	}
}

func TestQwpSfManifestRoundTripAlternationAndClamps(t *testing.T) {
	dir := t.TempDir()
	m, err := qwpSfManifestCreate(dir, 10, 20)
	require.NoError(t, err)
	path := filepath.Join(dir, qwpSfManifestFileName)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, b, int(qwpSfDualRecordFileSize))
	assert.Equal(t, make([]byte, qwpSfDualRecordSize), b[:qwpSfDualRecordSize])
	assert.Equal(t, qwpSfManifestMagic, binary.LittleEndian.Uint32(b[qwpSfDualRecordSlotSize:qwpSfDualRecordSlotSize+4]))

	require.NoError(t, m.update(5, 15))
	assert.Equal(t, int64(1), m.generation, "independent regressions clamp to a no-op")
	require.NoError(t, m.update(12, 18))
	assert.Equal(t, int64(12), m.headBase)
	assert.Equal(t, int64(20), m.activeBase)
	require.NoError(t, m.update(12, 25))
	require.NoError(t, m.close())

	reopened, err := qwpSfManifestOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, reopened)
	defer reopened.close()
	assert.Equal(t, int64(12), reopened.headBase)
	assert.Equal(t, int64(25), reopened.activeBase)
}

func TestQwpSfManifestRejectsGenerationOverflow(t *testing.T) {
	dir := t.TempDir()
	m, err := qwpSfManifestCreate(dir, 0, 0)
	require.NoError(t, err)
	defer func() { _ = m.close() }()

	m.generation = math.MaxInt64
	err = m.update(0, 1)
	require.ErrorContains(t, err, "manifest generation overflow")
	require.Equal(t, int64(0), m.activeBase)
}

func TestQwpSfManifestSurvivesOneTornRecord(t *testing.T) {
	dir := t.TempDir()
	m, err := qwpSfManifestCreate(dir, 10, 20)
	require.NoError(t, err)
	require.NoError(t, m.update(12, 25))
	require.NoError(t, m.close())

	path := filepath.Join(dir, qwpSfManifestFileName)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, qwpSfDualRecordSize), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reopened, err := qwpSfManifestOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, reopened)
	defer reopened.close()
	assert.Equal(t, int64(10), reopened.headBase)
	assert.Equal(t, int64(20), reopened.activeBase)
}

func TestQwpSfManifestQuarantinesCreationDebris(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name: "wrong-size", body: []byte("bad")},
		{name: "both-invalid", body: make([]byte, qwpSfDualRecordFileSize)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, qwpSfManifestFileName)
			require.NoError(t, os.WriteFile(path, tc.body, 0o644))
			m, err := qwpSfManifestOpen(dir)
			require.NoError(t, err)
			assert.Nil(t, m)
			_, err = os.Stat(path + ".corrupt")
			require.NoError(t, err)
			_, err = os.Stat(path)
			assert.True(t, os.IsNotExist(err))
		})
	}
}

// TestQwpSfQuarantineCreationDebrisPreservesEvidence pins that setting an
// unusable manifest aside never destroys anything. It runs from
// qwpSfManifestOpen, before recovery has decided whether the slot fails closed,
// so a slot that is about to be preserved whole must still carry the boundary
// record that explains it — and an earlier quarantine's copy must survive too.
func TestQwpSfQuarantineCreationDebrisPreservesEvidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfManifestFileName)
	require.NoError(t, os.WriteFile(path, []byte("first evidence"), 0o644))
	require.NoError(t, qwpSfQuarantineCreationDebris(path))

	first, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, "first evidence", string(first))

	// A second pass must not overwrite the first copy.
	require.NoError(t, os.WriteFile(path, []byte("second evidence"), 0o644))
	require.NoError(t, qwpSfQuarantineCreationDebris(path))

	first, err = os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, "first evidence", string(first), "an earlier quarantine must survive")
	second, err := os.ReadFile(path + ".corrupt-1")
	require.NoError(t, err)
	require.Equal(t, "second evidence", string(second))
}

// TestQwpSfQuarantineFaultIsRetriableNeverFailClosed pins retry-always: no
// filesystem fault out of the manifest quarantine condemns the slot.
// Fail-closed is a permanent verdict — a foreground sender moves the slot's
// rows into quarantined/, where nothing scans for them again, and a drainer
// writes the .failed sentinel that disqualifies the slot from every later
// adoption — so it may only follow from what the slot's bytes say. A rename
// failure says nothing about them: full disks empty, read-only mounts get
// remounted, permissions get fixed. Every fault is reported as the retriable
// ErrSfDurability class, with the syscall cause reachable via errors.Is, and
// the manifest is left exactly where it was.
func TestQwpSfQuarantineFaultIsRetriableNeverFailClosed(t *testing.T) {
	for _, errno := range []syscall.Errno{
		syscall.ENOSPC, syscall.EROFS, syscall.EPERM, syscall.EACCES, syscall.ENAMETOOLONG,
	} {
		t.Run(errno.Error(), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, qwpSfManifestFileName)
			require.NoError(t, os.WriteFile(path, []byte("unusable"), 0o644))

			original := qwpSfManifestQuarantineRename.load()
			t.Cleanup(func() { qwpSfManifestQuarantineRename.store(original) })
			qwpSfManifestQuarantineRename.store(func(string, string) error { return errno })

			err := qwpSfQuarantineCreationDebris(path)
			require.Error(t, err)
			require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed,
				"an environmental fault must not condemn the slot")
			require.ErrorIs(t, err, ErrSfDurability,
				"the fault must be reported as the retriable local-storage class")
			require.ErrorIs(t, err, errno,
				"and the cause must stay reachable for a caller that inspects it")
			preserved, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, "unusable", string(preserved))
		})
	}
}

// TestQwpSfQuarantineFaultFailsConstructionUntilHealed pins the foreground
// consequence of retry-always: while the fault persists, constructing a sender
// on that slot fails with a retriable error and moves nothing — no slot
// quarantine, no fresh slot, every byte where it was. Once the disk recovers,
// the same construction succeeds. Availability waits on the operator;
// preservation wins that tradeoff by design.
func TestQwpSfQuarantineFaultFailsConstructionUntilHealed(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ENOSPC, syscall.EROFS} {
		t.Run(errno.Error(), func(t *testing.T) {
			root := t.TempDir()
			slot := filepath.Join(root, "faulted")
			require.NoError(t, os.MkdirAll(slot, 0o755))
			// A manifest of the wrong size, which qwpSfManifestOpen sets aside.
			require.NoError(t, os.WriteFile(filepath.Join(slot, qwpSfManifestFileName),
				[]byte("too short"), 0o644))

			original := qwpSfManifestQuarantineRename.load()
			t.Cleanup(func() { qwpSfManifestQuarantineRename.store(original) })
			qwpSfManifestQuarantineRename.store(func(string, string) error { return errno })

			engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
			require.Error(t, err, "construction must fail while the fault persists")
			require.Nil(t, engine)
			require.ErrorIs(t, err, ErrSfDurability)
			require.ErrorIs(t, err, errno)
			require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed)

			_, statErr := os.Stat(filepath.Join(root, "quarantined"))
			require.True(t, os.IsNotExist(statErr),
				"a retriable fault must not quarantine the slot")
			_, statErr = os.Stat(filepath.Join(slot, qwpSfFailedSentinelName))
			require.True(t, os.IsNotExist(statErr))
			body, readErr := os.ReadFile(filepath.Join(slot, qwpSfManifestFileName))
			require.NoError(t, readErr, "the boundary record must stay in place")
			require.Equal(t, "too short", string(body))

			// Heal the disk: the same construction succeeds and the debris is
			// set aside where recovery evidence lives.
			qwpSfManifestQuarantineRename.store(original)
			engine, err = qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
			require.NoError(t, err)
			require.NotNil(t, engine)
			t.Cleanup(func() { _ = engine.engineClose() })
			preserved, readErr := os.ReadFile(filepath.Join(slot, qwpSfManifestFileName+".corrupt"))
			require.NoError(t, readErr)
			require.Equal(t, "too short", string(preserved))
		})
	}
}
