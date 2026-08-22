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

// TestQwpSfQuarantineCreationDebrisReportsAnUnrenameableManifest pins that a
// rename that cannot succeed is reported rather than escalated to a delete.
// This runs before recovery has decided whether the slot fails closed, and the
// manifest is the boundary record that explains the segments it is preserved
// with — so removing it is never the way forward. It would not help anyway:
// every segment this client writes carries the manifest-required flag, so a
// slot whose manifest is gone fails closed rather than degrading.
func TestQwpSfQuarantineCreationDebrisReportsAnUnrenameableManifest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfManifestFileName)
	require.NoError(t, os.WriteFile(path, []byte("unusable"), 0o644))

	original := qwpSfManifestQuarantineRename.load()
	t.Cleanup(func() { qwpSfManifestQuarantineRename.store(original) })
	qwpSfManifestQuarantineRename.store(func(string, string) error { return syscall.EPERM })

	err := qwpSfQuarantineCreationDebris(path)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed,
		"a permanent fault must be fail-closed so the caller preserves the whole slot")
	require.ErrorIs(t, err, syscall.EPERM,
		"and the cause must stay reachable for a caller that inspects it")
	preserved, readErr := os.ReadFile(path)
	require.NoError(t, readErr, "the manifest must still be there for the slot's quarantine")
	require.Equal(t, "unusable", string(preserved))
}

// TestQwpSfUnrenameableManifestDoesNotBrickTheSlot is the consequence that
// matters. A plain error out of the manifest quarantine is not fail-closed, so
// the recovery policy neither quarantines the slot nor starts a fresh one, and
// every later construction for that sender_id fails identically — ingestion
// stops until an operator deletes the file by hand.
func TestQwpSfUnrenameableManifestDoesNotBrickTheSlot(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "wedged")
	require.NoError(t, os.MkdirAll(slot, 0o755))
	// A manifest of the wrong size, which qwpSfManifestOpen sets aside.
	require.NoError(t, os.WriteFile(filepath.Join(slot, qwpSfManifestFileName),
		[]byte("too short"), 0o644))

	original := qwpSfManifestQuarantineRename.load()
	t.Cleanup(func() { qwpSfManifestQuarantineRename.store(original) })
	qwpSfManifestQuarantineRename.store(func(string, string) error { return syscall.EROFS })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err, "the sender must still be constructible")
	require.NotNil(t, engine)
	t.Cleanup(func() { _ = engine.engineClose() })

	entries, err := os.ReadDir(filepath.Join(root, "quarantined"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "the whole slot must be preserved instead")
	require.NotEmpty(t, engine.engineQuarantinedSlotPath())
}

// TestQwpSfTransientQuarantineFaultIsNotFailClosed pins the other half. A full
// disk or an exhausted fd table says nothing about the slot's bytes, and
// fail-closed is a permanent verdict: a foreground sender would move a legacy
// slot's undelivered rows into quarantined/, where nothing scans for them
// again, and a drainer would write the .failed sentinel that disqualifies the
// slot from every later adoption.
func TestQwpSfTransientQuarantineFaultIsNotFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfManifestFileName)
	require.NoError(t, os.WriteFile(path, []byte("unusable"), 0o644))

	original := qwpSfManifestQuarantineRename.load()
	t.Cleanup(func() { qwpSfManifestQuarantineRename.store(original) })
	qwpSfManifestQuarantineRename.store(func(string, string) error { return syscall.ENOSPC })

	err := qwpSfQuarantineCreationDebris(path)
	require.Error(t, err)
	require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed,
		"a transient fault must not condemn the slot")
	require.ErrorIs(t, err, syscall.ENOSPC)
	preserved, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "unusable", string(preserved))
}
