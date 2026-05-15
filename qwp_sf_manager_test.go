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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfManagerProvisionsSpare(t *testing.T) {
	const segSize int64 = 4096
	mgr, err := qwpSfNewSegmentManager(segSize, 100*time.Microsecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	defer mgr.segmentManagerClose()

	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()

	require.NoError(t, mgr.segmentManagerRegister(r, "")) // memory mode

	// Wait for the worker to provision a spare.
	require.Eventually(t, func() bool {
		return !r.needsHotSpare()
	}, 1*time.Second, 1*time.Millisecond)
}

func TestQwpSfManagerTrimsAckedSegments(t *testing.T) {
	const segSize int64 = 72 // two minimal frames per segment
	mgr, err := qwpSfNewSegmentManager(segSize, 100*time.Microsecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	defer mgr.segmentManagerClose()

	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()
	require.NoError(t, mgr.segmentManagerRegister(r, ""))

	// Wait for the manager to provision a spare.
	require.Eventually(t, func() bool {
		return !r.needsHotSpare()
	}, 1*time.Second, 1*time.Millisecond)

	// Append three frames to roll one segment into sealed.
	payload := make([]byte, 16)
	for i := 0; i < 3; i++ {
		fsn := r.appendOrFsn(payload)
		require.GreaterOrEqual(t, fsn, int64(0), "iteration %d", i)
	}
	// The manager worker is running, so observe the ring through the
	// lock-protected accessors (sealedSegmentCount / firstSealed), not
	// the non-thread-safe getSealedSegments.
	require.Equal(t, 1, r.sealedSegmentCount())
	sealedBefore := r.firstSealed()
	require.NotNil(t, sealedBefore)
	r.acknowledge(sealedBefore.segmentBaseSeq() + sealedBefore.segmentFrameCount() - 1)

	// Manager should pick up the trim within a few ticks.
	require.Eventually(t, func() bool {
		return r.sealedSegmentCount() == 0
	}, 1*time.Second, 1*time.Millisecond)
}

func TestQwpSfManagerProvisionsDiskSpare(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096
	mgr, err := qwpSfNewSegmentManager(segSize, 100*time.Microsecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	defer mgr.segmentManagerClose()

	first, err := qwpSfCreateSegment(filepath.Join(dir, "sf-initial.sfa"), 0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()
	require.NoError(t, mgr.segmentManagerRegister(r, dir))

	require.Eventually(t, func() bool {
		return !r.needsHotSpare()
	}, 1*time.Second, 1*time.Millisecond)

	// A second .sfa file (the spare) should now exist on disk.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	count := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sfa" {
			count++
		}
	}
	assert.GreaterOrEqual(t, count, 2)
}

func TestQwpSfManagerCapBlocksSpare(t *testing.T) {
	const segSize int64 = 4096
	// Cap at exactly one segment — manager refuses to provision a
	// spare while the active is the only segment.
	mgr, err := qwpSfNewSegmentManager(segSize, 100*time.Microsecond, segSize)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	defer mgr.segmentManagerClose()

	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()
	require.NoError(t, mgr.segmentManagerRegister(r, ""))

	// Give the manager a few ticks. It should keep refusing to
	// install — needsHotSpare stays true.
	time.Sleep(50 * time.Millisecond)
	assert.True(t, r.needsHotSpare())
}

func TestQwpSfManagerRegisterAfterCloseRejects(t *testing.T) {
	mgr, err := qwpSfNewSegmentManager(4096, time.Millisecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	mgr.segmentManagerClose()

	first, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, 4096)
	defer func() { _ = r.segmentRingClose() }()
	err = mgr.segmentManagerRegister(r, "")
	require.Error(t, err)
}

func TestQwpSfManagerScanMaxGenerationOnEmptyDir(t *testing.T) {
	dir := t.TempDir()
	v := qwpSfScanMaxGeneration(dir)
	// Sentinel: no segments → caller adds 1 to get generation 0.
	assert.Equal(t, ^uint64(0), v)
}

func TestQwpSfManagerScanMaxGenerationFindsHighest(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"sf-0000000000000005.sfa",
		"sf-000000000000000a.sfa",
		"sf-000000000000000c.sfa",
		"sf-initial.sfa", // skipped (legacy non-hex name)
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte{}, 0o644))
	}
	v := qwpSfScanMaxGeneration(dir)
	assert.Equal(t, uint64(0xc), v)
}

func TestQwpSfManagerNextSparePathIncrements(t *testing.T) {
	mgr, err := qwpSfNewSegmentManager(4096, time.Millisecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	dir := t.TempDir()
	a := mgr.nextSparePath(dir)
	b := mgr.nextSparePath(dir)
	assert.NotEqual(t, a, b)
	assert.Equal(t, filepath.Join(dir, "sf-0000000000000000.sfa"), a)
	assert.Equal(t, filepath.Join(dir, "sf-0000000000000001.sfa"), b)
}
