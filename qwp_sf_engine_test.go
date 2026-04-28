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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfEngineMemoryModeAppend(t *testing.T) {
	e, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	for i := int64(0); i < 5; i++ {
		fsn, err := e.engineAppendBlocking([]byte("frame"))
		require.NoError(t, err)
		assert.Equal(t, i, fsn)
	}
	assert.Equal(t, int64(4), e.enginePublishedFsn())
	assert.False(t, e.engineWasRecoveredFromDisk())
	assert.Equal(t, "", e.engineSfDir())
}

func TestQwpSfEngineDiskModeWritesAndRecovers(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096

	{
		e, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		assert.False(t, e.engineWasRecoveredFromDisk())

		for i := 0; i < 5; i++ {
			_, err := e.engineAppendBlocking([]byte{byte(i), byte(i + 1)})
			require.NoError(t, err)
		}
		assert.Equal(t, int64(4), e.enginePublishedFsn())
		require.NoError(t, e.engineClose())
	}

	// Files should still be on disk (no ACKs were processed).
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	sfaCount := 0
	for _, en := range entries {
		if filepath.Ext(en.Name()) == ".sfa" {
			sfaCount++
		}
	}
	assert.GreaterOrEqual(t, sfaCount, 1)

	// Recover.
	e2, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e2.engineClose() }()
	assert.True(t, e2.engineWasRecoveredFromDisk())
	// publishedFsn must still be 4 (5 frames were written).
	assert.Equal(t, int64(4), e2.enginePublishedFsn())
}

func TestQwpSfEngineSlotLockBlocksDouble(t *testing.T) {
	dir := t.TempDir()
	e1, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e1.engineClose() }()

	_, err = qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "slot already in use")
}

func TestQwpSfEngineFullDrainUnlinksFiles(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096
	e, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		fsn, err := e.engineAppendBlocking([]byte("hi"))
		require.NoError(t, err)
		// Immediately ACK each frame so the ring fully drains.
		e.engineAcknowledge(fsn)
	}
	require.NoError(t, e.engineClose())

	// On full drain, the engine unlinks residual .sfa files. Allow
	// for a small window where the manager hasn't yet seen the trim;
	// engineClose itself unlinks anything still on disk.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, en := range entries {
		assert.NotEqual(t, ".sfa", filepath.Ext(en.Name()),
			"unexpected leftover segment file %s", en.Name())
	}
}

func TestQwpSfEngineBackpressureTimeout(t *testing.T) {
	const segSize int64 = 96 // 24 header + 72 payload region
	// Cap at one segment so the manager never provisions a spare:
	// after the active fills, every append blocks until the deadline.
	e, err := qwpSfNewCursorEngine("", segSize, segSize, 50*time.Millisecond)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	// Fill the active until the next append blocks. capacity = 96-24
	// = 72; each frame uses 8+16 = 24, so 3 frames fit.
	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(make([]byte, 16))
		require.NoError(t, err, "iteration %d", i)
	}
	// The next append must time out.
	start := time.Now()
	_, err = e.engineAppendBlocking(make([]byte, 16))
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.True(t, errors.Is(err, qwpSfErrBackpressureTimeout))
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond)
	// Backpressure stall counter incremented.
	assert.GreaterOrEqual(t, e.engineTotalBackpressureStalls(), int64(1))
}

func TestQwpSfEnginePayloadTooLarge(t *testing.T) {
	const segSize int64 = 256
	e, err := qwpSfNewCursorEngine("", segSize, segSize*4, time.Second)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	huge := make([]byte, segSize) // can never fit (header + envelope alone exceeds)
	_, err = e.engineAppendBlocking(huge)
	require.Error(t, err)
	assert.True(t, errors.Is(err, qwpSfErrPayloadTooLarge))
}

func TestQwpSfEngineSharedManager(t *testing.T) {
	mgr, err := qwpSfNewSegmentManager(4096, 100*time.Microsecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	defer mgr.segmentManagerClose()

	e1, err := qwpSfNewCursorEngineWithManager("", 4096, mgr, time.Second)
	require.NoError(t, err)
	e2, err := qwpSfNewCursorEngineWithManager("", 4096, mgr, time.Second)
	require.NoError(t, err)

	// Both engines should be able to append and have the manager
	// supply spares to both rings.
	for i := 0; i < 3; i++ {
		_, err := e1.engineAppendBlocking([]byte("a"))
		require.NoError(t, err)
		_, err = e2.engineAppendBlocking([]byte("b"))
		require.NoError(t, err)
	}
	require.NoError(t, e1.engineClose())
	require.NoError(t, e2.engineClose())
}
