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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfScanOrphansFindsCandidates(t *testing.T) {
	root := t.TempDir()

	// orphan-1: has a .sfa file → candidate
	require.NoError(t, os.MkdirAll(filepath.Join(root, "orphan-1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orphan-1", "sf-x.sfa"), []byte{}, 0o644))

	// orphan-2: has .sfa AND .failed sentinel → NOT a candidate
	require.NoError(t, os.MkdirAll(filepath.Join(root, "orphan-2"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orphan-2", "sf-x.sfa"), []byte{}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orphan-2", qwpSfFailedSentinelName), []byte{}, 0o644))

	// orphan-3: empty dir → NOT a candidate
	require.NoError(t, os.MkdirAll(filepath.Join(root, "orphan-3"), 0o755))

	// orphan-4: has .lock but no .sfa → NOT a candidate
	require.NoError(t, os.MkdirAll(filepath.Join(root, "orphan-4"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orphan-4", ".lock"), []byte{}, 0o644))

	// own-slot: filtered by name
	require.NoError(t, os.MkdirAll(filepath.Join(root, "own-slot"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "own-slot", "sf-x.sfa"), []byte{}, 0o644))

	orphans := qwpSfScanOrphans(root, "own-slot")
	require.Len(t, orphans, 1)
	assert.Equal(t, filepath.Join(root, "orphan-1"), orphans[0])
}

func TestQwpSfScanOrphansEmptyDirReturnsNothing(t *testing.T) {
	root := t.TempDir()
	assert.Empty(t, qwpSfScanOrphans(root, ""))
}

func TestQwpSfScanOrphansMissingDirReturnsNothing(t *testing.T) {
	assert.Empty(t, qwpSfScanOrphans("/nonexistent/path", ""))
}

func TestQwpSfMarkSlotFailed(t *testing.T) {
	root := t.TempDir()
	qwpSfMarkSlotFailed(root, "test reason")
	body, err := os.ReadFile(filepath.Join(root, qwpSfFailedSentinelName))
	require.NoError(t, err)
	assert.Equal(t, "test reason", string(body))
}

func TestQwpSfDrainerDrainsRealOrphan(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	dir := t.TempDir()

	// Stand up a "previous session" that wrote frames + closed.
	// Since the engine clears residual files on full drain, we need
	// to leave the slot un-drained. Easiest: use a separate engine
	// with no I/O loop to populate the slot, then close without
	// ACKing.
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			_, err := engine.engineAppendBlocking(context.Background(), []byte{byte(i)})
			require.NoError(t, err)
		}
		// Don't acknowledge → engineClose leaves residual .sfa files.
		require.NoError(t, engine.engineClose())
	}
	// Confirm there's a .sfa file to drain.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	hasFile := false
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sfa" {
			hasFile = true
		}
	}
	require.True(t, hasFile, "expected leftover .sfa for drainer to pick up")

	// Run a drainer.
	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeSuccess, drainer.drainerOutcome())
	assert.Equal(t, drainer.drainerTargetFsn(), drainer.drainerAckedFsn())
	assert.GreaterOrEqual(t, srv.totalFramesReceived.Load(), int64(1))
}

func TestQwpSfDrainerSkipsLockedSlot(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	dir := t.TempDir()
	// Hold the slot lock for the duration of the drainer's run.
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	defer func() { _ = lock.close() }()

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeLockedByOther, drainer.drainerOutcome())
	// Locked slots must NOT be marked .failed (contention is normal).
	_, err = os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(err), "drainer wrongly created .failed on lock contention")
}

func TestQwpSfDrainerMarksFailedOnAuthRejection(t *testing.T) {
	authSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{upgradeStatus: 401})
	defer authSrv.Close()

	dir := t.TempDir()
	// Populate the slot with unacked data.
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(authSrv),
		200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	body, err := os.ReadFile(filepath.Join(dir, qwpSfFailedSentinelName))
	require.NoError(t, err)
	assert.Contains(t, string(body), "connect")
}

func TestQwpSfDrainerSucceedsOnAlreadyDrainedSlot(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	dir := t.TempDir()

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeSuccess, drainer.drainerOutcome())
}

func TestQwpSfDrainerPoolSubmitAndClose(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	pool := qwpSfNewDrainerPool(2)
	defer pool.drainerPoolClose()

	const segSize int64 = 4096
	dirs := make([]string, 3)
	for i := range dirs {
		dirs[i] = t.TempDir()
		engine, err := qwpSfNewCursorEngine(dirs[i], segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte{byte(i)})
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	for _, dir := range dirs {
		drainer := qwpSfNewOrphanDrainer(
			dir, segSize, qwpSfUnlimitedTotalBytes,
			qwpSfDialFor(srv),
			1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
		)
		require.NoError(t, pool.drainerPoolSubmit(context.Background(), drainer))
	}
	pool.drainerPoolClose()
	// All drainers should have run.
	snap := pool.drainerPoolSnapshot()
	require.Len(t, snap, 3)
	for _, d := range snap {
		// We don't strictly require Success since close grace might
		// cut some off, but the outcome must not be PENDING.
		assert.NotEqual(t, qwpSfDrainOutcomePending, d.drainerOutcome())
	}
}

func TestQwpSfDrainerPoolRejectsAfterClose(t *testing.T) {
	pool := qwpSfNewDrainerPool(1)
	pool.drainerPoolClose()
	d := qwpSfNewOrphanDrainer(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes,
		nil, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	err := pool.drainerPoolSubmit(context.Background(), d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestSfConfDrainOrphansEndToEnd(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	root := t.TempDir()
	// Pre-populate an orphan slot with un-drained data.
	orphanDir := filepath.Join(root, "old-sender")
	require.NoError(t, os.MkdirAll(orphanDir, 0o755))
	{
		engine, err := qwpSfNewCursorEngine(orphanDir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("orphaned-frame"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	addr := strings.TrimPrefix(srv.URL, "http://")
	confStr := strings.Join([]string{
		"ws::addr=" + addr,
		"sf_dir=" + root,
		"sender_id=foreground",
		"drain_orphans=on",
		"max_background_drainers=2",
		"close_flush_timeout_millis=2000;",
	}, ";")
	ls, err := LineSenderFromConf(context.Background(), confStr)
	require.NoError(t, err)

	// Wait briefly for the drainer to consume the orphan frame.
	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(orphanDir)
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".sfa" {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, ls.Close(context.Background()))
	// At least the orphan frame must have reached the server.
	assert.GreaterOrEqual(t, srv.totalFramesReceived.Load(), int64(1))
}
