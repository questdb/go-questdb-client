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
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

	orphans := qwpSfScanOrphans(root, func(name string) bool { return name == "own-slot" })
	require.Len(t, orphans, 1)
	assert.Equal(t, filepath.Join(root, "orphan-1"), orphans[0])
}

func TestQwpSfManifestOnlySlotIsCandidateButQuarantineRootIsNot(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "manifest-only")
	require.NoError(t, os.MkdirAll(slot, 0o755))
	m, err := qwpSfManifestCreate(slot, 0, 1)
	require.NoError(t, err)
	require.NoError(t, m.close())
	assert.True(t, qwpSfIsCandidateOrphan(slot))

	quarantineRoot := filepath.Join(root, "quarantined")
	require.NoError(t, os.MkdirAll(filepath.Join(quarantineRoot, "sender-1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(quarantineRoot, "sender-1", "sf-initial.sfa"), []byte("preserved"), 0o644))
	assert.False(t, qwpSfIsCandidateOrphan(quarantineRoot))
}

func TestQwpSfDrainerHandlesManifestOnlySlots(t *testing.T) {
	t.Run("data-boundaries-mark-failed-with-reason", func(t *testing.T) {
		slot := t.TempDir()
		m, err := qwpSfManifestCreate(slot, 0, 1)
		require.NoError(t, err)
		require.NoError(t, m.close())

		d := qwpSfNewOrphanDrainer(slot, 4096, qwpSfUnlimitedTotalBytes, nil, nil, 0, 0, 0)
		d.drainerRun(context.Background())
		assert.Equal(t, qwpSfDrainOutcomeFailed, d.drainerOutcome())
		body, err := os.ReadFile(filepath.Join(slot, qwpSfFailedSentinelName))
		require.NoError(t, err)
		assert.Contains(t, string(body), "sf-manifest.bin references durable data")
	})

	t.Run("collapsed-boundaries-clean-up", func(t *testing.T) {
		slot := t.TempDir()
		m, err := qwpSfManifestCreate(slot, 4, 4)
		require.NoError(t, err)
		require.NoError(t, m.close())

		d := qwpSfNewOrphanDrainer(slot, 4096, qwpSfUnlimitedTotalBytes, nil, nil, 0, 0, 0)
		d.drainerRun(context.Background())
		assert.Equal(t, qwpSfDrainOutcomeSuccess, d.drainerOutcome())
		_, err = os.Stat(filepath.Join(slot, qwpSfManifestFileName))
		assert.True(t, os.IsNotExist(err))
		_, err = os.Stat(filepath.Join(slot, qwpSfFailedSentinelName))
		assert.True(t, os.IsNotExist(err))
	})
}

func TestQwpSfScanOrphansEmptyDirReturnsNothing(t *testing.T) {
	root := t.TempDir()
	assert.Empty(t, qwpSfScanOrphans(root, nil))
}

func TestQwpSfScanOrphansMissingDirReturnsNothing(t *testing.T) {
	assert.Empty(t, qwpSfScanOrphans("/nonexistent/path", nil))
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
		nil,
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
		nil,
		1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeLockedByOther, drainer.drainerOutcome())
	// Locked slots must NOT be marked .failed (contention is normal).
	_, err = os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(err), "drainer wrongly created .failed on lock contention")
}

// TestQwpSfDrainerLocalIOFaultLeavesNoFailedSentinel pins the drainer half of
// retry-always: a local filesystem fault while opening the slot — here, a
// manifest-debris quarantine rename refused with ENOSPC — says nothing about
// the slot's bytes. The run fails, but the slot keeps its data and its
// eligibility: no .failed sentinel, so the next foreground scan adopts it
// again once the fault clears.
func TestQwpSfDrainerLocalIOFaultLeavesNoFailedSentinel(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	dir := t.TempDir()
	// A manifest of the wrong size, which engine open tries to set aside.
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfManifestFileName),
		[]byte("too short"), 0o644))

	original := qwpSfManifestQuarantineRename.load()
	t.Cleanup(func() { qwpSfManifestQuarantineRename.store(original) })
	qwpSfManifestQuarantineRename.store(func(string, string) error { return syscall.ENOSPC })

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		nil,
		200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	_, err := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(err),
		"a local I/O fault must leave the slot eligible for a later adoption")
	body, err := os.ReadFile(filepath.Join(dir, qwpSfManifestFileName))
	require.NoError(t, err)
	assert.Equal(t, "too short", string(body), "the boundary record must stay in place")
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
		nil,
		200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond,
	)
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	body, err := os.ReadFile(filepath.Join(dir, qwpSfFailedSentinelName))
	require.NoError(t, err)
	assert.Contains(t, string(body), "connect")
}

// TestQwpSfDrainerDurableAckMismatchQuarantines pins §5.8 / Hazard I: a
// durable-ack drainer against an endpoint that does not advertise durable-ack
// retries (its source is pinned), notifies the listener each attempt, and after
// the cap quarantines the slot with a .failed sentinel — never trimming.
func TestQwpSfDrainerDurableAckMismatchQuarantines(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{}) // does not advertise durable-ack
	defer srv.Close()

	dir := t.TempDir()
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	var unavailable, persistent, lastAttempts atomic.Int32
	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		qwpSfDurableDialFor(srv),
		nil,
		5*time.Second, time.Millisecond, 5*time.Millisecond,
	)
	drainer.durableAckMode = true
	drainer.listener = QwpBackgroundDrainerListener{
		OnDurableAckUnavailable:       func(string, int) { unavailable.Add(1) },
		OnDurableAckPersistentFailure: func(_ string, attempts int, _ time.Duration) { lastAttempts.Store(int32(attempts)); persistent.Add(1) },
	}
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	body, err := os.ReadFile(filepath.Join(dir, qwpSfFailedSentinelName))
	require.NoError(t, err)
	assert.Contains(t, string(body), "durable-ack")
	require.Eventually(t, func() bool { return persistent.Load() == 1 }, time.Second, time.Millisecond)
	assert.EqualValues(t, qwpMaxDurableAckMismatchAttempts, unavailable.Load(), "cooperative listener receives each queued mismatch")
	assert.EqualValues(t, 1, persistent.Load())
	assert.EqualValues(t, qwpMaxDurableAckMismatchAttempts, lastAttempts.Load())

	// Positively confirm the drainer never trimmed the un-uploaded data: its
	// backing .sfa segment must survive quarantine (Hazard I — never unlink an
	// un-durable segment). A regression that trimmed on the OK ack in durable
	// drainer mode would delete it and still pass the assertions above.
	segs, err := filepath.Glob(filepath.Join(dir, "*.sfa"))
	require.NoError(t, err)
	assert.NotEmpty(t, segs, "quarantine must leave the un-uploaded .sfa segment intact")
}

// TestQwpSfDrainerListenerPanicIsolated pins the isolation invariant: a panic in a user-supplied
// background-drainer callback is recovered rather than unwinding into the
// drainer's goroutine (where the top-level recover would quarantine an
// otherwise-recoverable slot).
func TestQwpSfDrainerListenerPanicIsolated(t *testing.T) {
	t.Run("HelperRecovers", func(t *testing.T) {
		qwpDrainerListenerCall(nil, func() { panic("boom") }) // must not propagate
		qwpDrainerListenerCall(nil, nil)                      // nil-safe
	})

	t.Run("PanickingLoggerContained", func(t *testing.T) {
		// The handler that reports the caught panic is user code too, so a
		// panic there must not reach the drainer's goroutine either.
		logger := slog.New(panicOnHandleSlog{})
		require.NotPanics(t, func() {
			qwpDrainerListenerCall(logger, func() { panic("boom") })
		})
	})

	t.Run("OnDurableAckUnavailablePanicContained", func(t *testing.T) {
		d := qwpSfNewOrphanDrainer(
			t.TempDir(), 4096, qwpSfUnlimitedTotalBytes,
			nil, nil,
			time.Second, time.Millisecond, 5*time.Millisecond,
		)
		d.durableAckMode = true
		d.listener = QwpBackgroundDrainerListener{
			OnDurableAckUnavailable: func(string, int) { panic("user callback boom") },
		}
		// onRoundExhausted runs on the goroutine driving the connect walk
		// (the drainerRun goroutine or the send-loop I/O goroutine); a
		// panic in the callback must be contained there, and the
		// capability-gap sweep must still be counted.
		d.onRoundExhausted(qwpSfSweepOutcome{SawDurableMismatch: true})
		assert.Equal(t, int64(1), d.mismatchAttempts.Load())
	})

	t.Run("OnDurableAckPersistentFailurePanicContained", func(t *testing.T) {
		d := qwpSfNewOrphanDrainer(
			t.TempDir(), 4096, qwpSfUnlimitedTotalBytes,
			nil, nil,
			time.Second, time.Millisecond, 5*time.Millisecond,
		)
		d.durableAckMode = true
		d.listener = QwpBackgroundDrainerListener{
			OnDurableAckPersistentFailure: func(string, int, time.Duration) { panic("give-up boom") },
		}
		// recordDurableGiveUp runs on the drainerRun goroutine; the panic must
		// be contained and the slot still quarantined (.failed) as usual.
		d.recordDurableGiveUp()
		assert.Equal(t, qwpSfDrainOutcomeFailed, d.drainerOutcome())
	})
}

// TestQwpSfDrainerSettleBudgetPausesOnMixedSweep pins that a reconnect sweep
// which saw both a durable-ack mismatch (a reachable non-durable node) and a
// transport error (the durable primary transiently down) neither charges nor
// resets the capability-gap settle budget: the primary may merely be rebooting,
// so the budget must pause with the outage (Invariant B) rather than quarantine
// a recoverable slot. Only a pure capability-gap sweep charges.
func TestQwpSfDrainerSettleBudgetPausesOnMixedSweep(t *testing.T) {
	newDrainer := func() *qwpSfOrphanDrainer {
		d := qwpSfNewOrphanDrainer(
			t.TempDir(), 4096, qwpSfUnlimitedTotalBytes,
			nil, nil,
			time.Second, time.Millisecond, 5*time.Millisecond,
		)
		d.durableAckMode = true
		return d
	}

	t.Run("MixedSweepNeitherChargesNorQuarantines", func(t *testing.T) {
		d := newDrainer()
		for i := 0; i < qwpMaxDurableAckMismatchAttempts+5; i++ {
			d.onRoundExhausted(qwpSfSweepOutcome{SawDurableMismatch: true, SawTransportError: true})
		}
		assert.Equal(t, int64(0), d.mismatchAttempts.Load())
		assert.False(t, d.durableMismatchGaveUp.Load())
		assert.False(t, d.stopRequested.Load())
	})

	t.Run("PureCapabilityGapStillCharges", func(t *testing.T) {
		d := newDrainer()
		d.onRoundExhausted(qwpSfSweepOutcome{SawDurableMismatch: true})
		assert.Equal(t, int64(1), d.mismatchAttempts.Load())
	})

	t.Run("PureTransportSweepDoesNotResetTheCharge", func(t *testing.T) {
		d := newDrainer()
		d.onRoundExhausted(qwpSfSweepOutcome{SawDurableMismatch: true}) // charge to 1
		d.onRoundExhausted(qwpSfSweepOutcome{SawTransportError: true})  // pause, no reset
		assert.Equal(t, int64(1), d.mismatchAttempts.Load())
	})

	t.Run("AllReplicaSweepResetsTheCharge", func(t *testing.T) {
		d := newDrainer()
		d.onRoundExhausted(qwpSfSweepOutcome{SawDurableMismatch: true}) // charge to 1
		d.onRoundExhausted(qwpSfSweepOutcome{SawRoleReject: true})      // topology churn: reset
		assert.Equal(t, int64(0), d.mismatchAttempts.Load())
	})
}

func TestQwpSfDrainerSucceedsOnAlreadyDrainedSlot(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	dir := t.TempDir()

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		nil,
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

	drainers := make([]*qwpSfOrphanDrainer, 0, len(dirs))
	for _, dir := range dirs {
		drainer := qwpSfNewOrphanDrainer(
			dir, segSize, qwpSfUnlimitedTotalBytes,
			qwpSfDialFor(srv),
			nil,
			1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
		)
		drainers = append(drainers, drainer)
		require.NoError(t, pool.drainerPoolSubmit(context.Background(), drainer))
	}
	pool.drainerPoolClose()
	// Every submitted drainer must reach a terminal state — we
	// don't strictly require Success since close grace might cut
	// some off, but the outcome must not be PENDING.
	for _, d := range drainers {
		assert.NotEqual(t, qwpSfDrainOutcomePending, d.drainerOutcome())
	}
	// Snapshot must be empty after close: completed drainers are
	// pruned from the active list as their goroutines exit.
	assert.Empty(t, pool.drainerPoolSnapshot())
}

func TestQwpSfDrainerPoolQueuedWorkOutlivesSetupContext(t *testing.T) {
	for _, closePool := range []bool{false, true} {
		name := "drain-after-capacity-frees"
		if closePool {
			name = "pool-close-stops-queued-work"
		}
		t.Run(name, func(t *testing.T) {
			srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true})
			t.Cleanup(srv.Close)
			const segSize int64 = 4096
			dirs := []string{t.TempDir(), t.TempDir()}
			for i, dir := range dirs {
				engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.engineClose() })
				_, err = engine.engineAppendBlocking(context.Background(), []byte{byte(i)})
				require.NoError(t, err)
				require.NoError(t, engine.engineClose())
			}
			entered, allowDial := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(allowDial) }) }
			factory := func(ctx context.Context, idx int) (*qwpTransport, error) {
				enteredOnce.Do(func() { close(entered) })
				select {
				case <-allowDial:
					return qwpSfDialFor(srv)(ctx, idx)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			pool := qwpSfNewDrainerPool(1)
			oldGrace := qwpSfDrainerPoolCloseGrace.load()
			qwpSfDrainerPoolCloseGrace.store(20 * time.Millisecond)
			t.Cleanup(func() {
				// On assertion failure, cancel before opening the dial gate so
				// teardown cannot start a fresh connection while cleaning up.
				pool.cancel()
				release()
				pool.drainerPoolClose()
				qwpSfDrainerPoolCloseGrace.store(oldGrace)
			})
			setupCtx, cancelSetup := context.WithCancel(context.Background())
			t.Cleanup(cancelSetup)
			first := qwpSfNewOrphanDrainer(dirs[0], segSize, qwpSfUnlimitedTotalBytes,
				factory, nil, time.Second, time.Millisecond, 10*time.Millisecond)
			second := qwpSfNewOrphanDrainer(dirs[1], segSize, qwpSfUnlimitedTotalBytes,
				qwpSfDialFor(srv), nil, time.Second, time.Millisecond, 10*time.Millisecond)
			require.NoError(t, pool.drainerPoolSubmit(setupCtx, first))
			waitQwpCleanupSignal(t, entered, "first drainer occupying the only slot")
			require.NoError(t, pool.drainerPoolSubmit(setupCtx, second))
			cancelSetup()
			require.Never(t, func() bool {
				return second.drainerOutcome() != qwpSfDrainOutcomePending || len(pool.drainerPoolSnapshot()) != 2
			}, 50*time.Millisecond, time.Millisecond, "accepted queued work must survive setup cancellation")
			require.Equal(t, int64(-1), second.drainerTargetFsn(), "capacity must still prevent the queued drainer from starting")

			if closePool {
				pool.drainerPoolClose()
				require.Empty(t, pool.drainerPoolSnapshot())
				require.Equal(t, qwpSfDrainOutcomeStopped, first.drainerOutcome())
				require.Equal(t, qwpSfDrainOutcomeStopped, second.drainerOutcome())
				require.Equal(t, int64(-1), second.drainerTargetFsn(), "pool close must not start the queued drainer")
				require.Zero(t, srv.totalFramesReceived.Load())
				for _, dir := range dirs {
					require.True(t, qwpSfIsCandidateOrphan(dir), "stopped work must remain recoverable")
					require.NoFileExists(t, filepath.Join(dir, qwpSfFailedSentinelName))
				}
			} else {
				release()
				require.Eventually(t, func() bool {
					return len(pool.drainerPoolSnapshot()) == 0
				}, 3*time.Second, time.Millisecond, "both accepted drainers must finish on the live pool")
				require.False(t, pool.closed.Load())
				require.Equal(t, qwpSfDrainOutcomeSuccess, first.drainerOutcome())
				require.Equal(t, qwpSfDrainOutcomeSuccess, second.drainerOutcome())
				var frames []string
				for _, received := range srv.recordedFrames() {
					frames = append(frames, received...)
				}
				require.ElementsMatch(t, []string{string([]byte{0}), string([]byte{1})}, frames,
					"setup cancellation must not prevent either slot's saved frame from reaching the server")
			}
			for _, dir := range dirs {
				lock, err := qwpSfAcquireSlotLock(dir)
				require.NoError(t, err, "finished or stopped drainers must release their slots")
				require.NoError(t, lock.close())
			}
		})
	}
}

func TestQwpSfDrainerPoolRejectsCancelledSubmission(t *testing.T) {
	pool := qwpSfNewDrainerPool(1)
	t.Cleanup(pool.drainerPoolClose)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := qwpSfNewOrphanDrainer(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes,
		nil, nil, time.Second, time.Millisecond, time.Millisecond)
	require.ErrorIs(t, pool.drainerPoolSubmit(ctx, d), context.Canceled)
	require.Empty(t, pool.drainerPoolSnapshot(), "rejected submissions must not become pool obligations")
	require.Equal(t, int64(-1), d.drainerTargetFsn())
}

func TestQwpSfOrphanSubmissionCancellationFailsConstruction(t *testing.T) {
	root := t.TempDir()
	orphanDir := filepath.Join(root, "orphan")
	engine, err := qwpSfNewCursorEngine(orphanDir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.engineClose() })
	_, err = engine.engineAppendBlocking(context.Background(), []byte("saved row"))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	hook := func() error {
		cancel()
		return nil
	}
	qwpSfTestAfterEngineCreateHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestAfterEngineCreateHook.Store(nil) })
	// Async connect does not consume the cancelled context in a foreground
	// dial. Construction must reach orphan submission and propagate its error.
	sender, err := LineSenderFromConf(ctx, "ws::addr=127.0.0.1:1;sf_dir="+root+
		";sender_id=foreground;initial_connect_retry=async;drain_orphans=on;")
	if sender != nil {
		t.Cleanup(func() { _ = sender.Close(context.Background()) })
	}
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, sender, "construction must not silently skip a rejected orphan submission")
	// Cancellation bounds the constructor's wait, not its acquired-resource
	// cleanup. The retained owner must finish before the slot can be reused.
	var cleanup closeLifecycleReporter
	if errors.As(err, &cleanup) {
		require.Eventually(t, cleanup.closeCompleted, 3*time.Second, time.Millisecond)
	}
	require.True(t, qwpSfIsCandidateOrphan(orphanDir))
	require.NoFileExists(t, filepath.Join(orphanDir, qwpSfFailedSentinelName))
	for _, dir := range []string{orphanDir, filepath.Join(root, "foreground")} {
		lock, err := qwpSfAcquireSlotLock(dir)
		require.NoError(t, err, "completed construction cleanup must release its slot and leave the orphan unlocked")
		require.NoError(t, lock.close())
	}
}

// TestQwpSfDrainerPoolEnforcesConcurrencyCapAtRuntime proves the
// max_background_drainers cap is a *runtime* bound, not just a parsed
// config value: submitting more drainers than the cap must never run
// more than `cap` drainerRun bodies at once. The clientFactory is the
// observation point — it is invoked from inside drainerRun only after
// the goroutine has taken its semaphore slot, so the number of
// concurrent factory entries equals the number of concurrently
// running drainers. A factory that parks until the pool's master ctx
// is cancelled holds every slot occupied, so a cap-violating drainer
// (if the semaphore were missing) would show up as a (cap+1)th entry.
func TestQwpSfDrainerPoolEnforcesConcurrencyCapAtRuntime(t *testing.T) {
	prevGrace := qwpSfDrainerPoolCloseGrace.load()
	qwpSfDrainerPoolCloseGrace.store(50 * time.Millisecond)
	defer func() { qwpSfDrainerPoolCloseGrace.store(prevGrace) }()

	const (
		maxConcurrent = 2
		total         = 5
	)

	var running atomic.Int32
	var peak atomic.Int32
	entered := make(chan struct{}, total)

	// Parks until the pool's master ctx is cancelled (drainerPoolClose).
	blockingFactory := func(ctx context.Context, _ int) (*qwpTransport, error) {
		cur := running.Add(1)
		defer running.Add(-1)
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				break
			}
		}
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	pool := qwpSfNewDrainerPool(maxConcurrent)

	const segSize int64 = 4096
	drainers := make([]*qwpSfOrphanDrainer, total)
	for i := range drainers {
		dir := t.TempDir()
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte{byte(i)})
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())

		d := qwpSfNewOrphanDrainer(
			dir, segSize, qwpSfUnlimitedTotalBytes,
			blockingFactory,
			nil,
			time.Second, 10*time.Millisecond, 100*time.Millisecond,
		)
		drainers[i] = d
		require.NoError(t, pool.drainerPoolSubmit(context.Background(), d))
	}

	// Exactly `maxConcurrent` drainers must reach the factory.
	for i := 0; i < maxConcurrent; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d drainers entered the factory, want %d", i, maxConcurrent)
		}
	}
	// No further drainer may enter while the first `maxConcurrent`
	// hold their slots — the rest are parked on the semaphore.
	select {
	case <-entered:
		t.Fatalf("a %dth drainer entered the factory: runtime cap not enforced", maxConcurrent+1)
	case <-time.After(250 * time.Millisecond):
	}
	assert.LessOrEqual(t, peak.Load(), int32(maxConcurrent),
		"at most %d drainers may run concurrently, observed peak %d", maxConcurrent, peak.Load())

	// Close cancels the master ctx; parked factories unwind, the
	// queued drainers never enter. The cap must still hold.
	pool.drainerPoolClose()
	assert.LessOrEqual(t, peak.Load(), int32(maxConcurrent),
		"concurrency cap must hold across the full run, observed peak %d", peak.Load())
	for i, d := range drainers {
		assert.NotEqual(t, qwpSfDrainOutcomePending, d.drainerOutcome(),
			"drainer %d still pending after close", i)
	}
	assert.Empty(t, pool.drainerPoolSnapshot())
}

// This test's connection attempt waits until its context is cancelled, like a
// connection to an unresponsive server. Closing the drainer pool cancels that
// context after the first wait. The attempt then returns and cleanup finishes
// within the time allowed by this test.
func TestQwpSfDrainerPoolCancelsBlockingDialOnClose(t *testing.T) {
	prevGrace := qwpSfDrainerPoolCloseGrace.load()
	qwpSfDrainerPoolCloseGrace.store(50 * time.Millisecond)
	defer func() { qwpSfDrainerPoolCloseGrace.store(prevGrace) }()

	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	dialEntered := make(chan struct{}, 1)
	blockingFactory := func(ctx context.Context, _ int) (*qwpTransport, error) {
		select {
		case dialEntered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	pool := qwpSfNewDrainerPool(1)
	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		blockingFactory,
		nil,
		1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	require.NoError(t, pool.drainerPoolSubmit(context.Background(), drainer))

	// Make sure the drainer is actually parked in the dial before
	// we close — otherwise we'd be testing the polite-stop path.
	select {
	case <-dialEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("drainer never entered clientFactory")
	}

	closeDone := make(chan struct{})
	go func() {
		pool.drainerPoolClose()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drainerPoolClose did not return after grace + ctx cancel")
	}

	// Drainer must have exited cleanly as Stopped (not Failed) —
	// a ctx-cancel during dial should NOT leave a .failed sentinel
	// in the slot, since the slot is still recoverable.
	assert.Equal(t, qwpSfDrainOutcomeStopped, drainer.drainerOutcome())
	_, statErr := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(statErr), "must not leave .failed sentinel on close-during-dial")

	// Active list must be pruned: drainer goroutine has exited.
	assert.Empty(t, pool.drainerPoolSnapshot())
}

// Regression test for M15: drainerPoolClose must return even if a worker ignores
// cancellation. The test's connection factory blocks like a file operation or
// recovery scan that cannot be cancelled. After both close waits expire, the
// pool stops waiting but keeps track of the worker and its locked directory
// until cleanup finishes.
func TestQwpSfDrainerPoolBoundedOnUncancellableDrainer(t *testing.T) {
	prevGrace := qwpSfDrainerPoolCloseGrace.load()
	prevHard := qwpSfDrainerPoolHardCloseGrace.load()
	qwpSfDrainerPoolCloseGrace.store(50 * time.Millisecond)
	qwpSfDrainerPoolHardCloseGrace.store(50 * time.Millisecond)
	defer func() {
		qwpSfDrainerPoolCloseGrace.store(prevGrace)
		qwpSfDrainerPoolHardCloseGrace.store(prevHard)
	}()

	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	// A factory that ignores its ctx stands in for a drainer wedged in
	// I/O the master-ctx cancel cannot interrupt.
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	wedgeFactory := func(_ context.Context, _ int) (*qwpTransport, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block // ignores ctx
		return nil, errors.New("released")
	}

	pool := qwpSfNewDrainerPool(1)
	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		wedgeFactory,
		nil,
		time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	defer func() {
		close(block)
		pool.drainerPoolClose()
		require.Eventually(t, pool.cleanupCompleted, 5*time.Second, time.Millisecond,
			"join cleanup before TempDir removes the retained slot")
	}()
	require.NoError(t, pool.drainerPoolSubmit(context.Background(), drainer))

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("drainer never entered the factory")
	}

	closeDone := make(chan struct{})
	go func() {
		pool.drainerPoolClose()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drainerPoolClose hung on an un-cancellable drainer")
	}

	// Close returned, but the worker is still blocked while trying to connect.
	// The directory is still locked, even though it has no .failed file.
	assert.NotEmpty(t, pool.drainerPoolSnapshot(),
		"wedged drainer must remain tracked until actual cleanup")
	lock, lockErr := qwpSfAcquireSlotLock(dir)
	if lockErr == nil {
		_ = lock.close()
	}
	assert.ErrorIs(t, lockErr, qwpSfErrLockBusy)
	assert.Equal(t, qwpSfDrainOutcomePending, drainer.drainerOutcome())
	_, statErr := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(statErr), "a timed-out observation must not quarantine the retained slot")
}

func TestQwpSfDrainerPoolRejectsAfterClose(t *testing.T) {
	pool := qwpSfNewDrainerPool(1)
	pool.drainerPoolClose()
	d := qwpSfNewOrphanDrainer(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes,
		nil, nil, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	err := pool.drainerPoolSubmit(context.Background(), d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// A panic while opening a drainer's connection must report a permanent failure,
// not crash the process. Keep its disk slot locked without marking the data as
// corrupt. The pool must still be able to run a different drainer: with a limit
// of one running task, the next task's success shows that the failed task no
// longer uses that running-task allowance. Its disk slot remains locked.
func TestQwpSfDrainerPoolSurvivesFactoryPanic(t *testing.T) {
	panicDir, child := terminalDrainerTestDir(t)
	if !child {
		return
	}
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	pool := qwpSfNewDrainerPool(1)
	defer pool.drainerPoolClose()

	const segSize int64 = 4096

	// Unacked slot + a factory that panics. The drainer must get past
	// drainerRun's already-drained short-circuit and into the connect
	// phase for the factory to be reached.
	{
		engine, err := qwpSfNewCursorEngine(panicDir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}
	panicFactory := func(context.Context, int) (*qwpTransport, error) {
		panic("boom from clientFactory")
	}
	panicDrainer := qwpSfNewOrphanDrainer(
		panicDir, segSize, qwpSfUnlimitedTotalBytes,
		panicFactory, nil,
		time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	require.NoError(t, pool.drainerPoolSubmit(context.Background(), panicDrainer))

	// Keep the failed drainer's resources. The panic does not prove that its
	// stored data is corrupt.
	require.Eventually(t, func() bool {
		return panicDrainer.drainerOutcome() == qwpSfDrainOutcomeFailed
	}, 2*time.Second, 5*time.Millisecond,
		"factory panic must surface as a Failed outcome, not crash the host")
	require.Eventually(t, func() bool { return errors.Is(pool.cleanupResult(), ErrCleanupFailed) }, time.Second, time.Millisecond)
	assertTerminalDrainerRetained(t, panicDir)

	// A different drainer can run even though the failed one's disk slot stays
	// locked. The pool allows only one running task, so this also checks that
	// the failed task no longer occupies that allowance.
	healthyDir := t.TempDir()
	{
		engine, err := qwpSfNewCursorEngine(healthyDir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}
	healthyDrainer := qwpSfNewOrphanDrainer(
		healthyDir, segSize, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv), nil,
		time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	require.NoError(t, pool.drainerPoolSubmit(context.Background(), healthyDrainer))
	require.Eventually(t, func() bool {
		return healthyDrainer.drainerOutcome() == qwpSfDrainOutcomeSuccess
	}, 5*time.Second, 10*time.Millisecond,
		"follow-up drainer must run after the panicking one freed its semaphore slot")
	pool.drainerPoolClose()
	require.ErrorIs(t, pool.cleanupResult(), ErrCleanupFailed)
	panicDrainer = nil
	assertTerminalDrainerRetained(t, panicDir)
}

// TestQwpSfDrainerUsesSharedTracker verifies the Phase 5 wiring:
// a drainer constructed with a shared tracker records its initial
// dial outcome onto that tracker (idx=0 becomes Healthy), so
// foreground PickNext observations are kept consistent across
// every caller drawing from the same connect-string addr= list.
func TestQwpSfDrainerUsesSharedTracker(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	dir := t.TempDir()
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("drainme"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	tracker := newQwpHostTracker(1, "", qwpTargetAny)
	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		tracker,
		1*time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.drainerRun(context.Background())
	require.Equal(t, qwpSfDrainOutcomeSuccess, drainer.drainerOutcome())

	// The shared tracker must now show host 0 as Healthy — the
	// drainer's bind landed there and reported success.
	snap := tracker.snapshot()
	assert.Equal(t, qwpHostHealthy, snap[0].state,
		"shared tracker must reflect drainer's successful bind")
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

// Regression: a server that completes the WS upgrade and accepts our
// frames but never ACKs and never drops the connection must not wedge
// the drainer forever. Without a no-progress watchdog the drain loop
// spins on the poll interval indefinitely; on Close it would exit
// Stopped (no .failed sentinel), so every future process start would
// re-adopt the same slot in full — an unbounded re-adoption livelock.
// The watchdog must quarantine the slot with a .failed sentinel after
// reconnectMaxDuration of zero ACK progress on a live connection.
func TestQwpSfDrainerMarksFailedWhenConnectedButNeverAcked(t *testing.T) {
	// The 300ms budget below is deliberately sub-floor to keep the watchdog
	// fast; lower the production floor (30s) for the duration of this test.
	defer func(orig time.Duration) { qwpSfMinNoProgressBudget.store(orig) }(qwpSfMinNoProgressBudget.load())
	qwpSfMinNoProgressBudget.store(10 * time.Millisecond)

	// silentAcks: read frames forever, never ACK, keep the
	// connection open — exactly the wedged-but-connected scenario.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{silentAcks: true})
	defer srv.Close()

	dir := t.TempDir()
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	// reconnectMaxDuration doubles as the no-progress budget. Keep it
	// short so the watchdog fires quickly; the connection stays up
	// the whole time, so the (separately bounded) reconnect path is
	// never entered and cannot mask the watchdog.
	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		nil,
		300*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond,
	)

	done := make(chan struct{})
	go func() {
		drainer.drainerRun(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("drainer never terminated — no-progress watchdog missing (livelock)")
	}

	assert.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	body, err := os.ReadFile(filepath.Join(dir, qwpSfFailedSentinelName))
	require.NoError(t, err)
	assert.Contains(t, string(body), "no drain progress")
	// The slot now carries .sfa + .failed, so it is no longer a
	// re-adoption candidate: a future process start won't re-adopt it.
	assert.False(t, qwpSfIsCandidateOrphan(dir),
		"slot must be quarantined (not a re-adoption candidate) after the watchdog fires")
}

// TestQwpSfDrainerRetriesDownServerInsteadOfQuarantining pins Invariant B:
// a transport outage that outlasts the former reconnect budget must not
// quarantine the slot — the drainer keeps retrying with capped backoff and
// drains once the server is reachable again.
func TestQwpSfDrainerRetriesDownServerInsteadOfQuarantining(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	dir := t.TempDir()
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	var up atomic.Bool
	factory := func(ctx context.Context, idx int) (*qwpTransport, error) {
		if !up.Load() {
			return nil, errors.New("dial tcp: connect: connection refused")
		}
		return qwpSfDialFor(srv)(ctx, idx)
	}

	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		factory,
		nil,
		50*time.Millisecond /* former budget */, time.Millisecond, 5*time.Millisecond,
	)
	done := make(chan struct{})
	go func() { drainer.drainerRun(context.Background()); close(done) }()

	// Outlast the former budget several times over: still retrying, no sentinel.
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, qwpSfDrainOutcomePending, drainer.drainerOutcome(),
		"drainer must keep retrying a down server, not fail")
	_, statErr := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(statErr), "a down server must not drop .failed")

	up.Store(true)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainer did not finish after the server came up")
	}
	assert.Equal(t, qwpSfDrainOutcomeSuccess, drainer.drainerOutcome())
}

// TestQwpSfDrainerAllReplicaWindowRetriesAndFiresPrimaryUnavailable pins the
// graceful-failover window: while every endpoint 421-role-rejects (all
// replicas), the drainer fires OnPrimaryUnavailable per sweep, keeps retrying
// without quarantining, and drains once a primary reappears.
func TestQwpSfDrainerAllReplicaWindowRetriesAndFiresPrimaryUnavailable(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	dir := t.TempDir()
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}

	var promoted atomic.Bool
	factory := func(ctx context.Context, idx int) (*qwpTransport, error) {
		if !promoted.Load() {
			return nil, &QwpUpgradeRejectError{StatusCode: 421, Role: "REPLICA"}
		}
		return qwpSfDialFor(srv)(ctx, idx)
	}

	var primaryUnavailable atomic.Int64
	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		factory,
		nil,
		50*time.Millisecond, time.Millisecond, 5*time.Millisecond,
	)
	drainer.listener = QwpBackgroundDrainerListener{
		OnPrimaryUnavailable: func(_ string, attempt int) { primaryUnavailable.Store(int64(attempt)) },
	}
	done := make(chan struct{})
	go func() { drainer.drainerRun(context.Background()); close(done) }()

	require.Eventually(t, func() bool { return primaryUnavailable.Load() >= 2 },
		2*time.Second, 5*time.Millisecond,
		"OnPrimaryUnavailable should fire once per all-replica sweep")
	assert.Equal(t, qwpSfDrainOutcomePending, drainer.drainerOutcome(),
		"an all-replica window must keep the drainer retrying")
	_, statErr := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(statErr), "an all-replica window must not drop .failed")

	promoted.Store(true)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainer did not finish after a primary reappeared")
	}
	assert.Equal(t, qwpSfDrainOutcomeSuccess, drainer.drainerOutcome())
}

func TestQwpSfDrainerNoProgressBudgetFloor(t *testing.T) {
	// A tiny reconnect_max_duration (set to fail the blocking initial connect
	// fast) must not shrink the live-connection no-progress watchdog below the
	// floor, or a healthy-but-slow adopted slot could be quarantined early.
	d := &qwpSfOrphanDrainer{reconnectMaxDuration: time.Millisecond}
	assert.Equal(t, qwpSfMinNoProgressBudget.load(), d.noProgressBudget(),
		"a sub-floor reconnectMaxDuration must be raised to the floor")

	// A value above the floor is honored exactly.
	d = &qwpSfOrphanDrainer{reconnectMaxDuration: 10 * time.Minute}
	assert.Equal(t, 10*time.Minute, d.noProgressBudget(),
		"a reconnectMaxDuration above the floor is used as-is")

	// Unset (zero) falls back to the default, which is above the floor.
	d = &qwpSfOrphanDrainer{}
	assert.Equal(t, qwpSfDefaultReconnectMaxDuration, d.noProgressBudget(),
		"an unset reconnectMaxDuration falls back to the default")
}

// The .failed sentinel is permanent — nothing in the client removes it — so a
// local I/O fault must not earn one. The slot keeps its data and stays
// eligible for the next foreground scan; only a recovery that proves the slot
// inconsistent quarantines it.
func TestQwpSfDrainerLocalIOErrorLeavesSlotEligible(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod-based permission denial is not portable to Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission bits; cannot induce EACCES")
	}
	slot := t.TempDir()
	const segSize int64 = 4096
	{
		engine, err := qwpSfNewCursorEngine(slot, segSize, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	}
	segments, err := filepath.Glob(filepath.Join(slot, "*.sfa"))
	require.NoError(t, err)
	require.NotEmpty(t, segments)
	for _, path := range segments {
		require.NoError(t, os.Chmod(path, 0o000))
		t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	}

	d := qwpSfNewOrphanDrainer(slot, segSize, qwpSfUnlimitedTotalBytes, nil, nil, 0, 0, 0)
	d.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeFailed, d.drainerOutcome())
	assert.Contains(t, d.drainerLastError(), "permission denied")
	_, statErr := os.Stat(filepath.Join(slot, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(statErr),
		"a transient local fault must not disqualify the slot forever")
	assert.True(t, qwpSfIsCandidateOrphan(slot),
		"the slot must still be adopted by the next scan")
}

// TestQwpSfDrainerOpenFailureSurvivesPanickingLogger pins that reporting an
// operational open failure cannot kill the process. Each drainer runs on its
// own goroutine whose only recover is the one at the top of drainerRun, and
// this report reaches a user-supplied slog handler that is free to panic.
func TestQwpSfDrainerOpenFailureSurvivesPanickingLogger(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	dir := t.TempDir()
	// A full disk is an operational failure, not proof that the slot is
	// inconsistent, so the drainer logs it and leaves the slot for a later scan
	// — the branch this test needs to reach.
	originalReserve := qwpSfReserveNewBlocksFn.load()
	qwpSfReserveNewBlocksFn.store(func(*os.File, int64, int64) error { return syscall.ENOSPC })
	t.Cleanup(func() { qwpSfReserveNewBlocksFn.store(originalReserve) })

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		nil,
		time.Second, 10*time.Millisecond, 100*time.Millisecond,
	)
	drainer.logger = slog.New(panicOnHandleSlog{})

	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	_, statErr := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(statErr), "an operational open failure must leave the slot eligible")
}
