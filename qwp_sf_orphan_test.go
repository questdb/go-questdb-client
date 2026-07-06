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
	"strings"
	"sync/atomic"
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

	var unavailable, persistent, lastAttempts int
	drainer := qwpSfNewOrphanDrainer(
		dir, segSize, qwpSfUnlimitedTotalBytes,
		qwpSfDurableDialFor(srv),
		nil,
		5*time.Second, time.Millisecond, 5*time.Millisecond,
	)
	drainer.durableAckMode = true
	drainer.listener = QwpBackgroundDrainerListener{
		OnDurableAckUnavailable:       func(string, int) { unavailable++ },
		OnDurableAckPersistentFailure: func(_ string, attempts int, _ time.Duration) { persistent++; lastAttempts = attempts },
	}
	drainer.drainerRun(context.Background())

	assert.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	body, err := os.ReadFile(filepath.Join(dir, qwpSfFailedSentinelName))
	require.NoError(t, err)
	assert.Contains(t, string(body), "durable-ack")
	assert.Equal(t, qwpMaxDurableAckMismatchAttempts, unavailable, "one OnDurableAckUnavailable per mismatch up to the cap")
	assert.Equal(t, 1, persistent, "OnDurableAckPersistentFailure fires exactly once")
	assert.Equal(t, qwpMaxDurableAckMismatchAttempts, lastAttempts)

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
	prevGrace := qwpSfDrainerPoolCloseGrace
	qwpSfDrainerPoolCloseGrace = 50 * time.Millisecond
	defer func() { qwpSfDrainerPoolCloseGrace = prevGrace }()

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

// Regression: a drainer parked inside clientFactory(ctx) — e.g. a
// long-running TCP dial / WS upgrade against a black-holed peer —
// must not survive past drainerPoolClose. The pool cancels its
// master ctx after the polite-stop grace; the dial unwinds; the
// drainer goroutine exits.
func TestQwpSfDrainerPoolCancelsBlockingDialOnClose(t *testing.T) {
	prevGrace := qwpSfDrainerPoolCloseGrace
	qwpSfDrainerPoolCloseGrace = 50 * time.Millisecond
	defer func() { qwpSfDrainerPoolCloseGrace = prevGrace }()

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

// TestQwpSfDrainerPoolBoundedOnUncancellableDrainer is a regression
// test for M15: a drainer wedged in I/O the master-ctx cancel cannot
// reach — modelled here by a clientFactory that ignores its ctx, the
// way drainerRun's engine-open flock / mmap / CRC scan does — must
// not make drainerPoolClose hang forever. After the polite grace and
// the post-cancel hard grace both elapse, close abandons the
// straggler and returns; the slot stays adoptable.
func TestQwpSfDrainerPoolBoundedOnUncancellableDrainer(t *testing.T) {
	prevGrace := qwpSfDrainerPoolCloseGrace
	prevHard := qwpSfDrainerPoolHardCloseGrace
	qwpSfDrainerPoolCloseGrace = 50 * time.Millisecond
	qwpSfDrainerPoolHardCloseGrace = 50 * time.Millisecond
	defer func() {
		qwpSfDrainerPoolCloseGrace = prevGrace
		qwpSfDrainerPoolHardCloseGrace = prevHard
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
	defer close(block) // release at test end so the goroutine unwinds
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

	// Abandoned, not joined: the goroutine is still parked in the
	// factory, so it is still tracked and still Pending. Its slot is
	// left intact (no .failed sentinel) for a future sender to adopt.
	assert.NotEmpty(t, pool.drainerPoolSnapshot(),
		"wedged drainer must still be tracked (abandoned, not joined)")
	assert.Equal(t, qwpSfDrainOutcomePending, drainer.drainerOutcome())
	_, statErr := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	assert.True(t, os.IsNotExist(statErr), "must not quarantine an abandoned slot")
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

// TestQwpSfDrainerPoolSurvivesFactoryPanic asserts a panic in the
// user-supplied clientFactory — invoked on the drainer goroutine from
// drainerRun's connect phase, before the send loop's own recover is in
// play — is converted into a latched terminal failure rather than
// crashing the host process (which would take the whole pool and the
// foreground sender down with it). The panicking drainer must end Failed
// with a quarantine sentinel, and the pool's semaphore/active-list
// bookkeeping must survive intact: on a cap-1 pool a follow-up drainer
// can only run once the panicking one releases its slot, so its reaching
// Success proves the worker's cleanup defers ran (no crash, no leaked
// slot).
func TestQwpSfDrainerPoolSurvivesFactoryPanic(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	pool := qwpSfNewDrainerPool(1)
	defer pool.drainerPoolClose()

	const segSize int64 = 4096

	// Unacked slot + a factory that panics. The drainer must get past
	// drainerRun's already-drained short-circuit and into the connect
	// phase for the factory to be reached.
	panicDir := t.TempDir()
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

	// The panic surfaces as a Failed outcome with a quarantine sentinel,
	// not a process crash.
	require.Eventually(t, func() bool {
		return panicDrainer.drainerOutcome() == qwpSfDrainOutcomeFailed
	}, 2*time.Second, 5*time.Millisecond,
		"factory panic must surface as a Failed outcome, not crash the host")
	body, err := os.ReadFile(filepath.Join(panicDir, qwpSfFailedSentinelName))
	require.NoError(t, err)
	assert.Contains(t, string(body), "panicked")

	// Pool machinery survived: a healthy drainer submitted to the same
	// cap-1 pool runs to Success, which is only possible once the
	// panicking drainer's worker released its semaphore slot on its way
	// out.
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
