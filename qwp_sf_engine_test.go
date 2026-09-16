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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A full disk is the ordinary reason storage will not put blocks behind the
// ack watermark, and for a drainer that slot's rows are what would free the
// disk. The engine opens without the watermark instead of refusing the slot;
// the cost is that already-acked frames can replay.
func TestQwpSfEngineOpensWithoutAnUnbackableAckWatermark(t *testing.T) {
	original := qwpSfAckWatermarkWriteAt.load()
	qwpSfAckWatermarkWriteAt.store(func(*os.File, []byte, int64) (int, error) {
		return 0, syscall.ENOSPC
	})
	t.Cleanup(func() { qwpSfAckWatermarkWriteAt.store(original) })

	dir := t.TempDir()
	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err, "a watermark storage cannot back must not stop the slot")
	require.NotNil(t, e)
	defer func() { _ = e.engineClose() }()
	assert.Nil(t, e.watermark, "the engine runs without the watermark")

	fsn, appendErr := e.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, appendErr)
	assert.Equal(t, int64(0), fsn)
}

func TestQwpSfEngineMemoryModeAppend(t *testing.T) {
	e, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	for i := int64(0); i < 5; i++ {
		fsn, err := e.engineAppendBlocking(context.Background(), []byte("frame"))
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
			_, err := e.engineAppendBlocking(context.Background(), []byte{byte(i), byte(i + 1)})
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

func TestQwpSfEngineRecoveredEmptyActiveTailPreservesUnackedSegmentsOnClose(t *testing.T) {
	dir := t.TempDir()
	sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	active := createRecoverySegment(t, dir, "sf-active.sfa", 1)
	createRecoveryManifest(t, dir, 0, 1, sealed, active)
	closeRecoverySegments(t, sealed, active)

	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(0), e.enginePublishedFsn())
	require.Equal(t, int64(-1), e.engineAckedFsn())
	require.NoError(t, e.engineClose())

	for _, name := range []string{"sf-initial.sfa", "sf-active.sfa"} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, "%s must survive while FSN 0 is unacknowledged", name)
	}
	_, err = os.Stat(filepath.Join(dir, qwpSfManifestFileName))
	require.NoError(t, err, "the manifest must survive with the unacknowledged chain")
}

func TestQwpSfSenderRotationManifestFailureRetainsRowsForRetry(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096

	active, err := qwpSfCreateSegment(filepath.Join(dir, "sf-initial.sfa"), 0, segSize)
	require.NoError(t, err)
	manifest, err := qwpSfManifestCreate(dir, 0, 0)
	require.NoError(t, err)
	ring := qwpSfNewSegmentRing(active, segSize)
	ring.manifest = manifest
	defer func() { _ = ring.segmentRingClose() }()

	// Leave too little room for even the smallest encoded QWP row, forcing the
	// producer-visible Flush below through the live rotation path.
	filler := make([]byte, segSize-qwpSfHeaderSize-qwpSfFrameHeaderSize-16)
	firstFsn := ring.appendOrFsn(filler)
	require.Equal(t, int64(0), firstFsn)

	spare, err := qwpSfCreateSegment(filepath.Join(dir, "sf-spare.sfa"), ring.nextSeqHint(), segSize)
	require.NoError(t, err)
	require.NoError(t, ring.installHotSpare(spare))

	e := &qwpSfCursorEngine{ring: ring, appendDeadline: time.Second}
	s, err := newQwpCursorLineSender(0, 0, 0, 0, e, &qwpSfSendLoop{}, time.Second)
	require.NoError(t, err)
	require.NoError(t, s.Table("t").Int64Column("v", 42).AtNow(context.Background()))
	require.Equal(t, 1, s.pendingRowCount)

	injected := errors.New("injected manifest fsync failure")
	syncCalls := 0
	syncHook := func(f *os.File) error {
		syncCalls++
		if syncCalls == 1 {
			return injected
		}
		return f.Sync()
	}
	qwpSfManifestSync.Store(&syncHook)
	t.Cleanup(func() { qwpSfManifestSync.Store(nil) })

	err = s.Flush(context.Background())
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, err, ErrSfDurability)
	require.NotErrorIs(t, err, ErrBackpressureTimeout)
	require.Equal(t, 1, s.pendingRowCount, "an unappended row must remain pending")
	require.Equal(t, int64(0), ring.segmentRingPublishedFsn())
	require.Equal(t, int64(1), ring.nextSeqHint())
	require.Same(t, active, ring.getActiveSegment())
	require.Zero(t, ring.sealedSegmentCount())

	fsn, err := s.FlushAndGetSequence(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), fsn)
	require.Zero(t, s.pendingRowCount)
	require.Equal(t, int64(1), ring.segmentRingPublishedFsn())
	require.Same(t, spare, ring.getActiveSegment())
	require.Equal(t, 1, ring.sealedSegmentCount())
}

// TestQwpSfSenderRotationFailureInsideAtRetainsRowsForRetry is the auto-flush
// counterpart of the Flush case above, and the likelier one in practice: a
// producer that never calls Flush meets a rotation that cannot commit from
// inside At/AtNow. The promise that the rows stay pending rests on autoFlush
// skipping resetAfterFlush when the enqueue fails, which nothing else pins.
func TestQwpSfSenderRotationFailureInsideAtRetainsRowsForRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const segSize int64 = 4096

	active, err := qwpSfCreateSegment(filepath.Join(dir, "sf-initial.sfa"), 0, segSize)
	require.NoError(t, err)
	manifest, err := qwpSfManifestCreate(dir, 0, 0)
	require.NoError(t, err)
	ring := qwpSfNewSegmentRing(active, segSize)
	ring.manifest = manifest
	defer func() { _ = ring.segmentRingClose() }()

	// Leave too little room for even the smallest encoded QWP row, so the row
	// below has to rotate.
	filler := make([]byte, segSize-qwpSfHeaderSize-qwpSfFrameHeaderSize-16)
	require.Equal(t, int64(0), ring.appendOrFsn(filler))

	spare, err := qwpSfCreateSegment(filepath.Join(dir, "sf-spare.sfa"), ring.nextSeqHint(), segSize)
	require.NoError(t, err)
	require.NoError(t, ring.installHotSpare(spare))

	e := &qwpSfCursorEngine{ring: ring, appendDeadline: time.Second}
	// One row per auto-flush, so AtNow itself carries the enqueue.
	s, err := newQwpCursorLineSender(1, 0, 0, 0, e, &qwpSfSendLoop{}, time.Second)
	require.NoError(t, err)

	injected := errors.New("injected manifest fsync failure")
	syncCalls := 0
	syncHook := func(f *os.File) error {
		syncCalls++
		if syncCalls == 1 {
			return injected
		}
		return f.Sync()
	}
	qwpSfManifestSync.Store(&syncHook)
	t.Cleanup(func() { qwpSfManifestSync.Store(nil) })

	err = s.Table("t").Int64Column("v", 42).AtNow(ctx)
	require.ErrorIs(t, err, ErrSfDurability)
	require.ErrorIs(t, err, injected)
	require.NotErrorIs(t, err, ErrBackpressureTimeout)
	require.Equal(t, 1, s.pendingRowCount,
		"a row the auto-flush could not append must stay pending, not be counted as flushed")
	require.Equal(t, int64(0), ring.segmentRingPublishedFsn())
	require.Same(t, active, ring.getActiveSegment())

	// The very same row goes through once the disk accepts the commit.
	fsn, err := s.FlushAndGetSequence(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), fsn)
	require.Zero(t, s.pendingRowCount)
	require.Equal(t, int64(1), ring.segmentRingPublishedFsn())
	require.Same(t, spare, ring.getActiveSegment())
}

func TestQwpSfEngineSlotLockBlocksDouble(t *testing.T) {
	dir := t.TempDir()
	e1, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e1.engineClose() }()

	_, err = qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.ErrorIs(t, err, qwpSfErrLockBusy)
	assert.Contains(t, err.Error(), "held by this process",
		"both engines are ours; pointing at another process would send the reader hunting for one that does not exist")
}

func TestQwpSfEngineFullDrainUnlinksFiles(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 4096
	e, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		fsn, err := e.engineAppendBlocking(context.Background(), []byte("hi"))
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
		assert.NotEqual(t, qwpSfManifestFileName, en.Name())
		assert.NotEqual(t, qwpSfAckWatermarkFileName, en.Name())
	}
}

func TestQwpSfEngineFullDrainBarrierFailureRetainsSlot(t *testing.T) {
	for _, barrier := range []string{"watermark-overflow", "watermark-sync", "manifest-sync"} {
		t.Run(barrier, func(t *testing.T) {
			dir := t.TempDir()
			sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "acked")
			active := createRecoverySegment(t, dir, "sf-active.sfa", 1)
			createRecoveryManifest(t, dir, 0, 1, sealed, active)
			closeRecoverySegments(t, sealed, active)

			engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.NoError(t, err)
			resetHook := func() {}
			t.Cleanup(func() {
				resetHook()
				_ = engine.engineClose()
			})
			// Stop the manager before ACKing so it cannot trim the sealed segment
			// or collapse the manifest ahead of the close path under test.
			require.True(t, engine.manager.segmentManagerClose())
			engine.engineAcknowledge(0)
			require.Equal(t, int64(0), engine.enginePublishedFsn())
			require.Equal(t, int64(0), engine.engineAckedFsn())

			before, err := os.ReadDir(dir)
			require.NoError(t, err)
			beforeNames := make([]string, 0, len(before))
			for _, entry := range before {
				beforeNames = append(beforeNames, entry.Name())
			}

			injected := errors.New("injected " + barrier + " failure")
			switch barrier {
			case "watermark-overflow":
				engine.watermark.mu.Lock()
				engine.watermark.generation = math.MaxInt64
				engine.watermark.mu.Unlock()
				injected = qwpSfErrGenerationOverflow
				resetHook = func() {
					engine.watermark.mu.Lock()
					engine.watermark.generation = 0
					engine.watermark.mu.Unlock()
				}
			case "watermark-sync":
				hook := func(*os.File) error { return injected }
				qwpSfAckWatermarkSync.Store(&hook)
				resetHook = func() { qwpSfAckWatermarkSync.Store(nil) }
			case "manifest-sync":
				hook := func(*os.File) error { return injected }
				qwpSfManifestSync.Store(&hook)
				resetHook = func() { qwpSfManifestSync.Store(nil) }
			}
			err = engine.engineClose()
			require.ErrorIs(t, err, injected)
			require.False(t, engine.engineCloseCompleted())
			require.False(t, engine.ring.resourcesReleased())

			after, err := os.ReadDir(dir)
			require.NoError(t, err)
			afterNames := make([]string, 0, len(after))
			for _, entry := range after {
				afterNames = append(afterNames, entry.Name())
			}
			require.ElementsMatch(t, beforeNames, afterNames, "failed barrier must not unlink slot files")
			for _, name := range beforeNames {
				require.FileExists(t, filepath.Join(dir, name))
			}
			_, err = qwpSfAcquireSlotLock(dir)
			require.Error(t, err, "failed barrier must retain the slot flock")

			resetHook()
			waitQwpSfEngineCleanup(t, engine)
			require.NoError(t, engine.engineClose(), "later Close observes the original owner's completed retry")
			require.True(t, engine.engineCloseCompleted())
			for _, name := range beforeNames {
				if filepath.Ext(name) == ".sfa" ||
					name == qwpSfManifestFileName ||
					name == qwpSfAckWatermarkFileName ||
					name == qwpSfSymbolDictFileName {
					_, statErr := os.Stat(filepath.Join(dir, name))
					require.True(t, os.IsNotExist(statErr), "completed cleanup retained drained file %s", name)
				}
			}
			lock, err := qwpSfAcquireSlotLock(dir)
			require.NoError(t, err, "the joined cleanup owner must have released the slot flock")
			require.NoError(t, lock.close())
		})
	}
}

func TestQwpSfEngineTrimAdvancesManifest(t *testing.T) {
	dir := t.TempDir()
	const segSize int64 = 72
	e, err := qwpSfNewCursorEngine(dir, segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	require.Eventually(t, func() bool { return !e.ring.needsHotSpare() }, time.Second, time.Millisecond)
	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err)
	}
	e.engineAcknowledge(1)
	require.Eventually(t, func() bool { return e.ring.sealedSegmentCount() == 0 }, time.Second, time.Millisecond)

	m, err := qwpSfManifestOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, m)
	defer m.close()
	assert.Equal(t, int64(2), m.headBase)
	assert.Equal(t, int64(2), m.activeBase)
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
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "iteration %d", i)
	}
	// The next append must time out.
	start := time.Now()
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrBackpressureTimeout))
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond)
	// Backpressure stall counter incremented.
	assert.GreaterOrEqual(t, e.engineTotalBackpressureStalls(), int64(1))
	// Spec §16: with no loop wired (or loop reports "not
	// reconnecting"), the message must say "publishing but slow".
	assert.Contains(t, err.Error(), "wire publishing but slow")
}

// Spec §16 mandates the backpressure-timeout error distinguish
// "publishing but slow" from "reconnecting", and the reconnecting
// variant must include attempt count and outage start.
func TestQwpSfEngineBackpressureTimeoutReconnecting(t *testing.T) {
	const segSize int64 = 96
	e, err := qwpSfNewCursorEngine("", segSize, segSize, 50*time.Millisecond)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	outageStart := time.Now().Add(-3 * time.Second)
	e.engineSetReconnectStatusGetter(func() (bool, int64, time.Time) {
		return true, 7, outageStart
	})

	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "iteration %d", i)
	}
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrBackpressureTimeout))
	msg := err.Error()
	assert.Contains(t, msg, "reconnecting")
	assert.Contains(t, msg, "attempts=7")
	assert.Contains(t, msg, "outage-elapsed=")
	assert.Contains(t, msg, "outage-start=")

	// After the loop reports "no longer reconnecting", the next
	// timeout falls back to the slow-publish wording.
	e.engineSetReconnectStatusGetter(func() (bool, int64, time.Time) {
		return false, 0, time.Time{}
	})
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wire publishing but slow")
	assert.NotContains(t, err.Error(), "reconnecting")

	// Detaching the getter (nil) is also valid — same fallback wording.
	e.engineSetReconnectStatusGetter(nil)
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wire publishing but slow")
}

// A send-loop HALT latched while a producer is parked on a full ring
// must surface as the terminal error, fast — not as a backpressure
// timeout after the full append deadline. The engine polls the wired
// terminal-error getter on every spin iteration so the parked producer
// fails fast with the real cause.
func TestQwpSfEngineBackpressureTerminalErrorFailFast(t *testing.T) {
	const segSize int64 = 96 // 24 header + 72 payload; three 24B frames fill it
	// A long deadline makes the fail-fast unambiguous: without the
	// terminal-error check the call would block ~30s.
	e, err := qwpSfNewCursorEngine("", segSize, segSize, 30*time.Second)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	// Fill the active segment. These appends succeed outright and never
	// enter the backpressure spin, so no getter need be wired yet.
	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "iteration %d", i)
	}

	// Simulate the send loop HALTing: a terminal error is now latched
	// and the ring will never drain again (ACK-driven trim has ceased).
	halt := errors.New("qwp/sf: send loop HALTed")
	e.engineSetTerminalErrorGetter(func() error { return halt })

	// Appending into the full ring parks the producer; the first spin
	// iteration observes the terminal error and returns it.
	start := time.Now()
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, halt),
		"parked producer must return the latched terminal error, got: %v", err)
	assert.False(t, errors.Is(err, ErrBackpressureTimeout),
		"the terminal error must not be masked behind a backpressure timeout")
	assert.Less(t, elapsed, 5*time.Second,
		"must fail fast on the latched HALT, not wait out the 30s deadline")
	// It still counts as one backpressure stall: the producer did park
	// before observing the terminal error.
	assert.GreaterOrEqual(t, e.engineTotalBackpressureStalls(), int64(1))
}

// A getter that reports a healthy loop (nil) must not perturb the
// normal backpressure path: the spin still times out with the generic
// backpressure error.
func TestQwpSfEngineBackpressureHealthyGetterStillTimesOut(t *testing.T) {
	const segSize int64 = 96
	e, err := qwpSfNewCursorEngine("", segSize, segSize, 50*time.Millisecond)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	// Loop reports healthy throughout (mirrors steady state / an
	// in-progress reconnect that has not yet exhausted its budget).
	e.engineSetTerminalErrorGetter(func() error { return nil })

	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "iteration %d", i)
	}
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrBackpressureTimeout))
}

func TestQwpSfEnginePayloadTooLarge(t *testing.T) {
	const segSize int64 = 256
	e, err := qwpSfNewCursorEngine("", segSize, segSize*4, time.Second)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	huge := make([]byte, segSize) // can never fit (header + envelope alone exceeds)
	_, err = e.engineAppendBlocking(context.Background(), huge)
	require.Error(t, err)
	assert.True(t, errors.Is(err, qwpSfErrPayloadTooLarge))
}

// A drainer's recovery takes the sanitized-residue retry too. The
// sanitization is already durable when the error is raised, so the same bytes
// recover on the very next pass — a slot that a foreground sender recovers
// must not be abandoned just because a drainer reached it first.
func TestQwpSfDrainerEngineRetriesSanitizedResidue(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, dir, 0, 1, s0, s1)
	off := s0.publishedOffset()
	s0.buf[off+20] = 0x7f // all-zero bad header with non-zero payload farther on
	closeRecoverySegments(t, s0, s1)

	engine, err := qwpSfNewCursorEngineForDrainer(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.engineClose() })
	assert.Equal(t, int64(1), engine.enginePublishedFsn())
}

// Terminal cleanup completes when the slot directory has disappeared under the
// engine. The unlinks and the manifest removal already treat "gone" as done;
// so must the directory fsync, or the retry owner keeps the flock and (in the
// pool) the slot's index reservation forever.
func TestQwpSfEngineDrainedCleanupToleratesMissingSlotDir(t *testing.T) {
	dir := t.TempDir()
	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	fsn, err := e.engineAppendBlocking(context.Background(), []byte("hi"))
	require.NoError(t, err)
	e.engineAcknowledge(fsn)
	// Stop the worker so it cannot drop a fresh hot spare into the directory
	// while the test is removing it.
	require.True(t, e.manager.segmentManagerClose())

	require.NoError(t, os.RemoveAll(dir))

	require.NoError(t, e.engineClose())
	assert.True(t, e.engineCloseCompleted())
}

// A slot whose maintenance keeps failing reports the storage fault to the
// producer parked on the ring it can no longer trim, instead of the generic
// backpressure timeout that blames a slow or disconnected server.
func TestQwpSfEngineBackpressureSurfacesMaintenanceFailure(t *testing.T) {
	const segSize int64 = 96 // 24 header + 72 payload; three 24B frames fill it
	// A long deadline makes the fail-fast unambiguous: without the maintenance
	// check the call would block ~30s.
	e, err := qwpSfNewCursorEngine("", segSize, segSize, 30*time.Second)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "iteration %d", i)
	}
	// Stop the worker so the failure run below is this test's alone.
	require.True(t, e.manager.segmentManagerClose())

	diskErr := errors.New("update sf-manifest.bin: no space left on device")
	now := time.Unix(1_700_000_000, 0)
	e.manager.now = func() time.Time { return now }
	e.manager.lastMaintenanceLog = now
	e.manager.recordServiceError(e.managerEntry, diskErr)
	require.NoError(t, e.managerEntry.entryMaintenanceError(),
		"do not publish a short failure")
	now = now.Add(qwpSfManagerMaintenanceFailureDuration - time.Nanosecond)
	e.manager.recordServiceError(e.managerEntry, diskErr)
	require.NoError(t, e.managerEntry.entryMaintenanceError(),
		"do not publish before one second")
	now = now.Add(time.Nanosecond)
	e.manager.recordServiceError(e.managerEntry, diskErr)

	start := time.Now()
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrSfDurability),
		"the parked producer must classify this as a local durability failure, got: %v", err)
	assert.True(t, errors.Is(err, diskErr), "the filesystem error must stay matchable")
	assert.False(t, errors.Is(err, ErrBackpressureTimeout),
		"a disk that refuses writes must not read as a slow or disconnected server")
	assert.Less(t, elapsed, 5*time.Second)

	// Maintenance recovering ends the run.
	e.managerEntry.entryMaintenanceSucceeded()
	assert.NoError(t, e.managerEntry.entryMaintenanceError())
}

// Tests may restore a timeout while a cleanup worker is still reading it.
// A plain variable would cause a data race, so reads and writes must be atomic.
// This test checks concurrent reads and writes under the race detector, even
// though other tests wait for their own workers to finish.
func TestQwpSfSwappableVarSurvivesConcurrentSwap(t *testing.T) {
	v := qwpSfSwappable(time.Second)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = v.load()
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		v.store(time.Duration(i) * time.Millisecond)
	}
	close(stop)
	<-done
	require.Equal(t, 999*time.Millisecond, v.load())
}

// A manager whose worker never started has no worker to wait for. Waiting on
// m.done in that case would prevent cleanup from ever releasing the slot.
func TestQwpSfManagerCloseWithoutStartIsQuiescent(t *testing.T) {
	m, err := qwpSfNewSegmentManager(4096, time.Second, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	start := time.Now()
	require.True(t, m.segmentManagerClose())
	require.Less(t, time.Since(start), qwpSfManagerCloseGrace.load(),
		"close must not wait on a worker that was never started")
	select {
	case <-m.done:
	default:
		t.Fatal("unstarted manager must be quiescent")
	}
}

// buildDrainedSlot lays out a slot as a fully-acked close leaves it just
// before the unlink sweep: a sealed segment below the committed head, the
// active segment at head == active, and the hot spare the manager minted for
// the next rotation. spareBase picks between the two shapes that spare can
// have — one base above an active segment holding frames, or the same base as
// an active segment a rotation just installed.
func buildDrainedSlot(t *testing.T, spareBase int64) string {
	t.Helper()
	dir := t.TempDir()
	var activePayloads []string
	if spareBase > 1 {
		activePayloads = []string{"b"}
	}
	sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	active := createRecoverySegment(t, dir, "sf-0000000000000001.sfa", 1, activePayloads...)
	spare := createRecoverySegment(t, dir, "sf-0000000000000002.sfa", spareBase)
	createRecoveryManifest(t, dir, 1, 1, sealed, active, spare)
	closeRecoverySegments(t, sealed, active, spare)
	watermark, err := qwpSfAckWatermarkOpenRequired(dir)
	require.NoError(t, err)
	advanced, err := watermark.persistIfAdvanced(spareBase - 1)
	require.NoError(t, err)
	require.True(t, advanced)
	require.NoError(t, watermark.sync())
	require.NoError(t, watermark.close())
	return dir
}

type qwpSfCrashNamespaceFile struct {
	data []byte
	mode os.FileMode
}

type qwpSfCrashNamespaceEvent struct {
	remove  string
	barrier bool
}

func qwpSfSnapshotCrashNamespace(t *testing.T, dir string) map[string]qwpSfCrashNamespaceFile {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	files := make(map[string]qwpSfCrashNamespaceFile, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		info, statErr := entry.Info()
		require.NoError(t, statErr)
		files[entry.Name()] = qwpSfCrashNamespaceFile{data: data, mode: info.Mode().Perm()}
	}
	return files
}

func qwpSfPermuteCrashOps(ops []string, visit func([]string)) {
	if len(ops) == 0 {
		visit(nil)
		return
	}
	var walk func(int)
	walk = func(at int) {
		if at == len(ops) {
			visit(append([]string(nil), ops...))
			return
		}
		for i := at; i < len(ops); i++ {
			ops[at], ops[i] = ops[i], ops[at]
			walk(at + 1)
			ops[at], ops[i] = ops[i], ops[at]
		}
	}
	walk(0)
}

func qwpSfVisitCrashEpochOrders(epoch []string, visit func([]string)) {
	for subset := 0; subset < 1<<len(epoch); subset++ {
		selected := make([]string, 0, len(epoch))
		for i, op := range epoch {
			if subset&(1<<i) != 0 {
				selected = append(selected, op)
			}
		}
		qwpSfPermuteCrashOps(selected, visit)
	}
}

func qwpSfMaterializeCrashNamespace(t *testing.T, initial map[string]qwpSfCrashNamespaceFile, durableOps []string) string {
	t.Helper()
	dir := t.TempDir()
	for name, file := range initial {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), file.data, file.mode))
	}
	for _, name := range durableOps {
		err := os.Remove(filepath.Join(dir, name))
		require.True(t, err == nil || errors.Is(err, os.ErrNotExist), "apply durable remove %s: %v", name, err)
	}
	return dir
}

func qwpSfAssertDrainedCrashStateRecoverable(
	t *testing.T,
	initial map[string]qwpSfCrashNamespaceFile,
	durableOps []string,
) {
	t.Helper()
	dir := qwpSfMaterializeCrashNamespace(t, initial, durableOps)
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	if errors.Is(err, qwpSfErrRecoveryFailClosed) {
		t.Fatalf("allowed durable state failed closed after operations %v: %v", durableOps, err)
	}
	if err != nil {
		return // Environmental errors are retryable and preserve the slot.
	}
	if ring != nil {
		defer func() { require.NoError(t, ring.segmentRingClose()) }()
		if ring.segmentRingHoldsFrames() {
			watermark, openErr := qwpSfAckWatermarkOpenRequired(dir)
			require.NoError(t, openErr)
			require.NotNil(t, watermark)
			acked := watermark.read()
			require.NoError(t, watermark.close())
			require.GreaterOrEqual(t, acked, ring.segmentRingPublishedFsn(),
				"every surviving frame belongs to the already-acknowledged prefix")
		}
	}
	_, failedErr := os.Stat(filepath.Join(dir, qwpSfFailedSentinelName))
	require.True(t, errors.Is(failedErr, os.ErrNotExist), "recovery wrote .failed for acknowledged data: %v", failedErr)
}

// TestQwpSfDrainedCleanupCrashEpochsRecover pins the durability epochs of a
// fully drained close. Metadata calls issued since the last directory barrier
// may become durable in any subset and order; a successful barrier commits all
// earlier calls before a dependent epoch starts. Every allowed persisted state
// must therefore recover as empty/already-acked or remain retryable, never fail
// closed over rows the server already acknowledged.
func TestQwpSfDrainedCleanupCrashEpochsRecover(t *testing.T) {
	// The argument starts from what the drained close commits before it sweeps,
	// so take that from a real one rather than trusting the fixture below to
	// mirror it.
	t.Run("premise", func(t *testing.T) {
		dir := t.TempDir()
		e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		var fsn int64
		for i := 0; i < 3; i++ {
			fsn, err = e.engineAppendBlocking(context.Background(), []byte("frame"))
			require.NoError(t, err)
		}
		e.engineAcknowledge(fsn)
		activeBase := e.engineActiveSegment().segmentBaseSeq()

		var head, active int64
		var swept []string
		observer := func(path string) {
			if len(swept) == 0 {
				m, openErr := qwpSfManifestOpen(dir)
				require.NoError(t, openErr)
				require.NotNil(t, m, "the sweep runs with the manifest still on disk")
				head, active = m.headBase, m.activeBase
				require.NoError(t, m.close())
			}
			swept = append(swept, filepath.Base(path))
		}
		qwpSfTestBeforeSegmentUnlinkHook.Store(&observer)
		t.Cleanup(func() { qwpSfTestBeforeSegmentUnlinkHook.Store(nil) })
		require.NoError(t, e.engineClose())
		qwpSfTestBeforeSegmentUnlinkHook.Store(nil)

		require.NotEmpty(t, swept, "a fully acked close must reach the unlink sweep")
		assert.Equal(t, activeBase, head, "the sweep starts from a manifest collapsed onto the active base")
		assert.Equal(t, activeBase, active)
	})

	for _, spareBase := range []int64{2, 1} {
		name := "spare-above-active"
		if spareBase == 1 {
			name = "spare-at-active"
		}
		t.Run(name, func(t *testing.T) {
			dir := buildDrainedSlot(t, spareBase)
			initial := qwpSfSnapshotCrashNamespace(t, dir)
			var events []qwpSfCrashNamespaceEvent
			manifestRemoved := false
			unlinkRecorder := func(path string) {
				events = append(events, qwpSfCrashNamespaceEvent{remove: filepath.Base(path)})
			}
			barrierRecorder := func(syncDir string) error {
				require.Equal(t, dir, syncDir)
				if !manifestRemoved {
					_, statErr := os.Stat(filepath.Join(dir, qwpSfManifestFileName))
					if errors.Is(statErr, os.ErrNotExist) {
						events = append(events, qwpSfCrashNamespaceEvent{remove: qwpSfManifestFileName})
						manifestRemoved = true
					} else {
						require.NoError(t, statErr)
					}
				}
				events = append(events, qwpSfCrashNamespaceEvent{barrier: true})
				return nil
			}
			qwpSfTestBeforeSegmentUnlinkHook.Store(&unlinkRecorder)
			qwpSfTestDirSyncHook.Store(&barrierRecorder)
			t.Cleanup(func() { qwpSfTestBeforeSegmentUnlinkHook.Store(nil) })
			t.Cleanup(func() { qwpSfTestDirSyncHook.Store(nil) })
			require.NoError(t, qwpSfUnlinkSegmentsAndSyncDir(dir))
			require.NoError(t, qwpSfRemoveManifestAndSyncDir(dir))
			qwpSfTestBeforeSegmentUnlinkHook.Store(nil)
			qwpSfTestDirSyncHook.Store(nil)
			require.True(t, manifestRemoved, "cleanup must remove the manifest")

			committed := make([]string, 0, 4)
			epoch := make([]string, 0, 4)
			caseNo := 0
			for _, event := range events {
				if event.remove != "" {
					epoch = append(epoch, event.remove)
					qwpSfVisitCrashEpochOrders(epoch, func(order []string) {
						durable := append(append([]string(nil), committed...), order...)
						t.Run(fmt.Sprintf("state-%03d", caseNo), func(t *testing.T) {
							qwpSfAssertDrainedCrashStateRecoverable(t, initial, durable)
						})
						caseNo++
					})
					continue
				}
				require.True(t, event.barrier)
				committed = append(committed, epoch...)
				epoch = epoch[:0]
				durable := append([]string(nil), committed...)
				t.Run(fmt.Sprintf("state-%03d-after-barrier", caseNo), func(t *testing.T) {
					qwpSfAssertDrainedCrashStateRecoverable(t, initial, durable)
				})
				caseNo++
			}
			require.Empty(t, epoch)
		})
	}
}

// TestQwpSfQuarantinedBytesCountAgainstTheBudget pins part two of the
// never-delete rule's budget story. Quarantined .corrupt files are preserved
// evidence the client never reclaims, so they count against
// sf_max_total_bytes: when they exhaust the budget, no new segment is minted
// and the producer sees the same non-terminal ErrBackpressureTimeout as when
// live data fills the cap. The operator regains the space by deleting the
// evidence, and minting resumes.
func TestQwpSfQuarantinedBytesCountAgainstTheBudget(t *testing.T) {
	const segSize int64 = 96 // 24 header + 72 payload region
	dir := t.TempDir()
	// Budget for two segments: the active plus one spare. A segment-sized
	// .corrupt file eats the spare's share.
	corrupt := filepath.Join(dir, "sf-dead.sfa.corrupt")
	require.NoError(t, os.WriteFile(corrupt, make([]byte, segSize), 0o644))

	e, err := qwpSfNewCursorEngine(dir, segSize, 2*segSize, 60*time.Millisecond)
	require.NoError(t, err)
	defer func() { _ = e.engineClose() }()

	// Fill the active segment: capacity = 96-24 = 72, each frame 8+16 = 24.
	for i := 0; i < 3; i++ {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err, "iteration %d", i)
	}
	// The next append needs a rotation, and the quarantined bytes leave no
	// budget for a spare.
	_, err = e.engineAppendBlocking(context.Background(), make([]byte, 16))
	require.ErrorIs(t, err, ErrBackpressureTimeout,
		"quarantined bytes at the cap must backpressure the producer")

	// Deleting the evidence is the operator's move, and it is sufficient.
	require.NoError(t, os.Remove(corrupt))
	require.Eventually(t, func() bool {
		_, err := e.engineAppendBlocking(context.Background(), make([]byte, 16))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond,
		"minting must resume once the .corrupt files are gone")
}
