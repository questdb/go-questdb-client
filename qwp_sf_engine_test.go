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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	for _, barrier := range []string{"watermark-sync", "manifest-sync"} {
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
			require.False(t, engine.terminalResourcesClosed.Load())

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
			require.NoError(t, engine.engineClose(), "later Close must retry and converge")
			require.True(t, engine.engineCloseCompleted())
			for _, name := range beforeNames {
				if filepath.Ext(name) == ".sfa" ||
					name == qwpSfManifestFileName ||
					name == qwpSfAckWatermarkFileName ||
					name == qwpSfSymbolDictFileName {
					_, statErr := os.Stat(filepath.Join(dir, name))
					require.True(t, os.IsNotExist(statErr), "later Close retained drained file %s", name)
				}
			}
			lock, err := qwpSfAcquireSlotLock(dir)
			require.NoError(t, err, "later Close must release the slot flock")
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
		_, err := e1.engineAppendBlocking(context.Background(), []byte("a"))
		require.NoError(t, err)
		_, err = e2.engineAppendBlocking(context.Background(), []byte("b"))
		require.NoError(t, err)
	}
	require.NoError(t, e1.engineClose())
	require.NoError(t, e2.engineClose())
}

// TestQwpSfCursorEngineConstructorPanicReleasesSlotLock asserts the
// constructor's teardown is panic-safe: a panic after the slot flock is
// acquired (and the initial segment mmap'd) must still release the flock
// on the unwind. A leaked flock is durable — the kernel holds it for the
// life of the process — so it would wedge every future foreground open
// and orphan drainer for that slot, silently defeating recovery: a
// drainer's recover() drops a .failed sentinel believing the
// engine-close defer already freed the lock, but on a constructor panic
// that defer was never registered.
//
// A nil manager is the injection seam. The constructor touches the
// manager only at registration, the last step before the success
// return; dereferencing a nil receiver there panics with the flock
// already held and the segment already mapped — exactly the gap the
// deferred guard covers. (It also confirms cleanup itself is
// manager-free: a teardown that called a manager method would re-panic
// on this same nil receiver during the unwind.)
func TestQwpSfCursorEngineConstructorPanicReleasesSlotLock(t *testing.T) {
	dir := t.TempDir()

	func() {
		defer func() {
			require.NotNil(t, recover(),
				"nil manager must panic during registration")
		}()
		_, _ = qwpSfNewCursorEngineWithManager(dir, 4096, nil, time.Second)
	}()

	// Re-acquiring the flock proves the deferred teardown released it on
	// the panic unwind; flock is non-blocking, so a leak would surface
	// here as qwpSfErrLockBusy rather than hang.
	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err,
		"slot flock must be released after a constructor panic")
	require.NoError(t, lock.close())
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
	for i := 1; i < qwpSfManagerMaintenanceFailureThreshold; i++ {
		e.manager.recordServiceError(e.managerEntry, diskErr)
		require.NoError(t, e.managerEntry.entryMaintenanceError(),
			"a short run of failures is a log line, not a producer-visible error")
	}
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

// TestQwpSfSwappableVarSurvivesConcurrentSwap pins the property the swappable
// vars exist for. A test replacing one of these while a production goroutine
// reads it is a data race on a plain var, and the goroutines that read them —
// the close-retry owner above all — are ones a test has no way to join before
// t.Cleanup puts the original back. Run under -race this fails the moment the
// value stops going through an atomic.
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

// TestQwpSfEngineCloseRetryWaitsForManagerTeardown pins the ownership rule that
// keeps a repeated Close from releasing the slot while the manager worker still
// writes to it. engineCloseInternal publishes closed in its first line but only
// deregisters the ring and stops the worker after it wins appendMu, so a Close
// arriving inside that window would otherwise find an apparently closed engine
// with no cleanup owner, take the claim, and unlink files and drop the flock
// under a live worker that keeps minting segments into the same directory.
//
// The window is opened here the way a producer mid-rotation opens it: appendMu
// TestQwpSfEngineRetryOwnerDrivesACloseThatNeverStarted pins the recovery from
// a close that faulted ahead of the engine teardown -- a panic in the drain
// wait or the send-loop shutdown, recovered by the pool's closeSlotGuarded. The
// manager teardown never ran, so there is no ownerless terminal cleanup for a
// claim to take over; without re-driving the close the retry owner would spin
// at 1 Hz forever and the slot flock would never be released.
func TestQwpSfEngineRetryOwnerDrivesACloseThatNeverStarted(t *testing.T) {
	dir := t.TempDir()
	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)

	// Nothing has entered engineClose, which is exactly the post-panic state.
	require.False(t, e.managerTornDown.Load())
	require.False(t, e.engineTryClaimTerminalCleanup(),
		"cleanup is not ownerless yet -- the manager teardown has not run")

	require.NoError(t, e.engineRetryCloseIfNeeded())
	require.True(t, e.engineCloseCompleted(),
		"the retry has to drive the whole close, not wait for a claim that can never come")

	lock, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err, "the slot flock must be released")
	require.NoError(t, lock.close())
}

// TestQwpSfManagerCloseWithoutStartIsQuiescent pins that a manager whose worker
// was never launched reports quiescence at once. Waiting on m.done would burn
// the whole close grace and then hand cleanup to a goroutine that will never
// run it, leaving the engine with a deferred owner that never completes.
func TestQwpSfManagerCloseWithoutStartIsQuiescent(t *testing.T) {
	m, err := qwpSfNewSegmentManager(4096, time.Second, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	start := time.Now()
	require.True(t, m.segmentManagerClose())
	require.Less(t, time.Since(start), qwpSfManagerCloseGrace.load(),
		"close must not wait on a worker that was never started")
	require.False(t, m.deferOwnedCleanupUntilWorkerExit(func() {}),
		"there is no worker exit to defer to")
}

// is held while the first Close runs.
func TestQwpSfEngineCloseRetryWaitsForManagerTeardown(t *testing.T) {
	dir := t.TempDir()
	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)

	e.appendMu.Lock()
	done := make(chan error, 1)
	go func() { done <- e.engineClose() }()
	require.Eventually(t, e.closed.Load, time.Second, 100*time.Microsecond,
		"first Close must reach the closed CAS")

	require.False(t, e.managerTornDown.Load())
	require.False(t, e.engineCloseRetryable(),
		"a second Close must not claim cleanup while the manager teardown is still ahead of the first")
	require.False(t, e.terminalCleanupClaimed.Load(),
		"a rejected claim must leave the claim bit free for the first Close")

	e.appendMu.Unlock()
	require.NoError(t, <-done)
	require.True(t, e.engineCloseCompleted())
	require.True(t, e.managerTornDown.Load())

	e.manager.mu.Lock()
	managerClosed := e.manager.closed
	e.manager.mu.Unlock()
	require.True(t, managerClosed, "the manager must be shut down once Close reports completion")
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
	return dir
}

// TestQwpSfDrainedUnlinkLeavesEveryCrashPointRecoverable pins the crash-safety
// argument qwpSfUnlinkAllSegmentFiles' ordering rests on. The sweep runs after
// the manifest has collapsed to head == active, and a crash can stop it between
// any two unlinks, so every prefix of the removal order has to leave a
// directory the next recovery accepts — never one it fails closed on, which
// would quarantine a slot whose rows were all acknowledged.
//
// The order is taken from the production function rather than restated here,
// so a change to the sort is a change to what this test walks.
func TestQwpSfDrainedUnlinkLeavesEveryCrashPointRecoverable(t *testing.T) {
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
			var order []string
			recorder := func(path string) { order = append(order, filepath.Base(path)) }
			qwpSfTestBeforeSegmentUnlinkHook.Store(&recorder)
			t.Cleanup(func() { qwpSfTestBeforeSegmentUnlinkHook.Store(nil) })
			require.NoError(t, qwpSfUnlinkAllSegmentFiles(buildDrainedSlot(t, spareBase)))
			qwpSfTestBeforeSegmentUnlinkHook.Store(nil)
			require.Equal(t, []string{"sf-initial.sfa", "sf-0000000000000001.sfa", "sf-0000000000000002.sfa"}, order,
				"oldest first, and the hot spare outlives the active segment it was minted after")

			for k := 0; k <= len(order); k++ {
				t.Run(fmt.Sprintf("crash-after-%d", k), func(t *testing.T) {
					dir := buildDrainedSlot(t, spareBase)
					for _, removed := range order[:k] {
						require.NoError(t, os.Remove(filepath.Join(dir, removed)))
					}
					ring, _, err := qwpSfRecoverRing(dir, 4096)
					require.NoError(t, err, "a crash mid-sweep must leave a recoverable slot")
					require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed)
					if ring != nil {
						defer ring.segmentRingClose()
						assert.Equal(t, int64(1), ring.getActiveSegment().segmentBaseSeq())
					}
				})
			}
		})
	}
}

// TestQwpSfTerminalCleanupHasExactlyOneOwner pins what engineFinishClose says
// keeps two cleanup owners apart: the terminalCleanupClaimed CAS, not appendMu.
// The distinction is the whole property. Every entry point does hold appendMu,
// so a second owner would be serialized either way — but serialized means it
// runs the cleanup again once the first one lets go, unlinking files and
// releasing a lock that is no longer its own. Refused means it does not run at
// all.
//
// The claim is taken before appendMu, so the test can ask for it from inside a
// cleanup that is holding both and require the answer to come back promptly and
// negative. Queuing instead of refusing is a failure, not a slow pass.
func TestQwpSfTerminalCleanupHasExactlyOneOwner(t *testing.T) {
	dir := t.TempDir()
	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)

	// A failing flock release keeps terminal cleanup incomplete and ownerless
	// after every attempt, which is the state repeated Close calls, the retry
	// owner and pool reprobes all race to take over.
	failRelease := func() error { return syscall.EIO }
	qwpSfTestBeforeFlockReleaseHook.Store(&failRelease)
	t.Cleanup(func() { qwpSfTestBeforeFlockReleaseHook.Store(nil) })

	var (
		entries    atomic.Int64
		inFlight   atomic.Int32
		overlapped atomic.Bool
		refusals   atomic.Int64
		queued     atomic.Bool
	)
	hook := func() {
		entries.Add(1)
		if inFlight.Add(1) > 1 {
			overlapped.Store(true)
		}
		rival := make(chan bool, 1)
		go func() { rival <- e.engineTryClaimTerminalCleanup() }()
		select {
		case claimed := <-rival:
			if claimed {
				overlapped.Store(true)
			} else {
				refusals.Add(1)
			}
		case <-time.After(2 * time.Second):
			queued.Store(true)
		}
		inFlight.Add(-1)
	}
	qwpSfTestEngineFinishCloseHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestEngineFinishCloseHook.Store(nil) })

	require.Error(t, e.engineClose(), "the injected release failure must fail the close")
	require.False(t, e.engineCloseCompleted())

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if i%2 == 0 {
					// The repeated-Close path, as qwpLineSender.Close takes it.
					if e.engineCloseRetryable() {
						_ = e.engineFinishClaimedClose()
					}
					continue
				}
				// The retry owner's and the pool reprobe's path.
				_ = e.engineRetryCloseIfNeeded()
			}
		}(i)
	}
	wg.Wait()

	require.False(t, queued.Load(), "a rival owner must be refused, not queued behind appendMu")
	require.False(t, overlapped.Load(), "terminal cleanup must have exactly one owner at a time")
	require.Positive(t, entries.Load(), "the retries must actually reach terminal cleanup")
	require.Equal(t, entries.Load(), refusals.Load(), "every rival claim must be refused")

	// With the fault cleared, one attempt finishes and the claim stays taken:
	// a completed close is not up for adoption either.
	qwpSfTestBeforeFlockReleaseHook.Store(nil)
	require.NoError(t, e.engineRetryCloseIfNeeded())
	require.True(t, e.engineCloseCompleted())
	require.False(t, e.engineTryClaimTerminalCleanup())
	require.False(t, e.engineCloseRetryable())
}

// TestQwpEveryProductionLogCallIsPanicGuarded keeps the rule mechanically
// checkable. The logger is the application's slog handler, which is free to
// panic: qwpEffectiveLogger(nil) resolves to slog.Default(), so "no logger
// configured" is not "no user code". Every production log call therefore runs
// through qwpSfLogGuarded, because the step behind a log call is regularly the
// one that matters -- latching a fatal error, reporting on a channel, releasing
// a transport, or assigning a fallback policy.
//
// The check parses the package rather than matching text: a regex over lines
// misses a call whose logger argument contains a nested call, one split across
// lines, and one hoisted into a local -- all three of which already occur in
// this codebase, so a one-line refactor would defeat it silently.
//
// The two exceptions are the dispatchers' own handler-panic reports, which
// already carry an inner recover of their own.
func TestQwpEveryProductionLogCallIsPanicGuarded(t *testing.T) {
	// The two exceptions are the dispatchers' own handler-panic reports, which
	// already carry an inner recover. They are named by line rather than by
	// file: a whole-file exemption hid an unguarded default error handler in
	// one of them for a full round.
	allowed := map[string]string{
		"qwp_dispatcher.go":    "handler panicked",
		"qwp_sf_dispatcher.go": "error handler panicked",
	}
	levelMethods := map[string]bool{
		"Warn": true, "Error": true, "Info": true, "Debug": true, "Log": true,
		"WarnContext": true, "ErrorContext": true, "InfoContext": true, "DebugContext": true,
	}

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ParseComments)
	require.NoError(t, err)

	var unguarded []string
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			// Names bound to a *slog.Logger, so a hoisted local is caught
			// too. Both an assignment and a var declaration bind one.
			loggerLocals := map[string]bool{}
			bind := func(lhs, rhs ast.Expr) {
				if !callsAny(rhs, "qwpEffectiveLogger", "slog.Default", "slog.New") {
					return
				}
				if id, ok := lhs.(*ast.Ident); ok {
					loggerLocals[id.Name] = true
				}
			}
			ast.Inspect(file, func(n ast.Node) bool {
				switch d := n.(type) {
				case *ast.AssignStmt:
					if len(d.Lhs) == 1 && len(d.Rhs) == 1 {
						bind(d.Lhs[0], d.Rhs[0])
					}
				case *ast.ValueSpec:
					for i := range d.Values {
						if i < len(d.Names) {
							bind(d.Names[i], d.Values[i])
						}
					}
				}
				return true
			})
			inGuard := false
			ast.Inspect(file, func(n ast.Node) bool {
				if fn, ok := n.(*ast.FuncDecl); ok {
					inGuard = fn.Name.Name == "qwpSfLogGuarded"
					return true
				}
				call, ok := n.(*ast.CallExpr)
				if !ok || inGuard {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				// Every slog level method takes at least a message. Requiring
				// one argument keeps error.Error(), which shares the name and
				// takes none, from being mistaken for a log call.
				if !ok || !levelMethods[sel.Sel.Name] || len(call.Args) == 0 {
					return true
				}
				// A level method on a logger this file just produced, on a
				// name it bound to one, or on anything spelled like a logger.
				// The last case catches the shape the pool, the send loop and
				// the manager all use: a *slog.Logger in a struct field,
				// called as p.logger.Warn(...).
				reaches := callsAny(sel.X, "qwpEffectiveLogger", "slog.Default")
				switch recv := sel.X.(type) {
				case *ast.Ident:
					reaches = reaches || loggerLocals[recv.Name] || isLoggerName(recv.Name)
				case *ast.SelectorExpr:
					reaches = reaches || isLoggerName(recv.Sel.Name)
				case *ast.CallExpr:
					// The shape the manager and the engine use: a logger read
					// out of an atomic or an accessor, m.logger.Load() and
					// e.engineLogger(). A receiver that is itself a call was
					// invisible until a reviewer mutated one.
					reaches = reaches || callExprYieldsLogger(recv)
				}
				if !reaches {
					return true
				}
				// The exempt call is identified by the message it logs, so the
				// rest of its file stays checked.
				if msg, ok := allowed[filepath.Base(name)]; ok && len(call.Args) > 0 &&
					containsStringLiteral(call.Args[0], msg) {
					return true
				}
				unguarded = append(unguarded,
					fmt.Sprintf("%s:%d", filepath.Base(name), fset.Position(call.Pos()).Line))
				return true
			})
		}
	}
	require.NotEmpty(t, pkgs, "the scan must actually parse the package")
	require.Empty(t, unguarded,
		"these log calls reach the user's slog handler unguarded; route them through qwpSfLogGuarded")
}

// containsStringLiteral reports whether expr contains a string literal with the
// given substring, looking through concatenation so a prefixed message counts.
func containsStringLiteral(expr ast.Expr, want string) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && strings.Contains(lit.Value, want) {
			found = true
		}
		return true
	})
	return found
}

// callExprYieldsLogger reports whether a call expression reads out a logger:
// either the function is spelled like one, or the value it is called on is.
func callExprYieldsLogger(call *ast.CallExpr) bool {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return isLoggerName(fn.Name)
	case *ast.SelectorExpr:
		if isLoggerName(fn.Sel.Name) {
			return true
		}
		if inner, ok := fn.X.(*ast.SelectorExpr); ok {
			return isLoggerName(inner.Sel.Name)
		}
		if inner, ok := fn.X.(*ast.Ident); ok {
			return isLoggerName(inner.Name)
		}
	}
	return false
}

// isLoggerName reports whether an identifier is spelled like a *slog.Logger.
// A type-checked answer would be exact; this is the approximation that costs
// no build step, and the package spells every logger it holds this way.
func isLoggerName(name string) bool {
	lower := strings.ToLower(name)
	// Deliberately not "l": that is the receiver name for *qwpSfSendLoop and
	// several other types in this package, so matching it produced false
	// positives on their own methods.
	return lower == "lg" || strings.Contains(lower, "logger")
}

// callsAny reports whether expr is a call to any of the named functions,
// looking through the receiver chain so a nested call is still seen.
func callsAny(expr ast.Expr, names ...string) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var got string
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			got = fn.Name
		case *ast.SelectorExpr:
			if pkg, ok := fn.X.(*ast.Ident); ok {
				got = pkg.Name + "." + fn.Sel.Name
			}
		}
		for _, want := range names {
			if got == want {
				found = true
			}
		}
		return true
	})
	return found
}

// TestQwpSfCloseFaultBetweenOwnershipAndHandoffLeavesNoOwnerlessEngine pins the
// window between publishing deferred cleanup ownership and actually taking the
// handoff. A fault there used to leave deferredCleanupOwned set with nothing
// behind it, which refuses every later claim: engineRetryCloseIfNeeded then
// returns nil rather than an error, so the retry owner spins for the process
// lifetime holding the flock and never logs a reason.
func TestQwpSfCloseFaultBetweenOwnershipAndHandoffLeavesNoOwnerlessEngine(t *testing.T) {
	dir := t.TempDir()

	// Hold the manager worker inside spare creation so the close grace expires
	// and the teardown is not quiescent -- the only state that publishes
	// deferred cleanup ownership.
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	_, err = e.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("manager did not enter spare creation")
	}

	// Fault exactly in the window: after the manager teardown marker, before
	// the handoff decision.
	boom := func() { panic("teardown boom") }
	qwpSfTestAfterManagerTeardownHook.Store(&boom)
	t.Cleanup(func() { qwpSfTestAfterManagerTeardownHook.Store(nil) })

	func() {
		defer func() { require.NotNil(t, recover(), "the injected fault must unwind") }()
		_ = e.engineClose()
	}()
	qwpSfTestAfterManagerTeardownHook.Store(nil)

	require.False(t, e.deferredCleanupOwned.Load(),
		"a fault before the handoff must not leave the engine owned by nobody")
	require.False(t, e.managerTornDown.Load(),
		"nor claimable, since the worker is provably still in the slot")

	close(release)
	released = true

	// Cleanup is claimable again, so a retry finishes the close and frees the lock.
	require.Eventually(t, func() bool {
		return e.engineRetryCloseIfNeeded() == nil && e.engineCloseCompleted()
	}, 3*time.Second, time.Millisecond)
	require.True(t, e.engineCloseCompleted())
	lock, lockErr := qwpSfAcquireSlotLock(dir)
	require.NoError(t, lockErr, "the slot flock must be released")
	require.NoError(t, lock.close())
}
