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
