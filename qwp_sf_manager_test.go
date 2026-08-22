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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfManagerServiceErrorSurvivesPanickingLogger(t *testing.T) {
	mgr, err := qwpSfNewSegmentManager(4096, time.Millisecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.logger.Store(slog.New(panicOnHandleSlog{}))

	require.NotPanics(t, func() {
		mgr.recordServiceError(&qwpSfManagerRingEntry{dir: "slot"}, errors.New("injected maintenance failure"))
	})
}

func TestQwpSfManagerWorkerPanicStillSignalsExitWithPanickingLogger(t *testing.T) {
	dir := t.TempDir()
	first, err := qwpSfCreateSegment(filepath.Join(dir, "sf-initial.sfa"), 0, 4096)
	require.NoError(t, err)
	ring := qwpSfNewSegmentRing(first, 4096)
	defer func() { _ = ring.segmentRingClose() }()

	mgr, err := qwpSfNewSegmentManager(4096, time.Millisecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.logger.Store(slog.New(panicOnHandleSlog{}))
	require.NoError(t, mgr.segmentManagerRegister(ring, dir))

	createHook := func(string) { panic("injected spare-create panic") }
	qwpSfTestSegmentCreateHook.Store(&createHook)
	t.Cleanup(func() { qwpSfTestSegmentCreateHook.Store(nil) })
	mgr.segmentManagerStart()

	select {
	case <-mgr.done:
	case <-time.After(time.Second):
		t.Fatal("manager worker did not signal exit after panic")
	}
	require.NotNil(t, mgr.workerPanic.Load())
	require.True(t, mgr.segmentManagerClose())
}

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

func TestQwpSfManagerDeferredOwnedCleanupRefusesAfterWorkerExit(t *testing.T) {
	mgr, err := qwpSfNewSegmentManager(4096, time.Millisecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	require.True(t, mgr.segmentManagerClose())
	called := false
	require.False(t, mgr.deferOwnedCleanupUntilWorkerExit(func() { called = true }))
	require.False(t, called, "caller retains inline cleanup ownership")
}

func TestQwpSfManagerDeregisterDuringTrimKeepsSharedByteAccounting(t *testing.T) {
	const segSize int64 = 72
	mgr, err := qwpSfNewSegmentManager(segSize, 100*time.Microsecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	defer mgr.segmentManagerClose()

	e1, err := qwpSfNewCursorEngineWithManager("", segSize, mgr, time.Second)
	require.NoError(t, err)
	e2, err := qwpSfNewCursorEngineWithManager("", segSize, mgr, time.Second)
	require.NoError(t, err)
	defer func() { _ = e2.engineClose() }()
	require.Eventually(t, func() bool {
		return !e1.ring.needsHotSpare() && !e2.ring.needsHotSpare()
	}, time.Second, time.Millisecond)

	for i := 0; i < 3; i++ {
		_, err := e1.engineAppendBlocking(context.Background(), make([]byte, 16))
		require.NoError(t, err)
	}
	sealed := e1.ring.firstSealed()
	require.NotNil(t, sealed)

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	trimHook := func(entry *qwpSfManagerRingEntry) {
		if entry.ring != e1.ring {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	qwpSfTestBeforeTrimAccountingHook.Store(&trimHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestBeforeTrimAccountingHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	e1.engineAcknowledge(sealed.segmentBaseSeq() + sealed.segmentFrameCount() - 1)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("manager did not reach trim accounting hook")
	}
	require.NoError(t, e1.engineClose())
	require.False(t, e1.engineCloseCompleted())
	close(release)
	released = true
	require.Eventually(t, e1.engineCloseCompleted, time.Second, time.Millisecond)

	mgr.mu.Lock()
	got := mgr.totalBytes
	mgr.mu.Unlock()
	require.Equal(t, e2.ring.totalSegmentBytes(), got,
		"deregistered ring must leave exactly the sibling contribution")
}

func TestQwpSfManagerScanMaxGenerationOnEmptyDir(t *testing.T) {
	dir := t.TempDir()
	_, found := qwpSfScanMaxGeneration(dir)
	// No segments → not found; caller leaves fileGeneration unconstrained.
	assert.False(t, found)
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
	v, found := qwpSfScanMaxGeneration(dir)
	require.True(t, found)
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

// The manager worker's recover is final: it records the panic and the worker
// never comes back, so provisioning and trimming stop for every slot that
// manager serves and producers are told "segment manager worker stopped". A
// user-supplied slog handler must not be able to cause that, so the cap-reached
// warnings go through the same guard as every other log on this goroutine.
func TestQwpSfManagerCapWarningSurvivesPanickingLogger(t *testing.T) {
	for _, tc := range []struct {
		name string
		dir  string
	}{
		{name: "memory"},
		{name: "disk", dir: t.TempDir()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := qwpSfNewSegmentManager(4096, time.Hour, 4096)
			require.NoError(t, err)
			m.logger.Store(slog.New(panicOnHandleSlog{}))

			seg, err := qwpSfCreateInMemorySegment(0, 4096)
			require.NoError(t, err)
			r := qwpSfNewSegmentRing(seg, 4096)
			defer func() { _ = r.segmentRingClose() }()
			require.True(t, r.needsHotSpare())

			// Already at the cap, so provisioning is skipped and the warning
			// is written.
			m.totalBytes = 4096
			e := &qwpSfManagerRingEntry{dir: tc.dir, ring: r}
			require.NotPanics(t, func() { m.serviceRing(e) })
		})
	}
}

// A trim pass that gives up partway through still has to credit the bytes an
// earlier pass's leftover unlinks just reclaimed: those files are gone and off
// the retry list, so nothing else will ever account for them. Losing them
// charges the slot forever, which stops spare provisioning and, at
// deregistration, pushes the manager total negative and loosens the cap for
// every other slot that manager serves.
func TestQwpSfManagerCreditsReclaimedBytesWhenTrimAborts(t *testing.T) {
	dir := t.TempDir()
	m, err := qwpSfNewSegmentManager(4096, time.Hour, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)

	const segSize int64 = 72
	first, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	r := qwpSfNewSegmentRing(first, segSize)
	defer func() { _ = r.segmentRingClose() }()
	spare, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	require.NoError(t, r.installHotSpare(spare))
	payload := make([]byte, 16)
	for i := 0; i < 3; i++ {
		require.GreaterOrEqual(t, r.appendOrFsn(payload), int64(0))
	}
	sealed := r.getSealedSegments()
	require.Len(t, sealed, 1)
	r.acknowledge(sealed[0].segmentBaseSeq() + sealed[0].segmentFrameCount() - 1)

	// A disk-mode entry whose ring carries no manifest aborts the pass at the
	// "disk ring has no SF manifest during trim" check, after the leftover
	// unlink has already run.
	e := &qwpSfManagerRingEntry{dir: dir, ring: r, accountedBytes: 4096}
	m.totalBytes = 4096
	e.pendingUnlinks = append(e.pendingUnlinks, qwpSfPendingUnlink{
		path: filepath.Join(dir, "sf-already-gone.sfa"), sizeBytes: 4096,
	})

	m.serviceRing(e)

	require.Equal(t, 1, e.maintenanceFailures, "the pass aborted before trimming")
	assert.Empty(t, e.pendingUnlinks, "the reclaimed unlink is off the retry list")
	assert.Zero(t, e.accountedBytes, "the slot gets the reclaimed bytes back")
	assert.Zero(t, m.totalBytes, "and so does the manager total")
}

// TestQwpSfManagerHoldsBytesForAFailedTrimUnlink pins the accounting rule that
// keeps a slot inside its cap when deletes stop working. A trimmed segment
// whose file is still on disk gives no capacity back, so crediting its bytes
// would let the manager keep provisioning on top of files it never removed and
// grow the slot past sf_max_total_bytes while the disk fills.
//
// It also pins what the producer is told. Passes between failures find nothing
// new to trim, and a plain success there would clear the failure run every
// other tick, so the run would never reach the threshold and all a producer
// would ever see is a backpressure timeout blaming the server.
//
// A non-empty directory at the segment's path is what makes the failure
// deterministic: os.Remove refuses it on every platform and for every user,
// including root.
func TestQwpSfManagerHoldsBytesForAFailedTrimUnlink(t *testing.T) {
	dir := t.TempDir()
	stuck := filepath.Join(dir, "sf-stuck.sfa")
	require.NoError(t, os.MkdirAll(filepath.Join(stuck, "occupant"), 0o755))

	m, err := qwpSfNewSegmentManager(4096, time.Hour, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	m.segmentManagerStart()
	defer m.segmentManagerClose()

	e := &qwpSfManagerRingEntry{dir: dir}
	e.pendingUnlinks = append(e.pendingUnlinks, qwpSfPendingUnlink{path: stuck, sizeBytes: 4096})

	freed, retryErr := m.retryDeferredTrimWork(e)
	require.Error(t, retryErr)
	assert.Zero(t, freed, "bytes stay charged while the file is still on disk")
	assert.Len(t, e.pendingUnlinks, 1, "a failed unlink is retried, not forgotten")

	e.maintenanceFailures = qwpSfManagerMaintenanceFailureThreshold - 1
	e.entryMaintenanceSucceeded()
	assert.Equal(t, qwpSfManagerMaintenanceFailureThreshold-1, e.maintenanceFailures,
		"an empty trim list does not end a failure run that still owes an unlink")
	m.recordServiceError(e, retryErr)
	require.ErrorIs(t, e.entryMaintenanceError(), ErrSfDurability)

	require.NoError(t, os.RemoveAll(filepath.Join(stuck, "occupant")))
	freed, retryErr = m.retryDeferredTrimWork(e)
	require.NoError(t, retryErr)
	assert.Equal(t, int64(4096), freed, "the bytes come back once the file is gone")
	assert.Empty(t, e.pendingUnlinks)
	e.entryMaintenanceSucceeded()
	assert.NoError(t, e.entryMaintenanceError())
}

// TestQwpSfSpareCreationSyncsSlotDirectory pins the durability barrier that
// makes a minted spare findable after a crash. A rotation commits the spare's
// base into the manifest as the active one; if the spare's directory entry is
// not durable by then, a crash leaves the manifest naming a base with no
// segment at it. Recovery refuses such a slot, so a single lost name costs
// every undelivered row in it.
func TestQwpSfSpareCreationSyncsSlotDirectory(t *testing.T) {
	dir := t.TempDir()
	var mu sync.Mutex
	synced := map[string]int{}
	hook := func(d string) error {
		mu.Lock()
		synced[d]++
		mu.Unlock()
		return nil
	}
	qwpSfTestDirSyncHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestDirSyncHook.Store(nil) })

	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.engineClose() })

	// The fresh slot's own creation barrier.
	mu.Lock()
	afterCreate := synced[dir]
	mu.Unlock()
	require.GreaterOrEqual(t, afterCreate, 1, "creating a slot must sync its directory")

	// Wait for the manager to mint a spare and confirm it synced the directory
	// before that spare can be promoted.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return synced[dir] > afterCreate
	}, 3*time.Second, time.Millisecond,
		"minting a spare must make its name durable before a rotation can commit it")

	require.False(t, e.ring.needsHotSpare(), "the manager should have provisioned a spare")
}

// TestQwpSfDeferredOwnedCleanupRefusesASecondEngine pins that the manager's
// single cleanup slot cannot be claimed twice. Reporting a handoff that was not
// taken would leave the second engine's cleanup with no owner at all: it clears
// its own ownership marker on the strength of the answer, and the worker exit
// only ever runs the one cleanup it holds.
func TestQwpSfDeferredOwnedCleanupRefusesASecondEngine(t *testing.T) {
	mgr, err := qwpSfNewSegmentManager(4096, time.Millisecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	mgr.segmentManagerStart()
	t.Cleanup(func() { mgr.segmentManagerClose() })

	firstRan := make(chan struct{})
	require.True(t, mgr.deferOwnedCleanupUntilWorkerExit(func() { close(firstRan) }),
		"the first engine's cleanup must be accepted")

	secondRan := false
	require.False(t, mgr.deferOwnedCleanupUntilWorkerExit(func() { secondRan = true }),
		"a second engine must be told to clean up inline, not that it was handed off")

	require.True(t, mgr.segmentManagerClose())
	select {
	case <-firstRan:
	case <-time.After(3 * time.Second):
		t.Fatal("the accepted cleanup never ran")
	}
	require.False(t, secondRan, "the refused cleanup must not run on the worker exit")
}

// TestQwpSfManagerCloseWithoutStartDoesNotWait pins the never-started fast path
// against the close grace, so a regression shows up as a failure rather than as
// a slow test.
func TestQwpSfManagerCloseWithoutStartDoesNotWait(t *testing.T) {
	mgr, err := qwpSfNewSegmentManager(4096, time.Millisecond, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	old := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(2 * time.Second)
	t.Cleanup(func() { qwpSfManagerCloseGrace.store(old) })

	start := time.Now()
	require.True(t, mgr.segmentManagerClose())
	require.Less(t, time.Since(start), time.Second,
		"a manager with no worker must not wait on a channel nothing will close")
}

// TestQwpSfSpareProvisioningFailureSurfacesAsDurability pins that a slot
// directory barrier that will not succeed reaches the producer as what it is.
// The spare is unlinked and retried every tick, so rotation never resumes and
// the producer blocks — reporting that as ErrBackpressureTimeout would tell an
// operator the buffer is full when the real cause is local storage. A later
// clean trim in the same pass must not clear it either.
func TestQwpSfSpareProvisioningFailureSurfacesAsDurability(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("injected directory barrier failure")
	var fail atomic.Bool
	hook := func(string) error {
		if fail.Load() {
			return injected
		}
		return nil
	}
	qwpSfTestDirSyncHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestDirSyncHook.Store(nil) })

	e, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	// The close has to run with the injected failure switched off. Otherwise
	// every directory barrier still fails, engineFinishDrainedFileCleanup
	// fails with it, the terminal-cleanup claim is released, no retry owner is
	// installed, and the ring mappings and the slot flock leak for the rest of
	// the binary while t.TempDir deletes the directory underneath them.
	// (t.Cleanup is LIFO, so this runs before the hook is cleared either way.)
	t.Cleanup(func() {
		fail.Store(false)
		_ = e.engineClose()
	})

	// Fail every barrier from here on. The manager keeps trying to mint a
	// spare and keeps failing, and a run of failures latches the durability
	// error producers read -- which is the whole point: the alternative is a
	// silent stall reported to the caller as a full buffer.
	fail.Store(true)
	require.Eventually(t, func() bool {
		return errors.Is(e.managerEntry.entryMaintenanceError(), ErrSfDurability)
	}, 10*time.Second, time.Millisecond,
		"a failing slot directory barrier must surface as ErrSfDurability, not be discarded")
	require.ErrorIs(t, e.managerEntry.entryMaintenanceError(), injected,
		"and must name the barrier failure that caused it")
}
