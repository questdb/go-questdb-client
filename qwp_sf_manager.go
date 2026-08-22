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
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// qwpSfManager defaults and constants.
const (
	qwpSfManagerDefaultPoll         = 1 * time.Millisecond // poll cadence
	qwpSfManagerDiskFullLogThrottle = 30 * time.Second     // throttle disk-full WARNs

	// qwpSfManagerMaintenanceFailureThreshold is how many consecutive failed
	// maintenance passes a ring tolerates before its entry publishes the
	// failure to producers. Passes run on the manager poll tick, so a run this
	// long is a storage fault rather than one unlucky syscall.
	qwpSfManagerMaintenanceFailureThreshold = 3
)

// qwpSfManagerCloseGrace bounds how long close() waits for the worker
// goroutine or an individual ring service pass to quiesce. It is a variable so
// the timeout paths can be tested without sleeping for five seconds.
var qwpSfManagerCloseGrace = qwpSfSwappable(5 * time.Second)

// qwpSfTestBeforeTrimAccountingHook is a test seam for the narrow race between
// draining a trim batch and reconciling manager byte accounting. Production
// leaves it nil.
var qwpSfTestBeforeTrimAccountingHook atomic.Pointer[func(*qwpSfManagerRingEntry)]

// qwpSfUnlimitedTotalBytes disables the per-engine total-bytes cap.
const qwpSfUnlimitedTotalBytes int64 = math.MaxInt64

// qwpSfSegmentManager is the background worker that keeps every
// registered qwpSfSegmentRing supplied with a hot-spare segment and
// trims segments after their frames have been ACK'd. Off the
// user-thread / I/O-thread hot path entirely: the expensive
// open+truncate+mmap for spare creation and munmap+unlink for trim
// happen on this goroutine, never on the latency-sensitive paths.
//
// One instance can serve many rings (typically all sender instances
// in a process). Polls each ring on a configurable tick (default
// 1 ms) — short enough that a producer rarely sees
// qwpSfBackpressureNoSpare in the steady state, long enough that an
// idle process doesn't burn CPU.
type qwpSfSegmentManager struct {
	segmentSizeBytes int64
	pollInterval     time.Duration
	maxTotalBytes    int64

	// fileGeneration is a monotonic counter that names spare files
	// (sf-<gen:016x>.sfa). Per-process, not per-ring; recovery skips
	// the counter past existing on-disk segments at register time.
	fileGeneration atomic.Uint64

	// logger sinks the cap-reached backpressure diagnostic. The manager's
	// worker goroutine reads it, so it is an atomic.Pointer the engine's
	// constructor callers set race-free after the worker has started. nil
	// (the zero value) -> slog.Default() via qwpEffectiveLogger.
	logger atomic.Pointer[slog.Logger]

	// workerPanic holds the detail of a worker-goroutine panic. Once set, spare
	// provisioning and trim have stopped, so producers would otherwise stall in
	// backpressure forever; engineTerminalError surfaces it as a terminal on the
	// next append instead.
	workerPanic atomic.Pointer[string]

	mu                 sync.Mutex
	rings              []*qwpSfManagerRingEntry
	totalBytes         int64
	lastDiskFullLog    time.Time
	lastMaintenanceLog time.Time
	closed             bool

	// wakeup is a single-slot channel. wakeWorker pushes into it
	// non-blockingly; the worker drains in select to coalesce signals.
	wakeup chan struct{}
	// done is closed when the worker goroutine exits.
	done chan struct{}

	// workerGoid is used only to avoid self-waiting in the test-only shared
	// manager close path.
	workerGoid atomic.Int64

	// Protected by mu. started means segmentManagerStart launched the worker,
	// so m.done has a goroutine that will eventually close it. Without the
	// worker, close must not wait on m.done and no cleanup may be handed to it.
	started bool

	// Protected by mu. workerLoopExited means the worker is past the ring loop
	// and can no longer touch any registered ring. An owned engine may hand its
	// terminal cleanup to the worker's finite exit block after a close timeout.
	workerLoopExited       bool
	workerReaped           bool
	ownedEngineExitCleanup func()

	// ringSnapshot is workerLoop's reusable copy of rings. Each tick
	// refills it from rings under mu, then releases mu before the
	// per-ring service pass so the slow segment syscalls run without
	// the lock held. Owned solely by workerLoop; the locked refill is
	// its only synchronization.
	ringSnapshot []*qwpSfManagerRingEntry
}

const (
	qwpSfManagerRingRegistered int32 = iota
	qwpSfManagerRingInService
	qwpSfManagerRingDeregisteredInService
	qwpSfManagerRingDeregistered
)

// qwpSfManagerRingEntry holds a registered ring and the directory
// its segments live in (nil for memory-mode rings).
type qwpSfManagerRingEntry struct {
	ring *qwpSfSegmentRing
	dir  string
	// accountedBytes is this entry's contribution to manager.totalBytes.
	// Protected by manager.mu; unlike ring.totalSegmentBytes it deliberately
	// retains a drained batch until the service pass commits its accounting.
	accountedBytes int64
	// watermark is the engine-owned .ack-watermark for this slot, or
	// nil in memory mode / when the file could not be opened. The
	// manager writes through it on every tick where ackedFsn
	// advanced; it never closes it (the owning engine does, in
	// engineClose, after this entry is quiescent). Entries are shared
	// pointers so close and the worker observe one state machine.
	watermark *qwpSfAckWatermark
	state     atomic.Int32
	cleanup   atomic.Pointer[func()]
	// maintenanceFailures counts the current run of consecutive service passes
	// that could not complete their durability work. Touched only on the
	// manager's worker goroutine.
	maintenanceFailures int
	// maintenanceError publishes that run once it reaches
	// qwpSfManagerMaintenanceFailureThreshold. Producers read it through
	// engineTerminalError, so a slot whose trim has stopped reports the disk
	// fault that stopped it instead of the generic backpressure timeout that
	// the stalled ring would otherwise produce. Cleared by the next pass that
	// completes its maintenance.
	maintenanceError atomic.Pointer[error]
	// pendingUnlinks holds trimmed segments whose file is still on disk because
	// the unlink failed. Their bytes stay charged to the slot until the file is
	// really gone, and every later pass retries them: crediting a failed delete
	// would keep handing the ring capacity for space it never got back, letting
	// the slot grow past its cap while the disk fills. Worker goroutine only.
	pendingUnlinks []qwpSfPendingUnlink
	// dirSyncPending records that a post-trim directory fsync failed, so the
	// unlinks that pass did complete are not durable yet. Retried by later
	// passes for the same reason as pendingUnlinks. Worker goroutine only.
	dirSyncPending bool
}

// qwpSfPendingUnlink is one trimmed segment file that could not be removed,
// with the bytes still charged to the manager on its behalf.
type qwpSfPendingUnlink struct {
	path      string
	sizeBytes int64
}

// entryMaintenanceError returns the published local-storage maintenance
// failure for this ring, or nil while maintenance is healthy or the failures
// are still short of a run.
func (e *qwpSfManagerRingEntry) entryMaintenanceError() error {
	if e == nil {
		return nil
	}
	if p := e.maintenanceError.Load(); p != nil {
		return *p
	}
	return nil
}

// entryMaintenanceSucceeded ends the current failure run. Worker goroutine only.
// Deferred work left over from an earlier pass keeps the run alive: a slot whose
// deletes keep failing has nothing new to trim on the passes in between, and
// clearing the run there would let failures and successes alternate forever
// without ever reaching the threshold that tells the producer its disk is
// refusing writes.
func (e *qwpSfManagerRingEntry) entryMaintenanceSucceeded() {
	if e.maintenanceFailures == 0 || len(e.pendingUnlinks) > 0 || e.dirSyncPending {
		return
	}
	e.maintenanceFailures = 0
	e.maintenanceError.Store(nil)
}

func (e *qwpSfManagerRingEntry) isInService() bool {
	if e == nil {
		return false
	}
	s := e.state.Load()
	return s == qwpSfManagerRingInService || s == qwpSfManagerRingDeregisteredInService
}

func (e *qwpSfManagerRingEntry) deregister() {
	if e == nil {
		return
	}
	for {
		s := e.state.Load()
		switch s {
		case qwpSfManagerRingRegistered:
			if e.state.CompareAndSwap(s, qwpSfManagerRingDeregistered) {
				return
			}
		case qwpSfManagerRingInService:
			if e.state.CompareAndSwap(s, qwpSfManagerRingDeregisteredInService) {
				return
			}
		default:
			return
		}
	}
}

func (e *qwpSfManagerRingEntry) finishService() {
	if e.state.CompareAndSwap(qwpSfManagerRingDeregisteredInService, qwpSfManagerRingDeregistered) {
		return
	}
	e.state.CompareAndSwap(qwpSfManagerRingInService, qwpSfManagerRingRegistered)
}

// qwpSfNewSegmentManager constructs a manager with the given
// segment size, poll interval, and total-bytes cap. maxTotalBytes
// must be at least one segment.
func qwpSfNewSegmentManager(segmentSizeBytes int64, pollInterval time.Duration, maxTotalBytes int64) (*qwpSfSegmentManager, error) {
	if segmentSizeBytes < qwpSfHeaderSize+qwpSfFrameHeaderSize+1 {
		return nil, fmt.Errorf("qwp/sf: segmentSizeBytes too small: %d", segmentSizeBytes)
	}
	if maxTotalBytes < segmentSizeBytes {
		return nil, fmt.Errorf("qwp/sf: maxTotalBytes (%d) must allow at least one segment of %d bytes",
			maxTotalBytes, segmentSizeBytes)
	}
	if pollInterval <= 0 {
		pollInterval = qwpSfManagerDefaultPoll
	}
	return &qwpSfSegmentManager{
		segmentSizeBytes: segmentSizeBytes,
		pollInterval:     pollInterval,
		maxTotalBytes:    maxTotalBytes,
		wakeup:           make(chan struct{}, 1),
		done:             make(chan struct{}),
	}, nil
}

// segmentManagerStart spawns the worker goroutine. Idempotent — a
// second call is a panic, mirroring Java's IllegalStateException.
func (m *qwpSfSegmentManager) segmentManagerStart() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		panic("qwp/sf: segment manager already closed")
	}
	m.started = true
	m.mu.Unlock()
	go m.workerLoop()
}

// segmentManagerClose stops the worker goroutine and waits up to
// qwpSfManagerCloseGrace for it to exit. After close, the manager
// rejects new registrations and the worker no longer provisions or
// trims segments — but already-installed spares stay with their
// rings (the rings close them on their own segmentRingClose).
//
// Returns true only when no worker can still touch the slot: either one was
// never started, or the one that was is provably past its ring loop.
// Idempotent; safe to call from any goroutine other than the worker itself.
func (m *qwpSfSegmentManager) segmentManagerClose() bool {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
	}
	if m.workerReaped {
		m.mu.Unlock()
		return true
	}
	if !m.started {
		// No worker was ever launched, so m.done stays open forever. Waiting on
		// it would burn the whole grace and then report a non-quiescent manager,
		// handing cleanup to a goroutine that will never run it. Nothing can be
		// mid-service either: quiescence is immediate and provable.
		m.workerReaped = true
		m.workerLoopExited = true
		m.mu.Unlock()
		return true
	}
	m.mu.Unlock()
	// Wake the worker so it observes closed and exits promptly.
	select {
	case m.wakeup <- struct{}{}:
	default:
	}
	// Bound the wait so a stuck worker can't deadlock close().
	graceTimer := time.NewTimer(qwpSfManagerCloseGrace.load())
	select {
	case <-m.done:
		graceTimer.Stop()
		m.mu.Lock()
		m.workerReaped = true
		m.mu.Unlock()
		return true
	case <-graceTimer.C:
	}
	m.mu.Lock()
	exited := m.workerLoopExited
	m.mu.Unlock()
	if !exited {
		return false
	}
	// The loop is past every ring. A second bounded wait only avoids
	// reporting quiescence while a previously handed-off cleanup is running.
	graceTimer.Reset(qwpSfManagerCloseGrace.load())
	select {
	case <-m.done:
		if !graceTimer.Stop() {
			select {
			case <-graceTimer.C:
			default:
			}
		}
	case <-graceTimer.C:
	}
	m.mu.Lock()
	m.workerReaped = true
	m.mu.Unlock()
	return true
}

// segmentManagerDeregister stops tracking the given ring. Pending
// spares for the ring are NOT created after this returns, but
// already-installed spares stay with the ring. Idempotent; safe to
// call from any goroutine.
func (m *qwpSfSegmentManager) segmentManagerDeregister(ring *qwpSfSegmentRing) *qwpSfManagerRingEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.rings {
		if e.ring == ring {
			e.deregister()
			// Reverse the ring's contribution to totalBytes.
			m.totalBytes -= e.accountedBytes
			e.accountedBytes = 0
			// O(N) remove preserving order — register order matters
			// for log ordering, not correctness.
			m.rings = append(m.rings[:i], m.rings[i+1:]...)
			return e
		}
	}
	return nil
}

// segmentManagerRegister registers a ring with no ack-watermark
// (memory mode, or callers that don't persist a watermark — chiefly
// tests). Recovery for such a slot seeds from the segment-derived
// lowestBase-1 only.
func (m *qwpSfSegmentManager) segmentManagerRegister(ring *qwpSfSegmentRing, dir string) error {
	_, err := m.segmentManagerRegisterWithWatermark(ring, dir, nil)
	return err
}

// segmentManagerRegisterWithWatermark registers a ring for ongoing
// spare creation + trim. dir is the filesystem directory the ring's
// segments live in — used both for creating spare files and
// unlinking trimmed ones. watermark (may be nil) is the slot's
// engine-owned .ack-watermark the manager keeps current on every
// tick; the manager never closes it. The ring MUST already have its
// initial active segment in place. Wires the ring's "I need a spare"
// callback so the producer can preempt the polling tick.
func (m *qwpSfSegmentManager) segmentManagerRegisterWithWatermark(ring *qwpSfSegmentRing, dir string, watermark *qwpSfAckWatermark) (*qwpSfManagerRingEntry, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("qwp/sf: segment manager closed")
	}
	entry := &qwpSfManagerRingEntry{ring: ring, dir: dir, watermark: watermark, accountedBytes: ring.totalSegmentBytes()}
	m.rings = append(m.rings, entry)
	// Account for bytes the ring already owns when it joins. A
	// recovered ring (post-restart, orphan adoption) can come up
	// at-or-above the cap; without this seed, totalBytes stays at 0
	// and the per-tick cap check would let the manager keep
	// provisioning new spares on top of the recovered set.
	m.totalBytes += entry.accountedBytes
	m.mu.Unlock()
	if dir != "" {
		// Skip the file-generation counter past whatever's already on
		// disk in this slot. Without this, on recovery the manager
		// would mint a new spare at sf-0000000000000000.sfa — and
		// open-clean-RW would truncate the user's existing active
		// file out from under the I/O loop, scrambling the in-flight
		// mmap.
		if maxGen, found := qwpSfScanMaxGeneration(dir); found {
			minNext := maxGen + 1
			for {
				cur := m.fileGeneration.Load()
				if cur >= minNext {
					break
				}
				if m.fileGeneration.CompareAndSwap(cur, minNext) {
					break
				}
			}
		}
	}
	ring.setManagerWakeup(m.wakeWorker)
	return entry, nil
}

// wakeWorker pushes a non-blocking wakeup so the worker processes
// registered rings on the very next loop iteration. Cheap; safe to
// call from any goroutine; idempotent (multiple wakeups coalesce
// into a single channel slot). No-op when the worker is busy.
func (m *qwpSfSegmentManager) wakeWorker() {
	select {
	case m.wakeup <- struct{}{}:
	default:
	}
}

// qwpSfScanMaxGeneration returns the highest hex-encoded generation
// across sf-<gen>.sfa files in dir. found is false when dir is
// absent/unreadable or holds no matching files; maxGen is then
// unspecified and the caller must not constrain fileGeneration. Skips
// files that don't match the pattern (e.g. the legacy sf-initial.sfa).
func qwpSfScanMaxGeneration(dir string) (maxGen uint64, found bool) {
	if _, err := os.Stat(dir); err != nil {
		return 0, false
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "sf-") || !strings.HasSuffix(name, ".sfa") {
			continue
		}
		hex := name[3 : len(name)-4]
		if len(hex) != 16 {
			continue
		}
		gen, err := strconv.ParseUint(hex, 16, 64)
		if err != nil {
			continue
		}
		if !found || gen > maxGen {
			maxGen = gen
			found = true
		}
	}
	return maxGen, found
}

// nextSparePath returns the next available <dir>/sf-<gen:016x>.sfa
// path. Spare files use a process-wide monotonic counter rather than
// a baseSeq-derived name, because the spare's baseSeq is provisional
// at create time. Recovery discovers segments by extension + header
// magic, not by filename.
func (m *qwpSfSegmentManager) nextSparePath(dir string) string {
	gen := m.fileGeneration.Add(1) - 1
	return filepath.Join(dir, fmt.Sprintf("sf-%016x.sfa", gen))
}

// workerLoop runs until the manager is closed. Each iteration walks
// the registered rings, provisions a spare for any that need one
// (subject to the totalBytes cap), and trims fully-acked sealed
// segments. Sleeps pollInterval between iterations; pre-empted by a
// wakeWorker signal from the producer.
func (m *qwpSfSegmentManager) workerLoop() {
	m.workerGoid.Store(qwpGoid())
	timer := time.NewTimer(m.pollInterval)
	defer timer.Stop()
	// This goroutine drives no untrusted input, so a panic here is not a
	// known reachable path; recover anyway — an uncaught panic on it would
	// crash the host. Provisioning and trim are now dead, so latch the failure:
	// engineTerminalError surfaces it to producers on their next append rather
	// than letting them stall in backpressure forever with no signal. The
	// deferred close(m.done)/worker.Done still run and signal shutdown.
	defer func() {
		if r := recover(); r != nil {
			detail := fmt.Sprintf("%v\n%s", r, debug.Stack())
			m.workerPanic.Store(&detail)
			qwpSfLogGuarded(m.logger.Load(), slog.LevelError, "qwp/sf: segment manager worker panicked",
				"detail", detail)
		}
		m.workerGoid.Store(0)
		m.mu.Lock()
		m.workerLoopExited = true
		cleanup := m.ownedEngineExitCleanup
		m.ownedEngineExitCleanup = nil
		m.mu.Unlock()
		m.runDeferredCleanup(cleanup, "deferred owned-engine cleanup failed on manager-worker exit")
		close(m.done)
	}()
	for {
		// Refill the reusable ring snapshot so we don't hold the mutex
		// through the (potentially slow) syscalls during creation /
		// unlink.
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		m.ringSnapshot = append(m.ringSnapshot[:0], m.rings...)
		m.mu.Unlock()
		for _, e := range m.ringSnapshot {
			if !e.state.CompareAndSwap(qwpSfManagerRingRegistered, qwpSfManagerRingInService) {
				continue
			}
			func() {
				defer func() {
					e.finishService()
					cleanup := e.cleanup.Swap(nil)
					if cleanup != nil {
						m.runDeferredCleanup(*cleanup, "deferred ring cleanup failed after manager service")
					}
				}()
				m.serviceRing(e)
			}()
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(m.pollInterval)
		select {
		case <-m.wakeup:
		case <-timer.C:
		}
	}
}

func (m *qwpSfSegmentManager) runDeferredCleanup(cleanup func(), message string) {
	if cleanup == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			qwpSfLogGuarded(m.logger.Load(), slog.LevelError, "qwp/sf: "+message, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	cleanup()
}

// deferOwnedCleanupUntilWorkerExit transfers terminal engine cleanup to the
// owned manager's exit block. true means the worker owns it now. false means
// nothing was handed off -- the worker is past its loop, was never started, or
// its one cleanup slot is already taken -- so the caller cleans up inline.
func (m *qwpSfSegmentManager) deferOwnedCleanupUntilWorkerExit(cleanup func()) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started || m.workerLoopExited || m.workerReaped {
		return false
	}
	if m.ownedEngineExitCleanup != nil {
		// The slot is already taken by another engine's cleanup, so this one
		// was not handed off. Reporting true here would claim a handoff that
		// never happened and leave the caller's cleanup with no owner at all.
		return false
	}
	m.ownedEngineExitCleanup = cleanup
	return true
}

func (m *qwpSfSegmentManager) awaitRingQuiescence(entry *qwpSfManagerRingEntry) bool {
	if entry == nil || m.workerGoid.Load() == 0 || m.workerGoid.Load() == qwpGoid() {
		return true
	}
	deadline := time.Now().Add(qwpSfManagerCloseGrace.load())
	for entry.isInService() {
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// deferUntilRingQuiescent transfers cleanup to the current service pass with
// no ownerless gap. A false result means the pass finished and the caller
// reclaimed ownership; true means the worker owns it or already took it.
func (m *qwpSfSegmentManager) deferUntilRingQuiescent(entry *qwpSfManagerRingEntry, cleanup func()) bool {
	if entry == nil || !entry.isInService() {
		return false
	}
	f := cleanup
	ptr := &f
	if entry.cleanup.CompareAndSwap(nil, ptr) {
		if entry.isInService() {
			return true
		}
		return !entry.cleanup.CompareAndSwap(ptr, nil)
	}
	return true
}

// managerWorkerError returns a terminal error when the worker goroutine has
// panicked and stopped provisioning/trimming, or nil while it is healthy.
func (m *qwpSfSegmentManager) managerWorkerError() error {
	if d := m.workerPanic.Load(); d != nil {
		return fmt.Errorf("qwp/sf: segment manager worker stopped: %s", *d)
	}
	return nil
}

// serviceRing performs one round of spare provisioning and trim for
// a single ring. Cheap when the ring already has a spare and no
// trimmable sealed segments — the common steady-state case.
func (m *qwpSfSegmentManager) serviceRing(e *qwpSfManagerRingEntry) {
	memoryMode := e.dir == ""
	if e.ring.needsHotSpare() {
		// Snapshot totalBytes under lock — register/deregister can
		// mutate it from caller goroutines. Heavy provisioning I/O
		// happens outside the lock; the post-install commit
		// re-acquires it.
		m.mu.Lock()
		observedTotal := m.totalBytes
		m.mu.Unlock()
		if observedTotal+m.segmentSizeBytes > m.maxTotalBytes {
			// Disk/memory cap reached: skip provisioning. Producers
			// will block on engineAppendBlocking until in-flight
			// segments are ACK'd and trimmed, so this state is exactly
			// the one operators need surfaced. Logged at most once per
			// qwpSfManagerDiskFullLogThrottle so a sustained cap-full
			// state doesn't drown logs. The log write happens after the
			// lock is released to keep the syscall off m.mu.
			now := time.Now()
			m.mu.Lock()
			shouldLog := now.Sub(m.lastDiskFullLog) >= qwpSfManagerDiskFullLogThrottle
			if shouldLog {
				m.lastDiskFullLog = now
			}
			m.mu.Unlock()
			if shouldLog {
				// Guarded like every other log this goroutine writes: the
				// worker's recover is final, so a panicking user handler here
				// would stop provisioning and trimming for every slot this
				// manager serves, and the producer would be told
				// "segment manager worker stopped" for what is a logging bug.
				logger := m.logger.Load()
				if memoryMode {
					qwpSfLogGuarded(logger, slog.LevelWarn,
						"qwp/sf: in-memory segment cap reached; spare provisioning "+
							"paused — producers block until in-flight segments are ACK'd and trimmed",
						"usedBytes", observedTotal, "maxBytes", m.maxTotalBytes,
						"segmentSize", m.segmentSizeBytes)
				} else {
					qwpSfLogGuarded(logger, slog.LevelWarn,
						"qwp/sf: disk cap reached; spare provisioning "+
							"paused — producers block until in-flight segments are ACK'd and trimmed",
						"dir", e.dir, "usedBytes", observedTotal, "maxBytes", m.maxTotalBytes,
						"segmentSize", m.segmentSizeBytes)
				}
			}
		} else {
			var (
				spare *qwpSfSegment
				path  string
				err   error
			)
			if memoryMode {
				spare, err = qwpSfCreateInMemorySegment(e.ring.nextSeqHint(), m.segmentSizeBytes)
			} else {
				path = m.nextSparePath(e.dir)
				spare, err = qwpSfCreateSegment(path, e.ring.nextSeqHint(), m.segmentSizeBytes)
				if err == nil && e.ring.ringManifest() != nil {
					err = spare.markManifestRequired()
				}
				if err == nil {
					// The spare's name has to be durable before a rotation can
					// commit its base as the manifest's active one. fsync on
					// the file does not carry the directory entry of a file
					// this new, so without this barrier a crash can leave the
					// manifest naming an active base with no segment at it --
					// which recovery refuses, quarantining the whole slot and
					// every undelivered row in it. The barrier runs here, on
					// the provisioning goroutine, so it costs the producer's
					// flush nothing.
					if syncErr := qwpSfSyncSlotDir(e.dir); syncErr != nil {
						err = fmt.Errorf("qwp/sf: fsync slot directory after minting spare %s: %w", path, syncErr)
					}
				}
			}
			if err == nil {
				// Install + commit atomically under the manager lock.
				// If e.ring was deregistered between the snapshot
				// above and now, abandoning the spare here is the
				// only way to keep totalBytes consistent.
				m.mu.Lock()
				stillRegistered := false
				for i := range m.rings {
					if m.rings[i].ring == e.ring {
						stillRegistered = true
						break
					}
				}
				installed := false
				if stillRegistered {
					installErr := e.ring.installHotSpare(spare)
					if installErr == nil {
						m.totalBytes += m.segmentSizeBytes
						e.accountedBytes += m.segmentSizeBytes
						installed = true
					}
				}
				m.mu.Unlock()
				if !installed {
					_ = spare.close()
					if path != "" {
						_ = os.Remove(path)
					}
				}
			} else if path != "" {
				if spare != nil {
					_ = spare.close()
				}
				// Defense-in-depth: qwpSfCreateSegment already best-
				// effort removes the file on its own failure paths
				// (truncate fail, mmap fail). If a future change
				// breaks that invariant — or if anything before the
				// try block leaves a file on disk — this second-line
				// remove keeps the slot from accumulating zero-content
				// .sfa files under sustained provisioning failure.
				// Repeated remove on an already-removed path is a
				// harmless no-op.
				_ = os.Remove(path)
			}
		}
	}

	// 2. Persist the current ackedFsn to the slot's .ack-watermark
	//    BEFORE the trim runs (sf-client.md §5.4). The ordering is
	//    what makes recovery's max(lowestSurvivingBaseSeq-1,
	//    watermark) clamp crash-safe in either direction: a crash
	//    after persist but before the unlinks leaves segments on disk
	//    with a correct watermark; a crash after the unlinks leaves a
	//    stale-low watermark the higher lowestBase overrides. The
	//    write is gated on advance, so a steady ackedFsn doesn't
	//    dirty the mapped page every tick. nil watermark (memory
	//    mode / open failed) is a no-op.
	e.watermark.persistIfAdvanced(e.ring.segmentRingAckedFsn())

	// 3. Retry the unlinks and the directory fsync an earlier pass could not
	//    complete. Their bytes are still charged to the slot, so this is what
	//    gives the ring its capacity back.
	deferredBytes, deferredErr := m.retryDeferredTrimWork(e)
	// Those bytes left the disk the moment their unlink succeeded, and the
	// retry list no longer holds them, so credit them here rather than at the
	// end of the pass. Several exits below this point return early, and each
	// one that skipped the credit would charge the slot for files that are
	// gone -- permanently, since nothing revisits them.
	m.commitTrimAccounting(e, deferredBytes)

	// 4. Trim any segments that the ring says are fully acked. For
	//    memory-mode rings, "trim" is just close (the slice is GC'd) —
	//    no file to unlink.
	trim := e.ring.peekTrimmable()
	if len(trim) == 0 {
		if deferredErr != nil {
			m.recordServiceError(e, deferredErr)
			return
		}
		// Nothing acked is waiting to be reclaimed, so whatever stopped an
		// earlier pass no longer holds anything back.
		e.entryMaintenanceSucceeded()
		return
	}
	if !memoryMode {
		if err := e.watermark.sync(); err != nil {
			m.recordServiceError(e, err)
			return
		}
		if err := qwpSfSyncSlotDir(e.dir); err != nil {
			m.recordServiceError(e, fmt.Errorf("pre-trim directory fsync: %w", err))
			return
		}
		newHead := e.ring.headAfterTrim(len(trim))
		active := e.ring.getActiveSegment()
		if active == nil {
			m.recordServiceError(e, errors.New("ring has no active segment during trim"))
			return
		}
		manifest := e.ring.ringManifest()
		if manifest == nil {
			m.recordServiceError(e, errors.New("disk ring has no SF manifest during trim"))
			return
		}
		if err := manifest.update(newHead, active.segmentBaseSeq()); err != nil {
			m.recordServiceError(e, err)
			return
		}
	}
	trim = e.ring.drainTrimBatch(len(trim))
	trimErr := deferredErr
	trimmedBytes := int64(0)
	for _, s := range trim {
		path := s.segmentPath()
		sz := s.segmentSize()
		if err := s.close(); err != nil && trimErr == nil {
			trimErr = err
		}
		if path != "" {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				// The file is still occupying the slot, so hold its bytes and
				// retry it rather than freeing capacity for space nothing gave
				// back.
				e.pendingUnlinks = append(e.pendingUnlinks, qwpSfPendingUnlink{path: path, sizeBytes: sz})
				if trimErr == nil {
					trimErr = err
				}
				continue
			}
		}
		trimmedBytes += sz
	}
	if !memoryMode {
		if err := qwpSfSyncSlotDir(e.dir); err != nil {
			e.dirSyncPending = true
			if trimErr == nil {
				trimErr = fmt.Errorf("post-trim directory fsync: %w", err)
			}
		} else {
			e.dirSyncPending = false
		}
	}
	if trimErr != nil {
		m.recordServiceError(e, trimErr)
	} else {
		e.entryMaintenanceSucceeded()
	}
	if hook := qwpSfTestBeforeTrimAccountingHook.Load(); hook != nil {
		(*hook)(e)
	}
	m.commitTrimAccounting(e, trimmedBytes)
}

// retryDeferredTrimWork re-attempts the unlinks and the directory fsync that
// earlier passes could not complete, and returns the bytes reclaimed by the
// files that are now gone. Worker goroutine only.
func (m *qwpSfSegmentManager) retryDeferredTrimWork(e *qwpSfManagerRingEntry) (int64, error) {
	if len(e.pendingUnlinks) == 0 && !e.dirSyncPending {
		return 0, nil
	}
	var freed int64
	var firstErr error
	kept := e.pendingUnlinks[:0]
	for _, pending := range e.pendingUnlinks {
		if err := os.Remove(pending.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			kept = append(kept, pending)
			if firstErr == nil {
				firstErr = fmt.Errorf("retry unlink of trimmed segment: %w", err)
			}
			continue
		}
		freed += pending.sizeBytes
		e.dirSyncPending = true
	}
	e.pendingUnlinks = kept
	if e.dirSyncPending && e.dir != "" {
		if err := qwpSfSyncSlotDir(e.dir); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("retry post-trim directory fsync: %w", err)
			}
		} else {
			e.dirSyncPending = false
		}
	}
	return freed, firstErr
}

// commitTrimAccounting returns reclaimed bytes to the manager's cap.
func (m *qwpSfSegmentManager) commitTrimAccounting(e *qwpSfManagerRingEntry, trimmedBytes int64) {
	if trimmedBytes == 0 {
		return
	}
	m.mu.Lock()
	// Deregistration removes entry.accountedBytes, which still includes this
	// batch until the accounting commit. If it won the race, the zero value here
	// proves there is nothing left to subtract; otherwise commit both totals.
	if state := e.state.Load(); state != qwpSfManagerRingDeregisteredInService && state != qwpSfManagerRingDeregistered {
		m.totalBytes -= trimmedBytes
		e.accountedBytes -= trimmedBytes
	}
	m.mu.Unlock()
}

// recordServiceError notes one failed maintenance pass for a ring. Maintenance
// keeps retrying on every tick, so a lone failure stays a log line. Once the
// failures form a run of qwpSfManagerMaintenanceFailureThreshold, the entry
// publishes the error: at that point trim has stopped for long enough that the
// ring fills, and the producer that ends up parked on it needs to be told its
// disk is refusing writes rather than being handed the backpressure timeout's
// "server slow / disconnected" reading. Worker goroutine only.
func (m *qwpSfSegmentManager) recordServiceError(e *qwpSfManagerRingEntry, err error) {
	dir := ""
	if e != nil {
		dir = e.dir
		e.maintenanceFailures++
		if e.maintenanceFailures >= qwpSfManagerMaintenanceFailureThreshold {
			sticky := fmt.Errorf("%w: slot maintenance has failed %d consecutive times: %w",
				ErrSfDurability, e.maintenanceFailures, err)
			e.maintenanceError.Store(&sticky)
		}
	}
	now := time.Now()
	m.mu.Lock()
	shouldLog := now.Sub(m.lastMaintenanceLog) >= qwpSfManagerDiskFullLogThrottle
	if shouldLog {
		m.lastMaintenanceLog = now
	}
	m.mu.Unlock()
	if shouldLog {
		qwpSfLogGuarded(m.logger.Load(), slog.LevelError, "qwp/sf: segment manager maintenance failed; will retry", "dir", dir, "error", err)
	}
}
