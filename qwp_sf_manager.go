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
	qwpSfManagerDefaultPoll                 = 1 * time.Millisecond // poll cadence
	qwpSfManagerDiskFullLogThrottle         = 30 * time.Second     // throttle disk-full WARNs
	qwpSfManagerQuarantineReconcileInterval = 1 * time.Second      // limit how often deletion checks read the directory

	// A maintenance error reaches producers only after failures last this long.
	// The worker's poll rate does not change the delay.
	qwpSfManagerMaintenanceFailureDuration = 1 * time.Second
)

// qwpSfManagerCloseGrace limits how long internal close calls wait, not how
// long cleanup may run. The engine's cleanup worker waits for the manager to
// stop, without a timeout. Tests may shorten the caller's wait.
var qwpSfManagerCloseGrace = qwpSfSwappable(5 * time.Second)

var qwpSfTestAfterSpareCreateHook atomic.Pointer[func(*qwpSfManagerRingEntry)]

func qwpSfSyncSpareCreationEpoch(dir, path string) error {
	if err := qwpSfSyncSlotDir(dir); err != nil {
		return qwpSfDurabilityError("sync slot directory after minting spare", path, err)
	}
	return nil
}

func qwpSfSyncPreTrimEpoch(dir string) error {
	if err := qwpSfSyncSlotDir(dir); err != nil {
		return qwpSfDurabilityError("sync pre-trim directory", dir, err)
	}
	return nil
}

func qwpSfSyncPostTrimEpoch(dir string) error {
	if err := qwpSfSyncSlotDir(dir); err != nil {
		return qwpSfDurabilityError("sync post-trim directory", dir, err)
	}
	return nil
}

// qwpSfUnlimitedTotalBytes disables the per-engine total-bytes cap.
const qwpSfUnlimitedTotalBytes int64 = math.MaxInt64

// qwpSfSegmentManager prepares spare segments and removes old ones after the
// server acknowledges their frames. This background worker handles slow file
// and memory-mapping operations so appending and sending do not have to.
//
// Each production engine has its own manager. Tests can register several rings
// with one manager to check space accounting. The manager checks for work every
// millisecond by default, keeping a spare ready without continuously polling
// when idle.
type qwpSfSegmentManager struct {
	segmentSizeBytes int64
	pollInterval     time.Duration
	maxTotalBytes    int64
	// Tests replace these functions to control time and file errors without
	// sleeping or changing global state. Production uses the real functions.
	now                  func() time.Time
	scanQuarantinedBytes func(string) (int64, error)
	removeFile           func(string) error

	// fileGeneration increases for each spare filename (sf-<gen:016x>.sfa).
	// When registering a recovered ring, skip past numbers already on disk.
	fileGeneration atomic.Uint64

	// logger sinks the cap-reached backpressure diagnostic. The manager's
	// worker goroutine reads it, so it is an atomic.Pointer the engine's
	// constructor callers set race-free after the worker has started. nil
	// (the zero value) -> slog.Default() via qwpEffectiveLogger.
	logger atomic.Pointer[slog.Logger]

	// A panic or internal cleanup failure stops this manager permanently.
	// Its engine keeps the entries and any resources opened before the failure.
	workerErr          atomic.Pointer[error]
	failedAcquisitions *qwpSfAcquiredResources // read only after the worker stops

	mu    sync.Mutex
	rings []*qwpSfManagerRingEntry
	// totalBytes counts live segments and cached `.corrupt` files for all rings.
	totalBytes         int64
	lastDiskFullLog    time.Time
	lastMaintenanceLog time.Time
	closed             bool

	// wakeup is a single-slot channel. wakeWorker pushes into it
	// non-blockingly; the worker drains in select to coalesce signals.
	wakeup chan struct{}
	// done is closed when the worker goroutine exits.
	done chan struct{}

	started bool

	// ringSnapshot is workerLoop's reusable copy of rings. Each tick
	// refills it from rings under mu, then releases mu before the
	// per-ring service pass so the slow segment syscalls run without
	// the lock held. Owned solely by workerLoop; the locked refill is
	// its only synchronization.
	ringSnapshot []*qwpSfManagerRingEntry
}

// qwpSfManagerRingEntry holds a registered ring and the directory
// its segments live in (nil for memory-mode rings).
type qwpSfManagerRingEntry struct {
	ring *qwpSfSegmentRing
	dir  string
	// accountedBytes is this entry's contribution to manager.totalBytes.
	// Protected by manager.mu; unlike ring.totalSegmentBytes it deliberately
	// retains a drained batch until the service pass commits its accounting.
	accountedBytes int64
	// quarantinedBytes is the last known size of this slot's `.corrupt` files.
	// manager.mu protects it, and manager.totalBytes includes it. Recovery creates
	// all quarantine files before registration, so the first scan sees them.
	quarantinedBytes int64
	// The worker owns lastQuarantineReconcile after registration. While the byte
	// limit blocks a new segment, it scans at most once per interval.
	lastQuarantineReconcile time.Time
	// watermark is the engine-owned .ack-watermark for this slot, or
	// nil in memory mode / when the file could not be opened. The
	// manager writes through it on every tick where ackedFsn
	// advanced; it never closes it (the owning engine does, in
	// engineClose, after this entry is quiescent). Entries are shared
	// pointers so close and the worker observe one state machine.
	watermark *qwpSfAckWatermark
	// These fields track the current run of maintenance failures. Only the worker
	// changes them. A pass that does no work leaves them unchanged. Successful
	// maintenance clears them.
	maintenanceFailureActive bool
	maintenanceFailureSince  time.Time
	maintenanceLastError     error
	// maintenanceError is the error that producers can read. The worker replaces
	// or clears the pointer, so readers do not need manager.mu.
	maintenanceError atomic.Pointer[error]
	// pendingUnlinks holds trimmed segments whose file is still on disk because
	// the unlink failed. Their bytes stay charged to the slot until the file is
	// really gone, and every later pass retries them: crediting a failed delete
	// would keep handing the ring capacity for space it never got back, letting
	// the slot grow past its cap while the disk fills. Worker goroutine only.
	pendingUnlinks []qwpSfPendingUnlink
	// Keep references to segments being created or removed, so a panic cannot
	// lose track of their mappings. Only the manager worker uses these fields
	// until it stops; then the engine's cleanup worker may read them.
	spareInProgress *qwpSfSegment
	trimInProgress  []*qwpSfSegment
	releaseErr      error
	// dirSyncPending records that a post-trim directory fsync failed, so the
	// unlinks that pass did complete are not durable yet. Retried by later
	// passes for the same reason as pendingUnlinks. Worker goroutine only.
	dirSyncPending bool
}

// qwpSfPendingUnlink is one trimmed segment file that could not be removed,
// with the bytes still charged to the manager on its behalf.
type qwpSfPendingUnlink struct {
	segment   *qwpSfSegment
	acquired  *qwpSfAcquiredResources
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

// entryMaintenanceSucceeded clears the current failure run. Only the worker
// calls it. Pending deletes or a pending directory sync keep the failure active
// until that work succeeds.
func (e *qwpSfManagerRingEntry) entryMaintenanceSucceeded() {
	if !e.maintenanceFailureActive || len(e.pendingUnlinks) > 0 || e.dirSyncPending {
		return
	}
	e.maintenanceFailureActive = false
	e.maintenanceFailureSince = time.Time{}
	e.maintenanceLastError = nil
	e.maintenanceError.Store(nil)
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
		segmentSizeBytes:     segmentSizeBytes,
		pollInterval:         pollInterval,
		maxTotalBytes:        maxTotalBytes,
		now:                  time.Now,
		scanQuarantinedBytes: qwpSfQuarantinedBytes,
		removeFile:           os.Remove,
		wakeup:               make(chan struct{}, 1),
		done:                 make(chan struct{}),
	}, nil
}

// segmentManagerStart spawns the worker goroutine. Idempotent — a
// second call is a panic, mirroring Java's IllegalStateException.
func (m *qwpSfSegmentManager) segmentManagerStart() {
	m.mu.Lock()
	if m.closed || m.started {
		m.mu.Unlock()
		panic("qwp/sf: segment manager already started or closed")
	}
	m.started = true
	m.mu.Unlock()
	go m.workerLoop()
}

// segmentManagerStop asks the worker to stop and returns a channel that closes
// once it has stopped. It does not release resources; engine cleanup does that.
func (m *qwpSfSegmentManager) segmentManagerStop() <-chan struct{} {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		if !m.started {
			close(m.done)
		}
	}
	m.mu.Unlock()
	m.wakeWorker()
	return m.done
}

// segmentManagerClose asks the worker to stop and waits for a limited time.
// It is used when calling the manager directly, including in tests. Engine
// cleanup instead waits on segmentManagerStop's channel without a timeout.
func (m *qwpSfSegmentManager) segmentManagerClose() bool {
	done := m.segmentManagerStop()
	timer := time.NewTimer(qwpSfManagerCloseGrace.load())
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
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
	quarantinedBytes := int64(0)
	lastQuarantineReconcile := time.Time{}
	if dir != "" {
		var err error
		quarantinedBytes, err = m.scanQuarantinedBytes(dir)
		if err != nil {
			return nil, qwpSfDurabilityError("scan quarantined bytes during manager registration", dir, err)
		}
		lastQuarantineReconcile = m.now()
		// Move the generation counter past existing files before the worker can
		// see this ring. This prevents the worker from reusing a file name.
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
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("qwp/sf: segment manager closed")
	}
	entry := &qwpSfManagerRingEntry{
		ring:                    ring,
		dir:                     dir,
		watermark:               watermark,
		accountedBytes:          ring.totalSegmentBytes(),
		quarantinedBytes:        quarantinedBytes,
		lastQuarantineReconcile: lastQuarantineReconcile,
	}
	m.rings = append(m.rings, entry)
	// Count the bytes the ring already owns. A recovered ring may already be at
	// or above the limit, so registration must count it before creating spares.
	m.totalBytes += entry.accountedBytes + entry.quarantinedBytes
	m.mu.Unlock()
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

// nextSparePath returns the next available <dir>/sf-<gen:016x>.sfa path.
// Spare filenames use an increasing counter because the segment's starting
// frame number is not yet final. Recovery identifies segments by their file
// extension and header, not by the number in the filename.
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
	timer := time.NewTimer(m.pollInterval)
	defer timer.Stop()
	// This goroutine drives no untrusted input, so a panic here is not a
	// known reachable path; recover anyway — an uncaught panic on it would
	// crash the host. Provisioning and trim are now dead, so latch the failure:
	// engineTerminalError surfaces it to producers on their next append rather
	// than letting them stall in backpressure forever with no signal. The
	// deferred close(m.done) signals worker exit.
	defer func() {
		if r := recover(); r != nil {
			if held, ok := r.(*qwpSfAcquisitionPanic); ok {
				m.failedAcquisitions = held.resources
			}
			detail := fmt.Sprintf("%v\n%s", r, debug.Stack())
			failure := fmt.Errorf("qwp/sf: segment manager worker stopped: %s", detail)
			m.workerErr.Store(&failure)
			qwpEffectiveLogger(m.logger.Load()).Error("qwp/sf: segment manager worker panicked",
				"detail", detail)
		}
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
			m.serviceRing(e)
			if m.managerWorkerError() != nil {
				return
			}
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

// managerWorkerError returns a terminal error when the worker goroutine has
// panicked and stopped provisioning/trimming, or nil while it is healthy.
func (m *qwpSfSegmentManager) managerWorkerError() error {
	if err := m.workerErr.Load(); err != nil {
		return *err
	}
	return nil
}

// serviceRing performs one round of spare provisioning and trim for
// a single ring. Cheap when the ring already has a spare and no
// trimmable sealed segments — the common steady-state case.
// qwpSfQuarantinedBytes adds the sizes of `.corrupt` files in a slot. The client
// keeps these files because they may contain undelivered rows, so they count
// toward the byte limit. Memory mode returns zero. The function returns scan
// and stat errors so callers can keep the previous count.
func qwpSfQuarantinedBytes(dir string) (int64, error) {
	if dir == "" {
		return 0, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.Contains(entry.Name(), ".corrupt") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// reconcileQuarantinedBytes recounts the bytes in a slot's .corrupt files
// without holding m.mu. Engine cleanup waits for this worker to stop before
// releasing the slot.
func (m *qwpSfSegmentManager) reconcileQuarantinedBytes(e *qwpSfManagerRingEntry, now time.Time) error {
	if e == nil || e.dir == "" || (!e.lastQuarantineReconcile.IsZero() && now.Sub(e.lastQuarantineReconcile) < qwpSfManagerQuarantineReconcileInterval) {
		return nil
	}
	// Record failed attempts too, so an unreadable directory is not scanned on
	// every worker poll.
	e.lastQuarantineReconcile = now
	quarantinedBytes, err := m.scanQuarantinedBytes(e.dir)
	if err != nil {
		return qwpSfDurabilityError("reconcile quarantined bytes", e.dir, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalBytes += quarantinedBytes - e.quarantinedBytes
	e.quarantinedBytes = quarantinedBytes
	return nil
}

func (m *qwpSfSegmentManager) wouldExceedCapWithSpare() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalBytes > m.maxTotalBytes-m.segmentSizeBytes
}

func (m *qwpSfSegmentManager) serviceRing(e *qwpSfManagerRingEntry) {
	memoryMode := e.dir == ""
	maintenanceWorked := false
	// The byte limit covers all rings. While the manager is at the limit, rescan
	// each disk ring when its interval expires. This also notices files deleted
	// from a ring that already has a spare.
	if !memoryMode && m.wouldExceedCapWithSpare() {
		if err := m.reconcileQuarantinedBytes(e, m.now()); err != nil {
			m.logServiceError(e.dir, err)
		}
	}
	// A spare this pass could not provision. Carried to the end so the trim
	// still runs -- on a full disk the trim is what frees the space -- and so
	// the pass cannot report success over it.
	var spareErr error
	if e.ring.needsHotSpare() {
		// Read totalBytes under the lock because other goroutines can change it
		// when registering rings. Create the spare outside the lock, then take
		// the lock again when installing it and updating the byte count.
		m.mu.Lock()
		observedTotal := m.totalBytes
		quarantined := e.quarantinedBytes
		m.mu.Unlock()
		// `.corrupt` files count toward the byte limit. Their cached size is
		// already part of observedTotal, so this check does no directory I/O.
		if observedTotal > m.maxTotalBytes-m.segmentSizeBytes {
			// Disk/memory cap reached: skip provisioning. Producers
			// will block on engineAppendBlocking until in-flight
			// segments are ACK'd and trimmed, so this state is exactly
			// the one operators need surfaced. Logged at most once per
			// qwpSfManagerDiskFullLogThrottle so a sustained cap-full
			// state doesn't drown logs. The log write happens after the
			// lock is released to keep the syscall off m.mu.
			now := m.now()
			m.mu.Lock()
			shouldLog := now.Sub(m.lastDiskFullLog) >= qwpSfManagerDiskFullLogThrottle
			if shouldLog {
				m.lastDiskFullLog = now
			}
			m.mu.Unlock()
			if shouldLog {
				// The guarded handler matters especially on this goroutine: the
				// worker.'s recover is final, so a panicking user handler here
				// would stop provisioning and trimming for every slot this
				// manager serves, and the producer would be told
				// "segment manager worker stopped" for what is a logging bug.
				logger := m.logger.Load()
				if memoryMode {
					qwpEffectiveLogger(logger).Warn("qwp/sf: in-memory segment cap reached; spare provisioning "+
						"paused — producers block until in-flight segments are ACK'd and trimmed",
						"usedBytes", observedTotal, "maxBytes", m.maxTotalBytes,
						"segmentSize", m.segmentSizeBytes)
				} else {
					qwpEffectiveLogger(logger).Warn("qwp/sf: disk cap reached; spare provisioning "+
						"paused — producers block until in-flight segments are ACK'd and trimmed"+
						" (quarantined .corrupt bytes count against the cap; deleting them regains space)",
						"dir", e.dir, "usedBytes", observedTotal, "quarantinedBytes", quarantined,
						"maxBytes", m.maxTotalBytes, "segmentSize", m.segmentSizeBytes)
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
				e.spareInProgress = spare
			} else {
				path = m.nextSparePath(e.dir)
				spare, err = qwpSfCreateSegment(path, e.ring.nextSeqHint(), m.segmentSizeBytes)
				e.spareInProgress = spare
				if err == nil {
					if hook := qwpSfTestAfterSpareCreateHook.Load(); hook != nil {
						(*hook)(e)
					}
				}
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
					if syncErr := qwpSfSyncSpareCreationEpoch(e.dir, path); syncErr != nil {
						err = syncErr
					}
				}
			}
			if err == nil {
				// Install the spare and update its byte count under the same lock.
				// Only rings registered with this manager may use its space.
				m.mu.Lock()
				stillRegistered := false
				for i := range m.rings {
					if m.rings[i].ring == e.ring {
						stillRegistered = true
						break
					}
				}
				installed := false
				var installErr error
				// Registration or a quarantine scan may use the remaining space
				// while the spare is being created. Check the current total before
				// installing it.
				if stillRegistered && m.totalBytes <= m.maxTotalBytes-m.segmentSizeBytes {
					installErr = e.ring.installHotSpare(spare)
					if installErr == nil {
						m.totalBytes += m.segmentSizeBytes
						e.accountedBytes += m.segmentSizeBytes
						installed = true
						maintenanceWorked = true
					}
				}
				m.mu.Unlock()
				if !installed {
					spareErr = errors.Join(installErr, m.cleanupUninstalledSpare(e, spare, path))
				}
			} else {
				// The provisioning failure has to reach someone. Silently
				// unlinking and retrying next tick leaves a slot whose spare
				// never installs, so rotation stops and the producer is told
				// the buffer is full -- ErrBackpressureTimeout, "SF out of
				// space" -- for what is actually a local storage fault. It is
				// carried to the end of the pass rather than reported here so
				// the trim below still runs: on a full disk the trim is what
				// frees the space the spare needs.
				var held *qwpSfAcquisitionError
				if errors.As(err, &held) {
					// Keep the opened resources before any error-handling path can
					// try to remove their file.
					e.pendingUnlinks = append(e.pendingUnlinks, qwpSfPendingUnlink{acquired: held.resources, path: path, sizeBytes: m.segmentSizeBytes})
					m.mu.Lock()
					m.totalBytes += m.segmentSizeBytes
					e.accountedBytes += m.segmentSizeBytes
					m.mu.Unlock()
					spareErr = err
					if errors.Is(err, ErrCleanupFailed) {
						m.recordServiceError(e, err)
						return
					}
				} else {
					spareErr = errors.Join(err, m.cleanupUninstalledSpare(e, spare, path))
				}
			}
			e.spareInProgress = nil // the reference is now saved in the ring or pendingUnlinks
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
	watermarkAdvanced, watermarkErr := e.watermark.persistIfAdvanced(e.ring.segmentRingAckedFsn())
	maintenanceWorked = maintenanceWorked || watermarkAdvanced

	// 3. Retry the unlinks and the directory fsync an earlier pass could not
	//    complete. Their bytes are still charged to the slot, so this is what
	//    gives the ring its capacity back.
	hadDeferredWork := len(e.pendingUnlinks) > 0 || e.dirSyncPending
	deferredBytes, deferredErr := m.retryDeferredTrimWork(e)
	if hadDeferredWork && deferredErr == nil {
		maintenanceWorked = true
	}
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
		if joined := errors.Join(spareErr, deferredErr, watermarkErr); joined != nil {
			m.recordServiceError(e, joined)
			return
		}
		if maintenanceWorked {
			e.entryMaintenanceSucceeded()
		}
		return
	}
	if !memoryMode {
		// A failed watermark advance cannot license deletion of newly acked
		// segments. Deferred work above was licensed by an earlier successful
		// barrier and remains safe to finish, but this trim must wait.
		if watermarkErr != nil {
			m.recordServiceError(e, errors.Join(spareErr, deferredErr, watermarkErr))
			return
		}
		// Every exit below joins both carried errors in. A pass that could not
		// mint a spare, or could not finish an earlier trim, AND then fails
		// here is the case an operator most needs to read correctly: the
		// failure they see is this one, but the reason rotation stopped is the
		// carried one.
		if err := e.watermark.sync(); err != nil {
			m.recordServiceError(e, errors.Join(spareErr, deferredErr, err))
			return
		}
		if err := qwpSfSyncPreTrimEpoch(e.dir); err != nil {
			m.recordServiceError(e, errors.Join(spareErr, deferredErr, err))
			return
		}
		newHead := e.ring.headAfterTrim(len(trim))
		active := e.ring.getActiveSegment()
		if active == nil {
			m.recordServiceError(e, errors.Join(spareErr, deferredErr, errors.New("ring has no active segment during trim")))
			return
		}
		manifest := e.ring.ringManifest()
		if manifest == nil {
			m.recordServiceError(e, errors.Join(spareErr, deferredErr, errors.New("disk ring has no SF manifest during trim")))
			return
		}
		if err := manifest.update(newHead, active.segmentBaseSeq()); err != nil {
			m.recordServiceError(e, errors.Join(spareErr, deferredErr, err))
			return
		}
	}
	e.trimInProgress = trim
	trim = e.ring.drainTrimBatch(len(trim))
	trimErr := errors.Join(spareErr, deferredErr)
	trimmedBytes := int64(0)
	for _, s := range trim {
		path := s.segmentPath()
		sz := s.segmentSize()
		closeErr := s.close()
		trimErr = errors.Join(trimErr, closeErr)
		if !s.resourcesReleased() {
			e.pendingUnlinks = append(e.pendingUnlinks, qwpSfPendingUnlink{segment: s, path: path, sizeBytes: sz})
			continue
		}
		e.releaseErr = errors.Join(e.releaseErr, s.closeErr)
		if path != "" {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				// The file is still occupying the slot, so hold its bytes and
				// retry it rather than freeing capacity for space nothing gave
				// back.
				e.pendingUnlinks = append(e.pendingUnlinks, qwpSfPendingUnlink{path: path, sizeBytes: sz})
				if trimErr == nil {
					trimErr = qwpSfDurabilityError("unlink trimmed segment", path, err)
				}
				continue
			}
		}
		trimmedBytes += sz
	}
	e.trimInProgress = nil
	if !memoryMode {
		if err := qwpSfSyncPostTrimEpoch(e.dir); err != nil {
			e.dirSyncPending = true
			if trimErr == nil {
				trimErr = err
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
	m.commitTrimAccounting(e, trimmedBytes)
}

// cleanupUninstalledSpare closes and removes a spare that was not installed.
// If removal fails, it counts one segment and tries again later. This may count
// more than a partial file uses, but it never lets the manager exceed the limit.
func (m *qwpSfSegmentManager) cleanupUninstalledSpare(e *qwpSfManagerRingEntry, spare *qwpSfSegment, path string) error {
	closeErr := spare.close()
	if spare.resourcesReleased() {
		if spare != nil {
			e.releaseErr = errors.Join(e.releaseErr, spare.closeErr)
		}
		if path == "" {
			return closeErr
		}
		removeErr := m.removeFile(path)
		if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
			return closeErr
		}
		closeErr = errors.Join(closeErr, qwpSfDurabilityError("remove uninstalled spare", path, removeErr))
	}
	// Save unfinished work for the manager's next attempt or engine cleanup.
	e.pendingUnlinks = append(e.pendingUnlinks, qwpSfPendingUnlink{segment: spare, path: path, sizeBytes: m.segmentSizeBytes})
	m.mu.Lock()
	m.totalBytes += m.segmentSizeBytes
	e.accountedBytes += m.segmentSizeBytes
	m.mu.Unlock()
	return closeErr
}

// retryDeferredTrimWork retries unfinished resource releases, file removals,
// and directory syncs. It returns the number of bytes freed by removed files.
// The manager calls it while running. After the manager stops, the engine's
// cleanup worker finishes any remaining work.
func (m *qwpSfSegmentManager) retryDeferredTrimWork(e *qwpSfManagerRingEntry) (int64, error) {
	if len(e.pendingUnlinks) == 0 && !e.dirSyncPending {
		return 0, nil
	}
	var freed int64
	var firstErr error
	kept := e.pendingUnlinks[:0]
	for _, pending := range e.pendingUnlinks {
		if pending.acquired != nil {
			closeErr := pending.acquired.close()
			if !pending.acquired.released() || errors.Is(closeErr, ErrCleanupFailed) {
				kept = append(kept, pending)
				firstErr = errors.Join(firstErr, closeErr)
				continue
			}
			e.releaseErr = errors.Join(e.releaseErr, closeErr)
			pending.acquired = nil
		}
		if pending.segment != nil {
			closeErr := pending.segment.close()
			if !pending.segment.resourcesReleased() {
				kept = append(kept, pending)
				firstErr = errors.Join(firstErr, closeErr)
				continue
			}
			e.releaseErr = errors.Join(e.releaseErr, pending.segment.closeErr)
			pending.segment = nil
		}
		var removeErr error
		if pending.path != "" {
			removeErr = m.removeFile(pending.path)
		}
		if err := removeErr; err != nil && !errors.Is(err, os.ErrNotExist) {
			kept = append(kept, pending)
			if firstErr == nil {
				firstErr = qwpSfDurabilityError("retry unlink of trimmed segment", pending.path, err)
			}
			continue
		}
		freed += pending.sizeBytes
		e.dirSyncPending = true
	}
	e.pendingUnlinks = kept
	if e.dirSyncPending && e.dir != "" {
		if err := qwpSfSyncPostTrimEpoch(e.dir); err != nil {
			if firstErr == nil {
				firstErr = err
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
	m.totalBytes -= trimmedBytes
	e.accountedBytes -= trimmedBytes
	m.mu.Unlock()
}

// recordServiceError tracks how long maintenance has kept failing. After the
// delay, producers see the disk error instead of a generic backpressure error.
// Only the worker calls this method.
func (m *qwpSfSegmentManager) recordServiceError(e *qwpSfManagerRingEntry, err error) {
	if errors.Is(err, ErrCleanupFailed) {
		m.workerErr.Store(&err)
	}
	dir := ""
	if e != nil {
		dir = e.dir
		now := m.now()
		if !e.maintenanceFailureActive {
			e.maintenanceFailureActive = true
			e.maintenanceFailureSince = now
		}
		e.maintenanceLastError = err
		if now.Sub(e.maintenanceFailureSince) >= qwpSfManagerMaintenanceFailureDuration {
			sticky := qwpSfDurabilityError(
				fmt.Sprintf("slot maintenance has failed continuously for %s", now.Sub(e.maintenanceFailureSince)),
				dir,
				e.maintenanceLastError,
			)
			e.maintenanceError.Store(&sticky)
		}
	}
	m.logServiceError(dir, err)
}

func (m *qwpSfSegmentManager) logServiceError(dir string, err error) {
	now := m.now()
	m.mu.Lock()
	shouldLog := now.Sub(m.lastMaintenanceLog) >= qwpSfManagerDiskFullLogThrottle
	if shouldLog {
		m.lastMaintenanceLog = now
	}
	m.mu.Unlock()
	if shouldLog {
		qwpEffectiveLogger(m.logger.Load()).Error("qwp/sf: segment manager maintenance failed; will retry", "dir", dir, "error", err)
	}
}

// closeServiceResidue finishes work left by the manager. Call it only after
// the manager worker has stopped.
func (m *qwpSfSegmentManager) closeServiceResidue(e *qwpSfManagerRingEntry) error {
	_, err := m.retryDeferredTrimWork(e)
	return errors.Join(e.releaseErr, err)
}
func (e *qwpSfManagerRingEntry) serviceResidueReleased() bool {
	return e.spareInProgress == nil && len(e.trimInProgress) == 0 && len(e.pendingUnlinks) == 0 && !e.dirSyncPending
}
