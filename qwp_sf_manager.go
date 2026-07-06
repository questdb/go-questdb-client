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
	// qwpSfManagerCloseGrace bounds how long close() waits for the
	// worker goroutine to exit cleanly. Mirrors Java's 5-second join.
	qwpSfManagerCloseGrace = 5 * time.Second
)

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

	mu              sync.Mutex
	rings           []qwpSfManagerRingEntry
	totalBytes      int64
	lastDiskFullLog time.Time
	closed          bool

	// wakeup is a single-slot channel. wakeWorker pushes into it
	// non-blockingly; the worker drains in select to coalesce signals.
	wakeup chan struct{}
	// done is closed when the worker goroutine exits.
	done   chan struct{}
	worker sync.WaitGroup

	// ringSnapshot is workerLoop's reusable copy of rings. Each tick
	// refills it from rings under mu, then releases mu before the
	// per-ring service pass so the slow segment syscalls run without
	// the lock held. Owned solely by workerLoop; the locked refill is
	// its only synchronization.
	ringSnapshot []qwpSfManagerRingEntry
}

// qwpSfManagerRingEntry holds a registered ring and the directory
// its segments live in (nil for memory-mode rings).
type qwpSfManagerRingEntry struct {
	ring *qwpSfSegmentRing
	dir  string
	// watermark is the engine-owned .ack-watermark for this slot, or
	// nil in memory mode / when the file could not be opened. The
	// manager writes through it on every tick where ackedFsn
	// advanced; it never closes it (the owning engine does, in
	// engineClose, after the manager has stopped). The pointer is
	// copied by value into the per-tick ring snapshot, but the
	// persist state (lastPersistedAck) lives behind the pointer on
	// the watermark itself, so the snapshot copy is harmless.
	watermark *qwpSfAckWatermark
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
	m.mu.Unlock()
	m.worker.Add(1)
	go m.workerLoop()
}

// segmentManagerClose stops the worker goroutine and waits up to
// qwpSfManagerCloseGrace for it to exit. After close, the manager
// rejects new registrations and the worker no longer provisions or
// trims segments — but already-installed spares stay with their
// rings (the rings close them on their own segmentRingClose).
//
// Idempotent; safe to call from any goroutine.
func (m *qwpSfSegmentManager) segmentManagerClose() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.mu.Unlock()
	// Wake the worker so it observes closed and exits promptly.
	select {
	case m.wakeup <- struct{}{}:
	default:
	}
	// Bound the wait so a stuck worker can't deadlock close().
	doneCh := make(chan struct{})
	go func() {
		m.worker.Wait()
		close(doneCh)
	}()
	graceTimer := time.NewTimer(qwpSfManagerCloseGrace)
	defer graceTimer.Stop()
	select {
	case <-doneCh:
	case <-graceTimer.C:
	}
}

// segmentManagerDeregister stops tracking the given ring. Pending
// spares for the ring are NOT created after this returns, but
// already-installed spares stay with the ring. Idempotent; safe to
// call from any goroutine.
func (m *qwpSfSegmentManager) segmentManagerDeregister(ring *qwpSfSegmentRing) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.rings {
		if e.ring == ring {
			// Reverse the ring's contribution to totalBytes.
			m.totalBytes -= ring.totalSegmentBytes()
			// O(N) remove preserving order — register order matters
			// for log ordering, not correctness.
			m.rings = append(m.rings[:i], m.rings[i+1:]...)
			return
		}
	}
}

// segmentManagerRegister registers a ring with no ack-watermark
// (memory mode, or callers that don't persist a watermark — chiefly
// tests). Recovery for such a slot seeds from the segment-derived
// lowestBase-1 only.
func (m *qwpSfSegmentManager) segmentManagerRegister(ring *qwpSfSegmentRing, dir string) error {
	return m.segmentManagerRegisterWithWatermark(ring, dir, nil)
}

// segmentManagerRegisterWithWatermark registers a ring for ongoing
// spare creation + trim. dir is the filesystem directory the ring's
// segments live in — used both for creating spare files and
// unlinking trimmed ones. watermark (may be nil) is the slot's
// engine-owned .ack-watermark the manager keeps current on every
// tick; the manager never closes it. The ring MUST already have its
// initial active segment in place. Wires the ring's "I need a spare"
// callback so the producer can preempt the polling tick.
func (m *qwpSfSegmentManager) segmentManagerRegisterWithWatermark(ring *qwpSfSegmentRing, dir string, watermark *qwpSfAckWatermark) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("qwp/sf: segment manager closed")
	}
	m.rings = append(m.rings, qwpSfManagerRingEntry{ring: ring, dir: dir, watermark: watermark})
	// Account for bytes the ring already owns when it joins. A
	// recovered ring (post-restart, orphan adoption) can come up
	// at-or-above the cap; without this seed, totalBytes stays at 0
	// and the per-tick cap check would let the manager keep
	// provisioning new spares on top of the recovered set.
	m.totalBytes += ring.totalSegmentBytes()
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
	return nil
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
	defer m.worker.Done()
	defer close(m.done)
	timer := time.NewTimer(m.pollInterval)
	defer timer.Stop()
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

// serviceRing performs one round of spare provisioning and trim for
// a single ring. Cheap when the ring already has a spare and no
// trimmable sealed segments — the common steady-state case.
func (m *qwpSfSegmentManager) serviceRing(e qwpSfManagerRingEntry) {
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
				logger := qwpEffectiveLogger(m.logger.Load())
				if memoryMode {
					logger.Warn("qwp/sf: in-memory segment cap reached; spare provisioning "+
						"paused — producers block until in-flight segments are ACK'd and trimmed",
						"usedBytes", observedTotal, "maxBytes", m.maxTotalBytes,
						"segmentSize", m.segmentSizeBytes)
				} else {
					logger.Warn("qwp/sf: disk cap reached; spare provisioning "+
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

	// 3. Trim any segments that the ring says are fully acked. For
	//    memory-mode rings, "trim" is just close (the slice is GC'd) —
	//    no file to unlink.
	trim := e.ring.drainTrimmable()
	for _, s := range trim {
		path := s.segmentPath()
		sz := s.segmentSize()
		_ = s.close()
		if path != "" {
			_ = os.Remove(path)
		}
		m.mu.Lock()
		m.totalBytes -= sz
		m.mu.Unlock()
	}
}
