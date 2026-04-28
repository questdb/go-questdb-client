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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// qwpSfEngineDefaultAppendDeadline is the default backpressure
// deadline for appendBlocking. Mirrors Java's
// CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS = 30s.
const qwpSfEngineDefaultAppendDeadline = 30 * time.Second

// qwpSfEngineParkInterval is how long appendBlocking sleeps between
// retries while waiting for the manager to free space. Mirrors
// Java's 50µs LockSupport.parkNanos.
const qwpSfEngineParkInterval = 50 * time.Microsecond

// qwpSfErrBackpressureTimeout is returned by appendBlocking when
// the configured deadline expires before space frees up.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrBackpressureTimeout = errors.New(
	"qwp/sf: cursor ring backpressured — wire path is not draining (server slow / disconnected, or sf_max_total_bytes too small)")

// qwpSfCursorEngine is the cursor-engine facade that bundles a
// qwpSfSegmentRing with a qwpSfSegmentManager and exposes the
// user-facing API the wire-send loop calls into. Keeps SF append
// work on the user goroutine (where it belongs) and segment
// lifecycle work on the manager goroutine (where it belongs).
//
// Responsibilities:
//   - Owning the ring + manager lifecycle (open / close / startup
//     recovery).
//   - Providing a user-thread append path that handles backpressure.
//   - Exposing read accessors for the I/O thread:
//     enginePublishedFsn, engineActiveSegment, engineSealedSegments.
//   - Routing server ACKs to the ring for trim.
//
// Not in scope:
//   - Multi-producer support. Single producer (one user goroutine)
//     only.
type qwpSfCursorEngine struct {
	sfDir            string
	segmentSizeBytes int64

	manager     *qwpSfSegmentManager
	ownsManager bool
	slotLock    *qwpSfSlotLock
	ring        *qwpSfSegmentRing

	appendDeadline time.Duration

	// recoveredFromDisk is true when the constructor recovered an
	// existing on-disk slot rather than starting fresh. Diagnostic
	// accessor for tests and observability; cursor frames are
	// self-sufficient (every frame carries full schema + full
	// symbol-dict delta), so producer-side schema reset on recovery
	// is not required at the engine level.
	recoveredFromDisk bool

	// backpressureStalls counts how many times appendBlocking
	// observed qwpSfBackpressureNoSpare on its first try and had to
	// wait. One increment per blocking-call (not per spin).
	backpressureStalls atomic.Int64

	// closed is set by engineClose. atomic.Bool so tests / status
	// accessors can sample it from any goroutine.
	closed atomic.Bool
}

// qwpSfNewCursorEngine creates an engine with a private
// qwpSfSegmentManager (owned by the engine, closed alongside it).
// Pass sfDir = "" for memory-mode (no disk involvement); a non-empty
// sfDir places the engine in store-and-forward mode against that
// slot directory.
//
// Returns an error if the slot lock can't be acquired (another
// process is using the slot), or if recovery encounters an
// inconsistent on-disk state.
func qwpSfNewCursorEngine(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	mgr, err := qwpSfNewSegmentManager(segmentSizeBytes, qwpSfManagerDefaultPoll, maxTotalBytes)
	if err != nil {
		return nil, err
	}
	mgr.segmentManagerStart()
	e, err := qwpSfNewCursorEngineWithManager(sfDir, segmentSizeBytes, mgr, appendDeadline)
	if err != nil {
		mgr.segmentManagerClose()
		return nil, err
	}
	e.ownsManager = true
	return e, nil
}

// qwpSfNewCursorEngineWithManager creates an engine that shares the
// given segment manager (must already be started). The caller
// retains ownership of the manager; engineClose will not stop it.
func qwpSfNewCursorEngineWithManager(sfDir string, segmentSizeBytes int64, mgr *qwpSfSegmentManager, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	if appendDeadline <= 0 {
		appendDeadline = qwpSfEngineDefaultAppendDeadline
	}
	memoryMode := sfDir == ""
	var (
		lock              *qwpSfSlotLock
		ring              *qwpSfSegmentRing
		recoveredFromDisk bool
		err               error
	)
	if !memoryMode {
		// Acquire the slot lock BEFORE touching any *.sfa files.
		// Two engines pointed at the same slot would otherwise race
		// on recovery and create overlapping FSN ranges.
		lock, err = qwpSfAcquireSlotLock(sfDir)
		if err != nil {
			return nil, err
		}
	}
	cleanupLock := func() {
		if lock != nil {
			_ = lock.close()
		}
	}
	// Disk mode: try to recover any *.sfa files left behind by a
	// prior session before deciding to start fresh. Without this the
	// engine would create a new sf-initial.sfa at baseSeq=0,
	// overlapping FSNs already on disk and corrupting ACK
	// translation, trim, and replay.
	if !memoryMode {
		ring, err = qwpSfOpenRing(sfDir, segmentSizeBytes)
		if err != nil {
			cleanupLock()
			return nil, err
		}
		recoveredFromDisk = ring != nil
		if ring != nil {
			// Seed ackedFsn to one below the lowest segment's baseSeq.
			// We don't know what was actually acked before the prior
			// session crashed, but anything trimmed off the ring's
			// bottom must have been acked (trim is ack-driven).
			// Without this seed, ackedFsn stays at -1 and the I/O
			// loop's start-time positioning would walk to FSN 0 —
			// which may not exist on disk if earlier segments have
			// been trimmed, causing it to fall through to the active
			// segment's tip and skip the unacked sealed segments
			// entirely.
			first := ring.firstSealed()
			lowest := int64(0)
			if first != nil {
				lowest = first.segmentBaseSeq()
			} else if a := ring.getActiveSegment(); a != nil {
				lowest = a.segmentBaseSeq()
			}
			if lowest > 0 {
				ring.acknowledge(lowest - 1)
			}
		}
	}
	if ring == nil {
		var initial *qwpSfSegment
		var initialPath string
		if memoryMode {
			initial, err = qwpSfCreateInMemorySegment(0, segmentSizeBytes)
		} else {
			initialPath = filepath.Join(sfDir, "sf-initial.sfa")
			initial, err = qwpSfCreateSegment(initialPath, 0, segmentSizeBytes)
		}
		if err != nil {
			cleanupLock()
			return nil, err
		}
		ring = qwpSfNewSegmentRing(initial, segmentSizeBytes)
	}
	if err := mgr.segmentManagerRegister(ring, sfDir); err != nil {
		_ = ring.segmentRingClose()
		cleanupLock()
		return nil, err
	}
	e := &qwpSfCursorEngine{
		sfDir:             sfDir,
		segmentSizeBytes:  segmentSizeBytes,
		manager:           mgr,
		ownsManager:       false,
		slotLock:          lock,
		ring:              ring,
		appendDeadline:    appendDeadline,
		recoveredFromDisk: recoveredFromDisk,
	}
	return e, nil
}

// engineAcknowledge records a server ACK for cumulative FSN seq.
// Triggers background trim of any sealed segments whose every frame
// is now acknowledged. Idempotent and monotonic.
func (e *qwpSfCursorEngine) engineAcknowledge(seq int64) {
	e.ring.acknowledge(seq)
}

// engineAckedFsn returns the highest FSN safe to send.
func (e *qwpSfCursorEngine) engineAckedFsn() int64 {
	return e.ring.segmentRingAckedFsn()
}

// engineActiveSegment returns the current active mmap'd segment.
// I/O thread accessor.
func (e *qwpSfCursorEngine) engineActiveSegment() *qwpSfSegment {
	return e.ring.getActiveSegment()
}

// engineSfDir returns the slot directory ("" for memory-mode).
func (e *qwpSfCursorEngine) engineSfDir() string {
	return e.sfDir
}

// engineWasRecoveredFromDisk reports whether the engine opened
// against a pre-existing on-disk slot. Memory-mode engines and
// fresh-disk engines return false.
func (e *qwpSfCursorEngine) engineWasRecoveredFromDisk() bool {
	return e.recoveredFromDisk
}

// enginePublishedFsn returns the highest FSN whose frame is fully
// written and visible to consumers (the I/O thread). -1 when nothing
// has been appended yet.
func (e *qwpSfCursorEngine) enginePublishedFsn() int64 {
	return e.ring.segmentRingPublishedFsn()
}

// engineNextSealedAfter walks one step forward in the sealed list.
func (e *qwpSfCursorEngine) engineNextSealedAfter(current *qwpSfSegment) *qwpSfSegment {
	return e.ring.nextSealedAfter(current)
}

// engineFirstSealed returns the oldest sealed segment, or nil.
func (e *qwpSfCursorEngine) engineFirstSealed() *qwpSfSegment {
	return e.ring.firstSealed()
}

// engineFindSegmentContaining returns the segment whose published
// frame range covers fsn, or nil. Used by the reconnect path to
// position the I/O thread's cursor at the first unacked frame.
func (e *qwpSfCursorEngine) engineFindSegmentContaining(fsn int64) *qwpSfSegment {
	return e.ring.findSegmentContaining(fsn)
}

// engineAppendBlocking appends payload, blocking up to the
// configured deadline when the cursor ring is at its memory/disk cap
// and waiting for ACK-driven trim to free space. Returns the
// assigned FSN on success.
//
// Backpressure is surfaced two ways:
//   - engineTotalBackpressureStalls() counter — incremented once per
//     blocking-call that had to wait for the manager.
//   - The error from a deadline expiry distinguishes "wire path is
//     wedged" from a genuine over-large payload.
func (e *qwpSfCursorEngine) engineAppendBlocking(payload []byte) (int64, error) {
	fsn := e.ring.appendOrFsn(payload)
	if fsn >= 0 {
		return fsn, nil
	}
	if fsn == qwpSfPayloadTooLarge {
		return 0, qwpSfErrPayloadTooLarge
	}
	// First miss → record one stall (not one per spin) and start the
	// deadline clock.
	e.backpressureStalls.Add(1)
	deadline := time.Now().Add(e.appendDeadline)
	for {
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("%w (deadline %s)", qwpSfErrBackpressureTimeout, e.appendDeadline)
		}
		time.Sleep(qwpSfEngineParkInterval)
		fsn = e.ring.appendOrFsn(payload)
		if fsn >= 0 {
			return fsn, nil
		}
		if fsn == qwpSfPayloadTooLarge {
			return 0, qwpSfErrPayloadTooLarge
		}
	}
}

// engineTotalBackpressureStalls returns the cumulative number of
// times engineAppendBlocking had to wait for the manager to free
// space. One increment per blocking-call, not per spin-park.
func (e *qwpSfCursorEngine) engineTotalBackpressureStalls() int64 {
	return e.backpressureStalls.Load()
}

// engineClose tears down the engine. Drains residual on-disk
// segment files when the ring confirms every published FSN has been
// acked — at that moment the slot has no recoverable work and the
// files are pure noise that would mislead the next sender's
// recovery. Best-effort: logs (via returned error) and continues on
// failures, since we're already on the close path.
//
// Order: deregister the ring from the manager (so no new spares
// arrive), close the ring (closes its segments), close the manager
// if we own it, unlink residual files if fully drained, release the
// slot lock LAST (so the kernel-held flock outlives any other
// cleanup work).
func (e *qwpSfCursorEngine) engineClose() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Capture drain state BEFORE closing the ring — once the ring is
	// closed, its accessors aren't safe to read. The active segment
	// is never trimmed by drainTrimmable (only sealed segments are),
	// so when everything published has been acked we have to unlink
	// the residual .sfa files here.
	fullyDrained := e.sfDir != "" &&
		(e.ring.segmentRingPublishedFsn() < 0 ||
			e.ring.segmentRingAckedFsn() >= e.ring.segmentRingPublishedFsn())

	var firstErr error
	e.manager.segmentManagerDeregister(e.ring)
	if e.ownsManager {
		e.manager.segmentManagerClose()
	}
	if err := e.ring.segmentRingClose(); err != nil && firstErr == nil {
		firstErr = err
	}
	if fullyDrained {
		if err := qwpSfUnlinkAllSegmentFiles(e.sfDir); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.slotLock != nil {
		if err := e.slotLock.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// qwpSfUnlinkAllSegmentFiles unlinks every .sfa file under dir.
// Called only on clean shutdown when the ring confirms every
// published FSN has been acked. Best-effort: returns the first error
// encountered but continues iterating.
func qwpSfUnlinkAllSegmentFiles(dir string) error {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var firstErr error
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sfa") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if rmErr := os.Remove(path); rmErr != nil && firstErr == nil {
			firstErr = rmErr
		}
	}
	return firstErr
}
