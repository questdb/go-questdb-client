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
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// ErrBackpressureTimeout is the sentinel a producer call
// (At / AtNow / Flush / FlushAndGetSequence) wraps when the
// store-and-forward append deadline (WithSfAppendDeadline /
// sf_append_deadline_millis) expires before the cursor engine frees
// space. The wire path is not draining — the server is slow or
// disconnected, or sf_max_total_bytes is too small. Match it with
// errors.Is; the wrapped error carries the deadline and reconnect
// diagnostics in its message.
var ErrBackpressureTimeout = errors.New(
	"qwp/sf: cursor ring backpressured — wire path is not draining (server slow / disconnected, or sf_max_total_bytes too small)")

// qwpSfErrEngineClosed is returned by engineAppendBlocking when the
// engine is closed underneath an in-flight or backpressure-parked
// append. The canonical trigger is a SenderErrorHandler calling
// Close() while the producer is stalled in the backpressure spin on a
// wedged wire (a HALT stops the send loop draining, so ackedFsn never
// advances and the ring stays full). The producer gets this clean
// error instead of dereferencing a segment that engineClose's
// segmentRingClose has just nil'd + munmapped.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrEngineClosed = errors.New("qwp/sf: cursor engine closed")

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

	// watermark is the engine-owned mmap'd .ack-watermark file
	// (sf-client.md §5.4). nil in memory mode and when the file
	// could not be opened (recovery then falls back to the
	// segment-derived lowestBase-1 seed). Lifetime is tied to the
	// engine: opened in the constructor after the slot lock is
	// acquired, read once to refine the recovery seed, written
	// through by the segment manager on every tick where ackedFsn
	// advanced, closed in engineClose AFTER the manager (the sole
	// writer) is gone.
	watermark *qwpSfAckWatermark

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

	// reconnectStatus is the (optional) snapshot getter wired in by
	// the I/O send loop after it is constructed. When nil (e.g. tests
	// using the engine standalone) the backpressure-timeout error
	// falls back to the loop-agnostic "wire path is not draining"
	// wording. When non-nil, engineAppendBlocking checks it on
	// deadline expiry to distinguish "publishing but slow" from
	// "reconnecting" per spec §16, and includes attempt count +
	// outage elapsed in the latter case.
	reconnectStatus atomic.Pointer[func() (bool, int64, time.Time)]

	// terminalError is the (optional) snapshot getter wired in by the
	// I/O send loop after it is constructed, alongside reconnectStatus.
	// It returns the loop's latched terminal error — a *SenderError on a
	// HALT, a plain error on a transport-fatal condition — or nil while
	// the loop is healthy or merely reconnecting. engineAppendBlocking
	// polls it on every backpressure
	// spin iteration: a HALT stops the send loop draining the ring (ACK-
	// driven trim ceases), so a producer parked on a full ring would
	// otherwise wait out the whole appendDeadline and return the generic
	// backpressure-timeout error instead of the real terminal cause.
	// Polling it lets the parked producer fail fast with the latched
	// error. nil (the engine used standalone in tests) disables the
	// check; the spin then relies on the deadline / ctx / closed exits
	// alone.
	terminalError atomic.Pointer[func() error]

	// closed is set by engineClose. atomic.Bool so tests / status
	// accessors can sample it from any goroutine.
	closed atomic.Bool

	// appendMu serializes the producer's ring-append path against
	// engineClose's segment teardown. The producer's only entry into
	// appendOrFsn is engineAppendBlocking, which takes this lock around
	// each ring touch (initial try and every backpressure-spin retry)
	// and re-checks closed under it; engineClose holds it across the
	// manager + ring teardown. Together they guarantee no append is
	// dereferencing the active segment while segmentRingClose nil's and
	// munmaps it, and that every append after close observes closed and
	// bails with qwpSfErrEngineClosed. Without it a Close() from a
	// SenderErrorHandler (running on the dispatcher goroutine) while the
	// producer is parked in the backpressure spin tears the segment down
	// under the producer — a nil-pointer deref in memory mode, a SIGBUS
	// on the munmapped pages in SF mode. Off the per-row hot path:
	// appendOrFsn runs once per flush, not per row.
	appendMu sync.Mutex
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
	// Close the manager (joining its worker goroutine) on any failure
	// exit of the inner constructor — error return AND panic. The inner
	// constructor's own deferred guard releases the slot flock on the
	// same unwind; this guard covers the one resource it can't see — the
	// manager we own here. ok flips true only once the engine adopts it.
	ok := false
	defer func() {
		if !ok {
			mgr.segmentManagerClose()
		}
	}()
	e, err := qwpSfNewCursorEngineWithManager(sfDir, segmentSizeBytes, mgr, appendDeadline)
	if err != nil {
		return nil, err
	}
	e.ownsManager = true
	ok = true
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
		watermark         *qwpSfAckWatermark
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
	// Teardown for every failure exit — error return AND panic. ok
	// flips true only once the engine adopts these resources, so the
	// deferred guard runs cleanup on any unwind between the flock
	// acquisition above and the success return below. Skipping it on a
	// panic would strand the slot: the kernel-held flock survives the
	// process, wedging every future foreground open and orphan drainer
	// (which can no longer release a lock it never took), so the slot's
	// unacked data becomes unrecoverable.
	//
	// Release order mirrors engineClose and the Java reference: the
	// ring's segment mmaps, then the watermark's own mmap + fd, then the
	// slot flock LAST so it outlives every other cleanup. A failed
	// registration never reaches the manager's ring list, so the ring
	// needs no deregister here — and cleanup touches no manager state,
	// which keeps it safe to run on the unwind of a registration panic.
	ok := false
	cleanup := func() {
		if ring != nil {
			_ = ring.segmentRingClose()
		}
		if watermark != nil {
			_ = watermark.close()
		}
		if lock != nil {
			_ = lock.close()
		}
	}
	defer func() {
		if !ok {
			cleanup()
		}
	}()
	// Disk mode: try to recover any *.sfa files left behind by a
	// prior session before deciding to start fresh. Without this the
	// engine would create a new sf-initial.sfa at baseSeq=0,
	// overlapping FSNs already on disk and corrupting ACK
	// translation, trim, and replay.
	if !memoryMode {
		ring, err = qwpSfOpenRing(sfDir, segmentSizeBytes)
		if err != nil {
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
			baseSeed := lowest - 1
			// Refine the seed with the persisted ack watermark
			// (sf-client.md §5.4 / §6.5 / §18.1). It may carry
			// durable-acks the previous sender — or another client
			// whose orphan slot this drainer adopted — received for
			// frames inside the lowest surviving sealed segment.
			// Without honouring it those frames get re-replayed on a
			// fresh connection, producing row-level duplicates against
			// a still-alive server unless the table dedupes.
			//
			// max(watermark, lowestBase-1) absorbs both orderings of
			// the manager's "persist then trim" tick:
			//   - persist crashed before trim: segments still on disk
			//     are >= lowest, watermark is correct; max picks it.
			//   - trim ran before persist: those segments are gone so
			//     lowestBase is higher, watermark is stale; max picks
			//     lowestBase-1.
			//
			// open() returns nil on any setup failure so a missing /
			// unmappable file never takes the engine down — we just
			// fall back to the bare lowestBase-1 seed.
			watermark = qwpSfAckWatermarkOpen(sfDir)
			watermarkFsn := watermark.read() // nil-safe → INVALID
			candidate := baseSeed
			if watermarkFsn > candidate {
				candidate = watermarkFsn
			}
			// Reject a watermark past publishedFsn: a correctly
			// operating prior session cannot produce one, so an
			// excess value is corruption (torn write on a non-atomic
			// FS, bit-rot, manual edit). Trusting it would seed
			// ackedFsn = publishedFsn after the ring's own clamp and
			// position the cursor past every un-acked frame — silent
			// loss of the un-acked tail. Fall back to the
			// segment-derived seed so that tail still replays.
			seed := candidate
			if seed > ring.segmentRingPublishedFsn() {
				seed = baseSeed
			}
			if seed >= 0 {
				ring.acknowledge(seed)
			}
		}
	}
	if ring == nil {
		var initial *qwpSfSegment
		var initialPath string
		if memoryMode {
			initial, err = qwpSfCreateInMemorySegment(0, segmentSizeBytes)
		} else {
			// Fresh disk slot: any stale watermark refers to a
			// fully-drained lifecycle now gone. Unlink it before
			// opening so the new session's first read() correctly
			// reports INVALID (magic=0 on a freshly zero-filled
			// file) rather than honouring an FSN with no segments
			// behind it.
			qwpSfAckWatermarkRemoveOrphan(sfDir)
			watermark = qwpSfAckWatermarkOpen(sfDir)
			initialPath = filepath.Join(sfDir, "sf-initial.sfa")
			initial, err = qwpSfCreateSegment(initialPath, 0, segmentSizeBytes)
		}
		if err != nil {
			return nil, err
		}
		ring = qwpSfNewSegmentRing(initial, segmentSizeBytes)
	}
	if err := mgr.segmentManagerRegisterWithWatermark(ring, sfDir, watermark); err != nil {
		return nil, err
	}
	e := &qwpSfCursorEngine{
		sfDir:             sfDir,
		segmentSizeBytes:  segmentSizeBytes,
		manager:           mgr,
		ownsManager:       false,
		slotLock:          lock,
		ring:              ring,
		watermark:         watermark,
		appendDeadline:    appendDeadline,
		recoveredFromDisk: recoveredFromDisk,
	}
	ok = true
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

// engineAckNotify returns a channel closed the next time ackedFsn
// advances. Lets AwaitAckedFsn block until a server ACK lands instead
// of polling. See qwpSfSegmentRing.segmentRingAckNotify for the
// subscribe-then-sample ordering callers must follow.
func (e *qwpSfCursorEngine) engineAckNotify() <-chan struct{} {
	return e.ring.segmentRingAckNotify()
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

// engineMaxFrameBytes returns the largest frame payload a single
// segment can hold: the segment size minus the file header and the
// per-frame header. A payload above this can never be appended —
// appendOrFsn returns qwpSfPayloadTooLarge for it even against a
// freshly-rotated spare — so the producer uses this bound to (a)
// clamp its byte-size auto-flush trigger and (b) drop, rather than
// retain, an oversize batch at the flush boundary. Kept here so it
// tracks the segment header layout automatically and cannot drift
// from what tryAppend actually enforces.
func (e *qwpSfCursorEngine) engineMaxFrameBytes() int64 {
	return e.segmentSizeBytes - qwpSfHeaderSize - qwpSfFrameHeaderSize
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
// ctx is honoured during the backpressure spin: a cancelled or
// deadline-expired ctx returns ctx.Err() immediately, so callers
// passing a tighter deadline than e.appendDeadline get their
// deadline respected.
//
// A send-loop HALT latched while the producer is parked here (the wire
// failed terminally, or the reconnect budget ran out) short-circuits
// the spin: the loop has stopped draining the ring, so the deadline
// would only ever expire. engineAppendBlocking returns the latched
// terminal error directly via the engineSetTerminalErrorGetter hook, so
// the parked producer fails fast with the real cause instead of a
// generic backpressure timeout.
//
// Backpressure is surfaced two ways:
//   - engineTotalBackpressureStalls() counter — incremented once per
//     blocking-call that had to wait for the manager.
//   - The error from a deadline expiry distinguishes "wire path is
//     wedged" from a genuine over-large payload.
func (e *qwpSfCursorEngine) engineAppendBlocking(ctx context.Context, payload []byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	fsn, closed := e.tryAppendOrFsn(payload)
	if closed {
		return 0, qwpSfErrEngineClosed
	}
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
	timer := time.NewTimer(qwpSfEngineParkInterval)
	defer timer.Stop()
	for {
		// A send-loop HALT stops the
		// loop draining the ring: ACK-driven trim ceases, so the
		// backpressure can never clear and the deadline would only ever
		// expire. Surface the latched terminal error immediately instead
		// of spinning it out and masking the real cause behind a generic
		// backpressure timeout.
		if err := e.engineTerminalError(); err != nil {
			return 0, err
		}
		if time.Now().After(deadline) {
			return 0, e.formatBackpressureTimeout()
		}
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		timer.Reset(qwpSfEngineParkInterval)
		fsn, closed = e.tryAppendOrFsn(payload)
		if closed {
			return 0, qwpSfErrEngineClosed
		}
		if fsn >= 0 {
			return fsn, nil
		}
		if fsn == qwpSfPayloadTooLarge {
			return 0, qwpSfErrPayloadTooLarge
		}
	}
}

// tryAppendOrFsn runs one ring.appendOrFsn under appendMu, re-checking
// closed first so a concurrent engineClose can never tear the active
// segment down mid-append. Returns (fsn, false) with the appendOrFsn
// sentinel/result, or (0, true) when the engine has been closed — the
// signal engineAppendBlocking turns into qwpSfErrEngineClosed so a
// parked producer unwinds cleanly instead of dereferencing a nil'd /
// munmapped segment. Lock scope is exactly the ring touch; the spin's
// park happens with the lock released so engineClose is never delayed
// by more than one in-flight append.
func (e *qwpSfCursorEngine) tryAppendOrFsn(payload []byte) (fsn int64, closed bool) {
	e.appendMu.Lock()
	defer e.appendMu.Unlock()
	if e.closed.Load() {
		return 0, true
	}
	return e.ring.appendOrFsn(payload), false
}

// engineTotalBackpressureStalls returns the cumulative number of
// times engineAppendBlocking had to wait for the manager to free
// space. One increment per blocking-call, not per spin-park.
func (e *qwpSfCursorEngine) engineTotalBackpressureStalls() int64 {
	return e.backpressureStalls.Load()
}

// engineSetReconnectStatusGetter wires a snapshot accessor that
// reports whether the I/O loop is currently inside its
// reconnect-with-backoff phase. Called once by the QWP sender
// constructor right after the send loop is created. Pass nil to
// detach (used by tests that tear down the loop independently).
//
// The getter is invoked only on the deadline-expiry path of
// engineAppendBlocking, so the cost is paid only on a true
// backpressure timeout — never on the steady-state hot path.
func (e *qwpSfCursorEngine) engineSetReconnectStatusGetter(getter func() (bool, int64, time.Time)) {
	if getter == nil {
		e.reconnectStatus.Store(nil)
		return
	}
	e.reconnectStatus.Store(&getter)
}

// engineSetTerminalErrorGetter wires a snapshot accessor that returns
// the I/O send loop's latched terminal error (nil while healthy or
// merely reconnecting). Called once by the QWP sender constructor right
// after the send loop is created, alongside
// engineSetReconnectStatusGetter. Pass nil to detach (used by tests
// that tear down the loop independently).
//
// The getter is polled inside engineAppendBlocking's backpressure spin
// so a producer parked on a full ring unwinds the moment the send loop
// HALTs. A HALT stops the send loop, so ACK-driven trim ceases and the
// ring can never drain again; without this the producer would spin out
// the full append deadline and mask the real terminal error behind a
// generic backpressure timeout.
func (e *qwpSfCursorEngine) engineSetTerminalErrorGetter(getter func() error) {
	if getter == nil {
		e.terminalError.Store(nil)
		return
	}
	e.terminalError.Store(&getter)
}

// engineTerminalError returns the send loop's latched terminal error
// via the wired getter, or nil when no getter is installed or the loop
// is healthy. Consulted only on engineAppendBlocking's backpressure
// spin, so the getter cost is paid only while a producer is actually
// parked — never on the steady-state hot path.
func (e *qwpSfCursorEngine) engineTerminalError() error {
	if g := e.terminalError.Load(); g != nil {
		return (*g)()
	}
	return nil
}

// engineSetSendLoopWakeup wires the producer→send-loop doorbell:
// appendOrFsn invokes fn after every publish so an idle send loop
// reacts immediately instead of polling at parkInterval. Called once
// by qwpSfNewSendLoop before producing starts.
func (e *qwpSfCursorEngine) engineSetSendLoopWakeup(fn func()) {
	e.ring.setSendLoopWakeup(fn)
}

// formatBackpressureTimeout builds the LineSenderException-equivalent
// error returned by engineAppendBlocking when the deadline expires.
// Per spec §16 the message MUST distinguish "publishing but slow"
// from "reconnecting"; in the latter case it includes the per-outage
// attempt count and the wall-clock outage start.
func (e *qwpSfCursorEngine) formatBackpressureTimeout() error {
	if g := e.reconnectStatus.Load(); g != nil {
		if reconnecting, attempts, outageStart := (*g)(); reconnecting {
			return fmt.Errorf("%w (deadline %s, reconnecting: attempts=%d, outage-elapsed=%s, outage-start=%s)",
				ErrBackpressureTimeout,
				e.appendDeadline,
				attempts,
				time.Since(outageStart).Round(time.Millisecond),
				outageStart.Format(time.RFC3339Nano))
		}
	}
	return fmt.Errorf("%w (deadline %s, wire publishing but slow)", ErrBackpressureTimeout, e.appendDeadline)
}

// engineClose tears down the engine. Drains residual on-disk
// segment files when the ring confirms every published FSN has been
// acked — at that moment the slot has no recoverable work and the
// files are pure noise that would mislead the next sender's
// recovery. Best-effort: logs (via returned error) and continues on
// failures, since we're already on the close path.
//
// Order: deregister the ring from the manager (so no new spares
// arrive), close the manager if we own it, close the ring (closes
// its segments), close the ack-watermark mmap AFTER the manager (its
// sole writer) is gone, unlink residual files + the now-meaningless
// watermark if fully drained, release the slot lock LAST (so the
// kernel-held flock outlives any other cleanup work).
func (e *qwpSfCursorEngine) engineClose() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Serialize the manager + ring teardown against the producer's
	// append path. closed is now true, so any tryAppendOrFsn that
	// acquires appendMu after us bails before touching the ring;
	// acquiring it here drains any append currently in flight. Held
	// across segmentRingClose so the active segment is nil'd + munmapped
	// with no producer dereferencing it (C3: a SenderErrorHandler's
	// Close() racing a producer parked in engineAppendBlocking's
	// backpressure spin). appendMu is never held by the manager
	// goroutine, so joining it under the lock cannot deadlock.
	e.appendMu.Lock()
	defer e.appendMu.Unlock()
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
	// Close the watermark mmap/fd after the manager (the sole writer
	// through it) is gone but before the slot lock is released. With
	// ownsManager set, segmentManagerClose above has already joined
	// the worker goroutine, so no persistIfAdvanced can race this
	// close; the watermark's own mutex covers the residual
	// shared-manager (test-only) case.
	if e.watermark != nil {
		if err := e.watermark.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if fullyDrained {
		if err := qwpSfUnlinkAllSegmentFiles(e.sfDir); err != nil && firstErr == nil {
			firstErr = err
		}
		// A watermark with no segments behind it would only confuse
		// the next session's recovery seed — drop it, matching the
		// .sfa unlink and the fresh-slot removeOrphan above.
		qwpSfAckWatermarkRemoveOrphan(e.sfDir)
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
