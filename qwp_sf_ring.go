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
	"sync"
	"sync/atomic"
)

// qwpSfRing append/seal sentinels.
const (
	// qwpSfBackpressureNoSpare: append failed because no hot spare was
	// available to rotate into. The caller spins / parks; the segment
	// manager polls and provisions a spare.
	qwpSfBackpressureNoSpare int64 = -1
	// qwpSfPayloadTooLarge: append failed because the payload doesn't
	// fit in a fresh segment. Terminal for that frame.
	qwpSfPayloadTooLarge int64 = -2
	qwpSfRotationFailed  int64 = -3
)

// qwpSfErrPayloadTooLarge surfaces qwpSfPayloadTooLarge to the caller
// as an error value, avoiding magic-number comparisons in user code.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrPayloadTooLarge = errors.New("qwp/sf: payload too large for segment")

// qwpSfErrRingClosed is returned from installHotSpare when the ring
// has been closed since the manager started provisioning the spare.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrRingClosed = errors.New("qwp/sf: ring closed")

// qwpSfSegmentRing is a chain of qwpSfSegments presented to the user
// thread as one logical append-only log keyed by frame sequence
// number (FSN). Owns segment lifecycle: rotation when the active
// segment fills, ACK-driven trim of the oldest sealed segments.
//
// Built for the cursor engine's split-brain threading:
//   - Producer goroutine (single user goroutine): appendOrFsn,
//     installHotSpare consumer side, publishedFsn.
//   - I/O goroutine: publishedFsn (read-only), acknowledge (single
//     writer), nextSealedAfter, firstSealed, findSegmentContaining.
//   - Segment-manager goroutine: needsHotSpare, installHotSpare,
//     peekTrimmable + drainTrimBatch on its own cadence.
//
// Backpressure model: appendOrFsn returns qwpSfBackpressureNoSpare
// when the active is full and no spare is available. The caller (the
// engine) is expected to spin-park until the segment manager catches
// up, OR until acknowledge advances ackedFsn far enough that the
// manager can recycle a sealed segment.
type qwpSfSegmentRing struct {
	maxBytesPerSegment int64
	signalAtBytes      int64

	// active and hotSpare are accessed cross-thread. Producer writes;
	// I/O thread and manager read. atomic.Pointer mirrors the Java
	// volatile reference contract.
	active   atomic.Pointer[qwpSfSegment]
	hotSpare atomic.Pointer[qwpSfSegment]

	// ackedFsn and publishedFsn are atomic int64s shared with readers.
	// Both start at -1 (no ACK / no publish yet).
	ackedFsn     atomic.Int64
	publishedFsn atomic.Int64

	// ackNotify is a broadcast channel that acknowledge closes and
	// replaces each time it advances ackedFsn, so a blocked waiter
	// (AwaitAckedFsn) wakes immediately instead of polling. Lazily
	// created by the first subscriber and nil whenever nobody is
	// waiting, so an ACK with no waiter costs only the mutex. Guarded
	// by ackNotifyMu; lives off the producer hot path (acknowledge runs
	// on the I/O goroutine).
	ackNotifyMu sync.Mutex
	ackNotify   chan struct{}

	// nextSeq is the FSN that appendOrFsn will assign next.
	// Producer-only mutator (single-threaded), but the segment
	// manager goroutine reads it via nextSeqHint to seed a fresh
	// spare's baseSeq, so the field has to be atomic to avoid a
	// torn-read race under -race.
	nextSeq atomic.Int64

	// mu protects sealedSegments and serialises against close. It also
	// covers the producer's mutation when adding a sealed segment to
	// the list.
	mu             sync.Mutex
	sealedSegments []*qwpSfSegment
	closed         bool
	manifest       *qwpSfManifest
	rotationErr    atomic.Pointer[qwpSfRingError]

	// managerWakeup is invoked by the producer on rotation or
	// high-water-mark crossings to ask the manager to provision a
	// fresh spare immediately. Producer-thread-only field; set once
	// before producing starts.
	managerWakeup func()
	// sendLoopWakeup is invoked by the producer after every publish
	// so an idle send loop reacts immediately instead of polling.
	// Producer-thread-only field; set once before producing starts.
	// nil in unit tests that drive the ring without a send loop.
	sendLoopWakeup func()
	// wakeupRequestedForActive coalesces multiple high-water-mark
	// crossings into a single backup manager unpark per active segment.
	// Set when that backup wakeup fires; reset on rotation so each
	// freshly promoted active segment gets its own one-shot backup.
	wakeupRequestedForActive bool
}

type qwpSfRingError struct{ err error }

// qwpSfNewSegmentRing creates a ring with the given segment cap and an
// already-prepared initial active segment. The initial segment must
// be empty (just headers, frameCount == 0); typically supplied by the
// engine at startup.
func qwpSfNewSegmentRing(initialActive *qwpSfSegment, maxBytesPerSegment int64) *qwpSfSegmentRing {
	if initialActive == nil {
		panic("qwp/sf: initialActive must not be nil")
	}
	r := &qwpSfSegmentRing{
		maxBytesPerSegment: maxBytesPerSegment,
		signalAtBytes:      (maxBytesPerSegment >> 2) * 3,
	}
	r.active.Store(initialActive)
	// Initialize counters from the segment's recovery state. For a
	// fresh segment, frameCount == 0, so nextSeq == baseSeq and
	// publishedFsn == nextSeq - 1 == -1 (or baseSeq-1 for a
	// rebased-recovered segment).
	frameCount := initialActive.segmentFrameCount()
	r.nextSeq.Store(initialActive.segmentBaseSeq() + frameCount)
	if frameCount > 0 {
		r.publishedFsn.Store(r.nextSeq.Load() - 1)
	} else {
		r.publishedFsn.Store(-1)
	}
	r.ackedFsn.Store(-1)
	return r
}

// segmentRingAckedFsn returns the highest FSN that the server has
// ACK'd. Read by the segment manager to decide which sealed segments
// are safe to munmap + unlink.
func (r *qwpSfSegmentRing) segmentRingAckedFsn() int64 {
	return r.ackedFsn.Load()
}

// acknowledge advances the ACK cursor. seq is cumulative — the
// server has confirmed every FSN up to and including this value.
// Idempotent: a second call with the same or smaller value is a
// no-op.
//
// Defense-in-depth: clamp at publishedFsn so a malformed/poisoned
// server response with a bogus wireSeq cannot move ackedFsn past
// what the producer has actually written. Without the clamp, the
// segment manager could trim segments the I/O thread is still
// iterating and SEGV the process on the next mmap read.
func (r *qwpSfSegmentRing) acknowledge(seq int64) {
	pub := r.publishedFsn.Load()
	if seq > pub {
		seq = pub
	}
	for {
		cur := r.ackedFsn.Load()
		if seq <= cur {
			return
		}
		if r.ackedFsn.CompareAndSwap(cur, seq) {
			// ackedFsn moved — wake any AwaitAckedFsn waiters. Done after
			// the store so a woken waiter that re-reads ackedFsn observes
			// the new value (close happens-before the receive that wakes
			// it).
			r.notifyAckAdvance()
			return
		}
	}
}

// segmentRingAckNotify returns a channel that is closed the next time
// acknowledge advances ackedFsn. The contract for a no-lost-wakeup
// wait is: subscribe (call this) first, then read segmentRingAckedFsn,
// then block on the returned channel — acknowledge's atomic store of
// the new FSN precedes its close of this channel, so any advance that
// races the FSN read still wakes the waiter via the closed channel.
func (r *qwpSfSegmentRing) segmentRingAckNotify() <-chan struct{} {
	r.ackNotifyMu.Lock()
	defer r.ackNotifyMu.Unlock()
	if r.ackNotify == nil {
		r.ackNotify = make(chan struct{})
	}
	return r.ackNotify
}

// notifyAckAdvance wakes every current ack-notify subscriber and clears
// the channel so the next subscriber lazily installs a fresh one. A
// no-op (just the mutex) when nobody is waiting, which is the common
// case — only AwaitAckedFsn subscribes.
func (r *qwpSfSegmentRing) notifyAckAdvance() {
	r.ackNotifyMu.Lock()
	ch := r.ackNotify
	r.ackNotify = nil
	r.ackNotifyMu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// appendOrFsn is the single-producer append path. Reserves an FSN,
// writes the frame into the active segment, advances publishedFsn.
// Returns the assigned FSN on success, or one of the
// qwpSfBackpressureNoSpare / qwpSfPayloadTooLarge sentinels on
// failure.
//
// Rotation is automatic: when the active is full, the hot spare (if
// installed) is promoted, the previous active joins the sealed list,
// and the segment manager is signaled (implicitly by polling, plus
// explicitly via managerWakeup) to prepare the next spare.
func (r *qwpSfSegmentRing) appendOrFsn(payload []byte) int64 {
	active := r.active.Load()
	off, err := active.tryAppend(payload)
	if err != nil {
		if !errors.Is(err, qwpSfErrSegmentFull) {
			// Unexpected error from tryAppend (negative len, etc.).
			// Surface as PAYLOAD_TOO_LARGE — the only programmatic
			// failure mode the producer can act on.
			return qwpSfPayloadTooLarge
		}
		// Active is full. Try to rotate.
		spare := r.hotSpare.Load()
		if spare == nil {
			return qwpSfBackpressureNoSpare
		}
		// Pin the spare's baseSeq to whatever the active's nextSeq
		// actually is right now. This is the right moment because
		// (a) the active is full so its frameCount is stable, and
		// (b) the spare hasn't been appended to yet (rebaseSeq
		// enforces that). The segment manager's earlier guess at
		// baseSeq is irrelevant.
		actualBase := active.segmentBaseSeq() + active.segmentFrameCount()
		if rebaseErr := spare.rebaseSeq(actualBase); rebaseErr != nil {
			// Spare already has appended frames — programming error.
			// Surface as PAYLOAD_TOO_LARGE (the most actionable
			// failure code) so the user sees a clear error rather
			// than silent corruption.
			return qwpSfPayloadTooLarge
		}
		if syncErr := spare.syncHeader(); syncErr != nil {
			r.rotationErr.Store(&qwpSfRingError{err: syncErr})
			return qwpSfRotationFailed
		}
		// Snapshot the current head under the sealed-list mutex, but do not
		// hold it across the manifest fsync. The send loop takes this mutex
		// to walk sealed segments and must remain able to send while a
		// durability syscall is slow. A concurrent trim may advance the
		// manifest head after this snapshot; manifest.update's monotonic
		// clamp prevents this stale-low head from moving it backwards.
		headBase := active.segmentBaseSeq()
		r.mu.Lock()
		manifest := r.manifest
		if len(r.sealedSegments) > 0 {
			headBase = r.sealedSegments[0].segmentBaseSeq()
		}
		r.mu.Unlock()
		if manifest != nil {
			if updateErr := manifest.update(headBase, actualBase); updateErr != nil {
				r.rotationErr.Store(&qwpSfRingError{err: updateErr})
				return qwpSfRotationFailed
			}
		}

		// Publish the sealed-list mutation atomically to its readers after
		// the durable topology update succeeds.
		r.mu.Lock()
		r.sealedSegments = append(r.sealedSegments, active)
		r.mu.Unlock()
		r.rotationErr.Store(nil)
		r.active.Store(spare)
		r.hotSpare.Store(nil)
		// The freshly promoted active has no spare behind it yet, so
		// re-arm its one-shot backup wakeup: a later high-water-mark
		// crossing on this new segment must be able to nudge the manager
		// again if the next spare is slow to arrive. The unconditional
		// wakeup just below is the separate "make the next spare" signal.
		r.wakeupRequestedForActive = false
		// Fresh active just consumed the spare → ask the manager to
		// start making the next one immediately.
		if w := r.managerWakeup; w != nil {
			w()
		}
		off, err = spare.tryAppend(payload)
		if err != nil {
			// Doesn't fit even in a fresh segment — payload is
			// genuinely too big.
			return qwpSfPayloadTooLarge
		}
	} else if !r.wakeupRequestedForActive &&
		r.hotSpare.Load() == nil &&
		r.managerWakeup != nil &&
		active.publishedOffset() >= r.signalAtBytes {
		// Backup signal: we're past the high-water mark and still
		// don't have a spare. Fire once per active segment.
		r.wakeupRequestedForActive = true
		r.managerWakeup()
	}
	_ = off // offset is not used by callers; kept for parity with the Java return.
	fsn := r.nextSeq.Load()
	r.nextSeq.Store(fsn + 1)
	r.publishedFsn.Store(fsn)
	// Ring the send loop's doorbell after publishedFsn is visible so
	// a woken loop is guaranteed to observe this frame (the atomic
	// store happens-before the channel send). Non-blocking and
	// alloc-free; nil in send-loop-less unit tests.
	if w := r.sendLoopWakeup; w != nil {
		w()
	}
	return fsn
}

// segmentRingClose releases all segments and marks the ring closed.
// Subsequent installHotSpare calls return qwpSfErrRingClosed. Segment
// ordering here provides no reader synchronization — a reader holding an
// address() reference is unprotected regardless of order — so the caller must
// ensure the send loop has joined before closing, or pass leakMappings via
// segmentRingCloseInternal when it may still be reading (the send-loop-abandon
// path, where a goroutine wedged in an un-cancellable page fault may still be
// dereferencing a segment).
func (r *qwpSfSegmentRing) segmentRingClose() error {
	return r.segmentRingCloseInternal(false)
}

func (r *qwpSfSegmentRing) segmentRingCloseInternal(leakMappings bool) error {
	r.mu.Lock()
	r.closed = true
	sealed := r.sealedSegments
	r.sealedSegments = nil
	// Detach the manifest under the same mutex that publishes it, so the
	// manager's service pass either sees a live manifest it may still update or
	// sees none at all, and never reads the field while this close writes it.
	manifest := r.manifest
	r.manifest = nil
	r.mu.Unlock()

	var firstErr error
	if a := r.active.Swap(nil); a != nil {
		if err := a.closeInternal(leakMappings); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if hs := r.hotSpare.Swap(nil); hs != nil {
		if err := hs.closeInternal(leakMappings); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, s := range sealed {
		if s == nil {
			continue
		}
		if err := s.closeInternal(leakMappings); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if manifest != nil {
		if err := manifest.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ringManifest returns the ring's manifest, or nil once the ring is closed.
// The field is published by recovery/construction and cleared by
// segmentRingCloseInternal, both under r.mu; every cross-goroutine read goes
// through here.
func (r *qwpSfSegmentRing) ringManifest() *qwpSfManifest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.manifest
}

func (r *qwpSfSegmentRing) rotationError() error {
	if holder := r.rotationErr.Load(); holder != nil {
		return holder.err
	}
	return errors.New("qwp/sf: segment rotation failed")
}

func (r *qwpSfSegmentRing) peekTrimmable() []*qwpSfSegment {
	r.mu.Lock()
	defer r.mu.Unlock()
	acked := r.ackedFsn.Load()
	var out []*qwpSfSegment
	for _, s := range r.sealedSegments {
		if s.segmentBaseSeq()+s.segmentFrameCount()-1 > acked {
			break
		}
		out = append(out, s)
	}
	return out
}

func (r *qwpSfSegmentRing) headAfterTrim(trimCount int) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if trimCount < len(r.sealedSegments) {
		return r.sealedSegments[trimCount].segmentBaseSeq()
	}
	if active := r.active.Load(); active != nil {
		return active.segmentBaseSeq()
	}
	return -1
}

func (r *qwpSfSegmentRing) drainTrimBatch(count int) []*qwpSfSegment {
	r.mu.Lock()
	defer r.mu.Unlock()
	if count <= 0 || count > len(r.sealedSegments) {
		return nil
	}
	out := r.sealedSegments[:count]
	r.sealedSegments = r.sealedSegments[count:]
	return out
}

// getActiveSegment returns the active segment — exposed for the I/O
// thread's "send next batch" path. Returns nil after the ring has
// been closed.
func (r *qwpSfSegmentRing) getActiveSegment() *qwpSfSegment {
	return r.active.Load()
}

// getSealedSegments returns a direct view of sealed segments
// (oldest first). NOT thread-safe — use only from the producer
// goroutine, or alongside a lock that excludes concurrent rotation.
// Cross-thread readers (typically the I/O loop) should use
// snapshotSealedSegments instead.
func (r *qwpSfSegmentRing) getSealedSegments() []*qwpSfSegment {
	return r.sealedSegments
}

// snapshotSealedSegments copies references into the caller-supplied
// target slice (oldest first, packed left). Returns the number of
// references copied. If target is too small, copies the first
// len(target) references and returns -1 as a signal that the caller
// needs to grow the buffer and retry.
//
// Mutex-protected against rotation. Cost is one Lock/Unlock per
// call, paid by the I/O loop at most once per tick.
func (r *qwpSfSegmentRing) snapshotSealedSegments(target []*qwpSfSegment) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.sealedSegments)
	if n > len(target) {
		copy(target, r.sealedSegments[:len(target)])
		return -1
	}
	copy(target, r.sealedSegments)
	return n
}

// nextSealedAfter returns the sealed segment whose baseSeq
// immediately follows current.baseSeq, or nil if no such segment
// exists. Used by the I/O loop to walk forward through the sealed
// list one segment at a time without snapshotting the whole list —
// important when the producer outpaces the I/O thread.
//
// Identity match is intentionally avoided: we compare baseSeq so the
// loop is robust against current having been trimmed out from under
// us — we still return the next segment in baseSeq order rather than
// failing.
func (r *qwpSfSegmentRing) nextSealedAfter(current *qwpSfSegment) *qwpSfSegment {
	r.mu.Lock()
	defer r.mu.Unlock()
	currentBase := current.segmentBaseSeq()
	for _, s := range r.sealedSegments {
		if s.segmentBaseSeq() > currentBase {
			return s
		}
	}
	return nil
}

// firstSealed returns the oldest sealed segment, or nil if the
// sealed list is empty.
func (r *qwpSfSegmentRing) firstSealed() *qwpSfSegment {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sealedSegments) > 0 {
		return r.sealedSegments[0]
	}
	return nil
}

// sealedSegmentCount returns the number of sealed segments under the
// ring mutex. Thread-safe sibling of getSealedSegments for callers
// (e.g. tests) that observe the ring while the segment manager
// concurrently trims via drainTrimBatch.
func (r *qwpSfSegmentRing) sealedSegmentCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sealedSegments)
}

// findSegmentContaining returns the segment whose published frame
// range covers fsn, or nil if no segment currently holds it.
// Walks sealed first (oldest → newest) then the active.
func (r *qwpSfSegmentRing) findSegmentContaining(fsn int64) *qwpSfSegment {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.sealedSegments {
		base := s.segmentBaseSeq()
		if fsn >= base && fsn < base+s.segmentFrameCount() {
			return s
		}
	}
	a := r.active.Load()
	if a != nil {
		base := a.segmentBaseSeq()
		if fsn >= base && fsn < base+a.segmentFrameCount() {
			return a
		}
	}
	return nil
}

// installHotSpare parks a freshly-created spare. The producer
// consumes it on its next rotation. Returns an error if a spare is
// already installed (the manager should have polled needsHotSpare
// first; double-install is a programming error), or if the ring has
// been closed since the manager started provisioning the spare. The
// latter is a benign race — the manager's catch block closes the
// unused spare and unlinks its file.
func (r *qwpSfSegmentRing) installHotSpare(spare *qwpSfSegment) error {
	if spare == nil {
		return errors.New("qwp/sf: spare must not be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return qwpSfErrRingClosed
	}
	if r.hotSpare.Load() != nil {
		return errors.New("qwp/sf: hot spare already installed")
	}
	r.hotSpare.Store(spare)
	return nil
}

// totalSegmentBytes returns the sum of all segment sizes the ring
// currently owns: active + hot spare (if installed) + every sealed
// segment. Used by qwpSfSegmentManager to seed its totalBytes
// accounting at register time and reverse it at deregister time.
func (r *qwpSfSegmentRing) totalSegmentBytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total int64
	if a := r.active.Load(); a != nil {
		total += a.segmentSize()
	}
	if hs := r.hotSpare.Load(); hs != nil {
		total += hs.segmentSize()
	}
	for _, s := range r.sealedSegments {
		if s != nil {
			total += s.segmentSize()
		}
	}
	return total
}

// setManagerWakeup registers a callback the producer goroutine will
// invoke when a hot spare is needed — either right after a rotation
// has consumed the previous spare, or when the active segment
// crosses the 75% high-water mark while no spare is installed. Set
// once before producing starts; idempotent re-set is allowed but not
// thread-safe.
func (r *qwpSfSegmentRing) setManagerWakeup(wakeup func()) {
	r.managerWakeup = wakeup
}

// setSendLoopWakeup installs the callback appendOrFsn rings after
// every publish so the send loop drains promptly without polling.
// Set once before producing starts; not thread-safe.
func (r *qwpSfSegmentRing) setSendLoopWakeup(wakeup func()) {
	r.sendLoopWakeup = wakeup
}

// needsHotSpare reports whether the segment manager should provision
// a fresh spare for this ring.
func (r *qwpSfSegmentRing) needsHotSpare() bool {
	return r.hotSpare.Load() == nil
}

// nextSeqHint returns the next FSN appendOrFsn will assign — useful
// for the segment manager to know what baseSeq to stamp the next
// spare with (provisional; rebased at rotation).
func (r *qwpSfSegmentRing) nextSeqHint() int64 {
	return r.nextSeq.Load()
}

// segmentRingPublishedFsn returns the highest FSN whose frame is
// fully written and visible to consumers. Returns -1 when nothing
// has been appended yet.
func (r *qwpSfSegmentRing) segmentRingPublishedFsn() int64 {
	return r.publishedFsn.Load()
}
