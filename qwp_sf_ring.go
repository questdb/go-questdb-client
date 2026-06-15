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
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
//     drainTrimmable on its own cadence.
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
	mu              sync.Mutex
	sealedSegments  []*qwpSfSegment
	closed          bool

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

// qwpSfOpenRing recovers a ring from segments already on disk in
// sfDir. Used at sender startup when the user's previous session
// left durable but not-yet-acked frames behind. Walks every *.sfa
// file in the directory, opens each via qwpSfOpenSegment, and
// arranges them by baseSeq:
//   - Highest-baseSeq segment becomes the active.
//   - All others become sealed segments awaiting ACK and trim.
//
// Returns nil if the directory is empty or contains no recognizable
// .sfa files. A bad-content file (qwpSfErrSegmentCorrupt: bad magic,
// unsupported version, short file, negative baseSeq) is skipped and
// logged — a stray or hand-damaged .sfa holds no recoverable frames
// and shouldn't take the whole sender down. Any other error from
// qwpSfOpenSegment is a syscall/I-O failure (open/stat/mmap returning
// EMFILE/ENFILE/ENOMEM/EACCES/EIO) on a file that may well be a valid
// segment, and is fatal: the caller's data integrity depends on every
// segment being readable, so recovery refuses to start rather than
// silently amputate the durable-but-unacked log.
func qwpSfOpenRing(sfDir string, maxBytesPerSegment int64) (*qwpSfSegmentRing, error) {
	if _, err := os.Stat(sfDir); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("qwp/sf: stat %s: %w", sfDir, err)
	}
	entries, err := os.ReadDir(sfDir)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: read %s: %w", sfDir, err)
	}
	var opened []*qwpSfSegment
	// Defense-in-depth: anything escaping the recovery body — a panic
	// from native munmap, an OOM from a future concurrent allocator,
	// the FSN-gap error below — must close every recovered fd+mmap
	// before propagating. After the success path opened is reassigned
	// to drop the active segment (transferred to the ring) and the
	// sealed segments (transferred to ring.sealedSegments), so this
	// cleanup is a no-op once we reach the bottom.
	defer func() {
		for _, s := range opened {
			_ = s.close()
		}
	}()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sfa") {
			continue
		}
		path := filepath.Join(sfDir, name)
		seg, err := qwpSfOpenSegment(path)
		if err != nil {
			if errors.Is(err, qwpSfErrSegmentCorrupt) {
				// Bad-content .sfa (bad magic/version/header/baseSeq):
				// a stray or hand-damaged file with no recoverable
				// frames behind it. Skip rather than fail the whole
				// recovery, but log so the skip is never silent.
				log.Printf("[WARN] qwp/sf: skipping corrupt segment during recovery: %v", err)
				continue
			}
			// A syscall/I-O failure (EMFILE/ENFILE/ENOMEM/EACCES/EIO)
			// on a file that may be a perfectly valid segment: the
			// system is under pressure, not the data corrupt. Refuse to
			// start rather than silently amputate the log. Skipping the
			// newest segment would drop its persisted-but-unacked frames
			// with no FSN gap to flag it; skipping the oldest would make
			// the engine seed ackedFsn from a surviving segment and
			// treat the skipped frames as already-acked, so neither case
			// ever replays. The deferred cleanup above closes whatever
			// was opened before this point.
			return nil, fmt.Errorf("qwp/sf: open segment %s during recovery: %w", path, err)
		}
		// Filter out empty leftovers — typically hot-spare segments
		// the manager pre-allocated for a prior session that never
		// got rotated into active. They carry the provisional
		// baseSeq=0 and frameCount=0, which would otherwise collide
		// with the real baseSeq=0 segment and trip the contiguity
		// check below. No data to recover; close and unlink.
		//
		// CAUTION: only unlink when the file is genuinely empty past
		// the header. If frame[0] failed CRC (bit-rot, partial-page-
		// write at crash, etc.) but valid frames followed, scanFrames
		// returns lastGood=HEADER_SIZE and frameCount=0 — yet
		// tornTailBytes is non-zero. Treating that as "empty hot
		// spare" would silently destroy every surviving frame.
		// Quarantine to <path>.corrupt instead so a postmortem can
		// recover what's left.
		if seg.segmentFrameCount() == 0 {
			torn := seg.segmentTornTailBytes()
			_ = seg.close()
			if torn > 0 {
				_ = os.Rename(path, path+".corrupt")
			} else {
				_ = os.Remove(path)
			}
			continue
		}
		opened = append(opened, seg)
	}
	if len(opened) == 0 {
		return nil, nil
	}
	sort.Slice(opened, func(i, j int) bool {
		// Unsigned comparison to match Java's Long.compareUnsigned —
		// future-proofs against baseSeq wrapping into negatives.
		return uint64(opened[i].segmentBaseSeq()) < uint64(opened[j].segmentBaseSeq())
	})
	// Sanity: the recovered segments must form a contiguous FSN
	// range. Detect gaps so a partial-write/manual-deletion mishap
	// doesn't silently produce duplicate or missing FSNs. The deferred
	// cleanup above handles closing on the error path.
	for i := 1; i < len(opened); i++ {
		prev := opened[i-1]
		curr := opened[i]
		expected := prev.segmentBaseSeq() + prev.segmentFrameCount()
		if curr.segmentBaseSeq() != expected {
			return nil, fmt.Errorf(
				"qwp/sf: FSN gap in recovered segments: prev baseSeq=%d frameCount=%d expected next baseSeq=%d but got %d",
				prev.segmentBaseSeq(), prev.segmentFrameCount(), expected, curr.segmentBaseSeq())
		}
	}
	// The newest segment becomes the active. Even if it's full, that's
	// OK: the next appendOrFsn returns BACKPRESSURE_NO_SPARE, the
	// manager installs a hot spare, the producer rotates.
	last := len(opened) - 1
	active := opened[last]
	sealed := opened[:last]
	r := qwpSfNewSegmentRing(active, maxBytesPerSegment)
	r.sealedSegments = sealed
	// Ownership transferred to the ring — clear opened so the deferred
	// cleanup leaves the recovered segments alone.
	opened = nil
	return r, nil
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
		// Mutate sealedSegments under the same mutex used by the
		// snapshot accessors — the I/O thread reads through that
		// path and must not see a half-resized slice.
		r.mu.Lock()
		r.sealedSegments = append(r.sealedSegments, active)
		r.mu.Unlock()
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
// Subsequent installHotSpare calls return qwpSfErrRingClosed; the
// active segment is closed last so any reader that captured a
// reference can finish reading before unmap.
func (r *qwpSfSegmentRing) segmentRingClose() error {
	r.mu.Lock()
	r.closed = true
	sealed := r.sealedSegments
	r.sealedSegments = nil
	r.mu.Unlock()

	var firstErr error
	if a := r.active.Swap(nil); a != nil {
		if err := a.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if hs := r.hotSpare.Swap(nil); hs != nil {
		if err := hs.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, s := range sealed {
		if s == nil {
			continue
		}
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// drainTrimmable removes and returns sealed segments whose every
// frame has been ACK'd (i.e. baseSeq + frameCount - 1 <= ackedFsn).
// Caller takes ownership and is responsible for close() + unlinking
// the file. Called by the segment manager off the hot path. Returns
// nil when nothing is eligible (avoids slice allocation in the
// steady state where most polls are no-ops).
func (r *qwpSfSegmentRing) drainTrimmable() []*qwpSfSegment {
	r.mu.Lock()
	defer r.mu.Unlock()
	acked := r.ackedFsn.Load()
	var out []*qwpSfSegment
	// Sealed segments are in baseSeq order, oldest first; once we hit
	// one that isn't fully acked, none of the later ones can be either.
	for len(r.sealedSegments) > 0 {
		s := r.sealedSegments[0]
		lastSeq := s.segmentBaseSeq() + s.segmentFrameCount() - 1
		if lastSeq > acked {
			break
		}
		out = append(out, s)
		r.sealedSegments = r.sealedSegments[1:]
	}
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
// concurrently trims via drainTrimmable.
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
