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
	"math"
	"sync"
	"sync/atomic"
)

// qwpDurableMaxTrackedTables caps the distinct table names intern will hold, far
// above any realistic per-sender count. Enforced in intern (the shared choke
// point for enqueueOk and applyDurable), it stops a server streaming distinct
// unknown names from growing the maps without bound: applyDurable skips an
// over-cap watermark, enqueueOk rejects the frame so durableOnOk HALTs.
const qwpDurableMaxTrackedTables = 1 << 16

// qwpDurablePoolCap bounds the qwpPendingDurable freelist so the memory of a
// pending-queue peak (a long durable-ack stall) is returned to the GC instead
// of being held for the sender's lifetime.
const qwpDurablePoolCap = 256

// qwpDurableTracker is the durable-ack trim state machine
// (design/qwp-cursor-durability.md §5.4). Under request_durable_ack, an OK ACK
// no longer advances the trim/replay/await watermark; instead each OK-acked
// batch is stashed (enqueueOk) with the per-table (name, seqTxn) it committed,
// and released only once the server's cumulative STATUS_DURABLE_ACK frames
// (applyDurable) cover every one of its tables. drain then advances the engine
// watermark to the highest fully-durable wire sequence.
//
// All mutating entry points (seqGap, enqueueOk, applyDurable, drainUpTo, drain,
// reset) serialize on mu, so the sender goroutine can drive a drain while the
// receiver goroutine keeps applying acks. The receiver hot path takes one
// uncontended lock per ack — negligible next to the websocket read. pendingLen
// stays atomic so hasPending remains a lock-free gate for the keepalive ping.
type qwpDurableTracker struct {
	mu         sync.Mutex
	watermarks map[string]int64  // interned table name -> highest durable seqTxn
	interned   map[string]string // canonical name strings; reused across frames
	// pending is a head-cursor queue: live entries are pending[head:]. Pops
	// advance head in O(1); the slice is compacted only once head passes the
	// midpoint, so a drain is amortized O(1) per entry instead of O(n).
	pending []*qwpPendingDurable
	head    int
	pool    []*qwpPendingDurable
	// pendingLen mirrors len(pending) - head for the lock-free hasPending gate.
	pendingLen atomic.Int64
	// lastSeq is the highest OK/DROP wire sequence enqueued on the current
	// connection (-1 = none). Under the one-OK-ack-per-frame contract these
	// arrive densely, so a forward jump means the server coalesced or dropped
	// an OK ack and left frames untracked — seqGap catches it fail-closed.
	lastSeq int64
}

// qwpPendingDurable is one OK-acked batch awaiting durable confirmation. An entry
// with no tables (an empty batch or a dropped NACK) is trivially durable.
type qwpPendingDurable struct {
	wireSeq int64
	tables  []string
	seqTxns []int64
}

func (e *qwpPendingDurable) coveredBy(w map[string]int64) bool {
	for i, name := range e.tables {
		// A map miss yields 0, not a -1 sentinel, so test presence
		// explicitly: an absent (not-yet-durable) table is never covered.
		if v, ok := w[name]; !ok || v < e.seqTxns[i] {
			return false
		}
	}
	return true
}

func newQwpDurableTracker() *qwpDurableTracker {
	return &qwpDurableTracker{
		watermarks: make(map[string]int64),
		interned:   make(map[string]string),
		lastSeq:    -1,
	}
}

// seqGap reports whether seq skips past the next expected per-frame wire
// sequence — the signature of a server that coalesced or dropped an OK ack,
// leaving the skipped frames without a pending entry and thus untracked for
// durable trimming. It returns the expected sequence for diagnostics and, when
// there is no gap, records seq as the high-water mark. A duplicate or reordered
// seq (<= lastSeq) is left to the conservative drain (never over-advances).
func (t *qwpDurableTracker) seqGap(seq int64) (expected int64, gap bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	expected = t.lastSeq + 1
	if seq > expected {
		return expected, true
	}
	if seq > t.lastSeq {
		t.lastSeq = seq
	}
	return expected, false
}

// intern returns a stable string for b, allocating once per distinct table so a
// repeated name neither re-allocates nor aliases the frame buffer (the watermarks
// key and pending reference must outlive the frame). It reports false without
// growing the map past qwpDurableMaxTrackedTables distinct names. Caller holds mu.
func (t *qwpDurableTracker) intern(b []byte) (string, bool) {
	if s, ok := t.interned[string(b)]; ok { // string(b) lookup does not allocate
		return s, true
	}
	if len(t.interned) >= qwpDurableMaxTrackedTables {
		return "", false
	}
	s := string(b)
	t.interned[s] = s
	return s, true
}

// enqueueOk stashes an OK / NACK frame's wire sequence and per-table entries.
// tail is the frame's validated table trailer (empty for a NACK). It returns
// false without enqueuing when a trailer name overflows the intern cap, so the
// caller HALTs fail-closed instead of trimming on a truncated table set.
func (t *qwpDurableTracker) enqueueOk(wireSeq int64, tail []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.acquire()
	e.wireSeq = wireSeq
	e.tables = e.tables[:0]
	e.seqTxns = e.seqTxns[:0]
	overflow := false
	qwpForEachAckTableEntry(tail, func(name []byte, seqTxn int64) {
		s, ok := t.intern(name)
		if !ok {
			overflow = true
			return
		}
		e.tables = append(e.tables, s)
		e.seqTxns = append(e.seqTxns, seqTxn)
	})
	if overflow {
		t.release(e)
		return false
	}
	t.pending = append(t.pending, e)
	t.pendingLen.Store(int64(len(t.pending) - t.head))
	return true
}

// applyDurable advances the per-table watermarks from a durable frame, taking the
// max so a reordered or older cumulative frame cannot move one backward.
func (t *qwpDurableTracker) applyDurable(tail []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	qwpForEachAckTableEntry(tail, func(name []byte, seqTxn int64) {
		key, ok := t.intern(name)
		if !ok {
			return // over the cap: this watermark can never cover a real batch
		}
		// Presence-checked so a first watermark of 0 is inserted (0 > 0 is false)
		// and a reordered older frame never lowers an existing one.
		if cur, seen := t.watermarks[key]; !seen || seqTxn > cur {
			t.watermarks[key] = seqTxn
		}
	})
}

// drain pops every head entry fully covered by the watermarks and returns the
// highest popped wire sequence, or -1 if none popped. No send-progress ceiling —
// the send loop uses drainUpTo; this convenience is for coverage-only tests.
func (t *qwpDurableTracker) drain() int64 {
	return t.drainUpTo(math.MaxInt64)
}

// drainUpTo pops every head entry that is both fully covered by the watermarks
// AND fully sent (wireSeq <= maxWireSeq), returning the highest popped wire
// sequence or -1. The ceiling keeps the engine watermark off any frame the send
// goroutine may still be reading out of an mmap'd segment: a forged/early durable
// ack naming an in-flight frame is held back until the send completes and
// highestFullySent catches up — mirroring the non-durable applyAckWatermark
// clamp. Under the one-OK-ack-per-frame contract entries are enqueued in
// ascending wireSeq order, so the first entry above the ceiling ends the scan
// and the last popped wireSeq is the maximum popped. A contract-violating
// out-of-order enqueue could make the returned value trail the true max, but
// engineAcknowledge's monotonic clamp discards a low value, so it only ever
// under-advances — never past a not-yet-durable frame. This scan only guards its own
// queue: a negative or out-of-order seq in t.pending at worst stops it early and
// skips an advance. It does NOT harden against a server that gaps the OK-ack
// sequence — one coalescing ACKs (a cumulative seq skipping frames, with a
// trailer omitting the skipped tables) could let the drain advance past a
// not-yet-durable frame. Immunity to that rests on the one-OK-ack-per-frame
// invariant documented on durableOnOk (the server's per-frame ACK contract), not on
// this function.
func (t *qwpDurableTracker) drainUpTo(maxWireSeq int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	highest := int64(-1)
	for t.head < len(t.pending) {
		e := t.pending[t.head]
		if e.wireSeq > maxWireSeq || !e.coveredBy(t.watermarks) {
			break
		}
		highest = e.wireSeq
		t.release(e)
		// Clear the popped slot: the entry now belongs to the freelist and
		// reset only releases pending[head:], so a live alias here would
		// invite a double-release.
		t.pending[t.head] = nil
		t.head++
	}
	if highest >= 0 {
		if t.head == len(t.pending) {
			t.pending = t.pending[:0]
			t.head = 0
		} else if t.head > len(t.pending)/2 {
			n := copy(t.pending, t.pending[t.head:])
			clear(t.pending[n:])
			t.pending = t.pending[:n]
			t.head = 0
		}
		t.pendingLen.Store(int64(len(t.pending) - t.head))
	}
	return highest
}

// reset clears the durable state on reconnect: a fresh connection re-emits
// cumulative durable watermarks from scratch. The intern cache is kept (the
// tables are unchanged).
func (t *qwpDurableTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.watermarks)
	for _, e := range t.pending[t.head:] {
		t.release(e)
	}
	clear(t.pending)
	t.pending = t.pending[:0]
	t.head = 0
	t.pendingLen.Store(0)
	t.lastSeq = -1
}

func (t *qwpDurableTracker) hasPending() bool { return t.pendingLen.Load() > 0 }

// acquire pops a pooled entry or allocates one. Caller holds mu.
func (t *qwpDurableTracker) acquire() *qwpPendingDurable {
	if n := len(t.pool); n > 0 {
		e := t.pool[n-1]
		t.pool = t.pool[:n-1]
		return e
	}
	return &qwpPendingDurable{}
}

// release returns an entry to the freelist, dropping it once the pool is at
// capacity so a pending-queue peak does not pin its memory forever. Caller
// holds mu.
func (t *qwpDurableTracker) release(e *qwpPendingDurable) {
	if len(t.pool) >= qwpDurablePoolCap {
		return
	}
	t.pool = append(t.pool, e)
}
