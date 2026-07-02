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
	"sync/atomic"
)

// qwpDurableMaxTrackedTables caps the distinct table names the tracker will
// track from server durable frames — far above any realistic per-sender table
// count. A durable watermark for a table the client never wrote can never cover
// a pending batch, so refusing to intern beyond the cap neutralizes a
// misbehaving server that streams distinct unknown names to grow the maps
// without bound, while never dropping a watermark a real batch depends on.
const qwpDurableMaxTrackedTables = 1 << 16

// qwpDurableTracker is the durable-ack trim state machine
// (design/qwp-cursor-durability.md §5.4). Under request_durable_ack, an OK ACK
// no longer advances the trim/replay/await watermark; instead each OK-acked
// batch is stashed (enqueueOk) with the per-table (name, seqTxn) it committed,
// and released only once the server's cumulative STATUS_DURABLE_ACK frames
// (applyDurable) cover every one of its tables. drain then advances the engine
// watermark to the highest fully-durable wire sequence.
//
// All state except pendingLen is owned by the receiver goroutine; pendingLen is
// atomic so the sender goroutine can gate the keepalive ping on it.
type qwpDurableTracker struct {
	watermarks map[string]int64  // interned table name -> highest durable seqTxn
	interned   map[string]string // canonical name strings; reused across frames
	pending    []*qwpPendingDurable
	pool       []*qwpPendingDurable
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
		// A map miss yields 0, not Java's -1 sentinel, so test presence
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
// repeated table name is neither re-allocated nor able to alias the frame buffer
// (the watermarks key and the pending reference must outlive the frame).
func (t *qwpDurableTracker) intern(b []byte) string {
	if s, ok := t.interned[string(b)]; ok { // string(b) lookup does not allocate
		return s
	}
	s := string(b)
	t.interned[s] = s
	return s
}

// enqueueOk stashes an OK / NACK frame's wire sequence and per-table entries.
// tail is the frame's validated table trailer (empty for a NACK).
func (t *qwpDurableTracker) enqueueOk(wireSeq int64, tail []byte) {
	e := t.acquire()
	e.wireSeq = wireSeq
	e.tables = e.tables[:0]
	e.seqTxns = e.seqTxns[:0]
	qwpForEachAckTableEntry(tail, func(name []byte, seqTxn int64) {
		e.tables = append(e.tables, t.intern(name))
		e.seqTxns = append(e.seqTxns, seqTxn)
	})
	t.pending = append(t.pending, e)
	t.pendingLen.Store(int64(len(t.pending)))
}

// enqueueEmpty stashes a wire sequence with no tables — a DROP_AND_CONTINUE
// rejection, which carries no table trailer. It is trivially durable but still
// chains in FIFO order, so it advances the watermark only once every preceding
// OK batch is durable.
func (t *qwpDurableTracker) enqueueEmpty(wireSeq int64) {
	e := t.acquire()
	e.wireSeq = wireSeq
	e.tables = e.tables[:0]
	e.seqTxns = e.seqTxns[:0]
	t.pending = append(t.pending, e)
	t.pendingLen.Store(int64(len(t.pending)))
}

// applyDurable advances the per-table watermarks from a durable frame, taking the
// max so a reordered or older cumulative frame cannot move one backward.
func (t *qwpDurableTracker) applyDurable(tail []byte) {
	qwpForEachAckTableEntry(tail, func(name []byte, seqTxn int64) {
		key, ok := t.interned[string(name)] // string(b) lookup does not allocate
		if !ok {
			if len(t.interned) >= qwpDurableMaxTrackedTables {
				return
			}
			key = t.intern(name)
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
// invariant documented on durableOnOk (the Java per-frame ACK contract), not on
// this function.
func (t *qwpDurableTracker) drainUpTo(maxWireSeq int64) int64 {
	highest := int64(-1)
	i := 0
	for ; i < len(t.pending); i++ {
		e := t.pending[i]
		if e.wireSeq > maxWireSeq || !e.coveredBy(t.watermarks) {
			break
		}
		highest = e.wireSeq
		t.release(e)
	}
	if i > 0 {
		t.pending = append(t.pending[:0], t.pending[i:]...)
		t.pendingLen.Store(int64(len(t.pending)))
	}
	return highest
}

// reset clears the durable state on reconnect: a fresh connection re-emits
// cumulative durable watermarks from scratch. The intern cache is kept (the
// tables are unchanged).
func (t *qwpDurableTracker) reset() {
	clear(t.watermarks)
	for _, e := range t.pending {
		t.release(e)
	}
	t.pending = t.pending[:0]
	t.pendingLen.Store(0)
	t.lastSeq = -1
}

func (t *qwpDurableTracker) hasPending() bool { return t.pendingLen.Load() > 0 }

func (t *qwpDurableTracker) acquire() *qwpPendingDurable {
	if n := len(t.pool); n > 0 {
		e := t.pool[n-1]
		t.pool = t.pool[:n-1]
		return e
	}
	return &qwpPendingDurable{}
}

func (t *qwpDurableTracker) release(e *qwpPendingDurable) {
	t.pool = append(t.pool, e)
}
