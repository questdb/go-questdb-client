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
	"encoding/binary"
	"testing"
)

type tableEntry struct {
	name   string
	seqTxn int64
}

// durableTrailer builds the per-table trailer (tableCount + [nameLen name seqTxn]*)
// that qwpForEachAckTableEntry walks.
func durableTrailer(entries ...tableEntry) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(len(entries)))
	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.name)))
		buf = append(buf, e.name...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e.seqTxn))
	}
	return buf
}

// TestDurableTrackerDrainUpToCeiling pins the munmap-safety clamp: a covered
// entry whose wire sequence exceeds the fully-sent ceiling is held back (so a
// forged/early durable ack cannot trim a frame still being sent), then released
// once the ceiling rises — no stranding.
func TestDurableTrackerDrainUpToCeiling(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(0, durableTrailer(tableEntry{"trades", 0}))
	tr.enqueueOk(1, durableTrailer(tableEntry{"trades", 1}))
	// Both batches are durably covered...
	tr.applyDurable(durableTrailer(tableEntry{"trades", 1}))
	// ...but only frame 0 is fully sent: frame 1 must stay pending.
	if got := tr.drainUpTo(0); got != 0 {
		t.Fatalf("drainUpTo(0) = %d, want 0", got)
	}
	if !tr.hasPending() {
		t.Fatal("covered-but-unsent entry must remain pending under the ceiling")
	}
	// Ceiling rises to admit frame 1.
	if got := tr.drainUpTo(1); got != 1 {
		t.Fatalf("drainUpTo(1) = %d, want 1", got)
	}
	if tr.hasPending() {
		t.Fatal("entry should drain once the ceiling admits it")
	}
}

func TestDurableTrackerBasicCoverage(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(0, durableTrailer(tableEntry{"trades", 5}))
	if got := tr.drain(); got != -1 {
		t.Fatalf("drain before durable = %d, want -1", got)
	}
	if !tr.hasPending() {
		t.Fatal("expected pending after OK, before durable")
	}
	tr.applyDurable(durableTrailer(tableEntry{"trades", 5}))
	if got := tr.drain(); got != 0 {
		t.Fatalf("drain after covering durable = %d, want 0", got)
	}
	if tr.hasPending() {
		t.Fatal("expected no pending after full drain")
	}
}

func TestDurableTrackerPerTableMaxAndFIFO(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(0, durableTrailer(tableEntry{"t", 5}))
	tr.enqueueOk(1, durableTrailer(tableEntry{"t", 7}))
	// Durable up to 6: seq0 (t@5) covered, seq1 (t@7) not; FIFO stops at seq1.
	tr.applyDurable(durableTrailer(tableEntry{"t", 6}))
	if got := tr.drain(); got != 0 {
		t.Fatalf("drain = %d, want 0 (stop at uncovered seq1)", got)
	}
	// An older cumulative frame must not lower the watermark.
	tr.applyDurable(durableTrailer(tableEntry{"t", 3}))
	if got := tr.drain(); got != -1 {
		t.Fatalf("stale durable drained %d, want -1", got)
	}
	tr.applyDurable(durableTrailer(tableEntry{"t", 7}))
	if got := tr.drain(); got != 1 {
		t.Fatalf("drain = %d, want 1", got)
	}
}

func TestDurableTrackerAbsentTableNotCovered(t *testing.T) {
	tr := newQwpDurableTracker()
	// seqTxn 0 on an absent table must NOT be treated as covered (Go map miss
	// yields 0, not Java's -1 sentinel).
	tr.enqueueOk(0, durableTrailer(tableEntry{"never_durable", 0}))
	if got := tr.drain(); got != -1 {
		t.Fatalf("absent-table drain = %d, want -1", got)
	}
	tr.applyDurable(durableTrailer(tableEntry{"never_durable", 0}))
	if got := tr.drain(); got != 0 {
		t.Fatalf("after durable(0) drain = %d, want 0", got)
	}
}

func TestDurableTrackerMultiTableBatch(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(0, durableTrailer(tableEntry{"a", 3}, tableEntry{"b", 4}))
	tr.applyDurable(durableTrailer(tableEntry{"a", 3}))
	if got := tr.drain(); got != -1 {
		t.Fatalf("one-of-two durable drain = %d, want -1", got)
	}
	tr.applyDurable(durableTrailer(tableEntry{"b", 4}))
	if got := tr.drain(); got != 0 {
		t.Fatalf("both-durable drain = %d, want 0", got)
	}
}

func TestDurableTrackerEmptyBatchTriviallyDurable(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(7, durableTrailer()) // empty OK / dropped NACK
	if got := tr.drain(); got != 7 {
		t.Fatalf("empty batch drain = %d, want 7 (trivially durable)", got)
	}
}

func TestDurableTrackerDropChainsBehindOk(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(0, durableTrailer(tableEntry{"t", 5})) // OK, needs t@5
	tr.enqueueOk(1, durableTrailer())                   // DROP: empty, trivially durable
	// The empty drop must not advance past the still-unconfirmed OK ahead of it.
	if got := tr.drain(); got != -1 {
		t.Fatalf("drop drained ahead of uncovered OK = %d, want -1", got)
	}
	tr.applyDurable(durableTrailer(tableEntry{"t", 5}))
	if got := tr.drain(); got != 1 {
		t.Fatalf("drain = %d, want 1 (OK then drop both pop)", got)
	}
}

// TestDurableTrackerEnqueueEmptyChainsFIFO exercises enqueueEmpty directly — the
// DROP-in-durable path (a DROP_AND_CONTINUE rejection carries no table trailer, so
// the send loop stashes it via enqueueEmpty rather than enqueueOk). It is trivially
// durable on its own, but must still chain FIFO behind an unconfirmed OK ahead of
// it so the watermark never advances past not-yet-durable data.
func TestDurableTrackerEnqueueEmptyChainsFIFO(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(0, durableTrailer(tableEntry{"trades", 5})) // OK, needs trades@5
	tr.enqueueEmpty(1)                                        // DROP: no trailer, trivially durable
	// The empty drop must not advance past the still-unconfirmed OK ahead of it.
	if got := tr.drain(); got != -1 {
		t.Fatalf("empty drop drained ahead of uncovered OK = %d, want -1", got)
	}
	if !tr.hasPending() {
		t.Fatal("both entries must remain pending until the OK is covered")
	}
	// Once the OK is covered, the OK then the chained empty both pop.
	tr.applyDurable(durableTrailer(tableEntry{"trades", 5}))
	if got := tr.drain(); got != 1 {
		t.Fatalf("drain = %d, want 1 (OK then empty drop both pop)", got)
	}
	if tr.hasPending() {
		t.Fatal("queue must be empty after draining both")
	}
	// A standalone empty batch (nothing ahead of it) is trivially durable and
	// drains immediately.
	tr.enqueueEmpty(2)
	if got := tr.drain(); got != 2 {
		t.Fatalf("standalone empty drain = %d, want 2 (trivially durable)", got)
	}
}

func TestDurableTrackerDurableBeforeOk(t *testing.T) {
	tr := newQwpDurableTracker()
	// A durable frame can arrive before the OK for the same batch; the OK's
	// drain-on-enqueue must release it immediately.
	tr.applyDurable(durableTrailer(tableEntry{"t", 5}))
	tr.enqueueOk(3, durableTrailer(tableEntry{"t", 5}))
	if got := tr.drain(); got != 3 {
		t.Fatalf("durable-before-ok drain = %d, want 3", got)
	}
}

func TestDurableTrackerResetOnReconnect(t *testing.T) {
	tr := newQwpDurableTracker()
	tr.enqueueOk(0, durableTrailer(tableEntry{"t", 5}))
	tr.applyDurable(durableTrailer(tableEntry{"t", 5}))
	tr.reset()
	if tr.hasPending() {
		t.Fatal("pending survived reset")
	}
	// A durable frame from the old connection must not satisfy a re-sent batch.
	tr.enqueueOk(0, durableTrailer(tableEntry{"t", 5}))
	if got := tr.drain(); got != -1 {
		t.Fatalf("stale watermark survived reset: drain = %d, want -1", got)
	}
}

func TestDurableTrackerSteadyStateZeroAllocs(t *testing.T) {
	tr := newQwpDurableTracker()
	ok := durableTrailer(tableEntry{"trades", 1})
	dur := durableTrailer(tableEntry{"trades", 1})
	var seq int64
	// Warm up the pool, intern cache, and map so steady state is allocation-free.
	for i := 0; i < 100; i++ {
		tr.enqueueOk(seq, ok)
		tr.applyDurable(dur)
		tr.drain()
		seq++
	}
	got := testing.AllocsPerRun(1000, func() {
		tr.enqueueOk(seq, ok)
		tr.applyDurable(dur)
		tr.drain()
		seq++
	})
	if got != 0 {
		t.Errorf("steady-state enqueue/apply/drain allocs = %v, want 0", got)
	}
}
