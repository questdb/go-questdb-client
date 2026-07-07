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

import "testing"

func te(name string, seqTxn int64) qwpAckTableEntry {
	return qwpAckTableEntry{name: name, seqTxn: seqTxn}
}

func entries(es ...qwpAckTableEntry) []qwpAckTableEntry { return es }

// OK in durable mode parks a pending entry and does NOT advance the
// durable frontier — the frame is received/local-WAL, not yet durable.
func TestDurableTrackerOkDoesNotAdvance(t *testing.T) {
	tr := newQwpSfDurableTracker()
	frontier, adv := tr.onOk(0, entries(te("trades", 42)))
	if adv {
		t.Fatalf("OK must not advance the durable frontier; got frontier=%d advanced=true", frontier)
	}
	if tr.pendingLen() != 1 {
		t.Fatalf("pendingLen = %d, want 1", tr.pendingLen())
	}
}

// A DURABLE_ACK covering every table of the pending OK releases it.
func TestDurableTrackerFullCoverageAdvances(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(0, entries(te("trades", 10), te("orders", 20)))
	frontier, adv := tr.onDurableAck(entries(te("trades", 10), te("orders", 20)))
	if !adv || frontier != 0 {
		t.Fatalf("full coverage: got frontier=%d advanced=%v, want 0/true", frontier, adv)
	}
	if tr.pendingLen() != 0 {
		t.Fatalf("pendingLen = %d, want 0 after full drain", tr.pendingLen())
	}
}

// Partial coverage holds the frontier until every table is covered.
func TestDurableTrackerPartialCoverageWaits(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(0, entries(te("trades", 10), te("orders", 20)))

	if _, adv := tr.onDurableAck(entries(te("trades", 10))); adv {
		t.Fatal("partial coverage (only trades) must not advance")
	}
	if tr.pendingLen() != 1 {
		t.Fatalf("pendingLen = %d, want 1 while partially covered", tr.pendingLen())
	}
	frontier, adv := tr.onDurableAck(entries(te("orders", 20)))
	if !adv || frontier != 0 {
		t.Fatalf("second DA covering orders: got frontier=%d advanced=%v, want 0/true", frontier, adv)
	}
}

// A DURABLE_ACK with a smaller seqTxn never rewinds an already-durable
// table (monotonic max).
func TestDurableTrackerBackwardsWatermarkIgnored(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(0, entries(te("trades", 100)))
	tr.onDurableAck(entries(te("trades", 100))) // trades durable to 100
	tr.onOk(1, entries(te("trades", 150)))
	// A stale DA at 50 must not un-durable anything nor advance.
	if _, adv := tr.onDurableAck(entries(te("trades", 50))); adv {
		t.Fatal("backwards DA must not advance")
	}
	// The real DA at 150 releases OK@1.
	frontier, adv := tr.onDurableAck(entries(te("trades", 150)))
	if !adv || frontier != 1 {
		t.Fatalf("got frontier=%d advanced=%v, want 1/true", frontier, adv)
	}
}

// FIFO order is load-bearing: an uncovered earlier OK blocks a later,
// trivially-durable (empty) OK behind it.
func TestDurableTrackerEmptyOkChainsBehindPending(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(0, entries(te("trades", 10))) // needs trades>=10
	// An OK that touched no table would be durable on its own, but it is
	// behind an uncovered entry, so nothing may advance yet.
	if _, adv := tr.onOk(1, nil); adv {
		t.Fatal("empty OK@1 must not advance past uncovered OK@0")
	}
	if tr.pendingLen() != 2 {
		t.Fatalf("pendingLen = %d, want 2", tr.pendingLen())
	}
	// Cover OK@0; both @0 and the trivially-durable @1 drain in order.
	frontier, adv := tr.onDurableAck(entries(te("trades", 10)))
	if !adv || frontier != 1 {
		t.Fatalf("got frontier=%d advanced=%v, want 1/true (both drained)", frontier, adv)
	}
}

// A DURABLE_ACK that arrives before its OK is folded into the watermarks;
// the subsequent OK drains immediately.
func TestDurableTrackerDurableAckBeforeOk(t *testing.T) {
	tr := newQwpSfDurableTracker()
	if _, adv := tr.onDurableAck(entries(te("trades", 10))); adv {
		t.Fatal("DA with no pending OK must not advance")
	}
	frontier, adv := tr.onOk(0, entries(te("trades", 10)))
	if !adv || frontier != 0 {
		t.Fatalf("OK after covering DA: got frontier=%d advanced=%v, want 0/true", frontier, adv)
	}
}

// The frontier is monotonic across a chain of OKs drained together.
func TestDurableTrackerMonotonicChain(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(0, entries(te("t", 1)))
	tr.onOk(1, entries(te("t", 2)))
	tr.onOk(2, entries(te("t", 3)))
	// DA to seqTxn 2 releases OK@0 and OK@1 but not OK@2.
	frontier, adv := tr.onDurableAck(entries(te("t", 2)))
	if !adv || frontier != 1 {
		t.Fatalf("got frontier=%d advanced=%v, want 1/true", frontier, adv)
	}
	if tr.pendingLen() != 1 {
		t.Fatalf("pendingLen = %d, want 1 (OK@2 still pending)", tr.pendingLen())
	}
	frontier, adv = tr.onDurableAck(entries(te("t", 3)))
	if !adv || frontier != 2 {
		t.Fatalf("got frontier=%d advanced=%v, want 2/true", frontier, adv)
	}
}

// Stale / duplicate OK wire sequences are not re-queued (keeps the FIFO
// strictly ascending), but still trigger a drain attempt.
func TestDurableTrackerStaleOkIgnored(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(5, entries(te("t", 10)))
	// A stale OK (sentinel -1) and a duplicate (5) must not grow the queue.
	tr.onOk(-1, entries(te("t", 1)))
	tr.onOk(5, entries(te("t", 10)))
	if tr.pendingLen() != 1 {
		t.Fatalf("pendingLen = %d, want 1 (stale/dup ignored)", tr.pendingLen())
	}
	frontier, adv := tr.onDurableAck(entries(te("t", 10)))
	if !adv || frontier != 5 {
		t.Fatalf("got frontier=%d advanced=%v, want 5/true", frontier, adv)
	}
}

// reset drops all pending state so a reconnect starts clean.
func TestDurableTrackerReset(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(0, entries(te("trades", 10)))
	tr.onOk(1, entries(te("orders", 20)))
	tr.reset()
	if tr.pendingLen() != 0 {
		t.Fatalf("pendingLen = %d after reset, want 0", tr.pendingLen())
	}
	// Watermarks cleared: a fresh OK for the same table at the same seqTxn
	// must NOT be considered already-durable from a pre-reset DA.
	tr.onDurableAck(entries(te("trades", 10)))
	tr.reset()
	if _, adv := tr.onOk(0, entries(te("trades", 10))); adv {
		t.Fatal("after reset the pre-reset durable watermark must not linger")
	}
}

// Multi-table frame: a frontier only advances once ALL tables in the
// blocking OK are durable, even across interleaved DAs.
func TestDurableTrackerInterleavedMultiTable(t *testing.T) {
	tr := newQwpSfDurableTracker()
	tr.onOk(0, entries(te("a", 5)))
	tr.onOk(1, entries(te("a", 5), te("b", 7)))
	// DA covers a to 5 → OK@0 drains, OK@1 blocked on b.
	frontier, adv := tr.onDurableAck(entries(te("a", 5)))
	if !adv || frontier != 0 {
		t.Fatalf("got frontier=%d advanced=%v, want 0/true", frontier, adv)
	}
	// DA covers b to 7 → OK@1 drains.
	frontier, adv = tr.onDurableAck(entries(te("b", 7)))
	if !adv || frontier != 1 {
		t.Fatalf("got frontier=%d advanced=%v, want 1/true", frontier, adv)
	}
}
