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

import "sync/atomic"

// Durable-ack frontier tracking — the "piggyback durable-ack" contract
// (sf-client.md §9.3 / §10). This mirrors the Java client's
// CursorWebSocketSendLoop.pendingDurable + durableTableWatermarks.
//
// Why a tracker is needed at all: a DURABLE_ACK frame carries NO
// cumulative wire sequence — only per-table (name, seqTxn) watermarks
// (see qwp_transport.go readAck). An OK frame carries both the cumulative
// wire sequence AND the per-table seqTxn snapshot at that point. So the
// client cannot map a DURABLE_ACK directly to a frame sequence: it must
// remember, for each OK'd wire sequence, the per-table seqTxns that OK
// reported, then advance the durable frontier to the highest wire seq
// whose every table has since been confirmed durable.
//
// In durable-ack mode an OK therefore does NOT trim: it parks a pending
// entry here. DURABLE_ACK frames raise the per-table durable watermarks
// and release the longest FIFO prefix of pending OKs now fully covered.
// Only that release advances engine.engineAcknowledge (trim + replay
// floor + AwaitAckedFsn). Non-durable mode is unaffected — OK trims
// directly and DURABLE_ACK frames are ignored.

// qwpAckTableEntry is one per-table watermark parsed from an OK or
// DURABLE_ACK frame trailer: the WAL sequencer transaction the server has
// assigned (OK) or durably uploaded (DURABLE_ACK) for that table. Both
// are cumulative — "everything <= seqTxn is done".
type qwpAckTableEntry struct {
	name   string
	seqTxn int64
}

// qwpSfPendingOk records an OK frame's wire sequence together with the
// per-table seqTxn watermarks the OK reported. It stays pending until
// DURABLE_ACK frames raise every one of its tables' durable watermark to
// >= the recorded seqTxn.
type qwpSfPendingOk struct {
	wireSeq int64
	tables  []qwpAckTableEntry
}

// isDurableUnder reports whether every table this OK touched is covered
// by the accumulated durable watermarks. An OK that touched no table
// (tables empty — e.g. an empty-WAL commit) is trivially durable, matching
// the Java PendingDurableEntry.tableCount==0 fast path.
func (p *qwpSfPendingOk) isDurableUnder(wm map[string]int64) bool {
	for i := range p.tables {
		if wm[p.tables[i].name] < p.tables[i].seqTxn {
			return false
		}
	}
	return true
}

// qwpSfDurableTracker accumulates per-table durable watermarks and the
// FIFO of not-yet-durable OKs.
//
// Concurrency: owned by the receiver goroutine for the life of a
// connection (onOk / onDurableAck) and reset by the reconnect driver
// between connections (run() → swapClient), when neither inner loop is
// running — so the maps/slice are single-writer and need no lock.
// pendingCount is an atomic snapshot of the queue depth so the sender
// goroutine can gate the durable-ack keepalive ping lock-free.
type qwpSfDurableTracker struct {
	pending    []qwpSfPendingOk
	head       int              // index of the first live entry in pending
	watermarks map[string]int64 // table name -> highest durable seqTxn seen
	// lastWireSeq is the highest OK wire sequence recorded so far; a
	// stale or duplicate OK (wireSeq <= lastWireSeq, e.g. the -1 sentinel
	// some servers emit) is not re-queued, keeping pending strictly
	// ascending so the drained frontier is always monotonic.
	lastWireSeq  int64
	pendingCount atomic.Int64
}

func newQwpSfDurableTracker() *qwpSfDurableTracker {
	return &qwpSfDurableTracker{
		watermarks:  make(map[string]int64),
		lastWireSeq: -1,
	}
}

// onOk parks the OK's per-table watermarks against its wire sequence,
// then drains any now-durable prefix. Draining on OK (not only on
// DURABLE_ACK) handles a DURABLE_ACK that raced ahead of its OK: the
// watermarks are already high enough, so the OK is immediately durable.
// Returns the highest wire sequence newly proven durable and whether the
// frontier advanced.
func (t *qwpSfDurableTracker) onOk(wireSeq int64, tables []qwpAckTableEntry) (int64, bool) {
	if wireSeq > t.lastWireSeq {
		t.pending = append(t.pending, qwpSfPendingOk{wireSeq: wireSeq, tables: tables})
		t.lastWireSeq = wireSeq
		t.pendingCount.Store(int64(len(t.pending) - t.head))
	}
	return t.drain()
}

// onDurableAck folds the DURABLE_ACK's per-table watermarks (monotonic
// max — a smaller seqTxn never rewinds an already-durable table) and
// drains the now-durable prefix.
func (t *qwpSfDurableTracker) onDurableAck(tables []qwpAckTableEntry) (int64, bool) {
	for i := range tables {
		if tables[i].seqTxn > t.watermarks[tables[i].name] {
			t.watermarks[tables[i].name] = tables[i].seqTxn
		}
	}
	return t.drain()
}

// drain pops the longest FIFO prefix of pending OKs whose every table is
// covered by the accumulated watermarks, returning the last popped wire
// sequence. FIFO order is load-bearing: an uncovered earlier OK blocks a
// later (even trivially-durable) one, so the returned frontier can never
// skip a frame that is not yet durable.
func (t *qwpSfDurableTracker) drain() (int64, bool) {
	frontier := int64(-1)
	advanced := false
	for t.head < len(t.pending) && t.pending[t.head].isDurableUnder(t.watermarks) {
		frontier = t.pending[t.head].wireSeq
		advanced = true
		t.pending[t.head].tables = nil // release the parsed entry for GC
		t.head++
	}
	if t.head == len(t.pending) && t.head > 0 {
		// Fully drained: rewind to the front of the backing array so it
		// is reused rather than growing unbounded across many drains.
		t.pending = t.pending[:0]
		t.head = 0
	}
	t.pendingCount.Store(int64(len(t.pending) - t.head))
	return frontier, advanced
}

// reset clears all state on (re)connect. The new connection restarts wire
// sequences from 0 and the server re-emits cumulative durable-acks for the
// replayed frames from scratch, so stale pending entries and watermarks
// must not leak across the boundary (mirrors Java clearDurableAckTracking).
func (t *qwpSfDurableTracker) reset() {
	t.pending = t.pending[:0]
	t.head = 0
	for k := range t.watermarks {
		delete(t.watermarks, k)
	}
	t.lastWireSeq = -1
	t.pendingCount.Store(0)
}

// pendingLen reports the number of not-yet-durable OKs. Lock-free; read by
// the sender goroutine to gate the durable-ack keepalive ping.
func (t *qwpSfDurableTracker) pendingLen() int64 {
	return t.pendingCount.Load()
}
