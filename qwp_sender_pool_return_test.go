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
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Settings for pools whose senders fault on one slot directory while the
// server stays up: small segments so a rotation comes every few leases.
const qwpReturnTestSlotFaultSettings = ";sf_max_segment_bytes=8192;sf_max_total_bytes=131072" +
	";sf_append_deadline_millis=300;close_flush_timeout_millis=300;auto_flush=off;"

// Settings for pools that run through an outage: a 32 KiB ring fills after a
// dozen leases, and a full ring fails a return after 150 ms.
const qwpReturnTestOutageSettings = ";sf_max_segment_bytes=8192;sf_max_total_bytes=32768" +
	";sf_append_deadline_millis=150;close_flush_timeout_millis=150;auto_flush=off;"

const qwpReturnTestPad = 100

// newQwpReturnTestPool builds an SF pool on a fresh sf_dir and closes it when
// the test ends. It returns the pool and its sf_dir.
func newQwpReturnTestPool(t *testing.T, addr, settings string, minSize, maxSize int, acquireTimeout time.Duration, logger *slog.Logger) (*qwpSenderPool, string) {
	t.Helper()
	dir := t.TempDir()
	conf := "ws::addr=" + addr + ";sf_dir=" + dir + settings
	p, err := newQwpSenderPool(context.Background(), conf, minSize, maxSize, acquireTimeout, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = p.close(ctx)
	})
	return p, dir
}

// qwpReturnTestSlotDir is the directory prefix of a pooled sender's SF slot,
// for scoping an injected fault to it.
func qwpReturnTestSlotDir(sfDir string, index int) string {
	return filepath.Join(sfDir, qwpSfDefaultSenderId+"-"+strconv.Itoa(index)) + string(os.PathSeparator)
}

// qwpReturnTestWriteRows writes 20 rows to table t: symbol sym, the marker in
// m, a 100-byte pad and an int64.
func qwpReturnTestWriteRows(t *testing.T, s LineSender, symbol, marker string) {
	t.Helper()
	pad := strings.Repeat("x", qwpReturnTestPad)
	for r := 0; r < 20; r++ {
		require.NoError(t, s.Table("t").Symbol("sym", symbol).StringColumn("m", marker).
			StringColumn("pad", pad).Int64Column("v", int64(r)).At(context.Background(), time.Unix(1, int64(r))))
	}
}

// qwpReturnTestLease borrows a sender, writes 20 rows, optionally flushes, and
// returns the sender. ok reports whether Flush and Close both returned nil.
func qwpReturnTestLease(t *testing.T, p *qwpSenderPool, symbol, marker string, flush bool) (slot *qwpSenderSlot, ok bool, err error) {
	t.Helper()
	ctx := context.Background()
	s, err := p.borrow(ctx)
	require.NoError(t, err)
	slot = s.(*qwpPooledSender).slot
	qwpReturnTestWriteRows(t, s, symbol, marker)
	var flushErr error
	if flush {
		flushErr = s.Flush(ctx)
	}
	closeErr := s.Close(ctx)
	err = errors.Join(flushErr, closeErr)
	return slot, err == nil, err
}

// qwpReturnTestFillRing commits 1 KiB rows on s and flushes after each one
// until a flush fails with backpressure, so the sender's ring is full and the
// last row is still pending.
func qwpReturnTestFillRing(t *testing.T, s LineSender) {
	t.Helper()
	ctx := context.Background()
	filler := strings.Repeat("x", 1024)
	for i := 0; i < 1000; i++ {
		require.NoError(t, s.Table("t").StringColumn("m", filler).At(ctx, time.Unix(0, int64(i+1))))
		if err := s.Flush(ctx); err != nil {
			require.ErrorIs(t, err, ErrBackpressureTimeout)
			return
		}
	}
	t.Fatal("the ring never filled")
}

func qwpReturnTestInAvailable(p *qwpSenderPool, slot *qwpSenderSlot) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Contains(p.available, slot)
}

type qwpReturnTestFault struct {
	name   string
	inject func(t *testing.T, slotDir string) (restore func())
}

// qwpReturnTestFaults are the two slot-local storage faults the pool must
// route serial leases around: a failing fsync when a segment is sealed, and
// a failing .symbol-dict append.
var qwpReturnTestFaults = []qwpReturnTestFault{
	{"rotation fsync", qwpReturnTestInjectRotationFsyncFault},
	{"dictionary append", qwpReturnTestInjectDictAppendFault},
}

func qwpReturnTestInjectRotationFsyncFault(t *testing.T, slotDir string) func() {
	t.Helper()
	orig := qwpSfSyncSealedSegment.load()
	qwpSfSyncSealedSegment.store(func(s *qwpSfSegment) error {
		if strings.HasPrefix(s.segmentPath(), slotDir) {
			return errors.New("injected EIO on sealed-segment fsync")
		}
		return orig(s)
	})
	restored := false
	restore := func() {
		if !restored {
			restored = true
			qwpSfSyncSealedSegment.store(orig)
		}
	}
	t.Cleanup(restore)
	return restore
}

func qwpReturnTestInjectDictAppendFault(t *testing.T, slotDir string) func() {
	t.Helper()
	orig := qwpSfSymbolDictWriteAt.load()
	qwpSfSymbolDictWriteAt.store(func(f *os.File, b []byte, off int64) (int, error) {
		if strings.HasPrefix(f.Name(), slotDir) {
			return 0, errors.New("injected EIO on dictionary append")
		}
		return orig(f, b, off)
	})
	restored := false
	restore := func() {
		if !restored {
			restored = true
			qwpSfSymbolDictWriteAt.store(orig)
		}
	}
	t.Cleanup(restore)
	return restore
}

// qwpReturnTestFaultySlot borrows and returns one sender to learn which slot
// the next serial lease lands on, and injects fault on that slot's directory.
func qwpReturnTestFaultySlot(t *testing.T, p *qwpSenderPool, sfDir string, fault qwpReturnTestFault) (index int, restore func()) {
	t.Helper()
	slot, ok, err := qwpReturnTestLease(t, p, "warmup", "", true)
	require.True(t, ok, "warm-up lease: %v", err)
	return slot.slotIndex, fault.inject(t, qwpReturnTestSlotDir(sfDir, slot.slotIndex))
}

// A storage fault confined to one slot fails that slot's next return. Serial
// leases must then move to another sender: the pool keeps the faulty sender,
// because its ring holds earlier borrowers' rows, but lends it last.
func TestQwpSfPoolSlotFaultMovesSerialLeasesOff(t *testing.T) {
	for _, fault := range qwpReturnTestFaults {
		t.Run(fault.name, func(t *testing.T) {
			srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
			t.Cleanup(srv.Close)
			p, sfDir := newQwpReturnTestPool(t, strings.TrimPrefix(srv.URL, "http://"), qwpReturnTestSlotFaultSettings, 2, 4, 2*time.Second, nil)
			faultyIndex, restore := qwpReturnTestFaultySlot(t, p, sfDir, fault)

			var ok, failed int
			var seq []int
			for i := 0; i < 40; i++ {
				slot, leaseOK, err := qwpReturnTestLease(t, p, "burst_"+strconv.Itoa(i), "", true)
				seq = append(seq, slot.slotIndex)
				if leaseOK {
					ok++
					if failed > 0 {
						require.NotEqual(t, faultyIndex, slot.slotIndex, "lease %d went back to the faulty slot after its failure", i)
					}
					continue
				}
				failed++
				require.Equal(t, faultyIndex, slot.slotIndex, "a lease on a healthy slot failed: %v", err)
				require.ErrorIs(t, err, ErrSfDurability)
				// The faulty sender's ring can hold earlier borrowers' rows, so
				// the pool keeps it. giveBack updates the list before Close
				// returns, so this check does not depend on timing.
				require.True(t, qwpReturnTestInAvailable(p, slot), "the faulty sender left the pool after its failed return")
			}
			t.Logf("with the fault: ok=%d failed=%d slots=%v", ok, failed, seq)
			require.Equal(t, 1, failed, "slots=%v", seq)

			restore()
			for i := 0; i < 10; i++ {
				_, leaseOK, err := qwpReturnTestLease(t, p, "after_"+strconv.Itoa(i), "", true)
				require.True(t, leaseOK, "lease %d after the fault was removed: %v", i, err)
			}
		})
	}
}

// The same fault with leases 700 ms apart and min=1. A pool that closed the
// faulty sender would finish its cleanup within the spacing and reopen the
// faulty directory for the next growth; a pool that lent the degraded sender
// before growing would keep using it.
func TestQwpSfPoolSlotFaultSpacedLeasesStayOff(t *testing.T) {
	for _, fault := range qwpReturnTestFaults {
		t.Run(fault.name, func(t *testing.T) {
			srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
			t.Cleanup(srv.Close)
			p, sfDir := newQwpReturnTestPool(t, strings.TrimPrefix(srv.URL, "http://"), qwpReturnTestSlotFaultSettings, 1, 4, 2*time.Second, nil)
			faultyIndex, _ := qwpReturnTestFaultySlot(t, p, sfDir, fault)

			var ok, failed int
			var seq []int
			for i := 0; i < 16; i++ {
				if i > 0 {
					time.Sleep(700 * time.Millisecond)
				}
				slot, leaseOK, err := qwpReturnTestLease(t, p, "spaced_"+strconv.Itoa(i), "", true)
				seq = append(seq, slot.slotIndex)
				if leaseOK {
					ok++
					if failed > 0 {
						require.NotEqual(t, faultyIndex, slot.slotIndex, "lease %d went back to the faulty slot after its failure", i)
					}
					continue
				}
				failed++
				require.Equal(t, faultyIndex, slot.slotIndex, "a lease on a healthy slot failed: %v", err)
			}
			t.Logf("with the fault: ok=%d failed=%d slots=%v", ok, failed, seq)
			require.Equal(t, 1, failed, "slots=%v", seq)
		})
	}
}

func qwpReturnTestAnyUnacked(p *qwpSenderPool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, slot := range p.all {
		if slotHasUnackedRows(slot.delegate) {
			return true
		}
	}
	return false
}

// During an outage every sender's ring fills and its returns fail with
// backpressure. The pool must fill every sender's buffer, growing to max,
// instead of dropping leases while other buffers sit empty, and the rows it
// accepted must arrive once the server is back.
func TestQwpSfPoolOutageFillsEverySendersBuffer(t *testing.T) {
	for _, size := range []struct{ min, max int }{{2, 2}, {1, 4}} {
		t.Run(fmt.Sprintf("min=%d,max=%d", size.min, size.max), func(t *testing.T) {
			const leases = 60
			markers := make([]string, leases)
			for i := range markers {
				markers[i] = fmt.Sprintf("LEASE_%04d_", i)
			}
			srv := newQwpRestartableAckServer(t, markers...)
			p, _ := newQwpReturnTestPool(t, srv.addr, qwpReturnTestOutageSettings, size.min, size.max, 2*time.Second, nil)
			srv.stop()

			kept := make([]bool, leases)
			keptBySlot := map[int]int{}
			for i, marker := range markers {
				slot, ok, err := qwpReturnTestLease(t, p, "sym_"+strconv.Itoa(i), marker, false)
				if !ok {
					require.ErrorIs(t, err, ErrBackpressureTimeout)
					continue
				}
				kept[i] = true
				keptBySlot[slot.slotIndex]++
			}
			keptCount := 0
			lo, hi := leases, 0
			for _, n := range keptBySlot {
				keptCount += n
				lo, hi = min(lo, n), max(hi, n)
			}
			t.Logf("kept %d of %d leases; kept per slot: %v", keptCount, leases, keptBySlot)

			// Each lease writes at least 20 × (pad + marker) bytes, so one
			// sender's 32 KiB ring holds at most this many leases.
			maxPerSender := 32768 / (20 * (qwpReturnTestPad + len(markers[0])))
			require.Len(t, keptBySlot, size.max, "the kept leases must fill every sender's buffer, not one")
			require.LessOrEqual(t, hi-lo, 1, "every sender's ring holds about the same number of leases")
			require.LessOrEqual(t, hi, maxPerSender)

			srv.start(t)
			require.Eventually(t, func() bool {
				for i, m := range markers {
					if kept[i] && srv.count(m) == 0 {
						return false
					}
				}
				return true
			}, 20*time.Second, 50*time.Millisecond, "every kept lease's rows must arrive after the restart")
			for i, m := range markers {
				if !kept[i] {
					require.Zero(t, srv.count(m), "lease %d was reported dropped, but its rows arrived", i)
				}
			}

			// Once acknowledgements catch up, leases queue their rows again. The
			// pool is at max with every sender degraded, so it lends one as a
			// last resort, and the lease that queues rows clears its mark.
			require.Eventually(t, func() bool { return !qwpReturnTestAnyUnacked(p) }, qwpTestWaitTimeout, 10*time.Millisecond)
			for i := 0; i < size.max; i++ {
				slot, ok, err := qwpReturnTestLease(t, p, "recovered_"+strconv.Itoa(i), "", false)
				require.True(t, ok, "lease %d after the outage: %v", i, err)
				require.False(t, qwpReturnTestDegraded(p, slot), "a lease that queued rows must clear its sender's mark")
			}
		})
	}
}

// A return that drops rows logs one Warn record, so the drop is visible even
// when the caller ignores Close's error.
func TestQwpSfPoolDroppedReturnLogsWarn(t *testing.T) {
	capture := &recordCapturingHandler{}
	srv := newQwpRestartableAckServer(t)
	p, _ := newQwpReturnTestPool(t, srv.addr, qwpReturnTestOutageSettings, 1, 1, 2*time.Second, slog.New(capture))
	srv.stop()

	ctx := context.Background()
	s, err := p.borrow(ctx)
	require.NoError(t, err)
	slot := s.(*qwpPooledSender).slot
	qwpReturnTestFillRing(t, s)
	require.NoError(t, s.Table("t").StringColumn("m", "last").At(ctx, time.Unix(0, 5000)))
	pending := slot.delegate.(*qwpLineSender).pendingRowCount
	closeErr := s.Close(ctx)
	require.ErrorIs(t, closeErr, ErrBackpressureTimeout)

	capture.mu.Lock()
	var drops []slog.Record
	for _, r := range capture.records {
		if r.Message == "qwp pool: dropped rows a returned lease could not queue" {
			drops = append(drops, r)
		}
	}
	capture.mu.Unlock()
	require.Len(t, drops, 1)
	require.Equal(t, slog.LevelWarn, drops[0].Level)
	attrs := map[string]slog.Value{}
	drops[0].Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		return true
	})
	require.Equal(t, int64(slot.slotIndex), attrs["slot_index"].Int64())
	require.Equal(t, int64(pending), attrs["rows"].Int64())
	logged, isErr := attrs["error"].Any().(error)
	require.True(t, isErr, "error attribute: %v", attrs["error"])
	require.ErrorIs(t, logged, ErrBackpressureTimeout)
	require.ErrorContains(t, logged, "unqueuedRows="+strconv.Itoa(pending))
}

// A batch too large for one segment is split per table. When the first
// table's frame is queued and the second runs into backpressure, Close reports
// only the second table's rows as unqueued, and the first table's rows still
// arrive.
func TestQwpSfPoolSplitReturnReportsOnlyUnqueuedRows(t *testing.T) {
	srv := newQwpRestartableAckServer(t, "PARTIAL_T1", "PARTIAL_T2")
	// One 8 KiB segment. Together the two tables exceed it, so the batch is
	// split; t1's frame fits, and t2's would need a second segment.
	settings := ";sf_max_segment_bytes=8192;sf_max_total_bytes=8192" +
		";sf_append_deadline_millis=150;close_flush_timeout_millis=150;auto_flush=off;"
	p, _ := newQwpReturnTestPool(t, srv.addr, settings, 1, 1, 2*time.Second, nil)
	srv.stop()

	ctx := context.Background()
	s, err := p.borrow(ctx)
	require.NoError(t, err)
	pad := strings.Repeat("y", 200)
	for _, table := range []string{"t1", "t2"} {
		marker := "PARTIAL_" + strings.ToUpper(table)
		for r := 0; r < 20; r++ {
			require.NoError(t, s.Table(table).StringColumn("m", marker).StringColumn("pad", pad).At(ctx, time.Unix(1, int64(r))))
		}
	}
	err = s.Close(ctx)
	require.ErrorIs(t, err, ErrBackpressureTimeout)
	require.ErrorContains(t, err, "unqueuedRows=20")

	srv.start(t)
	require.Eventually(t, func() bool { return srv.count("PARTIAL_T1") > 0 }, 20*time.Second, 10*time.Millisecond,
		"t1's queued rows must arrive")
	require.Zero(t, srv.count("PARTIAL_T2"), "t2's rows were reported unqueued, but they arrived")
}

func qwpReturnTestDegraded(p *qwpSenderPool, slot *qwpSenderSlot) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slot.degraded
}

// qwpReturnTestMarkWithDictFault marks the sender the next lease lands on. It
// injects the dictionary fault on that sender's directory, returns a lease
// that writes a symbol the sender has not seen, so the return fails with
// ErrSfDurability, and removes the fault.
func qwpReturnTestMarkWithDictFault(t *testing.T, p *qwpSenderPool, sfDir string) *qwpSenderSlot {
	t.Helper()
	ctx := context.Background()
	s, err := p.borrow(ctx)
	require.NoError(t, err)
	slot := s.(*qwpPooledSender).slot
	restore := qwpReturnTestInjectDictAppendFault(t, qwpReturnTestSlotDir(sfDir, slot.slotIndex))
	defer restore()
	qwpReturnTestWriteRows(t, s, "mark_"+strconv.FormatInt(time.Now().UnixNano(), 10), "")
	require.ErrorIs(t, s.Close(ctx), ErrSfDurability)
	require.True(t, qwpReturnTestDegraded(p, slot), "a return that dropped rows must mark its sender")
	return slot
}

func qwpReturnTestRecords(capture *recordCapturingHandler, message string) []slog.Record {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	var out []slog.Record
	for _, r := range capture.records {
		if r.Message == message {
			out = append(out, r)
		}
	}
	return out
}

const qwpReturnTestGrowthFailedMsg = "qwp pool: could not add a sender past one whose return failed"

// qwpReturnTestRequireGrowthFailureLogged asserts one Warn for a failed build
// past a degraded sender, carrying the injected error.
func qwpReturnTestRequireGrowthFailureLogged(t *testing.T, capture *recordCapturingHandler, injected error) {
	t.Helper()
	logged := qwpReturnTestRecords(capture, qwpReturnTestGrowthFailedMsg)
	require.Len(t, logged, 1)
	var logErr error
	logged[0].Attrs(func(a slog.Attr) bool {
		if a.Key == "error" {
			logErr, _ = a.Value.Any().(error)
		}
		return true
	})
	require.ErrorIs(t, logErr, injected)
}

// qwpReturnTestWaitBuildsSettled waits until no build is in flight. A build
// handed off by borrow is logged and starts the backoff before
// settleBuiltSlot stops counting it.
func qwpReturnTestWaitBuildsSettled(t *testing.T, p *qwpSenderPool) {
	t.Helper()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.inFlightCreations == 0
	}, qwpTestWaitTimeout, 10*time.Millisecond)
}

func qwpReturnTestGrowthBlockedUntil(p *qwpSenderPool) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.degradedGrowthBlockedUntil
}

// A mark clears only when a lease on the sender queues rows. A lease that
// writes nothing says nothing about the sender's storage, so the mark stays.
func TestQwpSfPoolMarkClearsOnlyWhenLeaseQueuesRows(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	t.Cleanup(srv.Close)
	p, sfDir := newQwpReturnTestPool(t, strings.TrimPrefix(srv.URL, "http://"), qwpReturnTestSlotFaultSettings, 1, 1, 2*time.Second, nil)
	marked := qwpReturnTestMarkWithDictFault(t, p, sfDir)

	ctx := context.Background()
	empty, err := p.borrow(ctx)
	require.NoError(t, err)
	require.Same(t, marked, empty.(*qwpPooledSender).slot, "a pool at max lends its degraded sender as a last resort")
	require.NoError(t, empty.Close(ctx))
	require.True(t, qwpReturnTestDegraded(p, marked), "a lease that queued nothing must leave the mark")

	slot, ok, err := qwpReturnTestLease(t, p, "recovered", "", false)
	require.True(t, ok, "lease after the fault was removed: %v", err)
	require.Same(t, marked, slot)
	require.False(t, qwpReturnTestDegraded(p, marked), "a lease that queued rows must clear the mark")
}

// When every free sender is degraded and the pool is at max, borrow still
// lends one: the one returned longest ago, so degraded senders take turns.
func TestQwpSfPoolAllDegradedLendsLongestReturned(t *testing.T) {
	srv := newQwpRestartableAckServer(t)
	p, _ := newQwpReturnTestPool(t, srv.addr, qwpReturnTestOutageSettings, 2, 2, 2*time.Second, nil)
	srv.stop()

	// Borrow both senders at once: serial leases would not reach the second
	// sender if the pool lent the last returned one.
	ctx := context.Background()
	s1, err := p.borrow(ctx)
	require.NoError(t, err)
	s2, err := p.borrow(ctx)
	require.NoError(t, err)
	slot1, slot2 := s1.(*qwpPooledSender).slot, s2.(*qwpPooledSender).slot
	for _, s := range []LineSender{s1, s2} {
		qwpReturnTestFillRing(t, s)
		require.NoError(t, s.Table("t").StringColumn("m", "pending").At(ctx, time.Unix(0, 5000)))
	}
	require.ErrorIs(t, s1.Close(ctx), ErrBackpressureTimeout)
	require.ErrorIs(t, s2.Close(ctx), ErrBackpressureTimeout)
	require.True(t, qwpReturnTestDegraded(p, slot1))
	require.True(t, qwpReturnTestDegraded(p, slot2))

	next, err := p.borrow(ctx)
	require.NoError(t, err, "a pool at max lends a degraded sender as a last resort")
	require.Same(t, slot1, next.(*qwpPooledSender).slot, "the degraded sender returned longest ago")
	// A row as large as the one the full ring refused, so this return fails too.
	require.NoError(t, next.Table("t").StringColumn("m", strings.Repeat("x", 1024)).At(ctx, time.Unix(0, 6000)))
	require.ErrorIs(t, next.Close(ctx), ErrBackpressureTimeout)

	after, err := p.borrow(ctx)
	require.NoError(t, err)
	require.Same(t, slot2, after.(*qwpPooledSender).slot, "a degraded sender that fails again goes behind the other")
	require.NoError(t, after.Close(ctx))
}

// When only a degraded sender is free and the pool can grow, borrow builds a
// new sender. If that build fails, outlasts the acquire timeout, or the
// borrower's context ends first, borrow lends the degraded sender instead of
// failing. A build that fails or times out pauses such builds until the
// backoff ends, and so does one that fails after its borrower stopped
// waiting. A context that ends does not pause them, and the build still adds
// its sender.
//
// The pools use max=3. A failed build keeps its index until its engine's
// cleanup finishes, and a blocked build keeps its index too; with max=2 that
// index and the degraded sender would fill the pool, and the borrows under
// test would lend the degraded sender whether or not the backoff works.
func TestQwpSfPoolGrowthFailureFallsBackToDegradedSender(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	t.Run("failed build", func(t *testing.T) {
		capture := &recordCapturingHandler{}
		p, sfDir := newQwpReturnTestPool(t, addr, qwpReturnTestSlotFaultSettings, 1, 3, 2*time.Second, slog.New(capture))
		marked := qwpReturnTestMarkWithDictFault(t, p, sfDir)

		injected := errors.New("injected build failure")
		var builds atomic.Int32
		fail := func() error {
			builds.Add(1)
			return injected
		}
		qwpSfTestAfterEngineCreateHook.Store(&fail)
		t.Cleanup(func() { qwpSfTestAfterEngineCreateHook.Store(nil) })

		ctx := context.Background()
		l1, err := p.borrow(ctx)
		require.NoError(t, err, "a failed build must fall back to the degraded sender")
		require.Same(t, marked, l1.(*qwpPooledSender).slot)
		require.EqualValues(t, 1, builds.Load())
		qwpReturnTestRequireGrowthFailureLogged(t, capture, injected)
		// Return it empty, so the mark stays and the next borrow sees a
		// degraded sender again.
		require.NoError(t, l1.Close(ctx))
		require.True(t, qwpReturnTestDegraded(p, marked))

		l2, err := p.borrow(ctx)
		require.NoError(t, err)
		require.Same(t, marked, l2.(*qwpPooledSender).slot)
		require.EqualValues(t, 1, builds.Load(), "a borrow inside the backoff must not build")

		// With the degraded sender leased, the free list is empty, and a
		// failing build returns its error whatever the backoff.
		_, err = p.borrow(ctx)
		require.ErrorIs(t, err, injected)
		require.EqualValues(t, 2, builds.Load())
		require.NoError(t, l2.Close(ctx))
	})

	t.Run("timed-out build", func(t *testing.T) {
		const acquire = 300 * time.Millisecond
		capture := &recordCapturingHandler{}
		p, sfDir := newQwpReturnTestPool(t, addr, qwpReturnTestSlotFaultSettings, 1, 3, acquire, slog.New(capture))
		marked := qwpReturnTestMarkWithDictFault(t, p, sfDir)

		// Install the hook only after construction: the constructor builds the
		// pool's first sender through the same path. The build stays blocked
		// past the acquire timeout, then fails.
		injected := errors.New("injected build failure")
		release := make(chan struct{})
		var releaseOnce sync.Once
		var builds atomic.Int32
		block := func() error {
			builds.Add(1)
			<-release
			return injected
		}
		qwpSfTestAfterEngineCreateHook.Store(&block)
		t.Cleanup(func() {
			qwpSfTestAfterEngineCreateHook.Store(nil)
			releaseOnce.Do(func() { close(release) })
		})

		ctx := context.Background()
		start := time.Now()
		l1, err := p.borrow(ctx)
		require.NoError(t, err, "a build that outlasts the borrow must fall back to the degraded sender")
		require.Same(t, marked, l1.(*qwpPooledSender).slot)
		require.GreaterOrEqual(t, time.Since(start), acquire)
		require.NoError(t, l1.Close(ctx))

		l2, err := p.borrow(ctx)
		require.NoError(t, err)
		require.Same(t, marked, l2.(*qwpPooledSender).slot)
		require.EqualValues(t, 1, builds.Load(), "a borrow inside the backoff must not start another build")
		require.NoError(t, l2.Close(ctx))

		// The build fails after its borrower stopped waiting; the pool still
		// logs the failure.
		releaseOnce.Do(func() { close(release) })
		qwpReturnTestWaitBuildsSettled(t, p)
		qwpReturnTestRequireGrowthFailureLogged(t, capture, injected)
	})

	t.Run("caller context ends", func(t *testing.T) {
		capture := &recordCapturingHandler{}
		p, sfDir := newQwpReturnTestPool(t, addr, qwpReturnTestSlotFaultSettings+"drain_orphans=on;", 1, 3, 2*time.Second, slog.New(capture))
		marked := qwpReturnTestMarkWithDictFault(t, p, sfDir)
		// A pool build reads its context only when it submits an orphan
		// drainer. Plant the orphan after construction, so the pool's first
		// build does not adopt it.
		qwpSfTestWriteOneFrameOrphan(t, filepath.Join(sfDir, "orphan"), 8192)

		// The build ends the borrow's context and stays blocked, so the
		// context's end is the only thing borrow can see.
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		release := make(chan struct{})
		var releaseOnce sync.Once
		var builds atomic.Int32
		block := func() error {
			builds.Add(1)
			cancel()
			<-release
			return nil
		}
		qwpSfTestAfterEngineCreateHook.Store(&block)
		t.Cleanup(func() {
			qwpSfTestAfterEngineCreateHook.Store(nil)
			releaseOnce.Do(func() { close(release) })
		})

		l1, err := p.borrow(ctx)
		require.NoError(t, err, "a borrow whose context ends during the build must fall back to the degraded sender")
		require.Same(t, marked, l1.(*qwpPooledSender).slot)
		require.EqualValues(t, 1, builds.Load())
		require.True(t, qwpReturnTestGrowthBlockedUntil(p).IsZero(), "the build has not failed, so growth must not pause")
		require.Empty(t, qwpReturnTestRecords(capture, qwpReturnTestGrowthFailedMsg))

		releaseOnce.Do(func() { close(release) })
		var added *qwpSenderSlot
		require.Eventually(t, func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, slot := range p.available {
				if slot != marked && !slot.degraded {
					added = slot
					return true
				}
			}
			return false
		}, qwpTestWaitTimeout, 10*time.Millisecond, "the build must add its sender after its borrower left")
		p.mu.Lock()
		drainers := added.delegate.(*qwpLineSender).drainerPool
		p.mu.Unlock()
		require.NotNil(t, drainers, "the build must have submitted the orphan drainer, the step that reads its context")
		require.Empty(t, qwpReturnTestRecords(capture, qwpReturnTestGrowthFailedMsg))
		require.NoError(t, l1.Close(context.Background()))
	})

	t.Run("handed-off build fails", func(t *testing.T) {
		capture := &recordCapturingHandler{}
		p, sfDir := newQwpReturnTestPool(t, addr, qwpReturnTestSlotFaultSettings, 1, 3, 2*time.Second, slog.New(capture))
		marked := qwpReturnTestMarkWithDictFault(t, p, sfDir)

		// The build ends the borrow's context, then fails once released.
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		injected := errors.New("injected build failure")
		release := make(chan struct{})
		var releaseOnce sync.Once
		var builds atomic.Int32
		fail := func() error {
			builds.Add(1)
			cancel()
			<-release
			return injected
		}
		qwpSfTestAfterEngineCreateHook.Store(&fail)
		t.Cleanup(func() {
			qwpSfTestAfterEngineCreateHook.Store(nil)
			releaseOnce.Do(func() { close(release) })
		})

		l1, err := p.borrow(ctx)
		require.NoError(t, err)
		require.Same(t, marked, l1.(*qwpPooledSender).slot)

		releaseOnce.Do(func() { close(release) })
		qwpReturnTestWaitBuildsSettled(t, p)
		qwpReturnTestRequireGrowthFailureLogged(t, capture, injected)
		require.True(t, qwpReturnTestGrowthBlockedUntil(p).After(time.Now()),
			"a build that fails after its borrower left must pause growth past degraded senders")

		// Return it empty, so the mark stays. The failed build's index may
		// still be reserved while its cleanup runs; even so, max=3 leaves room
		// to grow, and only the backoff stops a second build.
		bg := context.Background()
		require.NoError(t, l1.Close(bg))
		l2, err := p.borrow(bg)
		require.NoError(t, err)
		require.Same(t, marked, l2.(*qwpPooledSender).slot)
		require.EqualValues(t, 1, builds.Load(), "a borrow inside the backoff must not build")
		require.NoError(t, l2.Close(bg))
	})
}

// A panic while borrow reserves a build in place of lending a degraded sender
// poisons the pool, but leaves the degraded sender's state known, so shutdown
// still closes it.
func TestQwpSfPoolGrowthPanicKeepsDegradedSenderClosable(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	t.Cleanup(srv.Close)
	p, sfDir := newQwpReturnTestPool(t, strings.TrimPrefix(srv.URL, "http://"), qwpReturnTestSlotFaultSettings, 1, 2, 2*time.Second, nil)
	marked := qwpReturnTestMarkWithDictFault(t, p, sfDir)

	hook := func(_ *qwpSenderPool, _ int, from, to qwpSfSlotState) {
		if from == qwpSfSlotFree && to == qwpSfSlotCreating {
			panic("injected growth panic")
		}
	}
	qwpTestPoolTransitionHook.Store(&hook)
	t.Cleanup(func() { qwpTestPoolTransitionHook.Store(nil) })

	_, err := p.borrow(context.Background())
	require.ErrorIs(t, err, ErrPoolPoisoned)
	qwpTestPoolTransitionHook.Store(nil)
	p.mu.Lock()
	cleanupFailed := marked.cleanupFailed
	p.mu.Unlock()
	require.False(t, cleanupFailed, "the panic did not happen while the degraded sender was being changed")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = p.close(ctx)
	require.Eventually(t, marked.delegate.(*qwpLineSender).closed.Load, qwpTestWaitTimeout, 10*time.Millisecond,
		"shutdown must close the degraded sender")
}

// G1: a return whose committed rows cannot be queued and which also carries a
// pending fluent-API error must still report the backpressure, so the pool
// keeps the sender that holds an earlier borrower's queued row.
func TestQwpSenderPoolReturnWithPendingFluentErrorKeepsSender(t *testing.T) {
	srv := newQwpRestartableAckServer(t, "ROW_A1", "ROW_A2_DROPPED")
	ctx := context.Background()
	// A two-segment ring and a 100 ms deadline, as in
	// TestQwpSenderPoolBackpressuredReturnKeepsEarlierBorrowersRows, so the
	// outage fills the ring within a few flushes.
	engine, err := qwpSfNewCursorEngine("", 8192, 16384, 100*time.Millisecond)
	require.NoError(t, err)
	transport, err := srv.dial()(ctx, 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, srv.dial(),
		time.Millisecond, time.Minute, 10*time.Millisecond, 50*time.Millisecond)
	loop.sendLoopStart()
	delegate, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 200*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = delegate.Close(context.Background())
		_ = loop.sendLoopClose()
	})
	slot := &qwpSenderSlot{slotIndex: -1, delegate: delegate}
	p := &qwpSenderPool{maxSize: 1, notify: make(chan struct{}), all: []*qwpSenderSlot{slot}, available: []*qwpSenderSlot{slot}}

	srv.stop() // outage

	a1, err := p.borrow(ctx)
	require.NoError(t, err)
	require.NoError(t, a1.Table("t").StringColumn("m", "ROW_A1").At(ctx, time.Unix(0, 1)))
	require.NoError(t, a1.Close(ctx), "A1's row is queued in the ring")

	a2, err := p.borrow(ctx)
	require.NoError(t, err)
	qwpReturnTestFillRing(t, a2)
	require.NoError(t, a2.Table("t").StringColumn("m", "ROW_A2_DROPPED").At(ctx, time.Unix(0, 5000)))
	a2.Table("t").Int64Column("", 1) // a pending fluent-API error, with no At
	require.ErrorIs(t, a2.Close(ctx), ErrBackpressureTimeout)
	require.True(t, qwpReturnTestInAvailable(p, slot), "the pool must keep the sender holding A1's row")

	// Keep the server down past the 200 ms close-flush window: a closed sender
	// would give up on A1's row before the server is back.
	time.Sleep(time.Second)
	srv.start(t)
	require.Eventually(t, func() bool { return srv.count("ROW_A1") >= 1 }, qwpTestWaitTimeout, 10*time.Millisecond,
		"A1's row, whose Close returned nil, was lost")
	require.Zero(t, srv.count("ROW_A2_DROPPED"), "a row A2's Close reported as dropped reached the server")
}

// A lease returned while the pool is closing still gets the definite drop
// answer for its rows, and the pool closes the sender instead of keeping it.
func TestQwpSfPoolReturnDuringCloseDropsRowsAndClosesSender(t *testing.T) {
	srv := newQwpRestartableAckServer(t)
	p, _ := newQwpReturnTestPool(t, srv.addr, qwpReturnTestOutageSettings, 1, 1, 2*time.Second, nil)
	srv.stop()

	ctx := context.Background()
	s, err := p.borrow(ctx)
	require.NoError(t, err)
	slot := s.(*qwpPooledSender).slot
	qwpReturnTestFillRing(t, s)
	require.NoError(t, s.Table("t").StringColumn("m", "pending").At(ctx, time.Unix(0, 5000)))

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_ = p.close(cctx)
	}()
	require.Eventually(t, p.closing.Load, qwpTestWaitTimeout, time.Millisecond)

	err = s.Close(ctx)
	require.ErrorIs(t, err, ErrBackpressureTimeout)
	require.ErrorContains(t, err, "unqueuedRows=")
	p.mu.Lock()
	inAll := slices.Contains(p.all, slot)
	p.mu.Unlock()
	require.False(t, inAll, "the pool must close a sender returned during shutdown")
	select {
	case <-closeDone:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("pool close did not finish")
	}
}
