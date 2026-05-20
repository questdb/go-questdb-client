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

//go:build !windows

package questdb

// Go port of QuestDB's QwpIngressServerRestartFuzzTest. The contract
// being asserted (same as Java):
//
//   Every row that the user thread successfully handed off to
//   sender.Flush() (durable on disk inside sf_dir) must end up in the
//   table after the server comes back, regardless of how many times
//   the server bounces or whether the sender held its connection
//   across the bounce.
//
// Server-side dedup is required: when an SF sender reconnects (or is
// replaced by a fresh sender pointed at the same sf_dir) it is free to
// resend any frame whose ACK was lost in the bounce. The target table
// is created with DEDUP UPSERT KEYS(ts, id) so replays collapse onto
// the original row.
//
// Versus the four QwpIngressOracle tests this file deliberately uses a
// simpler oracle (count + count_distinct(id)): the property under test
// here is "no row lost across server restarts / no row over-counted by
// replay", not per-cell type fidelity. The richer typed-cell oracle is
// already covered by qwp_ingress_oracle_fuzz_test.go.
//
// Each test that bounces the server skips when !srv.owns (the
// QDB_FUZZ_ADDR mode talks to a server we don't control and can't
// SIGTERM); the smoke-no-restart test runs in both modes.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const restartFuzzTableName = "qwp_restart_fuzz"

const restartFuzzCreateSQL = "CREATE TABLE " + restartFuzzTableName + " (" +
	"id LONG, val DOUBLE, ts TIMESTAMP) " +
	"TIMESTAMP(ts) PARTITION BY DAY WAL " +
	"DEDUP UPSERT KEYS(ts, id)"

// restartFuzzSetup drops and re-creates the target table at the start
// of each test and registers a final cleanup drop. Mirrors Java's
// createTargetTable + assertMemoryLeak block setup.
func restartFuzzSetup(t *testing.T, srv *qwpFuzzServer) {
	t.Helper()
	srv.mustExec(t, "DROP TABLE IF EXISTS '"+restartFuzzTableName+"'")
	t.Cleanup(func() {
		_, _ = srv.execSQL("DROP TABLE IF EXISTS '" + restartFuzzTableName + "'")
	})
	srv.mustExec(t, restartFuzzCreateSQL)
}

// restartFuzzWriteRows pushes a deterministic (id, val, ts) sequence
// through the QWP sender: id ∈ [idBase, idBase+count), ts spaced 1µs
// apart from tsBaseNanos, val = id * 1.5. The QWP sender's column API
// is fluent through the typed methods so this mirrors Java's writeRows
// faithfully. The caller flushes.
//
// We do NOT call Flush here — Java relies on a final flush after the
// loop, and the QWP sender's auto_flush_rows can fire mid-loop.
func restartFuzzWriteRows(t *testing.T, qs QwpSender, idBase int64, count int, tsBaseNanos int64) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		id := idBase + int64(i)
		ts := time.Unix(0, tsBaseNanos+int64(i)*1000).UTC()
		qs.Table(restartFuzzTableName)
		qs.Int64Column("id", id)
		qs.Float64Column("val", float64(id)*1.5)
		if err := qs.At(ctx, ts); err != nil {
			t.Fatalf("write row id=%d: %v", id, err)
		}
	}
}

// restartFuzzRunOneSender opens an SF sender at the given sf_dir,
// pushes count rows with the deterministic grid, flushes, and closes.
// An sf_dir is owned by exactly one sender at a time — callers MUST
// serialize senders that share a dir (across epochs). Faithful to
// Java's runOneSfSender.
func restartFuzzRunOneSender(t *testing.T, srv *qwpFuzzServer, sfDir string,
	idBase int64, count int, tsBaseNanos int64) {
	t.Helper()
	ctx := context.Background()
	conf := fmt.Sprintf("ws::addr=%s;sf_dir=%s;close_flush_timeout_millis=120000;",
		srv.wsAddr(), sfDir)
	octx, ocancel := context.WithTimeout(context.Background(), 15*time.Second)
	ls, err := LineSenderFromConf(octx, conf)
	ocancel()
	if err != nil {
		t.Fatalf("open sender (sf_dir=%s): %v", sfDir, err)
	}
	qs := ls.(QwpSender)
	restartFuzzWriteRows(t, qs, idBase, count, tsBaseNanos)
	if err := qs.Flush(ctx); err != nil {
		t.Fatalf("flush sender (sf_dir=%s): %v", sfDir, err)
	}
	cctx, ccancel := context.WithTimeout(context.Background(), 60*time.Second)
	if err := qs.Close(cctx); err != nil {
		ccancel()
		t.Fatalf("close sender (sf_dir=%s): %v", sfDir, err)
	}
	ccancel()
}

// restartFuzzAssertRowCount polls the table until count() reaches the
// expected value or the deadline elapses; matches WAL apply being
// asynchronous in QuestDB. Mirrors Java's assertRowCount + the
// engine.awaitTable wait pattern. The last execSQL error (if any) is
// surfaced on timeout so "server unreachable the whole window" is
// distinguishable from "WAL never caught up".
func restartFuzzAssertRowCount(t *testing.T, srv *qwpFuzzServer, expected int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	q := "SELECT count() FROM " + restartFuzzTableName
	var lastN int64
	var lastErr error
	for {
		res, err := srv.execSQL(q)
		if err != nil {
			lastErr = err
		} else if len(res.Dataset) == 1 && len(res.Dataset[0]) == 1 {
			if n, ok := toInt64(res.Dataset[0][0]); ok {
				lastN = n
				if n == expected {
					return
				}
				if n > expected {
					t.Fatalf("row count overshoot: got %d expected %d", n, expected)
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("row count did not reach %d within %s (last seen %d, last execSQL err: %v)",
				expected, timeout, lastN, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// restartFuzzAssertDistinctIds verifies count() == count_distinct(id)
// and min/max id define the [0, expected) range exactly. Mirrors the
// SELECT count() c, count_distinct(id) d, min(id) lo, max(id) hi shape
// from Java's testSenderPushesContinuouslyWhileServerBounces.
func restartFuzzAssertDistinctIds(t *testing.T, srv *qwpFuzzServer, expected int64) {
	t.Helper()
	sql := "SELECT count(), count_distinct(id), min(id), max(id) FROM " + restartFuzzTableName
	res, err := srv.execSQL(sql)
	if err != nil {
		t.Fatalf("distinct-id assert: %v", err)
	}
	if len(res.Dataset) != 1 || len(res.Dataset[0]) != 4 {
		t.Fatalf("distinct-id assert: unexpected shape %+v", res.Dataset)
	}
	c, okC := toInt64(res.Dataset[0][0])
	d, okD := toInt64(res.Dataset[0][1])
	lo, okLo := toInt64(res.Dataset[0][2])
	hi, okHi := toInt64(res.Dataset[0][3])
	if !okC || !okD || !okLo || !okHi {
		t.Fatalf("distinct-id assert: non-numeric cell in %+v", res.Dataset[0])
	}
	if c != expected || d != expected || lo != 0 || hi != expected-1 {
		t.Fatalf("distinct-id mismatch: want c=%d d=%d lo=0 hi=%d, got c=%d d=%d lo=%d hi=%d",
			expected, expected, expected-1, c, d, lo, hi)
	}
}

// restartFuzzAssertValInvariant verifies the per-row payload invariant
// val == id * 1.5 holds for every row. Independent of any in-process
// counter: a bug that inflates both rowsProduced and the on-table id
// range in lockstep — so count(), count_distinct(id) and max(id) all
// agree with the producer's counter — would still trip this check if
// it corrupted, mis-associated, or split (id, val) pairs. Both writers
// (restartFuzzWriteRows and the continuous-bounces inline loop) encode
// val = float64(id) * 1.5; IEEE-754 binary64 makes the SQL comparison
// against id*1.5 exact for the row counts these tests reach (well under
// 2^53).
func restartFuzzAssertValInvariant(t *testing.T, srv *qwpFuzzServer) {
	t.Helper()
	sql := "SELECT count() FROM " + restartFuzzTableName +
		" WHERE val <> id * 1.5"
	res, err := srv.execSQL(sql)
	if err != nil {
		t.Fatalf("val-invariant assert: %v", err)
	}
	if len(res.Dataset) != 1 || len(res.Dataset[0]) != 1 {
		t.Fatalf("val-invariant assert: unexpected shape %+v", res.Dataset)
	}
	n, ok := toInt64(res.Dataset[0][0])
	if !ok {
		t.Fatalf("val-invariant assert: non-int count cell %+v", res.Dataset[0][0])
	}
	if n != 0 {
		// Surface a sample of the violators to make the failure actionable.
		sample := "SELECT id, val FROM " + restartFuzzTableName +
			" WHERE val <> id * 1.5 LIMIT 5"
		sres, serr := srv.execSQL(sample)
		t.Fatalf("val-invariant violated: %d rows where val != id*1.5 "+
			"(sample=%+v, sample err=%v)", n, sres.Dataset, serr)
	}
}

// restartFuzzAssertSegmentsOnDisk verifies that <sfDir>/default/
// contains at least one .sfa segment file. Used after a paused-server
// fast close to confirm the on-disk durability invariant the next
// epoch's adoption / replay depends on — catches a close-time
// regression (premature unlink, panic mid-shutdown, a refactor that
// drops segment preservation) eagerly, before the end-of-test row
// count inherits the diagnosis. label is prefixed onto any failure
// message so multi-epoch callers can locate which call site fired.
func restartFuzzAssertSegmentsOnDisk(t *testing.T, sfDir, label string) {
	t.Helper()
	slotDir := filepath.Join(sfDir, qwpSfDefaultSenderId)
	entries, err := os.ReadDir(slotDir)
	if err != nil {
		t.Fatalf("%s: read slot dir %s: %v", label, slotDir, err)
	}
	var sfaFiles []string
	var allNames []string
	for _, e := range entries {
		allNames = append(allNames, e.Name())
		if strings.HasSuffix(e.Name(), ".sfa") {
			sfaFiles = append(sfaFiles, e.Name())
		}
	}
	if len(sfaFiles) == 0 {
		t.Fatalf("%s: expected at least one .sfa segment in %s for next-epoch replay, got entries %v",
			label, slotDir, allNames)
	}
}

// --- entry points -------------------------------------------------

// TestQwpFuzzIngressServerRestartSmokeNoRestart — port of Java
// testSmokeNoRestart. Wire-path control: N parallel writers, each
// with its own sf_dir, push rows without a server bounce. Verifies
// the happy-path SF send loop in isolation from any restart logic.
// Runs in both fixture-launched AND QDB_FUZZ_ADDR mode (no restart
// involved).
func TestQwpFuzzIngressServerRestartSmokeNoRestart(t *testing.T) {
	srv := fuzzServer(t)
	restartFuzzSetup(t, srv)

	const (
		writers       = 2
		rowsPerWriter = 500
	)
	baseTsNanos := int64(1_700_000_000_000_000_000)

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs[w] = fmt.Errorf("writer %d panic: %v", w, rec)
				}
			}()
			sfDir := t.TempDir()
			idBase := int64(w) * rowsPerWriter
			tsBase := baseTsNanos + int64(w)*rowsPerWriter*1000
			restartFuzzRunOneSender(t, srv, sfDir, idBase, rowsPerWriter, tsBase)
		}()
	}
	wg.Wait()
	for w, e := range errs {
		if e != nil {
			t.Fatalf("writer %d: %v", w, e)
		}
	}
	restartFuzzAssertRowCount(t, srv, int64(writers*rowsPerWriter), 60*time.Second)
	restartFuzzAssertValInvariant(t, srv)
}

// TestQwpFuzzIngressServerRestartNewSenderRecoversFromSfDir — port of
// Java testNewSenderRecoversFromSfDir. Two epochs, same sf_dir slot.
// Epoch 1 uses close_flush_timeout_millis=0 (fast close) and pauses
// the server BEFORE the sender exits, leaving unacked frames on disk
// in <sfDir>/default/. Epoch 2 brings the server back on the same
// port, opens a new sender at the same sf_dir, and the slot adopts +
// replays the leftovers before pushing its own new rows. Dedup on
// (ts, id) collapses any wire-level replays.
//
// Skips in QDB_FUZZ_ADDR mode (can't pause an external server).
func TestQwpFuzzIngressServerRestartNewSenderRecoversFromSfDir(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("requires fixture-launched server (cannot pause QDB_FUZZ_ADDR target)")
	}
	restartFuzzSetup(t, srv)
	t.Cleanup(func() {
		// Ensure the server is up for subsequent tests no matter how we exit.
		_ = srv.start()
	})

	sfDir := t.TempDir()
	const rowsPerEpoch = 5_000
	baseTsNanos := int64(1_700_000_000_000_000_000)
	ctx := context.Background()

	// --- Epoch 1: write, pause server BEFORE sender close ---
	conf1 := fmt.Sprintf("ws::addr=%s;sf_dir=%s;close_flush_timeout_millis=0;",
		srv.wsAddr(), sfDir)
	octx, ocancel := context.WithTimeout(context.Background(), 15*time.Second)
	ls1, err := LineSenderFromConf(octx, conf1)
	ocancel()
	if err != nil {
		t.Fatalf("epoch 1 open: %v", err)
	}
	qs1 := ls1.(QwpSender)
	restartFuzzWriteRows(t, qs1, 0, rowsPerEpoch, baseTsNanos)
	publishedFsn, err := qs1.FlushAndGetSequence(ctx)
	if err != nil {
		t.Fatalf("epoch 1 flush: %v", err)
	}
	// Kill BEFORE close so genuinely-unacked frames remain on disk.
	// srv.kill() (SIGKILL) is used rather than pause() (SIGTERM): a
	// graceful JVM shutdown lets the worker pool flush every queued
	// ACK before exit, which in practice always full-drains the
	// 5000-row batch and skips the disk-durability code path. SIGKILL
	// blocks until the process is reaped, so ackedFsn is stable from
	// this point: no further ACKs can reach the loop.
	srv.kill()
	ackedBeforeClose := qs1.AckedFsn()
	cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := qs1.Close(cctx); err != nil {
		// Fast close is best-effort here — sender is disconnected,
		// frames are durable on disk for epoch 2 to replay.
		t.Logf("epoch 1 close (expected disconnect): %v", err)
	}
	ccancel()
	// Durability invariant: any frame published but not acked at the
	// time Close ran must survive on disk for epoch 2 to adopt. With
	// kill() above, the expected outcome is ackedBeforeClose <
	// publishedFsn — the disk-assert branch fires and catches a
	// close-time unlink regression eagerly. The else branch only
	// exists as defense against a rare path where in-flight ACKs were
	// already in the client's OS receive buffer at kill time and the
	// send loop drained them before our snapshot; in that case the
	// end-of-test row count still covers the property.
	if ackedBeforeClose < publishedFsn {
		restartFuzzAssertSegmentsOnDisk(t, sfDir, "epoch 1")
	} else {
		t.Logf("epoch 1: full drain raced ahead of kill (publishedFsn=%d, ackedBeforeClose=%d) — skipping eager disk check",
			publishedFsn, ackedBeforeClose)
	}

	// --- Epoch 2: server back on the same port, new sender adopts ---
	if err := srv.start(); err != nil {
		t.Fatalf("epoch 2 start server: %v", err)
	}
	restartFuzzRunOneSender(t, srv, sfDir,
		rowsPerEpoch, rowsPerEpoch,
		baseTsNanos+int64(rowsPerEpoch)*1000)
	restartFuzzAssertRowCount(t, srv, 2*rowsPerEpoch, 90*time.Second)
	restartFuzzAssertValInvariant(t, srv)
}

// TestQwpFuzzIngressServerRestartSameSenderSurvives — port of Java
// testSameSenderSurvivesServerRestart. One long-lived sender across
// a single server bounce. Phase 1 writes rowsPerPhase rows + flush,
// then we bounce the server, then Phase 2 writes the next slice on
// the SAME sender. The QWP sender's I/O loop must transparently
// reconnect; the user thread never sees the disconnect.
//
// Skips in QDB_FUZZ_ADDR mode.
func TestQwpFuzzIngressServerRestartSameSenderSurvives(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("requires fixture-launched server (cannot bounce QDB_FUZZ_ADDR target)")
	}
	restartFuzzSetup(t, srv)
	t.Cleanup(func() { _ = srv.start() })

	sfDir := t.TempDir()
	const rowsPerPhase = 500
	baseTsNanos := int64(1_700_000_000_000_000_000)
	ctx := context.Background()

	conf := fmt.Sprintf("ws::addr=%s;sf_dir=%s;"+
		"reconnect_max_duration_millis=120000;"+
		"close_flush_timeout_millis=120000;",
		srv.wsAddr(), sfDir)
	octx, ocancel := context.WithTimeout(context.Background(), 15*time.Second)
	ls, err := LineSenderFromConf(octx, conf)
	ocancel()
	if err != nil {
		t.Fatalf("open sender: %v", err)
	}
	qs := ls.(QwpSender)
	defer func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 60*time.Second)
		_ = qs.Close(dctx)
		dcancel()
	}()

	// Phase 1.
	restartFuzzWriteRows(t, qs, 0, rowsPerPhase, baseTsNanos)
	if err := qs.Flush(ctx); err != nil {
		t.Fatalf("phase 1 flush: %v", err)
	}

	// Bounce the server.
	if err := srv.bounce(); err != nil {
		t.Fatalf("bounce: %v", err)
	}

	// Phase 2 — same sender, must reconnect transparently.
	restartFuzzWriteRows(t, qs, rowsPerPhase, rowsPerPhase,
		baseTsNanos+int64(rowsPerPhase)*1000)
	if err := qs.Flush(ctx); err != nil {
		t.Fatalf("phase 2 flush: %v", err)
	}
	restartFuzzAssertRowCount(t, srv, 2*rowsPerPhase, 90*time.Second)
	restartFuzzAssertValInvariant(t, srv)
}

// TestQwpFuzzIngressServerRestartMultipleRestartsNewSender — port of
// Java testFuzzMultipleRestartsNewSender. Multi-epoch loop with a new
// sender per epoch and the server killed BEFORE each sender exits;
// every leftover frame stays on disk and is replayed by the next
// epoch's sender via the shared sf_dir slot. Final epoch is a
// drain-only sender on the now-stable server, with the default
// (long) close timeout so any residual replay completes.
//
// Skips in QDB_FUZZ_ADDR mode.
func TestQwpFuzzIngressServerRestartMultipleRestartsNewSender(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("requires fixture-launched server (cannot kill QDB_FUZZ_ADDR target)")
	}
	restartFuzzSetup(t, srv)
	t.Cleanup(func() { _ = srv.start() })

	r := newFuzzRand(t)
	sfDir := t.TempDir()
	epochs := 3 + r.Intn(3)              // 3..5
	rowsPerEpoch := 500 + r.Intn(1500)   // 500..1999
	baseTsNanos := int64(1_700_000_000_000_000_000)

	var totalRows, idBase int64
	ctx := context.Background()

	for epoch := 0; epoch < epochs; epoch++ {
		t.Logf("epoch %d/%d rows=%d idBase=%d", epoch+1, epochs, rowsPerEpoch, idBase)
		// Server must be up at the start of each epoch.
		if err := srv.start(); err != nil {
			t.Fatalf("epoch %d start: %v", epoch, err)
		}
		conf := fmt.Sprintf("ws::addr=%s;sf_dir=%s;close_flush_timeout_millis=0;",
			srv.wsAddr(), sfDir)
		octx, ocancel := context.WithTimeout(context.Background(), 15*time.Second)
		ls, err := LineSenderFromConf(octx, conf)
		ocancel()
		if err != nil {
			t.Fatalf("epoch %d open: %v", epoch, err)
		}
		qs := ls.(QwpSender)
		restartFuzzWriteRows(t, qs, idBase, rowsPerEpoch,
			baseTsNanos+idBase*1000)
		publishedFsn, err := qs.FlushAndGetSequence(ctx)
		if err != nil {
			t.Fatalf("epoch %d flush: %v", epoch, err)
		}
		// Random pause: sometimes the server drains everything,
		// sometimes not.
		time.Sleep(time.Duration(r.Intn(50)) * time.Millisecond)
		// Kill server BEFORE sender exits → unacked frames stay on
		// disk. SIGKILL rather than the graceful SIGTERM (pause) so
		// the JVM cannot flush queued ACKs through its shutdown hooks
		// and full-drain the batch; blocks until the process is
		// reaped, so ackedFsn is stable from this point.
		srv.kill()
		ackedBeforeClose := qs.AckedFsn()
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := qs.Close(cctx); err != nil {
			// Fast close best-effort across the disconnect.
			t.Logf("epoch %d close (expected disconnect): %v", epoch, err)
		}
		ccancel()
		// Durability invariant: when at least one frame was unacked
		// at Close time, the slot's .sfa files must survive so the
		// next epoch can adopt and replay. With kill() above, the
		// expected outcome each epoch is ackedBeforeClose <
		// publishedFsn — the disk-assert branch fires. The random
		// sleep before kill spreads the unacked count across the
		// 500-1999-row range, so different epochs exercise different
		// partial-drain depths. The else branch is defensive for the
		// rare OS-buffered-ACK race; the end-of-test row count keeps
		// coverage there.
		if ackedBeforeClose < publishedFsn {
			restartFuzzAssertSegmentsOnDisk(t, sfDir,
				fmt.Sprintf("epoch %d", epoch))
		} else {
			t.Logf("epoch %d: full drain raced ahead of kill (publishedFsn=%d, ackedBeforeClose=%d) — skipping eager disk check",
				epoch, publishedFsn, ackedBeforeClose)
		}
		totalRows += int64(rowsPerEpoch)
		idBase += int64(rowsPerEpoch)
	}

	// Final epoch: server up, default long close timeout so the drain
	// sender replays any leftover unacked frames and waits for ACKs.
	if err := srv.start(); err != nil {
		t.Fatalf("final start: %v", err)
	}
	confFinal := fmt.Sprintf("ws::addr=%s;sf_dir=%s;close_flush_timeout_millis=120000;",
		srv.wsAddr(), sfDir)
	octx, ocancel := context.WithTimeout(context.Background(), 15*time.Second)
	lsFinal, err := LineSenderFromConf(octx, confFinal)
	ocancel()
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	qsFinal := lsFinal.(QwpSender)
	if err := qsFinal.Flush(ctx); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	cctx, ccancel := context.WithTimeout(context.Background(), 180*time.Second)
	if err := qsFinal.Close(cctx); err != nil {
		ccancel()
		t.Fatalf("final close: %v", err)
	}
	ccancel()
	restartFuzzAssertRowCount(t, srv, totalRows, 180*time.Second)
	restartFuzzAssertValInvariant(t, srv)
}

// TestQwpFuzzIngressServerRestartContinuousBounces — port of Java
// testSenderPushesContinuouslyWhileServerBounces. The realistic
// outage scenario: one user thread writes rows continuously through
// a single long-lived SF sender while a sibling goroutine bounces the
// server 3-5 times. Producer must not surface a failure to its caller,
// and after Close every row that was handed to At(...) must be present
// exactly once (count == count_distinct(id), ids exactly [0, n)).
//
// Skips in QDB_FUZZ_ADDR mode. The Go fixture's bounce is ~500ms
// (SIGTERM + ready-poll) versus Java's 30-79ms downtime; the long
// reconnect/close timeouts cover both.
func TestQwpFuzzIngressServerRestartContinuousBounces(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("requires fixture-launched server (cannot bounce QDB_FUZZ_ADDR target)")
	}
	restartFuzzSetup(t, srv)
	t.Cleanup(func() { _ = srv.start() })

	r := newFuzzRand(t)
	sfDir := t.TempDir()
	bounces := 3 + r.Intn(3) // 3..5
	const (
		batchRows        = 25
		batchPauseMillis = 2
	)
	baseTsNanos := int64(1_700_000_000_000_000_000)
	tsStepNanos := int64(1000) // 1µs per row

	conf := fmt.Sprintf("ws::addr=%s;sf_dir=%s;"+
		"reconnect_max_duration_millis=120000;"+
		"close_flush_timeout_millis=120000;",
		srv.wsAddr(), sfDir)

	var stopProducer atomic.Bool
	var producerErr atomic.Value
	var bouncerErr atomic.Value
	var rowsProduced atomic.Int64

	producerDone := make(chan struct{})
	bouncerDone := make(chan struct{})

	go func() {
		defer close(producerDone)
		ctx := context.Background()
		octx, ocancel := context.WithTimeout(context.Background(), 15*time.Second)
		ls, err := LineSenderFromConf(octx, conf)
		ocancel()
		if err != nil {
			producerErr.Store(fmt.Errorf("open: %w", err))
			return
		}
		qs := ls.(QwpSender)
		defer func() {
			cctx, ccancel := context.WithTimeout(context.Background(), 180*time.Second)
			if err := qs.Close(cctx); err != nil {
				producerErr.Store(fmt.Errorf("close: %w", err))
			}
			ccancel()
		}()
		var id int64
		for !stopProducer.Load() {
			for i := 0; i < batchRows; i++ {
				currentId := id
				id++
				ts := time.Unix(0, baseTsNanos+currentId*tsStepNanos).UTC()
				qs.Table(restartFuzzTableName)
				qs.Int64Column("id", currentId)
				qs.Float64Column("val", float64(currentId)*1.5)
				if err := qs.At(ctx, ts); err != nil {
					producerErr.Store(fmt.Errorf("at id=%d: %w", currentId, err))
					return
				}
			}
			// Publish what we just buffered into the SF cursor so a
			// bounce mid-batch can't lose rows still sitting in the
			// client auto-flush buffer.
			if err := qs.Flush(ctx); err != nil {
				producerErr.Store(fmt.Errorf("flush@id=%d: %w", id, err))
				return
			}
			rowsProduced.Store(id)
			time.Sleep(batchPauseMillis * time.Millisecond)
		}
	}()

	go func() {
		defer close(bouncerDone)
		// Let the producer get into a steady-state rhythm before the
		// first bounce so we exercise the mid-flight reconnect path
		// rather than first-connect.
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < bounces; i++ {
			t.Logf("bounce %d/%d", i+1, bounces)
			srv.pause()
			// Java sleeps 30-79ms here; the Go fixture's start() polls
			// /ping so the effective downtime is longer regardless.
			time.Sleep(time.Duration(30+r.Intn(50)) * time.Millisecond)
			if err := srv.start(); err != nil {
				bouncerErr.Store(fmt.Errorf("bounce %d start: %w", i+1, err))
				return
			}
			time.Sleep(time.Duration(120+r.Intn(200)) * time.Millisecond)
		}
	}()

	select {
	case <-bouncerDone:
	case <-time.After(180 * time.Second):
		stopProducer.Store(true)
		t.Fatalf("bouncer did not finish within 180s")
	}
	if v := bouncerErr.Load(); v != nil {
		stopProducer.Store(true)
		t.Fatalf("bouncer: %v", v)
	}

	// Grace window: a few more producer batches against the now-stable
	// server, then signal stop and wait for the producer to drain.
	time.Sleep(200 * time.Millisecond)
	stopProducer.Store(true)

	select {
	case <-producerDone:
	case <-time.After(240 * time.Second):
		t.Fatalf("producer did not finish within 240s (rowsProduced=%d)",
			rowsProduced.Load())
	}
	if v := producerErr.Load(); v != nil {
		t.Fatalf("producer (rowsProduced=%d): %v", rowsProduced.Load(), v)
	}

	expected := rowsProduced.Load()
	if expected <= 0 {
		t.Fatalf("producer wrote zero rows")
	}
	t.Logf("producer wrote %d rows across %d server bounces", expected, bounces)
	restartFuzzAssertRowCount(t, srv, expected, 180*time.Second)
	restartFuzzAssertDistinctIds(t, srv, expected)
	restartFuzzAssertValInvariant(t, srv)
}
