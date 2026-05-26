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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// newQwpTestServerWithMaxBatch returns a mock QWP server that
// advertises the supplied X-QWP-Max-Batch-Size in its upgrade
// response. A value <= 0 omits the header entirely (matches the
// older-server case the clamp must treat as "no cap").
func newQwpTestServerWithMaxBatch(t *testing.T, maxBatchSize int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		if maxBatchSize > 0 {
			w.Header().Set(qwpHeaderMaxBatchSize, fmt.Sprintf("%d", maxBatchSize))
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("websocket accept error: %v", err)
			return
		}
		defer conn.CloseNow()

		var seq int64
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
}

// TestQwpServerMaxBatchSizeParsed pins the raw header→transport
// plumbing: the parsed cap lands on qwpTransport.serverMaxBatchSize
// for any positive integer value, and stays at 0 when the header
// is absent or unparseable.
func TestQwpServerMaxBatchSizeParsed(t *testing.T) {
	cases := []struct {
		name     string
		header   string // "" means do not send the header
		expected int32
	}{
		{"absent", "", 0},
		{"positive_2mb", "2097152", 2 * 1024 * 1024},
		{"positive_16mb", "16777216", 16 * 1024 * 1024},
		{"zero", "0", 0},
		{"negative", "-1", 0},
		{"garbage", "not-a-number", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(qwpHeaderVersion, "1")
				if tc.header != "" {
					w.Header().Set(qwpHeaderMaxBatchSize, tc.header)
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				for {
					if _, _, err := conn.Read(context.Background()); err != nil {
						return
					}
				}
			}))
			defer srv.Close()

			s := newQwpSenderForTest(t, srv.URL)
			defer s.Close(context.Background())
			tr := s.cursorSendLoop.transport.Load()
			if tr == nil {
				t.Fatalf("no transport bound after initial connect")
			}
			if tr.serverMaxBatchSize != tc.expected {
				t.Fatalf("serverMaxBatchSize = %d, want %d (header=%q)",
					tr.serverMaxBatchSize, tc.expected, tc.header)
			}
		})
	}
}

// TestQwpApplyServerBatchSizeLimit exercises the clamp resolution
// table in isolation. Constructed without a real transport so each
// case can dial in autoFlushBytes + the synthetic cap directly,
// matching Java's applyServerBatchSizeLimit case analysis.
//
// Also pins that s.serverMaxBatchSize mirrors the transport's cap
// regardless of the opt-out / no-cap branches — the per-row hard
// guard and the flush-time defensive guard read this mirror,
// independent of the soft auto-flush trigger.
func TestQwpApplyServerBatchSizeLimit(t *testing.T) {
	cases := []struct {
		name             string
		autoFlushBytes   int
		serverCap        int32
		expectEffective  int64
		expectMirrorCap  int32
		passNilTransport bool
	}{
		// User opt-out wins for the auto-flush trigger; the raw cap
		// still mirrors so the per-row hard guard fires.
		{"optout_no_cap", 0, 0, 0, 0, false},
		{"optout_with_cap", 0, 1024 * 1024, 0, 1024 * 1024, false},
		// No server cap: configured value passes through; mirror is 0.
		{"no_cap_keeps_configured", 8 << 20, 0, 8 << 20, 0, false},
		{"nil_transport_keeps_configured", 8 << 20, 0, 8 << 20, 0, true},
		// Configured below safe budget: configured wins.
		{"configured_below_90pct", 1 << 20, 16 << 20, 1 << 20, 16 << 20, false},
		// Configured above safe budget: clamped to floor(cap*9/10).
		{"clamp_to_90pct_of_16mb", 16 << 20, 16 << 20, int64(16<<20) * 9 / 10, 16 << 20, false},
		{"clamp_to_90pct_of_2mb", 8 << 20, 2 << 20, int64(2<<20) * 9 / 10, 2 << 20, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &qwpLineSender{autoFlushBytes: tc.autoFlushBytes}
			var tr *qwpTransport
			if !tc.passNilTransport {
				tr = &qwpTransport{serverMaxBatchSize: tc.serverCap}
			}
			s.applyServerBatchSizeLimit(tr)
			if got := s.effectiveAutoFlushBytes.Load(); got != tc.expectEffective {
				t.Fatalf("effectiveAutoFlushBytes = %d, want %d", got, tc.expectEffective)
			}
			if got := s.serverMaxBatchSize.Load(); got != tc.expectMirrorCap {
				t.Fatalf("serverMaxBatchSize mirror = %d, want %d", got, tc.expectMirrorCap)
			}
		})
	}
}

// TestQwpEffectiveAutoFlushBytesSeededOnConnect verifies that the
// end-to-end conf-driven path (LineSenderFromConf → memory mode)
// seeds the sender's effectiveAutoFlushBytes from the server's
// advertised cap on the initial connect, without relying on a
// follow-up reconnect.
func TestQwpEffectiveAutoFlushBytesSeededOnConnect(t *testing.T) {
	// Advertise a 4 MiB cap. Configured auto_flush_bytes default
	// is 8 MiB (qwpDefaultAutoFlushBytes), so the clamp must
	// reduce it to floor(4 MiB * 9/10).
	const serverCap = 4 * 1024 * 1024
	srv := newQwpTestServerWithMaxBatch(t, serverCap)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	conf, err := confFromStr("ws::addr=" + addr + ";")
	if err != nil {
		t.Fatalf("confFromStr: %v", err)
	}
	if conf.autoFlushBytes != qwpDefaultAutoFlushBytes {
		t.Fatalf("test precondition: autoFlushBytes default = %d, want %d",
			conf.autoFlushBytes, qwpDefaultAutoFlushBytes)
	}

	ls, err := LineSenderFromConf(context.Background(), "ws::addr="+addr+";")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	s, ok := ls.(*qwpLineSender)
	if !ok {
		t.Fatalf("LineSenderFromConf returned %T, want *qwpLineSender", ls)
	}
	got := s.effectiveAutoFlushBytes.Load()
	want := int64(serverCap) * 9 / 10
	if got != want {
		t.Fatalf("effectiveAutoFlushBytes = %d, want %d (90%% of %d)",
			got, want, serverCap)
	}
}

// TestQwpEffectiveAutoFlushBytesKeptWhenServerHasNoCap pins the
// "older server" case: when the upgrade response omits
// X-QWP-Max-Batch-Size, the configured auto_flush_bytes flows
// through to the trigger unchanged.
func TestQwpEffectiveAutoFlushBytesKeptWhenServerHasNoCap(t *testing.T) {
	srv := newQwpTestServerWithMaxBatch(t, 0) // header omitted
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(), "ws::addr="+addr+";")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	s := ls.(*qwpLineSender)
	if got, want := s.effectiveAutoFlushBytes.Load(), int64(qwpDefaultAutoFlushBytes); got != want {
		t.Fatalf("effectiveAutoFlushBytes = %d, want %d (server cap unset)",
			got, want)
	}
}

// TestQwpEffectiveAutoFlushBytesPreservesOptout pins that
// auto_flush_bytes=off survives a server cap advertisement: the
// user's explicit opt-out wins.
func TestQwpEffectiveAutoFlushBytesPreservesOptout(t *testing.T) {
	srv := newQwpTestServerWithMaxBatch(t, 4*1024*1024)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	s := ls.(*qwpLineSender)
	if got := s.effectiveAutoFlushBytes.Load(); got != 0 {
		t.Fatalf("effectiveAutoFlushBytes = %d, want 0 (auto_flush_bytes=off)",
			got)
	}
}

// TestQwpSwapClientFiresOnTransportSwap pins the swap → callback
// wire: invoking swapClient with a fresh transport runs the
// installed onTransportSwap callback, passing the freshly bound
// transport. This is the seam the sender relies on to re-apply
// the auto_flush_bytes clamp after every reconnect, so a
// regression that severed the wire would silently strand the
// clamp at its initial value across a rolling-upgrade boundary.
func TestQwpSwapClientFiresOnTransportSwap(t *testing.T) {
	srv := newQwpTestServerWithMaxBatch(t, 4*1024*1024)
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	if err != nil {
		t.Fatalf("qwpSfNewCursorEngine: %v", err)
	}
	defer func() { _ = engine.engineClose() }()

	dial := func(ctx context.Context, _ int) (*qwpTransport, error) {
		var tr qwpTransport
		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		if err := tr.connect(ctx, wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
			return nil, err
		}
		return &tr, nil
	}

	initial, err := dial(context.Background(), 0)
	if err != nil {
		t.Fatalf("initial dial: %v", err)
	}
	loop := qwpSfNewSendLoop(engine, initial, dial,
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	defer func() { _ = loop.sendLoopClose() }()

	// Don't start the loop — swapClient is callable independently
	// of the run() loop, and avoiding sendLoopStart keeps the test
	// deterministic (no reconnect-machinery races).
	var (
		fired      int
		lastCapArg int32
	)
	loop.sendLoopSetOnTransportSwap(func(t *qwpTransport) {
		fired++
		if t != nil {
			lastCapArg = t.serverMaxBatchSize
		}
	})

	// Swap to a fresh transport (re-dialled against the same
	// server, so the same cap should re-arrive).
	replacement, err := dial(context.Background(), 0)
	if err != nil {
		t.Fatalf("replacement dial: %v", err)
	}
	if err := loop.swapClient(replacement); err != nil {
		t.Fatalf("swapClient: %v", err)
	}
	if fired != 1 {
		t.Fatalf("onTransportSwap fired %d times, want 1", fired)
	}
	if lastCapArg != 4*1024*1024 {
		t.Fatalf("callback saw cap=%d, want %d", lastCapArg, 4*1024*1024)
	}

	// Second swap also fires the callback — the wire is not
	// one-shot.
	replacement2, err := dial(context.Background(), 0)
	if err != nil {
		t.Fatalf("second replacement dial: %v", err)
	}
	if err := loop.swapClient(replacement2); err != nil {
		t.Fatalf("second swapClient: %v", err)
	}
	if fired != 2 {
		t.Fatalf("onTransportSwap fired %d times after second swap, want 2", fired)
	}

	// Clearing the callback turns off the wire.
	loop.sendLoopSetOnTransportSwap(nil)
	replacement3, err := dial(context.Background(), 0)
	if err != nil {
		t.Fatalf("third replacement dial: %v", err)
	}
	if err := loop.swapClient(replacement3); err != nil {
		t.Fatalf("third swapClient: %v", err)
	}
	if fired != 2 {
		t.Fatalf("onTransportSwap fired %d times after clear, want 2", fired)
	}
}

// TestQwpPerRowGuardFires verifies the per-row hard guard catches a
// single row whose buffered bytes already exceed the server's wire
// cap, before commitRow makes the row visible to the batch. Uses
// auto_flush_bytes=off so the soft trigger does not race the guard.
func TestQwpPerRowGuardFires(t *testing.T) {
	// 64 bytes cap — any non-trivial row trips it.
	srv := newQwpTestServerWithMaxBatch(t, 64)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	// 200-byte string column alone exceeds the 64-byte cap.
	err = ls.Table("t").
		StringColumn("big", strings.Repeat("x", 200)).
		AtNow(context.Background())
	if err == nil {
		t.Fatal("expected per-row guard to fire, got nil error")
	}
	if !strings.Contains(err.Error(), "row too large for server batch cap") {
		t.Fatalf("error = %q, want substring %q", err.Error(),
			"row too large for server batch cap")
	}
	if !strings.Contains(err.Error(), "serverMaxBatchSize=64") {
		t.Fatalf("error = %q, want it to name the cap", err.Error())
	}

	// Sender stays usable: the failed row's bytes were discarded
	// via cancelRow, and the next Table() call starts a clean row.
	// We can't easily flush anything meaningful through a 64-byte
	// cap, so just check that the sender does not latch an error.
	s := ls.(*qwpLineSender)
	if s.pendingRowCount != 0 {
		t.Fatalf("pendingRowCount = %d after guard fire, want 0",
			s.pendingRowCount)
	}
}

// TestQwpPerRowGuardPreservesPriorCommittedRows verifies the per-row
// guard rolls back ONLY the offending row — earlier rows in the
// batch stay intact and remain flushable. This is the property that
// makes the guard recoverable instead of catastrophic.
func TestQwpPerRowGuardPreservesPriorCommittedRows(t *testing.T) {
	// 1024 bytes cap: small rows fit, a 2000-byte string does not.
	srv := newQwpTestServerWithMaxBatch(t, 1024)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	ctx := context.Background()
	// Two small rows commit cleanly.
	for i := 0; i < 2; i++ {
		if err := ls.Table("t").
			Symbol("s", "a").
			Int64Column("x", int64(i)).
			AtNow(ctx); err != nil {
			t.Fatalf("AtNow[%d]: %v", i, err)
		}
	}

	// Third row is oversize; guard fires.
	err = ls.Table("t").
		StringColumn("big", strings.Repeat("x", 2000)).
		AtNow(ctx)
	if err == nil {
		t.Fatal("expected per-row guard to fire, got nil error")
	}
	if !strings.Contains(err.Error(), "row too large for server batch cap") {
		t.Fatalf("error = %q, want guard-fire substring", err.Error())
	}

	// The two earlier rows are still pending; flush succeeds (the
	// encoded frame for two small rows + schema stays under 1024).
	s := ls.(*qwpLineSender)
	if s.pendingRowCount != 2 {
		t.Fatalf("pendingRowCount = %d after guard fire, want 2 (prior rows preserved)",
			s.pendingRowCount)
	}
	if err := ls.Flush(ctx); err != nil {
		t.Fatalf("Flush of prior rows: %v", err)
	}
	if s.pendingRowCount != 0 {
		t.Fatalf("pendingRowCount = %d after flush, want 0", s.pendingRowCount)
	}
}

// TestQwpPerRowGuardClearsCachedDesignatedTs is a regression test
// for a silent data-loss bug: when the per-row guard fires on the
// FIRST row of a fresh table, cancelRow removes the just-created
// designated-TS column from tb.columns (committedColumnCount is 0,
// so all uncommitted columns get wiped). But s.cachedDesignatedTs
// still holds a pointer to that now-orphaned column, with col.table
// still pointing at the same tb. On the user's retry, the cache
// staleness check at atWithTimestamp passes (col.table matches,
// typeCode matches), the orphan is reused, addTimestamp writes
// into a column that is no longer in tb.columns, and commitRow
// (which only iterates tb.columns) commits the row without a
// designated timestamp. The encoder then ships the row on the wire
// with NO designated-TS column. The fix is to nil out
// s.cachedDesignatedTs on every cancelRow path in atWithTimestamp.
func TestQwpPerRowGuardClearsCachedDesignatedTs(t *testing.T) {
	srv := newQwpTestServerWithMaxBatch(t, 64)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	ctx := context.Background()

	// First attempt on a fresh table: a 200-byte string trips the
	// per-row guard. cancelRow wipes both the string column and the
	// designated-TS column from tb.columns because committedColumnCount
	// is 0. s.cachedDesignatedTs is left pointing at the orphaned
	// "" column.
	err = ls.Table("t").
		StringColumn("big", strings.Repeat("x", 200)).
		At(ctx, time.Unix(0, 1_000_000_000))
	if err == nil {
		t.Fatal("expected per-row guard to fire, got nil error")
	}
	if !strings.Contains(err.Error(), "row too large for server batch cap") {
		t.Fatalf("error = %q, want guard-fire substring", err.Error())
	}

	// Retry on the same table with a small row + explicit At(ts).
	// If cachedDesignatedTs was cleared, getOrCreateDesignatedTimestamp
	// runs and re-creates the "" column in tb.columns. If not, the
	// staleness check skips the lookup, the orphan is reused, and
	// commitRow runs without a "" column in tb.columns.
	if err := ls.Table("t").
		Symbol("s", "a").
		At(ctx, time.Unix(0, 2_000_000_000)); err != nil {
		t.Fatalf("retry At: %v", err)
	}

	s := ls.(*qwpLineSender)
	tb, ok := s.tableBuffers["t"]
	if !ok || tb == nil {
		t.Fatal("table buffer for 't' missing after retry")
	}

	// The designated-TS column lives under the empty-string key.
	// If the bug is present, cancelRow removed it from columnIndex
	// and the cached-orphan reuse meant nothing re-added it.
	if _, ok := tb.columnIndex[""]; !ok {
		names := make([]string, 0, len(tb.columns))
		for _, c := range tb.columns {
			names = append(names, c.name)
		}
		t.Fatalf("designated-TS column missing from columnIndex after retry; tb.columns=%v", names)
	}
	var dtCol *qwpColumnBuffer
	for _, c := range tb.columns {
		if c.name == "" {
			dtCol = c
			break
		}
	}
	if dtCol == nil {
		names := make([]string, 0, len(tb.columns))
		for _, c := range tb.columns {
			names = append(names, c.name)
		}
		t.Fatalf("designated-TS column not present in tb.columns after retry; tb.columns=%v", names)
	}
	// The retry committed exactly one row; the designated-TS column
	// must reflect that row in its data (i.e., be encoded with the
	// rest of the table).
	if dtCol.rowCount != 1 {
		t.Fatalf("designated-TS column rowCount = %d, want 1 (retry row committed with timestamp)",
			dtCol.rowCount)
	}
}

// TestQwpPerRowGuardNoOpWhenServerHasNoCap pins the older-server
// path: when the upgrade response omits X-QWP-Max-Batch-Size, the
// per-row guard short-circuits and an arbitrarily large row commits
// without complaint. Important so an older server in a rolling
// upgrade doesn't suddenly start rejecting rows the client was
// happily sending before.
func TestQwpPerRowGuardNoOpWhenServerHasNoCap(t *testing.T) {
	srv := newQwpTestServerWithMaxBatch(t, 0) // header omitted
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	// Append a row with a moderately large string. Would trip the
	// guard against any reasonable cap, but here the server
	// advertised none.
	if err := ls.Table("t").
		StringColumn("big", strings.Repeat("x", 10_000)).
		AtNow(context.Background()); err != nil {
		t.Fatalf("AtNow with large string and no advertised cap: %v", err)
	}
	s := ls.(*qwpLineSender)
	if s.pendingRowCount != 1 {
		t.Fatalf("pendingRowCount = %d, want 1", s.pendingRowCount)
	}
}

// TestQwpFlushTimeGuardFires verifies the defensive cap check at
// encode time catches the case where individual rows fit under the
// cap but their cumulative encoded frame (schema, dict, headers,
// row data) does not. Drops all pending state in-place and surfaces
// a typed error naming the size, cap, and dropped-row count.
func TestQwpFlushTimeGuardFires(t *testing.T) {
	// Small cap; many small rows; auto-flush off so we control
	// exactly when the flush triggers.
	const serverCap = 256
	srv := newQwpTestServerWithMaxBatch(t, serverCap)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	ctx := context.Background()
	const rows = 100
	for i := 0; i < rows; i++ {
		if err := ls.Table("t").
			Symbol("s", "abc").
			Int64Column("x", int64(i)).
			AtNow(ctx); err != nil {
			t.Fatalf("AtNow[%d]: %v", i, err)
		}
	}
	s := ls.(*qwpLineSender)
	if s.pendingRowCount != rows {
		t.Fatalf("pendingRowCount before flush = %d, want %d (per-row guard misfire?)",
			s.pendingRowCount, rows)
	}

	err = ls.Flush(ctx)
	if err == nil {
		t.Fatalf("expected flush-time defensive guard to fire, got nil error")
	}
	if !strings.Contains(err.Error(), "batch too large for server batch cap") {
		t.Fatalf("error = %q, want guard-fire substring", err.Error())
	}
	wantDroppedSub := fmt.Sprintf("droppedRows=%d", rows)
	if !strings.Contains(err.Error(), wantDroppedSub) {
		t.Fatalf("error = %q, want %q substring", err.Error(), wantDroppedSub)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("serverMaxBatchSize=%d", serverCap)) {
		t.Fatalf("error = %q, want serverMaxBatchSize=%d substring", err.Error(), serverCap)
	}
}

// TestQwpFlushTimeGuardResetsPendingState verifies the sender is
// usable after the defensive guard fires: pendingRowCount returns
// to 0, table buffers are cleared, and a subsequent small flush
// goes through cleanly.
func TestQwpFlushTimeGuardResetsPendingState(t *testing.T) {
	const serverCap = 256
	srv := newQwpTestServerWithMaxBatch(t, serverCap)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := ls.Table("t").
			Symbol("s", "abc").
			Int64Column("x", int64(i)).
			AtNow(ctx); err != nil {
			t.Fatalf("AtNow[%d]: %v", i, err)
		}
	}
	if err := ls.Flush(ctx); err == nil {
		t.Fatal("expected flush-time guard to fire")
	}

	s := ls.(*qwpLineSender)
	if s.pendingRowCount != 0 {
		t.Fatalf("pendingRowCount = %d after guard fire, want 0", s.pendingRowCount)
	}
	if s.pendingBytes != 0 {
		t.Fatalf("pendingBytes = %d after guard fire, want 0", s.pendingBytes)
	}

	// Sender should still accept new rows. The encoded frame for
	// a single small row fits under the cap.
	if err := ls.Table("t").
		Int64Column("x", 1).
		AtNow(ctx); err != nil {
		t.Fatalf("AtNow after guard reset: %v", err)
	}
	if err := ls.Flush(ctx); err != nil {
		t.Fatalf("Flush of single row after reset: %v", err)
	}
}

// TestQwpFlushTimeGuardNoOpWhenServerHasNoCap is the
// flush-time equivalent of TestQwpPerRowGuardNoOpWhenServerHasNoCap:
// no advertised cap means the encoder's output flows straight to
// engineAppendBlocking, regardless of how large the encoded frame
// is.
func TestQwpFlushTimeGuardNoOpWhenServerHasNoCap(t *testing.T) {
	srv := newQwpTestServerWithMaxBatch(t, 0)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush_bytes=off;")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := ls.Table("t").
			Symbol("s", "abc").
			Int64Column("x", int64(i)).
			AtNow(ctx); err != nil {
			t.Fatalf("AtNow[%d]: %v", i, err)
		}
	}
	if err := ls.Flush(ctx); err != nil {
		t.Fatalf("Flush with no advertised cap: %v", err)
	}
}

// TestQwpClampDrivesAutoFlushTrigger demonstrates the end-to-end
// behavior change: with auto_flush_bytes configured to 8 MiB and
// the server advertising a 256 KiB cap, the per-row trigger fires
// at the clamped threshold (~230 KiB) instead of the configured
// 8 MiB. Verified by counting flushes after writing enough bytes
// to cross the clamped threshold but stay under the configured
// one.
func TestQwpClampDrivesAutoFlushTrigger(t *testing.T) {
	// Force a tiny server cap so a small number of rows crosses
	// the clamped threshold within a reasonable test runtime.
	const serverCap = 256 * 1024 // 256 KiB
	srv := newQwpTestServerWithMaxBatch(t, serverCap)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	// Configured value (8 MiB default) is well above the clamp,
	// so any flush observed here is the clamp's doing.
	ls, err := LineSenderFromConf(context.Background(), "ws::addr="+addr+";")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer ls.Close(context.Background())

	s := ls.(*qwpLineSender)
	wantThreshold := int64(serverCap) * 9 / 10
	if got := s.effectiveAutoFlushBytes.Load(); got != wantThreshold {
		t.Fatalf("effectiveAutoFlushBytes = %d, want %d", got, wantThreshold)
	}

	// Write rows until pendingBytes would cross the clamped
	// threshold. Each row is small (under 1 KiB once encoded), so
	// the trigger fires partway through the loop. After auto-flush,
	// pendingRowCount resets to 0; we assert it did at least once.
	ctx := context.Background()
	flushed := false
	for i := 0; i < 4096 && !flushed; i++ {
		if err := s.Table("clamp_test").
			Symbol("host", "h1").
			Int64Column("v", int64(i)).
			At(ctx, time.Unix(0, int64(i+1)*1_000_000)); err != nil {
			t.Fatalf("At[%d]: %v", i, err)
		}
		if s.pendingRowCount == 0 {
			// auto-flush triggered and reset state.
			flushed = true
		}
	}
	if !flushed {
		t.Fatalf("auto-flush never triggered: pendingBytes=%d, threshold=%d",
			s.pendingBytes, wantThreshold)
	}
}
