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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQwpSplitFlushSegmentCapSendsFitTablesDropsOversize is the core
// per-table-split regression test: a multi-table batch
// whose combined frame overruns the per-segment cap must NOT destroy
// every table's rows. enqueueCursor falls back to a per-table split that
// flushes each table whose own frame fits and drops only the table that
// is individually over-cap. Mirrors Java
// QwpWebSocketSender.flushPendingRowsSplit.
func TestQwpSplitFlushSegmentCapSendsFitTablesDropsOversize(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true})
	defer srv.Close()

	// Memory-mode cursor with a 4 KiB segment and no auto-flush, so the
	// whole batch lands in one combined frame at the explicit Flush.
	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// No server cap advertised: the 4 KiB segment is the binding limit.
	require.Zero(t, s.serverMaxBatchSize.Load(),
		"test precondition: no server cap, so the segment is the binding limit")

	ctx := context.Background()

	// fit_table: a few small rows; its own single-table frame fits the
	// 4 KiB segment.
	const fitRows = 3
	for i := 0; i < fitRows; i++ {
		require.NoError(t, s.Table("fit_table").Int64Column("i", int64(i)).AtNow(ctx),
			"fit row %d", i)
	}
	// big_table: ~20 KiB of column data — far past one 4 KiB segment even
	// re-encoded on its own.
	const bigRows = 100
	big := strings.Repeat("x", 200)
	for i := 0; i < bigRows; i++ {
		require.NoError(t, s.Table("big_table").StringColumn("s", big).AtNow(ctx),
			"big row %d", i)
	}
	require.Equal(t, fitRows+bigRows, s.pendingRowCount)

	publishedBefore := engine.enginePublishedFsn()

	// The combined frame overruns the segment cap, so enqueueCursor
	// splits per table: fit_table goes out; big_table is irreducible.
	err := s.Flush(ctx)
	require.Error(t, err, "the irreducible big_table must surface an error")
	require.Contains(t, err.Error(), "big_table", "error must name the dropped table")
	require.NotContains(t, err.Error(), "fit_table",
		"the fit table must not be reported as dropped")
	require.Contains(t, err.Error(), fmt.Sprintf("droppedRows=%d", bigRows),
		"only big_table's rows are dropped, not the whole batch")
	require.Contains(t, err.Error(), "cursor segment")

	// Whole batch resolved; nothing retained.
	require.Zero(t, s.pendingRowCount)
	require.Zero(t, s.pendingBytes)

	// Exactly one frame (fit_table) was published; big_table never was.
	require.Equal(t, publishedBefore+1, engine.enginePublishedFsn(),
		"exactly the fit_table frame should have been published")

	// Wait for that frame to reach the server, then assert the server saw
	// fit_table and never saw big_table. Captured before the usability
	// flush below so only the split's output is in the recording.
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 2*time.Second, time.Millisecond)

	var payloads []string
	for _, frames := range srv.recordedFrames() {
		payloads = append(payloads, frames...)
	}
	require.Len(t, payloads, 1, "server should receive exactly the one fit_table frame")
	require.Contains(t, payloads[0], "fit_table")
	require.NotContains(t, payloads[0], "big_table")

	// Sender stays usable after the partial drop.
	require.NoError(t, s.Table("fit_table").Int64Column("i", 99).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
	require.Zero(t, s.pendingRowCount)
}

// TestQwpSplitFlushServerCapDropsOnlyOversizeTable is the server-cap
// analogue: when the server-advertised batch cap (not the segment cap)
// is the binding limit, the split still flushes the fit table and drops
// only the individually-over-cap one, reporting just that table's rows.
func TestQwpSplitFlushServerCapDropsOnlyOversizeTable(t *testing.T) {
	const serverCap = 256
	srv := newQwpTestServerWithMaxBatch(t, serverCap)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";auto_flush=off;")
	require.NoError(t, err)
	defer ls.Close(context.Background())
	s := ls.(*qwpLineSender)

	ctx := context.Background()

	// The cap rides the upgrade response; wait until the transport-swap
	// callback has mirrored it onto the sender.
	require.Eventually(t, func() bool {
		return s.serverMaxBatchSize.Load() == serverCap
	}, 2*time.Second, time.Millisecond)

	// fit_one: a single tiny row — its own frame is well under 256 B.
	require.NoError(t, s.Table("fit_one").Int64Column("i", 1).AtNow(ctx))
	// big_many: enough rows that its own frame exceeds 256 B.
	const bigRows = 80
	for i := 0; i < bigRows; i++ {
		require.NoError(t, s.Table("big_many").Int64Column("i", int64(i)).AtNow(ctx),
			"big row %d", i)
	}
	require.Equal(t, bigRows+1, s.pendingRowCount)

	publishedBefore := s.cursorEngine.enginePublishedFsn()

	err = s.Flush(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "batch too large for server batch cap")
	require.Contains(t, err.Error(), "big_many", "error must name the dropped table")
	require.NotContains(t, err.Error(), "fit_one",
		"the fit table must not be reported as dropped")
	require.Contains(t, err.Error(), fmt.Sprintf("serverMaxBatchSize=%d", serverCap))
	require.Contains(t, err.Error(), fmt.Sprintf("droppedRows=%d", bigRows),
		"only big_many's rows are dropped, not the fit table's")

	require.Zero(t, s.pendingRowCount)
	require.Equal(t, publishedBefore+1, s.cursorEngine.enginePublishedFsn(),
		"only the fit table should have been published")

	// Sender stays usable.
	require.NoError(t, s.Table("fit_one").Int64Column("i", 2).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
}

// TestQwpSplitFlushAllFitTablesFlushAcrossFrames pins the all-reducible
// case: a combined frame over the segment cap purely by aggregation (no
// single table is over-cap) flushes every table, one frame per table,
// with no error and nothing dropped.
func TestQwpSplitFlushAllFitTablesFlushAcrossFrames(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true})
	defer srv.Close()

	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()
	require.Zero(t, s.serverMaxBatchSize.Load())

	ctx := context.Background()

	// Each table's own frame fits the 4 KiB segment comfortably, but the
	// combined frame across all of them overruns it — forcing the split
	// without any irreducible table.
	const (
		tableCount  = 8
		rowsPerTbl  = 4
		strBytesLen = 180
	)
	filler := strings.Repeat("y", strBytesLen)
	for tbl := 0; tbl < tableCount; tbl++ {
		name := fmt.Sprintf("tbl_%d", tbl)
		for r := 0; r < rowsPerTbl; r++ {
			require.NoError(t, s.Table(name).
				StringColumn("s", filler).
				Int64Column("i", int64(r)).
				AtNow(ctx), "%s row %d", name, r)
		}
	}
	require.Equal(t, tableCount*rowsPerTbl, s.pendingRowCount)

	publishedBefore := engine.enginePublishedFsn()

	// Sanity: the combined frame really does overrun the segment cap, so
	// this test exercises the split rather than the single-frame path.
	tables, err := s.buildTableEncodeInfo()
	require.NoError(t, err)
	combined := s.encoder.encodeMultiTableWithDeltaDict(tables, s.globalSymbolList, -1, s.batchMaxSymbolId)
	require.Greater(t, int64(len(combined)), s.maxFrameBytes,
		"test setup: combined frame must overrun the segment cap")

	require.NoError(t, s.Flush(ctx), "an all-fit batch must flush fully with no error")
	require.Zero(t, s.pendingRowCount)

	// One frame per table was published.
	require.Equal(t, publishedBefore+int64(tableCount), engine.enginePublishedFsn(),
		"each table should be published as its own frame")

	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 2*time.Second, time.Millisecond)

	var payloads []string
	for _, frames := range srv.recordedFrames() {
		payloads = append(payloads, frames...)
	}
	require.Len(t, payloads, tableCount, "server should receive one frame per table")
}
