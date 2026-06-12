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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQwpSegmentCapGuardDropsOversizeBatch is the regression test for
// the self-wedging cursor sender on the irreducible single-table case: a
// flush whose only table encodes to a frame larger than the per-segment
// byte cap must be DROPPED with a typed error, not retained forever.
//
// The per-table split can rescue a multi-table batch that overruns the
// cap only by aggregation (TestQwpSplitFlush* covers that), but a lone
// table over the cap is irreducible: the segment cap never grows, so
// re-encoding it on every subsequent Flush — and on Close — would fail
// identically forever and lose the batch anyway. This pins the
// recoverable behavior: the over-cap batch is dropped in place and the
// sender stays usable. Segment-cap analogue of TestQwpFlushTimeGuardFires.
func TestQwpSegmentCapGuardDropsOversizeBatch(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	// Memory-mode cursor with a 4096-byte segment and no auto-flush
	// (autoFlushRows=0; the constructor wires autoFlushBytes=0 and
	// maxBufSize=0). Every row stays pending until we explicitly Flush,
	// so the whole batch lands in a single frame.
	s, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// The SF test server advertises no X-QWP-Max-Batch-Size, so the
	// server-cap guards are inert and the 4 KiB segment is the only
	// binding limit.
	require.Zero(t, s.serverMaxBatchSize.Load(),
		"test precondition: no server cap, so the segment is the binding limit")

	ctx := context.Background()

	// ~20 KiB of column data — far past anything a 4 KiB segment can
	// hold even after rotation into a fresh spare.
	const rows = 100
	big := strings.Repeat("x", 200)
	for i := 0; i < rows; i++ {
		require.NoError(t, s.Table("t").
			StringColumn("s", big).
			Int64Column("i", int64(i)).
			AtNow(ctx), "row %d", i)
	}
	require.Equal(t, rows, s.pendingRowCount)

	// Flush must surface a typed error AND drop the batch.
	err := s.Flush(ctx)
	require.Error(t, err, "an over-segment batch must surface an error")
	require.Contains(t, err.Error(), fmt.Sprintf("droppedRows=%d", rows),
		"error must name the dropped row count")

	// Keystone: the batch was DROPPED, not retained. Pre-fix this stays
	// at `rows` and the sender is wedged.
	require.Zero(t, s.pendingRowCount, "over-segment batch must be dropped, not retained")
	require.Zero(t, s.pendingBytes, "pendingBytes must reset alongside the dropped batch")

	// A second Flush is a clean no-op — proving the wedge is gone (pre-fix
	// it re-failed identically forever).
	require.NoError(t, s.Flush(ctx), "second Flush must be a clean no-op after the drop")

	// The sender remains usable: a small batch flushes through the same
	// 4 KiB segment without error.
	require.NoError(t, s.Table("t").Int64Column("i", 1).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
	require.Zero(t, s.pendingRowCount)
}

// TestQwpSegmentCapGuardSurfacesOnClose pins the "data loss on Close"
// half of the report: an over-segment batch left pending at Close must
// be surfaced as a typed error (not silently lost) and Close must not
// hang or re-fail forever. closeCursor drops the batch via the same
// guard and returns the error as its first fault.
func TestQwpSegmentCapGuardSurfacesOnClose(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, _, _, _ := newCursorSenderForTest(t, srv, 0)

	ctx := context.Background()
	big := strings.Repeat("x", 200)
	for i := 0; i < 100; i++ {
		require.NoError(t, s.Table("t").StringColumn("s", big).AtNow(ctx), "row %d", i)
	}

	// Close returns promptly with the drop error — nothing was ever
	// published, so the drain wait is a no-op; the batch is dropped
	// rather than retained-and-re-failed.
	done := make(chan error, 1)
	go func() { done <- s.Close(ctx) }()
	select {
	case err := <-done:
		require.Error(t, err, "Close must surface the dropped batch, not swallow it")
		require.Contains(t, err.Error(), "cursor segment")
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung on an over-segment batch (wedge not fixed)")
	}
}

// TestQwpSegmentClampKeepsTriggerBelowSegmentCap pins the no-wedge
// invariant behind the byte-trigger clamp: when the configured
// auto_flush_bytes exceeds what a segment can hold (the shipped-default
// shape: 8 MiB trigger over a 4 MiB segment), the effective trigger is
// clamped strictly below the segment frame cap, so the soft auto-flush
// always fires before a batch can grow into the drop guard.
func TestQwpSegmentClampKeepsTriggerBelowSegmentCap(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// Configure a byte trigger larger than the 4 KiB segment — the
	// self-wedging shape — then re-seed the effective trigger (no server
	// cap advertised, so only the segment clamp applies).
	s.autoFlushBytes = 2 * 4096
	s.applyServerBatchSizeLimit(nil)

	require.Equal(t, int64(4096)-qwpSfHeaderSize-qwpSfFrameHeaderSize, s.maxFrameBytes)
	require.Equal(t, s.maxFrameBytes*9/10, s.effectiveAutoFlushBytes.Load(),
		"trigger must clamp to 90%% of the segment frame cap")
	require.Less(t, s.effectiveAutoFlushBytes.Load(), s.maxFrameBytes,
		"clamped trigger must sit below the segment cap so auto-flush fires first")
}

// TestQwpMaxFrameBytesMatchesSegmentBoundary pins the no-drift
// invariant the flush-time drop guard and the clamp both rely on:
// engineMaxFrameBytes() is exactly the largest payload a fresh segment
// accepts. A frame of that size fits; one byte more does not. If the
// segment header layout ever changes without engineMaxFrameBytes
// tracking it, this fails loudly instead of silently re-opening the
// wedge.
func TestQwpMaxFrameBytesMatchesSegmentBoundary(t *testing.T) {
	const segSize int64 = 4096
	maxFrame := segSize - qwpSfHeaderSize - qwpSfFrameHeaderSize

	eng, err := qwpSfNewCursorEngine("", segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = eng.engineClose() }()
	require.Equal(t, maxFrame, eng.engineMaxFrameBytes())

	fits, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	defer func() { _ = fits.close() }()
	_, err = fits.tryAppend(make([]byte, maxFrame))
	require.NoError(t, err, "a payload of exactly engineMaxFrameBytes must fit a fresh segment")

	overflows, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	defer func() { _ = overflows.close() }()
	_, err = overflows.tryAppend(make([]byte, maxFrame+1))
	require.ErrorIs(t, err, qwpSfErrSegmentFull, "one byte past engineMaxFrameBytes must not fit")
}

// TestQwpTooManyTablesDropsBatchInsteadOfWedging covers the other
// member of the retain-forever family the report named: a batch with
// more than qwpMaxTablesPerBatch (65535, the uint16 table-count limit)
// distinct tables can never be encoded, so it is dropped with a typed
// error instead of being retained and re-failing on every flush.
func TestQwpTooManyTablesDropsBatchInsteadOfWedging(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	ctx := context.Background()
	// One row into each of (cap + 1) distinct tables — one past the
	// uint16 table-count limit.
	const tables = qwpMaxTablesPerBatch + 1
	for i := 0; i < tables; i++ {
		require.NoError(t, s.Table("t"+strconv.Itoa(i)).
			Int64Column("v", int64(i)).
			AtNow(ctx))
	}
	require.Equal(t, tables, s.pendingRowCount)

	err := s.Flush(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many tables")
	require.Contains(t, err.Error(), "droppedRows=")

	// Dropped, not wedged.
	require.Zero(t, s.pendingRowCount)

	// Sender stays usable.
	require.NoError(t, s.Table("ok").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
	require.Zero(t, s.pendingRowCount)
}
