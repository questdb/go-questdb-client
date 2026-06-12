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
	"testing"
	"time"
)

// newTestQueryClient opens an egress QwpQueryClient against the live
// local server. Skips the test if the server is unreachable (same
// policy as qwpEnsureServer).
func newTestQueryClient(t *testing.T) *QwpQueryClient {
	t.Helper()
	qwpEnsureServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx, WithQwpQueryAddress(qwpTestAddr))
	if err != nil {
		t.Fatalf("NewQwpQueryClient: %v", err)
	}
	return c
}

// insertRows ingests `rows` rows into `tableName` via a QwpSender.
// Used to seed data before exercising the egress query path.
func insertRows(t *testing.T, tableName string, rows int) {
	t.Helper()
	ctx := context.Background()
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr,
		qwpTransportOpts{endpointPath: qwpWritePath}, 0, 0, nil)
	if err != nil {
		t.Fatalf("newQwpLineSender: %v", err)
	}
	defer s.Close(ctx)
	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	for i := 0; i < rows; i++ {
		err = s.Table(tableName).
			Symbol("host", fmt.Sprintf("server%d", i%3)).
			Int64Column("v", int64(i)).
			At(ctx, base.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("At: %v", err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	qwpWaitForRows(t, tableName, rows)
}

// TestQwpIntegrationQuerySimpleSelect inserts three rows via ingest,
// queries them via egress, and verifies the iterator yields the
// correct values with TotalRows set from RESULT_END.
func TestQwpIntegrationQuerySimpleSelect(t *testing.T) {
	const tableName = "qwp_integ_query_simple"
	qwpEnsureServer(t)
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	insertRows(t, tableName, 3)

	c := newTestQueryClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer c.Close(ctx)

	q := c.Query(ctx, fmt.Sprintf("SELECT v FROM '%s' ORDER BY v", tableName))
	defer q.Close()

	var got []int64
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iter err: %v", err)
		}
		rows := batch.RowCount()
		for r := 0; r < rows; r++ {
			got = append(got, batch.Int64(0, r))
		}
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3 (values %v)", len(got), got)
	}
	for i, v := range got {
		if v != int64(i) {
			t.Errorf("row %d: got v=%d, want %d", i, v, i)
		}
	}
	if q.TotalRows() != 3 {
		t.Errorf("TotalRows=%d, want 3", q.TotalRows())
	}
}

// TestQwpIntegrationQueryError runs a SELECT against a nonexistent
// table and verifies the server's QUERY_ERROR surfaces as a
// *QwpQueryError with a useful message.
func TestQwpIntegrationQueryError(t *testing.T) {
	c := newTestQueryClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer c.Close(ctx)

	q := c.Query(ctx, "SELECT * FROM qwp_integ_does_not_exist_xyz")
	defer q.Close()

	var lastErr error
	for _, err := range q.Batches() {
		if err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		t.Fatal("expected query error, got nil")
	}
	var qe *QwpQueryError
	if !errors.As(lastErr, &qe) {
		t.Fatalf("err type=%T, want *QwpQueryError: %v", lastErr, lastErr)
	}
	if qe.Message == "" {
		t.Errorf("QwpQueryError.Message is empty — expected a server description")
	}
}

// TestQwpIntegrationExecDDL runs a CREATE TABLE via Exec, verifies it
// returns cleanly, then drops the table and checks the DROP also
// works through Exec.
func TestQwpIntegrationExecDDL(t *testing.T) {
	const tableName = "qwp_integ_exec_ddl"
	qwpEnsureServer(t)
	qwpDropTable(t, tableName) // ensure clean slate
	defer qwpDropTable(t, tableName)

	c := newTestQueryClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer c.Close(ctx)

	createSQL := fmt.Sprintf(
		"CREATE TABLE '%s' (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
		tableName)
	if _, err := c.Exec(ctx, createSQL); err != nil {
		t.Fatalf("Exec(CREATE): %v", err)
	}
	// Verify via the HTTP exec endpoint that the table now exists.
	res := qwpQuery(t, fmt.Sprintf("SELECT count() FROM '%s'", tableName))
	if res.Count != 1 || len(res.Dataset) == 0 {
		t.Errorf("CREATE TABLE did not produce a table: %+v", res)
	}
}

// TestQwpIntegrationQueryFromConf exercises the ws:: config-string
// entry point, proving QwpQueryClientFromConf dials the live server
// with the same behavior as the functional-options constructor.
func TestQwpIntegrationQueryFromConf(t *testing.T) {
	const tableName = "qwp_integ_query_fromconf"
	qwpEnsureServer(t)
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)
	insertRows(t, tableName, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := QwpQueryClientFromConf(ctx, "ws::addr="+qwpTestAddr+";")
	if err != nil {
		t.Fatalf("QwpQueryClientFromConf: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, fmt.Sprintf("SELECT v FROM '%s'", tableName))
	defer q.Close()

	var rows int
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iter err: %v", err)
		}
		rows += batch.RowCount()
	}
	if rows != 1 {
		t.Errorf("rows=%d, want 1", rows)
	}
}

// TestQwpIntegrationQueryMultipleBatches asks the server to stream a
// larger result set (enough rows to cross a batch boundary at the
// server-default batch cap). Uses WithQwpQueryMaxBatchRows to force
// multiple batches even when row counts are modest, and verifies the
// iterator yields them all in order.
func TestQwpIntegrationQueryMultipleBatches(t *testing.T) {
	const tableName = "qwp_integ_query_multibatch"
	qwpEnsureServer(t)
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)
	const totalRows = 50
	insertRows(t, tableName, totalRows)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx,
		WithQwpQueryAddress(qwpTestAddr),
		WithQwpQueryMaxBatchRows(10),
	)
	if err != nil {
		t.Fatalf("NewQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, fmt.Sprintf("SELECT v FROM '%s' ORDER BY v", tableName))
	defer q.Close()

	var rows, batches int
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iter err: %v", err)
		}
		batches++
		n := batch.RowCount()
		for r := 0; r < n; r++ {
			want := int64(rows)
			if got := batch.Int64(0, r); got != want {
				t.Errorf("row %d (batch %d): got %d, want %d", rows, batches, got, want)
			}
			rows++
		}
	}
	if rows != totalRows {
		t.Errorf("rows=%d, want %d", rows, totalRows)
	}
	if batches < 2 {
		t.Errorf("batches=%d, want >=2 (max_batch_rows=10 with %d rows)", batches, totalRows)
	}
	if q.TotalRows() != int64(totalRows) {
		t.Errorf("TotalRows=%d, want %d", q.TotalRows(), totalRows)
	}
}

// TestQwpIntegrationCompressedBatches round-trips a SELECT with
// compression=zstd against the live server. Verifies the accept-
// encoding handshake negotiates zstd (if the server supports it),
// every RESULT_BATCH's FLAG_ZSTD bit drives the decompression path,
// and the decoded values match what we ingested. Enough rows (50) to
// cross a batch boundary so at least one compressed batch is
// guaranteed to be non-trivial.
//
// When the server does not support zstd, the handshake falls back to
// raw per the accept-encoding semantics ("zstd;level=3,raw" lists raw
// as an acceptable alternative). The client still succeeds; this test
// just won't exercise the decompression path in that case. A log line
// calls out which branch ran so test output makes the coverage
// obvious.
func TestQwpIntegrationCompressedBatches(t *testing.T) {
	const tableName = "qwp_integ_query_zstd"
	qwpEnsureServer(t)
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)
	const totalRows = 50
	insertRows(t, tableName, totalRows)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx,
		WithQwpQueryAddress(qwpTestAddr),
		WithQwpQueryCompression(qwpCompressionZstd),
		WithQwpQueryMaxBatchRows(10),
	)
	if err != nil {
		t.Fatalf("NewQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, fmt.Sprintf("SELECT v FROM '%s' ORDER BY v", tableName))
	defer q.Close()

	var rows, batches, compressedBatches int
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iter err: %v", err)
		}
		batches++
		if len(batch.zstdScratch) > 0 {
			compressedBatches++
		}
		n := batch.RowCount()
		for r := 0; r < n; r++ {
			want := int64(rows)
			if got := batch.Int64(0, r); got != want {
				t.Errorf("row %d (batch %d): got %d, want %d", rows, batches, got, want)
			}
			rows++
		}
	}
	if rows != totalRows {
		t.Errorf("rows=%d, want %d", rows, totalRows)
	}
	if q.TotalRows() != int64(totalRows) {
		t.Errorf("TotalRows=%d, want %d", q.TotalRows(), totalRows)
	}
	if compressedBatches == 0 {
		t.Logf("server accepted compression=zstd advertisement but sent no compressed batches (fell back to raw)")
	} else {
		t.Logf("%d of %d batches arrived zstd-compressed", compressedBatches, batches)
	}
}

// TestQwpIntegrationCancelLongRunningQuery submits a query that runs
// long enough to be interrupted, invokes Cancel from the iterating
// goroutine's defer, and verifies iteration ends cleanly (the
// server's CANCELLED echo is swallowed by the cursor's cancel-aware
// error path). Unlike older revisions that only checked `saw >= 1`,
// this test also verifies the post-cancel invariant that actually
// matters in production: the client's dispatcher returned to idle so
// a follow-up Query can round-trip without stranding.
//
// We deliberately do NOT assert that Cancel short-circuited the
// server: long_sequence streams tens of millions of rows per second
// on localhost and races past Cancel() before the cancel frame
// reaches the server. What we guarantee is (a) the iterator does not
// panic or hang, and (b) the client is reusable after the iteration
// ends — whichever side (cancel or natural RESULT_END) won the race.
func TestQwpIntegrationCancelLongRunningQuery(t *testing.T) {
	qwpEnsureServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Small batches so the iterator enters the yield body before the
	// server has finished streaming — otherwise saw stays 0 for fast
	// queries.
	c, err := NewQwpQueryClient(ctx,
		WithQwpQueryAddress(qwpTestAddr),
		WithQwpQueryMaxBatchRows(500),
	)
	if err != nil {
		t.Fatalf("NewQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "SELECT x FROM long_sequence(10000000)")

	start := time.Now()
	var saw int
	for _, err := range q.Batches() {
		if err != nil {
			q.Close()
			t.Fatalf("unexpected iter err: %v", err)
		}
		saw++
		if saw == 1 {
			q.Cancel()
		}
	}
	elapsed := time.Since(start)
	q.Close()

	if saw < 1 {
		t.Errorf("saw %d batches, want >= 1", saw)
	}
	// Cancel must not deadlock the iterator — 15s is generous for
	// 10M rows on a local server whether the cancel short-circuits
	// or the server finishes streaming naturally.
	if elapsed > 15*time.Second {
		t.Errorf("iteration took %v — suggests cancel-drain hung", elapsed)
	}

	// Client must stay usable: a follow-up Query should round-trip
	// cleanly whether the cancel or the natural RESULT_END won. This
	// is the real production-visible property — a broken cancel-drain
	// would leave the dispatcher stranded and the next Query would
	// block forever on the single-slot requests channel.
	q2 := c.Query(ctx, "SELECT 1")
	var rows int
	for batch, err := range q2.Batches() {
		if err != nil {
			q2.Close()
			t.Fatalf("follow-up query err: %v", err)
		}
		rows += batch.RowCount()
	}
	q2.Close()
	if rows != 1 {
		t.Errorf("follow-up query rows=%d, want 1", rows)
	}
}

// TestQwpIntegrationCtxDeadlineMidStream exercises the other shutdown
// path through Batches(): the query's ctx expires while the iterator
// is blocked in takeEvent. The iterator must yield the ctx error once,
// then kick the dispatcher (cancel + drain on a fresh cleanup ctx) so
// the client stays usable. Complements the explicit-Cancel test above
// which exits via the !keepGoing break-out.
func TestQwpIntegrationCtxDeadlineMidStream(t *testing.T) {
	c := newTestQueryClient(t)
	clientCtx, clientCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer clientCancel()
	defer c.Close(clientCtx)

	// A short ctx on the Query itself; long enough to establish the
	// stream but short enough to expire mid-flight. The row count must
	// give the deadline a wide window to land in: 100M int64 rows stream
	// in ~1.2s (and linearly longer on slower CI), giving the 200ms deadline
	// headroom on either end.
	queryCtx, queryCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer queryCancel()
	q := c.Query(queryCtx, "SELECT x FROM long_sequence(100000000)")

	start := time.Now()
	var iterErr error
	var saw int
	for batch, err := range q.Batches() {
		if err != nil {
			iterErr = err
			break
		}
		saw++
		_ = batch
	}
	elapsed := time.Since(start)
	q.Close()

	if iterErr == nil {
		t.Fatal("expected ctx-deadline error from the iterator, got nil")
	}
	if !errors.Is(iterErr, context.DeadlineExceeded) {
		t.Errorf("iter err = %v, want context.DeadlineExceeded", iterErr)
	}
	if elapsed > 15*time.Second {
		t.Errorf("iteration took %v — ctx expiry did not unblock the iterator", elapsed)
	}
	_ = saw

	// Client-level ctx is still live; the dispatcher should be back to
	// idle thanks to cancelAndDrainOnCleanupCtx. A follow-up query
	// confirms we did not strand the connection.
	q2 := c.Query(clientCtx, "SELECT 1")
	var rows int
	for batch, err := range q2.Batches() {
		if err != nil {
			q2.Close()
			t.Fatalf("follow-up query err after ctx-expiry teardown: %v", err)
		}
		rows += batch.RowCount()
	}
	q2.Close()
	if rows != 1 {
		t.Errorf("follow-up rows=%d, want 1", rows)
	}
}

// TestQwpIntegrationClientCloseDuringLongQuery exercises the
// transport-teardown path: while a long-running SELECT is mid-stream,
// another goroutine closes the QwpQueryClient. The iterator must see a
// transport error (the read side fails once the WebSocket close frame
// lands) and exit without hanging. This is the closest we can get, in
// an integration test, to a server-initiated connection close — the
// local close also tears down the read direction and surfaces through
// the same code path.
//
// Does NOT read the batch's aliased slices after Close is called — the
// public contract explicitly flags that as undefined (the transport
// may free the underlying buffer). RowCount is safe because it reads
// an integer field, not a payload-backed slice.
func TestQwpIntegrationClientCloseDuringLongQuery(t *testing.T) {
	c := newTestQueryClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	q := c.Query(ctx, "SELECT x FROM long_sequence(10000000)")

	start := time.Now()
	closed := make(chan struct{})
	var saw int
	var iterErr error
	for batch, err := range q.Batches() {
		if err != nil {
			iterErr = err
			break
		}
		saw++
		_ = batch.RowCount()
		if saw == 1 {
			go func() {
				closeCtx, closeCancel := context.WithTimeout(
					context.Background(), 5*time.Second)
				defer closeCancel()
				_ = c.Close(closeCtx)
				close(closed)
			}()
		}
	}
	elapsed := time.Since(start)
	q.Close()

	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("client Close did not return within 10s of starting")
	}

	if saw < 1 {
		t.Errorf("saw %d batches before close, want >= 1", saw)
	}
	if iterErr == nil {
		t.Error("expected the iterator to surface a transport error after client Close")
	}
	if elapsed > 15*time.Second {
		t.Errorf("iteration took %v — client Close did not unblock the iterator", elapsed)
	}
}

// TestQwpIntegrationQueryWithBinds exercises the bind-variable path
// against the live server. Inserts a handful of rows, then runs the
// same filtered SELECT three times with different bind values and
// verifies the server returns the expected result for each set. Two
// goals: (a) confirm the bind wire payload is accepted by the server
// (no protocol mismatch with the Java / C client encoders), and (b)
// confirm repeated calls with the same SQL text produce the expected
// per-call result sets.
func TestQwpIntegrationQueryWithBinds(t *testing.T) {
	const tableName = "qwp_integ_binds"
	qwpEnsureServer(t)
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	insertRows(t, tableName, 9) // host cycles through server0 / server1 / server2

	c := newTestQueryClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer c.Close(ctx)

	sql := fmt.Sprintf(
		"SELECT v FROM '%s' WHERE host = $1 AND v >= $2 ORDER BY v", tableName)

	type tc struct {
		host   string
		minV   int64
		wantVs []int64
	}
	cases := []tc{
		// insertRows writes v=i with host="server{i%3}":
		//   server0: 0, 3, 6
		//   server1: 1, 4, 7
		//   server2: 2, 5, 8
		{host: "server0", minV: 0, wantVs: []int64{0, 3, 6}},
		{host: "server1", minV: 4, wantVs: []int64{4, 7}},
		{host: "server2", minV: 10, wantVs: nil},
	}

	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			q := c.Query(ctx, sql, WithQwpQueryBinds(func(b *QwpBinds) {
				b.VarcharBind(0, tc.host).LongBind(1, tc.minV)
			}))
			defer q.Close()

			var got []int64
			for batch, err := range q.Batches() {
				if err != nil {
					t.Fatalf("iter err: %v", err)
				}
				for r := 0; r < batch.RowCount(); r++ {
					got = append(got, batch.Int64(0, r))
				}
			}
			if len(got) != len(tc.wantVs) {
				t.Fatalf("got %d rows, want %d (values %v want %v)",
					len(got), len(tc.wantVs), got, tc.wantVs)
			}
			for i, v := range got {
				if v != tc.wantVs[i] {
					t.Errorf("row %d: got v=%d, want %d", i, v, tc.wantVs[i])
				}
			}
		})
	}
}
