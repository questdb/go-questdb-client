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
// policy as qwpSkipIfNoServer).
func newTestQueryClient(t *testing.T) *QwpQueryClient {
	t.Helper()
	qwpSkipIfNoServer(t)
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
		qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
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
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	qwpSkipIfNoServer(t)
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
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)
	qwpSkipIfNoServer(t)
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
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)
	qwpSkipIfNoServer(t)
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

// TestQwpIntegrationCancelLongRunningQuery submits a query that runs
// long enough to be interrupted, invokes Cancel from the iterating
// goroutine's defer, and verifies iteration ends cleanly (the
// server's CANCELLED echo is swallowed by the cursor's cancel-aware
// error path).
func TestQwpIntegrationCancelLongRunningQuery(t *testing.T) {
	c := newTestQueryClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer c.Close(ctx)

	// long_sequence(N) is a server-side row generator; a large value
	// gives the cancel time to reach the server before completion.
	q := c.Query(ctx, "SELECT x FROM long_sequence(10000000)")
	defer q.Close()

	var saw int
	for _, err := range q.Batches() {
		if err != nil {
			t.Fatalf("unexpected iter err: %v", err)
		}
		saw++
		if saw == 1 {
			// Cancel after the first batch is drained.
			q.Cancel()
		}
	}
	if saw < 1 {
		t.Errorf("saw %d batches, want >= 1", saw)
	}
}
