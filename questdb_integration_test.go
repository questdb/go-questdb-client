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
	"testing"
	"time"
)

// TestQwpIntegrationFacadeRoundTrip drives the QuestDB facade against a real
// server: borrow a sender, write rows, then borrow a query session and read
// them back — proving the facade wires both pools to one cluster config.
// TestQwpIntegration* so the qwp-fuzz.yml server-bound -run filters select it (M3).
func TestQwpIntegrationFacadeRoundTrip(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()
	table := fmt.Sprintf("facade_rt_%d", time.Now().UnixNano())
	t.Cleanup(func() { qwpDropTable(t, table) })

	db, err := NewQuestDB(ctx, "ws::addr="+qwpTestAddr+";",
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer db.Close(ctx)

	const rows = 5
	s, err := db.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("BorrowSender: %v", err)
	}
	for i := 0; i < rows; i++ {
		if err := s.Table(table).Symbol("sym", "BTC").Int64Column("qty", int64(i)).AtNow(ctx); err != nil {
			t.Fatalf("write row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("sender close: %v", err)
	}

	// Ingest landed (verified out-of-band over HTTP).
	res := qwpWaitForRows(t, table, rows)
	if res.Count < rows {
		t.Fatalf("rows landed = %d, want >= %d", res.Count, rows)
	}

	// Read it back through the facade's query pool.
	q, err := db.BorrowQuery(ctx)
	if err != nil {
		t.Fatalf("BorrowQuery: %v", err)
	}
	defer q.Close()
	cursor := q.Query(ctx, "select count() from "+table)
	batches, gotCount := 0, int64(-1)
	for batch, err := range cursor.Batches() {
		if err != nil {
			t.Fatalf("query batch: %v", err)
		}
		if batch.RowCount() != 1 {
			t.Errorf("count() batch RowCount=%d, want 1", batch.RowCount())
		}
		// count() is one LONG cell; read it so the read path is asserted for
		// content, not just shape (RowCount()==1 holds even for a wrong value).
		gotCount = batch.Int64(0, 0)
		batches++
	}
	if batches == 0 {
		t.Fatal("query returned no batches")
	}
	if gotCount != rows {
		t.Errorf("count() via query pool = %d, want %d", gotCount, rows)
	}

	// Cover the leased-handle Exec path: a DROP IF EXISTS on a missing table
	// returns EXEC_DONE (a non-SELECT statement).
	if _, err := q.Exec(ctx, "drop table if exists facade_exec_probe_missing"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

// TestQwpIntegrationFacadeQueryWireReuse proves a query abandoned mid-iteration
// does not leak leftover RESULT_BATCH frames into the next borrower on a reused
// (max=1) query worker: the lease return drains the wire clean or evicts the
// worker, so the follow-up query reads correct results.
func TestQwpIntegrationFacadeQueryWireReuse(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()
	table := fmt.Sprintf("facade_reuse_%d", time.Now().UnixNano())
	t.Cleanup(func() { qwpDropTable(t, table) })

	// One pooled query worker so the follow-up borrow provably reuses (or
	// evict-replaces) the same slot; max_batch_rows=1 makes the SELECT stream
	// one row per batch, so an early break leaves many undrained batches + END.
	db, err := NewQuestDB(ctx,
		"ws::addr="+qwpTestAddr+";query_pool_min=1;query_pool_max=1;max_batch_rows=1;",
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer db.Close(ctx)

	const rows = 20
	s, err := db.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("BorrowSender: %v", err)
	}
	for i := 0; i < rows; i++ {
		if err := s.Table(table).Symbol("sym", "BTC").Int64Column("qty", int64(i)).AtNow(ctx); err != nil {
			t.Fatalf("write row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("sender close: %v", err)
	}
	qwpWaitForRows(t, table, rows)

	// Start a multi-batch SELECT, then abandon after the first batch.
	q1, err := db.BorrowQuery(ctx)
	if err != nil {
		t.Fatalf("BorrowQuery q1: %v", err)
	}
	got := 0
	for _, berr := range q1.Query(ctx, "select qty from "+table).Batches() {
		if berr != nil {
			t.Fatalf("q1 batch: %v", berr)
		}
		got++
		break // batches 2..N + END stay undrained on the wire
	}
	if got == 0 {
		t.Fatal("q1 returned no batches")
	}
	if err := q1.Close(); err != nil {
		t.Fatalf("q1 lease close: %v", err)
	}

	// Re-borrow the (reused or evict-replaced) worker and read the full count;
	// a leaked frame from the abandoned q1 would corrupt this result.
	q2, err := db.BorrowQuery(ctx)
	if err != nil {
		t.Fatalf("BorrowQuery q2: %v", err)
	}
	defer q2.Close()
	gotCount := int64(-1)
	for batch, berr := range q2.Query(ctx, "select count() from "+table).Batches() {
		if berr != nil {
			t.Fatalf("q2 batch: %v", berr)
		}
		gotCount = batch.Int64(0, 0)
	}
	if gotCount != rows {
		t.Fatalf("count() after abandoned query = %d, want %d (leaked frames from the abandoned q1?)", gotCount, rows)
	}
}

// TestQwpIntegrationFacadeLazyConnect proves lazy_connect builds against a
// (reachable) server without prewarming the read pool, then ingests and reads.
func TestQwpIntegrationFacadeLazyConnect(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()
	table := fmt.Sprintf("facade_lazy_%d", time.Now().UnixNano())
	t.Cleanup(func() { qwpDropTable(t, table) })

	db, err := NewQuestDB(ctx, "ws::addr="+qwpTestAddr+";lazy_connect=true;",
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("Connect(lazy): %v", err)
	}
	defer db.Close(ctx)
	if total, _ := db.queryPool.poolSnapshot(); total != 0 {
		t.Errorf("lazy read pool prewarmed total=%d, want 0", total)
	}

	s, err := db.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("BorrowSender: %v", err)
	}
	if err := s.Table(table).Int64Column("v", 1).AtNow(ctx); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	s.Close(ctx)
	qwpWaitForRows(t, table, 1)

	// First query connects the lazy read pool on demand.
	q, err := db.BorrowQuery(ctx)
	if err != nil {
		t.Fatalf("BorrowQuery (lazy connect): %v", err)
	}
	defer q.Close()
	if total, _ := db.queryPool.poolSnapshot(); total != 1 {
		t.Errorf("read pool after first borrow total=%d, want 1", total)
	}
}
