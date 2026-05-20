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
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	qwpTestWaitPeriod = 5 * time.Second
	qwpTestPollPeriod = 100 * time.Millisecond
)

// qwpTestAddr is the host:port the QWP integration tests target. It
// used to be a const pinned to localhost:9000 (a developer's live
// server), which caused these tests to silently skip in CI where no
// such server runs. qwpEnsureServer now boots the shared fuzz
// fixture and writes the fixture's address here, so the same tests
// run against a real QuestDB under qwp-fuzz.yml (and any QDB_FUZZ_ADDR
// the developer points at on their machine, including localhost:9000).
var qwpTestAddr string

var qwpTestHTTPClient = &http.Client{Timeout: qwpTestWaitPeriod}

// qwpTableResult holds query results from QuestDB's /exec endpoint.
type qwpTableResult struct {
	Columns []qwpColumnInfo  `json:"columns"`
	Dataset [][]interface{}  `json:"dataset"`
	Count   int              `json:"count"`
	Query   string           `json:"query"`
}

type qwpColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// qwpEnsureServer ensures a real QuestDB is reachable for the
// caller's integration test and writes its host:port into the
// package-level qwpTestAddr.
//
// Resolution policy (matches the fuzz fixture):
//   1. QDB_FUZZ_ADDR — talk to an externally-managed server (a
//      developer's live localhost:9000, or a long-lived CI box).
//   2. Otherwise boot a private QuestDB JVM from a QDB_JAR / QDB_REPO
//      / sibling questdb checkout. Auto-runs under qwp-fuzz.yml.
//   3. If neither resolves, t.Skip (unless QDB_FUZZ_STRICT=1, in which
//      case t.Fatal so CI loudly fails instead of silently passing).
//
// As a side effect the caller's subsequent qwpQuery / qwpDropTable /
// "ws://"+qwpTestAddr connect strings all target the resolved server.
func qwpEnsureServer(t *testing.T) {
	t.Helper()
	srv := fuzzServer(t)
	qwpTestAddr = srv.wsAddr()
}

// qwpDropTable drops a table via QuestDB's HTTP API.
func qwpDropTable(t *testing.T, tableName string) {
	t.Helper()
	u, _ := url.Parse("http://" + qwpTestAddr)
	u.Path = "/exec"
	params := url.Values{}
	params.Add("query", "DROP TABLE IF EXISTS '"+tableName+"';")
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
	if err != nil {
		t.Logf("warning: could not build request to drop table %q: %v", tableName, err)
		return
	}
	resp, err := qwpTestHTTPClient.Do(req)
	if err != nil {
		t.Logf("warning: could not drop table %q: %v", tableName, err)
		return
	}
	_ = resp.Body.Close()
}

// qwpQuery executes a SQL query against QuestDB's HTTP API.
func qwpQuery(t *testing.T, query string) qwpTableResult {
	t.Helper()
	u, _ := url.Parse("http://" + qwpTestAddr)
	u.Path = "/exec"
	params := url.Values{}
	params.Add("query", query)
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := qwpTestHTTPClient.Do(req)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var result qwpTableResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse response: %v (body: %s)", err, string(body))
	}
	return result
}

// qwpWaitForRows polls until the table has the expected row count.
func qwpWaitForRows(t *testing.T, tableName string, expectedRows int) qwpTableResult {
	t.Helper()
	deadline := time.Now().Add(qwpTestWaitPeriod)
	for time.Now().Before(deadline) {
		result := qwpQuery(t, fmt.Sprintf("SELECT * FROM '%s'", tableName))
		if result.Count >= expectedRows {
			return result
		}
		time.Sleep(qwpTestPollPeriod)
	}
	t.Fatalf("timeout waiting for %d rows in table %q", expectedRows, tableName)
	return qwpTableResult{}
}

// --- Basic integration test ---

func TestQwpIntegrationBasicTypes(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_basic_types"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	err = s.Table(tableName).
		Symbol("host", "server1").
		Int64Column("cpu", 42).
		Float64Column("mem", 85.5).
		StringColumn("msg", "hello world").
		BoolColumn("active", true).
		TimestampColumn("event_ts", ts).
		At(ctx, ts)
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	err = s.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 1)

	if result.Count != 1 {
		t.Fatalf("count = %d, want 1", result.Count)
	}

	// Verify column names exist.
	colNames := make(map[string]string)
	for _, c := range result.Columns {
		colNames[c.Name] = c.Type
	}

	expectedCols := map[string]string{
		"host":      "SYMBOL",
		"cpu":       "LONG",
		"mem":       "DOUBLE",
		"msg":       "VARCHAR",
		"active":    "BOOLEAN",
		"event_ts":  "TIMESTAMP",
		"timestamp": "TIMESTAMP",
	}

	for name, expectedType := range expectedCols {
		actualType, ok := colNames[name]
		if !ok {
			t.Errorf("missing column %q", name)
			continue
		}
		if actualType != expectedType {
			t.Errorf("column %q type = %q, want %q", name, actualType, expectedType)
		}
	}

	t.Log("QWP basic types integration test passed")
}

// --- Multi-row, multi-flush test ---

func TestQwpIntegrationMultipleFlushes(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_multi_flush"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Flush 1: 3 rows.
	for i := 0; i < 3; i++ {
		err = s.Table(tableName).
			Symbol("sym", fmt.Sprintf("s%d", i)).
			Int64Column("val", int64(i)).
			At(ctx, ts.Add(time.Duration(i)*time.Microsecond))
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	// Flush 2: 2 more rows (schema reference mode).
	for i := 3; i < 5; i++ {
		err = s.Table(tableName).
			Symbol("sym", fmt.Sprintf("s%d", i)).
			Int64Column("val", int64(i)).
			At(ctx, ts.Add(time.Duration(i)*time.Microsecond))
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 5)
	if result.Count != 5 {
		t.Fatalf("count = %d, want 5", result.Count)
	}

	t.Log("QWP multiple flushes integration test passed")
}

// --- Symbol deduplication test ---

func TestQwpIntegrationSymbolDedup(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_symbol_dedup"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Flush 1: symbols AAPL, MSFT.
	if err := s.Table(tableName).Symbol("sym", "AAPL").Int64Column("v", 1).At(ctx, ts); err != nil {
		t.Fatalf("row AAPL: %v", err)
	}
	if err := s.Table(tableName).Symbol("sym", "MSFT").Int64Column("v", 2).At(ctx, ts.Add(time.Microsecond)); err != nil {
		t.Fatalf("row MSFT: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	// Flush 2: reuse AAPL, add GOOG.
	if err := s.Table(tableName).Symbol("sym", "AAPL").Int64Column("v", 3).At(ctx, ts.Add(2*time.Microsecond)); err != nil {
		t.Fatalf("row AAPL (flush 2): %v", err)
	}
	if err := s.Table(tableName).Symbol("sym", "GOOG").Int64Column("v", 4).At(ctx, ts.Add(3*time.Microsecond)); err != nil {
		t.Fatalf("row GOOG: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 4)
	if result.Count != 4 {
		t.Fatalf("count = %d, want 4", result.Count)
	}

	t.Log("QWP symbol dedup integration test passed")
}

// --- Multi-table batch test ---

func TestQwpIntegrationMultiTable(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	table1 := "qwp_integ_multi_t1"
	table2 := "qwp_integ_multi_t2"
	qwpDropTable(t, table1)
	qwpDropTable(t, table2)
	defer qwpDropTable(t, table1)
	defer qwpDropTable(t, table2)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Interleave rows into two tables.
	if err := s.Table(table1).Int64Column("x", 1).At(ctx, ts); err != nil {
		t.Fatalf("table1 row 1: %v", err)
	}
	if err := s.Table(table2).Float64Column("y", 2.5).At(ctx, ts); err != nil {
		t.Fatalf("table2 row 1: %v", err)
	}
	if err := s.Table(table1).Int64Column("x", 3).At(ctx, ts.Add(time.Microsecond)); err != nil {
		t.Fatalf("table1 row 2: %v", err)
	}

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r1 := qwpWaitForRows(t, table1, 2)
	r2 := qwpWaitForRows(t, table2, 1)

	if r1.Count != 2 {
		t.Fatalf("table1 count = %d, want 2", r1.Count)
	}
	if r2.Count != 1 {
		t.Fatalf("table2 count = %d, want 1", r2.Count)
	}

	t.Log("QWP multi-table integration test passed")
}

// --- Large batch test ---

func TestQwpIntegrationLargeBatch(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_large_batch"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	const rowCount = 10000
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	for i := 0; i < rowCount; i++ {
		err = s.Table(tableName).
			Symbol("tag", fmt.Sprintf("t%d", i%10)).
			Int64Column("val", int64(i)).
			Float64Column("score", float64(i)*0.1).
			At(ctx, ts.Add(time.Duration(i)*time.Microsecond))
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, rowCount)
	if result.Count != rowCount {
		t.Fatalf("count = %d, want %d", result.Count, rowCount)
	}

	t.Log("QWP large batch (10k rows) integration test passed")
}

// --- Config string creation test ---

func TestQwpIntegrationFromConf(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_from_conf"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	confStr := fmt.Sprintf("ws::addr=%s;auto_flush=off;retry_timeout=1000;", qwpTestAddr)
	sender, err := LineSenderFromConf(ctx, confStr)
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer sender.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	err = sender.Table(tableName).
		Symbol("src", "conf").
		Int64Column("v", 99).
		At(ctx, ts)
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	if err := sender.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 1)
	if result.Count != 1 {
		t.Fatalf("count = %d, want 1", result.Count)
	}

	t.Log("QWP LineSenderFromConf integration test passed")
}

// --- Async mode integration test ---

func TestQwpIntegrationAsyncMode(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_async"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// Create sender with in-flight window = 4.
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 0, 0, nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	if s.cursorEngine == nil || s.cursorSendLoop == nil {
		t.Fatal("expected cursor engine + send loop to be wired")
	}

	const rowCount = 1000
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	for i := 0; i < rowCount; i++ {
		err = s.Table(tableName).
			Symbol("tag", fmt.Sprintf("t%d", i%5)).
			Int64Column("val", int64(i)).
			At(ctx, ts.Add(time.Duration(i)*time.Microsecond))
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, rowCount)
	if result.Count != rowCount {
		t.Fatalf("count = %d, want %d", result.Count, rowCount)
	}

	t.Log("QWP async mode integration test passed")
}

// --- Async mode via config string ---

func TestQwpIntegrationAsyncFromConf(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_async_conf"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	confStr := fmt.Sprintf("ws::addr=%s;auto_flush=off;in_flight_window=2;", qwpTestAddr)
	sender, err := LineSenderFromConf(ctx, confStr)
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer sender.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 10; i++ {
		err = sender.Table(tableName).
			Int64Column("v", int64(i)).
			At(ctx, ts.Add(time.Duration(i)*time.Microsecond))
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	if err := sender.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 10)
	if result.Count != 10 {
		t.Fatalf("count = %d, want 10", result.Count)
	}

	t.Log("QWP async from conf integration test passed")
}

// --- Auto-flush integration test ---

func TestQwpIntegrationAutoFlush(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_autoflush"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// auto-flush every 3 rows.
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 3, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Send 5 rows — should auto-flush at row 3.
	for i := 0; i < 5; i++ {
		err = s.Table(tableName).
			Int64Column("v", int64(i)).
			At(ctx, ts.Add(time.Duration(i)*time.Microsecond))
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	// Only 2 rows should be pending (3 were auto-flushed).
	if s.pendingRowCount != 2 {
		t.Fatalf("pendingRowCount = %d, want 2", s.pendingRowCount)
	}

	// Flush the remaining 2.
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 5)
	if result.Count != 5 {
		t.Fatalf("count = %d, want 5", result.Count)
	}

	t.Log("QWP auto-flush integration test passed")
}

// TestQwpIntegrationNullableColumns verifies that nullable columns
// with interleaved null and non-null values are correctly encoded,
// sent via QWP, and stored in QuestDB. This test validates the
// Phase 13 null-packing fix against the real server.
func TestQwpIntegrationNullableColumns(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_nullable"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Row 0: all columns have values.
	err = s.Table(tableName).
		Symbol("sym", "AAPL").
		Int64Column("qty", 100).
		Float64Column("price", 150.5).
		StringColumn("note", "buy").
		BoolColumn("flag", true).
		At(ctx, ts)
	if err != nil {
		t.Fatalf("row 0: %v", err)
	}

	// Row 1: qty and note are null (not set), others present.
	err = s.Table(tableName).
		Symbol("sym", "GOOG").
		Float64Column("price", 2800.0).
		BoolColumn("flag", false).
		At(ctx, ts.Add(time.Microsecond))
	if err != nil {
		t.Fatalf("row 1: %v", err)
	}

	// Row 2: price and flag are null, others present.
	err = s.Table(tableName).
		Symbol("sym", "MSFT").
		Int64Column("qty", 50).
		StringColumn("note", "sell").
		At(ctx, ts.Add(2*time.Microsecond))
	if err != nil {
		t.Fatalf("row 2: %v", err)
	}

	// Row 3: all nullable columns are null (only symbol + timestamp).
	err = s.Table(tableName).
		Symbol("sym", "TSLA").
		At(ctx, ts.Add(3*time.Microsecond))
	if err != nil {
		t.Fatalf("row 3: %v", err)
	}

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 4)
	if result.Count != 4 {
		t.Fatalf("count = %d, want 4", result.Count)
	}

	// Verify data by querying specific values.
	// Row 0: all values present.
	r0 := qwpQuery(t, fmt.Sprintf("SELECT sym, qty, price, note, flag FROM '%s' WHERE sym = 'AAPL'", tableName))
	if r0.Count != 1 {
		t.Fatalf("AAPL count = %d, want 1", r0.Count)
	}
	row0 := r0.Dataset[0]
	if row0[0] != "AAPL" {
		t.Fatalf("row0 sym = %v, want AAPL", row0[0])
	}
	if qty, ok := row0[1].(float64); !ok || int64(qty) != 100 {
		t.Fatalf("row0 qty = %v, want 100", row0[1])
	}
	if price, ok := row0[2].(float64); !ok || price != 150.5 {
		t.Fatalf("row0 price = %v, want 150.5", row0[2])
	}
	if row0[3] != "buy" {
		t.Fatalf("row0 note = %v, want buy", row0[3])
	}
	if row0[4] != true {
		t.Fatalf("row0 flag = %v, want true", row0[4])
	}

	// Row 1: qty and note are null.
	r1 := qwpQuery(t, fmt.Sprintf("SELECT sym, qty, price, note, flag FROM '%s' WHERE sym = 'GOOG'", tableName))
	if r1.Count != 1 {
		t.Fatalf("GOOG count = %d, want 1", r1.Count)
	}
	row1 := r1.Dataset[0]
	if row1[1] != nil {
		t.Fatalf("row1 qty = %v, want nil", row1[1])
	}
	if price, ok := row1[2].(float64); !ok || price != 2800.0 {
		t.Fatalf("row1 price = %v, want 2800.0", row1[2])
	}
	// QuestDB JSON API returns "" for null STRING, not JSON null.
	if row1[3] != nil && row1[3] != "" {
		t.Fatalf("row1 note = %v, want nil or empty", row1[3])
	}
	if row1[4] != false {
		t.Fatalf("row1 flag = %v, want false", row1[4])
	}

	// Row 2: price and flag are null.
	r2 := qwpQuery(t, fmt.Sprintf("SELECT sym, qty, price, note, flag FROM '%s' WHERE sym = 'MSFT'", tableName))
	if r2.Count != 1 {
		t.Fatalf("MSFT count = %d, want 1", r2.Count)
	}
	row2 := r2.Dataset[0]
	if qty, ok := row2[1].(float64); !ok || int64(qty) != 50 {
		t.Fatalf("row2 qty = %v, want 50", row2[1])
	}
	if row2[2] != nil {
		t.Fatalf("row2 price = %v, want nil", row2[2])
	}
	if row2[3] != "sell" {
		t.Fatalf("row2 note = %v, want sell", row2[3])
	}
	// QuestDB JSON API may return false for null BOOLEAN.
	if row2[4] != nil && row2[4] != false {
		t.Fatalf("row2 flag = %v, want nil or false", row2[4])
	}

	// Row 3: all nullable columns null.
	r3 := qwpQuery(t, fmt.Sprintf("SELECT sym, qty, price, note, flag FROM '%s' WHERE sym = 'TSLA'", tableName))
	if r3.Count != 1 {
		t.Fatalf("TSLA count = %d, want 1", r3.Count)
	}
	row3 := r3.Dataset[0]
	if row3[1] != nil {
		t.Fatalf("row3 qty = %v, want nil", row3[1])
	}
	if row3[2] != nil {
		t.Fatalf("row3 price = %v, want nil", row3[2])
	}
	// STRING nulls may appear as "" in JSON.
	if row3[3] != nil && row3[3] != "" {
		t.Fatalf("row3 note = %v, want nil or empty", row3[3])
	}
	// BOOLEAN nulls may appear as false in JSON.
	if row3[4] != nil && row3[4] != false {
		t.Fatalf("row3 flag = %v, want nil or false", row3[4])
	}

	t.Log("QWP nullable columns integration test passed")
}

// --- Long256 round-trip ---

func TestQwpIntegrationLong256(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_long256"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Distinct-per-limb pattern round-trips all four 64-bit words.
	hexStr := "0101010101010101020202020202020203030303030303030404040404040404"
	val, ok := new(big.Int).SetString(hexStr, 16)
	if !ok {
		t.Fatal("SetString failed")
	}

	// Also cover: zero, one, and 2^256-1 (max).
	maxVal := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

	rows := []struct {
		tag string
		v   *big.Int
	}{
		{"pattern", val},
		{"zero", big.NewInt(0)},
		{"one", big.NewInt(1)},
		{"max", maxVal},
	}

	for i, r := range rows {
		err = s.Table(tableName).
			Symbol("tag", r.tag).
			Long256Column("h", r.v).
			At(ctx, ts.Add(time.Duration(i)*time.Microsecond))
		if err != nil {
			t.Fatalf("row %s: %v", r.tag, err)
		}
	}

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, len(rows))
	if result.Count != len(rows) {
		t.Fatalf("count = %d, want %d", result.Count, len(rows))
	}

	for _, r := range rows {
		q := qwpQuery(t, fmt.Sprintf("SELECT tag, h FROM '%s' WHERE tag = '%s'", tableName, r.tag))
		if q.Count != 1 {
			t.Fatalf("%s: count = %d, want 1", r.tag, q.Count)
		}
		gotStr, ok := q.Dataset[0][1].(string)
		if !ok {
			t.Fatalf("%s: h not a string: %#v", r.tag, q.Dataset[0][1])
		}
		// Compare numerically — QuestDB may render with varying zero-padding (0x0 vs 0x00).
		gotBig, ok := new(big.Int).SetString(strings.TrimPrefix(gotStr, "0x"), 16)
		if !ok {
			t.Fatalf("%s: could not parse returned hex %q", r.tag, gotStr)
		}
		if gotBig.Cmp(r.v) != 0 {
			t.Fatalf("%s: h = %s, want %s", r.tag, gotBig.Text(16), r.v.Text(16))
		}
	}

	t.Log("QWP long256 integration test passed")
}

// --- Ported from Java QwpSenderE2ETest.testAtNowServerAssignedTimestamp ---
//
// AtNow closes a row without supplying a designated timestamp — the
// server fills it in at receive time. Success means the row lands and
// the ts column is populated.
func TestQwpIntegrationAtNow(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_at_now"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	if err := s.Table(tableName).Int64Column("value", 100).AtNow(ctx); err != nil {
		t.Fatalf("AtNow: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result := qwpWaitForRows(t, tableName, 1)
	if result.Count != 1 {
		t.Fatalf("count = %d, want 1", result.Count)
	}
	if v, ok := result.Dataset[0][0].(float64); !ok || int64(v) != 100 {
		t.Fatalf("value = %v, want 100", result.Dataset[0][0])
	}
}

// --- Ported from Java QwpSenderE2ETest: per-type round-trips for
// QWP-only column types that the existing TestQwpIntegrationBasicTypes
// does not cover (Byte, Short, Int32, Float32, Char, Uuid, Date,
// TimestampNanos). These are not reachable through ILP.
//
// Grouped into one driver test so the table-drop/setup cost runs once
// per type rather than once for the suite.

func TestQwpIntegrationQwpOnlyTypes(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	t.Run("Byte", func(t *testing.T) {
		tableName := "qwp_integ_byte"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		s.Table(tableName)
		s.ByteColumn("b", -42)
		if err := s.At(ctx, ts); err != nil {
			t.Fatalf("At: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		// QuestDB stores BYTE as an integer; JSON returns a float64.
		if v, ok := r.Dataset[0][0].(float64); !ok || int8(v) != -42 {
			t.Fatalf("byte = %v, want -42", r.Dataset[0][0])
		}
	})

	t.Run("Short", func(t *testing.T) {
		tableName := "qwp_integ_short"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		s.Table(tableName)
		s.ShortColumn("s", -12345)
		if err := s.At(ctx, ts); err != nil {
			t.Fatalf("At: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		if v, ok := r.Dataset[0][0].(float64); !ok || int16(v) != -12345 {
			t.Fatalf("short = %v, want -12345", r.Dataset[0][0])
		}
	})

	t.Run("Int32", func(t *testing.T) {
		tableName := "qwp_integ_int32"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		s.Table(tableName)
		s.Int32Column("i", -1_000_000)
		if err := s.At(ctx, ts); err != nil {
			t.Fatalf("At: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		if v, ok := r.Dataset[0][0].(float64); !ok || int32(v) != -1_000_000 {
			t.Fatalf("int = %v, want -1000000", r.Dataset[0][0])
		}
	})

	t.Run("Float32", func(t *testing.T) {
		tableName := "qwp_integ_float32"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		s.Table(tableName)
		s.Float32Column("f", 3.25)
		if err := s.At(ctx, ts); err != nil {
			t.Fatalf("At: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		// 3.25 is exactly representable in float32, so no rounding noise.
		if v, ok := r.Dataset[0][0].(float64); !ok || v != 3.25 {
			t.Fatalf("float = %v, want 3.25", r.Dataset[0][0])
		}
	})

	t.Run("Char", func(t *testing.T) {
		tableName := "qwp_integ_char"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		s.Table(tableName)
		s.CharColumn("c", 'Z')
		if err := s.At(ctx, ts); err != nil {
			t.Fatalf("At: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		if r.Dataset[0][0] != "Z" {
			t.Fatalf("char = %v, want Z", r.Dataset[0][0])
		}
	})

	t.Run("Uuid", func(t *testing.T) {
		tableName := "qwp_integ_uuid"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		// a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 — borrowed from the
		// Java testOmittedColumns case for direct comparison.
		const hi uint64 = 0xa0eebc999c0b4ef8
		const lo uint64 = 0xbb6d6bb9bd380a11
		want := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

		s.Table(tableName)
		s.UuidColumn("u", hi, lo)
		if err := s.At(ctx, ts); err != nil {
			t.Fatalf("At: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		if r.Dataset[0][0] != want {
			t.Fatalf("uuid = %v, want %s", r.Dataset[0][0], want)
		}
	})

	t.Run("Date", func(t *testing.T) {
		tableName := "qwp_integ_date"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		// Whole second so there's no sub-millisecond noise to worry about.
		dateVal := time.Date(2022, 2, 25, 0, 0, 0, 0, time.UTC)

		s.Table(tableName)
		s.DateColumn("d", dateVal)
		if err := s.At(ctx, ts); err != nil {
			t.Fatalf("At: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		got, ok := r.Dataset[0][0].(string)
		if !ok {
			t.Fatalf("date not a string: %#v", r.Dataset[0][0])
		}
		if !strings.HasPrefix(got, "2022-02-25T00:00:00.000") {
			t.Fatalf("date = %q, want prefix 2022-02-25T00:00:00.000", got)
		}
	})

	t.Run("TimestampNanos", func(t *testing.T) {
		tableName := "qwp_integ_ts_nanos"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)

		// Use a nanosecond-precision designated timestamp (AtNano). The
		// value is stored with nanosecond resolution and rendered with
		// the trailing nanos visible.
		tsNano := time.Date(2022, 2, 25, 12, 0, 0, 123_456_789, time.UTC)

		s.Table(tableName)
		s.Int64Column("v", 1)
		if err := s.AtNano(ctx, tsNano); err != nil {
			t.Fatalf("AtNano: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		r := qwpWaitForRows(t, tableName, 1)
		// Confirm the designated-timestamp column shows nanosecond detail.
		rts := qwpQuery(t, fmt.Sprintf("SELECT timestamp FROM '%s'", tableName))
		if rts.Count != 1 {
			t.Fatalf("ts count = %d, want 1", rts.Count)
		}
		got, ok := rts.Dataset[0][0].(string)
		if !ok {
			t.Fatalf("ts not a string: %#v", rts.Dataset[0][0])
		}
		if !strings.Contains(got, "123456789") {
			t.Fatalf("ts = %q, expected nanos 123456789", got)
		}
		_ = r
	})
}

// --- Ported from Java QwpSenderE2ETest.testDecimal / testDecimalRescale ---
//
// Round-trips Decimal64/128/256 with distinct scales so QuestDB
// auto-creates columns typed to each of the three fixed widths.
func TestQwpIntegrationDecimalColumns(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_decimal"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	// Mirror the Java port's Decimal{64,128,256}.fromLong(unscaled, scale)
	// constructors via NewDecimalFromInt64.
	d64 := NewDecimalFromInt64(12345, 2)  // 123.45
	d128 := NewDecimalFromInt64(67890, 3) // 67.890
	d256 := NewDecimalFromInt64(11111, 1) // 1111.1

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	s.Table(tableName)
	s.Decimal64Column("d64", d64)
	s.Decimal128Column("d128", d128)
	s.Decimal256Column("d256", d256)
	if err := s.At(ctx, ts); err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 1)

	// Compare textual renderings — QuestDB prints decimals at their
	// native scale, so the output is deterministic.
	r := qwpQuery(t, fmt.Sprintf("SELECT d64, d128, d256 FROM '%s'", tableName))
	if r.Count != 1 {
		t.Fatalf("count = %d, want 1", r.Count)
	}
	cases := []struct {
		idx  int
		want string
	}{
		{0, "123.45"},
		{1, "67.890"},
		{2, "1111.1"},
	}
	for _, c := range cases {
		got, ok := r.Dataset[0][c.idx].(string)
		if !ok {
			t.Fatalf("col %d not a string: %#v", c.idx, r.Dataset[0][c.idx])
		}
		if got != c.want {
			t.Fatalf("col %d = %q, want %q", c.idx, got, c.want)
		}
	}
}

// --- Ported from Java QwpSenderE2ETest.testDoubleArray ---
//
// Round-trips a 1D float64 array. 2D/3D arrays use the same wire
// encoding and the same buffer path (qwpColumnBuffer.addDoubleArray),
// so one dimension exercises the full stack end-to-end.
func TestQwpIntegrationFloat64Arrays(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_f64_array"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	s.Table(tableName).
		Float64Array1DColumn("arr", []float64{1.5, 2.5, 3.5, 4.5}).
		At(ctx, ts)
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 1)

	// QuestDB renders arrays as a bracketed, comma-separated string.
	r := qwpQuery(t, fmt.Sprintf("SELECT arr FROM '%s'", tableName))
	got, ok := r.Dataset[0][0].(string)
	if !ok {
		// JSON sometimes returns arrays as []interface{}. Accept either.
		if arr, ok := r.Dataset[0][0].([]interface{}); ok {
			if len(arr) != 4 {
				t.Fatalf("array len = %d, want 4", len(arr))
			}
			return
		}
		t.Fatalf("arr unexpected: %#v", r.Dataset[0][0])
	}
	for _, want := range []string{"1.5", "2.5", "3.5", "4.5"} {
		if !strings.Contains(got, want) {
			t.Fatalf("arr = %q, expected to contain %s", got, want)
		}
	}
}

// --- Ported from Java QwpSenderE2ETest.testCoercionToGeoHash /
// testOmittedGeoHashColumn ---
//
// Round-trips GEOHASH at a representative precision. The geohash wire
// format truncates to ceil(precision/8) bytes per value, so
// intermediate precisions exercise that truncation path.
//
// QuestDB cannot auto-create a GEOHASH column from QWP wire data — the
// precision must be fixed in the schema. Mirroring the Java test,
// pre-create the table with GEOHASH(8c) = 40 bits.
func TestQwpIntegrationGeohash(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_geohash"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	qwpExec(t, fmt.Sprintf(
		"CREATE TABLE '%s' (gh GEOHASH(8c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL",
		tableName))

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	// Any 40-bit pattern round-trips as long as the client's wire
	// truncation matches the server's expectation (low ceil(40/8)=5
	// bytes in LE order).
	const hash40 uint64 = 0xCA5F6DD6D6
	const prec = 40

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	s.Table(tableName)
	s.GeohashColumn("gh", hash40, prec)
	if err := s.At(ctx, ts); err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 1)

	r := qwpQuery(t, fmt.Sprintf("SELECT gh FROM '%s'", tableName))
	if r.Count != 1 {
		t.Fatalf("count = %d, want 1", r.Count)
	}
	got, ok := r.Dataset[0][0].(string)
	if !ok {
		t.Fatalf("geohash not a string: %#v", r.Dataset[0][0])
	}
	// 40 bits / 5 bits per char = 8-char base32 geohash.
	if len(got) != 8 {
		t.Fatalf("geohash len = %d (%q), want 8", len(got), got)
	}
}

// qwpExec runs a SQL statement via QuestDB's /exec endpoint. Used when
// a test needs to pre-create a table with a specific schema (e.g.
// GEOHASH columns that can't be auto-created from wire data alone).
func qwpExec(t *testing.T, query string) {
	t.Helper()
	u, _ := url.Parse("http://" + qwpTestAddr)
	u.Path = "/exec"
	params := url.Values{}
	params.Add("query", query)
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
	if err != nil {
		t.Fatalf("build exec request: %v", err)
	}
	resp, err := qwpTestHTTPClient.Do(req)
	if err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		t.Fatalf("exec %q: HTTP %d: %s", query, resp.StatusCode, string(body))
	}
	if strings.Contains(string(body), `"error"`) {
		t.Fatalf("exec %q: server error: %s", query, string(body))
	}
}

// --- Ported from Java QwpSenderE2ETest.testOmittedColumns ---
//
// Writes four rows alternating between "all columns set" and "no
// columns set (only the designated timestamp)", and verifies every
// nullable column reports null for the omitted rows while non-nullable
// types (BYTE, SHORT, BOOL) fall back to their type-specific sentinel.
func TestQwpIntegrationOmittedColumns(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_omitted"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	// Row 0: all columns set. Table() starts the row; subsequent
	// column setters are statements because some (Byte/Short/Int32)
	// belong to QwpSender while others belong to LineSender — mixing
	// them in a fluent chain would require repeated type assertions.
	s.Table(tableName)
	s.ByteColumn("b", 1)
	s.ShortColumn("sh", 100)
	s.Int32Column("i", 42)
	s.Int64Column("l", 100)
	s.Float64Column("d", 1.5)
	s.StringColumn("str", "hello")
	s.Symbol("sym", "alpha")
	s.BoolColumn("bl", true)
	if err := s.At(ctx, base); err != nil {
		t.Fatalf("row 0: %v", err)
	}

	// Row 1: all columns omitted.
	if err := s.Table(tableName).At(ctx, base.Add(time.Microsecond)); err != nil {
		t.Fatalf("row 1: %v", err)
	}

	// Row 2: distinct second value set.
	s.Table(tableName)
	s.ByteColumn("b", -1)
	s.ShortColumn("sh", -200)
	s.Int32Column("i", -100)
	s.Int64Column("l", -200)
	s.Float64Column("d", -2.5)
	s.StringColumn("str", "world")
	s.Symbol("sym", "beta")
	s.BoolColumn("bl", false)
	if err := s.At(ctx, base.Add(2*time.Microsecond)); err != nil {
		t.Fatalf("row 2: %v", err)
	}

	// Row 3: omitted again.
	if err := s.Table(tableName).At(ctx, base.Add(3*time.Microsecond)); err != nil {
		t.Fatalf("row 3: %v", err)
	}

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 4)

	// Pull the four rows in timestamp order. We drop the ts column from
	// the projection since it differs per row and isn't the focus here.
	r := qwpQuery(t, fmt.Sprintf(
		"SELECT l, d, str, sym, bl, b, sh, i FROM '%s' ORDER BY timestamp",
		tableName))
	if r.Count != 4 {
		t.Fatalf("count = %d, want 4", r.Count)
	}

	// Row 0: all present.
	row := r.Dataset[0]
	if v, ok := row[0].(float64); !ok || int64(v) != 100 {
		t.Errorf("row0 l = %v, want 100", row[0])
	}
	if v, ok := row[1].(float64); !ok || v != 1.5 {
		t.Errorf("row0 d = %v, want 1.5", row[1])
	}
	if row[2] != "hello" {
		t.Errorf("row0 str = %v, want hello", row[2])
	}
	if row[3] != "alpha" {
		t.Errorf("row0 sym = %v, want alpha", row[3])
	}
	if row[4] != true {
		t.Errorf("row0 bl = %v, want true", row[4])
	}

	// Row 1: nullable columns null, non-nullable fall back to sentinels.
	row = r.Dataset[1]
	if row[0] != nil {
		t.Errorf("row1 l = %v, want nil", row[0])
	}
	if row[1] != nil {
		t.Errorf("row1 d = %v, want nil", row[1])
	}
	// STRING null may render as "" or nil in the REST JSON.
	if row[2] != nil && row[2] != "" {
		t.Errorf("row1 str = %v, want nil or empty", row[2])
	}
	if row[3] != nil && row[3] != "" {
		t.Errorf("row1 sym = %v, want nil or empty", row[3])
	}
	// BOOLEAN has no null sentinel — nulls surface as false.
	if row[4] != nil && row[4] != false {
		t.Errorf("row1 bl = %v, want nil or false", row[4])
	}
	// BYTE and SHORT have no nulls — sentinel is 0.
	if v, ok := row[5].(float64); !ok || int8(v) != 0 {
		t.Errorf("row1 b = %v, want 0", row[5])
	}
	if v, ok := row[6].(float64); !ok || int16(v) != 0 {
		t.Errorf("row1 sh = %v, want 0", row[6])
	}
	// INT is nullable in QuestDB, so nil here.
	if row[7] != nil {
		t.Errorf("row1 i = %v, want nil", row[7])
	}

	// Row 2: second distinct set.
	row = r.Dataset[2]
	if v, ok := row[0].(float64); !ok || int64(v) != -200 {
		t.Errorf("row2 l = %v, want -200", row[0])
	}
	if row[2] != "world" {
		t.Errorf("row2 str = %v, want world", row[2])
	}
	if row[3] != "beta" {
		t.Errorf("row2 sym = %v, want beta", row[3])
	}

	// Row 3: all omitted again — same expectations as row 1.
	row = r.Dataset[3]
	if row[0] != nil {
		t.Errorf("row3 l = %v, want nil", row[0])
	}
	if v, ok := row[5].(float64); !ok || int8(v) != 0 {
		t.Errorf("row3 b = %v, want 0", row[5])
	}
}

// newQwpIntegSender constructs a QWP sender for the integration suite.
// Used by per-subtest helpers so each subtest gets its own sender.
// Callers must have already run qwpEnsureServer(t), so a connect
// failure here is a real sender bug, not a deployment gap — Fatalf
// (also makes QDB_FUZZ_STRICT=1 in qwp-fuzz.yml fail loudly instead of
// silently passing).
func newQwpIntegSender(t *testing.T, ctx context.Context) QwpSender {
	t.Helper()
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, time.Second, 0, 0, nil)
	if err != nil {
		t.Fatalf("connect ws://%s: %v", qwpTestAddr, err)
	}
	return s
}

// --- Ported from Java QwpSenderE2ETest.testWriteAllTypesInOneRow ---
//
// One row that exercises every column type the Go sender exposes:
// Symbol, Bool, Short, Int32, Int64, Float32, Float64, String, Char,
// TimestampColumn, UuidColumn, Long256, Float64Array1D, Decimal64.
// Verifies the encoder assembles a single table block with diverse
// column types and the server ingests it without coercion errors.
func TestQwpIntegrationWriteAllTypesInOneRow(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_all_types"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2022, 2, 25, 0, 0, 0, 0, time.UTC)

	// Start the row with LineSender-only columns that chain fluently.
	s.Table(tableName).
		Symbol("sym", "test_symbol").
		BoolColumn("bool_col", true).
		Int64Column("long_col", 1_000_000_000).
		Float64Column("double_col", 3.14).
		StringColumn("string_col", "hello").
		TimestampColumn("ts_col", ts).
		Long256Column("long256_col", big.NewInt(1))

	// QwpSender-only columns must be called as statements.
	s.ShortColumn("short_col", 42)
	s.Int32Column("int_col", 100_000)
	s.Float32Column("float_col", 2.5)
	s.CharColumn("char_col", 'Z')
	s.UuidColumn("uuid_col", 0xa0eebc999c0b4ef8, 0xbb6d6bb9bd380a11)
	s.Decimal64Column("decimal_col", NewDecimalFromInt64(9999, 2))

	// Float64 array sits on LineSender and returns LineSender, so it
	// works either as a statement or as a final chain step.
	s.Float64Array1DColumn("arr_col", []float64{1.0, 2.0, 3.0})

	if err := s.At(ctx, ts); err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Just verify the row landed — per-column correctness is already
	// covered by the per-type round-trip tests.
	r := qwpWaitForRows(t, tableName, 1)
	if r.Count != 1 {
		t.Fatalf("count = %d, want 1", r.Count)
	}
}

// --- Ported from Java QwpSenderE2ETest.testSameColumnNameDifferentTypesDifferentTables ---
//
// Two tables that share a column name but type it differently. Each
// table gets its own schema on the wire, so the client's schema
// registry must isolate them. Interleaved writes must not cross-
// contaminate (i.e. send a LONG payload under the DOUBLE schema or
// vice versa).
func TestQwpIntegrationSchemaIsolation(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableA := "qwp_integ_iso_a"
	tableB := "qwp_integ_iso_b"
	qwpDropTable(t, tableA)
	qwpDropTable(t, tableB)
	defer qwpDropTable(t, tableA)
	defer qwpDropTable(t, tableB)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		row := base.Add(time.Duration(i) * time.Microsecond)
		// Table A: "value" is LONG.
		if err := s.Table(tableA).
			Int64Column("value", int64(i)*100).
			At(ctx, row); err != nil {
			t.Fatalf("tableA row %d: %v", i, err)
		}
		// Table B: same column name, DOUBLE type.
		if err := s.Table(tableB).
			Float64Column("value", float64(i)*1.5).
			At(ctx, row); err != nil {
			t.Fatalf("tableB row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	rA := qwpWaitForRows(t, tableA, 10)
	rB := qwpWaitForRows(t, tableB, 10)
	if rA.Count != 10 {
		t.Fatalf("tableA count = %d, want 10", rA.Count)
	}
	if rB.Count != 10 {
		t.Fatalf("tableB count = %d, want 10", rB.Count)
	}

	// Confirm tableA's "value" is LONG and tableB's is DOUBLE.
	aType := columnType(t, tableA, "value")
	if aType != "LONG" {
		t.Errorf("tableA.value type = %q, want LONG", aType)
	}
	bType := columnType(t, tableB, "value")
	if bType != "DOUBLE" {
		t.Errorf("tableB.value type = %q, want DOUBLE", bType)
	}

	// Verify a couple of values round-tripped at the right type.
	qA := qwpQuery(t, fmt.Sprintf(
		"SELECT value FROM '%s' ORDER BY timestamp LIMIT 3", tableA))
	for i, want := range []int64{0, 100, 200} {
		got, ok := qA.Dataset[i][0].(float64)
		if !ok || int64(got) != want {
			t.Errorf("tableA row %d value = %v, want %d", i, qA.Dataset[i][0], want)
		}
	}
	qB := qwpQuery(t, fmt.Sprintf(
		"SELECT value FROM '%s' ORDER BY timestamp LIMIT 3", tableB))
	for i, want := range []float64{0.0, 1.5, 3.0} {
		got, ok := qB.Dataset[i][0].(float64)
		if !ok || got != want {
			t.Errorf("tableB row %d value = %v, want %v", i, qB.Dataset[i][0], want)
		}
	}
}

// columnType returns the declared QuestDB column type for the given
// table/column via the table_columns() system function.
func columnType(t *testing.T, tableName, column string) string {
	t.Helper()
	r := qwpQuery(t, fmt.Sprintf(
		"SELECT type FROM table_columns('%s') WHERE \"column\" = '%s'",
		tableName, column))
	if r.Count == 0 {
		t.Fatalf("column %q not found in %s", column, tableName)
	}
	s, ok := r.Dataset[0][0].(string)
	if !ok {
		t.Fatalf("column type not a string: %#v", r.Dataset[0][0])
	}
	return s
}

// --- Ported from Java QwpSenderE2ETest.testAutoCreateVarcharColumn ---
//
// The Go StringColumn method must produce a VARCHAR column on the
// server (QWP wire type 0x0F), not the deprecated STRING type. The
// Java test pre-creates a bare table and verifies StringColumn adds
// a VARCHAR column.
func TestQwpIntegrationAutoCreateVarcharColumn(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_auto_varchar"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	if err := s.Table(tableName).
		StringColumn("msg", "hello").
		At(ctx, ts); err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 1)

	if got := columnType(t, tableName, "msg"); got != "VARCHAR" {
		t.Fatalf("msg type = %q, want VARCHAR", got)
	}
}

// --- Ported from Java QwpSenderE2ETest.testEmptyTableNameRejected /
// testEmptyColumnNameRejected ---
//
// Both are client-side validation, so the error surfaces at the next
// At/Flush as a latched buffer error. We still need a live sender to
// exercise the buffer path, so this is an integration test, not a
// pure unit test.
func TestQwpIntegrationNameValidation(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	t.Run("EmptyTableName", func(t *testing.T) {
		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)
		err := s.Table("").Int64Column("v", 1).At(ctx, ts)
		if err == nil {
			t.Fatal("expected error for empty table name, got nil")
		}
		if !strings.Contains(err.Error(), "table name") {
			t.Fatalf("error = %q, expected it to mention 'table name'", err.Error())
		}
	})

	t.Run("EmptyColumnName", func(t *testing.T) {
		tableName := "qwp_integ_empty_col"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		s := newQwpIntegSender(t, ctx)
		defer s.Close(ctx)
		err := s.Table(tableName).Int64Column("", 42).At(ctx, ts)
		if err == nil {
			t.Fatal("expected error for empty column name, got nil")
		}
		if !strings.Contains(err.Error(), "column name") {
			t.Fatalf("error = %q, expected it to mention 'column name'", err.Error())
		}
	})
}

// --- Port of testDoubleArray variants, client-side nDims+shape encoding ---
//
// The Go client serializes arrays as nDims + (nDims × uint32 LE shape)
// + flattened elements in row-major order. 2D and 3D exercise the
// multi-dimensional branch that the 1D test does not.

func TestQwpIntegrationFloat64Array2D(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_f64_array_2d"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	// 2×3 matrix laid out row-major on the wire.
	m := [][]float64{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
	}
	if err := s.Table(tableName).
		Float64Array2DColumn("arr", m).
		At(ctx, ts); err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 1)
	// Rendering varies across server versions — just confirm all six
	// element values survive the client→server→client round-trip.
	r := qwpQuery(t, fmt.Sprintf("SELECT arr FROM '%s'", tableName))
	got := fmt.Sprintf("%v", r.Dataset[0][0])
	for _, want := range []string{"1", "2", "3", "4", "5", "6"} {
		if !strings.Contains(got, want) {
			t.Errorf("arr = %q, missing %s", got, want)
		}
	}
}

func TestQwpIntegrationFloat64Array3D(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_f64_array_3d"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	// 2×2×2 = 8 distinct values so each element can be identified
	// independently after round-trip.
	a := [][][]float64{
		{{1.0, 2.0}, {3.0, 4.0}},
		{{5.0, 6.0}, {7.0, 8.0}},
	}
	if err := s.Table(tableName).
		Float64Array3DColumn("arr", a).
		At(ctx, ts); err != nil {
		t.Fatalf("At: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 1)
	r := qwpQuery(t, fmt.Sprintf("SELECT arr FROM '%s'", tableName))
	got := fmt.Sprintf("%v", r.Dataset[0][0])
	for _, want := range []string{"1", "2", "3", "4", "5", "6", "7", "8"} {
		if !strings.Contains(got, want) {
			t.Errorf("arr = %q, missing %s", got, want)
		}
	}
}

// --- Client-side validation tests ---
//
// These exercise error paths in the Go buffer/sender — the server is
// never reached because validation fails before Flush can send bytes.
// The live connection is still required because every public sender
// entry point goes through the websocket handshake.

func TestQwpIntegrationDecimalScaleConflict(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_decimal_scale_conflict"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Row 1 establishes scale=2.
	s.Table(tableName)
	s.Decimal64Column("d", NewDecimalFromInt64(12345, 2))
	if err := s.At(ctx, ts); err != nil {
		t.Fatalf("row 1: %v", err)
	}

	// Row 2: same column, different scale. Must error before any
	// Flush reaches the server.
	s.Table(tableName)
	s.Decimal64Column("d", NewDecimalFromInt64(999, 3))
	err := s.At(ctx, ts.Add(time.Microsecond))
	if err == nil {
		t.Fatal("expected scale conflict error, got nil")
	}
	if !strings.Contains(err.Error(), "scale") {
		t.Fatalf("error = %q, expected it to mention 'scale'", err.Error())
	}
}

func TestQwpIntegrationColumnTypeConflict(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_type_conflict"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// First row: "x" is LONG.
	if err := s.Table(tableName).Int64Column("x", 1).At(ctx, ts); err != nil {
		t.Fatalf("row 1: %v", err)
	}

	// Second row: same column as DOUBLE. Client must detect the
	// type conflict and surface the error on the next At/Flush.
	err := s.Table(tableName).Float64Column("x", 2.5).At(ctx, ts.Add(time.Microsecond))
	if err == nil {
		t.Fatal("expected type conflict error, got nil")
	}
	if !strings.Contains(err.Error(), "type conflict") {
		t.Fatalf("error = %q, expected it to mention 'type conflict'", err.Error())
	}
}

func TestQwpIntegrationDuplicateColumnInRow(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_dup_col"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Set the same column twice in one row — client must reject.
	err := s.Table(tableName).
		Int64Column("x", 1).
		Int64Column("x", 2).
		At(ctx, ts)
	if err == nil {
		t.Fatal("expected duplicate-column error, got nil")
	}
	if !strings.Contains(err.Error(), "already set") {
		t.Fatalf("error = %q, expected it to mention 'already set'", err.Error())
	}
}

func TestQwpIntegrationGeohashPrecisionConflict(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_geohash_prec_conflict"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// Pre-create with a fixed precision so server-side validation
	// isn't the one that trips first.
	qwpExec(t, fmt.Sprintf(
		"CREATE TABLE '%s' (g GEOHASH(8c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL",
		tableName))

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Row 1 establishes precision=40.
	s.Table(tableName)
	s.GeohashColumn("g", 0xCA5F6DD6D6, 40)
	if err := s.At(ctx, ts); err != nil {
		t.Fatalf("row 1: %v", err)
	}

	// Row 2 uses a different precision on the same column — client
	// buffer must reject.
	s.Table(tableName)
	s.GeohashColumn("g", 0x123456, 25)
	err := s.At(ctx, ts.Add(time.Microsecond))
	if err == nil {
		t.Fatal("expected precision conflict error, got nil")
	}
	if !strings.Contains(err.Error(), "precision") {
		t.Fatalf("error = %q, expected it to mention 'precision'", err.Error())
	}
}

// --- Ported from Java QwpSenderE2ETest.testAsyncModeAutoFlushOnClose ---
//
// Client contract: Close() on an async sender must drain any pending
// rows and wait for ACKs before returning. A buggy Close that only
// cancels the goroutine without flushing would silently drop data.
func TestQwpIntegrationAsyncCloseFlushes(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_async_close_flushes"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// Async sender (in-flight window = 4). No explicit Flush.
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 0, 0, nil, 4)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	const rowCount = 25
	for i := 0; i < rowCount; i++ {
		if err := s.Table(tableName).
			Int64Column("id", int64(i)).
			At(ctx, base.Add(time.Duration(i)*time.Microsecond)); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	// Close must flush buffered rows.
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := qwpWaitForRows(t, tableName, rowCount)
	if r.Count != rowCount {
		t.Fatalf("count = %d, want %d", r.Count, rowCount)
	}
}

// --- Ported from Java QwpSenderE2ETest.testAsyncModeStressAcks ---
//
// Client contract: with a small auto-flush row threshold the client
// issues many tiny batches in rapid succession. Its double-buffer
// scheme can only recycle a buffer after the server ACKs the prior
// batch; if ACK handling is broken the sender stalls or drops rows.
// Java uses 200 rows with autoFlushRows=2 (100 batches) — scaled to
// 100 rows / 50 batches here.
func TestQwpIntegrationAsyncStressAcks(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_async_stress_acks"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// autoFlushRows=2 → 50 batches in flight for 100 rows, with the
	// default in-flight window the sender must recycle buffers via ACKs.
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 2, 0, nil, 4)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close(ctx)

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	const rowCount = 100
	for i := 0; i < rowCount; i++ {
		if err := s.Table(tableName).
			Int64Column("id", int64(i)).
			At(ctx, base.Add(time.Duration(i)*time.Microsecond)); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("final Flush: %v", err)
	}

	r := qwpWaitForRows(t, tableName, rowCount)
	if r.Count != rowCount {
		t.Fatalf("count = %d, want %d", r.Count, rowCount)
	}
}

// --- Ported from Java QwpSenderE2ETest.testAsyncModeWithMultipleTables ---
//
// Client contract: in async mode, interleaved writes to multiple
// tables must all land. The client aggregates rows by table in
// per-table buffers and emits one multi-table message per flush;
// losing a table mid-batch would drop rows silently.
func TestQwpIntegrationAsyncMultiTable(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableA := "qwp_integ_async_multi_a"
	tableB := "qwp_integ_async_multi_b"
	qwpDropTable(t, tableA)
	qwpDropTable(t, tableB)
	defer qwpDropTable(t, tableA)
	defer qwpDropTable(t, tableB)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 0, 0, nil, 4)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close(ctx)

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	const perTable = 50
	for i := 0; i < perTable; i++ {
		ts := base.Add(time.Duration(i) * time.Microsecond)
		if err := s.Table(tableA).
			Int64Column("id", int64(i)).
			At(ctx, ts); err != nil {
			t.Fatalf("tableA row %d: %v", i, err)
		}
		if err := s.Table(tableB).
			Float64Column("value", float64(i)*2.5).
			At(ctx, ts); err != nil {
			t.Fatalf("tableB row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	rA := qwpWaitForRows(t, tableA, perTable)
	rB := qwpWaitForRows(t, tableB, perTable)
	if rA.Count != perTable {
		t.Fatalf("tableA count = %d, want %d", rA.Count, perTable)
	}
	if rB.Count != perTable {
		t.Fatalf("tableB count = %d, want %d", rB.Count, perTable)
	}
}

// --- Ported from Java QwpSenderE2ETest.testAsyncModeWithRowBasedFlush ---
//
// Client contract: when autoFlushRows is set, the client emits a
// batch every N rows without waiting for explicit Flush(). A bug in
// the row-count trigger would either stall the sender or over-flush.
func TestQwpIntegrationAsyncRowBasedFlush(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_async_row_flush"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// autoFlushRows=10, so 50 rows → 5 automatic flushes in async mode.
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 10, 0, nil, 4)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close(ctx)

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	const rowCount = 50
	for i := 0; i < rowCount; i++ {
		if err := s.Table(tableName).
			Int64Column("id", int64(i)).
			At(ctx, base.Add(time.Duration(i)*time.Microsecond)); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	// Trailing rows that fall short of the threshold still need a
	// final Flush() — exactly the Java test's expectation.
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r := qwpWaitForRows(t, tableName, rowCount)
	if r.Count != rowCount {
		t.Fatalf("count = %d, want %d", r.Count, rowCount)
	}
}

// --- Ported from Java QwpSenderE2ETest.testConcurrentSenders_differentTables /
// testConcurrentSenders_sameTable ---
//
// Client contract: each sender is an independent unit — per-connection
// schema IDs, symbol dictionaries, and WebSocket state. Multiple
// concurrent senders writing to disjoint or shared tables must all
// succeed without interfering with each other. A shared global in the
// client (e.g. a symbol map without per-sender scoping) would corrupt
// ingestion under concurrency.
func TestQwpIntegrationConcurrentSenders(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	t.Run("DifferentTables", func(t *testing.T) {
		const senderCount = 3
		const rowsPerSender = 250

		tables := make([]string, senderCount)
		for i := range tables {
			tables[i] = fmt.Sprintf("qwp_integ_concurrent_diff_%d", i)
			qwpDropTable(t, tables[i])
			defer qwpDropTable(t, tables[i])
		}

		base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		errs := make(chan error, senderCount)
		var wg sync.WaitGroup
		wg.Add(senderCount)
		for s := 0; s < senderCount; s++ {
			go func(idx int) {
				defer wg.Done()
				sender, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 10, 0, nil, 4)
				if err != nil {
					errs <- fmt.Errorf("sender %d connect: %w", idx, err)
					return
				}
				defer sender.Close(ctx)

				for i := 0; i < rowsPerSender; i++ {
					if err := sender.Table(tables[idx]).
						Int64Column("id", int64(i)).
						At(ctx, base.Add(time.Duration(i)*time.Microsecond)); err != nil {
						errs <- fmt.Errorf("sender %d row %d: %w", idx, i, err)
						return
					}
				}
				if err := sender.Flush(ctx); err != nil {
					errs <- fmt.Errorf("sender %d flush: %w", idx, err)
				}
			}(s)
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			t.Errorf("concurrent sender: %v", e)
		}
		if t.Failed() {
			return
		}

		for i, name := range tables {
			r := qwpWaitForRows(t, name, rowsPerSender)
			if r.Count != rowsPerSender {
				t.Errorf("table %d count = %d, want %d", i, r.Count, rowsPerSender)
			}
		}
	})

	t.Run("SameTable", func(t *testing.T) {
		const senderCount = 3
		const rowsPerSender = 250
		tableName := "qwp_integ_concurrent_same"
		qwpDropTable(t, tableName)
		defer qwpDropTable(t, tableName)

		base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		errs := make(chan error, senderCount)
		var wg sync.WaitGroup
		wg.Add(senderCount)
		for s := 0; s < senderCount; s++ {
			go func(idx int) {
				defer wg.Done()
				sender, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath}, 5*time.Second, 10, 0, nil, 4)
				if err != nil {
					errs <- fmt.Errorf("sender %d connect: %w", idx, err)
					return
				}
				defer sender.Close(ctx)

				for i := 0; i < rowsPerSender; i++ {
					// Stagger timestamps per sender so WAL sees them as
					// distinct rows rather than dedup candidates.
					ts := base.Add(time.Duration(idx)*time.Second + time.Duration(i)*time.Microsecond)
					if err := sender.Table(tableName).
						Int64Column("sender_id", int64(idx)).
						Int64Column("row_id", int64(i)).
						At(ctx, ts); err != nil {
						errs <- fmt.Errorf("sender %d row %d: %w", idx, i, err)
						return
					}
				}
				if err := sender.Flush(ctx); err != nil {
					errs <- fmt.Errorf("sender %d flush: %w", idx, err)
				}
			}(s)
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			t.Errorf("concurrent sender: %v", e)
		}
		if t.Failed() {
			return
		}

		total := senderCount * rowsPerSender
		r := qwpWaitForRows(t, tableName, total)
		if r.Count != total {
			t.Fatalf("total count = %d, want %d", r.Count, total)
		}
		// Verify each sender contributed its share.
		for i := 0; i < senderCount; i++ {
			q := qwpQuery(t, fmt.Sprintf(
				"SELECT count() FROM '%s' WHERE sender_id = %d", tableName, i))
			if v, ok := q.Dataset[0][0].(float64); !ok || int(v) != rowsPerSender {
				t.Errorf("sender_id=%d count = %v, want %d", i, q.Dataset[0][0], rowsPerSender)
			}
		}
	})
}

// --- Client-focused Gorilla round-trip test ---
//
// The Go client compresses timestamp columns with ≥3 values and DoDs
// that fit in int32. A bug anywhere in the delta-of-delta encoding,
// bit-packing, or the uncompressed-fallback path would corrupt
// timestamps in flight. This writes 128 rows whose designated
// timestamps cycle through every Gorilla bucket (0-bit, 7-bit, 9-bit,
// 12-bit, 32-bit) and verifies exact per-row round-trip.
func TestQwpIntegrationGorillaTimestampRoundTrip(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_gorilla_ts"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	// Build 128 timestamps whose DoDs span all five Gorilla buckets.
	const rowCount = 128
	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	offsets := make([]time.Duration, rowCount)
	offsets[0] = 0
	offsets[1] = time.Microsecond
	dodCycle := []int64{0, 50, -50, 200, -200, 1000, -1000, 50_000, -50_000}
	prevDelta := int64(1)
	for i := 2; i < rowCount; i++ {
		dod := dodCycle[i%len(dodCycle)]
		prevDelta += dod
		if prevDelta <= 0 {
			prevDelta = 1 // keep deltas positive so timestamps advance
		}
		offsets[i] = offsets[i-1] + time.Duration(prevDelta)*time.Microsecond
	}

	for i := 0; i < rowCount; i++ {
		if err := s.Table(tableName).
			Int64Column("idx", int64(i)).
			At(ctx, base.Add(offsets[i])); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, rowCount)

	// Fetch all rows in insertion order (idx ascending) and compare
	// timestamps microsecond-by-microsecond.
	r := qwpQuery(t, fmt.Sprintf(
		"SELECT idx, timestamp FROM '%s' ORDER BY idx", tableName))
	if r.Count != rowCount {
		t.Fatalf("count = %d, want %d", r.Count, rowCount)
	}
	for i := 0; i < rowCount; i++ {
		wantTs := base.Add(offsets[i]).UTC().Format("2006-01-02T15:04:05.000000Z")
		got, ok := r.Dataset[i][1].(string)
		if !ok {
			t.Fatalf("row %d ts not string: %#v", i, r.Dataset[i][1])
		}
		if got != wantTs {
			t.Fatalf("row %d ts = %q, want %q", i, got, wantTs)
		}
	}
}

// --- Client-focused schema evolution test ---
//
// When the user adds a new column mid-session, the client must reset
// the table's schemaId so the next flush re-registers the expanded
// schema in FULL mode (not REFERENCE mode against the stale ID).
// Otherwise the server would decode subsequent rows against the wrong
// column set and either reject them or mis-map columns.
func TestQwpIntegrationSchemaEvolution(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_schema_evolution"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Phase 1: 5 rows with columns {a, b}. First flush registers the
	// schema in FULL mode.
	for i := 0; i < 5; i++ {
		if err := s.Table(tableName).
			Int64Column("a", int64(i)).
			Float64Column("b", float64(i)*1.5).
			At(ctx, base.Add(time.Duration(i)*time.Microsecond)); err != nil {
			t.Fatalf("phase1 row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("phase1 Flush: %v", err)
	}

	// Phase 2: 5 rows with an added column c. The client must reset
	// the schemaId so a new FULL-mode schema is registered.
	for i := 5; i < 10; i++ {
		if err := s.Table(tableName).
			Int64Column("a", int64(i)).
			Float64Column("b", float64(i)*1.5).
			StringColumn("c", fmt.Sprintf("row_%d", i)).
			At(ctx, base.Add(time.Duration(i)*time.Microsecond)); err != nil {
			t.Fatalf("phase2 row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("phase2 Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, 10)

	r := qwpQuery(t, fmt.Sprintf(
		"SELECT a, b, c FROM '%s' ORDER BY timestamp", tableName))
	if r.Count != 10 {
		t.Fatalf("count = %d, want 10", r.Count)
	}
	// Rows 0..4: c is null (or empty); rows 5..9: c = "row_i".
	for i := 0; i < 5; i++ {
		if r.Dataset[i][2] != nil && r.Dataset[i][2] != "" {
			t.Errorf("row %d c = %v, want nil/empty", i, r.Dataset[i][2])
		}
	}
	for i := 5; i < 10; i++ {
		want := fmt.Sprintf("row_%d", i)
		if r.Dataset[i][2] != want {
			t.Errorf("row %d c = %v, want %q", i, r.Dataset[i][2], want)
		}
	}
	// a and b must be consistent across both phases.
	for i := 0; i < 10; i++ {
		if v, ok := r.Dataset[i][0].(float64); !ok || int64(v) != int64(i) {
			t.Errorf("row %d a = %v, want %d", i, r.Dataset[i][0], i)
		}
		wantB := float64(i) * 1.5
		if v, ok := r.Dataset[i][1].(float64); !ok || v != wantB {
			t.Errorf("row %d b = %v, want %v", i, r.Dataset[i][1], wantB)
		}
	}
}

// --- Client-focused bool bit-packing test ---
//
// The Go client bit-packs boolean values LSB-first into bytes. A bug
// in the packing (wrong bit order, byte-boundary off-by-one, or bits
// reset per row) would flip values after row 7 or misalign across
// flushes. This writes 24 rows of alternating true/false across 3
// bytes to cover two byte boundaries.
func TestQwpIntegrationBoolBitPacking(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_bool_packing"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s := newQwpIntegSender(t, ctx)
	defer s.Close(ctx)

	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	pattern := []bool{
		true, false, true, true, false, false, true, false, // byte 0
		false, true, true, true, true, false, false, true, // byte 1
		true, true, false, false, true, false, true, false, // byte 2
	}
	for i, v := range pattern {
		if err := s.Table(tableName).
			Int64Column("idx", int64(i)).
			BoolColumn("flag", v).
			At(ctx, base.Add(time.Duration(i)*time.Microsecond)); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	qwpWaitForRows(t, tableName, len(pattern))

	r := qwpQuery(t, fmt.Sprintf(
		"SELECT idx, flag FROM '%s' ORDER BY idx", tableName))
	if r.Count != len(pattern) {
		t.Fatalf("count = %d, want %d", r.Count, len(pattern))
	}
	for i, want := range pattern {
		got, ok := r.Dataset[i][1].(bool)
		if !ok {
			t.Fatalf("row %d flag not bool: %#v", i, r.Dataset[i][1])
		}
		if got != want {
			t.Errorf("row %d flag = %v, want %v", i, got, want)
		}
	}
}

func TestQwpIntegrationConnect(t *testing.T) {
	qwpEnsureServer(t)
	ctx := context.Background()

	var tr qwpTransport
	err := tr.connect(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{endpointPath: qwpWritePath})
	if err != nil {
		t.Fatalf("connect ws://%s: %v", qwpTestAddr, err)
	}
	defer tr.close()

	// Send a simple QWP message with delta symbol dict (required
	// by the server for symbol columns) and verify the ACK.
	tb := newQwpTableBuffer("qwp_transport_test")
	col, _ := tb.getOrCreateColumn("value", qwpTypeLong, false)
	col.addLong(42)
	colTs, _ := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
	colTs.addTimestamp(1000000)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	t.Logf("sending QWP message (%d bytes): %x", len(msg), msg)

	if err := tr.sendMessage(ctx, msg); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	status, data, err := tr.readAck(ctx)
	if err != nil {
		t.Fatalf("readAck: %v", err)
	}

	if status != QwpStatusOK {
		errStr := parseAckError(data)
		t.Logf("raw ACK response (%d bytes): %x", len(data), data)
		t.Fatalf("expected OK, got status 0x%02X: %s", status, errStr)
	}
	t.Logf("ACK OK, sequence=%d", parseAckSequence(data))
}
