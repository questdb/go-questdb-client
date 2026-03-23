/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
	"net/http"
	"net/url"
	"testing"
	"time"
)

const (
	qwpTestAddr       = "localhost:9000"
	qwpTestWaitPeriod = 5 * time.Second
	qwpTestPollPeriod = 100 * time.Millisecond
)

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

// qwpSkipIfNoServer skips the test if QuestDB is not available.
func qwpSkipIfNoServer(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, 0, 0, 0)
	if err != nil {
		t.Skipf("QuestDB not available at %s: %v", qwpTestAddr, err)
	}
	s.Close(ctx)
}

// qwpDropTable drops a table via QuestDB's HTTP API.
func qwpDropTable(t *testing.T, tableName string) {
	t.Helper()
	u, _ := url.Parse("http://" + qwpTestAddr)
	u.Path = "/exec"
	params := url.Values{}
	params.Add("query", "DROP TABLE IF EXISTS '"+tableName+"';")
	u.RawQuery = params.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		t.Logf("warning: could not drop table %q: %v", tableName, err)
		return
	}
	resp.Body.Close()
}

// qwpQuery executes a SQL query against QuestDB's HTTP API.
func qwpQuery(t *testing.T, query string) qwpTableResult {
	t.Helper()
	u, _ := url.Parse("http://" + qwpTestAddr)
	u.Path = "/exec"
	params := url.Values{}
	params.Add("query", query)
	u.RawQuery = params.Encode()

	resp, err := http.Get(u.String())
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
	qwpSkipIfNoServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_basic_types"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, time.Second, 0, 0)
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
	qwpSkipIfNoServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_multi_flush"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, time.Second, 0, 0)
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
	qwpSkipIfNoServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_symbol_dedup"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, time.Second, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Flush 1: symbols AAPL, MSFT.
	s.Table(tableName).Symbol("sym", "AAPL").Int64Column("v", 1).At(ctx, ts)
	s.Table(tableName).Symbol("sym", "MSFT").Int64Column("v", 2).At(ctx, ts.Add(time.Microsecond))
	s.Flush(ctx)

	// Flush 2: reuse AAPL, add GOOG.
	s.Table(tableName).Symbol("sym", "AAPL").Int64Column("v", 3).At(ctx, ts.Add(2*time.Microsecond))
	s.Table(tableName).Symbol("sym", "GOOG").Int64Column("v", 4).At(ctx, ts.Add(3*time.Microsecond))
	s.Flush(ctx)

	result := qwpWaitForRows(t, tableName, 4)
	if result.Count != 4 {
		t.Fatalf("count = %d, want 4", result.Count)
	}

	t.Log("QWP symbol dedup integration test passed")
}

// --- Multi-table batch test ---

func TestQwpIntegrationMultiTable(t *testing.T) {
	qwpSkipIfNoServer(t)
	ctx := context.Background()

	table1 := "qwp_integ_multi_t1"
	table2 := "qwp_integ_multi_t2"
	qwpDropTable(t, table1)
	qwpDropTable(t, table2)
	defer qwpDropTable(t, table1)
	defer qwpDropTable(t, table2)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, time.Second, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Interleave rows into two tables.
	s.Table(table1).Int64Column("x", 1).At(ctx, ts)
	s.Table(table2).Float64Column("y", 2.5).At(ctx, ts)
	s.Table(table1).Int64Column("x", 3).At(ctx, ts.Add(time.Microsecond))

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
	qwpSkipIfNoServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_large_batch"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, 5*time.Second, 0, 0)
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
	qwpSkipIfNoServer(t)
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
	qwpSkipIfNoServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_async"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// Create sender with in-flight window = 4.
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, 5*time.Second, 0, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	if s.asyncState == nil {
		t.Fatal("expected async mode with window=4")
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
	qwpSkipIfNoServer(t)
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
	qwpSkipIfNoServer(t)
	ctx := context.Background()

	tableName := "qwp_integ_autoflush"
	qwpDropTable(t, tableName)
	defer qwpDropTable(t, tableName)

	// auto-flush every 3 rows.
	s, err := newQwpLineSender(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{}, time.Second, 3, 0)
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
