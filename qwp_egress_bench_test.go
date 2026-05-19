/*******************************************************************************
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

// End-to-end QWP egress (query) latency benchmarks. These are the Go
// counterparts of the Java client's two JMH latency benchmarks in the
// QuestDB OSS repo (benchmarks/src/main/java/org/questdb):
// QwpEgressLatencyBenchmark and QwpEgressBindLatencyBenchmark.
//
// The third Java egress benchmark -- the application-style, cross-protocol
// QwpEgressReadBenchmark (QWP vs PG-wire vs HTTP) -- is ported separately as
// the standalone program in bench/qwp-egress-read, not as a `go test`
// benchmark. There is deliberately no BenchmarkQwpEgressRead here: a `go
// test` benchmark would only re-measure the QWP read path the standalone
// program already covers.
//
// Unlike the rest of qwp_bench_test.go (pure encode/decode microbenchmarks
// that never touch a socket) these run against a *live* QuestDB listening on
// localhost:9000 (HTTP/WS) -- the same live-server policy as the
// TestQwpIntegration* suite. They self-skip when no server is reachable, so
// `go test -bench .` stays green on a machine without QuestDB.
//
// Go has no JMH, so the JMH SampleTime + AverageTime split maps onto:
//   - ns/op           -> the arithmetic mean (testing.B's native number)
//   - p50/p90/p99/p999 -> custom metrics reported via b.ReportMetric, using
//                          the same percentile harness as the Java client's
//                          CursorEngineAppendLatencyBenchmark.
//
// Tunables are environment variables (the Go analog of Java's -Dkey=value),
// all read through benchEnv* helpers below:
//
//   QDB_BENCH_ADDR           host:port of the server          (default localhost:9000)
//   QDB_BENCH_SKIP_POPULATE  reuse the existing table          (default false)
//   QDB_BENCH_SQL            override the latency-bench SQL     (default "SELECT 1")
//
// Examples:
//
//   go test -run '^$' -bench BenchmarkQwpEgressLatency        -benchtime 3000x .
//   QDB_BENCH_SQL='SELECT id FROM latency_bench' \
//     go test -run '^$' -bench BenchmarkQwpEgressLatency      -benchtime 2000x .
//   go test -run '^$' -bench BenchmarkQwpEgressBindLatency    -benchtime 3000x .

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Environment knobs
// ---------------------------------------------------------------------------

func benchEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func benchEnvBool(key string) bool {
	v := os.Getenv(key)
	return v == "1" || v == "true" || v == "TRUE" || v == "yes"
}

// benchEgressAddr is the server the benchmarks talk to. Defaults to the same
// localhost:9000 the integration suite uses.
func benchEgressAddr() string { return benchEnvStr("QDB_BENCH_ADDR", qwpTestAddr) }

// ---------------------------------------------------------------------------
// Live-server helpers (testing.B-typed; mirror the *testing.T helpers in
// qwp_integration_test.go without refactoring the shared ones).
// ---------------------------------------------------------------------------

// benchSkipIfNoServer skips the benchmark when no QuestDB egress endpoint is
// reachable. Same intent as qwpSkipIfNoServer, but it dials the actual egress
// path (the read socket) so a server with only ingest wired up still skips
// cleanly rather than failing deep in @Setup-equivalent code.
func benchSkipIfNoServer(b *testing.B) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx, WithQwpQueryAddress(benchEgressAddr()))
	if err != nil {
		b.Skipf("QuestDB egress not available at %s: %v", benchEgressAddr(), err)
	}
	_ = c.Close(ctx)
}

// benchHTTPExec runs a statement through the server's HTTP /exec endpoint and
// returns the parsed result. Used for table setup/teardown and the WAL-apply
// poll -- deliberately off the QWP wire so it never perturbs the path under
// measurement (the same separation the Java benches get from using PG-wire).
func benchHTTPExec(b *testing.B, statement string) qwpTableResult {
	b.Helper()
	u, _ := url.Parse("http://" + benchEgressAddr())
	u.Path = "/exec"
	params := url.Values{}
	params.Add("query", statement)
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
	if err != nil {
		b.Fatalf("build /exec request: %v", err)
	}
	resp, err := qwpTestHTTPClient.Do(req)
	if err != nil {
		b.Fatalf("/exec %q failed: %v", statement, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("/exec %q: HTTP %d", statement, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		b.Fatalf("/exec %q: read body: %v", statement, err)
	}
	var result qwpTableResult
	if err := json.Unmarshal(body, &result); err != nil {
		b.Fatalf("/exec %q: decode: %v (body: %s)", statement, err, string(body))
	}
	return result
}

// jsonNumToInt64 extracts an integer from a generic-decoded JSON cell. The
// /exec endpoint emits numbers, which encoding/json unmarshals into float64
// when the target is interface{}.
func jsonNumToInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

// benchWaitTimeout is how long benchWaitForRows waits for asynchronous WAL
// apply to catch up after the seed Flush returns. QDB_BENCH_WAIT (a Go
// duration, e.g. "30m") overrides it; the default scales with row count
// because server-side apply is the slow part for large seeds. The timeout is
// only the give-up point -- the poll returns the instant the count matches --
// so a generous ceiling costs nothing on a healthy server.
func benchWaitTimeout(b *testing.B, rows int) time.Duration {
	if v := os.Getenv("QDB_BENCH_WAIT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			b.Fatalf("QDB_BENCH_WAIT=%q: %v", v, err)
		}
		return d
	}
	// 5m floor + ~1s per 100k rows (assumes >=100k rows/s end-to-end apply,
	// comfortably conservative). 100M rows -> ~22m ceiling.
	return 5*time.Minute + time.Duration(rows/100_000)*time.Second
}

// benchWaitForRows polls until table holds exactly want rows (WAL apply is
// asynchronous; ingest Flush returning does not mean the rows are queryable).
// Logs progress periodically so a multi-minute large-seed apply is observable
// under `go test -v -bench`.
func benchWaitForRows(b *testing.B, table string, want int) {
	b.Helper()
	timeout := benchWaitTimeout(b, want)
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		res := benchHTTPExec(b, fmt.Sprintf("SELECT count() FROM '%s'", table))
		if len(res.Dataset) == 1 && len(res.Dataset[0]) == 1 {
			got, ok := jsonNumToInt64(res.Dataset[0][0])
			if ok && got == int64(want) {
				return
			}
			if ok && time.Since(lastLog) >= 15*time.Second {
				b.Logf("WAL apply: %d / %d rows", got, want)
				lastLog = time.Now()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	b.Fatalf("timed out after %s waiting for %d rows in %q (override with QDB_BENCH_WAIT)",
		timeout, want, table)
}

// benchTableCount returns table's row count, or -1 if the table is absent or
// the count can't be read (so callers treat "unknown" as "needs populating").
// Unlike benchHTTPExec it never fails the benchmark -- a missing table is the
// expected pre-seed state, and /exec answers a missing table with HTTP 400.
func benchTableCount(table string) int64 {
	u, _ := url.Parse("http://" + benchEgressAddr())
	u.Path = "/exec"
	params := url.Values{}
	params.Add("query", fmt.Sprintf("SELECT count() FROM '%s'", table))
	u.RawQuery = params.Encode()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
	if err != nil {
		return -1
	}
	resp, err := qwpTestHTTPClient.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return -1
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1
	}
	var res qwpTableResult
	if err := json.Unmarshal(body, &res); err != nil {
		return -1
	}
	if len(res.Dataset) != 1 || len(res.Dataset[0]) != 1 {
		return -1
	}
	if n, ok := jsonNumToInt64(res.Dataset[0][0]); ok {
		return n
	}
	return -1
}

// benchEnsurePopulated runs populate() to (re)create and seed `table`, then
// waits for WAL apply -- unless the work can be safely skipped, in which case
// it returns fast. It is skipped when QDB_BENCH_SKIP_POPULATE is set, or when
// `table` already holds exactly wantRows rows.
//
// The row-count short-circuit is load-bearing, not just an optimization:
// `go test` invokes a benchmark body once at b.N=1 (the launch/estimate pass)
// and again at the real -benchtime N. Setup that lives in the body would
// otherwise run on every invocation -- which at QDB_BENCH_ROWS=100000000 means
// seeding 100M rows twice. The first pass seeds; the second sees the matching
// count and skips. It also makes re-runs against an existing table instant.
func benchEnsurePopulated(b *testing.B, table string, wantRows int, populate func()) {
	b.Helper()
	if benchEnvBool("QDB_BENCH_SKIP_POPULATE") {
		b.Logf("QDB_BENCH_SKIP_POPULATE set, reusing existing %s", table)
		return
	}
	if n := benchTableCount(table); n == int64(wantRows) {
		b.Logf("%s already holds %d rows, skipping populate "+
			"(DROP it or change QDB_BENCH_ROWS to force a reseed)", table, wantRows)
		return
	}
	populate()
	benchWaitForRows(b, table, wantRows)
}

// ---------------------------------------------------------------------------
// Latency percentile harness (shared by the two latency benchmarks)
// ---------------------------------------------------------------------------

// runQueryLatency drives `b.N` single-query round-trips through queryOnce,
// recording per-call wall time, and reports p50/p90/p99/p99.9 alongside the
// native ns/op mean. queryOnce must submit one query, drain it fully, and
// return any error -- exactly the work whose latency we attribute.
//
// This is the symmetric counterpart of the ingress side's per-row
// .At()+Flush() loop, and mirrors QwpEgressLatencyBenchmark: the client is
// opened once by the caller and reused across every measured invocation;
// table/connection setup is outside the timed region.
func runQueryLatency(b *testing.B, queryOnce func() error) {
	samples := make([]time.Duration, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		if err := queryOnce(); err != nil {
			b.Fatalf("query %d: %v", i, err)
		}
		samples[i] = time.Since(t0)
	}
	b.StopTimer()
	reportLatencyPercentiles(b, samples)
}

func reportLatencyPercentiles(b *testing.B, samples []time.Duration) {
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	n := len(samples)
	pick := func(p float64) float64 {
		idx := int(float64(n-1) * p)
		if idx > n-1 {
			idx = n - 1
		}
		return float64(samples[idx].Nanoseconds()) / 1e3 // -> microseconds
	}
	// Distinct unit strings so benchstat treats each as its own metric.
	// The ".0"/".9" suffixes keep them lexicographically ordered in
	// `go test` output (p50.0 < p90.0 < p99.0 < p99.9).
	b.ReportMetric(pick(0.50), "p50.0us/op")
	b.ReportMetric(pick(0.90), "p90.0us/op")
	b.ReportMetric(pick(0.99), "p99.0us/op")
	b.ReportMetric(pick(0.999), "p99.9us/op")
}

// ---------------------------------------------------------------------------
// BenchmarkQwpEgressLatency -- Go counterpart of QwpEgressLatencyBenchmark
// ---------------------------------------------------------------------------

// BenchmarkQwpEgressLatency measures the end-to-end wall time of a single
// query round-trip over QWP/WebSocket against a live local QuestDB, with the
// QwpQueryClient opened once and reused (connection setup excluded).
//
// Default SQL is "SELECT 1" -- no storage/cursor cost, so the number is the
// parse + protocol round-trip floor. Set QDB_BENCH_SQL to anything else (e.g.
// "SELECT id FROM latency_bench") to fold in storage and cursor cost; the
// latency_bench table is created and seeded with one row in setup so the
// default override works out of the box. QDB_BENCH_SKIP_POPULATE=1 reuses the
// existing table instead of dropping/recreating it.
func BenchmarkQwpEgressLatency(b *testing.B) {
	benchSkipIfNoServer(b)

	const table = "latency_bench"
	benchEnsurePopulated(b, table, 1, func() {
		benchHTTPExec(b, "DROP TABLE IF EXISTS '"+table+"'")
		benchHTTPExec(b, "CREATE TABLE '"+table+"' (id LONG, ts TIMESTAMP) "+
			"TIMESTAMP(ts) PARTITION BY DAY WAL")
		seedRows(b, table, 1, func(s LineSender, i int) error {
			return s.Table(table).Int64Column("id", 1).
				At(context.Background(), time.Unix(0, 0).UTC())
		})
	})

	sql := benchEnvStr("QDB_BENCH_SQL", "SELECT 1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := NewQwpQueryClient(ctx,
		WithQwpQueryAddress(benchEgressAddr()),
		WithQwpQueryClientID("qwp-egress-bench-go/1.0"),
	)
	if err != nil {
		b.Fatalf("NewQwpQueryClient: %v", err)
	}
	defer client.Close(ctx)

	queryOnce := func() error {
		q := client.Query(ctx, sql)
		_, _, err := drainQuery(q)
		q.Close()
		return err
	}

	// Prime: first query allocates the client's codec scratch and registers
	// the result schema. Keeps that one-time cost out of the window, exactly
	// like the Java benchmark's throwaway @Setup query.
	if err := queryOnce(); err != nil {
		b.Fatalf("prime query: %v", err)
	}

	runQueryLatency(b, queryOnce)
}

// ---------------------------------------------------------------------------
// BenchmarkQwpEgressBindLatency -- Go counterpart of
// QwpEgressBindLatencyBenchmark
// ---------------------------------------------------------------------------

// BenchmarkQwpEgressBindLatency measures the same single-query round-trip but
// with a bind-variable query: SELECT x FROM long_sequence(10) WHERE x = $1,
// where $1 is a random LONG in [1,10] per call. The value randomizes but the
// bind TYPE does not, so the server's select cache should hit every call
// after the first. Comparing this against BenchmarkQwpEgressLatency running
// the literal "SELECT 1" isolates bind encode/decode + cache-lookup overhead.
//
// long_sequence(10) is the row source, so this benchmark needs no table and
// no WAL-apply wait.
func BenchmarkQwpEgressBindLatency(b *testing.B) {
	benchSkipIfNoServer(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := NewQwpQueryClient(ctx,
		WithQwpQueryAddress(benchEgressAddr()),
		WithQwpQueryClientID("qwp-egress-bind-bench-go/1.0"),
	)
	if err != nil {
		b.Fatalf("NewQwpQueryClient: %v", err)
	}
	defer client.Close(ctx)

	const sql = "SELECT x FROM long_sequence(10) WHERE x = $1"
	rng := rand.New(rand.NewSource(1)) // deterministic value stream

	queryOnce := func() error {
		v := int64(rng.Intn(10) + 1)
		q := client.Query(ctx, sql, WithQueryBinds(func(bv *QwpBinds) {
			bv.LongBind(0, v)
		}))
		_, _, err := drainQuery(q)
		q.Close()
		return err
	}

	if err := queryOnce(); err != nil {
		b.Fatalf("prime query: %v", err)
	}
	runQueryLatency(b, queryOnce)
}

// ---------------------------------------------------------------------------
// Shared low-level helpers
// ---------------------------------------------------------------------------

// drainQuery consumes every batch of q, doing no per-row work -- the egress
// equivalent of QwpEgressLatencyBenchmark's deliberately empty batch handler.
// Returns rows seen and total batch-payload bytes.
func drainQuery(q *QwpQuery) (rows int, bytes int64, err error) {
	for batch, e := range q.Batches() {
		if e != nil {
			return rows, bytes, e
		}
		rows += batch.RowCount()
		bytes += int64(len(batch.Payload()))
	}
	return rows, bytes, nil
}

// seedRows ingests `n` rows into `table` over a fresh public QWP LineSender
// (ws://, auto-flush every 50k rows -- same shape as the Java read bench's
// Sender.fromConfig). rowFn fills one row; it must call At/AtNow itself so
// the caller controls the designated timestamp.
func seedRows(b *testing.B, table string, n int, rowFn func(s LineSender, i int) error) {
	b.Helper()
	ctx := context.Background()
	conf := fmt.Sprintf("ws::addr=%s;auto_flush_rows=50000;", benchEgressAddr())
	s, err := LineSenderFromConf(ctx, conf)
	if err != nil {
		b.Fatalf("LineSenderFromConf(%q): %v", conf, err)
	}
	defer s.Close(ctx)
	start := time.Now()
	lastLog := start
	for i := 0; i < n; i++ {
		if err := rowFn(s, i); err != nil {
			b.Fatalf("seed row %d: %v", i, err)
		}
		// Progress for large seeds: a 100M-row ingest is several minutes
		// of otherwise-silent work. Matches the Java benches' per-1M log.
		if n >= 1_000_000 && (i+1)%1_000_000 == 0 && time.Since(lastLog) >= 10*time.Second {
			elapsed := time.Since(start).Seconds()
			b.Logf("seeded %d / %d rows (%.0f rows/s)", i+1, n, float64(i+1)/elapsed)
			lastLog = time.Now()
		}
	}
	if err := s.Flush(ctx); err != nil {
		b.Fatalf("seed flush: %v", err)
	}
	if n >= 1_000_000 {
		b.Logf("seeded %d rows in %s, waiting for WAL apply...", n, time.Since(start).Round(time.Second))
	}
}
