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

// Wide variant of the QWP egress benchmark. Compares SELECT throughput
// from a locally running QuestDB instance over three wire protocols on
// a 15-column row:
//
//   - QWP egress (WebSocket, binary columnar)
//   - PostgreSQL wire (binary transfer)
//   - HTTP /exec (JSON)
//
// Schema: designated TIMESTAMP, one LONG, one DOUBLE, six SYMBOLs (one
// low-cardinality with 8 distinct values, five high-cardinality with
// 100k distinct values each), one VARCHAR, and five additional DOUBLEs.
// Mirrors QwpEgressReadBenchmarkWide.java in benchmarks/.
//
// Prerequisites:
//   - A QuestDB server listening on 9000 (HTTP/WS) and 8812 (PG wire).
//
// Tune the workload via flags:
//
//	-rows N           row count to ingest (default 10_000_000)
//	-skip-populate    re-use the existing table (default false)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	qdb "github.com/questdb/go-questdb-client/v4"
)

const (
	host          = "localhost"
	httpPort      = 9000
	pgPort        = 8812
	progressEvery = 1_000_000
	tableName     = "egress_bench_wide"
	// highCard is the distinct value count for each of s1..s5. Sized
	// large enough to stress the SYMBOL dict path: 100k unique values
	// per column means the connection-scoped delta dict grows for most
	// of the batch sequence rather than settling into a cached state.
	highCard = 100_000
)

var (
	rowCount     int64
	skipPopulate bool
)

type result struct {
	elapsed time.Duration
	rows    int64
	bytes   int64
}

func main() {
	flag.Int64Var(&rowCount, "rows", 10_000_000, "row count")
	flag.BoolVar(&skipPopulate, "skip-populate", false, "skip table create + ingest, re-use existing data")
	flag.Parse()

	ctx := context.Background()

	if !skipPopulate {
		mustOK(recreateTable(ctx))
		mustOK(ingestRows(ctx))
	} else {
		fmt.Printf("skip-populate=true, re-using existing %s\n", tableName)
	}

	fmt.Println()
	fmt.Println("=== Cold warm-up (runs discarded) ===")
	if _, err := runQwp(ctx, true); err != nil {
		log.Fatalf("QWP warmup: %v", err)
	}
	if _, err := runPgWire(ctx, true); err != nil {
		log.Fatalf("PG warmup: %v", err)
	}
	if _, err := runHTTPExec(ctx, true); err != nil {
		log.Fatalf("HTTP warmup: %v", err)
	}

	fmt.Println()
	fmt.Println("=== Measurement ===")
	qwp, err := runQwp(ctx, false)
	if err != nil {
		log.Fatalf("QWP: %v", err)
	}
	pg, err := runPgWire(ctx, false)
	if err != nil {
		log.Fatalf("PG: %v", err)
	}
	httpRes, err := runHTTPExec(ctx, false)
	if err != nil {
		log.Fatalf("HTTP: %v", err)
	}

	fmt.Println()
	fmt.Println("=== Comparison ===")
	fmt.Printf("%-20s %12s %12s %12s\n", "Protocol", "time(ms)", "rows/sec", "MiB/sec")
	fmt.Printf("%-20s %12s %12s %12s\n", "--------", "--------", "--------", "-------")
	printRow("QWP egress (WS)", qwp)
	printRow("PostgreSQL wire", pg)
	printRow("HTTP /exec JSON", httpRes)
}

func mustOK(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func printRow(label string, r result) {
	secs := r.elapsed.Seconds()
	rowsPerSec := float64(r.rows) / secs
	mibPerSec := float64(r.bytes) / secs / (1024.0 * 1024.0)
	fmt.Printf("%-20s %12d %12.0f %12.2f\n",
		label, r.elapsed.Milliseconds(), rowsPerSec, mibPerSec)
}

// ------------------------------------------------------------------
// Workload
// ------------------------------------------------------------------

func pgConnString() string {
	return fmt.Sprintf("postgres://admin:quest@%s:%d/qdb?sslmode=disable", host, pgPort)
}

// selectColumns is the comma-separated SELECT list shared by every
// runner. Kept in one place so adding/removing a column needs a single
// edit, and the QWP column-index mapping in runQwp stays trivially
// auditable against this list.
const selectColumns = "ts, id, price, sym, note," +
	" d1, d2, d3, d4, d5," +
	" s1, s2, s3, s4, s5"

func recreateTable(ctx context.Context) error {
	c, err := qdb.NewQwpQueryClient(ctx, qdb.WithQwpQueryAddress(fmt.Sprintf("%s:%d", host, httpPort)))
	if err != nil {
		return fmt.Errorf("recreateTable: connect: %w", err)
	}
	defer c.Close(ctx)

	if _, err := c.Exec(ctx, "DROP TABLE IF EXISTS '"+tableName+"'"); err != nil {
		return fmt.Errorf("recreateTable: drop: %w", err)
	}
	// Wide schema: low-cardinality sym + five high-cardinality SYMBOLs
	// (capacity 200000 to fit the 100k distinct values per column
	// with comfortable slack) + five extra DOUBLEs. Representative of
	// a realistic analytics row with mixed numerics and several
	// categorical dimensions of differing cardinality.
	createSQL := "CREATE TABLE '" + tableName + "' (" +
		"ts TIMESTAMP, id LONG, price DOUBLE, sym SYMBOL, note VARCHAR," +
		" d1 DOUBLE, d2 DOUBLE, d3 DOUBLE, d4 DOUBLE, d5 DOUBLE," +
		" s1 SYMBOL capacity 200000, s2 SYMBOL capacity 200000," +
		" s3 SYMBOL capacity 200000, s4 SYMBOL capacity 200000," +
		" s5 SYMBOL capacity 200000" +
		") TIMESTAMP(ts) PARTITION BY HOUR WAL"
	if _, err := c.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("recreateTable: create: %w", err)
	}
	return nil
}

func ingestRows(ctx context.Context) error {
	fmt.Printf("Ingesting %d rows over QWP/WebSocket...\n", rowCount)
	start := time.Now()
	symbols := []string{"AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "NFLX"}
	// Pre-generate the 100k unique values per high-cardinality column
	// so the ingest loop reuses interned strings instead of allocating
	// fresh ones per row. Rotating s1..s5 through different offsets
	// makes any cross-column correlation coincidental.
	s1Pool := buildSymbolPool("s1_")
	s2Pool := buildSymbolPool("s2_")
	s3Pool := buildSymbolPool("s3_")
	s4Pool := buildSymbolPool("s4_")
	s5Pool := buildSymbolPool("s5_")

	// auto_flush_rows sized so each ILP frame stays under the server's
	// 2 MiB WebSocket buffer given the 15-column row layout (~130
	// bytes/row encoded).
	conf := fmt.Sprintf("ws::addr=%s:%d;auto_flush_rows=10000;", host, httpPort)
	sender, err := qdb.LineSenderFromConf(ctx, conf)
	if err != nil {
		return fmt.Errorf("ingest: open sender: %w", err)
	}
	defer sender.Close(ctx)

	for i := int64(1); i <= rowCount; i++ {
		h1 := i % highCard
		h2 := (i + 20_000) % highCard
		h3 := (i + 40_000) % highCard
		h4 := (i + 60_000) % highCard
		h5 := (i + 80_000) % highCard
		// ILP requires all Symbol calls before any non-symbol column setters.
		if err := sender.Table(tableName).
			Symbol("sym", symbols[i%int64(len(symbols))]).
			Symbol("s1", s1Pool[h1]).
			Symbol("s2", s2Pool[h2]).
			Symbol("s3", s3Pool[h3]).
			Symbol("s4", s4Pool[h4]).
			Symbol("s5", s5Pool[h5]).
			Int64Column("id", i).
			Float64Column("price", float64(i)*1.5).
			Float64Column("d1", float64(i)*0.25).
			Float64Column("d2", float64(i)*0.5).
			Float64Column("d3", float64(i)*0.75).
			Float64Column("d4", float64(i)*1.25).
			Float64Column("d5", float64(i)*1.75).
			StringColumn("note", "n"+strconv.FormatInt(i&0xFFF, 10)).
			At(ctx, time.UnixMicro(i*10_000)); err != nil {
			return fmt.Errorf("ingest: At row %d: %w", i, err)
		}
		if i%progressEvery == 0 {
			fmt.Printf("  %d / %d rows (%d ms)\n", i, rowCount, time.Since(start).Milliseconds())
		}
	}
	if err := sender.Flush(ctx); err != nil {
		return fmt.Errorf("ingest: flush: %w", err)
	}

	fmt.Println("Waiting for WAL apply to complete...")
	return waitForWalApply(ctx)
}

func buildSymbolPool(prefix string) []string {
	pool := make([]string, highCard)
	for i := 0; i < highCard; i++ {
		pool[i] = prefix + strconv.Itoa(i)
	}
	return pool
}

func waitForWalApply(ctx context.Context) error {
	c, err := qdb.NewQwpQueryClient(ctx, qdb.WithQwpQueryAddress(fmt.Sprintf("%s:%d", host, httpPort)))
	if err != nil {
		return fmt.Errorf("wait: connect: %w", err)
	}
	defer c.Close(ctx)

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		count, err := selectCount(ctx, c)
		if err != nil {
			return fmt.Errorf("wait: count: %w", err)
		}
		if count == rowCount {
			fmt.Printf("  applied %d rows\n", count)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.New("timed out waiting for WAL apply")
}

func selectCount(ctx context.Context, c *qdb.QwpQueryClient) (int64, error) {
	q := c.Query(ctx, "SELECT count() FROM "+tableName)
	defer q.Close()
	var count int64
	for batch, err := range q.Batches() {
		if err != nil {
			return 0, err
		}
		if batch.RowCount() > 0 {
			count = batch.Column(0).Int64(0)
		}
	}
	return count, nil
}

// ------------------------------------------------------------------
// QWP egress
// ------------------------------------------------------------------

func runQwp(ctx context.Context, warmup bool) (result, error) {
	var rowsSeen, bytesSeen, checksum int64
	start := time.Now()

	c, err := qdb.NewQwpQueryClient(ctx,
		qdb.WithQwpQueryAddress(fmt.Sprintf("%s:%d", host, httpPort)),
		qdb.WithQwpQueryClientID("qwp-egress-bench-wide/1.0"),
		qdb.WithQwpQueryCompression("raw"),
	)
	if err != nil {
		return result{}, err
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "SELECT "+selectColumns+" FROM "+tableName)
	defer q.Close()
	for batch, err := range q.Batches() {
		if err != nil {
			return result{}, err
		}
		n := batch.RowCount()
		// Cache the per-column handles once per batch so each cell
		// access skips re-deriving the layout pointer — same idiom as
		// the Java path that grabs valuesAddr / nonNullIndex up front.
		// Read 8-byte fixed-width columns (ts, id, price, d1..d5) as
		// raw int64 bits and XOR them straight in; matches Java's
		// Unsafe.getLong on the DOUBLE column bases.
		tsCol := batch.Column(0)
		idCol := batch.Column(1)
		priceCol := batch.Column(2)
		symCol := batch.Column(3)
		noteCol := batch.Column(4)
		d1Col := batch.Column(5)
		d2Col := batch.Column(6)
		d3Col := batch.Column(7)
		d4Col := batch.Column(8)
		d5Col := batch.Column(9)
		s1Col := batch.Column(10)
		s2Col := batch.Column(11)
		s3Col := batch.Column(12)
		s4Col := batch.Column(13)
		s5Col := batch.Column(14)
		for r := 0; r < n; r++ {
			ts := tsCol.Int64(r)
			id := idCol.Int64(r)
			priceBits := priceCol.Int64(r)
			d1 := d1Col.Int64(r)
			d2 := d2Col.Int64(r)
			d3 := d3Col.Int64(r)
			d4 := d4Col.Int64(r)
			d5 := d5Col.Int64(r)
			sym := symCol.Str(r)
			note := noteCol.Str(r)
			s1 := s1Col.Str(r)
			s2 := s2Col.Str(r)
			s3 := s3Col.Str(r)
			s4 := s4Col.Str(r)
			s5 := s5Col.Str(r)
			checksum ^= ts ^ id ^ priceBits ^ d1 ^ d2 ^ d3 ^ d4 ^ d5 ^
				int64(len(sym)) ^ int64(len(note)) ^
				int64(len(s1)) ^ int64(len(s2)) ^ int64(len(s3)) ^
				int64(len(s4)) ^ int64(len(s5))
		}
		rowsSeen += int64(n)
		// Sum the actual QWP message bytes delivered in this frame
		// plus a 10-byte WebSocket-header approximation, matching the
		// calculation in the Java benchmark, so the bytes/sec column
		// is comparable.
		bytesSeen += int64(len(batch.Payload())) + 10
	}
	elapsed := time.Since(start)
	logRun("QWP", warmup, elapsed, rowsSeen, fmt.Sprintf("0x%x", uint64(checksum)))
	return result{elapsed: elapsed, rows: rowsSeen, bytes: bytesSeen}, nil
}

// ------------------------------------------------------------------
// PostgreSQL wire
// ------------------------------------------------------------------

func runPgWire(ctx context.Context, warmup bool) (result, error) {
	var rows, checksum, bytes int64
	start := time.Now()

	cfg, err := pgx.ParseConfig(pgConnString())
	if err != nil {
		return result{}, err
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return result{}, err
	}
	defer conn.Close(ctx)

	qrows, err := conn.Query(ctx, "SELECT "+selectColumns+" FROM "+tableName)
	if err != nil {
		return result{}, err
	}
	defer qrows.Close()

	for qrows.Next() {
		var ts time.Time
		var id int64
		var price, d1, d2, d3, d4, d5 float64
		var sym, note, s1, s2, s3, s4, s5 string
		if err := qrows.Scan(
			&ts, &id, &price, &sym, &note,
			&d1, &d2, &d3, &d4, &d5,
			&s1, &s2, &s3, &s4, &s5,
		); err != nil {
			return result{}, err
		}
		// Normalise to epoch microseconds so the checksum matches the
		// QWP path. Java's getTimestamp().getTime()*1000 truncates to
		// ms*1000; QuestDB's micros are 10ms-aligned in this dataset
		// so both forms agree.
		tsMicros := ts.UnixMicro()
		checksum ^= tsMicros ^ id ^
			int64(math.Float64bits(price)) ^
			int64(math.Float64bits(d1)) ^ int64(math.Float64bits(d2)) ^
			int64(math.Float64bits(d3)) ^ int64(math.Float64bits(d4)) ^
			int64(math.Float64bits(d5)) ^
			int64(len(sym)) ^ int64(len(note)) ^
			int64(len(s1)) ^ int64(len(s2)) ^ int64(len(s3)) ^
			int64(len(s4)) ^ int64(len(s5))
		// PG DataRow wire size per row in binary mode: 1 byte 'D' msg
		// tag, 4 bytes msg length, 2 bytes col count, then a 4-byte
		// length prefix + value for each of the 15 columns. 8 fixed-
		// width 8-byte cols (ts, id, price, d1..d5), 7 variable-length
		// cols (sym, note, s1..s5).
		bytes += 7 + 15*4 + 8*8 +
			int64(len(sym)) + int64(len(note)) +
			int64(len(s1)) + int64(len(s2)) + int64(len(s3)) +
			int64(len(s4)) + int64(len(s5))
		rows++
	}
	if err := qrows.Err(); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)
	logRun("PG", warmup, elapsed, rows, fmt.Sprintf("0x%x", uint64(checksum)))
	return result{elapsed: elapsed, rows: rows, bytes: bytes}, nil
}

// ------------------------------------------------------------------
// HTTP /exec JSON
// ------------------------------------------------------------------

func runHTTPExec(ctx context.Context, warmup bool) (result, error) {
	var bytes int64
	start := time.Now()

	sql := "SELECT " + selectColumns + " FROM " + tableName
	u := fmt.Sprintf("http://%s:%d/exec?query=%s&count=true",
		host, httpPort, url.QueryEscape(sql))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return result{}, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result{}, fmt.Errorf("HTTP /exec: status %s", resp.Status)
	}

	// JSON response is one line with {"columns":[...],"dataset":[[...],...]}.
	// Scan for '[' to count rows — same approximation as the Java path.
	var brackets int64
	buf := make([]byte, 16*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			bytes += int64(n)
			for i := 0; i < n; i++ {
				if buf[i] == '[' {
					brackets++
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return result{}, err
		}
	}
	// Brackets counter incremented for every '[' including the outer
	// "columns" wrapper and the "dataset" wrapper; subtract those two.
	var rows int64
	if brackets > 1 {
		rows = brackets - 2
	}
	elapsed := time.Since(start)
	logRun("HTTP", warmup, elapsed, rows, strconv.FormatInt(bytes, 10))
	return result{elapsed: elapsed, rows: rows, bytes: bytes}, nil
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

func logRun(label string, warmup bool, elapsed time.Duration, rows int64, suffix string) {
	phase := "[measure]"
	if warmup {
		phase = "[warmup]"
	}
	fmt.Printf("%s %s : %d rows in %d ms (checksum/bytes=%s)\n",
		phase, label, rows, elapsed.Milliseconds(), suffix)
}

