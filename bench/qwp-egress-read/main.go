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

// Application-style benchmark that measures SELECT throughput from a
// locally running QuestDB instance over three wire protocols and prints
// a comparison:
//
//   - QWP egress (WebSocket, binary columnar)
//   - PostgreSQL wire (binary transfer)
//   - HTTP /exec (JSON)
//
// Narrow variant: five columns (designated timestamp, one LONG, one
// DOUBLE, one low-cardinality SYMBOL, one VARCHAR). Mirrors the Java
// QwpEgressReadBenchmark.java in benchmarks/.
//
// Prerequisites:
//   - A QuestDB server listening on 9000 (HTTP/WS) and 8812 (PG wire).
//
// Tune the workload via flags:
//   -rows N           row count to ingest (default 10_000_000)
//   -skip-populate    re-use the existing table (default false)
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
	tableName     = "egress_bench"
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
	// Java has these commented out — the JVM JIT warmup for QWP is
	// the only thing that matters in the original. The Go runtime
	// has no JIT to warm, but warming the server-side buffer cache
	// and TCP windows is still useful, so leave the calls available
	// for callers who want symmetrical warmups.
	//
	// if _, err := runPgWire(ctx, true); err != nil {
	// 	log.Fatalf("PG warmup: %v", err)
	// }
	// if _, err := runHTTPExec(ctx, true); err != nil {
	// 	log.Fatalf("HTTP warmup: %v", err)
	// }

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

func recreateTable(ctx context.Context) error {
	// DDL goes through the QWP query channel (Exec) so the bench does
	// not need a working PG connection just to set up the table — the
	// PG run later will fail loudly if the wire is unreachable, but
	// schema management does not have to.
	c, err := qdb.NewQwpQueryClient(ctx, qdb.WithQwpQueryAddress(fmt.Sprintf("%s:%d", host, httpPort)))
	if err != nil {
		return fmt.Errorf("recreateTable: connect: %w", err)
	}
	defer c.Close(ctx)

	if _, err := c.Exec(ctx, "DROP TABLE IF EXISTS '"+tableName+"'"); err != nil {
		return fmt.Errorf("recreateTable: drop: %w", err)
	}
	createSQL := "CREATE TABLE '" + tableName + "' (" +
		"ts TIMESTAMP, id LONG, price DOUBLE, sym SYMBOL, note VARCHAR" +
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

	conf := fmt.Sprintf("ws::addr=%s:%d;auto_flush_rows=50000;", host, httpPort)
	sender, err := qdb.LineSenderFromConf(ctx, conf)
	if err != nil {
		return fmt.Errorf("ingest: open sender: %w", err)
	}
	defer sender.Close(ctx)

	for i := int64(1); i <= rowCount; i++ {
		// ILP requires all Symbol calls before any non-symbol column setters.
		if err := sender.Table(tableName).
			Symbol("sym", symbols[i%int64(len(symbols))]).
			Int64Column("id", i).
			Float64Column("price", float64(i)*1.5).
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
		qdb.WithQwpQueryClientID("qwp-egress-bench/1.0"),
		qdb.WithQwpQueryCompression("raw"),
	)
	if err != nil {
		return result{}, err
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "SELECT ts, id, price, sym, note FROM "+tableName)
	defer q.Close()
	for batch, err := range q.Batches() {
		if err != nil {
			return result{}, err
		}
		n := batch.RowCount()
		// Cache the per-column handles once per batch so each cell
		// access skips re-deriving the layout pointer — same idiom as
		// the Java path that grabs valuesAddr / nonNullIndex up front.
		tsCol := batch.Column(0)
		idCol := batch.Column(1)
		priceCol := batch.Column(2)
		symCol := batch.Column(3)
		noteCol := batch.Column(4)
		for r := 0; r < n; r++ {
			ts := tsCol.Int64(r)
			id := idCol.Int64(r)
			priceBits := int64(math.Float64bits(priceCol.Float64(r)))
			sym := symCol.Str(r)
			note := noteCol.Str(r)
			checksum ^= ts ^ id ^ priceBits ^ int64(len(sym)) ^ int64(len(note))
		}
		rowsSeen += int64(n)
		// Sum the actual QWP message bytes delivered in this frame
		// plus a 10-byte WebSocket-header approximation, matching the
		// calculation in Java benchmark, so the bytes/sec column is comparable.
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

	qrows, err := conn.Query(ctx, "SELECT ts, id, price, sym, note FROM "+tableName)
	if err != nil {
		return result{}, err
	}
	defer qrows.Close()

	for qrows.Next() {
		var ts time.Time
		var id int64
		var price float64
		var sym, note string
		if err := qrows.Scan(&ts, &id, &price, &sym, &note); err != nil {
			return result{}, err
		}
		// Normalise to epoch microseconds so the checksum matches the
		// QWP path. Java's getTimestamp().getTime()*1000 truncates to
		// ms*1000; QuestDB's micros are 10ms-aligned in this dataset
		// so both forms agree.
		tsMicros := ts.UnixMicro()
		priceBits := int64(math.Float64bits(price))
		checksum ^= tsMicros ^ id ^ priceBits ^ int64(len(sym)) ^ int64(len(note))
		// PG DataRow wire size per row in binary mode: 1 byte 'D' msg
		// tag, 4 bytes msg length, 2 bytes col count, then a 4-byte
		// length prefix + value for each of the 5 columns. ts/id/price
		// are 8 bytes each.
		bytes += 7 + 5*4 + 8*3 + int64(len(sym)) + int64(len(note))
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

	sql := "SELECT ts,id,price,sym,note FROM " + tableName
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
