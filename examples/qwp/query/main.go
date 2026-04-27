package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

const (
	tableName = "qwp_query_example"
	rowCount  = 1000
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := qdb.NewQwpQueryClient(ctx,
		qdb.WithQwpQueryAddress("localhost:9000"),
	)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close(ctx)

	if _, err := client.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS '%s'", tableName)); err != nil {
		log.Fatalf("drop: %v", err)
	}
	createSQL := fmt.Sprintf(
		"CREATE TABLE '%s' (ts TIMESTAMP, v LONG) TIMESTAMP(ts)",
		tableName)
	if _, err := client.Exec(ctx, createSQL); err != nil {
		log.Fatalf("create: %v", err)
	}

	insertSQL := buildBulkInsert(tableName, rowCount)
	res, err := client.Exec(ctx, insertSQL)
	if err != nil {
		log.Fatalf("insert: %v", err)
	}
	fmt.Printf("inserted %d rows\n", res.RowsAffected)

	expected := expectedSum(rowCount)
	fmt.Printf("expected sum: %d\n", expected)
	fmt.Printf("per-row sum:  %d\n", sumPerRow(ctx, client))
	fmt.Printf("bulk sum:     %d\n", sumBulk(ctx, client))
}

// sumPerRow demonstrates the zero-allocation, per-row idiom.
//
// QwpColumn caches the column's layout pointer once per batch, so every
// Int64(r) call reads straight out of the QWP buffer — no intermediate
// slice. Best for ad-hoc consumers and when you also need per-row
// branching (null checks, mixed-column row builders).
func sumPerRow(ctx context.Context, client *qdb.QwpQueryClient) int64 {
	q := client.Query(ctx, fmt.Sprintf("SELECT ts, v FROM '%s'", tableName))
	defer q.Close()

	var sum int64
	for batch, err := range q.Batches() {
		if err != nil {
			log.Fatalf("per-row query: %v", err)
		}
		vCol := batch.Column(1) // column 1 is `v` (LONG)
		n := vCol.RowCount()
		for r := 0; r < n; r++ {
			sum += vCol.Int64(r)
		}
	}
	return sum
}

// sumBulk demonstrates the bulk-decode idiom for a tight column sweep.
//
// Int64Range decodes a row range into a caller-owned []int64 in one
// shot. On a no-null column it lowers to a single memmove out of the
// QWP buffer, after which the inner sum is a branch-free range loop the
// compiler can vectorize. Reuse the buffer across batches with [:0] —
// allocation happens once for the whole query.
func sumBulk(ctx context.Context, client *qdb.QwpQueryClient) int64 {
	q := client.Query(ctx, fmt.Sprintf("SELECT ts, v FROM '%s'", tableName))
	defer q.Close()

	var (
		sum int64
		buf = make([]int64, 0, rowCount)
	)
	for batch, err := range q.Batches() {
		if err != nil {
			log.Fatalf("bulk query: %v", err)
		}
		buf = batch.Column(1).Int64Range(0, batch.RowCount(), buf[:0])
		for _, v := range buf {
			sum += v
		}
	}
	return sum
}

func buildBulkInsert(table string, n int) string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO '%s' (ts, v) VALUES ", table)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		// QuestDB TIMESTAMP literals are microseconds since epoch.
		ts := base.Add(time.Duration(i) * time.Second).UnixMicro()
		fmt.Fprintf(&sb, "(%d,%d)", ts, int64(i))
	}
	return sb.String()
}

func expectedSum(n int) int64 {
	return int64(n) * int64(n-1) / 2
}
