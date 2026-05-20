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

//go:build !windows

package questdb

// Go port of QuestDB's QwpEgressFragmentationFuzzTest. Stress the QWP
// egress state machines under artificial network fragmentation: a
// sidecar server is booted with BOTH the recv- and send-side debug
// chunk-size knobs forced to a tiny value, so every wire frame spans
// many partial socket reads/writes and the server's frame parser,
// HTTP response sink, and egress streamResults loop must survive being
// preempted / resumed at arbitrary byte boundaries.
//
// The Java property keys map to env vars via the QDB_* prefix:
//   debug.http.force.recv.fragmentation.chunk.size ->
//     QDB_DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE
//   debug.http.force.send.fragmentation.chunk.size ->
//     QDB_DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE
//
// The client side is plain QwpQueryClient — the property under test
// is server-side handling of micro-chunked wire bytes. The client
// only needs longer-than-default deadlines because tiny chunks slow
// the handshake / drain dramatically.
//
// All tests require fixture-launched mode (sidecar JVM with env
// overrides); skip in QDB_FUZZ_ADDR mode.

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"
)

// bootEgressFragmentedServer boots a sidecar QuestDB with both
// fragmentation chunk-size knobs forced to chunk. The smaller chunk
// is, the more aggressive the wire fragmentation: chunk=1 makes every
// byte its own socket-level event (including the WebSocket handshake
// response, every WS frame header, every QWP prelude, every CREDIT
// frame body).
func bootEgressFragmentedServer(t *testing.T, chunk int) *qwpFuzzServer {
	t.Helper()
	return bootSidecarServer(t, map[string]string{
		"QDB_DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE": strconv.Itoa(chunk),
		"QDB_DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE": strconv.Itoa(chunk),
	})
}

// fragFuzzPickChunk mirrors Java QwpEgressFragmentationFuzzTest.pickChunk:
// 1..500-byte chunk. The mode is "be aggressive enough that even tiny
// wire frames span many iterations and the state machine must survive
// preemption at arbitrary points".
func fragFuzzPickChunk(r interface {
	Intn(int) int
}) int {
	return 1 + r.Intn(500)
}

// fragFuzzRunAndVerify runs `SELECT * FROM <table>` against the
// fragmented server and verifies rowCount + sum(id). The id sum
// expectation (n*(n+1)/2) follows from QuestDB's long_sequence(n)
// producing 1..n in id. Mirrors Java's runAndVerify.
//
// Uses a long context so the handshake/drain has room — at chunk=1
// the entire wire path is single-byte socket events and even a small
// result set takes seconds.
func fragFuzzRunAndVerify(t *testing.T, c *QwpQueryClient, table string, expectedRows int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT * FROM '"+table+"'")
	defer q.Close()
	var idSum int64
	rows := 0
	idCol := -1
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("table %q query error: %v", table, err)
		}
		if idCol < 0 {
			for i := 0; i < batch.ColumnCount(); i++ {
				if batch.ColumnName(i) == "id" {
					idCol = i
					break
				}
			}
			if idCol < 0 {
				t.Fatalf("table %q: no 'id' column in result (cols: %d)", table, batch.ColumnCount())
			}
		}
		for r := 0; r < batch.RowCount(); r++ {
			if batch.IsNull(idCol, r) {
				t.Fatalf("table %q row %d: id is NULL — wire fragmentation lost a value", table, rows+r)
			}
			idSum += batch.Int64(idCol, r)
		}
		rows += batch.RowCount()
	}
	if rows != expectedRows {
		t.Fatalf("table %q: got %d rows, expected %d", table, rows, expectedRows)
	}
	wantSum := int64(expectedRows) * int64(expectedRows+1) / 2
	if idSum != wantSum {
		t.Fatalf("table %q: id sum %d != expected %d (rowCount matches but values drifted)",
			table, idSum, wantSum)
	}
}

// fragFuzzNewClient opens a QwpQueryClient against the sidecar with
// optional extra connect-string options (e.g. initial_credit). The
// sidecar's connConf gives the bare address; the caller appends.
func fragFuzzNewClient(t *testing.T, srv *qwpFuzzServer, extra string) *QwpQueryClient {
	t.Helper()
	conf := srv.connConf() + extra
	// Generous connect timeout — at chunk=1 the WS handshake alone takes
	// hundreds of socket events to complete.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c, err := QwpQueryClientFromConf(ctx, conf)
	if err != nil {
		t.Fatalf("QwpQueryClientFromConf(%q): %v", conf, err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer ccancel()
		_ = c.Close(cctx)
	})
	return c
}

// --- entry points -------------------------------------------------

// TestQwpFuzzEgressFragmentedBackToBackQueries — port of Java
// testFragmentedBackToBackQueries. Five sequential queries on the
// same connection — shakes out cross-query state that might have
// picked up residue from a fragmented prior query.
func TestQwpFuzzEgressFragmentedBackToBackQueries(t *testing.T) {
	r := newFuzzRand(t)
	chunk := fragFuzzPickChunk(r)
	t.Logf("chunk=%d", chunk)

	srv := bootEgressFragmentedServer(t, chunk)
	srv.mustExec(t, "CREATE TABLE btb(id LONG, v DOUBLE, ts TIMESTAMP) "+
		"TIMESTAMP(ts) PARTITION BY DAY WAL")
	srv.mustExec(t, "INSERT INTO btb SELECT x, CAST(x * 2.5 AS DOUBLE), x::TIMESTAMP "+
		"FROM long_sequence(8000)")
	awaitTableRowsViaCount(t, srv, "btb", 8000, 60*time.Second)

	c := fragFuzzNewClient(t, srv, "")
	for q := 0; q < 5; q++ {
		fragFuzzRunAndVerify(t, c, "btb", 8000)
	}
}

// TestQwpFuzzEgressFragmentedCreditFlow — port of Java
// testFragmentedCreditFlow. Small initial credit (2 KiB) forces the
// server to interleave RESULT_BATCH bytes with CREDIT frames from the
// client; both directions are chunked so the server's recv-side
// parser must stitch CREDIT bodies split across multiple partial
// reads.
func TestQwpFuzzEgressFragmentedCreditFlow(t *testing.T) {
	r := newFuzzRand(t)
	chunk := fragFuzzPickChunk(r)
	t.Logf("chunk=%d", chunk)

	srv := bootEgressFragmentedServer(t, chunk)
	srv.mustExec(t,
		"CREATE TABLE cf AS (SELECT x AS id, x::TIMESTAMP AS ts FROM long_sequence(20000)) "+
			"TIMESTAMP(ts) PARTITION BY DAY WAL")
	awaitTableRowsViaCount(t, srv, "cf", 20_000, 60*time.Second)

	c := fragFuzzNewClient(t, srv, "initial_credit=2048;")
	fragFuzzRunAndVerify(t, c, "cf", 20_000)
}

// TestQwpFuzzEgressFragmentedStreamingBigResult — port of Java
// testFragmentedStreamingBigResult. 50K rows over a chunked wire —
// stresses the egress streamResults loop's long-running drain path.
func TestQwpFuzzEgressFragmentedStreamingBigResult(t *testing.T) {
	r := newFuzzRand(t)
	chunk := fragFuzzPickChunk(r)
	t.Logf("chunk=%d", chunk)

	srv := bootEgressFragmentedServer(t, chunk)
	srv.mustExec(t,
		"CREATE TABLE bigt AS ("+
			"SELECT x AS id, CAST(x * 1.5 AS DOUBLE) AS v, "+
			"CAST('s_' || (x % 100) AS SYMBOL) AS s, "+
			"x::TIMESTAMP AS ts "+
			"FROM long_sequence(50000)) TIMESTAMP(ts) PARTITION BY DAY WAL")
	awaitTableRowsViaCount(t, srv, "bigt", 50_000, 90*time.Second)

	c := fragFuzzNewClient(t, srv, "")
	fragFuzzRunAndVerify(t, c, "bigt", 50_000)
}

// TestQwpFuzzEgressHandshakeSurvivesMicroChunk — port of Java
// testHandshakeSurvivesMicroChunk. Pin chunk to 5 bytes: the ~220 B
// WebSocket 101 handshake response fragments across ~44 socket writes,
// forcing rawSocket.send() to park repeatedly. Regression for the
// "Egress 101 handshake blocked" bug that surfaced when any chunk was
// smaller than the handshake response.
func TestQwpFuzzEgressHandshakeSurvivesMicroChunk(t *testing.T) {
	const chunk = 5
	srv := bootEgressFragmentedServer(t, chunk)
	srv.mustExec(t, "CREATE TABLE tiny(id LONG, ts TIMESTAMP) "+
		"TIMESTAMP(ts) PARTITION BY DAY WAL")
	srv.mustExec(t, "INSERT INTO tiny SELECT x, x::TIMESTAMP FROM long_sequence(3)")
	awaitTableRowsViaCount(t, srv, "tiny", 3, 60*time.Second)

	c := fragFuzzNewClient(t, srv, "")
	fragFuzzRunAndVerify(t, c, "tiny", 3)
}

// awaitTableRowsViaCount polls SELECT count() until the table reports
// at least `want` rows (mirrors engine.awaitTable in Java's in-process
// tests, but via the public /exec endpoint). Fragmentation knobs slow
// the WAL-apply rate enough that a tight inline assertion races; this
// helper keeps row-count expectations stable across chunk sizes.
func awaitTableRowsViaCount(t *testing.T, srv *qwpFuzzServer, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	sql := fmt.Sprintf("SELECT count() FROM '%s'", table)
	var lastN int64
	for {
		res, err := srv.execSQL(sql)
		if err == nil && len(res.Dataset) == 1 && len(res.Dataset[0]) == 1 {
			if n, ok := toInt64(res.Dataset[0][0]); ok {
				lastN = n
				if n >= int64(want) {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("table %q did not reach %d rows within %s (last %d)",
				table, want, timeout, lastN)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
