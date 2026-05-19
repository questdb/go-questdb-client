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

// Go port of QuestDB's QwpEgressBindFuzzTest. Property-based fuzz for the
// client-side bind encoder: each iteration picks random scalar bind
// values, runs SELECT $n::TYPE through the QWP query client, and asserts
// the round-trip value per cell. Complements the hand-picked boundary
// vectors in qwp_bind_values_test.go by stressing the encoder with
// arbitrary random inputs that catch bit-level encoding bugs.
//
// Reproducibility: every sub-test logs its master seed. Re-run a failing
// case with QWP_FUZZ_SEED=<logged value> go test -run <name>.

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

const bindFuzzIterations = 25

// newFuzzRand lives in qwp_fuzz_seed_test.go (shared, build-tag-free).

func pickNonNullLong(r *rand.Rand) int64 {
	for {
		v := int64(r.Uint64())
		if v != math.MinInt64 { // QuestDB LONG null sentinel
			return v
		}
	}
}

func pickNonNullInt(r *rand.Rand) int32 {
	for {
		v := int32(r.Uint32())
		if v != math.MinInt32 { // QuestDB INT null sentinel
			return v
		}
	}
}

// pickSpecialOrRandomDouble mirrors the Java helper: small odds of a
// special value, otherwise a random finite double. ±Inf and -0.0 are
// skipped because QuestDB's ::DOUBLE cast normalises them, which would
// make a raw round-trip comparison flap for reasons unrelated to the
// bind encoder.
func pickSpecialOrRandomDouble(r *rand.Rand) float64 {
	switch r.Intn(4) {
	case 0:
		return math.NaN()
	case 1:
		return 0.0
	default:
		for {
			d := math.Float64frombits(r.Uint64())
			if !math.IsInf(d, 0) {
				return d
			}
		}
	}
}

// queryOneRow runs sql with the given binds and invokes read on the
// single result batch. Fails the test (with iteration context) on a
// transport/query error, matching the Java onError → Assert.fail path.
func queryOneRow(t *testing.T, c *QwpQueryClient, sql, ctxMsg string, binds QwpBindFunc, read func(b *QwpColumnBatch)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q := c.Query(ctx, sql, WithQueryBinds(binds))
	defer q.Close()
	seen := false
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("%s: query error: %v", ctxMsg, err)
		}
		if batch.RowCount() > 0 && !seen {
			seen = true
			read(batch)
		}
	}
	if !seen {
		t.Fatalf("%s: query returned no rows", ctxMsg)
	}
}

func newBindFuzzClient(t *testing.T, srv *qwpFuzzServer) *QwpQueryClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := QwpQueryClientFromConf(ctx, srv.connConf())
	if err != nil {
		t.Fatalf("QwpQueryClientFromConf(%q): %v", srv.connConf(), err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = c.Close(cctx)
	})
	return c
}

func TestQwpFuzzDoubleBinds(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	c := newBindFuzzClient(t, srv)

	for i := 0; i < bindFuzzIterations; i++ {
		d := pickSpecialOrRandomDouble(r)
		var got float64
		var gotNull bool
		queryOneRow(t, c,
			"SELECT $1::DOUBLE AS d FROM long_sequence(1)",
			"iter "+strconv.Itoa(i),
			func(b *QwpBinds) { b.DoubleBind(0, d) },
			func(b *QwpColumnBatch) {
				gotNull = b.IsNull(0, 0)
				got = b.Float64(0, 0)
			},
		)
		if math.IsNaN(d) {
			// QuestDB's ::DOUBLE cast maps the NaN bit pattern to its
			// DOUBLE NULL sentinel. Java surfaces that as NaN; the Go
			// batch API deliberately returns 0 for NULL rows (see the
			// Float64/Float32 doc comments) and exposes the null via
			// IsNull. Either signal is a correct round-trip of a bound
			// NaN — the bind encoder did its job.
			if !gotNull && !math.IsNaN(got) {
				t.Fatalf("iter %d: bound NaN, expected NULL/NaN, got %v (null=%v)", i, got, gotNull)
			}
			continue
		}
		if gotNull {
			t.Fatalf("iter %d: d=%v came back NULL", i, d)
		}
		// Go == treats -0.0 == 0.0 as equal, matching QuestDB's cast
		// normalisation; Inf was excluded by the generator.
		if got != d {
			t.Fatalf("iter %d: d=%v (bits=%#x) got=%v (bits=%#x)",
				i, d, math.Float64bits(d), got, math.Float64bits(got))
		}
	}
}

func TestQwpFuzzIntegralBindsProjection(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	c := newBindFuzzClient(t, srv)

	for i := 0; i < bindFuzzIterations; i++ {
		longVal := pickNonNullLong(r)
		intVal := pickNonNullInt(r)
		shortVal := int16(r.Uint32())
		byteVal := int8(r.Uint32())
		boolVal := r.Intn(2) == 0

		var gotLong int64
		var gotInt int32
		var gotShort int16
		var gotByte int8
		var gotBool bool
		queryOneRow(t, c,
			"SELECT $1::LONG AS l, $2::INT AS i, $3::SHORT AS s, $4::BYTE AS b, $5::BOOLEAN AS x FROM long_sequence(1)",
			"iter "+strconv.Itoa(i),
			func(b *QwpBinds) {
				b.LongBind(0, longVal).
					IntBind(1, intVal).
					ShortBind(2, shortVal).
					ByteBind(3, byteVal).
					BooleanBind(4, boolVal)
			},
			func(b *QwpColumnBatch) {
				gotLong = b.Int64(0, 0)
				gotInt = b.Int32(1, 0)
				gotShort = b.Int16(2, 0)
				gotByte = b.Int8(3, 0)
				gotBool = b.Bool(4, 0)
			},
		)
		if gotLong != longVal {
			t.Fatalf("iter %d long: want %d got %d", i, longVal, gotLong)
		}
		if gotInt != intVal {
			t.Fatalf("iter %d int: want %d got %d", i, intVal, gotInt)
		}
		if gotShort != shortVal {
			t.Fatalf("iter %d short: want %d got %d", i, shortVal, gotShort)
		}
		if gotByte != byteVal {
			t.Fatalf("iter %d byte: want %d got %d", i, byteVal, gotByte)
		}
		if gotBool != boolVal {
			t.Fatalf("iter %d bool: want %v got %v", i, boolVal, gotBool)
		}
	}
}

func TestQwpFuzzUuidBinds(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	c := newBindFuzzClient(t, srv)

	for i := 0; i < bindFuzzIterations; i++ {
		lo := pickNonNullLong(r)
		hi := pickNonNullLong(r)
		var gotLo, gotHi int64
		queryOneRow(t, c,
			"SELECT $1::UUID AS u FROM long_sequence(1)",
			"iter "+strconv.Itoa(i),
			// Go's UuidBind takes (hi, lo); the Java test's
			// setUuid(0, lo, hi) is the same logical UUID.
			func(b *QwpBinds) { b.UuidBind(0, uint64(hi), uint64(lo)) },
			func(b *QwpColumnBatch) {
				gotLo = b.UuidLo(0, 0)
				gotHi = b.UuidHi(0, 0)
			},
		)
		if gotLo != lo {
			t.Fatalf("iter %d uuid lo: want %d got %d", i, lo, gotLo)
		}
		if gotHi != hi {
			t.Fatalf("iter %d uuid hi: want %d got %d", i, hi, gotHi)
		}
	}
}

// TestQwpFuzzSameSqlDifferentBindsCacheReuse stresses the
// same-SQL-different-binds path that the server's factory cache is meant
// to accelerate. Random integer lookups, 50 iterations.
func TestQwpFuzzSameSqlDifferentBindsCacheReuse(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)

	const table = "qwp_fuzz_bind_cache"
	srv.mustExec(t, "DROP TABLE IF EXISTS '"+table+"'")
	defer srv.mustExec(t, "DROP TABLE IF EXISTS '"+table+"'")
	srv.mustExec(t, "CREATE TABLE "+table+"(id LONG, v LONG, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL")

	const rows = 100
	var insert strings.Builder
	insert.WriteString("INSERT INTO " + table + " VALUES ")
	for rr := 0; rr < rows; rr++ {
		if rr > 0 {
			insert.WriteString(", ")
		}
		insert.WriteString("(")
		insert.WriteString(strconv.Itoa(rr))
		insert.WriteString(", ")
		insert.WriteString(strconv.FormatInt(int64(rr)*7, 10))
		insert.WriteString(", ")
		insert.WriteString(strconv.Itoa(rr + 1))
		insert.WriteString("::TIMESTAMP)")
	}
	srv.mustExec(t, insert.String())
	srv.awaitRows(t, table, rows, 30*time.Second)

	c := newBindFuzzClient(t, srv)
	const sql = "SELECT v FROM " + table + " WHERE id = $1"
	for i := 0; i < 50; i++ {
		target := r.Intn(rows)
		var observed int64 = -1
		var rowCount int
		queryOneRow(t, c, sql, "iter "+strconv.Itoa(i)+" target="+strconv.Itoa(target),
			func(b *QwpBinds) { b.IntBind(0, int32(target)) },
			func(b *QwpColumnBatch) {
				rowCount = b.RowCount()
				observed = b.Int64(0, 0)
			},
		)
		if rowCount != 1 {
			t.Fatalf("iter %d target=%d: want 1 row, got %d", i, target, rowCount)
		}
		if want := int64(target) * 7; observed != want {
			t.Fatalf("iter %d target=%d: want v=%d got %d", i, target, want, observed)
		}
	}
}
