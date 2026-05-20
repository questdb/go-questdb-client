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

// Go port of QuestDB's QwpIngressOracleFuzzTest (the multi-sender,
// no-bounce scenario). Every row the test intends to publish is
// materialised up front as an oracleRow (covering the full QWP-only
// type system: bool / all int widths / float / double / char / string /
// symbol / uuid / long256 / nanosecond timestamp / decimal 64-128-256 /
// 1D-2D-3D double arrays) and added to an oracleTable keyed by
// (ts, id). Concurrent producer goroutines each own a contiguous slice
// of rows and publish them through the Go QWP sender into a DEDUP
// UPSERT KEYS(ts, id) table. After ingestion, every cell of every row
// is asserted against the oracle via a `SELECT * ORDER BY ts, id`
// streamed back over the QWP query client. Because the oracle is
// pre-generated and (ts, id) is globally unique, any wire-level replay
// collapses cleanly under DEDUP and cannot drift the contract.
//
// Faithful-port divergences from the Java source (cf. the egress /
// bind / bounds ports' headers):
//
//   - No server bounce / no sf_dir / no async-connect. The Java suite's
//     bounce-torture, restart-replay, and async-connect scenarios need
//     a controllable start/stop server (RestartableQwpServer); the Go
//     fixture is a shared long-lived server (and only fixture-launched
//     mode could bounce it). This slice ports the pure correctness
//     property — concurrent multi-sender ingest + DEDUP + full-type
//     round-trip — and runs against the shared server (live or
//     fixture-launched). The deferred scenarios are tracked separately.
//   - Verification is via QWP `SELECT * ORDER BY ts, id` over the query
//     client, not an in-process RecordCursor; absent/skipped cells are
//     asserted NULL via QwpColumnBatch.IsNull (mirrors
//     QwpTable.assertCursor).
//   - Decimal values are kept non-negative; Java's per-decimal sign
//     flip (two's-complement limb negation) is deferred — it is
//     orthogonal to the ingest/dedup/round-trip property and is the
//     part most likely to need its own debugging pass. Scales and
//     auto-precision-extra columns are still fully exercised.
//   - Per-producer auto_flush_rows variation is simplified to explicit
//     Flush() at batch boundaries (correctness-equivalent without
//     bounces). Row counts are bounded smaller than the Java suite to
//     keep CI time in check while still crossing batch boundaries and
//     stressing DEDUP under concurrency.
//   - Reproducible via QWP_FUZZ_SEED (shared newFuzzRand).

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	oracleTableName     = "qwp_oracle_fuzz"
	oracleColumnSkip    = 8  // ~12% of rows skip a base column
	oracleNewColumn     = 16 // ~6% of rows inject an extra column
	oracleNonASCII      = 4  // ~25% of string/symbol values get a non-ASCII suffix
	oracleBaseTsMicros  = int64(1_700_000_000_000_000)
)

// oracleCreateSQL is the DEDUP target-table DDL shared by the ingress
// oracle tests (mirrors QwpIngressOracleFuzzTest.createTargetTable).
const oracleCreateSQL = "CREATE TABLE " + oracleTableName + " (" +
	"id LONG, b BOOLEAN, b8 BYTE, s16 SHORT, c CHAR, i INT, l LONG, " +
	"f FLOAT, d DOUBLE, s STRING, sym SYMBOL, u UUID, l256 LONG256, " +
	"tn TIMESTAMP_NS, da DOUBLE[], da2 DOUBLE[][], da3 DOUBLE[][][], " +
	"dec64 DECIMAL(12,3), dec128 DECIMAL(25,4), dec256 DECIMAL(50,6), " +
	"ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL " +
	"DEDUP UPSERT KEYS(ts, id)"

// oracleNonASCIISuffixes spans the UTF-8 byte-length spectrum (2/3/4
// byte) so the wire path exercises multi-byte encoding.
var oracleNonASCIISuffixes = []string{
	"é", "ñ", "ж", "Я", "日", "中", "한", "🎉",
}

// --- typed oracle cell -------------------------------------------------

type oracleKind int

const (
	ocAbsent oracleKind = iota // column not written this row -> expect NULL
	ocBool
	ocByte
	ocShort
	ocChar
	ocInt
	ocLong
	ocFloat
	ocDouble
	ocString
	ocSymbol
	ocUUID
	ocLong256
	ocTsNano
	ocDec64
	ocDec128
	ocDec256
	ocArr // 1D/2D/3D double array (flattened row-major + shape)
)

type oracleCell struct {
	kind oracleKind
	// scalars
	i64 int64   // byte/short/int/long/tsnano/dec64-unscaled
	f64 float64 // float (as float64 of the float32) / double
	b   bool
	ch  rune
	str string
	// uuid
	uhi, ulo int64
	// long256: words[0] = least-significant
	words [4]int64
	// decimal
	dec   *big.Int // unscaled (dec128/dec256); dec64 uses i64
	scale int
	// array
	arr   []float64 // flattened row-major
	shape []int
}

type oracleRow struct {
	id       int64
	tsMicros int64
	cells    map[string]oracleCell
}

func newOracleRow(id, tsMicros int64) *oracleRow {
	return &oracleRow{id: id, tsMicros: tsMicros, cells: make(map[string]oracleCell, 24)}
}

func (r *oracleRow) set(name string, c oracleCell) { r.cells[name] = c }

// oracleTable is the pre-generated expectation: rows in (ts, id) order
// (== generation order, since ts/id are globally unique and monotonic
// with the global index) plus the set of every column name ever
// written (so a SELECT * column the oracle never set can be asserted
// as wholly absent).
type oracleTable struct {
	rows     []*oracleRow
	colNames map[string]struct{}
}

func newOracleTable() *oracleTable {
	return &oracleTable{colNames: make(map[string]struct{}, 64)}
}

func (t *oracleTable) addRow(r *oracleRow) {
	t.rows = append(t.rows, r)
	for n := range r.cells {
		t.colNames[n] = struct{}{}
	}
}

// --- random value generation (faithful port of generateRow) ----------

func oracleShouldFuzz(r *rand.Rand, factor int) bool {
	return factor > 0 && r.Intn(factor) == 0
}

func oracleMaybeNegateF(r *rand.Rand, v float64) float64 {
	if r.Intn(2) == 0 {
		return -v
	}
	return v
}

func oracleMaybeNegateI(r *rand.Rand, v int64) int64 {
	if r.Intn(2) == 0 {
		return -v
	}
	return v
}

func oracleMaybeNonASCII(r *rand.Rand) string {
	if oracleShouldFuzz(r, oracleNonASCII) {
		return oracleNonASCIISuffixes[r.Intn(len(oracleNonASCIISuffixes))]
	}
	return ""
}

func oracleArr1d(id int64, sign float64) ([]float64, []int) {
	return []float64{float64(id) * sign, float64(id) * 2 * sign, float64(id) * 3 * sign}, []int{3}
}

func oracleArr2d(id int64, sign float64) ([]float64, []int) {
	return []float64{
		float64(id) * sign, float64(id) * 2 * sign,
		float64(id) * 3 * sign, float64(id) * 4 * sign,
	}, []int{2, 2}
}

func oracleArr3d(id int64, sign float64) ([]float64, []int) {
	out := make([]float64, 0, 12)
	for _, m := range []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12} {
		out = append(out, float64(id)*m*sign)
	}
	return out, []int{2, 2, 3}
}

func oracleSign(r *rand.Rand) float64 {
	if r.Intn(2) == 0 {
		return -1.0
	}
	return 1.0
}

// u128 builds a non-negative big.Int from hi:lo (unsigned 64-bit limbs).
func u128(hi, lo uint64) *big.Int {
	h := new(big.Int).SetUint64(hi)
	h.Lsh(h, 64)
	return h.Or(h, new(big.Int).SetUint64(lo))
}

// u256 builds a non-negative big.Int from hh:hl:lh:ll (unsigned limbs).
func u256(hh, hl, lh, ll uint64) *big.Int {
	v := new(big.Int).SetUint64(hh)
	for _, limb := range []uint64{hl, lh, ll} {
		v.Lsh(v, 64)
		v.Or(v, new(big.Int).SetUint64(limb))
	}
	return v
}

func oracleGenerateRow(r *rand.Rand, id, tsMicros int64) *oracleRow {
	row := newOracleRow(id, tsMicros)

	// BOOLEAN/BYTE/SHORT/CHAR are mandatory: no NULL representation, so
	// an absent cell would be indistinguishable from a stored zero.
	row.set("b", oracleCell{kind: ocBool, b: (id & 1) == 0})
	bv := byte((id & 0x7F))
	if r.Intn(2) == 0 {
		bv -= 0x40
	}
	row.set("b8", oracleCell{kind: ocByte, i64: int64(int8(bv))})
	row.set("s16", oracleCell{kind: ocShort, i64: oracleMaybeNegateI(r, (id*31)&0x7FFF)})
	row.set("c", oracleCell{kind: ocChar, ch: rune('A' + (id & 0x1F))})

	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("i", oracleCell{kind: ocInt, i64: oracleMaybeNegateI(r, (id*65537)&0x7FFFFFFF)})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("l", oracleCell{kind: ocLong, i64: oracleMaybeNegateI(r, id*1_000_003)})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("f", oracleCell{kind: ocFloat, f64: float64(float32(oracleMaybeNegateF(r, float64(id)*0.125)))})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("d", oracleCell{kind: ocDouble, f64: oracleMaybeNegateF(r, float64(id)*1.5)})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("s", oracleCell{kind: ocString, str: "s_" + strconv.FormatInt(id, 10) + oracleMaybeNonASCII(r)})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("sym", oracleCell{kind: ocSymbol, str: "sym_" + strconv.FormatInt(id&0xF, 10) + oracleMaybeNonASCII(r)})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("u", oracleCell{kind: ocUUID,
			uhi: id*0x00000000CAFEBABE + 17,
			ulo: id*0x00000000DEADBEEF - 13})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("l256", oracleCell{kind: ocLong256, words: [4]int64{
			id*0x11111111 + 1,
			id*0x22222222 + 2,
			id*0x33333333 + 3,
			id*0x44444444 + 4,
		}})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("tn", oracleCell{kind: ocTsNano, i64: tsMicros*1_000 + (id & 0x3FF)})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		a, sh := oracleArr1d(id, oracleSign(r))
		row.set("da", oracleCell{kind: ocArr, arr: a, shape: sh})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		a, sh := oracleArr2d(id, oracleSign(r))
		row.set("da2", oracleCell{kind: ocArr, arr: a, shape: sh})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		a, sh := oracleArr3d(id, oracleSign(r))
		row.set("da3", oracleCell{kind: ocArr, arr: a, shape: sh})
	}
	// Decimals: non-negative magnitudes inside each declared precision
	// (see createTargetTable). dec64 DECIMAL(12,3), dec128 DECIMAL(25,4),
	// dec256 DECIMAL(50,6).
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("dec64", oracleCell{kind: ocDec64, i64: id*10_000_007 + 13, scale: 3})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("dec128", oracleCell{kind: ocDec128,
			dec:   u128(uint64(id*40+7), uint64(id*0x00000000DEADBEEF+17)),
			scale: 4})
	}
	if !oracleShouldFuzz(r, oracleColumnSkip) {
		row.set("dec256", oracleCell{kind: ocDec256,
			dec: u256(0,
				uint64(id*0x123456+31),
				uint64(id*0x00000000CAFEBABE+17),
				uint64(id*0x00000000DEADBEEF+13)),
			scale: 6})
	}
	if oracleShouldFuzz(r, oracleNewColumn) {
		oracleInjectExtra(r, row, id)
	}
	return row
}

func oracleInjectExtra(r *rand.Rand, row *oracleRow, id int64) {
	switch r.Intn(19) {
	case 0:
		row.set("ex_l_0", oracleCell{kind: ocLong, i64: oracleMaybeNegateI(r, id*7)})
	case 1:
		row.set("ex_l_1", oracleCell{kind: ocLong, i64: oracleMaybeNegateI(r, id+100)})
	case 2:
		row.set("ex_l_2", oracleCell{kind: ocLong, i64: oracleMaybeNegateI(r, id)})
	case 3:
		row.set("ex_d_0", oracleCell{kind: ocDouble, f64: oracleMaybeNegateF(r, float64(id)*0.25)})
	case 4:
		row.set("ex_d_1", oracleCell{kind: ocDouble, f64: oracleMaybeNegateF(r, float64(id))})
	case 5:
		row.set("ex_d_2", oracleCell{kind: ocDouble, f64: oracleMaybeNegateF(r, float64(id)*13.7)})
	case 6:
		row.set("ex_s_0", oracleCell{kind: ocString, str: "ex0_" + strconv.FormatInt(id, 10) + oracleMaybeNonASCII(r)})
	case 7:
		row.set("ex_s_1", oracleCell{kind: ocString, str: "ex1_" + strconv.FormatInt(id, 10) + oracleMaybeNonASCII(r)})
	case 8:
		row.set("ex_sym_0", oracleCell{kind: ocSymbol, str: "exsym0_" + strconv.FormatInt(id&0x7, 10) + oracleMaybeNonASCII(r)})
	case 9:
		row.set("ex_sym_1", oracleCell{kind: ocSymbol, str: "exsym1_" + strconv.FormatInt(id&0x3, 10) + oracleMaybeNonASCII(r)})
	case 10:
		sign := oracleSign(r)
		row.set("ex_da_0", oracleCell{kind: ocArr,
			arr:   []float64{float64(id) * sign, float64(id+1) * sign, float64(id+2) * sign},
			shape: []int{3}})
	case 11:
		scale := r.Intn(16)
		row.set("ex_dec64_s"+strconv.Itoa(scale), oracleCell{kind: ocDec64, i64: id*7 + 11, scale: scale})
	case 12:
		scale := r.Intn(19)
		row.set("ex_dec128_s"+strconv.Itoa(scale), oracleCell{kind: ocDec128,
			dec:   u128(uint64(id*11+3), uint64(id*0x00000000DEADBEEF+17)),
			scale: scale})
	case 13:
		scale := r.Intn(31)
		row.set("ex_dec256_s"+strconv.Itoa(scale), oracleCell{kind: ocDec256,
			dec: u256(uint64(id*0x00000000ABCDEF01+7),
				uint64(id*0x123456+31),
				uint64(id*0x00000000CAFEBABE+17),
				uint64(id*0x00000000DEADBEEF+13)),
			scale: scale})
	case 14:
		row.set("ex_i_0", oracleCell{kind: ocInt, i64: oracleMaybeNegateI(r, (id*65537)&0x7FFFFFFF)})
	case 15:
		row.set("ex_f_0", oracleCell{kind: ocFloat, f64: float64(float32(oracleMaybeNegateF(r, float64(id)*0.0625)))})
	case 16:
		row.set("ex_u_0", oracleCell{kind: ocUUID, uhi: id*0x00000000ABCD1234 + 5, ulo: id*0x000000005678FEDC + 11})
	case 17:
		row.set("ex_l256_0", oracleCell{kind: ocLong256, words: [4]int64{
			id*0x0F0F0F0F + 1, id*0x1E1E1E1E + 2, id*0x2D2D2D2D + 3, id*0x3C3C3C3C + 4,
		}})
	case 18:
		row.set("ex_tn_0", oracleCell{kind: ocTsNano, i64: row.tsMicros*1_000 + (id & 0x1FF)})
	}
}

// --- publish a row through the QWP sender ----------------------------

func oraclePublish(t *testing.T, qs QwpSender, ctx context.Context, row *oracleRow) {
	t.Helper()
	qs.Table(oracleTableName)
	// Symbols must precede non-symbol columns (ILP/QWP ordering); map
	// iteration order is random, so emit symbols in a first pass.
	for name, c := range row.cells {
		if c.kind == ocSymbol {
			qs.Symbol(name, c.str)
		}
	}
	qs.Int64Column("id", row.id)
	for name, c := range row.cells {
		switch c.kind {
		case ocSymbol:
			// already emitted in the symbol pass above
		case ocBool:
			qs.BoolColumn(name, c.b)
		case ocByte:
			qs.ByteColumn(name, int8(c.i64))
		case ocShort:
			qs.ShortColumn(name, int16(c.i64))
		case ocChar:
			qs.CharColumn(name, c.ch)
		case ocInt:
			qs.Int32Column(name, int32(c.i64))
		case ocLong:
			qs.Int64Column(name, c.i64)
		case ocFloat:
			qs.Float32Column(name, float32(c.f64))
		case ocDouble:
			qs.Float64Column(name, c.f64)
		case ocString:
			qs.StringColumn(name, c.str)
		case ocUUID:
			qs.UuidColumn(name, uint64(c.uhi), uint64(c.ulo))
		case ocLong256:
			v := u256(uint64(c.words[3]), uint64(c.words[2]), uint64(c.words[1]), uint64(c.words[0]))
			qs.Long256Column(name, v)
		case ocTsNano:
			qs.TimestampNanosColumn(name, time.Unix(0, c.i64).UTC())
		case ocDec64:
			qs.Decimal64Column(name, NewDecimalFromInt64(c.i64, uint32(c.scale)))
		case ocDec128:
			d, err := NewDecimal(c.dec, uint32(c.scale))
			if err != nil {
				t.Fatalf("NewDecimal(dec128 %s): %v", name, err)
			}
			qs.Decimal128Column(name, d)
		case ocDec256:
			d, err := NewDecimal(c.dec, uint32(c.scale))
			if err != nil {
				t.Fatalf("NewDecimal(dec256 %s): %v", name, err)
			}
			qs.Decimal256Column(name, d)
		case ocArr:
			switch len(c.shape) {
			case 1:
				qs.Float64Array1DColumn(name, c.arr)
			case 2:
				qs.Float64Array2DColumn(name, oracleUnflatten2d(c.arr, c.shape))
			case 3:
				qs.Float64Array3DColumn(name, oracleUnflatten3d(c.arr, c.shape))
			}
		}
	}
	if err := qs.At(ctx, time.UnixMicro(row.tsMicros).UTC()); err != nil {
		t.Fatalf("sender.At(id=%d): %v", row.id, err)
	}
}

func oracleUnflatten2d(flat []float64, shape []int) [][]float64 {
	out := make([][]float64, shape[0])
	for i := 0; i < shape[0]; i++ {
		out[i] = flat[i*shape[1] : (i+1)*shape[1]]
	}
	return out
}

func oracleUnflatten3d(flat []float64, shape []int) [][][]float64 {
	out := make([][][]float64, shape[0])
	k := 0
	for i := 0; i < shape[0]; i++ {
		out[i] = make([][]float64, shape[1])
		for j := 0; j < shape[1]; j++ {
			out[i][j] = flat[k : k+shape[2]]
			k += shape[2]
		}
	}
	return out
}

// --- verification: SELECT * ORDER BY ts, id vs the oracle ------------

func oracleAssert(t *testing.T, c *QwpQueryClient, table *oracleTable) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT * FROM "+oracleTableName+" ORDER BY ts, id")
	defer q.Close()

	rowIdx := 0
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("oracle query: %v", err)
		}
		// Map column name -> batch column index for this batch.
		colIdx := make(map[string]int, batch.ColumnCount())
		for i := 0; i < batch.ColumnCount(); i++ {
			colIdx[batch.ColumnName(i)] = i
		}
		for br := 0; br < batch.RowCount(); br++ {
			if rowIdx >= len(table.rows) {
				t.Fatalf("more rows returned (%d) than the oracle holds (%d)",
					rowIdx+1, len(table.rows))
			}
			want := table.rows[rowIdx]
			rowIdx++

			idCi, ok := colIdx["id"]
			if !ok {
				t.Fatalf("row %d: SELECT * missing mandatory column \"id\"", rowIdx-1)
			}
			if got := batch.Int64(idCi, br); got != want.id {
				t.Fatalf("row %d id: want %d got %d", rowIdx-1, want.id, got)
			}
			tsCi, ok := colIdx["ts"]
			if !ok {
				t.Fatalf("id=%d: SELECT * missing mandatory column \"ts\"", want.id)
			}
			if got := batch.Int64(tsCi, br); got != want.tsMicros {
				t.Fatalf("id=%d ts: want %d got %d", want.id, want.tsMicros, got)
			}
			for name := range table.colNames {
				ci, present := colIdx[name]
				cell, set := want.cells[name]
				if !present {
					// Column never created at all: the oracle must
					// also never have written it.
					if set {
						t.Fatalf("id=%d: column %q set in oracle but absent from schema",
							want.id, name)
					}
					continue
				}
				if !set || cell.kind == ocAbsent {
					if !batch.IsNull(ci, br) {
						t.Fatalf("id=%d col %q: expected NULL (unset), got non-null", want.id, name)
					}
					continue
				}
				if batch.IsNull(ci, br) {
					t.Fatalf("id=%d col %q: expected value, got NULL", want.id, name)
				}
				oracleAssertCell(t, batch, ci, br, name, want.id, cell)
			}
		}
	}
	if rowIdx != len(table.rows) {
		t.Fatalf("row count: oracle holds %d, query returned %d", len(table.rows), rowIdx)
	}
}

func oracleAssertCell(t *testing.T, b *QwpColumnBatch, ci, br int, name string, id int64, c oracleCell) {
	t.Helper()
	switch c.kind {
	case ocBool:
		if got := b.Bool(ci, br); got != c.b {
			t.Fatalf("id=%d %s: want %v got %v", id, name, c.b, got)
		}
	case ocByte:
		if got := int64(b.Int8(ci, br)); got != c.i64 {
			t.Fatalf("id=%d %s(byte): want %d got %d", id, name, c.i64, got)
		}
	case ocShort:
		if got := int64(b.Int16(ci, br)); got != c.i64 {
			t.Fatalf("id=%d %s(short): want %d got %d", id, name, c.i64, got)
		}
	case ocChar:
		if got := b.Char(ci, br); got != c.ch {
			t.Fatalf("id=%d %s(char): want %q got %q", id, name, c.ch, got)
		}
	case ocInt:
		if got := int64(b.Int32(ci, br)); got != c.i64 {
			t.Fatalf("id=%d %s(int): want %d got %d", id, name, c.i64, got)
		}
	case ocLong:
		if got := b.Int64(ci, br); got != c.i64 {
			t.Fatalf("id=%d %s(long): want %d got %d", id, name, c.i64, got)
		}
	case ocFloat:
		if got := float64(b.Float32(ci, br)); got != c.f64 {
			t.Fatalf("id=%d %s(float): want %v got %v", id, name, c.f64, got)
		}
	case ocDouble:
		if got := b.Float64(ci, br); got != c.f64 {
			t.Fatalf("id=%d %s(double): want %v got %v", id, name, c.f64, got)
		}
	case ocString, ocSymbol:
		if got := b.String(ci, br); got != c.str {
			t.Fatalf("id=%d %s(str): want %q got %q", id, name, c.str, got)
		}
	case ocUUID:
		if gh, gl := b.UuidHi(ci, br), b.UuidLo(ci, br); gh != c.uhi || gl != c.ulo {
			t.Fatalf("id=%d %s(uuid): want hi=%d lo=%d got hi=%d lo=%d",
				id, name, c.uhi, c.ulo, gh, gl)
		}
	case ocLong256:
		for w := 0; w < 4; w++ {
			if got := b.Long256Word(ci, br, w); got != c.words[w] {
				t.Fatalf("id=%d %s(long256) word%d: want %d got %d",
					id, name, w, c.words[w], got)
			}
		}
	case ocTsNano:
		if got := b.Int64(ci, br); got != c.i64 {
			t.Fatalf("id=%d %s(tsnano): want %d got %d", id, name, c.i64, got)
		}
	case ocDec64:
		if got := b.Int64(ci, br); got != c.i64 {
			t.Fatalf("id=%d %s(dec64): want unscaled %d got %d", id, name, c.i64, got)
		}
		if got := b.DecimalScale(ci); got != c.scale {
			t.Fatalf("id=%d %s(dec64) scale: want %d got %d", id, name, c.scale, got)
		}
	case ocDec128:
		got := u128(uint64(b.Decimal128Hi(ci, br)), uint64(b.Decimal128Lo(ci, br)))
		if got.Cmp(c.dec) != 0 {
			t.Fatalf("id=%d %s(dec128): want %s got %s", id, name, c.dec, got)
		}
		if gs := b.DecimalScale(ci); gs != c.scale {
			t.Fatalf("id=%d %s(dec128) scale: want %d got %d", id, name, c.scale, gs)
		}
	case ocDec256:
		got := u256(
			uint64(b.Long256Word(ci, br, 3)),
			uint64(b.Long256Word(ci, br, 2)),
			uint64(b.Long256Word(ci, br, 1)),
			uint64(b.Long256Word(ci, br, 0)),
		)
		if got.Cmp(c.dec) != 0 {
			t.Fatalf("id=%d %s(dec256): want %s got %s", id, name, c.dec, got)
		}
		if gs := b.DecimalScale(ci); gs != c.scale {
			t.Fatalf("id=%d %s(dec256) scale: want %d got %d", id, name, c.scale, gs)
		}
	case ocArr:
		nd := b.ArrayNDims(ci, br)
		if nd != len(c.shape) {
			t.Fatalf("id=%d %s(arr) ndims: want %d got %d", id, name, len(c.shape), nd)
		}
		for d := 0; d < nd; d++ {
			if got := b.ArrayDim(ci, br, d); got != c.shape[d] {
				t.Fatalf("id=%d %s(arr) dim%d: want %d got %d", id, name, d, c.shape[d], got)
			}
		}
		got := b.Float64Array(ci, br)
		if len(got) != len(c.arr) {
			t.Fatalf("id=%d %s(arr) len: want %d got %d", id, name, len(c.arr), len(got))
		}
		for k := range c.arr {
			if got[k] != c.arr[k] {
				t.Fatalf("id=%d %s(arr)[%d]: want %v got %v", id, name, k, c.arr[k], got[k])
			}
		}
	}
}

// --- the test ---------------------------------------------------------

func oracleNewSender(t *testing.T, srv *qwpFuzzServer) (QwpSender, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ls, err := LineSenderFromConf(ctx, srv.connConf())
	if err != nil {
		t.Fatalf("LineSenderFromConf(%q): %v", srv.connConf(), err)
	}
	qs, ok := ls.(QwpSender)
	if !ok {
		t.Fatalf("ws sender is not a QwpSender (%T)", ls)
	}
	closer := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_ = qs.Close(cctx)
	}
	return qs, closer
}

// TestQwpFuzzIngressOracleMultiSender pre-generates a typed oracle,
// publishes it from several concurrent producer goroutines (each
// owning a contiguous, globally-unique (ts,id) slice) into a DEDUP
// table, then asserts every cell of every row via a streamed
// SELECT * ORDER BY ts, id. Catches per-type wire-encoding bugs,
// cross-batch misalignment, dedup/dup loss, and concurrency races.
func TestQwpFuzzIngressOracleMultiSender(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)

	producerCount := 2 + r.Intn(3)        // 2..4
	rowsPerProducer := 250 + r.Intn(350)  // 250..599 (bounded for CI)
	batchSizes := make([]int, producerCount)
	for p := range batchSizes {
		batchSizes[p] = 10 + r.Intn(60) // 10..69
	}
	totalRows := producerCount * rowsPerProducer
	t.Logf("ingress oracle: producers=%d rowsPerProducer=%d total=%d",
		producerCount, rowsPerProducer, totalRows)

	// Fresh table each run; DEDUP UPSERT KEYS(ts,id) collapses any
	// wire-level replay cleanly onto the pre-generated oracle.
	srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	defer srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	srv.mustExec(t, oracleCreateSQL)

	// Pre-generate: each producer owns a contiguous slice; ids and
	// timestamps are globally unique and interleaved so ts,id order
	// has a single deterministic interpretation.
	oracle := newOracleTable()
	perProducer := make([][]*oracleRow, producerCount)
	var globalIdx int64
	for p := 0; p < producerCount; p++ {
		genR := rand.New(rand.NewSource(r.Int63()))
		perProducer[p] = make([]*oracleRow, rowsPerProducer)
		for i := 0; i < rowsPerProducer; i++ {
			id := globalIdx
			ts := oracleBaseTsMicros + globalIdx
			row := oracleGenerateRow(genR, id, ts)
			perProducer[p][i] = row
			oracle.addRow(row)
			globalIdx++
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, producerCount)
	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs[p] = fmt.Errorf("producer %d panicked: %v", p, rec)
				}
			}()
			qs, closeSender := oracleNewSender(t, srv)
			defer closeSender()
			ctx := context.Background()
			rows := perProducer[p]
			bs := batchSizes[p]
			for i := 0; i < len(rows); i++ {
				oraclePublish(t, qs, ctx, rows[i])
				if (i+1)%bs == 0 {
					if err := qs.Flush(ctx); err != nil {
						errs[p] = fmt.Errorf("producer %d flush@%d: %w", p, i, err)
						return
					}
				}
			}
			if err := qs.Flush(ctx); err != nil {
				errs[p] = fmt.Errorf("producer %d final flush: %w", p, err)
			}
		}(p)
	}
	wg.Wait()
	for p, e := range errs {
		if e != nil {
			t.Fatalf("producer %d: %v", p, e)
		}
	}

	// Wait for the WAL apply job to materialise every (ts,id).
	srv.awaitRows(t, oracleTableName, totalRows, 120*time.Second)

	c := newBindFuzzClient(t, srv) // reused query-client helper
	oracleAssert(t, c, oracle)
}

// --- bounce-torture scenario -----------------------------------------

// oraclePickSfMaxBytes mirrors Java pickSfMaxBytes: small segments force
// frequent rotation (stresses purge bookkeeping), large segments resemble
// the production default. The chosen value also scales the post-close
// slot-purge bound.
func oraclePickSfMaxBytes(r *rand.Rand) int64 {
	pool := []int64{256 * 1024, 1024 * 1024, 4 * 1024 * 1024}
	return pool[r.Intn(len(pool))]
}

// oracleSfDirSize sums every file under dir. The Go SF slot lives at
// <sf_dir>/<sender_id>/...; Java asserts <sf_dir>/default. Summing the
// whole tree is faithful to the intent (slot purged after clean close)
// and robust to the exact nesting. Walk errors are returned so callers
// fail fast — silently returning 0 would let "sz > capBytes" pass
// vacuously when the directory was unreadable.
func oracleSfDirSize(dir string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// oracleSenderFromConf builds a QwpSender from a hand-assembled connect
// string (sf_dir / reconnect / auto_flush tuning the shared
// oracleNewSender does not expose). The closer's ctx outlasts
// close_flush_timeout_millis=120000 so a clean drain across an
// in-flight bounce can complete.
func oracleSenderFromConf(t *testing.T, conf string) (QwpSender, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ls, err := LineSenderFromConf(ctx, conf)
	if err != nil {
		t.Fatalf("LineSenderFromConf(%q): %v", conf, err)
	}
	qs, ok := ls.(QwpSender)
	if !ok {
		t.Fatalf("ws sender is not a QwpSender (%T)", ls)
	}
	closer := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer ccancel()
		_ = qs.Close(cctx)
	}
	return qs, closer
}

// TestQwpFuzzIngressOracleMultiSenderBounce is the bounce-torture port of
// QwpIngressOracleFuzzTest.testOracleMultiSenderTortureUnderServerBounces:
// concurrent sf_dir-backed producers publish the pre-generated typed
// oracle while a bouncer SIGTERMs and restarts the server several times
// on the same port/dataDir. The Go SF send loop owns reconnect + replay
// from the last ACKed FSN, and DEDUP UPSERT KEYS(ts,id) collapses any
// wire-level replay, so the final table must match the oracle exactly
// with zero loss across every server outage.
//
// Faithful-port divergences (cf. the file header and the egress/bounds
// ports' headers):
//
//   - Requires a fixture-LAUNCHED server (JDK+jar). In QDB_FUZZ_ADDR mode
//     the fixture does not own the process and cannot bounce it, so the
//     test skips — the non-bounce TestQwpFuzzIngressOracleMultiSender
//     still covers the correctness property against any server.
//   - The server down-interval is the fixture bounce()'s SIGTERM + fixed
//     ~500ms gap + JVM reboot, not Java's 40-100ms in-process stop/start
//     (a network-launched JVM cannot restart that fast). The property
//     under test — reconnect + gap-free replay across a real outage on a
//     stable port — is unchanged; a randomized post-bounce settle keeps
//     producers spanning multiple up/down windows.
//   - Row counts bounded smaller than the Java suite for CI time while
//     still crossing batch boundaries and outliving multiple bounces.
//     Decimals are non-negative (see the file header).
//   - Reproducible via QWP_FUZZ_SEED (shared newFuzzRand).
func TestQwpFuzzIngressOracleMultiSenderBounce(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("bounce-torture needs a fixture-launched server; " +
			"QDB_FUZZ_ADDR mode cannot restart the process")
	}
	r := newFuzzRand(t)

	producerCount := 2 + r.Intn(3)       // 2..4
	rowsPerProducer := 300 + r.Intn(400) // 300..699 (CI-bounded)
	bounces := 2 + r.Intn(3)             // 2..4
	sfMaxBytes := oraclePickSfMaxBytes(r)
	batchSizes := make([]int, producerCount)
	autoFlush := make([]int, producerCount)
	for p := 0; p < producerCount; p++ {
		batchSizes[p] = 10 + r.Intn(80) // 10..89
		autoFlush[p] = 50 + r.Intn(200) // 50..249
	}
	bRnd := rand.New(rand.NewSource(r.Int63()))
	totalRows := producerCount * rowsPerProducer
	t.Logf("ingress oracle bounce: producers=%d rows/producer=%d total=%d bounces=%d sf_max_bytes=%d",
		producerCount, rowsPerProducer, totalRows, bounces, sfMaxBytes)

	srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	defer srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	srv.mustExec(t, oracleCreateSQL)

	// Pre-generate: each producer owns a contiguous slice; ids and
	// timestamps are globally unique so ts,id order is deterministic and
	// every wire-level replay collapses cleanly under DEDUP.
	oracle := newOracleTable()
	perProducer := make([][]*oracleRow, producerCount)
	var globalIdx int64
	for p := 0; p < producerCount; p++ {
		genR := rand.New(rand.NewSource(r.Int63()))
		perProducer[p] = make([]*oracleRow, rowsPerProducer)
		for i := 0; i < rowsPerProducer; i++ {
			id := globalIdx
			ts := oracleBaseTsMicros + globalIdx
			row := oracleGenerateRow(genR, id, ts)
			perProducer[p][i] = row
			oracle.addRow(row)
			globalIdx++
		}
	}

	sfRoot := t.TempDir()
	sfDirs := make([]string, producerCount)
	for p := 0; p < producerCount; p++ {
		sfDirs[p] = filepath.Join(sfRoot, fmt.Sprintf("p%d", p))
		if err := os.MkdirAll(sfDirs[p], 0o755); err != nil {
			t.Fatalf("mkdir sf_dir: %v", err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, producerCount)
	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs[p] = fmt.Errorf("producer %d panicked: %v", p, rec)
				}
			}()
			conf := fmt.Sprintf(
				"ws::addr=%s;sf_dir=%s;initial_connect_retry=async;"+
					"reconnect_max_duration_millis=120000;"+
					"close_flush_timeout_millis=120000;"+
					"sf_max_bytes=%d;auto_flush_rows=%d;",
				srv.wsAddr(), sfDirs[p], sfMaxBytes, autoFlush[p])
			qs, closeSender := oracleSenderFromConf(t, conf)
			defer closeSender()
			ctx := context.Background()
			rows := perProducer[p]
			bs := batchSizes[p]
			written := 0
			for written < len(rows) {
				end := min(written+bs, len(rows))
				for i := written; i < end; i++ {
					oraclePublish(t, qs, ctx, rows[i])
				}
				if err := qs.Flush(ctx); err != nil {
					errs[p] = fmt.Errorf("producer %d flush@%d: %w", p, written, err)
					return
				}
				written = end
				time.Sleep(time.Millisecond) // mirror Java Os.sleep(1)
			}
		}(p)
	}

	bouncerDone := make(chan struct{})
	var bounceErr error
	go func() {
		defer close(bouncerDone)
		time.Sleep(150 * time.Millisecond) // let producers warm up
		for i := 0; i < bounces; i++ {
			t.Logf("oracle bounce %d/%d", i+1, bounces)
			if err := srv.bounce(); err != nil {
				bounceErr = fmt.Errorf("bounce %d/%d: %w", i+1, bounces, err)
				return
			}
			time.Sleep(time.Duration(150+bRnd.Intn(250)) * time.Millisecond)
		}
	}()

	// Match the Java ordering: join the bouncer, then the producers.
	// Always drain producers before any t.Fatalf so no goroutine
	// touches t after the test function returns.
	<-bouncerDone
	wg.Wait()
	if bounceErr != nil {
		t.Fatalf("%v", bounceErr)
	}
	for p, e := range errs {
		if e != nil {
			t.Fatalf("producer %d: %v", p, e)
		}
	}

	srv.awaitRows(t, oracleTableName, totalRows, 120*time.Second)

	c := newBindFuzzClient(t, srv)
	oracleAssert(t, c, oracle)

	// Clean close ACKed every frame; the SF cursor unlinks rotated
	// segments. A small residue (lock, ack-watermark, active header) is
	// normal — Java's slotCapFor is sf_max_bytes + 256 KiB.
	capBytes := sfMaxBytes + 256*1024
	for p, dir := range sfDirs {
		sz, err := oracleSfDirSize(dir)
		if err != nil {
			t.Fatalf("producer %d sf_dir %q: walk failed: %v", p, dir, err)
		}
		if sz > capBytes {
			t.Fatalf("producer %d sf_dir %q not purged after clean close: %d bytes (cap %d)",
				p, dir, sz, capBytes)
		}
	}
}

// --- poison-rows / per-frame-drop scenario ---------------------------

// TestQwpFuzzIngressOraclePoisonErrorHandler ports
// QwpIngressOracleFuzzTest.testOraclePoisonRowsTriggerErrorHandler. It
// pins the per-batch error contract:
//
//  1. the async error handler fires for every poisoned chunk;
//  2. rows from clean chunks land exactly per the oracle;
//  3. no row from a poisoned chunk leaks — the WHOLE frame is dropped,
//     including the well-formed rows next to the bad one (SF drops per
//     frame, not per row).
//
// A poisoned chunk carries one row whose dec256 unscaled value is 2^192
// (~6.3e57, 58 digits) — well past DECIMAL(50,6)'s 10^50 cap. The
// server returns CategoryWriteError, whose spec-default policy is
// DROP_AND_CONTINUE (qwp_sf_classify.go), so the producer keeps going
// and the rejection surfaces only via the async handler. No server
// bounce on purpose — the failure mode must be unambiguously the
// per-frame rejection, not a transport blip.
//
// Faithful-port divergences (cf. the file header and the bounce port):
//
//   - The sender is built with NewLineSender(...) options rather than a
//     connect string: Go has no conf+option combiner and WithErrorHandler
//     is option-only. The options are 1:1 with the Java connect string
//     (sf_dir, initial_connect_retry=true→sync, close_flush_timeout,
//     error_inbox_capacity) plus the error handler.
//   - reconnect_max_duration_millis is omitted (no outage in this
//     scenario; the default budget is irrelevant).
//   - Clean rows verified via the QWP query client (oracleAssert);
//     poisoned-id absence via the fixture /exec count (mirrors Java's
//     assertSql). Counts are CI-bounded; chunk size stays small enough
//     to map to a single frame so the per-frame drop is deterministic.
//   - errCalls >= poisoned-chunk count (inequality, like Java: tolerates
//     the rare chunk that splits across more than one frame).
//   - Reproducible via QWP_FUZZ_SEED (shared newFuzzRand).
func TestQwpFuzzIngressOraclePoisonErrorHandler(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)

	producerCount := 2 + r.Intn(2)        // 2..3
	chunksPerProducer := 30 + r.Intn(30)  // 30..59
	chunkSize := 5 + r.Intn(6)            // 5..10 rows (maps to one frame)
	const poisonChunkInN = 4              // ~25% of chunks poisoned
	sfMaxBytes := oraclePickSfMaxBytes(r) // shared with the bounce port

	// Constructible client-side? 2^192 is 58 digits — inside Decimal256's
	// 76-digit envelope, so NewDecimal accepts it and the rejection is
	// purely server-side (the whole point of the poison).
	if _, e := NewDecimal(u256(1, 0, 0, 0), 6); e != nil {
		t.Fatalf("poison value 2^192 not constructible client-side: %v", e)
	}

	oracle := newOracleTable()
	perProducerChunks := make([][][]*oracleRow, producerCount)
	var poisonedIDs []string
	totalPoisonedChunks := 0
	var globalIdx int64
	for p := 0; p < producerCount; p++ {
		genR := rand.New(rand.NewSource(r.Int63()))
		poisonR := rand.New(rand.NewSource(r.Int63()))
		perProducerChunks[p] = make([][]*oracleRow, chunksPerProducer)
		for c := 0; c < chunksPerProducer; c++ {
			poisoned := poisonR.Intn(poisonChunkInN) == 0
			if poisoned {
				totalPoisonedChunks++
			}
			chunk := make([]*oracleRow, chunkSize)
			for rr := 0; rr < chunkSize; rr++ {
				id := globalIdx
				ts := oracleBaseTsMicros + globalIdx
				row := oracleGenerateRow(genR, id, ts)
				if poisoned {
					// Force dec256 past the column cap. setSignedDecimal
					// is unconditional in Java; overwrite whatever
					// generateRow produced (skipped or not).
					row.set("dec256", oracleCell{kind: ocDec256, dec: u256(1, 0, 0, 0), scale: 6})
					poisonedIDs = append(poisonedIDs, strconv.FormatInt(id, 10))
				} else {
					oracle.addRow(row)
				}
				chunk[rr] = row
				globalIdx++
			}
			perProducerChunks[p][c] = chunk
		}
	}
	cleanRows := len(oracle.rows)
	t.Logf("ingress oracle poison: producers=%d chunks/producer=%d chunkSize=%d "+
		"poisonedChunks=%d cleanRows=%d sf_max_bytes=%d",
		producerCount, chunksPerProducer, chunkSize, totalPoisonedChunks, cleanRows, sfMaxBytes)

	srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	defer srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	srv.mustExec(t, oracleCreateSQL)

	sfRoot := t.TempDir()
	sfDirs := make([]string, producerCount)
	for p := 0; p < producerCount; p++ {
		sfDirs[p] = filepath.Join(sfRoot, fmt.Sprintf("p%d", p))
		if err := os.MkdirAll(sfDirs[p], 0o755); err != nil {
			t.Fatalf("mkdir sf_dir: %v", err)
		}
	}

	var errCalls atomic.Int64 // shared across every producer's handler
	var wg sync.WaitGroup
	errs := make([]error, producerCount)
	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs[p] = fmt.Errorf("producer %d panicked: %v", p, rec)
				}
			}()
			ctx := context.Background()
			ls, err := NewLineSender(ctx,
				WithQwp(),
				WithAddress(srv.wsAddr()),
				WithSfDir(sfDirs[p]),
				WithSfMaxBytes(sfMaxBytes),
				WithInitialConnectRetry(true), // initial_connect_retry=true (sync)
				WithCloseFlushTimeout(120*time.Second),
				WithErrorInboxCapacity(4096),
				WithErrorHandler(func(*SenderError) { errCalls.Add(1) }),
			)
			if err != nil {
				errs[p] = fmt.Errorf("producer %d NewLineSender: %w", p, err)
				return
			}
			qs, ok := ls.(QwpSender)
			if !ok {
				errs[p] = fmt.Errorf("producer %d: ws sender is not a QwpSender (%T)", p, ls)
				_ = ls.Close(ctx)
				return
			}
			defer func() {
				cctx, ccancel := context.WithTimeout(context.Background(), 150*time.Second)
				defer ccancel()
				_ = qs.Close(cctx)
			}()
			for c := 0; c < len(perProducerChunks[p]); c++ {
				for _, row := range perProducerChunks[p][c] {
					oraclePublish(t, qs, ctx, row)
				}
				// Explicit flush per chunk -> chunk == frame, so the
				// per-frame drop is deterministic. DROP_AND_CONTINUE means
				// Flush does NOT error on a poisoned chunk (no HALT latch).
				if err := qs.Flush(ctx); err != nil {
					errs[p] = fmt.Errorf("producer %d flush chunk %d: %w", p, c, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()
	for p, e := range errs {
		if e != nil {
			t.Fatalf("producer %d: %v", p, e)
		}
	}

	// Poisoned frames are dropped, so the table converges to exactly the
	// clean-row count (globally-unique ts,id + DEDUP -> no dup inflation).
	srv.awaitRows(t, oracleTableName, cleanRows, 120*time.Second)

	// (a) Clean rows: every clean-chunk row lands once; oracle drives a
	// typed cell-by-cell check (and asserts the row count is exact).
	c := newBindFuzzClient(t, srv)
	oracleAssert(t, c, oracle)

	// (b) Poisoned rows: not a single id from any poisoned chunk leaked
	// -- this pins the per-frame drop (good rows in a bad frame are gone
	// too).
	if len(poisonedIDs) > 0 {
		res, err := srv.execSQL("SELECT count() FROM '" + oracleTableName +
			"' WHERE id IN (" + strings.Join(poisonedIDs, ",") + ")")
		if err != nil {
			t.Fatalf("poisoned-id count query: %v", err)
		}
		if len(res.Dataset) != 1 || len(res.Dataset[0]) != 1 {
			t.Fatalf("poisoned-id count: unexpected shape %v", res.Dataset)
		}
		if n, ok := toInt64(res.Dataset[0][0]); !ok || n != 0 {
			t.Fatalf("poisoned rows leaked: %d ids from poisoned chunks present "+
				"(expected 0) -- per-frame drop violated", n)
		}
	}

	// (c) Async notifications: at least one per poisoned chunk reached a
	// handler. Inequality tolerates a chunk split across >1 frame.
	got := errCalls.Load()
	if got < int64(totalPoisonedChunks) {
		t.Fatalf("error handler fired %d times, expected >= %d (poisoned chunks)",
			got, totalPoisonedChunks)
	}
	t.Logf("poison: poisonedChunks=%d handlerCalls=%d", totalPoisonedChunks, got)

	// Clean close ACKed/handled every frame; the SF cursor unlinks
	// rotated segments. Java's slotCapFor: sf_max_bytes + 256 KiB.
	capBytes := sfMaxBytes + 256*1024
	for p, dir := range sfDirs {
		sz, err := oracleSfDirSize(dir)
		if err != nil {
			t.Fatalf("producer %d sf_dir %q: walk failed: %v", p, dir, err)
		}
		if sz > capBytes {
			t.Fatalf("producer %d sf_dir %q not purged after clean close: %d bytes (cap %d)",
				p, dir, sz, capBytes)
		}
	}
}

// --- restart-replay scenario -----------------------------------------

// TestQwpFuzzIngressOracleSenderRestartReplay ports
// QwpIngressOracleFuzzTest.testOracleSenderRestartReplaysAcrossBounces.
// Each producer opens-and-closes a fresh sender repeatedly with
// close_flush_timeout_millis=0 so unacked frames stay on disk in the
// per-producer sf_dir. The next sender on the same slot adopts those
// frames and replays them (SF on-disk format is shared with the Java
// client, so the slot-recovery contract is the same). A bouncer
// interleaves a couple of server restarts. Each producer finishes with
// one drain-pass sender on default close_flush_timeout to ensure all
// residual frames have ACKed before the oracle check.
//
// Final state must match the oracle exactly — the property under test
// is "no row loss across sender close/reopen + server bounce, only
// dedup-collapsed wire-level replays."
//
// Faithful-port divergences (cf. file header + bounce / poison ports):
//
//   - Uses fixture bounce() for the bouncer; same SIGTERM + ~500ms +
//     JVM-reboot interval as the bounce-torture port. Needs
//     fixture-launched mode (skips !owns).
//   - The drain pass sets close_flush_timeout_millis=120000 explicitly
//     (Java uses the default; equivalent intent — give the final pass
//     time to ACK every residual frame).
//   - Counts are CI-bounded; decimals non-negative.
//   - Reproducible via QWP_FUZZ_SEED.
func TestQwpFuzzIngressOracleSenderRestartReplay(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("restart-replay needs a fixture-launched server " +
			"(QDB_FUZZ_ADDR mode cannot bounce the process)")
	}
	r := newFuzzRand(t)

	producerCount := 2 + r.Intn(2)       // 2..3
	rowsPerProducer := 300 + r.Intn(400) // 300..699 (CI-bounded)
	bounces := 1 + r.Intn(2)             // 1..2
	sfMaxBytes := oraclePickSfMaxBytes(r)
	lifetimeSeeds := make([]int64, producerCount)
	for p := 0; p < producerCount; p++ {
		lifetimeSeeds[p] = r.Int63()
	}
	bRnd := rand.New(rand.NewSource(r.Int63()))
	totalRows := producerCount * rowsPerProducer
	t.Logf("ingress oracle restart-replay: producers=%d rows/producer=%d total=%d bounces=%d sf_max_bytes=%d",
		producerCount, rowsPerProducer, totalRows, bounces, sfMaxBytes)

	srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	defer srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	srv.mustExec(t, oracleCreateSQL)

	oracle := newOracleTable()
	perProducer := make([][]*oracleRow, producerCount)
	var globalIdx int64
	for p := 0; p < producerCount; p++ {
		genR := rand.New(rand.NewSource(r.Int63()))
		perProducer[p] = make([]*oracleRow, rowsPerProducer)
		for i := 0; i < rowsPerProducer; i++ {
			id := globalIdx
			ts := oracleBaseTsMicros + globalIdx
			row := oracleGenerateRow(genR, id, ts)
			perProducer[p][i] = row
			oracle.addRow(row)
			globalIdx++
		}
	}

	sfRoot := t.TempDir()
	sfDirs := make([]string, producerCount)
	for p := 0; p < producerCount; p++ {
		sfDirs[p] = filepath.Join(sfRoot, fmt.Sprintf("p%d", p))
		if err := os.MkdirAll(sfDirs[p], 0o755); err != nil {
			t.Fatalf("mkdir sf_dir: %v", err)
		}
	}

	openSender := func(p int, conf string) (QwpSender, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		ls, err := LineSenderFromConf(ctx, conf)
		if err != nil {
			return nil, fmt.Errorf("producer %d open: %w", p, err)
		}
		qs, ok := ls.(QwpSender)
		if !ok {
			_ = ls.Close(ctx)
			return nil, fmt.Errorf("producer %d: not a QwpSender (%T)", p, ls)
		}
		return qs, nil
	}
	closeSender := func(qs QwpSender) {
		cctx, ccancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer ccancel()
		_ = qs.Close(cctx)
	}

	var wg sync.WaitGroup
	errs := make([]error, producerCount)
	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs[p] = fmt.Errorf("producer %d panicked: %v", p, rec)
				}
			}()
			lifeR := rand.New(rand.NewSource(lifetimeSeeds[p]))
			ctx := context.Background()
			rows := perProducer[p]
			loopConf := fmt.Sprintf(
				"ws::addr=%s;sf_dir=%s;initial_connect_retry=async;"+
					"reconnect_max_duration_millis=120000;"+
					"sf_max_bytes=%d;close_flush_timeout_millis=0;",
				srv.wsAddr(), sfDirs[p], sfMaxBytes)
			written := 0
			for written < len(rows) {
				chunk := 30 + lifeR.Intn(200) // 30..229 rows per sender
				end := min(written+chunk, len(rows))
				qs, err := openSender(p, loopConf)
				if err != nil {
					errs[p] = err
					return
				}
				for i := written; i < end; i++ {
					oraclePublish(t, qs, ctx, rows[i])
				}
				if lifeR.Intn(2) == 0 {
					if err := qs.Flush(ctx); err != nil {
						errs[p] = fmt.Errorf("producer %d flush: %w", p, err)
						closeSender(qs)
						return
					}
				}
				// Close with timeout=0 -> abandon any unacked frames to
				// disk for the next sender on the same slot to adopt
				// and replay.
				closeSender(qs)
				written = end
			}
			// Final drain pass: open one more sender with a generous
			// close_flush_timeout so residual frames replay + ACK
			// before the oracle check.
			drainConf := fmt.Sprintf(
				"ws::addr=%s;sf_dir=%s;initial_connect_retry=async;"+
					"reconnect_max_duration_millis=120000;"+
					"sf_max_bytes=%d;close_flush_timeout_millis=120000;",
				srv.wsAddr(), sfDirs[p], sfMaxBytes)
			qs, err := openSender(p, drainConf)
			if err != nil {
				errs[p] = err
				return
			}
			if err := qs.Flush(ctx); err != nil {
				errs[p] = fmt.Errorf("producer %d drain flush: %w", p, err)
				closeSender(qs)
				return
			}
			closeSender(qs)
		}(p)
	}

	bouncerDone := make(chan struct{})
	var bounceErr error
	go func() {
		defer close(bouncerDone)
		time.Sleep(200 * time.Millisecond)
		for i := 0; i < bounces; i++ {
			t.Logf("restart-replay bounce %d/%d", i+1, bounces)
			if err := srv.bounce(); err != nil {
				bounceErr = fmt.Errorf("bounce %d/%d: %w", i+1, bounces, err)
				return
			}
			time.Sleep(time.Duration(300+bRnd.Intn(400)) * time.Millisecond)
		}
	}()

	<-bouncerDone
	wg.Wait()
	if bounceErr != nil {
		t.Fatalf("%v", bounceErr)
	}
	for p, e := range errs {
		if e != nil {
			t.Fatalf("producer %d: %v", p, e)
		}
	}

	srv.awaitRows(t, oracleTableName, totalRows, 180*time.Second)

	c := newBindFuzzClient(t, srv)
	oracleAssert(t, c, oracle)

	capBytes := sfMaxBytes + 256*1024
	for p, dir := range sfDirs {
		sz, err := oracleSfDirSize(dir)
		if err != nil {
			t.Fatalf("producer %d sf_dir %q: walk failed: %v", p, dir, err)
		}
		if sz > capBytes {
			t.Fatalf("producer %d sf_dir %q not purged after clean close: %d bytes (cap %d)",
				p, dir, sz, capBytes)
		}
	}
}

// --- async-connect-queues-before-server-starts scenario --------------

// TestQwpFuzzIngressOracleAsyncConnectQueues ports
// QwpIngressOracleFuzzTest.testOracleAsyncConnectQueuesBeforeServerStarts.
// The offline-first contract of initial_connect_retry=async: the
// sender constructor must return promptly even when nothing is
// listening, the producer thread keeps writing immediately, frames
// accumulate in sf_dir while the I/O thread retries connect in the
// background. Once the server is brought up, the queued frames drain.
// Final cell-by-cell oracle check confirms no loss across the
// offline -> online transition.
//
// Shape: pause the fixture so its port is closed; producers open
// async, publish everything and signal "enqueued"; a starter
// goroutine waits for that signal, settles briefly (so the first
// connect attempt is guaranteed to have hit ECONNREFUSED — proving
// the ASYNC contract rather than letting the dial happen
// post-resume), then calls start(); senders' close blocks on
// close_flush_timeout to drain.
//
// Faithful-port divergences (cf. file header + bounce / restart-replay
// / poison ports):
//
//   - Needs the new fixture pause()/start() pair (skips !owns). The
//     test always leaves the server up via t.Cleanup(start) regardless
//     of outcome — start() is idempotent.
//   - Constructor latency assertion: <2s for async mode (same as Java).
//   - Counts are CI-bounded; decimals non-negative; reproducible via
//     QWP_FUZZ_SEED.
func TestQwpFuzzIngressOracleAsyncConnectQueues(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("async-connect needs a fixture-launched server " +
			"(QDB_FUZZ_ADDR mode cannot pause/resume the process)")
	}
	// Always restore the server to a running state — start() is
	// idempotent so this is safe regardless of test outcome.
	t.Cleanup(func() {
		if err := srv.start(); err != nil {
			t.Logf("cleanup: failed to restart server: %v", err)
			return
		}
		if _, err := srv.execSQL("DROP TABLE IF EXISTS '" + oracleTableName + "'"); err != nil {
			t.Logf("cleanup: drop table: %v", err)
		}
	})

	r := newFuzzRand(t)
	producerCount := 2 + r.Intn(2)       // 2..3
	rowsPerProducer := 250 + r.Intn(400) // 250..649 (CI-bounded)
	sfMaxBytes := oraclePickSfMaxBytes(r)
	totalRows := producerCount * rowsPerProducer
	t.Logf("ingress oracle async-connect: producers=%d rows/producer=%d total=%d sf_max_bytes=%d",
		producerCount, rowsPerProducer, totalRows, sfMaxBytes)

	srv.mustExec(t, "DROP TABLE IF EXISTS '"+oracleTableName+"'")
	srv.mustExec(t, oracleCreateSQL)

	// Pre-generate the oracle BEFORE pausing the server.
	oracle := newOracleTable()
	perProducer := make([][]*oracleRow, producerCount)
	var globalIdx int64
	for p := 0; p < producerCount; p++ {
		genR := rand.New(rand.NewSource(r.Int63()))
		perProducer[p] = make([]*oracleRow, rowsPerProducer)
		for i := 0; i < rowsPerProducer; i++ {
			id := globalIdx
			ts := oracleBaseTsMicros + globalIdx
			row := oracleGenerateRow(genR, id, ts)
			perProducer[p][i] = row
			oracle.addRow(row)
			globalIdx++
		}
	}

	sfRoot := t.TempDir()
	sfDirs := make([]string, producerCount)
	for p := 0; p < producerCount; p++ {
		sfDirs[p] = filepath.Join(sfRoot, fmt.Sprintf("p%d", p))
		if err := os.MkdirAll(sfDirs[p], 0o755); err != nil {
			t.Fatalf("mkdir sf_dir: %v", err)
		}
	}

	// Bring the server down. From here until the starter goroutine
	// calls srv.start(), the wsAddr port is closed.
	srv.pause()

	var wg sync.WaitGroup
	errs := make([]error, producerCount)
	allEnqueued := make(chan struct{}, producerCount)

	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs[p] = fmt.Errorf("producer %d panicked: %v", p, rec)
				}
			}()

			conf := fmt.Sprintf(
				"ws::addr=%s;sf_dir=%s;initial_connect_retry=async;"+
					"reconnect_max_duration_millis=120000;"+
					"reconnect_initial_backoff_millis=20;"+
					"reconnect_max_backoff_millis=200;"+
					"sf_max_bytes=%d;"+
					"close_flush_timeout_millis=120000;",
				srv.wsAddr(), sfDirs[p], sfMaxBytes)

			// Time the constructor: async mode must return promptly
			// even when no server listens — the whole point.
			openCtx, openCancel := context.WithTimeout(context.Background(), 15*time.Second)
			t0 := time.Now()
			ls, err := LineSenderFromConf(openCtx, conf)
			openCancel()
			ctorElapsed := time.Since(t0)
			if err != nil {
				errs[p] = fmt.Errorf("producer %d open: %w", p, err)
				allEnqueued <- struct{}{}
				return
			}
			if ctorElapsed > 500*time.Millisecond {
				errs[p] = fmt.Errorf("producer %d: async ctor took %s (must be <500ms; offline path should not block on network)", p, ctorElapsed)
				_ = ls.Close(context.Background())
				allEnqueued <- struct{}{}
				return
			}
			qs, ok := ls.(QwpSender)
			if !ok {
				errs[p] = fmt.Errorf("producer %d: not a QwpSender (%T)", p, ls)
				_ = ls.Close(context.Background())
				allEnqueued <- struct{}{}
				return
			}

			pubCtx := context.Background()
			const chunkSize = 50
			rows := perProducer[p]
			for i := 0; i < len(rows); i++ {
				oraclePublish(t, qs, pubCtx, rows[i])
				if (i+1)%chunkSize == 0 {
					if err := qs.Flush(pubCtx); err != nil {
						errs[p] = fmt.Errorf("producer %d flush@%d: %w", p, i, err)
						allEnqueued <- struct{}{}
						cctx, ccancel := context.WithTimeout(context.Background(), 150*time.Second)
						_ = qs.Close(cctx)
						ccancel()
						return
					}
				}
			}
			if err := qs.Flush(pubCtx); err != nil {
				errs[p] = fmt.Errorf("producer %d final flush: %w", p, err)
			}
			// Signal "everything enqueued to sf_dir" BEFORE the
			// close-block. Frame I/O has not yet started talking to
			// any server — that only begins once the starter brings
			// it up and Close() drives the drain.
			allEnqueued <- struct{}{}
			cctx, ccancel := context.WithTimeout(context.Background(), 150*time.Second)
			_ = qs.Close(cctx)
			ccancel()
		}(p)
	}

	starterDone := make(chan struct{})
	var starterErr error
	go func() {
		defer close(starterDone)
		enqWait := time.After(60 * time.Second)
		seen := 0
		for seen < producerCount {
			select {
			case <-allEnqueued:
				seen++
			case <-enqWait:
				starterErr = fmt.Errorf("only %d/%d producers enqueued within 60s", seen, producerCount)
				return
			}
		}
		// Brief settle so the I/O thread has at minimum hit one
		// ECONNREFUSED retry — exercises the ASYNC contract
		// (background connect loop) rather than letting the first
		// connect happen post-server-up.
		time.Sleep(100 * time.Millisecond)
		if err := srv.start(); err != nil {
			starterErr = fmt.Errorf("starter: %w", err)
		}
	}()

	<-starterDone
	wg.Wait()
	if starterErr != nil {
		t.Fatalf("%v", starterErr)
	}
	for p, e := range errs {
		if e != nil {
			t.Fatalf("producer %d: %v", p, e)
		}
	}

	srv.awaitRows(t, oracleTableName, totalRows, 180*time.Second)

	c := newBindFuzzClient(t, srv)
	oracleAssert(t, c, oracle)

	capBytes := sfMaxBytes + 256*1024
	for p, dir := range sfDirs {
		sz, err := oracleSfDirSize(dir)
		if err != nil {
			t.Fatalf("producer %d sf_dir %q: walk failed: %v", p, dir, err)
		}
		if sz > capBytes {
			t.Fatalf("producer %d sf_dir %q not purged after clean close: %d bytes (cap %d)",
				p, dir, sz, capBytes)
		}
	}
}
