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

// Go port of QuestDB's QwpEgressFuzzTest. Property-based fuzz coverage
// for QWP egress: each case builds a random schema (1-16 columns drawn
// from a catalogue covering every QWP wire type the server ships),
// rolls per-cell random values in Go so the expected (row, col) value
// is known before the query runs, inserts them as literal rows, picks a
// random query shape (full scan / projection reorder / id-range filter /
// reverse-order limit), streams the result over QWP, and asserts per
// row, per cell that the observed value matches the stored expectation.
// Row-by-row verification catches bugs a per-column sum hides: row
// reordering within a batch, cross-batch boundary misalignment,
// null-bitmap bit swaps, partial varint reads.
//
// Faithful-port divergences from the Java source (cf. the bind / bounds
// ports' headers):
//
//   - No network fragmentation. Java rotates a server debug env var
//     (DEBUG_HTTP_FORCE_{RECV,SEND}_FRAGMENTATION_CHUNK_SIZE) per @Test
//     via startFragmented(chunk). The Go fixture is one shared,
//     long-lived server (sync.Once); per-test server env can't vary.
//     Server-side fragmentation is exhaustively covered by the server
//     repo's QwpEgressFragmentationFuzzTest and is a transport concern
//     orthogonal to the Go decoder's per-cell correctness, which is
//     what this port validates. All cases run against the shared
//     unfragmented server.
//   - No compression rotation. The Go QWP connection string exposes no
//     compression key (conf_parse.go has none), so Java's
//     pickCompression() fragment has nothing to port.
//   - Chunked INSERT. Java emits one giant INSERT ... VALUES; the Go
//     fixture's /exec is a GET, so the rows are split into length-
//     budgeted sub-INSERTs. Identical data, transport detail only.
//   - GEOHASH is existence-only. The Go batch surface has no geohash
//     scalar accessor (only GeohashPrecisionBits); the existence
//     guarantee is "the frame decoded and the null bitmap is correct"
//     (null cells assert IsNull, non-null assert !IsNull) — the same
//     intent as Java's discard-the-value getGeohashValue() call.
//     BINARY / DECIMAL128 / DECIMAL256 / DOUBLE[] keep Java's existing
//     existence-only treatment (encoding not re-implemented client
//     side); DECIMAL64 is bit-verified via its scaled int64.
//   - Reproducibility via QWP_FUZZ_SEED (shared newFuzzRand); the Go
//     RNG sequence need not match Java's Rnd bit-for-bit, only be
//     Go-internally reproducible.

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

const egressFuzzMaxRowsPerCase = 500

// egInsertChunkBudget caps the character length of a single generated
// INSERT statement so the fixture's GET /exec request line stays well
// under any server header-buffer default. A worst-case row (16 wide
// columns, ~70-char LONG256 literals) is ~1.2 KB, comfortably below
// this, so no single row ever overflows a chunk.
const egInsertChunkBudget = 6000

// --- value generators -------------------------------------------------
//
// Mirrors the Java ColumnGenerator catalogue 1:1. randomValue fills a
// SQL literal safely usable inside VALUES(...) plus a deterministic
// int64 hash that must equal observedHash after a faithful QWP round
// trip. supportsNull is false for types QuestDB coerces NULL into a
// zero value (BOOLEAN / BYTE / SHORT / CHAR) and for BINARY (sourced
// via rnd_bin, no NULL literal path).

type egRandomValue struct {
	hash    int64
	literal string
}

type egColumnGenerator interface {
	observedHash(b *QwpColumnBatch, col, row int) int64
	randomValue(r *rand.Rand, out *egRandomValue)
	sqlType() string
	supportsNull() bool
}

// egGenerators is the catalogue, in the exact order of the Java
// GENERATORS array.
var egGenerators = []egColumnGenerator{
	egLongGen{},
	egIntGen{},
	egShortGen{},
	egByteGen{},
	egCharGen{},
	egDoubleGen{},
	egFloatGen{},
	egBooleanGen{},
	newEgSymbolGen("lo", 8),
	newEgSymbolGen("hi", 1000),
	egVarcharGen{},
	egStringGen{},
	egTimestampGen{},
	egTimestampNanosGen{},
	egDateGen{},
	egIpv4Gen{},
	egUuidGen{},
	egLong256Gen{},
	// Existence-only: exercise the decode path but don't assert
	// bit-level equality (encoding not re-implemented in Go).
	egBinaryGen{},
	egGeoHashGen{4, "#b"},
	egGeoHashGen{8, "#bb"},
	egGeoHashGen{24, "#bbbbb"},
	egGeoHashGen{48, "#bbbbbbbbbb"},
	// Three scales exercise distinct scale bytes + divisor paths.
	newEgDecimal64Gen(18, 0),
	newEgDecimal64Gen(18, 4),
	newEgDecimal64Gen(18, 10),
	egDecimal128Gen{},
	egDecimal256Gen{},
	egDoubleArrayGen{},
}

// egHashAscii is the Java hashAsciiString / hashBytes oracle. For ASCII
// input the two Java helpers agree (char vs byte&0xFF), so one Go hash
// over bytes serves both the expected (literal bytes) and observed
// (batch.Str bytes) sides. int64 overflow wraps two's-complement,
// matching Java long arithmetic.
func egHashAscii(b []byte) int64 {
	h := int64(1125899906842597) // large prime seed
	for _, c := range b {
		h = h*31 + int64(c)
	}
	return h ^ int64(len(b)) // mix length so padding changes surface
}

// egRandomASCII mirrors Java randomAsciiString: printable ASCII
// 0x20..0x7D minus 0x27 (single quote) to keep literal building simple.
func egRandomASCII(r *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	bs := make([]byte, n)
	for i := 0; i < n; i++ {
		var cp int
		for {
			cp = 0x20 + r.Intn(0x5E)
			if cp != 0x27 {
				break
			}
		}
		bs[i] = byte(cp)
	}
	return string(bs)
}

func egQuote(s string) string { return strings.ReplaceAll(s, "'", "''") }

type egLongGen struct{}

func (egLongGen) observedHash(b *QwpColumnBatch, col, row int) int64 { return b.Int64(col, row) }
func (egLongGen) randomValue(r *rand.Rand, out *egRandomValue) {
	v := pickNonNullLong(r) // excludes the LONG_NULL sentinel
	out.hash = v
	out.literal = strconv.FormatInt(v, 10) + "L"
}
func (egLongGen) sqlType() string    { return "LONG" }
func (egLongGen) supportsNull() bool { return true }

type egIntGen struct{}

func (egIntGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return int64(b.Int32(col, row))
}
func (egIntGen) randomValue(r *rand.Rand, out *egRandomValue) {
	v := pickNonNullInt(r) // excludes the INT_NULL sentinel
	out.hash = int64(v)
	out.literal = strconv.Itoa(int(v))
}
func (egIntGen) sqlType() string    { return "INT" }
func (egIntGen) supportsNull() bool { return true }

type egShortGen struct{}

func (egShortGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return int64(b.Int16(col, row))
}
func (egShortGen) randomValue(r *rand.Rand, out *egRandomValue) {
	v := int16(r.Intn(65535) - 32767)
	out.hash = int64(v)
	out.literal = "CAST(" + strconv.Itoa(int(v)) + " AS SHORT)"
}
func (egShortGen) sqlType() string    { return "SHORT" }
func (egShortGen) supportsNull() bool { return false }

type egByteGen struct{}

func (egByteGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return int64(b.Int8(col, row))
}
func (egByteGen) randomValue(r *rand.Rand, out *egRandomValue) {
	v := int8(r.Intn(255) - 127)
	out.hash = int64(v)
	out.literal = "CAST(" + strconv.Itoa(int(v)) + " AS BYTE)"
}
func (egByteGen) sqlType() string    { return "BYTE" }
func (egByteGen) supportsNull() bool { return false }

type egCharGen struct{}

func (egCharGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return int64(b.Char(col, row))
}
func (egCharGen) randomValue(r *rand.Rand, out *egRandomValue) {
	c := rune('A' + r.Intn(26))
	out.hash = int64(c)
	out.literal = "'" + string(c) + "'"
}
func (egCharGen) sqlType() string    { return "CHAR" }
func (egCharGen) supportsNull() bool { return false }

type egDoubleGen struct{}

func (egDoubleGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return int64(math.Float64bits(b.Float64(col, row)))
}
func (egDoubleGen) randomValue(r *rand.Rand, out *egRandomValue) {
	var v float64
	for {
		v = (r.Float64() - 0.5) * 1e9
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			break
		}
	}
	out.hash = int64(math.Float64bits(v))
	// 17 significant digits round-trips a float64 bit-for-bit.
	out.literal = "CAST(" + strconv.FormatFloat(v, 'e', 17, 64) + " AS DOUBLE)"
}
func (egDoubleGen) sqlType() string    { return "DOUBLE" }
func (egDoubleGen) supportsNull() bool { return true }

type egFloatGen struct{}

func (egFloatGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return int64(int32(math.Float32bits(b.Float32(col, row))))
}
func (egFloatGen) randomValue(r *rand.Rand, out *egRandomValue) {
	var v float32
	for {
		v = (r.Float32() - 0.5) * 1e5
		if !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) {
			break
		}
	}
	out.hash = int64(int32(math.Float32bits(v)))
	// 9 significant digits round-trips a float32 bit-for-bit.
	out.literal = "CAST(" + strconv.FormatFloat(float64(v), 'e', 8, 32) + " AS FLOAT)"
}
func (egFloatGen) sqlType() string    { return "FLOAT" }
func (egFloatGen) supportsNull() bool { return true }

type egBooleanGen struct{}

func (egBooleanGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	if b.Bool(col, row) {
		return 1
	}
	return 0
}
func (egBooleanGen) randomValue(r *rand.Rand, out *egRandomValue) {
	v := r.Intn(2) == 0
	if v {
		out.hash = 1
	} else {
		out.hash = 0
	}
	out.literal = strconv.FormatBool(v)
}
func (egBooleanGen) sqlType() string    { return "BOOLEAN" }
func (egBooleanGen) supportsNull() bool { return false }

type egSymbolGen struct {
	pool []string
}

func newEgSymbolGen(tag string, n int) egSymbolGen {
	p := make([]string, n)
	for i := 0; i < n; i++ {
		p[i] = "s_" + tag + "_" + strconv.Itoa(i)
	}
	return egSymbolGen{pool: p}
}
func (g egSymbolGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	v := b.Str(col, row)
	if v == nil {
		return 0
	}
	return egHashAscii(v)
}
func (g egSymbolGen) randomValue(r *rand.Rand, out *egRandomValue) {
	s := g.pool[r.Intn(len(g.pool))]
	out.hash = egHashAscii([]byte(s))
	out.literal = "CAST('" + s + "' AS SYMBOL)"
}
func (g egSymbolGen) sqlType() string    { return "SYMBOL" }
func (g egSymbolGen) supportsNull() bool { return true }

type egVarcharGen struct{}

func (egVarcharGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	v := b.Str(col, row)
	if v == nil {
		return 0
	}
	return egHashAscii(v)
}
func (egVarcharGen) randomValue(r *rand.Rand, out *egRandomValue) {
	// Mix short inlinable (<=9 bytes) with longer heap-backed varchar.
	s := egRandomASCII(r, r.Intn(30))
	out.hash = egHashAscii([]byte(s))
	out.literal = "CAST('" + egQuote(s) + "' AS VARCHAR)"
}
func (egVarcharGen) sqlType() string    { return "VARCHAR" }
func (egVarcharGen) supportsNull() bool { return true }

type egStringGen struct{}

func (egStringGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	v := b.Str(col, row)
	if v == nil {
		return 0
	}
	return egHashAscii(v)
}
func (egStringGen) randomValue(r *rand.Rand, out *egRandomValue) {
	s := egRandomASCII(r, r.Intn(16))
	out.hash = egHashAscii([]byte(s))
	out.literal = "'" + egQuote(s) + "'"
}
func (egStringGen) sqlType() string    { return "STRING" }
func (egStringGen) supportsNull() bool { return true }

type egTimestampGen struct{}

func (egTimestampGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return b.Int64(col, row)
}
func (egTimestampGen) randomValue(r *rand.Rand, out *egRandomValue) {
	us := int64(r.Uint64()) & 0x0FFFFFFFFFFFFFFF // positive, representable
	out.hash = us
	out.literal = "CAST(" + strconv.FormatInt(us, 10) + " AS TIMESTAMP)"
}
func (egTimestampGen) sqlType() string    { return "TIMESTAMP" }
func (egTimestampGen) supportsNull() bool { return true }

type egTimestampNanosGen struct{}

func (egTimestampNanosGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return b.Int64(col, row)
}
func (egTimestampNanosGen) randomValue(r *rand.Rand, out *egRandomValue) {
	ns := int64(r.Uint64()) & 0x0FFFFFFFFFFFFFFF
	out.hash = ns
	out.literal = "CAST(" + strconv.FormatInt(ns, 10) + " AS TIMESTAMP_NS)"
}
func (egTimestampNanosGen) sqlType() string    { return "TIMESTAMP_NS" }
func (egTimestampNanosGen) supportsNull() bool { return true }

type egDateGen struct{}

func (egDateGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return b.Int64(col, row)
}
func (egDateGen) randomValue(r *rand.Rand, out *egRandomValue) {
	ms := int64(r.Uint64()) & 0x0000FFFFFFFFFFFF // fits comfortably as a Date
	out.hash = ms
	out.literal = "CAST(" + strconv.FormatInt(ms, 10) + " AS DATE)"
}
func (egDateGen) sqlType() string    { return "DATE" }
func (egDateGen) supportsNull() bool { return true }

type egIpv4Gen struct{}

func (egIpv4Gen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return int64(uint32(b.Int32(col, row)))
}
func (egIpv4Gen) randomValue(r *rand.Rand, out *egRandomValue) {
	a := 1 + r.Intn(254)
	b := r.Intn(256)
	c := r.Intn(256)
	d := 1 + r.Intn(254) // last octet non-zero to avoid the NULL match
	out.hash = (int64(a) << 24) | (int64(b) << 16) | (int64(c) << 8) | int64(d)
	out.literal = fmt.Sprintf("CAST('%d.%d.%d.%d' AS IPv4)", a, b, c, d)
}
func (egIpv4Gen) sqlType() string    { return "IPv4" }
func (egIpv4Gen) supportsNull() bool { return true }

type egUuidGen struct{}

func (egUuidGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return b.UuidHi(col, row) ^ b.UuidLo(col, row)
}
func (egUuidGen) randomValue(r *rand.Rand, out *egRandomValue) {
	hi := int64(r.Uint64())
	lo := int64(r.Uint64())
	// Avoid the QuestDB UUID NULL sentinel (both halves Long.MIN_VALUE).
	if hi == math.MinInt64 && lo == math.MinInt64 {
		lo = 0
	}
	out.hash = hi ^ lo
	out.literal = "CAST('" + egUUIDCanonical(hi, lo) + "' AS UUID)"
}
func (egUuidGen) sqlType() string    { return "UUID" }
func (egUuidGen) supportsNull() bool { return true }

// egUUIDCanonical replicates java.util.UUID.toString for a (mostSig,
// leastSig) pair so the SQL CAST yields exactly the intended 128 bits.
func egUUIDCanonical(hi, lo int64) string {
	h := uint64(hi)
	l := uint64(lo)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		h>>32, (h>>16)&0xffff, h&0xffff, l>>48, l&0xffffffffffff)
}

type egLong256Gen struct{}

func (egLong256Gen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return b.Long256Word(col, row, 0) ^ b.Long256Word(col, row, 1) ^
		b.Long256Word(col, row, 2) ^ b.Long256Word(col, row, 3)
}
func (egLong256Gen) randomValue(r *rand.Rand, out *egRandomValue) {
	var w [4]int64
	for i := 0; i < 4; i++ {
		w[i] = int64(r.Uint64())
	}
	var sb strings.Builder
	sb.WriteString("CAST('0x")
	// Big-endian hex: w[3] high bytes ... w[0] low bytes.
	for i := 3; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("%016x", uint64(w[i])))
	}
	sb.WriteString("' AS LONG256)")
	out.hash = w[0] ^ w[1] ^ w[2] ^ w[3]
	out.literal = sb.String()
}
func (egLong256Gen) sqlType() string    { return "LONG256" }
func (egLong256Gen) supportsNull() bool { return true }

type egBinaryGen struct{}

const egBinaryFixedLen = 12

func (egBinaryGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	v := b.Binary(col, row)
	if v == nil {
		return 0
	}
	return int64(len(v))
}
func (egBinaryGen) randomValue(r *rand.Rand, out *egRandomValue) {
	out.hash = egBinaryFixedLen
	// rnd_bin produces random bytes at INSERT time -- value isn't known
	// client-side, only its fixed length is.
	out.literal = fmt.Sprintf("rnd_bin(%d, %d, 0)", egBinaryFixedLen, egBinaryFixedLen)
}
func (egBinaryGen) sqlType() string    { return "BINARY" }
func (egBinaryGen) supportsNull() bool { return false }

type egGeoHashGen struct {
	precisionBits int
	literal       string
}

func (g egGeoHashGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	// No geohash scalar accessor on the Go batch surface. The frame
	// having decoded (this code runs only on non-null cells, and the
	// null-bitmap is asserted separately) is the existence guarantee,
	// matching Java's discard-the-value getGeohashValue() call.
	return 1
}
func (g egGeoHashGen) randomValue(r *rand.Rand, out *egRandomValue) {
	out.hash = 1
	out.literal = g.literal
}
func (g egGeoHashGen) sqlType() string    { return fmt.Sprintf("GEOHASH(%db)", g.precisionBits) }
func (g egGeoHashGen) supportsNull() bool { return true }

// egDecimal64Gen: value*10^scale stored as a long, so the on-wire bits
// are known and CAN be bit-verified. Scale is captured at construction.
type egDecimal64Gen struct {
	precision int
	scale     int
	divisor   int64
}

func newEgDecimal64Gen(precision, scale int) egDecimal64Gen {
	if scale < 0 || scale > 18 || scale > precision {
		panic(fmt.Sprintf("bad DECIMAL64 (p=%d, s=%d)", precision, scale))
	}
	d := int64(1)
	for i := 0; i < scale; i++ {
		d *= 10
	}
	return egDecimal64Gen{precision: precision, scale: scale, divisor: d}
}
func (g egDecimal64Gen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	return b.Int64(col, row)
}
func (g egDecimal64Gen) randomValue(r *rand.Rand, out *egRandomValue) {
	// Scaled long: the on-wire bits. 6-digit magnitude keeps literal
	// construction cheap; the bit-level assertion is magnitude-agnostic.
	scaled := int64(r.Intn(1_000_000)) - 500_000
	out.hash = scaled
	out.literal = g.toDecimalLiteral(scaled)
}
func (g egDecimal64Gen) sqlType() string {
	return fmt.Sprintf("DECIMAL(%d,%d)", g.precision, g.scale)
}
func (g egDecimal64Gen) supportsNull() bool { return true }
func (g egDecimal64Gen) toDecimalLiteral(scaled int64) string {
	if g.scale == 0 {
		return strconv.FormatInt(scaled, 10) + "m"
	}
	negative := scaled < 0
	abs := scaled
	if negative {
		abs = -abs
	}
	whole := abs / g.divisor
	frac := abs % g.divisor
	var sb strings.Builder
	if negative {
		sb.WriteByte('-')
	}
	sb.WriteString(strconv.FormatInt(whole, 10))
	sb.WriteByte('.')
	fs := strconv.FormatInt(frac, 10)
	for i := 0; i < g.scale-len(fs); i++ {
		sb.WriteByte('0')
	}
	sb.WriteString(fs)
	sb.WriteByte('m')
	return sb.String()
}

type egDecimal128Gen struct{}

var egDecimal128Literals = []string{
	"1.000001m", "2.500500m", "1234567.123456m", "-999999.999999m",
}

func (egDecimal128Gen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	b.Decimal128Lo(col, row)
	b.Decimal128Hi(col, row)
	return 1
}
func (egDecimal128Gen) randomValue(r *rand.Rand, out *egRandomValue) {
	out.hash = 1
	out.literal = egDecimal128Literals[r.Intn(len(egDecimal128Literals))]
}
func (egDecimal128Gen) sqlType() string    { return "DECIMAL(38,6)" }
func (egDecimal128Gen) supportsNull() bool { return true }

type egDecimal256Gen struct{}

var egDecimal256Literals = []string{
	"1.0000000001m", "100.1234567890m", "-1.5m", "99999999.0000000001m",
}

func (egDecimal256Gen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	for w := 0; w < 4; w++ {
		b.Long256Word(col, row, w)
	}
	return 1
}
func (egDecimal256Gen) randomValue(r *rand.Rand, out *egRandomValue) {
	out.hash = 1
	out.literal = egDecimal256Literals[r.Intn(len(egDecimal256Literals))]
}
func (egDecimal256Gen) sqlType() string    { return "DECIMAL(76,10)" }
func (egDecimal256Gen) supportsNull() bool { return true }

type egDoubleArrayGen struct{}

func (egDoubleArrayGen) observedHash(b *QwpColumnBatch, col, row int) int64 {
	arr := b.Float64Array(col, row)
	if arr == nil {
		return 0
	}
	return int64(len(arr))
}
func (egDoubleArrayGen) randomValue(r *rand.Rand, out *egRandomValue) {
	n := 1 + r.Intn(4)
	var sb strings.Builder
	sb.WriteString("ARRAY[")
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		d := (r.Float64() - 0.5) * 100
		sb.WriteString("CAST(")
		sb.WriteString(strconv.FormatFloat(d, 'e', 17, 64))
		sb.WriteString(" AS DOUBLE)")
	}
	sb.WriteByte(']')
	out.hash = int64(n)
	out.literal = sb.String()
}
func (egDoubleArrayGen) sqlType() string    { return "DOUBLE[]" }
func (egDoubleArrayGen) supportsNull() bool { return true }

// --- query planning ---------------------------------------------------

// egQueryPlan describes one random query: SQL text, resultCol->origCol
// map, the inclusive 1-based row-id range that should appear, and
// whether rows come back descending.
type egQueryPlan struct {
	sql        string
	colMap     []int
	firstRow   int
	lastRow    int
	descending bool
}

func egIdentity(n int) []int {
	a := make([]int, n)
	for i := range a {
		a[i] = i
	}
	return a
}

func egAllDataCols(colCount int) string {
	var sb strings.Builder
	for i := 0; i < colCount; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('c')
		sb.WriteString(strconv.Itoa(i))
	}
	return sb.String()
}

func egPickRowCount(r *rand.Rand) int {
	// Skewed distribution hitting small, mid, and batch-boundary sizes.
	choices := []int{1, 2, 7, 64, 257, egressFuzzMaxRowsPerCase - 1, egressFuzzMaxRowsPerCase}
	return choices[r.Intn(len(choices))]
}

// egJavaStringHashCode reproduces java.lang.String.hashCode so the
// per-table shape rotation matches the Java test's caseSalt semantics.
func egJavaStringHashCode(s string) int32 {
	var h int32
	for i := 0; i < len(s); i++ {
		h = 31*h + int32(s[i])
	}
	return h
}

func egFloorMod(x, m int) int { return ((x % m) + m) % m }

func egPlanQuery(r *rand.Rand, table string, colCount, rowCount, caseIdx int) egQueryPlan {
	// 4 shapes rotate deterministically so every shape is exercised
	// across iterations regardless of seed.
	shape := egFloorMod(caseIdx, 4)
	if rowCount < 4 {
		shape = 0 // small cases: just scan everything
	}

	switch shape {
	case 1: // projection subset in scrambled order
		pickCount := 1 + r.Intn(colCount)
		used := make([]bool, colCount)
		m := make([]int, pickCount)
		for i := 0; i < pickCount; i++ {
			var pick int
			for {
				pick = r.Intn(colCount)
				if !used[pick] {
					break
				}
			}
			used[pick] = true
			m[i] = pick
		}
		var sql strings.Builder
		sql.WriteString("SELECT ")
		for i := 0; i < pickCount; i++ {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteByte('c')
			sql.WriteString(strconv.Itoa(m[i]))
		}
		sql.WriteString(" FROM ")
		sql.WriteString(table)
		sql.WriteString(" ORDER BY id")
		return egQueryPlan{sql.String(), m, 1, rowCount, false}
	case 2: // id-range filter -- null-bitmap handling across dropped rows
		lo := 1 + r.Intn(rowCount)
		hi := lo + r.Intn(max(1, rowCount-lo+1))
		sql := "SELECT " + egAllDataCols(colCount) + " FROM " + table +
			" WHERE id >= " + strconv.Itoa(lo) + " AND id <= " + strconv.Itoa(hi) +
			" ORDER BY id"
		return egQueryPlan{sql, egIdentity(colCount), lo, hi, false}
	case 3: // reverse + LIMIT -- last K rows, descending
		k := 1 + r.Intn(rowCount)
		sql := "SELECT " + egAllDataCols(colCount) + " FROM " + table +
			" ORDER BY id DESC LIMIT " + strconv.Itoa(k)
		return egQueryPlan{sql, egIdentity(colCount), rowCount - k + 1, rowCount, true}
	default:
		sql := "SELECT " + egAllDataCols(colCount) + " FROM " + table + " ORDER BY id"
		return egQueryPlan{sql, egIdentity(colCount), 1, rowCount, false}
	}
}

// --- per-cell verification --------------------------------------------

type egAssertionState struct {
	plan         egQueryPlan
	cols         []egColumnGenerator
	expected     [][]int64
	expectedNull [][]bool
	observed     int
}

func (s *egAssertionState) observe(t *testing.T, b *QwpColumnBatch) {
	t.Helper()
	n := b.RowCount()
	resultColCount := len(s.plan.colMap)
	for rr := 0; rr < n; rr++ {
		var logicalRow int
		if s.plan.descending {
			logicalRow = s.plan.lastRow - s.observed
		} else {
			logicalRow = s.plan.firstRow + s.observed
		}
		rowIdx := logicalRow - 1
		for rc := 0; rc < resultColCount; rc++ {
			origCol := s.plan.colMap[rc]
			ctx := fmt.Sprintf("row=%d resultCol=%d origCol=%d type=%s sql=%s",
				logicalRow, rc, origCol, s.cols[origCol].sqlType(), s.plan.sql)
			if s.expectedNull[rowIdx][origCol] {
				if !b.IsNull(rc, rr) {
					t.Fatalf("expected NULL: %s", ctx)
				}
			} else {
				if b.IsNull(rc, rr) {
					t.Fatalf("expected non-NULL: %s", ctx)
				}
				got := s.cols[origCol].observedHash(b, rc, rr)
				if want := s.expected[rowIdx][origCol]; got != want {
					t.Fatalf("value mismatch: %s want=%d got=%d", ctx, want, got)
				}
			}
		}
		s.observed++
	}
}

func (s *egAssertionState) end(t *testing.T, totalRows int64) {
	t.Helper()
	expectedRows := s.plan.lastRow - s.plan.firstRow + 1
	if totalRows != int64(expectedRows) {
		t.Fatalf("row count (TotalRows) for %s: want %d got %d",
			s.plan.sql, expectedRows, totalRows)
	}
	if s.observed != expectedRows {
		t.Fatalf("row count (observed) for %s: want %d got %d",
			s.plan.sql, expectedRows, s.observed)
	}
}

// --- one fuzz case ----------------------------------------------------

func newEgressClient(t *testing.T, srv *qwpFuzzServer) *QwpQueryClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := QwpQueryClientFromConf(ctx, srv.connConf())
	if err != nil {
		t.Fatalf("QwpQueryClientFromConf(%q): %v", srv.connConf(), err)
	}
	return c
}

func closeEgressClient(c *QwpQueryClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.Close(ctx)
}

// egInsertRows builds and runs the multi-row INSERT, split into
// length-budgeted sub-statements. id = r+1; ts = r*1000us (1ms/row)
// keeps the whole run inside one partition for any practical row count.
func egInsertRows(t *testing.T, srv *qwpFuzzServer, table string,
	colCount int, literals [][]string, rowCount int) {
	t.Helper()
	prefix := "INSERT INTO " + table + " VALUES "
	var sb strings.Builder
	rowsInChunk := 0
	flush := func() {
		if rowsInChunk == 0 {
			return
		}
		srv.mustExec(t, sb.String())
		sb.Reset()
		rowsInChunk = 0
	}
	for rIdx := 0; rIdx < rowCount; rIdx++ {
		var row strings.Builder
		row.WriteByte('(')
		row.WriteString(strconv.Itoa(rIdx + 1))
		row.WriteString(", CAST(")
		row.WriteString(strconv.FormatInt(int64(rIdx)*1000, 10))
		row.WriteString(" AS TIMESTAMP)")
		for c := 0; c < colCount; c++ {
			row.WriteString(", ")
			row.WriteString(literals[rIdx][c])
		}
		row.WriteByte(')')

		if rowsInChunk > 0 && sb.Len()+2+row.Len() > egInsertChunkBudget {
			flush()
		}
		if rowsInChunk == 0 {
			sb.WriteString(prefix)
		} else {
			sb.WriteString(", ")
		}
		sb.WriteString(row.String())
		rowsInChunk++
	}
	flush()
}

func egRunOneCase(t *testing.T, srv *qwpFuzzServer, c *QwpQueryClient,
	table string, colCount int, r *rand.Rand) {
	t.Helper()

	cols := make([]egColumnGenerator, colCount)
	nullable := make([]bool, colCount)
	for i := 0; i < colCount; i++ {
		cols[i] = egGenerators[r.Intn(len(egGenerators))]
		nullable[i] = cols[i].supportsNull() && r.Intn(2) == 0
	}
	rowCount := egPickRowCount(r)

	// id anchors ORDER BY; ts is the designated timestamp so the table
	// runs as WAL (matches production; DROP goes through WAL apply).
	var ddl strings.Builder
	ddl.WriteString("CREATE TABLE ")
	ddl.WriteString(table)
	ddl.WriteString(" (id LONG, ts TIMESTAMP")
	for i := 0; i < colCount; i++ {
		ddl.WriteString(", c")
		ddl.WriteString(strconv.Itoa(i))
		ddl.WriteByte(' ')
		ddl.WriteString(cols[i].sqlType())
	}
	ddl.WriteString(") TIMESTAMP(ts) PARTITION BY DAY WAL")
	srv.mustExec(t, ddl.String())
	defer srv.mustExec(t, "DROP TABLE IF EXISTS '"+table+"'")

	// Roll values in Go; remember expected hash + null-ness per cell.
	expected := make([][]int64, rowCount)
	expectedNull := make([][]bool, rowCount)
	literals := make([][]string, rowCount)
	var buf egRandomValue
	for rr := 0; rr < rowCount; rr++ {
		expected[rr] = make([]int64, colCount)
		expectedNull[rr] = make([]bool, colCount)
		literals[rr] = make([]string, colCount)
		for cc := 0; cc < colCount; cc++ {
			isNull := nullable[cc] && r.Intn(5) == 0
			if isNull {
				expectedNull[rr][cc] = true
				literals[rr][cc] = "CAST(NULL AS " + cols[cc].sqlType() + ")"
			} else {
				cols[cc].randomValue(r, &buf)
				expected[rr][cc] = buf.hash
				literals[rr][cc] = buf.literal
			}
		}
	}

	egInsertRows(t, srv, table, colCount, literals, rowCount)
	// WAL tables commit asynchronously; wait for the apply job before
	// the SELECT or we'd race the stream against an empty table view.
	srv.awaitRows(t, table, rowCount, 60*time.Second)

	caseSalt := int(egJavaStringHashCode(table))
	plan := egPlanQuery(r, table, colCount, rowCount, caseSalt)

	state := &egAssertionState{
		plan: plan, cols: cols, expected: expected, expectedNull: expectedNull,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	q := c.Query(ctx, plan.sql)
	defer q.Close()
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("egress error [%s]: %v", table, err)
		}
		state.observe(t, batch)
	}
	state.end(t, q.TotalRows())
}

// --- @Test entry points -----------------------------------------------

// TestQwpFuzzEgressRandomSchemaRoundtrip is the main sweep: a fresh
// connection per case so state pollution can't mask a bug.
func TestQwpFuzzEgressRandomSchemaRoundtrip(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	for i := 0; i < 15; i++ {
		func() {
			c := newEgressClient(t, srv)
			defer closeEgressClient(c)
			egRunOneCase(t, srv, c, fmt.Sprintf("egfz_iter_%d", i), 1+r.Intn(6), r)
		}()
	}
}

// TestQwpFuzzEgressBackToBackSameConnection exercises per-connection
// state that survives across queries: the conn symbol dict, schema
// registry, and Gorilla decoder state.
func TestQwpFuzzEgressBackToBackSameConnection(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	c := newEgressClient(t, srv)
	defer closeEgressClient(c)
	for q := 0; q < 12; q++ {
		egRunOneCase(t, srv, c, fmt.Sprintf("egfz_back_%d", q), 1+r.Intn(4), r)
	}
}

// TestQwpFuzzEgressWideTables stresses the batch buffer's per-column
// state arrays and the schema block encoder with 10-16 columns.
func TestQwpFuzzEgressWideTables(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	c := newEgressClient(t, srv)
	defer closeEgressClient(c)
	egRunOneCase(t, srv, c, "egfz_wide", 10+r.Intn(7), r)
}

// --- select / alter sequence fuzz -------------------------------------

func egCatCount(totalRows, kMod int) int64 {
	if kMod == 0 {
		return int64(totalRows / 4)
	}
	return int64((totalRows + 4 - kMod) / 4)
}

func egCatFor(id int64) byte { return "abcd"[id%4] }

func egExpectedV(id int64) float64 { return float64(id) * 1.5 }

func egExpectedTs(id, spacingMicros int64) int64 { return (id - 1) * spacingMicros }

// egAssertRows drives client.Query(sql) and dispatches every batch to
// verifier, which returns the running total of rows checked. After the
// stream ends both the server-reported total and the observed total
// must equal expected. Per-cell assertions live in the verifier.
func egAssertRows(t *testing.T, c *QwpQueryClient, sql string, expected int64,
	verify func(b *QwpColumnBatch, startRow int64) int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	q := c.Query(ctx, sql)
	defer q.Close()
	var seen int64
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("query failed [%s]: %v", sql, err)
		}
		seen = verify(batch, seen)
	}
	if got := q.TotalRows(); got != expected {
		t.Fatalf("row count (TotalRows) [%s]: want %d got %d", sql, expected, got)
	}
	if seen != expected {
		t.Fatalf("row count (observed) [%s]: want %d got %d", sql, expected, seen)
	}
}

func egVerifyBaseColumn(t *testing.T, b *QwpColumnBatch, col, batchRow int,
	name string, id, spacingMicros int64, tag string) {
	t.Helper()
	switch name {
	case "id":
		if got := b.Int64(col, batchRow); got != id {
			t.Fatalf("%s id @ id=%d: want %d got %d", tag, id, id, got)
		}
	case "v":
		if got := b.Float64(col, batchRow); got != egExpectedV(id) {
			t.Fatalf("%s v @ id=%d: want %v got %v", tag, id, egExpectedV(id), got)
		}
	case "cat":
		seq := b.Str(col, batchRow)
		if seq == nil {
			t.Fatalf("%s cat must not be NULL @ id=%d", tag, id)
		}
		if len(seq) != 1 {
			t.Fatalf("%s cat byte length @ id=%d: want 1 got %d", tag, id, len(seq))
		}
		if seq[0] != egCatFor(id) {
			t.Fatalf("%s cat char @ id=%d: want %q got %q",
				tag, id, egCatFor(id), seq[0])
		}
	case "ts":
		if got := b.Int64(col, batchRow); got != egExpectedTs(id, spacingMicros) {
			t.Fatalf("%s ts @ id=%d: want %d got %d",
				tag, id, egExpectedTs(id, spacingMicros), got)
		}
	default:
		t.Fatalf("%s unknown base column: %s", tag, name)
	}
}

// egAwaitColumnCount polls table_columns until the column count matches
// want. This is the network-client analog of the Java test's
// server.awaitTable() after a structural ALTER: it blocks until the WAL
// apply job has materialised the ADD/DROP COLUMN so a subsequent SELECT
// *'s column set is stable.
func egAwaitColumnCount(t *testing.T, srv *qwpFuzzServer, table string,
	want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	q := fmt.Sprintf("SELECT count() FROM table_columns('%s')", table)
	for {
		res, err := srv.execSQL(q)
		if err == nil && len(res.Dataset) == 1 && len(res.Dataset[0]) == 1 {
			if n, ok := toInt64(res.Dataset[0][0]); ok && n == int64(want) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: table %q did not reach %d columns within %s",
				table, want, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// egRunSelectShape runs one of six SELECT shapes against the stable
// fz_seq table and asserts BOTH the row count AND per-cell correctness.
// Shapes span both server cursor paths: PageFrameCursor (plain /
// predicate / interval / projection / star) and RecordCursor (GROUP BY).
func egRunSelectShape(t *testing.T, srv *qwpFuzzServer, c *QwpQueryClient,
	r *rand.Rand, shape, totalRows int, spacingMicros int64, liveAdded []string) {
	t.Helper()
	switch shape {
	case 0: // plain full scan, ts-ordered -> id-ordered; globalRow N -> id N+1
		egAssertRows(t, c, "SELECT id FROM fz_seq", int64(totalRows),
			func(b *QwpColumnBatch, startRow int64) int64 {
				n := b.RowCount()
				for rr := 0; rr < n; rr++ {
					expectedId := startRow + int64(rr) + 1
					if got := b.Int64(0, rr); got != expectedId {
						t.Fatalf("shape 0 id @ row %d: want %d got %d",
							startRow+int64(rr), expectedId, got)
					}
				}
				return startRow + int64(n)
			})
	case 1: // id-range predicate, random threshold
		threshold := 1 + r.Intn(max(1, totalRows-1))
		expected := int64(totalRows - threshold)
		egAssertRows(t, c,
			fmt.Sprintf("SELECT id, v FROM fz_seq WHERE id > %d", threshold),
			expected, func(b *QwpColumnBatch, startRow int64) int64 {
				n := b.RowCount()
				for rr := 0; rr < n; rr++ {
					expectedId := int64(threshold) + startRow + int64(rr) + 1
					if got := b.Int64(0, rr); got != expectedId {
						t.Fatalf("shape 1 id @ row %d: want %d got %d",
							startRow+int64(rr), expectedId, got)
					}
					if got := b.Float64(1, rr); got != egExpectedV(expectedId) {
						t.Fatalf("shape 1 v @ row %d: want %v got %v",
							startRow+int64(rr), egExpectedV(expectedId), got)
					}
				}
				return startRow + int64(n)
			})
	case 2: // GROUP BY -- RecordCursor path; cat cycles 4 symbols -> 4 rows
		counts := make(map[byte]int64)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		q := c.Query(ctx, "SELECT cat, COUNT(*) c FROM fz_seq")
		for batch, err := range q.Batches() {
			if err != nil {
				q.Close()
				t.Fatalf("shape 2 query failed: %v", err)
			}
			for rr := 0; rr < batch.RowCount(); rr++ {
				seq := batch.Str(0, rr)
				if seq == nil {
					q.Close()
					t.Fatalf("shape 2 cat must not be NULL")
				}
				if len(seq) != 1 {
					q.Close()
					t.Fatalf("shape 2 cat byte length: want 1 got %d", len(seq))
				}
				counts[seq[0]] = batch.Int64(1, rr)
			}
		}
		q.Close()
		if len(counts) != 4 {
			t.Fatalf("shape 2 distinct cat count: want 4 got %d", len(counts))
		}
		for k, kMod := range map[byte]int{'a': 0, 'b': 1, 'c': 2, 'd': 3} {
			if got, want := counts[k], egCatCount(totalRows, kMod); got != want {
				t.Fatalf("shape 2 count(%q): want %d got %d", k, want, got)
			}
		}
	case 3: // interval on designated ts -- PageFrameCursor + partition skip
		loRow := 1 + r.Intn(max(1, totalRows-2))
		span := 1 + r.Intn(max(1, totalRows-loRow))
		hiRow := loRow + span
		tsLo := int64(loRow-1) * spacingMicros
		tsHi := int64(hiRow-1) * spacingMicros
		egAssertRows(t, c,
			fmt.Sprintf("SELECT id FROM fz_seq WHERE ts >= CAST(%d AS TIMESTAMP) "+
				"AND ts < CAST(%d AS TIMESTAMP)", tsLo, tsHi),
			int64(span), func(b *QwpColumnBatch, startRow int64) int64 {
				n := b.RowCount()
				for rr := 0; rr < n; rr++ {
					expectedId := int64(loRow) + startRow + int64(rr)
					if got := b.Int64(0, rr); got != expectedId {
						t.Fatalf("shape 3 id @ row %d: want %d got %d",
							startRow+int64(rr), expectedId, got)
					}
				}
				return startRow + int64(n)
			})
	case 4: // random projection of the stable base columns
		base := []string{"id", "v", "cat", "ts"}
		pickCount := 1 + r.Intn(len(base))
		shuffled := append([]string(nil), base...)
		for i := len(shuffled) - 1; i > 0; i-- {
			j := r.Intn(i + 1)
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		}
		projection := shuffled[:pickCount]
		sql := "SELECT " + strings.Join(projection, ", ") + " FROM fz_seq ORDER BY id"
		egAssertRows(t, c, sql, int64(totalRows),
			func(b *QwpColumnBatch, startRow int64) int64 {
				if b.ColumnCount() != len(projection) {
					t.Fatalf("shape 4 column count: want %d got %d",
						len(projection), b.ColumnCount())
				}
				n := b.RowCount()
				for rr := 0; rr < n; rr++ {
					id := startRow + int64(rr) + 1
					for cc := 0; cc < len(projection); cc++ {
						egVerifyBaseColumn(t, b, cc, rr, projection[cc],
							id, spacingMicros, "shape 4")
					}
				}
				return startRow + int64(n)
			})
	case 5: // SELECT * -- column set follows ADD / DROP automatically
		expectedExtras := len(liveAdded)
		egAssertRows(t, c, "SELECT * FROM fz_seq", int64(totalRows),
			func(b *QwpColumnBatch, startRow int64) int64 {
				if b.ColumnCount() != 4+expectedExtras {
					t.Fatalf("shape 5 column count: want %d got %d",
						4+expectedExtras, b.ColumnCount())
				}
				n := b.RowCount()
				for rr := 0; rr < n; rr++ {
					id := startRow + int64(rr) + 1
					egVerifyBaseColumn(t, b, 0, rr, "id", id, spacingMicros, "shape 5")
					egVerifyBaseColumn(t, b, 1, rr, "v", id, spacingMicros, "shape 5")
					egVerifyBaseColumn(t, b, 2, rr, "cat", id, spacingMicros, "shape 5")
					egVerifyBaseColumn(t, b, 3, rr, "ts", id, spacingMicros, "shape 5")
					for cc := 4; cc < 4+expectedExtras; cc++ {
						if !b.IsNull(cc, rr) {
							t.Fatalf("shape 5 extra col %d @ row %d must be NULL",
								cc, startRow+int64(rr))
						}
					}
				}
				return startRow + int64(n)
			})
	default:
		t.Fatalf("unknown shape: %d", shape)
	}
}

// TestQwpFuzzEgressSelectAlterSequence fuzzes sequences of SELECT /
// ALTER TABLE ADD|DROP COLUMN against one stable table, mixing six
// SELECT shapes in random order with occasional schema evolutions. Each
// ALTER stamps a new tableId and invalidates the server's compile
// cache, so the next SELECT with the same SQL text must detect the
// stale factory and recompile. Added columns are left NULL.
func TestQwpFuzzEgressSelectAlterSequence(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)

	rowCount := 50 + r.Intn(951)
	// Spacing options (microseconds) stress different partition
	// densities for the designated-ts interval predicate.
	spacingChoices := []int64{
		300_000_000, 864_000_000, 3_600_000_000, 21_600_000_000,
	}
	spacingMicros := spacingChoices[r.Intn(len(spacingChoices))]
	opCount := 15 + r.Intn(26)
	structuralProbPermil := 150 + r.Intn(251)
	maxLiveAddedColumns := 2 + r.Intn(5)
	t.Logf("select/alter sequence fuzz: rowCount=%d spacingMicros=%d opCount=%d "+
		"structuralProbPermil=%d maxLiveAddedColumns=%d",
		rowCount, spacingMicros, opCount, structuralProbPermil, maxLiveAddedColumns)

	srv.mustExec(t, "DROP TABLE IF EXISTS 'fz_seq'")
	defer srv.mustExec(t, "DROP TABLE IF EXISTS 'fz_seq'")
	srv.mustExec(t, "CREATE TABLE fz_seq(id LONG, v DOUBLE, cat SYMBOL, ts TIMESTAMP) "+
		"TIMESTAMP(ts) PARTITION BY DAY WAL")
	srv.mustExec(t, fmt.Sprintf("INSERT INTO fz_seq SELECT x, x * 1.5, "+
		"CASE WHEN x %% 4 = 0 THEN 'a' WHEN x %% 4 = 1 THEN 'b' "+
		"WHEN x %% 4 = 2 THEN 'c' ELSE 'd' END, "+
		"CAST((x - 1) * %d AS TIMESTAMP) FROM long_sequence(%d)",
		spacingMicros, rowCount))
	srv.awaitRows(t, "fz_seq", rowCount, 90*time.Second)

	liveAdded := make([]string, 0, maxLiveAddedColumns)
	nextColumnId := 0

	c := newEgressClient(t, srv)
	defer closeEgressClient(c)

	// Seed the cache with a SELECT we'll rerun, so the first structural
	// op actually invalidates something.
	egRunSelectShape(t, srv, c, r, 0, rowCount, spacingMicros, liveAdded)

	for op := 0; op < opCount; op++ {
		structural := r.Intn(1000) < structuralProbPermil
		if structural {
			canAdd := len(liveAdded) < maxLiveAddedColumns
			canDrop := len(liveAdded) > 0
			doAdd := (canAdd && !canDrop) || (canAdd && r.Intn(10) < 6)
			if doAdd {
				newCol := fmt.Sprintf("extra_%d", nextColumnId)
				nextColumnId++
				srv.mustExec(t, "ALTER TABLE fz_seq ADD COLUMN "+newCol+" VARCHAR")
				liveAdded = append(liveAdded, newCol)
				egAwaitColumnCount(t, srv, "fz_seq", 4+len(liveAdded), 60*time.Second)
				t.Logf("[op=%d] ALTER ADD %s", op, newCol)
			} else if canDrop {
				idx := r.Intn(len(liveAdded))
				victim := liveAdded[idx]
				liveAdded = append(liveAdded[:idx], liveAdded[idx+1:]...)
				srv.mustExec(t, "ALTER TABLE fz_seq DROP COLUMN "+victim)
				egAwaitColumnCount(t, srv, "fz_seq", 4+len(liveAdded), 60*time.Second)
				t.Logf("[op=%d] ALTER DROP %s", op, victim)
			} else {
				egRunSelectShape(t, srv, c, r, r.Intn(6), rowCount, spacingMicros, liveAdded)
			}
		} else {
			egRunSelectShape(t, srv, c, r, r.Intn(6), rowCount, spacingMicros, liveAdded)
		}
	}
}
