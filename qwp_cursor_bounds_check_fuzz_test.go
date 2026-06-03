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

// Go port of QuestDB's QwpCursorBoundsCheckFuzzTest. Generates valid QWP
// egress (RESULT_BATCH) messages with random schemas/rows/types, then:
//
//   - truncates them at every byte position, and
//   - corrupts random bytes,
//
// asserting the decoder rejects bad input with an error and NEVER panics
// (index-out-of-range / nil-deref are the Go analogue of the Java test's
// "unexpected NPE/AIOOBE = missing bounds check"). The decoder is
// error-only by design, so a panic anywhere is a real validation gap.
//
// This test is pure and server-free (it drives (*qwpQueryDecoder).decode
// directly), so it carries no //go:build tag and runs under the normal
// `go test ./...` in build.yml as well as the qwp-fuzz workflow.
//
// Faithful divergences from the Java source:
//   - The Java test builds 1-3 *tables* (ingress-style cursor); the Go
//     egress decoder is single-table per RESULT_BATCH, so we generate one
//     table with 1-6 columns.
//   - SYMBOL is excluded: the Go egress decoder requires a
//     connection-scoped delta symbol dictionary (FLAG_DELTA_SYMBOL_DICT),
//     which is out of scope for a single stateless decode call and is
//     already covered by the decoder hardening tests.
//   - DATE is excluded: it is asymmetric on the wire (ingestion = plain
//     int64; egress = timestamp-ish framing), so the ingestion encoder
//     used here cannot synthesise a valid egress DATE column. Egress
//     DATE decode is covered by TestQwpDecoderEgressDate.
//   - We build the valid seed message with the real encoder rather than a
//     hand-rolled byte writer, so "valid" is guaranteed valid for the Go
//     decoder; rows are 1-19 (the 0-row/0-col degenerate frame is pinned
//     separately by TestQwpDecoderHardening).

import (
	"math"
	"math/rand"
	"runtime/debug"
	"strconv"
	"testing"
)

const (
	boundsFuzzIterations    = 50
	boundsCorruptionsPerMsg = 30
)

// boundsCandidateTypes is the Java FUZZABLE_TYPES set minus SYMBOL and
// DATE (see file header for why).
var boundsCandidateTypes = []qwpTypeCode{
	qwpTypeBoolean, qwpTypeByte, qwpTypeShort, qwpTypeInt, qwpTypeLong,
	qwpTypeFloat, qwpTypeDouble, qwpTypeTimestamp, qwpTypeTimestampNano,
	qwpTypeUuid, qwpTypeLong256, qwpTypeChar, qwpTypeVarchar, qwpTypeGeohash,
	qwpTypeDecimal64, qwpTypeDecimal128, qwpTypeDecimal256, qwpTypeDoubleArray,
}

// boundsAdderFor returns a per-column value generator for code. DECIMAL
// scale and GEOHASH precision are chosen ONCE here and captured, because
// the wire format pins them per column — the encoder rejects a row whose
// scale/precision differs from the column's established value. Everything
// else may vary freely per row.
func boundsAdderFor(t *testing.T, code qwpTypeCode, r *rand.Rand) func(*qwpColumnBuffer, *rand.Rand) {
	switch code {
	case qwpTypeBoolean:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addBool(r.Intn(2) == 0) }
	case qwpTypeByte:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addByte(int8(r.Uint32())) }
	case qwpTypeShort:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addShort(int16(r.Uint32())) }
	case qwpTypeInt:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addInt32(int32(r.Uint32())) }
	case qwpTypeLong:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addLong(int64(r.Uint64())) }
	case qwpTypeFloat:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addFloat32(math.Float32frombits(r.Uint32())) }
	case qwpTypeDouble:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addDouble(math.Float64frombits(r.Uint64())) }
	case qwpTypeTimestamp, qwpTypeDate, qwpTypeTimestampNano:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addTimestamp(int64(r.Uint64())) }
	case qwpTypeUuid:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addUuid(r.Uint64(), r.Uint64()) }
	case qwpTypeLong256:
		return func(c *qwpColumnBuffer, r *rand.Rand) {
			c.addLong256(r.Uint64(), r.Uint64(), r.Uint64(), r.Uint64())
		}
	case qwpTypeChar:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addChar(rune(0x20 + r.Intn(95))) }
	case qwpTypeVarchar:
		return func(c *qwpColumnBuffer, r *rand.Rand) { c.addString(boundsRandASCII(r)) }
	case qwpTypeGeohash:
		prec := int8(1 + r.Intn(60)) // fixed per column, 1-60 bits
		return func(c *qwpColumnBuffer, r *rand.Rand) {
			v := r.Uint64() & ((uint64(1) << uint(prec)) - 1)
			if err := c.addGeohash(v, prec); err != nil {
				t.Fatalf("addGeohash(prec=%d): %v", prec, err)
			}
		}
	case qwpTypeDecimal64, qwpTypeDecimal128, qwpTypeDecimal256:
		scale := uint32(r.Intn(11)) // fixed per column
		return func(c *qwpColumnBuffer, r *rand.Rand) {
			// <= 16 digits keeps the unscaled value inside DECIMAL64's
			// 18-digit precision (and trivially 128/256); the value is
			// irrelevant to a bounds fuzz, only frame validity.
			u := r.Int63n(1_000_000_000_000_000)
			if r.Intn(2) == 0 {
				u = -u
			}
			if err := c.addDecimal(NewDecimalFromInt64(u, scale)); err != nil {
				t.Fatalf("addDecimal(scale=%d): %v", scale, err)
			}
		}
	case qwpTypeDoubleArray:
		return func(c *qwpColumnBuffer, r *rand.Rand) {
			// 1-3 elements: the Go decoder rejects a 0-length dim
			// ("ARRAY dim 0 must be >= 1"), so a valid seed needs >= 1;
			// the corruption/truncation passes still explore dim 0.
			n := 1 + r.Intn(3)
			flat := make([]float64, n)
			for i := range flat {
				flat[i] = math.Float64frombits(r.Uint64())
			}
			c.addDoubleArray(1, []int32{int32(n)}, flat)
		}
	default:
		t.Fatalf("boundsAdderFor: unhandled type %#x", code)
		return nil
	}
}

// boundsRandASCII returns 0-19 printable-ASCII bytes (Java's
// writeStringColumnData uses 0x20 + rnd(95)).
func boundsRandASCII(r *rand.Rand) string {
	n := r.Intn(20)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(0x20 + r.Intn(95))
	}
	return string(b)
}

// genValidBoundsMessage builds a valid single-table RESULT_BATCH with
// 1-6 columns of random fuzzable types and 1-19 rows, using the real
// encoder so the frame is guaranteed valid for the Go decoder.
func genValidBoundsMessage(t *testing.T, r *rand.Rand) []byte {
	t.Helper()
	colCount := 1 + r.Intn(6)
	rowCount := 1 + r.Intn(19)

	codes := make([]qwpTypeCode, colCount)
	nullable := make([]bool, colCount)
	adders := make([]func(*qwpColumnBuffer, *rand.Rand), colCount)
	for i := 0; i < colCount; i++ {
		codes[i] = boundsCandidateTypes[r.Intn(len(boundsCandidateTypes))]
		nullable[i] = r.Intn(2) == 0
		adders[i] = boundsAdderFor(t, codes[i], r)
	}

	tb := newQwpTableBuffer("t" + strconv.Itoa(r.Intn(100)))
	for row := 0; row < rowCount; row++ {
		for ci := 0; ci < colCount; ci++ {
			col, err := tb.getOrCreateColumn("c"+strconv.Itoa(ci), codes[ci], nullable[ci])
			if err != nil {
				t.Fatalf("getOrCreateColumn(c%d, type=%#x): %v", ci, codes[ci], err)
			}
			if nullable[ci] && r.Intn(5) == 0 {
				col.addNull()
			} else {
				adders[ci](col, r)
			}
		}
		tb.commitRow()
	}
	var enc qwpEncoder
	ingress := enc.encodeTable(tb)
	return wrapAsResultBatch(ingress, 1, 0)
}

// walkBoundsBatch exercises the parsed batch the way the Java test's
// parseAndIterate walks every row/column. It only uses each column's
// correct accessor (never a mismatched one — that would be accessor
// misuse, not a decoder bug). Variable-length accessors (Str/array) are
// the interesting ones: their offset/dim bounds logic is what a
// corrupted-but-still-decodable frame would trip.
func walkBoundsBatch(b *QwpColumnBatch) {
	for col := 0; col < b.ColumnCount(); col++ {
		_ = b.ColumnName(col)
		_ = b.DecimalScale(col)
		_ = b.GeohashPrecisionBits(col)
		_ = b.NonNullCount(col)
		ct := qwpTypeCode(b.ColumnType(col))
		rows := b.RowCount()
		for row := 0; row < rows; row++ {
			if b.IsNull(col, row) {
				continue
			}
			switch ct {
			case qwpTypeBoolean:
				_ = b.Bool(col, row)
			case qwpTypeByte:
				_ = b.Int8(col, row)
			case qwpTypeShort:
				_ = b.Int16(col, row)
			case qwpTypeInt, qwpTypeIPv4:
				_ = b.Int32(col, row)
			case qwpTypeLong, qwpTypeTimestamp, qwpTypeDate, qwpTypeTimestampNano, qwpTypeDecimal64:
				_ = b.Int64(col, row)
			case qwpTypeFloat:
				_ = b.Float32(col, row)
			case qwpTypeDouble:
				_ = b.Float64(col, row)
			case qwpTypeChar:
				_ = b.Char(col, row)
			case qwpTypeUuid:
				_ = b.UuidLo(col, row)
				_ = b.UuidHi(col, row)
			case qwpTypeLong256:
				for w := 0; w < 4; w++ {
					_ = b.Long256Word(col, row, w)
				}
			case qwpTypeDecimal128:
				_ = b.Decimal128Lo(col, row)
				_ = b.Decimal128Hi(col, row)
			case qwpTypeVarchar, qwpTypeBinary:
				_ = b.Str(col, row)
			case qwpTypeDoubleArray:
				_ = b.Float64Array(col, row)
			case qwpTypeLongArray:
				_ = b.Int64Array(col, row)
			default:
				// DECIMAL256 (no scalar accessor), GEOHASH (precision
				// read above), SYMBOL, etc. — IsNull above already
				// touched the parsed layout.
			}
		}
	}
}

// decodeBoundsNoPanic runs one decode (+ full walk on success) under a
// panic guard. A returned error is fine — that is the parser correctly
// rejecting truncated/corrupt input. A panic is a missing bounds check
// and fails the test (the Go analogue of the Java test's
// `catch (Throwable t) Assert.fail`).
func decodeBoundsNoPanic(t *testing.T, payload []byte, ctx string) {
	t.Helper()
	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("%s: decoder panicked (missing bounds check): %v\n%s",
				ctx, rec, debug.Stack())
		}
	}()
	d := newTestQueryDecoder()
	var b QwpColumnBatch
	if err := d.decode(payload, &b); err == nil {
		walkBoundsBatch(&b)
	}
}

func TestQwpFuzzCursorBoundsTruncation(t *testing.T) {
	r := newFuzzRand(t)
	for iter := 0; iter < boundsFuzzIterations; iter++ {
		msg := genValidBoundsMessage(t, r)
		// Sanity: the generated message must parse cleanly in full.
		d := newTestQueryDecoder()
		var b QwpColumnBatch
		if err := d.decode(msg, &b); err != nil {
			t.Fatalf("iter %d: generated message failed full parse: %v", iter, err)
		}
		for truncLen := 0; truncLen < len(msg); truncLen++ {
			decodeBoundsNoPanic(t, msg[:truncLen],
				"iter "+strconv.Itoa(iter)+" truncLen="+strconv.Itoa(truncLen)+"/"+strconv.Itoa(len(msg)))
		}
	}
}

func TestQwpFuzzCursorBoundsCorruption(t *testing.T) {
	r := newFuzzRand(t)
	for iter := 0; iter < boundsFuzzIterations; iter++ {
		msg := genValidBoundsMessage(t, r)
		d := newTestQueryDecoder()
		var b QwpColumnBatch
		if err := d.decode(msg, &b); err != nil {
			t.Fatalf("iter %d: generated message failed full parse: %v", iter, err)
		}
		for c := 0; c < boundsCorruptionsPerMsg; c++ {
			corrupted := make([]byte, len(msg))
			copy(corrupted, msg)
			nCorrupt := 1 + r.Intn(3)
			for i := 0; i < nCorrupt; i++ {
				corrupted[r.Intn(len(corrupted))] = byte(r.Intn(256))
			}
			decodeBoundsNoPanic(t, corrupted,
				"iter "+strconv.Itoa(iter)+" corruption="+strconv.Itoa(c))
		}
	}
}
