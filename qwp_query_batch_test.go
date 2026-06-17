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

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
)

// buildFixedLayout produces a qwpColumnLayout with no nulls and the
// given values region. Used as a helper across the fixed-width tests.
func buildFixedLayout(info *qwpColumnSchemaInfo, values []byte, rowCount int) qwpColumnLayout {
	return qwpColumnLayout{
		info:         info,
		values:       values,
		nonNullCount: rowCount,
	}
}

// buildNullableLayout produces a qwpColumnLayout with the given null
// pattern (true = NULL) and a dense values region assembled from the
// non-null rows of `rowBytes`. `rowBytes` must contain one entry per
// row (nil for NULL rows, fixed-size bytes for non-null).
func buildNullableLayout(info *qwpColumnSchemaInfo, rowBytes [][]byte) qwpColumnLayout {
	rowCount := len(rowBytes)
	bitmap := make([]byte, (rowCount+7)>>3)
	nonNullIdx := make([]int32, rowCount)
	var dense int32
	var values []byte
	for i, b := range rowBytes {
		if b == nil {
			bitmap[i>>3] |= 1 << (i & 7)
			nonNullIdx[i] = -1
		} else {
			nonNullIdx[i] = dense
			dense++
			values = append(values, b...)
		}
	}
	return qwpColumnLayout{
		info:         info,
		nullBitmap:   bitmap,
		nonNullIdx:   nonNullIdx,
		values:       values,
		nonNullCount: int(dense),
	}
}

// newSingleColumnBatch assembles a QwpColumnBatch with one column for
// tests that only care about a single accessor path.
func newSingleColumnBatch(info qwpColumnSchemaInfo, layout qwpColumnLayout, rowCount int) *QwpColumnBatch {
	return &QwpColumnBatch{
		requestId:   1,
		batchSeq:    0,
		rowCount:    rowCount,
		columnCount: 1,
		columns:     []qwpColumnSchemaInfo{info},
		layouts:     []qwpColumnLayout{layout},
	}
}

// --- Fixed-width accessor coverage ---

func TestQwpColumnBatchFixedWidth(t *testing.T) {
	t.Run("Bool_bitpacked", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "b", wireType: qwpTypeBoolean}
		// 10 rows, pattern: T F T F T F T F T F.
		// Packed: byte 0 bits 0..7 = 0b01010101 = 0x55, byte 1 bits 0..1 = 0b01 = 0x01.
		layout := buildFixedLayout(&info, []byte{0x55, 0x01}, 10)
		batch := newSingleColumnBatch(info, layout, 10)
		for i := 0; i < 10; i++ {
			want := i%2 == 0
			if got := batch.Bool(0, i); got != want {
				t.Fatalf("Bool(0, %d) = %v, want %v", i, got, want)
			}
		}
	})

	t.Run("Int8", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "b", wireType: qwpTypeByte}
		layout := buildFixedLayout(&info, []byte{0x01, 0xFF, 0x7F}, 3)
		batch := newSingleColumnBatch(info, layout, 3)
		if got := batch.Int8(0, 0); got != 1 {
			t.Fatalf("Int8(0, 0) = %d", got)
		}
		if got := batch.Int8(0, 1); got != -1 {
			t.Fatalf("Int8(0, 1) = %d", got)
		}
		if got := batch.Int8(0, 2); got != 127 {
			t.Fatalf("Int8(0, 2) = %d", got)
		}
	})

	t.Run("Int16", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "s", wireType: qwpTypeShort}
		values := make([]byte, 4)
		var negShort int16 = -1000
		binary.LittleEndian.PutUint16(values[0:], uint16(negShort))
		binary.LittleEndian.PutUint16(values[2:], 32767)
		layout := buildFixedLayout(&info, values, 2)
		batch := newSingleColumnBatch(info, layout, 2)
		if got := batch.Int16(0, 0); got != -1000 {
			t.Fatalf("Int16[0] = %d", got)
		}
		if got := batch.Int16(0, 1); got != 32767 {
			t.Fatalf("Int16[1] = %d", got)
		}
	})

	t.Run("Char", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "c", wireType: qwpTypeChar}
		values := make([]byte, 4)
		binary.LittleEndian.PutUint16(values[0:], 0x0041) // 'A'
		binary.LittleEndian.PutUint16(values[2:], 0x00E9) // 'é'
		layout := buildFixedLayout(&info, values, 2)
		batch := newSingleColumnBatch(info, layout, 2)
		if got := batch.Char(0, 0); got != 'A' {
			t.Fatalf("Char[0] = %c (%d)", got, got)
		}
		if got := batch.Char(0, 1); got != 'é' {
			t.Fatalf("Char[1] = %c (%d)", got, got)
		}
	})

	t.Run("Int32_and_IPv4", func(t *testing.T) {
		// INT and IPv4 share the 4-byte LE wire layout.
		values := make([]byte, 8)
		var negInt int32 = -42
		binary.LittleEndian.PutUint32(values[0:], uint32(negInt))
		binary.LittleEndian.PutUint32(values[4:], 0x7F_00_00_01) // 127.0.0.1 LE
		for _, wt := range []qwpTypeCode{qwpTypeInt, qwpTypeIPv4} {
			info := qwpColumnSchemaInfo{name: "i", wireType: wt}
			layout := buildFixedLayout(&info, values, 2)
			batch := newSingleColumnBatch(info, layout, 2)
			if got := batch.Int32(0, 0); got != -42 {
				t.Fatalf("Int32 (%#x) [0] = %d", wt, got)
			}
			if got := batch.Int32(0, 1); got != int32(0x7F_00_00_01) {
				t.Fatalf("Int32 (%#x) [1] = %#x", wt, got)
			}
		}
	})

	t.Run("Int64", func(t *testing.T) {
		// LONG, DATE, TIMESTAMP, TIMESTAMP_NANOS, DECIMAL64 all share
		// the int64 LE layout. Spot-check the dispatch through the
		// single accessor.
		values := make([]byte, 16)
		var negLong int64 = -1
		binary.LittleEndian.PutUint64(values[0:], uint64(negLong))
		binary.LittleEndian.PutUint64(values[8:], uint64(math.MaxInt64))
		for _, wt := range []qwpTypeCode{qwpTypeLong, qwpTypeDate, qwpTypeTimestamp, qwpTypeTimestampNano, qwpTypeDecimal64} {
			info := qwpColumnSchemaInfo{name: "l", wireType: wt}
			layout := buildFixedLayout(&info, values, 2)
			batch := newSingleColumnBatch(info, layout, 2)
			if got := batch.Int64(0, 0); got != -1 {
				t.Fatalf("Int64 (%#x) [0] = %d", wt, got)
			}
			if got := batch.Int64(0, 1); got != math.MaxInt64 {
				t.Fatalf("Int64 (%#x) [1] = %d", wt, got)
			}
		}
	})

	t.Run("Float32", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "f", wireType: qwpTypeFloat}
		values := make([]byte, 8)
		binary.LittleEndian.PutUint32(values[0:], math.Float32bits(3.14))
		binary.LittleEndian.PutUint32(values[4:], math.Float32bits(-0.5))
		layout := buildFixedLayout(&info, values, 2)
		batch := newSingleColumnBatch(info, layout, 2)
		if got := batch.Float32(0, 0); got != 3.14 {
			t.Fatalf("Float32[0] = %v", got)
		}
		if got := batch.Float32(0, 1); got != -0.5 {
			t.Fatalf("Float32[1] = %v", got)
		}
	})

	t.Run("Float64", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "d", wireType: qwpTypeDouble}
		values := make([]byte, 16)
		binary.LittleEndian.PutUint64(values[0:], math.Float64bits(1.3))
		binary.LittleEndian.PutUint64(values[8:], math.Float64bits(-2.5))
		layout := buildFixedLayout(&info, values, 2)
		batch := newSingleColumnBatch(info, layout, 2)
		if got := batch.Float64(0, 0); got != 1.3 {
			t.Fatalf("Float64[0] = %v", got)
		}
		if got := batch.Float64(0, 1); got != -2.5 {
			t.Fatalf("Float64[1] = %v", got)
		}
	})

	t.Run("Uuid", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "u", wireType: qwpTypeUuid}
		values := make([]byte, 16)
		binary.LittleEndian.PutUint64(values[0:], 0x0706050403020100)
		binary.LittleEndian.PutUint64(values[8:], 0x0F0E0D0C0B0A0908)
		layout := buildFixedLayout(&info, values, 1)
		batch := newSingleColumnBatch(info, layout, 1)
		if lo := batch.UuidLo(0, 0); lo != 0x0706050403020100 {
			t.Fatalf("UuidLo = %#x", lo)
		}
		if hi := batch.UuidHi(0, 0); hi != 0x0F0E0D0C0B0A0908 {
			t.Fatalf("UuidHi = %#x", hi)
		}
	})

	t.Run("Decimal128", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "d128", wireType: qwpTypeDecimal128}
		values := make([]byte, 16)
		binary.LittleEndian.PutUint64(values[0:], 0xAAAA_BBBB_CCCC_DDDD)
		binary.LittleEndian.PutUint64(values[8:], 0x1111_2222_3333_4444)
		layout := buildFixedLayout(&info, values, 1)
		layout.scale = 4
		batch := newSingleColumnBatch(info, layout, 1)
		if got := batch.Decimal128Lo(0, 0); uint64(got) != 0xAAAA_BBBB_CCCC_DDDD {
			t.Fatalf("Decimal128Lo = %#x", uint64(got))
		}
		if got := batch.Decimal128Hi(0, 0); uint64(got) != 0x1111_2222_3333_4444 {
			t.Fatalf("Decimal128Hi = %#x", uint64(got))
		}
		if s := batch.DecimalScale(0); s != 4 {
			t.Fatalf("DecimalScale = %d, want 4", s)
		}
	})

	t.Run("Long256_and_Decimal256", func(t *testing.T) {
		for _, wt := range []qwpTypeCode{qwpTypeLong256, qwpTypeDecimal256} {
			info := qwpColumnSchemaInfo{name: "l256", wireType: wt}
			values := make([]byte, 32)
			for i := 0; i < 4; i++ {
				binary.LittleEndian.PutUint64(values[i*8:], uint64(i+1)*0x1111111111111111)
			}
			layout := buildFixedLayout(&info, values, 1)
			batch := newSingleColumnBatch(info, layout, 1)
			for w := 0; w < 4; w++ {
				want := int64(uint64(w+1) * 0x1111111111111111)
				if got := batch.Long256Word(0, 0, w); got != want {
					t.Fatalf("%#x word %d = %#x", wt, w, got)
				}
			}
		}
	})
}

// --- Null handling ---

func TestQwpColumnBatchNullsDenseIndex(t *testing.T) {
	// Pattern N V V N V (rowCount=5, denseCount=3). Non-null values:
	// int32 values 100, 200, 300 at dense indices 0, 1, 2.
	info := qwpColumnSchemaInfo{name: "i", wireType: qwpTypeInt}
	values := make([]byte, 12)
	binary.LittleEndian.PutUint32(values[0:], 100)
	binary.LittleEndian.PutUint32(values[4:], 200)
	binary.LittleEndian.PutUint32(values[8:], 300)
	rowBytes := [][]byte{
		nil,        // row 0 NULL
		values[0:4],
		values[4:8],
		nil,        // row 3 NULL
		values[8:12],
	}
	layout := buildNullableLayout(&info, rowBytes)
	batch := newSingleColumnBatch(info, layout, 5)

	if !batch.IsNull(0, 0) || !batch.IsNull(0, 3) {
		t.Fatal("row 0 and 3 should be NULL")
	}
	if batch.IsNull(0, 1) || batch.IsNull(0, 2) || batch.IsNull(0, 4) {
		t.Fatal("non-null rows must not report as NULL")
	}
	want := []int32{0, 100, 200, 0, 300}
	for i, w := range want {
		if got := batch.Int32(0, i); got != w {
			t.Fatalf("Int32(0, %d) = %d, want %d", i, got, w)
		}
	}
	if c := batch.NonNullCount(0); c != 3 {
		t.Fatalf("NonNullCount = %d, want 3", c)
	}
}

func TestQwpColumnBatchNullableAllNulls(t *testing.T) {
	// Every row NULL: nonNullCount=0, every accessor returns zero.
	info := qwpColumnSchemaInfo{name: "x", wireType: qwpTypeLong}
	rowBytes := [][]byte{nil, nil, nil}
	layout := buildNullableLayout(&info, rowBytes)
	batch := newSingleColumnBatch(info, layout, 3)
	for i := 0; i < 3; i++ {
		if !batch.IsNull(0, i) {
			t.Fatalf("row %d should be NULL", i)
		}
		if v := batch.Int64(0, i); v != 0 {
			t.Fatalf("Int64(0, %d) = %d, want 0", i, v)
		}
	}
	if c := batch.NonNullCount(0); c != 0 {
		t.Fatalf("NonNullCount = %d, want 0", c)
	}
}

// --- Strings, varchars, binary ---

func buildStringLayout(info *qwpColumnSchemaInfo, values []string) qwpColumnLayout {
	// Offsets array: (len(values)+1) uint32 LE, then concatenated bytes.
	offsets := make([]byte, 4*(len(values)+1))
	var heap []byte
	var cur uint32
	for i, s := range values {
		binary.LittleEndian.PutUint32(offsets[i*4:], cur)
		heap = append(heap, s...)
		cur += uint32(len(s))
	}
	binary.LittleEndian.PutUint32(offsets[len(values)*4:], cur)
	return qwpColumnLayout{
		info:         info,
		values:       offsets,
		stringBytes:  heap,
		nonNullCount: len(values),
	}
}

func TestQwpColumnBatchStringsAndVarcharsAndBinary(t *testing.T) {
	for _, tc := range []struct {
		name string
		wt   qwpTypeCode
	}{
		{"VARCHAR", qwpTypeVarchar},
		{"BINARY", qwpTypeBinary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := qwpColumnSchemaInfo{name: "s", wireType: tc.wt}
			vals := []string{"", "hello", "日本語", "x"}
			layout := buildStringLayout(&info, vals)
			batch := newSingleColumnBatch(info, layout, len(vals))
			for i, v := range vals {
				var got []byte
				if tc.wt == qwpTypeBinary {
					got = batch.Binary(0, i)
				} else {
					got = batch.Str(0, i)
				}
				if !bytes.Equal(got, []byte(v)) {
					t.Fatalf("%s row %d: got %q, want %q", tc.name, i, got, v)
				}
			}
			// Two accessor calls return independent slice values
			// (different Go slice headers), even though they alias
			// the same backing bytes.
			if tc.wt == qwpTypeVarchar {
				a := batch.Str(0, 1)
				b := batch.Str(0, 2)
				if bytes.Equal(a, b) {
					t.Fatalf("independent views should differ: a=%q b=%q", a, b)
				}
			}
		})
	}
}

func TestQwpColumnBatchStringAllocatingHelper(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "s", wireType: qwpTypeVarchar}
	vals := []string{"hello", "", "world"}
	layout := buildStringLayout(&info, vals)
	batch := newSingleColumnBatch(info, layout, len(vals))
	if got := batch.String(0, 0); got != "hello" {
		t.Fatalf("String[0] = %q", got)
	}
	if got := batch.String(0, 2); got != "world" {
		t.Fatalf("String[2] = %q", got)
	}
}

// --- Symbol ---

func TestQwpColumnBatchSymbol(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "sy", wireType: qwpTypeSymbol}
	// Dict: ["alpha", "beta", "gamma"], one heap region with packed
	// (offset, length) entries.
	heap := []byte("alphabetagamma")
	entries := []qwpSymbolEntry{
		{offset: 0, length: 5},
		{offset: 5, length: 4},
		{offset: 9, length: 5},
	}
	dict := qwpSymbolDictView{heap: heap, entries: entries}

	// Four rows: alpha, beta, NULL, gamma.
	rowCount := 4
	bitmap := make([]byte, 1)
	bitmap[0] = 1 << 2 // row 2 NULL
	nonNullIdx := []int32{0, 1, -1, 2}
	symbolRowIds := []int32{0, 1, 0 /* stale, row is NULL */, 2}

	layout := qwpColumnLayout{
		info:         &info,
		nullBitmap:   bitmap,
		nonNullIdx:   nonNullIdx,
		nonNullCount: 3,
		symbolRowIds: symbolRowIds,
		symbolDict:   dict,
	}
	batch := newSingleColumnBatch(info, layout, rowCount)

	want := []string{"alpha", "beta", "", "gamma"}
	for i, w := range want {
		if got := batch.String(0, i); got != w {
			t.Fatalf("Symbol row %d: got %q, want %q", i, got, w)
		}
	}
	if !batch.IsNull(0, 2) {
		t.Fatalf("row 2 must be NULL")
	}
}

// --- Arrays ---

func TestQwpColumnBatchFloat64Array1D(t *testing.T) {
	// One row: 1D array [1.5, 2.5, 3.5].
	info := qwpColumnSchemaInfo{name: "a", wireType: qwpTypeDoubleArray}
	var buf bytes.Buffer
	buf.WriteByte(1) // nDims
	_ = binary.Write(&buf, binary.LittleEndian, int32(3))
	_ = binary.Write(&buf, binary.LittleEndian, 1.5)
	_ = binary.Write(&buf, binary.LittleEndian, 2.5)
	_ = binary.Write(&buf, binary.LittleEndian, 3.5)
	values := buf.Bytes()

	layout := qwpColumnLayout{
		info:          &info,
		values:        values,
		arrayRowStart: []int32{0},
		arrayElems:    []int32{3},
		nonNullCount:  1,
	}
	batch := newSingleColumnBatch(info, layout, 1)

	if n := batch.ArrayNDims(0, 0); n != 1 {
		t.Fatalf("ArrayNDims = %d", n)
	}
	if d := batch.ArrayDim(0, 0, 0); d != 3 {
		t.Fatalf("ArrayDim(0) = %d", d)
	}
	got := batch.Float64Array(0, 0)
	want := []float64{1.5, 2.5, 3.5}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Float64Array[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestQwpColumnBatchInt64Array2D(t *testing.T) {
	// One row: 2×3 array, row-major: [[1,2,3],[4,5,6]].
	info := qwpColumnSchemaInfo{name: "a", wireType: qwpTypeLongArray}
	var buf bytes.Buffer
	buf.WriteByte(2) // nDims
	_ = binary.Write(&buf, binary.LittleEndian, int32(2))
	_ = binary.Write(&buf, binary.LittleEndian, int32(3))
	for _, v := range []int64{1, 2, 3, 4, 5, 6} {
		_ = binary.Write(&buf, binary.LittleEndian, v)
	}
	values := buf.Bytes()

	layout := qwpColumnLayout{
		info:          &info,
		values:        values,
		arrayRowStart: []int32{0},
		arrayElems:    []int32{6},
		nonNullCount:  1,
	}
	batch := newSingleColumnBatch(info, layout, 1)

	if n := batch.ArrayNDims(0, 0); n != 2 {
		t.Fatalf("ArrayNDims = %d", n)
	}
	if d0, d1 := batch.ArrayDim(0, 0, 0), batch.ArrayDim(0, 0, 1); d0 != 2 || d1 != 3 {
		t.Fatalf("ArrayDim = %dx%d", d0, d1)
	}
	got := batch.Int64Array(0, 0)
	want := []int64{1, 2, 3, 4, 5, 6}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Int64Array[%d] = %d", i, got[i])
		}
	}
}

func TestQwpColumnBatchEmptyArrayViaZeroShape(t *testing.T) {
	// A non-null 1-D empty array is encoded as (nDims=1, dim0=0): 5
	// bytes of shape, 0 bytes of elements. Distinct from a NULL row
	// (null bitmap bit set, no inline bytes) — accessors should
	// report a real 1-D array with zero length.
	info := qwpColumnSchemaInfo{name: "a", wireType: qwpTypeDoubleArray}
	var buf bytes.Buffer
	buf.WriteByte(1) // nDims
	_ = binary.Write(&buf, binary.LittleEndian, int32(0))
	values := buf.Bytes()
	layout := qwpColumnLayout{
		info:          &info,
		values:        values,
		arrayRowStart: []int32{0},
		arrayElems:    []int32{0},
		nonNullCount:  1,
	}
	batch := newSingleColumnBatch(info, layout, 1)
	if n := batch.ArrayNDims(0, 0); n != 1 {
		t.Fatalf("ArrayNDims = %d, want 1", n)
	}
	if d := batch.ArrayDim(0, 0, 0); d != 0 {
		t.Fatalf("ArrayDim(0) = %d, want 0", d)
	}
	if got := batch.Float64Array(0, 0); len(got) != 0 {
		t.Fatalf("Float64Array len = %d, want 0", len(got))
	}
}

// TestQwpColumnFloat64ArrayInto exercises the append-into-dst variant
// of Float64Array: it must extend dst with the row's elements, leave
// dst unchanged on a NULL row, and reuse dst's backing array across
// successive calls (the hot-loop pattern this accessor exists for).
func TestQwpColumnFloat64ArrayInto(t *testing.T) {
	// Two non-null rows back-to-back: row 0 = [1.5, 2.5], row 1 = [3.5].
	info := qwpColumnSchemaInfo{name: "a", wireType: qwpTypeDoubleArray}
	var buf bytes.Buffer
	buf.WriteByte(1) // row 0 nDims
	_ = binary.Write(&buf, binary.LittleEndian, int32(2))
	_ = binary.Write(&buf, binary.LittleEndian, 1.5)
	_ = binary.Write(&buf, binary.LittleEndian, 2.5)
	row1Start := int32(buf.Len())
	buf.WriteByte(1) // row 1 nDims
	_ = binary.Write(&buf, binary.LittleEndian, int32(1))
	_ = binary.Write(&buf, binary.LittleEndian, 3.5)
	values := buf.Bytes()

	layout := qwpColumnLayout{
		info:          &info,
		values:        values,
		arrayRowStart: []int32{0, row1Start},
		arrayElems:    []int32{2, 1},
		nonNullCount:  2,
	}
	batch := newSingleColumnBatch(info, layout, 2)
	col := batch.Column(0)

	dst := make([]float64, 0, 8)
	dst = col.Float64ArrayInto(0, dst)
	if len(dst) != 2 || dst[0] != 1.5 || dst[1] != 2.5 {
		t.Fatalf("row 0 into dst = %v", dst)
	}
	// Append-style: a second call without truncating extends dst.
	dst = col.Float64ArrayInto(1, dst)
	if len(dst) != 3 || dst[2] != 3.5 {
		t.Fatalf("row 1 appended dst = %v", dst)
	}
	// Hot-loop pattern: truncate before each row to reuse the backing
	// array. Capacity must be preserved across the truncation.
	beforeCap := cap(dst)
	dst = dst[:0]
	dst = col.Float64ArrayInto(0, dst)
	if len(dst) != 2 || cap(dst) != beforeCap {
		t.Fatalf("reuse: len=%d cap=%d (was %d)", len(dst), cap(dst), beforeCap)
	}
}

// TestQwpColumnFloat64ArrayIntoNull verifies that a NULL row leaves
// dst unchanged (no zero-fill, no truncation) — distinct from the
// per-cell Float64Array which returns nil for NULL.
func TestQwpColumnFloat64ArrayIntoNull(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "a", wireType: qwpTypeDoubleArray}
	// Null bitmap has bit 0 set → row 0 is NULL.
	layout := qwpColumnLayout{
		info:          &info,
		values:        []byte{},
		arrayRowStart: []int32{0},
		arrayElems:    []int32{0},
		nullBitmap:    []byte{0x01},
		nonNullCount:  0,
	}
	batch := newSingleColumnBatch(info, layout, 1)
	col := batch.Column(0)

	dst := []float64{99.0, 99.0}
	got := col.Float64ArrayInto(0, dst)
	if len(got) != 2 || got[0] != 99.0 || got[1] != 99.0 {
		t.Fatalf("NULL row mutated dst = %v", got)
	}
}

// TestQwpColumnInt64ArrayInto mirrors the Float64ArrayInto test for
// LONG_ARRAY columns.
func TestQwpColumnInt64ArrayInto(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "a", wireType: qwpTypeLongArray}
	var buf bytes.Buffer
	buf.WriteByte(1)
	_ = binary.Write(&buf, binary.LittleEndian, int32(3))
	for _, v := range []int64{10, 20, 30} {
		_ = binary.Write(&buf, binary.LittleEndian, v)
	}
	values := buf.Bytes()

	layout := qwpColumnLayout{
		info:          &info,
		values:        values,
		arrayRowStart: []int32{0},
		arrayElems:    []int32{3},
		nonNullCount:  1,
	}
	batch := newSingleColumnBatch(info, layout, 1)
	col := batch.Column(0)

	dst := col.Int64ArrayInto(0, nil)
	want := []int64{10, 20, 30}
	if len(dst) != len(want) {
		t.Fatalf("Int64ArrayInto len = %d, want %d", len(dst), len(want))
	}
	for i, w := range want {
		if dst[i] != w {
			t.Fatalf("Int64ArrayInto[%d] = %d, want %d", i, dst[i], w)
		}
	}
}

// --- CopyAll ---

// TestQwpColumnBatchCopyAllSurvivesPoolReuse is the contract CopyAll
// exists to satisfy: a snapshot taken from batch N remains valid and
// correct after batch N's pool-owned layout slices are reused for
// batch N+1. The live batch aliases the decoder's layout pool, so
// without the copy the snapshot's nonNullIdx / symbolRowIds /
// timestampBuf entries would read batch N+1 data.
func TestQwpColumnBatchCopyAllSurvivesPoolReuse(t *testing.T) {
	// Build a nullable Int64 column so nonNullIdx is non-trivial and
	// we can observe it getting overwritten.
	info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	rowBytes := [][]byte{
		binary.LittleEndian.AppendUint64(nil, uint64(100)),
		nil, // NULL
		binary.LittleEndian.AppendUint64(nil, uint64(300)),
	}
	layout := buildNullableLayout(&info, rowBytes)
	batch := newSingleColumnBatch(info, layout, 3)

	snapshot := batch.CopyAll()

	// Simulate the decoder overwriting the pool-owned fields in place,
	// the same way qwpColumnLayout.clear() + parseNullSection would.
	for i := range batch.layouts[0].nonNullIdx {
		batch.layouts[0].nonNullIdx[i] = 0xBAD
	}
	batch.layouts[0].values = []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0}

	// Snapshot must still see the original values.
	if got := snapshot.Int64(0, 0); got != 100 {
		t.Fatalf("snapshot.Int64(0,0) = %d, want 100", got)
	}
	if !snapshot.IsNull(0, 1) {
		t.Fatal("snapshot row 1 should be NULL")
	}
	if got := snapshot.Int64(0, 2); got != 300 {
		t.Fatalf("snapshot.Int64(0,2) = %d, want 300", got)
	}
	if snapshot.RowCount() != 3 || snapshot.ColumnCount() != 1 {
		t.Fatalf("snapshot row/col count = (%d, %d), want (3, 1)",
			snapshot.RowCount(), snapshot.ColumnCount())
	}
	if snapshot.ColumnName(0) != "v" {
		t.Fatalf("snapshot column name = %q", snapshot.ColumnName(0))
	}
}

// TestQwpColumnBatchCopyAllGorillaTimestampSurvivesPoolReuse covers
// the Gorilla-TIMESTAMP corner of CopyAll. For Gorilla-encoded
// columns the decoder sets layout.values to alias layout.timestampBuf
// (see parseTimestamp), so the snapshot must re-point values at the
// CLONED timestampBuf. Without that re-point, decoding a second frame
// into the same QwpColumnBatch overwrites the source's timestampBuf
// in place and the snapshot's Int64 accessor starts reading batch
// N+1 values.
func TestQwpColumnBatchCopyAllGorillaTimestampSurvivesPoolReuse(t *testing.T) {
	// Small, regular DoDs push the encoder onto the Gorilla path;
	// nonNullCount >= 3 is required for Gorilla (parseTimestamp
	// rejects otherwise).
	orig := []int64{1_000_000, 1_000_100, 1_000_200, 1_000_310, 1_000_520}
	origRows := make([]func(*qwpColumnBuffer), len(orig))
	for i, v := range orig {
		v := v
		origRows[i] = func(c *qwpColumnBuffer) { c.addLong(v) }
	}
	frame1 := encodeSingleColumnBatch(t, "ts", qwpTypeTimestamp, false, origRows)

	// A second batch whose values are nowhere near the first, so a
	// stale alias produces obviously-wrong reads rather than
	// coincidentally-matching values.
	fresh := []int64{5_000_000, 5_000_999, 5_001_888, 5_002_555, 5_003_333}
	freshRows := make([]func(*qwpColumnBuffer), len(fresh))
	for i, v := range fresh {
		v := v
		freshRows[i] = func(c *qwpColumnBuffer) { c.addLong(v) }
	}
	frame2 := encodeSingleColumnBatch(t, "ts", qwpTypeTimestamp, false, freshRows)

	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frame1, &batch); err != nil {
		t.Fatalf("decode 1: %v", err)
	}
	// Precondition: the first decode must actually have taken the
	// Gorilla path. If encoder heuristics change and this falls back
	// to the uncompressed branch, the test no longer covers the bug.
	if len(batch.layouts[0].timestampBuf) == 0 {
		t.Fatal("test precondition: expected Gorilla path to populate timestampBuf")
	}

	snapshot := batch.CopyAll()

	// Decode a second frame into the SAME batch. The decoder reuses
	// batch.layouts[0].timestampBuf in place, so the source's backing
	// array is now clobbered.
	if err := dec.decode(frame2, &batch); err != nil {
		t.Fatalf("decode 2: %v", err)
	}

	for i, w := range orig {
		if got := snapshot.Int64(0, i); got != w {
			t.Fatalf("snapshot.Int64(0, %d) = %d, want %d", i, got, w)
		}
	}
}

// TestQwpColumnBatchCopyAllRawSurvivesPayloadReuse covers the raw
// (non-zstd) sibling of TestQwpColumnBatchCopyAllZstdSurvivesPoolReuse.
// The egress I/O loop reads each WS frame into a buffer borrowed from
// qwpEgressIO.readBufPool; on the raw path the decoded batch's column
// slices (values, stringBytes, nullBitmap) alias that pooled buffer
// directly. releaseBuffer returns the buffer to the pool, and the next
// inbound frame is decoded into the same backing array in place. A
// CopyAll result the caller retained from the released batch must
// remain valid across that recycle — i.e. CopyAll must deep-clone the
// payload bytes on the raw path the same way it already does on the
// zstd path.
//
// Reproduces the in-place clobber without touching the I/O loop:
// allocate one backing array, write frame 1 into it, hand the slice to
// the decoder, snapshot, then overwrite the array's bytes with frame 2.
// snapshot.Int64 reads its values from the same backing array the
// decoder aliased; without the fix the post-clobber read returns the
// frame-2 little-endian word at that offset, not the original.
func TestQwpColumnBatchCopyAllRawSurvivesPayloadReuse(t *testing.T) {
	frame1 := encodeSingleColumnBatch(t, "v", qwpTypeLong, false,
		[]func(*qwpColumnBuffer){
			func(c *qwpColumnBuffer) { c.addLong(111) },
			func(c *qwpColumnBuffer) { c.addLong(222) },
		})
	frame2 := encodeSingleColumnBatch(t, "v", qwpTypeLong, false,
		[]func(*qwpColumnBuffer){
			func(c *qwpColumnBuffer) { c.addLong(-9999) },
			func(c *qwpColumnBuffer) { c.addLong(-8888) },
		})
	if len(frame2) < len(frame1) {
		t.Fatalf("test precondition: frame2 (%d) must be >= frame1 (%d) so the clobber overlaps the column data", len(frame2), len(frame1))
	}

	// One backing array that stands in for a recycled readBufPool
	// buffer: it holds frame1 first, then the next frame is read into
	// the same memory in place.
	pooled := make([]byte, len(frame2))
	copy(pooled, frame1)
	payload := pooled[:len(frame1)]

	dec := newTestQueryDecoder()
	var b QwpColumnBatch
	if err := dec.decode(payload, &b); err != nil {
		t.Fatalf("decode 1: %v", err)
	}
	if len(b.zstdScratch) != 0 {
		t.Fatalf("test precondition: expected raw (non-zstd) path; zstdScratch=%d", len(b.zstdScratch))
	}

	snapshot := b.CopyAll()
	if got := snapshot.Int64(0, 0); got != 111 {
		t.Fatalf("pre-clobber snapshot.Int64(0,0) = %d, want 111", got)
	}
	if got := snapshot.Int64(0, 1); got != 222 {
		t.Fatalf("pre-clobber snapshot.Int64(0,1) = %d, want 222", got)
	}

	// Recycle: the I/O loop hands the buffer back to readBufPool and
	// the reader's qwpReadFrameInto writes the next frame into the
	// same backing array. Simulate that with a copy().
	copy(pooled, frame2)

	// Snapshot must still report frame-1 values.
	if got := snapshot.Int64(0, 0); got != 111 {
		t.Fatalf("post-clobber snapshot.Int64(0,0) = %d, want 111 (CopyAll didn't clone the raw payload)", got)
	}
	if got := snapshot.Int64(0, 1); got != 222 {
		t.Fatalf("post-clobber snapshot.Int64(0,1) = %d, want 222 (CopyAll didn't clone the raw payload)", got)
	}
}

// TestQwpGeohashAccessorWithNulls verifies the Geohash accessors on both
// the batch and the cached-column surface at a sub-8-byte precision with
// interleaved nulls. Precision 12 packs 2 bytes per non-null value, so the
// dense stride is 2, not 8, and the nulls route every read through the
// null-bitmap / denseIndex mapping. A wrong (Int64-style *8) stride would
// index the wrong cell or run past the dense region; this pins the
// precision-sized stride the accessor must use.
func TestQwpGeohashAccessorWithNulls(t *testing.T) {
	const prec = 12
	type row struct {
		val  uint64
		null bool
	}
	rows := []row{
		{0xABC, false},
		{0, true},
		{0x123, false},
		{0, true},
		{0xFFF, false},
	}
	tb := newQwpTableBuffer("t")
	for _, r := range rows {
		col, err := tb.getOrCreateColumn("g", qwpTypeGeohash, true)
		if err != nil {
			t.Fatalf("getOrCreateColumn: %v", err)
		}
		if r.null {
			col.addNull()
		} else if err := col.addGeohash(r.val, prec); err != nil {
			t.Fatalf("addGeohash: %v", err)
		}
		tb.commitRow()
	}

	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)
	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got := batch.GeohashPrecisionBits(0); got != prec {
		t.Fatalf("GeohashPrecisionBits = %d, want %d", got, prec)
	}
	col := batch.Column(0)
	for i, r := range rows {
		if got := batch.IsNull(0, i); got != r.null {
			t.Errorf("IsNull(0,%d) = %v, want %v", i, got, r.null)
		}
		want := r.val
		if r.null {
			want = 0 // NULL rows read as 0 on both surfaces.
		}
		if got := batch.Geohash(0, i); got != want {
			t.Errorf("batch.Geohash(0,%d) = %#x, want %#x", i, got, want)
		}
		if got := col.Geohash(i); got != want {
			t.Errorf("col.Geohash(%d) = %#x, want %#x", i, got, want)
		}
	}
}

// buildDecimalGeohashFrame produces a one-row RESULT_BATCH frame with
// a DECIMAL64 column (given scale) and a GEOHASH column (given precision
// bits). The decoder reads the per-batch scale / precision off the DATA
// section and stores them on qwpColumnLayout, which is what the race
// test below observes concurrently.
func buildDecimalGeohashFrame(t *testing.T, scale uint32, precision int8, unscaled int64) []byte {
	t.Helper()
	tb := newQwpTableBuffer("t")
	dcol, err := tb.getOrCreateColumn("d", qwpTypeDecimal64, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn d: %v", err)
	}
	if err := dcol.addDecimal(NewDecimalFromInt64(unscaled, scale)); err != nil {
		t.Fatalf("addDecimal: %v", err)
	}
	gcol, err := tb.getOrCreateColumn("g", qwpTypeGeohash, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn g: %v", err)
	}
	if err := gcol.addGeohash(uint64(unscaled), precision); err != nil {
		t.Fatalf("addGeohash: %v", err)
	}
	tb.commitRow()
	var enc qwpEncoder
	ingress := enc.encodeTable(tb)
	return wrapAsResultBatch(ingress, 1, 0)
}

// TestQwpColumnBatchCopyAllScaleAndPrecisionAreRaceFree exercises the
// concurrency invariant that commit 58e1915 ("Fix data race on decimal
// scale and geohash precision") added: a held CopyAll snapshot
// must be safe to read while the decoder writes the next batch's scale
// / precision into the source QwpColumnBatch.
//
// Before that fix both fields lived on the connection-scoped
// qwpColumnSchemaInfo, which the decoder mutated per batch and which
// every snapshot aliased via layouts[i].info — so this test paired
// with `go test -race` flagged the write/read overlap. Post-fix the
// fields are on qwpColumnLayout and CopyAll takes value copies, so the
// snapshot's accessors read memory the decoder never touches again.
//
// Without -race this test is still meaningful: a snapshot must keep
// its frame-A values even after frame B is decoded into the source
// batch.
func TestQwpColumnBatchCopyAllScaleAndPrecisionAreRaceFree(t *testing.T) {
	frameA := buildDecimalGeohashFrame(t, 2, 20, 12345)
	frameB := buildDecimalGeohashFrame(t, 7, 40, 99999)

	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frameA, &batch); err != nil {
		t.Fatalf("decode A: %v", err)
	}
	if s := batch.DecimalScale(0); s != 2 {
		t.Fatalf("A scale = %d, want 2", s)
	}
	if p := batch.GeohashPrecisionBits(1); p != 20 {
		t.Fatalf("A precision = %d, want 20", p)
	}

	snapshot := batch.CopyAll()

	const readers = 4
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if s := snapshot.DecimalScale(0); s != 2 {
					t.Errorf("snapshot.DecimalScale = %d, want 2", s)
					return
				}
				if p := snapshot.GeohashPrecisionBits(1); p != 20 {
					t.Errorf("snapshot.GeohashPrecisionBits = %d, want 20", p)
					return
				}
			}
		}()
	}

	// Repeatedly re-decode frame B into the same batch. Each decode
	// writes frame-B scale / precision into the layout; -race catches
	// any overlap with the readers above.
	for i := 0; i < 200; i++ {
		if err := dec.decode(frameB, &batch); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("decode B [%d]: %v", i, err)
		}
		if s := batch.DecimalScale(0); s != 7 {
			close(stop)
			wg.Wait()
			t.Fatalf("live batch scale = %d, want 7", s)
		}
		if p := batch.GeohashPrecisionBits(1); p != 40 {
			close(stop)
			wg.Wait()
			t.Fatalf("live batch precision = %d, want 40", p)
		}
	}

	close(stop)
	wg.Wait()
}

// --- Column handle ---

// TestQwpColumnHandleMirrorsBatchAccessors asserts the captured column
// handle returns the same values as the batch-level (col, row)
// accessors for every fixed-width type, including NULL rows.
func TestQwpColumnHandleMirrorsBatchAccessors(t *testing.T) {
	// Nullable Int64 column: 5 rows (V N V V N), values 100/300/400.
	intInfo := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	rowBytes := [][]byte{
		binary.LittleEndian.AppendUint64(nil, 100),
		nil,
		binary.LittleEndian.AppendUint64(nil, 300),
		binary.LittleEndian.AppendUint64(nil, 400),
		nil,
	}
	intLayout := buildNullableLayout(&intInfo, rowBytes)

	// VARCHAR column: 3 rows, no nulls.
	strInfo := qwpColumnSchemaInfo{name: "s", wireType: qwpTypeVarchar}
	strLayout := buildStringLayout(&strInfo, []string{"foo", "bar", "baz"})

	// Build a two-column batch manually (same rowCount across columns
	// isn't a hard invariant here — the string accessor only indexes
	// into its own column's values/offsets).
	batch := &QwpColumnBatch{
		requestId:   1,
		rowCount:    5,
		columnCount: 2,
		columns:     []qwpColumnSchemaInfo{intInfo, strInfo},
		layouts:     []qwpColumnLayout{intLayout, strLayout},
	}

	icol := batch.Column(0)
	if icol.Name() != "v" {
		t.Fatalf("Name = %q", icol.Name())
	}
	if icol.Type() != byte(qwpTypeLong) {
		t.Fatalf("Type = %#x", icol.Type())
	}
	if icol.RowCount() != 5 {
		t.Fatalf("RowCount = %d", icol.RowCount())
	}
	if icol.NonNullCount() != 3 {
		t.Fatalf("NonNullCount = %d", icol.NonNullCount())
	}
	if !icol.HasNulls() {
		t.Fatal("HasNulls should be true for nullable column")
	}
	for row := 0; row < 5; row++ {
		if icol.IsNull(row) != batch.IsNull(0, row) {
			t.Fatalf("IsNull mismatch at %d", row)
		}
		if icol.Int64(row) != batch.Int64(0, row) {
			t.Fatalf("Int64 mismatch at %d: col=%d batch=%d",
				row, icol.Int64(row), batch.Int64(0, row))
		}
	}

	scol := batch.Column(1)
	if scol.HasNulls() {
		t.Fatal("HasNulls should be false for non-nullable column")
	}
	for row, want := range []string{"foo", "bar", "baz"} {
		if got := scol.String(row); got != want {
			t.Fatalf("String(%d) = %q, want %q", row, got, want)
		}
		if !bytes.Equal(scol.Str(row), []byte(want)) {
			t.Fatalf("Str(%d) mismatch", row)
		}
	}
}

// --- Bulk range accessors ---

func TestQwpColumnRangeNoNulls(t *testing.T) {
	intInfo := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	// 6 rows of 8 bytes, values 10..60 step 10.
	values := make([]byte, 48)
	for i := 0; i < 6; i++ {
		binary.LittleEndian.PutUint64(values[i*8:], uint64((i+1)*10))
	}
	layout := buildFixedLayout(&intInfo, values, 6)
	batch := newSingleColumnBatch(intInfo, layout, 6)

	col := batch.Column(0)
	got := col.Int64Range(1, 5, nil)
	want := []int64{20, 30, 40, 50}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("Int64Range[%d] = %d, want %d", i, got[i], w)
		}
	}

	// Empty / reversed ranges return dst unchanged.
	if out := col.Int64Range(3, 3, []int64{7}); len(out) != 1 || out[0] != 7 {
		t.Fatalf("empty range altered dst: %v", out)
	}
	if out := col.Int64Range(5, 2, nil); len(out) != 0 {
		t.Fatalf("reversed range should return empty, got %v", out)
	}

	// Append into a prealloc'd buffer: no realloc should happen.
	dst := make([]int64, 0, 6)
	dst = col.Int64Range(0, 6, dst)
	if cap(dst) != 6 {
		t.Fatalf("cap grew unexpectedly: %d", cap(dst))
	}
	for i, w := range []int64{10, 20, 30, 40, 50, 60} {
		if dst[i] != w {
			t.Fatalf("full range [%d] = %d, want %d", i, dst[i], w)
		}
	}
}

func TestQwpColumnInt64RangeWithNulls(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	rowBytes := [][]byte{
		binary.LittleEndian.AppendUint64(nil, 100),
		nil,
		binary.LittleEndian.AppendUint64(nil, 300),
		binary.LittleEndian.AppendUint64(nil, 400),
		nil,
	}
	layout := buildNullableLayout(&info, rowBytes)
	batch := newSingleColumnBatch(info, layout, 5)

	col := batch.Column(0)
	dst := col.Int64Range(0, 5, nil)
	// NULL rows become 0 (matching the per-cell Int64 accessor).
	want := []int64{100, 0, 300, 400, 0}
	for i, w := range want {
		if dst[i] != w {
			t.Fatalf("Int64Range[%d] = %d, want %d", i, dst[i], w)
		}
	}
}

func TestQwpColumnFloat64Range(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "d", wireType: qwpTypeDouble}
	values := make([]byte, 24)
	binary.LittleEndian.PutUint64(values[0:], math.Float64bits(1.1))
	binary.LittleEndian.PutUint64(values[8:], math.Float64bits(2.2))
	binary.LittleEndian.PutUint64(values[16:], math.Float64bits(3.3))
	layout := buildFixedLayout(&info, values, 3)
	batch := newSingleColumnBatch(info, layout, 3)

	col := batch.Column(0)
	dst := col.Float64Range(0, 3, nil)
	want := []float64{1.1, 2.2, 3.3}
	for i, w := range want {
		if dst[i] != w {
			t.Fatalf("Float64Range[%d] = %v, want %v", i, dst[i], w)
		}
	}
}

func TestQwpColumnInt32Range(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "i", wireType: qwpTypeInt}
	values := make([]byte, 16)
	for i := 0; i < 4; i++ {
		binary.LittleEndian.PutUint32(values[i*4:], uint32(i*111))
	}
	layout := buildFixedLayout(&info, values, 4)
	batch := newSingleColumnBatch(info, layout, 4)

	col := batch.Column(0)
	dst := col.Int32Range(1, 4, nil)
	want := []int32{111, 222, 333}
	for i, w := range want {
		if dst[i] != w {
			t.Fatalf("Int32Range[%d] = %d, want %d", i, dst[i], w)
		}
	}
}

func TestQwpColumnFloat32Range(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "f", wireType: qwpTypeFloat}
	values := make([]byte, 12)
	binary.LittleEndian.PutUint32(values[0:], math.Float32bits(1.5))
	binary.LittleEndian.PutUint32(values[4:], math.Float32bits(-2.5))
	binary.LittleEndian.PutUint32(values[8:], math.Float32bits(3.25))
	layout := buildFixedLayout(&info, values, 3)
	batch := newSingleColumnBatch(info, layout, 3)

	col := batch.Column(0)
	dst := col.Float32Range(0, 3, nil)
	want := []float32{1.5, -2.5, 3.25}
	for i, w := range want {
		if dst[i] != w {
			t.Fatalf("Float32Range[%d] = %v, want %v", i, dst[i], w)
		}
	}
}

// TestQwpColumnRangeOOBPanicsInNoNullsPath pins the safety contract
// of the no-nulls fast path: misuse with toRow > rowCount must panic
// the same way the per-cell accessor does, instead of silently reading
// past the values buffer via unsafe.Slice.
func TestQwpColumnRangeOOBPanicsInNoNullsPath(t *testing.T) {
	cases := []struct {
		name     string
		wireType qwpTypeCode
		rowBytes int
		run      func(col QwpColumn)
	}{
		{"Int64Range", qwpTypeLong, 8, func(col QwpColumn) { col.Int64Range(0, 5, nil) }},
		{"Float64Range", qwpTypeDouble, 8, func(col QwpColumn) { col.Float64Range(0, 5, nil) }},
		{"Int32Range", qwpTypeInt, 4, func(col QwpColumn) { col.Int32Range(0, 5, nil) }},
		{"Float32Range", qwpTypeFloat, 4, func(col QwpColumn) { col.Float32Range(0, 5, nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info := qwpColumnSchemaInfo{name: "v", wireType: tc.wireType}
			values := make([]byte, 2*tc.rowBytes) // exactly 2 rows wide
			layout := buildFixedLayout(&info, values, 2)
			batch := newSingleColumnBatch(info, layout, 2)
			col := batch.Column(0)

			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("%s: expected panic for toRow > rowCount, got none", tc.name)
				}
			}()
			tc.run(col)
		})
	}
}

// TestQwpColumnRangeZeroAllocWhenPrealloc asserts Range accessors
// don't allocate when dst has sufficient capacity — the intended usage
// pattern for steady-state row sweeps.
func TestQwpColumnRangeZeroAllocWhenPrealloc(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	values := make([]byte, 8*100)
	for i := 0; i < 100; i++ {
		binary.LittleEndian.PutUint64(values[i*8:], uint64(i))
	}
	layout := buildFixedLayout(&info, values, 100)
	batch := newSingleColumnBatch(info, layout, 100)

	col := batch.Column(0)
	buf := make([]int64, 0, 100)
	allocs := testing.AllocsPerRun(100, func() {
		buf = buf[:0]
		buf = col.Int64Range(0, 100, buf)
	})
	if allocs != 0 {
		t.Fatalf("Int64Range with prealloc dst allocated %v/run, want 0", allocs)
	}
}

// --- Zero-alloc contract ---

func TestQwpColumnBatchZeroAlloc(t *testing.T) {
	// The Int64, Float64, and Str accessors must not allocate on the
	// hot path. Str allocates only when crossing into String (the
	// materialising helper) — we exclude that here.
	intInfo := qwpColumnSchemaInfo{name: "i", wireType: qwpTypeLong}
	intValues := make([]byte, 8)
	binary.LittleEndian.PutUint64(intValues, 42)
	intLayout := buildFixedLayout(&intInfo, intValues, 1)

	strInfo := qwpColumnSchemaInfo{name: "s", wireType: qwpTypeVarchar}
	strLayout := buildStringLayout(&strInfo, []string{"hello"})

	batch := &QwpColumnBatch{
		requestId:   1,
		rowCount:    1,
		columnCount: 2,
		columns:     []qwpColumnSchemaInfo{intInfo, strInfo},
		layouts:     []qwpColumnLayout{intLayout, strLayout},
	}

	allocs := testing.AllocsPerRun(100, func() {
		_ = batch.Int64(0, 0)
		_ = batch.Str(1, 0)
		_ = batch.IsNull(0, 0)
		_ = batch.NonNullCount(0)
	})
	if allocs != 0 {
		t.Fatalf("hot-path accessors allocated %v times/run, want 0", allocs)
	}
}

// --- Mis-typed / out-of-bounds accessor contract ---

// assertPanics runs fn, fails if it does not panic, and (when wantSubstr
// is non-empty) fails if the recovered value's string form does not
// contain wantSubstr. Pinning the substring stops a test from passing on
// an unrelated panic (e.g. a nil deref) instead of the guard it targets.
func assertPanics(t *testing.T, wantSubstr string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, got none", wantSubstr)
		}
		if msg := fmt.Sprintf("%v", r); wantSubstr != "" && !strings.Contains(msg, wantSubstr) {
			t.Fatalf("panic %q does not contain %q", msg, wantSubstr)
		}
	}()
	fn()
}

// TestQwpColumnRangeTypeMismatchPanics pins the *Range width guard: a
// Range accessor on a column whose wire type is not the matching fixed
// width panics with a typed message (carrying the column name + wire
// type) rather than the opaque slice-bounds panic the unguarded memmove
// path produced — most visibly Int64Range on a bit-packed BOOLEAN,
// whose dense region is far shorter than toRow*8.
func TestQwpColumnRangeTypeMismatchPanics(t *testing.T) {
	mkCol := func(wt qwpTypeCode, rows int) QwpColumn {
		info := qwpColumnSchemaInfo{name: "c", wireType: wt}
		// 8 bytes/row of backing storage regardless of the column's real
		// width, so a too-narrow read could *silently* succeed without
		// the guard. That proves it is the guard, not an incidental OOB,
		// that fires.
		layout := buildFixedLayout(&info, make([]byte, rows*8), rows)
		return newSingleColumnBatch(info, layout, rows).Column(0)
	}
	for _, tc := range []struct {
		name string
		wt   qwpTypeCode
		run  func(c QwpColumn)
	}{
		{"Int64Range/BOOLEAN", qwpTypeBoolean, func(c QwpColumn) { c.Int64Range(0, 4, nil) }},
		{"Int64Range/INT", qwpTypeInt, func(c QwpColumn) { c.Int64Range(0, 4, nil) }},
		{"Int64Range/SYMBOL", qwpTypeSymbol, func(c QwpColumn) { c.Int64Range(0, 4, nil) }},
		{"Float64Range/FLOAT", qwpTypeFloat, func(c QwpColumn) { c.Float64Range(0, 4, nil) }},
		{"Int32Range/LONG", qwpTypeLong, func(c QwpColumn) { c.Int32Range(0, 4, nil) }},
		{"Int32Range/BOOLEAN", qwpTypeBoolean, func(c QwpColumn) { c.Int32Range(0, 4, nil) }},
		{"Float32Range/DOUBLE", qwpTypeDouble, func(c QwpColumn) { c.Float32Range(0, 4, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := mkCol(tc.wt, 4)
			assertPanics(t, "fixed-width", func() { tc.run(c) })
		})
	}
}

// TestQwpColumnRangeSameWidthReinterpretAllowed pins the deliberately
// permitted case: a Range accessor on a different type of the SAME
// element width passes the guard and reinterprets the raw bits, so the
// documented "numeric noise" contract for Int64Range on a DOUBLE column
// still holds.
func TestQwpColumnRangeSameWidthReinterpretAllowed(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "d", wireType: qwpTypeDouble}
	values := make([]byte, 16)
	binary.LittleEndian.PutUint64(values[0:], math.Float64bits(1.5))
	binary.LittleEndian.PutUint64(values[8:], math.Float64bits(2.5))
	layout := buildFixedLayout(&info, values, 2)
	col := newSingleColumnBatch(info, layout, 2).Column(0)

	got := col.Int64Range(0, 2, nil) // 8-byte DOUBLE read as int64: allowed
	if len(got) != 2 ||
		uint64(got[0]) != math.Float64bits(1.5) ||
		uint64(got[1]) != math.Float64bits(2.5) {
		t.Fatalf("Int64Range on DOUBLE = %v, want raw float64 bits", got)
	}
}

// TestQwpArrayAccessorsOnNonArrayPanic pins the array-type guard: every
// array accessor, on both the QwpColumnBatch and QwpColumn surfaces,
// panics with a typed message when the column is not an array — instead
// of the opaque "index out of range [n] with length 0" from indexing
// the empty arrayRowStart side table.
func TestQwpArrayAccessorsOnNonArrayPanic(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	values := make([]byte, 8)
	binary.LittleEndian.PutUint64(values, 42)
	layout := buildFixedLayout(&info, values, 1)
	batch := newSingleColumnBatch(info, layout, 1)
	col := batch.Column(0)

	for _, tc := range []struct {
		name string
		run  func()
	}{
		{"batch.Float64Array", func() { batch.Float64Array(0, 0) }},
		{"batch.Int64Array", func() { batch.Int64Array(0, 0) }},
		{"batch.ArrayNDims", func() { batch.ArrayNDims(0, 0) }},
		{"batch.ArrayDim", func() { batch.ArrayDim(0, 0, 0) }},
		{"col.Float64Array", func() { col.Float64Array(0) }},
		{"col.Int64Array", func() { col.Int64Array(0) }},
		{"col.ArrayNDims", func() { col.ArrayNDims(0) }},
		{"col.ArrayDim", func() { col.ArrayDim(0, 0) }},
		{"col.Float64ArrayInto", func() { col.Float64ArrayInto(0, nil) }},
		{"col.Int64ArrayInto", func() { col.Int64ArrayInto(0, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPanics(t, "not an array type", tc.run)
		})
	}
}

// TestQwpArrayElementTypeReinterpretAllowed pins the permitted same-width
// reinterpretation across the two array element types: the guard checks
// "is an array", not "is THIS array type", so Int64Array on a
// DOUBLE_ARRAY column decodes the 8-byte elements as raw int64 bits
// rather than panicking — the array analogue of the *Range reinterpret.
func TestQwpArrayElementTypeReinterpretAllowed(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "a", wireType: qwpTypeDoubleArray}
	var buf bytes.Buffer
	buf.WriteByte(1) // nDims
	_ = binary.Write(&buf, binary.LittleEndian, int32(2))
	_ = binary.Write(&buf, binary.LittleEndian, 1.5)
	_ = binary.Write(&buf, binary.LittleEndian, 2.5)
	layout := qwpColumnLayout{
		info:          &info,
		values:        buf.Bytes(),
		arrayRowStart: []int32{0},
		arrayElems:    []int32{2},
		nonNullCount:  1,
	}
	batch := newSingleColumnBatch(info, layout, 1)
	got := batch.Int64Array(0, 0) // must not panic
	if len(got) != 2 ||
		uint64(got[0]) != math.Float64bits(1.5) ||
		uint64(got[1]) != math.Float64bits(2.5) {
		t.Fatalf("Int64Array on DOUBLE_ARRAY = %v, want raw float64 bits", got)
	}
}

// TestQwpColumnBatchPerCellMistypeAndOOB characterises the per-cell
// fixed-width accessors under misuse — the behavior the package
// documents as "undefined" but which must never silently read out of
// bounds. Two regimes: a same-width mis-type reinterprets the bytes (no
// panic); a too-narrow column or an out-of-range row slices past the
// dense values region and surfaces Go's bounds-check panic rather than
// returning adjacent memory. Pinned so a future "optimisation" to
// unsafe per-cell indexing that drops the bounds check is caught.
func TestQwpColumnBatchPerCellMistypeAndOOB(t *testing.T) {
	t.Run("same_width_reinterpret_no_panic", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "d", wireType: qwpTypeDouble}
		values := make([]byte, 8)
		binary.LittleEndian.PutUint64(values, math.Float64bits(1.5))
		layout := buildFixedLayout(&info, values, 1)
		batch := newSingleColumnBatch(info, layout, 1)
		if got := batch.Int64(0, 0); uint64(got) != math.Float64bits(1.5) {
			t.Fatalf("Int64 on DOUBLE = %#x, want float64 bits %#x",
				uint64(got), math.Float64bits(1.5))
		}
	})

	t.Run("too_narrow_type_panics", func(t *testing.T) {
		// BYTE column: 1 byte/value, so an 8-byte Int64 read slices past
		// the 2-byte dense region.
		info := qwpColumnSchemaInfo{name: "b", wireType: qwpTypeByte}
		layout := buildFixedLayout(&info, []byte{0x01, 0x02}, 2)
		batch := newSingleColumnBatch(info, layout, 2)
		assertPanics(t, "", func() { _ = batch.Int64(0, 0) })
	})

	t.Run("oob_row_no_nulls_panics", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
		layout := buildFixedLayout(&info, make([]byte, 8), 1)
		batch := newSingleColumnBatch(info, layout, 1)
		assertPanics(t, "", func() { _ = batch.Int64(0, 5) })
	})

	t.Run("oob_row_nullable_panics", func(t *testing.T) {
		info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
		rowBytes := [][]byte{
			binary.LittleEndian.AppendUint64(nil, 100),
			nil,
		}
		layout := buildNullableLayout(&info, rowBytes)
		batch := newSingleColumnBatch(info, layout, 2)
		assertPanics(t, "", func() { _ = batch.Int64(0, 99) })
	})
}

// --- CopyAll: symbol dict + array metadata ---

// TestQwpColumnBatchCopyAllSymbolSurvivesPoolReuse covers the SYMBOL
// corner of CopyAll. A snapshot must keep resolving its rows to the
// right strings after the decoder (a) reuses the batch's pool-owned
// symbolRowIds for the next frame and (b) append-grows the
// connection-scoped dict. CopyAll clones symbolRowIds and snapshots the
// append-only dict view, so both survive.
func TestQwpColumnBatchCopyAllSymbolSurvivesPoolReuse(t *testing.T) {
	globalDict := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	// frame1 (batch_seq 0): rows alpha,beta,alpha (ids 0,1,0); advertises
	// dict ids 0..1.
	tb1 := newQwpTableBuffer("t")
	for _, id := range []int32{0, 1, 0} {
		col, _ := tb1.getOrCreateColumn("s", qwpTypeSymbol, false)
		col.addSymbolID(id)
		tb1.commitRow()
	}
	var enc qwpEncoder
	frame1 := wrapAsResultBatch(enc.encodeTableWithDeltaDict(tb1, globalDict, -1, 1), 1, 0)

	// frame2 (continuation, batch_seq 1): rows beta,epsilon (ids 1,4).
	// Row 0's id differs from frame1's, so the snapshot reading "alpha"
	// at row 0 proves it walks its own cloned symbolRowIds rather than
	// the reused pool slice. Advertising ids 2..4 append-grows the dict
	// heap past frame1's frozen prefix.
	tb2 := newQwpTableBuffer("t")
	for _, id := range []int32{1, 4} {
		col, _ := tb2.getOrCreateColumn("s", qwpTypeSymbol, false)
		col.addSymbolID(id)
		tb2.commitRow()
	}
	frame2 := wrapAsResultBatch(enc.encodeTableWithDeltaDict(tb2, globalDict, 1, 4), 1, 1)

	dec := newTestQueryDecoder()
	var b QwpColumnBatch
	if err := dec.decode(frame1, &b); err != nil {
		t.Fatalf("decode 1: %v", err)
	}
	want := []string{"alpha", "beta", "alpha"}
	for i, w := range want {
		if got := b.String(0, i); got != w {
			t.Fatalf("live batch1 row %d = %q, want %q", i, got, w)
		}
	}

	snapshot := b.CopyAll()

	// Decode the continuation into the SAME batch: reuses b's pool-owned
	// symbolRowIds in place and append-extends the decoder's dict.
	if err := dec.decode(frame2, &b); err != nil {
		t.Fatalf("decode 2: %v", err)
	}
	if got := b.String(0, 0); got != "beta" {
		t.Fatalf("live batch2 row 0 = %q, want %q", got, "beta")
	}
	if got := b.String(0, 1); got != "epsilon" {
		t.Fatalf("live batch2 row 1 = %q, want %q", got, "epsilon")
	}

	// Snapshot must still resolve frame1's per-row symbols.
	for i, w := range want {
		if got := snapshot.String(0, i); got != w {
			t.Fatalf("snapshot row %d = %q, want %q (CopyAll didn't snapshot symbol state)", i, got, w)
		}
	}
}

// TestQwpColumnBatchCopyAllSymbolCopiesOutReferencedBytes asserts the
// memory-independence half of CopyAll's contract for SYMBOL columns. A
// live symbolDict view aliases the decoder's connection-scoped heap,
// which append-grows up to 256 MiB; re-sharing its slice headers would
// pin the whole heap behind a retained snapshot. CopyAll instead
// copies out only the entries the column's rows reference. Proven three
// ways: the snapshot heap does not alias the source heap, it holds only
// the referenced bytes, and clobbering the source heap leaves the
// snapshot intact.
func TestQwpColumnBatchCopyAllSymbolCopiesOutReferencedBytes(t *testing.T) {
	info := qwpColumnSchemaInfo{name: "sy", wireType: qwpTypeSymbol}
	// Connection-scoped dict of five symbols; the column references two.
	heap := []byte("alphabetagammadeltaepsilon")
	entries := []qwpSymbolEntry{
		{offset: 0, length: 5},  // alpha
		{offset: 5, length: 4},  // beta
		{offset: 9, length: 5},  // gamma
		{offset: 14, length: 5}, // delta
		{offset: 19, length: 7}, // epsilon
	}
	dict := qwpSymbolDictView{heap: heap, entries: entries}

	// Rows: gamma, alpha, NULL, gamma. Distinct referenced = {alpha,
	// gamma} = 10 bytes / 2 entries, a strict subset of the 26-byte,
	// 5-entry connection dict. Row 0 and row 3 share an id so the remap
	// must collapse them; row 1 is a different id.
	rowCount := 4
	bitmap := make([]byte, 1)
	bitmap[0] = 1 << 2 // row 2 NULL
	nonNullIdx := []int32{0, 1, -1, 2}
	symbolRowIds := []int32{2, 0, 2 /* stale, row is NULL */, 2}

	layout := qwpColumnLayout{
		info:         &info,
		nullBitmap:   bitmap,
		nonNullIdx:   nonNullIdx,
		nonNullCount: 3,
		symbolRowIds: symbolRowIds,
		symbolDict:   dict,
	}
	batch := newSingleColumnBatch(info, layout, rowCount)

	snapshot := batch.CopyAll()
	snapDict := snapshot.layouts[0].symbolDict

	// (1) The snapshot heap is a private allocation, not a view into the
	// connection-scoped heap.
	if aliases(snapDict.heap, heap) {
		t.Fatalf("snapshot symbol heap aliases the connection heap; CopyAll must copy out referenced bytes")
	}
	// (2) Only the referenced symbols were copied out: alpha(5)+gamma(5)
	// = 10 bytes across 2 entries, not the full 26-byte / 5-entry dict.
	if len(snapDict.heap) != 10 {
		t.Fatalf("snapshot heap = %d bytes, want 10 (only referenced alpha+gamma)", len(snapDict.heap))
	}
	if len(snapDict.entries) != 2 {
		t.Fatalf("snapshot entries = %d, want 2 (distinct referenced symbols)", len(snapDict.entries))
	}

	// (3) Clobber the source connection heap; a correct copy is wholly
	// independent of it.
	for i := range heap {
		heap[i] = 'X'
	}
	want := []string{"gamma", "alpha", "", "gamma"}
	for i, w := range want {
		if got := snapshot.String(0, i); got != w {
			t.Fatalf("snapshot row %d = %q, want %q (CopyAll didn't copy out symbol bytes)", i, got, w)
		}
	}
	if !snapshot.IsNull(0, 2) {
		t.Fatalf("snapshot row 2 must remain NULL")
	}
}

// TestQwpColumnBatchCopyAllArraySurvivesPoolReuse covers the ARRAY
// corner of CopyAll: a snapshot must keep its shape + elements after the
// decoder reuses the batch for the next frame. That clobbers two kinds
// of state at once — the pool-owned arrayRowStart / arrayElems side
// tables (overwritten in place) and the array bytes in `values` (which
// alias the recycled payload buffer). CopyAll clones the side tables and
// rebinds values onto a private payload clone.
func TestQwpColumnBatchCopyAllArraySurvivesPoolReuse(t *testing.T) {
	// frame1: two 1-D DOUBLE_ARRAY rows of different lengths, so the
	// arrayRowStart / arrayElems side tables carry distinct per-row values.
	frame1 := encodeSingleColumnBatch(t, "a", qwpTypeDoubleArray, false,
		[]func(*qwpColumnBuffer){
			func(c *qwpColumnBuffer) { c.addDoubleArray(1, []int32{3}, []float64{1.5, 2.5, 3.5}) },
			func(c *qwpColumnBuffer) { c.addDoubleArray(1, []int32{2}, []float64{4.5, 5.5}) },
		})
	// frame2: different shapes and a larger byte footprint, so writing it
	// into the recycled buffer fully overwrites frame1's bytes and the
	// re-decode rewrites arrayRowStart / arrayElems with new values.
	frame2 := encodeSingleColumnBatch(t, "a", qwpTypeDoubleArray, false,
		[]func(*qwpColumnBuffer){
			func(c *qwpColumnBuffer) { c.addDoubleArray(1, []int32{5}, []float64{-1, -2, -3, -4, -5}) },
			func(c *qwpColumnBuffer) { c.addDoubleArray(1, []int32{4}, []float64{-6, -7, -8, -9}) },
		})
	if len(frame2) < len(frame1) {
		t.Fatalf("precondition: frame2 (%d) must be >= frame1 (%d)", len(frame2), len(frame1))
	}

	// One backing array recycled across two decodes, standing in for the
	// egress I/O loop's readBufPool buffer.
	pooled := make([]byte, len(frame2))
	copy(pooled, frame1)

	dec := newTestQueryDecoder()
	var b QwpColumnBatch
	if err := dec.decode(pooled[:len(frame1)], &b); err != nil {
		t.Fatalf("decode 1: %v", err)
	}
	if len(b.zstdScratch) != 0 {
		t.Fatalf("precondition: expected raw (non-zstd) path; zstdScratch=%d", len(b.zstdScratch))
	}

	snapshot := b.CopyAll()

	assertArrayRow := func(label string, row int, wantDim int, want []float64) {
		t.Helper()
		if n := snapshot.ArrayNDims(0, row); n != 1 {
			t.Fatalf("%s: snapshot ArrayNDims(row %d) = %d, want 1", label, row, n)
		}
		if d := snapshot.ArrayDim(0, row, 0); d != wantDim {
			t.Fatalf("%s: snapshot ArrayDim(row %d) = %d, want %d", label, row, d, wantDim)
		}
		got := snapshot.Float64Array(0, row)
		if len(got) != len(want) {
			t.Fatalf("%s: snapshot Float64Array(row %d) len = %d, want %d", label, row, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: snapshot Float64Array(row %d)[%d] = %v, want %v", label, row, i, got[i], want[i])
			}
		}
	}

	assertArrayRow("pre-clobber", 0, 3, []float64{1.5, 2.5, 3.5})
	assertArrayRow("pre-clobber", 1, 2, []float64{4.5, 5.5})

	// Recycle the buffer and re-decode into the SAME batch: overwrites the
	// payload bytes the live batch aliased and rewrites its arrayRowStart
	// / arrayElems in place.
	copy(pooled, frame2)
	if err := dec.decode(pooled[:len(frame2)], &b); err != nil {
		t.Fatalf("decode 2: %v", err)
	}
	if d := b.ArrayDim(0, 0, 0); d != 5 {
		t.Fatalf("live batch2 ArrayDim(row 0) = %d, want 5", d)
	}

	// The snapshot keeps frame1's shape + elements.
	assertArrayRow("post-clobber", 0, 3, []float64{1.5, 2.5, 3.5})
	assertArrayRow("post-clobber", 1, 2, []float64{4.5, 5.5})
}
