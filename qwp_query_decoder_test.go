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
	"errors"
	"math"
	"strings"
	"testing"
)

// --- Test helpers ---

// wrapAsResultBatch takes an ingress-style frame (header + delta-dict
// + table block, as produced by qwpEncoder.encodeTable) and splices in
// the egress-style prelude (msg_kind + request_id + batch_seq) between
// the header and the delta dict. The header's payloadLength field and
// tableCount are preserved; only the bytes between are rewritten.
//
// Ingress layout:
//
//	[12 header][deltaDict][tableBlock]
//
// Egress RESULT_BATCH layout:
//
//	[12 header][msg_kind:1][requestId:8][batchSeq:varint][deltaDict][tableBlock]
//
// payload length must be patched to the new body length.
func wrapAsResultBatch(ingress []byte, requestId int64, batchSeq uint64) []byte {
	if len(ingress) < qwpHeaderSize {
		panic("ingress frame too short to wrap")
	}
	header := ingress[:qwpHeaderSize]
	body := ingress[qwpHeaderSize:]

	var prelude bytes.Buffer
	prelude.WriteByte(byte(qwpMsgKindResultBatch))
	var reqBuf [8]byte
	binary.LittleEndian.PutUint64(reqBuf[:], uint64(requestId))
	prelude.Write(reqBuf[:])
	varBuf := make([]byte, qwpMaxVarintLen)
	n := qwpPutVarint(varBuf, batchSeq)
	prelude.Write(varBuf[:n])

	out := make([]byte, 0, qwpHeaderSize+prelude.Len()+len(body))
	out = append(out, header...)
	out = append(out, prelude.Bytes()...)
	out = append(out, body...)
	// Patch payload length (offset 8..12).
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
	return out
}

// encodeSingleColumnBatch is a convenience that builds a one-column
// table, populates it via the supplied per-row callbacks, and wraps
// the output as a RESULT_BATCH frame. Each entry in `rows` is called
// for one row; the helper calls tb.commitRow() after each.
func encodeSingleColumnBatch(
	t *testing.T,
	name string,
	typeCode qwpTypeCode,
	nullable bool,
	rows []func(col *qwpColumnBuffer),
) []byte {
	t.Helper()
	tb := newQwpTableBuffer("t")
	for _, populate := range rows {
		col, err := tb.getOrCreateColumn(name, typeCode, nullable)
		if err != nil {
			t.Fatalf("getOrCreateColumn: %v", err)
		}
		populate(col)
		tb.commitRow()
	}
	var enc qwpEncoder
	ingress := enc.encodeTable(tb, qwpSchemaModeFull, 0)
	return wrapAsResultBatch(ingress, 1, 0)
}

// --- Positive-path round trips (driven by the real encoder) ---

func TestQwpDecoderRoundTripFixedWidth(t *testing.T) {
	type testCase struct {
		name  string
		wt    qwpTypeCode
		rows  []func(col *qwpColumnBuffer)
		check func(t *testing.T, b *QwpColumnBatch)
	}
	cases := []testCase{
		{
			name: "LONG", wt: qwpTypeLong,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addLong(1) },
				func(c *qwpColumnBuffer) { c.addLong(-2) },
				func(c *qwpColumnBuffer) { c.addLong(math.MaxInt64) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				for i, w := range []int64{1, -2, math.MaxInt64} {
					if got := b.Int64(0, i); got != w {
						t.Fatalf("Int64[%d] = %d, want %d", i, got, w)
					}
				}
			},
		},
		{
			name: "DOUBLE", wt: qwpTypeDouble,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addDouble(1.3) },
				func(c *qwpColumnBuffer) { c.addDouble(-2.5) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				for i, w := range []float64{1.3, -2.5} {
					if got := b.Float64(0, i); got != w {
						t.Fatalf("Float64[%d] = %v, want %v", i, got, w)
					}
				}
			},
		},
		{
			name: "INT", wt: qwpTypeInt,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addInt32(-7) },
				func(c *qwpColumnBuffer) { c.addInt32(100_000) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				for i, w := range []int32{-7, 100_000} {
					if got := b.Int32(0, i); got != w {
						t.Fatalf("Int32[%d] = %d, want %d", i, got, w)
					}
				}
			},
		},
		{
			name: "BOOLEAN", wt: qwpTypeBoolean,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addBool(true) },
				func(c *qwpColumnBuffer) { c.addBool(false) },
				func(c *qwpColumnBuffer) { c.addBool(true) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				for i, w := range []bool{true, false, true} {
					if got := b.Bool(0, i); got != w {
						t.Fatalf("Bool[%d] = %v, want %v", i, got, w)
					}
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			frame := encodeSingleColumnBatch(t, "c", c.wt, false, c.rows)
			var dec qwpQueryDecoder
			var batch QwpColumnBatch
			if err := dec.decode(frame, &batch); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if batch.RowCount() != len(c.rows) {
				t.Fatalf("RowCount = %d, want %d", batch.RowCount(), len(c.rows))
			}
			c.check(t, &batch)
		})
	}
}

func TestQwpDecoderRoundTripNullable(t *testing.T) {
	// Long column with pattern V N V N V (3 non-null, 2 null).
	frame := encodeSingleColumnBatch(t, "l", qwpTypeLong, true, []func(*qwpColumnBuffer){
		func(c *qwpColumnBuffer) { c.addLong(10) },
		func(c *qwpColumnBuffer) { c.addNull() },
		func(c *qwpColumnBuffer) { c.addLong(20) },
		func(c *qwpColumnBuffer) { c.addNull() },
		func(c *qwpColumnBuffer) { c.addLong(30) },
	})
	var dec qwpQueryDecoder
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if batch.RowCount() != 5 {
		t.Fatalf("RowCount = %d", batch.RowCount())
	}
	for i, want := range []int64{10, 0, 20, 0, 30} {
		if got := batch.Int64(0, i); got != want {
			t.Fatalf("Int64[%d] = %d, want %d (null=%v)", i, got, want, batch.IsNull(0, i))
		}
	}
	for _, i := range []int{1, 3} {
		if !batch.IsNull(0, i) {
			t.Fatalf("row %d should be NULL", i)
		}
	}
	if batch.NonNullCount(0) != 3 {
		t.Fatalf("NonNullCount = %d", batch.NonNullCount(0))
	}
}

func TestQwpDecoderRoundTripVarcharAndBinary(t *testing.T) {
	// Go encoder supports VARCHAR via addString; BINARY is read-only
	// from the server side and has no encoder path in this client, so
	// the VARCHAR test exercises the shared offsets + bytes layout
	// used by both types.
	for _, wt := range []qwpTypeCode{qwpTypeVarchar} {
		t.Run(typeCodeName(wt), func(t *testing.T) {
			frame := encodeSingleColumnBatch(t, "v", wt, false, []func(*qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addString("") },
				func(c *qwpColumnBuffer) { c.addString("hello") },
				func(c *qwpColumnBuffer) { c.addString("日本語") },
				func(c *qwpColumnBuffer) { c.addString("x") },
			})
			var dec qwpQueryDecoder
			var batch QwpColumnBatch
			if err := dec.decode(frame, &batch); err != nil {
				t.Fatalf("decode: %v", err)
			}
			want := []string{"", "hello", "日本語", "x"}
			for i, w := range want {
				if got := batch.String(0, i); got != w {
					t.Fatalf("String[%d] = %q, want %q", i, got, w)
				}
			}
		})
	}
}

func TestQwpDecoderRoundTripTimestampGorilla(t *testing.T) {
	// >3 timestamps with small DoDs → encoder picks the Gorilla path.
	values := []int64{1_000_000, 1_000_100, 1_000_200, 1_000_310, 1_000_520}
	rows := make([]func(*qwpColumnBuffer), len(values))
	for i, v := range values {
		v := v
		rows[i] = func(c *qwpColumnBuffer) { c.addLong(v) }
	}
	frame := encodeSingleColumnBatch(t, "ts", qwpTypeTimestamp, false, rows)
	var dec qwpQueryDecoder
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []int64{1_000_000, 1_000_100, 1_000_200, 1_000_310, 1_000_520}
	for i, w := range want {
		if got := batch.Int64(0, i); got != w {
			t.Fatalf("Int64[%d] = %d, want %d", i, got, w)
		}
	}
}

func TestQwpDecoderRoundTripTimestampUncompressed(t *testing.T) {
	// <= 2 timestamps force the encoder's uncompressed branch even
	// with FLAG_GORILLA set.
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	col.addLong(42)
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
	col.addLong(43)
	tb.commitRow()
	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb, qwpSchemaModeFull, 0), 1, 0)

	var dec qwpQueryDecoder
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := batch.Int64(0, 0); got != 42 {
		t.Fatalf("Int64[0] = %d", got)
	}
	if got := batch.Int64(0, 1); got != 43 {
		t.Fatalf("Int64[1] = %d", got)
	}
}

func TestQwpDecoderRoundTripGeohash(t *testing.T) {
	for _, prec := range []int{8, 40, 60} {
		t.Run("prec_"+itoa(prec), func(t *testing.T) {
			tb := newQwpTableBuffer("t")
			// A handful of valid geohash bit patterns. Constrain to
			// the requested precision by masking to the low `prec`
			// bits; higher bits aren't meaningful on the wire.
			mask := uint64(1)<<uint(prec) - 1
			values := []uint64{0x1, 0x1234, 0xDEADBEEF, 0x0102030405060708 & mask}
			for _, v := range values {
				col, err := tb.getOrCreateColumn("g", qwpTypeGeohash, false)
				if err != nil {
					t.Fatalf("getOrCreateColumn: %v", err)
				}
				if err := col.addGeohash(v&mask, int8(prec)); err != nil {
					t.Fatalf("addGeohash: %v", err)
				}
				tb.commitRow()
			}
			var enc qwpEncoder
			frame := wrapAsResultBatch(enc.encodeTable(tb, qwpSchemaModeFull, 0), 1, 0)

			var dec qwpQueryDecoder
			var batch QwpColumnBatch
			if err := dec.decode(frame, &batch); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got := batch.GeohashPrecisionBits(0); got != prec {
				t.Fatalf("precision = %d, want %d", got, prec)
			}
			// Reconstruct the packed uint64 per row from the wire's
			// ceil(prec/8) bytes.
			l := &batch.layouts[0]
			bytesPerValue := (prec + 7) / 8
			for i, want := range values {
				want &= mask
				start := i * bytesPerValue
				var got uint64
				for b := 0; b < bytesPerValue; b++ {
					got |= uint64(l.values[start+b]) << (8 * b)
				}
				if got != want {
					t.Fatalf("geohash[%d] = %#x, want %#x", i, got, want)
				}
			}
		})
	}
}

func TestQwpDecoderRoundTripFloat64Array(t *testing.T) {
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("a", qwpTypeDoubleArray, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	// 2x3 double array.
	col.addDoubleArray(2, []int32{2, 3}, []float64{1, 2, 3, 4, 5, 6})
	tb.commitRow()
	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb, qwpSchemaModeFull, 0), 1, 0)

	var dec qwpQueryDecoder
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if batch.ArrayNDims(0, 0) != 2 {
		t.Fatalf("ArrayNDims = %d", batch.ArrayNDims(0, 0))
	}
	if d0, d1 := batch.ArrayDim(0, 0, 0), batch.ArrayDim(0, 0, 1); d0 != 2 || d1 != 3 {
		t.Fatalf("ArrayDim = %dx%d", d0, d1)
	}
	got := batch.Float64Array(0, 0)
	want := []float64{1, 2, 3, 4, 5, 6}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("elem[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestQwpDecoderRoundTripArrayNullSentinel(t *testing.T) {
	// Non-nullable DOUBLE_ARRAY column with an interleaved null row.
	// The encoder emits the 1-byte nDims=0 NULL sentinel for that row
	// and the decoder must report it as NULL through IsNull and the
	// array accessors.
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("a", qwpTypeDoubleArray, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	col.addDoubleArray(1, []int32{2}, []float64{1.5, 2.5})
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("a", qwpTypeDoubleArray, false)
	col.addNull()
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("a", qwpTypeDoubleArray, false)
	col.addDoubleArray(1, []int32{1}, []float64{3.5})
	tb.commitRow()

	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb, qwpSchemaModeFull, 0), 1, 0)

	var dec qwpQueryDecoder
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if batch.IsNull(0, 0) {
		t.Fatalf("row 0 should be non-null")
	}
	if !batch.IsNull(0, 1) {
		t.Fatalf("row 1 should be NULL (nDims=0 sentinel)")
	}
	if batch.IsNull(0, 2) {
		t.Fatalf("row 2 should be non-null")
	}
	if got := batch.ArrayNDims(0, 1); got != 0 {
		t.Fatalf("ArrayNDims(0, 1) = %d, want 0", got)
	}
	if got := batch.Float64Array(0, 1); got != nil {
		t.Fatalf("Float64Array(0, 1) = %v, want nil", got)
	}
	if got := batch.Float64Array(0, 0); len(got) != 2 || got[0] != 1.5 || got[1] != 2.5 {
		t.Fatalf("Float64Array(0, 0) = %v, want [1.5 2.5]", got)
	}
	if got := batch.Float64Array(0, 2); len(got) != 1 || got[0] != 3.5 {
		t.Fatalf("Float64Array(0, 2) = %v, want [3.5]", got)
	}
}

func TestQwpDecoderRoundTripSymbolDelta(t *testing.T) {
	// Batch 1 introduces three symbols; Batch 2 adds one more via a
	// delta section. The decoder's connection-scoped dict must grow
	// across batches, and SYMBOL accessors in both batches must read
	// through the same dict heap.
	globalDict := []string{"AAPL", "MSFT", "GOOG", "TSLA"}

	tb1 := newQwpTableBuffer("t")
	for _, id := range []int32{0, 1, 2, 0} { // AAPL, MSFT, GOOG, AAPL
		col, _ := tb1.getOrCreateColumn("s", qwpTypeSymbol, false)
		col.addSymbolID(id)
		tb1.commitRow()
	}
	var enc qwpEncoder
	// maxSentId=-1 (no symbols sent), batchMaxId=2 → delta advertises
	// ids 0..2.
	ingress1 := enc.encodeTableWithDeltaDict(tb1, globalDict, -1, 2, qwpSchemaModeFull, 0)
	frame1 := wrapAsResultBatch(ingress1, 1, 0)

	tb2 := newQwpTableBuffer("t")
	for _, id := range []int32{3, 1} { // TSLA, MSFT
		col, _ := tb2.getOrCreateColumn("s", qwpTypeSymbol, false)
		col.addSymbolID(id)
		tb2.commitRow()
	}
	// maxSentId=2, batchMaxId=3 → delta advertises id 3 only.
	ingress2 := enc.encodeTableWithDeltaDict(tb2, globalDict, 2, 3, qwpSchemaModeReference, 0)
	frame2 := wrapAsResultBatch(ingress2, 1, 1)

	var dec qwpQueryDecoder
	var b1, b2 QwpColumnBatch
	if err := dec.decode(frame1, &b1); err != nil {
		t.Fatalf("decode frame1: %v", err)
	}
	want1 := []string{"AAPL", "MSFT", "GOOG", "AAPL"}
	for i, w := range want1 {
		if got := b1.String(0, i); got != w {
			t.Fatalf("batch1 row %d = %q, want %q", i, got, w)
		}
	}
	if err := dec.decode(frame2, &b2); err != nil {
		t.Fatalf("decode frame2: %v", err)
	}
	want2 := []string{"TSLA", "MSFT"}
	for i, w := range want2 {
		if got := b2.String(0, i); got != w {
			t.Fatalf("batch2 row %d = %q, want %q", i, got, w)
		}
	}
}

func TestQwpDecoderSchemaModeReference(t *testing.T) {
	// Batch 1 registers schema id 7 (full). Batch 2 references it.
	tb1 := newQwpTableBuffer("t")
	for _, v := range []int64{1, 2} {
		col, _ := tb1.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(v)
		tb1.commitRow()
	}
	var enc qwpEncoder
	frame1 := wrapAsResultBatch(enc.encodeTable(tb1, qwpSchemaModeFull, 7), 1, 0)

	tb2 := newQwpTableBuffer("t")
	col2, _ := tb2.getOrCreateColumn("a", qwpTypeLong, false)
	col2.addLong(10)
	tb2.commitRow()
	frame2 := wrapAsResultBatch(enc.encodeTable(tb2, qwpSchemaModeReference, 7), 1, 1)

	var dec qwpQueryDecoder
	var batch QwpColumnBatch
	if err := dec.decode(frame1, &batch); err != nil {
		t.Fatalf("decode frame1: %v", err)
	}
	if err := dec.decode(frame2, &batch); err != nil {
		t.Fatalf("decode frame2: %v", err)
	}
	if batch.ColumnName(0) != "a" {
		t.Fatalf("reference-mode batch lost column name: %q", batch.ColumnName(0))
	}
	if got := batch.Int64(0, 0); got != 10 {
		t.Fatalf("Int64[0] (frame2) = %d, want 10", got)
	}
}

// --- Hardening tests (ports of QwpResultBatchDecoderHardeningTest) ---

// writeMinimalResultBatch builds a minimal valid RESULT_BATCH frame
// with 0 rows and 0 columns. The schemaId is written as a plain varint
// from the given value. Matches QwpResultBatchDecoderHardeningTest.
// writeMinimalResultBatch.
func writeMinimalResultBatch(schemaId uint64) []byte {
	var buf bytes.Buffer
	// Header
	_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
	buf.WriteByte(qwpVersion)
	buf.WriteByte(0) // flags
	// tableCount = 1, payloadLength placeholder
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
	// Body
	buf.WriteByte(byte(qwpMsgKindResultBatch))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(1)) // requestId
	putVarintBytes(&buf, 0)                                 // batch_seq
	putVarintBytes(&buf, 0)                                 // name_len
	putVarintBytes(&buf, 0)                                 // row_count
	putVarintBytes(&buf, 0)                                 // column_count
	buf.WriteByte(byte(qwpSchemaModeFull))
	putVarintBytes(&buf, schemaId)
	// Patch payloadLength at offset 8.
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
	return out
}

// writeMinimalResultBatchWithRawSchemaIdVarint writes the fixed
// prelude, then injects a raw varint byte sequence for the schema_id.
func writeMinimalResultBatchWithRawSchemaIdVarint(schemaIdVarint []byte) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
	buf.WriteByte(qwpVersion)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
	buf.WriteByte(byte(qwpMsgKindResultBatch))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
	putVarintBytes(&buf, 0)
	putVarintBytes(&buf, 0)
	putVarintBytes(&buf, 0)
	putVarintBytes(&buf, 0)
	buf.WriteByte(byte(qwpSchemaModeFull))
	buf.Write(schemaIdVarint)
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
	return out
}

// writeMinimalResultBatchWithRawNameLenVarint injects a raw varint
// byte sequence for the table name length (the first varint after the
// batch_seq).
func writeMinimalResultBatchWithRawNameLenVarint(nameLenVarint []byte) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
	buf.WriteByte(qwpVersion)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
	buf.WriteByte(byte(qwpMsgKindResultBatch))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
	putVarintBytes(&buf, 0)
	buf.Write(nameLenVarint)
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
	return out
}

// writeStringResultBatch builds a RESULT_BATCH with one VARCHAR column,
// nonNull rows, and the given totalBytes value stamped into
// offsets[nonNull]. Used by the negative-totalBytes regression.
func writeStringResultBatch(nonNull int, totalBytes int32) []byte {
	var buf bytes.Buffer
	// Header
	_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
	buf.WriteByte(qwpVersion)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
	// Body
	buf.WriteByte(byte(qwpMsgKindResultBatch))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(7))
	putVarintBytes(&buf, 0)                          // batch_seq
	putVarintBytes(&buf, 0)                          // table_name_len
	putVarintBytes(&buf, uint64(nonNull))            // row_count
	putVarintBytes(&buf, 1)                          // column_count
	buf.WriteByte(byte(qwpSchemaModeFull))
	putVarintBytes(&buf, 0) // schema_id
	// Schema: column "s" : VARCHAR (egress may send STRING 0x08 but
	// the encoder-side tests use VARCHAR so the shared offsets+bytes
	// layout is exercised).
	putVarintBytes(&buf, 1)
	buf.WriteByte('s')
	buf.WriteByte(byte(qwpTypeVarchar))
	// Column body: null_flag = 0 (no nulls).
	buf.WriteByte(0)
	// Offsets: nonNull zeros, then totalBytes.
	for i := 0; i < nonNull; i++ {
		_ = binary.Write(&buf, binary.LittleEndian, uint32(i*5))
	}
	_ = binary.Write(&buf, binary.LittleEndian, uint32(totalBytes))
	// 5 bytes "hello" for the success case; the rejection case must
	// error before reading these.
	buf.WriteString("hello")
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
	return out
}

func putVarintBytes(buf *bytes.Buffer, v uint64) {
	tmp := make([]byte, qwpMaxVarintLen)
	n := qwpPutVarint(tmp, v)
	buf.Write(tmp[:n])
}

// itoa: base-10 int → string, without pulling in strconv at package
// level for test-only use.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestQwpDecoderHardening(t *testing.T) {
	t.Run("H1_PayloadTooShort", func(t *testing.T) {
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(make([]byte, 5), &b)
		assertDecodeErrContains(t, err, "too short")
	})

	t.Run("H2_BadMagic", func(t *testing.T) {
		buf := writeMinimalResultBatch(0)
		buf[0] = 0xFF
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "bad magic")
	})

	t.Run("H3_UnsupportedVersion", func(t *testing.T) {
		buf := writeMinimalResultBatch(0)
		buf[4] = 0x02
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "unsupported version")
	})

	t.Run("H4_UnexpectedMsgKind", func(t *testing.T) {
		buf := writeMinimalResultBatch(0)
		// msg_kind is the first byte after the 12-byte header.
		buf[qwpHeaderSize] = 0x00
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "expected RESULT_BATCH")
	})

	t.Run("H6_TableNameLengthOverflowVarint", func(t *testing.T) {
		// 10-byte varint with bit 63 set on byte 10.
		buf := writeMinimalResultBatchWithRawNameLenVarint([]byte{
			0x80, 0x80, 0x80, 0x80, 0x80,
			0x80, 0x80, 0x80, 0x80, 0x01,
		})
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		// Both phrasings acceptable — we fail at varintInt63 with
		// "exceeds int63" or at the table-name cap with "out of
		// range".
		if err == nil || (!containsAny(err.Error(), []string{"int63", "table name length", "out of range"})) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("H7_RowCountOutOfRange", func(t *testing.T) {
		// Craft a frame whose row_count exceeds qwpMaxRowsPerBatch.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0) // batch_seq
		putVarintBytes(&buf, 0) // name_len
		putVarintBytes(&buf, uint64(qwpMaxRowsPerBatch+1))
		putVarintBytes(&buf, 0)
		buf.WriteByte(0)
		putVarintBytes(&buf, 0)
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "row_count")
	})

	t.Run("H11_HugeSchemaId", func(t *testing.T) {
		buf := writeMinimalResultBatch(1_000_000_000)
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "schema_id")
	})

	t.Run("H12_NegativeSchemaIdVarint", func(t *testing.T) {
		// 5-byte varint encoding 0x80000000 (Integer.MIN_VALUE after
		// cast). Verbatim port of the Java regression.
		buf := writeMinimalResultBatchWithRawSchemaIdVarint([]byte{
			0x80, 0x80, 0x80, 0x80, 0x08,
		})
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "schema_id")
	})

	t.Run("H13_ReferenceUnknownId", func(t *testing.T) {
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0) // batch_seq
		putVarintBytes(&buf, 0) // name_len
		putVarintBytes(&buf, 0) // row_count
		putVarintBytes(&buf, 0) // column_count
		buf.WriteByte(byte(qwpSchemaModeReference))
		putVarintBytes(&buf, 42) // unknown id
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "not registered")
	})

	t.Run("H15_UnknownSchemaMode", func(t *testing.T) {
		buf := writeMinimalResultBatch(0)
		// Schema mode byte sits right after column_count = 0. Header
		// (12) + msg_kind(1) + reqId(8) + batch_seq(1) + name_len(1)
		// + row_count(1) + col_count(1) = 25 → offset 25 is the
		// schema mode byte.
		buf[qwpHeaderSize+1+8+1+1+1+1] = 0x42
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "unknown schema mode")
	})

	t.Run("H16_StringNegativeTotalBytes", func(t *testing.T) {
		buf := writeStringResultBatch(1, -1)
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "total bytes")
	})

	t.Run("H17_StringValidTotalBytesAccepted", func(t *testing.T) {
		buf := writeStringResultBatch(1, 5)
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		if err := dec.decode(buf, &b); err != nil {
			t.Fatalf("valid totalBytes rejected: %v", err)
		}
		if got := b.String(0, 0); got != "hello" {
			t.Fatalf("String = %q, want hello", got)
		}
	})

	t.Run("H25_UnsupportedWireTypeString", func(t *testing.T) {
		// Build a minimal frame that declares one column of type
		// 0x08 (old STRING; this client does not support it).
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0) // batch_seq
		putVarintBytes(&buf, 0) // name_len
		putVarintBytes(&buf, 1) // row_count = 1
		putVarintBytes(&buf, 1) // col_count = 1
		buf.WriteByte(byte(qwpSchemaModeFull))
		putVarintBytes(&buf, 0)
		putVarintBytes(&buf, 1)
		buf.WriteByte('s')
		buf.WriteByte(0x08) // STRING — unsupported
		buf.WriteByte(0)   // null flag = 0
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "unsupported wire type")
	})

	t.Run("H26_ZstdFlagRejected", func(t *testing.T) {
		buf := writeMinimalResultBatch(0)
		buf[qwpHeaderOffsetFlags] |= qwpFlagZstd
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "zstd")
	})

	t.Run("H18_DeltaDictOutOfSync", func(t *testing.T) {
		// Hand-build a frame with FLAG_DELTA_SYMBOL_DICT and a
		// delta_start that doesn't match the (empty) decoder dict.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(qwpFlagDeltaSymbolDict)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0) // batch_seq
		// Delta dict: delta_start = 3 (should be 0), count = 0.
		putVarintBytes(&buf, 3)
		putVarintBytes(&buf, 0)
		// Minimal table block (0 rows, 0 cols).
		putVarintBytes(&buf, 0)
		putVarintBytes(&buf, 0)
		putVarintBytes(&buf, 0)
		buf.WriteByte(byte(qwpSchemaModeFull))
		putVarintBytes(&buf, 0)
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "out of sync")
	})

	t.Run("H23_GorillaNonNullLessThanThree", func(t *testing.T) {
		// Build a frame with FLAG_GORILLA, one TIMESTAMP column,
		// nonNull=2, encoding byte 0x01 (Gorilla). Expect rejection.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(qwpFlagGorilla)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0)
		putVarintBytes(&buf, 0)
		putVarintBytes(&buf, 2) // row_count = 2
		putVarintBytes(&buf, 1)
		buf.WriteByte(byte(qwpSchemaModeFull))
		putVarintBytes(&buf, 0)
		putVarintBytes(&buf, 1)
		buf.WriteByte('t')
		buf.WriteByte(byte(qwpTypeTimestamp))
		buf.WriteByte(0)                    // null flag = 0
		buf.WriteByte(qwpTsEncodingGorilla) // 0x01
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(2))
		// No bitstream follows — nonNull=2, so Gorilla shouldn't be
		// in use. Decoder must reject before reading bitstream.
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "nonNull<3")
	})

	t.Run("H8_ColumnCountOutOfRange", func(t *testing.T) {
		// col_count > qwpMaxColumnsPerTable must be rejected before the
		// decoder allocates per-column layouts.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0)                             // batch_seq
		putVarintBytes(&buf, 0)                             // table_name_len
		putVarintBytes(&buf, 0)                             // row_count
		putVarintBytes(&buf, uint64(qwpMaxColumnsPerTable)+1) // col_count
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "column_count")
	})

	t.Run("H9_TableNameLengthCap", func(t *testing.T) {
		// table_name_len = qwpMaxTableNameLen + 1 is a valid varint but
		// exceeds the cap. The decoder must reject before slicing.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0)                         // batch_seq
		putVarintBytes(&buf, uint64(qwpMaxTableNameLen)+1) // name_len
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "table name length")
	})

	t.Run("H10_ColumnNameLengthCap", func(t *testing.T) {
		// Full schema with a single column whose name length exceeds
		// qwpMaxColumnNameLen.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0) // batch_seq
		putVarintBytes(&buf, 0) // table_name_len
		putVarintBytes(&buf, 0) // row_count
		putVarintBytes(&buf, 1) // col_count = 1
		buf.WriteByte(byte(qwpSchemaModeFull))
		putVarintBytes(&buf, 0)                             // schema_id
		putVarintBytes(&buf, uint64(qwpMaxColumnNameLen)+1) // col name_len
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "column name length")
	})

	t.Run("H19_DeltaDictRangeOverflow", func(t *testing.T) {
		// delta_start + delta_count must stay inside uint32; the Java
		// reference decoder and this one both reject the overflow case
		// before doing any per-entry reads.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(qwpFlagDeltaSymbolDict)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0)                 // batch_seq
		putVarintBytes(&buf, 0)                 // delta_start
		putVarintBytes(&buf, uint64(1)<<32)     // delta_count = 2^32 (overflows uint32)
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "delta symbol section")
	})

	t.Run("H27_ArrayNegativeDim", func(t *testing.T) {
		// DOUBLE_ARRAY column, row_count=1, non-null, nDims=1,
		// shape[0] = -1 (as int32). The decoder must reject.
		frame := buildArrayHardeningFrame(t, 1, []int32{-1})
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "ARRAY dim")
	})

	t.Run("H28_ArrayElementCountExceeded", func(t *testing.T) {
		// Two dims whose product overflows qwpMaxArrayElements.
		big := int32(1<<20 + 1)
		frame := buildArrayHardeningFrame(t, 2, []int32{big, big})
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "element count")
	})

	t.Run("H29_ArrayNDimsOutOfRange", func(t *testing.T) {
		// nDims > qwpMaxArrayNDims is still rejected.
		frame := buildArrayHardeningFrame(t, qwpMaxArrayNDims+1, nil)
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "ARRAY nDims")
	})

	t.Run("H29b_ArrayNDimsZeroIsNull", func(t *testing.T) {
		// nDims = 0 is the Java reference's NULL sentinel: the decoder
		// must mark the row null, consume no further bytes, and return
		// zero-value accessors for that row.
		frame := buildArrayHardeningFrame(t, 0, nil)
		var dec qwpQueryDecoder
		var b QwpColumnBatch
		if err := dec.decode(frame, &b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !b.IsNull(0, 0) {
			t.Fatalf("row 0 should be NULL for inline nDims=0")
		}
		if got := b.ArrayNDims(0, 0); got != 0 {
			t.Fatalf("ArrayNDims = %d, want 0", got)
		}
		if got := b.Float64Array(0, 0); got != nil {
			t.Fatalf("Float64Array = %v, want nil", got)
		}
		if nnc := b.NonNullCount(0); nnc != 0 {
			t.Fatalf("NonNullCount = %d, want 0", nnc)
		}
	})

	t.Run("H30_GeohashPrecisionOutOfRange", func(t *testing.T) {
		// GEOHASH column, row_count=0 so no data follows, but the
		// precision varint is read up front and must be <= 60.
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0) // batch_seq
		putVarintBytes(&buf, 0) // table_name_len
		putVarintBytes(&buf, 0) // row_count
		putVarintBytes(&buf, 1) // col_count
		buf.WriteByte(byte(qwpSchemaModeFull))
		putVarintBytes(&buf, 0) // schema_id
		putVarintBytes(&buf, 1) // col name_len
		buf.WriteByte('g')
		buf.WriteByte(byte(qwpTypeGeohash))
		buf.WriteByte(0)         // null flag
		putVarintBytes(&buf, 61) // precision > 60
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		var dec qwpQueryDecoder
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "geohash precision")
	})
}

// buildArrayHardeningFrame crafts a minimal RESULT_BATCH carrying a
// single DOUBLE_ARRAY column with one non-null row whose per-row entry
// is (nDims byte, shape int32s, then as many float64 elements as the
// shape's product). This is enough to exercise the array-section
// hardening branches.
func buildArrayHardeningFrame(t *testing.T, nDims int, shape []int32) []byte {
	t.Helper()
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
	buf.WriteByte(qwpVersion)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
	buf.WriteByte(byte(qwpMsgKindResultBatch))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
	putVarintBytes(&buf, 0) // batch_seq
	putVarintBytes(&buf, 0) // table_name_len
	putVarintBytes(&buf, 1) // row_count = 1
	putVarintBytes(&buf, 1) // col_count = 1
	buf.WriteByte(byte(qwpSchemaModeFull))
	putVarintBytes(&buf, 0) // schema_id
	putVarintBytes(&buf, 1)
	buf.WriteByte('a')
	buf.WriteByte(byte(qwpTypeDoubleArray))
	buf.WriteByte(0) // null flag
	// Row body.
	buf.WriteByte(byte(nDims))
	for _, d := range shape {
		_ = binary.Write(&buf, binary.LittleEndian, d)
	}
	// The decoder either consumes no further bytes (nDims=0 → NULL) or
	// rejects on the shape/nDims check before reading any element
	// bytes, so we don't need to append them for those paths. Append
	// zero padding just to avoid a truncated-frame error masking the
	// real one.
	buf.Write(make([]byte, 8))
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
	return out
}

func assertDecodeErrContains(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
	}
	var de *qwpDecodeError
	if !errors.As(err, &de) {
		// Magic / version / msgKind errors don't go through qwpDecodeError
		// right now if they are constructed directly — accept either type,
		// but still check for substring.
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("error %q does not contain %q", err.Error(), substr)
	}
}

func containsAny(haystack string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(haystack, n) {
			return true
		}
	}
	return false
}

// typeCodeName is a test-local pretty-printer for qwpTypeCode values,
// kept as a free function so it doesn't attach a String() method to
// the production type (which would alter fmt.%v output during tests).
func typeCodeName(t qwpTypeCode) string {
	switch t {
	case qwpTypeVarchar:
		return "VARCHAR"
	case qwpTypeBinary:
		return "BINARY"
	case qwpTypeSymbol:
		return "SYMBOL"
	default:
		return "TYPE_" + itoa(int(t))
	}
}
