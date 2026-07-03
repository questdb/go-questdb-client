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
	"math"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
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

	// A continuation RESULT_BATCH (batch_seq > 0) carries no col_count
	// and no inline column schema — the decoder reuses the schema parsed
	// from batch 0. The ingress encoder always writes them, so strip the
	// schema section here to mirror a real continuation frame.
	if batchSeq > 0 {
		body = stripContinuationSchema(body)
	}

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

// stripContinuationSchema removes the col_count + inline column schema
// from an ingress-encoded body (deltaDict followed by a table block),
// leaving deltaDict + table_name + row_count + column data — the shape
// a real continuation RESULT_BATCH (batch_seq > 0) carries on the wire.
// The ingress encoder always emits the schema; this drops it so a
// wrapped continuation frame matches what the server would actually
// send for batch_seq > 0.
func stripContinuationSchema(body []byte) []byte {
	var r qwpByteReader
	r.reset(body)
	mustVarint := func(what string) int64 {
		v, err := r.readVarintInt63()
		if err != nil {
			panic("stripContinuationSchema: " + what + ": " + err.Error())
		}
		return v
	}
	mustAdvance := func(n int, what string) {
		if err := r.advance(n); err != nil {
			panic("stripContinuationSchema: " + what + ": " + err.Error())
		}
	}
	// Delta dict: deltaStart, deltaCount, then deltaCount strings.
	mustVarint("deltaStart")
	deltaCount := mustVarint("deltaCount")
	for i := int64(0); i < deltaCount; i++ {
		mustAdvance(int(mustVarint("dict string len")), "dict string")
	}
	// Table block prefix kept on every batch: table_name, row_count.
	mustAdvance(int(mustVarint("table name len")), "table name")
	mustVarint("row_count")
	schemaStart := r.pos
	// Schema section dropped on continuation batches: col_count, then
	// per column a name (varint len + bytes) and a 1-byte type code.
	colCount := mustVarint("col_count")
	for i := int64(0); i < colCount; i++ {
		mustAdvance(int(mustVarint("col name len")), "col name")
		if _, err := r.readByte(); err != nil {
			panic("stripContinuationSchema: type code: " + err.Error())
		}
	}
	colDataStart := r.pos
	out := make([]byte, 0, schemaStart+(len(body)-colDataStart))
	out = append(out, body[:schemaStart]...)
	out = append(out, body[colDataStart:]...)
	return out
}

// newTestQueryDecoder returns a zero-valued decoder seeded with the
// negotiated version every test fixture stamps into its frames
// (qwpVersion = 1). Production code sets this field via
// qwpEgressIO.start; tests construct decoders directly.
func newTestQueryDecoder() qwpQueryDecoder {
	return qwpQueryDecoder{negotiatedVersion: qwpVersion}
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
	ingress := enc.encodeTable(tb)
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
				func(c *qwpColumnBuffer) { c.addBool(false) },
				func(c *qwpColumnBuffer) { c.addBool(false) },
				func(c *qwpColumnBuffer) { c.addBool(true) },
				func(c *qwpColumnBuffer) { c.addBool(true) },
				func(c *qwpColumnBuffer) { c.addBool(false) },
				func(c *qwpColumnBuffer) { c.addBool(true) },
				func(c *qwpColumnBuffer) { c.addBool(false) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				// 10 booleans cross a byte boundary in the bit-packed
				// wire payload (8 bits/byte). The decoder must walk
				// across both bytes.
				want := []bool{true, false, true, false, false, true, true, false, true, false}
				for i, w := range want {
					if got := b.Bool(0, i); got != w {
						t.Fatalf("Bool[%d] = %v, want %v", i, got, w)
					}
				}
			},
		},
		{
			name: "BYTE", wt: qwpTypeByte,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addByte(math.MinInt8) },
				func(c *qwpColumnBuffer) { c.addByte(-1) },
				func(c *qwpColumnBuffer) { c.addByte(0) },
				func(c *qwpColumnBuffer) { c.addByte(7) },
				func(c *qwpColumnBuffer) { c.addByte(math.MaxInt8) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				for i, w := range []int8{math.MinInt8, -1, 0, 7, math.MaxInt8} {
					if got := b.Int8(0, i); got != w {
						t.Fatalf("Int8[%d] = %d, want %d", i, got, w)
					}
				}
			},
		},
		{
			name: "SHORT", wt: qwpTypeShort,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addShort(math.MinInt16) },
				func(c *qwpColumnBuffer) { c.addShort(-1) },
				func(c *qwpColumnBuffer) { c.addShort(0) },
				func(c *qwpColumnBuffer) { c.addShort(42) },
				func(c *qwpColumnBuffer) { c.addShort(math.MaxInt16) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				for i, w := range []int16{math.MinInt16, -1, 0, 42, math.MaxInt16} {
					if got := b.Int16(0, i); got != w {
						t.Fatalf("Int16[%d] = %d, want %d", i, got, w)
					}
				}
			},
		},
		{
			name: "CHAR", wt: qwpTypeChar,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addChar('a') },
				func(c *qwpColumnBuffer) { c.addChar('Z') },
				func(c *qwpColumnBuffer) { c.addChar('0') },
				func(c *qwpColumnBuffer) { c.addChar(' ') },
				// Highest BMP code point — pins the LE 2-byte
				// reassembly path against off-by-one shifts in the
				// decoder.
				func(c *qwpColumnBuffer) { c.addChar(0xFFFE) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				for i, w := range []rune{'a', 'Z', '0', ' ', 0xFFFE} {
					if got := b.Char(0, i); got != w {
						t.Fatalf("Char[%d] = %U, want %U", i, got, w)
					}
				}
			},
		},
		{
			name: "FLOAT", wt: qwpTypeFloat,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addFloat32(float32(math.Inf(-1))) },
				func(c *qwpColumnBuffer) { c.addFloat32(-1.5) },
				func(c *qwpColumnBuffer) { c.addFloat32(0) },
				func(c *qwpColumnBuffer) { c.addFloat32(1.5) },
				func(c *qwpColumnBuffer) { c.addFloat32(float32(math.Inf(1))) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				want := []float32{
					float32(math.Inf(-1)), -1.5, 0, 1.5, float32(math.Inf(1)),
				}
				for i, w := range want {
					if got := b.Float32(0, i); got != w {
						t.Fatalf("Float32[%d] = %v, want %v", i, got, w)
					}
				}
			},
		},
		// DATE has no Go-encode <-> Go-decode round trip: ingestion
		// frames DATE as plain int64 but egress frames it timestamp-ish
		// (protocol asymmetry). Egress DATE decode is covered by
		// TestQwpDecoderEgressDate; ingestion by TestQwpIntegrationQwpOnlyTypes.
		{
			name: "TIMESTAMP_NANO", wt: qwpTypeTimestampNano,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) { c.addTimestamp(1_700_000_000_000_000_000) },
				func(c *qwpColumnBuffer) { c.addTimestamp(1_700_000_000_000_000_001) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				want := []int64{1_700_000_000_000_000_000, 1_700_000_000_000_000_001}
				for i, w := range want {
					if got := b.Int64(0, i); got != w {
						t.Fatalf("TsNano Int64[%d] = %d, want %d", i, got, w)
					}
				}
			},
		},
		{
			name: "UUID", wt: qwpTypeUuid,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) {
					c.addUuid(0x99AABBCCDDEEFF00, 0x1122334455667788)
				},
				func(c *qwpColumnBuffer) { c.addUuid(0, 0) },
				func(c *qwpColumnBuffer) {
					c.addUuid(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
				},
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				type uuidPair struct{ hi, lo uint64 }
				want := []uuidPair{
					{0x99AABBCCDDEEFF00, 0x1122334455667788},
					{0, 0},
					{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
				}
				for i, w := range want {
					if got := uint64(b.UuidLo(0, i)); got != w.lo {
						t.Fatalf("UuidLo[%d] = %#x, want %#x", i, got, w.lo)
					}
					if got := uint64(b.UuidHi(0, i)); got != w.hi {
						t.Fatalf("UuidHi[%d] = %#x, want %#x", i, got, w.hi)
					}
				}
			},
		},
		{
			name: "LONG256", wt: qwpTypeLong256,
			rows: []func(col *qwpColumnBuffer){
				func(c *qwpColumnBuffer) {
					c.addLong256(0x1111111111111111, 0x2222222222222222,
						0x3333333333333333, 0x4444444444444444)
				},
				func(c *qwpColumnBuffer) { c.addLong256(0, 0, 0, 0) },
			},
			check: func(t *testing.T, b *QwpColumnBatch) {
				want := [][4]uint64{
					{0x1111111111111111, 0x2222222222222222,
						0x3333333333333333, 0x4444444444444444},
					{0, 0, 0, 0},
				}
				for i, row := range want {
					for w := 0; w < 4; w++ {
						if got := uint64(b.Long256Word(0, i, w)); got != row[w] {
							t.Fatalf("Long256[%d].word[%d] = %#x, want %#x",
								i, w, got, row[w])
						}
					}
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			frame := encodeSingleColumnBatch(t, "c", c.wt, false, c.rows)
			dec := newTestQueryDecoder()
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
	dec := newTestQueryDecoder()
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
			dec := newTestQueryDecoder()
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

// patchSchemaTypeToDate rewrites the schema type code of column colName
// in a raw qwpEncoder.encodeTable() payload (BEFORE wrapAsResultBatch)
// to qwpTypeDate. DATE shares TIMESTAMP's *egress* framing (1-byte
// encoding discriminator + RAW/Gorilla), so encoding the column as
// TIMESTAMP and relabelling the schema yields byte-for-byte what the
// server's QwpResultBatchBuffer emits for a DATE column. The ingestion
// encoder cannot synthesise egress DATE directly (it writes plain
// int64, by protocol asymmetry). Offsets mirror the proven walk in
// TestQwpEncoderAllFixedTypes (raw encodeTable layout: qwpHeaderSize
// header + 2-byte empty delta symbol dict, then name / counts / schema).
func patchSchemaTypeToDate(t *testing.T, ingress []byte, colName string) {
	t.Helper()
	off := qwpHeaderSize + 2 // header + empty delta symbol dict (2 bytes)
	nameLen, n, err := qwpReadVarint(ingress[off:])
	if err != nil {
		t.Fatalf("table-name varint: %v", err)
	}
	off += n + int(nameLen) // table name
	if _, n, err = qwpReadVarint(ingress[off:]); err != nil {
		t.Fatalf("rowCount varint: %v", err)
	}
	off += n // rowCount
	colCount, n, err := qwpReadVarint(ingress[off:])
	if err != nil {
		t.Fatalf("colCount varint: %v", err)
	}
	off += n
	for i := 0; i < int(colCount); i++ {
		cnLen, n, err := qwpReadVarint(ingress[off:])
		if err != nil {
			t.Fatalf("col-name varint: %v", err)
		}
		off += n
		name := string(ingress[off : off+int(cnLen)])
		off += int(cnLen)
		if name == colName {
			ingress[off] = byte(qwpTypeDate)
			return
		}
		off++ // skip this column's type code
	}
	t.Fatalf("column %q not found in schema", colName)
}

func TestQwpDecoderEgressDate(t *testing.T) {
	// DATE egress is framed exactly like TIMESTAMP: a 1-byte encoding
	// discriminator then RAW int64 / Gorilla. The decoder must route
	// DATE through parseTimestamp (regression guard for the DATE-as-
	// plain-int64 bug the egress fuzz caught). Cover both branches.
	run := func(t *testing.T, vals []int64) {
		t.Helper()
		tb := newQwpTableBuffer("t")
		for _, v := range vals {
			col, err := tb.getOrCreateColumn("d", qwpTypeTimestamp, false)
			if err != nil {
				t.Fatalf("getOrCreateColumn: %v", err)
			}
			col.addLong(v)
			tb.commitRow()
		}
		var enc qwpEncoder
		ingress := enc.encodeTable(tb)
		patchSchemaTypeToDate(t, ingress, "d")
		frame := wrapAsResultBatch(ingress, 1, 0)
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		if err := dec.decode(frame, &b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if b.RowCount() != len(vals) {
			t.Fatalf("RowCount = %d, want %d", b.RowCount(), len(vals))
		}
		for i, w := range vals {
			if got := b.Int64(0, i); got != w {
				t.Fatalf("Int64[%d] = %d, want %d", i, got, w)
			}
		}
	}
	// <=2 values force the encoder's uncompressed (0x00) branch.
	t.Run("Uncompressed", func(t *testing.T) {
		run(t, []int64{0, 1_700_000_000_000})
	})
	// >2 values with small delta-of-deltas pick Gorilla (0x01).
	t.Run("Gorilla", func(t *testing.T) {
		run(t, []int64{1_000_000, 1_000_100, 1_000_200, 1_000_310, 1_000_520})
	})
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
	dec := newTestQueryDecoder()
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
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)

	dec := newTestQueryDecoder()
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
			frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)

			dec := newTestQueryDecoder()
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
			// The public Geohash accessors encapsulate exactly the
			// stride-sized read done by hand above; both surfaces must
			// agree with the raw layout.
			col := batch.Column(0)
			for i, want := range values {
				want &= mask
				if got := batch.Geohash(0, i); got != want {
					t.Errorf("batch.Geohash(0,%d) = %#x, want %#x", i, got, want)
				}
				if got := col.Geohash(i); got != want {
					t.Errorf("col.Geohash(%d) = %#x, want %#x", i, got, want)
				}
			}
		})
	}
}

func TestQwpDecoderRoundTripDecimal128(t *testing.T) {
	// Drives a DECIMAL128 column through the encoder→decoder pipeline
	// so the per-type DECIMAL128 layout (1-byte scale + 16 LE bytes)
	// is exercised end-to-end. The hand-built layout test in
	// qwp_query_batch_test.go bypasses the decoder; this round-trip
	// catches regressions in the DECIMAL128-specific decoder branch.
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("d", qwpTypeDecimal128, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	type d128 struct {
		scale    uint32
		hi, lo   uint64
		signedHi int64
	}
	cases := []d128{
		{scale: 4, hi: 0x0102030405060708, lo: 0xCAFEBABEDEADBEEF, signedHi: 0x0102030405060708},
		{scale: 4, hi: 0, lo: 1, signedHi: 0},
		{scale: 4, hi: 0xFFFFFFFFFFFFFFFF, lo: 0xFFFFFFFFFFFFFFFF, signedHi: -1},
	}
	// Build a Decimal value at the desired scale and unscaled coefficient
	// so addDecimal picks DECIMAL128 width. NewDecimalUnsafe takes a
	// two's complement big-endian unscaled buffer directly.
	for _, c := range cases {
		// Build a big-endian 16-byte two's complement unscaled value
		// (hi || lo). NewDecimalUnsafe interprets the sign over these 16
		// bytes, matching the DECIMAL128 column width, so an all-ones
		// pattern is the signed value -1.
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[0:], c.hi)
		binary.BigEndian.PutUint64(buf[8:], c.lo)
		dec, err := NewDecimalUnsafe(buf, c.scale)
		if err != nil {
			t.Fatalf("NewDecimalUnsafe: %v", err)
		}
		// addDecimal will pick a wireSize matching the column's
		// fixedSize (16 for DECIMAL128). The Decimal's own significant
		// bytes can be wider; for that case the encoder rejects with
		// an overflow error rather than truncate. We picked values
		// whose significant bytes fit in 16.
		if err := col.addDecimal(dec); err != nil {
			t.Fatalf("addDecimal: %v", err)
		}
		tb.commitRow()
		col, err = tb.getOrCreateColumn("d", qwpTypeDecimal128, false)
		if err != nil {
			t.Fatalf("getOrCreateColumn (next row): %v", err)
		}
	}
	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)

	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := batch.DecimalScale(0); got != 4 {
		t.Fatalf("DecimalScale = %d, want 4", got)
	}
	for i, c := range cases {
		if got := uint64(batch.Decimal128Lo(0, i)); got != c.lo {
			t.Fatalf("Decimal128Lo[%d] = %#x, want %#x", i, got, c.lo)
		}
		if got := batch.Decimal128Hi(0, i); got != c.signedHi {
			t.Fatalf("Decimal128Hi[%d] = %#x, want %#x", i, uint64(got), c.hi)
		}
	}
}

func TestQwpDecoderRoundTripDecimal256(t *testing.T) {
	// Drives a DECIMAL256 column through the full pipeline to cover
	// the wide-decimal branch of the decoder. DECIMAL256 stores 32 LE
	// bytes after a 1-byte scale. Three rows so the per-row dense
	// indexing into the values slice has to advance correctly.
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("d", qwpTypeDecimal256, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	type d256 struct {
		scale          uint32
		w0, w1, w2, w3 uint64
	}
	cases := []d256{
		{scale: 7, w0: 0x1111111111111111, w1: 0x2222222222222222,
			w2: 0x3333333333333333, w3: 0x4444444444444444},
		{scale: 7, w0: 0, w1: 0, w2: 0, w3: 1},
	}
	for _, c := range cases {
		// Build big-endian 32-byte unscaled value: w3 || w2 || w1 || w0.
		buf := make([]byte, 32)
		binary.BigEndian.PutUint64(buf[0:], c.w3)
		binary.BigEndian.PutUint64(buf[8:], c.w2)
		binary.BigEndian.PutUint64(buf[16:], c.w1)
		binary.BigEndian.PutUint64(buf[24:], c.w0)
		d, err := NewDecimalUnsafe(buf, c.scale)
		if err != nil {
			t.Fatalf("NewDecimalUnsafe: %v", err)
		}
		if err := col.addDecimal(d); err != nil {
			t.Fatalf("addDecimal: %v", err)
		}
		tb.commitRow()
		col, err = tb.getOrCreateColumn("d", qwpTypeDecimal256, false)
		if err != nil {
			t.Fatalf("getOrCreateColumn (next row): %v", err)
		}
	}
	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)

	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := batch.DecimalScale(0); got != 7 {
		t.Fatalf("DecimalScale = %d, want 7", got)
	}
	for i, c := range cases {
		want := [4]uint64{c.w0, c.w1, c.w2, c.w3}
		for w := 0; w < 4; w++ {
			if got := uint64(batch.Long256Word(0, i, w)); got != want[w] {
				t.Fatalf("Decimal256[%d].word[%d] = %#x, want %#x",
					i, w, got, want[w])
			}
		}
	}
}

func TestQwpDecoderRoundTripInt64Array(t *testing.T) {
	// Mirrors TestQwpDecoderRoundTripFloat64Array for the LONG_ARRAY
	// type code. Two rows of a 2x3 int64 array, decoded via
	// Int64Array; the shape and dense-index machinery is shared with
	// the DOUBLE_ARRAY path.
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("a", qwpTypeLongArray, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	col.addLongArray(2, []int32{2, 3}, []int64{1, 2, 3, 4, 5, 6})
	tb.commitRow()
	col, err = tb.getOrCreateColumn("a", qwpTypeLongArray, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn (row 2): %v", err)
	}
	col.addLongArray(2, []int32{2, 3}, []int64{10, 20, 30, 40, 50, 60})
	tb.commitRow()

	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)

	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for row := 0; row < 2; row++ {
		if got := batch.ArrayNDims(0, row); got != 2 {
			t.Fatalf("ArrayNDims[%d] = %d, want 2", row, got)
		}
		if d0, d1 := batch.ArrayDim(0, row, 0), batch.ArrayDim(0, row, 1); d0 != 2 || d1 != 3 {
			t.Fatalf("ArrayDim[%d] = %dx%d, want 2x3", row, d0, d1)
		}
	}
	want := [][]int64{
		{1, 2, 3, 4, 5, 6},
		{10, 20, 30, 40, 50, 60},
	}
	for row, w := range want {
		got := batch.Int64Array(0, row)
		if len(got) != len(w) {
			t.Fatalf("Int64Array[%d] len = %d, want %d", row, len(got), len(w))
		}
		for i := range w {
			if got[i] != w[i] {
				t.Fatalf("Int64Array[%d][%d] = %d, want %d", row, i, got[i], w[i])
			}
		}
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
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)

	dec := newTestQueryDecoder()
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

// TestQwpDecoderRoundTripEmptyVsNullArray pins the empty-vs-NULL array
// distinction across the wire. QuestDB stores a non-null empty array
// (cardinality 0) of a given shape as a value distinct from a NULL
// array: the encoder writes an empty array inline (nDims >= 1 with a
// 0-length dimension) and a NULL via the null bitmap, and the decoder
// round-trips both without conflating them. A 0-length dimension is the
// boundary the per-dimension guard in parseArray must admit.
//
// Two columns carry the same four row shapes: "a" is a DOUBLE_ARRAY and
// "b" a LONG_ARRAY, exercising the shared parseArray path for both
// element types. "a" precedes "b" in the frame, so "b" decoding
// correctly also pins that the empty and NULL rows of "a" consumed
// exactly their wire bytes (inter-column alignment).
func TestQwpDecoderRoundTripEmptyVsNullArray(t *testing.T) {
	tb := newQwpTableBuffer("t")
	mkDouble := func() *qwpColumnBuffer {
		col, err := tb.getOrCreateColumn("a", qwpTypeDoubleArray, true)
		if err != nil {
			t.Fatalf("getOrCreateColumn(a): %v", err)
		}
		return col
	}
	mkLong := func() *qwpColumnBuffer {
		col, err := tb.getOrCreateColumn("b", qwpTypeLongArray, true)
		if err != nil {
			t.Fatalf("getOrCreateColumn(b): %v", err)
		}
		return col
	}
	// Row 0: regular 1x3 array.
	mkDouble().addDoubleArray(1, []int32{3}, []float64{1, 2, 3})
	mkLong().addLongArray(1, []int32{3}, []int64{1, 2, 3})
	tb.commitRow()
	// Row 1: empty 1D array, shape {0}.
	mkDouble().addDoubleArray(1, []int32{0}, nil)
	mkLong().addLongArray(1, []int32{0}, nil)
	tb.commitRow()
	// Row 2: NULL array.
	mkDouble().addNull()
	mkLong().addNull()
	tb.commitRow()
	// Row 3: empty 2D array, shape {2, 0} — still cardinality 0.
	mkDouble().addDoubleArray(2, []int32{2, 0}, nil)
	mkLong().addLongArray(2, []int32{2, 0}, nil)
	tb.commitRow()

	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)
	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frame, &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Row 0: regular, non-null, [1 2 3].
	if batch.IsNull(0, 0) {
		t.Fatal("row 0 (regular array) must NOT be null")
	}
	got0, want0 := batch.Float64Array(0, 0), []float64{1, 2, 3}
	if len(got0) != len(want0) {
		t.Fatalf("row 0 len = %d, want %d", len(got0), len(want0))
	}
	for i := range want0 {
		if got0[i] != want0[i] {
			t.Fatalf("row 0[%d] = %v, want %v", i, got0[i], want0[i])
		}
	}

	// Row 1: empty 1D — non-null, nDims 1, dim 0 == 0, non-nil empty slice.
	if batch.IsNull(0, 1) {
		t.Fatal("row 1 (empty 1D array) must NOT be null")
	}
	if nd := batch.ArrayNDims(0, 1); nd != 1 {
		t.Fatalf("row 1 ArrayNDims = %d, want 1", nd)
	}
	if d0 := batch.ArrayDim(0, 1, 0); d0 != 0 {
		t.Fatalf("row 1 ArrayDim(0) = %d, want 0", d0)
	}
	if got := batch.Float64Array(0, 1); got == nil || len(got) != 0 {
		t.Fatalf("row 1 Float64Array = %v (nil=%t), want non-nil empty", got, got == nil)
	}

	// Row 2: NULL — IsNull true, Float64Array nil.
	if !batch.IsNull(0, 2) {
		t.Fatal("row 2 (NULL array) must be null")
	}
	if got := batch.Float64Array(0, 2); got != nil {
		t.Fatalf("row 2 Float64Array = %v, want nil (NULL)", got)
	}

	// Row 3: empty 2D {2, 0} — non-null, nDims 2, cardinality 0.
	if batch.IsNull(0, 3) {
		t.Fatal("row 3 (empty 2D array) must NOT be null")
	}
	if nd := batch.ArrayNDims(0, 3); nd != 2 {
		t.Fatalf("row 3 ArrayNDims = %d, want 2", nd)
	}
	if d0, d1 := batch.ArrayDim(0, 3, 0), batch.ArrayDim(0, 3, 1); d0 != 2 || d1 != 0 {
		t.Fatalf("row 3 ArrayDim = %dx%d, want 2x0", d0, d1)
	}
	if got := batch.Float64Array(0, 3); got == nil || len(got) != 0 {
		t.Fatalf("row 3 Float64Array = %v (nil=%t), want non-nil empty", got, got == nil)
	}

	// Column 1 = "b" (LONG_ARRAY): same four shapes, read via Int64Array.
	// Row 0: regular, non-null, [1 2 3].
	if batch.IsNull(1, 0) {
		t.Fatal("col b row 0 (regular array) must NOT be null")
	}
	gotL0, wantL0 := batch.Int64Array(1, 0), []int64{1, 2, 3}
	if len(gotL0) != len(wantL0) {
		t.Fatalf("col b row 0 len = %d, want %d", len(gotL0), len(wantL0))
	}
	for i := range wantL0 {
		if gotL0[i] != wantL0[i] {
			t.Fatalf("col b row 0[%d] = %v, want %v", i, gotL0[i], wantL0[i])
		}
	}

	// Row 1: empty 1D — non-null, nDims 1, dim 0 == 0, non-nil empty slice.
	if batch.IsNull(1, 1) {
		t.Fatal("col b row 1 (empty 1D array) must NOT be null")
	}
	if nd := batch.ArrayNDims(1, 1); nd != 1 {
		t.Fatalf("col b row 1 ArrayNDims = %d, want 1", nd)
	}
	if d0 := batch.ArrayDim(1, 1, 0); d0 != 0 {
		t.Fatalf("col b row 1 ArrayDim(0) = %d, want 0", d0)
	}
	if got := batch.Int64Array(1, 1); got == nil || len(got) != 0 {
		t.Fatalf("col b row 1 Int64Array = %v (nil=%t), want non-nil empty", got, got == nil)
	}

	// Row 2: NULL — IsNull true, Int64Array nil.
	if !batch.IsNull(1, 2) {
		t.Fatal("col b row 2 (NULL array) must be null")
	}
	if got := batch.Int64Array(1, 2); got != nil {
		t.Fatalf("col b row 2 Int64Array = %v, want nil (NULL)", got)
	}

	// Row 3: empty 2D {2, 0} — non-null, nDims 2, cardinality 0.
	if batch.IsNull(1, 3) {
		t.Fatal("col b row 3 (empty 2D array) must NOT be null")
	}
	if nd := batch.ArrayNDims(1, 3); nd != 2 {
		t.Fatalf("col b row 3 ArrayNDims = %d, want 2", nd)
	}
	if d0, d1 := batch.ArrayDim(1, 3, 0), batch.ArrayDim(1, 3, 1); d0 != 2 || d1 != 0 {
		t.Fatalf("col b row 3 ArrayDim = %dx%d, want 2x0", d0, d1)
	}
	if got := batch.Int64Array(1, 3); got == nil || len(got) != 0 {
		t.Fatalf("col b row 3 Int64Array = %v (nil=%t), want non-nil empty", got, got == nil)
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
	ingress1 := enc.encodeTableWithDeltaDict(tb1, globalDict, -1, 2)
	frame1 := wrapAsResultBatch(ingress1, 1, 0)

	tb2 := newQwpTableBuffer("t")
	for _, id := range []int32{3, 1} { // TSLA, MSFT
		col, _ := tb2.getOrCreateColumn("s", qwpTypeSymbol, false)
		col.addSymbolID(id)
		tb2.commitRow()
	}
	// maxSentId=2, batchMaxId=3 → delta advertises id 3 only. Batch 2 is
	// a continuation (batch_seq=1): no inline schema, reuses batch 1's.
	ingress2 := enc.encodeTableWithDeltaDict(tb2, globalDict, 2, 3)
	frame2 := wrapAsResultBatch(ingress2, 1, 1)

	dec := newTestQueryDecoder()
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

func TestQwpDecoderContinuationReusesSchema(t *testing.T) {
	// Batch 0 carries the inline schema. Batch 1 is a continuation
	// (batch_seq=1) with no schema on the wire; the decoder must reuse
	// the schema parsed from batch 0 to decode it.
	tb1 := newQwpTableBuffer("t")
	for _, v := range []int64{1, 2} {
		col, _ := tb1.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(v)
		tb1.commitRow()
	}
	var enc qwpEncoder
	frame1 := wrapAsResultBatch(enc.encodeTable(tb1), 1, 0)

	tb2 := newQwpTableBuffer("t")
	col2, _ := tb2.getOrCreateColumn("a", qwpTypeLong, false)
	col2.addLong(10)
	tb2.commitRow()
	frame2 := wrapAsResultBatch(enc.encodeTable(tb2), 1, 1)

	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	if err := dec.decode(frame1, &batch); err != nil {
		t.Fatalf("decode frame1: %v", err)
	}
	if err := dec.decode(frame2, &batch); err != nil {
		t.Fatalf("decode frame2: %v", err)
	}
	if batch.ColumnName(0) != "a" {
		t.Fatalf("continuation batch lost column name: %q", batch.ColumnName(0))
	}
	if got := batch.Int64(0, 0); got != 10 {
		t.Fatalf("Int64[0] (frame2) = %d, want 10", got)
	}
}

func TestQwpDecoderContinuationBeforeSchemaRejected(t *testing.T) {
	// A continuation batch (batch_seq > 0) that arrives before any
	// batch_seq==0 schema batch has no schema to reuse and must be
	// rejected rather than misparsed.
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
	col.addLong(10)
	tb.commitRow()
	var enc qwpEncoder
	frame := wrapAsResultBatch(enc.encodeTable(tb), 1, 1)

	dec := newTestQueryDecoder()
	var batch QwpColumnBatch
	err := dec.decode(frame, &batch)
	assertDecodeErrContains(t, err, "before its schema batch")
}

// --- Hardening tests (ports of QwpResultBatchDecoderHardeningTest) ---

// writeMinimalResultBatch builds a minimal valid RESULT_BATCH frame
// with 0 rows and 0 columns. Matches QwpResultBatchDecoderHardeningTest.
// writeMinimalResultBatch.
func writeMinimalResultBatch() []byte {
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
	putVarintBytes(&buf, 0)                                // batch_seq
	putVarintBytes(&buf, 0)                                // name_len
	putVarintBytes(&buf, 0)                                // row_count
	putVarintBytes(&buf, 0)                                // column_count
	// Patch payloadLength at offset 8.
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

// writeStringResultBatchCustom builds a RESULT_BATCH with one VARCHAR
// column, len(offsets)-1 non-null rows, and the provided offsets /
// payload stamped verbatim into the frame. Used by the offset-validation
// hardening subtests.
func writeStringResultBatchCustom(offsets []uint32, payload []byte) []byte {
	nonNull := len(offsets) - 1
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
	buf.WriteByte(qwpVersion)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
	buf.WriteByte(byte(qwpMsgKindResultBatch))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(7))
	putVarintBytes(&buf, 0)
	putVarintBytes(&buf, 0)
	putVarintBytes(&buf, uint64(nonNull))
	putVarintBytes(&buf, 1)
	putVarintBytes(&buf, 1)
	buf.WriteByte('s')
	buf.WriteByte(byte(qwpTypeVarchar))
	buf.WriteByte(0)
	for _, off := range offsets {
		_ = binary.Write(&buf, binary.LittleEndian, off)
	}
	buf.Write(payload)
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
	putVarintBytes(&buf, 0)               // batch_seq
	putVarintBytes(&buf, 0)               // table_name_len
	putVarintBytes(&buf, uint64(nonNull)) // row_count
	putVarintBytes(&buf, 1)               // column_count
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
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(make([]byte, 5), &b)
		assertDecodeErrContains(t, err, "too short")
	})

	t.Run("H1a_BatchExceedsWireCap", func(t *testing.T) {
		// Spec §14: RESULT_BATCH wire size is capped at 16 MiB. A
		// conformant server stays under; a hostile / buggy server that
		// goes over must be rejected up front, before any header,
		// schema, or body bound is exercised. The frame contents do
		// not need to be valid — the cap fires first.
		payload := make([]byte, qwpMaxBatchSize+1)
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(payload, &b)
		assertDecodeErrContains(t, err, "exceeds protocol cap")
	})

	t.Run("H1b_BatchAtWireCapAccepted", func(t *testing.T) {
		// A frame whose total wire size equals the 16 MiB cap exactly
		// must pass the size guard and continue into the regular
		// parse. We use a minimal valid frame padded out to the cap
		// via a long table name (still inside the per-table-name
		// limits enforced downstream — here the parse fails for an
		// unrelated reason: name_len > qwpMaxTableNameLen). The point
		// of this test is only to pin that the size guard does NOT
		// reject a frame at exactly qwpMaxBatchSize bytes.
		buf := writeMinimalResultBatch()
		// Pad with arbitrary trailing bytes so len(buf) == qwpMaxBatchSize.
		// The decoder rejects on a downstream check (specifically the
		// table-name-length cap or end-of-frame mismatch), not on the
		// size guard, which is what this test asserts.
		pad := qwpMaxBatchSize - len(buf)
		if pad < 0 {
			t.Fatalf("minimal frame already exceeds cap (%d > %d)", len(buf), qwpMaxBatchSize)
		}
		buf = append(buf, make([]byte, pad)...)
		if len(buf) != qwpMaxBatchSize {
			t.Fatalf("padded frame has %d bytes, want %d", len(buf), qwpMaxBatchSize)
		}
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		// Any non-size error is fine; the test fails only if the size
		// guard incorrectly rejects a frame at the cap.
		if err != nil && strings.Contains(err.Error(), "exceeds protocol cap") {
			t.Fatalf("frame at cap rejected by size guard: %v", err)
		}
	})

	t.Run("H2_BadMagic", func(t *testing.T) {
		buf := writeMinimalResultBatch()
		buf[0] = 0xFF
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "bad magic")
	})

	t.Run("H3_VersionMismatch", func(t *testing.T) {
		// Spec §3 requires strict equality between the frame's header
		// version byte and the negotiated version. The default test
		// decoder is pinned to qwpVersion (= 1); any other value must
		// be rejected — including a value within the supported range
		// (0x02), not just 0xFF.
		for _, v := range []byte{0x02, 0xFF} {
			buf := writeMinimalResultBatch()
			buf[4] = v
			dec := newTestQueryDecoder()
			var b QwpColumnBatch
			err := dec.decode(buf, &b)
			assertDecodeErrContains(t, err, "does not match negotiated version")
		}
	})

	t.Run("H3a_PayloadLengthMismatch", func(t *testing.T) {
		// parseFrameHeader validates the header's declared
		// payload_length against the body it actually received. A frame
		// whose declared length disagrees with its size is a framing
		// desync and must be rejected up front, not decoded.
		correct := uint32(len(writeMinimalResultBatch()) - qwpHeaderSize)
		for _, declared := range []uint32{correct + 1, correct - 1, 0} {
			buf := writeMinimalResultBatch()
			binary.LittleEndian.PutUint32(buf[qwpHeaderOffsetPayloadLen:], declared)
			dec := newTestQueryDecoder()
			var b QwpColumnBatch
			err := dec.decode(buf, &b)
			assertDecodeErrContains(t, err, "does not match body size")
		}
	})

	t.Run("H4_UnexpectedMsgKind", func(t *testing.T) {
		// Use a frame whose table_count matches the spoofed msg_kind so
		// the per-kind RESULT_BATCH check is what fires (not the
		// table_count guard that runs first inside parseFrameHeader).
		// RESULT_END expects table_count=0; matches the value that
		// writeQwpFrame sets.
		buf := writeQwpFrame(0, buildResultEndBody(1, 0, 0))
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "expected RESULT_BATCH")
	})

	t.Run("H4a_TableCountWrongOnResultBatch", func(t *testing.T) {
		// Spec §4: RESULT_BATCH must carry table_count = 1. A
		// conformant decoder must reject any other value rather than
		// treat it as a hint. writeMinimalResultBatch sets the field
		// to 1; flip it to 0 and 5 to cover both directions.
		for _, tc := range []uint16{0, 5} {
			buf := writeMinimalResultBatch()
			binary.LittleEndian.PutUint16(
				buf[qwpHeaderOffsetTableCount:qwpHeaderOffsetTableCount+2], tc)
			dec := newTestQueryDecoder()
			var b QwpColumnBatch
			err := dec.decode(buf, &b)
			assertDecodeErrContains(t, err, "table_count")
		}
	})

	t.Run("H4b_TableCountNonZeroOnResultEnd", func(t *testing.T) {
		// Spec §4 / §8: RESULT_END must carry table_count = 0.
		frame := writeQwpFrame(0, buildResultEndBody(1, 0, 0))
		binary.LittleEndian.PutUint16(
			frame[qwpHeaderOffsetTableCount:qwpHeaderOffsetTableCount+2], 1)
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeResultEnd(frame)
		assertDecodeErrContains(t, err, "table_count")
	})

	t.Run("H4c_TableCountNonZeroOnQueryError", func(t *testing.T) {
		// Spec §4 / §9: QUERY_ERROR must carry table_count = 0.
		frame := writeQwpFrame(0, buildQueryErrorBody(1, byte(QwpStatusParseError), "bad", -1))
		binary.LittleEndian.PutUint16(
			frame[qwpHeaderOffsetTableCount:qwpHeaderOffsetTableCount+2], 1)
		dec := newTestQueryDecoder()
		_, err := dec.decodeQueryError(frame)
		assertDecodeErrContains(t, err, "table_count")
	})

	t.Run("H4d_TableCountNonZeroOnExecDone", func(t *testing.T) {
		// Spec §4 / §11.6: EXEC_DONE must carry table_count = 0.
		frame := writeQwpFrame(0, buildExecDoneBody(1, 0, 0))
		binary.LittleEndian.PutUint16(
			frame[qwpHeaderOffsetTableCount:qwpHeaderOffsetTableCount+2], 1)
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeExecDone(frame)
		assertDecodeErrContains(t, err, "table_count")
	})

	t.Run("H4e_TableCountNonZeroOnCacheReset", func(t *testing.T) {
		// Spec §4 / §11.7: CACHE_RESET must carry table_count = 0.
		frame := writeQwpFrame(0, buildCacheResetBody(qwpResetMaskDict))
		binary.LittleEndian.PutUint16(
			frame[qwpHeaderOffsetTableCount:qwpHeaderOffsetTableCount+2], 1)
		dec := newTestQueryDecoder()
		_, err := dec.decodeCacheReset(frame)
		assertDecodeErrContains(t, err, "table_count")
	})

	t.Run("H6_TableNameLengthOverflowVarint", func(t *testing.T) {
		// 10-byte varint with bit 63 set on byte 10.
		buf := writeMinimalResultBatchWithRawNameLenVarint([]byte{
			0x80, 0x80, 0x80, 0x80, 0x80,
			0x80, 0x80, 0x80, 0x80, 0x01,
		})
		dec := newTestQueryDecoder()
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
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "row_count")
	})

	t.Run("H7a_CellCountAmplificationRejected", func(t *testing.T) {
		// Cell-count amplification regression. An all-null column is nearly free on the wire (a
		// rowCount/8 null bitmap, zstd-compressible to almost nothing) yet
		// forces a rowCount-sized index array. row_count and column_count
		// are each individually within their caps here, but their product
		// overruns qwpMaxCellsPerBatch — a frame that, if decoded, would
		// drive a multi-GiB transient allocation. The decoder must reject
		// it up front, before the per-column loop sizes any index array.
		//
		// The frame carries the inline schema for every column but NO
		// column data: a decoder that skipped the cell-count guard would
		// fault later reading the first column's null section off the end
		// of the buffer, never with this "cell count" error.
		const rowCount = qwpMaxRowsPerBatch
		columnCount := int(qwpMaxCellsPerBatch/rowCount) + 1
		if columnCount > qwpMaxColumnsPerTable {
			t.Fatalf("test setup: columnCount %d exceeds the column cap", columnCount)
		}
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0)                   // batch_seq
		putVarintBytes(&buf, 0)                   // table_name_len
		putVarintBytes(&buf, uint64(rowCount))    // row_count
		putVarintBytes(&buf, uint64(columnCount)) // column_count
		// Inline schema: one tiny LONG column def each (1-byte name +
		// type code). No column data follows.
		for i := 0; i < columnCount; i++ {
			putVarintBytes(&buf, 1)
			buf.WriteByte('c')
			buf.WriteByte(byte(qwpTypeLong))
		}
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "cell count")
	})

	t.Run("H7b_CellCountAtCapNotRejectedByGuard", func(t *testing.T) {
		// Boundary: a batch whose cell count is exactly at the cap clears
		// the guard. The frame again carries no column data, so decoding
		// fails while reading the first column — proving the guard did NOT
		// fire (it would have produced a "cell count" error instead) and
		// that a maximal conformant batch is not rejected.
		const rowCount = qwpMaxRowsPerBatch
		columnCount := int(qwpMaxCellsPerBatch / rowCount) // exactly at the cap
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
		buf.WriteByte(qwpVersion)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		buf.WriteByte(byte(qwpMsgKindResultBatch))
		_ = binary.Write(&buf, binary.LittleEndian, uint64(1))
		putVarintBytes(&buf, 0)                   // batch_seq
		putVarintBytes(&buf, 0)                   // table_name_len
		putVarintBytes(&buf, uint64(rowCount))    // row_count
		putVarintBytes(&buf, uint64(columnCount)) // column_count
		for i := 0; i < columnCount; i++ {
			putVarintBytes(&buf, 1)
			buf.WriteByte('c')
			buf.WriteByte(byte(qwpTypeLong))
		}
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		if err == nil {
			t.Fatal("expected a truncation error reading the first column, got nil")
		}
		if containsAny(err.Error(), []string{"cell count"}) {
			t.Fatalf("cell-count guard fired at the cap boundary: %v", err)
		}
	})

	t.Run("H16_StringNegativeTotalBytes", func(t *testing.T) {
		buf := writeStringResultBatch(1, -1)
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "total bytes")
	})

	t.Run("H17_StringValidTotalBytesAccepted", func(t *testing.T) {
		buf := writeStringResultBatch(1, 5)
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		if err := dec.decode(buf, &b); err != nil {
			t.Fatalf("valid totalBytes rejected: %v", err)
		}
		if got := b.String(0, 0); got != "hello" {
			t.Fatalf("String = %q, want hello", got)
		}
	})

	t.Run("H17a_StringOffsetsNotMonotonic", func(t *testing.T) {
		// Row 0 spans [0, 8), row 1 spans [8, 5) — slicing would
		// panic in qwpStringSlice.
		buf := writeStringResultBatchCustom([]uint32{0, 8, 5}, []byte("helloworld"))
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "offset at index")
	})

	t.Run("H17b_StringOffsetExceedsTotalBytes", func(t *testing.T) {
		// Row 0 claims to run to offset 11 but totalBytes = 10 —
		// the final slice is length 10, so end=11 would panic.
		buf := writeStringResultBatchCustom([]uint32{0, 11, 10}, []byte("0123456789"))
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "offset at index")
	})

	t.Run("H17c_StringFirstOffsetNotZero", func(t *testing.T) {
		buf := writeStringResultBatchCustom([]uint32{3, 5}, []byte("hello"))
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "first offset")
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
		putVarintBytes(&buf, 1)
		buf.WriteByte('s')
		buf.WriteByte(0x08) // STRING — unsupported
		buf.WriteByte(0)    // null flag = 0
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "unsupported wire type")
	})

	t.Run("H26_ZstdFlagOnGarbageRejected", func(t *testing.T) {
		// FLAG_ZSTD set but the body after the prelude is plain
		// (uncompressed) bytes — the zstd frame-header parser rejects
		// as "invalid zstd frame header". Same guarantee as the old
		// "not yet supported" check: a malformed or mis-flagged batch
		// cannot sneak past the decoder.
		buf := writeMinimalResultBatch()
		buf[qwpHeaderOffsetFlags] |= qwpFlagZstd
		dec := newTestQueryDecoder()
		defer dec.close()
		var b QwpColumnBatch
		err := dec.decode(buf, &b)
		assertDecodeErrContains(t, err, "invalid zstd frame header")
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
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
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

		dec := newTestQueryDecoder()
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
		putVarintBytes(&buf, 0)                               // batch_seq
		putVarintBytes(&buf, 0)                               // table_name_len
		putVarintBytes(&buf, 0)                               // row_count
		putVarintBytes(&buf, uint64(qwpMaxColumnsPerTable)+1) // col_count
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
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
		putVarintBytes(&buf, 0)                            // batch_seq
		putVarintBytes(&buf, uint64(qwpMaxTableNameLen)+1) // name_len
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
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
		putVarintBytes(&buf, 0)                             // batch_seq
		putVarintBytes(&buf, 0)                             // table_name_len
		putVarintBytes(&buf, 0)                             // row_count
		putVarintBytes(&buf, 1)                             // col_count = 1
		putVarintBytes(&buf, uint64(qwpMaxColumnNameLen)+1) // col name_len
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
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
		putVarintBytes(&buf, 0)             // batch_seq
		putVarintBytes(&buf, 0)             // delta_start
		putVarintBytes(&buf, uint64(1)<<32) // delta_count = 2^32 (overflows uint32)
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "delta symbol section")
	})

	t.Run("H27_ArrayNegativeDim", func(t *testing.T) {
		// DOUBLE_ARRAY column, row_count=1, non-null, nDims=1,
		// shape[0] = -1 (as int32). The decoder must reject.
		frame := buildArrayHardeningFrame(t, 1, []int32{-1})
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "ARRAY dim")
	})

	t.Run("H27b_ArrayZeroDimIsEmptyArray", func(t *testing.T) {
		// shape[0] = 0 is a valid empty 1D array (cardinality 0), distinct
		// from a NULL array (which the null bitmap carries). The decoder
		// must accept it and report a non-null, zero-element row. The
		// per-dimension guard still collapses the element-count product to
		// 0, so the trailing padding bytes are simply left unconsumed.
		frame := buildArrayHardeningFrame(t, 1, []int32{0})
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		if err := dec.decode(frame, &b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if b.IsNull(0, 0) {
			t.Fatal("empty array must NOT be null")
		}
		if nd := b.ArrayNDims(0, 0); nd != 1 {
			t.Fatalf("ArrayNDims = %d, want 1", nd)
		}
		if d0 := b.ArrayDim(0, 0, 0); d0 != 0 {
			t.Fatalf("ArrayDim(0) = %d, want 0", d0)
		}
		if got := b.Float64Array(0, 0); got == nil || len(got) != 0 {
			t.Fatalf("Float64Array = %v (nil=%t), want non-nil empty", got, got == nil)
		}
	})

	t.Run("H28_ArrayElementCountExceeded", func(t *testing.T) {
		// Two dims whose product overflows qwpMaxArrayElements.
		big := int32(1<<20 + 1)
		frame := buildArrayHardeningFrame(t, 2, []int32{big, big})
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "element count")
	})

	t.Run("H29_ArrayNDimsOutOfRange", func(t *testing.T) {
		// nDims > qwpMaxArrayNDims is still rejected.
		frame := buildArrayHardeningFrame(t, qwpMaxArrayNDims+1, nil)
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "ARRAY nDims")
	})

	t.Run("H29b_ArrayNDimsZeroRejected", func(t *testing.T) {
		// The server always encodes NULL arrays via the null bitmap, so
		// an inline nDims=0 on a row the bitmap marked non-null is a
		// malformed frame. The decoder must reject it.
		frame := buildArrayHardeningFrame(t, 0, nil)
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "ARRAY nDims")
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
		putVarintBytes(&buf, 1) // col name_len
		buf.WriteByte('g')
		buf.WriteByte(byte(qwpTypeGeohash))
		buf.WriteByte(0)         // null flag
		putVarintBytes(&buf, 61) // precision > 60
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "geohash precision")
	})

	t.Run("H30b_GeohashPrecisionZero", func(t *testing.T) {
		// Lower bound: precision must be >= 1. The server enforces
		// [1, 60] on GEOLONG precision; a zero would drive
		// bytesPerValue = 0 into the length calculation. Mirror Java's
		// varintValue < 1 guard.
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
		putVarintBytes(&buf, 1) // col name_len
		buf.WriteByte('g')
		buf.WriteByte(byte(qwpTypeGeohash))
		buf.WriteByte(0)        // null flag
		putVarintBytes(&buf, 0) // precision < 1
		out := buf.Bytes()
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
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
	putVarintBytes(&buf, 1)
	buf.WriteByte('a')
	buf.WriteByte(byte(qwpTypeDoubleArray))
	buf.WriteByte(0) // null flag
	// Row body.
	buf.WriteByte(byte(nDims))
	for _, d := range shape {
		_ = binary.Write(&buf, binary.LittleEndian, d)
	}
	// Each caller's shape is either rejected at the shape/nDims check or
	// accepted as a 0-element (empty) array, so the decoder consumes no
	// element bytes. The 8 trailing bytes are slack so a short frame
	// can't raise a truncated-frame error that masks the behavior under
	// test.
	buf.Write(make([]byte, 8))
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:], uint32(len(out)-qwpHeaderSize))
	return out
}

// writeQwpFrame builds a complete QWP frame: a 12-byte header with the
// given flags plus the supplied body bytes. The body must start with the
// msg_kind byte. payload_length is patched in; table_count is written
// as 0, which spec §4 mandates for every non-RESULT_BATCH kind.
func writeQwpFrame(flags byte, body []byte) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, qwpMagic)
	buf.WriteByte(qwpVersion)
	buf.WriteByte(flags)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(0)) // table_count
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0)) // payload_length placeholder
	buf.Write(body)
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:],
		uint32(len(out)-qwpHeaderSize))
	return out
}

// buildResultEndBody assembles a RESULT_END body given requestId,
// finalSeq, and totalRows. Returns the msg_kind byte followed by the
// fixed and varint fields (no header).
func buildResultEndBody(requestId int64, finalSeq uint64, totalRows uint64) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(qwpMsgKindResultEnd))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(requestId))
	putVarintBytes(&buf, finalSeq)
	putVarintBytes(&buf, totalRows)
	return buf.Bytes()
}

// buildQueryErrorBody assembles a QUERY_ERROR body. rawMsgLen overrides
// the msg_len field on the wire (used to inject hostile values); pass -1
// to fall back to len(msg).
func buildQueryErrorBody(requestId int64, status byte, msg string, rawMsgLen int) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(qwpMsgKindQueryError))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(requestId))
	buf.WriteByte(status)
	msgLen := uint16(len(msg))
	if rawMsgLen >= 0 {
		msgLen = uint16(rawMsgLen)
	}
	_ = binary.Write(&buf, binary.LittleEndian, msgLen)
	buf.WriteString(msg)
	return buf.Bytes()
}

// buildExecDoneBody assembles an EXEC_DONE body.
func buildExecDoneBody(requestId int64, opType byte, rowsAffected uint64) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(qwpMsgKindExecDone))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(requestId))
	buf.WriteByte(opType)
	putVarintBytes(&buf, rowsAffected)
	return buf.Bytes()
}

func TestQwpDecoderResultEnd(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		frame := writeQwpFrame(0, buildResultEndBody(42, 7, 1234))
		dec := newTestQueryDecoder()
		reqId, total, err := dec.decodeResultEnd(frame)
		if err != nil {
			t.Fatalf("decodeResultEnd: %v", err)
		}
		if reqId != 42 {
			t.Fatalf("requestId = %d, want 42", reqId)
		}
		if total != 1234 {
			t.Fatalf("totalRows = %d, want 1234", total)
		}
	})

	t.Run("ZeroRows", func(t *testing.T) {
		frame := writeQwpFrame(0, buildResultEndBody(1, 0, 0))
		dec := newTestQueryDecoder()
		_, total, err := dec.decodeResultEnd(frame)
		if err != nil {
			t.Fatalf("decodeResultEnd: %v", err)
		}
		if total != 0 {
			t.Fatalf("totalRows = %d, want 0", total)
		}
	})

	t.Run("WrongMsgKind", func(t *testing.T) {
		body := buildResultEndBody(1, 0, 0)
		body[0] = byte(qwpMsgKindExecDone)
		frame := writeQwpFrame(0, body)
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeResultEnd(frame)
		assertDecodeErrContains(t, err, "expected RESULT_END")
	})

	t.Run("TruncatedBeforeRequestId", func(t *testing.T) {
		// Header + msg_kind only.
		frame := writeQwpFrame(0, []byte{byte(qwpMsgKindResultEnd)})
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeResultEnd(frame)
		assertDecodeErrContains(t, err, "end of buffer")
	})

	t.Run("TruncatedBeforeFinalSeq", func(t *testing.T) {
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindResultEnd))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeResultEnd(frame)
		assertDecodeErrContains(t, err, "truncated")
	})

	t.Run("TotalRowsVarintOverflow", func(t *testing.T) {
		// 10 bytes with continuation bit through byte 9 and a value
		// bit past bit 63 — rejects at readVarint's overflow guard.
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindResultEnd))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		putVarintBytes(&body, 0) // final_seq = 0
		body.Write([]byte{
			0x80, 0x80, 0x80, 0x80, 0x80,
			0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
		})
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeResultEnd(frame)
		if err == nil {
			t.Fatal("expected varint overflow error, got nil")
		}
	})

	t.Run("BadMagic", func(t *testing.T) {
		frame := writeQwpFrame(0, buildResultEndBody(1, 0, 0))
		frame[0] = 0xFF
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeResultEnd(frame)
		assertDecodeErrContains(t, err, "bad magic")
	})

	t.Run("ZstdFlagRejected", func(t *testing.T) {
		// FLAG_ZSTD is only valid on RESULT_BATCH; carrying it on a
		// RESULT_END frame is a protocol violation that the decoder
		// catches at the top of decodeResultEnd.
		frame := writeQwpFrame(qwpFlagZstd, buildResultEndBody(1, 0, 0))
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeResultEnd(frame)
		assertDecodeErrContains(t, err, "FLAG_ZSTD set on non-RESULT_BATCH")
	})
}

func TestQwpDecoderQueryError(t *testing.T) {
	// Port of Java QwpResultBatchDecoderHardeningTest.testQueryErrorValidMessageDecodes.
	t.Run("ValidMessageDecodes", func(t *testing.T) {
		frame := writeQwpFrame(0, buildQueryErrorBody(99, 0x05, "boom", -1))
		dec := newTestQueryDecoder()
		qe, err := dec.decodeQueryError(frame)
		if err != nil {
			t.Fatalf("decodeQueryError: %v", err)
		}
		if qe.RequestId != 99 {
			t.Fatalf("RequestId = %d, want 99", qe.RequestId)
		}
		if qe.Status != QwpStatusCode(0x05) {
			t.Fatalf("Status = 0x%02X, want 0x05", byte(qe.Status))
		}
		if qe.Message != "boom" {
			t.Fatalf("Message = %q, want %q", qe.Message, "boom")
		}
	})

	// Port of Java testQueryErrorMsgLenOverrunIsRejected: msgLen claims
	// 0xFFFF but the frame has no bytes of message.
	t.Run("MsgLenOverrunRejected", func(t *testing.T) {
		frame := writeQwpFrame(0, buildQueryErrorBody(0, 0, "", 0xFFFF))
		dec := newTestQueryDecoder()
		_, err := dec.decodeQueryError(frame)
		assertDecodeErrContains(t, err, "msg_len")
		if !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("expected 'exceeds' in error, got: %v", err)
		}
	})

	t.Run("EmptyMessage", func(t *testing.T) {
		frame := writeQwpFrame(0, buildQueryErrorBody(1, byte(qwpStatusCancelled), "", -1))
		dec := newTestQueryDecoder()
		qe, err := dec.decodeQueryError(frame)
		if err != nil {
			t.Fatalf("decodeQueryError: %v", err)
		}
		if qe.Status != qwpStatusCancelled {
			t.Fatalf("Status = 0x%02X, want CANCELLED", byte(qe.Status))
		}
		if qe.Message != "" {
			t.Fatalf("Message = %q, want empty", qe.Message)
		}
	})

	t.Run("CancelledStatusSurfaces", func(t *testing.T) {
		frame := writeQwpFrame(0, buildQueryErrorBody(1, byte(qwpStatusCancelled),
			"query cancelled", -1))
		dec := newTestQueryDecoder()
		qe, err := dec.decodeQueryError(frame)
		if err != nil {
			t.Fatalf("decodeQueryError: %v", err)
		}
		// Error() must mention CANCELLED and the message.
		if s := qe.Error(); !strings.Contains(s, "CANCELLED") ||
			!strings.Contains(s, "query cancelled") {
			t.Fatalf("Error() = %q, missing status name or message", s)
		}
	})

	t.Run("LimitExceededStatusSurfaces", func(t *testing.T) {
		frame := writeQwpFrame(0, buildQueryErrorBody(1, byte(qwpStatusLimitExceeded),
			"rows cap hit", -1))
		dec := newTestQueryDecoder()
		qe, err := dec.decodeQueryError(frame)
		if err != nil {
			t.Fatalf("decodeQueryError: %v", err)
		}
		if qe.Status != qwpStatusLimitExceeded {
			t.Fatalf("Status = 0x%02X, want LIMIT_EXCEEDED", byte(qe.Status))
		}
	})

	t.Run("WrongMsgKind", func(t *testing.T) {
		// Use a non-RESULT_BATCH stand-in (RESULT_END, table_count=0)
		// so the per-kind QUERY_ERROR check is what fires, not the
		// table_count guard inside parseFrameHeader.
		body := buildQueryErrorBody(1, 0x05, "x", -1)
		body[0] = byte(qwpMsgKindResultEnd)
		frame := writeQwpFrame(0, body)
		dec := newTestQueryDecoder()
		_, err := dec.decodeQueryError(frame)
		assertDecodeErrContains(t, err, "expected QUERY_ERROR")
	})

	t.Run("TruncatedBeforeStatus", func(t *testing.T) {
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindQueryError))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, err := dec.decodeQueryError(frame)
		assertDecodeErrContains(t, err, "end of buffer")
	})

	t.Run("TruncatedBeforeMsgLen", func(t *testing.T) {
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindQueryError))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		body.WriteByte(0x05)
		// Only 1 byte after status — msg_len needs 2.
		body.WriteByte(0x00)
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, err := dec.decodeQueryError(frame)
		assertDecodeErrContains(t, err, "end of buffer")
	})

	t.Run("UnicodeMessage", func(t *testing.T) {
		msg := "ünïcødé ⚠"
		frame := writeQwpFrame(0, buildQueryErrorBody(1, 0x06, msg, -1))
		dec := newTestQueryDecoder()
		qe, err := dec.decodeQueryError(frame)
		if err != nil {
			t.Fatalf("decodeQueryError: %v", err)
		}
		if qe.Message != msg {
			t.Fatalf("Message = %q, want %q", qe.Message, msg)
		}
	})
}

func TestQwpDecoderExecDone(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		frame := writeQwpFrame(0, buildExecDoneBody(100, 0x04, 42))
		dec := newTestQueryDecoder()
		reqId, res, err := dec.decodeExecDone(frame)
		if err != nil {
			t.Fatalf("decodeExecDone: %v", err)
		}
		if reqId != 100 {
			t.Fatalf("requestId = %d, want 100", reqId)
		}
		if res.OpType != 0x04 {
			t.Fatalf("OpType = 0x%02X, want 0x04", res.OpType)
		}
		if res.RowsAffected != 42 {
			t.Fatalf("RowsAffected = %d, want 42", res.RowsAffected)
		}
	})

	t.Run("PureDDLZeroRows", func(t *testing.T) {
		frame := writeQwpFrame(0, buildExecDoneBody(1, 0x01, 0))
		dec := newTestQueryDecoder()
		_, res, err := dec.decodeExecDone(frame)
		if err != nil {
			t.Fatalf("decodeExecDone: %v", err)
		}
		if res.RowsAffected != 0 {
			t.Fatalf("RowsAffected = %d, want 0", res.RowsAffected)
		}
	})

	t.Run("WrongMsgKind", func(t *testing.T) {
		body := buildExecDoneBody(1, 0x01, 0)
		body[0] = byte(qwpMsgKindQueryError)
		frame := writeQwpFrame(0, body)
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeExecDone(frame)
		assertDecodeErrContains(t, err, "expected EXEC_DONE")
	})

	t.Run("TruncatedBeforeOpType", func(t *testing.T) {
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindExecDone))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeExecDone(frame)
		assertDecodeErrContains(t, err, "end of buffer")
	})

	t.Run("TruncatedBeforeRowsAffected", func(t *testing.T) {
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindExecDone))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		body.WriteByte(0x04)
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeExecDone(frame)
		assertDecodeErrContains(t, err, "truncated")
	})

	t.Run("RowsAffectedVarintOverflow", func(t *testing.T) {
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindExecDone))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		body.WriteByte(0x04)
		body.Write([]byte{
			0x80, 0x80, 0x80, 0x80, 0x80,
			0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
		})
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeExecDone(frame)
		if err == nil {
			t.Fatal("expected varint overflow error, got nil")
		}
	})

	t.Run("RowsAffectedInt63Overflow", func(t *testing.T) {
		// 10-byte varint encoding exactly 2^63 — a valid uint64 but
		// readVarintInt63 rejects because the int64 cast sign-flips.
		// 9 continuation bytes of zero, then 0x01 (bit 63).
		var body bytes.Buffer
		body.WriteByte(byte(qwpMsgKindExecDone))
		_ = binary.Write(&body, binary.LittleEndian, uint64(1))
		body.WriteByte(0x04)
		body.Write([]byte{
			0x80, 0x80, 0x80, 0x80, 0x80,
			0x80, 0x80, 0x80, 0x80, 0x01,
		})
		frame := writeQwpFrame(0, body.Bytes())
		dec := newTestQueryDecoder()
		_, _, err := dec.decodeExecDone(frame)
		assertDecodeErrContains(t, err, "int63")
	})
}

// buildCacheResetBody assembles a CACHE_RESET body: msg_kind + 1-byte
// reset_mask. Returned bytes are ready to drop into writeQwpFrame.
func buildCacheResetBody(mask byte) []byte {
	return []byte{byte(qwpMsgKindCacheReset), mask}
}

func TestQwpDecoderCacheReset(t *testing.T) {
	t.Run("RoundTripMaskValues", func(t *testing.T) {
		// The defined dict bit, a reserved bit (0x02, formerly the
		// schemas bit), their combination, and the zero reset. The
		// decoder surfaces the byte verbatim — the I/O layer is what
		// maps bits to cache clears.
		for _, mask := range []byte{
			0x00,
			qwpResetMaskDict,
			0x02,
			qwpResetMaskDict | 0x02,
		} {
			frame := writeQwpFrame(0, buildCacheResetBody(mask))
			dec := newTestQueryDecoder()
			got, err := dec.decodeCacheReset(frame)
			if err != nil {
				t.Fatalf("mask=0x%02X: decodeCacheReset: %v", mask, err)
			}
			if got != mask {
				t.Fatalf("mask=0x%02X: got 0x%02X", mask, got)
			}
		}
	})

	t.Run("UnknownMaskBitsPreserved", func(t *testing.T) {
		// The decoder must not filter unknown bits — a future server
		// extension may introduce new bits, and rejecting them would
		// make forward compatibility impossible. Caller (applyCacheReset)
		// ignores bits it does not recognise; decode preserves them.
		frame := writeQwpFrame(0, buildCacheResetBody(0xFF))
		dec := newTestQueryDecoder()
		got, err := dec.decodeCacheReset(frame)
		if err != nil {
			t.Fatalf("decodeCacheReset: %v", err)
		}
		if got != 0xFF {
			t.Fatalf("mask=0x%02X, want 0xFF", got)
		}
	})

	t.Run("WrongMsgKind", func(t *testing.T) {
		body := buildCacheResetBody(qwpResetMaskDict)
		body[0] = byte(qwpMsgKindResultEnd)
		frame := writeQwpFrame(0, body)
		dec := newTestQueryDecoder()
		_, err := dec.decodeCacheReset(frame)
		assertDecodeErrContains(t, err, "expected CACHE_RESET")
	})

	t.Run("TruncatedBeforeMask", func(t *testing.T) {
		// Header + msg_kind only, reset_mask missing. Java mirrors this
		// with "CACHE_RESET frame truncated before reset_mask".
		frame := writeQwpFrame(0, []byte{byte(qwpMsgKindCacheReset)})
		dec := newTestQueryDecoder()
		_, err := dec.decodeCacheReset(frame)
		assertDecodeErrContains(t, err, "truncated before reset_mask")
	})

	t.Run("BadMagic", func(t *testing.T) {
		frame := writeQwpFrame(0, buildCacheResetBody(qwpResetMaskDict))
		frame[0] = 0xFF
		dec := newTestQueryDecoder()
		_, err := dec.decodeCacheReset(frame)
		assertDecodeErrContains(t, err, "bad magic")
	})

	t.Run("ZstdFlagRejected", func(t *testing.T) {
		// CACHE_RESET is a 2-byte control frame; FLAG_ZSTD is only
		// valid on RESULT_BATCH. Match the other non-RESULT_BATCH
		// decoder guards.
		frame := writeQwpFrame(qwpFlagZstd, buildCacheResetBody(qwpResetMaskDict))
		dec := newTestQueryDecoder()
		_, err := dec.decodeCacheReset(frame)
		assertDecodeErrContains(t, err, "FLAG_ZSTD set on non-RESULT_BATCH")
	})
}

func TestQwpDecoderApplyCacheReset(t *testing.T) {
	// Decode a frame that populates the connection dict (delta with
	// three symbols), then exercise applyCacheReset with each mask and
	// assert the dict is cleared only when the dict bit is set. The
	// schema is per-query (reset at query start), not a connection
	// cache, so CACHE_RESET no longer touches it.
	seedDecoder := func() qwpQueryDecoder {
		globalDict := []string{"AAPL", "MSFT", "GOOG"}
		tb := newQwpTableBuffer("t")
		for _, id := range []int32{0, 1, 2} {
			col, _ := tb.getOrCreateColumn("s", qwpTypeSymbol, false)
			col.addSymbolID(id)
			tb.commitRow()
		}
		var enc qwpEncoder
		ingress := enc.encodeTableWithDeltaDict(tb, globalDict, -1, 2)
		frame := wrapAsResultBatch(ingress, 1, 0)
		dec := newTestQueryDecoder()
		var b QwpColumnBatch
		if err := dec.decode(frame, &b); err != nil {
			t.Fatalf("seed decode: %v", err)
		}
		if dec.dict.size() != 3 {
			t.Fatalf("seed dict size = %d, want 3", dec.dict.size())
		}
		return dec
	}

	t.Run("MaskZeroIsNoOp", func(t *testing.T) {
		dec := seedDecoder()
		dec.applyCacheReset(0)
		if dec.dict.size() != 3 {
			t.Errorf("dict mutated by zero mask: size=%d", dec.dict.size())
		}
	})

	t.Run("DictBitClearsDict", func(t *testing.T) {
		dec := seedDecoder()
		dec.applyCacheReset(qwpResetMaskDict)
		if dec.dict.size() != 0 {
			t.Errorf("dict not cleared: size=%d", dec.dict.size())
		}
	})

	t.Run("UnknownBitsIgnored", func(t *testing.T) {
		// 0xF0 touches none of the defined reset bits — the dict must be
		// preserved for forward compat. 0x02 (formerly the schemas bit)
		// is now reserved and likewise clears nothing.
		dec := seedDecoder()
		dec.applyCacheReset(0xF0)
		if dec.dict.size() != 3 {
			t.Errorf("dict cleared by unknown bits: size=%d", dec.dict.size())
		}
		dec.applyCacheReset(0x02)
		if dec.dict.size() != 3 {
			t.Errorf("dict cleared by reserved bit 0x02: size=%d", dec.dict.size())
		}
	})
}

// TestQwpConnDictClearDetachesSnapshot documents the core safety
// invariant of qwpConnDict.clear: a snapshot a user handler is still
// iterating on a prior batch must keep reading the original bytes,
// even after clear() followed by a fresh appendDelta fills the new
// backing array. [:0] reuse would fail this test because the new
// symbols would overwrite the old heap region the snapshot aliases.
func TestQwpConnDictClearDetachesSnapshot(t *testing.T) {
	var dict qwpConnDict

	// Prime with three symbols.
	seedBytes := buildDeltaBytes(0, []string{"AAPL", "MSFT", "GOOG"})
	var br qwpByteReader
	br.reset(seedBytes)
	if err := dict.appendDelta(&br); err != nil {
		t.Fatalf("seed appendDelta: %v", err)
	}

	// Take a snapshot — simulates a user handler iterating a batch.
	snap := dict.snapshot()

	// Reset and append different symbols — these must land in a fresh
	// backing array so the snapshot's heap remains untouched.
	dict.clear()
	replacementBytes := buildDeltaBytes(0, []string{"ZZZZ", "YYYY", "XXXX"})
	br.reset(replacementBytes)
	if err := dict.appendDelta(&br); err != nil {
		t.Fatalf("post-clear appendDelta: %v", err)
	}

	want := []string{"AAPL", "MSFT", "GOOG"}
	for i, w := range want {
		e := snap.entries[i]
		got := string(snap.heap[e.offset : e.offset+e.length])
		if got != w {
			t.Fatalf("snapshot[%d] = %q, want %q (clear did not detach snapshot)", i, got, w)
		}
	}
}

// TestQwpConnDictClearPreservesCapacity checks that clear() retains
// the backing-array capacity so a workload that churns just above the
// server's soft cap does not reallocate on every CACHE_RESET. The
// invariant matches the Java client's QwpResultBatchDecoder comment
// on applyCacheReset.
func TestQwpConnDictClearPreservesCapacity(t *testing.T) {
	var dict qwpConnDict
	// Grow the dict to a non-trivial size so cap is well above the
	// initial empty.
	var br qwpByteReader
	br.reset(buildDeltaBytes(0, []string{"AAAA", "BBBB", "CCCC", "DDDD"}))
	if err := dict.appendDelta(&br); err != nil {
		t.Fatalf("seed appendDelta: %v", err)
	}
	heapCapBefore := cap(dict.heap)
	entriesCapBefore := cap(dict.entries)
	if heapCapBefore == 0 || entriesCapBefore == 0 {
		t.Fatalf("precondition: caps must be non-zero (heap=%d entries=%d)",
			heapCapBefore, entriesCapBefore)
	}

	dict.clear()

	if cap(dict.heap) < heapCapBefore {
		t.Errorf("heap cap shrunk after clear: before=%d after=%d",
			heapCapBefore, cap(dict.heap))
	}
	if cap(dict.entries) < entriesCapBefore {
		t.Errorf("entries cap shrunk after clear: before=%d after=%d",
			entriesCapBefore, cap(dict.entries))
	}
	if len(dict.heap) != 0 || len(dict.entries) != 0 {
		t.Errorf("cleared dict not empty: heap=%d entries=%d",
			len(dict.heap), len(dict.entries))
	}
}

// TestQwpConnDictRejectsOversizedDeltaCount verifies the per-connection
// entry-count cap blocks a hostile (or buggy) server frame that would
// otherwise grow the dict past the bound the uint32 entry offset assumes.
func TestQwpConnDictRejectsOversizedDeltaCount(t *testing.T) {
	var dict qwpConnDict
	var buf bytes.Buffer
	putVarintBytes(&buf, 0) // deltaStart
	// One past the cap — server-side framing must reject this even
	// before we try to allocate.
	putVarintBytes(&buf, uint64(qwpMaxConnDictSize)+1)
	var br qwpByteReader
	br.reset(buf.Bytes())
	err := dict.appendDelta(&br)
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("expected out-of-range error, got %v", err)
	}
}

// TestQwpConnDictRejectsOversizedHeap verifies the per-connection heap
// cap blocks a single delta entry whose length would push the heap past
// the cap. Tests with a synthetic short header — the appendDelta loop
// must check before allocating, since uint32 offset overflow on the
// next entry would be silent corruption.
func TestQwpConnDictRejectsOversizedHeap(t *testing.T) {
	var dict qwpConnDict
	var buf bytes.Buffer
	putVarintBytes(&buf, 0) // deltaStart
	putVarintBytes(&buf, 1) // deltaCount = 1
	// Advertise an entry length larger than the heap cap. Only the
	// header is read; the loop must reject on the cap check before
	// looking at the body.
	putVarintBytes(&buf, uint64(qwpMaxConnDictHeapBytes)+1)
	var br qwpByteReader
	br.reset(buf.Bytes())
	err := dict.appendDelta(&br)
	if err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("expected exceeds-cap error, got %v", err)
	}
}

// buildDeltaBytes emits a (deltaStart + deltaCount + per-entry
// len+bytes) block as appendDelta expects to read.
func buildDeltaBytes(deltaStart int, entries []string) []byte {
	var buf bytes.Buffer
	putVarintBytes(&buf, uint64(deltaStart))
	putVarintBytes(&buf, uint64(len(entries)))
	for _, s := range entries {
		putVarintBytes(&buf, uint64(len(s)))
		buf.WriteString(s)
	}
	return buf.Bytes()
}

func assertDecodeErrContains(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
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

// --- zstd helpers ---

// zstdCompressForTest compresses src using a real klauspost encoder
// configured to always write the FrameContentSize field, matching what
// the server's libzstd encoder produces with its default
// ZSTD_c_contentSizeFlag=on.
//
// WithSingleSegment(true) is required because klauspost omits the FCS
// field for frames <256 bytes in multi-segment mode (see frameenc.go).
// libzstd has no such behavior — when the source size is known it
// always emits FCS. Without the flag our small test payloads would
// produce HasFCS=false frames, which the decoder correctly rejects as
// a protocol violation, but that is not what we want to exercise on
// the happy path. SingleSegment changes no decoded bytes — only the
// presence of the FCS field.
func zstdCompressForTest(t *testing.T, src []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
		zstd.WithSingleSegment(true),
	)
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	defer enc.Close()
	return enc.EncodeAll(src, nil)
}

// compressResultBatchBody rewrites a RESULT_BATCH frame so its
// post-prelude body is zstd-compressed. Input must be the raw output
// of wrapAsResultBatch (uncompressed). The header's FLAG_ZSTD bit is
// set and the payload-length field is rewritten to reflect the
// shorter compressed body.
//
// Layout mirrors QwpWebSocketEncoder.java:
//
//	[12 header (FLAG_ZSTD set)] [msg_kind:1] [requestId:8] [batchSeq:varint] [ZSTD(delta + table block)]
func compressResultBatchBody(t *testing.T, frame []byte) []byte {
	t.Helper()
	if len(frame) < qwpHeaderSize+1+8+1 {
		t.Fatalf("compressResultBatchBody: frame too short (%d)", len(frame))
	}
	// Re-parse the prelude to know where the compressible body starts.
	// msg_kind(1) + requestId(8) + batchSeq(varint)
	p := qwpHeaderSize
	if frame[p] != byte(qwpMsgKindResultBatch) {
		t.Fatalf("compressResultBatchBody: msg_kind = 0x%02X, want RESULT_BATCH",
			frame[p])
	}
	p += 1 + 8
	_, n := binary.Uvarint(frame[p:])
	if n <= 0 {
		t.Fatalf("compressResultBatchBody: bad batchSeq varint at offset %d", p)
	}
	p += n

	prelude := frame[qwpHeaderSize:p]
	body := frame[p:]
	compressed := zstdCompressForTest(t, body)

	out := make([]byte, 0, qwpHeaderSize+len(prelude)+len(compressed))
	out = append(out, frame[:qwpHeaderSize]...)
	out = append(out, prelude...)
	out = append(out, compressed...)
	// Set FLAG_ZSTD on the header and patch the payload length.
	out[qwpHeaderOffsetFlags] |= qwpFlagZstd
	binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:],
		uint32(len(out)-qwpHeaderSize))
	return out
}

// --- zstd decoder tests ---

func TestQwpDecoderZstdHappyPath(t *testing.T) {
	// Encoder-driven positive path: build a real RESULT_BATCH with a
	// handful of rows, compress the body with klauspost's zstd encoder,
	// and decode through the production decompression path. Asserts
	// every typed accessor reads the same values as the uncompressed
	// reference.
	tb := newQwpTableBuffer("t")
	for _, v := range []int64{1, -2, 1234567890, math.MaxInt64} {
		col, err := tb.getOrCreateColumn("x", qwpTypeLong, false)
		if err != nil {
			t.Fatalf("getOrCreateColumn: %v", err)
		}
		col.addLong(v)
		tb.commitRow()
	}
	var enc qwpEncoder
	ingress := enc.encodeTable(tb)
	raw := wrapAsResultBatch(ingress, 42, 0)
	compressed := compressResultBatchBody(t, raw)

	if compressed[qwpHeaderOffsetFlags]&qwpFlagZstd == 0 {
		t.Fatal("compressResultBatchBody did not set FLAG_ZSTD")
	}
	if len(compressed) >= len(raw) {
		// Small frames may not compress — we still want to assert the
		// decoder succeeds either way. Log for visibility.
		t.Logf("compressed frame (%d bytes) >= raw (%d bytes)",
			len(compressed), len(raw))
	}

	dec := newTestQueryDecoder()
	defer dec.close()
	var b QwpColumnBatch
	if err := dec.decode(compressed, &b); err != nil {
		t.Fatalf("decode(zstd): %v", err)
	}
	if b.RequestId() != 42 {
		t.Fatalf("RequestId = %d, want 42", b.RequestId())
	}
	if b.BatchSeq() != 0 {
		t.Fatalf("BatchSeq = %d, want 0", b.BatchSeq())
	}
	if b.RowCount() != 4 {
		t.Fatalf("RowCount = %d, want 4", b.RowCount())
	}
	for i, w := range []int64{1, -2, 1234567890, math.MaxInt64} {
		if got := b.Int64(0, i); got != w {
			t.Fatalf("Int64[%d] = %d, want %d", i, got, w)
		}
	}
	if len(b.zstdScratch) == 0 {
		t.Fatal("zstdScratch empty after compressed decode")
	}
}

func TestQwpDecoderZstdReusesScratchAcrossDecodes(t *testing.T) {
	// Decode two compressed batches into the SAME QwpColumnBatch.
	// The decoder's zstd scratch is per-batch (on QwpColumnBatch, not
	// on the decoder), so batch N+1's decompressed bytes must land in
	// the same backing array as batch N — growing only if N+1 needs
	// more capacity.
	build := func(v int64, batchSeq uint64) []byte {
		tb := newQwpTableBuffer("t")
		col, err := tb.getOrCreateColumn("x", qwpTypeLong, false)
		if err != nil {
			t.Fatalf("getOrCreateColumn: %v", err)
		}
		col.addLong(v)
		tb.commitRow()
		var enc qwpEncoder
		ingress := enc.encodeTable(tb)
		raw := wrapAsResultBatch(ingress, 1, batchSeq)
		return compressResultBatchBody(t, raw)
	}

	dec := newTestQueryDecoder()
	defer dec.close()
	var b QwpColumnBatch

	// Batch 0 carries the schema; the decoder holds it for the query.
	if err := dec.decode(build(111, 0), &b); err != nil {
		t.Fatalf("first decode: %v", err)
	}
	if got := b.Int64(0, 0); got != 111 {
		t.Fatalf("first Int64 = %d, want 111", got)
	}
	scratchCap0 := cap(b.zstdScratch)

	// Batch 1 is a continuation (no inline schema); it reuses batch 0's.
	if err := dec.decode(build(222, 1), &b); err != nil {
		t.Fatalf("second decode: %v", err)
	}
	if got := b.Int64(0, 0); got != 222 {
		t.Fatalf("second Int64 = %d, want 222", got)
	}
	// Capacity should not shrink; rarely grows because batch 2 is the
	// same shape as batch 1. Either outcome is valid — this asserts the
	// amortisation invariant (cap is at least what we had before).
	if cap(b.zstdScratch) < scratchCap0 {
		t.Fatalf("zstdScratch cap shrank: was %d, now %d",
			scratchCap0, cap(b.zstdScratch))
	}
}

func TestQwpDecoderZstdHardening(t *testing.T) {
	// Build a reusable uncompressed frame that every subtest derives
	// from via surgical header / body mutation.
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("x", qwpTypeLong, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	col.addLong(99)
	tb.commitRow()
	var enc qwpEncoder
	baseRaw := wrapAsResultBatch(enc.encodeTable(tb), 1, 0)

	t.Run("InvalidZstdFrame", func(t *testing.T) {
		// FLAG_ZSTD set but the body is plain (uncompressed) bytes —
		// the Header.Decode call rejects.
		frame := make([]byte, len(baseRaw))
		copy(frame, baseRaw)
		frame[qwpHeaderOffsetFlags] |= qwpFlagZstd
		dec := newTestQueryDecoder()
		defer dec.close()
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "invalid zstd frame header")
	})

	t.Run("TruncatedZstdStream", func(t *testing.T) {
		// Compress the body, then truncate the final byte so zstd
		// DecodeAll fails mid-stream. Header.Decode still succeeds
		// because the header lives at the front of the frame.
		frame := compressResultBatchBody(t, baseRaw)
		frame = frame[:len(frame)-1]
		// Patch payload length to reflect the shorter body.
		binary.LittleEndian.PutUint32(frame[qwpHeaderOffsetPayloadLen:],
			uint32(len(frame)-qwpHeaderSize))
		dec := newTestQueryDecoder()
		defer dec.close()
		var b QwpColumnBatch
		err := dec.decode(frame, &b)
		assertDecodeErrContains(t, err, "zstd decompression failed")
	})

	t.Run("MissingContentSize", func(t *testing.T) {
		// A streaming zstd encoder with no pre-declared size writes
		// a frame where HasFCS=false. Protocol violation per Java —
		// the decoder must reject without decompressing.
		//
		// Build the body as usual but run it through NewWriter +
		// Write + Close rather than EncodeAll.
		p := qwpHeaderSize + 1 + 8
		_, n := binary.Uvarint(baseRaw[p:])
		p += n
		body := baseRaw[p:]

		var cbuf bytes.Buffer
		w, err := zstd.NewWriter(&cbuf,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if _, err := w.Write(body); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		compressed := cbuf.Bytes()

		// Sanity: verify the streaming writer really didn't set FCS.
		// If klauspost changes its behavior in a future version we
		// want to know here instead of in a confusing test failure
		// downstream.
		var hdr zstd.Header
		if err := hdr.Decode(compressed); err != nil {
			t.Fatalf("streaming zstd header.Decode: %v", err)
		}
		if hdr.HasFCS {
			t.Skip("streaming zstd writer emitted HasFCS=true; skipping")
		}

		out := make([]byte, 0, p+len(compressed))
		out = append(out, baseRaw[:p]...)
		out = append(out, compressed...)
		out[qwpHeaderOffsetFlags] |= qwpFlagZstd
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:],
			uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
		defer dec.close()
		var b QwpColumnBatch
		err = dec.decode(out, &b)
		assertDecodeErrContains(t, err, "zstd frame missing content size")
	})

	t.Run("ContentSizeExceedsCap", func(t *testing.T) {
		// Hand-craft a minimal but valid zstd frame whose FCS field
		// declares a size just above the client cap. Header.Decode
		// must accept the header (FCS >64 MiB is valid zstd); the
		// decoder must reject before calling DecodeAll.
		//
		// zstd frame shape (RFC 8478 §3.1):
		//   magic(4) = 0xFD2FB528
		//   frame_header_descriptor(1) = 0b11100000
		//     (frame_content_size_flag=3 → 8-byte FCS,
		//      single_segment=1, no dict, no checksum)
		//   frame_content_size(8) = huge
		//   ... (blocks follow, but Header.Decode only needs the
		//   prelude)
		const hugeFCS = uint64(qwpZstdMaxDecompressedSize) + 1
		hdr := make([]byte, 0, 13)
		hdr = binary.LittleEndian.AppendUint32(hdr, 0xFD2FB528)
		hdr = append(hdr, 0b11100000)
		hdr = binary.LittleEndian.AppendUint64(hdr, hugeFCS)
		// Add a single "raw" block (3-byte header signaling 0-byte
		// last raw block) so Header.Decode succeeds on bounded input.
		// block_header: last=1, block_type=raw(0), block_size=0
		hdr = append(hdr, 0x01, 0x00, 0x00)

		// Splice into a frame.
		p := qwpHeaderSize + 1 + 8
		_, n := binary.Uvarint(baseRaw[p:])
		p += n
		out := make([]byte, 0, p+len(hdr))
		out = append(out, baseRaw[:p]...)
		out = append(out, hdr...)
		out[qwpHeaderOffsetFlags] |= qwpFlagZstd
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:],
			uint32(len(out)-qwpHeaderSize))

		dec := newTestQueryDecoder()
		defer dec.close()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "exceeds client cap")
	})

	t.Run("EmptyCompressedPayload", func(t *testing.T) {
		// FLAG_ZSTD set but there is nothing after the prelude. Not
		// a hostile case — just a wire-level bug we must surface
		// instead of calling Header.Decode on zero bytes.
		p := qwpHeaderSize + 1 + 8
		_, n := binary.Uvarint(baseRaw[p:])
		p += n
		out := make([]byte, p)
		copy(out, baseRaw[:p])
		out[qwpHeaderOffsetFlags] |= qwpFlagZstd
		binary.LittleEndian.PutUint32(out[qwpHeaderOffsetPayloadLen:],
			uint32(p-qwpHeaderSize))

		dec := newTestQueryDecoder()
		defer dec.close()
		var b QwpColumnBatch
		err := dec.decode(out, &b)
		assertDecodeErrContains(t, err, "FLAG_ZSTD set but no compressed payload")
	})
}

func TestQwpDecoderZstdCloseIsIdempotent(t *testing.T) {
	// decoder.close() must be safe to call more than once and must
	// cope with a never-initialised zstd decoder. Exercises the nil
	// branch of the close path.
	dec := newTestQueryDecoder()
	dec.close()
	dec.close()
}

func TestQwpColumnBatchCopyAllZstdSurvivesPoolReuse(t *testing.T) {
	// CopyAll must deep-clone the zstd scratch so a snapshot stays
	// valid after the decoder reuses the source batch's scratch for a
	// later frame. Without the clone + alias-translation branch in
	// CopyAll, the snapshot's byte-aliasing slices would drift onto
	// garbage bytes.
	buildStrings := func(values []string, batchSeq uint64) []byte {
		tb := newQwpTableBuffer("t")
		for _, v := range values {
			col, err := tb.getOrCreateColumn("s", qwpTypeVarchar, false)
			if err != nil {
				t.Fatalf("getOrCreateColumn: %v", err)
			}
			col.addString(v)
			tb.commitRow()
		}
		var enc qwpEncoder
		ingress := enc.encodeTable(tb)
		raw := wrapAsResultBatch(ingress, 1, batchSeq)
		return compressResultBatchBody(t, raw)
	}

	dec := newTestQueryDecoder()
	defer dec.close()
	var b QwpColumnBatch
	if err := dec.decode(buildStrings([]string{"hello", "world"}, 0), &b); err != nil {
		t.Fatalf("first decode: %v", err)
	}
	snap := b.CopyAll()
	if got := snap.String(0, 0); got != "hello" {
		t.Fatalf("snap[0] = %q, want %q", got, "hello")
	}

	// Decode a second batch (a continuation, reusing batch 0's schema)
	// into the SAME b. The decoder reuses b.zstdScratch — without the
	// deep-clone in CopyAll the snapshot would now see the second
	// batch's bytes.
	if err := dec.decode(buildStrings([]string{"x", "y"}, 1), &b); err != nil {
		t.Fatalf("second decode: %v", err)
	}
	if got := snap.String(0, 0); got != "hello" {
		t.Fatalf("snap[0] after reuse = %q, want %q (CopyAll didn't clone scratch)",
			got, "hello")
	}
	if got := snap.String(0, 1); got != "world" {
		t.Fatalf("snap[1] after reuse = %q, want %q",
			got, "world")
	}
}

// TestQwpDecoderResetQuerySchemaBlocksLeak pins the "schema never leaks across
// query boundaries" property. resetQuerySchema (which dispatcherRun calls at the
// start of every query) must drop the prior query's held schema so a
// continuation batch cannot decode against it: the decode path gates a
// batch_seq > 0 batch on querySchemaValid, and after a reset that gate is closed.
func TestQwpDecoderResetQuerySchemaBlocksLeak(t *testing.T) {
	dec := newTestQueryDecoder()
	var batch QwpColumnBatch

	schemaFrame := encodeSingleColumnBatch(t, "v", qwpTypeLong, false,
		[]func(*qwpColumnBuffer){func(c *qwpColumnBuffer) { c.addLong(1) }})
	if err := dec.decode(schemaFrame, &batch); err != nil {
		t.Fatalf("decode schema batch: %v", err)
	}
	if !dec.querySchemaValid || dec.querySchema == nil {
		t.Fatal("schema batch should leave a held, valid query schema")
	}

	dec.resetQuerySchema()

	if dec.querySchemaValid {
		t.Fatal("resetQuerySchema left querySchemaValid set; a continuation batch could reuse the prior query's schema")
	}
	if dec.querySchema != nil {
		t.Fatal("resetQuerySchema left the prior schema slice held")
	}
}
