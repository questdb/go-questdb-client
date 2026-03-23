/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
	"testing"
)

func TestQwpEncoderFixedWidthGoldenBytes(t *testing.T) {
	// Table "t" with 2 rows: column "a" (LONG), column "b" (DOUBLE).
	// Row 0: a=1, b=1.5
	// Row 1: a=2, b=2.5
	tb := newQwpTableBuffer("t")

	colA, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
	colA.addLong(1)
	colB, _ := tb.getOrCreateColumn("b", qwpTypeDouble, false)
	colB.addDouble(1.5)
	tb.commitRow()

	colA, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
	colA.addLong(2)
	colB, _ = tb.getOrCreateColumn("b", qwpTypeDouble, false)
	colB.addDouble(2.5)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Build expected bytes manually.
	var expected []byte

	// Header (12 bytes):
	// Magic "QWP1" as uint32 LE = 0x31505751
	expected = append(expected, 0x51, 0x57, 0x50, 0x31)
	// Version = 1
	expected = append(expected, 0x01)
	// Flags = 0
	expected = append(expected, 0x00)
	// TableCount = 1 (uint16 LE)
	expected = append(expected, 0x01, 0x00)
	// PayloadLength placeholder (will be patched)
	expected = append(expected, 0x00, 0x00, 0x00, 0x00)

	// Payload:
	// Table name "t": varint(1) + 't'
	expected = append(expected, 0x01, 0x74)
	// RowCount = 2: varint(2)
	expected = append(expected, 0x02)
	// ColCount = 2: varint(2)
	expected = append(expected, 0x02)
	// SchemaMode = FULL (0x00)
	expected = append(expected, 0x00)
	// Column "a": name varint(1) + 'a', type LONG (0x05)
	expected = append(expected, 0x01, 0x61, 0x05)
	// Column "b": name varint(1) + 'b', type DOUBLE (0x07)
	expected = append(expected, 0x01, 0x62, 0x07)

	// Column "a" data: 2 × int64 LE
	buf8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf8, 1)
	expected = append(expected, buf8...)
	binary.LittleEndian.PutUint64(buf8, 2)
	expected = append(expected, buf8...)

	// Column "b" data: 2 × float64 LE
	binary.LittleEndian.PutUint64(buf8, math.Float64bits(1.5))
	expected = append(expected, buf8...)
	binary.LittleEndian.PutUint64(buf8, math.Float64bits(2.5))
	expected = append(expected, buf8...)

	// Patch payload length: total - 12
	payloadLen := uint32(len(expected) - qwpHeaderSize)
	binary.LittleEndian.PutUint32(expected[qwpHeaderOffsetPayloadLen:], payloadLen)

	if !bytes.Equal(msg, expected) {
		t.Fatalf("encoded message mismatch:\ngot  = %x\nwant = %x", msg, expected)
	}
}

func TestQwpEncoderHeader(t *testing.T) {
	tb := newQwpTableBuffer("test")
	colA, _ := tb.getOrCreateColumn("x", qwpTypeLong, false)
	colA.addLong(42)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Verify header fields.
	if len(msg) < qwpHeaderSize {
		t.Fatalf("message too short: %d bytes", len(msg))
	}

	// Magic "QWP1"
	magic := binary.LittleEndian.Uint32(msg[0:4])
	if magic != qwpMagic {
		t.Fatalf("magic = 0x%08X, want 0x%08X", magic, qwpMagic)
	}

	// Version
	if msg[4] != qwpVersion {
		t.Fatalf("version = %d, want %d", msg[4], qwpVersion)
	}

	// Flags
	if msg[5] != 0x00 {
		t.Fatalf("flags = 0x%02X, want 0x00", msg[5])
	}

	// TableCount = 1
	tableCount := binary.LittleEndian.Uint16(msg[6:8])
	if tableCount != 1 {
		t.Fatalf("tableCount = %d, want 1", tableCount)
	}

	// PayloadLength = total - 12
	payloadLen := binary.LittleEndian.Uint32(msg[8:12])
	expectedLen := uint32(len(msg) - qwpHeaderSize)
	if payloadLen != expectedLen {
		t.Fatalf("payloadLength = %d, want %d", payloadLen, expectedLen)
	}
}

func TestQwpEncoderSchemaReference(t *testing.T) {
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
	col.addLong(10)
	tb.commitRow()

	schemaHash := tb.getSchemaHash()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeReference, schemaHash)

	// Parse past header (12) + table name "t" (2) + rowCount (1) + colCount (1).
	off := 12 + 2 + 1 + 1

	// Schema mode should be 0x01 (reference).
	if msg[off] != byte(qwpSchemaModeReference) {
		t.Fatalf("schemaMode = 0x%02X, want 0x%02X", msg[off], qwpSchemaModeReference)
	}
	off++

	// Schema hash: int64 LE (8 bytes).
	gotHash := int64(binary.LittleEndian.Uint64(msg[off : off+8]))
	if gotHash != schemaHash {
		t.Fatalf("schemaHash = %d, want %d", gotHash, schemaHash)
	}
	off += 8

	// Column data: 1 × int64 LE = 10.
	gotVal := int64(binary.LittleEndian.Uint64(msg[off : off+8]))
	if gotVal != 10 {
		t.Fatalf("column value = %d, want 10", gotVal)
	}
}

func TestQwpEncoderAllFixedTypes(t *testing.T) {
	tb := newQwpTableBuffer("types")

	colByte, _ := tb.getOrCreateColumn("b", qwpTypeByte, false)
	colByte.addByte(42)

	colShort, _ := tb.getOrCreateColumn("s", qwpTypeShort, false)
	colShort.addShort(1000)

	colInt, _ := tb.getOrCreateColumn("i", qwpTypeInt, false)
	colInt.addInt32(100000)

	colLong, _ := tb.getOrCreateColumn("l", qwpTypeLong, false)
	colLong.addLong(9876543210)

	colFloat, _ := tb.getOrCreateColumn("f", qwpTypeFloat, false)
	colFloat.addFloat32(3.14)

	colDouble, _ := tb.getOrCreateColumn("d", qwpTypeDouble, false)
	colDouble.addDouble(2.71828)

	colTs, _ := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
	colTs.addTimestamp(1234567890)

	colChar, _ := tb.getOrCreateColumn("ch", qwpTypeChar, false)
	colChar.addChar('A')

	colDate, _ := tb.getOrCreateColumn("dt", qwpTypeDate, false)
	colDate.addTimestamp(9999999999) // addTimestamp works for DATE too

	colUuid, _ := tb.getOrCreateColumn("u", qwpTypeUuid, false)
	colUuid.addUuid(0xAABBCCDDEEFF0011, 0x1122334455667788)

	colL256, _ := tb.getOrCreateColumn("l256", qwpTypeLong256, false)
	colL256.addLong256(1, 2, 3, 4)

	colTsNano, _ := tb.getOrCreateColumn("tsn", qwpTypeTimestampNano, false)
	colTsNano.addTimestamp(1234567890123456789)

	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Verify basic structure: message should be valid and non-empty.
	if len(msg) < qwpHeaderSize {
		t.Fatal("message too short")
	}

	// Verify payload length is consistent.
	payloadLen := binary.LittleEndian.Uint32(msg[8:12])
	if int(payloadLen) != len(msg)-qwpHeaderSize {
		t.Fatalf("payload length mismatch: header says %d, actual %d",
			payloadLen, len(msg)-qwpHeaderSize)
	}

	// Parse to verify column data integrity. We can decode the payload
	// and check that each column's data matches what we put in.
	off := qwpHeaderSize

	// Table name "types": varint(5) + "types"
	nameLen, n, _ := qwpReadVarint(msg[off:])
	off += n
	tableName := string(msg[off : off+int(nameLen)])
	off += int(nameLen)
	if tableName != "types" {
		t.Fatalf("tableName = %q, want %q", tableName, "types")
	}

	// RowCount
	rowCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", rowCount)
	}

	// ColCount
	colCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if colCount != 12 {
		t.Fatalf("colCount = %d, want 12", colCount)
	}

	// Schema mode
	if msg[off] != 0x00 {
		t.Fatalf("schemaMode = 0x%02X, want 0x00", msg[off])
	}
	off++

	// Skip schema definitions (12 columns).
	for i := 0; i < 12; i++ {
		nLen, n, _ := qwpReadVarint(msg[off:])
		off += n + int(nLen)
		off++ // type code
	}

	// Now verify column data bytes.
	// Column "b" (BYTE): 1 byte
	if msg[off] != 42 {
		t.Fatalf("byte col = %d, want 42", msg[off])
	}
	off++

	// Column "s" (SHORT): 2 bytes LE
	gotShort := int16(binary.LittleEndian.Uint16(msg[off:]))
	if gotShort != 1000 {
		t.Fatalf("short col = %d, want 1000", gotShort)
	}
	off += 2

	// Column "i" (INT): 4 bytes LE
	gotInt := int32(binary.LittleEndian.Uint32(msg[off:]))
	if gotInt != 100000 {
		t.Fatalf("int col = %d, want 100000", gotInt)
	}
	off += 4

	// Column "l" (LONG): 8 bytes LE
	gotLong := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotLong != 9876543210 {
		t.Fatalf("long col = %d, want 9876543210", gotLong)
	}
	off += 8

	// Column "f" (FLOAT): 4 bytes LE
	gotFloat := math.Float32frombits(binary.LittleEndian.Uint32(msg[off:]))
	if gotFloat != 3.14 {
		t.Fatalf("float col = %v, want 3.14", gotFloat)
	}
	off += 4

	// Column "d" (DOUBLE): 8 bytes LE
	gotDouble := math.Float64frombits(binary.LittleEndian.Uint64(msg[off:]))
	if gotDouble != 2.71828 {
		t.Fatalf("double col = %v, want 2.71828", gotDouble)
	}
	off += 8

	// Column "ts" (TIMESTAMP): 8 bytes LE
	gotTs := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotTs != 1234567890 {
		t.Fatalf("timestamp col = %d, want 1234567890", gotTs)
	}
	off += 8

	// Column "ch" (CHAR): 2 bytes LE
	gotChar := binary.LittleEndian.Uint16(msg[off:])
	if gotChar != 0x0041 {
		t.Fatalf("char col = 0x%04X, want 0x0041", gotChar)
	}
	off += 2

	// Column "dt" (DATE): 8 bytes LE
	gotDate := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotDate != 9999999999 {
		t.Fatalf("date col = %d, want 9999999999", gotDate)
	}
	off += 8

	// Column "u" (UUID): 16 bytes (lo LE then hi LE)
	gotLo := binary.LittleEndian.Uint64(msg[off:])
	gotHi := binary.LittleEndian.Uint64(msg[off+8:])
	if gotLo != 0x1122334455667788 || gotHi != 0xAABBCCDDEEFF0011 {
		t.Fatalf("uuid = lo:%016X hi:%016X, want lo:1122334455667788 hi:AABBCCDDEEFF0011", gotLo, gotHi)
	}
	off += 16

	// Column "l256" (LONG256): 32 bytes (4 × int64 LE)
	for i, want := range []uint64{1, 2, 3, 4} {
		got := binary.LittleEndian.Uint64(msg[off:])
		if got != want {
			t.Fatalf("long256[%d] = %d, want %d", i, got, want)
		}
		off += 8
	}

	// Column "tsn" (TIMESTAMP_NANOS): 8 bytes LE
	gotTsNano := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotTsNano != 1234567890123456789 {
		t.Fatalf("timestamp_nanos col = %d, want 1234567890123456789", gotTsNano)
	}
	off += 8

	// Verify we consumed all bytes.
	if off != len(msg) {
		t.Fatalf("did not consume all bytes: off=%d, len=%d", off, len(msg))
	}
}

func TestQwpEncoderNullableColumn(t *testing.T) {
	tb := newQwpTableBuffer("t")

	col, _ := tb.getOrCreateColumn("v", qwpTypeLong, true)
	col.addLong(100) // row 0: non-null
	tb.commitRow()

	col, _ = tb.getOrCreateColumn("v", qwpTypeLong, true)
	col.addNull() // row 1: null
	tb.commitRow()

	col, _ = tb.getOrCreateColumn("v", qwpTypeLong, true)
	col.addLong(200) // row 2: non-null
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Parse to column data.
	off := qwpHeaderSize

	// Table name "t": varint(1) + 't'
	off += 2
	// rowCount=3, colCount=1
	off += 1 + 1
	// schemaMode=FULL
	off++
	// Column "v": varint(1) + 'v' + typeCode (LONG|nullable = 0x85)
	off += 2
	if msg[off] != 0x85 {
		t.Fatalf("wireTypeCode = 0x%02X, want 0x85", msg[off])
	}
	off++

	// Null bitmap: (3+7)/8 = 1 byte. Row 1 is null → bit 1 → 0x02.
	if msg[off] != 0x02 {
		t.Fatalf("nullBitmap = 0x%02X, want 0x02", msg[off])
	}
	off++

	// Column data: 3 × int64 LE
	// Row 0: 100
	if int64(binary.LittleEndian.Uint64(msg[off:])) != 100 {
		t.Fatalf("row 0 = %d, want 100", int64(binary.LittleEndian.Uint64(msg[off:])))
	}
	off += 8

	// Row 1: null sentinel (MinInt64)
	if binary.LittleEndian.Uint64(msg[off:]) != qwpLongNull {
		t.Fatalf("row 1 = 0x%016X, want null sentinel", binary.LittleEndian.Uint64(msg[off:]))
	}
	off += 8

	// Row 2: 200
	if int64(binary.LittleEndian.Uint64(msg[off:])) != 200 {
		t.Fatalf("row 2 = %d, want 200", int64(binary.LittleEndian.Uint64(msg[off:])))
	}
	off += 8

	if off != len(msg) {
		t.Fatalf("unconsumed bytes: off=%d, len=%d", off, len(msg))
	}
}

func TestQwpEncoderMultipleColumns(t *testing.T) {
	// Verify encoder handles multiple columns correctly by
	// checking total message length.
	tb := newQwpTableBuffer("multi")

	colA, _ := tb.getOrCreateColumn("a", qwpTypeInt, false)
	colA.addInt32(1)
	colB, _ := tb.getOrCreateColumn("b", qwpTypeInt, false)
	colB.addInt32(2)
	colC, _ := tb.getOrCreateColumn("c", qwpTypeInt, false)
	colC.addInt32(3)
	tb.commitRow()

	colA, _ = tb.getOrCreateColumn("a", qwpTypeInt, false)
	colA.addInt32(4)
	colB, _ = tb.getOrCreateColumn("b", qwpTypeInt, false)
	colB.addInt32(5)
	colC, _ = tb.getOrCreateColumn("c", qwpTypeInt, false)
	colC.addInt32(6)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Expected payload:
	// tableName "multi": 1 + 5 = 6 bytes
	// rowCount=2: 1 byte
	// colCount=3: 1 byte
	// schemaMode: 1 byte
	// 3 columns × (varint(1) + name(1) + type(1)) = 9 bytes
	// 3 columns × 2 rows × 4 bytes = 24 bytes
	// Total payload = 6 + 1 + 1 + 1 + 9 + 24 = 42
	// Total message = 12 + 42 = 54

	payloadLen := binary.LittleEndian.Uint32(msg[8:12])
	if payloadLen != 42 {
		t.Fatalf("payloadLength = %d, want 42", payloadLen)
	}
	if len(msg) != 54 {
		t.Fatalf("message length = %d, want 54", len(msg))
	}
}

func TestQwpEncoderEmptyTable(t *testing.T) {
	// A table with 0 rows but known columns.
	tb := newQwpTableBuffer("empty")
	// Create columns but commit no rows.
	col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
	col.addLong(42)
	tb.cancelRow() // Cancel the in-progress row.

	// Now the table has columns defined but 0 committed rows.
	// After cancel, columns created during the row are removed.
	// So we need a different approach: commit a row, then reset.
	tb2 := newQwpTableBuffer("empty")
	col2, _ := tb2.getOrCreateColumn("x", qwpTypeLong, false)
	col2.addLong(1)
	tb2.commitRow()
	tb2.reset()

	var enc qwpEncoder
	msg := enc.encodeTable(tb2, qwpSchemaModeFull, 0)

	// Parse basic header.
	if len(msg) < qwpHeaderSize {
		t.Fatal("message too short")
	}
	payloadLen := binary.LittleEndian.Uint32(msg[8:12])
	if int(payloadLen) != len(msg)-qwpHeaderSize {
		t.Fatalf("payload length mismatch")
	}

	// After header: table name "empty" + rowCount=0 + colCount=1 +
	// schema + no column data (0 rows).
	off := qwpHeaderSize
	nameLen, n, _ := qwpReadVarint(msg[off:])
	off += n
	off += int(nameLen) // skip name
	rowCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if rowCount != 0 {
		t.Fatalf("rowCount = %d, want 0", rowCount)
	}
	colCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if colCount != 1 {
		t.Fatalf("colCount = %d, want 1", colCount)
	}
}

func TestQwpEncoderReuse(t *testing.T) {
	// Verify the encoder can be reused for multiple tables.
	var enc qwpEncoder

	tb1 := newQwpTableBuffer("t1")
	col, _ := tb1.getOrCreateColumn("a", qwpTypeLong, false)
	col.addLong(1)
	tb1.commitRow()

	msg1 := enc.encodeTable(tb1, qwpSchemaModeFull, 0)
	msg1Copy := make([]byte, len(msg1))
	copy(msg1Copy, msg1)

	tb2 := newQwpTableBuffer("t2")
	col, _ = tb2.getOrCreateColumn("b", qwpTypeDouble, false)
	col.addDouble(2.0)
	tb2.commitRow()

	msg2 := enc.encodeTable(tb2, qwpSchemaModeFull, 0)

	// msg1's backing buffer may have been reused, but msg1Copy is safe.
	// Verify msg2 encodes table "t2".
	off := qwpHeaderSize
	nameLen, n, _ := qwpReadVarint(msg2[off:])
	off += n
	name := string(msg2[off : off+int(nameLen)])
	if name != "t2" {
		t.Fatalf("table name = %q, want %q", name, "t2")
	}

	// Verify msg1Copy still has "t1".
	off = qwpHeaderSize
	nameLen, n, _ = qwpReadVarint(msg1Copy[off:])
	off += n
	name = string(msg1Copy[off : off+int(nameLen)])
	if name != "t1" {
		t.Fatalf("table name = %q, want %q", name, "t1")
	}
}

func TestQwpEncoderDecimalSchema(t *testing.T) {
	// Verify that decimal columns get the extra scale byte in schema.
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("d", qwpTypeDecimal64, false)
	if err := col.addDecimal(NewDecimalFromInt64(100, 3)); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Parse to schema.
	off := qwpHeaderSize
	// Table name "t": varint(1) + 't'
	off += 2
	// rowCount=1, colCount=1
	off += 1 + 1
	// schemaMode=FULL
	off++

	// Column "d": name varint(1) + 'd' = 2 bytes
	off += 2

	// Type code: DECIMAL64 = 0x13
	if msg[off] != 0x13 {
		t.Fatalf("typeCode = 0x%02X, want 0x13", msg[off])
	}
	off++

	// Scale byte: 3
	if msg[off] != 3 {
		t.Fatalf("scale = %d, want 3", msg[off])
	}
	off++

	// Column data: 8 bytes big-endian (100 = 0x64)
	expected := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64}
	if !bytes.Equal(msg[off:off+8], expected) {
		t.Fatalf("decimal data = %x, want %x", msg[off:off+8], expected)
	}
}
