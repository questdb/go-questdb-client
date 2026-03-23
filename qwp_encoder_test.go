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
	// Flags = 0x00 (no Gorilla, no compression)
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

	// Column "a" data: null bitmap flag (0x00) + 2 × int64 LE
	expected = append(expected, 0x00) // null bitmap flag: no nulls
	buf8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf8, 1)
	expected = append(expected, buf8...)
	binary.LittleEndian.PutUint64(buf8, 2)
	expected = append(expected, buf8...)

	// Column "b" data: null bitmap flag (0x00) + 2 × float64 LE
	expected = append(expected, 0x00) // null bitmap flag: no nulls
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

	// Flags = 0x00 (no Gorilla, no compression, no delta dict)
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

	// Column data: null bitmap flag (0x00) + 1 × int64 LE = 10.
	if msg[off] != 0x00 {
		t.Fatalf("null bitmap flag = 0x%02X, want 0x00", msg[off])
	}
	off++
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
	// Each column starts with null bitmap flag byte (0x00 = no nulls).

	// Column "b" (BYTE): flag + 1 byte
	off++ // null bitmap flag
	if msg[off] != 42 {
		t.Fatalf("byte col = %d, want 42", msg[off])
	}
	off++

	// Column "s" (SHORT): flag + 2 bytes LE
	off++ // null bitmap flag
	gotShort := int16(binary.LittleEndian.Uint16(msg[off:]))
	if gotShort != 1000 {
		t.Fatalf("short col = %d, want 1000", gotShort)
	}
	off += 2

	// Column "i" (INT): flag + 4 bytes LE
	off++ // null bitmap flag
	gotInt := int32(binary.LittleEndian.Uint32(msg[off:]))
	if gotInt != 100000 {
		t.Fatalf("int col = %d, want 100000", gotInt)
	}
	off += 4

	// Column "l" (LONG): flag + 8 bytes LE
	off++ // null bitmap flag
	gotLong := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotLong != 9876543210 {
		t.Fatalf("long col = %d, want 9876543210", gotLong)
	}
	off += 8

	// Column "f" (FLOAT): flag + 4 bytes LE
	off++ // null bitmap flag
	gotFloat := math.Float32frombits(binary.LittleEndian.Uint32(msg[off:]))
	if gotFloat != 3.14 {
		t.Fatalf("float col = %v, want 3.14", gotFloat)
	}
	off += 4

	// Column "d" (DOUBLE): flag + 8 bytes LE
	off++ // null bitmap flag
	gotDouble := math.Float64frombits(binary.LittleEndian.Uint64(msg[off:]))
	if gotDouble != 2.71828 {
		t.Fatalf("double col = %v, want 2.71828", gotDouble)
	}
	off += 8

	// Column "ts" (TIMESTAMP): flag + 8 bytes LE (no encoding prefix without Gorilla)
	off++ // null bitmap flag
	gotTs := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotTs != 1234567890 {
		t.Fatalf("timestamp col = %d, want 1234567890", gotTs)
	}
	off += 8

	// Column "ch" (CHAR): flag + 2 bytes LE
	off++ // null bitmap flag
	gotChar := binary.LittleEndian.Uint16(msg[off:])
	if gotChar != 0x0041 {
		t.Fatalf("char col = 0x%04X, want 0x0041", gotChar)
	}
	off += 2

	// Column "dt" (DATE): flag + 8 bytes LE
	off++ // null bitmap flag
	gotDate := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotDate != 9999999999 {
		t.Fatalf("date col = %d, want 9999999999", gotDate)
	}
	off += 8

	// Column "u" (UUID): flag + 16 bytes (lo LE then hi LE)
	off++ // null bitmap flag
	gotLo := binary.LittleEndian.Uint64(msg[off:])
	gotHi := binary.LittleEndian.Uint64(msg[off+8:])
	if gotLo != 0x1122334455667788 || gotHi != 0xAABBCCDDEEFF0011 {
		t.Fatalf("uuid = lo:%016X hi:%016X, want lo:1122334455667788 hi:AABBCCDDEEFF0011", gotLo, gotHi)
	}
	off += 16

	// Column "l256" (LONG256): flag + 32 bytes (4 × int64 LE)
	off++ // null bitmap flag
	for i, want := range []uint64{1, 2, 3, 4} {
		got := binary.LittleEndian.Uint64(msg[off:])
		if got != want {
			t.Fatalf("long256[%d] = %d, want %d", i, got, want)
		}
		off += 8
	}

	// Column "tsn" (TIMESTAMP_NANOS): flag + 8 bytes LE (no encoding prefix without Gorilla)
	off++ // null bitmap flag
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
	// Column "v": varint(1) + 'v' + typeCode (LONG = 0x05, no nullable flag)
	off += 2
	if msg[off] != 0x05 {
		t.Fatalf("wireTypeCode = 0x%02X, want 0x05", msg[off])
	}
	off++

	// Null bitmap flag: 0x01 (has nulls).
	if msg[off] != 0x01 {
		t.Fatalf("nullBitmapFlag = 0x%02X, want 0x01", msg[off])
	}
	off++

	// Null bitmap: row 1 null → bit 1 → 0x02.
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
	// 3 columns × (1 flag byte + 2 rows × 4 bytes) = 3 × 9 = 27 bytes
	// Total payload = 6 + 1 + 1 + 1 + 9 + 27 = 45
	// Total message = 12 + 45 = 57

	payloadLen := binary.LittleEndian.Uint32(msg[8:12])
	if payloadLen != 45 {
		t.Fatalf("payloadLength = %d, want 45", payloadLen)
	}
	if len(msg) != 57 {
		t.Fatalf("message length = %d, want 57", len(msg))
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

	// Null bitmap flag: 0x00 (no nulls)
	off++

	// Column data: 8 bytes big-endian (100 = 0x64)
	expected := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64}
	if !bytes.Equal(msg[off:off+8], expected) {
		t.Fatalf("decimal data = %x, want %x", msg[off:off+8], expected)
	}
}

// --- Golden byte tests for boolean, string, symbol, array, decimal encoding ---

func TestQwpEncoderBoolGoldenBytes(t *testing.T) {
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
	col.addBool(true)
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
	col.addBool(false)
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
	col.addBool(true)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2                // table name "t"
	off += 1                // rowCount=3
	off += 1                // colCount=1
	off += 1                // schemaMode=FULL
	off += 1 + 4 + 1        // col "flag": varint(4) + "flag" + type

	// Null bitmap flag (0x00) then bool data: 3 bits packed.
	off++ // null bitmap flag
	// bits: 1,0,1 = 0b00000101 = 0x05
	if msg[off] != 0x05 {
		t.Fatalf("bool data = 0x%02X, want 0x05", msg[off])
	}
}

func TestQwpEncoderBoolNullableGoldenBytes(t *testing.T) {
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("flag", qwpTypeBoolean, true)
	col.addBool(true) // row 0
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, true)
	col.addNull() // row 1: null
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, true)
	col.addBool(false) // row 2
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2         // table name "t"
	off += 1         // rowCount=3
	off += 1         // colCount=1
	off += 1         // schemaMode=FULL
	off += 1 + 4 + 1 // col "flag": varint(4) + "flag" + wireTypeCode (0x81)

	// Null bitmap flag: 0x01 (has nulls)
	if msg[off] != 0x01 {
		t.Fatalf("null bitmap flag = 0x%02X, want 0x01", msg[off])
	}
	off++

	// Null bitmap: 1 byte. Row 1 null → bit 1 → 0x02.
	if msg[off] != 0x02 {
		t.Fatalf("null bitmap = 0x%02X, want 0x02", msg[off])
	}
	off++

	// Bool data: bits 0=true, 1=false(sentinel), 2=false.
	// Only bit 0 is set → 0x01.
	if msg[off] != 0x01 {
		t.Fatalf("bool data = 0x%02X, want 0x01", msg[off])
	}
}

func TestQwpEncoderStringGoldenBytes(t *testing.T) {
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("s", qwpTypeString, false)
	col.addString("hello")
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("s", qwpTypeString, false)
	col.addString("world")
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=2
	off += 1     // colCount=1
	off += 1     // schemaMode=FULL
	off += 1 + 1 + 1 // col "s": varint(1) + "s" + type

	// Null bitmap flag (0x00)
	off++

	// String encoding: (rowCount+1) × uint32 LE offsets + data.
	// Offsets: [0, 5, 10]
	expectedOffsets := []uint32{0, 5, 10}
	for i, want := range expectedOffsets {
		got := binary.LittleEndian.Uint32(msg[off:])
		if got != want {
			t.Fatalf("offset[%d] = %d, want %d", i, got, want)
		}
		off += 4
	}

	// String data: "helloworld"
	if string(msg[off:off+10]) != "helloworld" {
		t.Fatalf("string data = %q, want %q", msg[off:off+10], "helloworld")
	}
}

func TestQwpEncoderSymbolGoldenBytes(t *testing.T) {
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
	col.addSymbolID(0)
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
	col.addSymbolID(1)
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
	col.addSymbolID(42)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=3
	off += 1     // colCount=1
	off += 1     // schemaMode=FULL
	off += 1 + 3 + 1 // col "sym": varint(3) + "sym" + type

	// Null bitmap flag (0x00)
	off++

	// Symbol IDs as varints: 0, 1, 42.
	// varint(0) = [0x00]
	if msg[off] != 0x00 {
		t.Fatalf("symbol[0] = 0x%02X, want 0x00", msg[off])
	}
	off++

	// varint(1) = [0x01]
	if msg[off] != 0x01 {
		t.Fatalf("symbol[1] = 0x%02X, want 0x01", msg[off])
	}
	off++

	// varint(42) = [0x2A]
	if msg[off] != 0x2A {
		t.Fatalf("symbol[2] = 0x%02X, want 0x2A", msg[off])
	}
}

func TestQwpEncoderArrayGoldenBytes(t *testing.T) {
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("arr", qwpTypeDoubleArray, false)
	col.addDoubleArray(1, []int32{2}, []float64{1.5, 2.5})
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=1
	off += 1     // colCount=1
	off += 1     // schemaMode=FULL
	off += 1 + 3 + 1 // col "arr": varint(3) + "arr" + type

	// Null bitmap flag (0x00)
	off++

	// Array data: nDims=1, shape=[2], elements=[1.5, 2.5]
	// nDims
	if msg[off] != 0x01 {
		t.Fatalf("nDims = %d, want 1", msg[off])
	}
	off++

	// shape[0] = 2 (uint32 LE)
	if binary.LittleEndian.Uint32(msg[off:]) != 2 {
		t.Fatalf("shape[0] = %d, want 2", binary.LittleEndian.Uint32(msg[off:]))
	}
	off += 4

	// element 0: 1.5
	got := math.Float64frombits(binary.LittleEndian.Uint64(msg[off:]))
	if got != 1.5 {
		t.Fatalf("element[0] = %v, want 1.5", got)
	}
	off += 8

	// element 1: 2.5
	got = math.Float64frombits(binary.LittleEndian.Uint64(msg[off:]))
	if got != 2.5 {
		t.Fatalf("element[1] = %v, want 2.5", got)
	}
}

func TestQwpEncoderVarcharGoldenBytes(t *testing.T) {
	// VARCHAR uses same encoding as STRING.
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("v", qwpTypeVarchar, false)
	col.addString("abc")
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=1
	off += 1     // colCount=1
	off += 1     // schemaMode=FULL
	off += 1 + 1 + 1 // col "v": varint(1) + "v" + type (0x0F)

	// Null bitmap flag (0x00)
	off++

	// Offsets: [0, 3] → 2 × uint32 LE
	if binary.LittleEndian.Uint32(msg[off:]) != 0 {
		t.Fatalf("offset[0] = %d, want 0", binary.LittleEndian.Uint32(msg[off:]))
	}
	off += 4
	if binary.LittleEndian.Uint32(msg[off:]) != 3 {
		t.Fatalf("offset[1] = %d, want 3", binary.LittleEndian.Uint32(msg[off:]))
	}
	off += 4

	// Data: "abc"
	if string(msg[off:off+3]) != "abc" {
		t.Fatalf("data = %q, want %q", msg[off:off+3], "abc")
	}
}

// --- Delta symbol dictionary tests ---

func TestQwpEncoderDeltaDictGoldenBytes(t *testing.T) {
	// Scenario: globalDict = ["AAPL", "MSFT", "GOOG"]
	// maxSentId = 0 (only AAPL was sent before)
	// batchMaxId = 2 (GOOG is the highest ID in this batch)
	// Delta: symbols 1 and 2 → ["MSFT", "GOOG"]
	globalDict := []string{"AAPL", "MSFT", "GOOG"}

	tb := newQwpTableBuffer("trades")
	col, _ := tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
	col.addSymbolID(0) // AAPL
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
	col.addSymbolID(2) // GOOG
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTableWithDeltaDict(tb, globalDict, 0, 2, qwpSchemaModeFull, 0)

	// Verify header.
	magic := binary.LittleEndian.Uint32(msg[0:4])
	if magic != qwpMagic {
		t.Fatalf("magic = 0x%08X, want 0x%08X", magic, qwpMagic)
	}
	if msg[4] != qwpVersion {
		t.Fatalf("version = %d, want %d", msg[4], qwpVersion)
	}
	// Flags: FLAG_DELTA_SYMBOL_DICT only (no Gorilla)
	if msg[5] != qwpFlagDeltaSymbolDict {
		t.Fatalf("flags = 0x%02X, want 0x%02X", msg[5], qwpFlagDeltaSymbolDict)
	}
	// TableCount = 1
	if binary.LittleEndian.Uint16(msg[6:8]) != 1 {
		t.Fatalf("tableCount = %d, want 1", binary.LittleEndian.Uint16(msg[6:8]))
	}
	// Payload length
	payloadLen := binary.LittleEndian.Uint32(msg[8:12])
	if int(payloadLen) != len(msg)-qwpHeaderSize {
		t.Fatalf("payloadLength = %d, want %d", payloadLen, len(msg)-qwpHeaderSize)
	}

	// Parse delta dictionary after header.
	off := qwpHeaderSize

	// deltaStart = 1 (maxSentId + 1)
	deltaStart, n, _ := qwpReadVarint(msg[off:])
	off += n
	if deltaStart != 1 {
		t.Fatalf("deltaStart = %d, want 1", deltaStart)
	}

	// deltaCount = 2
	deltaCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if deltaCount != 2 {
		t.Fatalf("deltaCount = %d, want 2", deltaCount)
	}

	// Symbol 1: "MSFT"
	symLen, n, _ := qwpReadVarint(msg[off:])
	off += n
	sym := string(msg[off : off+int(symLen)])
	off += int(symLen)
	if sym != "MSFT" {
		t.Fatalf("delta symbol[0] = %q, want %q", sym, "MSFT")
	}

	// Symbol 2: "GOOG"
	symLen, n, _ = qwpReadVarint(msg[off:])
	off += n
	sym = string(msg[off : off+int(symLen)])
	off += int(symLen)
	if sym != "GOOG" {
		t.Fatalf("delta symbol[1] = %q, want %q", sym, "GOOG")
	}

	// Now table block follows.
	// Table name "trades"
	nameLen, n, _ := qwpReadVarint(msg[off:])
	off += n
	tableName := string(msg[off : off+int(nameLen)])
	off += int(nameLen)
	if tableName != "trades" {
		t.Fatalf("tableName = %q, want %q", tableName, "trades")
	}

	// rowCount = 2
	rowCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if rowCount != 2 {
		t.Fatalf("rowCount = %d, want 2", rowCount)
	}

	// colCount = 1
	colCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if colCount != 1 {
		t.Fatalf("colCount = %d, want 1", colCount)
	}

	// schemaMode = FULL
	if msg[off] != 0x00 {
		t.Fatalf("schemaMode = 0x%02X, want 0x00", msg[off])
	}
	off++

	// Column "sym": name + type (SYMBOL = 0x09)
	symNameLen, n, _ := qwpReadVarint(msg[off:])
	off += n
	off += int(symNameLen) // skip name
	if msg[off] != 0x09 {
		t.Fatalf("typeCode = 0x%02X, want 0x09 (SYMBOL)", msg[off])
	}
	off++

	// Null bitmap flag (0x00)
	off++

	// Column data: 2 symbol IDs as varints.
	// ID 0 (AAPL)
	id0, n, _ := qwpReadVarint(msg[off:])
	off += n
	if id0 != 0 {
		t.Fatalf("symbolID[0] = %d, want 0", id0)
	}

	// ID 2 (GOOG)
	id1, n, _ := qwpReadVarint(msg[off:])
	off += n
	if id1 != 2 {
		t.Fatalf("symbolID[1] = %d, want 2", id1)
	}

	// Verify we consumed all bytes.
	if off != len(msg) {
		t.Fatalf("unconsumed bytes: off=%d, len=%d", off, len(msg))
	}
}

func TestQwpEncoderDeltaDictEmptyDelta(t *testing.T) {
	// No new symbols: maxSentId=2, batchMaxId=2.
	globalDict := []string{"A", "B", "C"}

	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
	col.addSymbolID(0)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTableWithDeltaDict(tb, globalDict, 2, 2, qwpSchemaModeFull, 0)

	// Flags should have delta dict flag only (no Gorilla).
	if msg[5] != qwpFlagDeltaSymbolDict {
		t.Fatalf("flags = 0x%02X, want 0x%02X", msg[5], qwpFlagDeltaSymbolDict)
	}

	off := qwpHeaderSize

	// deltaStart = 3
	deltaStart, n, _ := qwpReadVarint(msg[off:])
	off += n
	if deltaStart != 3 {
		t.Fatalf("deltaStart = %d, want 3", deltaStart)
	}

	// deltaCount = 0
	deltaCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if deltaCount != 0 {
		t.Fatalf("deltaCount = %d, want 0", deltaCount)
	}

	// Table block should follow immediately.
	nameLen, n, _ := qwpReadVarint(msg[off:])
	off += n
	tableName := string(msg[off : off+int(nameLen)])
	if tableName != "t" {
		t.Fatalf("tableName = %q, want %q", tableName, "t")
	}
}

func TestQwpEncoderDeltaDictAllNew(t *testing.T) {
	// All symbols are new: maxSentId=-1 (none sent).
	globalDict := []string{"X", "Y"}

	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("s", qwpTypeSymbol, false)
	col.addSymbolID(1) // Y
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTableWithDeltaDict(tb, globalDict, -1, 1, qwpSchemaModeFull, 0)

	off := qwpHeaderSize

	// deltaStart = 0 (maxSentId=-1, so -1+1=0)
	deltaStart, n, _ := qwpReadVarint(msg[off:])
	off += n
	if deltaStart != 0 {
		t.Fatalf("deltaStart = %d, want 0", deltaStart)
	}

	// deltaCount = 2 (batchMaxId=1, 1-(-1)=2)
	deltaCount, n, _ := qwpReadVarint(msg[off:])
	off += n
	if deltaCount != 2 {
		t.Fatalf("deltaCount = %d, want 2", deltaCount)
	}

	// Symbol 0: "X"
	symLen, n, _ := qwpReadVarint(msg[off:])
	off += n
	sym := string(msg[off : off+int(symLen)])
	off += int(symLen)
	if sym != "X" {
		t.Fatalf("delta symbol[0] = %q, want %q", sym, "X")
	}

	// Symbol 1: "Y"
	symLen, n, _ = qwpReadVarint(msg[off:])
	off += n
	sym = string(msg[off : off+int(symLen)])
	off += int(symLen)
	if sym != "Y" {
		t.Fatalf("delta symbol[1] = %q, want %q", sym, "Y")
	}
}

func TestQwpEncoderDeltaDictWithSchemaRef(t *testing.T) {
	// Delta dict + schema reference mode.
	globalDict := []string{"A"}

	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("s", qwpTypeSymbol, false)
	col.addSymbolID(0)
	tb.commitRow()

	schemaHash := tb.getSchemaHash()

	var enc qwpEncoder
	msg := enc.encodeTableWithDeltaDict(tb, globalDict, -1, 0, qwpSchemaModeReference, schemaHash)

	off := qwpHeaderSize

	// Skip delta dict: deltaStart=0, deltaCount=1, "A"
	off += 1 + 1 + 1 + 1 // varint(0) + varint(1) + varint(1) + 'A'

	// Skip table name "t"
	off += 1 + 1
	// rowCount=1, colCount=1
	off += 1 + 1
	// schemaMode = REFERENCE (0x01)
	if msg[off] != 0x01 {
		t.Fatalf("schemaMode = 0x%02X, want 0x01", msg[off])
	}
	off++

	// Schema hash: int64 LE
	gotHash := int64(binary.LittleEndian.Uint64(msg[off : off+8]))
	if gotHash != schemaHash {
		t.Fatalf("schemaHash = %d, want %d", gotHash, schemaHash)
	}
}

// --- Geohash encoder tests ---

func TestQwpEncoderGeohashGoldenBytes(t *testing.T) {
	// Precision=20 bits → 3 bytes per row on wire.
	// 3 rows: 0x12345, 0xABCDE, 0x00001
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("geo", qwpTypeGeohash, false)
	if err := col.addGeohash(0x12345, 20); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("geo", qwpTypeGeohash, false)
	if err := col.addGeohash(0xABCDE, 20); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("geo", qwpTypeGeohash, false)
	if err := col.addGeohash(0x00001, 20); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=3
	off += 1     // colCount=1
	off += 1     // schemaMode=FULL
	off += 1 + 3 + 1 // col "geo": varint(3) + "geo" + type (0x0E)

	// Null bitmap flag (0x00)
	off++

	// Precision varint: 20 = 0x14
	if msg[off] != 0x14 {
		t.Fatalf("precision varint = 0x%02X, want 0x14", msg[off])
	}
	off++

	// Row 0: 0x12345 → LE bytes [0x45, 0x23, 0x01] (3 bytes)
	expected0 := []byte{0x45, 0x23, 0x01}
	if !bytes.Equal(msg[off:off+3], expected0) {
		t.Fatalf("row 0 = %x, want %x", msg[off:off+3], expected0)
	}
	off += 3

	// Row 1: 0xABCDE → LE bytes [0xDE, 0xBC, 0x0A]
	expected1 := []byte{0xDE, 0xBC, 0x0A}
	if !bytes.Equal(msg[off:off+3], expected1) {
		t.Fatalf("row 1 = %x, want %x", msg[off:off+3], expected1)
	}
	off += 3

	// Row 2: 0x00001 → LE bytes [0x01, 0x00, 0x00]
	expected2 := []byte{0x01, 0x00, 0x00}
	if !bytes.Equal(msg[off:off+3], expected2) {
		t.Fatalf("row 2 = %x, want %x", msg[off:off+3], expected2)
	}
	off += 3

	if off != len(msg) {
		t.Fatalf("unconsumed bytes: off=%d, len=%d", off, len(msg))
	}
}

func TestQwpEncoderGeohashNullable(t *testing.T) {
	// Precision=12 bits → 2 bytes per row.
	// Row 0: 0xABC (non-null), Row 1: null, Row 2: 0x123 (non-null)
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("geo", qwpTypeGeohash, true)
	if err := col.addGeohash(0xABC, 12); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("geo", qwpTypeGeohash, true)
	col.addNull()
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("geo", qwpTypeGeohash, true)
	if err := col.addGeohash(0x123, 12); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=3
	off += 1     // colCount=1
	off += 1     // schemaMode=FULL
	off += 1 + 3 + 1 // col "geo": varint(3) + "geo" + type (0x0E, no nullable flag)

	// Null bitmap flag: 0x01 (has nulls)
	if msg[off] != 0x01 {
		t.Fatalf("null bitmap flag = 0x%02X, want 0x01", msg[off])
	}
	off++

	// Null bitmap: row 1 null → bit 1 → 0x02
	if msg[off] != 0x02 {
		t.Fatalf("null bitmap = 0x%02X, want 0x02", msg[off])
	}
	off++

	// Precision varint: 12 = 0x0C
	if msg[off] != 0x0C {
		t.Fatalf("precision varint = 0x%02X, want 0x0C", msg[off])
	}
	off++

	// Row 0: 0xABC → LE [0xBC, 0x0A]
	expected0 := []byte{0xBC, 0x0A}
	if !bytes.Equal(msg[off:off+2], expected0) {
		t.Fatalf("row 0 = %x, want %x", msg[off:off+2], expected0)
	}
	off += 2

	// Row 1: null sentinel → 0xFFFFFFFFFFFFFFFF → LE [0xFF, 0xFF]
	expected1 := []byte{0xFF, 0xFF}
	if !bytes.Equal(msg[off:off+2], expected1) {
		t.Fatalf("row 1 (null) = %x, want %x", msg[off:off+2], expected1)
	}
	off += 2

	// Row 2: 0x123 → LE [0x23, 0x01]
	expected2 := []byte{0x23, 0x01}
	if !bytes.Equal(msg[off:off+2], expected2) {
		t.Fatalf("row 2 = %x, want %x", msg[off:off+2], expected2)
	}
	off += 2

	if off != len(msg) {
		t.Fatalf("unconsumed bytes: off=%d, len=%d", off, len(msg))
	}
}

func TestQwpEncoderGeohashPrecision8(t *testing.T) {
	// Precision=8 bits → exactly 1 byte per row.
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("g", qwpTypeGeohash, false)
	if err := col.addGeohash(0xAB, 8); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=1
	off += 1     // colCount=1
	off += 1     // schemaMode=FULL
	off += 1 + 1 + 1 // col "g": varint(1) + "g" + type

	// Null bitmap flag (0x00)
	off++

	// Precision: 8
	if msg[off] != 0x08 {
		t.Fatalf("precision = 0x%02X, want 0x08", msg[off])
	}
	off++

	// 1 byte: 0xAB
	if msg[off] != 0xAB {
		t.Fatalf("row 0 = 0x%02X, want 0xAB", msg[off])
	}
	off++

	if off != len(msg) {
		t.Fatalf("unconsumed bytes: off=%d, len=%d", off, len(msg))
	}
}

func TestQwpEncoderGeohashPrecision60(t *testing.T) {
	// Precision=60 bits → 8 bytes per row (max).
	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("g", qwpTypeGeohash, false)
	if err := col.addGeohash(0x0FFFFFFFFFFFFFFF, 60); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Skip to column data.
	off := qwpHeaderSize
	off += 2     // table name "t"
	off += 1     // rowCount=1
	off += 1     // colCount=1
	off += 1     // schemaMode
	off += 1 + 1 + 1 // col "g"

	// Null bitmap flag (0x00)
	off++

	// Precision: 60 = 0x3C
	if msg[off] != 0x3C {
		t.Fatalf("precision = 0x%02X, want 0x3C", msg[off])
	}
	off++

	// 8 bytes LE of 0x0FFFFFFFFFFFFFFF
	expected := make([]byte, 8)
	binary.LittleEndian.PutUint64(expected, 0x0FFFFFFFFFFFFFFF)
	if !bytes.Equal(msg[off:off+8], expected) {
		t.Fatalf("row 0 = %x, want %x", msg[off:off+8], expected)
	}
}

func TestQwpEncoderNoGorillaFlag(t *testing.T) {
	// Verify that the encoder does NOT set FLAG_GORILLA (0x04) in the
	// header flags. Setting it without implementing Gorilla compression
	// would cause the server to misinterpret timestamp data.

	t.Run("encodeTable", func(t *testing.T) {
		tb := newQwpTableBuffer("t")
		col, _ := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
		col.addTimestamp(1000000)
		tb.commitRow()

		var enc qwpEncoder
		msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

		flags := msg[qwpHeaderOffsetFlags]
		if flags&qwpFlagGorilla != 0 {
			t.Fatalf("FLAG_GORILLA (0x04) must not be set, got flags=0x%02X", flags)
		}
	})

	t.Run("encodeTableWithDeltaDict", func(t *testing.T) {
		tb := newQwpTableBuffer("t")
		col, _ := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
		col.addTimestamp(1000000)
		tb.commitRow()

		globalDict := []string{"sym0"}
		var enc qwpEncoder
		msg := enc.encodeTableWithDeltaDict(tb, globalDict, -1, 0, qwpSchemaModeFull, 0)

		flags := msg[qwpHeaderOffsetFlags]
		if flags&qwpFlagGorilla != 0 {
			t.Fatalf("FLAG_GORILLA (0x04) must not be set, got flags=0x%02X", flags)
		}
		// Delta dict flag should be set.
		if flags&qwpFlagDeltaSymbolDict == 0 {
			t.Fatalf("FLAG_DELTA_SYMBOL_DICT should be set, got flags=0x%02X", flags)
		}
	})
}

func TestQwpEncoderTimestampNoEncodingPrefix(t *testing.T) {
	// Verify that timestamp columns are encoded as raw 8-byte LE values
	// without a 0x00 encoding prefix byte. The prefix is only required
	// when FLAG_GORILLA is set (which we don't support).

	tb := newQwpTableBuffer("t")
	col, _ := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
	col.addTimestamp(1234567890)
	tb.commitRow()

	var enc qwpEncoder
	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)

	// Parse to column data section.
	off := qwpHeaderSize
	// Table name "t": varint(1) + 't'
	off += 2
	// rowCount=1
	off++
	// colCount=1
	off++
	// schemaMode=FULL
	off++
	// Column "ts": varint(1) + 't' + 's' + typeCode TIMESTAMP (0x0A)
	off += 4

	// Null bitmap flag: 0x00 (no nulls)
	if msg[off] != 0x00 {
		t.Fatalf("null bitmap flag = 0x%02X, want 0x00", msg[off])
	}
	off++

	// Next 8 bytes should be the raw timestamp value (no 0x00 prefix).
	gotTs := int64(binary.LittleEndian.Uint64(msg[off:]))
	if gotTs != 1234567890 {
		t.Fatalf("timestamp = %d, want 1234567890", gotTs)
	}
	off += 8

	// Verify we consumed all bytes.
	if off != len(msg) {
		t.Fatalf("did not consume all bytes: off=%d, len=%d", off, len(msg))
	}
}
