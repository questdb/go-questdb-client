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

// qwpEncoder encodes qwpTableBuffer data into QWP v1 binary messages.
// It owns a reusable qwpWireBuffer to minimize allocations across
// successive encode calls.
//
// Usage:
//
//	var enc qwpEncoder
//	msg := enc.encodeTable(tb, qwpSchemaModeFull, 0)
//	// msg is valid until the next encode call.
type qwpEncoder struct {
	wb qwpWireBuffer
}

// encodeTable encodes a single table buffer into a complete QWP
// message. The returned byte slice references the encoder's internal
// buffer and is valid until the next encode call.
//
// The message layout is:
//
//	Header (12 bytes) → TableName → RowCount → ColCount →
//	Schema → ColumnData... → patched PayloadLength.
func (e *qwpEncoder) encodeTable(tb *qwpTableBuffer, schemaMode qwpSchemaMode, schemaHash int64) []byte {
	e.wb.reset()

	// --- Header (12 bytes) ---
	e.wb.putUint32LE(qwpMagic)  // offset 0-3: "QWP1"
	e.wb.putByte(qwpVersion)    // offset 4: version
	e.wb.putByte(0x00)          // offset 5: flags
	e.wb.putUint16LE(1)         // offset 6-7: tableCount = 1
	e.wb.putUint32LE(0)         // offset 8-11: payloadLength placeholder

	// --- Table block ---
	e.wb.putString(tb.tableName)
	e.wb.putVarint(uint64(tb.rowCount))
	e.wb.putVarint(uint64(len(tb.columns)))

	// --- Schema ---
	e.wb.putByte(byte(schemaMode))
	if schemaMode == qwpSchemaModeFull {
		e.encodeSchemaFull(tb)
	} else {
		e.wb.putInt64LE(schemaHash)
	}

	// --- Column data ---
	for _, col := range tb.columns {
		e.encodeColumnData(col)
	}

	// --- Patch payload length ---
	payloadLen := uint32(e.wb.len() - qwpHeaderSize)
	e.wb.patchUint32LE(qwpHeaderOffsetPayloadLen, payloadLen)

	return e.wb.bytes()
}

// encodeSchemaFull writes full column definitions: for each column,
// the name (varint string) and wire type code. Decimal columns get
// an extra scale byte.
func (e *qwpEncoder) encodeSchemaFull(tb *qwpTableBuffer) {
	for _, col := range tb.columns {
		e.wb.putString(col.name)
		e.wb.putByte(col.wireTypeCode())
		if qwpIsDecimalType(col.typeCode) {
			if col.scale >= 0 {
				e.wb.putByte(byte(col.scale))
			} else {
				e.wb.putByte(0)
			}
		}
	}
}

// encodeColumnData writes the wire-format data for a single column.
// For nullable columns, the null bitmap is written first. Then the
// type-specific data follows.
func (e *qwpEncoder) encodeColumnData(col *qwpColumnBuffer) {
	// Null bitmap for nullable columns.
	if col.nullable {
		e.encodeNullBitmap(col)
	}

	switch col.typeCode {
	case qwpTypeBoolean:
		e.encodeBoolColumn(col)

	case qwpTypeString, qwpTypeVarchar:
		e.encodeStringColumn(col)

	case qwpTypeSymbol:
		e.encodeSymbolColumn(col)

	case qwpTypeDoubleArray, qwpTypeLongArray:
		e.encodeArrayColumn(col)

	case qwpTypeGeohash:
		e.encodeGeohashColumn(col)

	default:
		// Fixed-width types: raw bytes from fixedData.
		if col.fixedSize > 0 {
			e.wb.putBytes(col.fixedData)
		}
	}
}

// encodeNullBitmap writes the null bitmap for a nullable column.
// The bitmap has (rowCount+7)/8 bytes with LSB-first bit packing.
// The column's nullBitmap may be shorter if trailing rows are
// non-null; missing bytes are padded with zeros.
func (e *qwpEncoder) encodeNullBitmap(col *qwpColumnBuffer) {
	bitmapLen := col.nullBitmapLen()
	for i := 0; i < bitmapLen; i++ {
		if i < len(col.nullBitmap) {
			e.wb.putByte(col.nullBitmap[i])
		} else {
			e.wb.putByte(0)
		}
	}
}

// encodeBoolColumn writes bit-packed boolean values. Like the null
// bitmap, the boolData may be shorter than needed; missing bytes
// are padded with zeros.
func (e *qwpEncoder) encodeBoolColumn(col *qwpColumnBuffer) {
	boolLen := (col.rowCount + 7) / 8
	for i := 0; i < boolLen; i++ {
		if i < len(col.boolData) {
			e.wb.putByte(col.boolData[i])
		} else {
			e.wb.putByte(0)
		}
	}
}

// encodeStringColumn writes (rowCount+1) cumulative uint32 LE
// offsets followed by the concatenated string data.
func (e *qwpEncoder) encodeStringColumn(col *qwpColumnBuffer) {
	for _, off := range col.strOffsets {
		e.wb.putUint32LE(off)
	}
	e.wb.putBytes(col.strData)
}

// encodeSymbolColumn writes per-row global symbol dictionary IDs
// as varints.
func (e *qwpEncoder) encodeSymbolColumn(col *qwpColumnBuffer) {
	for _, id := range col.symbolIDs {
		e.wb.putVarint(uint64(id))
	}
}

// encodeArrayColumn writes per-row array data. Each row's encoded
// data (nDims + shape + elements) is already stored contiguously
// in arrayData, so we just emit the raw bytes.
func (e *qwpEncoder) encodeArrayColumn(col *qwpColumnBuffer) {
	e.wb.putBytes(col.arrayData)
}

// encodeGeohashColumn writes geohash column data. Currently a
// placeholder — geohash encoding will be added in a later phase.
func (e *qwpEncoder) encodeGeohashColumn(col *qwpColumnBuffer) {
	// Geohash encoding: precision varint + packed bits per row.
	// Will be implemented in Phase 3 geohash task.
}
