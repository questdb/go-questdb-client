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
// message without a delta symbol dictionary. The returned byte slice
// references the encoder's internal buffer and is valid until the
// next encode call.
//
// The message layout is:
//
//	Header (12 bytes) → TableBlock → patched PayloadLength.
func (e *qwpEncoder) encodeTable(tb *qwpTableBuffer, schemaMode qwpSchemaMode, schemaHash int64) []byte {
	e.wb.reset()
	e.writeHeader(qwpFlagGorilla, 1)
	e.writeTableBlock(tb, schemaMode, schemaHash)
	e.patchPayloadLength()
	return e.wb.bytes()
}

// encodeTableWithDeltaDict encodes a single table buffer with a
// prepended delta symbol dictionary. The dictionary sends only new
// symbols (those with IDs from maxSentId+1 through batchMaxId)
// and sets FLAG_DELTA_SYMBOL_DICT in the header.
//
// The message layout is:
//
//	Header (12 bytes, flags=0x08) → DeltaDict → TableBlock →
//	patched PayloadLength.
//
// globalDict maps symbol IDs (indices) to symbol strings.
// maxSentId is the last successfully ACKed symbol ID (-1 if none).
// batchMaxId is the highest symbol ID used in this batch.
func (e *qwpEncoder) encodeTableWithDeltaDict(
	tb *qwpTableBuffer,
	globalDict []string,
	maxSentId int,
	batchMaxId int,
	schemaMode qwpSchemaMode,
	schemaHash int64,
) []byte {
	e.wb.reset()
	e.writeHeader(qwpFlagDeltaSymbolDict|qwpFlagGorilla, 1)
	e.writeDeltaDict(globalDict, maxSentId, batchMaxId)
	e.writeTableBlock(tb, schemaMode, schemaHash)
	e.patchPayloadLength()
	return e.wb.bytes()
}

// --- header and payload helpers ---

// writeHeader writes the 12-byte QWP message header with the given
// flags and table count. PayloadLength is set to 0 (placeholder).
func (e *qwpEncoder) writeHeader(flags byte, tableCount uint16) {
	e.wb.putUint32LE(qwpMagic)
	e.wb.putByte(qwpVersion)
	e.wb.putByte(flags)
	e.wb.putUint16LE(tableCount)
	e.wb.putUint32LE(0) // payloadLength placeholder
}

// patchPayloadLength patches the payload length field at offset 8
// with the actual number of bytes after the 12-byte header.
func (e *qwpEncoder) patchPayloadLength() {
	payloadLen := uint32(e.wb.len() - qwpHeaderSize)
	e.wb.patchUint32LE(qwpHeaderOffsetPayloadLen, payloadLen)
}

// --- delta symbol dictionary ---

// writeDeltaDict writes the delta symbol dictionary section. It
// encodes symbols from ID maxSentId+1 through batchMaxId (inclusive).
//
// Wire format:
//
//	[deltaStart: varint] [deltaCount: varint]
//	[symbol0: string] [symbol1: string] ...
func (e *qwpEncoder) writeDeltaDict(globalDict []string, maxSentId, batchMaxId int) {
	deltaStart := maxSentId + 1
	deltaCount := batchMaxId - maxSentId
	if deltaCount < 0 {
		deltaCount = 0
	}

	e.wb.putVarint(uint64(deltaStart))
	e.wb.putVarint(uint64(deltaCount))
	for i := deltaStart; i < deltaStart+deltaCount; i++ {
		e.wb.putString(globalDict[i])
	}
}

// --- table block ---

// writeTableBlock writes a single table block: table name, row/col
// counts, schema, and column data.
func (e *qwpEncoder) writeTableBlock(tb *qwpTableBuffer, schemaMode qwpSchemaMode, schemaHash int64) {
	e.wb.putString(tb.tableName)
	e.wb.putVarint(uint64(tb.rowCount))
	e.wb.putVarint(uint64(len(tb.columns)))

	e.wb.putByte(byte(schemaMode))
	if schemaMode == qwpSchemaModeFull {
		e.encodeSchemaFull(tb)
	} else {
		e.wb.putInt64LE(schemaHash)
	}

	for _, col := range tb.columns {
		e.encodeColumnData(col)
	}
}

// --- schema ---

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

// --- column data ---

// qwpEncodingUncompressed is the encoding prefix byte for
// uncompressed timestamp data when Gorilla mode is enabled.
const qwpEncodingUncompressed byte = 0x00

// encodeColumnData writes the wire-format data for a single column.
// For nullable columns, the null bitmap is written first. Then the
// type-specific data follows.
func (e *qwpEncoder) encodeColumnData(col *qwpColumnBuffer) {
	if col.nullable {
		e.encodeNullBitmap(col)
	}

	switch col.typeCode {
	case qwpTypeBoolean:
		e.encodeBoolColumn(col)

	case qwpTypeTimestamp, qwpTypeTimestampNano:
		// Timestamp columns require an encoding prefix byte
		// when Gorilla mode is enabled (FLAG_GORILLA in header).
		// We always enable Gorilla mode, so always write the
		// uncompressed encoding byte (0x00) followed by raw data.
		e.wb.putByte(qwpEncodingUncompressed)
		e.wb.putBytes(col.fixedData)

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

// encodeGeohashColumn writes geohash column data: a precision
// varint followed by per-row packed bytes. Each row's 8-byte
// value from fixedData is truncated to ceil(precision/8) bytes
// on the wire, written in little-endian byte order.
func (e *qwpEncoder) encodeGeohashColumn(col *qwpColumnBuffer) {
	precision := col.geohashPrecision
	if precision <= 0 {
		// No precision established (column has only nulls).
		// Write precision 0, no per-row data needed beyond
		// the null bitmap (already written).
		e.wb.putVarint(0)
		return
	}

	e.wb.putVarint(uint64(precision))

	valueSize := (int(precision) + 7) / 8
	for i := 0; i < col.rowCount; i++ {
		off := i * 8
		// Write only the low valueSize bytes from each 8-byte
		// LE value. This is the same as writing the value in
		// little-endian order truncated to valueSize bytes.
		e.wb.putBytes(col.fixedData[off : off+valueSize])
	}
}
