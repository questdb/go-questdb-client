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

import "fmt"

// qwpEncoder encodes qwpTableBuffer data into QWP binary messages.
// It owns a reusable qwpWireBuffer to minimize allocations across
// successive encode calls.
//
// Usage:
//
//	var enc qwpEncoder
//	msg := enc.encodeTable(tb)
//	// msg is valid until the next encode call.
type qwpEncoder struct {
	wb      qwpWireBuffer
	gorilla qwpGorillaEncoder
}

// encodeTable encodes a single table buffer into a complete QWP
// message with an empty delta symbol dictionary. The returned byte
// slice references the encoder's internal buffer and is valid until
// the next encode call.
//
// The production cursor sender never invokes this method — it goes
// through encodeMultiTableWithDeltaDict. encodeTable is retained as a
// single-table convenience for tests that build wire-format fixtures
// for the egress decoder.
//
// It sets FLAG_DELTA_SYMBOL_DICT (the only symbol-encoding mode
// WebSocket clients emit) and FLAG_GORILLA (timestamp columns are
// always preceded by a 1-byte encoding flag; see QWP spec §12).
//
// The message layout is:
//
//	Header (12 bytes, flags=0x0C) → empty DeltaDict →
//	TableBlock → patched PayloadLength.
func (e *qwpEncoder) encodeTable(tb *qwpTableBuffer) []byte {
	return e.encodeTableWithDeltaDict(tb, nil, -1, -1)
}

// encodeTableWithDeltaDict encodes a single table buffer with a
// prepended delta symbol dictionary. The dictionary sends only new
// symbols (those with IDs from maxSentId+1 through batchMaxId) and
// sets FLAG_DELTA_SYMBOL_DICT | FLAG_GORILLA in the header.
//
// The message layout is:
//
//	Header (12 bytes, flags=0x0C) → DeltaDict → TableBlock →
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
) []byte {
	e.wb.reset()
	e.writeHeader(e.headerFlags(), 1)
	e.writeDeltaDict(globalDict, maxSentId, batchMaxId)
	e.writeTableBlock(tb)
	e.patchPayloadLength()
	return e.wb.bytes()
}

// encodeMultiTableWithDeltaDict encodes multiple table buffers into
// a single QWP message with a shared delta symbol dictionary. The
// header's tableCount field is set to len(tables), allowing the
// server to process all tables from one WebSocket frame. This
// reduces round-trips compared to one message per table.
//
// Every table block carries its inline column definitions —
// cursor-architecture self-sufficient frames repeat the full schema
// on every frame so reconnect / replay stays safe against a freshly
// connected server.
//
// The message layout is:
//
//	Header (12 bytes, tableCount=N) → DeltaDict →
//	TableBlock₁ → TableBlock₂ → ... → TableBlockₙ →
//	patched PayloadLength.
func (e *qwpEncoder) encodeMultiTableWithDeltaDict(
	tables []*qwpTableBuffer,
	globalDict []string,
	maxSentId int,
	batchMaxId int,
) []byte {
	e.wb.reset()
	// buildTableEncodeInfo enforces qwpMaxTablesPerBatch (0xFFFF); guard
	// locally so a regression cannot silently truncate the header's
	// 16-bit tableCount and produce a malformed frame.
	if len(tables) > qwpMaxTablesPerBatch {
		panic(fmt.Sprintf(
			"qwp: encoder got %d tables, exceeds uint16 tableCount limit %d",
			len(tables), qwpMaxTablesPerBatch,
		))
	}
	e.writeHeader(e.headerFlags(), uint16(len(tables)))
	e.writeDeltaDict(globalDict, maxSentId, batchMaxId)
	for i := range tables {
		e.writeTableBlock(tables[i])
	}
	e.patchPayloadLength()
	return e.wb.bytes()
}

// --- header and payload helpers ---

// headerFlags returns the header flags byte for the current encoder
// state. QWP ingress always sets FLAG_DELTA_SYMBOL_DICT and FLAG_GORILLA.
func (e *qwpEncoder) headerFlags() byte {
	return qwpFlagDeltaSymbolDict | qwpFlagGorilla
}

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

// writeTableBlock writes a single table block: table name, row and
// column counts, the inline column schema, and the column data.
//
// Per the QWP ingress wire format the table block is table_name,
// row_count, col_count, inline columns (a name + type-code pair per
// column), then the per-column data. The schema is always inline —
// the wire carries no schema mode byte and no schema id.
func (e *qwpEncoder) writeTableBlock(tb *qwpTableBuffer) {
	e.wb.putString(tb.tableName)
	e.wb.putVarint(uint64(tb.rowCount))
	e.wb.putVarint(uint64(len(tb.columns)))

	e.encodeSchemaFull(tb)

	for _, col := range tb.columns {
		e.encodeColumnData(col)
	}
}

// --- schema ---

// encodeSchemaFull writes full column definitions: for each column,
// the name (varint string) and base type code (without the 0x80
// nullable flag — nullability is signaled via the null bitmap flag
// byte in the data section). The schema section does NOT include
// decimal scale — the scale byte is written in the column data
// section, matching the Java server's QwpSchema.parseFullSchema().
func (e *qwpEncoder) encodeSchemaFull(tb *qwpTableBuffer) {
	for _, col := range tb.columns {
		e.wb.putString(col.name)
		e.wb.putByte(byte(col.typeCode)) // base type code, no nullable flag
	}
}

// --- column data ---

// encodeColumnData writes the wire-format data for a single column.
// Every column starts with a null bitmap flag byte:
//   - 0x00: no nulls in this column
//   - non-zero: null bitmap follows (ceil(rowCount/8) bytes)
//
// After the flag (and optional bitmap), the type-specific data follows.
func (e *qwpEncoder) encodeColumnData(col *qwpColumnBuffer) {
	// Write null bitmap flag + optional bitmap for every column.
	e.encodeNullBitmapFlag(col)

	switch col.typeCode {
	case qwpTypeBoolean:
		e.encodeBoolColumn(col)

	case qwpTypeTimestamp, qwpTypeTimestampNano:
		e.encodeTimestampColumn(col)

	case qwpTypeVarchar:
		e.encodeStringColumn(col)

	case qwpTypeSymbol:
		e.encodeSymbolColumn(col)

	case qwpTypeDoubleArray, qwpTypeLongArray:
		e.encodeArrayColumn(col)

	case qwpTypeGeohash:
		e.encodeGeohashColumn(col)

	case qwpTypeDecimal64, qwpTypeDecimal128, qwpTypeDecimal256:
		// Decimal columns: scale byte before the packed values.
		// Matches QwpDecimalColumnCursor.of() in the Java server.
		if col.scale >= 0 {
			e.wb.putByte(byte(col.scale))
		} else {
			e.wb.putByte(0)
		}
		e.wb.putBytes(col.fixedData)

	default:
		// Fixed-width types: raw bytes from fixedData.
		if col.fixedSize > 0 {
			e.wb.putBytes(col.fixedData)
		}
	}
}

// encodeNullBitmapFlag writes the null bitmap flag byte and optional
// bitmap for a column. The format is:
//   - 0x00 flag byte if the column has no nulls (1 byte total)
//   - 0x01 flag byte + bitmap bytes if nulls are present
//     (1 + ceil(rowCount/8) bytes total)
//
// The bitmap is grown lazily (only up to the last index that was
// marked null), so the backed prefix may be shorter than the
// required bitmapLen; the remaining bytes are zero-padded.
func (e *qwpEncoder) encodeNullBitmapFlag(col *qwpColumnBuffer) {
	if col.nullCount == 0 {
		e.wb.putByte(0x00)
		return
	}

	e.wb.putByte(0x01)
	bitmapLen := col.nullBitmapLen()
	prefix := len(col.nullBitmap)
	if prefix > bitmapLen {
		prefix = bitmapLen
	}
	e.wb.putBytes(col.nullBitmap[:prefix])
	e.wb.putZeros(bitmapLen - prefix)
}

// encodeBoolColumn writes bit-packed boolean values for non-null
// rows only. The number of bits is valueCount (= rowCount - nullCount
// for nullable columns, = rowCount for non-nullable). boolData is
// grown lazily (only up to the last set bit), so the backed prefix
// may be shorter than the required boolLen; the remaining bytes are
// zero-padded.
func (e *qwpEncoder) encodeBoolColumn(col *qwpColumnBuffer) {
	vc := col.valueCount()
	boolLen := (vc + 7) / 8
	prefix := len(col.boolData)
	if prefix > boolLen {
		prefix = boolLen
	}
	e.wb.putBytes(col.boolData[:prefix])
	e.wb.putZeros(boolLen - prefix)
}

// encodeStringColumn writes (valueCount+1) cumulative uint32 LE
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

// encodeTimestampColumn writes a timestamp column's payload. QWP ingress
// always sets FLAG_GORILLA at the message level, so the payload is a
// 1-byte encoding flag (0x01 = Gorilla, 0x00 = uncompressed) followed by
// the values. Gorilla is used when the column has more than two non-null
// values and every DoD fits in int32; all other cases fall back to raw
// int64 LE values, matching QwpColumnWriter.writeTimestampColumn in the
// Java client.
//
// Note: DATE is NOT routed here. Ingestion frames DATE as a plain
// int64 (matching the Java QwpColumnWriter); only server *egress*
// frames DATE timestamp-ish. The asymmetry is by protocol design —
// see the DATE case in qwp_query_decoder.go's parseColumn.
func (e *qwpEncoder) encodeTimestampColumn(col *qwpColumnBuffer) {
	count := col.valueCount()
	if count > 2 && qwpGorillaEncodedSize(col.fixedData, count) >= 0 {
		e.wb.putByte(qwpTsEncodingGorilla)
		e.gorilla.encodeTimestamps(&e.wb, col.fixedData, count)
		return
	}
	e.wb.putByte(qwpTsEncodingUncompressed)
	e.wb.putBytes(col.fixedData)
}

// encodeGeohashColumn writes geohash column data: a precision
// varint followed by per-value packed bytes. Each value's 8-byte
// entry from fixedData is truncated to ceil(precision/8) bytes
// on the wire, written in little-endian byte order. Only non-null
// values are included (valueCount entries for nullable columns).
func (e *qwpEncoder) encodeGeohashColumn(col *qwpColumnBuffer) {
	precision := col.geohashPrecision
	if precision <= 0 {
		// No row established a precision (the column holds only nulls).
		// The server validates precision against [1, 60]
		// (QwpGeoHashColumnCursor.of) even for all-null columns and
		// rejects the whole message otherwise, so emit the minimum valid
		// precision. Mirrors the Java client's
		// QwpColumnWriter.writeGeoHashColumn clamp.
		precision = 1
	}

	e.wb.putVarint(uint64(precision))

	// The value loop is bounded by valueCount(), not short-circuited on
	// the clamp above: a nullable all-null column has valueCount()==0, so
	// nothing follows the precision varint; a non-nullable column stores a
	// sentinel per row (valueCount()==rowCount), so those bytes must still
	// reach the wire or the column data is short. The non-nullable
	// all-null case is unreachable through the public API — GeohashColumn
	// establishes precision on the row that creates the column — so the
	// clamp is defense-in-depth.
	vc := col.valueCount()
	valueSize := (int(precision) + 7) / 8
	for i := 0; i < vc; i++ {
		off := i * 8
		// Write only the low valueSize bytes from each 8-byte
		// LE value. This is the same as writing the value in
		// little-endian order truncated to valueSize bytes.
		e.wb.putBytes(col.fixedData[off : off+valueSize])
	}
}
