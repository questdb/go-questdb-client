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
	"encoding/binary"
	"fmt"
	"unsafe"
)

// ExecResult is the outcome of a non-SELECT statement (DDL / INSERT /
// UPDATE / ...) submitted via the QWP egress protocol. It mirrors the
// body of an EXEC_DONE frame.
type ExecResult struct {
	// OpType is the server's CompiledQuery.TYPE_* discriminator for
	// the executed statement (opaque to the client — surfaced for
	// callers that want to distinguish INSERT from UPDATE from DELETE
	// from pure DDL).
	OpType byte

	// RowsAffected is the number of rows modified. 0 for pure DDL.
	RowsAffected int64
}

// qwpConnDict is the connection-scoped symbol dictionary. The server
// sends a delta section at the head of every RESULT_BATCH listing
// symbols assigned since the previous batch; the decoder appends them
// to the heap + entries arrays here. Subsequent batches refer to
// prior dictionary ids without retransmitting the strings.
//
// The heap is append-only and never shrinks — this is the invariant
// that lets a qwpSymbolDictView snapshot taken during decode stay
// valid even if the user's handler is still iterating a previous
// batch. Growth is amortised by Go's append; no explicit capacity
// tuning needed.
type qwpConnDict struct {
	heap    []byte
	entries []qwpSymbolEntry
}

// size returns the current number of entries.
func (d *qwpConnDict) size() int { return len(d.entries) }

// appendDelta consumes the delta-dictionary section at the current
// position of br: (deltaStart, deltaCount, per-entry len+bytes). The
// server is required to send deltaStart == d.size() (otherwise the two
// ends are out of sync); any other value is a decoder-side rejection.
func (d *qwpConnDict) appendDelta(br *qwpByteReader) error {
	deltaStart, err := br.readVarintInt63()
	if err != nil {
		return err
	}
	deltaCount, err := br.readVarintInt63()
	if err != nil {
		return err
	}
	if deltaStart < 0 || deltaCount < 0 ||
		deltaStart+deltaCount > int64(^uint32(0)) {
		return newQwpDecodeError(fmt.Sprintf(
			"delta symbol section out of range: start=%d count=%d",
			deltaStart, deltaCount))
	}
	if int(deltaStart) != d.size() {
		return newQwpDecodeError(fmt.Sprintf(
			"delta symbol dict out of sync: expected start=%d, got=%d",
			d.size(), deltaStart))
	}
	for i := int64(0); i < deltaCount; i++ {
		entryLen, err := br.readVarintInt63()
		if err != nil {
			return err
		}
		if entryLen < 0 {
			return newQwpDecodeError(fmt.Sprintf(
				"negative delta symbol entry length: %d", entryLen))
		}
		bytes, err := br.slice(int(entryLen))
		if err != nil {
			return err
		}
		offset := uint32(len(d.heap))
		d.heap = append(d.heap, bytes...)
		d.entries = append(d.entries, qwpSymbolEntry{
			offset: offset,
			length: uint32(entryLen),
		})
	}
	return nil
}

// snapshot returns a qwpSymbolDictView bound to the current heap +
// entries state. The slice headers freeze at call time, so even if
// d.dict.entries is later grown via append, the returned view keeps
// the old length (and either the old backing array on reallocation,
// or the old length into the same array). Because the heap is
// append-only, bytes addressed by the frozen entries stay valid.
func (d *qwpConnDict) snapshot() qwpSymbolDictView {
	return qwpSymbolDictView{
		heap:    d.heap,
		entries: d.entries,
	}
}

// qwpSchemaRegistry indexes column-info slices by server-assigned
// schema id. Subsequent RESULT_BATCH frames that reference a prior
// schema (mode=0x01) look up by id instead of retransmitting the
// columns. The registry is dense (slice by id) because server ids are
// monotonic from 0 and capped by qwpDefaultMaxSchemasPerConnection.
type qwpSchemaRegistry struct {
	slots [][]qwpColumnSchemaInfo
}

// get returns the columns registered for id, or (nil, false).
func (r *qwpSchemaRegistry) get(id int) ([]qwpColumnSchemaInfo, bool) {
	if id < 0 || id >= len(r.slots) || r.slots[id] == nil {
		return nil, false
	}
	return r.slots[id], true
}

// put records the given columns under id, extending the registry slice
// to reach id if needed. Caller is responsible for bounding id against
// qwpDefaultMaxSchemasPerConnection.
func (r *qwpSchemaRegistry) put(id int, cols []qwpColumnSchemaInfo) {
	for len(r.slots) <= id {
		r.slots = append(r.slots, nil)
	}
	r.slots[id] = cols
}

// qwpQueryDecoder is a stateful, reusable decoder for RESULT_BATCH
// frames. One instance per connection: it accumulates the symbol
// dictionary and schema registry across every batch of the connection.
// Decoding is zero-copy where possible — column-layout slices alias
// into the payload []byte the caller hands to decode().
//
// The decoder is not safe for concurrent use.
type qwpQueryDecoder struct {
	dict      qwpConnDict
	schemas   qwpSchemaRegistry
	gorilla   qwpGorillaDecoder
	br        qwpByteReader
	layouts   []qwpColumnLayout // pool, grown to max observed column count
	deltaOn   bool              // current frame has FLAG_DELTA_SYMBOL_DICT set
	gorillaOn bool              // current frame has FLAG_GORILLA set
}

// decode parses the payload of a RESULT_BATCH frame into out. The
// caller must have already accepted the outer WebSocket frame; payload
// is the full frame bytes (12-byte header + message kind byte +
// per-kind body). On success, `out` is populated with slice views into
// payload and is valid until the caller reuses payload.
//
// Caller contract: the returned batch's slices alias payload. Do not
// reuse payload (or close the WebSocket buffer that backs it) until
// the caller is done reading out.
func (d *qwpQueryDecoder) decode(payload []byte, out *QwpColumnBatch) error {
	msgKind, err := d.parseFrameHeader(payload)
	if err != nil {
		return err
	}
	if msgKind != qwpMsgKindResultBatch {
		return newQwpDecodeError(fmt.Sprintf(
			"expected RESULT_BATCH (0x11), got 0x%02X", byte(msgKind)))
	}
	requestId, err := d.br.readInt64LE()
	if err != nil {
		return err
	}
	batchSeq, err := d.br.readVarintInt63()
	if err != nil {
		return err
	}

	if d.deltaOn {
		if err := d.dict.appendDelta(&d.br); err != nil {
			return err
		}
	}

	// Table block header: name_length varint, name bytes, row_count,
	// column_count.
	nameLen, err := d.br.readVarintInt63()
	if err != nil {
		return err
	}
	if nameLen < 0 || nameLen > qwpMaxTableNameLen {
		return newQwpDecodeError(fmt.Sprintf(
			"table name length out of range: %d", nameLen))
	}
	if err := d.br.advance(int(nameLen)); err != nil {
		return err
	}

	rowCount64, err := d.br.readVarintInt63()
	if err != nil {
		return err
	}
	if rowCount64 < 0 || rowCount64 > qwpMaxRowsPerBatch {
		return newQwpDecodeError(fmt.Sprintf(
			"row_count out of range: %d", rowCount64))
	}
	rowCount := int(rowCount64)

	colCount64, err := d.br.readVarintInt63()
	if err != nil {
		return err
	}
	if colCount64 < 0 || colCount64 > qwpMaxColumnsPerTable {
		return newQwpDecodeError(fmt.Sprintf(
			"column_count out of range: %d", colCount64))
	}
	columnCount := int(colCount64)

	// Schema section
	schemaMode, err := d.br.readByte()
	if err != nil {
		return err
	}
	schemaId64, err := d.br.readVarintInt63()
	if err != nil {
		return err
	}
	if schemaId64 < 0 || schemaId64 >= qwpDefaultMaxSchemasPerConnection {
		return newQwpDecodeError(fmt.Sprintf(
			"schema_id out of range: %d", schemaId64))
	}
	schemaId := int(schemaId64)

	var cols []qwpColumnSchemaInfo
	switch qwpSchemaMode(schemaMode) {
	case qwpSchemaModeFull:
		cols, err = d.parseFullSchema(columnCount)
		if err != nil {
			return err
		}
		d.schemas.put(schemaId, cols)
	case qwpSchemaModeReference:
		var ok bool
		cols, ok = d.schemas.get(schemaId)
		if !ok {
			return newQwpDecodeError(fmt.Sprintf(
				"schema id %d not registered on this connection",
				schemaId))
		}
		if len(cols) != columnCount {
			return newQwpDecodeError(fmt.Sprintf(
				"schema id %d column count mismatch: registered=%d frame=%d",
				schemaId, len(cols), columnCount))
		}
	default:
		return newQwpDecodeError(fmt.Sprintf(
			"unknown schema mode 0x%02X", schemaMode))
	}

	// Grow the layout pool to columnCount. Pool-owned slices are
	// preserved so subsequent batches with the same width don't
	// reallocate.
	for len(d.layouts) < columnCount {
		d.layouts = append(d.layouts, qwpColumnLayout{})
	}

	// Populate `out` up-front so per-column parsers can index into its
	// layouts slice via d.layouts (the batch and the decoder share the
	// same backing layouts). This avoids a second copy at the end.
	out.payload = payload
	out.requestId = requestId
	out.batchSeq = batchSeq
	out.rowCount = rowCount
	out.columnCount = columnCount
	out.columns = cols
	out.layouts = d.layouts[:columnCount]

	// Per-column parse
	for i := 0; i < columnCount; i++ {
		l := &out.layouts[i]
		l.clear()
		l.info = &cols[i]
		if err := d.parseColumn(l, rowCount); err != nil {
			return err
		}
	}
	return nil
}

// parseFullSchema reads full schema entries: per column, (colNameLen
// varint, name bytes, wireType byte). Decimal scale and geohash
// precision are NOT in the schema section — they are per-column and
// live in the data section.
func (d *qwpQueryDecoder) parseFullSchema(columnCount int) ([]qwpColumnSchemaInfo, error) {
	// Use a fresh slice per call (rather than pooling). The slice is
	// handed to the schema registry and must outlive the decode, so
	// reusing buffer pools here would invalidate the registry on the
	// next batch.
	cols := make([]qwpColumnSchemaInfo, columnCount)
	for i := 0; i < columnCount; i++ {
		nameLen64, err := d.br.readVarintInt63()
		if err != nil {
			return nil, err
		}
		if nameLen64 < 0 || nameLen64 > qwpMaxColumnNameLen {
			return nil, newQwpDecodeError(fmt.Sprintf(
				"column name length out of range: %d", nameLen64))
		}
		nameBytes, err := d.br.slice(int(nameLen64))
		if err != nil {
			return nil, err
		}
		wireType, err := d.br.readByte()
		if err != nil {
			return nil, err
		}
		// Copy name: nameBytes aliases the payload, which becomes stale
		// once the frame is recycled. Schema info is kept across frames
		// via the registry, so we need an owned string.
		cols[i] = qwpColumnSchemaInfo{
			name:     string(nameBytes),
			wireType: qwpTypeCode(wireType),
		}
	}
	return cols, nil
}

// parseColumn dispatches per-column decoding by wire type.
func (d *qwpQueryDecoder) parseColumn(l *qwpColumnLayout, rowCount int) error {
	if err := d.parseNullSection(l, rowCount); err != nil {
		return err
	}
	wt := l.info.wireType
	switch wt {
	case qwpTypeBoolean:
		bits := (l.nonNullCount + 7) >> 3
		s, err := d.br.slice(bits)
		if err != nil {
			return err
		}
		l.values = s
		return nil
	case qwpTypeByte:
		return d.readFixed(l, 1)
	case qwpTypeShort, qwpTypeChar:
		return d.readFixed(l, 2)
	case qwpTypeInt, qwpTypeFloat, qwpTypeIPv4:
		return d.readFixed(l, 4)
	case qwpTypeLong, qwpTypeDouble:
		return d.readFixed(l, 8)
	case qwpTypeDate, qwpTypeTimestamp, qwpTypeTimestampNano:
		return d.parseTimestamp(l)
	case qwpTypeUuid:
		return d.readFixed(l, 16)
	case qwpTypeLong256:
		return d.readFixed(l, 32)
	case qwpTypeDecimal64:
		return d.parseDecimal(l, 8)
	case qwpTypeDecimal128:
		return d.parseDecimal(l, 16)
	case qwpTypeDecimal256:
		return d.parseDecimal(l, 32)
	case qwpTypeVarchar, qwpTypeBinary:
		return d.parseString(l)
	case qwpTypeSymbol:
		return d.parseSymbol(l, rowCount)
	case qwpTypeGeohash:
		return d.parseGeohash(l)
	case qwpTypeDoubleArray, qwpTypeLongArray:
		return d.parseArray(l, rowCount)
	default:
		return newQwpDecodeError(fmt.Sprintf(
			"unsupported wire type 0x%02X", byte(wt)))
	}
}

// parseNullSection reads the null flag + optional bitmap. Non-zero
// flag means a bitmap follows; zero flag means no nulls (nonNullCount
// == rowCount, no per-row index materialisation needed).
func (d *qwpQueryDecoder) parseNullSection(l *qwpColumnLayout, rowCount int) error {
	flag, err := d.br.readByte()
	if err != nil {
		return err
	}
	if flag == 0 {
		l.nullBitmap = nil
		l.nonNullIdx = l.nonNullIdx[:0]
		l.nonNullCount = rowCount
		return nil
	}
	bitmapLen := (rowCount + 7) >> 3
	bitmap, err := d.br.slice(bitmapLen)
	if err != nil {
		return err
	}
	l.nullBitmap = bitmap
	// Grow nonNullIdx to rowCount (preserve backing array across
	// batches — pool semantics from qwpColumnLayout.clear).
	if cap(l.nonNullIdx) < rowCount {
		l.nonNullIdx = make([]int32, rowCount)
	} else {
		l.nonNullIdx = l.nonNullIdx[:rowCount]
	}
	dense := int32(0)
	for i := 0; i < rowCount; i++ {
		if bitmap[i>>3]&(1<<(i&7)) != 0 {
			l.nonNullIdx[i] = -1
		} else {
			l.nonNullIdx[i] = dense
			dense++
		}
	}
	l.nonNullCount = int(dense)
	return nil
}

// readFixed advances past nonNullCount * sizeBytes of dense values.
func (d *qwpQueryDecoder) readFixed(l *qwpColumnLayout, sizeBytes int) error {
	total := sizeBytes * l.nonNullCount
	s, err := d.br.slice(total)
	if err != nil {
		return err
	}
	l.values = s
	return nil
}

// parseTimestamp handles DATE/TIMESTAMP/TIMESTAMP_NANOS columns. With
// FLAG_GORILLA set at the message level, each column is prefixed with
// an encoding discriminator (0x00 raw / 0x01 Gorilla). Without the
// flag, the column is plain int64 LE values (no discriminator).
func (d *qwpQueryDecoder) parseTimestamp(l *qwpColumnLayout) error {
	if !d.gorillaOn {
		return d.readFixed(l, 8)
	}
	enc, err := d.br.readByte()
	if err != nil {
		return err
	}
	switch enc {
	case qwpTsEncodingUncompressed:
		return d.readFixed(l, 8)
	case qwpTsEncodingGorilla:
		if l.nonNullCount < 3 {
			return newQwpDecodeError(fmt.Sprintf(
				"Gorilla-encoded TIMESTAMP with nonNull<3: %d",
				l.nonNullCount))
		}
		firstTs, err := d.br.readInt64LE()
		if err != nil {
			return err
		}
		secondTs, err := d.br.readInt64LE()
		if err != nil {
			return err
		}
		// Decode the remaining values into the layout's owned buffer.
		if cap(l.timestampBuf) < l.nonNullCount {
			l.timestampBuf = make([]int64, l.nonNullCount)
		} else {
			l.timestampBuf = l.timestampBuf[:l.nonNullCount]
		}
		l.timestampBuf[0] = firstTs
		l.timestampBuf[1] = secondTs

		// The bitstream covers the remainder of the column's byte
		// region, but we don't yet know how many bytes it consumes
		// until the decoder tells us via bytesConsumed(). Feed it the
		// rest of the reader's buffer; the decoder will only read
		// what's needed.
		remaining := d.br.buf[d.br.pos:]
		d.gorilla.reset(firstTs, secondTs, remaining)
		for i := 2; i < l.nonNullCount; i++ {
			ts, err := d.gorilla.decodeNext()
			if err != nil {
				return err
			}
			l.timestampBuf[i] = ts
		}
		// bytesConsumed() is bounded by the slice we passed into reset()
		// (which was d.br.buf[d.br.pos:]), so advance cannot overrun the
		// outer reader. If it ever does, a decoder invariant was broken.
		consumed := d.gorilla.bytesConsumed()
		if err := d.br.advance(consumed); err != nil {
			panic(fmt.Sprintf("qwp: internal: Gorilla bytesConsumed=%d overruns frame (pos=%d, buflen=%d)",
				consumed, d.br.pos, len(d.br.buf)))
		}
		// Reinterpret the int64 slice as []byte so the Int64 accessor
		// path stays uniform (it reads 8 LE bytes per dense index).
		// This is safe: Go guarantees int64 is 8 bytes and little-
		// endian on every architecture the client supports.
		l.values = int64sAsBytes(l.timestampBuf)
		return nil
	default:
		return newQwpDecodeError(fmt.Sprintf(
			"unknown TIMESTAMP encoding 0x%02X", enc))
	}
}

// parseDecimal reads the per-column scale byte followed by
// nonNullCount * sizeBytes of dense value data.
func (d *qwpQueryDecoder) parseDecimal(l *qwpColumnLayout, sizeBytes int) error {
	scale, err := d.br.readByte()
	if err != nil {
		return err
	}
	l.info.scale = scale
	return d.readFixed(l, sizeBytes)
}

// parseString handles VARCHAR and BINARY — they share the
// (N+1)*4-byte offsets array + concatenated bytes layout. STRING
// (wire type 0x08) is dispatched as "unsupported wire type" upstream
// and never reaches this function.
func (d *qwpQueryDecoder) parseString(l *qwpColumnLayout) error {
	offsetsLen := 4 * (l.nonNullCount + 1)
	offsets, err := d.br.slice(offsetsLen)
	if err != nil {
		return err
	}
	// totalBytes = offsets[nonNullCount] (uint32 LE). It's signed on
	// the wire by implication: a negative value cast from uint32
	// passes the slice bound check (slice would then address a large
	// prefix of the buffer), so we explicitly reject negative totals
	// before allocating the bytes slice.
	var totalBytes int32
	if l.nonNullCount == 0 {
		totalBytes = 0
	} else {
		totalBytes = int32(binary.LittleEndian.Uint32(offsets[l.nonNullCount*4:]))
	}
	if totalBytes < 0 {
		return newQwpDecodeError(fmt.Sprintf(
			"invalid string column total bytes: %d", totalBytes))
	}
	stringBytes, err := d.br.slice(int(totalBytes))
	if err != nil {
		return err
	}
	l.values = offsets
	l.stringBytes = stringBytes
	return nil
}

// parseSymbol reads one varint dictionary id per non-null row and
// snapshots the connection-scoped dict so the resulting column layout
// resolves ids against the dict state at decode time (not read time —
// subsequent batches may grow the dict).
func (d *qwpQueryDecoder) parseSymbol(l *qwpColumnLayout, rowCount int) error {
	if !d.deltaOn {
		// Phase 1 server always sets FLAG_DELTA_SYMBOL_DICT. A frame
		// without it would require a per-column dictionary path we
		// haven't implemented — refuse cleanly rather than mis-parse.
		return newQwpDecodeError(
			"SYMBOL column without FLAG_DELTA_SYMBOL_DICT is not supported")
	}
	l.symbolDict = d.dict.snapshot()

	// Size symbolRowIds to rowCount; NULL rows hold undefined values
	// (accessors null-check first).
	if cap(l.symbolRowIds) < rowCount {
		l.symbolRowIds = make([]int32, rowCount)
	} else {
		l.symbolRowIds = l.symbolRowIds[:rowCount]
	}
	dictSize := len(l.symbolDict.entries)
	noNulls := l.nullBitmap == nil
	for i := 0; i < rowCount; i++ {
		if !noNulls && l.nonNullIdx[i] < 0 {
			continue
		}
		id64, err := d.br.readVarintInt63()
		if err != nil {
			return err
		}
		if id64 < 0 || int(id64) >= dictSize {
			return newQwpDecodeError(fmt.Sprintf(
				"symbol index out of range: %d", id64))
		}
		l.symbolRowIds[i] = int32(id64)
	}
	return nil
}

// parseGeohash reads the precision varint and per-row packed bits.
func (d *qwpQueryDecoder) parseGeohash(l *qwpColumnLayout) error {
	precBits64, err := d.br.readVarintInt63()
	if err != nil {
		return err
	}
	if precBits64 < 0 || precBits64 > 60 {
		return newQwpDecodeError(fmt.Sprintf(
			"geohash precision out of range: %d", precBits64))
	}
	l.info.precisionBits = uint16(precBits64)
	bytesPerValue := int((precBits64 + 7) / 8)
	return d.readFixed(l, bytesPerValue)
}

// parseArray reads per-row array entries (skipping NULL rows per the
// Java reference decoder) and bookkeeps (start, length) into
// layout.values for each row. The values slice is set to alias the
// entire array-data region of the payload so accessors can address
// elements by (row-start + offset).
//
// An inline nDims byte of 0 is the Java reference's NULL sentinel for
// an array row: the decoder marks the row NULL (promoting the null
// bitmap to an owned, mutable copy the first time it is needed) and
// consumes no further bytes for that row.
func (d *qwpQueryDecoder) parseArray(l *qwpColumnLayout, rowCount int) error {
	base := d.br.pos
	if cap(l.arrayRowStart) < rowCount {
		l.arrayRowStart = make([]int32, rowCount)
	} else {
		l.arrayRowStart = l.arrayRowStart[:rowCount]
	}
	if cap(l.arrayRowLen) < rowCount {
		l.arrayRowLen = make([]int32, rowCount)
	} else {
		l.arrayRowLen = l.arrayRowLen[:rowCount]
	}
	noNulls := l.nullBitmap == nil
	ownedBitmap := false
	for i := 0; i < rowCount; i++ {
		if !noNulls && l.nonNullIdx[i] < 0 {
			l.arrayRowStart[i] = 0
			l.arrayRowLen[i] = 0
			continue
		}
		rowStart := d.br.pos
		nDimsByte, err := d.br.readByte()
		if err != nil {
			return err
		}
		nDims := int(nDimsByte)
		if nDims == 0 {
			// nDims=0 is the NULL sentinel in the Java reference.
			// Promote the null bitmap to an owned copy (creating a
			// fresh zeroed one if none was sent) so we can set the
			// bit, then consume no further bytes for this row.
			if !ownedBitmap {
				owned := make([]byte, (rowCount+7)>>3)
				copy(owned, l.nullBitmap)
				l.nullBitmap = owned
				ownedBitmap = true
			}
			l.nullBitmap[i>>3] |= 1 << (i & 7)
			l.nonNullCount--
			l.arrayRowStart[i] = 0
			l.arrayRowLen[i] = 0
			continue
		}
		if nDims > qwpMaxArrayNDims {
			return newQwpDecodeError(fmt.Sprintf(
				"ARRAY nDims out of range [0, %d]: %d", qwpMaxArrayNDims, nDims))
		}
		shapeBytes, err := d.br.slice(4 * nDims)
		if err != nil {
			return err
		}
		elements := int64(1)
		for dim := 0; dim < nDims; dim++ {
			dl := int32(binary.LittleEndian.Uint32(shapeBytes[dim*4:]))
			if dl < 0 {
				return newQwpDecodeError(fmt.Sprintf(
					"ARRAY dim %d is negative: %d", dim, dl))
			}
			elements *= int64(dl)
			if elements > qwpMaxArrayElements {
				return newQwpDecodeError(fmt.Sprintf(
					"ARRAY element count exceeds limit (%d > %d)",
					elements, qwpMaxArrayElements))
			}
		}
		if err := d.br.advance(int(elements) * 8); err != nil {
			return err
		}
		l.arrayRowStart[i] = int32(rowStart - base)
		l.arrayRowLen[i] = int32(d.br.pos - rowStart)
	}
	// values slice covers the entire array region read above.
	l.values = d.br.buf[base:d.br.pos]
	return nil
}

// parseFrameHeader validates the 12-byte QWP header, primes d.br to the
// frame body, reads the msg_kind byte, and returns it. Sets d.deltaOn /
// d.gorillaOn from the flags byte. Rejects FLAG_ZSTD — this client does
// not yet implement zstd decompression.
//
// Shared by every per-kind decoder (decode / decodeResultEnd /
// decodeQueryError / decodeExecDone) so header validation stays uniform.
func (d *qwpQueryDecoder) parseFrameHeader(payload []byte) (qwpMsgKind, error) {
	if len(payload) < qwpHeaderSize+1 {
		return 0, newQwpDecodeError(fmt.Sprintf(
			"frame payload too short: %d", len(payload)))
	}
	magic := binary.LittleEndian.Uint32(payload[0:4])
	if magic != qwpMagic {
		return 0, newQwpDecodeError(fmt.Sprintf("bad magic 0x%08X", magic))
	}
	if payload[4] != qwpVersion {
		return 0, newQwpDecodeError(fmt.Sprintf(
			"unsupported version %d", payload[4]))
	}
	flags := payload[qwpHeaderOffsetFlags]
	d.deltaOn = flags&qwpFlagDeltaSymbolDict != 0
	d.gorillaOn = flags&qwpFlagGorilla != 0
	if flags&qwpFlagZstd != 0 {
		return 0, newQwpDecodeError(
			"FLAG_ZSTD set but zstd not yet supported in this client")
	}
	d.br.reset(payload[qwpHeaderSize:])
	kindByte, err := d.br.readByte()
	if err != nil {
		return 0, err
	}
	return qwpMsgKind(kindByte), nil
}

// decodeResultEnd parses a RESULT_END (0x12) frame. The frame announces
// the end of a streaming query and carries the server-reported total
// row count.
//
// Wire layout (after the 12-byte header):
//
//	msg_kind(1) + request_id(int64 LE) + final_seq(varint) + total_rows(varint)
//
// final_seq is currently unused by this client — it matches the last
// batch's seq and is already tracked by the I/O layer. It is still
// consumed so the cursor is aligned when reading total_rows.
func (d *qwpQueryDecoder) decodeResultEnd(payload []byte) (requestId int64, totalRows int64, err error) {
	msgKind, err := d.parseFrameHeader(payload)
	if err != nil {
		return 0, 0, err
	}
	if msgKind != qwpMsgKindResultEnd {
		return 0, 0, newQwpDecodeError(fmt.Sprintf(
			"expected RESULT_END (0x12), got 0x%02X", byte(msgKind)))
	}
	requestId, err = d.br.readInt64LE()
	if err != nil {
		return 0, 0, err
	}
	// final_seq: read and discard. readVarint already rejects
	// overflowing 10-byte sequences, matching the Java guard.
	if _, err = d.br.readVarint(); err != nil {
		return 0, 0, err
	}
	totalRows, err = d.br.readVarintInt63()
	if err != nil {
		return 0, 0, err
	}
	return requestId, totalRows, nil
}

// decodeQueryError parses a QUERY_ERROR (0x13) frame. The returned
// QwpQueryError carries the server's status byte and UTF-8 message.
//
// Wire layout (after the 12-byte header):
//
//	msg_kind(1) + request_id(int64 LE) + status(1) + msg_len(uint16 LE) + message(msg_len UTF-8 bytes)
//
// msg_len is treated as unsigned (range 0..65535); the qwpByteReader.slice
// call below rejects a msg_len that overruns the frame — this is the
// port of Java's "msg_len ... exceeds frame remainder" hardening guard.
func (d *qwpQueryDecoder) decodeQueryError(payload []byte) (*QwpQueryError, error) {
	msgKind, err := d.parseFrameHeader(payload)
	if err != nil {
		return nil, err
	}
	if msgKind != qwpMsgKindQueryError {
		return nil, newQwpDecodeError(fmt.Sprintf(
			"expected QUERY_ERROR (0x13), got 0x%02X", byte(msgKind)))
	}
	requestId, err := d.br.readInt64LE()
	if err != nil {
		return nil, err
	}
	status, err := d.br.readByte()
	if err != nil {
		return nil, err
	}
	msgLen, err := d.br.readUint16LE()
	if err != nil {
		return nil, err
	}
	msgBytes, err := d.br.slice(int(msgLen))
	if err != nil {
		return nil, wrapQwpDecodeError(fmt.Sprintf(
			"QUERY_ERROR msg_len %d exceeds frame remainder", msgLen), err)
	}
	return &QwpQueryError{
		RequestId: requestId,
		Status:    qwpStatusCode(status),
		// Copy: msgBytes aliases the payload, which is reclaimed once
		// the I/O goroutine advances past the frame. QwpQueryError is
		// surfaced to the user and outlives the frame.
		Message: string(msgBytes),
	}, nil
}

// decodeExecDone parses an EXEC_DONE (0x16) frame — the terminal ack
// for a non-SELECT statement.
//
// Wire layout (after the 12-byte header):
//
//	msg_kind(1) + request_id(int64 LE) + op_type(1) + rows_affected(varint)
func (d *qwpQueryDecoder) decodeExecDone(payload []byte) (requestId int64, result ExecResult, err error) {
	msgKind, err := d.parseFrameHeader(payload)
	if err != nil {
		return 0, ExecResult{}, err
	}
	if msgKind != qwpMsgKindExecDone {
		return 0, ExecResult{}, newQwpDecodeError(fmt.Sprintf(
			"expected EXEC_DONE (0x16), got 0x%02X", byte(msgKind)))
	}
	requestId, err = d.br.readInt64LE()
	if err != nil {
		return 0, ExecResult{}, err
	}
	opType, err := d.br.readByte()
	if err != nil {
		return 0, ExecResult{}, err
	}
	rowsAffected, err := d.br.readVarintInt63()
	if err != nil {
		return 0, ExecResult{}, err
	}
	return requestId, ExecResult{
		OpType:       opType,
		RowsAffected: rowsAffected,
	}, nil
}

// int64sAsBytes reinterprets an []int64 as []byte (len*8, cap*8)
// without copying. Used by parseTimestamp to make the Gorilla-decoded
// values region look identical to a raw int64 LE region, so the
// QwpColumnBatch.Int64 accessor path stays uniform.
//
// Safety: int64 is 8 bytes on every supported architecture and Go
// stores them little-endian on all targets questdb-client supports.
// unsafe.Slice is the canonical way to do this reinterpretation since
// Go 1.17.
func int64sAsBytes(s []int64) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&s[0])), len(s)*8)
}
