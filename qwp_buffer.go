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
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

// qwpLongNull is the uint64 bit pattern for int64 MinInt64
// (0x8000000000000000), used as the null sentinel for LONG,
// TIMESTAMP, DATE, UUID, and LONG256 columns.
const qwpLongNull uint64 = 0x8000000000000000

// qwpColumnBuffer stores columnar data for a single column within a
// QWP table buffer. Data is stored in a layout optimized for the QWP
// wire encoding: fixed-width values in a contiguous byte slice,
// booleans bit-packed, and strings as cumulative offsets plus a data
// arena.
//
// Each column type uses a different subset of fields:
//   - Fixed-width types (BYTE..LONG256): fixedData with fixedSize stride
//   - BOOLEAN: boolData (bit-packed, LSB-first)
//   - STRING/VARCHAR: strOffsets + strData
//   - SYMBOL: symbolIDs (global dictionary IDs)
//
// For nullable columns, a null bitmap tracks which rows are NULL.
type qwpColumnBuffer struct {
	name     string
	typeCode qwpTypeCode // base type code (without nullable flag)
	nullable bool

	// fixedSize is the per-value byte stride for fixed-width types,
	// 0 for BOOLEAN (bit-packed), or -1 for variable-width types.
	fixedSize int

	// fixedData stores contiguous fixed-width values. Row i occupies
	// bytes [i*fixedSize : (i+1)*fixedSize]. Null rows contain
	// sentinel values (e.g. MinInt64 for LONG, NaN for DOUBLE).
	fixedData []byte

	// boolData stores bit-packed boolean values (TYPE_BOOLEAN only).
	// Bit i corresponds to row i, LSB-first within each byte.
	boolData []byte

	// strOffsets and strData store variable-width string/varchar
	// data. strOffsets has rowCount+1 entries with strOffsets[0]==0.
	// String for row i spans strData[strOffsets[i]:strOffsets[i+1]].
	// Null rows repeat the previous offset (zero-length string).
	strOffsets []uint32
	strData    []byte

	// symbolIDs stores one global symbol dictionary ID per row
	// (TYPE_SYMBOL only). Null rows use -1 as sentinel.
	symbolIDs []int32

	// nullBitmap has one bit per row for nullable columns only.
	// A set bit means the row is NULL, LSB-first within each byte.
	// May be shorter than (rowCount+7)/8 when trailing rows are
	// non-null; the encoder pads with zero bytes.
	nullBitmap []byte
	nullCount  int

	// rowCount is the total number of rows including nulls.
	rowCount int

	// scale is the decimal scale (0–76) for DECIMAL types, or -1
	// if not yet established. Set on the first non-null decimal value.
	scale int8

	// geohashPrecision is the bit precision (1–60) for GEOHASH,
	// or -1 if not yet established.
	geohashPrecision int8
}

// newQwpColumnBuffer creates a column buffer for the given name,
// type, and nullability. The fixedSize is derived from the type code.
func newQwpColumnBuffer(name string, typeCode qwpTypeCode, nullable bool) *qwpColumnBuffer {
	c := &qwpColumnBuffer{
		name:             name,
		typeCode:         typeCode,
		nullable:         nullable,
		fixedSize:        qwpFixedTypeSize(typeCode),
		scale:            -1,
		geohashPrecision: -1,
	}
	if typeCode == qwpTypeString || typeCode == qwpTypeVarchar {
		c.strOffsets = []uint32{0}
	}
	return c
}

// wireTypeCode returns the type code byte for the wire format,
// including the nullable flag in the high bit.
func (c *qwpColumnBuffer) wireTypeCode() byte {
	tc := byte(c.typeCode)
	if c.nullable {
		tc |= byte(qwpTypeNullableFlag)
	}
	return tc
}

// --- null bitmap helpers ------------------------------------------------

// markNull sets the null bit for the current row (at index rowCount)
// and increments nullCount. The bitmap is grown as needed.
func (c *qwpColumnBuffer) markNull() {
	byteIdx := c.rowCount / 8
	for len(c.nullBitmap) <= byteIdx {
		c.nullBitmap = append(c.nullBitmap, 0)
	}
	c.nullBitmap[byteIdx] |= 1 << uint(c.rowCount%8)
	c.nullCount++
}

// nullBitmapLen returns the number of bytes needed for the null
// bitmap to cover all rowCount rows.
func (c *qwpColumnBuffer) nullBitmapLen() int {
	return (c.rowCount + 7) / 8
}

// --- fixed-width append helpers -----------------------------------------
// These write directly into fixedData with no intermediate allocation.

func (c *qwpColumnBuffer) appendByte(v byte) {
	c.fixedData = append(c.fixedData, v)
}

func (c *qwpColumnBuffer) appendU16(v uint16) {
	n := len(c.fixedData)
	c.fixedData = append(c.fixedData, 0, 0)
	binary.LittleEndian.PutUint16(c.fixedData[n:], v)
}

func (c *qwpColumnBuffer) appendU32(v uint32) {
	n := len(c.fixedData)
	c.fixedData = append(c.fixedData, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(c.fixedData[n:], v)
}

func (c *qwpColumnBuffer) appendU64(v uint64) {
	n := len(c.fixedData)
	c.fixedData = append(c.fixedData, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(c.fixedData[n:], v)
}

func (c *qwpColumnBuffer) appendI64BE(v int64) {
	n := len(c.fixedData)
	c.fixedData = append(c.fixedData, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(c.fixedData[n:], uint64(v))
}

// --- per-type add methods -----------------------------------------------

// addByte appends an int8 value (TYPE_BYTE).
func (c *qwpColumnBuffer) addByte(v int8) {
	c.appendByte(byte(v))
	c.rowCount++
}

// addShort appends an int16 value in LE byte order (TYPE_SHORT).
func (c *qwpColumnBuffer) addShort(v int16) {
	c.appendU16(uint16(v))
	c.rowCount++
}

// addInt32 appends an int32 value in LE byte order (TYPE_INT).
func (c *qwpColumnBuffer) addInt32(v int32) {
	c.appendU32(uint32(v))
	c.rowCount++
}

// addLong appends an int64 value in LE byte order (TYPE_LONG).
func (c *qwpColumnBuffer) addLong(v int64) {
	c.appendU64(uint64(v))
	c.rowCount++
}

// addFloat32 appends an IEEE 754 float32 in LE byte order (TYPE_FLOAT).
func (c *qwpColumnBuffer) addFloat32(v float32) {
	c.appendU32(math.Float32bits(v))
	c.rowCount++
}

// addDouble appends an IEEE 754 float64 in LE byte order (TYPE_DOUBLE).
func (c *qwpColumnBuffer) addDouble(v float64) {
	c.appendU64(math.Float64bits(v))
	c.rowCount++
}

// addTimestamp appends an int64 microsecond timestamp in LE byte
// order (TYPE_TIMESTAMP).
func (c *qwpColumnBuffer) addTimestamp(v int64) {
	c.appendU64(uint64(v))
	c.rowCount++
}

// addBool appends a boolean value, bit-packed into boolData.
// Bit i = row i's value, LSB-first within each byte (TYPE_BOOLEAN).
func (c *qwpColumnBuffer) addBool(v bool) {
	byteIdx := c.rowCount / 8
	for len(c.boolData) <= byteIdx {
		c.boolData = append(c.boolData, 0)
	}
	if v {
		c.boolData[byteIdx] |= 1 << uint(c.rowCount%8)
	}
	c.rowCount++
}

// addString appends a UTF-8 string value. The string bytes are
// concatenated into strData and a cumulative offset is appended
// to strOffsets (TYPE_STRING, TYPE_VARCHAR).
func (c *qwpColumnBuffer) addString(v string) {
	c.strData = append(c.strData, v...)
	c.strOffsets = append(c.strOffsets, uint32(len(c.strData)))
	c.rowCount++
}

// addSymbolID appends a global symbol dictionary ID (TYPE_SYMBOL).
func (c *qwpColumnBuffer) addSymbolID(id int32) {
	c.symbolIDs = append(c.symbolIDs, id)
	c.rowCount++
}

// addChar appends a rune as a UTF-16 code unit in LE byte order
// (TYPE_CHAR). Only BMP code points (U+0000..U+FFFF) are supported.
func (c *qwpColumnBuffer) addChar(v rune) {
	c.appendU16(uint16(v))
	c.rowCount++
}

// addUuid appends a UUID as two uint64s in wire order: lo first,
// then hi, both little-endian (TYPE_UUID, 16 bytes total).
func (c *qwpColumnBuffer) addUuid(hi, lo uint64) {
	c.appendU64(lo)
	c.appendU64(hi)
	c.rowCount++
}

// addLong256 appends a 256-bit integer as four uint64s in
// little-endian byte order (TYPE_LONG256, 32 bytes total).
func (c *qwpColumnBuffer) addLong256(l0, l1, l2, l3 uint64) {
	c.appendU64(l0)
	c.appendU64(l1)
	c.appendU64(l2)
	c.appendU64(l3)
	c.rowCount++
}

// addNull appends a type-appropriate null sentinel value. For
// nullable columns, the corresponding null bitmap bit is also set.
// Sentinel values match the QuestDB conventions:
//   - LONG/TIMESTAMP/DATE: math.MinInt64
//   - DOUBLE: NaN
//   - FLOAT: NaN
//   - INT/SHORT/BYTE/CHAR: 0
//   - STRING/VARCHAR: empty (repeated offset)
//   - SYMBOL: -1
//   - UUID: two MinInt64
//   - LONG256: four MinInt64
func (c *qwpColumnBuffer) addNull() {
	if c.nullable {
		c.markNull()
	}

	switch c.typeCode {
	case qwpTypeBoolean:
		// False sentinel; bit stays 0 from zero-initialization.
		byteIdx := c.rowCount / 8
		for len(c.boolData) <= byteIdx {
			c.boolData = append(c.boolData, 0)
		}

	case qwpTypeByte:
		c.appendByte(0)

	case qwpTypeShort, qwpTypeChar:
		c.fixedData = append(c.fixedData, 0, 0)

	case qwpTypeInt:
		c.fixedData = append(c.fixedData, 0, 0, 0, 0)

	case qwpTypeFloat:
		c.appendU32(math.Float32bits(float32(math.NaN())))

	case qwpTypeLong, qwpTypeTimestamp, qwpTypeDate, qwpTypeTimestampNano:
		c.appendU64(qwpLongNull)

	case qwpTypeDouble:
		c.appendU64(math.Float64bits(math.NaN()))

	case qwpTypeString, qwpTypeVarchar:
		// Repeat current offset → zero-length string sentinel.
		c.strOffsets = append(c.strOffsets, uint32(len(c.strData)))

	case qwpTypeSymbol:
		c.symbolIDs = append(c.symbolIDs, -1)

	case qwpTypeUuid:
		c.appendU64(qwpLongNull)
		c.appendU64(qwpLongNull)

	case qwpTypeLong256:
		c.appendU64(qwpLongNull)
		c.appendU64(qwpLongNull)
		c.appendU64(qwpLongNull)
		c.appendU64(qwpLongNull)

	case qwpTypeGeohash:
		// -1 (all bits set) is the QuestDB geohash null sentinel.
		c.appendU64(math.MaxUint64)

	case qwpTypeDecimal64:
		c.fixedData = append(c.fixedData, 0, 0, 0, 0, 0, 0, 0, 0)

	case qwpTypeDecimal128:
		c.fixedData = append(c.fixedData,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0)

	case qwpTypeDecimal256:
		c.fixedData = append(c.fixedData,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0)

	default:
		panic("qwp: addNull called on unsupported column type")
	}

	c.rowCount++
}

// reset clears all row data but retains the backing array capacity
// and column metadata (name, type, nullable, scale, precision).
func (c *qwpColumnBuffer) reset() {
	c.fixedData = c.fixedData[:0]
	c.boolData = c.boolData[:0]
	if c.typeCode == qwpTypeString || c.typeCode == qwpTypeVarchar {
		c.strOffsets = c.strOffsets[:1]
		c.strOffsets[0] = 0
	} else {
		c.strOffsets = c.strOffsets[:0]
	}
	c.strData = c.strData[:0]
	c.symbolIDs = c.symbolIDs[:0]
	c.nullBitmap = c.nullBitmap[:0]
	c.nullCount = 0
	c.rowCount = 0
}

// truncateTo rolls the column back to exactly n rows, discarding
// any data beyond that point. Used by qwpTableBuffer.cancelRow().
func (c *qwpColumnBuffer) truncateTo(n int) {
	if n >= c.rowCount {
		return
	}

	switch c.typeCode {
	case qwpTypeBoolean:
		newLen := (n + 7) / 8
		if newLen < len(c.boolData) {
			c.boolData = c.boolData[:newLen]
		}
		// Clear any trailing bits in the last byte.
		if n > 0 && n%8 != 0 {
			c.boolData[newLen-1] &= (1 << uint(n%8)) - 1
		}

	case qwpTypeString, qwpTypeVarchar:
		c.strOffsets = c.strOffsets[:n+1]
		c.strData = c.strData[:c.strOffsets[n]]

	case qwpTypeSymbol:
		c.symbolIDs = c.symbolIDs[:n]

	default:
		// Fixed-width types.
		if c.fixedSize > 0 {
			c.fixedData = c.fixedData[:n*c.fixedSize]
		}
	}

	// Truncate null bitmap and recount.
	if c.nullable {
		newBitmapLen := (n + 7) / 8
		if newBitmapLen < len(c.nullBitmap) {
			c.nullBitmap = c.nullBitmap[:newBitmapLen]
		}
		if n > 0 && n%8 != 0 && newBitmapLen > 0 {
			c.nullBitmap[newBitmapLen-1] &= (1 << uint(n%8)) - 1
		}
		c.nullCount = 0
		for _, b := range c.nullBitmap {
			c.nullCount += bits.OnesCount8(b)
		}
	}

	c.rowCount = n
}

// --- qwpTableBuffer ---------------------------------------------------

// qwpTableBuffer aggregates columnar data for a single table. It
// manages multiple qwpColumnBuffer instances and handles row commits
// with automatic gap-filling for columns not set in a given row.
type qwpTableBuffer struct {
	tableName   string
	columns     []*qwpColumnBuffer
	columnIndex map[string]int // column name → index in columns slice

	// rowCount is the number of committed (finalized) rows.
	rowCount int

	// committedColumnCount tracks how many columns existed at the
	// last commitRow() call. Columns with index >= this value were
	// added during the current in-progress row and should be
	// removed on cancelRow().
	committedColumnCount int

	// Schema hash caching. The hash is lazily computed on first
	// call to getSchemaHash() and invalidated when columns change.
	schemaHash      int64
	schemaHashValid bool
}

// newQwpTableBuffer creates a table buffer for the given table name.
func newQwpTableBuffer(tableName string) *qwpTableBuffer {
	return &qwpTableBuffer{
		tableName:   tableName,
		columnIndex: make(map[string]int),
	}
}

// getOrCreateColumn looks up an existing column by name or creates a
// new one. Returns an error if a column with the same name but a
// different type already exists, or if the column was already set
// for the current in-progress row (duplicate column in same row).
func (tb *qwpTableBuffer) getOrCreateColumn(name string, typeCode qwpTypeCode, nullable bool) (*qwpColumnBuffer, error) {
	idx, exists := tb.columnIndex[name]
	if exists {
		col := tb.columns[idx]
		if col.typeCode != typeCode {
			return nil, fmt.Errorf(
				"qwp: column %q type conflict: existing %d, got %d",
				name, col.typeCode, typeCode,
			)
		}
		// Check for duplicate column within the same row.
		if col.rowCount > tb.rowCount {
			return nil, fmt.Errorf("qwp: column %q already set for current row", name)
		}
		return col, nil
	}

	// New column. Check limits.
	if len(tb.columns) >= qwpMaxColumnsPerTable {
		return nil, fmt.Errorf(
			"qwp: table %q exceeds maximum column count (%d)",
			tb.tableName, qwpMaxColumnsPerTable,
		)
	}

	col := newQwpColumnBuffer(name, typeCode, nullable)

	// Backfill with nulls for all previously committed rows so
	// the new column has the same row count as the table.
	for i := 0; i < tb.rowCount; i++ {
		col.addNull()
	}

	tb.columnIndex[name] = len(tb.columns)
	tb.columns = append(tb.columns, col)
	tb.schemaHashValid = false
	return col, nil
}

// commitRow finalizes the current in-progress row. Any column that
// was not set for this row is gap-filled with a null sentinel.
func (tb *qwpTableBuffer) commitRow() {
	for _, col := range tb.columns {
		if col.rowCount <= tb.rowCount {
			col.addNull()
		}
	}
	tb.rowCount++
	tb.committedColumnCount = len(tb.columns)
}

// cancelRow discards the current in-progress row, rolling back any
// column values that were set since the last commitRow(). Columns
// that were created during this row are removed entirely.
func (tb *qwpTableBuffer) cancelRow() {
	// Remove columns created during this row.
	if len(tb.columns) > tb.committedColumnCount {
		for i := tb.committedColumnCount; i < len(tb.columns); i++ {
			delete(tb.columnIndex, tb.columns[i].name)
		}
		tb.columns = tb.columns[:tb.committedColumnCount]
		tb.schemaHashValid = false
	}

	// Truncate any columns that were set during this row.
	for _, col := range tb.columns {
		if col.rowCount > tb.rowCount {
			col.truncateTo(tb.rowCount)
		}
	}
}

// reset clears all row data and columns, retaining the table name.
func (tb *qwpTableBuffer) reset() {
	for _, col := range tb.columns {
		col.reset()
	}
	tb.rowCount = 0
	tb.committedColumnCount = 0
	tb.schemaHashValid = false
}

// getSchemaHash returns a hash over the column definitions (names
// and wire type codes). The hash is lazily computed and cached until
// the column set changes.
func (tb *qwpTableBuffer) getSchemaHash() int64 {
	if !tb.schemaHashValid {
		tb.schemaHash = qwpComputeSchemaHash(tb.columns)
		tb.schemaHashValid = true
	}
	return tb.schemaHash
}
