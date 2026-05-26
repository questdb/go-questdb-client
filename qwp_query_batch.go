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
	"math"
	"slices"
	"unsafe"
)

// qwpColumnSchemaInfo captures the per-column metadata carried in the
// schema section of a RESULT_BATCH frame. One instance per column;
// persisted in the decoder's connection-scoped schema registry so
// subsequent batches that reference a prior schema id can reuse them.
//
// Named with the "Schema" infix to avoid colliding with the
// `qwpColumnInfo` struct already defined in `qwp_integration_test.go`
// (which is the JSON shape returned by QuestDB's /exec endpoint).
type qwpColumnSchemaInfo struct {
	name     string
	wireType qwpTypeCode
}

// qwpSymbolEntry points to one entry in a connection-scoped symbol
// dictionary: (offset, length) into the heap, packed into two uint32s
// so the aggregate entries slice has predictable per-element size.
type qwpSymbolEntry struct {
	offset uint32
	length uint32
}

// qwpSymbolDictView is a snapshot view over the decoder's connection-
// scoped symbol dictionary. The underlying heap is append-only, so a
// snapshot taken at the time a column is decoded remains valid after
// subsequent batches extend the dict. The view is two slice headers
// whose lengths are frozen at snapshot time; len(entries) is the
// snapshot's dictionary size.
type qwpSymbolDictView struct {
	heap    []byte
	entries []qwpSymbolEntry
}

// qwpColumnLayout is the per-column parsed state for one RESULT_BATCH.
// All slice fields alias into the frame's payload (the WebSocket recv
// buffer) except `timestampBuf`, which the decoder owns because the
// Gorilla-decoded int64 values cannot be produced in-place.
//
// Lifetime: layouts live on the enclosing QwpColumnBatch and are
// grown in place by the decoder across decodes into the SAME batch.
// Two QwpBatchBuffers that the I/O goroutine alternates between
// therefore never share layout storage, so emitting batch N while
// decoding batch N+1 does not corrupt N's view. `clear` nil-s the
// slice headers but preserves backing arrays on the non-aliasing
// fields (`nonNullIdx`, `symbolRowIds`, `timestampBuf`,
// `arrayRowStart`, `arrayElems`), so subsequent decodes into the
// same batch with the same column width avoid reallocation.
type qwpColumnLayout struct {
	info *qwpColumnSchemaInfo

	// scale is the decimal scale for DECIMAL64/128/256 columns. Read
	// from the DATA section per batch; zero for non-decimal columns.
	// Stored per-layout (not on the shared qwpColumnSchemaInfo) so the
	// decoder's write is exclusive to the dispatcher's per-batch
	// storage — the consumer reads its own batch's layout, which
	// cannot be mutated concurrently.
	scale uint8

	// precisionBits is the precision in bits for GEOHASH columns. Read
	// from the DATA section per batch; zero for non-GEOHASH columns.
	// See `scale` for the per-layout placement rationale.
	precisionBits uint16

	// null bitmap (LSB-first; 1 = NULL). Nil when the column has no
	// nulls in this batch; the decoder skips allocating `nonNullIdx`
	// on this branch and typed accessors fall back to identity indexing.
	nullBitmap []byte

	// Count of non-null rows in this batch (== rowCount when nullBitmap
	// is nil, else rowCount - popcount(nullBitmap)).
	nonNullCount int

	// Dense index per row: nonNullIdx[row] is the position of row
	// within the non-null values region, or -1 if the row is NULL.
	// Nil when nullBitmap == nil (identity mapping row → row).
	nonNullIdx []int32

	// Dense values region. For fixed-width types this is nonNullCount *
	// sizeBytes bytes of packed values. For STRING/VARCHAR/BINARY this
	// is the (nonNullCount+1)*4-byte offsets array; the concatenated
	// bytes live in `stringBytes`. For SYMBOL/ARRAY this is left nil
	// and per-cell readers go through `symbolRowIds` / arrayRow*.
	// For Gorilla TIMESTAMP, this aliases `timestampBuf` reinterpreted
	// as bytes so the Int64 accessor path stays uniform.
	values []byte

	// Concatenated UTF-8 / opaque byte region for STRING/VARCHAR/BINARY.
	stringBytes []byte

	// Per-row symbol dictionary id (size rowCount; NULL rows hold
	// undefined values — callers must null-check first).
	symbolRowIds []int32

	// Connection-scoped dictionary snapshot for this column's SYMBOL
	// values. Zero value (nil heap) for non-SYMBOL columns.
	symbolDict qwpSymbolDictView

	// arrayRowStart is the byte offset within `values` where each
	// array row's nDims byte begins. Size rowCount; NULL rows hold 0.
	arrayRowStart []int32
	// arrayElems is the precomputed element count for each array row,
	// cached at decode time so per-cell accessors avoid re-walking the
	// shape header. Bounded by qwpMaxArrayElements (fits in int32).
	// Size rowCount; NULL rows hold 0.
	arrayElems []int32

	// Decoder-owned decode buffer for Gorilla-encoded TIMESTAMP columns.
	// Sized to nonNullCount; `values` aliases this as bytes.
	timestampBuf []int64
}

// clear resets the layout between reuses. Backing arrays on the
// non-aliasing fields are kept so the decoder amortises allocation
// across batches of the same column width.
func (l *qwpColumnLayout) clear() {
	l.info = nil
	l.scale = 0
	l.precisionBits = 0
	l.nullBitmap = nil
	l.nonNullCount = 0
	l.nonNullIdx = l.nonNullIdx[:0]
	l.values = nil
	l.stringBytes = nil
	l.symbolRowIds = l.symbolRowIds[:0]
	l.symbolDict = qwpSymbolDictView{}
	l.arrayRowStart = l.arrayRowStart[:0]
	l.arrayElems = l.arrayElems[:0]
	l.timestampBuf = l.timestampBuf[:0]
}

// denseIndex maps a logical row index to the dense-values index. The
// typed accessors call this after a null-check to find the byte offset
// of a non-null value in `values`.
func (l *qwpColumnLayout) denseIndex(row int) int {
	if l.nullBitmap == nil {
		return row
	}
	return int(l.nonNullIdx[row])
}

// isNull reports whether the cell at `row` is NULL in this batch.
func (l *qwpColumnLayout) isNull(row int) bool {
	if l.nullBitmap == nil {
		return false
	}
	b := l.nullBitmap[row>>3]
	return b&(1<<(row&7)) != 0
}

// QwpColumnBatch is a column-major view over one decoded RESULT_BATCH
// frame. The batch is valid only for the duration of the current
// iteration of a *QwpQuery's `Batches()` range — its accessors return
// slice views that alias the underlying WebSocket recv buffer. Do not
// retain any returned slice or string beyond the loop iteration; use
// `CopyAll` (once implemented in the I/O-goroutine slab) if you need
// persistent copies.
//
// All typed accessors are safe to call on NULL rows: they return the
// zero value of their return type (0, false, "", nil). Use `IsNull`
// first if you need to distinguish.
type QwpColumnBatch struct {
	payload     []byte
	requestId   int64
	batchSeq    int64
	rowCount    int
	columnCount int
	columns     []qwpColumnSchemaInfo // alias into the schema registry
	layouts     []qwpColumnLayout     // one per column; pool-owned

	// zstdScratch holds the decompressed body when the owning
	// RESULT_BATCH carried FLAG_ZSTD. The decoder grows it to match
	// the frame's content size and reuses the backing array across
	// decodes into the SAME batch. The layout byte-slices alias into
	// this buffer when the batch is compressed; `payload` is pointed
	// at it by the decoder so the existing aliasing invariants hold
	// without duplicating state.
	//
	// Lives on the batch (not on the decoder) for the same reason the
	// layout pool does: two qwpBatchBuffers that the I/O goroutine
	// alternates between must not share scratch storage, else
	// decoding batch N+1 would clobber batch N's view.
	zstdScratch []byte
}

// Payload returns the raw frame payload that backs this batch. Exposed
// for byte-counting / metrics — callers must not mutate or retain it.
func (b *QwpColumnBatch) Payload() []byte { return b.payload }

// RequestId returns the client-assigned 64-bit id from the originating
// QUERY_REQUEST. All frames for one query share the same id.
func (b *QwpColumnBatch) RequestId() int64 { return b.requestId }

// BatchSeq returns the monotonic per-request sequence number (starts at
// 0 for the first batch of a query, increments by 1 per RESULT_BATCH).
func (b *QwpColumnBatch) BatchSeq() int64 { return b.batchSeq }

// RowCount returns the number of rows in this batch.
func (b *QwpColumnBatch) RowCount() int { return b.rowCount }

// ColumnCount returns the number of columns.
func (b *QwpColumnBatch) ColumnCount() int { return b.columnCount }

// ColumnName returns the server-reported column name.
func (b *QwpColumnBatch) ColumnName(col int) string { return b.columns[col].name }

// ColumnType returns the wire-type byte for the column (one of the
// `qwpType*` constants, e.g. 0x04 for INT). Callers dispatch on this
// to pick the right typed accessor.
func (b *QwpColumnBatch) ColumnType(col int) byte { return byte(b.columns[col].wireType) }

// DecimalScale returns the decimal scale for DECIMAL64/128/256 columns.
// Not meaningful for other types; returns 0.
func (b *QwpColumnBatch) DecimalScale(col int) int { return int(b.layouts[col].scale) }

// GeohashPrecisionBits returns the precision in bits for a GEOHASH
// column. Not meaningful for other types; returns 0.
func (b *QwpColumnBatch) GeohashPrecisionBits(col int) int {
	return int(b.layouts[col].precisionBits)
}

// IsNull reports whether the cell at (col, row) is NULL in this batch.
//
// Note: QuestDB uses sentinel values for several primitive types (e.g.
// Long.MinValue for LONG, NaN for FLOAT/DOUBLE, -1 for GEOHASH). Those
// rows also return true from IsNull — the server encodes them via the
// null bitmap, so "real NaN" and "explicit NULL" are indistinguishable
// over the wire.
func (b *QwpColumnBatch) IsNull(col, row int) bool {
	return b.layouts[col].isNull(row)
}

// NonNullCount returns the count of non-null rows in a column —
// i.e. the size of the dense values region before row-level dispatch.
func (b *QwpColumnBatch) NonNullCount(col int) int {
	return b.layouts[col].nonNullCount
}

// --- Fixed-width typed accessors ---
//
// Each accessor assumes the caller knows the column's wire type. Call
// ColumnType(col) for generic dispatch; in a schema-aware query runner
// the caller already knows. NULL rows return the zero value of the
// accessor's return type.
//
// The QwpColumn handle (`Column(col)`) duplicates each accessor body.
// Routing the batch surface through `b.Column(col).X(row)` would halve
// the maintenance surface but ~doubles per-cell latency on Go 1.26 —
// the inliner does not flatten the by-value receiver chain, so the
// QwpColumn struct construction stays on the hot path. When adding a
// new wire type, mirror it on both surfaces.

// Bool returns the BOOLEAN value at (col, row). BOOLEAN is bit-packed
// on the wire: 8 non-null values per byte, LSB-first.
func (b *QwpColumnBatch) Bool(col, row int) bool {
	l := &b.layouts[col]
	if l.isNull(row) {
		return false
	}
	idx := l.denseIndex(row)
	return l.values[idx>>3]&(1<<(idx&7)) != 0
}

// Int8 returns the BYTE value at (col, row).
func (b *QwpColumnBatch) Int8(col, row int) int8 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	return int8(l.values[l.denseIndex(row)])
}

// Int16 returns the SHORT value at (col, row).
func (b *QwpColumnBatch) Int16(col, row int) int16 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 2
	return int16(binary.LittleEndian.Uint16(l.values[i : i+2]))
}

// Char returns the CHAR value at (col, row) as a rune. The wire format
// stores CHAR as a 2-byte UTF-16 code unit — code points outside the
// BMP are not representable and the encoder refuses to emit them, so
// the returned value always fits in a uint16.
func (b *QwpColumnBatch) Char(col, row int) rune {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 2
	return rune(binary.LittleEndian.Uint16(l.values[i : i+2]))
}

// Int32 returns the INT or IPv4 value at (col, row). Both share the
// 4-byte LE wire layout; IPv4's four octets are the four bytes of the
// int32 in network-independent little-endian order.
func (b *QwpColumnBatch) Int32(col, row int) int32 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 4
	return int32(binary.LittleEndian.Uint32(l.values[i : i+4]))
}

// Int64 returns an 8-byte column value at (col, row). Applicable to
// LONG, DATE, TIMESTAMP, TIMESTAMP_NANOS, and DECIMAL64 columns —
// they all share the int64 LE wire format.
func (b *QwpColumnBatch) Int64(col, row int) int64 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 8
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// Float32 returns the FLOAT value at (col, row). NULL rows return 0,
// NOT NaN — callers who want to distinguish explicit NaN from NULL
// must check IsNull first (see the note on IsNull about QuestDB's
// sentinel-based null encoding).
func (b *QwpColumnBatch) Float32(col, row int) float32 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 4
	return math.Float32frombits(binary.LittleEndian.Uint32(l.values[i : i+4]))
}

// Float64 returns the DOUBLE value at (col, row).
func (b *QwpColumnBatch) Float64(col, row int) float64 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 8
	return math.Float64frombits(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// --- Wide fixed-width: UUID, LONG256, DECIMAL128, DECIMAL256 ---

// UuidLo returns the low 64 bits of a UUID (byte offset 0 within the
// 16-byte cell).
func (b *QwpColumnBatch) UuidLo(col, row int) int64 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 16
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// UuidHi returns the high 64 bits of a UUID (byte offset 8).
func (b *QwpColumnBatch) UuidHi(col, row int) int64 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row)*16 + 8
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// Decimal128Lo returns the low 64 bits of a DECIMAL128 unscaled value.
// Pair with `DecimalScale(col)` to reconstruct the full decimal.
func (b *QwpColumnBatch) Decimal128Lo(col, row int) int64 {
	return b.UuidLo(col, row) // same wire layout: 16 LE bytes
}

// Decimal128Hi returns the high 64 bits of a DECIMAL128 unscaled value.
func (b *QwpColumnBatch) Decimal128Hi(col, row int) int64 {
	return b.UuidHi(col, row)
}

// Long256Word returns word `word` of a LONG256 or DECIMAL256 value at
// (col, row). word=0 is the least-significant 64 bits, word=3 the most.
// Panics on word out of [0,3] regardless of whether the row is NULL —
// that is always programmer error and should not be masked by a NULL.
func (b *QwpColumnBatch) Long256Word(col, row, word int) int64 {
	if word < 0 || word > 3 {
		panic(fmt.Sprintf("QwpColumnBatch.Long256Word: word %d out of [0,3]", word))
	}
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row)*32 + word*8
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// --- Strings, varchars, binary ---
//
// Each zero-copy accessor returns a []byte sub-slice of the frame
// payload. The slice is valid only for the current iteration of
// `*QwpQuery.Batches()`; the next iteration reuses the same underlying
// recv buffer and the bytes are no longer stable. Call `bytes.Clone`
// (or the materializing helper) if you need to retain a value.
//
// The Java client carries two parallel "A/B" view objects per string
// column because each accessor re-points one mutable DirectUtf8String
// and the user needs two slots to hold two views at once. Go slices
// are independent value-copies of a {ptr, len, cap} triple, so every
// call produces an independent view — no A/B distinction needed.

// Str returns the UTF-8 bytes of a STRING, VARCHAR, SYMBOL, or BINARY
// cell. Returns nil for NULL rows and for any column whose wire type
// is not one of those four — there is no way to distinguish "the row
// is NULL" from "this column is not a string" through the return value
// alone, so callers that care must know the column type up front (e.g.
// from ColumnType). The returned slice aliases the payload; do not
// retain it past the current batch iteration.
func (b *QwpColumnBatch) Str(col, row int) []byte {
	l := &b.layouts[col]
	if l.isNull(row) {
		return nil
	}
	wt := l.info.wireType
	if wt == qwpTypeSymbol {
		rowIdx := l.symbolRowIds[row]
		if int(rowIdx) >= len(l.symbolDict.entries) {
			return nil
		}
		e := l.symbolDict.entries[rowIdx]
		return l.symbolDict.heap[e.offset : e.offset+e.length]
	}
	if wt == qwpTypeVarchar || wt == qwpTypeBinary {
		// Treat BINARY under Str as an opaque byte-bag view for
		// callers that want the bytes without explicit BINARY
		// typing. The dedicated Binary accessor is the idiomatic
		// entry point for BINARY columns.
		return qwpStringSlice(l, row)
	}
	return nil
}

// String returns the cell at (col, row) as a newly-allocated string.
// Applicable to STRING, VARCHAR, and SYMBOL columns. Returns "" for
// NULL rows.
func (b *QwpColumnBatch) String(col, row int) string {
	s := b.Str(col, row)
	if s == nil {
		return ""
	}
	return string(s)
}

// Binary returns the opaque bytes of a BINARY cell. Returns nil for
// NULL rows. The returned slice aliases the payload.
func (b *QwpColumnBatch) Binary(col, row int) []byte {
	l := &b.layouts[col]
	if l.isNull(row) {
		return nil
	}
	if l.info.wireType != qwpTypeBinary {
		return nil
	}
	return qwpStringSlice(l, row)
}

// qwpStringSlice implements the shared offset-decode for STRING /
// VARCHAR / BINARY. The `values` region holds a (nonNullCount+1) *
// 4-byte array of uint32 offsets into `stringBytes`; row i covers
// bytes [off[dense], off[dense+1]).
func qwpStringSlice(l *qwpColumnLayout, row int) []byte {
	dense := l.denseIndex(row)
	start := binary.LittleEndian.Uint32(l.values[dense*4:])
	end := binary.LittleEndian.Uint32(l.values[dense*4+4:])
	return l.stringBytes[start:end]
}

// --- Arrays ---

// ArrayNDims returns the dimensionality of the array value at (col, row),
// or 0 for NULL rows.
func (b *QwpColumnBatch) ArrayNDims(col, row int) int {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	start := l.arrayRowStart[row]
	return int(l.values[start])
}

// ArrayDim returns the extent of dimension `dim` of the array at
// (col, row). `dim` must be in [0, ArrayNDims(col, row)).
func (b *QwpColumnBatch) ArrayDim(col, row, dim int) int {
	l := &b.layouts[col]
	if l.isNull(row) {
		return 0
	}
	start := int(l.arrayRowStart[row])
	nDims := int(l.values[start])
	if dim < 0 || dim >= nDims {
		panic(fmt.Sprintf("QwpColumnBatch.ArrayDim: dim %d out of [0, %d)", dim, nDims))
	}
	off := start + 1 + dim*4
	return int(int32(binary.LittleEndian.Uint32(l.values[off : off+4])))
}

// arrayElementCount returns the cached element count for the array at
// row `row` in layout `l`, plus the byte offset within `l.values`
// where the flattened data region begins (one byte past the shape
// header). The decoder precomputes the element count into l.arrayElems
// at parse time so per-cell accessors do not re-walk the shape header
// on every call. The decoder also bounds-checks the per-dimension
// extents against qwpMaxArrayElements, so callers that reach this
// helper know the row is non-null and the product fits in int.
func arrayElementCount(l *qwpColumnLayout, row int) (elems, dataBase int) {
	start := int(l.arrayRowStart[row])
	nDims := int(l.values[start])
	elems = int(l.arrayElems[row])
	dataBase = start + 1 + nDims*4
	return elems, dataBase
}

// Float64Array returns the flattened (row-major) elements of a
// DOUBLE_ARRAY cell. Returns nil for NULL rows. The returned slice is a
// fresh []float64 owned by the caller; the payload is memmove'd from
// the wire bytes via an unsafe reinterpretation. Use `ArrayDim` to
// reshape.
//
// Safety: float64 is 8 bytes on every supported architecture and Go
// stores them little-endian on all targets questdb-client supports, so
// the wire layout matches the in-memory layout. The reinterpreted
// source slice is only ever read by `copy`, which lowers to memmove —
// no 8-byte-aligned load is issued against the unaligned payload.
func (b *QwpColumnBatch) Float64Array(col, row int) []float64 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return nil
	}
	elems, base := arrayElementCount(l, row)
	out := make([]float64, elems)
	if elems > 0 {
		src := unsafe.Slice((*float64)(unsafe.Pointer(&l.values[base])), elems)
		copy(out, src)
	}
	return out
}

// Int64Array returns the flattened (row-major) elements of a LONG_ARRAY
// cell. Returns nil for NULL rows. See `Float64Array` for the memmove /
// endianness contract.
func (b *QwpColumnBatch) Int64Array(col, row int) []int64 {
	l := &b.layouts[col]
	if l.isNull(row) {
		return nil
	}
	elems, base := arrayElementCount(l, row)
	out := make([]int64, elems)
	if elems > 0 {
		src := unsafe.Slice((*int64)(unsafe.Pointer(&l.values[base])), elems)
		copy(out, src)
	}
	return out
}

// QwpColumn is a cached view over a single column of a QwpColumnBatch.
// It captures the column's layout pointer once so per-row accessors
// avoid the per-cell bounds-checked indexing into the batch's layout
// slice. Use this when iterating many rows of one column — the common
// shape for row-major consumers.
//
// Lifetime matches the parent QwpColumnBatch: valid only inside the
// current iteration of *QwpQuery.Batches(). Do not retain past the
// iteration. Returned by value (a layout pointer plus the row count) so
// storing the handle is allocation-free.
type QwpColumn struct {
	layout   *qwpColumnLayout
	rowCount int
}

// Column returns a cached handle over column `col`. Prefer the handle's
// typed accessors (`Int64(row)`, `Str(row)`, …) when iterating many
// rows of the same column; it eliminates the per-cell `&b.layouts[col]`
// bounds check and slice re-derivation the batch-level accessors pay.
func (b *QwpColumnBatch) Column(col int) QwpColumn {
	return QwpColumn{layout: &b.layouts[col], rowCount: b.rowCount}
}

// Name returns the server-reported column name.
func (c QwpColumn) Name() string { return c.layout.info.name }

// Type returns the wire-type byte for this column (one of the
// `qwpType*` constants).
func (c QwpColumn) Type() byte { return byte(c.layout.info.wireType) }

// RowCount returns the row count of the owning batch.
func (c QwpColumn) RowCount() int { return c.rowCount }

// NonNullCount returns the count of non-null rows in this column.
func (c QwpColumn) NonNullCount() int { return c.layout.nonNullCount }

// DecimalScale returns the scale for DECIMAL64/128/256 columns; 0 otherwise.
func (c QwpColumn) DecimalScale() int { return int(c.layout.scale) }

// GeohashPrecisionBits returns the precision in bits for GEOHASH columns.
func (c QwpColumn) GeohashPrecisionBits() int { return int(c.layout.precisionBits) }

// HasNulls reports whether this column carries a null bitmap in the
// current batch. When false, every per-cell null check resolves to
// false in one branch and Range accessors take the bulk-memmove path.
func (c QwpColumn) HasNulls() bool { return c.layout.nullBitmap != nil }

// IsNull reports whether the cell at row is NULL.
func (c QwpColumn) IsNull(row int) bool { return c.layout.isNull(row) }

// Bool returns the BOOLEAN value at row.
func (c QwpColumn) Bool(row int) bool {
	l := c.layout
	if l.isNull(row) {
		return false
	}
	idx := l.denseIndex(row)
	return l.values[idx>>3]&(1<<(idx&7)) != 0
}

// Int8 returns the BYTE value at row.
func (c QwpColumn) Int8(row int) int8 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	return int8(l.values[l.denseIndex(row)])
}

// Int16 returns the SHORT value at row.
func (c QwpColumn) Int16(row int) int16 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 2
	return int16(binary.LittleEndian.Uint16(l.values[i : i+2]))
}

// Char returns the CHAR value at row as a rune (2-byte UTF-16 code unit).
func (c QwpColumn) Char(row int) rune {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 2
	return rune(binary.LittleEndian.Uint16(l.values[i : i+2]))
}

// Int32 returns the INT or IPv4 value at row.
func (c QwpColumn) Int32(row int) int32 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 4
	return int32(binary.LittleEndian.Uint32(l.values[i : i+4]))
}

// Int64 returns an 8-byte column value at row (LONG, DATE, TIMESTAMP,
// TIMESTAMP_NANOS, DECIMAL64).
func (c QwpColumn) Int64(row int) int64 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 8
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// Float32 returns the FLOAT value at row.
func (c QwpColumn) Float32(row int) float32 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 4
	return math.Float32frombits(binary.LittleEndian.Uint32(l.values[i : i+4]))
}

// Float64 returns the DOUBLE value at row.
func (c QwpColumn) Float64(row int) float64 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 8
	return math.Float64frombits(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// UuidLo returns the low 64 bits of a UUID at row.
func (c QwpColumn) UuidLo(row int) int64 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row) * 16
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// UuidHi returns the high 64 bits of a UUID at row.
func (c QwpColumn) UuidHi(row int) int64 {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row)*16 + 8
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// Decimal128Lo returns the low 64 bits of a DECIMAL128 unscaled value.
func (c QwpColumn) Decimal128Lo(row int) int64 { return c.UuidLo(row) }

// Decimal128Hi returns the high 64 bits of a DECIMAL128 unscaled value.
func (c QwpColumn) Decimal128Hi(row int) int64 { return c.UuidHi(row) }

// Long256Word returns word `word` of a LONG256 or DECIMAL256 value at row.
// Panics on word out of [0,3] regardless of whether the row is NULL —
// that is always programmer error and should not be masked by a NULL.
func (c QwpColumn) Long256Word(row, word int) int64 {
	if word < 0 || word > 3 {
		panic(fmt.Sprintf("QwpColumn.Long256Word: word %d out of [0,3]", word))
	}
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	i := l.denseIndex(row)*32 + word*8
	return int64(binary.LittleEndian.Uint64(l.values[i : i+8]))
}

// Str returns the UTF-8 bytes of a STRING, VARCHAR, SYMBOL, or BINARY
// cell. Returns nil for NULL rows and for any column whose wire type
// is not one of those four — there is no way to distinguish "the row
// is NULL" from "this column is not a string" through the return value
// alone, so callers that care must know the column type up front (e.g.
// from QwpColumn.Type). The returned slice aliases the payload; do not
// retain past the batch iteration.
func (c QwpColumn) Str(row int) []byte {
	l := c.layout
	if l.isNull(row) {
		return nil
	}
	wt := l.info.wireType
	if wt == qwpTypeSymbol {
		rowIdx := l.symbolRowIds[row]
		if int(rowIdx) >= len(l.symbolDict.entries) {
			return nil
		}
		e := l.symbolDict.entries[rowIdx]
		return l.symbolDict.heap[e.offset : e.offset+e.length]
	}
	if wt == qwpTypeVarchar || wt == qwpTypeBinary {
		return qwpStringSlice(l, row)
	}
	return nil
}

// String returns the cell at row as a newly-allocated string.
func (c QwpColumn) String(row int) string {
	s := c.Str(row)
	if s == nil {
		return ""
	}
	return string(s)
}

// Binary returns the opaque bytes of a BINARY cell. Returns nil for
// NULL rows. The returned slice aliases the payload.
func (c QwpColumn) Binary(row int) []byte {
	l := c.layout
	if l.isNull(row) {
		return nil
	}
	if l.info.wireType != qwpTypeBinary {
		return nil
	}
	return qwpStringSlice(l, row)
}

// ArrayNDims returns the dimensionality of the array at row, or 0 for NULL.
func (c QwpColumn) ArrayNDims(row int) int {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	start := l.arrayRowStart[row]
	return int(l.values[start])
}

// ArrayDim returns the extent of dimension `dim` of the array at row.
func (c QwpColumn) ArrayDim(row, dim int) int {
	l := c.layout
	if l.isNull(row) {
		return 0
	}
	start := int(l.arrayRowStart[row])
	nDims := int(l.values[start])
	if dim < 0 || dim >= nDims {
		panic(fmt.Sprintf("QwpColumn.ArrayDim: dim %d out of [0, %d)", dim, nDims))
	}
	off := start + 1 + dim*4
	return int(int32(binary.LittleEndian.Uint32(l.values[off : off+4])))
}

// Float64Array returns the flattened (row-major) elements of a
// DOUBLE_ARRAY cell. Returns nil for NULL rows.
func (c QwpColumn) Float64Array(row int) []float64 {
	l := c.layout
	if l.isNull(row) {
		return nil
	}
	elems, base := arrayElementCount(l, row)
	out := make([]float64, elems)
	if elems > 0 {
		src := unsafe.Slice((*float64)(unsafe.Pointer(&l.values[base])), elems)
		copy(out, src)
	}
	return out
}

// Int64Array returns the flattened (row-major) elements of a LONG_ARRAY
// cell. Returns nil for NULL rows.
func (c QwpColumn) Int64Array(row int) []int64 {
	l := c.layout
	if l.isNull(row) {
		return nil
	}
	elems, base := arrayElementCount(l, row)
	out := make([]int64, elems)
	if elems > 0 {
		src := unsafe.Slice((*int64)(unsafe.Pointer(&l.values[base])), elems)
		copy(out, src)
	}
	return out
}

// Float64ArrayInto appends the flattened (row-major) elements of a
// DOUBLE_ARRAY cell at row to dst and returns the extended slice. NULL
// rows contribute nothing — dst is returned unchanged. Use this in hot
// loops where the per-cell allocation of Float64Array would dominate;
// reuse dst across rows by truncating with `dst = dst[:0]` between
// calls.
func (c QwpColumn) Float64ArrayInto(row int, dst []float64) []float64 {
	l := c.layout
	if l.isNull(row) {
		return dst
	}
	elems, base := arrayElementCount(l, row)
	if elems == 0 {
		return dst
	}
	dstBase := len(dst)
	dst = slices.Grow(dst, elems)[:dstBase+elems]
	src := unsafe.Slice((*float64)(unsafe.Pointer(&l.values[base])), elems)
	copy(dst[dstBase:], src)
	return dst
}

// Int64ArrayInto appends the flattened (row-major) elements of a
// LONG_ARRAY cell at row to dst and returns the extended slice. See
// Float64ArrayInto for the contract — NULL rows contribute nothing.
func (c QwpColumn) Int64ArrayInto(row int, dst []int64) []int64 {
	l := c.layout
	if l.isNull(row) {
		return dst
	}
	elems, base := arrayElementCount(l, row)
	if elems == 0 {
		return dst
	}
	dstBase := len(dst)
	dst = slices.Grow(dst, elems)[:dstBase+elems]
	src := unsafe.Slice((*int64)(unsafe.Pointer(&l.values[base])), elems)
	copy(dst[dstBase:], src)
	return dst
}

// --- Bulk row-range accessors ---
//
// Each *Range method appends values for rows [fromRow, toRow) onto dst
// and returns the extended slice (the append pattern). NULL rows become
// the zero value of the element type. When the column has no nulls the
// dense region is bulk-copied via a single memmove (identity indexing);
// otherwise the accessor walks the null bitmap once, writing a zero for
// NULL rows and a decoded value for non-NULL rows.
//
// Preallocate dst (e.g. `dst := make([]int64, 0, batch.RowCount())`) to
// keep the common row-sweep path allocation-free. When dst's remaining
// capacity is short, slices.Grow performs one resize.
//
// The caller is responsible for matching the method to the column's
// wire type. Mis-typed calls (e.g. Int64Range on a DOUBLE column) will
// produce numeric noise, not a type error — follow the same discipline
// as the per-row typed accessors.

// Int64Range appends int64 values for rows [fromRow, toRow).
func (c QwpColumn) Int64Range(fromRow, toRow int, dst []int64) []int64 {
	n := toRow - fromRow
	if n <= 0 {
		return dst
	}
	l := c.layout
	base := len(dst)
	dst = slices.Grow(dst, n)[:base+n]
	if l.nullBitmap == nil {
		// Bounds-checked sub-slice first so caller misuse panics the
		// same way as the per-cell accessor (l.values[i:i+8]); only
		// then reinterpret as []int64.
		chunk := l.values[fromRow*8 : toRow*8]
		src := unsafe.Slice((*int64)(unsafe.Pointer(&chunk[0])), n)
		copy(dst[base:], src)
		return dst
	}
	for i := 0; i < n; i++ {
		row := fromRow + i
		if l.nullBitmap[row>>3]&(1<<(row&7)) != 0 {
			dst[base+i] = 0
			continue
		}
		idx := int(l.nonNullIdx[row]) * 8
		dst[base+i] = int64(binary.LittleEndian.Uint64(l.values[idx : idx+8]))
	}
	return dst
}

// Float64Range appends float64 values for rows [fromRow, toRow).
func (c QwpColumn) Float64Range(fromRow, toRow int, dst []float64) []float64 {
	n := toRow - fromRow
	if n <= 0 {
		return dst
	}
	l := c.layout
	base := len(dst)
	dst = slices.Grow(dst, n)[:base+n]
	if l.nullBitmap == nil {
		chunk := l.values[fromRow*8 : toRow*8]
		src := unsafe.Slice((*float64)(unsafe.Pointer(&chunk[0])), n)
		copy(dst[base:], src)
		return dst
	}
	for i := 0; i < n; i++ {
		row := fromRow + i
		if l.nullBitmap[row>>3]&(1<<(row&7)) != 0 {
			dst[base+i] = 0
			continue
		}
		idx := int(l.nonNullIdx[row]) * 8
		dst[base+i] = math.Float64frombits(binary.LittleEndian.Uint64(l.values[idx : idx+8]))
	}
	return dst
}

// Int32Range appends int32 values for rows [fromRow, toRow).
func (c QwpColumn) Int32Range(fromRow, toRow int, dst []int32) []int32 {
	n := toRow - fromRow
	if n <= 0 {
		return dst
	}
	l := c.layout
	base := len(dst)
	dst = slices.Grow(dst, n)[:base+n]
	if l.nullBitmap == nil {
		chunk := l.values[fromRow*4 : toRow*4]
		src := unsafe.Slice((*int32)(unsafe.Pointer(&chunk[0])), n)
		copy(dst[base:], src)
		return dst
	}
	for i := 0; i < n; i++ {
		row := fromRow + i
		if l.nullBitmap[row>>3]&(1<<(row&7)) != 0 {
			dst[base+i] = 0
			continue
		}
		idx := int(l.nonNullIdx[row]) * 4
		dst[base+i] = int32(binary.LittleEndian.Uint32(l.values[idx : idx+4]))
	}
	return dst
}

// Float32Range appends float32 values for rows [fromRow, toRow).
func (c QwpColumn) Float32Range(fromRow, toRow int, dst []float32) []float32 {
	n := toRow - fromRow
	if n <= 0 {
		return dst
	}
	l := c.layout
	base := len(dst)
	dst = slices.Grow(dst, n)[:base+n]
	if l.nullBitmap == nil {
		chunk := l.values[fromRow*4 : toRow*4]
		src := unsafe.Slice((*float32)(unsafe.Pointer(&chunk[0])), n)
		copy(dst[base:], src)
		return dst
	}
	for i := 0; i < n; i++ {
		row := fromRow + i
		if l.nullBitmap[row>>3]&(1<<(row&7)) != 0 {
			dst[base+i] = 0
			continue
		}
		idx := int(l.nonNullIdx[row]) * 4
		dst[base+i] = math.Float32frombits(binary.LittleEndian.Uint32(l.values[idx : idx+4]))
	}
	return dst
}

// --- Materializing escape hatch ---

// SerializedBatch is a heap-owned copy of a QwpColumnBatch, safe to
// retain past the iteration that produced it. It is a type alias for
// QwpColumnBatch so every typed accessor (Int64, Str, Float64Array, …)
// works identically on the serialized copy.
//
// The shape of a SerializedBatch differs from a live batch in two ways,
// both of which are invisible to callers:
//
//  1. The pool-owned layout arrays (nonNullIdx, symbolRowIds,
//     arrayRowStart, arrayElems, timestampBuf) are freshly-allocated
//     heap slices, not aliases into the decoder's reused pool.
//  2. The payload bytes are deep-cloned, and every layout slice that
//     aliased the source payload (values, stringBytes, nullBitmap) is
//     re-pointed at the clone via offset translation, so the snapshot
//     is independent of the source's backing buffer.
//
// Both transport paths produce snapshots that survive reuse: the zstd
// path's `payload` aliased the per-batch decompression scratch the
// decoder reuses across decodes into the same QwpColumnBatch, and the
// raw path's `payload` aliased the recycled WS read buffer the egress
// I/O loop returns to qwpEgressIO.readBufPool on releaseBuffer (see
// qwp_query_io.go). Cloning covers both.
type SerializedBatch = QwpColumnBatch

// CopyAll materialises the batch into a heap-owned *SerializedBatch
// that the caller may retain past the current iteration of
// *QwpQuery.Batches(). The I/O goroutine's decoder reuses its per-column
// layout pool on the next frame, so a raw *QwpColumnBatch is only valid
// for the current iteration; CopyAll is the escape hatch.
//
// Cost: one []qwpColumnLayout slice + one fresh backing slice per
// pool-owned layout field, plus a one-shot deep clone of the payload
// bytes so the aliasing layout slices (values, stringBytes,
// nullBitmap) are translated onto storage the source's
// buffer-recycling cannot reach.
func (b *QwpColumnBatch) CopyAll() *SerializedBatch {
	sb := &SerializedBatch{
		requestId:   b.requestId,
		batchSeq:    b.batchSeq,
		rowCount:    b.rowCount,
		columnCount: b.columnCount,
		columns:     b.columns,
		layouts:     make([]qwpColumnLayout, b.columnCount),
	}
	// Both transport paths recycle the buffer payload aliases — the
	// per-batch zstdScratch on the compressed path, the readBufPool WS
	// read buffer on the raw path. Clone the whole payload once and
	// translate every aliasing layout slice onto the clone, so the
	// snapshot is independent of later decodes / pool reuse.
	srcPayload := b.payload
	clonedPayload := slices.Clone(srcPayload)
	sb.payload = clonedPayload
	if len(b.zstdScratch) > 0 {
		// Mirror the source's shape: a snapshot built from a compressed
		// batch keeps its payload addressable as zstdScratch too, since
		// on the source the two slice headers pointed at the same bytes.
		sb.zstdScratch = clonedPayload
	}
	for i := 0; i < b.columnCount; i++ {
		src := &b.layouts[i]
		dst := &sb.layouts[i]
		dst.info = src.info
		dst.scale = src.scale
		dst.precisionBits = src.precisionBits
		// nullBitmap: aliases payload for server-sent bitmaps; owned heap
		// buffer after array nDims=0 NULL promotion. Either way, retaining
		// the slice header keeps the backing array reachable for the life
		// of the SerializedBatch.
		dst.nullBitmap = rebindIfAliased(src.nullBitmap, srcPayload, clonedPayload)
		dst.nonNullCount = src.nonNullCount
		dst.nonNullIdx = slices.Clone(src.nonNullIdx)
		dst.values = rebindIfAliased(src.values, srcPayload, clonedPayload)
		dst.stringBytes = rebindIfAliased(src.stringBytes, srcPayload, clonedPayload)
		dst.symbolRowIds = slices.Clone(src.symbolRowIds)
		// symbolDict snapshot: heap + entries lengths are frozen at
		// snapshot time and the decoder only ever append-extends them,
		// so the view stays valid without copying.
		dst.symbolDict = src.symbolDict
		dst.arrayRowStart = slices.Clone(src.arrayRowStart)
		dst.arrayElems = slices.Clone(src.arrayElems)
		dst.timestampBuf = slices.Clone(src.timestampBuf)
		// Gorilla TIMESTAMP: values aliases timestampBuf (not payload).
		// Re-point at the cloned buffer so the snapshot survives the
		// decoder reusing the source's timestampBuf on a later decode.
		// Detected by timestampBuf being non-empty — parseTimestamp's
		// non-Gorilla branches leave it cleared to :0.
		if len(src.timestampBuf) > 0 {
			dst.values = int64sAsBytes(dst.timestampBuf)
		}
	}
	return sb
}

// rebindIfAliased returns src unchanged when it doesn't alias
// srcPayload — heap-owned slices (`int64sAsBytes(timestampBuf)`,
// promoted array null bitmaps) fall through as-is so the caller's
// follow-up branches can re-point them explicitly. When src does
// alias, the function translates its offset+length onto clonedPayload
// so the snapshot references the clone rather than the source's
// reusable buffer. The empty-src early return guards the &src[0]
// address read below.
func rebindIfAliased(src, srcPayload, clonedPayload []byte) []byte {
	if len(src) == 0 {
		return src
	}
	if !aliases(src, srcPayload) {
		return src
	}
	offset := int(uintptr(unsafe.Pointer(&src[0])) - uintptr(unsafe.Pointer(&srcPayload[0])))
	return clonedPayload[offset : offset+len(src)]
}

// aliases reports whether sub points into the backing array of parent.
// Compares addresses directly — the slice headers may have different
// lengths, so len-based checks are not sufficient.
func aliases(sub, parent []byte) bool {
	if len(sub) == 0 || len(parent) == 0 {
		return false
	}
	subAddr := uintptr(unsafe.Pointer(&sub[0]))
	parentAddr := uintptr(unsafe.Pointer(&parent[0]))
	parentEnd := parentAddr + uintptr(len(parent))
	return subAddr >= parentAddr && subAddr+uintptr(len(sub)) <= parentEnd
}
