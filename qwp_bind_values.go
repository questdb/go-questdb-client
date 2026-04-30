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
)

// QwpBinds is a typed bind-parameter sink for a single QWP egress query.
// Encodes the per-bind wire layout directly into a reusable scratch
// buffer:
//
//	type_code(1B) | null_flag(1B) | [bitmap(1B) if null_flag != 0] | [value bytes if !null]
//
// Non-null: type | 0x00 | value. Null: type | 0x01 | 0x01 (no value bytes).
//
// Indexes must be assigned in strictly ascending order starting at 0.
// The sink tracks the next expected index and latches an error on gaps
// or duplicates; the latched error is surfaced from Query / Exec instead
// of submitting the query.
//
// SQL parameter placeholders are 1-based ($1, $2, ...); indexes here
// are 0-based and map to $(index + 1).
//
// Not safe for concurrent use. One instance per QwpQueryClient is
// reused across calls; the client resets it before invoking the user-
// supplied bind function.
type QwpBinds struct {
	buf               []byte
	count             int
	expectedNextIndex int
	// err latches the first encoding failure; subsequent bind calls
	// become no-ops so the caller can write a straight-line setter
	// and surface the error from Query / Exec. Matches the ILP / QWP
	// sender lastErr pattern.
	err error
}

// Bind header bytes (matches Java QwpBindValues wire layout).
const (
	qwpBindNonNullFlag byte = 0x00
	qwpBindNullFlag    byte = 0x01
	qwpBindNullBitmap  byte = 0x01
	// qwpGeohashMinBits matches QuestDB's ColumnType.GEOLONG_MIN_BITS
	// check on the server (rejects precision 0).
	qwpGeohashMinBits = 1
	// qwpGeohashMaxBits matches ColumnType.GEOLONG_MAX_BITS.
	qwpGeohashMaxBits = 60
)

// Per-width DECIMAL scale caps. Mirrors Java QwpBindValues constants
// DECIMAL64_MAX_SCALE / DECIMAL128_MAX_SCALE / DECIMAL256_MAX_SCALE.
// The server only enforces scale <= maxDecimalScale (76) regardless of
// width; the per-width caps are a client-side preflight that surfaces
// "scale exceeds the type's representable digits" as a typed error
// before bytes leave the process.
const (
	qwpDecimal64MaxScale  = 18
	qwpDecimal128MaxScale = 38
	qwpDecimal256MaxScale = 76
)

// Err returns the first latched bind-encoding error, or nil. Exposed for
// tests; the client checks this directly before submitting.
func (b *QwpBinds) Err() error { return b.err }

// Count returns the number of binds encoded since the last reset.
func (b *QwpBinds) Count() int { return b.count }

// bufferBytes returns the encoded payload. Consumed by the client to
// copy the bytes into a per-request slice before handoff to the I/O
// goroutine; not part of the public API.
func (b *QwpBinds) bufferBytes() []byte { return b.buf }

// reset clears prior state so this instance can accumulate binds for
// a new query. Called by QwpQueryClient before every submit.
func (b *QwpBinds) reset() {
	b.buf = b.buf[:0]
	b.count = 0
	b.expectedNextIndex = 0
	b.err = nil
}

// advance validates the index and bumps the counters. Returns false
// (and latches the error) on out-of-order / duplicate index or on
// exceeding the max column count.
func (b *QwpBinds) advance(index int) bool {
	if b.err != nil {
		return false
	}
	if index != b.expectedNextIndex {
		b.err = fmt.Errorf(
			"qwp bind: index out of order: expected %d, got %d",
			b.expectedNextIndex, index)
		return false
	}
	if b.count >= qwpMaxColumnsPerTable {
		b.err = fmt.Errorf(
			"qwp bind: too many binds: exceeds %d", qwpMaxColumnsPerTable)
		return false
	}
	b.expectedNextIndex++
	b.count++
	return true
}

// writeHeader appends the type code, null flag, and (when isNull) the
// null bitmap byte.
func (b *QwpBinds) writeHeader(t qwpTypeCode, isNull bool) {
	b.buf = append(b.buf, byte(t))
	if isNull {
		b.buf = append(b.buf, qwpBindNullFlag, qwpBindNullBitmap)
	} else {
		b.buf = append(b.buf, qwpBindNonNullFlag)
	}
}

func (b *QwpBinds) appendUint16LE(v uint16) {
	b.buf = binary.LittleEndian.AppendUint16(b.buf, v)
}

func (b *QwpBinds) appendUint32LE(v uint32) {
	b.buf = binary.LittleEndian.AppendUint32(b.buf, v)
}

func (b *QwpBinds) appendUint64LE(v uint64) {
	b.buf = binary.LittleEndian.AppendUint64(b.buf, v)
}

func (b *QwpBinds) appendVarint(v uint64) {
	var tmp [qwpMaxVarintLen]byte
	n := qwpPutVarint(tmp[:], v)
	b.buf = append(b.buf, tmp[:n]...)
}

// BooleanBind binds a BOOLEAN ($(index+1)) parameter.
func (b *QwpBinds) BooleanBind(index int, value bool) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeBoolean, false)
	if value {
		b.buf = append(b.buf, 1)
	} else {
		b.buf = append(b.buf, 0)
	}
	return b
}

// NullBooleanBind binds a NULL BOOLEAN parameter.
func (b *QwpBinds) NullBooleanBind(index int) *QwpBinds { return b.setNull(index, qwpTypeBoolean) }

// ByteBind binds a BYTE (int8) parameter.
func (b *QwpBinds) ByteBind(index int, value int8) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeByte, false)
	b.buf = append(b.buf, byte(value))
	return b
}

// NullByteBind binds a NULL BYTE parameter.
func (b *QwpBinds) NullByteBind(index int) *QwpBinds { return b.setNull(index, qwpTypeByte) }

// ShortBind binds a SHORT (int16) parameter.
func (b *QwpBinds) ShortBind(index int, value int16) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeShort, false)
	b.appendUint16LE(uint16(value))
	return b
}

// NullShortBind binds a NULL SHORT parameter.
func (b *QwpBinds) NullShortBind(index int) *QwpBinds { return b.setNull(index, qwpTypeShort) }

// CharBind binds a CHAR parameter stored as a UTF-16 code unit.
// Runes outside the BMP (> U+FFFF) are rejected — QuestDB's CHAR is a
// single UTF-16 code unit, matching Java char semantics.
func (b *QwpBinds) CharBind(index int, value rune) *QwpBinds {
	if b.err != nil {
		return b
	}
	if value < 0 || value > 0xFFFF {
		b.err = fmt.Errorf("qwp bind: CHAR rune %U does not fit in a UTF-16 code unit", value)
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeChar, false)
	b.appendUint16LE(uint16(value))
	return b
}

// NullCharBind binds a NULL CHAR parameter.
func (b *QwpBinds) NullCharBind(index int) *QwpBinds { return b.setNull(index, qwpTypeChar) }

// IntBind binds an INT (int32) parameter.
func (b *QwpBinds) IntBind(index int, value int32) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeInt, false)
	b.appendUint32LE(uint32(value))
	return b
}

// NullIntBind binds a NULL INT parameter.
func (b *QwpBinds) NullIntBind(index int) *QwpBinds { return b.setNull(index, qwpTypeInt) }

// LongBind binds a LONG (int64) parameter.
func (b *QwpBinds) LongBind(index int, value int64) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeLong, false)
	b.appendUint64LE(uint64(value))
	return b
}

// NullLongBind binds a NULL LONG parameter.
func (b *QwpBinds) NullLongBind(index int) *QwpBinds { return b.setNull(index, qwpTypeLong) }

// FloatBind binds a FLOAT (float32) parameter.
func (b *QwpBinds) FloatBind(index int, value float32) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeFloat, false)
	b.appendUint32LE(math.Float32bits(value))
	return b
}

// NullFloatBind binds a NULL FLOAT parameter.
func (b *QwpBinds) NullFloatBind(index int) *QwpBinds { return b.setNull(index, qwpTypeFloat) }

// DoubleBind binds a DOUBLE (float64) parameter.
func (b *QwpBinds) DoubleBind(index int, value float64) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDouble, false)
	b.appendUint64LE(math.Float64bits(value))
	return b
}

// NullDoubleBind binds a NULL DOUBLE parameter.
func (b *QwpBinds) NullDoubleBind(index int) *QwpBinds { return b.setNull(index, qwpTypeDouble) }

// DateBind binds a DATE parameter (milliseconds since epoch).
func (b *QwpBinds) DateBind(index int, millisSinceEpoch int64) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDate, false)
	b.appendUint64LE(uint64(millisSinceEpoch))
	return b
}

// NullDateBind binds a NULL DATE parameter.
func (b *QwpBinds) NullDateBind(index int) *QwpBinds { return b.setNull(index, qwpTypeDate) }

// TimestampMicrosBind binds a TIMESTAMP parameter (microseconds since epoch).
func (b *QwpBinds) TimestampMicrosBind(index int, microsSinceEpoch int64) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeTimestamp, false)
	b.appendUint64LE(uint64(microsSinceEpoch))
	return b
}

// NullTimestampMicrosBind binds a NULL TIMESTAMP parameter.
func (b *QwpBinds) NullTimestampMicrosBind(index int) *QwpBinds {
	return b.setNull(index, qwpTypeTimestamp)
}

// TimestampNanosBind binds a TIMESTAMP_NANOS parameter (nanoseconds since epoch).
func (b *QwpBinds) TimestampNanosBind(index int, nanosSinceEpoch int64) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeTimestampNano, false)
	b.appendUint64LE(uint64(nanosSinceEpoch))
	return b
}

// NullTimestampNanosBind binds a NULL TIMESTAMP_NANOS parameter.
func (b *QwpBinds) NullTimestampNanosBind(index int) *QwpBinds {
	return b.setNull(index, qwpTypeTimestampNano)
}

// UuidBind binds a UUID parameter from high and low 64-bit halves.
// Wire order matches QuestDB's UUID layout: lo first, then hi.
func (b *QwpBinds) UuidBind(index int, hi, lo uint64) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeUuid, false)
	b.appendUint64LE(lo)
	b.appendUint64LE(hi)
	return b
}

// NullUuidBind binds a NULL UUID parameter.
func (b *QwpBinds) NullUuidBind(index int) *QwpBinds { return b.setNull(index, qwpTypeUuid) }

// Long256Bind binds a LONG256 parameter from four 64-bit limbs in LE order.
func (b *QwpBinds) Long256Bind(index int, l0, l1, l2, l3 uint64) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeLong256, false)
	b.appendUint64LE(l0)
	b.appendUint64LE(l1)
	b.appendUint64LE(l2)
	b.appendUint64LE(l3)
	return b
}

// NullLong256Bind binds a NULL LONG256 parameter.
func (b *QwpBinds) NullLong256Bind(index int) *QwpBinds { return b.setNull(index, qwpTypeLong256) }

// GeohashBind binds a GEOHASH parameter with the given precision in
// bits (1..60) and packed value. The low ceil(precisionBits/8) bytes of
// value are written little-endian on the wire.
//
// value is masked to precisionBits before encoding, so bits above the
// declared precision cannot leak into the top wire byte (which would
// otherwise pass through when precisionBits is not a multiple of 8).
func (b *QwpBinds) GeohashBind(index int, value uint64, precisionBits int) *QwpBinds {
	if b.err != nil {
		return b
	}
	if precisionBits < qwpGeohashMinBits || precisionBits > qwpGeohashMaxBits {
		b.err = fmt.Errorf(
			"qwp bind: GEOHASH precision must be in [%d, %d], got %d",
			qwpGeohashMinBits, qwpGeohashMaxBits, precisionBits)
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeGeohash, false)
	b.appendVarint(uint64(precisionBits))
	if precisionBits < 64 {
		value &= (uint64(1) << precisionBits) - 1
	}
	byteCount := (precisionBits + 7) >> 3
	for i := 0; i < byteCount; i++ {
		b.buf = append(b.buf, byte(value>>(i*8)))
	}
	return b
}

// NullGeohashBind binds a NULL GEOHASH parameter with the minimum
// precision (1 bit). The server reads the precision_bits varint
// regardless of null, so a precision must be present on the wire even
// for null. Use NullGeohashBindWithPrecision for explicit control.
func (b *QwpBinds) NullGeohashBind(index int) *QwpBinds {
	return b.NullGeohashBindWithPrecision(index, qwpGeohashMinBits)
}

// NullGeohashBindWithPrecision binds a NULL GEOHASH parameter with the
// given precision. Mirrors Java's setNullGeohash.
func (b *QwpBinds) NullGeohashBindWithPrecision(index int, precisionBits int) *QwpBinds {
	if b.err != nil {
		return b
	}
	if precisionBits < qwpGeohashMinBits || precisionBits > qwpGeohashMaxBits {
		b.err = fmt.Errorf(
			"qwp bind: GEOHASH precision must be in [%d, %d], got %d",
			qwpGeohashMinBits, qwpGeohashMaxBits, precisionBits)
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeGeohash, true)
	b.appendVarint(uint64(precisionBits))
	return b
}

// VarcharBind binds a VARCHAR parameter. Wire encoding is:
// offset0(u32 LE = 0) | length_bytes(u32 LE) | UTF-8 bytes.
func (b *QwpBinds) VarcharBind(index int, value string) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeVarchar, false)
	b.appendUint32LE(0)
	b.appendUint32LE(uint32(len(value)))
	b.buf = append(b.buf, value...)
	return b
}

// NullVarcharBind binds a NULL VARCHAR parameter.
func (b *QwpBinds) NullVarcharBind(index int) *QwpBinds { return b.setNull(index, qwpTypeVarchar) }

// Decimal64Bind binds a DECIMAL64 parameter from an explicit scale and
// unscaled int64. Scale must be in [0, 18]; DECIMAL64 can only store 18
// digits of precision, so a higher scale is mathematically invalid.
func (b *QwpBinds) Decimal64Bind(index int, scale int, unscaled int64) *QwpBinds {
	if !b.checkScale64(scale) {
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDecimal64, false)
	b.buf = append(b.buf, byte(scale))
	b.appendUint64LE(uint64(unscaled))
	return b
}

// NullDecimal64Bind binds a NULL DECIMAL64 parameter with implicit
// scale 0. The server reads the scale byte regardless of null, so the
// scale must be present on the wire even for null. Use
// NullDecimal64BindWithScale to bind a NULL with a specific scale.
func (b *QwpBinds) NullDecimal64Bind(index int) *QwpBinds {
	return b.NullDecimal64BindWithScale(index, 0)
}

// NullDecimal64BindWithScale binds a NULL DECIMAL64 parameter with the
// given scale. The scale becomes part of the bound variable's type on
// the server, so it is required for NULL the same way as for non-null.
// Mirrors Java's setNullDecimal64.
func (b *QwpBinds) NullDecimal64BindWithScale(index int, scale int) *QwpBinds {
	if !b.checkScale64(scale) {
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDecimal64, true)
	b.buf = append(b.buf, byte(scale))
	return b
}

// Decimal128Bind binds a DECIMAL128 parameter from an explicit scale and
// 128-bit unscaled value split into lo / hi 64-bit halves (wire order:
// lo then hi, little-endian). Scale must be in [0, 38].
func (b *QwpBinds) Decimal128Bind(index int, scale int, lo, hi uint64) *QwpBinds {
	if !b.checkScale128(scale) {
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDecimal128, false)
	b.buf = append(b.buf, byte(scale))
	b.appendUint64LE(lo)
	b.appendUint64LE(hi)
	return b
}

// NullDecimal128Bind binds a NULL DECIMAL128 parameter with implicit
// scale 0. See NullDecimal64Bind for the rationale. Use
// NullDecimal128BindWithScale for an explicit scale.
func (b *QwpBinds) NullDecimal128Bind(index int) *QwpBinds {
	return b.NullDecimal128BindWithScale(index, 0)
}

// NullDecimal128BindWithScale binds a NULL DECIMAL128 parameter with
// the given scale. Mirrors Java's setNullDecimal128.
func (b *QwpBinds) NullDecimal128BindWithScale(index int, scale int) *QwpBinds {
	if !b.checkScale128(scale) {
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDecimal128, true)
	b.buf = append(b.buf, byte(scale))
	return b
}

// Decimal256Bind binds a DECIMAL256 parameter from an explicit scale and
// 256-bit unscaled value split into four 64-bit limbs (wire order:
// ll, lh, hl, hh, each little-endian). Scale must be in [0, 76].
func (b *QwpBinds) Decimal256Bind(index int, scale int, ll, lh, hl, hh uint64) *QwpBinds {
	if !b.checkScale256(scale) {
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDecimal256, false)
	b.buf = append(b.buf, byte(scale))
	b.appendUint64LE(ll)
	b.appendUint64LE(lh)
	b.appendUint64LE(hl)
	b.appendUint64LE(hh)
	return b
}

// NullDecimal256Bind binds a NULL DECIMAL256 parameter with implicit
// scale 0. See NullDecimal64Bind for the rationale. Use
// NullDecimal256BindWithScale for an explicit scale.
func (b *QwpBinds) NullDecimal256Bind(index int) *QwpBinds {
	return b.NullDecimal256BindWithScale(index, 0)
}

// NullDecimal256BindWithScale binds a NULL DECIMAL256 parameter with
// the given scale. Mirrors Java's setNullDecimal256.
func (b *QwpBinds) NullDecimal256BindWithScale(index int, scale int) *QwpBinds {
	if !b.checkScale256(scale) {
		return b
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(qwpTypeDecimal256, true)
	b.buf = append(b.buf, byte(scale))
	return b
}

// DecimalBind binds a parameter from a Decimal value, choosing the
// narrowest DECIMAL64 / 128 / 256 wire type that holds the unscaled
// coefficient. A NULL Decimal encodes as a typed DECIMAL256 null with
// scale 0.
func (b *QwpBinds) DecimalBind(index int, value Decimal) *QwpBinds {
	if b.err != nil {
		return b
	}
	if value.isNull() {
		return b.NullDecimal256BindWithScale(index, 0)
	}
	if err := value.ensureValidScale(); err != nil {
		b.err = fmt.Errorf("qwp bind: %w", err)
		return b
	}
	// Pick the smallest fixed-width form the coefficient fits into.
	// offset is the index of the most-significant byte in the 32-byte
	// big-endian unscaled buffer; 32-offset is the number of
	// significant bytes.
	sigBytes := 32 - int(value.offset)
	var wireSize int
	var typeCode qwpTypeCode
	switch {
	case sigBytes <= 8:
		wireSize = 8
		typeCode = qwpTypeDecimal64
	case sigBytes <= 16:
		wireSize = 16
		typeCode = qwpTypeDecimal128
	default:
		wireSize = 32
		typeCode = qwpTypeDecimal256
	}
	if !b.advance(index) {
		return b
	}
	b.writeHeader(typeCode, false)
	b.buf = append(b.buf, byte(value.scale))
	// Convert the 32-byte BE unscaled representation to a sign-extended
	// LE slice of wireSize bytes. wireSize is picked above so the
	// significant bytes always fit, so the inner loop only needs to
	// sign-extend across positions below value.offset. Shape matches
	// addDecimal's write loop in qwp_buffer.go.
	var signByte byte
	if value.offset < 32 && value.unscaled[value.offset]&0x80 != 0 {
		signByte = 0xFF
	}
	for i := 0; i < wireSize; i++ {
		srcIdx := 31 - i
		if uint8(srcIdx) < value.offset {
			b.buf = append(b.buf, signByte)
		} else {
			b.buf = append(b.buf, value.unscaled[srcIdx])
		}
	}
	return b
}

// setNull is the shared helper for per-type NullXxxBind methods.
func (b *QwpBinds) setNull(index int, t qwpTypeCode) *QwpBinds {
	if !b.advance(index) {
		return b
	}
	b.writeHeader(t, true)
	return b
}

func (b *QwpBinds) checkScale64(scale int) bool {
	return b.checkScaleRange(scale, qwpDecimal64MaxScale, "DECIMAL64")
}

func (b *QwpBinds) checkScale128(scale int) bool {
	return b.checkScaleRange(scale, qwpDecimal128MaxScale, "DECIMAL128")
}

func (b *QwpBinds) checkScale256(scale int) bool {
	return b.checkScaleRange(scale, qwpDecimal256MaxScale, "DECIMAL256")
}

func (b *QwpBinds) checkScaleRange(scale, maxScale int, typeName string) bool {
	if b.err != nil {
		return false
	}
	if scale < 0 || scale > maxScale {
		b.err = fmt.Errorf(
			"qwp bind: %s scale must be in [0, %d], got %d",
			typeName, maxScale, scale)
		return false
	}
	return true
}
