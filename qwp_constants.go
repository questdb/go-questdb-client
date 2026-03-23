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

// qwpTypeCode represents a QWP column type.
type qwpTypeCode byte

// QWP column type codes. Each type has a specific wire encoding
// defined in the QWP v1 protocol specification.
const (
	qwpTypeBoolean       qwpTypeCode = 0x01 // bit-packed, 1 bit per value
	qwpTypeByte          qwpTypeCode = 0x02 // int8, 1 byte
	qwpTypeShort         qwpTypeCode = 0x03 // int16, 2 bytes LE
	qwpTypeInt           qwpTypeCode = 0x04 // int32, 4 bytes LE
	qwpTypeLong          qwpTypeCode = 0x05 // int64, 8 bytes LE
	qwpTypeFloat         qwpTypeCode = 0x06 // IEEE 754 float32, 4 bytes LE
	qwpTypeDouble        qwpTypeCode = 0x07 // IEEE 754 float64, 8 bytes LE
	qwpTypeString        qwpTypeCode = 0x08 // variable, offset-based
	qwpTypeSymbol        qwpTypeCode = 0x09 // variable, dictionary-encoded
	qwpTypeTimestamp     qwpTypeCode = 0x0A // int64 microseconds, 8 bytes LE
	qwpTypeDate          qwpTypeCode = 0x0B // int64 milliseconds, 8 bytes LE
	qwpTypeUuid          qwpTypeCode = 0x0C // 16 bytes (lo then hi, LE)
	qwpTypeLong256       qwpTypeCode = 0x0D // 32 bytes (four int64s, LE)
	qwpTypeGeohash       qwpTypeCode = 0x0E // varint precision + packed bits
	qwpTypeVarchar       qwpTypeCode = 0x0F // variable, same encoding as string
	qwpTypeTimestampNano qwpTypeCode = 0x10 // int64 nanoseconds, 8 bytes LE
	qwpTypeDoubleArray   qwpTypeCode = 0x11 // N-dimensional float64 array
	qwpTypeLongArray     qwpTypeCode = 0x12 // N-dimensional int64 array
	qwpTypeDecimal64     qwpTypeCode = 0x13 // 8 bytes, big-endian unscaled
	qwpTypeDecimal128    qwpTypeCode = 0x14 // 16 bytes, big-endian unscaled
	qwpTypeDecimal256    qwpTypeCode = 0x15 // 32 bytes, big-endian unscaled
	qwpTypeChar          qwpTypeCode = 0x16 // UTF-16 code unit, 2 bytes LE
)

const (
	// qwpTypeNullableFlag is the high bit of the type code byte,
	// indicating that the column has a null bitmap.
	qwpTypeNullableFlag qwpTypeCode = 0x80

	// qwpTypeMask masks out the nullable flag to get the base type code.
	qwpTypeMask qwpTypeCode = 0x7F
)

// qwpMagic is the 4-byte magic at the start of every QWP message.
// Stored as a uint32 in little-endian byte order: "QWP1".
const qwpMagic uint32 = 0x31505751

// Capability negotiation magic values (little-endian uint32).
const (
	qwpMagicCapabilityRequest  uint32 = 0x3F504C49 // "ILP?"
	qwpMagicCapabilityResponse uint32 = 0x21504C49 // "ILP!"
	qwpMagicFallback           uint32 = 0x30504C49 // "ILP0"
)

// qwpVersion is the current protocol version.
const qwpVersion byte = 0x01

// QWP message header layout.
const (
	qwpHeaderSize       = 12
	qwpHeaderOffsetFlags = 5
)

// QWP header flag bits.
const (
	qwpFlagLZ4             byte = 0x01 // LZ4 compression
	qwpFlagZstd            byte = 0x02 // Zstd compression
	qwpFlagGorilla         byte = 0x04 // Gorilla timestamp encoding
	qwpFlagDeltaSymbolDict byte = 0x08 // delta symbol dictionary
	qwpFlagCompressionMask byte = 0x03 // bits 0-1
)

// qwpSchemaMode values control how column schema is transmitted.
type qwpSchemaMode byte

const (
	qwpSchemaModeFull      qwpSchemaMode = 0x00 // full column definitions
	qwpSchemaModeReference qwpSchemaMode = 0x01 // schema hash reference
)

// qwpStatusCode represents a server response status.
type qwpStatusCode byte

const (
	qwpStatusOK             qwpStatusCode = 0x00 // batch accepted
	qwpStatusPartial        qwpStatusCode = 0x01 // some rows failed
	qwpStatusSchemaRequired qwpStatusCode = 0x02 // schema hash not recognized
	qwpStatusSchemaMismatch qwpStatusCode = 0x03 // column type incompatible
	qwpStatusTableNotFound  qwpStatusCode = 0x04 // table doesn't exist
	qwpStatusParseError     qwpStatusCode = 0x05 // malformed message
	qwpStatusInternalError  qwpStatusCode = 0x06 // server error
	qwpStatusOverloaded     qwpStatusCode = 0x07 // backpressure, retry later
)

// QWP batch and table limits.
const (
	qwpDefaultMaxBatchSize      = 16 * 1024 * 1024 // 16 MB
	qwpDefaultMaxTablesPerBatch = 256
	qwpMaxColumnsPerTable       = 2048
	qwpDefaultMaxRowsPerTable   = 1_000_000
	qwpDefaultMaxInFlightWindow = 4
	qwpDefaultInitRecvBufSize   = 64 * 1024 // 64 KB
)

// qwpFixedTypeSize returns the per-value size in bytes for fixed-width
// types. Returns 0 for bit-packed types (BOOLEAN) and -1 for
// variable-width types.
func qwpFixedTypeSize(tc qwpTypeCode) int {
	switch tc & qwpTypeMask {
	case qwpTypeBoolean:
		return 0 // bit-packed
	case qwpTypeByte:
		return 1
	case qwpTypeShort, qwpTypeChar:
		return 2
	case qwpTypeInt, qwpTypeFloat:
		return 4
	case qwpTypeLong, qwpTypeDouble, qwpTypeTimestamp, qwpTypeDate,
		qwpTypeTimestampNano, qwpTypeDecimal64:
		return 8
	case qwpTypeUuid, qwpTypeDecimal128:
		return 16
	case qwpTypeLong256, qwpTypeDecimal256:
		return 32
	case qwpTypeString, qwpTypeSymbol, qwpTypeVarchar,
		qwpTypeGeohash, qwpTypeDoubleArray, qwpTypeLongArray:
		return -1 // variable-width
	default:
		return -1
	}
}

// qwpIsFixedWidthType reports whether the given type code is a
// fixed-width type (not bit-packed and not variable-width).
func qwpIsFixedWidthType(tc qwpTypeCode) bool {
	return qwpFixedTypeSize(tc) > 0
}

// qwpIsDecimalType reports whether the given type code is a decimal type
// that carries a scale byte in the schema.
func qwpIsDecimalType(tc qwpTypeCode) bool {
	base := tc & qwpTypeMask
	return base == qwpTypeDecimal64 || base == qwpTypeDecimal128 || base == qwpTypeDecimal256
}
