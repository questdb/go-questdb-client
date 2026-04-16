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

import "time"

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
	qwpTypeDecimal64     qwpTypeCode = 0x13 // 8 bytes, little-endian unscaled
	qwpTypeDecimal128    qwpTypeCode = 0x14 // 16 bytes, little-endian unscaled
	qwpTypeDecimal256    qwpTypeCode = 0x15 // 32 bytes, little-endian unscaled
	qwpTypeChar          qwpTypeCode = 0x16 // UTF-16 code unit, 2 bytes LE
)

// qwpMagic is the 4-byte magic at the start of every QWP message.
// Stored as a uint32 in little-endian byte order: "QWP1".
const qwpMagic uint32 = 0x31505751

// qwpVersion is the current protocol version.
const qwpVersion byte = 0x01

// QWP message header layout.
const (
	qwpHeaderSize              = 12
	qwpHeaderOffsetFlags       = 5
	qwpHeaderOffsetTableCount  = 6
	qwpHeaderOffsetPayloadLen  = 8
)

// QWP header flag bits.
const (
	qwpFlagGorilla         byte = 0x04 // Gorilla timestamp encoding
	qwpFlagDeltaSymbolDict byte = 0x08 // delta symbol dictionary
)

// qwpSchemaMode values control how column schema is transmitted.
type qwpSchemaMode byte

const (
	qwpSchemaModeFull      qwpSchemaMode = 0x00 // full column definitions
	qwpSchemaModeReference qwpSchemaMode = 0x01 // reference a schema already registered by ID
)

// qwpStatusCode represents a server response status.
type qwpStatusCode byte

const (
	qwpStatusOK             qwpStatusCode = 0x00 // batch accepted
	qwpStatusSchemaMismatch qwpStatusCode = 0x03 // column type incompatible with existing table
	qwpStatusParseError     qwpStatusCode = 0x05 // malformed message
	qwpStatusInternalError  qwpStatusCode = 0x06 // server-side error
	qwpStatusSecurityError  qwpStatusCode = 0x08 // authorization failure
	qwpStatusWriteError     qwpStatusCode = 0x09 // write failure (e.g., table not accepting writes)
)

// QWP sender defaults and limits.
//
// Values are aligned with the Java client (io.questdb.client.cutlass.qwp)
// unless marked Go-only. The Java analogue is noted on each constant so
// the two clients can be kept in lockstep. These are not all honored by
// the sender yet — wiring follows in a later change.
const (
	// qwpDefaultAutoFlushBytes is the byte-size trigger for auto-flush.
	// A value of 0 disables the byte-based trigger.
	// Java: QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES = 0.
	//lint:ignore U1000 auto-flush wiring pending
	qwpDefaultAutoFlushBytes = 0

	// qwpDefaultAutoFlushInterval is the time trigger for auto-flush.
	// Java: QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS = 100 ms.
	qwpDefaultAutoFlushInterval = 100 * time.Millisecond

	// qwpDefaultAutoFlushRows is the row-count trigger for auto-flush.
	// Java: QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS = 1_000.
	qwpDefaultAutoFlushRows = 1_000

	// qwpDefaultInFlightWindow is the default maximum number of batches
	// that may be outstanding (unacked) in async mode.
	// Java: QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE = 128.
	qwpDefaultInFlightWindow = 128

	// qwpDefaultMaxSchemasPerConnection caps the schema cache per
	// connection; callers may recycle the connection on overflow.
	// Java: QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION = 65_535.
	qwpDefaultMaxSchemasPerConnection = 65_535

	// qwpDefaultInitEncoderBufSize is the initial encoder buffer size.
	// Java: QwpWebSocketSender.DEFAULT_BUFFER_SIZE = 8192.
	qwpDefaultInitEncoderBufSize = 8 * 1024 // 8 KB

	// qwpDefaultMicrobatchBufSize is the per-encoder microbatch buffer
	// size used to coalesce rows before a WebSocket frame is sent.
	// Java: QwpWebSocketSender.DEFAULT_MICROBATCH_BUFFER_SIZE = 1 MB.
	qwpDefaultMicrobatchBufSize = 1 * 1024 * 1024 // 1 MB

	// qwpMaxTableNameLength caps table-name length. Honored by the
	// shared lineSenderConfig.fileNameLimit (defaultFileNameLimit in
	// sender.go), enforced in qwpValidateTableName.
	// Java: QwpWebSocketSender.MAX_TABLE_NAME_LENGTH = 127.
	//lint:ignore U1000 default wiring pending
	qwpMaxTableNameLength = 127

	// qwpMaxColumnNameLength caps column-name length. Honored by the
	// same lineSenderConfig.fileNameLimit, enforced in
	// qwpValidateColumnName.
	// Java: QwpTableBuffer.MAX_COLUMN_NAME_LENGTH = 127.
	//lint:ignore U1000 default wiring pending
	qwpMaxColumnNameLength = 127

	// qwpMaxColumnsPerTable caps columns per table. Go-only; the Java
	// client does not enforce a hard cap.
	qwpMaxColumnsPerTable = 2048

	// qwpDefaultInitRecvBufSize is the initial capacity of the ACK
	// receive buffer. Go-only; the Java client manages the read path
	// differently and has no direct counterpart.
	qwpDefaultInitRecvBufSize = 64 * 1024 // 64 KB
)

// qwpFixedTypeSize returns the per-value size in bytes for fixed-width
// types. Returns 0 for bit-packed types (BOOLEAN) and -1 for
// variable-width types.
func qwpFixedTypeSize(tc qwpTypeCode) int {
	switch tc {
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

