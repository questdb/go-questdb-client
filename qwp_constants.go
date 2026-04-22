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
	// 0x08 was TYPE_STRING in an earlier revision of the QWP spec; it
	// has been removed in favor of TYPE_VARCHAR (0x0F), which uses the
	// same wire encoding. Do not reuse this code.
	qwpTypeSymbol        qwpTypeCode = 0x09 // variable, dictionary-encoded
	qwpTypeTimestamp     qwpTypeCode = 0x0A // int64 microseconds, 8 bytes LE
	qwpTypeDate          qwpTypeCode = 0x0B // int64 milliseconds, 8 bytes LE
	qwpTypeUuid          qwpTypeCode = 0x0C // 16 bytes (lo then hi, LE)
	qwpTypeLong256       qwpTypeCode = 0x0D // 32 bytes (four int64s, LE)
	qwpTypeGeohash       qwpTypeCode = 0x0E // varint precision + packed bits
	qwpTypeVarchar       qwpTypeCode = 0x0F // variable, offset-based
	qwpTypeTimestampNano qwpTypeCode = 0x10 // int64 nanoseconds, 8 bytes LE
	qwpTypeDoubleArray   qwpTypeCode = 0x11 // N-dimensional float64 array
	qwpTypeLongArray     qwpTypeCode = 0x12 // N-dimensional int64 array
	qwpTypeDecimal64     qwpTypeCode = 0x13 // 8 bytes, little-endian unscaled
	qwpTypeDecimal128    qwpTypeCode = 0x14 // 16 bytes, little-endian unscaled
	qwpTypeDecimal256    qwpTypeCode = 0x15 // 32 bytes, little-endian unscaled
	qwpTypeChar          qwpTypeCode = 0x16 // UTF-16 code unit, 2 bytes LE
	// Decoder-only types: the Go encoder never emits them, but the
	// egress `RESULT_BATCH` decoder must handle columns the server
	// produces from arbitrary SELECTs (pg_catalog views, IP lookups,
	// binary columns, etc.).
	qwpTypeBinary qwpTypeCode = 0x17 // variable, offset+data (same layout as VARCHAR)
	qwpTypeIPv4   qwpTypeCode = 0x18 // 4 bytes LE, identical to INT
)

// qwpMsgKind is the one-byte discriminator at the start of every QWP
// egress payload (spec §5). Ingress DATA_BATCH messages use 0x00; the
// 0x10..0x16 range is reserved for egress request/response kinds.
type qwpMsgKind byte

const (
	qwpMsgKindDataBatch    qwpMsgKind = 0x00
	qwpMsgKindResponse     qwpMsgKind = 0x01
	qwpMsgKindQueryRequest qwpMsgKind = 0x10
	qwpMsgKindResultBatch  qwpMsgKind = 0x11
	qwpMsgKindResultEnd    qwpMsgKind = 0x12
	qwpMsgKindQueryError   qwpMsgKind = 0x13
	qwpMsgKindCancel       qwpMsgKind = 0x14
	qwpMsgKindCredit       qwpMsgKind = 0x15
	qwpMsgKindExecDone     qwpMsgKind = 0x16
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
	qwpFlagZstd            byte = 0x10 // payload after prelude is zstd-compressed (egress only)
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
	// Egress-specific status codes (spec §15).
	qwpStatusCancelled     qwpStatusCode = 0x0A // query terminated in response to CANCEL
	qwpStatusLimitExceeded qwpStatusCode = 0x0B // a protocol limit was hit
)

// QWP sender defaults and limits.
//
// Values are aligned with the Java client (io.questdb.client.cutlass.qwp)
// unless marked Go-only. The Java analogue is noted on each constant so
// the two clients can be kept in lockstep.
const (
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

	// qwpMaxColumnsPerTable caps columns per table. Go-only; the Java
	// client does not enforce a hard cap.
	qwpMaxColumnsPerTable = 2048

	// qwpMaxTablesPerBatch is the hard upper bound on distinct tables
	// in a single QWP message: the wire format encodes the table count
	// as uint16.
	qwpMaxTablesPerBatch = 0xFFFF

	// qwpDefaultInitRecvBufSize is the initial capacity of the ACK
	// receive buffer. Go-only; the Java client manages the read path
	// differently and has no direct counterpart.
	qwpDefaultInitRecvBufSize = 64 * 1024 // 64 KB

	// Hardening caps used by the egress `RESULT_BATCH` decoder. Match
	// the Java reference decoder (QwpResultBatchDecoder.java) so hostile
	// or buggy server frames that advertise out-of-range dimensions are
	// rejected before any large allocation.
	qwpMaxRowsPerBatch  = 1_048_576 // per-batch row cap
	qwpMaxTableNameLen  = 127       // UTF-8 bytes
	qwpMaxColumnNameLen = 127       // UTF-8 bytes
	qwpMaxArrayNDims    = 32        // max array dimensionality; matches Java reference
	// qwpMaxArrayElements caps the element count of a single ARRAY cell
	// so that element-count * 8 (element stride) plus the per-row shape
	// header (up to qwpMaxArrayNDims * 4 bytes) together stay inside
	// int32. The 1024-byte slack covers that shape header.
	qwpMaxArrayElements = (1<<31 - 1 - 1024) / 8
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
	case qwpTypeInt, qwpTypeFloat, qwpTypeIPv4:
		return 4
	case qwpTypeLong, qwpTypeDouble, qwpTypeTimestamp, qwpTypeDate,
		qwpTypeTimestampNano, qwpTypeDecimal64:
		return 8
	case qwpTypeUuid, qwpTypeDecimal128:
		return 16
	case qwpTypeLong256, qwpTypeDecimal256:
		return 32
	case qwpTypeSymbol, qwpTypeVarchar, qwpTypeBinary,
		qwpTypeGeohash, qwpTypeDoubleArray, qwpTypeLongArray:
		return -1 // variable-width
	default:
		return -1
	}
}
