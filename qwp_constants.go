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
// 0x10..0x17 range is reserved for egress request/response kinds.
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
	// qwpMsgKindCacheReset is a server → client connection-scoped
	// cache-reset notification. Body is a single reset_mask byte (see
	// qwpResetMask* below) whose bits tell the client which caches to
	// discard. Sent between queries when a cache reaches the server's
	// configured soft cap; after applying, the next RESULT_BATCH's
	// delta-dict deltaStart and schema-reference ids are expected to
	// line up with a fresh server counter. Does not surface to users.
	qwpMsgKindCacheReset qwpMsgKind = 0x17
	// qwpMsgKindServerInfo is the unsolicited server → client frame
	// delivered as the first WebSocket frame after a v2 upgrade. Body
	// (little-endian, after the 12-byte QWP header):
	// role(u8) + epoch(u64) + capabilities(u32) + server_wall_ns(i64)
	// + cluster_id(u16_len + utf8) + node_id(u16_len + utf8). v1
	// servers omit the frame entirely. The byte 0x18 is also bound to
	// qwpTypeIPv4 in the qwpTypeCode enum; no collision since the two
	// are distinct types.
	qwpMsgKindServerInfo qwpMsgKind = 0x18
)

// SERVER_INFO role byte values (spec §11.8). Mirror Java
// QwpEgressMsgKind.ROLE_*.
const (
	// qwpRoleStandalone marks a node with no replication configured.
	// OSS single-node default; behaves like a primary for routing
	// purposes and is accepted by target=primary.
	qwpRoleStandalone byte = 0x00
	// qwpRolePrimary is the authoritative write node; reads see latest
	// commits.
	qwpRolePrimary byte = 0x01
	// qwpRoleReplica is read-only and may lag the primary by up to the
	// replication poll interval.
	qwpRoleReplica byte = 0x02
	// qwpRolePrimaryCatchup signals a promotion in flight; behaves like
	// a primary but is still uploading in-flight segments. Accepted by
	// target=primary.
	qwpRolePrimaryCatchup byte = 0x03
)

// Bit flags carried in the reset_mask byte of a CACHE_RESET frame.
// Mirrors the Java QwpEgressMsgKind.RESET_MASK_* constants.
const (
	// qwpResetMaskDict clears the connection-scoped SYMBOL dict. After
	// applying, the next RESULT_BATCH's delta section must start at
	// deltaStart=0 — i.e. the server has also reset its dict to empty.
	qwpResetMaskDict byte = 0x01
	// qwpResetMaskSchemas clears the schema-fingerprint cache. After
	// applying, the next RESULT_BATCH must ship its schema in full
	// mode (not reference mode) with a fresh id.
	qwpResetMaskSchemas byte = 0x02
)

// qwpMagic is the 4-byte magic at the start of every QWP message.
// Stored as a uint32 in little-endian byte order: "QWP1".
const qwpMagic uint32 = 0x31505751

// qwpVersion is the version byte stamped into the 12-byte QWP header
// of every ingest frame this client encodes. Held at v1 so the
// encoded ingest stream stays compatible with both v1 and v2 QuestDB
// servers (v2 servers accept v1-stamped ingest frames as a subset of
// their wire protocol). The handshake max-version we advertise is
// qwpMaxSupportedVersion, which may exceed qwpVersion to opt the
// connection into v2 server-side features (SERVER_INFO frame, multi-
// endpoint routing, transparent failover) without changing the encoded
// frame format.
const qwpVersion byte = 0x01

// qwpMaxSupportedVersion is the highest QWP protocol version this
// client knows how to consume on the wire. Advertised in the
// X-QWP-Max-Version handshake header; the server echoes
// min(server_max, client_max) back as X-QWP-Version. v2 enables the
// server to emit SERVER_INFO and the v2-only egress features (target
// filter, transparent failover). Once the handshake settles, decoders
// enforce strict equality between every server frame's header version
// byte and the negotiated version (spec §3) — this constant only caps
// what we will agree to negotiate to, not what we will accept on a
// live connection.
const qwpMaxSupportedVersion byte = 0x02

// QWP message header layout.
const (
	qwpHeaderSize              = 12
	qwpHeaderOffsetFlags       = 5
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
	qwpStatusDurableAck     qwpStatusCode = 0x02 // batch WAL uploaded to object store (opt-in)
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

	// qwpMaxBindsPerQuery caps bind parameters per QUERY_REQUEST.
	// Spec §16. The server enforces this independently; the client-side
	// preflight surfaces a typed error before bytes leave the process.
	// Distinct from qwpMaxColumnsPerTable (an ingest concept) — egress
	// QUERY_REQUEST and ingest DATA_BATCH have independent limits.
	qwpMaxBindsPerQuery = 1024

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

	// qwpMaxConnDictHeapBytes caps the connection-scoped SYMBOL dict
	// UTF-8 heap at 256 MiB. Servers that approach this cap are
	// expected to emit CACHE_RESET; crossing it without a reset is a
	// misbehaving (or hostile) server. Below uint32 max so the
	// uint32 offsets stored on each entry cannot wrap. Mirrors Java
	// QwpResultBatchDecoder.MAX_CONN_DICT_HEAP_BYTES.
	qwpMaxConnDictHeapBytes = 256 * 1024 * 1024

	// qwpMaxConnDictSize caps the connection-scoped SYMBOL dict entry
	// count. Mirrors Java QwpResultBatchDecoder.MAX_CONN_DICT_SIZE
	// (2^23) — same defensive intent as the heap cap.
	qwpMaxConnDictSize = 8_388_608
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
