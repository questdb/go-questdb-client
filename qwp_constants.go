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
	qwpTypeDate          qwpTypeCode = 0x0B // int64 ms. Asymmetric: ingestion=plain int64; egress=timestamp-ish framing (enc byte + RAW/Gorilla, like qwpTypeTimestamp)
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
	// delta-dict deltaStart is expected to line up with a fresh server
	// counter. Does not surface to users.
	qwpMsgKindCacheReset qwpMsgKind = 0x17
	// qwpMsgKindServerInfo is the unsolicited server → client frame
	// the server emits as the first WebSocket frame after the upgrade,
	// before any client request. Body (little-endian, after the
	// 12-byte QWP header):
	// role(u8) + epoch(u64) + capabilities(u32) + server_wall_ns(i64)
	// + cluster_id(u16_len + utf8) + node_id(u16_len + utf8). The
	// server always emits it post-upgrade; ingest senders simply do
	// not read it. The byte 0x18 is also bound to qwpTypeIPv4 in the
	// qwpTypeCode enum; no collision since the two are distinct types.
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
)

// qwpMagic is the 4-byte magic at the start of every QWP message.
// Stored as a uint32 in little-endian byte order: "QWP1".
const qwpMagic uint32 = 0x31505751

// qwpVersion is the sole QWP protocol version. It is stamped into the
// 12-byte header of every frame this client encodes and advertised
// verbatim in the X-QWP-Max-Version handshake header on both the
// ingest and egress paths. The server echoes min(server_max,
// client_max) back as X-QWP-Version; decoders then enforce strict
// equality between every server frame's header version byte and the
// negotiated version (spec §3). The negotiation mechanism is retained
// so a future version bump has somewhere to grow, but today exactly
// one version exists.
const qwpVersion byte = 0x01

// qwpCapZone is the CAP_ZONE bit in SERVER_INFO.capabilities. When
// set, the server's SERVER_INFO frame carries an additional
// zone_id string after node_id; clients use it to drive the
// failover.md §2 zone-tier classification (Same / Unknown / Other).
// Absent CAP_ZONE leaves the host's zone tier at Unknown, which
// PickNext treats as a middle-priority bucket between Same and
// Other.
const qwpCapZone uint32 = 1 << 0

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

// QwpStatusCode represents a server response status. The byte value is
// stable on the QWP wire and is preserved on SenderError.ServerStatusByte
// for cross-language debugging; the recommended way to discriminate
// rejections is the higher-level Category enum.
type QwpStatusCode byte

const (
	QwpStatusOK             QwpStatusCode = 0x00 // batch accepted
	QwpStatusDurableAck     QwpStatusCode = 0x02 // per-table durable-upload ACK (replication primaries opted-in)
	QwpStatusSchemaMismatch QwpStatusCode = 0x03 // column type incompatible with existing table
	QwpStatusParseError     QwpStatusCode = 0x05 // malformed message
	QwpStatusInternalError  QwpStatusCode = 0x06 // server-side error
	QwpStatusSecurityError  QwpStatusCode = 0x08 // authorization failure
	QwpStatusWriteError     QwpStatusCode = 0x09 // write failure (e.g., table not accepting writes)
	// Egress-specific status codes (spec §15).
	qwpStatusCancelled     QwpStatusCode = 0x0A // query terminated in response to CANCEL
	qwpStatusLimitExceeded QwpStatusCode = 0x0B // a protocol limit was hit
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

	// qwpDefaultAutoFlushBytes is the byte-size trigger for auto-flush.
	// connect-string.md §Auto-flushing: "Default where supported: `8m`
	// (8 MiB)". Mirrors Java's DEFAULT_AUTO_FLUSH_BYTES. The effective
	// threshold the sender compares pendingBytes against is clamped
	// down to 90% of two limits — see qwpLineSender.applyServerBatchSizeLimit:
	//   - the server-advertised X-QWP-Max-Batch-Size, re-evaluated on
	//     every successful connect (initial bind and every reconnect); and
	//   - the per-segment frame cap (maxFrameBytes), fixed at construction
	//     from the cursor engine's segment size. Without this term the
	//     shipped defaults (8 MiB trigger over a 4 MiB segment) would let
	//     a batch grow past what a segment can hold and wedge on flush.
	// The clamp only reduces: a configured value below both caps is kept
	// as-is, and an explicit user opt-out (auto_flush_bytes=off / =0) is
	// preserved even when a cap applies.
	//
	// Three hard guards back the soft clamp in enqueueCursor /
	// atWithTimestamp, each dropping or rejecting with a typed error
	// before the frame leaves the process: a per-row guard (any single
	// row above the server cap), a flush-time server-cap guard, and a
	// flush-time segment-cap guard (an encoded frame larger than a
	// single segment can ever hold). The first two fire even when the
	// user opted out of byte-size auto-flush.
	qwpDefaultAutoFlushBytes = 8 * 1024 * 1024

	// qwpDefaultInFlightWindow is the default maximum number of batches
	// that may be outstanding (unacked) in async mode.
	// Java: QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE = 128.
	qwpDefaultInFlightWindow = 128

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

	// qwpMaxSqlTextBytes caps the UTF-8 byte length of the sql_bytes
	// field in a QUERY_REQUEST. Spec §16 pins this at 1 MiB. The server
	// enforces this independently; the client-side preflight produces a
	// friendlier error and avoids serializing a doomed payload.
	qwpMaxSqlTextBytes = 1 << 20

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

	// qwpMaxBatchSize is the headline protocol cap on a single
	// RESULT_BATCH frame's wire size, in bytes. Spec §14 "Protocol
	// Limits" pins this at 16 MiB; the Java server enforces the same
	// value via QwpConstants.DEFAULT_MAX_BATCH_SIZE. Acts as a direct
	// upper bound checked before per-section bounds (row count, column
	// count, dict heap, zstd content size) come into play — those
	// remain as defense-in-depth, but the single cap is the spec-level
	// limit a conformant server stays under.
	qwpMaxBatchSize     = 16 * 1024 * 1024
	qwpMaxRowsPerBatch  = 1_048_576 // per-batch row cap
	qwpMaxTableNameLen  = 127       // UTF-8 bytes
	qwpMaxColumnNameLen = 127       // UTF-8 bytes
	qwpMaxArrayNDims    = 32        // max array dimensionality; matches Java reference
	// qwpMaxArrayElements caps the element count of a single ARRAY cell
	// so that element-count * 8 (element stride) plus the per-row shape
	// header (up to qwpMaxArrayNDims * 4 bytes) together stay inside
	// int32. The 1024-byte slack covers that shape header.
	qwpMaxArrayElements = (1<<31 - 1 - 1024) / 8

	// qwpMaxCellsPerBatch caps the declared cell count (row_count ×
	// column_count) of one RESULT_BATCH. The decoder materialises a
	// row-indexed scratch array — rowCount entries wide — for every
	// column that carries nulls (nonNullIdx) and for every SYMBOL
	// (symbolRowIds) and ARRAY (arrayRowStart + arrayElems) column, so a
	// single column costs 4..12 bytes of heap per row. An all-null column
	// is nearly free on the wire — a rowCount/8 null bitmap that
	// zstd-compresses to almost nothing — yet still forces that full
	// rowCount-sized allocation: a 32–96× amplification. A frame packed
	// with such columns up to the decompressed-frame cap would otherwise
	// drive multi-GiB transient `make`s. A conformant server spends at
	// least one wire byte per cell, so a legitimate batch never declares
	// more cells than its maximum possible decompressed byte size. Tying
	// the cap to qwpZstdMaxDecompressedSize rejects amplified frames up
	// front — before the per-column loop sizes any index array — while
	// clearing every batch a real server emits.
	qwpMaxCellsPerBatch = qwpZstdMaxDecompressedSize

	// qwpReadLimitSlack is headroom added on top of qwpMaxBatchSize when
	// arming the WebSocket read limit. coder/websocket's limitReader
	// trips ErrMessageTooBig the moment its byte budget reaches zero —
	// before the terminal io.EOF is delivered — so a legitimate frame of
	// exactly qwpMaxBatchSize would be rejected without this margin (the
	// library applies the same +1 trick to its own default limit). The
	// band between qwpMaxBatchSize and the limit is never a valid frame:
	// the egress decoder rejects RESULT_BATCH payloads > qwpMaxBatchSize,
	// and every ACK / SERVER_INFO frame is far smaller.
	qwpReadLimitSlack = 4096

	// qwpMaxFrameReadLimit is the hard ceiling on a single inbound
	// WebSocket message. Egress RESULT_BATCH / SERVER_INFO and ingest
	// ACK frames share one connection, so this single cap covers both.
	// Armed via Conn.SetReadLimit so a hostile or buggy server cannot
	// OOM the host with a multi-GB frame: the limit is enforced *during*
	// the streamed read, before the whole message is resident, rather
	// than only after — qwpMaxBatchSize alone is checked post-assembly
	// by the decoder and not at all on the readAck path. It also caps
	// qwpReadFrameInto's buffer doubling as defense-in-depth.
	qwpMaxFrameReadLimit = qwpMaxBatchSize + qwpReadLimitSlack

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
