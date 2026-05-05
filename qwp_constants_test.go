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
	"testing"
)

// Ported from QwpConstantsTest in the Java QuestDB server so the two
// implementations stay in lockstep on wire-protocol constants.

func TestQwpMagicBytesValue(t *testing.T) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], qwpMagic)
	if buf != [4]byte{'Q', 'W', 'P', '1'} {
		t.Fatalf("magic bytes = % X, want 'Q' 'W' 'P' '1'", buf)
	}
}

func TestQwpStatusCodes(t *testing.T) {
	// ACK status codes the server emits. These must match the Java
	// reference so QwpError classification stays correct.
	cases := []struct {
		code qwpStatusCode
		want byte
	}{
		{qwpStatusOK, 0x00},
		{qwpStatusSchemaMismatch, 0x03},
		{qwpStatusParseError, 0x05},
		{qwpStatusInternalError, 0x06},
		{qwpStatusSecurityError, 0x08},
		{qwpStatusWriteError, 0x09},
		{qwpStatusCancelled, 0x0A},
		{qwpStatusLimitExceeded, 0x0B},
	}
	for _, c := range cases {
		if byte(c.code) != c.want {
			t.Errorf("status 0x%02X, want 0x%02X", byte(c.code), c.want)
		}
	}
}

func TestQwpTypeCodes(t *testing.T) {
	// Spec-defined wire type codes. Changing any of these is a
	// protocol break and requires a coordinated server/client update.
	cases := []struct {
		tc   qwpTypeCode
		want byte
	}{
		{qwpTypeBoolean, 0x01},
		{qwpTypeByte, 0x02},
		{qwpTypeShort, 0x03},
		{qwpTypeInt, 0x04},
		{qwpTypeLong, 0x05},
		{qwpTypeFloat, 0x06},
		{qwpTypeDouble, 0x07},
		// 0x08 is reserved (old TYPE_STRING, now removed).
		{qwpTypeSymbol, 0x09},
		{qwpTypeTimestamp, 0x0A},
		{qwpTypeDate, 0x0B},
		{qwpTypeUuid, 0x0C},
		{qwpTypeLong256, 0x0D},
		{qwpTypeGeohash, 0x0E},
		{qwpTypeVarchar, 0x0F},
		{qwpTypeTimestampNano, 0x10},
		{qwpTypeDoubleArray, 0x11},
		{qwpTypeLongArray, 0x12},
		{qwpTypeDecimal64, 0x13},
		{qwpTypeDecimal128, 0x14},
		{qwpTypeDecimal256, 0x15},
		{qwpTypeChar, 0x16},
		{qwpTypeBinary, 0x17},
		{qwpTypeIPv4, 0x18},
	}
	for _, c := range cases {
		if byte(c.tc) != c.want {
			t.Errorf("type code 0x%02X, want 0x%02X", byte(c.tc), c.want)
		}
	}
}

func TestQwpMsgKinds(t *testing.T) {
	// Egress message-kind discriminators (spec §5). Values here are
	// the wire bytes the egress server sends and the Go client must
	// dispatch on; they must match the Java QwpEgressMsgKind constants.
	cases := []struct {
		kind qwpMsgKind
		want byte
	}{
		{qwpMsgKindDataBatch, 0x00},
		{qwpMsgKindResponse, 0x01},
		{qwpMsgKindQueryRequest, 0x10},
		{qwpMsgKindResultBatch, 0x11},
		{qwpMsgKindResultEnd, 0x12},
		{qwpMsgKindQueryError, 0x13},
		{qwpMsgKindCancel, 0x14},
		{qwpMsgKindCredit, 0x15},
		{qwpMsgKindExecDone, 0x16},
		{qwpMsgKindCacheReset, 0x17},
	}
	for _, c := range cases {
		if byte(c.kind) != c.want {
			t.Errorf("msg kind 0x%02X, want 0x%02X", byte(c.kind), c.want)
		}
	}
}

func TestQwpFixedTypeSize(t *testing.T) {
	cases := []struct {
		tc   qwpTypeCode
		want int
	}{
		{qwpTypeBoolean, 0}, // bit-packed
		{qwpTypeByte, 1},
		{qwpTypeShort, 2},
		{qwpTypeChar, 2},
		{qwpTypeInt, 4},
		{qwpTypeFloat, 4},
		{qwpTypeIPv4, 4},
		{qwpTypeLong, 8},
		{qwpTypeDouble, 8},
		{qwpTypeTimestamp, 8},
		{qwpTypeTimestampNano, 8},
		{qwpTypeDate, 8},
		{qwpTypeDecimal64, 8},
		{qwpTypeUuid, 16},
		{qwpTypeDecimal128, 16},
		{qwpTypeLong256, 32},
		{qwpTypeDecimal256, 32},
		// Variable-width types report -1.
		{qwpTypeSymbol, -1},
		{qwpTypeVarchar, -1},
		{qwpTypeBinary, -1},
		{qwpTypeGeohash, -1},
		{qwpTypeDoubleArray, -1},
		{qwpTypeLongArray, -1},
	}
	for _, c := range cases {
		if got := qwpFixedTypeSize(c.tc); got != c.want {
			t.Errorf("qwpFixedTypeSize(0x%02X) = %d, want %d", byte(c.tc), got, c.want)
		}
	}
}

func TestQwpLongNullSentinel(t *testing.T) {
	// Int64 MinInt64 as uint64 — used as the null sentinel for
	// non-nullable LONG/TIMESTAMP/DATE/UUID/LONG256 columns.
	if qwpLongNull != 0x8000000000000000 {
		t.Fatalf("qwpLongNull = 0x%016X, want 0x8000000000000000", qwpLongNull)
	}
}

func TestQwpFlagBitPositions(t *testing.T) {
	// Header flag bits. Drift here is a wire-format break — the
	// server uses these exact bits to signal Gorilla / delta-dict /
	// zstd payload encoding. Mirrors Java's QwpConstantsTest
	// testFlagBitPositions.
	if qwpFlagGorilla != 0x04 {
		t.Errorf("qwpFlagGorilla = 0x%02X, want 0x04", qwpFlagGorilla)
	}
	if qwpFlagDeltaSymbolDict != 0x08 {
		t.Errorf("qwpFlagDeltaSymbolDict = 0x%02X, want 0x08", qwpFlagDeltaSymbolDict)
	}
	// qwpFlagZstd is Go-side specific (the egress server uses it for
	// RESULT_BATCH compression). Pinned to catch silent drift.
	if qwpFlagZstd != 0x10 {
		t.Errorf("qwpFlagZstd = 0x%02X, want 0x10", qwpFlagZstd)
	}
}

func TestQwpHeaderSize(t *testing.T) {
	// 12-byte header: 4 magic + 1 version + 2 reserved + 1 flags
	// + 4 payload-length. Drift here means the encoder and the
	// decoder won't agree on where the payload starts. Mirrors
	// Java's QwpConstantsTest testHeaderSize.
	if qwpHeaderSize != 12 {
		t.Errorf("qwpHeaderSize = %d, want 12", qwpHeaderSize)
	}
	// Pin the offsets the decoder actually reaches into too — a
	// reorganised header that kept the size but moved the flags or
	// payload-length fields would slip past the size check above.
	if qwpHeaderOffsetFlags != 5 {
		t.Errorf("qwpHeaderOffsetFlags = %d, want 5", qwpHeaderOffsetFlags)
	}
	if qwpHeaderOffsetPayloadLen != 8 {
		t.Errorf("qwpHeaderOffsetPayloadLen = %d, want 8", qwpHeaderOffsetPayloadLen)
	}
}

func TestQwpMaxColumnsPerTable(t *testing.T) {
	// Mirrors Java's QwpConstantsTest testMaxColumnsPerTable.
	if qwpMaxColumnsPerTable != 2048 {
		t.Errorf("qwpMaxColumnsPerTable = %d, want 2048", qwpMaxColumnsPerTable)
	}
}

func TestQwpMaxBindsPerQuery(t *testing.T) {
	// Pinned by spec §16 (max bind parameters per QUERY_REQUEST).
	if qwpMaxBindsPerQuery != 1024 {
		t.Errorf("qwpMaxBindsPerQuery = %d, want 1024", qwpMaxBindsPerQuery)
	}
}

func TestQwpMaxSqlTextBytes(t *testing.T) {
	// Pinned by spec §16 (max SQL text length: 1 MiB UTF-8 bytes).
	if qwpMaxSqlTextBytes != 1024*1024 {
		t.Errorf("qwpMaxSqlTextBytes = %d, want %d", qwpMaxSqlTextBytes, 1024*1024)
	}
}

func TestQwpMaxBatchSize(t *testing.T) {
	// Pinned by spec §14 "Protocol Limits": Max batch size 16 MB.
	// Mirrors Java QwpConstants.DEFAULT_MAX_BATCH_SIZE.
	if qwpMaxBatchSize != 16*1024*1024 {
		t.Errorf("qwpMaxBatchSize = %d, want %d", qwpMaxBatchSize, 16*1024*1024)
	}
}

func TestQwpIsFixedWidthType(t *testing.T) {
	// Go has no isFixedWidth() boolean — the same information is
	// encoded in qwpFixedTypeSize (>= 0 for fixed, -1 for variable).
	// Mirrors Java's QwpConstantsTest testIsFixedWidthType: the
	// classification is a wire-format invariant (fixed-width types
	// pack into the data section without offsets, variable-width
	// types carry a (nonNullCount+1)*4 offset table or a custom
	// per-cell layout).
	fixed := []qwpTypeCode{
		qwpTypeBoolean, qwpTypeByte, qwpTypeShort, qwpTypeChar,
		qwpTypeInt, qwpTypeLong, qwpTypeFloat, qwpTypeDouble,
		qwpTypeTimestamp, qwpTypeTimestampNano, qwpTypeDate,
		qwpTypeUuid, qwpTypeLong256,
		qwpTypeDecimal64, qwpTypeDecimal128, qwpTypeDecimal256,
		qwpTypeIPv4,
	}
	for _, tc := range fixed {
		if qwpFixedTypeSize(tc) < 0 {
			t.Errorf("qwpFixedTypeSize(0x%02X) = -1; expected fixed-width type", byte(tc))
		}
	}
	variable := []qwpTypeCode{
		qwpTypeSymbol, qwpTypeGeohash, qwpTypeVarchar, qwpTypeBinary,
		qwpTypeDoubleArray, qwpTypeLongArray,
	}
	for _, tc := range variable {
		if qwpFixedTypeSize(tc) != -1 {
			t.Errorf("qwpFixedTypeSize(0x%02X) = %d; expected -1 (variable-width)", byte(tc), qwpFixedTypeSize(tc))
		}
	}
}
