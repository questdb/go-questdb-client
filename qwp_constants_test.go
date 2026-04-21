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
	// "QWP1" in ASCII: Q=0x51, W=0x57, P=0x50, 1=0x31
	// Stored as uint32 in little-endian: 0x31505751
	if qwpMagic != 0x31505751 {
		t.Fatalf("qwpMagic = 0x%08X, want 0x31505751", qwpMagic)
	}

	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], qwpMagic)
	if buf != [4]byte{'Q', 'W', 'P', '1'} {
		t.Fatalf("magic bytes = % X, want 'Q' 'W' 'P' '1'", buf)
	}
}

func TestQwpHeaderSize(t *testing.T) {
	if qwpHeaderSize != 12 {
		t.Fatalf("qwpHeaderSize = %d, want 12", qwpHeaderSize)
	}
}

func TestQwpHeaderFieldOffsets(t *testing.T) {
	// Magic occupies offsets [0..4), version at 4. Then flags, table
	// count, payload length at documented offsets.
	if qwpHeaderOffsetFlags != 5 {
		t.Fatalf("qwpHeaderOffsetFlags = %d, want 5", qwpHeaderOffsetFlags)
	}
	if qwpHeaderOffsetTableCount != 6 {
		t.Fatalf("qwpHeaderOffsetTableCount = %d, want 6", qwpHeaderOffsetTableCount)
	}
	if qwpHeaderOffsetPayloadLen != 8 {
		t.Fatalf("qwpHeaderOffsetPayloadLen = %d, want 8", qwpHeaderOffsetPayloadLen)
	}
}

func TestQwpVersion(t *testing.T) {
	if qwpVersion != 0x01 {
		t.Fatalf("qwpVersion = 0x%02X, want 0x01", qwpVersion)
	}
}

func TestQwpFlagBitPositions(t *testing.T) {
	if qwpFlagGorilla != 0x04 {
		t.Fatalf("qwpFlagGorilla = 0x%02X, want 0x04", qwpFlagGorilla)
	}
	if qwpFlagDeltaSymbolDict != 0x08 {
		t.Fatalf("qwpFlagDeltaSymbolDict = 0x%02X, want 0x08", qwpFlagDeltaSymbolDict)
	}
	// Flags are independent bits, so OR'ing them yields both set.
	if qwpFlagGorilla&qwpFlagDeltaSymbolDict != 0 {
		t.Fatalf("flag bits overlap: gorilla=0x%02X, deltaDict=0x%02X",
			qwpFlagGorilla, qwpFlagDeltaSymbolDict)
	}
}

func TestQwpSchemaModes(t *testing.T) {
	if qwpSchemaModeFull != 0x00 {
		t.Fatalf("qwpSchemaModeFull = 0x%02X, want 0x00", qwpSchemaModeFull)
	}
	if qwpSchemaModeReference != 0x01 {
		t.Fatalf("qwpSchemaModeReference = 0x%02X, want 0x01", qwpSchemaModeReference)
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
	}
	for _, c := range cases {
		if byte(c.tc) != c.want {
			t.Errorf("type code 0x%02X, want 0x%02X", byte(c.tc), c.want)
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

func TestQwpMaxTablesPerBatch(t *testing.T) {
	// The table count field in the header is a uint16, so the max
	// addressable tables per batch is 0xFFFF.
	if qwpMaxTablesPerBatch != 0xFFFF {
		t.Fatalf("qwpMaxTablesPerBatch = %d, want 65535", qwpMaxTablesPerBatch)
	}
}

func TestQwpMaxColumnsPerTable(t *testing.T) {
	// Matches QwpConstants.MAX_COLUMNS_PER_TABLE in the server.
	if qwpMaxColumnsPerTable != 2048 {
		t.Fatalf("qwpMaxColumnsPerTable = %d, want 2048", qwpMaxColumnsPerTable)
	}
}

func TestQwpTimestampEncodingFlags(t *testing.T) {
	// Per-column timestamp encoding flag byte values (QWP spec §12).
	if qwpTsEncodingUncompressed != 0x00 {
		t.Fatalf("qwpTsEncodingUncompressed = 0x%02X, want 0x00", qwpTsEncodingUncompressed)
	}
	if qwpTsEncodingGorilla != 0x01 {
		t.Fatalf("qwpTsEncodingGorilla = 0x%02X, want 0x01", qwpTsEncodingGorilla)
	}
}

func TestQwpLongNullSentinel(t *testing.T) {
	// Int64 MinInt64 as uint64 — used as the null sentinel for
	// non-nullable LONG/TIMESTAMP/DATE/UUID/LONG256 columns.
	if qwpLongNull != 0x8000000000000000 {
		t.Fatalf("qwpLongNull = 0x%016X, want 0x8000000000000000", qwpLongNull)
	}
}
