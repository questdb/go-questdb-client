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
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

// Header bytes mirrored from qwp_bind_values.go to keep the test
// independent of the production constants (so flipping a byte there
// fails the tests rather than silently passing).
const (
	testBindNonNull    byte = 0x00
	testBindNullFlag   byte = 0x01
	testBindNullBitmap byte = 0x01
)

// --- Helpers -------------------------------------------------------------

type byteBuf struct{ b []byte }

func (w *byteBuf) put(b ...byte) { w.b = append(w.b, b...) }
func (w *byteBuf) putU16(v uint16) {
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}
func (w *byteBuf) putU32(v uint32) {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}
func (w *byteBuf) putU64(v uint64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}
func (w *byteBuf) putVarint(v uint64) {
	for v > 0x7F {
		w.b = append(w.b, byte(v&0x7F)|0x80)
		v >>= 7
	}
	w.b = append(w.b, byte(v))
}

func assertEncoded(t *testing.T, b *QwpBinds, wantCount int, want []byte) {
	t.Helper()
	if err := b.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Count() != wantCount {
		t.Fatalf("count=%d, want %d", b.Count(), wantCount)
	}
	if !bytes.Equal(b.bufferBytes(), want) {
		t.Fatalf("encoded bytes mismatch:\n got: % x\nwant: % x",
			b.bufferBytes(), want)
	}
}

// --- Per-type encoding tests --------------------------------------------

func TestQwpBindsBoolean(t *testing.T) {
	var b QwpBinds
	b.BooleanBind(0, true)
	var w byteBuf
	w.put(byte(qwpTypeBoolean), testBindNonNull, 1)
	assertEncoded(t, &b, 1, w.b)

	b.reset()
	b.BooleanBind(0, false)
	var w2 byteBuf
	w2.put(byte(qwpTypeBoolean), testBindNonNull, 0)
	assertEncoded(t, &b, 1, w2.b)
}

func TestQwpBindsByte(t *testing.T) {
	var b QwpBinds
	b.ByteBind(0, -128)
	b.ByteBind(1, 0)
	b.ByteBind(2, 127)
	minVal := int8(-128)
	var w byteBuf
	w.put(byte(qwpTypeByte), testBindNonNull, byte(minVal))
	w.put(byte(qwpTypeByte), testBindNonNull, 0)
	w.put(byte(qwpTypeByte), testBindNonNull, 127)
	assertEncoded(t, &b, 3, w.b)
}

func TestQwpBindsChar(t *testing.T) {
	var b QwpBinds
	b.CharBind(0, 'Z')
	var w byteBuf
	w.put(byte(qwpTypeChar), testBindNonNull)
	w.putU16(uint16('Z'))
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsCharRejectsNonBMP(t *testing.T) {
	var b QwpBinds
	b.CharBind(0, 0x1F600) // 😀
	if b.Err() == nil {
		t.Fatalf("expected CharBind to reject non-BMP rune")
	}
	if b.Count() != 0 {
		t.Fatalf("expected failed bind to leave count=0, got %d", b.Count())
	}
	if !strings.Contains(b.Err().Error(), "CHAR") {
		t.Fatalf("error message should mention CHAR: %v", b.Err())
	}
}

func TestQwpBindsDate(t *testing.T) {
	var b QwpBinds
	b.DateBind(0, 1_700_000_000_000)
	var w byteBuf
	w.put(byte(qwpTypeDate), testBindNonNull)
	w.putU64(uint64(int64(1_700_000_000_000)))
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsDecimal64(t *testing.T) {
	var b QwpBinds
	b.Decimal64Bind(0, 2, 12345)
	var w byteBuf
	w.put(byte(qwpTypeDecimal64), testBindNonNull, 2)
	w.putU64(uint64(int64(12345)))
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsDecimal128(t *testing.T) {
	var b QwpBinds
	b.Decimal128Bind(0, 6, 0x0123456789ABCDEF, 0x7766554433221100)
	var w byteBuf
	w.put(byte(qwpTypeDecimal128), testBindNonNull, 6)
	w.putU64(0x0123456789ABCDEF)
	w.putU64(0x7766554433221100)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsDecimal256(t *testing.T) {
	var b QwpBinds
	b.Decimal256Bind(0, 10,
		0x1111111111111111, 0x2222222222222222,
		0x3333333333333333, 0x4444444444444444)
	var w byteBuf
	w.put(byte(qwpTypeDecimal256), testBindNonNull, 10)
	w.putU64(0x1111111111111111)
	w.putU64(0x2222222222222222)
	w.putU64(0x3333333333333333)
	w.putU64(0x4444444444444444)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsDecimalRejectsBadScale(t *testing.T) {
	cases := []int{-1, int(maxDecimalScale) + 1}
	for _, scale := range cases {
		var b QwpBinds
		b.Decimal64Bind(0, scale, 1)
		if b.Err() == nil {
			t.Fatalf("scale=%d should have been rejected", scale)
		}
		if !strings.Contains(b.Err().Error(), "scale") {
			t.Fatalf("expected scale-related error, got: %v", b.Err())
		}
	}
}

func TestQwpBindsDecimalPerWidthScaleCaps(t *testing.T) {
	// Per-width scale caps: DECIMAL64 stores up to 18 digits,
	// DECIMAL128 up to 38, DECIMAL256 up to 76. The check is a
	// client-side preflight — the server enforces only the global
	// cap (76), so callers who bypass per-width validation can land
	// in a state where the bound parameter's coefficient overflows
	// the type's representable range. Mirrors Java's
	// QwpBindValuesTest scale-bound rejections.
	type scaleCase struct {
		name     string
		typeName string // expected substring in error message
		ok       int
		bad      int
		bind     func(b *QwpBinds, scale int) *QwpBinds
	}
	cases := []scaleCase{
		{
			name: "Decimal64", typeName: "DECIMAL64",
			ok:  qwpDecimal64MaxScale,
			bad: qwpDecimal64MaxScale + 1,
			bind: func(b *QwpBinds, scale int) *QwpBinds {
				return b.Decimal64Bind(0, scale, 1)
			},
		},
		{
			name: "Decimal128", typeName: "DECIMAL128",
			ok:  qwpDecimal128MaxScale,
			bad: qwpDecimal128MaxScale + 1,
			bind: func(b *QwpBinds, scale int) *QwpBinds {
				return b.Decimal128Bind(0, scale, 0, 0)
			},
		},
		{
			name: "Decimal256", typeName: "DECIMAL256",
			ok:  qwpDecimal256MaxScale,
			bad: qwpDecimal256MaxScale + 1,
			bind: func(b *QwpBinds, scale int) *QwpBinds {
				return b.Decimal256Bind(0, scale, 0, 0, 0, 0)
			},
		},
		{
			name: "NullDecimal64WithScale", typeName: "DECIMAL64",
			ok:  qwpDecimal64MaxScale,
			bad: qwpDecimal64MaxScale + 1,
			bind: func(b *QwpBinds, scale int) *QwpBinds {
				return b.NullDecimal64BindWithScale(0, scale)
			},
		},
		{
			name: "NullDecimal128WithScale", typeName: "DECIMAL128",
			ok:  qwpDecimal128MaxScale,
			bad: qwpDecimal128MaxScale + 1,
			bind: func(b *QwpBinds, scale int) *QwpBinds {
				return b.NullDecimal128BindWithScale(0, scale)
			},
		},
		{
			name: "NullDecimal256WithScale", typeName: "DECIMAL256",
			ok:  qwpDecimal256MaxScale,
			bad: qwpDecimal256MaxScale + 1,
			bind: func(b *QwpBinds, scale int) *QwpBinds {
				return b.NullDecimal256BindWithScale(0, scale)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name+"AcceptsBoundary", func(t *testing.T) {
			var b QwpBinds
			c.bind(&b, c.ok)
			if err := b.Err(); err != nil {
				t.Fatalf("scale=%d should be accepted: %v", c.ok, err)
			}
		})
		t.Run(c.name+"RejectsOverBoundary", func(t *testing.T) {
			var b QwpBinds
			c.bind(&b, c.bad)
			if b.Err() == nil {
				t.Fatalf("scale=%d should be rejected", c.bad)
			}
			// Error must call out the per-width type so the user knows
			// to upgrade rather than bisecting on scale.
			if !strings.Contains(b.Err().Error(), c.typeName) {
				t.Fatalf("error %q must mention %s", b.Err(), c.typeName)
			}
		})
		t.Run(c.name+"RejectsNegative", func(t *testing.T) {
			var b QwpBinds
			c.bind(&b, -1)
			if b.Err() == nil {
				t.Fatalf("scale=-1 should be rejected")
			}
		})
	}
}

func TestQwpBindsNullDecimalWithScale(t *testing.T) {
	// NullDecimalXBindWithScale must place the explicit scale byte
	// after the null bitmap so the server's setDecimal path picks up
	// the correct precision/type.
	cases := []struct {
		name  string
		bind  func(b *QwpBinds) *QwpBinds
		typ   qwpTypeCode
		scale byte
	}{
		{"Decimal64Scale5",
			func(b *QwpBinds) *QwpBinds { return b.NullDecimal64BindWithScale(0, 5) },
			qwpTypeDecimal64, 5},
		{"Decimal128Scale20",
			func(b *QwpBinds) *QwpBinds { return b.NullDecimal128BindWithScale(0, 20) },
			qwpTypeDecimal128, 20},
		{"Decimal256Scale50",
			func(b *QwpBinds) *QwpBinds { return b.NullDecimal256BindWithScale(0, 50) },
			qwpTypeDecimal256, 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var b QwpBinds
			c.bind(&b)
			var w byteBuf
			w.put(byte(c.typ), testBindNullFlag, testBindNullBitmap, c.scale)
			assertEncoded(t, &b, 1, w.b)
		})
	}
}

func TestQwpBindsNullGeohashWithPrecision(t *testing.T) {
	// NullGeohashBindWithPrecision must place the precision varint
	// after the null bitmap, matching the wire layout of a non-null
	// GEOHASH bind. The server reads the varint unconditionally.
	cases := []struct {
		name      string
		precision int
	}{
		{"Min", qwpGeohashMinBits},
		{"Mid", 30},
		{"Max", qwpGeohashMaxBits},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var b QwpBinds
			b.NullGeohashBindWithPrecision(0, c.precision)
			var w byteBuf
			w.put(byte(qwpTypeGeohash), testBindNullFlag, testBindNullBitmap)
			w.putVarint(uint64(c.precision))
			assertEncoded(t, &b, 1, w.b)
		})
	}
	t.Run("DefaultUsesMinBits", func(t *testing.T) {
		var b QwpBinds
		b.NullGeohashBind(0)
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNullFlag, testBindNullBitmap)
		w.putVarint(uint64(qwpGeohashMinBits))
		assertEncoded(t, &b, 1, w.b)
	})
	t.Run("RejectsZero", func(t *testing.T) {
		var b QwpBinds
		b.NullGeohashBindWithPrecision(0, 0)
		if b.Err() == nil {
			t.Fatalf("precision=0 must be rejected")
		}
	})
	t.Run("RejectsTooLarge", func(t *testing.T) {
		var b QwpBinds
		b.NullGeohashBindWithPrecision(0, qwpGeohashMaxBits+1)
		if b.Err() == nil {
			t.Fatalf("precision=%d must be rejected", qwpGeohashMaxBits+1)
		}
	})
}

func TestQwpBindsDouble(t *testing.T) {
	var b QwpBinds
	b.DoubleBind(0, 2.718281828)
	var w byteBuf
	w.put(byte(qwpTypeDouble), testBindNonNull)
	w.putU64(math.Float64bits(2.718281828))
	assertEncoded(t, &b, 1, w.b)

	b.reset()
	b.DoubleBind(0, math.NaN())
	var w2 byteBuf
	w2.put(byte(qwpTypeDouble), testBindNonNull)
	w2.putU64(math.Float64bits(math.NaN()))
	assertEncoded(t, &b, 1, w2.b)
}

func TestQwpBindsFloat(t *testing.T) {
	var b QwpBinds
	b.FloatBind(0, 3.14)
	var w byteBuf
	w.put(byte(qwpTypeFloat), testBindNonNull)
	w.putU32(math.Float32bits(3.14))
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsGeohashMinMax(t *testing.T) {
	t.Run("min", func(t *testing.T) {
		var b QwpBinds
		b.GeohashBind(0, 1, 1)
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNonNull)
		w.putVarint(1)
		w.put(0x01)
		assertEncoded(t, &b, 1, w.b)
	})
	t.Run("max", func(t *testing.T) {
		var b QwpBinds
		value := uint64(0x0FFF_FFFF_FFFF_FFFF)
		b.GeohashBind(0, value, 60)
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNonNull)
		w.putVarint(60)
		for i := 0; i < 8; i++ {
			w.put(byte(value >> (i * 8)))
		}
		assertEncoded(t, &b, 1, w.b)
	})
}

func TestQwpBindsGeohashMasksHighBits(t *testing.T) {
	// precisionBits=5 keeps only the low 5 bits; the wire byte should
	// be 0x1F regardless of the high bits in value.
	t.Run("subByte", func(t *testing.T) {
		var b QwpBinds
		b.GeohashBind(0, 0xFFFF_FFFF_FFFF_FFFF, 5)
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNonNull)
		w.putVarint(5)
		w.put(0x1F)
		assertEncoded(t, &b, 1, w.b)
	})
	// Non-byte-aligned across a byte boundary. precisionBits=12
	// emits 2 wire bytes; only the low 4 bits of the second byte
	// carry payload, the upper nibble must be zero. Mirrors Java's
	// boundary-bug regression: an unmasked value would leak the
	// shifted-in high bit into the second wire byte's upper nibble.
	t.Run("subNibbleAcrossByte_12", func(t *testing.T) {
		var b QwpBinds
		b.GeohashBind(0, 0xFFFF_FFFF_FFFF_FFFF, 12)
		masked := uint64(0x0FFF) // low 12 bits of all-ones
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNonNull)
		w.putVarint(12)
		w.put(byte(masked))
		w.put(byte(masked >> 8))
		assertEncoded(t, &b, 1, w.b)
	})
	// precisionBits=20 emits 3 wire bytes; only the low 4 bits of
	// the third byte carry payload.
	t.Run("nonByteAligned_20", func(t *testing.T) {
		var b QwpBinds
		b.GeohashBind(0, 0xFFFF_FFFF_FFFF_FFFF, 20)
		masked := uint64(0x0F_FFFF)
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNonNull)
		w.putVarint(20)
		for i := 0; i < 3; i++ {
			w.put(byte(masked >> (i * 8)))
		}
		assertEncoded(t, &b, 1, w.b)
	})
	// Byte-aligned mid-range. precisionBits=24 emits exactly 3 wire
	// bytes; every bit is payload.
	t.Run("byteAligned_24", func(t *testing.T) {
		var b QwpBinds
		b.GeohashBind(0, 0xFFFF_FFFF_FFFF_FFFF, 24)
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNonNull)
		w.putVarint(24)
		w.put(0xFF, 0xFF, 0xFF)
		assertEncoded(t, &b, 1, w.b)
	})
	// precisionBits=60 (max). Only the low 60 bits matter; the top
	// nibble of the highest wire byte must be zero.
	t.Run("maxPrecision", func(t *testing.T) {
		var b QwpBinds
		b.GeohashBind(0, 0xFFFF_FFFF_FFFF_FFFF, 60)
		var w byteBuf
		w.put(byte(qwpTypeGeohash), testBindNonNull)
		w.putVarint(60)
		masked := uint64(0x0FFF_FFFF_FFFF_FFFF)
		for i := 0; i < 8; i++ {
			w.put(byte(masked >> (i * 8)))
		}
		assertEncoded(t, &b, 1, w.b)
	})
}

func TestQwpBindsGeohashRejectsOutOfRange(t *testing.T) {
	cases := []int{0, 61, -1}
	for _, p := range cases {
		var b QwpBinds
		b.GeohashBind(0, 1, p)
		if b.Err() == nil {
			t.Fatalf("precision=%d should have been rejected", p)
		}
		if !strings.Contains(b.Err().Error(), "precision") {
			t.Fatalf("expected precision-related error, got: %v", b.Err())
		}
	}
}

func TestQwpBindsInt(t *testing.T) {
	var b QwpBinds
	minVal := int32(math.MinInt32)
	maxVal := int32(math.MaxInt32)
	b.IntBind(0, minVal).IntBind(1, 0).IntBind(2, maxVal)
	var w byteBuf
	w.put(byte(qwpTypeInt), testBindNonNull)
	w.putU32(uint32(minVal))
	w.put(byte(qwpTypeInt), testBindNonNull)
	w.putU32(0)
	w.put(byte(qwpTypeInt), testBindNonNull)
	w.putU32(uint32(maxVal))
	assertEncoded(t, &b, 3, w.b)
}

func TestQwpBindsLong(t *testing.T) {
	var b QwpBinds
	b.LongBind(0, 42)
	var w byteBuf
	w.put(byte(qwpTypeLong), testBindNonNull)
	w.putU64(42)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsLong256(t *testing.T) {
	var b QwpBinds
	b.Long256Bind(0, 0x1111111111111111, 0x2222222222222222,
		0x3333333333333333, 0x4444444444444444)
	var w byteBuf
	w.put(byte(qwpTypeLong256), testBindNonNull)
	w.putU64(0x1111111111111111)
	w.putU64(0x2222222222222222)
	w.putU64(0x3333333333333333)
	w.putU64(0x4444444444444444)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsMixedTypes(t *testing.T) {
	var b QwpBinds
	b.LongBind(0, 1234567890).
		VarcharBind(1, "hello").
		BooleanBind(2, true).
		DoubleBind(3, 1.5)

	var w byteBuf
	w.put(byte(qwpTypeLong), testBindNonNull)
	w.putU64(1234567890)
	w.put(byte(qwpTypeVarchar), testBindNonNull)
	w.putU32(0)
	w.putU32(5)
	w.put([]byte("hello")...)
	w.put(byte(qwpTypeBoolean), testBindNonNull, 1)
	w.put(byte(qwpTypeDouble), testBindNonNull)
	w.putU64(math.Float64bits(1.5))

	assertEncoded(t, &b, 4, w.b)
}

func TestQwpBindsNullExhaustive(t *testing.T) {
	var b QwpBinds
	b.NullBooleanBind(0).
		NullByteBind(1).
		NullShortBind(2).
		NullCharBind(3).
		NullIntBind(4).
		NullLongBind(5).
		NullFloatBind(6).
		NullDoubleBind(7).
		NullDateBind(8).
		NullTimestampMicrosBind(9).
		NullTimestampNanosBind(10).
		NullUuidBind(11).
		NullLong256Bind(12).
		NullGeohashBind(13).
		NullVarcharBind(14).
		NullDecimal64Bind(15).
		NullDecimal128Bind(16).
		NullDecimal256Bind(17)

	// Plain null types (no metadata after the bitmap byte).
	plainTypes := []qwpTypeCode{
		qwpTypeBoolean, qwpTypeByte, qwpTypeShort, qwpTypeChar,
		qwpTypeInt, qwpTypeLong, qwpTypeFloat, qwpTypeDouble,
		qwpTypeDate, qwpTypeTimestamp, qwpTypeTimestampNano,
		qwpTypeUuid, qwpTypeLong256, // 13 entries
	}

	var w byteBuf
	for _, tc := range plainTypes {
		w.put(byte(tc), testBindNullFlag, testBindNullBitmap)
	}
	// GEOHASH null carries the precision varint after the bitmap; the
	// server reads it unconditionally, even for null. Default precision
	// is qwpGeohashMinBits=1.
	w.put(byte(qwpTypeGeohash), testBindNullFlag, testBindNullBitmap)
	w.putVarint(uint64(qwpGeohashMinBits))
	// VARCHAR null is plain.
	w.put(byte(qwpTypeVarchar), testBindNullFlag, testBindNullBitmap)
	// DECIMAL64/128/256 null carry a 1-byte scale (default 0). The
	// server's setDecimal path reads this byte unconditionally; without
	// it the next bind's type code would be misread as a scale.
	w.put(byte(qwpTypeDecimal64), testBindNullFlag, testBindNullBitmap, 0x00)
	w.put(byte(qwpTypeDecimal128), testBindNullFlag, testBindNullBitmap, 0x00)
	w.put(byte(qwpTypeDecimal256), testBindNullFlag, testBindNullBitmap, 0x00)

	assertEncoded(t, &b, 18, w.b)
}

func TestQwpBindsShort(t *testing.T) {
	var b QwpBinds
	minVal := int16(math.MinInt16)
	maxVal := int16(math.MaxInt16)
	b.ShortBind(0, minVal).ShortBind(1, 0).ShortBind(2, maxVal)
	var w byteBuf
	w.put(byte(qwpTypeShort), testBindNonNull)
	w.putU16(uint16(minVal))
	w.put(byte(qwpTypeShort), testBindNonNull)
	w.putU16(0)
	w.put(byte(qwpTypeShort), testBindNonNull)
	w.putU16(uint16(maxVal))
	assertEncoded(t, &b, 3, w.b)
}

func TestQwpBindsTimestampMicros(t *testing.T) {
	var b QwpBinds
	b.TimestampMicrosBind(0, 1_700_000_000_000_000)
	var w byteBuf
	w.put(byte(qwpTypeTimestamp), testBindNonNull)
	w.putU64(uint64(int64(1_700_000_000_000_000)))
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsTimestampNanos(t *testing.T) {
	var b QwpBinds
	b.TimestampNanosBind(0, 1_700_000_000_000_000_000)
	var w byteBuf
	w.put(byte(qwpTypeTimestampNano), testBindNonNull)
	w.putU64(uint64(int64(1_700_000_000_000_000_000)))
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsUuid(t *testing.T) {
	var b QwpBinds
	b.UuidBind(0, 0x0BADF00DDEADBEEF, 0xFEEDFACECAFEBEEF)
	var w byteBuf
	// Wire order: lo first, then hi.
	w.put(byte(qwpTypeUuid), testBindNonNull)
	w.putU64(0xFEEDFACECAFEBEEF)
	w.putU64(0x0BADF00DDEADBEEF)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsVarcharAscii(t *testing.T) {
	var b QwpBinds
	b.VarcharBind(0, "hello")
	var w byteBuf
	w.put(byte(qwpTypeVarchar), testBindNonNull)
	w.putU32(0)
	w.putU32(5)
	w.put([]byte("hello")...)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsVarcharEmpty(t *testing.T) {
	var b QwpBinds
	b.VarcharBind(0, "")
	var w byteBuf
	w.put(byte(qwpTypeVarchar), testBindNonNull)
	w.putU32(0)
	w.putU32(0)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsVarcharUnicode(t *testing.T) {
	const value = "café"
	var b QwpBinds
	b.VarcharBind(0, value)
	utf8Bytes := []byte(value)
	var w byteBuf
	w.put(byte(qwpTypeVarchar), testBindNonNull)
	w.putU32(0)
	w.putU32(uint32(len(utf8Bytes)))
	w.put(utf8Bytes...)
	assertEncoded(t, &b, 1, w.b)
}

// --- Decimal bind from Decimal struct ------------------------------------

func TestQwpBindsDecimalAutoWidthFitsInt64(t *testing.T) {
	d := NewDecimalFromInt64(12345, 2)
	var b QwpBinds
	b.DecimalBind(0, d)
	// unscaled 12345 fits in 8 bytes -> DECIMAL64.
	var w byteBuf
	w.put(byte(qwpTypeDecimal64), testBindNonNull, 2)
	var signExtended [8]byte
	binary.LittleEndian.PutUint64(signExtended[:], uint64(int64(12345)))
	w.put(signExtended[:]...)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsDecimalAutoWidthNegativeInt64(t *testing.T) {
	d := NewDecimalFromInt64(-1, 0)
	var b QwpBinds
	b.DecimalBind(0, d)
	var w byteBuf
	w.put(byte(qwpTypeDecimal64), testBindNonNull, 0)
	var signExtended [8]byte
	negOne := int64(-1)
	binary.LittleEndian.PutUint64(signExtended[:], uint64(negOne))
	w.put(signExtended[:]...)
	assertEncoded(t, &b, 1, w.b)
}

func TestQwpBindsDecimalAutoWidthNull(t *testing.T) {
	nullDecimal, err := NewDecimalUnsafe(nil, 0)
	if err != nil {
		t.Fatalf("NewDecimalUnsafe: %v", err)
	}
	var b QwpBinds
	b.DecimalBind(0, nullDecimal)
	// NULL Decimal canonicalises to DECIMAL256 with scale 0; the scale
	// byte must be on the wire (the server reads it unconditionally).
	var w byteBuf
	w.put(byte(qwpTypeDecimal256), testBindNullFlag, testBindNullBitmap, 0x00)
	assertEncoded(t, &b, 1, w.b)
}

// --- Ordering and limit checks -------------------------------------------

func TestQwpBindsRejectsDuplicateIndex(t *testing.T) {
	var b QwpBinds
	b.LongBind(0, 1).LongBind(0, 2)
	if b.Err() == nil {
		t.Fatal("expected duplicate index to be rejected")
	}
	if !strings.Contains(b.Err().Error(), "out of order") {
		t.Fatalf("got error: %v", b.Err())
	}
}

func TestQwpBindsRejectsOutOfOrderIndex(t *testing.T) {
	var b QwpBinds
	b.LongBind(0, 1).LongBind(2, 3)
	if b.Err() == nil {
		t.Fatal("expected non-contiguous index to be rejected")
	}
}

func TestQwpBindsTooMany(t *testing.T) {
	var b QwpBinds
	for i := 0; i < qwpMaxBindsPerQuery; i++ {
		b.IntBind(i, int32(i))
	}
	if err := b.Err(); err != nil {
		t.Fatalf("filling %d binds should succeed: %v", qwpMaxBindsPerQuery, err)
	}
	b.IntBind(qwpMaxBindsPerQuery, 0)
	if b.Err() == nil {
		t.Fatalf("exceeding %d binds should fail", qwpMaxBindsPerQuery)
	}
	if !strings.Contains(b.Err().Error(), "too many") {
		t.Fatalf("got error: %v", b.Err())
	}
}

// --- Reset invariants ----------------------------------------------------

func TestQwpBindsResetPreservesBuffer(t *testing.T) {
	var b QwpBinds
	b.LongBind(0, 42).IntBind(1, 7)
	first := append([]byte(nil), b.bufferBytes()...)

	b.reset()
	if b.Count() != 0 || len(b.bufferBytes()) != 0 || b.Err() != nil {
		t.Fatalf("reset did not clear state")
	}

	b.LongBind(0, 42).IntBind(1, 7)
	if !bytes.Equal(first, b.bufferBytes()) {
		t.Fatalf("re-encoding after reset differs")
	}
}

func TestQwpBindsBufferGrowsBeyondDefault(t *testing.T) {
	var b QwpBinds
	big := strings.Repeat("x", 20_000)
	b.VarcharBind(0, big)
	if b.Err() != nil {
		t.Fatalf("unexpected error: %v", b.Err())
	}
	// type(1) + flag(1) + offset0(4) + len(4) + 20000 bytes = 20010
	if got, want := len(b.bufferBytes()), 1+1+4+4+20_000; got != want {
		t.Fatalf("encoded length=%d, want %d", got, want)
	}
}

// --- Fluent-chain short-circuit -----------------------------------------

// Once an error is latched, subsequent setters must not allocate or
// mutate the buffer. Matches the ILP / QWP ingress sender pattern.
func TestQwpBindsLatchedErrorShortCircuits(t *testing.T) {
	var b QwpBinds
	b.LongBind(0, 1).LongBind(5, 2) // out-of-order -> latches error at index=5
	bufBefore := append([]byte(nil), b.bufferBytes()...)
	b.LongBind(6, 3).IntBind(7, 4) // must be no-ops
	if !bytes.Equal(bufBefore, b.bufferBytes()) {
		t.Fatalf("bind calls after latched error mutated the buffer")
	}
}
