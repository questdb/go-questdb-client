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

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestQwpColumnBufferFixedWidth(t *testing.T) {
	t.Run("Byte", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeByte, false)
		c.addByte(42)
		c.addByte(-1)

		expected := []byte{0x2A, 0xFF}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
	})

	t.Run("Short", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeShort, false)
		c.addShort(0x0102)
		// 0x0102 LE = [0x02, 0x01]
		expected := []byte{0x02, 0x01}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})

	t.Run("Int32", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeInt, false)
		c.addInt32(0x01020304)
		// 0x01020304 LE = [0x04, 0x03, 0x02, 0x01]
		expected := []byte{0x04, 0x03, 0x02, 0x01}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})

	t.Run("Long", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, false)
		c.addLong(0x0102030405060708)
		expected := []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})

	t.Run("Float32", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeFloat, false)
		c.addFloat32(1.5)
		// float32(1.5) = 0x3FC00000 → LE = [0x00, 0x00, 0xC0, 0x3F]
		expected := make([]byte, 4)
		binary.LittleEndian.PutUint32(expected, math.Float32bits(1.5))
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})

	t.Run("Double", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDouble, false)
		c.addDouble(1.5)
		// float64(1.5) = 0x3FF8000000000000 → LE
		expected := make([]byte, 8)
		binary.LittleEndian.PutUint64(expected, math.Float64bits(1.5))
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeTimestamp, false)
		c.addTimestamp(1234567890)
		expected := make([]byte, 8)
		binary.LittleEndian.PutUint64(expected, uint64(1234567890))
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})

	t.Run("Char", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeChar, false)
		c.addChar('A')
		// 'A' = 0x0041 → LE = [0x41, 0x00]
		expected := []byte{0x41, 0x00}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})

	t.Run("CharNonBMP", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeChar, false)
		// Euro sign U+20AC → uint16 = 0x20AC → LE = [0xAC, 0x20]
		c.addChar('€')
		expected := []byte{0xAC, 0x20}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})
}

func TestQwpColumnBufferUuid(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeUuid, false)
	c.addUuid(0xAABBCCDDEEFF0011, 0x1122334455667788)

	// Wire order: lo LE bytes first, then hi LE bytes.
	expected := make([]byte, 16)
	binary.LittleEndian.PutUint64(expected[0:8], 0x1122334455667788) // lo
	binary.LittleEndian.PutUint64(expected[8:16], 0xAABBCCDDEEFF0011) // hi
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
	if c.rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", c.rowCount)
	}
}

func TestQwpColumnBufferLong256(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeLong256, false)
	c.addLong256(1, 2, 3, 4)

	expected := make([]byte, 32)
	binary.LittleEndian.PutUint64(expected[0:8], 1)
	binary.LittleEndian.PutUint64(expected[8:16], 2)
	binary.LittleEndian.PutUint64(expected[16:24], 3)
	binary.LittleEndian.PutUint64(expected[24:32], 4)
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
	if c.rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", c.rowCount)
	}
}

func TestQwpColumnBufferBool(t *testing.T) {
	t.Run("ThreeValues", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeBoolean, false)
		c.addBool(true)
		c.addBool(false)
		c.addBool(true)

		// bits: 1,0,1 → byte 0b00000101 = 0x05
		if !bytes.Equal(c.boolData, []byte{0x05}) {
			t.Fatalf("boolData = %x, want [05]", c.boolData)
		}
		if c.rowCount != 3 {
			t.Fatalf("rowCount = %d, want 3", c.rowCount)
		}
	})

	t.Run("NineValues", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeBoolean, false)
		// 8 values fill first byte, 9th goes to second byte.
		vals := []bool{true, false, true, true, false, false, true, false, true}
		for _, v := range vals {
			c.addBool(v)
		}

		// Byte 0: bits 0-7 = 1,0,1,1,0,0,1,0 = 0b01001101 = 0x4D
		// Byte 1: bit 0    = 1                 = 0b00000001 = 0x01
		expected := []byte{0x4D, 0x01}
		if !bytes.Equal(c.boolData, expected) {
			t.Fatalf("boolData = %x, want %x", c.boolData, expected)
		}
		if c.rowCount != 9 {
			t.Fatalf("rowCount = %d, want 9", c.rowCount)
		}
	})

	t.Run("AllFalse", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeBoolean, false)
		c.addBool(false)
		c.addBool(false)
		c.addBool(false)

		if !bytes.Equal(c.boolData, []byte{0x00}) {
			t.Fatalf("boolData = %x, want [00]", c.boolData)
		}
	})
}

func TestQwpColumnBufferString(t *testing.T) {
	t.Run("TwoStrings", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeString, false)
		c.addString("hello")
		c.addString("world")

		if string(c.strData) != "helloworld" {
			t.Fatalf("strData = %q, want %q", c.strData, "helloworld")
		}
		expectedOffsets := []uint32{0, 5, 10}
		if len(c.strOffsets) != len(expectedOffsets) {
			t.Fatalf("strOffsets len = %d, want %d", len(c.strOffsets), len(expectedOffsets))
		}
		for i, off := range expectedOffsets {
			if c.strOffsets[i] != off {
				t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
			}
		}
		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
	})

	t.Run("EmptyString", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeString, false)
		c.addString("")
		c.addString("abc")

		if string(c.strData) != "abc" {
			t.Fatalf("strData = %q, want %q", c.strData, "abc")
		}
		expectedOffsets := []uint32{0, 0, 3}
		for i, off := range expectedOffsets {
			if c.strOffsets[i] != off {
				t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
			}
		}
	})

	t.Run("Varchar", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeVarchar, false)
		c.addString("test")

		if string(c.strData) != "test" {
			t.Fatalf("strData = %q, want %q", c.strData, "test")
		}
		expectedOffsets := []uint32{0, 4}
		for i, off := range expectedOffsets {
			if c.strOffsets[i] != off {
				t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
			}
		}
	})
}

func TestQwpColumnBufferSymbolID(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeSymbol, false)
	c.addSymbolID(0)
	c.addSymbolID(1)
	c.addSymbolID(42)

	if len(c.symbolIDs) != 3 {
		t.Fatalf("symbolIDs len = %d, want 3", len(c.symbolIDs))
	}
	expected := []int32{0, 1, 42}
	for i, id := range expected {
		if c.symbolIDs[i] != id {
			t.Fatalf("symbolIDs[%d] = %d, want %d", i, c.symbolIDs[i], id)
		}
	}
	if c.rowCount != 3 {
		t.Fatalf("rowCount = %d, want 3", c.rowCount)
	}
}

func TestQwpColumnBufferAddNull(t *testing.T) {
	t.Run("LongNonNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, false)
		c.addNull()

		// Sentinel: MinInt64 LE = [0x00..0x80]
		expected := make([]byte, 8)
		binary.LittleEndian.PutUint64(expected, qwpLongNull)
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
		if len(c.nullBitmap) != 0 {
			t.Fatalf("nullBitmap should be empty for non-nullable, got %x", c.nullBitmap)
		}
		if c.nullCount != 0 {
			t.Fatalf("nullCount = %d, want 0 for non-nullable", c.nullCount)
		}
	})

	t.Run("LongNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, true)
		c.addNull()

		expected := make([]byte, 8)
		binary.LittleEndian.PutUint64(expected, qwpLongNull)
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
		// Bitmap: row 0 is null → bit 0 set → [0x01]
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
	})

	t.Run("DoubleNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDouble, true)
		c.addNull()

		// Verify the value reads back as NaN.
		v := math.Float64frombits(binary.LittleEndian.Uint64(c.fixedData))
		if !math.IsNaN(v) {
			t.Fatalf("null double sentinel should be NaN, got %v", v)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("FloatNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeFloat, true)
		c.addNull()

		v := math.Float32frombits(binary.LittleEndian.Uint32(c.fixedData))
		if !math.IsNaN(float64(v)) {
			t.Fatalf("null float sentinel should be NaN, got %v", v)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("IntNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeInt, true)
		c.addNull()

		if !bytes.Equal(c.fixedData, []byte{0, 0, 0, 0}) {
			t.Fatalf("fixedData = %x, want [00000000]", c.fixedData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("ByteNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeByte, true)
		c.addNull()

		if !bytes.Equal(c.fixedData, []byte{0}) {
			t.Fatalf("fixedData = %x, want [00]", c.fixedData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("BoolNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeBoolean, true)
		c.addNull()

		// Bit 0 should be 0 (false sentinel).
		if !bytes.Equal(c.boolData, []byte{0x00}) {
			t.Fatalf("boolData = %x, want [00]", c.boolData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("StringNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeString, true)
		c.addNull()

		// Offset repeats: [0, 0] → empty string sentinel.
		expectedOffsets := []uint32{0, 0}
		for i, off := range expectedOffsets {
			if c.strOffsets[i] != off {
				t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
			}
		}
		if len(c.strData) != 0 {
			t.Fatalf("strData should be empty, got %q", c.strData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("SymbolNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeSymbol, true)
		c.addNull()

		if len(c.symbolIDs) != 1 || c.symbolIDs[0] != -1 {
			t.Fatalf("symbolIDs = %v, want [-1]", c.symbolIDs)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("UuidNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeUuid, true)
		c.addNull()

		// Two MinInt64 LE values = 16 bytes.
		expected := make([]byte, 16)
		binary.LittleEndian.PutUint64(expected[0:8], qwpLongNull)
		binary.LittleEndian.PutUint64(expected[8:16], qwpLongNull)
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("Long256Nullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong256, true)
		c.addNull()

		// Four MinInt64 LE values = 32 bytes.
		expected := make([]byte, 32)
		for i := 0; i < 4; i++ {
			binary.LittleEndian.PutUint64(expected[i*8:(i+1)*8], qwpLongNull)
		}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})
}

func TestQwpColumnBufferNullBitmapPattern(t *testing.T) {
	// Test interleaved null and non-null values to verify bitmap
	// bit positioning.
	c := newQwpColumnBuffer("col", qwpTypeLong, true)
	c.addLong(100)  // row 0: non-null
	c.addNull()     // row 1: null
	c.addLong(200)  // row 2: non-null
	c.addNull()     // row 3: null
	c.addNull()     // row 4: null
	c.addLong(300)  // row 5: non-null

	if c.rowCount != 6 {
		t.Fatalf("rowCount = %d, want 6", c.rowCount)
	}
	if c.nullCount != 3 {
		t.Fatalf("nullCount = %d, want 3", c.nullCount)
	}

	// Bitmap: rows 1,3,4 are null → bits 1,3,4 set.
	// byte 0 = 0b00011010 = 0x1A
	if !bytes.Equal(c.nullBitmap, []byte{0x1A}) {
		t.Fatalf("nullBitmap = %x, want [1A]", c.nullBitmap)
	}

	// fixedData: 6 rows × 8 bytes = 48 bytes.
	if len(c.fixedData) != 48 {
		t.Fatalf("fixedData len = %d, want 48", len(c.fixedData))
	}

	// Verify non-null values are at correct offsets.
	v0 := int64(binary.LittleEndian.Uint64(c.fixedData[0:8]))
	if v0 != 100 {
		t.Fatalf("row 0 = %d, want 100", v0)
	}
	v2 := int64(binary.LittleEndian.Uint64(c.fixedData[16:24]))
	if v2 != 200 {
		t.Fatalf("row 2 = %d, want 200", v2)
	}
	v5 := int64(binary.LittleEndian.Uint64(c.fixedData[40:48]))
	if v5 != 300 {
		t.Fatalf("row 5 = %d, want 300", v5)
	}

	// Verify null sentinels (MinInt64) at null row offsets.
	for _, rowIdx := range []int{1, 3, 4} {
		off := rowIdx * 8
		v := int64(binary.LittleEndian.Uint64(c.fixedData[off : off+8]))
		if v != math.MinInt64 {
			t.Fatalf("null sentinel at row %d = %d, want MinInt64", rowIdx, v)
		}
	}
}

func TestQwpColumnBufferNullBitmapMultipleBytes(t *testing.T) {
	// Verify bitmap grows across byte boundaries.
	c := newQwpColumnBuffer("col", qwpTypeByte, true)

	// Add 10 rows: nulls at rows 0, 8, 9.
	c.addNull()    // row 0
	for i := 1; i < 8; i++ {
		c.addByte(int8(i))
	}
	c.addNull()    // row 8
	c.addNull()    // row 9

	if c.rowCount != 10 {
		t.Fatalf("rowCount = %d, want 10", c.rowCount)
	}
	if c.nullCount != 3 {
		t.Fatalf("nullCount = %d, want 3", c.nullCount)
	}
	if c.nullBitmapLen() != 2 {
		t.Fatalf("nullBitmapLen = %d, want 2", c.nullBitmapLen())
	}

	// Byte 0: bit 0 set → 0x01
	// Byte 1: bits 0,1 set (rows 8,9) → 0x03
	expected := []byte{0x01, 0x03}
	if !bytes.Equal(c.nullBitmap, expected) {
		t.Fatalf("nullBitmap = %x, want %x", c.nullBitmap, expected)
	}
}

func TestQwpColumnBufferNullStringInterleaved(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeString, true)
	c.addString("hello") // row 0
	c.addNull()          // row 1
	c.addString("world") // row 2

	if string(c.strData) != "helloworld" {
		t.Fatalf("strData = %q, want %q", c.strData, "helloworld")
	}

	// Offsets: [0, 5, 5, 10] — null row repeats offset.
	expectedOffsets := []uint32{0, 5, 5, 10}
	if len(c.strOffsets) != len(expectedOffsets) {
		t.Fatalf("strOffsets len = %d, want %d", len(c.strOffsets), len(expectedOffsets))
	}
	for i, off := range expectedOffsets {
		if c.strOffsets[i] != off {
			t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
		}
	}

	// Bitmap: row 1 null → bit 1 set → 0x02
	if !bytes.Equal(c.nullBitmap, []byte{0x02}) {
		t.Fatalf("nullBitmap = %x, want [02]", c.nullBitmap)
	}
}

func TestQwpColumnBufferWireTypeCode(t *testing.T) {
	t.Run("NonNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, false)
		if c.wireTypeCode() != 0x05 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x05", c.wireTypeCode())
		}
	})

	t.Run("Nullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, true)
		// 0x05 | 0x80 = 0x85
		if c.wireTypeCode() != 0x85 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x85", c.wireTypeCode())
		}
	})
}

func TestQwpColumnBufferReset(t *testing.T) {
	t.Run("FixedWidth", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, true)
		c.addLong(42)
		c.addNull()
		c.addLong(100)

		origCap := cap(c.fixedData)
		c.reset()

		if c.rowCount != 0 {
			t.Fatalf("rowCount = %d, want 0", c.rowCount)
		}
		if c.nullCount != 0 {
			t.Fatalf("nullCount = %d, want 0", c.nullCount)
		}
		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData len = %d, want 0", len(c.fixedData))
		}
		if len(c.nullBitmap) != 0 {
			t.Fatalf("nullBitmap len = %d, want 0", len(c.nullBitmap))
		}
		// Capacity should be retained.
		if cap(c.fixedData) != origCap {
			t.Fatalf("fixedData cap changed from %d to %d", origCap, cap(c.fixedData))
		}
	})

	t.Run("String", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeString, false)
		c.addString("hello")
		c.addString("world")

		c.reset()

		if c.rowCount != 0 {
			t.Fatalf("rowCount = %d, want 0", c.rowCount)
		}
		if len(c.strData) != 0 {
			t.Fatalf("strData len = %d, want 0", len(c.strData))
		}
		// strOffsets should have initial [0].
		if len(c.strOffsets) != 1 || c.strOffsets[0] != 0 {
			t.Fatalf("strOffsets = %v, want [0]", c.strOffsets)
		}
	})

	t.Run("Symbol", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeSymbol, false)
		c.addSymbolID(5)
		c.addSymbolID(10)

		c.reset()

		if c.rowCount != 0 {
			t.Fatalf("rowCount = %d, want 0", c.rowCount)
		}
		if len(c.symbolIDs) != 0 {
			t.Fatalf("symbolIDs len = %d, want 0", len(c.symbolIDs))
		}
	})

	t.Run("Bool", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeBoolean, false)
		c.addBool(true)
		c.addBool(false)

		c.reset()

		if c.rowCount != 0 {
			t.Fatalf("rowCount = %d, want 0", c.rowCount)
		}
		if len(c.boolData) != 0 {
			t.Fatalf("boolData len = %d, want 0", len(c.boolData))
		}
	})

	t.Run("ResetAndReuse", func(t *testing.T) {
		// Verify that after reset, new data can be added correctly.
		c := newQwpColumnBuffer("col", qwpTypeString, true)
		c.addString("first")
		c.addNull()

		c.reset()

		c.addString("second")
		c.addString("third")

		if string(c.strData) != "secondthird" {
			t.Fatalf("strData = %q, want %q", c.strData, "secondthird")
		}
		expectedOffsets := []uint32{0, 6, 11}
		for i, off := range expectedOffsets {
			if c.strOffsets[i] != off {
				t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
			}
		}
		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
		if c.nullCount != 0 {
			t.Fatalf("nullCount = %d, want 0", c.nullCount)
		}
	})
}

func TestQwpColumnBufferMultipleRows(t *testing.T) {
	// Verify correct stride for multiple fixed-width rows.
	c := newQwpColumnBuffer("col", qwpTypeInt, false)
	c.addInt32(1)
	c.addInt32(2)
	c.addInt32(3)

	if len(c.fixedData) != 12 {
		t.Fatalf("fixedData len = %d, want 12", len(c.fixedData))
	}

	expected := make([]byte, 12)
	binary.LittleEndian.PutUint32(expected[0:4], 1)
	binary.LittleEndian.PutUint32(expected[4:8], 2)
	binary.LittleEndian.PutUint32(expected[8:12], 3)
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
}

func TestQwpColumnBufferConstructor(t *testing.T) {
	t.Run("FixedType", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDouble, true)
		if c.name != "price" {
			t.Fatalf("name = %q, want %q", c.name, "price")
		}
		if c.typeCode != qwpTypeDouble {
			t.Fatalf("typeCode = 0x%02X, want 0x%02X", c.typeCode, qwpTypeDouble)
		}
		if !c.nullable {
			t.Fatal("nullable should be true")
		}
		if c.fixedSize != 8 {
			t.Fatalf("fixedSize = %d, want 8", c.fixedSize)
		}
		if c.scale != -1 {
			t.Fatalf("scale = %d, want -1", c.scale)
		}
		if c.geohashPrecision != -1 {
			t.Fatalf("geohashPrecision = %d, want -1", c.geohashPrecision)
		}
	})

	t.Run("StringType", func(t *testing.T) {
		c := newQwpColumnBuffer("msg", qwpTypeString, false)
		if c.fixedSize != -1 {
			t.Fatalf("fixedSize = %d, want -1", c.fixedSize)
		}
		// strOffsets should be initialized with [0].
		if len(c.strOffsets) != 1 || c.strOffsets[0] != 0 {
			t.Fatalf("strOffsets = %v, want [0]", c.strOffsets)
		}
	})

	t.Run("BoolType", func(t *testing.T) {
		c := newQwpColumnBuffer("flag", qwpTypeBoolean, false)
		if c.fixedSize != 0 {
			t.Fatalf("fixedSize = %d, want 0 (bit-packed)", c.fixedSize)
		}
	})
}

func TestQwpColumnBufferNegativeValues(t *testing.T) {
	t.Run("NegativeByte", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeByte, false)
		c.addByte(-128)
		if c.fixedData[0] != 0x80 {
			t.Fatalf("fixedData = %x, want [80]", c.fixedData)
		}
	})

	t.Run("NegativeShort", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeShort, false)
		c.addShort(-1)
		// -1 as uint16 = 0xFFFF → LE = [0xFF, 0xFF]
		if !bytes.Equal(c.fixedData, []byte{0xFF, 0xFF}) {
			t.Fatalf("fixedData = %x, want [FFFF]", c.fixedData)
		}
	})

	t.Run("NegativeInt32", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeInt, false)
		c.addInt32(-1)
		if !bytes.Equal(c.fixedData, []byte{0xFF, 0xFF, 0xFF, 0xFF}) {
			t.Fatalf("fixedData = %x, want [FFFFFFFF]", c.fixedData)
		}
	})

	t.Run("NegativeLong", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, false)
		c.addLong(-1)
		expected := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		if !bytes.Equal(c.fixedData, expected) {
			t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
		}
	})
}

// --- truncateTo tests ---

func TestQwpColumnBufferTruncateTo(t *testing.T) {
	t.Run("FixedWidth", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, false)
		c.addLong(100)
		c.addLong(200)
		c.addLong(300)

		c.truncateTo(2)

		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
		if len(c.fixedData) != 16 {
			t.Fatalf("fixedData len = %d, want 16", len(c.fixedData))
		}
		v := int64(binary.LittleEndian.Uint64(c.fixedData[8:16]))
		if v != 200 {
			t.Fatalf("row 1 = %d, want 200", v)
		}
	})

	t.Run("Bool", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeBoolean, false)
		for i := 0; i < 10; i++ {
			c.addBool(i%2 == 0)
		}

		c.truncateTo(3)

		if c.rowCount != 3 {
			t.Fatalf("rowCount = %d, want 3", c.rowCount)
		}
		// Bits 0,1,2 → true,false,true → 0b00000101 = 0x05
		if len(c.boolData) != 1 || c.boolData[0] != 0x05 {
			t.Fatalf("boolData = %x, want [05]", c.boolData)
		}
	})

	t.Run("String", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeString, false)
		c.addString("hello")
		c.addString("world")
		c.addString("foo")

		c.truncateTo(2)

		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
		if string(c.strData) != "helloworld" {
			t.Fatalf("strData = %q, want %q", c.strData, "helloworld")
		}
		expectedOff := []uint32{0, 5, 10}
		if len(c.strOffsets) != len(expectedOff) {
			t.Fatalf("strOffsets len = %d, want %d", len(c.strOffsets), len(expectedOff))
		}
		for i, off := range expectedOff {
			if c.strOffsets[i] != off {
				t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
			}
		}
	})

	t.Run("Symbol", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeSymbol, false)
		c.addSymbolID(0)
		c.addSymbolID(1)
		c.addSymbolID(2)

		c.truncateTo(1)

		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
		if len(c.symbolIDs) != 1 || c.symbolIDs[0] != 0 {
			t.Fatalf("symbolIDs = %v, want [0]", c.symbolIDs)
		}
	})

	t.Run("NullableWithBitmap", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, true)
		c.addLong(100)
		c.addNull()
		c.addLong(200)
		c.addNull()

		c.truncateTo(2)

		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
		// Bitmap: only row 1 null → bit 1 set → 0x02
		if !bytes.Equal(c.nullBitmap, []byte{0x02}) {
			t.Fatalf("nullBitmap = %x, want [02]", c.nullBitmap)
		}
	})

	t.Run("NoOp", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong, false)
		c.addLong(42)

		c.truncateTo(5) // n >= rowCount, should be no-op

		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
	})
}

// --- qwpTableBuffer tests ---

func TestQwpTableBufferBasic(t *testing.T) {
	tb := newQwpTableBuffer("test_table")
	if tb.tableName != "test_table" {
		t.Fatalf("tableName = %q, want %q", tb.tableName, "test_table")
	}
	if tb.rowCount != 0 {
		t.Fatalf("rowCount = %d, want 0", tb.rowCount)
	}
	if len(tb.columns) != 0 {
		t.Fatalf("columns len = %d, want 0", len(tb.columns))
	}
}

func TestQwpTableBufferGetOrCreateColumn(t *testing.T) {
	t.Run("CreateNew", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col, err := tb.getOrCreateColumn("price", qwpTypeDouble, false)
		if err != nil {
			t.Fatal(err)
		}
		if col.name != "price" {
			t.Fatalf("name = %q, want %q", col.name, "price")
		}
		if col.typeCode != qwpTypeDouble {
			t.Fatalf("typeCode = %d, want %d", col.typeCode, qwpTypeDouble)
		}
		if len(tb.columns) != 1 {
			t.Fatalf("columns len = %d, want 1", len(tb.columns))
		}
	})

	t.Run("GetExisting", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col1, _ := tb.getOrCreateColumn("price", qwpTypeDouble, false)
		col1.addDouble(1.5)
		tb.commitRow()

		col2, err := tb.getOrCreateColumn("price", qwpTypeDouble, false)
		if err != nil {
			t.Fatal(err)
		}
		if col2 != col1 {
			t.Fatal("should return same column pointer")
		}
		if len(tb.columns) != 1 {
			t.Fatal("should not create duplicate column")
		}
	})

	t.Run("TypeConflict", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		_, err := tb.getOrCreateColumn("price", qwpTypeDouble, false)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tb.getOrCreateColumn("price", qwpTypeLong, false)
		if err == nil {
			t.Fatal("expected type conflict error")
		}
	})

	t.Run("DuplicateColumnInRow", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col, _ := tb.getOrCreateColumn("price", qwpTypeDouble, false)
		col.addDouble(1.5)

		_, err := tb.getOrCreateColumn("price", qwpTypeDouble, false)
		if err == nil {
			t.Fatal("expected duplicate column error")
		}
	})

	t.Run("BackfillOnCreate", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		// Commit 2 rows with only "a".
		col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(10)
		tb.commitRow()

		col, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(20)
		tb.commitRow()

		// Now create column "b" — should be backfilled with 2 nulls.
		colB, _ := tb.getOrCreateColumn("b", qwpTypeLong, true)
		if colB.rowCount != 2 {
			t.Fatalf("new column rowCount = %d, want 2 (backfilled)", colB.rowCount)
		}
		if colB.nullCount != 2 {
			t.Fatalf("new column nullCount = %d, want 2", colB.nullCount)
		}
	})
}

func TestQwpTableBufferCommitRow(t *testing.T) {
	t.Run("GapFilling", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		// Row 0: set both columns.
		colA, _ := tb.getOrCreateColumn("a", qwpTypeLong, true)
		colA.addLong(100)
		colB, _ := tb.getOrCreateColumn("b", qwpTypeDouble, true)
		colB.addDouble(1.5)
		tb.commitRow()

		// Row 1: set only column "a" — column "b" should be gap-filled.
		colA, _ = tb.getOrCreateColumn("a", qwpTypeLong, true)
		colA.addLong(200)
		tb.commitRow()

		if tb.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", tb.rowCount)
		}
		if colB.rowCount != 2 {
			t.Fatalf("colB rowCount = %d, want 2", colB.rowCount)
		}
		if colB.nullCount != 1 {
			t.Fatalf("colB nullCount = %d, want 1", colB.nullCount)
		}
	})

	t.Run("MultipleGaps", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		// Create 3 columns. Set different subsets each row.
		colA, _ := tb.getOrCreateColumn("a", qwpTypeLong, true)
		colA.addLong(1)
		colB, _ := tb.getOrCreateColumn("b", qwpTypeLong, true)
		colB.addLong(2)
		colC, _ := tb.getOrCreateColumn("c", qwpTypeLong, true)
		colC.addLong(3)
		tb.commitRow()

		// Row 1: only set "b".
		colB, _ = tb.getOrCreateColumn("b", qwpTypeLong, true)
		colB.addLong(20)
		tb.commitRow()

		// "a" and "c" should have null at row 1.
		if colA.nullCount != 1 {
			t.Fatalf("colA nullCount = %d, want 1", colA.nullCount)
		}
		if colC.nullCount != 1 {
			t.Fatalf("colC nullCount = %d, want 1", colC.nullCount)
		}
		if colB.nullCount != 0 {
			t.Fatalf("colB nullCount = %d, want 0", colB.nullCount)
		}
	})
}

func TestQwpTableBufferCancelRow(t *testing.T) {
	t.Run("RollbackValues", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		// Commit row 0.
		col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(100)
		tb.commitRow()

		// Start row 1, then cancel.
		col, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(200)
		tb.cancelRow()

		if tb.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", tb.rowCount)
		}
		if col.rowCount != 1 {
			t.Fatalf("col rowCount = %d, want 1", col.rowCount)
		}
		// Verify the column only has the first value.
		v := int64(binary.LittleEndian.Uint64(col.fixedData[0:8]))
		if v != 100 {
			t.Fatalf("row 0 = %d, want 100", v)
		}
	})

	t.Run("RemoveNewColumns", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		// Commit row 0 with column "a".
		col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(100)
		tb.commitRow()

		// Start row 1, add column "b" (new), then cancel.
		col, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(200)
		_, _ = tb.getOrCreateColumn("b", qwpTypeDouble, false)
		tb.cancelRow()

		// Column "b" should be removed.
		if len(tb.columns) != 1 {
			t.Fatalf("columns len = %d, want 1", len(tb.columns))
		}
		if _, exists := tb.columnIndex["b"]; exists {
			t.Fatal("column 'b' should not exist after cancel")
		}
	})

	t.Run("CancelWithStringColumn", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col, _ := tb.getOrCreateColumn("msg", qwpTypeString, false)
		col.addString("hello")
		tb.commitRow()

		col, _ = tb.getOrCreateColumn("msg", qwpTypeString, false)
		col.addString("world")
		tb.cancelRow()

		if col.rowCount != 1 {
			t.Fatalf("col rowCount = %d, want 1", col.rowCount)
		}
		if string(col.strData) != "hello" {
			t.Fatalf("strData = %q, want %q", col.strData, "hello")
		}
	})

	t.Run("CancelBoolColumn", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col, _ := tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
		col.addBool(true)
		tb.commitRow()

		col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
		col.addBool(false)
		tb.cancelRow()

		if col.rowCount != 1 {
			t.Fatalf("col rowCount = %d, want 1", col.rowCount)
		}
		// Only bit 0 (true) should remain.
		if col.boolData[0] != 0x01 {
			t.Fatalf("boolData = %x, want [01]", col.boolData)
		}
	})

	t.Run("CancelWithNoCommittedRows", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(42)
		tb.cancelRow()

		// Everything should be rolled back.
		if tb.rowCount != 0 {
			t.Fatalf("rowCount = %d, want 0", tb.rowCount)
		}
		// Column "a" was created during this row, so it should be removed.
		if len(tb.columns) != 0 {
			t.Fatalf("columns len = %d, want 0", len(tb.columns))
		}
	})
}

func TestQwpTableBufferReset(t *testing.T) {
	tb := newQwpTableBuffer("t")

	col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
	col.addLong(100)
	tb.commitRow()
	col, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
	col.addLong(200)
	tb.commitRow()

	tb.reset()

	if tb.rowCount != 0 {
		t.Fatalf("rowCount = %d, want 0", tb.rowCount)
	}
	// Columns still exist but with no data.
	if len(tb.columns) != 1 {
		t.Fatalf("columns len = %d, want 1", len(tb.columns))
	}
	if tb.columns[0].rowCount != 0 {
		t.Fatalf("col rowCount = %d, want 0", tb.columns[0].rowCount)
	}
}

func TestQwpTableBufferSchemaHash(t *testing.T) {
	t.Run("Deterministic", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col, _ := tb.getOrCreateColumn("price", qwpTypeDouble, false)
		col.addDouble(1.5)
		tb.commitRow()

		h1 := tb.getSchemaHash()
		h2 := tb.getSchemaHash()
		if h1 != h2 {
			t.Fatalf("schema hash not deterministic: %d vs %d", h1, h2)
		}
	})

	t.Run("ChangeOnNewColumn", func(t *testing.T) {
		tb := newQwpTableBuffer("t")

		col, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(1)
		tb.commitRow()
		h1 := tb.getSchemaHash()

		colB, _ := tb.getOrCreateColumn("b", qwpTypeDouble, false)
		colB.addDouble(2.0)
		col, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
		col.addLong(2)
		tb.commitRow()
		h2 := tb.getSchemaHash()

		if h1 == h2 {
			t.Fatal("schema hash should change when a new column is added")
		}
	})

	t.Run("NullableAffectsHash", func(t *testing.T) {
		tb1 := newQwpTableBuffer("t")
		col, _ := tb1.getOrCreateColumn("x", qwpTypeLong, false)
		col.addLong(1)
		tb1.commitRow()

		tb2 := newQwpTableBuffer("t")
		col, _ = tb2.getOrCreateColumn("x", qwpTypeLong, true)
		col.addLong(1)
		tb2.commitRow()

		if tb1.getSchemaHash() == tb2.getSchemaHash() {
			t.Fatal("nullable flag should affect schema hash")
		}
	})

	t.Run("XXHash64EmptyInput", func(t *testing.T) {
		// Verify xxhash64 matches canonical test vector.
		h := xxhash64([]byte{}, 0)
		if h != 0xEF46DB3751D8E999 {
			t.Fatalf("xxhash64(empty, 0) = %016X, want EF46DB3751D8E999", h)
		}
	})

	t.Run("KnownValue", func(t *testing.T) {
		// Schema: col "ts" (TIMESTAMP, non-nullable)
		// Input bytes: "ts" + 0x0A = [0x74, 0x73, 0x0A]
		tb := newQwpTableBuffer("t")
		col, _ := tb.getOrCreateColumn("ts", qwpTypeTimestamp, false)
		col.addTimestamp(0)
		tb.commitRow()

		h := tb.getSchemaHash()
		expected := int64(xxhash64([]byte{0x74, 0x73, 0x0A}, 0))
		if h != expected {
			t.Fatalf("schema hash = %d, want %d", h, expected)
		}
	})
}
