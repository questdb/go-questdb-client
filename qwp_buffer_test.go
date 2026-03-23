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
	"math/big"
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

		// Nullable null: no sentinel data, only bitmap.
		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
		}
		if c.valueCount() != 0 {
			t.Fatalf("valueCount = %d, want 0", c.valueCount())
		}
		// Bitmap: row 0 is null → bit 0 set → [0x01]
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
	})

	t.Run("DoubleNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDouble, true)
		c.addNull()

		// Nullable null: no sentinel data.
		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("FloatNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeFloat, true)
		c.addNull()

		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("IntNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeInt, true)
		c.addNull()

		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("ByteNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeByte, true)
		c.addNull()

		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("BoolNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeBoolean, true)
		c.addNull()

		// Nullable null: no data appended to boolData.
		if len(c.boolData) != 0 {
			t.Fatalf("boolData should be empty for nullable null, got %x", c.boolData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("StringNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeString, true)
		c.addNull()

		// Nullable null: only the initial offset [0], no extra offset.
		if len(c.strOffsets) != 1 || c.strOffsets[0] != 0 {
			t.Fatalf("strOffsets = %v, want [0]", c.strOffsets)
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

		// Nullable null: no symbol ID appended.
		if len(c.symbolIDs) != 0 {
			t.Fatalf("symbolIDs should be empty for nullable null, got %v", c.symbolIDs)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("UuidNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeUuid, true)
		c.addNull()

		// Nullable null: no sentinel data.
		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}
	})

	t.Run("Long256Nullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLong256, true)
		c.addNull()

		// Nullable null: no sentinel data.
		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
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

	// fixedData: 3 non-null values × 8 bytes = 24 bytes (no sentinels for nullable).
	if len(c.fixedData) != 24 {
		t.Fatalf("fixedData len = %d, want 24", len(c.fixedData))
	}
	if c.valueCount() != 3 {
		t.Fatalf("valueCount = %d, want 3", c.valueCount())
	}

	// Non-null values are packed contiguously (no gaps for null rows).
	v0 := int64(binary.LittleEndian.Uint64(c.fixedData[0:8]))
	if v0 != 100 {
		t.Fatalf("value 0 = %d, want 100", v0)
	}
	v1 := int64(binary.LittleEndian.Uint64(c.fixedData[8:16]))
	if v1 != 200 {
		t.Fatalf("value 1 = %d, want 200", v1)
	}
	v2 := int64(binary.LittleEndian.Uint64(c.fixedData[16:24]))
	if v2 != 300 {
		t.Fatalf("value 2 = %d, want 300", v2)
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
	c.addNull()          // row 1 (nullable: no offset appended)
	c.addString("world") // row 2

	if string(c.strData) != "helloworld" {
		t.Fatalf("strData = %q, want %q", c.strData, "helloworld")
	}

	// Offsets: [0, 5, 10] — only non-null values, no entry for null row.
	expectedOffsets := []uint32{0, 5, 10}
	if len(c.strOffsets) != len(expectedOffsets) {
		t.Fatalf("strOffsets len = %d, want %d", len(c.strOffsets), len(expectedOffsets))
	}
	for i, off := range expectedOffsets {
		if c.strOffsets[i] != off {
			t.Fatalf("strOffsets[%d] = %d, want %d", i, c.strOffsets[i], off)
		}
	}
	if c.valueCount() != 2 {
		t.Fatalf("valueCount = %d, want 2", c.valueCount())
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

// --- array column buffer tests ---

func TestQwpColumnBufferDoubleArray1D(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)
	c.addDoubleArray(1, []int32{3}, []float64{1.0, 2.0, 3.0})

	if c.rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", c.rowCount)
	}
	if c.fixedSize != -1 {
		t.Fatalf("fixedSize = %d, want -1", c.fixedSize)
	}
	// Offsets: [0, 29] → 1 + 4 + 3*8 = 29 bytes
	if len(c.arrayOffsets) != 2 {
		t.Fatalf("arrayOffsets len = %d, want 2", len(c.arrayOffsets))
	}
	if c.arrayOffsets[0] != 0 || c.arrayOffsets[1] != 29 {
		t.Fatalf("arrayOffsets = %v, want [0 29]", c.arrayOffsets)
	}

	data := c.arrayData
	if len(data) != 29 {
		t.Fatalf("arrayData len = %d, want 29", len(data))
	}

	// nDims = 1
	if data[0] != 0x01 {
		t.Fatalf("nDims = 0x%02X, want 0x01", data[0])
	}
	// shape[0] = 3 LE
	dim0 := binary.LittleEndian.Uint32(data[1:5])
	if dim0 != 3 {
		t.Fatalf("shape[0] = %d, want 3", dim0)
	}
	// elements: 1.0, 2.0, 3.0
	for i, want := range []float64{1.0, 2.0, 3.0} {
		off := 5 + i*8
		got := math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
		if got != want {
			t.Fatalf("element[%d] = %v, want %v", i, got, want)
		}
	}
}

func TestQwpColumnBufferDoubleArray2D(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)
	// 2×3 array, row-major: [1,2,3,4,5,6]
	c.addDoubleArray(2, []int32{2, 3}, []float64{1, 2, 3, 4, 5, 6})

	// Size: 1 + 2*4 + 6*8 = 1 + 8 + 48 = 57
	if len(c.arrayData) != 57 {
		t.Fatalf("arrayData len = %d, want 57", len(c.arrayData))
	}

	data := c.arrayData
	if data[0] != 0x02 {
		t.Fatalf("nDims = %d, want 2", data[0])
	}
	if binary.LittleEndian.Uint32(data[1:5]) != 2 {
		t.Fatalf("shape[0] = %d, want 2", binary.LittleEndian.Uint32(data[1:5]))
	}
	if binary.LittleEndian.Uint32(data[5:9]) != 3 {
		t.Fatalf("shape[1] = %d, want 3", binary.LittleEndian.Uint32(data[5:9]))
	}

	// Verify all 6 elements
	for i := 0; i < 6; i++ {
		off := 9 + i*8
		got := math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
		if got != float64(i+1) {
			t.Fatalf("element[%d] = %v, want %v", i, got, float64(i+1))
		}
	}

	if c.arrayOffsets[0] != 0 || c.arrayOffsets[1] != 57 {
		t.Fatalf("arrayOffsets = %v, want [0 57]", c.arrayOffsets)
	}
}

func TestQwpColumnBufferDoubleArray3D(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)
	// 2×2×2 array
	flat := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	c.addDoubleArray(3, []int32{2, 2, 2}, flat)

	// Size: 1 + 3*4 + 8*8 = 1 + 12 + 64 = 77
	if len(c.arrayData) != 77 {
		t.Fatalf("arrayData len = %d, want 77", len(c.arrayData))
	}

	data := c.arrayData
	if data[0] != 0x03 {
		t.Fatalf("nDims = %d, want 3", data[0])
	}
	for d := 0; d < 3; d++ {
		off := 1 + d*4
		dim := binary.LittleEndian.Uint32(data[off : off+4])
		if dim != 2 {
			t.Fatalf("shape[%d] = %d, want 2", d, dim)
		}
	}
}

func TestQwpColumnBufferLongArray1D(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeLongArray, false)
	c.addLongArray(1, []int32{4}, []int64{10, 20, 30, 40})

	if c.rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", c.rowCount)
	}
	// Size: 1 + 4 + 4*8 = 37
	if len(c.arrayData) != 37 {
		t.Fatalf("arrayData len = %d, want 37", len(c.arrayData))
	}

	data := c.arrayData
	if data[0] != 0x01 {
		t.Fatalf("nDims = 0x%02X, want 0x01", data[0])
	}
	dim0 := binary.LittleEndian.Uint32(data[1:5])
	if dim0 != 4 {
		t.Fatalf("shape[0] = %d, want 4", dim0)
	}

	for i, want := range []int64{10, 20, 30, 40} {
		off := 5 + i*8
		got := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		if got != want {
			t.Fatalf("element[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestQwpColumnBufferLongArray2D(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeLongArray, false)
	// 3×2 array
	c.addLongArray(2, []int32{3, 2}, []int64{100, 200, 300, 400, 500, 600})

	// Size: 1 + 2*4 + 6*8 = 57
	if len(c.arrayData) != 57 {
		t.Fatalf("arrayData len = %d, want 57", len(c.arrayData))
	}

	data := c.arrayData
	if data[0] != 0x02 {
		t.Fatalf("nDims = %d, want 2", data[0])
	}
	if binary.LittleEndian.Uint32(data[1:5]) != 3 {
		t.Fatalf("shape[0] = %d, want 3", binary.LittleEndian.Uint32(data[1:5]))
	}
	if binary.LittleEndian.Uint32(data[5:9]) != 2 {
		t.Fatalf("shape[1] = %d, want 2", binary.LittleEndian.Uint32(data[5:9]))
	}
}

func TestQwpColumnBufferArrayMultipleRows(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)

	// Row 0: 1D with 2 elements → 1 + 4 + 2*8 = 21 bytes
	c.addDoubleArray(1, []int32{2}, []float64{1.5, 2.5})
	// Row 1: 1D with 3 elements → 1 + 4 + 3*8 = 29 bytes
	c.addDoubleArray(1, []int32{3}, []float64{10, 20, 30})

	if c.rowCount != 2 {
		t.Fatalf("rowCount = %d, want 2", c.rowCount)
	}

	expectedOffsets := []uint32{0, 21, 50}
	if len(c.arrayOffsets) != 3 {
		t.Fatalf("arrayOffsets len = %d, want 3", len(c.arrayOffsets))
	}
	for i, want := range expectedOffsets {
		if c.arrayOffsets[i] != want {
			t.Fatalf("arrayOffsets[%d] = %d, want %d", i, c.arrayOffsets[i], want)
		}
	}

	// Verify row 0's first element
	row0Data := c.arrayData[c.arrayOffsets[0]:c.arrayOffsets[1]]
	elem0 := math.Float64frombits(binary.LittleEndian.Uint64(row0Data[5:13]))
	if elem0 != 1.5 {
		t.Fatalf("row0 elem[0] = %v, want 1.5", elem0)
	}

	// Verify row 1's first element
	row1Data := c.arrayData[c.arrayOffsets[1]:c.arrayOffsets[2]]
	elem1 := math.Float64frombits(binary.LittleEndian.Uint64(row1Data[5:13]))
	if elem1 != 10 {
		t.Fatalf("row1 elem[0] = %v, want 10", elem1)
	}
}

func TestQwpColumnBufferArrayEmpty(t *testing.T) {
	// A 1D array with 0 elements (not null, but empty).
	c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)
	c.addDoubleArray(1, []int32{0}, nil)

	// Size: 1 + 4 + 0 = 5 bytes
	if len(c.arrayData) != 5 {
		t.Fatalf("arrayData len = %d, want 5", len(c.arrayData))
	}
	if c.arrayData[0] != 0x01 {
		t.Fatalf("nDims = %d, want 1", c.arrayData[0])
	}
	dim0 := binary.LittleEndian.Uint32(c.arrayData[1:5])
	if dim0 != 0 {
		t.Fatalf("shape[0] = %d, want 0", dim0)
	}
}

func TestQwpColumnBufferArrayNull(t *testing.T) {
	t.Run("DoubleArrayNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDoubleArray, true)
		c.addNull()

		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x01}) {
			t.Fatalf("nullBitmap = %x, want [01]", c.nullBitmap)
		}

		// Nullable null: no sentinel data appended to arrayData.
		if len(c.arrayData) != 0 {
			t.Fatalf("arrayData len = %d, want 0", len(c.arrayData))
		}
		if c.valueCount() != 0 {
			t.Fatalf("valueCount = %d, want 0", c.valueCount())
		}
		// Only the initial offset [0] remains.
		if len(c.arrayOffsets) != 1 || c.arrayOffsets[0] != 0 {
			t.Fatalf("arrayOffsets = %v, want [0]", c.arrayOffsets)
		}
	})

	t.Run("LongArrayNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLongArray, true)
		c.addNull()

		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
		// Nullable null: no sentinel data.
		if len(c.arrayData) != 0 {
			t.Fatalf("arrayData len = %d, want 0", len(c.arrayData))
		}
	})

	t.Run("InterleavedNullAndData", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDoubleArray, true)
		c.addDoubleArray(1, []int32{2}, []float64{1.0, 2.0}) // row 0: 21 bytes
		c.addNull()                                            // row 1: nullable, no data
		c.addDoubleArray(1, []int32{1}, []float64{3.0})       // row 2: 13 bytes

		if c.rowCount != 3 {
			t.Fatalf("rowCount = %d, want 3", c.rowCount)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
		if c.valueCount() != 2 {
			t.Fatalf("valueCount = %d, want 2", c.valueCount())
		}

		// Bitmap: row 1 null → bit 1 → 0x02
		if !bytes.Equal(c.nullBitmap, []byte{0x02}) {
			t.Fatalf("nullBitmap = %x, want [02]", c.nullBitmap)
		}

		// Only non-null data: 21 + 13 = 34 bytes, 2 values + initial offset.
		expectedOffsets := []uint32{0, 21, 34}
		if len(c.arrayOffsets) != 3 {
			t.Fatalf("arrayOffsets len = %d, want 3", len(c.arrayOffsets))
		}
		for i, want := range expectedOffsets {
			if c.arrayOffsets[i] != want {
				t.Fatalf("arrayOffsets[%d] = %d, want %d", i, c.arrayOffsets[i], want)
			}
		}
	})
}

func TestQwpColumnBufferArrayReset(t *testing.T) {
	c := newQwpColumnBuffer("col", qwpTypeDoubleArray, true)
	c.addDoubleArray(1, []int32{2}, []float64{1.0, 2.0})
	c.addNull()

	origArrayDataCap := cap(c.arrayData)
	c.reset()

	if c.rowCount != 0 {
		t.Fatalf("rowCount = %d, want 0", c.rowCount)
	}
	if c.nullCount != 0 {
		t.Fatalf("nullCount = %d, want 0", c.nullCount)
	}
	if len(c.arrayData) != 0 {
		t.Fatalf("arrayData len = %d, want 0", len(c.arrayData))
	}
	if len(c.arrayOffsets) != 1 || c.arrayOffsets[0] != 0 {
		t.Fatalf("arrayOffsets = %v, want [0]", c.arrayOffsets)
	}
	if len(c.nullBitmap) != 0 {
		t.Fatalf("nullBitmap len = %d, want 0", len(c.nullBitmap))
	}
	// Capacity retained.
	if cap(c.arrayData) != origArrayDataCap {
		t.Fatalf("arrayData cap changed from %d to %d", origArrayDataCap, cap(c.arrayData))
	}

	// Reuse after reset.
	c.addDoubleArray(1, []int32{1}, []float64{42.0})
	if c.rowCount != 1 {
		t.Fatalf("after reuse: rowCount = %d, want 1", c.rowCount)
	}
	if len(c.arrayOffsets) != 2 {
		t.Fatalf("after reuse: arrayOffsets len = %d, want 2", len(c.arrayOffsets))
	}
}

func TestQwpColumnBufferArrayTruncateTo(t *testing.T) {
	t.Run("TruncateMiddle", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)
		c.addDoubleArray(1, []int32{2}, []float64{1.0, 2.0}) // row 0: 21 bytes
		c.addDoubleArray(1, []int32{3}, []float64{3, 4, 5})   // row 1: 29 bytes
		c.addDoubleArray(1, []int32{1}, []float64{6.0})       // row 2: 13 bytes

		c.truncateTo(2)

		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
		// Offsets: [0, 21, 50]
		expectedOffsets := []uint32{0, 21, 50}
		if len(c.arrayOffsets) != 3 {
			t.Fatalf("arrayOffsets len = %d, want 3", len(c.arrayOffsets))
		}
		for i, want := range expectedOffsets {
			if c.arrayOffsets[i] != want {
				t.Fatalf("arrayOffsets[%d] = %d, want %d", i, c.arrayOffsets[i], want)
			}
		}
		if len(c.arrayData) != 50 {
			t.Fatalf("arrayData len = %d, want 50", len(c.arrayData))
		}
	})

	t.Run("TruncateToZero", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLongArray, false)
		c.addLongArray(1, []int32{2}, []int64{10, 20})

		c.truncateTo(0)

		if c.rowCount != 0 {
			t.Fatalf("rowCount = %d, want 0", c.rowCount)
		}
		if len(c.arrayOffsets) != 1 || c.arrayOffsets[0] != 0 {
			t.Fatalf("arrayOffsets = %v, want [0]", c.arrayOffsets)
		}
		if len(c.arrayData) != 0 {
			t.Fatalf("arrayData len = %d, want 0", len(c.arrayData))
		}
	})

	t.Run("TruncateWithNulls", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDoubleArray, true)
		c.addDoubleArray(1, []int32{1}, []float64{1.0}) // row 0: 13 bytes
		c.addNull()                                       // row 1: nullable null, no data
		c.addDoubleArray(1, []int32{1}, []float64{2.0}) // row 2: 13 bytes

		c.truncateTo(2)

		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
		if !bytes.Equal(c.nullBitmap, []byte{0x02}) {
			t.Fatalf("nullBitmap = %x, want [02]", c.nullBitmap)
		}
		// After truncate to 2 rows: 1 null in first 2, so valueCount = 1.
		// arrayOffsets: [0, 13] (newVC + 1 = 2 entries).
		expectedOffsets := []uint32{0, 13}
		if len(c.arrayOffsets) != len(expectedOffsets) {
			t.Fatalf("arrayOffsets len = %d, want %d", len(c.arrayOffsets), len(expectedOffsets))
		}
		for i, want := range expectedOffsets {
			if c.arrayOffsets[i] != want {
				t.Fatalf("arrayOffsets[%d] = %d, want %d", i, c.arrayOffsets[i], want)
			}
		}
		if len(c.arrayData) != 13 {
			t.Fatalf("arrayData len = %d, want 13", len(c.arrayData))
		}
	})

	t.Run("NoOp", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)
		c.addDoubleArray(1, []int32{1}, []float64{42.0})

		c.truncateTo(5) // n >= rowCount

		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
	})
}

func TestQwpColumnBufferArrayGoldenBytes(t *testing.T) {
	// Golden byte test: 1D double array [1.5, -2.5]
	c := newQwpColumnBuffer("vals", qwpTypeDoubleArray, false)
	c.addDoubleArray(1, []int32{2}, []float64{1.5, -2.5})

	expected := []byte{
		0x01,                   // nDims = 1
		0x02, 0x00, 0x00, 0x00, // shape[0] = 2 (uint32 LE)
	}
	// element 0: float64(1.5) LE
	e0 := make([]byte, 8)
	binary.LittleEndian.PutUint64(e0, math.Float64bits(1.5))
	expected = append(expected, e0...)
	// element 1: float64(-2.5) LE
	e1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(e1, math.Float64bits(-2.5))
	expected = append(expected, e1...)

	if !bytes.Equal(c.arrayData, expected) {
		t.Fatalf("arrayData = %x\nwant      = %x", c.arrayData, expected)
	}
}

func TestQwpColumnBufferArrayConstructor(t *testing.T) {
	t.Run("DoubleArray", func(t *testing.T) {
		c := newQwpColumnBuffer("arr", qwpTypeDoubleArray, false)
		if c.fixedSize != -1 {
			t.Fatalf("fixedSize = %d, want -1", c.fixedSize)
		}
		if len(c.arrayOffsets) != 1 || c.arrayOffsets[0] != 0 {
			t.Fatalf("arrayOffsets = %v, want [0]", c.arrayOffsets)
		}
		// strOffsets should NOT be initialized for array types.
		if len(c.strOffsets) != 0 {
			t.Fatalf("strOffsets len = %d, want 0", len(c.strOffsets))
		}
	})

	t.Run("LongArray", func(t *testing.T) {
		c := newQwpColumnBuffer("arr", qwpTypeLongArray, true)
		if c.fixedSize != -1 {
			t.Fatalf("fixedSize = %d, want -1", c.fixedSize)
		}
		if len(c.arrayOffsets) != 1 || c.arrayOffsets[0] != 0 {
			t.Fatalf("arrayOffsets = %v, want [0]", c.arrayOffsets)
		}
		if !c.nullable {
			t.Fatal("nullable should be true")
		}
	})
}

func TestQwpTableBufferArrayGapFill(t *testing.T) {
	tb := newQwpTableBuffer("t")

	// Row 0: set both columns.
	colA, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
	colA.addLong(100)
	colArr, _ := tb.getOrCreateColumn("arr", qwpTypeDoubleArray, true)
	colArr.addDoubleArray(1, []int32{2}, []float64{1.0, 2.0})
	tb.commitRow()

	// Row 1: set only "a" → "arr" should be gap-filled with null.
	colA, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
	colA.addLong(200)
	tb.commitRow()

	if colArr.rowCount != 2 {
		t.Fatalf("arr rowCount = %d, want 2", colArr.rowCount)
	}
	if colArr.nullCount != 1 {
		t.Fatalf("arr nullCount = %d, want 1", colArr.nullCount)
	}
	if colArr.valueCount() != 1 {
		t.Fatalf("arr valueCount = %d, want 1", colArr.valueCount())
	}

	// Nullable null: no data appended for gap-filled row.
	// Only row 0's data (21 bytes) is in arrayData.
	if len(colArr.arrayData) != 21 {
		t.Fatalf("arrayData len = %d, want 21", len(colArr.arrayData))
	}
	expectedOffsets := []uint32{0, 21}
	if len(colArr.arrayOffsets) != len(expectedOffsets) {
		t.Fatalf("arrayOffsets len = %d, want %d", len(colArr.arrayOffsets), len(expectedOffsets))
	}
	for i, want := range expectedOffsets {
		if colArr.arrayOffsets[i] != want {
			t.Fatalf("arrayOffsets[%d] = %d, want %d", i, colArr.arrayOffsets[i], want)
		}
	}

	// Null bitmap: row 1 is null → bit 1 → 0x02.
	if !bytes.Equal(colArr.nullBitmap, []byte{0x02}) {
		t.Fatalf("nullBitmap = %x, want [02]", colArr.nullBitmap)
	}
}

func TestQwpTableBufferArrayCancelRow(t *testing.T) {
	tb := newQwpTableBuffer("t")

	// Row 0: commit with array.
	colArr, _ := tb.getOrCreateColumn("arr", qwpTypeDoubleArray, false)
	colArr.addDoubleArray(1, []int32{2}, []float64{1.0, 2.0})
	tb.commitRow()

	// Start row 1, then cancel.
	colArr, _ = tb.getOrCreateColumn("arr", qwpTypeDoubleArray, false)
	colArr.addDoubleArray(1, []int32{3}, []float64{3, 4, 5})
	tb.cancelRow()

	if tb.rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", tb.rowCount)
	}
	if colArr.rowCount != 1 {
		t.Fatalf("col rowCount = %d, want 1", colArr.rowCount)
	}
	// Only row 0's data should remain (21 bytes).
	if len(colArr.arrayData) != 21 {
		t.Fatalf("arrayData len = %d, want 21", len(colArr.arrayData))
	}
	if len(colArr.arrayOffsets) != 2 {
		t.Fatalf("arrayOffsets len = %d, want 2", len(colArr.arrayOffsets))
	}
}

func TestQwpColumnBufferArrayWireTypeCode(t *testing.T) {
	t.Run("DoubleArrayNonNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDoubleArray, false)
		if c.wireTypeCode() != 0x11 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x11", c.wireTypeCode())
		}
	})

	t.Run("DoubleArrayNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDoubleArray, true)
		// 0x11 | 0x80 = 0x91
		if c.wireTypeCode() != 0x91 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x91", c.wireTypeCode())
		}
	})

	t.Run("LongArrayNonNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLongArray, false)
		if c.wireTypeCode() != 0x12 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x12", c.wireTypeCode())
		}
	})

	t.Run("LongArrayNullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeLongArray, true)
		// 0x12 | 0x80 = 0x92
		if c.wireTypeCode() != 0x92 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x92", c.wireTypeCode())
		}
	})
}

// --- decimal column buffer tests ---

func TestQwpColumnBufferDecimal64Basic(t *testing.T) {
	c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
	// 12345 with scale 2 → represents 123.45
	d := NewDecimalFromInt64(12345, 2)
	if err := c.addDecimal(d); err != nil {
		t.Fatal(err)
	}

	if c.rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", c.rowCount)
	}
	if c.scale != 2 {
		t.Fatalf("scale = %d, want 2", c.scale)
	}
	if len(c.fixedData) != 8 {
		t.Fatalf("fixedData len = %d, want 8", len(c.fixedData))
	}

	// 12345 = 0x3039 → big-endian 8 bytes: [0,0,0,0,0,0,0x30,0x39]
	expected := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39}
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
}

func TestQwpColumnBufferDecimal64Negative(t *testing.T) {
	c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
	// -42 with scale 0
	d := NewDecimalFromInt64(-42, 0)
	if err := c.addDecimal(d); err != nil {
		t.Fatal(err)
	}

	// -42 in two's complement big-endian 8 bytes:
	// [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD6]
	expected := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD6}
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
}

func TestQwpColumnBufferDecimal64Zero(t *testing.T) {
	c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
	d := NewDecimalFromInt64(0, 5)
	if err := c.addDecimal(d); err != nil {
		t.Fatal(err)
	}

	expected := make([]byte, 8) // all zeros
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
	if c.scale != 5 {
		t.Fatalf("scale = %d, want 5", c.scale)
	}
}

func TestQwpColumnBufferDecimal128(t *testing.T) {
	c := newQwpColumnBuffer("amount", qwpTypeDecimal128, false)
	d := NewDecimalFromInt64(999999999, 4)
	if err := c.addDecimal(d); err != nil {
		t.Fatal(err)
	}

	if len(c.fixedData) != 16 {
		t.Fatalf("fixedData len = %d, want 16", len(c.fixedData))
	}

	// 999999999 = 0x3B9AC9FF → big-endian 16 bytes
	expected := make([]byte, 16)
	expected[12] = 0x3B
	expected[13] = 0x9A
	expected[14] = 0xC9
	expected[15] = 0xFF
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
}

func TestQwpColumnBufferDecimal128Negative(t *testing.T) {
	c := newQwpColumnBuffer("amount", qwpTypeDecimal128, false)
	d := NewDecimalFromInt64(-1, 0)
	if err := c.addDecimal(d); err != nil {
		t.Fatal(err)
	}

	// -1 in two's complement 16 bytes: all 0xFF
	expected := bytes.Repeat([]byte{0xFF}, 16)
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
}

func TestQwpColumnBufferDecimal256(t *testing.T) {
	c := newQwpColumnBuffer("amount", qwpTypeDecimal256, false)
	d := NewDecimalFromInt64(42, 10)
	if err := c.addDecimal(d); err != nil {
		t.Fatal(err)
	}

	if len(c.fixedData) != 32 {
		t.Fatalf("fixedData len = %d, want 32", len(c.fixedData))
	}

	// 42 = 0x2A → big-endian 32 bytes: last byte is 0x2A, rest 0x00
	expected := make([]byte, 32)
	expected[31] = 0x2A
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
	if c.scale != 10 {
		t.Fatalf("scale = %d, want 10", c.scale)
	}
}

func TestQwpColumnBufferDecimal256Negative(t *testing.T) {
	c := newQwpColumnBuffer("amount", qwpTypeDecimal256, false)
	d := NewDecimalFromInt64(-42, 0)
	if err := c.addDecimal(d); err != nil {
		t.Fatal(err)
	}

	// -42 in two's complement 32 bytes: leading 0xFF, last byte 0xD6
	expected := bytes.Repeat([]byte{0xFF}, 32)
	expected[31] = 0xD6
	if !bytes.Equal(c.fixedData, expected) {
		t.Fatalf("fixedData = %x, want %x", c.fixedData, expected)
	}
}

func TestQwpColumnBufferDecimalScaleTracking(t *testing.T) {
	t.Run("FirstValueSetsScale", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
		if c.scale != -1 {
			t.Fatalf("initial scale = %d, want -1", c.scale)
		}

		d := NewDecimalFromInt64(100, 3)
		if err := c.addDecimal(d); err != nil {
			t.Fatal(err)
		}
		if c.scale != 3 {
			t.Fatalf("scale = %d, want 3", c.scale)
		}
	})

	t.Run("ConsistentScale", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
		if err := c.addDecimal(NewDecimalFromInt64(100, 2)); err != nil {
			t.Fatal(err)
		}
		if err := c.addDecimal(NewDecimalFromInt64(200, 2)); err != nil {
			t.Fatal(err)
		}
		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
	})

	t.Run("ScaleConflict", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
		if err := c.addDecimal(NewDecimalFromInt64(100, 2)); err != nil {
			t.Fatal(err)
		}

		err := c.addDecimal(NewDecimalFromInt64(200, 5))
		if err == nil {
			t.Fatal("expected scale conflict error")
		}
		// rowCount should not have incremented on error.
		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1 (unchanged after error)", c.rowCount)
		}
	})

	t.Run("NullDoesNotSetScale", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, true)
		c.addNull() // null first

		if c.scale != -1 {
			t.Fatalf("scale after null = %d, want -1", c.scale)
		}

		// Now set a non-null value.
		if err := c.addDecimal(NewDecimalFromInt64(100, 7)); err != nil {
			t.Fatal(err)
		}
		if c.scale != 7 {
			t.Fatalf("scale = %d, want 7", c.scale)
		}
	})

	t.Run("NullDecimalCallsAddNull", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, true)
		// Construct a null Decimal.
		d, err := NewDecimalUnsafe(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.addDecimal(d); err != nil {
			t.Fatal(err)
		}

		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
		if c.nullCount != 1 {
			t.Fatalf("nullCount = %d, want 1", c.nullCount)
		}
		// Nullable null: no data appended.
		if len(c.fixedData) != 0 {
			t.Fatalf("fixedData should be empty for nullable null, got %x", c.fixedData)
		}
		if c.valueCount() != 0 {
			t.Fatalf("valueCount = %d, want 0", c.valueCount())
		}
	})

	t.Run("ScalePersistsAcrossReset", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
		if err := c.addDecimal(NewDecimalFromInt64(100, 4)); err != nil {
			t.Fatal(err)
		}
		c.reset()

		// Scale should persist — it's a column property, not row data.
		if c.scale != 4 {
			t.Fatalf("scale after reset = %d, want 4", c.scale)
		}

		// Adding with same scale should still work.
		if err := c.addDecimal(NewDecimalFromInt64(200, 4)); err != nil {
			t.Fatal(err)
		}
		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
	})
}

func TestQwpColumnBufferDecimalOverflow(t *testing.T) {
	t.Run("Decimal64Overflow", func(t *testing.T) {
		c := newQwpColumnBuffer("val", qwpTypeDecimal64, false)

		// Create a value that requires more than 8 bytes.
		// Use a big.Int larger than int64 max.
		bigVal := new(big.Int).Lsh(big.NewInt(1), 64) // 2^64
		d, err := NewDecimal(bigVal, 0)
		if err != nil {
			t.Fatal(err)
		}

		err = c.addDecimal(d)
		if err == nil {
			t.Fatal("expected overflow error for DECIMAL64")
		}
		if c.rowCount != 0 {
			t.Fatalf("rowCount = %d, want 0 (unchanged after error)", c.rowCount)
		}
	})

	t.Run("Decimal128FitsLargeValue", func(t *testing.T) {
		c := newQwpColumnBuffer("val", qwpTypeDecimal128, false)

		// 2^64 fits in 16 bytes (needs 9 bytes: 0x01 + 8 zero bytes).
		bigVal := new(big.Int).Lsh(big.NewInt(1), 64)
		d, err := NewDecimal(bigVal, 0)
		if err != nil {
			t.Fatal(err)
		}

		if err := c.addDecimal(d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1", c.rowCount)
		}
	})
}

func TestQwpColumnBufferDecimalMultipleRows(t *testing.T) {
	c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
	if err := c.addDecimal(NewDecimalFromInt64(100, 2)); err != nil {
		t.Fatal(err)
	}
	if err := c.addDecimal(NewDecimalFromInt64(-50, 2)); err != nil {
		t.Fatal(err)
	}

	if len(c.fixedData) != 16 { // 2 rows × 8 bytes
		t.Fatalf("fixedData len = %d, want 16", len(c.fixedData))
	}

	// Row 0: 100 = 0x64 → [0,0,0,0,0,0,0,0x64]
	row0 := c.fixedData[0:8]
	expected0 := []byte{0, 0, 0, 0, 0, 0, 0, 0x64}
	if !bytes.Equal(row0, expected0) {
		t.Fatalf("row 0 = %x, want %x", row0, expected0)
	}

	// Row 1: -50 = 0xCE in two's complement → [0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xCE]
	row1 := c.fixedData[8:16]
	expected1 := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xCE}
	if !bytes.Equal(row1, expected1) {
		t.Fatalf("row 1 = %x, want %x", row1, expected1)
	}
}

func TestQwpColumnBufferDecimalNullInterleaved(t *testing.T) {
	c := newQwpColumnBuffer("price", qwpTypeDecimal64, true)
	if err := c.addDecimal(NewDecimalFromInt64(100, 2)); err != nil {
		t.Fatal(err)
	}
	c.addNull() // row 1: null
	if err := c.addDecimal(NewDecimalFromInt64(200, 2)); err != nil {
		t.Fatal(err)
	}

	if c.rowCount != 3 {
		t.Fatalf("rowCount = %d, want 3", c.rowCount)
	}
	if c.nullCount != 1 {
		t.Fatalf("nullCount = %d, want 1", c.nullCount)
	}
	// Bitmap: row 1 null → bit 1 → 0x02
	if !bytes.Equal(c.nullBitmap, []byte{0x02}) {
		t.Fatalf("nullBitmap = %x, want [02]", c.nullBitmap)
	}

	// Nullable null: only 2 non-null values stored (16 bytes total).
	if c.valueCount() != 2 {
		t.Fatalf("valueCount = %d, want 2", c.valueCount())
	}
	if len(c.fixedData) != 16 {
		t.Fatalf("fixedData len = %d, want 16", len(c.fixedData))
	}
}

func TestQwpColumnBufferDecimalWireTypeCode(t *testing.T) {
	t.Run("Decimal64", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDecimal64, false)
		if c.wireTypeCode() != 0x13 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x13", c.wireTypeCode())
		}
	})
	t.Run("Decimal128Nullable", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDecimal128, true)
		// 0x14 | 0x80 = 0x94
		if c.wireTypeCode() != 0x94 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x94", c.wireTypeCode())
		}
	})
	t.Run("Decimal256", func(t *testing.T) {
		c := newQwpColumnBuffer("col", qwpTypeDecimal256, false)
		if c.wireTypeCode() != 0x15 {
			t.Fatalf("wireTypeCode = 0x%02X, want 0x15", c.wireTypeCode())
		}
	})
}

func TestQwpTableBufferDecimalGapFill(t *testing.T) {
	tb := newQwpTableBuffer("t")

	colA, _ := tb.getOrCreateColumn("a", qwpTypeLong, false)
	colA.addLong(100)
	colD, _ := tb.getOrCreateColumn("d", qwpTypeDecimal64, true)
	if err := colD.addDecimal(NewDecimalFromInt64(42, 2)); err != nil {
		t.Fatal(err)
	}
	tb.commitRow()

	// Row 1: only set "a" → "d" should be gap-filled with null.
	colA, _ = tb.getOrCreateColumn("a", qwpTypeLong, false)
	colA.addLong(200)
	tb.commitRow()

	if colD.rowCount != 2 {
		t.Fatalf("decimal col rowCount = %d, want 2", colD.rowCount)
	}
	if colD.nullCount != 1 {
		t.Fatalf("decimal col nullCount = %d, want 1", colD.nullCount)
	}
	// Scale should still be 2 (set by row 0).
	if colD.scale != 2 {
		t.Fatalf("decimal col scale = %d, want 2", colD.scale)
	}
}

// --- geohash column buffer tests ---

func TestQwpColumnBufferGeohashBasic(t *testing.T) {
	c := newQwpColumnBuffer("geo", qwpTypeGeohash, false)
	if err := c.addGeohash(0x12345, 20); err != nil {
		t.Fatal(err)
	}

	if c.rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", c.rowCount)
	}
	if c.geohashPrecision != 20 {
		t.Fatalf("geohashPrecision = %d, want 20", c.geohashPrecision)
	}
	// Stored as 8 bytes LE in fixedData.
	if len(c.fixedData) != 8 {
		t.Fatalf("fixedData len = %d, want 8", len(c.fixedData))
	}
	got := binary.LittleEndian.Uint64(c.fixedData)
	if got != 0x12345 {
		t.Fatalf("fixedData value = 0x%X, want 0x12345", got)
	}
}

func TestQwpColumnBufferGeohashPrecisionTracking(t *testing.T) {
	t.Run("ConsistentPrecision", func(t *testing.T) {
		c := newQwpColumnBuffer("geo", qwpTypeGeohash, false)
		if err := c.addGeohash(1, 15); err != nil {
			t.Fatal(err)
		}
		if err := c.addGeohash(2, 15); err != nil {
			t.Fatal(err)
		}
		if c.rowCount != 2 {
			t.Fatalf("rowCount = %d, want 2", c.rowCount)
		}
	})

	t.Run("PrecisionConflict", func(t *testing.T) {
		c := newQwpColumnBuffer("geo", qwpTypeGeohash, false)
		if err := c.addGeohash(1, 15); err != nil {
			t.Fatal(err)
		}
		err := c.addGeohash(2, 30)
		if err == nil {
			t.Fatal("expected precision conflict error")
		}
		if c.rowCount != 1 {
			t.Fatalf("rowCount = %d, want 1 (unchanged)", c.rowCount)
		}
	})

	t.Run("NullDoesNotSetPrecision", func(t *testing.T) {
		c := newQwpColumnBuffer("geo", qwpTypeGeohash, true)
		c.addNull()
		if c.geohashPrecision != -1 {
			t.Fatalf("geohashPrecision = %d, want -1", c.geohashPrecision)
		}
	})
}

func TestQwpColumnBufferGeohashNull(t *testing.T) {
	c := newQwpColumnBuffer("geo", qwpTypeGeohash, true)
	if err := c.addGeohash(0xABC, 12); err != nil {
		t.Fatal(err)
	}
	c.addNull()

	if c.rowCount != 2 {
		t.Fatalf("rowCount = %d, want 2", c.rowCount)
	}
	if c.nullCount != 1 {
		t.Fatalf("nullCount = %d, want 1", c.nullCount)
	}
	if c.valueCount() != 1 {
		t.Fatalf("valueCount = %d, want 1", c.valueCount())
	}
	// Nullable null: no sentinel data. Only 1 non-null value (8 bytes).
	if len(c.fixedData) != 8 {
		t.Fatalf("fixedData len = %d, want 8", len(c.fixedData))
	}
	// Verify the non-null value.
	v := binary.LittleEndian.Uint64(c.fixedData[0:8])
	if v != 0xABC {
		t.Fatalf("value = 0x%X, want 0xABC", v)
	}
	// Bitmap: row 1 null → bit 1 → 0x02.
	if !bytes.Equal(c.nullBitmap, []byte{0x02}) {
		t.Fatalf("nullBitmap = %x, want [02]", c.nullBitmap)
	}
}

func TestQwpColumnBufferGeohashTruncateTo(t *testing.T) {
	c := newQwpColumnBuffer("geo", qwpTypeGeohash, false)
	if err := c.addGeohash(1, 20); err != nil {
		t.Fatal(err)
	}
	if err := c.addGeohash(2, 20); err != nil {
		t.Fatal(err)
	}
	if err := c.addGeohash(3, 20); err != nil {
		t.Fatal(err)
	}

	c.truncateTo(2)

	if c.rowCount != 2 {
		t.Fatalf("rowCount = %d, want 2", c.rowCount)
	}
	if len(c.fixedData) != 16 {
		t.Fatalf("fixedData len = %d, want 16", len(c.fixedData))
	}
}

// TestQwpTableBufferRunningDataSize verifies that the running dataSize
// counter on qwpTableBuffer matches the iterative computation for a
// table with mixed column types: symbol, long, double, string, bool,
// timestamp.
func TestQwpTableBufferRunningDataSize(t *testing.T) {
	tb := newQwpTableBuffer("test_running_size")

	// Helper to recompute data size iteratively.
	iterativeSize := func() int {
		size := 0
		for _, col := range tb.columns {
			size += len(col.fixedData)
			size += len(col.strData)
			size += len(col.boolData)
			size += len(col.arrayData)
			size += len(col.symbolIDs) * 4
			size += len(col.strOffsets) * 4
			size += len(col.nullBitmap)
		}
		return size
	}

	// Insert 10 rows with mixed types.
	for i := 0; i < 10; i++ {
		col, _ := tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
		col.addSymbolID(int32(i % 3))

		col, _ = tb.getOrCreateColumn("val", qwpTypeLong, false)
		col.addLong(int64(i * 100))

		col, _ = tb.getOrCreateColumn("price", qwpTypeDouble, false)
		col.addDouble(float64(i) * 1.5)

		col, _ = tb.getOrCreateColumn("msg", qwpTypeString, false)
		col.addString("hello")

		col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
		col.addBool(i%2 == 0)

		col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
		col.addTimestamp(int64(1000000 + i))

		tb.commitRow()

		got := tb.approxDataSize()
		want := iterativeSize()
		if got != want {
			t.Fatalf("row %d: approxDataSize() = %d, iterative = %d", i, got, want)
		}
	}

	// Verify after reset: running counter matches iterative.
	tb.reset()
	if tb.approxDataSize() != iterativeSize() {
		t.Fatalf("after reset: approxDataSize() = %d, iterative = %d",
			tb.approxDataSize(), iterativeSize())
	}

	// Insert again and verify the counter still works.
	for i := 0; i < 5; i++ {
		col, _ := tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
		col.addSymbolID(int32(i))

		col, _ = tb.getOrCreateColumn("val", qwpTypeLong, false)
		col.addLong(int64(i))

		col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
		col.addTimestamp(int64(2000000 + i))

		tb.commitRow()
	}

	got := tb.approxDataSize()
	want := iterativeSize()
	if got != want {
		t.Fatalf("after re-insert: approxDataSize() = %d, iterative = %d", got, want)
	}
}

// TestQwpTableBufferRunningDataSizeCancelRow verifies that the
// running dataSize is correct after cancelRow().
func TestQwpTableBufferRunningDataSizeCancelRow(t *testing.T) {
	tb := newQwpTableBuffer("test_cancel_size")

	// Add a committed row.
	col, _ := tb.getOrCreateColumn("val", qwpTypeLong, false)
	col.addLong(42)
	col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
	col.addTimestamp(1000000)
	tb.commitRow()

	sizeAfterCommit := tb.approxDataSize()

	// Start a row and cancel it.
	col, _ = tb.getOrCreateColumn("val", qwpTypeLong, false)
	col.addLong(99)
	col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
	col.addTimestamp(2000000)
	tb.cancelRow()

	if tb.approxDataSize() != sizeAfterCommit {
		t.Fatalf("after cancelRow: approxDataSize() = %d, want %d",
			tb.approxDataSize(), sizeAfterCommit)
	}
}

// TestQwpTableBufferDecimalTrackDataGrowth verifies that addDecimal
// increments the running dataSize counter so maxBufSize enforcement
// works correctly with decimal columns.
func TestQwpTableBufferDecimalTrackDataGrowth(t *testing.T) {
	iterativeSize := func(tb *qwpTableBuffer) int {
		size := 0
		for _, col := range tb.columns {
			size += len(col.fixedData)
			size += len(col.strData)
			size += len(col.boolData)
			size += len(col.arrayData)
			size += len(col.symbolIDs) * 4
			size += len(col.strOffsets) * 4
			size += len(col.nullBitmap)
		}
		return size
	}

	t.Run("Decimal64", func(t *testing.T) {
		tb := newQwpTableBuffer("dec64")
		for i := 0; i < 5; i++ {
			col, _ := tb.getOrCreateColumn("price", qwpTypeDecimal64, false)
			if err := col.addDecimal(NewDecimalFromInt64(int64(100+i), 2)); err != nil {
				t.Fatal(err)
			}
			col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
			col.addTimestamp(int64(1000000 + i))
			tb.commitRow()

			got := tb.approxDataSize()
			want := iterativeSize(tb)
			if got != want {
				t.Fatalf("row %d: approxDataSize() = %d, iterative = %d", i, got, want)
			}
			if got == 0 {
				t.Fatalf("row %d: approxDataSize() should be > 0", i)
			}
		}
	})

	t.Run("Decimal128", func(t *testing.T) {
		tb := newQwpTableBuffer("dec128")
		for i := 0; i < 3; i++ {
			col, _ := tb.getOrCreateColumn("amount", qwpTypeDecimal128, false)
			if err := col.addDecimal(NewDecimalFromInt64(int64(999+i), 4)); err != nil {
				t.Fatal(err)
			}
			col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
			col.addTimestamp(int64(2000000 + i))
			tb.commitRow()

			got := tb.approxDataSize()
			want := iterativeSize(tb)
			if got != want {
				t.Fatalf("row %d: approxDataSize() = %d, iterative = %d", i, got, want)
			}
		}
	})

	t.Run("Decimal256", func(t *testing.T) {
		tb := newQwpTableBuffer("dec256")
		col, _ := tb.getOrCreateColumn("big", qwpTypeDecimal256, false)
		if err := col.addDecimal(NewDecimalFromInt64(42, 10)); err != nil {
			t.Fatal(err)
		}
		col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
		col.addTimestamp(3000000)
		tb.commitRow()

		got := tb.approxDataSize()
		want := iterativeSize(tb)
		if got != want {
			t.Fatalf("approxDataSize() = %d, iterative = %d", got, want)
		}
		// Decimal256 writes 32 bytes per value.
		if got < 32 {
			t.Fatalf("approxDataSize() = %d, expected >= 32 for Decimal256", got)
		}
	})
}

// TestQwpColumnBufferDecimalScaleValidation verifies that addDecimal
// rejects scale values exceeding the maximum (76).
func TestQwpColumnBufferDecimalScaleValidation(t *testing.T) {
	t.Run("MaxScaleOK", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
		d := NewDecimalFromInt64(1, 76)
		if err := c.addDecimal(d); err != nil {
			t.Fatalf("scale 76 should be valid: %v", err)
		}
	})

	t.Run("ScaleTooLarge", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
		d := NewDecimalFromInt64(1, 77)
		err := c.addDecimal(d)
		if err == nil {
			t.Fatal("expected error for scale 77")
		}
		if !bytes.Contains([]byte(err.Error()), []byte("exceeds maximum")) {
			t.Fatalf("unexpected error message: %v", err)
		}
	})

	t.Run("ScaleWayTooLarge", func(t *testing.T) {
		c := newQwpColumnBuffer("price", qwpTypeDecimal64, false)
		d := NewDecimalFromInt64(1, 100)
		err := c.addDecimal(d)
		if err == nil {
			t.Fatal("expected error for scale 100")
		}
	})
}
