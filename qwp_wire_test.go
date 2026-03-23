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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQwpWireBufferPutByte(t *testing.T) {
	var w qwpWireBuffer
	w.putByte(0x42)
	w.putByte(0xFF)
	assert.Equal(t, []byte{0x42, 0xFF}, w.bytes())
	assert.Equal(t, 2, w.len())
}

func TestQwpWireBufferPutUint16LE(t *testing.T) {
	var w qwpWireBuffer
	w.putUint16LE(0x0102)
	assert.Equal(t, []byte{0x02, 0x01}, w.bytes())
}

func TestQwpWireBufferPutUint32LE(t *testing.T) {
	var w qwpWireBuffer
	w.putUint32LE(0x01020304)
	assert.Equal(t, []byte{0x04, 0x03, 0x02, 0x01}, w.bytes())
}

func TestQwpWireBufferPutInt32LE(t *testing.T) {
	var w qwpWireBuffer
	w.putInt32LE(-1)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF}, w.bytes())

	w.reset()
	w.putInt32LE(256)
	assert.Equal(t, []byte{0x00, 0x01, 0x00, 0x00}, w.bytes())
}

func TestQwpWireBufferPutUint64LE(t *testing.T) {
	var w qwpWireBuffer
	w.putUint64LE(0x0102030405060708)
	assert.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, w.bytes())
}

func TestQwpWireBufferPutInt64LE(t *testing.T) {
	var w qwpWireBuffer
	w.putInt64LE(-1)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, w.bytes())

	w.reset()
	w.putInt64LE(1)
	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, w.bytes())
}

func TestQwpWireBufferPutFloat32LE(t *testing.T) {
	var w qwpWireBuffer
	w.putFloat32LE(1.0)
	// IEEE 754: 1.0f = 0x3F800000
	assert.Equal(t, []byte{0x00, 0x00, 0x80, 0x3F}, w.bytes())
}

func TestQwpWireBufferPutFloat64LE(t *testing.T) {
	var w qwpWireBuffer
	w.putFloat64LE(1.0)
	// IEEE 754: 1.0 = 0x3FF0000000000000
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}, w.bytes())
}

func TestQwpWireBufferPutFloat64LE_NaN(t *testing.T) {
	var w qwpWireBuffer
	nan := math.NaN()
	w.putFloat64LE(nan)
	// Verify round-trip: read back and confirm it's NaN.
	bits := uint64(w.bytes()[0]) | uint64(w.bytes()[1])<<8 |
		uint64(w.bytes()[2])<<16 | uint64(w.bytes()[3])<<24 |
		uint64(w.bytes()[4])<<32 | uint64(w.bytes()[5])<<40 |
		uint64(w.bytes()[6])<<48 | uint64(w.bytes()[7])<<56
	assert.True(t, math.IsNaN(math.Float64frombits(bits)), "expected NaN")
}

func TestQwpWireBufferPutInt64BE(t *testing.T) {
	var w qwpWireBuffer
	w.putInt64BE(0x0102030405060708)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, w.bytes())
}

func TestQwpWireBufferPutVarint(t *testing.T) {
	var w qwpWireBuffer
	w.putVarint(300)
	assert.Equal(t, []byte{0xAC, 0x02}, w.bytes())
}

func TestQwpWireBufferPutString(t *testing.T) {
	var w qwpWireBuffer
	w.putString("hello")
	// varint(5) = 0x05, then "hello"
	assert.Equal(t, []byte{0x05, 'h', 'e', 'l', 'l', 'o'}, w.bytes())
}

func TestQwpWireBufferPutStringEmpty(t *testing.T) {
	var w qwpWireBuffer
	w.putString("")
	// varint(0) = 0x00
	assert.Equal(t, []byte{0x00}, w.bytes())
}

func TestQwpWireBufferPutBytes(t *testing.T) {
	var w qwpWireBuffer
	w.putBytes([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, w.bytes())
}

func TestQwpWireBufferPatchUint32LE(t *testing.T) {
	var w qwpWireBuffer
	w.putUint32LE(0) // placeholder
	w.putByte(0xFF)
	w.patchUint32LE(0, 0x12345678)
	assert.Equal(t, []byte{0x78, 0x56, 0x34, 0x12, 0xFF}, w.bytes())
}

func TestQwpWireBufferReset(t *testing.T) {
	var w qwpWireBuffer
	w.putUint64LE(12345)
	assert.Equal(t, 8, w.len())

	origCap := cap(w.buf)
	w.reset()
	assert.Equal(t, 0, w.len())
	assert.Equal(t, origCap, cap(w.buf), "reset should preserve capacity")
}

func TestQwpWireBufferComposite(t *testing.T) {
	// Build a mini QWP header to test combined operations.
	var w qwpWireBuffer
	w.putUint32LE(qwpMagic)                     // magic "QWP1"
	w.putByte(qwpVersion)                        // version
	w.putByte(qwpFlagDeltaSymbolDict)            // flags
	w.putUint16LE(1)                             // table count = 1
	w.putUint32LE(0)                             // payload length placeholder

	assert.Equal(t, qwpHeaderSize, w.len())

	expected := []byte{
		0x51, 0x57, 0x50, 0x31, // "QWP1"
		0x01,                   // version
		0x08,                   // flags = delta symbol dict
		0x01, 0x00,             // table count = 1
		0x00, 0x00, 0x00, 0x00, // payload length placeholder
	}
	assert.Equal(t, expected, w.bytes())

	// Patch payload length
	w.patchUint32LE(8, 42)
	expected[8] = 42
	assert.Equal(t, expected, w.bytes())
}
