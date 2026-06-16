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

func TestQwpVarintGoldenBytes(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		bytes []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"max_1byte", 127, []byte{0x7F}},
		{"min_2byte", 128, []byte{0x80, 0x01}},
		{"300", 300, []byte{0xAC, 0x02}},
		{"max_2byte", 16383, []byte{0xFF, 0x7F}},
		{"min_3byte", 16384, []byte{0x80, 0x80, 0x01}},
		{"max_uint32", math.MaxUint32, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
		{"max_int64", math.MaxInt64, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
		{"max_uint64", math.MaxUint64, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encode
			buf := make([]byte, qwpMaxVarintLen)
			n := qwpPutVarint(buf, tt.value)
			assert.Equal(t, tt.bytes, buf[:n], "encoded bytes mismatch")

			// Test decode
			val, consumed, err := qwpReadVarint(tt.bytes)
			assert.NoError(t, err)
			assert.Equal(t, tt.value, val, "decoded value mismatch")
			assert.Equal(t, len(tt.bytes), consumed, "consumed bytes mismatch")

			// Test size
			assert.Equal(t, len(tt.bytes), qwpVarintSize(tt.value), "size mismatch")
		})
	}
}

func TestQwpVarintRoundTrip(t *testing.T) {
	values := []uint64{
		0, 1, 2, 126, 127, 128, 129, 255, 256,
		300, 16383, 16384, 65535, 65536,
		math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 + 1,
		math.MaxInt64 - 1, math.MaxInt64,
		math.MaxUint64 - 1, math.MaxUint64,
	}

	buf := make([]byte, qwpMaxVarintLen)
	for _, v := range values {
		n := qwpPutVarint(buf, v)
		decoded, consumed, err := qwpReadVarint(buf[:n])
		assert.NoError(t, err, "round-trip error for %d", v)
		assert.Equal(t, v, decoded, "round-trip value mismatch for %d", v)
		assert.Equal(t, n, consumed, "round-trip consumed mismatch for %d", v)
		assert.Equal(t, n, qwpVarintSize(v), "size mismatch for %d", v)
	}
}

func TestQwpVarintDecodeErrors(t *testing.T) {
	// Truncated: continuation bit set but no more bytes
	_, _, err := qwpReadVarint([]byte{0x80})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "truncated")

	// Empty input
	_, _, err = qwpReadVarint([]byte{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "truncated")

	// Overflow: more than 10 continuation bytes
	overflow := make([]byte, 11)
	for i := range overflow {
		overflow[i] = 0x80
	}
	_, _, err = qwpReadVarint(overflow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "overflow")

	// Byte-10 guard: 10th byte (shift=63) may only contribute bit 0.
	// Any of data bits 1..6 set means the decoded value would silently
	// overflow uint64 via the shift. Mirrors the Java byte-10 guard in
	// QwpResultBatchDecoder.decodeVarint.
	//
	// Hostile encoding: 9 continuation bytes + 0x40 (sets bit 62 of byte 10,
	// i.e. bit 125 of the value — pure garbage, must be rejected).
	bit62 := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40}
	_, _, err = qwpReadVarint(bit62)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "overflow")

	// Hostile encoding: 9 continuation bytes + 0x02 (sets bit 64 of the
	// value — exactly one bit past uint64 range; the shift would discard
	// it silently without the guard).
	bit64 := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}
	_, _, err = qwpReadVarint(bit64)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "overflow")

	// Sanity: the in-range byte-10 pattern (bit 63 set, encoding 1<<63)
	// is NOT rejected by the guard — it's a valid uint64.
	inRange := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	v, n, err := qwpReadVarint(inRange)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1)<<63, v)
	assert.Equal(t, 10, n)
}

// --- qwpByteReader ---

func TestQwpByteReaderHappyPath(t *testing.T) {
	// Build a mixed-type buffer, then read back.
	var w qwpWireBuffer
	w.putByte(0x42)
	w.putUint16LE(0x1234)
	w.putUint32LE(0xDEADBEEF)
	w.putInt32LE(-7)
	w.putUint64LE(0x0102030405060708)
	w.putInt64LE(-42)
	w.putFloat64LE(3.5)
	w.putVarint(300)
	w.putBytes([]byte{0xCA, 0xFE, 0xBA, 0xBE})

	var r qwpByteReader
	r.reset(w.bytes())

	assert.Equal(t, len(w.bytes()), r.remaining())

	b, err := r.readByte()
	assert.NoError(t, err)
	assert.Equal(t, byte(0x42), b)

	u16, err := r.readUint16LE()
	assert.NoError(t, err)
	assert.Equal(t, uint16(0x1234), u16)

	u32, err := r.readUint32LE()
	assert.NoError(t, err)
	assert.Equal(t, uint32(0xDEADBEEF), u32)

	i32, err := r.readInt32LE()
	assert.NoError(t, err)
	assert.Equal(t, int32(-7), i32)

	u64, err := r.readUint64LE()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x0102030405060708), u64)

	i64, err := r.readInt64LE()
	assert.NoError(t, err)
	assert.Equal(t, int64(-42), i64)

	f64, err := r.readFloat64LE()
	assert.NoError(t, err)
	assert.Equal(t, 3.5, f64)

	varint, err := r.readVarint()
	assert.NoError(t, err)
	assert.Equal(t, uint64(300), varint)

	tail, err := r.slice(4)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0xCA, 0xFE, 0xBA, 0xBE}, tail)

	assert.True(t, r.atEnd())
	assert.Equal(t, 0, r.remaining())
}

func TestQwpByteReaderTruncatedAtEveryReader(t *testing.T) {
	// For each typed reader, supply a buffer one byte short and assert
	// the read errors instead of reading past the end.
	cases := []struct {
		name string
		buf  []byte
		fn   func(*qwpByteReader) error
	}{
		{"readByte", []byte{}, func(r *qwpByteReader) error { _, err := r.readByte(); return err }},
		{"readUint16LE", []byte{0x01}, func(r *qwpByteReader) error { _, err := r.readUint16LE(); return err }},
		{"readUint32LE", []byte{0x01, 0x02, 0x03}, func(r *qwpByteReader) error { _, err := r.readUint32LE(); return err }},
		{"readInt32LE", []byte{0x01, 0x02, 0x03}, func(r *qwpByteReader) error { _, err := r.readInt32LE(); return err }},
		{"readUint64LE", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, func(r *qwpByteReader) error { _, err := r.readUint64LE(); return err }},
		{"readInt64LE", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, func(r *qwpByteReader) error { _, err := r.readInt64LE(); return err }},
		{"readFloat64LE", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, func(r *qwpByteReader) error { _, err := r.readFloat64LE(); return err }},
		{"readVarint_truncated", []byte{0x80}, func(r *qwpByteReader) error { _, err := r.readVarint(); return err }},
		{"slice_past_end", []byte{0x01}, func(r *qwpByteReader) error { _, err := r.slice(2); return err }},
		{"advance_past_end", []byte{0x01}, func(r *qwpByteReader) error { return r.advance(2) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var r qwpByteReader
			r.reset(c.buf)
			err := c.fn(&r)
			assert.Error(t, err)
			var decodeErr *qwpDecodeError
			assert.ErrorAs(t, err, &decodeErr)
		})
	}
}

func TestQwpByteReaderVarintInt63RejectsSignBit(t *testing.T) {
	// Varint for 1<<63 (one past int64.MaxValue). The uint64 decoder
	// accepts it; readVarintInt63 must reject it as overflowing the
	// signed int63 range used by length / count / id fields on the wire.
	var w qwpWireBuffer
	w.putVarint(uint64(1) << 63)

	var r qwpByteReader
	r.reset(w.bytes())

	_, err := r.readVarintInt63()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "int63")

	// Sanity: math.MaxInt64 fits and round-trips.
	w.reset()
	w.putVarint(uint64(math.MaxInt64))
	r.reset(w.bytes())
	v, err := r.readVarintInt63()
	assert.NoError(t, err)
	assert.Equal(t, int64(math.MaxInt64), v)
}

func TestQwpByteReaderAdvanceAndSlice(t *testing.T) {
	buf := []byte{1, 2, 3, 4, 5, 6}
	var r qwpByteReader
	r.reset(buf)

	assert.NoError(t, r.advance(2))
	assert.Equal(t, 4, r.remaining())

	s, err := r.slice(2)
	assert.NoError(t, err)
	assert.Equal(t, []byte{3, 4}, s)

	// Slice aliases the input — mutating the source surfaces in the view.
	buf[2] = 0xEE
	assert.Equal(t, byte(0xEE), s[0])

	// Negative n rejected.
	assert.Error(t, r.advance(-1))
	_, err = r.slice(-1)
	assert.Error(t, err)

	// Running off the end errors.
	assert.Error(t, r.advance(10))
}

func TestQwpByteReaderZeroAlloc(t *testing.T) {
	// Hot-path reads must not allocate. This pins the contract that the
	// decoder (Step 4) relies on to meet the zero-alloc invariant.
	buf := make([]byte, 64)
	for i := range buf {
		buf[i] = byte(i)
	}
	var r qwpByteReader

	allocs := testing.AllocsPerRun(100, func() {
		r.reset(buf)
		_, _ = r.readByte()
		_, _ = r.readUint32LE()
		_, _ = r.readInt64LE()
		_, _ = r.readFloat64LE()
		_, _ = r.slice(4)
	})
	assert.Equal(t, float64(0), allocs, "qwpByteReader hot path must not allocate")
}

func TestQwpStringSize(t *testing.T) {
	// Empty string: varint(0) = 1 byte, data = 0 bytes
	assert.Equal(t, 1, qwpStringSize(""))

	// "hello": varint(5) = 1 byte, data = 5 bytes
	assert.Equal(t, 6, qwpStringSize("hello"))

	// 128-byte string: varint(128) = 2 bytes, data = 128 bytes
	s128 := string(make([]byte, 128))
	assert.Equal(t, 130, qwpStringSize(s128))
}
