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
