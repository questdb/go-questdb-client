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
	"errors"
	"math/bits"
)

// qwpMaxVarintLen is the maximum number of bytes a varint-encoded
// uint64 can occupy (ceil(64/7) = 10).
const qwpMaxVarintLen = 10

// qwpPutVarint encodes v as an unsigned LEB128 varint into buf and
// returns the number of bytes written. buf must have at least
// qwpMaxVarintLen bytes of space.
func qwpPutVarint(buf []byte, v uint64) int {
	i := 0
	for v > 0x7F {
		buf[i] = byte(v&0x7F) | 0x80
		v >>= 7
		i++
	}
	buf[i] = byte(v)
	return i + 1
}

// qwpReadVarint decodes an unsigned LEB128 varint from buf. It returns
// the decoded value and the number of bytes consumed, or an error if
// the varint is malformed or truncated.
func qwpReadVarint(buf []byte) (uint64, int, error) {
	var v uint64
	var shift uint
	for i, b := range buf {
		if i >= qwpMaxVarintLen {
			return 0, 0, errors.New("qwp: varint overflow")
		}
		v |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return v, i + 1, nil
		}
		shift += 7
	}
	return 0, 0, errors.New("qwp: varint truncated")
}

// qwpVarintSize returns the number of bytes needed to encode v as an
// unsigned LEB128 varint.
func qwpVarintSize(v uint64) int {
	if v == 0 {
		return 1
	}
	// Number of bits needed, divided by 7, rounded up.
	return (bits.Len64(v) + 6) / 7
}

// qwpStringSize returns the number of bytes needed to encode s as a
// varint-prefixed UTF-8 string (varint length + string bytes).
func qwpStringSize(s string) int {
	return qwpVarintSize(uint64(len(s))) + len(s)
}
