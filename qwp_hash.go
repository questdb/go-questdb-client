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
	"encoding/binary"
	"math/bits"
)

// XXHash64 constants.
const (
	xxhPrime1 uint64 = 0x9E3779B185EBCA87
	xxhPrime2 uint64 = 0xC2B2AE3D27D4EB4F
	xxhPrime3 uint64 = 0x165667B19E3779F9
	xxhPrime4 uint64 = 0x85EBCA77C2B2AE63
	xxhPrime5 uint64 = 0x27D4EB2F165667C5
)

// qwpComputeSchemaHash computes the XXHash64 schema hash over the
// column definitions. The input is the concatenation of each
// column's UTF-8 name bytes followed by its wire type code byte.
// This matches the Java QwpSchemaHash implementation.
func qwpComputeSchemaHash(columns []*qwpColumnBuffer) int64 {
	// Build the input byte sequence: for each column, name + wireTypeCode.
	var buf []byte
	for _, col := range columns {
		buf = append(buf, col.name...)
		buf = append(buf, col.wireTypeCode())
	}
	return int64(xxhash64(buf, 0))
}

// xxhash64 computes the XXHash64 digest of data with the given seed.
// This is a standard implementation of Yann Collet's XXHash64.
func xxhash64(data []byte, seed uint64) uint64 {
	n := len(data)
	var h64 uint64

	if n >= 32 {
		v1 := seed + xxhPrime1 + xxhPrime2
		v2 := seed + xxhPrime2
		v3 := seed
		v4 := seed - xxhPrime1

		// Process 32-byte blocks.
		for len(data) >= 32 {
			v1 = xxhRound(v1, binary.LittleEndian.Uint64(data[0:8]))
			v2 = xxhRound(v2, binary.LittleEndian.Uint64(data[8:16]))
			v3 = xxhRound(v3, binary.LittleEndian.Uint64(data[16:24]))
			v4 = xxhRound(v4, binary.LittleEndian.Uint64(data[24:32]))
			data = data[32:]
		}

		h64 = bits.RotateLeft64(v1, 1) +
			bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) +
			bits.RotateLeft64(v4, 18)

		h64 = xxhMergeRound(h64, v1)
		h64 = xxhMergeRound(h64, v2)
		h64 = xxhMergeRound(h64, v3)
		h64 = xxhMergeRound(h64, v4)
	} else {
		h64 = seed + xxhPrime5
	}

	h64 += uint64(n)

	// Process remaining 8-byte blocks.
	for len(data) >= 8 {
		k1 := xxhRound(0, binary.LittleEndian.Uint64(data[0:8]))
		h64 ^= k1
		h64 = bits.RotateLeft64(h64, 27)*xxhPrime1 + xxhPrime4
		data = data[8:]
	}

	// Process remaining 4-byte block.
	if len(data) >= 4 {
		h64 ^= uint64(binary.LittleEndian.Uint32(data[0:4])) * xxhPrime1
		h64 = bits.RotateLeft64(h64, 23)*xxhPrime2 + xxhPrime3
		data = data[4:]
	}

	// Process remaining bytes.
	for _, b := range data {
		h64 ^= uint64(b) * xxhPrime5
		h64 = bits.RotateLeft64(h64, 11) * xxhPrime1
	}

	// Final avalanche.
	h64 ^= h64 >> 33
	h64 *= xxhPrime2
	h64 ^= h64 >> 29
	h64 *= xxhPrime3
	h64 ^= h64 >> 32

	return h64
}

func xxhRound(acc, input uint64) uint64 {
	acc += input * xxhPrime2
	acc = bits.RotateLeft64(acc, 31)
	acc *= xxhPrime1
	return acc
}

func xxhMergeRound(acc, val uint64) uint64 {
	val = xxhRound(0, val)
	acc ^= val
	acc = acc*xxhPrime1 + xxhPrime4
	return acc
}
