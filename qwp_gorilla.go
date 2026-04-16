/*******************************************************************************
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
	"math"
)

// Timestamp column encoding flag values (QWP spec §12). Present
// immediately after the null bitmap when FLAG_GORILLA is set on the
// message header.
const (
	qwpTsEncodingUncompressed byte = 0x00
	qwpTsEncodingGorilla      byte = 0x01
)

// Gorilla delta-of-delta bucket boundaries (two's complement signed ranges).
const (
	qwpGorillaBucket7Min  = -64
	qwpGorillaBucket7Max  = 63
	qwpGorillaBucket9Min  = -256
	qwpGorillaBucket9Max  = 255
	qwpGorillaBucket12Min = -2048
	qwpGorillaBucket12Max = 2047
)

// qwpBitWriter packs bits LSB-first into a qwpWireBuffer. Each byte
// emitted uses its low bit for the first written bit, extending
// upward. Callers must invoke finish() to flush a trailing partial
// byte (padded with zero in the high bits).
type qwpBitWriter struct {
	wb           *qwpWireBuffer
	bitBuffer    uint64
	bitsInBuffer int
}

// reset binds the writer to wb and clears buffered state.
func (w *qwpBitWriter) reset(wb *qwpWireBuffer) {
	w.wb = wb
	w.bitBuffer = 0
	w.bitsInBuffer = 0
}

// writeBits writes the low numBits of value, LSB-first. numBits must
// be in [1, 64].
func (w *qwpBitWriter) writeBits(value uint64, numBits int) {
	if numBits < 64 {
		value &= (uint64(1) << numBits) - 1
	}
	bitsToWrite := numBits
	for bitsToWrite > 0 {
		available := 64 - w.bitsInBuffer
		n := bitsToWrite
		if n > available {
			n = available
		}
		var mask uint64
		if n == 64 {
			mask = ^uint64(0)
		} else {
			mask = (uint64(1) << n) - 1
		}
		w.bitBuffer |= (value & mask) << w.bitsInBuffer
		w.bitsInBuffer += n
		value >>= n
		bitsToWrite -= n
		for w.bitsInBuffer >= 8 {
			w.wb.putByte(byte(w.bitBuffer))
			w.bitBuffer >>= 8
			w.bitsInBuffer -= 8
		}
	}
}

// writeBit writes a single bit (0 or 1).
func (w *qwpBitWriter) writeBit(bit uint) {
	w.writeBits(uint64(bit&1), 1)
}

// writeSigned writes value as a two's complement integer occupying
// numBits bits. Bits above numBits are discarded — the caller must
// ensure value fits in the requested width.
func (w *qwpBitWriter) writeSigned(value int64, numBits int) {
	w.writeBits(uint64(value), numBits)
}

// finish flushes a trailing partial byte, padding the high bits with
// zero. Safe to call when already byte-aligned.
func (w *qwpBitWriter) finish() {
	if w.bitsInBuffer > 0 {
		w.wb.putByte(byte(w.bitBuffer))
		w.bitBuffer = 0
		w.bitsInBuffer = 0
	}
}

// qwpGorillaBucket selects a DoD bucket. Return values:
//
//	0 = 1-bit   (DoD == 0,          prefix "0")
//	1 = 9-bit   (DoD in [-64, 63],  prefix "10"   + 7 signed)
//	2 = 12-bit  (DoD in [-256, 255], prefix "110"  + 9 signed)
//	3 = 16-bit  (DoD in [-2048, 2047], prefix "1110" + 12 signed)
//	4 = 36-bit  (otherwise,         prefix "1111" + 32 signed)
func qwpGorillaBucket(dod int64) int {
	switch {
	case dod == 0:
		return 0
	case dod >= qwpGorillaBucket7Min && dod <= qwpGorillaBucket7Max:
		return 1
	case dod >= qwpGorillaBucket9Min && dod <= qwpGorillaBucket9Max:
		return 2
	case dod >= qwpGorillaBucket12Min && dod <= qwpGorillaBucket12Max:
		return 3
	default:
		return 4
	}
}

// qwpGorillaBitsRequired returns the total bit count (prefix + value)
// to encode a single DoD.
func qwpGorillaBitsRequired(dod int64) int {
	switch qwpGorillaBucket(dod) {
	case 0:
		return 1
	case 1:
		return 9
	case 2:
		return 12
	case 3:
		return 16
	default:
		return 36
	}
}

// qwpGorillaEncoder emits Gorilla-compressed timestamps into a
// qwpWireBuffer. The encoder is stateless between calls — only the
// bit writer is retained for reuse.
type qwpGorillaEncoder struct {
	bw qwpBitWriter
}

// encodeDoD writes a delta-of-delta. Prefix bits are issued LSB-first:
// "10" on the wire is emitted as the 2-bit value 0b01, "110" as 0b011,
// and so on. The decoder reads them in the opposite order and observes
// the spec-defined patterns.
func (e *qwpGorillaEncoder) encodeDoD(dod int64) {
	switch qwpGorillaBucket(dod) {
	case 0:
		e.bw.writeBit(0)
	case 1:
		e.bw.writeBits(0b01, 2)
		e.bw.writeSigned(dod, 7)
	case 2:
		e.bw.writeBits(0b011, 3)
		e.bw.writeSigned(dod, 9)
	case 3:
		e.bw.writeBits(0b0111, 4)
		e.bw.writeSigned(dod, 12)
	default:
		e.bw.writeBits(0b1111, 4)
		e.bw.writeSigned(dod, 32)
	}
}

// qwpGorillaEncodedSize validates and sizes a timestamp sequence in a
// single pass. src holds count contiguous int64 timestamps in LE byte
// order (the column-buffer layout). Returns the encoded byte length
// excluding the encoding flag byte, or -1 if any DoD falls outside
// int32 range — in which case the caller must fall back to the
// uncompressed encoding.
func qwpGorillaEncodedSize(src []byte, count int) int {
	if count == 0 {
		return 0
	}
	size := 8
	if count == 1 {
		return size
	}
	size += 8
	if count == 2 {
		return size
	}
	prevTs := int64(binary.LittleEndian.Uint64(src[8:16]))
	prevDelta := prevTs - int64(binary.LittleEndian.Uint64(src[0:8]))
	totalBits := 0
	for i := 2; i < count; i++ {
		off := i * 8
		ts := int64(binary.LittleEndian.Uint64(src[off : off+8]))
		delta := ts - prevTs
		dod := delta - prevDelta
		if dod < math.MinInt32 || dod > math.MaxInt32 {
			return -1
		}
		totalBits += qwpGorillaBitsRequired(dod)
		prevDelta = delta
		prevTs = ts
	}
	size += (totalBits + 7) / 8
	return size
}

// encodeTimestamps emits a timestamp column's Gorilla payload to wb.
// Layout per QWP spec §12 (Gorilla mode):
//
//	count == 0: nothing
//	count == 1: ts[0] as int64 LE
//	count == 2: ts[0], ts[1] as int64 LE
//	count >= 3: ts[0], ts[1], bit-packed DoDs for ts[2..count-1]
//
// src holds count contiguous int64 timestamps in LE byte order. The
// encoding flag byte is NOT written — the caller is responsible for
// prefixing 0x01 (qwpTsEncodingGorilla) before invoking this method.
//
// Precondition: qwpGorillaEncodedSize(src, count) must have returned
// a non-negative value. If any DoD exceeds int32 range, it is
// silently truncated, producing output that decodes to corrupt
// timestamps.
//
// Returns the number of bytes appended.
func (e *qwpGorillaEncoder) encodeTimestamps(wb *qwpWireBuffer, src []byte, count int) int {
	if count == 0 {
		return 0
	}
	startLen := wb.len()
	ts0 := int64(binary.LittleEndian.Uint64(src[0:8]))
	wb.putInt64LE(ts0)
	if count == 1 {
		return wb.len() - startLen
	}
	ts1 := int64(binary.LittleEndian.Uint64(src[8:16]))
	wb.putInt64LE(ts1)
	if count == 2 {
		return wb.len() - startLen
	}
	e.bw.reset(wb)
	prevTs := ts1
	prevDelta := ts1 - ts0
	for i := 2; i < count; i++ {
		off := i * 8
		ts := int64(binary.LittleEndian.Uint64(src[off : off+8]))
		delta := ts - prevTs
		dod := delta - prevDelta
		e.encodeDoD(dod)
		prevDelta = delta
		prevTs = ts
	}
	e.bw.finish()
	return wb.len() - startLen
}
