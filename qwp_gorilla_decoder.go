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

import "encoding/binary"

// qwpBitReader reads bits LSB-first from a byte slice using a 64-bit
// accumulator. It is the inverse of qwpBitWriter in qwp_gorilla.go and
// is used by qwpGorillaDecoder to consume the delta-of-delta bitstream
// emitted by the encoder.
//
// Refills go through a single 8-byte little-endian load whenever the
// source has 8 bytes available, falling back to a byte-by-byte tail for
// the last <8 bytes of the buffer. The Gorilla DoD path issues several
// single-bit reads followed by a wide signed payload per row; once the
// accumulator is loaded, all reads up to its 64-bit capacity hit the
// fast path (a single shift+mask) without touching the source slice.
//
// Error model: every read returns *qwpDecodeError (via
// newQwpDecodeError) when the underlying byte slice is exhausted before
// the requested bits are available. The decoder caller bubbles these up
// as a decode failure on the enclosing RESULT_BATCH frame.
type qwpBitReader struct {
	data      []byte
	bitBuffer uint64 // accumulator; bits are LSB-aligned, count is bitsAvail
	bitsAvail int    // bits currently held in bitBuffer; in [0, 64]
	pos       int    // index of the next byte to load from data
	bitsRead  int64  // total bits consumed since reset
}

// reset rebinds the reader to a new byte slice and zeroes all residual
// state. Safe to call before every column decode so leftovers from a
// prior column never bleed in.
func (r *qwpBitReader) reset(data []byte) {
	r.data = data
	r.bitBuffer = 0
	r.bitsAvail = 0
	r.pos = 0
	r.bitsRead = 0
}

// bytesConsumed returns ceil(bitsRead / 8) — the byte count of the
// bitstream region read so far, rounded up to the next byte boundary.
// Matches the encoder's byte-aligned output (qwpBitWriter.finish
// always pads trailing bits with zeros to a full byte). The 8-byte
// fast-path refill may speculatively load bytes beyond the bits the
// caller actually consumes; bytesConsumed reflects bits read, not
// bytes loaded, so it remains a faithful cursor for the outer reader.
func (r *qwpBitReader) bytesConsumed() int { return int((r.bitsRead + 7) >> 3) }

// readBit reads a single bit, LSB-first within each source byte.
// Specialised so the hot Gorilla prefix-decoding path stays inlinable
// when the accumulator is already populated — the common case after
// the first refill.
func (r *qwpBitReader) readBit() (uint64, error) {
	if r.bitsAvail >= 1 {
		bit := r.bitBuffer & 1
		r.bitBuffer >>= 1
		r.bitsAvail--
		r.bitsRead++
		return bit, nil
	}
	return r.readBits(1)
}

// readBits reads the low n bits of the stream and returns them
// LSB-aligned in a uint64. n must be in [0, 64]. n == 0 returns 0
// without consuming any bits, matching the Java QwpBitReader contract
// — callers in the decoder occasionally pass a width derived from a
// runtime computation and rely on the zero case being a no-op.
//
// Mask construction is branchless via `^uint64(0) >> (64 - n)`: for n
// in [1, 64] the shift count is in [0, 63] and the result is the
// expected n-bit mask, with no n == 64 special case (`uint64(1) << 64`
// is 0 in Go, which would make the obvious `(1 << n) - 1` form wrong).
// The accumulator drain uses the same idea via two chained shifts so
// the inner shift count is always in [0, 63] and Go does not have to
// emit a runtime guard for shift-by-width.
func (r *qwpBitReader) readBits(n int) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	if n < 0 || n > 64 {
		return 0, newQwpDecodeError("bit count out of range")
	}
	if r.bitsAvail >= n {
		mask := ^uint64(0) >> (64 - n)
		result := r.bitBuffer & mask
		r.bitBuffer = (r.bitBuffer >> 1) >> (n - 1)
		r.bitsAvail -= n
		r.bitsRead += int64(n)
		return result, nil
	}
	return r.readBitsSlow(n)
}

// readBitsSlow is the cold path for reads that span a refill. Each
// iteration drains whatever's already in the accumulator, then refills
// — preferring an 8-byte LE load when 8 source bytes are available,
// falling back to a 1-byte load for the tail. Multi-iteration logic is
// only exercised when n exceeds the bits already buffered (which, after
// the first refill, can be at most one extra iteration since a single
// 64-bit accumulator load satisfies any n <= 64).
//
// Mask construction and accumulator drain use the same branchless
// idioms as readBits — `take` is in [1, 64] inside the loop body, so
// `^uint64(0) >> (64 - take)` and `(buf >> 1) >> (take - 1)` both have
// shift counts in [0, 63] and need no runtime guard.
func (r *qwpBitReader) readBitsSlow(n int) (uint64, error) {
	var result uint64
	shift := 0
	remaining := n
	for remaining > 0 {
		if r.bitsAvail == 0 {
			if r.pos+8 <= len(r.data) {
				r.bitBuffer = binary.LittleEndian.Uint64(r.data[r.pos:])
				r.pos += 8
				r.bitsAvail = 64
			} else if r.pos < len(r.data) {
				r.bitBuffer = uint64(r.data[r.pos])
				r.pos++
				r.bitsAvail = 8
			} else {
				return 0, newQwpDecodeError("bit read past end of buffer")
			}
		}
		take := remaining
		if take > r.bitsAvail {
			take = r.bitsAvail
		}
		mask := ^uint64(0) >> (64 - take)
		result |= (r.bitBuffer & mask) << shift
		r.bitBuffer = (r.bitBuffer >> 1) >> (take - 1)
		r.bitsAvail -= take
		shift += take
		remaining -= take
	}
	r.bitsRead += int64(n)
	return result, nil
}

// readSigned reads n bits as a two's complement signed integer,
// sign-extending bit n-1 into the rest of the result.
func (r *qwpBitReader) readSigned(n int) (int64, error) {
	u, err := r.readBits(n)
	if err != nil {
		return 0, err
	}
	if n < 64 && u&(uint64(1)<<(n-1)) != 0 {
		u |= ^uint64(0) << n
	}
	return int64(u), nil
}

// qwpGorillaDecoder reverses qwpGorillaEncoder: it consumes a delta-of-
// delta bitstream (without the two leading raw timestamps — the caller
// reads those out of band and passes them to reset) and yields one
// int64 timestamp per decodeNext call.
//
// Mirror of the Java QwpGorillaDecoder. Buckets and prefix patterns:
//
//	"0"            → DoD = 0                     (1 bit)
//	"10"  + s7     → DoD in [-64, 63]            (9 bits)
//	"110" + s9     → DoD in [-256, 255]          (12 bits)
//	"1110"+ s12    → DoD in [-2048, 2047]        (16 bits)
//	"1111"+ s32    → any other DoD               (36 bits)
//
// Prefix bits are read LSB-first, so the encoder's 0b01 for "10" is
// observed here as readBit=1 then readBit=0 — the leading 1 falls past
// the "b==0 → DoD=0" check, and the trailing 0 selects the 7-bit
// signed payload for the "10" bucket.
type qwpGorillaDecoder struct {
	br        qwpBitReader
	prevTs    int64
	prevDelta int64
}

// reset seeds the decoder with the two leading timestamps (read by the
// caller from the uncompressed prefix of the column's wire bytes) and
// the bitstream that follows them. After reset, the caller invokes
// decodeNext exactly nonNull-2 times; the first two timestamps are
// already known and returned outside this decoder.
func (d *qwpGorillaDecoder) reset(firstTs, secondTs int64, bitstream []byte) {
	d.prevTs = secondTs
	d.prevDelta = secondTs - firstTs
	d.br.reset(bitstream)
}

// decodeNext decodes one timestamp and advances the decoder's rolling
// state (prevTs, prevDelta). Errors bubble up as *qwpDecodeError from
// qwpBitReader when the bitstream is truncated.
func (d *qwpGorillaDecoder) decodeNext() (int64, error) {
	dod, err := d.decodeDoD()
	if err != nil {
		return 0, err
	}
	delta := d.prevDelta + dod
	ts := d.prevTs + delta
	d.prevDelta = delta
	d.prevTs = ts
	return ts, nil
}

// bytesConsumed proxies the underlying bit reader's byte accounting.
// Used by the RESULT_BATCH column parser to advance the outer byte
// reader past the bitstream region once decoding finishes.
func (d *qwpGorillaDecoder) bytesConsumed() int { return d.br.bytesConsumed() }

// decodeDoD walks the bucket prefix tree. Each successive readBit
// refines the bucket; once a 0 bit or the all-ones path terminates the
// prefix, the remaining signed payload is read and returned.
func (d *qwpGorillaDecoder) decodeDoD() (int64, error) {
	b, err := d.br.readBit()
	if err != nil {
		return 0, err
	}
	if b == 0 {
		return 0, nil
	}
	b, err = d.br.readBit()
	if err != nil {
		return 0, err
	}
	if b == 0 {
		return d.br.readSigned(7)
	}
	b, err = d.br.readBit()
	if err != nil {
		return 0, err
	}
	if b == 0 {
		return d.br.readSigned(9)
	}
	b, err = d.br.readBit()
	if err != nil {
		return 0, err
	}
	if b == 0 {
		return d.br.readSigned(12)
	}
	return d.br.readSigned(32)
}
