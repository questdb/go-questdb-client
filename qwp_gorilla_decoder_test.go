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
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"
)

// --- qwpBitReader ---

func TestQwpBitReaderLSBFirstRoundTrip(t *testing.T) {
	// Inverse of TestQwpBitWriterLSBFirst: write bits 1,0,1,1 — the
	// writer packs them LSB-first into byte 0x0D. Reading them back in
	// the same order must return the same sequence.
	var wb qwpWireBuffer
	var bw qwpBitWriter
	bw.reset(&wb)
	bw.writeBit(1)
	bw.writeBit(0)
	bw.writeBit(1)
	bw.writeBit(1)
	bw.finish()

	var br qwpBitReader
	br.reset(wb.bytes())
	for i, want := range []uint64{1, 0, 1, 1} {
		got, err := br.readBit()
		if err != nil {
			t.Fatalf("readBit[%d]: %v", i, err)
		}
		if got != want {
			t.Fatalf("readBit[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestQwpBitReaderSpanningBytes(t *testing.T) {
	// Mirror of TestQwpBitWriterSpanningBytes: writer emits 0xABC in
	// 12 bits across bytes [0xBC, 0x0A]. Reading 12 bits must return
	// the same value.
	var wb qwpWireBuffer
	var bw qwpBitWriter
	bw.reset(&wb)
	bw.writeBits(0xABC, 12)
	bw.finish()

	var br qwpBitReader
	br.reset(wb.bytes())
	got, err := br.readBits(12)
	if err != nil {
		t.Fatalf("readBits: %v", err)
	}
	if got != 0xABC {
		t.Fatalf("readBits(12) = %#X, want 0xABC", got)
	}
}

func TestQwpBitReaderSignedSignExtension(t *testing.T) {
	// 7-bit field with bit 6 set must read back as -1. Encode it LSB-
	// first as 0x7F (seven ones).
	var wb qwpWireBuffer
	var bw qwpBitWriter
	bw.reset(&wb)
	bw.writeSigned(-1, 7)
	bw.finish()
	var br qwpBitReader
	br.reset(wb.bytes())
	got, err := br.readSigned(7)
	if err != nil {
		t.Fatalf("readSigned(7): %v", err)
	}
	if got != -1 {
		t.Fatalf("readSigned(7) = %d, want -1", got)
	}

	// 12-bit field with only bit 11 set = 0x800 — most-negative value
	// -2048.
	wb.reset()
	bw.reset(&wb)
	bw.writeSigned(-2048, 12)
	bw.finish()
	br.reset(wb.bytes())
	got, err = br.readSigned(12)
	if err != nil {
		t.Fatalf("readSigned(12): %v", err)
	}
	if got != -2048 {
		t.Fatalf("readSigned(12) = %d, want -2048", got)
	}
}

func TestQwpBitReaderTruncated(t *testing.T) {
	// One byte supplied, read 16 bits → error.
	var br qwpBitReader
	br.reset([]byte{0xFF})
	_, err := br.readBits(16)
	if err == nil {
		t.Fatalf("expected error reading past end")
	}
	var de *qwpDecodeError
	if !errors.As(err, &de) {
		t.Fatalf("expected *qwpDecodeError, got %T", err)
	}
}

func TestQwpBitReaderOutOfRangeBitCount(t *testing.T) {
	// Guard against n<0 and n>64 — caller bugs would otherwise return
	// garbage (mask computation relies on 1 <= n <= 64). n=0 is a
	// no-op success path (matches Java contract); see
	// TestQwpBitReaderReadBitsZeroIsNoop.
	var br qwpBitReader
	br.reset([]byte{0xFF})
	for _, n := range []int{-1, 65, 100} {
		_, err := br.readBits(n)
		if err == nil {
			t.Fatalf("readBits(%d) should error", n)
		}
	}
}

func TestQwpBitReaderReadBitsZeroIsNoop(t *testing.T) {
	// Mirror of Java's testReadBitsZeroBitsReturnsZeroWithoutAdvancing:
	// a zero-width read must yield 0 and leave the bit position
	// unchanged so the next read still sees byte 0 intact.
	var br qwpBitReader
	br.reset([]byte{0xFF})
	got, err := br.readBits(0)
	if err != nil {
		t.Fatalf("readBits(0): %v", err)
	}
	if got != 0 {
		t.Fatalf("readBits(0) = %d, want 0", got)
	}
	if br.bitsRead != 0 {
		t.Fatalf("bitsRead after readBits(0) = %d, want 0", br.bitsRead)
	}
	if bit, err := br.readBit(); err != nil || bit != 1 {
		t.Fatalf("readBit after readBits(0) = (%d, %v), want (1, nil)", bit, err)
	}
}

func TestQwpBitReaderReadBits64FullWord(t *testing.T) {
	// 64-bit read must use the branchless mask path (^uint64(0) for
	// n=64) and reproduce the input verbatim. Mirror of Java's
	// testReadBits64ReadsFullWord.
	value := uint64(0x0123456789ABCDEF)
	src := make([]byte, 8)
	binary.LittleEndian.PutUint64(src, value)

	var br qwpBitReader
	br.reset(src)
	got, err := br.readBits(64)
	if err != nil {
		t.Fatalf("readBits(64): %v", err)
	}
	if got != value {
		t.Fatalf("readBits(64) = %#x, want %#x", got, value)
	}
	if br.bitsRead != 64 {
		t.Fatalf("bitsRead = %d, want 64", br.bitsRead)
	}
}

func TestQwpBitReaderReadBits64TwiceDoesNotLeakStaleBuffer(t *testing.T) {
	// Regression: a full-width readBits(64) must clear the
	// accumulator so the next read sees a clean slate. Java's
	// `bitBuffer >>>= 64` is a no-op (shift mod 64 == 0); the same
	// pitfall applies in Go via `r.bitBuffer >> 1 >> 63 == bitBuffer`
	// without the chained-shift form. Two disjoint halves (8 bytes
	// of 0xFF then 8 of 0x00) catch the regression: the second 64-bit
	// read must be exactly 0, not the OR of stale all-ones with the
	// fresh zeros.
	src := make([]byte, 16)
	for i := 0; i < 8; i++ {
		src[i] = 0xFF
	}

	var br qwpBitReader
	br.reset(src)

	first, err := br.readBits(64)
	if err != nil {
		t.Fatalf("first readBits(64): %v", err)
	}
	if first != ^uint64(0) {
		t.Fatalf("first readBits(64) = %#x, want %#x", first, ^uint64(0))
	}
	if br.bitsRead != 64 {
		t.Fatalf("bitsRead after first read = %d, want 64", br.bitsRead)
	}

	second, err := br.readBits(64)
	if err != nil {
		t.Fatalf("second readBits(64): %v", err)
	}
	if second != 0 {
		t.Fatalf("second readBits(64) = %#x, want 0 (stale-buffer regression)", second)
	}
	if br.bitsRead != 128 {
		t.Fatalf("bitsRead after second read = %d, want 128", br.bitsRead)
	}
}

func TestQwpBitReaderArbitraryWidths(t *testing.T) {
	// Sequence of mixed widths within and across byte boundaries.
	// Source: 0xFF, 0x55, 0xAA, 0x00 (32 bits).
	// LSB-first decoding of byte 0xFF: 5 bits = 0b11111 = 0x1F,
	// then remaining 3 bits = 0b111 = 0x7. Byte 0x55 read whole
	// = 0x55. Last 16 bits combine 0xAA (low) and 0x00 (high) =
	// 0x00AA. Mirrors Java's testReadBitsArbitraryWidths.
	src := []byte{0xFF, 0x55, 0xAA, 0x00}

	var br qwpBitReader
	br.reset(src)

	got, err := br.readBits(5)
	if err != nil || got != 0x1F {
		t.Fatalf("readBits(5) = (%#x, %v), want (0x1F, nil)", got, err)
	}
	got, err = br.readBits(3)
	if err != nil || got != 0x7 {
		t.Fatalf("readBits(3) = (%#x, %v), want (0x7, nil)", got, err)
	}
	if br.bitsRead != 8 {
		t.Fatalf("bitsRead after byte 0 = %d, want 8", br.bitsRead)
	}
	got, err = br.readBits(8)
	if err != nil || got != 0x55 {
		t.Fatalf("readBits(8) = (%#x, %v), want (0x55, nil)", got, err)
	}
	got, err = br.readBits(16)
	if err != nil || got != 0x00AA {
		t.Fatalf("readBits(16) = (%#x, %v), want (0x00AA, nil)", got, err)
	}
	if br.bitsRead != 32 {
		t.Fatalf("bitsRead final = %d, want 32", br.bitsRead)
	}
}

func TestQwpBitReaderSpansSlowPathRefills(t *testing.T) {
	// 24-bit read must traverse the refill loop in readBitsSlow more
	// than once when the accumulator is empty and the source has
	// fewer than 8 bytes (forces the byte-by-byte refill branch).
	// LSB-first across [0x01, 0x02, 0x03, 0x00] = 0x030201.
	src := []byte{0x01, 0x02, 0x03, 0x00}

	var br qwpBitReader
	br.reset(src)
	got, err := br.readBits(24)
	if err != nil {
		t.Fatalf("readBits(24): %v", err)
	}
	if got != 0x030201 {
		t.Fatalf("readBits(24) = %#x, want 0x030201", got)
	}
	if br.bitsRead != 24 {
		t.Fatalf("bitsRead = %d, want 24", br.bitsRead)
	}
}

func TestQwpBitReaderMultiRefillAcrossLargeBuffer(t *testing.T) {
	// Walk a 16-byte buffer with a sequence of widths summing to
	// 128 bits. Each width forces an accumulator refill at a
	// different boundary point, and the trailing readBit must
	// surface the past-end error. Mirror of Java's
	// testReadBitsAcrossLargeRefill.
	src := make([]byte, 16)
	for i := range src {
		src[i] = byte(i)
	}

	var br qwpBitReader
	br.reset(src)
	widths := []int{1, 7, 13, 19, 23, 33, 32}
	totalBits := int64(0)
	for _, w := range widths {
		if _, err := br.readBits(w); err != nil {
			t.Fatalf("readBits(%d): %v", w, err)
		}
		totalBits += int64(w)
		if br.bitsRead != totalBits {
			t.Fatalf("bitsRead after readBits(%d) = %d, want %d", w, br.bitsRead, totalBits)
		}
	}
	if _, err := br.readBit(); err == nil {
		t.Fatalf("readBit after exhausting 128 bits should error")
	}
}

func TestQwpBitReaderSignedDoesNotExtendWhenMsbClear(t *testing.T) {
	// 5-bit field with MSB clear: encode +5 (0b00101), read back as
	// +5 — sign-extension must NOT fire for MSB=0. Mirrors Java's
	// testReadSignedDoesNotExtendWhenMsbClear.
	var br qwpBitReader
	br.reset([]byte{0b00000101})
	got, err := br.readSigned(5)
	if err != nil {
		t.Fatalf("readSigned(5): %v", err)
	}
	if got != 5 {
		t.Fatalf("readSigned(5) = %d, want 5", got)
	}
}

func TestQwpBitReaderSigned64BitsBehavesLikeReadBits(t *testing.T) {
	// readSigned(64) special-cases the sign-extend so the value
	// already occupies the full int64 unchanged. Mirror of Java's
	// testReadSigned64BitsBehavesLikeReadBits.
	want := int64(-0x0011223344556678) // i.e. 0xFFEEDDCCBBAA9988
	src := make([]byte, 8)
	binary.LittleEndian.PutUint64(src, uint64(want))

	var br qwpBitReader
	br.reset(src)
	got, err := br.readSigned(64)
	if err != nil {
		t.Fatalf("readSigned(64): %v", err)
	}
	if got != want {
		t.Fatalf("readSigned(64) = %d, want %d", got, want)
	}
}

func TestQwpBitReaderResetClearsAllState(t *testing.T) {
	// After a partial read on buffer 1, reset(buffer2) must reseed
	// the position to 0 and force the first read to come from
	// buffer2 — not from leftover bits in the accumulator. Mirror of
	// Java's testResetClearsAllState.
	var br qwpBitReader
	br.reset([]byte{0xAB, 0xCD})
	if _, err := br.readBits(10); err != nil {
		t.Fatalf("readBits(10): %v", err)
	}
	if br.bitsRead != 10 {
		t.Fatalf("bitsRead after first run = %d, want 10", br.bitsRead)
	}

	br.reset([]byte{0x12, 0x34})
	if br.bitsRead != 0 {
		t.Fatalf("bitsRead after reset = %d, want 0", br.bitsRead)
	}
	if br.bitsAvail != 0 || br.bitBuffer != 0 || br.pos != 0 {
		t.Fatalf("residual state after reset: bitsAvail=%d bitBuffer=%#x pos=%d",
			br.bitsAvail, br.bitBuffer, br.pos)
	}
	got, err := br.readBits(8)
	if err != nil {
		t.Fatalf("readBits(8) after reset: %v", err)
	}
	if got != 0x12 {
		t.Fatalf("readBits(8) after reset = %#x, want 0x12", got)
	}
	if br.bitsRead != 8 {
		t.Fatalf("bitsRead = %d, want 8", br.bitsRead)
	}
}

// --- qwpGorillaDecoder ---

func TestQwpGorillaDecoderBitPositionAfterDecode(t *testing.T) {
	// For a constant-delta series (every DoD = 0), each non-prefix
	// value contributes exactly 1 bit to the stream. The pre-computed
	// encoder size must match the decoder's bytesConsumed().
	ts := []int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	src := intsToBytes(ts)
	preSize := qwpGorillaEncodedSize(src, len(ts))

	var wb qwpWireBuffer
	var enc qwpGorillaEncoder
	n := enc.encodeTimestamps(&wb, src, len(ts))
	if n != preSize {
		t.Fatalf("encoder size %d != pre-computed %d", n, preSize)
	}

	var dec qwpGorillaDecoder
	dec.reset(ts[0], ts[1], wb.bytes()[16:])
	for i := 2; i < len(ts); i++ {
		if _, err := dec.decodeNext(); err != nil {
			t.Fatalf("decodeNext[%d]: %v", i, err)
		}
	}
	// Total stream length after the 16-byte prefix must equal the
	// decoder's accounting.
	wantTrailer := len(wb.bytes()) - 16
	if got := dec.bytesConsumed(); got != wantTrailer {
		t.Fatalf("bytesConsumed = %d, want %d", got, wantTrailer)
	}
}

func TestQwpGorillaDecoderTruncatedBitstream(t *testing.T) {
	// Encode a series that needs a wide DoD bucket so the bitstream is
	// long enough to lose bytes from. Then chop the final byte and
	// decode — at some point the reader must error.
	ts := []int64{
		0,
		1_000_000,
		3_000_000,
		3_000_001,
		3_000_002,
		3_000_003,
	}
	src := intsToBytes(ts)
	var wb qwpWireBuffer
	var enc qwpGorillaEncoder
	enc.encodeTimestamps(&wb, src, len(ts))
	truncated := wb.bytes()[:len(wb.bytes())-1]
	if len(truncated) < 16 {
		t.Fatalf("truncated smaller than prefix: %d bytes", len(truncated))
	}

	var dec qwpGorillaDecoder
	dec.reset(ts[0], ts[1], truncated[16:])
	var err error
	for i := 2; i < len(ts); i++ {
		_, err = dec.decodeNext()
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatalf("expected error from truncated bitstream")
	}
	var de *qwpDecodeError
	if !errors.As(err, &de) {
		t.Fatalf("expected *qwpDecodeError, got %T", err)
	}
}

func TestQwpGorillaDecoderRoundTripAllBuckets(t *testing.T) {
	// Drive one roundtrip per DoD bucket to confirm the decoder handles
	// every prefix branch. Distinct from the encoder-side boundary
	// tests — those only ensure the encoder emits correct bits. This
	// test specifically exercises the production decoder's prefix tree.
	cases := []struct {
		name string
		dod  int64
	}{
		{"bucket0", 0},
		{"bucket1_pos", 5},
		{"bucket1_neg", -10},
		{"bucket2_pos", 100},
		{"bucket2_neg", -200},
		{"bucket3_pos", 500},
		{"bucket3_neg", -1500},
		{"bucket4_pos", 100_000},
		{"bucket4_neg", -500_000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts := []int64{0, 10_000, 10_000 + 10_000 + c.dod}
			src := intsToBytes(ts)
			var wb qwpWireBuffer
			var enc qwpGorillaEncoder
			enc.encodeTimestamps(&wb, src, len(ts))

			var dec qwpGorillaDecoder
			dec.reset(ts[0], ts[1], wb.bytes()[16:])
			got, err := dec.decodeNext()
			if err != nil {
				t.Fatalf("decodeNext: %v", err)
			}
			if got != ts[2] {
				t.Fatalf("decoded %d, want %d", got, ts[2])
			}
		})
	}
}

func TestQwpGorillaDecoderRoundTripRandom(t *testing.T) {
	// Analogue of the encoder-side random round-trip: drives several
	// buckets in one bitstream and confirms decoder state threading
	// (prevTs, prevDelta) is correct across all of them.
	r := rand.New(rand.NewSource(0xBADFACE))
	ts := make([]int64, 256)
	cur := int64(0)
	delta := int64(1000)
	for i := range ts {
		delta += int64(r.Intn(2001) - 1000)
		cur += delta
		ts[i] = cur
	}
	src := intsToBytes(ts)
	var wb qwpWireBuffer
	var enc qwpGorillaEncoder
	enc.encodeTimestamps(&wb, src, len(ts))

	var dec qwpGorillaDecoder
	dec.reset(ts[0], ts[1], wb.bytes()[16:])
	for i := 2; i < len(ts); i++ {
		got, err := dec.decodeNext()
		if err != nil {
			t.Fatalf("decodeNext[%d]: %v", i, err)
		}
		if got != ts[i] {
			t.Fatalf("ts[%d] = %d, want %d", i, got, ts[i])
		}
	}
}

func TestQwpGorillaDecoderDecodePastEndOfEmptyBitstream(t *testing.T) {
	// Reset to a zero-length bitstream and verify decodeNext surfaces
	// the bit reader's "past end of buffer" error on the very first
	// call. Asking for a value when there are no bytes at all is the
	// unambiguous past-end case (a trailing-zero pattern would
	// resemble a valid 1-bit "DoD == 0" prefix). Mirror of Java's
	// testDecodePastEndOfEmptyBitstreamThrows.
	var dec qwpGorillaDecoder
	dec.reset(0, 100, nil)
	_, err := dec.decodeNext()
	if err == nil {
		t.Fatalf("decodeNext on empty bitstream must error")
	}
	var de *qwpDecodeError
	if !errors.As(err, &de) {
		t.Fatalf("expected *qwpDecodeError, got %T: %v", err, err)
	}
}

func TestQwpGorillaDecoderDecodePastEndOfLargeBucketBitstream(t *testing.T) {
	// Encode a sequence whose DoDs land in the 36-bit fallback bucket
	// (each emitted value consumes a known multi-byte chunk). After
	// decoding the encoded values, keep asking for more until the
	// past-end check fires. The cap is generous (64 spurious calls)
	// — the trailing bit pattern of the last byte determines exactly
	// when the reader runs out of payload, so we loop until it does.
	// Mirror of Java's testDecodePastEndOfLargeBucketBitstreamThrows.
	ts := []int64{1_000_000, 2_000_000, 3_500_000, 7_000_000}
	src := intsToBytes(ts)
	var wb qwpWireBuffer
	var enc qwpGorillaEncoder
	enc.encodeTimestamps(&wb, src, len(ts))

	var dec qwpGorillaDecoder
	dec.reset(ts[0], ts[1], wb.bytes()[16:])
	for i := 2; i < len(ts); i++ {
		got, err := dec.decodeNext()
		if err != nil {
			t.Fatalf("decodeNext[%d]: %v", i, err)
		}
		if got != ts[i] {
			t.Fatalf("decodeNext[%d] = %d, want %d", i, got, ts[i])
		}
	}

	var seenErr error
	for i := 0; i < 64; i++ {
		if _, err := dec.decodeNext(); err != nil {
			seenErr = err
			break
		}
	}
	if seenErr == nil {
		t.Fatalf("decodeNext past end must eventually error")
	}
	var de *qwpDecodeError
	if !errors.As(seenErr, &de) {
		t.Fatalf("expected *qwpDecodeError past end, got %T: %v", seenErr, seenErr)
	}
}

func TestQwpGorillaDecoderResetClearsResidualState(t *testing.T) {
	// After one decode run, a fresh reset must zero the bit buffer,
	// bitsAvail, and pos — residual bits from the first stream would
	// otherwise prepend garbage to the second decode.
	ts1 := []int64{100, 200, 300, 400}
	ts2 := []int64{1_000_000, 1_000_005, 1_000_015, 1_000_030}
	var wb1, wb2 qwpWireBuffer
	var enc qwpGorillaEncoder
	enc.encodeTimestamps(&wb1, intsToBytes(ts1), len(ts1))
	enc.encodeTimestamps(&wb2, intsToBytes(ts2), len(ts2))

	var dec qwpGorillaDecoder
	// Run 1: decode two values so the bit buffer is non-empty at exit.
	dec.reset(ts1[0], ts1[1], wb1.bytes()[16:])
	if _, err := dec.decodeNext(); err != nil {
		t.Fatalf("run1 decodeNext[2]: %v", err)
	}
	// Run 2: reset to the new bitstream and verify the full sequence.
	dec.reset(ts2[0], ts2[1], wb2.bytes()[16:])
	for i := 2; i < len(ts2); i++ {
		got, err := dec.decodeNext()
		if err != nil {
			t.Fatalf("run2 decodeNext[%d]: %v", i, err)
		}
		if got != ts2[i] {
			t.Fatalf("ts2[%d] = %d, want %d", i, got, ts2[i])
		}
	}
}
