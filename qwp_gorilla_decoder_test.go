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
	// Guard against n=0 and n>64 — caller bugs would otherwise return
	// garbage (mask computation relies on 1 <= n <= 64).
	var br qwpBitReader
	br.reset([]byte{0xFF})
	for _, n := range []int{0, -1, 65, 100} {
		_, err := br.readBits(n)
		if err == nil {
			t.Fatalf("readBits(%d) should error", n)
		}
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
