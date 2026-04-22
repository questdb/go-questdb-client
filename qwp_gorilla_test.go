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
	"math"
	"math/rand"
	"testing"
)

func TestQwpGorillaBucketBoundaries(t *testing.T) {
	cases := []struct {
		dod  int64
		want int
	}{
		{0, 0},
		{1, 1}, {-1, 1}, {63, 1}, {-64, 1},
		{64, 2}, {-65, 2}, {255, 2}, {-256, 2},
		{256, 3}, {-257, 3}, {2047, 3}, {-2048, 3},
		{2048, 4}, {-2049, 4}, {math.MaxInt32, 4}, {math.MinInt32, 4},
	}
	for _, c := range cases {
		if got := qwpGorillaBucket(c.dod); got != c.want {
			t.Errorf("qwpGorillaBucket(%d) = %d, want %d", c.dod, got, c.want)
		}
	}
}

func TestQwpGorillaBitsRequired(t *testing.T) {
	cases := []struct {
		dod  int64
		want int
	}{
		{0, 1},
		{63, 9}, {-64, 9},
		{255, 12}, {-256, 12},
		{2047, 16}, {-2048, 16},
		{2048, 36}, {-2049, 36},
	}
	for _, c := range cases {
		if got := qwpGorillaBitsRequired(c.dod); got != c.want {
			t.Errorf("qwpGorillaBitsRequired(%d) = %d, want %d", c.dod, got, c.want)
		}
	}
}

func TestQwpBitWriterLSBFirst(t *testing.T) {
	var wb qwpWireBuffer
	var bw qwpBitWriter
	bw.reset(&wb)
	// Write bits 1,0,1,1 — expect byte 0b00001101 = 0x0D
	bw.writeBit(1)
	bw.writeBit(0)
	bw.writeBit(1)
	bw.writeBit(1)
	bw.finish()
	if len(wb.bytes()) != 1 || wb.bytes()[0] != 0x0D {
		t.Fatalf("LSB-first bit layout: got % X, want [0D]", wb.bytes())
	}
}

func TestQwpBitWriterByteBoundary(t *testing.T) {
	var wb qwpWireBuffer
	var bw qwpBitWriter
	bw.reset(&wb)
	bw.writeBits(0xFF, 8) // exactly one byte, no trailing partial
	bw.finish()
	if got := wb.bytes(); len(got) != 1 || got[0] != 0xFF {
		t.Fatalf("aligned 8 bits: got % X, want [FF]", got)
	}
}

func TestQwpBitWriterSpanningBytes(t *testing.T) {
	var wb qwpWireBuffer
	var bw qwpBitWriter
	bw.reset(&wb)
	// 12 bits: low 8 go to byte 0, high 4 go to byte 1's low nibble.
	bw.writeBits(0xABC, 12)
	bw.finish()
	got := wb.bytes()
	// 0xABC = 0b101010111100. LSB first:
	// byte 0 bits 0..7 = 0b10111100 = 0xBC
	// byte 1 bits 0..3 = 0b1010 = 0x0A
	if len(got) != 2 || got[0] != 0xBC || got[1] != 0x0A {
		t.Fatalf("spanning write: got % X, want [BC 0A]", got)
	}
}

func TestQwpBitWriterSignedNegative(t *testing.T) {
	var wb qwpWireBuffer
	var bw qwpBitWriter
	bw.reset(&wb)
	bw.writeSigned(-1, 7) // 7-bit 2's complement = 0b1111111 (7 bits set)
	bw.finish()
	got := wb.bytes()
	// LSB-first 7 ones in a byte = 0b01111111 = 0x7F
	if len(got) != 1 || got[0] != 0x7F {
		t.Fatalf("signed -1 in 7 bits: got % X, want [7F]", got)
	}
}

// Hand-computed layout: ts = [0, 10, 25]. DoD for index 2 is 5, in the
// 7-bit bucket, encoded as prefix "10" + 7-bit signed(5).
//
//	Bits written, in order (LSB-first within each byte):
//	  1, 0            — bucket prefix "10"
//	  1, 0, 1, 0, 0, 0, 0 — value 5 in 7 bits, low bit first
//	Packed into byte 0: 0b00010101 = 0x15, plus trailing byte 0x00
//	for the 9th bit (value bit 6 = 0, zero-padded high).
func TestQwpGorillaEncodeKnownVector7Bit(t *testing.T) {
	ts := []int64{0, 10, 25}
	src := intsToBytes(ts)

	var wb qwpWireBuffer
	var enc qwpGorillaEncoder
	n := enc.encodeTimestamps(&wb, src, len(ts))

	expected := []byte{
		0, 0, 0, 0, 0, 0, 0, 0, // ts0 = 0
		10, 0, 0, 0, 0, 0, 0, 0, // ts1 = 10
		0x15, 0x00, // DoD=5: "10" + signed7(5), padded
	}
	if n != len(expected) {
		t.Fatalf("returned size %d, want %d", n, len(expected))
	}
	if string(wb.bytes()) != string(expected) {
		t.Fatalf("encoded bytes:\n got % X\nwant % X", wb.bytes(), expected)
	}
}

func TestQwpGorillaEncodeAllZeroDoDs(t *testing.T) {
	// Constant-delta series: DoDs all zero → each index 2..N-1 costs 1 bit.
	ts := []int64{1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000}
	src := intsToBytes(ts)
	var wb qwpWireBuffer
	var enc qwpGorillaEncoder
	n := enc.encodeTimestamps(&wb, src, len(ts))

	// 8 DoDs × 1 bit = 8 bits → exactly one padding byte (0x00).
	want := 16 + 1
	if n != want {
		t.Fatalf("size = %d, want %d", n, want)
	}
	if got := wb.bytes()[16]; got != 0x00 {
		t.Fatalf("trailing byte = %#x, want 0x00", got)
	}
	if size := qwpGorillaEncodedSize(src, len(ts)); size != n {
		t.Fatalf("size pre-compute %d != actual %d", size, n)
	}
}

func TestQwpGorillaEncodedSizeEdges(t *testing.T) {
	if got := qwpGorillaEncodedSize(nil, 0); got != 0 {
		t.Errorf("count=0: got %d, want 0", got)
	}
	one := intsToBytes([]int64{42})
	if got := qwpGorillaEncodedSize(one, 1); got != 8 {
		t.Errorf("count=1: got %d, want 8", got)
	}
	two := intsToBytes([]int64{1, 2})
	if got := qwpGorillaEncodedSize(two, 2); got != 16 {
		t.Errorf("count=2: got %d, want 16", got)
	}
}

// Constant-delta series: every DoD is 0, so every index 2..N-1 costs a
// single bit. Size = 8 (ts0) + 8 (ts1) + ceil((N-2)/8).
// Ported from QwpGorillaEncoderTest.testCalculateEncodedSizeConstantDelta.
func TestQwpGorillaEncodedSizeConstantDelta(t *testing.T) {
	const n = 100
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = 1_000_000_000 + int64(i)*1000
	}
	src := intsToBytes(ts)
	want := 8 + 8 + (n-2+7)/8 // = 29
	if got := qwpGorillaEncodedSize(src, n); got != want {
		t.Fatalf("constant-delta size: got %d, want %d", got, want)
	}
}

// Three timestamps with identical deltas: a single DoD=0 → 1 bit → 1 byte.
// Total = 8 + 8 + 1 = 17.
// Ported from QwpGorillaEncoderTest.testCalculateEncodedSizeIdenticalDeltas.
func TestQwpGorillaEncodedSizeIdenticalDeltas(t *testing.T) {
	ts := []int64{100, 200, 300}
	src := intsToBytes(ts)
	if got := qwpGorillaEncodedSize(src, len(ts)); got != 17 {
		t.Fatalf("identical-delta size: got %d, want 17", got)
	}
}

// Three timestamps with a small (7-bit bucket) DoD: 9 bits → 2 bytes.
// deltas = [100, 150], DoD = 50. Total = 8 + 8 + 2 = 18.
// Ported from QwpGorillaEncoderTest.testCalculateEncodedSizeSmallDoD.
func TestQwpGorillaEncodedSizeSmallDoD(t *testing.T) {
	ts := []int64{100, 200, 350}
	src := intsToBytes(ts)
	if got := qwpGorillaEncodedSize(src, len(ts)); got != 18 {
		t.Fatalf("small-DoD size: got %d, want 18", got)
	}
}

// Round-trip every bucket boundary (both endpoints) so an accidental
// off-by-one in qwpGorillaBucket won't silently corrupt the wire format.
// Ported from QwpGorillaEncoderTest.testEncodeDecodeBucketBoundaries.
func TestQwpGorillaRoundTripBucketBoundaries(t *testing.T) {
	// Fixed prefix: ts0=0, ts1=10_000 → initial delta=10_000.
	// Each test case sets ts2 so the DoD hits a specific bucket boundary.
	cases := []struct {
		name string
		dod  int64
	}{
		{"bucket0_dod0", 0},
		{"bucket1_max_63", 63},
		{"bucket1_min_neg64", -64},
		{"bucket2_start_64", 64},
		{"bucket2_start_neg65", -65},
		{"bucket2_max_255", 255},
		{"bucket2_min_neg256", -256},
		{"bucket3_start_256", 256},
		{"bucket3_start_neg257", -257},
		{"bucket3_max_2047", 2047},
		{"bucket3_min_neg2048", -2048},
		{"bucket4_start_2048", 2048},
		{"bucket4_start_neg2049", -2049},
		{"bucket4_large_pos", 100_000},
		{"bucket4_large_neg", -100_000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts := []int64{0, 10_000, 10_000 + 10_000 + c.dod}
			assertRoundTrip(t, ts)
		})
	}
}

func TestQwpGorillaEncodedSizeOverflowFallback(t *testing.T) {
	// Construct DoDs that exceed int32 range.
	// deltas: 0, INT32_MAX, 0  → DoD at i=3 = -INT32_MAX, still fits.
	// Instead use deltas 0, -INT64_MAX/2, 0 → DoD at i=3 = +INT64_MAX/2 (overflow).
	var ts []int64
	ts = append(ts, 0)
	ts = append(ts, 0)                          // delta = 0
	ts = append(ts, -math.MaxInt64/2)           // delta = -INT64_MAX/2
	ts = append(ts, -math.MaxInt64/2)           // delta = 0 → DoD = INT64_MAX/2
	src := intsToBytes(ts)
	if got := qwpGorillaEncodedSize(src, len(ts)); got != -1 {
		t.Fatalf("overflow: got %d, want -1", got)
	}
}

// Round-trip: encode and decode, verifying the timestamps match.
func TestQwpGorillaRoundTripSmall(t *testing.T) {
	ts := []int64{100, 200, 300, 500, 800, 1200, 1700, 2300}
	assertRoundTrip(t, ts)
}

func TestQwpGorillaRoundTripSingleton(t *testing.T) {
	assertRoundTrip(t, []int64{math.MaxInt64 - 1})
}

func TestQwpGorillaRoundTripTwoValues(t *testing.T) {
	assertRoundTrip(t, []int64{-500, 500})
}

func TestQwpGorillaRoundTripMixedBuckets(t *testing.T) {
	// Mix of DoDs in every bucket (0, 7-bit, 9-bit, 12-bit, 32-bit).
	ts := []int64{
		1_000_000,                        // ts0
		1_000_100,                        // ts1; delta=100
		1_000_200,                        // DoD=0
		1_000_310,                        // DoD=10     (7-bit)
		1_000_520,                        // DoD=100    (9-bit)
		1_000_730,                        // DoD=0
		1_001_940,                        // DoD=1000   (12-bit)
		1_003_150,                        // DoD=0
		2_000_000,                        // DoD in 32-bit bucket
	}
	assertRoundTrip(t, ts)
}

func TestQwpGorillaRoundTripRandom(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	ts := make([]int64, 256)
	cur := int64(0)
	delta := int64(1_000)
	for i := range ts {
		// Perturb delta by small amounts so DoDs hit several buckets.
		delta += int64(r.Intn(2001) - 1000)
		cur += delta
		ts[i] = cur
	}
	assertRoundTrip(t, ts)
}

func TestQwpGorillaRoundTrip32BitDoD(t *testing.T) {
	// DoD that fits int32 but not 12-bit — exercises the widest bucket.
	ts := []int64{
		0,
		1_000_000,        // delta = 1_000_000
		3_000_000,        // delta = 2_000_000, DoD = 1_000_000 (needs 32-bit bucket)
		3_000_001,        // delta = 1,         DoD = -1_999_999
	}
	assertRoundTrip(t, ts)
}

func intsToBytes(ts []int64) []byte {
	out := make([]byte, 8*len(ts))
	for i, v := range ts {
		binary.LittleEndian.PutUint64(out[i*8:], uint64(v))
	}
	return out
}

func assertRoundTrip(t *testing.T, ts []int64) {
	t.Helper()
	src := intsToBytes(ts)
	size := qwpGorillaEncodedSize(src, len(ts))
	if size < 0 {
		t.Fatalf("unexpected overflow signal for input %v", ts)
	}
	var wb qwpWireBuffer
	var enc qwpGorillaEncoder
	n := enc.encodeTimestamps(&wb, src, len(ts))
	if n != size {
		t.Fatalf("encoded %d bytes, pre-computed %d", n, size)
	}
	got := decodeGorilla(t, wb.bytes(), len(ts))
	if len(got) != len(ts) {
		t.Fatalf("decoded %d values, want %d", len(got), len(ts))
	}
	for i := range ts {
		if got[i] != ts[i] {
			t.Fatalf("ts[%d] = %d, want %d", i, got[i], ts[i])
		}
	}
}

// decodeGorilla delegates to the production qwpGorillaDecoder so the
// existing encoder tests double as decoder round-trip coverage. Errors
// from the production decoder are turned into t.Fatalf here because
// the encoder-side tests do not set up hostile inputs.
func decodeGorilla(t *testing.T, data []byte, count int) []int64 {
	t.Helper()
	if count == 0 {
		return nil
	}
	if len(data) < 8 {
		t.Fatalf("data too short for ts0: %d", len(data))
	}
	out := make([]int64, 0, count)
	ts0 := int64(binary.LittleEndian.Uint64(data[:8]))
	out = append(out, ts0)
	if count == 1 {
		return out
	}
	if len(data) < 16 {
		t.Fatalf("data too short for ts1: %d", len(data))
	}
	ts1 := int64(binary.LittleEndian.Uint64(data[8:16]))
	out = append(out, ts1)
	if count == 2 {
		return out
	}
	var dec qwpGorillaDecoder
	dec.reset(ts0, ts1, data[16:])
	for i := 2; i < count; i++ {
		ts, err := dec.decodeNext()
		if err != nil {
			t.Fatalf("decodeNext at i=%d: %v", i, err)
		}
		out = append(out, ts)
	}
	return out
}
