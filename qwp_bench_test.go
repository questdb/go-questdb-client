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
	"context"
	"testing"
	"time"
)

// BenchmarkQwpVarint measures varint encoding throughput.
func BenchmarkQwpVarint(b *testing.B) {
	buf := make([]byte, 10)
	vals := []uint64{0, 127, 128, 16383, 16384, 0xFFFFFFFF}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range vals {
			qwpPutVarint(buf, v)
		}
	}
}

// BenchmarkQwpEncode measures full table encoding throughput for a
// typical workload: 100 rows with symbol, long, double, string, bool,
// and designated timestamp columns.
func BenchmarkQwpEncode(b *testing.B) {
	tb := newQwpTableBuffer("bench_table")

	// Pre-populate with 100 rows.
	for i := 0; i < 100; i++ {
		col, _ := tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
		col.addSymbolID(int32(i % 5))

		col, _ = tb.getOrCreateColumn("val", qwpTypeLong, false)
		col.addLong(int64(i))

		col, _ = tb.getOrCreateColumn("score", qwpTypeDouble, false)
		col.addDouble(float64(i) * 0.1)

		col, _ = tb.getOrCreateColumn("msg", qwpTypeVarchar, false)
		col.addString("hello")

		col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
		col.addBool(i%2 == 0)

		col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
		col.addTimestamp(int64(1000000 + i))

		tb.commitRow()
	}

	symList := []string{"s0", "s1", "s2", "s3", "s4"}
	const schemaId = 0

	var enc qwpEncoder
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.encodeTableWithDeltaDict(tb, symList, -1, 4, qwpSchemaModeFull, schemaId)
	}
}

// BenchmarkQwpSymbolLookup measures symbol dictionary lookup speed.
func BenchmarkQwpSymbolLookup(b *testing.B) {
	symbols := make(map[string]int32)
	for i := 0; i < 1000; i++ {
		s := "symbol_" + string(rune('A'+i%26)) + string(rune('0'+i%10))
		symbols[s] = int32(i)
	}

	keys := make([]string, 0, len(symbols))
	for k := range symbols {
		keys = append(keys, k)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = symbols[keys[i%len(keys)]]
	}
}

// BenchmarkQwpFlush measures the full flush path: encoding + mock send.
// Uses a pre-populated sender with auto-flush disabled.
func BenchmarkQwpFlush(b *testing.B) {
	// We can't connect to a real server in benchmarks, so we measure
	// the encoding portion only (which is the hot path).
	tb := newQwpTableBuffer("bench_flush")

	var enc qwpEncoder
	symList := []string{"AAPL", "MSFT", "GOOG"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate adding 10 rows per iteration.
		for r := 0; r < 10; r++ {
			col, _ := tb.getOrCreateColumn("sym", qwpTypeSymbol, false)
			col.addSymbolID(int32(r % 3))

			col, _ = tb.getOrCreateColumn("price", qwpTypeDouble, false)
			col.addDouble(150.0 + float64(r))

			col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
			col.addTimestamp(int64(1000000 + r))

			tb.commitRow()
		}

		enc.encodeTableWithDeltaDict(tb, symList, -1, 2, qwpSchemaModeFull, 0)
		tb.reset()
	}
}

// qwpSteadyStateSetup returns a warmed qwpLineSender plus the iteration
// closure that both BenchmarkQwpSenderSteadyState and
// TestQwpSenderSteadyStateZeroAllocs exercise.
func qwpSteadyStateSetup() (*qwpLineSender, func()) {
	ctx := context.Background()
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s := &qwpLineSender{
		tableBuffers:     make(map[string]*qwpTableBuffer),
		globalSymbols:    make(map[string]int32),
		maxSentSymbolId:  -1,
		batchMaxSymbolId: -1,
	}

	s.globalSymbols["AAPL"] = 0
	s.globalSymbolList = append(s.globalSymbolList, "AAPL")
	s.batchMaxSymbolId = 0

	iter := func() {
		for r := 0; r < 10; r++ {
			if err := s.Table("t").
				Symbol("sym", "AAPL").
				Int64Column("qty", int64(100+r)).
				Float64Column("price", 150.5+float64(r)).
				StringColumn("note", "test").
				At(ctx, ts.Add(time.Duration(r)*time.Microsecond)); err != nil {
				panic(err)
			}
		}
		tables, _ := s.buildTableEncodeInfo()
		s.encoder.encodeMultiTableWithDeltaDict(
			tables,
			s.globalSymbolList,
			s.maxSentSymbolId,
			s.batchMaxSymbolId,
		)
		s.resetAfterFlush()
	}

	// Warmup: 2 flushes to grow all backing buffers.
	iter()
	iter()
	return s, iter
}

// BenchmarkQwpSenderSteadyState measures the full sender hot path:
// Table/Symbol/columns/At for 10 rows, then encode + reset.
// This exercises the complete pipeline (sender methods → columnar
// buffers → encoder) without network I/O. Target: 0 allocs/op
// after warmup, proving the hot path is allocation-free.
func BenchmarkQwpSenderSteadyState(b *testing.B) {
	_, iter := qwpSteadyStateSetup()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		iter()
	}
}

// TestQwpSenderSteadyStateZeroAllocs pins the 0-allocs/op invariant
// programmatically so the invariant survives refactors without a
// developer having to read the benchmark output. Only meaningful for
// non-race builds: race instrumentation forces some stack-allocatable
// values to escape and inflates allocs/op (see TestQwpSender
// SteadyStateNullsZeroAllocs for the variant that trips on this).
func TestQwpSenderSteadyStateZeroAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("zero-alloc invariant does not hold under -race")
	}
	_, iter := qwpSteadyStateSetup()
	if allocs := testing.AllocsPerRun(100, iter); allocs > 0 {
		t.Fatalf("steady-state allocs/op = %g, want 0", allocs)
	}
}

// qwpSteadyStateSetupWithNulls mirrors qwpSteadyStateSetup but
// introduces nulls (by skipping column calls on select rows) and
// adds a bool column. This exercises the nullable-column bitmap
// encoder path and the bit-packed bool payload path, which the
// all-values-set steady-state variant does not touch.
func qwpSteadyStateSetupWithNulls() (*qwpLineSender, func()) {
	ctx := context.Background()
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s := &qwpLineSender{
		tableBuffers:     make(map[string]*qwpTableBuffer),
		globalSymbols:    make(map[string]int32),
		maxSentSymbolId:  -1,
		batchMaxSymbolId: -1,
	}

	s.globalSymbols["AAPL"] = 0
	s.globalSymbolList = append(s.globalSymbolList, "AAPL")
	s.batchMaxSymbolId = 0

	iter := func() {
		for r := 0; r < 10; r++ {
			b := s.Table("t").Symbol("sym", "AAPL")
			if r%3 != 0 {
				b = b.Int64Column("qty", int64(100+r))
			}
			b = b.Float64Column("price", 150.5+float64(r))
			if r%2 == 0 {
				b = b.StringColumn("note", "test")
			}
			b = b.BoolColumn("active", r%2 == 0)
			if err := b.At(ctx, ts.Add(time.Duration(r)*time.Microsecond)); err != nil {
				panic(err)
			}
		}
		tables, _ := s.buildTableEncodeInfo()
		s.encoder.encodeMultiTableWithDeltaDict(
			tables,
			s.globalSymbolList,
			s.maxSentSymbolId,
			s.batchMaxSymbolId,
		)
		s.resetAfterFlush()
	}

	iter()
	iter()
	return s, iter
}

// BenchmarkQwpSenderSteadyStateNulls is the null-mix counterpart of
// BenchmarkQwpSenderSteadyState. Int64 and String columns each take
// nulls on some rows, and a bool column is present throughout — so
// this bench exercises the null-bitmap and bool-payload encoder paths.
func BenchmarkQwpSenderSteadyStateNulls(b *testing.B) {
	_, iter := qwpSteadyStateSetupWithNulls()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		iter()
	}
}

// TestQwpSenderSteadyStateNullsZeroAllocs pins the 0-allocs/op
// invariant for the null-mix variant. See sibling test for the -race
// caveat.
func TestQwpSenderSteadyStateNullsZeroAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("zero-alloc invariant does not hold under -race")
	}
	_, iter := qwpSteadyStateSetupWithNulls()
	if allocs := testing.AllocsPerRun(100, iter); allocs > 0 {
		t.Fatalf("steady-state-nulls allocs/op = %g, want 0", allocs)
	}
}

// BenchmarkQwpColumnAdd measures per-column add throughput.
func BenchmarkQwpColumnAdd(b *testing.B) {
	b.Run("Long", func(b *testing.B) {
		col := newQwpColumnBuffer("val", qwpTypeLong, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col.addLong(int64(i))
		}
	})

	b.Run("Double", func(b *testing.B) {
		col := newQwpColumnBuffer("val", qwpTypeDouble, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col.addDouble(float64(i) * 0.1)
		}
	})

	b.Run("String", func(b *testing.B) {
		col := newQwpColumnBuffer("val", qwpTypeVarchar, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col.addString("hello world")
		}
	})

	b.Run("Bool", func(b *testing.B) {
		col := newQwpColumnBuffer("val", qwpTypeBoolean, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col.addBool(i%2 == 0)
		}
	})

	b.Run("Symbol", func(b *testing.B) {
		col := newQwpColumnBuffer("val", qwpTypeSymbol, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col.addSymbolID(int32(i % 100))
		}
	})
}

// BenchmarkQwpGorillaDecode measures Gorilla DoD decoding throughput
// over a long timestamp column. The bit reader's hot loop issues up to
// four single-bit prefix reads plus one wide signed read per row, so
// this is the regression gate for the 8-byte LE refill optimisation in
// qwpBitReader.readBits / readBitsSlow.
func BenchmarkQwpGorillaDecode(b *testing.B) {
	const n = 4096
	mk := func(stepFn func(i int) int64) ([]byte, int64, int64) {
		ts := make([]int64, n)
		var cur int64
		for i := range ts {
			cur += stepFn(i)
			ts[i] = cur
		}
		var wb qwpWireBuffer
		var enc qwpGorillaEncoder
		enc.encodeTimestamps(&wb, intsToBytes(ts), n)
		// Strip the 16-byte uncompressed prefix the bit reader doesn't
		// touch — the decoder's reset() takes only the bit-packed tail.
		return append([]byte(nil), wb.bytes()[16:]...), ts[0], ts[1]
	}

	constantData, constantTs0, constantTs1 := mk(func(int) int64 { return 1000 })
	smallData, smallTs0, smallTs1 := mk(func(i int) int64 {
		// Most DoDs land in the 1- or 9-bit bucket.
		return 1000 + int64((i*37)%5) - 2
	})
	wideData, wideTs0, wideTs1 := mk(func(i int) int64 {
		// Forces the 32-bit bucket via large alternating jumps.
		if i%2 == 0 {
			return 1_000_000
		}
		return 1
	})

	cases := []struct {
		name string
		data []byte
		ts0  int64
		ts1  int64
	}{
		{"ConstantDelta", constantData, constantTs0, constantTs1},
		{"SmallJitter", smallData, smallTs0, smallTs1},
		{"WideJitter", wideData, wideTs0, wideTs1},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			var dec qwpGorillaDecoder
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dec.reset(c.ts0, c.ts1, c.data)
				for j := 2; j < n; j++ {
					if _, err := dec.decodeNext(); err != nil {
						b.Fatalf("decodeNext[%d]: %v", j, err)
					}
				}
			}
		})
	}
}
