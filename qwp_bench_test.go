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

		col, _ = tb.getOrCreateColumn("msg", qwpTypeString, false)
		col.addString("hello")

		col, _ = tb.getOrCreateColumn("flag", qwpTypeBoolean, false)
		col.addBool(i%2 == 0)

		col, _ = tb.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
		col.addTimestamp(int64(1000000 + i))

		tb.commitRow()
	}

	symList := []string{"s0", "s1", "s2", "s3", "s4"}
	schemaHash := tb.getSchemaHash()

	var enc qwpEncoder
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.encodeTableWithDeltaDict(tb, symList, -1, 4, qwpSchemaModeFull, schemaHash)
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

		schemaHash := tb.getSchemaHash()
		enc.encodeTableWithDeltaDict(tb, symList, -1, 2, qwpSchemaModeFull, schemaHash)
		tb.reset()
	}
}

// BenchmarkQwpSenderSteadyState measures the full sender hot path:
// Table/Symbol/columns/At for 10 rows, then encode + reset.
// This exercises the complete pipeline (sender methods → columnar
// buffers → encoder) without network I/O. Target: 0 allocs/op
// after warmup, proving the hot path is allocation-free.
func BenchmarkQwpSenderSteadyState(b *testing.B) {
	ctx := context.Background()
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a sender with auto-flush disabled (no network).
	s := &qwpLineSender{
		tableBuffers:     make(map[string]*qwpTableBuffer),
		globalSymbols:    make(map[string]int32),
		sentSchemaHashes: make(map[int64]struct{}),
		maxSentSymbolId:  -1,
		batchMaxSymbolId: -1,
	}

	// Pre-populate the symbol dictionary (warmup).
	s.globalSymbols["AAPL"] = 0
	s.globalSymbolList = append(s.globalSymbolList, "AAPL")
	s.batchMaxSymbolId = 0

	// Warmup: 2 flushes with 10 rows each to grow all backing buffers.
	for flush := 0; flush < 2; flush++ {
		for r := 0; r < 10; r++ {
			s.Table("t").
				Symbol("sym", "AAPL").
				Int64Column("qty", int64(100+r)).
				Float64Column("price", 150.5+float64(r)).
				StringColumn("note", "test").
				At(ctx, ts.Add(time.Duration(r)*time.Microsecond))
		}
		tables := s.buildTableEncodeInfo()
		s.encoders[0].encodeMultiTableWithDeltaDict(
			tables,
			s.globalSymbolList,
			s.maxSentSymbolId,
			s.batchMaxSymbolId,
		)
		// Mark schema as sent so subsequent flushes use reference mode.
		for _, t := range tables {
			skey := qwpSchemaKey(t.tb.tableNameHash, t.schemaHash)
			s.sentSchemaHashes[skey] = struct{}{}
		}
		s.resetAfterFlush()
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for r := 0; r < 10; r++ {
			s.Table("t").
				Symbol("sym", "AAPL").
				Int64Column("qty", int64(100+r)).
				Float64Column("price", 150.5+float64(r)).
				StringColumn("note", "test").
				At(ctx, ts.Add(time.Duration(r)*time.Microsecond))
		}
		tables := s.buildTableEncodeInfo()
		s.encoders[0].encodeMultiTableWithDeltaDict(
			tables,
			s.globalSymbolList,
			s.maxSentSymbolId,
			s.batchMaxSymbolId,
		)
		s.resetAfterFlush()
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
		col := newQwpColumnBuffer("val", qwpTypeString, false)
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
