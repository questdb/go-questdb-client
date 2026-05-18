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
	"testing"
)

const perfN = 10000

func perfFixedInt64Batch(n int) *QwpColumnBatch {
	info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	values := make([]byte, 8*n)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(values[i*8:], uint64(i))
	}
	layout := buildFixedLayout(&info, values, n)
	return newSingleColumnBatch(info, layout, n)
}

func perfNullableInt64Batch(n int) *QwpColumnBatch {
	info := qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
	rowBytes := make([][]byte, n)
	for i := 0; i < n; i++ {
		if i%4 == 0 {
			rowBytes[i] = nil
		} else {
			rowBytes[i] = binary.LittleEndian.AppendUint64(nil, uint64(i))
		}
	}
	layout := buildNullableLayout(&info, rowBytes)
	return newSingleColumnBatch(info, layout, n)
}

// BenchmarkBatchInt64PerCell measures the batch-level (col, row) accessor.
// After the delegation refactor this routes through Column(col).Int64(row);
// the question is whether the Go inliner fully elides the handle-construction.
func BenchmarkBatchInt64PerCell(b *testing.B) {
	batch := perfFixedInt64Batch(perfN)
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for r := 0; r < perfN; r++ {
			sink ^= batch.Int64(0, r)
		}
	}
	_ = sink
}

// BenchmarkColumnInt64PerCell is the control: Column-handle path, identical
// before and after. Differences here would point at noise in the harness.
func BenchmarkColumnInt64PerCell(b *testing.B) {
	batch := perfFixedInt64Batch(perfN)
	col := batch.Column(0)
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for r := 0; r < perfN; r++ {
			sink ^= col.Int64(r)
		}
	}
	_ = sink
}

// BenchmarkInt64RangeNoNulls measures the bulk fast path. The fix replaced
// raw `unsafe.Slice` from `&l.values[byteStart]` with a bounds-checked
// sub-slice expression — should be a wash, but worth confirming.
func BenchmarkInt64RangeNoNulls(b *testing.B) {
	batch := perfFixedInt64Batch(perfN)
	col := batch.Column(0)
	dst := make([]int64, 0, perfN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		dst = col.Int64Range(0, perfN, dst)
	}
	_ = dst
}

// BenchmarkInt64RangeWithNulls measures the per-row scalar loop. Untouched
// by the changes; included as a second control.
func BenchmarkInt64RangeWithNulls(b *testing.B) {
	batch := perfNullableInt64Batch(perfN)
	col := batch.Column(0)
	dst := make([]int64, 0, perfN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		dst = col.Int64Range(0, perfN, dst)
	}
	_ = dst
}

// --- Multi-column / wide-row patterns ---
//
// The single-column benchmarks above let the Go inliner hoist
// b.layouts[0] out of the inner loop because the column index is a
// loop-invariant literal — so they understate the column-handle's
// theoretical win. The benchmarks below measure shapes where the
// compiler cannot do that lift.

const (
	perfRows = 1000
	perfCols = 16
)

func perfMultiColInt64Batch(rows, cols int) *QwpColumnBatch {
	infos := make([]qwpColumnSchemaInfo, cols)
	layouts := make([]qwpColumnLayout, cols)
	values := make([]byte, 8*rows)
	for i := 0; i < rows; i++ {
		binary.LittleEndian.PutUint64(values[i*8:], uint64(i))
	}
	for c := 0; c < cols; c++ {
		infos[c] = qwpColumnSchemaInfo{name: "v", wireType: qwpTypeLong}
		layouts[c] = qwpColumnLayout{
			info:         &infos[c],
			values:       values,
			nonNullCount: rows,
		}
	}
	return &QwpColumnBatch{
		requestId:   1,
		rowCount:    rows,
		columnCount: cols,
		columns:     infos,
		layouts:     layouts,
	}
}

// BenchmarkBatchMultiColRowMajor: row-major full-batch scan via the
// (col, row) batch surface. Column index varies inside the inner loop,
// so b.layouts[c] is rebound every cell — the workload the original
// review comment described.
func BenchmarkBatchMultiColRowMajor(b *testing.B) {
	batch := perfMultiColInt64Batch(perfRows, perfCols)
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for r := 0; r < perfRows; r++ {
			for c := 0; c < perfCols; c++ {
				sink ^= batch.Int64(c, r)
			}
		}
	}
	_ = sink
}

// BenchmarkColumnMultiColRowMajor: same access pattern, but each
// column's QwpColumn handle is captured once up-front so the inner
// loop hits a hoisted *qwpColumnLayout. This is the "use the handle"
// variant of the row-major scan.
func BenchmarkColumnMultiColRowMajor(b *testing.B) {
	batch := perfMultiColInt64Batch(perfRows, perfCols)
	cols := make([]QwpColumn, perfCols)
	for c := 0; c < perfCols; c++ {
		cols[c] = batch.Column(c)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for r := 0; r < perfRows; r++ {
			for c := 0; c < perfCols; c++ {
				sink ^= cols[c].Int64(r)
			}
		}
	}
	_ = sink
}

// BenchmarkBatchColumnMajor: column-major scan via the batch surface.
// The column index is invariant in the inner loop; the compiler may or
// may not hoist b.layouts[c] out — this shows whether it does.
func BenchmarkBatchColumnMajor(b *testing.B) {
	batch := perfMultiColInt64Batch(perfRows, perfCols)
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for c := 0; c < perfCols; c++ {
			for r := 0; r < perfRows; r++ {
				sink ^= batch.Int64(c, r)
			}
		}
	}
	_ = sink
}

// BenchmarkColumnMajorHandle: column-major scan via QwpColumn handles
// captured per outer iteration — the textbook fast path.
func BenchmarkColumnMajorHandle(b *testing.B) {
	batch := perfMultiColInt64Batch(perfRows, perfCols)
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for c := 0; c < perfCols; c++ {
			col := batch.Column(c)
			for r := 0; r < perfRows; r++ {
				sink ^= col.Int64(r)
			}
		}
	}
	_ = sink
}

// BenchmarkColumnMajorRange: column-major scan via Int64Range with a
// per-row consumer (XOR sum). Realistic: caller does *something* with
// each value, so this is "Range plus typical processing."
func BenchmarkColumnMajorRange(b *testing.B) {
	batch := perfMultiColInt64Batch(perfRows, perfCols)
	dst := make([]int64, 0, perfRows)
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for c := 0; c < perfCols; c++ {
			col := batch.Column(c)
			dst = dst[:0]
			dst = col.Int64Range(0, perfRows, dst)
			for _, v := range dst {
				sink ^= v
			}
		}
	}
	_ = sink
}

// BenchmarkColumnMajorRangePure: column-major Range with NO per-row
// consumer. Measures the bulk read in isolation — the upper bound for
// "Range vs per-cell" speedup.
func BenchmarkColumnMajorRangePure(b *testing.B) {
	batch := perfMultiColInt64Batch(perfRows, perfCols)
	dst := make([]int64, 0, perfRows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for c := 0; c < perfCols; c++ {
			col := batch.Column(c)
			dst = dst[:0]
			dst = col.Int64Range(0, perfRows, dst)
		}
	}
	_ = dst
}
