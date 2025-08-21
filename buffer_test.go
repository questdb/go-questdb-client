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

package questdb_test

import (
	"encoding/binary"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

type bufWriterFn func(b *qdb.Buffer) error

func newTestBuffer() qdb.Buffer {
	return qdb.NewBuffer(128*1024, 1024*1024, 127)
}

func TestValidWrites(t *testing.T) {
	testCases := []struct {
		name          string
		writerFn      bufWriterFn
		expectedLines [][]byte
	}{
		{
			"multiple rows",
			func(b *qdb.Buffer) error {
				err := b.Table(testTable).StringColumn("str_col", "foo").Int64Column("long_col", 42).At(time.Time{}, false)
				if err != nil {
					return err
				}
				err = b.Table(testTable).StringColumn("str_col", "bar").Int64Column("long_col", -42).At(time.UnixMicro(42), true)
				if err != nil {
					return err
				}
				return nil
			},
			[][]byte{
				[]byte("my_test_table str_col=\"foo\",long_col=42i"),
				[]byte("my_test_table str_col=\"bar\",long_col=-42i 42000"),
			},
		},
		{
			"UTF-8 strings",
			func(s *qdb.Buffer) error {
				return s.Table("таблица").StringColumn("колонка", "значение").At(time.Time{}, false)
			},
			[][]byte{
				[]byte("таблица колонка=\"значение\""),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			err := tc.writerFn(&buf)
			assert.NoError(t, err)

			// Check the buffer
			var expectedLines []byte
			for i, line := range tc.expectedLines {
				if i != 0 {
					expectedLines = append(expectedLines, '\n')
				}
				expectedLines = append(expectedLines, line...)
			}
			expectedLines = append(expectedLines, '\n')
			assert.Equal(t, expectedLines, buf.Messages())
		})
	}
}

func TestTimestampSerialization(t *testing.T) {
	testCases := []struct {
		name string
		val  time.Time
	}{
		{"max value", time.UnixMicro(math.MaxInt64)},
		{"zero", time.UnixMicro(0)},
		{"small positive value", time.UnixMicro(10)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			err := buf.Table(testTable).TimestampColumn("a_col", tc.val).At(time.Time{}, false)
			assert.NoError(t, err)

			// Check the buffer
			expected := []byte("my_test_table a_col=" + strconv.FormatInt(tc.val.UnixMicro(), 10) + "t\n")
			assert.Equal(t, expected, buf.Messages())
		})
	}
}

func TestInt64Serialization(t *testing.T) {
	testCases := []struct {
		name string
		val  int64
	}{
		{"min value", math.MinInt64},
		{"max value", math.MaxInt64},
		{"zero", 0},
		{"small negative value", -10},
		{"small positive value", 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			err := buf.Table(testTable).Int64Column("a_col", tc.val).At(time.Time{}, false)
			assert.NoError(t, err)

			// Check the buffer
			expected := []byte("my_test_table a_col=" + strconv.FormatInt(tc.val, 10) + "i\n")
			assert.Equal(t, expected, buf.Messages())
		})
	}
}

func TestLong256Column(t *testing.T) {
	testCases := []struct {
		name     string
		val      string
		expected string
	}{
		{"zero", "0", "0x0"},
		{"one", "1", "0x1"},
		{"32-bit max", strconv.FormatInt(math.MaxInt32, 16), "0x7fffffff"},
		{"64-bit random", strconv.FormatInt(7423093023234231, 16), "0x1a5f4386c8d8b7"},
		{"256-bit max", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			newVal, _ := big.NewInt(0).SetString(tc.val, 16)
			err := buf.Table(testTable).Long256Column("a_col", newVal).At(time.Time{}, false)
			assert.NoError(t, err)

			// Check the buffer
			expected := []byte("my_test_table a_col=" + tc.expected + "i\n")
			assert.Equal(t, expected, buf.Messages())
		})
	}
}

func TestFloat64Serialization(t *testing.T) {
	testCases := []struct {
		name     string
		val      float64
		expected string
	}{
		{"NaN", math.NaN(), "NaN"},
		{"positive infinity", math.Inf(1), "Infinity"},
		{"negative infinity", math.Inf(-1), "-Infinity"},
		{"negative infinity", math.Inf(-1), "-Infinity"},
		{"positive number", 42.3, "42.3"},
		{"negative number", -42.3, "-42.3"},
		{"smallest value", math.SmallestNonzeroFloat64, "5E-324"},
		{"max value", math.MaxFloat64, "1.7976931348623157E+308"},
		{"negative with exponent", -4.2e-99, "-4.2E-99"},
		{"small with exponent", 4.2e-99, "4.2E-99"},
		{"large with exponent", 4.2e99, "4.2E+99"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			err := buf.Table(testTable).Float64Column("a_col", tc.val).At(time.Time{}, false)
			assert.NoError(t, err)

			// Check the buffer
			expected := []byte("my_test_table a_col=" + tc.expected + "\n")
			assert.Equal(t, expected, buf.Messages())
		})
	}
}

func TestErrorOnTooLargeBuffer(t *testing.T) {
	const initBufSize = 1
	const maxBufSize = 4

	testCases := []struct {
		name     string
		writerFn bufWriterFn
	}{
		{
			"table name and ts",
			func(s *qdb.Buffer) error {
				return s.Table("foobar").At(time.Time{}, false)
			},
		},
		{
			"string column",
			func(s *qdb.Buffer) error {
				return s.Table("a").StringColumn("str_col", "foo").At(time.Time{}, false)
			},
		},
		{
			"long column",
			func(s *qdb.Buffer) error {
				return s.Table("a").Int64Column("str_col", 1000000).At(time.Time{}, false)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := qdb.NewBuffer(initBufSize, maxBufSize, 127)

			err := tc.writerFn(&buf)
			assert.ErrorContains(t, err, "buffer size exceeded maximum limit")
		})
	}
}

func TestErrorOnLengthyNames(t *testing.T) {
	const nameLimit = 42

	var (
		lengthyStr = strings.Repeat("a", nameLimit+1)
	)

	testCases := []struct {
		name           string
		writerFn       bufWriterFn
		expectedErrMsg string
	}{
		{
			"lengthy table name",
			func(s *qdb.Buffer) error {
				return s.Table(lengthyStr).StringColumn("str_col", "foo").At(time.Time{}, false)
			},
			"table name length exceeds the limit",
		},
		{
			"lengthy column name",
			func(s *qdb.Buffer) error {
				return s.Table(testTable).StringColumn(lengthyStr, "foo").At(time.Time{}, false)
			},
			"column name length exceeds the limit",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := qdb.NewBuffer(128*1024, 1024*1024, nameLimit)

			err := tc.writerFn(&buf)
			assert.ErrorContains(t, err, tc.expectedErrMsg)
			assert.Empty(t, buf.Messages())
		})
	}
}

func TestErrorOnMissingTableCall(t *testing.T) {
	testCases := []struct {
		name     string
		writerFn bufWriterFn
	}{
		{
			"At",
			func(s *qdb.Buffer) error {
				return s.Symbol("sym", "abc").At(time.Time{}, false)
			},
		},
		{
			"symbol",
			func(s *qdb.Buffer) error {
				return s.Symbol("sym", "abc").At(time.Time{}, false)
			},
		},
		{
			"string column",
			func(s *qdb.Buffer) error {
				return s.StringColumn("str", "abc").At(time.Time{}, false)
			},
		},
		{
			"boolean column",
			func(s *qdb.Buffer) error {
				return s.BoolColumn("bool", true).At(time.Time{}, false)
			},
		},
		{
			"long column",
			func(s *qdb.Buffer) error {
				return s.Int64Column("int", 42).At(time.Time{}, false)
			},
		},
		{
			"double column",
			func(s *qdb.Buffer) error {
				return s.Float64Column("float", 4.2).At(time.Time{}, false)
			},
		},
		{
			"timestamp column",
			func(s *qdb.Buffer) error {
				return s.TimestampColumn("timestamp", time.UnixMicro(42)).At(time.Time{}, false)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			err := tc.writerFn(&buf)

			assert.ErrorContains(t, err, "table name was not provided")
			assert.Empty(t, buf.Messages())
		})
	}
}

func TestErrorOnMultipleTableCalls(t *testing.T) {
	buf := newTestBuffer()

	err := buf.Table(testTable).Table(testTable).At(time.Time{}, false)

	assert.ErrorContains(t, err, "table name already provided")
	assert.Empty(t, buf.Messages())
}

func TestErrorOnNegativeLong256(t *testing.T) {
	buf := newTestBuffer()

	err := buf.Table(testTable).Long256Column("long256_col", big.NewInt(-42)).At(time.Time{}, false)

	assert.ErrorContains(t, err, "long256 cannot be negative: -42")
	assert.Empty(t, buf.Messages())
}

func TestErrorOnLargerLong256(t *testing.T) {
	buf := newTestBuffer()

	bigVal, _ := big.NewInt(0).SetString("fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
	err := buf.Table(testTable).Long256Column("long256_col", bigVal).At(time.Time{}, false)

	assert.ErrorContains(t, err, "long256 cannot be larger than 256-bit: 260")
	assert.Empty(t, buf.Messages())
}

func TestErrorOnSymbolCallAfterColumn(t *testing.T) {
	testCases := []struct {
		name     string
		writerFn bufWriterFn
	}{
		{
			"string column",
			func(s *qdb.Buffer) error {
				return s.Table("awesome_table").StringColumn("str", "abc").Symbol("sym", "abc").At(time.Time{}, false)
			},
		},
		{
			"boolean column",
			func(s *qdb.Buffer) error {
				return s.Table("awesome_table").BoolColumn("bool", true).Symbol("sym", "abc").At(time.Time{}, false)
			},
		},
		{
			"integer column",
			func(s *qdb.Buffer) error {
				return s.Table("awesome_table").Int64Column("int", 42).Symbol("sym", "abc").At(time.Time{}, false)
			},
		},
		{
			"float column",
			func(s *qdb.Buffer) error {
				return s.Table("awesome_table").Float64Column("float", 4.2).Symbol("sym", "abc").At(time.Time{}, false)
			},
		},
		{
			"timestamp column",
			func(s *qdb.Buffer) error {
				return s.Table("awesome_table").TimestampColumn("timestamp", time.UnixMicro(42)).Symbol("sym", "abc").At(time.Time{}, false)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			err := tc.writerFn(&buf)

			assert.ErrorContains(t, err, "symbols have to be written before any other column")
			assert.Empty(t, buf.Messages())
		})
	}
}

func TestInvalidMessageGetsDiscarded(t *testing.T) {
	buf := newTestBuffer()

	// Write a valid message.
	err := buf.Table(testTable).StringColumn("foo", "bar").At(time.Time{}, false)
	assert.NoError(t, err)
	// Then write perform an incorrect chain of calls.
	err = buf.Table(testTable).StringColumn("foo", "bar").Symbol("sym", "42").At(time.Time{}, false)
	assert.Error(t, err)

	// The second message should be discarded.
	expected := []byte(testTable + " foo=\"bar\"\n")
	assert.Equal(t, expected, buf.Messages())
}

func TestInvalidTableName(t *testing.T) {
	buf := newTestBuffer()

	err := buf.Table("invalid\ntable").StringColumn("foo", "bar").At(time.Time{}, false)
	assert.ErrorContains(t, err, "table name contains an illegal char")
	assert.Empty(t, buf.Messages())
}

func TestInvalidColumnName(t *testing.T) {
	buf := newTestBuffer()

	err := buf.Table(testTable).StringColumn("invalid\ncolumn", "bar").At(time.Time{}, false)
	assert.ErrorContains(t, err, "column name contains an illegal char")
	assert.Empty(t, buf.Messages())
}

func TestFloat64ColumnBinary(t *testing.T) {
	testCases := []struct {
		name string
		val  float64
	}{
		{"positive number", 42.3},
		{"negative number", -42.3},
		{"zero", 0.0},
		{"NaN", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"smallest value", math.SmallestNonzeroFloat64},
		{"max value", math.MaxFloat64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()
			err := buf.Table(testTable).Float64ColumnBinary("a_col", tc.val).At(time.Time{}, false)
			assert.NoError(t, err)
			assert.Equal(t, buf.Messages(), float64ToByte(testTable, "a_col", tc.val))
		})
	}
}

func TestFloat64Array1DColumn(t *testing.T) {
	testCases := []struct {
		name   string
		values []float64
	}{
		{"single value", []float64{42.5}},
		{"multiple values", []float64{1.1, 2.2, 3.3}},
		{"empty array", []float64{}},
		{"with special values", []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0.0}},
		{"null array", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, littleEndian := range []bool{true, false} {
				qdb.SetLittleEndian(littleEndian)
				buf := newTestBuffer()

				err := buf.Table(testTable).Float64Array1DColumn("array_col", tc.values).At(time.Time{}, false)
				assert.NoError(t, err)
				assert.Equal(t, float641DArrayToByte(testTable, "array_col", tc.values), buf.Messages())
			}
		})
	}
}

func TestFloat64Array2DColumn(t *testing.T) {
	testCases := []struct {
		name   string
		values [][]float64
	}{
		{"2x2 array", [][]float64{{1.1, 2.2}, {3.3, 4.4}}},
		{"1x3 array", [][]float64{{1.0, 2.0, 3.0}}},
		{"3x1 array", [][]float64{{1.0}, {2.0}, {3.0}}},
		{"empty array", [][]float64{}},
		{"null array", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, littleEndian := range []bool{true, false} {
				qdb.SetLittleEndian(littleEndian)
				buf := newTestBuffer()
				err := buf.Table(testTable).Float64Array2DColumn("array_col", tc.values).At(time.Time{}, false)
				assert.NoError(t, err)
				assert.Equal(t, float642DArrayToByte(testTable, "array_col", tc.values), buf.Messages())
			}
		})
	}
}

func TestFloat64Array2DColumnIrregularShape(t *testing.T) {
	buf := newTestBuffer()

	irregularArray := [][]float64{{1.0, 2.0}, {3.0, 4.0, 5.0}}
	err := buf.Table(testTable).Float64Array2DColumn("array_col", irregularArray).At(time.Time{}, false)

	assert.ErrorContains(t, err, "irregular 2D array shape")
	assert.Empty(t, buf.Messages())
}

func TestFloat64Array3DColumn(t *testing.T) {
	testCases := []struct {
		name   string
		values [][][]float64
	}{
		{"2x2x2 array", [][][]float64{{{1.1, 2.2}, {3.3, 4.4}}, {{5.5, 6.6}, {7.7, 8.8}}}},
		{"1x1x3 array", [][][]float64{{{1.0, 2.0, 3.0}}}},
		{"empty array", [][][]float64{}},
		{"null array", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, littleEndian := range []bool{true, false} {
				qdb.SetLittleEndian(littleEndian)
				buf := newTestBuffer()
				err := buf.Table(testTable).Float64Array3DColumn("array_col", tc.values).At(time.Time{}, false)
				assert.NoError(t, err)
				assert.Equal(t, float643DArrayToByte(testTable, "array_col", tc.values), buf.Messages())
			}
		})
	}
}

func TestFloat64Array3DColumnIrregularShape(t *testing.T) {
	buf := newTestBuffer()
	irregularArray := [][][]float64{{{1.0, 2.0}, {3.0}}, {{4.0, 5.0}, {6.0, 7.0}}}
	err := buf.Table(testTable).Float64Array3DColumn("array_col", irregularArray).At(time.Time{}, false)
	assert.ErrorContains(t, err, "irregular 3D array shape: level2[0][1] has length 1, expected 2")
	assert.Empty(t, buf.Messages())
	irregularArray2 := [][][]float64{{{1.0, 2.0}, {3.0, 4.0}}, {{4.0, 5.0}}}
	err = buf.Table(testTable).Float64Array3DColumn("array_col", irregularArray2).At(time.Time{}, false)
	assert.ErrorContains(t, err, "irregular 3D array shape: level1[1] has length 1, expected 2")
	assert.Empty(t, buf.Messages())
}

func TestFloat64ArrayNDColumn(t *testing.T) {
	testCases := []struct {
		name  string
		shape []uint
		data  []float64
	}{
		{"1D array", []uint{3}, []float64{1.0, 2.0, 3.0}},
		{"2D array", []uint{2, 2}, []float64{1.0, 2.0, 3.0, 4.0}},
		{"3D array", []uint{2, 1, 2}, []float64{1.0, 2.0, 3.0, 4.0}},
		{"null array", nil, nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, littleEndian := range []bool{true, false} {
				qdb.SetLittleEndian(littleEndian)
				buf := newTestBuffer()
				var err error
				if tc.data == nil {
					err = buf.Table(testTable).Float64ArrayNDColumn("ndarray_col", nil).At(time.Time{}, false)
					assert.NoError(t, err)
					assert.Equal(t, float64NDArrayToByte(testTable, "ndarray_col", nil), buf.Messages())
				} else {
					ndArray, err := qdb.NewNDArray[float64](tc.shape...)
					assert.NoError(t, err)
					for _, val := range tc.data {
						ndArray.Append(val)
					}
					err = buf.Table(testTable).Float64ArrayNDColumn("ndarray_col", ndArray).At(time.Time{}, false)
					assert.NoError(t, err)
					assert.Equal(t, float64NDArrayToByte(testTable, "ndarray_col", ndArray), buf.Messages())
				}
			}
		})
	}
}

func TestFloat64Array1DColumnExceedsMaxElements(t *testing.T) {
	buf := newTestBuffer()
	largeSize := qdb.MaxArrayElements + 1
	values := make([]float64, largeSize)

	err := buf.Table(testTable).Float64Array1DColumn("array_col", values).At(time.Time{}, false)
	assert.ErrorContains(t, err, "array size 268435456 exceeds maximum limit 268435455")
	assert.Empty(t, buf.Messages())
}

func TestFloat64Array2DColumnExceedsMaxElements(t *testing.T) {
	buf := newTestBuffer()
	dim1 := 65536
	dim2 := 4097
	values := make([][]float64, dim1)
	for i := range values {
		values[i] = make([]float64, dim2)
	}

	err := buf.Table(testTable).Float64Array2DColumn("array_col", values).At(time.Time{}, false)
	assert.ErrorContains(t, err, "array size 268500992 exceeds maximum limit 268435455")
	assert.Empty(t, buf.Messages())
}

func TestFloat64Array3DColumnExceedsMaxElements(t *testing.T) {
	buf := newTestBuffer()
	dim1 := 1024
	dim2 := 1024
	dim3 := 256
	values := make([][][]float64, dim1)
	for i := range values {
		values[i] = make([][]float64, dim2)
		for j := range values[i] {
			values[i][j] = make([]float64, dim3)
		}
	}

	err := buf.Table(testTable).Float64Array3DColumn("array_col", values).At(time.Time{}, false)
	assert.ErrorContains(t, err, "array size 268435456 exceeds maximum limit 268435455")
	assert.Empty(t, buf.Messages())
}

func float64ToByte(table, col string, val float64) []byte {
	buf := make([]byte, 0, 128)
	buf = append(buf, ([]byte)(table)...)
	buf = append(buf, ' ')
	buf = append(buf, ([]byte)(col)...)
	buf = append(buf, '=')
	buf = append(buf, '=')
	buf = append(buf, 16)
	buf1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf1, math.Float64bits(val))
	buf = append(buf, buf1...)
	buf = append(buf, '\n')
	return buf
}

func nullArrayToByte(table, col string) []byte {
	buf := make([]byte, 0, 128)
	buf = append(buf, ([]byte)(table)...)
	buf = append(buf, ' ')
	buf = append(buf, ([]byte)(col)...)
	buf = append(buf, '=')
	buf = append(buf, '=')
	buf = append(buf, 14)
	buf = append(buf, 33)
	buf = append(buf, '\n')
	return buf
}

func float641DArrayToByte(table, col string, vals []float64) []byte {
	if vals == nil {
		return nullArrayToByte(table, col)
	}
	buf := make([]byte, 0, 128)
	buf = append(buf, ([]byte)(table)...)
	buf = append(buf, ' ')
	buf = append(buf, ([]byte)(col)...)
	buf = append(buf, '=')
	buf = append(buf, '=')
	buf = append(buf, 14)
	buf = append(buf, 10)
	buf = append(buf, 1)

	shapeData := make([]byte, 4)
	binary.LittleEndian.PutUint32(shapeData, uint32(len(vals)))
	buf = append(buf, shapeData...)

	// Write values
	for _, val := range vals {
		valData := make([]byte, 8)
		binary.LittleEndian.PutUint64(valData, math.Float64bits(val))
		buf = append(buf, valData...)
	}
	buf = append(buf, '\n')
	return buf
}

func float642DArrayToByte(table, col string, vals [][]float64) []byte {
	if vals == nil {
		return nullArrayToByte(table, col)
	}
	buf := make([]byte, 0, 256)
	buf = append(buf, ([]byte)(table)...)
	buf = append(buf, ' ')
	buf = append(buf, ([]byte)(col)...)
	buf = append(buf, '=')
	buf = append(buf, '=')
	buf = append(buf, 14)
	buf = append(buf, 10)
	buf = append(buf, 2)

	dim1 := len(vals)
	var dim2 int
	if dim1 > 0 {
		dim2 = len(vals[0])
	}

	shapeData := make([]byte, 8)
	binary.LittleEndian.PutUint32(shapeData[:4], uint32(dim1))
	binary.LittleEndian.PutUint32(shapeData[4:8], uint32(dim2))
	buf = append(buf, shapeData...)

	for _, row := range vals {
		for _, val := range row {
			valData := make([]byte, 8)
			binary.LittleEndian.PutUint64(valData, math.Float64bits(val))
			buf = append(buf, valData...)
		}
	}
	buf = append(buf, '\n')
	return buf
}

func float643DArrayToByte(table, col string, vals [][][]float64) []byte {
	if vals == nil {
		return nullArrayToByte(table, col)
	}
	buf := make([]byte, 0, 512)
	buf = append(buf, ([]byte)(table)...)
	buf = append(buf, ' ')
	buf = append(buf, ([]byte)(col)...)
	buf = append(buf, '=')
	buf = append(buf, '=')
	buf = append(buf, 14)
	buf = append(buf, 10)
	buf = append(buf, 3)

	dim1 := len(vals)
	var dim2, dim3 int
	if dim1 > 0 {
		dim2 = len(vals[0])
		if dim2 > 0 {
			dim3 = len(vals[0][0])
		}
	}

	shapeData := make([]byte, 12)
	binary.LittleEndian.PutUint32(shapeData[:4], uint32(dim1))
	binary.LittleEndian.PutUint32(shapeData[4:8], uint32(dim2))
	binary.LittleEndian.PutUint32(shapeData[8:12], uint32(dim3))
	buf = append(buf, shapeData...)

	for _, level1 := range vals {
		for _, level2 := range level1 {
			for _, val := range level2 {
				valData := make([]byte, 8)
				binary.LittleEndian.PutUint64(valData, math.Float64bits(val))
				buf = append(buf, valData...)
			}
		}
	}
	buf = append(buf, '\n')
	return buf
}

func float64NDArrayToByte(table, col string, ndarray *qdb.NdArray[float64]) []byte {
	if ndarray == nil {
		return nullArrayToByte(table, col)
	}
	buf := make([]byte, 0, 512)
	buf = append(buf, ([]byte)(table)...)
	buf = append(buf, ' ')
	buf = append(buf, ([]byte)(col)...)
	buf = append(buf, '=')
	buf = append(buf, '=')
	buf = append(buf, 14)
	buf = append(buf, 10)
	buf = append(buf, byte(ndarray.NDims()))

	shape := ndarray.Shape()
	for _, dim := range shape {
		shapeData := make([]byte, 4)
		binary.LittleEndian.PutUint32(shapeData, uint32(dim))
		buf = append(buf, shapeData...)
	}

	data := ndarray.Data()
	for _, val := range data {
		valData := make([]byte, 8)
		binary.LittleEndian.PutUint64(valData, math.Float64bits(val))
		buf = append(buf, valData...)
	}
	buf = append(buf, '\n')
	return buf
}
