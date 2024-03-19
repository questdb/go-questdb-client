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
		expectedLines []string
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
			[]string{
				"my_test_table str_col=\"foo\",long_col=42i",
				"my_test_table str_col=\"bar\",long_col=-42i 42000",
			},
		},
		{
			"UTF-8 strings",
			func(s *qdb.Buffer) error {
				return s.Table("таблица").StringColumn("колонка", "значение").At(time.Time{}, false)
			},
			[]string{
				"таблица колонка=\"значение\"",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := newTestBuffer()

			err := tc.writerFn(&buf)
			assert.NoError(t, err)

			// Check the buffer
			assert.Equal(t, strings.Join(tc.expectedLines, "\n")+"\n", buf.Messages())

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
			expectedLines := []string{"my_test_table a_col=" + strconv.FormatInt(tc.val.UnixMicro(), 10) + "t"}
			assert.Equal(t, strings.Join(expectedLines, "\n")+"\n", buf.Messages())
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
			expectedLines := []string{"my_test_table a_col=" + strconv.FormatInt(tc.val, 10) + "i"}
			assert.Equal(t, strings.Join(expectedLines, "\n")+"\n", buf.Messages())
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
			expectedLines := []string{"my_test_table a_col=" + tc.expected + "i"}
			assert.Equal(t, strings.Join(expectedLines, "\n")+"\n", buf.Messages())
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
			expectedLines := []string{"my_test_table a_col=" + tc.expected}
			assert.Equal(t, strings.Join(expectedLines, "\n")+"\n", buf.Messages())
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
	expectedLines := []string{testTable + " foo=\"bar\""}
	assert.Equal(t, strings.Join(expectedLines, "\n")+"\n", buf.Messages())
}
