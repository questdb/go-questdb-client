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

package tcp

import (
	"context"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/questdb/go-questdb-client/v3/pkg/test/utils"
	"github.com/stretchr/testify/assert"
)

const (
	testTable   = "my_test_table"
	networkName = "test-network-v3"
)

type writerFn func(s *LineSender) error

func TestValidWrites(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		writerFn      writerFn
		expectedLines []string
	}{
		{
			"multiple rows",
			func(s *LineSender) error {
				err := s.Table(testTable).StringColumn("str_col", "foo").Int64Column("long_col", 42).AtNow(ctx)
				if err != nil {
					return err
				}
				err = s.Table(testTable).StringColumn("str_col", "bar").Int64Column("long_col", -42).At(ctx, time.UnixMicro(42))
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
			func(s *LineSender) error {
				return s.Table("таблица").StringColumn("колонка", "значение").AtNow(ctx)
			},
			[]string{
				"таблица колонка=\"значение\"",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := utils.NewTestTcpServer(utils.SendToBackChannel)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
			assert.NoError(t, err)

			err = tc.writerFn(sender)
			assert.NoError(t, err)

			// Check the buffer before flushing it.
			assert.Equal(t, strings.Join(tc.expectedLines, "\n")+"\n", sender.Messages())

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			utils.ExpectLines(t, srv.BackCh, tc.expectedLines)

			srv.Close()
		})
	}
}

func TestTimestampSerialization(t *testing.T) {
	ctx := context.Background()

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
			srv, err := utils.NewTestTcpServer(utils.SendToBackChannel)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
			assert.NoError(t, err)

			err = sender.Table(testTable).TimestampColumn("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			utils.ExpectLines(t, srv.BackCh, []string{"my_test_table a_col=" + strconv.FormatInt(tc.val.UnixMicro(), 10) + "t"})

			srv.Close()
		})
	}
}

func TestInt64Serialization(t *testing.T) {
	ctx := context.Background()

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
			srv, err := utils.NewTestTcpServer(utils.SendToBackChannel)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
			assert.NoError(t, err)

			err = sender.Table(testTable).Int64Column("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			utils.ExpectLines(t, srv.BackCh, []string{"my_test_table a_col=" + strconv.FormatInt(tc.val, 10) + "i"})

			srv.Close()
		})
	}
}

func TestLong256Column(t *testing.T) {
	ctx := context.Background()

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
			srv, err := utils.NewTestTcpServer(utils.SendToBackChannel)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
			assert.NoError(t, err)

			newVal, _ := big.NewInt(0).SetString(tc.val, 16)
			err = sender.Table(testTable).Long256Column("a_col", newVal).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			utils.ExpectLines(t, srv.BackCh, []string{"my_test_table a_col=" + tc.expected + "i"})

			srv.Close()
		})
	}
}

func TestFloat64Serialization(t *testing.T) {
	ctx := context.Background()

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
			srv, err := utils.NewTestTcpServer(utils.SendToBackChannel)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
			assert.NoError(t, err)

			err = sender.Table(testTable).Float64Column("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			utils.ExpectLines(t, srv.BackCh, []string{"my_test_table a_col=" + tc.expected})

			srv.Close()
		})
	}
}

func TestErrorOnLengthyNames(t *testing.T) {
	const nameLimit = 42

	var (
		lengthyStr = strings.Repeat("a", nameLimit+1)
		ctx        = context.Background()
	)

	testCases := []struct {
		name           string
		writerFn       writerFn
		expectedErrMsg string
	}{
		{
			"lengthy table name",
			func(s *LineSender) error {
				return s.Table(lengthyStr).StringColumn("str_col", "foo").AtNow(ctx)
			},
			"table name length exceeds the limit",
		},
		{
			"lengthy column name",
			func(s *LineSender) error {
				return s.Table(testTable).StringColumn(lengthyStr, "foo").AtNow(ctx)
			},
			"column name length exceeds the limit",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()), WithFileNameLimit(nameLimit))
			assert.NoError(t, err)

			err = tc.writerFn(sender)
			assert.ErrorContains(t, err, tc.expectedErrMsg)
			assert.Empty(t, sender.Messages())

			sender.Close()
			srv.Close()
		})
	}
}

func TestErrorOnMissingTableCall(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		writerFn writerFn
	}{
		{
			"AtNow",
			func(s *LineSender) error {
				return s.Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"At",
			func(s *LineSender) error {
				return s.Symbol("sym", "abc").At(ctx, time.UnixMicro(0))
			},
		},
		{
			"symbol",
			func(s *LineSender) error {
				return s.Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"string column",
			func(s *LineSender) error {
				return s.StringColumn("str", "abc").AtNow(ctx)
			},
		},
		{
			"boolean column",
			func(s *LineSender) error {
				return s.BoolColumn("bool", true).AtNow(ctx)
			},
		},
		{
			"long column",
			func(s *LineSender) error {
				return s.Int64Column("int", 42).AtNow(ctx)
			},
		},
		{
			"double column",
			func(s *LineSender) error {
				return s.Float64Column("float", 4.2).AtNow(ctx)
			},
		},
		{
			"timestamp column",
			func(s *LineSender) error {
				return s.TimestampColumn("timestamp", time.UnixMicro(42)).AtNow(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
			assert.NoError(t, err)

			err = tc.writerFn(sender)

			assert.ErrorContains(t, err, "table name was not provided")
			assert.Empty(t, sender.Messages())

			sender.Close()
			srv.Close()
		})
	}
}

func TestErrorOnMultipleTableCalls(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.Table(testTable).Table(testTable).AtNow(ctx)

	assert.ErrorContains(t, err, "table name already provided")
	assert.Empty(t, sender.Messages())
}

func TestErrorOnNegativeLong256(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.Table(testTable).Long256Column("long256_col", big.NewInt(-42)).AtNow(ctx)

	assert.ErrorContains(t, err, "long256 cannot be negative: -42")
	assert.Empty(t, sender.Messages())
}

func TestErrorOnLargerLong256(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	bigVal, _ := big.NewInt(0).SetString("fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
	err = sender.Table(testTable).Long256Column("long256_col", bigVal).AtNow(ctx)

	assert.ErrorContains(t, err, "long256 cannot be larger than 256-bit: 260")
	assert.Empty(t, sender.Messages())
}

func TestErrorOnSymbolCallAfterColumn(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		writerFn writerFn
	}{
		{
			"string column",
			func(s *LineSender) error {
				return s.Table("awesome_table").StringColumn("str", "abc").Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"boolean column",
			func(s *LineSender) error {
				return s.Table("awesome_table").BoolColumn("bool", true).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"integer column",
			func(s *LineSender) error {
				return s.Table("awesome_table").Int64Column("int", 42).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"float column",
			func(s *LineSender) error {
				return s.Table("awesome_table").Float64Column("float", 4.2).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"timestamp column",
			func(s *LineSender) error {
				return s.Table("awesome_table").TimestampColumn("timestamp", time.UnixMicro(42)).Symbol("sym", "abc").AtNow(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
			assert.NoError(t, err)

			sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
			assert.NoError(t, err)

			err = tc.writerFn(sender)

			assert.ErrorContains(t, err, "symbols have to be written before any other column")
			assert.Empty(t, sender.Messages())

			sender.Close()
			srv.Close()
		})
	}
}

func TestErrorOnFlushWhenMessageIsPending(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "pending ILP message must be finalized with At or AtNow before calling Flush")
	assert.Empty(t, sender.Messages())
}

func TestInvalidMessageGetsDiscarded(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.SendToBackChannel)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	// Write a valid message.
	err = sender.Table(testTable).StringColumn("foo", "bar").AtNow(ctx)
	assert.NoError(t, err)
	// Then write perform an incorrect chain of calls.
	err = sender.Table(testTable).StringColumn("foo", "bar").Symbol("sym", "42").AtNow(ctx)
	assert.Error(t, err)

	// The second message should be discarded.
	err = sender.Flush(ctx)
	assert.NoError(t, err)
	utils.ExpectLines(t, srv.BackCh, []string{testTable + " foo=\"bar\""})
}

func TestErrorOnUnavailableServer(t *testing.T) {
	ctx := context.Background()

	_, err := NewLineSender(ctx)
	assert.ErrorContains(t, err, "failed to connect to server")
}

func TestErrorOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	// The context is not cancelled yet, so Flush should succeed.
	err = sender.Table(testTable).StringColumn("foo", "bar").AtNow(ctx)
	assert.NoError(t, err)
	err = sender.Flush(ctx)
	assert.NoError(t, err)

	cancel()

	// The context is now cancelled, so we expect an error.
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)
	err = sender.Flush(ctx)
	assert.Error(t, err)
}

func TestErrorOnContextDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	// Keep writing until we get an error due to the context deadline.
	for i := 0; i < 100_000; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		if err != nil {
			return
		}
		err = sender.Flush(ctx)
		if err != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fail()
}

func BenchmarkLineSenderBatch1000(b *testing.B) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(b, err)
	defer sender.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			sender.
				Table(testTable).
				Symbol("sym_col", "test_ilp1").
				Float64Column("double_col", float64(i)+0.42).
				Int64Column("long_col", int64(i)).
				StringColumn("str_col", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua").
				BoolColumn("bool_col", true).
				TimestampColumn("timestamp_col", time.UnixMicro(42)).
				At(ctx, time.UnixMicro(int64(1000*i)))
		}
		sender.Flush(ctx)
	}
}

func BenchmarkLineSenderNoFlush(b *testing.B) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(b, err)
	defer sender.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sender.
			Table(testTable).
			Symbol("sym_col", "test_ilp1").
			Float64Column("double_col", float64(i)+0.42).
			Int64Column("long_col", int64(i)).
			StringColumn("str_col", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua").
			BoolColumn("bool_col", true).
			TimestampColumn("timestamp_col", time.UnixMicro(42)).
			At(ctx, time.UnixMicro(int64(1000*i)))
	}
	sender.Flush(ctx)
}
