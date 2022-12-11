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
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client"
	"github.com/stretchr/testify/assert"
)

type writerFn func(s *qdb.LineSender) error

func TestValidWrites(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		writerFn      writerFn
		expectedLines []string
	}{
		{
			"multiple rows",
			func(s *qdb.LineSender) error {
				err := s.Table(testTable).StringColumn("str_col", "foo").Int64Column("long_col", 42).AtNow(ctx)
				if err != nil {
					return err
				}
				err = s.Table(testTable).StringColumn("str_col", "bar").Int64Column("long_col", -42).At(ctx, 42)
				if err != nil {
					return err
				}
				return nil
			},
			[]string{
				"my_test_table str_col=\"foo\",long_col=42i",
				"my_test_table str_col=\"bar\",long_col=-42i 42",
			},
		},
		{
			"UTF-8 strings",
			func(s *qdb.LineSender) error {
				return s.Table("таблица").StringColumn("колонка", "значение").AtNow(ctx)
			},
			[]string{
				"таблица колонка=\"значение\"",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = tc.writerFn(sender)
			assert.NoError(t, err)

			// Check the buffer before flushing it.
			assert.Equal(t, strings.Join(tc.expectedLines, "\n")+"\n", sender.Messages())

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, tc.expectedLines)

			srv.close()
		})
	}
}

func TestTimestampSerialization(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name string
		val  int64
	}{
		{"max value", math.MaxInt64},
		{"zero", 0},
		{"small positive value", 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = sender.Table(testTable).TimestampColumn("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, []string{"my_test_table a_col=" + strconv.FormatInt(tc.val, 10) + "t"})

			srv.close()
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
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = sender.Table(testTable).Int64Column("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, []string{"my_test_table a_col=" + strconv.FormatInt(tc.val, 10) + "i"})

			srv.close()
		})
	}
}

func TestLong256Column(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name string
		val  big.Int
		expected string
	}{
		{"max value", *big.NewInt(math.MaxInt64), strconv.FormatInt(math.MaxInt64, 10)},
		{"zero", *big.NewInt(0), "0"},
		{"positive ten", *big.NewInt(10), "10"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = sender.Table(testTable).Long256Column("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, []string{"my_test_table a_col=" + tc.expected + "i"})

			srv.close()
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
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = sender.Table(testTable).Float64Column("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, []string{"my_test_table a_col=" + tc.expected})

			srv.close()
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
			func(s *qdb.LineSender) error {
				return s.Table(lengthyStr).StringColumn("str_col", "foo").AtNow(ctx)
			},
			"table name length exceeds the limit",
		},
		{
			"lengthy column name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn(lengthyStr, "foo").AtNow(ctx)
			},
			"column name length exceeds the limit",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(readAndDiscard)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr), qdb.WithFileNameLimit(nameLimit))
			assert.NoError(t, err)

			err = tc.writerFn(sender)
			assert.ErrorContains(t, err, tc.expectedErrMsg)

			sender.Close()
			srv.close()
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
			func(s *qdb.LineSender) error {
				return s.Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"At",
			func(s *qdb.LineSender) error {
				return s.Symbol("sym", "abc").At(ctx, 0)
			},
		},
		{
			"symbol",
			func(s *qdb.LineSender) error {
				return s.Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"string column",
			func(s *qdb.LineSender) error {
				return s.StringColumn("str", "abc").AtNow(ctx)
			},
		},
		{
			"boolean column",
			func(s *qdb.LineSender) error {
				return s.BoolColumn("bool", true).AtNow(ctx)
			},
		},
		{
			"long column",
			func(s *qdb.LineSender) error {
				return s.Int64Column("int", 42).AtNow(ctx)
			},
		},
		{
			"double column",
			func(s *qdb.LineSender) error {
				return s.Float64Column("float", 4.2).AtNow(ctx)
			},
		},
		{
			"timestamp column",
			func(s *qdb.LineSender) error {
				return s.TimestampColumn("timestamp", 42).AtNow(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(readAndDiscard)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = tc.writerFn(sender)

			assert.ErrorContains(t, err, "table name was not provided")
			assert.Empty(t, sender.Messages())

			sender.Close()
			srv.close()
		})
	}
}

func TestErrorOnMultipleTableCalls(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.Table(testTable).Table(testTable).AtNow(ctx)

	assert.ErrorContains(t, err, "table name already provided")
	assert.Empty(t, sender.Messages())
}

func TestErrorOnNegativeTimestamp(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.Table(testTable).TimestampColumn("timestamp_col", -42).AtNow(ctx)

	assert.ErrorContains(t, err, "timestamp cannot be negative: -42")
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
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").StringColumn("str", "abc").Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"boolean column",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").BoolColumn("bool", true).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"integer column",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").Int64Column("int", 42).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"float column",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").Float64Column("float", 4.2).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"timestamp column",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").TimestampColumn("timestamp", 42).Symbol("sym", "abc").AtNow(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(readAndDiscard)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = tc.writerFn(sender)

			assert.ErrorContains(t, err, "symbols have to be written before any other column")
			assert.Empty(t, sender.Messages())

			sender.Close()
			srv.close()
		})
	}
}

func TestInvalidMessageGetsDiscarded(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServer(sendToBackChannel)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
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
	expectLines(t, srv.backCh, []string{testTable + " foo=\"bar\""})
}

func TestErrorOnUnavailableServer(t *testing.T) {
	ctx := context.Background()

	_, err := qdb.NewLineSender(ctx)
	assert.ErrorContains(t, err, "failed to connect to server")
}

func TestErrorOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	srv, err := newTestServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
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

	srv, err := newTestServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
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

	srv, err := newTestServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
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
				TimestampColumn("timestamp_col", 42).
				At(ctx, int64(1000*i))
		}
		sender.Flush(ctx)
	}
}

func BenchmarkLineSenderNoFlush(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
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
			TimestampColumn("timestamp_col", 42).
			At(ctx, int64(1000*i))
	}
	sender.Flush(ctx)
}

func expectLines(t *testing.T, linesCh chan string, expected []string) {
	actual := make([]string, 0)
	assert.Eventually(t, func() bool {
		select {
		case l := <-linesCh:
			actual = append(actual, l)
		default:
			return false
		}
		return reflect.DeepEqual(expected, actual)
	}, 3*time.Second, 100*time.Millisecond)
}

type serverType int64

const (
	sendToBackChannel serverType = 0
	readAndDiscard    serverType = 1
)

type testServer struct {
	addr       string
	listener   net.Listener
	serverType serverType
	backCh     chan string
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

func newTestServer(serverType serverType) (*testServer, error) {
	tcp, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return nil, err
	}
	s := &testServer{
		addr:       tcp.Addr().String(),
		listener:   tcp,
		serverType: serverType,
		backCh:     make(chan string),
		closeCh:    make(chan struct{}),
	}
	s.wg.Add(1)
	go s.serve()
	return s, nil
}

func (s *testServer) serve() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
				log.Println("could not accept", err)
			}
			continue
		}

		s.wg.Add(1)
		go func() {
			switch s.serverType {
			case sendToBackChannel:
				s.handleSendToBackChannel(conn)
			case readAndDiscard:
				s.handleReadAndDiscard(conn)
			default:
				panic(fmt.Sprintf("server type is not supported: %d", s.serverType))
			}
			s.wg.Done()
		}()
	}
}

func (s *testServer) handleSendToBackChannel(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	for {
		select {
		case <-s.closeCh:
			return
		default:
			l, err := r.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					continue
				} else {
					log.Println("could not read", err)
					return
				}
			}
			// Remove trailing \n and send line to back channel.
			s.backCh <- l[0 : len(l)-1]
		}
	}
}

func (s *testServer) handleReadAndDiscard(conn net.Conn) {
	defer conn.Close()

	for {
		select {
		case <-s.closeCh:
			return
		default:
			_, err := io.Copy(ioutil.Discard, conn)
			if err != nil {
				if err == io.EOF {
					continue
				} else {
					log.Println("could not read", err)
					return
				}
			}
		}
	}
}

func (s *testServer) close() {
	close(s.closeCh)
	s.listener.Close()
	s.wg.Wait()
}
