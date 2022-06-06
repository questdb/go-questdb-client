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

type writer func(s *qdb.LineSender) error

func TestValidWrites(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		writerFn      writer
		expectedLines []string
	}{
		{
			"multiple rows",
			func(s *qdb.LineSender) error {
				err := s.Table(testTable).StringColumn("str_col", "foo").IntColumn("long_col", 42).AtNow(ctx)
				if err != nil {
					return err
				}
				err = s.Table(testTable).StringColumn("str_col", "bar").IntColumn("long_col", -42).At(ctx, 42)
				if err != nil {
					return err
				}
				return nil
			},
			[]string{
				"my_test_table str_col=\"foo\",long_col=42i\n",
				"my_test_table str_col=\"bar\",long_col=-42i 42\n",
			},
		},
		{
			"escaped chars in table name",
			func(s *qdb.LineSender) error {
				return s.Table("test 1\"2=3").IntColumn("a_col", 42).AtNow(ctx)
			},
			[]string{
				"test\\ 1\\\"2\\=3 a_col=42i\n",
			},
		},
		{
			"escaped chars in string column",
			func(s *qdb.LineSender) error {
				return s.Table("test_table").StringColumn("name 1\"2=3", "value 1,2\"3\\4=5").AtNow(ctx)
			},
			[]string{
				"test_table name\\ 1\\\"2\\=3=\"value 1,2\\\"3\\\\4=5\"\n",
			},
		},
		{
			"escaped chars in symbol",
			func(s *qdb.LineSender) error {
				return s.Table("test_table").Symbol("name 1\"2=3", "value 1,2\"3\\4=5").AtNow(ctx)
			},
			[]string{
				"test_table,name\\ 1\\\"2\\=3=value\\ 1\\,2\\\"3\\\\4\\=5\n",
			},
		},
		{
			"escaped chars in UTF-8 string",
			func(s *qdb.LineSender) error {
				return s.Table("таблица").StringColumn("имя раз\"два=три", "значение раз,два\"три\\четыре=пять").AtNow(ctx)
			},
			[]string{
				"таблица имя\\ раз\\\"два\\=три=\"значение раз,два\\\"три\\\\четыре=пять\"\n",
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
			assert.Equal(t, strings.Join(tc.expectedLines, ""), sender.Messages())

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, tc.expectedLines)

			srv.close()
		})
	}
}

func TestIntSerialization(t *testing.T) {
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

			err = sender.Table(testTable).IntColumn("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, []string{"my_test_table a_col=" + strconv.FormatInt(tc.val, 10) + "i\n"})

			srv.close()
		})
	}
}

func TestFloatSerialization(t *testing.T) {
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
		{"smallest value", math.SmallestNonzeroFloat64, "5e-324"},
		{"max value", math.MaxFloat64, "1.7976931348623157e+308"},
		{"negative with exponent", -4.2e-99, "-4.2e-99"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = sender.Table(testTable).FloatColumn("a_col", tc.val).AtNow(ctx)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			// Now check what was received by the server.
			expectLines(t, srv.backCh, []string{"my_test_table a_col=" + tc.expected + "\n"})

			srv.close()
		})
	}
}

func TestIllegalChars(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		writerFn writer
		errMsg   string
	}{
		{
			"new line in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo\nbar").StringColumn("a_col", "foo").AtNow(ctx)
			},
			"table or column name contains a new line char",
		},
		{
			"carriage return in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo\rbar").StringColumn("a_col", "foo").AtNow(ctx)
			},
			"table or column name contains a carriage return char",
		},
		{
			"new line in column name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo\nbar", "foo").AtNow(ctx)
			},
			"table or column name contains a new line char",
		},
		{
			"carriage return in column name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo\rbar", "foo").AtNow(ctx)
			},
			"table or column name contains a carriage return char",
		},
		{
			"new line in column value",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("a_col", "foo\nbar").AtNow(ctx)
			},
			"symbol or string column value contains a new line char",
		},
		{
			"carriage return in column value",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("a_col", "foo\rbar").AtNow(ctx)
			},
			"symbol or string column value contains a carriage return char",
		},
		{
			"'.' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo.bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'?' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo?bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"',' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo,bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"':' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo:bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'\\' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo\\bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'/' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo/bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'\\0' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo\x00bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"')' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo)bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'(' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo(bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'+' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo+bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'*' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo*bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'~' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo~bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'%%' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo%%bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'-' in table name",
			func(s *qdb.LineSender) error {
				return s.Table("foo-bar").StringColumn("a_col", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'.' in column name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo.bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'?' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo?bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"',' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo,bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"':' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo:bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'\\' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo\\bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'/' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo/bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'\\0' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo\\0bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"')' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo)bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'(' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo(bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'+' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo+bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'*' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo*bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'~' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo~bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'%%' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo%%bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
		{
			"'-' in table name",
			func(s *qdb.LineSender) error {
				return s.Table(testTable).StringColumn("foo-bar", "42").AtNow(ctx)
			},
			"table or column name contains one of illegal chars",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = tc.writerFn(sender)
			assert.ErrorContains(t, err, tc.errMsg)

			sender.Close()
			srv.close()
		})
	}
}

func TestErrorOnMissingTableCall(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		writerFn writer
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
				return s.IntColumn("int", 42).AtNow(ctx)
			},
		},
		{
			"double column",
			func(s *qdb.LineSender) error {
				return s.FloatColumn("float", 4.2).AtNow(ctx)
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

func TestErrorOnSymbolCallAfterColumn(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		writerFn writer
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
				return s.Table("awesome_table").IntColumn("int", 42).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"float column",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").FloatColumn("float", 4.2).Symbol("sym", "abc").AtNow(ctx)
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
	expectLines(t, srv.backCh, []string{testTable + " foo=\"bar\"\n"})
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

func BenchmarkLineSender(b *testing.B) {
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
				FloatColumn("double_col", float64(i)+0.42).
				IntColumn("long_col", int64(i)).
				StringColumn("str_col", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua").
				BoolColumn("bool_col", true).
				At(ctx, int64(1000*i))
		}
		sender.Flush(ctx)
	}
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
			s.backCh <- l
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
