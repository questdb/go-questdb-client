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
	"io"
	"io/ioutil"
	"log"
	"net"
	"reflect"
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
			"single string column",
			func(s *qdb.LineSender) error {
				err := s.Table(testTable).StringField("a_col", "foo").AtNow(ctx)
				if err != nil {
					return err
				}
				err = s.Table(testTable).StringField("a_col", "bar").At(ctx, 42)
				if err != nil {
					return err
				}
				return nil
			},
			[]string{
				"my_test_table a_col=\"foo\"\n",
				"my_test_table a_col=\"bar\" 42\n",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(true)
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
			"string field",
			func(s *qdb.LineSender) error {
				return s.StringField("str", "abc").AtNow(ctx)
			},
		},
		{
			"boolean field",
			func(s *qdb.LineSender) error {
				return s.BooleanField("bool", true).AtNow(ctx)
			},
		},
		{
			"integer field",
			func(s *qdb.LineSender) error {
				return s.IntegerField("int", 42).AtNow(ctx)
			},
		},
		{
			"float field",
			func(s *qdb.LineSender) error {
				return s.FloatField("float", 4.2).AtNow(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(false)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = tc.writerFn(sender)

			assert.EqualError(t, err, "table name was not provided")
			assert.Empty(t, sender.Messages())

			sender.Close()
			srv.close()
		})
	}
}

func TestErrorOnMultipleTableCalls(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServer(false)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.Table(testTable).Table(testTable).AtNow(ctx)

	assert.EqualError(t, err, "table name already provided")
	assert.Empty(t, sender.Messages())
}

func TestErrorOnSymbolCallAfterField(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		writerFn writer
	}{
		{
			"string field",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").StringField("str", "abc").Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"boolean field",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").BooleanField("bool", true).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"integer field",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").IntegerField("int", 42).Symbol("sym", "abc").AtNow(ctx)
			},
		},
		{
			"float field",
			func(s *qdb.LineSender) error {
				return s.Table("awesome_table").FloatField("float", 4.2).Symbol("sym", "abc").AtNow(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newTestServer(false)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = tc.writerFn(sender)

			assert.EqualError(t, err, "symbol has to be written before any field")
			assert.Empty(t, sender.Messages())

			sender.Close()
			srv.close()
		})
	}
}

func TestInvalidMessageGetsDiscarded(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServer(true)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
	assert.NoError(t, err)
	defer sender.Close()

	// Write a valid message.
	err = sender.Table(testTable).StringField("foo", "bar").AtNow(ctx)
	assert.NoError(t, err)
	// Then write perform an incorrect chain of calls.
	err = sender.Table(testTable).StringField("foo", "bar").Symbol("sym", "42").AtNow(ctx)
	assert.Error(t, err)

	// The second message should be discarded.
	err = sender.Flush(ctx)
	assert.NoError(t, err)
	expectLines(t, srv.backCh, []string{testTable + " foo=\"bar\"\n"})
}

func BenchmarkLineSender(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestServer(false)
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
				FloatField("double_col", float64(i)+0.42).
				IntegerField("long_col", int64(i)).
				StringField("str_col", "foobar").
				BooleanField("bool_col", true).
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
	}, 10*time.Second, 100*time.Millisecond)
}

type testServer struct {
	addr      string
	listener  net.Listener
	useBackCh bool
	backCh    chan string
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

func newTestServer(useBackCh bool) (*testServer, error) {
	tcp, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return nil, err
	}
	s := &testServer{
		addr:      tcp.Addr().String(),
		listener:  tcp,
		useBackCh: useBackCh,
		backCh:    make(chan string),
		closeCh:   make(chan struct{}),
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
			if s.useBackCh {
				s.handleWithBackChannel(conn)
			} else {
				s.handleWithDiscard(conn)
			}
			s.wg.Done()
		}()
	}
}

func (s *testServer) handleWithBackChannel(conn net.Conn) {
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

func (s *testServer) handleWithDiscard(conn net.Conn) {
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
