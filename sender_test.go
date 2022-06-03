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
	"log"
	"net"
	"reflect"
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
			srv, err := newTestServer()
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			err = tc.writerFn(sender)
			assert.NoError(t, err)

			err = sender.Flush(ctx)
			assert.NoError(t, err)

			sender.Close()

			expectLines(t, srv.linesCh, tc.expectedLines)

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
			srv, err := newTestServer()
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
			srv, err := newTestServer()
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
	addr     string
	listener net.Listener
	linesCh  chan string
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

func newTestServer() (*testServer, error) {
	tcp, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return nil, err
	}
	s := &testServer{
		addr:     tcp.Addr().String(),
		listener: tcp,
		linesCh:  make(chan string),
		closeCh:  make(chan struct{}),
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
			s.handle(conn)
			s.wg.Done()
		}()
	}
}

func (s *testServer) handle(conn net.Conn) {
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
			s.linesCh <- l
		}
	}
}

func (s *testServer) close() {
	close(s.closeCh)
	s.listener.Close()
	s.wg.Wait()
}
