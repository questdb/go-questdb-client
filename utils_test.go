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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type serverType int64

const (
	sendToBackChannel              serverType = 0
	readAndDiscard                 serverType = 1
	returning500                   serverType = 2
	returning403                   serverType = 3
	returning404                   serverType = 4
	failFirstThenSendToBackChannel serverType = 5
)

type testServer struct {
	addr        string
	tcpListener net.Listener
	serverType  serverType
	BackCh      chan string
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

func (t *testServer) Addr() string {
	return t.addr
}

func newTestTcpServer(serverType serverType) (*testServer, error) {
	return newTestServerWithProtocol(serverType, "tcp")
}

func newTestHttpServer(serverType serverType) (*testServer, error) {
	return newTestServerWithProtocol(serverType, "http")
}

func newTestServerWithProtocol(serverType serverType, protocol string) (*testServer, error) {
	tcp, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return nil, err
	}
	s := &testServer{
		addr:        tcp.Addr().String(),
		tcpListener: tcp,
		serverType:  serverType,
		BackCh:      make(chan string, 1000),
		closeCh:     make(chan struct{}),
	}

	switch protocol {
	case "tcp":
		s.wg.Add(1)
		go s.serveTcp()
	case "http":
		go s.serveHttp()
	default:
		return nil, fmt.Errorf("invalid protocol %q", protocol)
	}

	return s, nil
}

func (s *testServer) serveTcp() {
	defer s.wg.Done()

	for {
		conn, err := s.tcpListener.Accept()
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
			s.BackCh <- l[0 : len(l)-1]
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
			_, err := io.Copy(io.Discard, conn)
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

func (s *testServer) serveHttp() {
	lineFeed := make(chan string)

	go func() {
		for {
			select {
			case <-s.closeCh:
				return
			case l := <-lineFeed:
				s.BackCh <- l
			}
		}
	}()

	var reqs int64
	http.Serve(s.tcpListener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			err error
		)

		switch s.serverType {
		case failFirstThenSendToBackChannel:
			if atomic.AddInt64(&reqs, 1) == 1 {
				// Consume request body.
				_, err = io.Copy(io.Discard, r.Body)
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				err = readAndSendToBackChannel(r, lineFeed)
			}
		case sendToBackChannel:
			err = readAndSendToBackChannel(r, lineFeed)
		case readAndDiscard:
			_, err = io.Copy(io.Discard, r.Body)
		case returning500:
			w.WriteHeader(http.StatusInternalServerError)
		case returning403:
			w.WriteHeader(http.StatusForbidden)
			io.WriteString(w, "Forbidden")
		case returning404:
			w.WriteHeader(http.StatusNotFound)
			data, err := json.Marshal(map[string]interface{}{
				"code":    "404",
				"message": "Not Found",
				"line":    42,
				"errorId": "Not Found",
			})
			if err != nil {
				panic(err)
			}
			w.Write(data)
		default:
			panic(fmt.Sprintf("server type is not supported: %d", s.serverType))
		}

		if err != nil {
			if err != io.EOF {
				log.Println("could not read", err)
			}
		}
	}))
}

func readAndSendToBackChannel(r *http.Request, lineFeed chan string) error {
	read := bufio.NewReader(r.Body)
	var (
		l   string
		err error
	)
	for err == nil {
		l, err = read.ReadString('\n')
		if err == nil && len(l) > 0 {
			lineFeed <- l[0 : len(l)-1]
		}
	}
	return err
}

func (s *testServer) Close() {
	close(s.closeCh)
	s.tcpListener.Close()
	s.wg.Wait()
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
