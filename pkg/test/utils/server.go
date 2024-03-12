package utils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type ServerType int64

const (
	SendToBackChannel ServerType = 0
	ReadAndDiscard    ServerType = 1
	Returning500      ServerType = 2
	Returning403      ServerType = 3
	Returning404      ServerType = 4
)

type TestServer struct {
	addr        string
	tcpListener net.Listener
	serverType  ServerType
	BackCh      chan string
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

func (t *TestServer) Addr() string {
	return t.addr
}

func NewTestTcpServer(serverType ServerType) (*TestServer, error) {
	return NewTestServerWithProtocol(serverType, "tcp")
}

func NewTestHttpServer(serverType ServerType) (*TestServer, error) {
	return NewTestServerWithProtocol(serverType, "http")
}

func NewTestServerWithProtocol(serverType ServerType, protocol string) (*TestServer, error) {
	tcp, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return nil, err
	}
	s := &TestServer{
		addr:        tcp.Addr().String(),
		tcpListener: tcp,
		serverType:  serverType,
		BackCh:      make(chan string, 5),
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

func (s *TestServer) serveTcp() {
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
			case SendToBackChannel:
				s.handleSendToBackChannel(conn)
			case ReadAndDiscard:
				s.handleReadAndDiscard(conn)
			default:
				panic(fmt.Sprintf("server type is not supported: %d", s.serverType))
			}
			s.wg.Done()
		}()
	}
}

func (s *TestServer) handleSendToBackChannel(conn net.Conn) {
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

func (s *TestServer) handleReadAndDiscard(conn net.Conn) {
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

func (s *TestServer) serveHttp() {
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

	http.Serve(s.tcpListener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			err error
		)

		switch s.serverType {
		case SendToBackChannel:
			r := bufio.NewReader(r.Body)
			var l string
			for err == nil {
				l, err = r.ReadString('\n')
				if err == nil && len(l) > 0 {
					lineFeed <- l[0 : len(l)-1]
				}
			}
		case ReadAndDiscard:
			_, err = io.Copy(ioutil.Discard, r.Body)
		case Returning500:
			w.WriteHeader(http.StatusInternalServerError)
		case Returning403:
			w.WriteHeader(http.StatusForbidden)
			io.WriteString(w, "Forbidden")
		case Returning404:
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

func (s *TestServer) Close() {
	close(s.closeCh)
	s.tcpListener.Close()
	s.wg.Wait()
}

func ExpectLines(t *testing.T, linesCh chan string, expected []string) {
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
