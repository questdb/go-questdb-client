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
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestConnectionListenerConfQwpOnly(t *testing.T) {
	if _, err := confFromStr("ws::addr=a:9000;connection_listener_inbox_capacity=64;"); err != nil {
		t.Fatalf("ws: %v", err)
	}
	for _, schema := range []string{"http", "tcp"} {
		_, err := confFromStr(schema + "::addr=a:9000;connection_listener_inbox_capacity=64;")
		if err == nil {
			t.Errorf("%s: expected connection_listener_inbox_capacity to be QWP-only", schema)
		}
	}
	for _, v := range []string{"5", "abc"} { // < min (16) and non-int
		if _, err := confFromStr("ws::addr=a:9000;connection_listener_inbox_capacity=" + v + ";"); err == nil {
			t.Errorf("connection_listener_inbox_capacity=%q: expected error", v)
		}
	}
}

func TestConnectionListenerOptionsQwpOnly(t *testing.T) {
	c := newLineSenderConfig(qwpSenderType)
	WithConnectionListener(func(SenderConnectionEvent) {})(c)
	WithConnectionListenerInboxCapacity(128)(c)
	if c.connectionListener == nil || c.connectionListenerInboxCapacity != 128 {
		t.Fatalf("options not applied: listener=%v cap=%d", c.connectionListener != nil, c.connectionListenerInboxCapacity)
	}
	// Rejected on a non-QWP sender.
	h := newLineSenderConfig(httpSenderType)
	WithConnectionListener(func(SenderConnectionEvent) {})(h)
	if err := sanitizeHttpConf(h); err == nil {
		t.Fatal("expected connection listener to be rejected on HTTP")
	}
}

func TestConnectionListenerFiresConnected(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()

	events := make(chan SenderConnectionEvent, 8)
	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addr),
		WithConnectionListener(func(e SenderConnectionEvent) { events <- e }),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(context.Background())

	select {
	case e := <-events:
		if e.Kind != SenderConnected {
			t.Fatalf("first event kind = %s, want CONNECTED", e.Kind)
		}
		if e.Host == "" || e.Port == 0 {
			t.Errorf("CONNECTED event missing endpoint: %s", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("listener never observed CONNECTED")
	}
}

func TestSenderConnectionEventString(t *testing.T) {
	e := SenderConnectionEvent{Kind: SenderFailedOver, Host: "h2", Port: 9000, PreviousHost: "h1", PreviousPort: 9000, AttemptNumber: 3}
	s := e.String()
	for _, want := range []string{"FAILED_OVER", "h2:9000", "h1:9000", "attempt=3"} {
		if !strings.Contains(s, want) {
			t.Errorf("String()=%q missing %q", s, want)
		}
	}
}

// TestConnectionListenerDispatcherDrops exercises the generic dispatcher behind
// the listener: under a blocked handler, overflow drops events and bumps the
// dropped counter (the listener path's first direct overflow test).
func TestConnectionListenerDispatcherDrops(t *testing.T) {
	block := make(chan struct{})
	d := newQwpConnDispatcher(func(SenderConnectionEvent) { <-block }, 2)
	defer d.close()
	defer close(block)
	for i := 0; i < 50; i++ {
		d.offer(&SenderConnectionEvent{Kind: SenderConnected})
	}
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 50; i++ {
		d.offer(&SenderConnectionEvent{Kind: SenderConnected})
	}
	if d.droppedNotifications() == 0 {
		t.Error("expected dropped > 0 under a blocked handler")
	}
}

// TestConnectionListenerObservesReconnect drives a server that drops the first
// connection after one ACK, forcing the send loop to reconnect, and asserts the
// listener observes CONNECTED → DISCONNECTED → RECONNECTED.
func TestConnectionListenerObservesReconnect(t *testing.T) {
	var connCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		first := connCount.Add(1) == 1
		var seq int64
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
			if first {
				return // drop after the first ACK → force a reconnect
			}
		}
	}))
	defer srv.Close()

	events := make(chan SenderConnectionEvent, 32)
	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(context.Background(),
		WithQwp(), WithAddress(addr),
		WithConnectionListener(func(e SenderConnectionEvent) { events <- e }),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(context.Background())
	_ = s.Table("t").Int64Column("v", 1).AtNow(context.Background())
	_ = s.Flush(context.Background())

	seen := map[SenderConnectionEventKind]bool{}
	deadline := time.After(8 * time.Second)
	for !(seen[SenderConnected] && seen[SenderDisconnected] && seen[SenderReconnected]) {
		select {
		case e := <-events:
			seen[e.Kind] = true
		case <-deadline:
			t.Fatalf("did not observe CONNECTED+DISCONNECTED+RECONNECTED; saw %v", seen)
		}
	}
}
