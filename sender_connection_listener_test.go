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

// TestDefaultSenderConnectionListener exercises the loud default listener (used
// when no WithConnectionListener is set) across every kind, including the WARN
// and ERROR branches and an unknown kind; it must classify and log without panic.
func TestDefaultSenderConnectionListener(t *testing.T) {
	for k := SenderConnected; k <= SenderAuthFailed+1; k++ {
		defaultSenderConnectionListener(SenderConnectionEvent{Kind: k, Host: "h", Port: 9000})
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

// listenAndIngest builds a QWP sender against addr with a connection listener
// feeding events, writes one row, and flushes so the send loop is live. It
// returns the sender and the event channel; the caller drains the channel.
func listenAndIngest(t *testing.T, addr string, opts ...LineSenderOption) (LineSender, <-chan SenderConnectionEvent) {
	t.Helper()
	events := make(chan SenderConnectionEvent, 128)
	base := []LineSenderOption{
		WithQwp(), WithAddress(addr),
		WithConnectionListener(func(e SenderConnectionEvent) { events <- e }),
	}
	s, err := NewLineSender(context.Background(), append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	_ = s.Table("t").Int64Column("v", 1).AtNow(context.Background())
	_ = s.Flush(context.Background())
	return s, events
}

// waitForKind drains events until kind is seen (returning it) or the deadline
// fires. seen records every kind observed along the way.
func waitForKind(t *testing.T, events <-chan SenderConnectionEvent, kind SenderConnectionEventKind, seen map[SenderConnectionEventKind]bool) SenderConnectionEvent {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-events:
			seen[e.Kind] = true
			if e.Kind == kind {
				return e
			}
		case <-deadline:
			t.Fatalf("never observed %s; saw %v", kind, seen)
			return SenderConnectionEvent{}
		}
	}
}

// TestConnectionListenerFiresAuthFailed drives a server that serves one
// connection then rejects every reconnect with 403, and asserts the listener
// observes the terminal AUTH_FAILED with a cause.
func TestConnectionListenerFiresAuthFailed(t *testing.T) {
	var connCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if connCount.Add(1) >= 2 {
			w.WriteHeader(http.StatusForbidden) // terminal auth reject on reconnect
			return
		}
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(context.Background()); err != nil {
			return
		}
		conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(0))
		// drop after the first ACK → force a reconnect into the 403
	}))
	defer srv.Close()

	s, events := listenAndIngest(t, strings.TrimPrefix(srv.URL, "http://"))
	defer s.Close(context.Background())

	e := waitForKind(t, events, SenderAuthFailed, map[SenderConnectionEventKind]bool{})
	if e.Cause == nil {
		t.Error("AUTH_FAILED event should carry a cause")
	}
}

// TestConnectionListenerFiresFailedOver drives two endpoints: the first serves
// one connection then rejects reconnects, the second always accepts. The
// listener must observe FAILED_OVER carrying the previous endpoint.
func TestConnectionListenerFiresFailedOver(t *testing.T) {
	var c1 atomic.Int64
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c1.Add(1) >= 2 {
			w.WriteHeader(http.StatusServiceUnavailable) // transient reject on reconnect
			return
		}
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(context.Background()); err != nil {
			return
		}
		conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(0))
	}))
	defer srv1.Close()
	srv2 := newQwpTestServer(t)
	defer srv2.Close()

	addr := strings.TrimPrefix(srv1.URL, "http://") + "," + strings.TrimPrefix(srv2.URL, "http://")
	s, events := listenAndIngest(t, addr)
	defer s.Close(context.Background())

	seen := map[SenderConnectionEventKind]bool{}
	e := waitForKind(t, events, SenderFailedOver, seen)
	if e.PreviousHost == "" {
		t.Errorf("FAILED_OVER should carry the previous endpoint: %s", e)
	}
	if e.Host == "" {
		t.Errorf("FAILED_OVER should carry the new endpoint: %s", e)
	}
}

// TestConnectionListenerOutageOutlastsBudgetNeverTerminal drives a server that
// serves one connection then rejects every reconnect for longer than the
// configured reconnect_max_duration. Under Invariant B the running loop keeps
// retrying — no terminal error latches, no terminal event fires — and it binds
// again the moment the server recovers.
func TestConnectionListenerOutageOutlastsBudgetNeverTerminal(t *testing.T) {
	var rejecting atomic.Bool
	rejecting.Store(true)
	var connCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if connCount.Add(1) >= 2 && rejecting.Load() {
			w.WriteHeader(http.StatusServiceUnavailable) // transient reject while "down"
			return
		}
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
			if connCount.Load() == 1 {
				return // drop the first connection to force the outage
			}
		}
	}))
	defer srv.Close()

	s, events := listenAndIngest(t, strings.TrimPrefix(srv.URL, "http://"),
		WithReconnectPolicy(200*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond))
	defer s.Close(context.Background())

	// The first post-disconnect sweep can exhaust before any dial (the
	// mid-stream demote consumes the only host), so wait for the dial
	// failure first and collect the sweep event along the way.
	seen := map[SenderConnectionEventKind]bool{}
	waitForKind(t, events, SenderEndpointAttemptFailed, seen)
	if !seen[SenderAllEndpointsUnreachable] {
		waitForKind(t, events, SenderAllEndpointsUnreachable, seen)
	}
	// Outlast the configured max duration; the running loop must still be
	// retrying rather than latching a terminal error.
	time.Sleep(500 * time.Millisecond)
	qs := s.(QwpSender)
	if se := qs.LastTerminalError(); se != nil {
		t.Fatalf("running loop went terminal during a transport outage: %v", se)
	}
	rejecting.Store(false)
	waitForKind(t, events, SenderReconnected, seen)
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
	connectedCount := 0
	deadline := time.After(8 * time.Second)
	for !(seen[SenderConnected] && seen[SenderDisconnected] && seen[SenderReconnected]) {
		select {
		case e := <-events:
			seen[e.Kind] = true
			if e.Kind == SenderConnected {
				connectedCount++
			}
		case <-deadline:
			t.Fatalf("did not observe CONNECTED+DISCONNECTED+RECONNECTED; saw %v", seen)
		}
	}
	// CONNECTED fires exactly once per sender lifetime; a reconnect is RECONNECTED,
	// not a second CONNECTED. Drain briefly to catch a spurious extra.
	drain := time.After(200 * time.Millisecond)
	for {
		select {
		case e := <-events:
			if e.Kind == SenderConnected {
				connectedCount++
			}
		case <-drain:
			if connectedCount != 1 {
				t.Fatalf("CONNECTED fired %d times, want exactly 1", connectedCount)
			}
			return
		}
	}
}

// TestConnectionListenerDroppedCounter exercises the sender-level
// DroppedConnectionNotifications() accessor at runtime (the compile-time
// var _ QwpSender never calls it): a fast listener under normal operation drops
// nothing, so it reports 0.
func TestConnectionListenerDroppedCounter(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(context.Background(),
		WithQwp(), WithAddress(addr),
		WithConnectionListener(func(SenderConnectionEvent) {}),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(context.Background())
	qs := s.(QwpSender)
	_ = s.Table("t").Int64Column("v", 1).AtNow(context.Background())
	_ = s.Flush(context.Background())
	if got := qs.DroppedConnectionNotifications(); got != 0 {
		t.Errorf("DroppedConnectionNotifications=%d, want 0 (fast listener, no overflow)", got)
	}
}

// TestConnectionListenerAsyncConnect asserts the async-connect path (used under
// lazy_connect / initial_connect_retry=async) fires exactly one CONNECTED with
// AttemptNumber 0 once the server the sender started against becomes reachable.
func TestConnectionListenerAsyncConnect(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	events := make(chan SenderConnectionEvent, 16)
	s, err := NewLineSender(context.Background(),
		WithQwp(), WithAddress(addr),
		WithInitialConnectMode(InitialConnectAsync),
		WithConnectionListener(func(e SenderConnectionEvent) { events <- e }),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(context.Background())
	_ = s.Table("t").Int64Column("v", 1).AtNow(context.Background())
	_ = s.Flush(context.Background())

	select {
	case e := <-events:
		if e.Kind != SenderConnected {
			t.Fatalf("first async event = %s, want CONNECTED", e.Kind)
		}
		if e.AttemptNumber != 0 {
			t.Errorf("async CONNECTED AttemptNumber = %d, want 0 (initial connect)", e.AttemptNumber)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("async connect never fired CONNECTED")
	}
}

// TestAsyncInitialConnectOutlastsBudgetThenConnects pins Invariant B on the
// async-initial-connect path (and thus lazy_connect pools): a server down for
// longer than reconnect_max_duration must not latch a terminal error; the
// sender connects once the server comes up.
func TestAsyncInitialConnectOutlastsBudgetThenConnects(t *testing.T) {
	var up atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
			conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	s, events := listenAndIngest(t, strings.TrimPrefix(srv.URL, "http://"),
		WithInitialConnectMode(InitialConnectAsync),
		WithReconnectPolicy(200*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond))
	defer s.Close(context.Background())

	// Outlast the configured max duration; the async dial must still be
	// retrying rather than latching a terminal error.
	time.Sleep(500 * time.Millisecond)
	qs := s.(QwpSender)
	if se := qs.LastTerminalError(); se != nil {
		t.Fatalf("async initial connect went terminal during a transport outage: %v", se)
	}
	up.Store(true)
	waitForKind(t, events, SenderConnected, map[SenderConnectionEventKind]bool{})
}
