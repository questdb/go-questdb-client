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
	"testing"
	"time"

	"github.com/coder/websocket"
)

// newQuestDBTestServer serves both QWP directions from one address like a real
// node: the egress (/read) path emits SERVER_INFO then idles; the ingest
// (/write) path ACKs every frame and signals gotData on the first one.
func newQuestDBTestServer(t *testing.T, gotData chan<- struct{}) *httptest.Server {
	t.Helper()
	var once bool
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if r.URL.Path == qwpReadPath {
			info := buildServerInfoFrame(qwpVersion, 0, qwpRolePrimary, 1, 0,
				1_700_000_000_000_000_000, "test-cluster", "node")
			_ = conn.Write(r.Context(), websocket.MessageBinary, info)
			for {
				if _, _, err := conn.Read(context.Background()); err != nil {
					return
				}
			}
		}
		var seq int64
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
			if !once && gotData != nil {
				once = true
				select {
				case gotData <- struct{}{}:
				default:
				}
			}
			_ = conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
}

func TestQuestDBFacadeEndToEnd(t *testing.T) {
	ctx := context.Background()
	gotData := make(chan struct{}, 1)
	srv := newQuestDBTestServer(t, gotData)
	defer srv.Close()

	db, err := NewQuestDB(ctx, "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";",
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer db.Close(ctx)

	// Ingest: borrow a sender, write a row, flush — the send loop delivers it.
	s, err := db.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("BorrowSender: %v", err)
	}
	if err := s.Table("trades").Symbol("sym", "BTC").Int64Column("qty", 1).AtNow(ctx); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case <-gotData:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the flushed batch")
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("sender close: %v", err)
	}

	// Egress: borrow a query session (the prewarmed client is already connected).
	q, err := db.BorrowQuery(ctx)
	if err != nil {
		t.Fatalf("BorrowQuery: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("query close: %v", err)
	}
}

// TestQuestDBBorrowSenderExposesQwpSender pins the Java-parity contract that a
// borrowed lease is the full sender: callers type-assert it to QwpSender and reach
// the binary-protocol-only column types, exactly as with a standalone QWP sender.
func TestQuestDBBorrowSenderExposesQwpSender(t *testing.T) {
	ctx := context.Background()
	gotData := make(chan struct{}, 1)
	srv := newQuestDBTestServer(t, gotData)
	defer srv.Close()

	db, err := NewQuestDB(ctx, "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer db.Close(ctx)

	s, err := db.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("BorrowSender: %v", err)
	}
	qs, ok := s.(QwpSender)
	if !ok {
		t.Fatalf("borrowed lease %T does not implement QwpSender", s)
	}
	// QWP-only column types, fluent through the lease, plus AtNano. Table/Symbol
	// return LineSender (errors latch), so the QWP chain starts from a QWP method —
	// the same idiom as a standalone QWP sender.
	qs.Table("trades")
	qs.Symbol("sym", "BTC")
	if err := qs.
		ByteColumn("flags", 7).
		Int32Column("seq", 42).
		UuidColumn("id", 1, 2).
		AtNano(ctx, time.Unix(0, 1_700_000_000_000_000_000)); err != nil {
		t.Fatalf("write via QwpSender lease: %v", err)
	}
	if _, err := qs.FlushAndGetSequence(ctx); err != nil {
		t.Fatalf("FlushAndGetSequence: %v", err)
	}
	select {
	case <-gotData:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the flushed batch")
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// After return, the lease is stale: QWP producer calls error, accessors report
	// a dead lease's zero value instead of leaking the re-borrowed slot's state.
	if err := qs.AtNano(ctx, time.Unix(0, 1)); err != errStaleLease {
		t.Errorf("stale AtNano err=%v, want errStaleLease", err)
	}
	if got := qs.AckedFsn(); got != -1 {
		t.Errorf("stale AckedFsn=%d, want -1", got)
	}
	if got := qs.TotalReconnectAttempts(); got != 0 {
		t.Errorf("stale TotalReconnectAttempts=%d, want 0", got)
	}
	if got := qs.BackgroundDrainers(); got != nil {
		t.Errorf("stale BackgroundDrainers=%v, want nil", got)
	}
}

func TestQuestDBRejectsNonWsSchema(t *testing.T) {
	for _, conf := range []string{"http::addr=localhost:9000;", "tcp::addr=localhost:9009;"} {
		if _, err := Connect(context.Background(), conf); err == nil {
			t.Errorf("Connect(%q): expected ws/wss schema rejection", conf)
		}
	}
}

func TestQuestDBLazyConnectConflicts(t *testing.T) {
	ctx := context.Background()
	// Non-async initial_connect_retry contradicts lazy_connect's non-blocking startup.
	_, err := Connect(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;initial_connect_retry=on;")
	if err == nil || !strings.Contains(err.Error(), "conflicting configuration") {
		t.Errorf("initial_connect_retry conflict err=%v", err)
	}
	// Explicit query_pool_min>0 (connect string) contradicts lazy reads.
	_, err = Connect(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;query_pool_min=2;")
	if err == nil || !strings.Contains(err.Error(), "conflicting configuration") {
		t.Errorf("query_pool_min conflict (string) err=%v", err)
	}
	// Same conflict via a builder option.
	_, err = NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;", WithQueryPoolMin(2))
	if err == nil || !strings.Contains(err.Error(), "conflicting configuration") {
		t.Errorf("query_pool_min conflict (option) err=%v", err)
	}
	// async is allowed.
	db, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;initial_connect_retry=async;",
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("lazy_connect + async should build: %v", err)
	}
	db.Close(ctx)
}

func TestQuestDBLazyConnectBuildsWithServerDown(t *testing.T) {
	ctx := context.Background()
	// 127.0.0.1:1 is refused/unreachable; lazy_connect must still build (ingest
	// async, read pool min=0 — neither side fail-fasts).
	db, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;",
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("lazy_connect build with server down: %v", err)
	}
	defer db.Close(ctx)
	if total, _ := db.queryPool.poolSnapshot(); total != 0 {
		t.Errorf("read pool prewarmed total=%d, want 0 (lazy)", total)
	}
	// The ingest pool stays at its default min (1), built async so it didn't block.
	if total, _, _ := db.senderPool.poolSnapshot(); total != 1 {
		t.Errorf("sender pool total=%d, want 1", total)
	}
}

func TestQuestDBPoolKeysResolved(t *testing.T) {
	// Pool keys + lazy_connect are accepted on a ws config by both parsers.
	if _, err := confFromStr("ws::addr=a:9000;sender_pool_max=8;query_pool_min=0;lazy_connect=true;"); err != nil {
		t.Errorf("ingress parser rejected pool keys: %v", err)
	}
	if _, err := parseQwpQueryConf("ws::addr=a:9000;sender_pool_max=8;acquire_timeout_ms=1000;lazy_connect=true;"); err != nil {
		t.Errorf("egress parser rejected pool keys: %v", err)
	}
}

func TestWithDefaultAsyncConnect(t *testing.T) {
	if got := withDefaultAsyncConnect("noseparator"); got != "noseparator" {
		t.Errorf("no-separator config changed: %q", got)
	}
	got := withDefaultAsyncConnect("ws::addr=x;")
	if !strings.HasPrefix(got, "ws::initial_connect_retry=async;") {
		t.Errorf("async not injected after schema: %q", got)
	}
}

func TestNewQuestDBErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := NewQuestDB(ctx, "not a config string"); err == nil {
		t.Error("malformed config should error")
	}
	if _, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;sender_pool_max=abc;lazy_connect=true;"); err == nil {
		t.Error("non-int pool key should error")
	}
	if _, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=maybe;"); err == nil {
		t.Error("invalid lazy_connect should error")
	}
}

func TestNewQuestDBSenderPrewarmFailure(t *testing.T) {
	// Not lazy: the sender pool prewarms (min=1) against a down server, so the
	// build fails at the sender pool (before the query pool).
	_, err := NewQuestDB(context.Background(), "ws::addr=127.0.0.1:1;query_pool_min=0;")
	if err == nil {
		t.Error("eager build against a down server should fail")
	}
}

func TestNewQuestDBResolveErrors(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		conf string
		opts []QuestDBOption
	}{
		{"bad-ingest-key", "ws::addr=127.0.0.1:1;init_buf_size=abc;", nil},
		{"sender-min", "ws::addr=127.0.0.1:1;", []QuestDBOption{WithSenderPoolMin(-2)}},
		{"query-min", "ws::addr=127.0.0.1:1;", []QuestDBOption{WithQueryPoolMin(-2)}},
		{"query-max", "ws::addr=127.0.0.1:1;", []QuestDBOption{WithQueryPoolMax(-2)}},
		{"acquire", "ws::addr=127.0.0.1:1;", []QuestDBOption{WithAcquireTimeout(-2)}},
		{"idle", "ws::addr=127.0.0.1:1;", []QuestDBOption{WithIdleTimeout(-2)}},
		{"lifetime", "ws::addr=127.0.0.1:1;", []QuestDBOption{WithMaxLifetime(-2)}},
		{"hk-interval", "ws::addr=127.0.0.1:1;", []QuestDBOption{WithHousekeeperInterval(-2)}},
		{"lazy-query-min-nonint", "ws::addr=127.0.0.1:1;lazy_connect=true;query_pool_min=abc;", nil},
	}
	for _, c := range cases {
		if _, err := NewQuestDB(ctx, c.conf, c.opts...); err == nil {
			t.Errorf("%s: expected build error", c.name)
		}
	}
}

func TestCloseStepRecoversPanic(t *testing.T) {
	if err := closeStep(func() error { panic("teardown boom") }); err == nil {
		t.Error("closeStep should convert a panic into an error")
	}
}
