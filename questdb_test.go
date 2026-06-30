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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// TestPoolKeysRejectedOnNonWs pins that the facade-owned pool keys are accepted
// only on ws/wss: an http or tcp config rejects each one, so they cannot be
// silently ignored on a transport with no pool.
func TestPoolKeysRejectedOnNonWs(t *testing.T) {
	keys := []string{
		"lazy_connect=true", "sender_pool_min=1", "sender_pool_max=2",
		"query_pool_min=0", "query_pool_max=2", "acquire_timeout_ms=1000",
		"idle_timeout_ms=1000", "max_lifetime_ms=1000", "housekeeper_interval_ms=100",
	}
	for _, schema := range []string{"http", "tcp"} {
		for _, key := range keys {
			conf := schema + "::addr=localhost:9000;" + key + ";"
			if _, err := confFromStr(conf); err == nil {
				t.Errorf("%s: pool key %q should be rejected on a %s config", schema, key, schema)
			}
		}
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

// TestNewQuestDBDownServerDefaultConfig pins the default config (both pools
// prewarming, no query_pool_min=0) against a down server: it must return an
// error and a nil handle, never panic or hand back a half-built land mine.
func TestNewQuestDBDownServerDefaultConfig(t *testing.T) {
	db, err := NewQuestDB(context.Background(), "ws::addr=127.0.0.1:1;")
	if err == nil {
		if db != nil {
			db.Close(context.Background())
		}
		t.Fatal("default-config build against a down server should return an error")
	}
	if db != nil {
		t.Errorf("expected a nil handle on build failure, got %v", db)
	}
}

// TestQuestDBCloseWithOutstandingLease checks that QuestDB.Close while a borrowed
// sender is still being written never reaches into the in-use delegate (a data
// race the -race build would catch), and that returning the lease afterward
// closes its connection cleanly.
func TestQuestDBCloseWithOutstandingLease(t *testing.T) {
	ctx := context.Background()
	srv := newQuestDBTestServer(t, nil)
	defer srv.Close()
	db, err := NewQuestDB(ctx, "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	s, err := db.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("BorrowSender: %v", err)
	}

	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Table("t").Int64Column("v", 1).AtNow(ctx)
				_ = s.Flush(ctx)
			}
		}
	}()
	time.Sleep(20 * time.Millisecond)

	if err := db.Close(ctx); err != nil {
		t.Logf("db.Close: %v", err)
	}
	close(stop)
	<-done

	// The outstanding lease closes its own delegate on return, post-teardown.
	if err := s.Close(ctx); err != nil {
		t.Logf("lease Close: %v", err)
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

// TestQuestDBCloseIdempotentAndConcurrent pins the three documented Close
// contracts: idempotent (a second Close is a no-op), safe under concurrent
// callers (closeOnce), and every caller observes the identical latched result.
func TestQuestDBCloseIdempotentAndConcurrent(t *testing.T) {
	ctx := context.Background()
	srv := newQuestDBTestServer(t, nil)
	defer srv.Close()
	db, err := NewQuestDB(ctx, "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = db.Close(ctx) // -race proves closeOnce serialises teardown
		}(i)
	}
	wg.Wait()

	// closeOnce → every concurrent caller sees the same latched error.
	for i, e := range errs {
		if e != errs[0] {
			t.Errorf("concurrent Close[%d]=%v, want identical to %v", i, e, errs[0])
		}
	}
	// A clean teardown latches no error.
	if errs[0] != nil {
		t.Errorf("clean teardown Close=%v, want nil", errs[0])
	}
	// A later Close stays a no-op and returns the same latched result.
	if err := db.Close(ctx); err != nil {
		t.Errorf("post-hoc Close=%v, want nil (idempotent)", err)
	}
}

// TestFirstCloseErrPrecedence covers the teardown-error precedence Close uses:
// sender pool (owns flocks/I/O) over query pool over housekeeper, with nil only
// when every step succeeded.
func TestFirstCloseErrPrecedence(t *testing.T) {
	sErr := errors.New("sender")
	qErr := errors.New("query")
	hErr := errors.New("housekeeper")
	cases := []struct {
		name          string
		s, q, h, want error
	}{
		{"sender wins over all", sErr, qErr, hErr, sErr},
		{"sender wins over query", sErr, qErr, nil, sErr},
		{"sender wins, query nil", sErr, nil, hErr, sErr},
		{"query wins over housekeeper", nil, qErr, hErr, qErr},
		{"housekeeper last", nil, nil, hErr, hErr},
		{"all clean", nil, nil, nil, nil},
	}
	for _, c := range cases {
		if got := firstCloseErr(c.s, c.q, c.h); got != c.want {
			t.Errorf("%s: firstCloseErr=%v, want %v", c.name, got, c.want)
		}
	}
}
