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
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func noopConnListener() QuestDBOption {
	return WithQuestDBConnectionListener(func(SenderConnectionEvent) {})
}

// TestQuestDBPoolConfigPrecedence is the PoolConfigHonoredTest analogue:
// explicit option > connect-string key > default, observed on the built pools.
// lazy_connect keeps build non-blocking against a down address.
func TestQuestDBPoolConfigPrecedence(t *testing.T) {
	ctx := context.Background()

	// Option wins over the connect-string key.
	db, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;sender_pool_max=2;lazy_connect=true;",
		noopConnListener(), WithSenderPoolMax(7))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if db.senderPool.maxSize != 7 {
		t.Errorf("sender max=%d, want 7 (option over string)", db.senderPool.maxSize)
	}
	db.Close(ctx)

	// Connect-string key wins over the default.
	db, err = NewQuestDB(ctx, "ws::addr=127.0.0.1:1;query_pool_max=3;acquire_timeout_ms=1234;lazy_connect=true;",
		noopConnListener())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if db.queryPool.maxSize != 3 {
		t.Errorf("query max=%d, want 3 (string over default)", db.queryPool.maxSize)
	}
	if db.senderPool.acquireTimeout != 1234*time.Millisecond {
		t.Errorf("acquire=%v, want 1234ms", db.senderPool.acquireTimeout)
	}
	db.Close(ctx)

	// Default when neither is set; lazy_connect forces query min to 0.
	db, err = NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;", noopConnListener())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if db.senderPool.maxSize != qwpDefaultPoolMax {
		t.Errorf("sender max=%d, want %d (default)", db.senderPool.maxSize, qwpDefaultPoolMax)
	}
	if db.queryPool.minSize != 0 {
		t.Errorf("query min=%d, want 0 (lazy_connect)", db.queryPool.minSize)
	}
	db.Close(ctx)
}

// TestQuestDBRejectsZeroAcquireTimeout pins the acquire_timeout_ms > 0
// contract: both pools derive the creation-path dial deadline from it, so an
// explicit 0 would pre-expire every slot-building borrow (and under
// lazy_connect leave the read pool permanently unusable).
func TestQuestDBRejectsZeroAcquireTimeout(t *testing.T) {
	ctx := context.Background()
	_, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;acquire_timeout_ms=0;",
		noopConnListener())
	if err == nil || !strings.Contains(err.Error(), "acquire_timeout_ms must be positive") {
		t.Errorf("acquire_timeout_ms=0: err=%v, want positive-value rejection", err)
	}
	_, err = NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;",
		noopConnListener(), WithAcquireTimeout(0))
	if err == nil || !strings.Contains(err.Error(), "acquire_timeout_ms must be positive") {
		t.Errorf("WithAcquireTimeout(0): err=%v, want positive-value rejection", err)
	}
}

// TestQuestDBHousekeeperIntervalZeroDisables pins housekeeper_interval_ms=0 =
// disabled: no reaper goroutine starts and Close does not sleep out the join
// budget waiting for one.
func TestQuestDBHousekeeperIntervalZeroDisables(t *testing.T) {
	ctx := context.Background()
	db, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;housekeeper_interval_ms=0;",
		noopConnListener())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if db.housekeeper.interval != 0 {
		t.Errorf("interval=%v, want 0 (disabled)", db.housekeeper.interval)
	}
	if db.housekeeper.started.Load() {
		t.Error("disabled housekeeper started its goroutine")
	}
	begin := time.Now()
	if err := db.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 3*time.Second {
		t.Errorf("Close took %v; a disabled housekeeper must not consume the join budget", elapsed)
	}
}

func TestResolvePoolErrors(t *testing.T) {
	if _, err := resolvePoolInt(false, 0, map[string]string{"x": "abc"}, "x", 4); err == nil {
		t.Error("non-int connect-string value should error")
	}
	if _, err := resolvePoolInt(true, -5, nil, "x", 4); err == nil {
		t.Error("negative option should error")
	}
	// -1 is a real argument now, not an "unset" sentinel: it must be rejected,
	// not silently treated as the default.
	if _, err := resolvePoolInt(true, -1, nil, "x", 4); err == nil {
		t.Error("negative option (-1) should error, not fall back to default")
	}
	if _, err := resolvePoolDur(false, 0, map[string]string{"x": "-3"}, "x", time.Second); err == nil {
		t.Error("negative duration value should error")
	}
	if _, err := resolvePoolDur(true, -2, nil, "x", time.Second); err == nil {
		t.Error("negative duration option should error")
	}
	if _, err := resolvePoolDur(true, -1*time.Nanosecond, nil, "x", time.Second); err == nil {
		t.Error("negative duration option (-1ns) should error, not fall back to default")
	}
	if _, err := poolBool(map[string]string{"x": "maybe"}, "x", false); err == nil {
		t.Error("invalid bool should error")
	}
	if b, err := poolBool(map[string]string{"x": "on"}, "x", false); err != nil || !b {
		t.Errorf("poolBool(on)=%v,%v want true,nil", b, err)
	}
	if b, err := poolBool(map[string]string{"x": "off"}, "x", true); err != nil || b {
		t.Errorf("poolBool(off)=%v,%v want false,nil", b, err)
	}
}

// TestQuestDBTeardownOnQueryPoolFailure covers the facade's teardown-hardening
// (Hazard I): when the query pool fails to build, the already-built sender pool
// is closed and the error propagates. The server ACKs ingest but rejects the
// egress upgrade.
func TestQuestDBTeardownOnQueryPoolFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == qwpReadPath {
			w.WriteHeader(http.StatusForbidden) // terminal egress reject → query prewarm fails fast
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

	base := stableGoroutineCount()
	_, err := NewQuestDB(context.Background(),
		"ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";sender_pool_min=2;sender_pool_max=2;",
		noopConnListener())
	if err == nil {
		t.Fatal("expected build to fail when the egress upgrade is rejected")
	}
	// The two prewarmed ingest senders must be torn down when the query pool build
	// fails (Hazard I); a leaked send loop keeps the count above baseline.
	if got := stableGoroutineCount(); got > base+2 {
		t.Fatalf("sender pool leaked goroutines after query-pool failure: base=%d got=%d", base, got)
	}
}

// TestQuestDBAllOptionsApplied covers every facade option and checks the
// resolved values land on the pools/housekeeper.
func TestQuestDBAllOptionsApplied(t *testing.T) {
	ctx := context.Background()
	db, err := NewQuestDB(ctx, "ws::addr=127.0.0.1:1;lazy_connect=true;",
		WithSenderPoolMin(0), WithSenderPoolMax(6),
		WithQueryPoolMin(0), WithQueryPoolMax(5),
		WithAcquireTimeout(2*time.Second),
		WithIdleTimeout(30*time.Second),
		WithMaxLifetime(10*time.Minute),
		WithHousekeeperInterval(time.Second),
		WithQuestDBErrorHandler(func(*SenderError) {}),
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}),
	)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer db.Close(ctx)
	if db.senderPool.minSize != 0 || db.senderPool.maxSize != 6 {
		t.Errorf("sender min/max=%d/%d, want 0/6", db.senderPool.minSize, db.senderPool.maxSize)
	}
	if db.queryPool.maxSize != 5 {
		t.Errorf("query max=%d, want 5", db.queryPool.maxSize)
	}
	if db.senderPool.acquireTimeout != 2*time.Second {
		t.Errorf("acquire=%v, want 2s", db.senderPool.acquireTimeout)
	}
	if db.senderPool.idleTimeout != 30*time.Second {
		t.Errorf("idle=%v, want 30s", db.senderPool.idleTimeout)
	}
	if db.senderPool.maxLifetime != 10*time.Minute {
		t.Errorf("lifetime=%v, want 10m", db.senderPool.maxLifetime)
	}
	if db.senderPool.errorHandler == nil {
		t.Error("errorHandler not wired to the sender pool")
	}
	if db.housekeeper.interval != time.Second {
		t.Errorf("housekeeper interval=%v, want 1s", db.housekeeper.interval)
	}
}

// TestQuestDBHousekeeperReaps drives the housekeeper goroutine end to end: with
// a short interval + idle timeout, surplus pooled senders are reaped to min.
// TestQuestDBReaperRaceVsBorrow drives BorrowSender/Close concurrently against a
// housekeeper reaping on a 1ms cadence with idle_timeout_ms=1, so the
// reap-vs-borrow and reap-vs-return interleavings — guarded by generation
// stamping and the pool mutex — are exercised under -race, not just at
// quiescence. sender_pool_max exceeds the goroutine count, so no borrow ever
// waits at capacity; against a healthy server every borrow must succeed.
func TestQuestDBReaperRaceVsBorrow(t *testing.T) {
	srv := newQuestDBTestServer(t, nil)
	defer srv.Close()
	ctx := context.Background()
	db, err := NewQuestDB(ctx,
		"ws::addr="+strings.TrimPrefix(srv.URL, "http://")+
			";sender_pool_min=0;sender_pool_max=12;idle_timeout_ms=1;housekeeper_interval_ms=1;",
		noopConnListener())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer db.Close(ctx)

	const goroutines = 8
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	deadline := time.Now().Add(400 * time.Millisecond)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				s, berr := db.BorrowSender(ctx)
				if berr != nil {
					errCh <- berr
					return
				}
				_ = s.Close(ctx)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("borrow under reap race: %v", e)
	}
}

func TestQuestDBHousekeeperReaps(t *testing.T) {
	srv := newQuestDBTestServer(t, nil)
	defer srv.Close()
	ctx := context.Background()
	db, err := NewQuestDB(ctx,
		"ws::addr="+strings.TrimPrefix(srv.URL, "http://")+
			";sender_pool_max=4;idle_timeout_ms=1;housekeeper_interval_ms=50;",
		noopConnListener())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer db.Close(ctx)

	var leases []LineSender
	for i := 0; i < 4; i++ {
		s, err := db.BorrowSender(ctx)
		if err != nil {
			t.Fatalf("borrow %d: %v", i, err)
		}
		leases = append(leases, s)
	}
	for _, s := range leases {
		_ = s.Close(ctx)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if total, _, _ := db.senderPool.poolSnapshot(); total <= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	total, _, _ := db.senderPool.poolSnapshot()
	t.Errorf("housekeeper did not reap to min: total=%d", total)
}
