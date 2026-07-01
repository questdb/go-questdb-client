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
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func senderPoolWithIdle(t *testing.T, extra string, min, max int, idle time.Duration) *qwpSenderPool {
	t.Helper()
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";" + extra
	p, err := newQwpSenderPool(context.Background(), conf, min, max,
		500*time.Millisecond, idle, 0, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })
	return p
}

// TestQwpSenderPoolAcquireWaitWakesOnGiveBack asserts the success-wait path: a
// borrow that blocks on an exhausted pool wakes and succeeds when another lease
// is returned, rather than riding the acquire timeout out.
func TestQwpSenderPoolAcquireWaitWakesOnGiveBack(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 1, 2*time.Second, 0, 0, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	defer p.close(context.Background())
	ctx := context.Background()

	s1, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow s1: %v", err)
	}

	got := make(chan error, 1)
	go func() {
		s2, err := p.borrow(ctx) // blocks: max=1 and s1 is out
		if err == nil {
			_ = s2.Close(ctx)
		}
		got <- err
	}()

	time.Sleep(50 * time.Millisecond) // let the waiter park
	if err := s1.Close(ctx); err != nil {
		t.Fatalf("close s1: %v", err)
	}
	select {
	case err := <-got:
		if err != nil {
			t.Errorf("waiting borrow should succeed on giveBack, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting borrow never woke on giveBack")
	}
}

// TestQwpSenderPoolSparesUnackedSlot pins M1: an idle memory-mode slot that
// still holds published-but-unacked rows is not reaped (which would destroy
// them), even past idle_timeout, so the reconnect/replay window can deliver them.
func TestQwpSenderPoolSparesUnackedSlot(t *testing.T) {
	// Accepts the upgrade and reads frames but never ACKs → published > acked.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	// close_flush_timeout_millis keeps the final teardown from draining the
	// unacked slot for the full default budget against the non-ACKing server.
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";close_flush_timeout_millis=100;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 2,
		500*time.Millisecond, time.Millisecond /* idle */, 0, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	defer p.close(context.Background())
	ctx := context.Background()

	s, err := p.borrow(ctx) // grows to 1
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	_ = s.Table("t").Int64Column("v", 1).AtNow(ctx)
	_ = s.Close(ctx) // Close flushes (publishes); the server never ACKs → unacked

	time.Sleep(10 * time.Millisecond) // exceed idle_timeout
	p.reapIdle()
	if total, _, _ := p.poolSnapshot(); total != 1 {
		t.Errorf("unacked idle slot should be spared from reap: total=%d, want 1", total)
	}
}

// TestQwpSenderPoolReapsOverAge drives the max_lifetime (over-age) reap branch
// in isolation: idle_timeout is 0 so only the lifetime test can fire.
func TestQwpSenderPoolReapsOverAge(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 2,
		500*time.Millisecond, 0 /* idle off */, time.Millisecond /* max_lifetime */, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	defer p.close(context.Background())
	ctx := context.Background()

	s, err := p.borrow(ctx) // grows to 1
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	_ = s.Close(ctx) // returned → idle in available, but over-age once the budget elapses
	if total, _, _ := p.poolSnapshot(); total != 1 {
		t.Fatalf("total=%d, want 1", total)
	}
	time.Sleep(5 * time.Millisecond) // exceed max_lifetime
	p.reapIdle()
	if total, _, _ := p.poolSnapshot(); total != 0 {
		t.Errorf("over-age slot not reaped: total=%d, want 0 (min=0)", total)
	}
}

// TestQwpSenderPoolReapsToMin pins the reapIdle fix: a single sweep must shrink
// the pool all the way to minSize (the earlier double-count bug reaped only
// ~half the excess per sweep).
func TestQwpSenderPoolReapsToMin(t *testing.T) {
	p := senderPoolWithIdle(t, "", 1, 4, time.Millisecond)
	ctx := context.Background()
	var leases []LineSender
	for i := 0; i < 4; i++ {
		s, err := p.borrow(ctx)
		if err != nil {
			t.Fatalf("borrow %d: %v", i, err)
		}
		leases = append(leases, s)
	}
	if total, _, _ := p.poolSnapshot(); total != 4 {
		t.Fatalf("grew to total=%d, want 4", total)
	}
	for _, s := range leases {
		_ = s.Close(ctx)
	}
	time.Sleep(5 * time.Millisecond) // exceed idle
	p.reapIdle()
	if total, avail, _ := p.poolSnapshot(); total != 1 || avail != 1 {
		t.Errorf("after reap total=%d avail=%d, want 1/1", total, avail)
	}
	// The freed SF/memory slots are reusable — a fresh borrow succeeds.
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow after reap: %v", err)
	}
	_ = s.Close(ctx)
}

// TestQwpSenderPoolSfStrandedSlotRecovered pins the critical Hazard-A fix:
// a recovered in-range slot reserves its index, so a later growth borrow does
// not re-allocate the live index and collide on the slot flock.
func TestQwpSenderPoolSfStrandedSlotRecovered(t *testing.T) {
	sfDir := t.TempDir()
	ctx := context.Background()

	// Phase 1: an async sender to a down host buffers a row durably into the
	// slot dir default-2, then closes without the server ever acking it.
	s1, err := LineSenderFromConf(ctx,
		"ws::addr=127.0.0.1:1;sf_dir="+sfDir+";sender_id=default-2;"+
			"initial_connect_retry=async;close_flush_timeout_millis=0;")
	if err != nil {
		t.Fatalf("phase 1 open: %v", err)
	}
	_ = s1.Table("t").Int64Column("v", 1).AtNow(ctx)
	_ = s1.Flush(ctx)
	time.Sleep(50 * time.Millisecond)
	_ = s1.Close(ctx)
	if !qwpSfIsCandidateOrphan(filepath.Join(sfDir, "default-2")) {
		t.Skip("no stranded segment produced in this environment")
	}

	// Phase 2: a pool on the same sf_dir against an up server recovers slot 2.
	srv := newQwpTestServer(t)
	defer srv.Close()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";sf_dir=" + sfDir + ";"
	p, err := newQwpSenderPool(ctx, conf, 1, 4, 500*time.Millisecond, 0, 0, nil, nil)
	if err != nil {
		t.Fatalf("phase 2 build: %v", err)
	}
	defer p.close(ctx)

	if total, _, _ := p.poolSnapshot(); total != 2 {
		t.Fatalf("total=%d, want 2 (prewarm slot 0 + recovered slot 2)", total)
	}
	p.mu.Lock()
	reserved := p.slotInUse[2]
	p.mu.Unlock()
	if !reserved {
		t.Fatal("slotInUse[2] not reserved after recovery — Hazard A regression")
	}
	// Grow to max: indices 1 and 3 are allocated, never the live index 2, so no
	// "slot already in use" collision.
	var leases []LineSender
	for i := 0; i < 2; i++ {
		s, err := p.borrow(ctx)
		if err != nil {
			t.Fatalf("growth borrow %d collided with the recovered slot: %v", i, err)
		}
		leases = append(leases, s)
	}
	for _, s := range leases {
		_ = s.Close(ctx)
	}
}

// TestQwpSenderPoolBrokenSlotDiscarded pins the broken-slot path: a lease broken
// by a genuine terminal HALT is discarded on Close, not recycled. A benign
// validation latch no longer breaks the slot (M4), so the break is driven by a
// real protocol-violation close here.
func TestQwpSenderPoolBrokenSlotDiscarded(t *testing.T) {
	srv, poison := poisonFirstConnQwpServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2, 500*time.Millisecond, 0, 0, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })

	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	ps := s.(*qwpPooledSender)
	poison()
	awaitSlotPoisoned(t, ps.slot)
	// A producer call observes the terminal error and marks the lease broken.
	if err := s.Flush(ctx); err == nil {
		t.Fatal("expected a terminal error after the HALT")
	}
	if !ps.broken {
		t.Fatal("terminal HALT did not mark the slot broken")
	}
	_ = s.Close(ctx)
	if _, avail, _ := p.poolSnapshot(); avail != 0 {
		t.Errorf("broken slot was returned to the pool (available=%d, want 0)", avail)
	}
}

func TestQwpSenderPoolBorrowCtxCancelled(t *testing.T) {
	p := senderPoolWithIdle(t, "", 0, 1, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx) // take the only slot
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer s.Close(ctx)
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.borrow(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("borrow with cancelled ctx err=%v, want context.Canceled", err)
	}
}

func queryPoolWithIdle(t *testing.T, min, max int, idle time.Duration) *qwpQueryPool {
	t.Helper()
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		for {
			if _, _, err := m.conn.Read(context.Background()); err != nil {
				return
			}
		}
	})
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpQueryPool(context.Background(), conf, min, max, 500*time.Millisecond, idle, 0)
	if err != nil {
		t.Fatalf("newQwpQueryPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })
	return p
}

// queryPoolWithCancelResponder mirrors queryPoolWithIdle but backs each
// connection with egressCancelResponder, which answers CANCEL frames with
// a QUERY_ERROR(CANCELLED) terminal. Use it for tests that drive a cursor
// cleanup drain (e.g. lease reopen), where the silent read-and-discard
// server would leave the drain racing the cancel-ack watchdog against its
// own ctx — both 5s — making the drain outcome (and any drainFailed latch)
// nondeterministic.
func queryPoolWithCancelResponder(t *testing.T, min, max int, idle time.Duration) *qwpQueryPool {
	t.Helper()
	srv := newQwpMockEgressServer(t, egressCancelResponder)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpQueryPool(context.Background(), conf, min, max, 500*time.Millisecond, idle, 0)
	if err != nil {
		t.Fatalf("newQwpQueryPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })
	return p
}

func TestQwpQueryPoolReapsToMin(t *testing.T) {
	p := queryPoolWithIdle(t, 1, 4, time.Millisecond)
	ctx := context.Background()
	var leases []*Query
	for i := 0; i < 4; i++ {
		q, err := p.borrow(ctx)
		if err != nil {
			t.Fatalf("borrow %d: %v", i, err)
		}
		leases = append(leases, q)
	}
	for _, q := range leases {
		_ = q.Close()
	}
	time.Sleep(5 * time.Millisecond)
	p.reapIdle()
	if total, avail := p.poolSnapshot(); total != 1 || avail != 1 {
		t.Errorf("after reap total=%d avail=%d, want 1/1", total, avail)
	}
}

func TestQwpQueryPoolBorrowCtxCancelled(t *testing.T) {
	p := queryPoolWithIdle(t, 0, 1, 0)
	ctx := context.Background()
	q, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer q.Close()
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.borrow(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("borrow with cancelled ctx err=%v, want context.Canceled", err)
	}
}

// testShopDecimal is a minimal ShopspringDecimal for the column-forward test.
type testShopDecimal struct{}

func (testShopDecimal) Coefficient() *big.Int { return big.NewInt(123) }
func (testShopDecimal) Exponent() int32       { return -2 }

// TestQwpPooledSenderForwardsAllColumns exercises every LineSender column
// forward on the pooled lease (each forward delegates to the underlying sender).
func TestQwpPooledSenderForwardsAllColumns(t *testing.T) {
	p := senderPoolWithIdle(t, "", 1, 2, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer s.Close(ctx)
	dec, _ := NewDecimal(big.NewInt(12345), 2)
	ndArr, _ := NewNDArray[float64](2)
	s.Table("t").
		Symbol("sym", "v").
		Int64Column("i", 1).
		Long256Column("l", big.NewInt(2)).
		TimestampColumn("ts", time.Now()).
		Float64Column("f", 1.5).
		DecimalColumn("dec", dec).
		DecimalColumnFromString("decs", "1.23").
		DecimalColumnShopspring("decss", testShopDecimal{}).
		StringColumn("s", "x").
		BoolColumn("b", true).
		Float64Array1DColumn("a1", []float64{1, 2}).
		Float64Array2DColumn("a2", [][]float64{{1}, {2}}).
		Float64Array3DColumn("a3", [][][]float64{{{1}}}).
		Float64ArrayNDColumn("aN", ndArr)
	// Every forward must produce well-formed buffer state: a mis-wired forward
	// (delegating to the wrong underlying method, or dropping the value) would
	// latch a fluent error that surfaces on At/Flush. Asserting both succeed
	// makes the test fail on such a regression rather than passing silently.
	if err := s.AtNow(ctx); err != nil {
		t.Fatalf("AtNow after all column forwards: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush after all column forwards: %v", err)
	}
}

// TestQwpSenderPoolSfReapsToMin covers the SF reap path: reaping a slot frees
// its on-disk slot index, which a later borrow reuses.
func TestQwpSenderPoolSfReapsToMin(t *testing.T) {
	p := senderPoolWithIdle(t, "sf_dir="+t.TempDir()+";", 1, 4, time.Millisecond)
	ctx := context.Background()
	var leases []LineSender
	for i := 0; i < 4; i++ {
		s, err := p.borrow(ctx)
		if err != nil {
			t.Fatalf("borrow %d: %v", i, err)
		}
		leases = append(leases, s)
	}
	for _, s := range leases {
		_ = s.Close(ctx)
	}
	time.Sleep(5 * time.Millisecond)
	p.reapIdle()
	if total, _, leaked := p.poolSnapshot(); total != 1 || leaked != 0 {
		t.Errorf("after SF reap total=%d leaked=%d, want 1/0", total, leaked)
	}
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow after SF reap: %v", err)
	}
	_ = s.Close(ctx)
}
