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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// poisonFirstConnQwpServer accepts WS upgrades. The first connection — the
// pool's prewarmed slot — is held idle and then closed with a terminal
// protocol-violation code when the returned poison func is called, simulating a
// background HALT of a slot with no lease watching. Every later connection (a
// replacement slot) is served normally so a re-borrowed slot is healthy.
func poisonFirstConnQwpServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	poison := make(chan struct{})
	var conns atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if conns.Add(1) == 1 {
			// First (prewarm) connection: the client's receiver is reading, so a
			// terminal close lands as a PROTOCOL_VIOLATION HALT even while idle.
			select {
			case <-poison:
				_ = conn.Close(websocket.StatusProtocolError, "poisoned")
			case <-r.Context().Done():
			}
			return
		}
		var seq int64
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
			_ = conn.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	var once sync.Once
	return srv, func() { once.Do(func() { close(poison) }) }
}

// awaitSlotPoisoned blocks until the slot's background send loop latches a
// terminal error, failing the test if it never does.
func awaitSlotPoisoned(t *testing.T, slot *qwpSenderSlot) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !slotTerminallyFailed(slot.delegate) {
		if time.Now().After(deadline) {
			t.Fatal("slot never latched a terminal error")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func newQwpSenderPoolForTest(t *testing.T, extra string, min, max int) *qwpSenderPool {
	t.Helper()
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	conf := "ws::addr=" + addr + ";" + extra
	p, err := newQwpSenderPool(context.Background(), conf, min, max,
		500*time.Millisecond, 0, 0, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })
	return p
}

func TestQwpSenderPoolBorrowReuse(t *testing.T) {
	p := newQwpSenderPoolForTest(t, "", 1, 4)
	if total, _, _ := p.poolSnapshot(); total != 1 {
		t.Fatalf("prewarm: total=%d, want 1", total)
	}
	s, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Re-borrow reuses the same warm slot — no growth.
	s2, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("re-borrow: %v", err)
	}
	defer s2.Close(context.Background())
	if total, _, _ := p.poolSnapshot(); total != 1 {
		t.Errorf("after reuse: total=%d, want 1 (no growth)", total)
	}
}

func TestQwpSenderPoolGrowsAndTimesOut(t *testing.T) {
	p := newQwpSenderPoolForTest(t, "", 0, 2)
	a, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow a: %v", err)
	}
	defer a.Close(context.Background())
	b, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow b: %v", err)
	}
	defer b.Close(context.Background())
	if total, _, _ := p.poolSnapshot(); total != 2 {
		t.Fatalf("grew to total=%d, want 2", total)
	}
	// At capacity with both in use → the third borrow times out.
	_, err = p.borrow(context.Background())
	if !errors.Is(err, errPoolExhausted) {
		t.Fatalf("third borrow err=%v, want errPoolExhausted", err)
	}
}

func TestQwpSenderPoolDoubleCloseIsIdempotent(t *testing.T) {
	p := newQwpSenderPoolForTest(t, "", 1, 2)
	s, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("first close: %v", err)
	}
	// Second close must be a no-op (Hazard B): no double-return, no panic.
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, avail, _ := p.poolSnapshot(); avail != 1 {
		t.Errorf("available=%d after double close, want 1 (no double-return)", avail)
	}
}

func TestQwpSenderPoolStaleLeaseErrors(t *testing.T) {
	p := newQwpSenderPoolForTest(t, "", 1, 2)
	s, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	s.Close(context.Background())
	// Using the returned lease must not corrupt the slot's next borrow.
	if err := s.AtNow(context.Background()); !errors.Is(err, errStaleLease) {
		t.Fatalf("stale AtNow err=%v, want errStaleLease", err)
	}
}

func TestQwpSenderPoolSfDistinctSlotDirs(t *testing.T) {
	sfDir := t.TempDir()
	p := newQwpSenderPoolForTest(t, "sf_dir="+sfDir+";", 2, 4)
	if !p.storeAndForward || p.slotBase != qwpSfDefaultSenderId {
		t.Fatalf("SF not detected: sf=%v base=%q", p.storeAndForward, p.slotBase)
	}
	// Each prewarmed slot owns a distinct <base>-<index> dir (Hazard A).
	for _, idx := range []int{0, 1} {
		dir := filepath.Join(sfDir, qwpSfDefaultSenderId+"-"+strconv.Itoa(idx))
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("expected slot dir %s: %v", dir, err)
		}
	}
}

func TestQwpSenderPoolInRangeFence(t *testing.T) {
	p := &qwpSenderPool{slotBase: "default", maxSize: 3}
	cases := map[string]bool{
		"default-0":   true,  // in range
		"default-2":   true,  // in range (max-1)
		"default-3":   false, // out of range (>= max) — drainable
		"default-9":   false, // out of range — drainable
		"other-0":     false, // foreign — drainable
		"defaultx-0":  false, // not the base prefix
		"default-abc": false, // non-numeric
	}
	for name, want := range cases {
		if got := p.inRangeFence(name); got != want {
			t.Errorf("inRangeFence(%q)=%v, want %v", name, got, want)
		}
	}
}

// TestQwpSenderPoolFenceExcludesLiveSiblings exercises Hazard-G end-to-end: a
// pool of ≥2 live SF slots with drain_orphans=on must never adopt a live
// sibling. It complements the pure-predicate TestQwpSenderPoolInRangeFence by
// running the real fence through the real qwpSfScanOrphans against real on-disk
// slot dirs created by real pooled SF senders — proving in-range live siblings
// are excluded while out-of-range and foreign orphans stay drainable.
func TestQwpSenderPoolFenceExcludesLiveSiblings(t *testing.T) {
	sfDir := t.TempDir()
	// 2 live SF slots; max=3 so the fenced in-range set is default-{0,1,2}. Each
	// pooled sender ran its own orphan scan at construction (drain_orphans=on).
	p := newQwpSenderPoolForTest(t, "sf_dir="+sfDir+";drain_orphans=on;", 2, 3)
	if !p.storeAndForward {
		t.Fatal("SF not detected")
	}
	base := p.slotBase

	// No pooled sender adopted a sibling slot at construction: the fence kept each
	// new slot's orphan scan off the already-live siblings.
	p.mu.Lock()
	slots := append([]*qwpSenderSlot(nil), p.available...)
	p.mu.Unlock()
	for _, s := range slots {
		if d := s.delegate.BackgroundDrainers(); d != nil {
			t.Errorf("pooled SF sender adopted a sibling at construction: drainers=%v", d)
		}
	}

	// Make the two live in-range slot dirs look like drainable orphans (unacked
	// .sfa, no .failed) so only the fence keeps the scan off them.
	for _, idx := range []int{0, 1} {
		seg := filepath.Join(sfDir, base+"-"+strconv.Itoa(idx), "sf-live.sfa")
		if err := os.WriteFile(seg, []byte{1}, 0o644); err != nil {
			t.Fatalf("seed live .sfa: %v", err)
		}
	}
	// A genuinely-orphaned out-of-range same-base dir and a foreign dir must stay
	// drainable — the fence is precise, not a blanket same-base exclusion.
	for _, name := range []string{base + "-9", "foreign-0"} {
		dir := filepath.Join(sfDir, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "sf-x.sfa"), []byte{1}, 0o644); err != nil {
			t.Fatalf("seed orphan .sfa: %v", err)
		}
	}

	gotSet := map[string]bool{}
	for _, o := range qwpSfScanOrphans(sfDir, p.inRangeFence) {
		gotSet[filepath.Base(o)] = true
	}
	for _, fenced := range []string{base + "-0", base + "-1"} {
		if gotSet[fenced] {
			t.Errorf("live in-range sibling %q was not fenced from the orphan scan", fenced)
		}
	}
	for _, drainable := range []string{base + "-9", "foreign-0"} {
		if !gotSet[drainable] {
			t.Errorf("drainable orphan %q was wrongly fenced", drainable)
		}
	}
}

// TestQwpPooledSenderAt covers the lease's At (vs AtNow exercised elsewhere).
func TestQwpPooledSenderAt(t *testing.T) {
	p := senderPoolWithIdle(t, "", 1, 2, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer s.Close(ctx)
	if err := s.Table("t").Int64Column("v", 1).At(ctx, time.Now()); err != nil {
		t.Fatalf("At: %v", err)
	}
}

func TestQwpSenderLeaseStaleAtFlush(t *testing.T) {
	p := senderPoolWithIdle(t, "", 1, 2, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	_ = s.Close(ctx)
	if err := s.At(ctx, time.Now()); !errors.Is(err, errStaleLease) {
		t.Errorf("stale At=%v, want errStaleLease", err)
	}
	if err := s.Flush(ctx); !errors.Is(err, errStaleLease) {
		t.Errorf("stale Flush=%v, want errStaleLease", err)
	}
}

func TestQwpSenderPoolBorrowCreateError(t *testing.T) {
	ctx := context.Background()
	p, err := newQwpSenderPool(ctx, "ws::addr=127.0.0.1:1;", 0, 1, 200*time.Millisecond, 0, 0, nil, nil)
	if err != nil {
		t.Fatalf("build (min=0 must not connect): %v", err)
	}
	defer p.close(ctx)
	if _, err := p.borrow(ctx); err == nil {
		t.Error("borrow against a down server should fail")
	}
}

func TestQwpSenderPoolPrewarmFailure(t *testing.T) {
	_, err := newQwpSenderPool(context.Background(), "ws::addr=127.0.0.1:1;", 1, 2, 200*time.Millisecond, 0, 0, nil, nil)
	if err == nil {
		t.Error("prewarm against a down server should fail the build")
	}
}

// TestQwpSenderPoolBenignErrorDoesNotBreakSlot covers M4: a benign fluent-API
// validation latch (illegal table name) must NOT mark the slot broken. The
// underlying connection is healthy, so the borrower recovers on the same slot
// and the slot is recycled on return rather than torn down — discarding it would
// churn the pool on a stream with sporadic bad records.
func TestQwpSenderPoolBenignErrorDoesNotBreakSlot(t *testing.T) {
	p := senderPoolWithIdle(t, "", 1, 2, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	// A bad table name surfaces a validation error...
	if err := s.Table("bad\tname").Int64Column("v", 1).At(ctx, time.Now()); err == nil {
		t.Fatal("expected the illegal table name to be rejected")
	}
	ps := s.(*qwpPooledSender)
	if ps.broken {
		t.Fatal("benign validation error marked the slot broken")
	}
	// ...and the borrower recovers on the same slot with a good row.
	if err := s.Table("good").Int64Column("v", 1).At(ctx, time.Now()); err != nil {
		t.Fatalf("good row after bad did not recover: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Healthy slot recycled, not discarded.
	if total, avail, _ := p.poolSnapshot(); total != 1 || avail != 1 {
		t.Errorf("recycled slot accounting: total=%d avail=%d, want 1/1", total, avail)
	}
}

// TestQwpSenderPoolDiscardsBackgroundHaltedSlot covers M1: a slot whose
// background send loop terminally HALTs while it sits idle in the pool must not
// be handed to the next borrower. borrow discards it and builds a fresh slot, so
// borrower isolation holds even during incident recovery.
func TestQwpSenderPoolDiscardsBackgroundHaltedSlot(t *testing.T) {
	srv, poison := poisonFirstConnQwpServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2, 500*time.Millisecond, 0, 0, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })

	// One prewarmed slot sitting idle in available.
	if total, avail, _ := p.poolSnapshot(); total != 1 || avail != 1 {
		t.Fatalf("prewarm: total=%d avail=%d, want 1/1", total, avail)
	}
	p.mu.Lock()
	poisoned := p.available[0]
	p.mu.Unlock()

	// Poison it via a background protocol-violation close — no lease watching.
	poison()
	awaitSlotPoisoned(t, poisoned)

	// Borrow must not hand out the poisoned slot.
	s, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	ps := s.(*qwpPooledSender)
	if ps.slot == poisoned {
		t.Fatal("borrow handed out the poisoned slot")
	}
	if slotTerminallyFailed(ps.slot.delegate) {
		t.Fatal("borrowed replacement slot is itself poisoned")
	}
	// The replacement actually works against a fresh connection.
	if err := s.Table("t").Int64Column("v", 1).At(context.Background(), time.Unix(0, 1000)); err != nil {
		t.Fatalf("At on replacement: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush on replacement: %v", err)
	}
}

// TestQwpSenderPoolReapsBackgroundHaltedSlot covers M1's reapIdle arm: a slot
// poisoned by a background HALT is reaped even when the pool is at minSize, so
// the housekeeper clears poisoned slots proactively rather than leaving every
// borrow to discard one.
func TestQwpSenderPoolReapsBackgroundHaltedSlot(t *testing.T) {
	srv, poison := poisonFirstConnQwpServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	// idle_timeout and max_lifetime off + min=1: only the poison check can reap.
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2, 500*time.Millisecond, 0, 0, nil, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })

	p.mu.Lock()
	poisoned := p.available[0]
	p.mu.Unlock()
	poison()
	awaitSlotPoisoned(t, poisoned)

	p.reapIdle()
	if total, avail, _ := p.poolSnapshot(); total != 0 || avail != 0 {
		t.Errorf("poisoned slot not reaped at minSize: total=%d avail=%d, want 0/0", total, avail)
	}
}

func TestQwpSenderPoolConstructorErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := newQwpSenderPool(ctx, "ws::addr=a:9000;", 3, 1, time.Second, 0, 0, nil, nil); err == nil {
		t.Error("min>max should error")
	}
	if _, err := newQwpSenderPool(ctx, "ws::addr=a:9000;init_buf_size=abc;", 0, 1, time.Second, 0, 0, nil, nil); err == nil {
		t.Error("malformed conf should error")
	}
	if _, err := newQwpSenderPool(ctx, "http::addr=a:9000;", 0, 1, time.Second, 0, 0, nil, nil); err == nil {
		t.Error("non-ws schema should error")
	}
}

func TestQwpSenderPoolClosedOps(t *testing.T) {
	ctx := context.Background()
	p := senderPoolWithIdle(t, "", 1, 2, time.Second)
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	_ = p.close(ctx)
	if _, err := p.borrow(ctx); !errors.Is(err, errPoolClosed) {
		t.Errorf("borrow after close=%v, want errPoolClosed", err)
	}
	p.reapIdle()
	// The lease was on loan at close, so close() left its delegate open; returning
	// it now flushes and giveBack closes the delegate (pool already closed).
	_ = s.Close(ctx)
}

func TestQwpSenderPoolFlushSurfacesLatchedError(t *testing.T) {
	p := senderPoolWithIdle(t, "", 1, 2, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer s.Close(ctx)
	s.Table("bad\tname").Int64Column("v", 1)
	// The latched fluent-API validation error must surface on the next Flush.
	// A require.Error-style guard (not t.Skip) so a validation regression fails
	// loudly instead of silently skipping (M8).
	if err := s.Flush(ctx); err == nil {
		t.Fatal("Flush did not surface the latched illegal-table-name error")
	}
	// The benign latch must not have poisoned the slot (M4).
	if s.(*qwpPooledSender).broken {
		t.Error("a surfaced validation latch marked the slot broken")
	}
}

// TestQwpSenderPoolSfBrokenSlotReclaimed covers discardLocked's SF branch via
// the giveBack-broken path: a lease broken by a genuine terminal HALT has its
// on-disk slot index reclaimed (not leaked) and reused. The break must be a real
// terminal error now that a benign validation latch no longer marks the slot
// broken (M4).
func TestQwpSenderPoolSfBrokenSlotReclaimed(t *testing.T) {
	srv, poison := poisonFirstConnQwpServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";sf_dir=" + t.TempDir() + ";"
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

	// HALT the borrowed slot's connection, then a producer call observes the
	// terminal error and marks the lease broken.
	poison()
	awaitSlotPoisoned(t, ps.slot)
	if err := s.Flush(ctx); err == nil {
		t.Fatal("expected a terminal error after the HALT")
	}
	if !ps.broken {
		t.Fatal("terminal HALT did not mark the slot broken")
	}

	_ = s.Close(ctx) // broken → discardLocked reclaims the SF slot index
	if _, _, leaked := p.poolSnapshot(); leaked != 0 {
		t.Errorf("broken SF slot leaked: %d", leaked)
	}
	s2, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow after SF discard: %v", err)
	}
	_ = s2.Close(ctx)
}
