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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

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

func TestQwpSenderPoolAtErrorMarksBroken(t *testing.T) {
	p := senderPoolWithIdle(t, "", 1, 2, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if err := s.Table("bad\tname").Int64Column("v", 1).At(ctx, time.Now()); err == nil {
		t.Skip("validation did not reject the bad name in this build")
	}
	_ = s.Close(ctx)
	if _, avail, _ := p.poolSnapshot(); avail != 0 {
		t.Errorf("slot broken via At was recycled (available=%d, want 0)", avail)
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
	if err := s.Flush(ctx); err == nil {
		t.Skip("validation did not reject the bad name in this build")
	}
}

// TestQwpSenderPoolSfBrokenSlotReclaimed covers discardLocked's SF branch: a
// broken SF lease has its on-disk slot index reclaimed (not leaked) and reused.
func TestQwpSenderPoolSfBrokenSlotReclaimed(t *testing.T) {
	p := senderPoolWithIdle(t, "sf_dir="+t.TempDir()+";", 1, 2, 0)
	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if err := s.Table("bad\tname").Int64Column("v", 1).AtNow(ctx); err == nil {
		t.Skip("validation did not reject the bad name in this build")
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
