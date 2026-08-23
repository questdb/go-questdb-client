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
	"log/slog"
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
	"github.com/stretchr/testify/require"
)

// poisonFirstConnQwpServer accepts WS upgrades. The first connection — the
// pool's prewarmed slot — is held idle and then rejected terminally when the
// returned poison func is called, simulating a background HALT of a slot with
// no lease watching: an unsolicited TERMINAL NACK (SECURITY_ERROR) latches the
// idle slot's send loop even with nothing in flight. Every later connection (a
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
			// First (prewarm) connection: the client's receiver is reading, so
			// an unsolicited terminal NACK lands as a SECURITY_ERROR latch even
			// while idle (WS close codes carry no policy semantics anymore).
			select {
			case <-poison:
				_ = conn.Write(context.Background(), websocket.MessageBinary,
					buildAckError(QwpStatusSecurityError, 0, "poisoned"))
				// Keep reading so the client's close handshake completes
				// promptly when the poisoned slot is discarded.
				for {
					if _, _, err := conn.Read(context.Background()); err != nil {
						return
					}
				}
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
		500*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	t.Cleanup(func() { p.close(context.Background()) })
	return p
}

func TestQwpSenderPoolMemoryBuildCleanupDoesNotConsumeCapacity(t *testing.T) {
	p := &qwpSenderPool{maxSize: 1}
	slot := &qwpSenderSlot{slotIndex: -1, cleanup: &qwpLineSender{}}

	p.reclaimFailedBuild(slot, -1, errors.New("injected memory build failure"))

	require.Zero(t, p.capUsedLocked())
}

func TestQwpSfSlotLifecycleTransitionTable(t *testing.T) {
	states := []qwpSfSlotState{
		qwpSfSlotFree,
		qwpSfSlotCreating,
		qwpSfSlotAvailable,
		qwpSfSlotLeased,
		qwpSfSlotClosing,
		qwpSfSlotRetired,
	}
	for _, from := range states {
		for _, to := range states {
			name := from.String() + "_to_" + to.String()
			t.Run(name, func(t *testing.T) {
				p := &qwpSenderPool{
					storeAndForward: true,
					sfSlots:         []qwpSfSlotLifecycle{{state: from}},
				}
				transition := func() { p.transitionSfSlotLocked(0, from, to) }
				if qwpSfSlotTransitionAllowed(from, to) {
					require.NotPanics(t, transition)
					require.Equal(t, to, p.sfSlots[0].state)
					return
				}
				require.Panics(t, transition, "illegal lifecycle edge must fail loudly")
				require.Equal(t, from, p.sfSlots[0].state)
			})
		}
	}
}

func TestQwpSfSlotLifecycleSnapshotIncludesEveryNonFreeState(t *testing.T) {
	states := []qwpSfSlotState{
		qwpSfSlotFree,
		qwpSfSlotCreating,
		qwpSfSlotAvailable,
		qwpSfSlotLeased,
		qwpSfSlotClosing,
		qwpSfSlotRetired,
	}
	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			p := &qwpSenderPool{
				storeAndForward: true,
				sfSlots:         []qwpSfSlotLifecycle{{state: state}},
			}
			pending := p.sfCloseSnapshotLocked()
			if state == qwpSfSlotFree {
				require.Empty(t, pending)
				require.Zero(t, p.capUsedLocked())
				return
			}
			require.Equal(t, []qwpSfSlotObligation{{index: 0, state: state}}, pending)
			require.Equal(t, 1, p.capUsedLocked())
		})
	}
}

// TestQwpSenderPoolCloseReportsBlockedSfCreation pins the shutdown obligation
// that exists before createSlotAt has acquired a flock. Once shutdown starts,
// the reserved index cannot disappear from Close's snapshot: the build may
// still acquire that flock after the first Close returns.
func TestQwpSenderPoolCloseReportsBlockedSfCreation(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 1,
		40*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.close(context.Background()) })

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	borrowDone := make(chan error, 1)
	borrowJoined := make(chan struct{})
	hook := func() {
		close(entered)
		<-release
	}
	createSlotHook.Store(&hook)
	t.Cleanup(func() {
		createSlotHook.Store(nil)
		releaseOnce.Do(func() { close(release) })
		select {
		case <-borrowJoined:
		case <-time.After(5 * time.Second):
			t.Error("blocked borrow did not finish during cleanup")
		}
	})

	go func() {
		defer close(borrowJoined)
		_, borrowErr := p.borrow(context.Background())
		borrowDone <- borrowErr
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("createSlotAt did not reach the blocking hook")
	}

	require.ErrorIs(t, p.close(context.Background()), ErrSfCleanupPending,
		"an in-flight SF construction is a shutdown obligation even before it owns the flock")

	releaseOnce.Do(func() { close(release) })
	require.ErrorIs(t, <-borrowDone, errPoolExhausted)
	require.Eventually(t, func() bool {
		return p.close(context.Background()) == nil
	}, 5*time.Second, time.Millisecond, "Close did not converge after the blocked construction settled")

	lock, err := qwpSfAcquireSlotLock(filepath.Join(sfDir, qwpSfDefaultSenderId+"-0"))
	require.NoError(t, err, "a clean Close must imply the late construction released its flock")
	require.NoError(t, lock.close())
}

func TestQwpSenderPoolBlockedSfCreationSettlesFailureAndPanic(t *testing.T) {
	for _, panicBuild := range []bool{false, true} {
		name := "failure"
		if panicBuild {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			srv := newQwpTestServer(t)
			t.Cleanup(srv.Close)
			conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
				";sf_dir=" + t.TempDir() + ";close_flush_timeout_millis=0;"
			p, err := newQwpSenderPool(context.Background(), conf, 0, 1,
				40*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = p.close(context.Background()) })

			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			borrowDone := make(chan error, 1)
			borrowJoined := make(chan struct{})
			hook := func() {
				close(entered)
				<-release
				if panicBuild {
					panic("injected blocked construction panic")
				}
			}
			createSlotHook.Store(&hook)
			t.Cleanup(func() {
				createSlotHook.Store(nil)
				releaseOnce.Do(func() { close(release) })
				select {
				case <-borrowJoined:
				case <-time.After(5 * time.Second):
					t.Error("blocked borrow did not finish during cleanup")
				}
			})

			go func() {
				defer close(borrowJoined)
				_, borrowErr := p.borrow(context.Background())
				borrowDone <- borrowErr
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("createSlotAt did not reach the blocking hook")
			}

			require.ErrorIs(t, p.close(context.Background()), ErrSfCleanupPending)
			if !panicBuild {
				// createSlotAt parses the config after the hook. The release channel
				// orders this mutation before that read, yielding a normal build error.
				p.baseConf = "invalid"
			}
			releaseOnce.Do(func() { close(release) })
			require.Error(t, <-borrowDone)
			require.Eventually(t, func() bool {
				return p.close(context.Background()) == nil
			}, 5*time.Second, time.Millisecond,
				"Close did not converge after the blocked construction settled")
		})
	}
}

type blockingPoolCloseSender struct {
	QwpSender
	entered chan<- struct{}
	release <-chan struct{}
}

func (s blockingPoolCloseSender) Close(context.Context) error {
	close(s.entered)
	<-s.release
	return nil
}

type resultPoolCloseSender struct {
	QwpSender
	err    error
	panic  any
	closed *atomic.Int32
}

func (s resultPoolCloseSender) Close(context.Context) error {
	s.closed.Add(1)
	if s.panic != nil {
		panic(s.panic)
	}
	return s.err
}

func TestQwpSenderPoolCloseRunsEveryDelegateAndJoinsErrorsInSlotOrder(t *testing.T) {
	firstErr := errors.New("slot zero close failure")
	thirdErr := errors.New("slot two close failure")
	closed := atomic.Int32{}
	p := &qwpSenderPool{
		notify:         make(chan struct{}),
		maxSize:        3,
		acquireTimeout: time.Second,
	}
	p.all = []*qwpSenderSlot{
		{delegate: resultPoolCloseSender{err: firstErr, closed: &closed}, slotIndex: -1},
		{delegate: resultPoolCloseSender{panic: "slot one close panic", closed: &closed}, slotIndex: -1},
		{delegate: resultPoolCloseSender{err: thirdErr, closed: &closed}, slotIndex: -1},
	}
	p.available = append([]*qwpSenderSlot(nil), p.all...)

	err := p.close(context.Background())
	require.Equal(t, int32(3), closed.Load(), "one delegate fault must not skip its siblings")
	require.ErrorIs(t, err, firstErr)
	require.ErrorContains(t, err, "slot one close panic")
	require.ErrorIs(t, err, thirdErr)
	require.Less(t, strings.Index(err.Error(), firstErr.Error()), strings.Index(err.Error(), "slot one close panic"),
		"joined diagnostics must follow stable slot order, not goroutine completion order")
	require.Less(t, strings.Index(err.Error(), "slot one close panic"), strings.Index(err.Error(), thirdErr.Error()),
		"joined diagnostics must follow stable slot order, not goroutine completion order")
}

// TestQwpSenderPoolConcurrentCloseWaitsForFirstTeardown exercises close
// directly, without the facade's sync.Once. A later caller must not take a
// success snapshot until the first-pass delegate teardown has completed.
func TestQwpSenderPoolConcurrentCloseWaitsForFirstTeardown(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	p := &qwpSenderPool{
		notify:         make(chan struct{}),
		maxSize:        1,
		acquireTimeout: time.Second,
	}
	slot := &qwpSenderSlot{
		delegate:  blockingPoolCloseSender{entered: entered, release: release},
		slotIndex: -1,
	}
	p.all = []*qwpSenderSlot{slot}
	p.available = []*qwpSenderSlot{slot}

	firstDone := make(chan error, 1)
	go func() { firstDone <- p.close(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first close did not enter delegate teardown")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- p.close(context.Background()) }()
	select {
	case err := <-secondDone:
		releaseOnce.Do(func() { close(release) })
		t.Fatalf("second close returned before first-pass teardown completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
}

// TestQwpPooledSenderForwardsEveryColumnType pins the pooled lease forwarding
// table: every LineSender + QwpSender column method must reach the delegate
// with the right arguments, and every accessor must delegate too. It builds the
// identical row through a pooled lease and a standalone sender of identical
// config and asserts byte-for-byte identical buffer state — a forwarder that
// called the wrong delegate method, or dropped the call, would change the
// encoded width or the pending-row count.
func TestQwpPooledSenderForwardsEveryColumnType(t *testing.T) {
	ctx := context.Background()
	pool := newQwpSenderPoolForTest(t, "", 1, 1)
	lease, err := pool.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	defer lease.Close(ctx)
	qs, ok := lease.(QwpSender)
	if !ok {
		t.Fatal("pooled lease does not satisfy QwpSender")
	}

	// Reference sender built through the same connect-string form the pool
	// uses, so its config (protocol version, gorilla, etc.) — and therefore
	// its encoding — is identical.
	refSrv := newQwpTestServer(t)
	defer refSrv.Close()
	refLS, err := LineSenderFromConf(ctx, "ws::addr="+strings.TrimPrefix(refSrv.URL, "http://")+";")
	if err != nil {
		t.Fatalf("build reference sender: %v", err)
	}
	defer refLS.Close(ctx)
	ref := refLS.(QwpSender)

	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	dec := NewDecimalFromInt64(12345, 2)
	// The QwpSender-only setters return QwpSender while the LineSender setters
	// return LineSender, so a single chain would narrow the type; drive each
	// through the latch as a separate statement instead.
	buildRow := func(s QwpSender) error {
		s.Table("types")
		s.Symbol("sym", "v")
		s.Int64Column("i64", 42)
		s.Float64Column("f64", 3.14)
		s.StringColumn("str", "hello")
		s.BoolColumn("b", true)
		s.TimestampColumn("ts", ts)
		s.ByteColumn("i8", -7)
		s.ShortColumn("i16", 300)
		s.Int32Column("i32", 70000)
		s.Float32Column("f32", 1.5)
		s.CharColumn("ch", 'Z')
		s.DateColumn("date", ts)
		s.TimestampNanosColumn("tsn", ts)
		s.UuidColumn("uuid", 0x1122, 0x3344)
		s.GeohashColumn("geo", 0xABC, 12)
		s.Float64Array1DColumn("fa1", []float64{1, 2})
		s.Int64Array1DColumn("ia1", []int64{3, 4})
		s.Int64Array2DColumn("ia2", [][]int64{{5}, {6}})
		s.Int64Array3DColumn("ia3", [][][]int64{{{7}}})
		s.Decimal64Column("d64", dec)
		s.Decimal128Column("d128", dec)
		s.Decimal256Column("d256", dec)
		return s.AtNano(ctx, ts)
	}
	if err := buildRow(qs); err != nil {
		t.Fatalf("build row via pooled lease: %v", err)
	}
	if err := buildRow(ref); err != nil {
		t.Fatalf("build row via reference sender: %v", err)
	}

	if got, want := MsgCount(lease), MsgCount(refLS); got != want || got != 1 {
		t.Fatalf("MsgCount lease=%d ref=%d, want both 1", got, want)
	}
	if got, want := BufLen(lease), BufLen(refLS); got != want || got == 0 {
		t.Fatalf("BufLen lease=%d ref=%d — a forwarder reached the wrong delegate method or dropped a column", got, want)
	}

	// End-to-end: the whole typed row round-trips through the pooled lease.
	seq, err := qs.FlushAndGetSequence(ctx)
	if err != nil {
		t.Fatalf("FlushAndGetSequence: %v", err)
	}
	if err := qs.AwaitAckedFsn(ctx, seq); err != nil {
		t.Fatalf("AwaitAckedFsn: %v", err)
	}

	// Accessor forwarders must reach the delegate and report a clean sender.
	if e := qs.LastTerminalError(); e != nil {
		t.Errorf("LastTerminalError = %v, want nil", e)
	}
	if n := qs.TotalServerErrors(); n != 0 {
		t.Errorf("TotalServerErrors = %d, want 0", n)
	}
	if n := qs.DroppedErrorNotifications(); n != 0 {
		t.Errorf("DroppedErrorNotifications = %d, want 0", n)
	}
	if n := qs.DroppedConnectionNotifications(); n != 0 {
		t.Errorf("DroppedConnectionNotifications = %d, want 0", n)
	}
	if n := qs.TotalDurableAcks(); n != 0 {
		t.Errorf("TotalDurableAcks = %d, want 0", n)
	}
	if n := qs.TotalDurableTrimAdvances(); n != 0 {
		t.Errorf("TotalDurableTrimAdvances = %d, want 0", n)
	}
	if n := qs.TotalBackpressureStalls(); n != 0 {
		t.Errorf("TotalBackpressureStalls = %d, want 0", n)
	}
	if d := qs.BackgroundDrainers(); d != nil {
		t.Errorf("BackgroundDrainers = %v, want nil (no sf_dir)", d)
	}
	if path := qs.QuarantinedSlotPath(); path != "" {
		t.Errorf("QuarantinedSlotPath = %q, want empty (nothing was set aside)", path)
	}
	if fsn := qs.AckedFsn(); fsn < seq {
		t.Errorf("AckedFsn = %d, want >= %d", fsn, seq)
	}
	// These have no deterministic value but must not panic on a lease.
	_ = qs.TotalErrorNotificationsDelivered()
	_ = qs.TotalReconnectAttempts()
	_ = qs.TotalReconnectsSucceeded()
	_ = qs.TotalFramesReplayed()
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

// TestQwpSenderPoolStaleLeaseCannotCorruptReborrow proves a stale handle whose
// slot has been re-borrowed by a live lease cannot write into it (Hazard B): the
// generation stamp inerts every stale-lease method, so the re-borrow's slot stays
// clean. min == max == 1 forces the re-borrow onto the very slot just returned.
func TestQwpSenderPoolStaleLeaseCannotCorruptReborrow(t *testing.T) {
	p := newQwpSenderPoolForTest(t, "", 1, 1)
	ctx := context.Background()

	stale, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if err := stale.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	live, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("re-borrow: %v", err)
	}
	defer live.Close(ctx)

	// The stale handle's whole fluent chain inerts and enqueues nothing into the
	// slot the live lease now owns.
	if err := stale.Table("stale").Symbol("s", "x").Int64Column("v", 1).AtNow(ctx); !errors.Is(err, errStaleLease) {
		t.Fatalf("stale AtNow err=%v, want errStaleLease", err)
	}
	// The live lease writes and flushes cleanly on that same slot — a leaked open
	// row or table from the stale chain would surface here (e.g. "table already set").
	if err := live.Table("good").Int64Column("v", 7).AtNow(ctx); err != nil {
		t.Fatalf("live AtNow: %v", err)
	}
	if err := live.Flush(ctx); err != nil {
		t.Fatalf("live flush: %v", err)
	}
}

// TestQwpSenderPoolReturnMidRowLeavesCleanSlot is the regression: a
// lease returned with an in-progress row (Table/Symbol/*Column but no
// finalizing At/AtNow) must not poison the next borrower. Routing the
// return through Flush early-returned errFlushWithPendingMessage without
// cancelling the open row, so the recycled slot kept hasTable /
// currentTable set and the next Table() latched a stale "table already
// set" naming a table the new borrower never touched. The return now goes
// through flushForReturn, which drops the open row first.
func TestQwpSenderPoolReturnMidRowLeavesCleanSlot(t *testing.T) {
	// min == max == 1 forces the re-borrow to reuse the very slot just
	// returned, so we exercise the recycled-slot path (not a fresh one).
	p := newQwpSenderPoolForTest(t, "", 1, 1)
	ctx := context.Background()

	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	// Open a row and abandon it — the idiomatic early-return between
	// Table and At.
	s.Table("x").Symbol("k", "v")
	// Parity with the standalone close path: the open row is silently
	// dropped, so the return succeeds rather than surfacing
	// errFlushWithPendingMessage.
	if err := s.Close(ctx); err != nil {
		t.Fatalf("close with open row: %v", err)
	}

	s2, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("re-borrow: %v", err)
	}
	defer s2.Close(ctx)
	// A clean write on a different table must succeed. A dirty slot would
	// have latched `table "x" already set` on Table("y") and surfaced it
	// here on At().
	if err := s2.Table("y").Int64Column("v", 1).At(ctx, time.Now()); err != nil {
		t.Fatalf("write after mid-row return failed (slot returned dirty?): %v", err)
	}
}

// TestQwpSenderPoolReturnLatchedErrorFlushesCommittedRow is the
// regression for the committed-row-plus-latch variant: a lease closed
// after committing a row AND then latching a fluent-API error (e.g. an
// illegal column name on untrusted input) must still flush the committed
// row into the engine on return, not recycle the slot with it buffered.
//
// The delegate's Flush early-returns the latched error ahead of its
// pending-rows branch, so routing the return through Flush left the
// committed row in the recycled slot; the next borrower then silently
// shipped it as a phantom/duplicate row (the reporter's exact repro). The
// return path now goes through flushForReturn, which mirrors closeCursor:
// capture the latch, drop the open row, but STILL flush committed rows.
func TestQwpSenderPoolReturnLatchedErrorFlushesCommittedRow(t *testing.T) {
	// min == max == 1 forces the re-borrow to reuse the very slot returned.
	p := newQwpSenderPoolForTest(t, "", 1, 1)
	ctx := context.Background()

	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	delegate := s.(*qwpPooledSender).slot.delegate.(*qwpLineSender)

	// Commit a good row, then latch a validation error on the next (open)
	// row — an illegal '.' in the column name, the untrusted-input path.
	if err := s.Table("t").Int64Column("v", 1).At(ctx, time.Unix(0, 1000)); err != nil {
		t.Fatalf("commit row: %v", err)
	}
	if delegate.pendingRowCount != 1 {
		t.Fatalf("pendingRowCount after commit = %d, want 1", delegate.pendingRowCount)
	}
	s.Table("t").Int64Column("bad.name", 2)

	// Close surfaces the latched error (retain-on-error) but MUST still
	// flush the committed row before recycling the slot.
	if err := s.Close(ctx); err == nil {
		t.Fatal("Close did not surface the latched illegal-column-name error")
	}
	if delegate.pendingRowCount != 0 {
		t.Fatalf("committed row stranded on recycled slot: pendingRowCount = %d, want 0", delegate.pendingRowCount)
	}

	// Re-borrow the same slot: it must start clean — no leftover committed
	// row waiting to ship on the new borrower's first flush.
	s2, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("re-borrow: %v", err)
	}
	defer s2.Close(ctx)
	d2 := s2.(*qwpPooledSender).slot.delegate.(*qwpLineSender)
	if d2 != delegate {
		t.Fatal("re-borrow did not reuse the same slot")
	}
	if d2.pendingRowCount != 0 {
		t.Fatalf("re-borrowed slot has stale rows: pendingRowCount = %d, want 0", d2.pendingRowCount)
	}
}

// TestQwpLineSenderFlushForReturnRetainsRowsOnEnqueueFailure is the unit half of
// the backpressure fix: when flushForReturn cannot enqueue the committed rows,
// it must report retained=true and keep the rows buffered — never silently drop
// them nor claim a clean return. A cancelled ctx makes engineAppendBlocking
// return ctx.Err() before sealing the frame, exactly as a saturated ring whose
// append deadline elapsed would. retained=true is the signal the pool uses to
// discard the dirty slot instead of recycling it under the next borrower.
func TestQwpLineSenderFlushForReturnRetainsRowsOnEnqueueFailure(t *testing.T) {
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(context.Background())

	if err := s.Table("t").Int64Column("v", 1).At(context.Background(), time.Unix(0, 1000)); err != nil {
		t.Fatalf("At: %v", err)
	}
	if s.pendingRowCount != 1 {
		t.Fatalf("pendingRowCount = %d, want 1", s.pendingRowCount)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retained, err := s.flushForReturn(ctx)
	if err == nil {
		t.Fatal("flushForReturn: want error from the cancelled-ctx enqueue, got nil")
	}
	if !retained {
		t.Fatal("flushForReturn: retained=false, want true — committed rows were left un-enqueued")
	}
	if s.pendingRowCount != 1 {
		t.Fatalf("committed rows dropped: pendingRowCount = %d, want 1 (retained for retry)", s.pendingRowCount)
	}
}

// TestQwpSenderPoolReturnBackpressureDiscardsDirtySlot is the end-to-end
// regression for the backpressure discard path. A lease returned while the cursor
// ring is saturated and the wire cannot drain (an outage) cannot flush its
// committed rows on return: flushForReturn hits the backpressure append deadline
// and RETAINS them. A backpressure timeout is not a terminal HALT, so
// markBrokenIfTerminal leaves the slot healthy — and recycling it would encode
// borrower A's retained rows into borrower B's next flush, shipping them under
// B's FSN (and a second time once A retries the error). Close must instead
// discard the dirty slot.
func TestQwpSenderPoolReturnBackpressureDiscardsDirtySlot(t *testing.T) {
	// A server that completes the upgrade and drains frames off the wire but
	// NEVER ACKs, so the ring's ACK-driven trim never advances and the ring
	// fills — the outage condition that makes backpressure bite.
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
			// Deliberately no ACK: the ring can never trim.
		}
	}))
	t.Cleanup(srv.Close)

	// Tiny single-segment ring + short append deadline so the ring saturates
	// within a handful of flushes and each backpressure wait is ~50ms. Fast
	// close so tearing the dirty slot down doesn't block on the dead wire.
	// auto_flush=off so flushing is entirely under the test's control.
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + t.TempDir() +
		";sf_max_segment_bytes=8192;sf_max_total_bytes=8192;sf_append_deadline_millis=50" +
		";close_flush_timeout_millis=1;auto_flush=off;"
	// min == max == 1 forces the re-borrow to reuse the recycled slot — unless
	// this Close discards it, which is exactly what we assert.
	p, err := newQwpSenderPool(context.Background(), conf, 1, 1, 500*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
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
	delegate := ps.slot.delegate.(*qwpLineSender)

	// Fill the ring: commit + flush until a flush hits the backpressure deadline
	// (ring full, can't drain against the non-ACKing server). That leaves the
	// last committed row retained in the delegate's producer buffers.
	var sawBackpressure bool
	for i := 0; i < 2000; i++ {
		if err := s.Table("t").Int64Column("v", int64(i)).At(ctx, time.Unix(0, int64(i+1)*1000)); err != nil {
			t.Fatalf("At row %d: %v", i, err)
		}
		if err := s.Flush(ctx); err != nil {
			if !errors.Is(err, ErrBackpressureTimeout) {
				t.Fatalf("flush %d: unexpected error %v, want ErrBackpressureTimeout", i, err)
			}
			sawBackpressure = true
			break
		}
	}
	if !sawBackpressure {
		t.Fatal("ring never saturated — test setup wrong (raise the flush count or shrink sf_max_total_bytes)")
	}
	if delegate.pendingRowCount == 0 {
		t.Fatal("expected committed rows retained after the backpressure flush")
	}
	if ps.broken {
		t.Fatal("backpressure is not a terminal HALT — the slot must not be broken yet")
	}

	// Return the lease. flushForReturn re-hits backpressure and retains the rows;
	// Close must discard the dirty slot rather than recycle it.
	if err := s.Close(ctx); err == nil {
		t.Fatal("Close did not surface the backpressure error")
	}
	if !ps.broken {
		t.Fatal("dirty slot (rows retained on backpressure) was NOT marked broken — it would be recycled carrying borrower A's rows")
	}
	if total, _, leaked := p.poolSnapshot(); total != 0 || leaked != 0 {
		t.Fatalf("dirty slot not discarded: total=%d leaked=%d, want 0/0", total, leaked)
	}

	// Re-borrow: min=1 rebuilds a FRESH slot (a different delegate), clean.
	s2, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("re-borrow after discard: %v", err)
	}
	defer s2.Close(ctx)
	d2 := s2.(*qwpPooledSender).slot.delegate.(*qwpLineSender)
	if d2 == delegate {
		t.Fatal("re-borrow reused the discarded dirty delegate")
	}
	if d2.pendingRowCount != 0 {
		t.Fatalf("re-borrowed slot has stale rows: pendingRowCount = %d, want 0", d2.pendingRowCount)
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
		// Non-canonical numeric suffixes: the pool only ever mints canonical
		// strconv.Itoa names, so a foreign dir with a leading zero or sign must
		// stay drainable, not be fenced by Atoi's tolerance (FIX 7 / Hazard G).
		"default-00":  false,
		"default-007": false,
		"default-+1":  false,
		"default--1":  false,
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
	// FlushAndGetSequence on a stale lease must return the same -1 no-FSN
	// sentinel the live sender returns on a failed flush and AckedFsn reports on
	// a dead lease — not a bare 0, which collides with a valid FSN.
	qs, ok := s.(QwpSender)
	if !ok {
		t.Fatalf("pooled lease %T does not implement QwpSender", s)
	}
	fsn, err := qs.FlushAndGetSequence(ctx)
	if !errors.Is(err, errStaleLease) {
		t.Errorf("stale FlushAndGetSequence err=%v, want errStaleLease", err)
	}
	if fsn != -1 {
		t.Errorf("stale FlushAndGetSequence fsn=%d, want -1", fsn)
	}
	if got := qs.AckedFsn(); got != -1 {
		t.Errorf("stale AckedFsn=%d, want -1", got)
	}
	// A stale lease must not read the re-borrowed slot's quarantine path.
	if path := qs.QuarantinedSlotPath(); path != "" {
		t.Errorf("stale QuarantinedSlotPath=%q, want empty", path)
	}
}

// TestQwpPooledSenderReportsQuarantinedSlotPath pins the live half of the
// forwarder: a pooled sender that had to set its slot aside must say where the
// bytes went, exactly as a standalone one does. Reading the log cannot be the
// only way to find them.
func TestQwpPooledSenderReportsQuarantinedSlotPath(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfRoot := t.TempDir()
	slot := filepath.Join(sfRoot, qwpSfDefaultSenderId+"-0")
	writeFailClosedSlot(t, slot)

	ctx := context.Background()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfRoot + ";close_flush_timeout_millis=200;"
	p, err := newQwpSenderPool(ctx, conf, 1, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.close(ctx) })

	lease, err := p.borrow(ctx)
	require.NoError(t, err)
	defer lease.Close(ctx)
	qs, ok := lease.(QwpSender)
	require.True(t, ok)

	entries, err := os.ReadDir(filepath.Join(sfRoot, "quarantined"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Join(sfRoot, "quarantined", entries[0].Name()), qs.QuarantinedSlotPath())
}

// TestQwpSenderQuarantinedSlotPathStandalone pins the same answer on a
// standalone sender, which is the form the README documents.
func TestQwpSenderQuarantinedSlotPathStandalone(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfRoot := t.TempDir()
	writeFailClosedSlot(t, filepath.Join(sfRoot, "solo"))

	ctx := context.Background()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfRoot + ";sender_id=solo;close_flush_timeout_millis=200;"
	ls, err := LineSenderFromConf(ctx, conf)
	require.NoError(t, err)
	defer ls.Close(ctx)
	qs, ok := ls.(QwpSender)
	require.True(t, ok)

	entries, err := os.ReadDir(filepath.Join(sfRoot, "quarantined"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Join(sfRoot, "quarantined", entries[0].Name()), qs.QuarantinedSlotPath())

	// A sender that opened a slot it could read set nothing aside.
	clean, err := LineSenderFromConf(ctx, "ws::addr="+strings.TrimPrefix(srv.URL, "http://")+
		";sf_dir="+t.TempDir()+";close_flush_timeout_millis=200;")
	require.NoError(t, err)
	defer clean.Close(ctx)
	require.Equal(t, "", clean.(QwpSender).QuarantinedSlotPath())
}

// TestQwpSenderPoolCloseSurvivesPanickingLoggerAndReleasesSlots pins that a
// buggy user slog handler cannot turn close() into a no-op. The leak warning
// fires after the available slots are already out of p.all and in closing, so
// a panic there would skip every teardown -- leaving each
// flock held with no retry owner and no retired slot to re-probe, while later
// close() calls happily report success.
func TestQwpSenderPoolCloseSurvivesPanickingLoggerAndReleasesSlots(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()
	ctx := context.Background()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=200;"
	// A short acquire timeout keeps close()'s wait for the never-returned lease
	// short; the leak branch it then takes is what this test is about.
	p, err := newQwpSenderPool(ctx, conf, 2, 2, 200*time.Millisecond, 0, 0,
		nil, nil, QwpBackgroundDrainerListener{}, slog.New(panicOnHandleSlog{}))
	require.NoError(t, err)

	// One lease stays out so close() takes the leak-warning branch.
	leaked, err := p.borrow(ctx)
	require.NoError(t, err)
	defer leaked.Close(ctx)

	require.ErrorIs(t, p.close(ctx), ErrSfCleanupPending,
		"the outstanding lease remains a lifecycle obligation")

	// The returned slot's flock must be gone; the borrowed one's is released
	// when its lease returns.
	lock, err := qwpSfAcquireSlotLock(filepath.Join(sfDir, qwpSfDefaultSenderId+"-0"))
	require.NoError(t, err, "close() skipped the teardown of an available slot")
	require.NoError(t, lock.close())
	require.NoError(t, leaked.Close(ctx))
	require.NoError(t, p.close(ctx))
}

// TestQwpSenderPoolCloseReportsPendingWhileTeardownHoldsFlock pins that close()
// counts the teardowns still in flight off-lock. A lease returning concurrently
// with close() is inside delegate.Close holding its slot flock, and close()'s
// lease wait is capped well below the drain it is racing -- reporting nil there
// would tell a caller the locks are gone while one is still held.
func TestQwpSenderPoolCloseReportsPendingWhileTeardownHoldsFlock(t *testing.T) {
	p := &qwpSenderPool{
		storeAndForward: true,
		maxSize:         1,
		sfSlots:         []qwpSfSlotLifecycle{{state: qwpSfSlotClosing}},
		notify:          make(chan struct{}),
	}

	err := p.close(context.Background())
	require.ErrorIs(t, err, ErrSfCleanupPending,
		"an in-flight teardown still holding its flock must not report a clean shutdown")

	// Once that teardown lands, a repeat close stops reporting it.
	p.mu.Lock()
	p.transitionSfSlotLocked(0, qwpSfSlotClosing, qwpSfSlotFree)
	p.mu.Unlock()
	require.NoError(t, p.close(context.Background()))
}

// writeFailClosedSlot lays down a manifest-backed slot whose committed active
// segment is unreadable, which recovery refuses; a foreground sender preserves
// such a slot under <sf_dir>/quarantined/ and starts fresh.
func writeFailClosedSlot(t *testing.T, slot string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(slot, 0o755))
	s0 := createRecoverySegment(t, slot, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, slot, "sf-active.sfa", 1, "b")
	createRecoveryManifest(t, slot, 0, 1, s0, s1)
	closeRecoverySegments(t, s0, s1)
	require.NoError(t, os.WriteFile(filepath.Join(slot, "sf-active.sfa"), make([]byte, 4096), 0o644))
}

// TestQwpSenderPoolBorrowGrowsAsyncWhenServerDown: a growth borrow while the
// server is down must NOT hard-fail (Invariant B). The pool is already running,
// so the growth slot connects asynchronously and buffers writes until the wire
// comes up; only the min-prewarm at build follows the configured connect mode.
func TestQwpSenderPoolBorrowGrowsAsyncWhenServerDown(t *testing.T) {
	ctx := context.Background()
	p, err := newQwpSenderPool(ctx, "ws::addr=127.0.0.1:1;close_flush_timeout_millis=100;",
		0, 1, 200*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err != nil {
		t.Fatalf("build (min=0 must not connect): %v", err)
	}
	defer p.close(ctx)
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("growth borrow against a down server must buffer, not fail (Invariant B): %v", err)
	}
	defer s.Close(ctx)
	if err := s.Table("t").Int64Column("v", 1).At(ctx, time.Now()); err != nil {
		t.Fatalf("At should buffer while the server is down, not error: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush should buffer via the cursor engine while down, not error: %v", err)
	}
}

func TestQwpSenderPoolPrewarmFailure(t *testing.T) {
	_, err := newQwpSenderPool(context.Background(), "ws::addr=127.0.0.1:1;", 1, 2, 200*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err == nil {
		t.Error("prewarm against a down server should fail the build")
	}
}

// TestQwpSenderPoolBenignErrorDoesNotBreakSlot covers the benign-error path: a benign fluent-API
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

// TestQwpSenderPoolDiscardsBackgroundHaltedSlot covers the background-HALT path: a slot whose
// background send loop terminally HALTs while it sits idle in the pool must not
// be handed to the next borrower. borrow discards it and builds a fresh slot, so
// borrower isolation holds even during incident recovery.
func TestQwpSenderPoolDiscardsBackgroundHaltedSlot(t *testing.T) {
	srv, poison := poisonFirstConnQwpServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2, 500*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
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

// TestQwpSenderPoolReapsBackgroundHaltedSlot covers the reapIdle arm: a slot
// poisoned by a background HALT is reaped even when the pool is at minSize, so
// the housekeeper clears poisoned slots proactively rather than leaving every
// borrow to discard one.
func TestQwpSenderPoolReapsBackgroundHaltedSlot(t *testing.T) {
	srv, poison := poisonFirstConnQwpServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	// idle_timeout and max_lifetime off + min=1: only the poison check can reap.
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2, 500*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
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
	if _, err := newQwpSenderPool(ctx, "ws::addr=a:9000;", 3, 1, time.Second, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil); err == nil {
		t.Error("min>max should error")
	}
	if _, err := newQwpSenderPool(ctx, "ws::addr=a:9000;", 0, qwpMaxPoolSize+1, time.Second, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil); err == nil {
		t.Errorf("sender_pool_max above the %d cap should error", qwpMaxPoolSize)
	}
	if _, err := newQwpSenderPool(ctx, "ws::addr=a:9000;init_buf_size=abc;", 0, 1, time.Second, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil); err == nil {
		t.Error("malformed conf should error")
	}
	if _, err := newQwpSenderPool(ctx, "http::addr=a:9000;", 0, 1, time.Second, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil); err == nil {
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
	// loudly instead of silently skipping.
	if err := s.Flush(ctx); err == nil {
		t.Fatal("Flush did not surface the latched illegal-table-name error")
	}
	// The benign latch must not have poisoned the slot.
	if s.(*qwpPooledSender).broken {
		t.Error("a surfaced validation latch marked the slot broken")
	}
}

// TestQwpSenderPoolSfBrokenSlotReclaimed covers discardLocked's SF branch via
// the giveBack-broken path: a lease broken by a genuine terminal HALT has its
// on-disk slot index reclaimed (not leaked) and reused. The break must be a real
// terminal error now that a benign validation latch no longer marks the slot
// broken.
func TestQwpSenderPoolSfBrokenSlotReclaimed(t *testing.T) {
	srv, poison := poisonFirstConnQwpServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";sf_dir=" + t.TempDir() + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2, 500*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
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

// TestQwpSenderPoolGiveBackAfterCloseBalancesSfAccounting pins the lifecycle
// transition on the lease-returned-after-close path: leased stays pending until
// giveBack moves it through closing to free.
func TestQwpSenderPoolGiveBackAfterCloseBalancesSfAccounting(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + t.TempDir() + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 2,
		100*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}

	ctx := context.Background()
	s, err := p.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	// Close with the lease outstanding: close() leaves the borrowed slot for
	// giveBack (bounded 100ms wait), which then runs the teardown itself.
	require.ErrorIs(t, p.close(ctx), ErrSfCleanupPending)
	if err := s.Close(ctx); err != nil {
		t.Fatalf("lease close: %v", err)
	}
	require.NoError(t, p.close(ctx))

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pendingLeaseTeardowns != 0 {
		t.Errorf("pendingLeaseTeardowns=%d, want 0", p.pendingLeaseTeardowns)
	}
	for i := range p.sfSlots {
		if state := p.sfSlots[i].state; state != qwpSfSlotFree {
			t.Errorf("SF slot %d state=%s after teardown, want free", i, state)
		}
	}
}

// qwpPoolTestUnhurriedAcquire is the acquire timeout for tests whose borrow
// waits on a hook or on deferred slot cleanup rather than on the clock. It has
// to outlast whatever the borrow is waiting for, or a loaded machine turns the
// assertion into a timeout error and the test fails for a reason it does not
// test. It is a ceiling, not a delay: nothing waits for it when the work it is
// waiting on completes.
const qwpPoolTestUnhurriedAcquire = 30 * time.Second

// TestQwpSenderPoolReprobesDeferredClose restores an SF slot index only after
// the delegate's manager worker has completed deferred terminal cleanup.
func TestQwpSenderPoolReprobesDeferredClose(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + t.TempDir() + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	t.Cleanup(func() { _ = p.close(context.Background()) })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("manager did not enter spare creation")
	}

	p.mu.Lock()
	slot := p.available[0]
	p.available = nil
	p.removeFromAllLocked(slot)
	p.transitionSfSlotLocked(slot.slotIndex, qwpSfSlotAvailable, qwpSfSlotClosing)
	p.mu.Unlock()
	if err := closeSlotGuarded(context.Background(), slot.delegate); err != nil {
		t.Fatalf("delegate close: %v", err)
	}
	p.mu.Lock()
	p.reclaimSlotLocked(slot, nil)
	if state := p.sfSlots[slot.slotIndex].state; state != qwpSfSlotRetired {
		t.Fatalf("deferred slot state=%s, want retired", state)
	}
	p.mu.Unlock()

	close(release)
	released = true
	// This standalone pool has no housekeeper. Borrow must poll/reprobe the
	// retired size-1 capacity and reuse index 0 once cleanup completes.
	replacement, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow after deferred cleanup: %v", err)
	}
	replacementSlot := replacement.(*qwpPooledSender).slot
	if replacementSlot.slotIndex != slot.slotIndex {
		t.Fatalf("replacement slot index=%d, want reused %d", replacementSlot.slotIndex, slot.slotIndex)
	}
	if err := replacement.Close(context.Background()); err != nil {
		t.Fatalf("return replacement: %v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if state := p.sfSlots[slot.slotIndex].state; state != qwpSfSlotAvailable {
		t.Fatalf("restored active slot state=%s, want available", state)
	}
}

func TestQwpSenderPoolCloseReportsDeferredSlotAndRetryConverges(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 1,
		100*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("manager did not enter spare creation")
	}

	err = p.close(context.Background())
	require.ErrorIs(t, err, ErrSfCleanupPending,
		"pool close must report the retained flock under a sentinel a caller can match")
	lockPath := filepath.Join(sfDir, qwpSfDefaultSenderId+"-0")
	_, err = qwpSfAcquireSlotLock(lockPath)
	if err == nil {
		t.Fatal("pool close released a slot still owned by the manager worker")
	}

	close(release)
	released = true
	// Only close() re-probes here. The pool is already closed, so nothing else
	// is left to notice the cleanup finishing, and a caller waiting for a clean
	// shutdown would wait forever on a cached answer.
	var closeErr error
	deadline := time.Now().Add(2 * time.Second)
	for {
		closeErr = p.close(context.Background())
		if closeErr == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, closeErr, "a repeat close must observe the slot cleanup finishing")
	p.mu.Lock()
	require.Equal(t, qwpSfSlotFree, p.sfSlots[0].state,
		"the re-probe must return the lifecycle record to free")
	p.mu.Unlock()
	lock, err := qwpSfAcquireSlotLock(lockPath)
	require.NoError(t, err)
	require.NoError(t, lock.close())
}

func TestQwpSenderPoolBuildFailureRetiresSlotUntilDeferredCleanup(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.close(context.Background()) })

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	afterEngineHook := func() error {
		select {
		case <-entered:
		case <-time.After(time.Second):
			return errors.New("manager did not enter spare creation")
		}
		return errors.New("injected post-engine build failure")
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	qwpSfTestAfterEngineCreateHook.Store(&afterEngineHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfTestAfterEngineCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	_, err = p.borrow(context.Background())
	require.ErrorContains(t, err, "injected post-engine build failure")
	p.mu.Lock()
	require.Equal(t, qwpSfSlotRetired, p.sfSlots[0].state,
		"failed build freed an index whose flock is retained")
	p.mu.Unlock()
	slotPath := filepath.Join(sfDir, qwpSfDefaultSenderId+"-0")
	_, err = qwpSfAcquireSlotLock(slotPath)
	require.Error(t, err)

	close(release)
	released = true
	require.Eventually(t, func() bool {
		p.reprobeRetiredSlots()
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.sfSlots[0].state == qwpSfSlotFree
	}, 2*time.Second, time.Millisecond)
}

// TestQwpSenderPoolRetiredSlotCapacityReturnsToBorrow is the end-to-end half of
// the retirement contract: with max=1 and no housekeeper at all, the pool has
// exactly one slot, that slot is retired with its flock still held, and the next
// borrow has to get it back on its own. It borrows for real rather than calling
// reprobeRetiredSlots by hand, which is what a user with
// housekeeper_interval_ms=0 actually does.
func TestQwpSenderPoolRetiredSlotCapacityReturnsToBorrow(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.close(context.Background()) })

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	afterEngineHook := func() error {
		select {
		case <-entered:
		case <-time.After(time.Second):
			return errors.New("manager did not enter spare creation")
		}
		return errors.New("injected post-engine build failure")
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	qwpSfTestAfterEngineCreateHook.Store(&afterEngineHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfTestAfterEngineCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	_, err = p.borrow(context.Background())
	require.ErrorContains(t, err, "injected post-engine build failure")
	p.mu.Lock()
	require.Equal(t, qwpSfSlotRetired, p.sfSlots[0].state,
		"the failed build must retire the slot, not free it")
	require.Equal(t, 1, p.capUsedLocked(), "a retired slot still counts against capacity")
	p.mu.Unlock()

	// Let the deferred cleanup finish, then borrow for real. The pool is at
	// capacity until the borrow's own reprobe observes the released flock.
	qwpSfTestSegmentCreateHook.Store(nil)
	qwpSfTestAfterEngineCreateHook.Store(nil)
	close(release)
	released = true

	lease, err := p.borrow(context.Background())
	require.NoError(t, err, "a borrow must recover the retired slot's capacity without a housekeeper")
	require.NoError(t, lease.Close(context.Background()))
	p.mu.Lock()
	require.Equal(t, qwpSfSlotAvailable, p.sfSlots[0].state)
	p.mu.Unlock()
}

func TestQwpSenderPoolRecoveryBuildFailureRetiresSlotUntilDeferredCleanup(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()
	slotPath := filepath.Join(sfDir, qwpSfDefaultSenderId+"-0")
	require.NoError(t, os.MkdirAll(slotPath, 0o755))
	manifest, err := qwpSfManifestCreate(slotPath, 0, 0)
	require.NoError(t, err)
	require.NoError(t, manifest.close())

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	afterEngineHook := func() error {
		select {
		case <-entered:
		case <-time.After(time.Second):
			return errors.New("manager did not enter spare creation")
		}
		return errors.New("injected recovered-slot build failure")
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	qwpSfTestAfterEngineCreateHook.Store(&afterEngineHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfTestAfterEngineCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 1,
		100*time.Millisecond, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.close(context.Background()) })

	p.mu.Lock()
	require.Equal(t, qwpSfSlotRetired, p.sfSlots[0].state,
		"recovery failure freed an index whose flock is retained")
	p.mu.Unlock()
	_, err = qwpSfAcquireSlotLock(slotPath)
	require.Error(t, err)

	close(release)
	released = true
	require.Eventually(t, func() bool {
		p.reprobeRetiredSlots()
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.sfSlots[0].state == qwpSfSlotFree
	}, 2*time.Second, time.Millisecond)
}

// TestQwpSenderPoolCloseNeverTearsDownBorrowedDelegate pins the invariant: close()
// must not close the delegate of a borrowed slot — a producer goroutine
// may be inside it. close() waits boundedly (acquire timeout, capped),
// leaks the lease with a log line, and the delegate stays fully usable
// until the borrower returns it, at which point the returning goroutine
// tears it down exactly once.
func TestQwpSenderPoolCloseNeverTearsDownBorrowedDelegate(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2,
		200*time.Millisecond /* acquire = close wait budget */, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}

	s, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}

	// A producer keeps appending on the borrowed lease while close() runs.
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Table("t").Int64Column("v", 1).AtNow(context.Background())
				_ = s.Flush(context.Background())
			}
		}
	}()

	start := time.Now()
	if err := p.close(context.Background()); err != nil {
		t.Logf("close: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("close took %s; want ~the 200ms lease-wait budget", elapsed)
	}

	// The borrowed delegate is untouched: the producer is still writing
	// with no terminal error even though the pool is gone.
	if slotTerminallyFailed(s.(*qwpPooledSender).slot.delegate) {
		t.Fatal("close() poisoned the borrowed delegate")
	}
	close(stop)
	<-done

	// Returning the lease tears the delegate down on this goroutine.
	if err := s.Close(context.Background()); err != nil {
		t.Logf("lease close: %v", err)
	}
	// Duplicate close is a no-op (stale generation).
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("duplicate lease close: %v", err)
	}
}

// TestQwpSenderPoolCloseUnblocksWhenLeaseReturns pins the graceful half
// of the lease-return protocol: a lease returned while close() is waiting lets
// close() finish well before the wait budget, with the delegate torn
// down by the returning goroutine (tracked, so close() does not return
// mid-teardown).
func TestQwpSenderPoolCloseUnblocksWhenLeaseReturns(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") + ";"
	p, err := newQwpSenderPool(context.Background(), conf, 1, 2,
		5*time.Second, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	if err != nil {
		t.Fatalf("newQwpSenderPool: %v", err)
	}

	s, err := p.borrow(context.Background())
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = s.Close(context.Background())
	}()

	start := time.Now()
	if err := p.close(context.Background()); err != nil {
		t.Logf("close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("close took %s; a returned lease must unblock it well before the 5s budget", elapsed)
	}
	total, avail, _ := p.poolSnapshot()
	if total != 0 || avail != 0 {
		t.Fatalf("post-close snapshot total=%d avail=%d, want 0/0", total, avail)
	}
}

// TestQwpSenderPoolCloseLeakWarningRunsOffTheLock pins both halves of where the
// "leaving borrowed sender(s) alive" warning runs. It reaches the user's slog
// handler only after the pool lock is released, because a panic thrown while
// p.mu was held would leave that lock owned by a dead goroutine and every later
// borrow, return, reprobe and repeat Close would wait on it forever. And the
// call is panic-guarded, because by that point the available slots are already
// out of p.all and in closing -- an escaping panic would skip
// every teardown below and retain each flock with no owner at all.
func TestQwpSenderPoolCloseLeakWarningRunsOffTheLock(t *testing.T) {
	p := &qwpSenderPool{
		logger:         slog.New(panicOnHandleSlog{}),
		acquireTimeout: time.Millisecond,
		notify:         make(chan struct{}),
	}
	// One slot in `all` and none available is a borrowed lease, which is what
	// makes close() report a leak.
	p.all = []*qwpSenderSlot{{slotIndex: -1}}

	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_ = p.close(context.Background())
	}()
	require.False(t, panicked, "a panicking handler must not escape close() and skip the teardown")

	locked := make(chan struct{})
	go func() {
		p.poolSnapshot() // takes p.mu
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("the pool lock is still held after the logger panicked")
	}
}

// TestQwpSenderPoolReprobeSurvivesPanickingLogger pins that restoring capacity
// after a deferred slot cleanup cannot kill the process. reprobeRetiredSlots
// runs on the housekeeper daemon and on the borrow path, neither of which has
// a recover, and the "restored capacity" line reaches a user-supplied slog
// handler that is free to panic.
func TestQwpSenderPoolReprobeSurvivesPanickingLogger(t *testing.T) {
	p := &qwpSenderPool{
		logger:          slog.New(panicOnHandleSlog{}),
		storeAndForward: true,
		sfSlots:         []qwpSfSlotLifecycle{{state: qwpSfSlotRetired}},
		// A slot with no cleanup reporter counts as closed, which is what puts
		// the reprobe on its capacity-restoring path.
		retiredSlots: []*qwpSenderSlot{{slotIndex: 0}},
	}

	p.reprobeRetiredSlots()

	p.mu.Lock()
	defer p.mu.Unlock()
	require.Empty(t, p.retiredSlots)
	require.Equal(t, qwpSfSlotFree, p.sfSlots[0].state,
		"the completed slot lifecycle must return to free")
}

// TestQwpSenderPoolReapVictimSelectionIsAllOrNothing pins that the reap's
// accounting cannot be interrupted part-way. selectReapVictims calls into each
// delegate to classify it, and reclaimSlotLocked -- reached only from reapIdle,
// with the victim list this function returns -- settles closing to free or
// retired. A fault after earlier victims were selected but before the list is
// returned must not change their lifecycle states, or
// close() would then report ErrSfCleanupPending on every call forever, so the
// documented retry-until-it-clears contract would never terminate. The fault
// is recovered at the operation boundary and poisons the pool: the state is
// byte-for-byte unchanged, and every later borrow, lease return and close
// reports the terminal error instead of lending on — a faulting classifier is
// a client bug, and a pool that kept lending after one could hand a single
// delegate to two goroutines.
func TestQwpSenderPoolReapVictimSelectionIsAllOrNothing(t *testing.T) {
	p := &qwpSenderPool{
		storeAndForward: true,
		maxSize:         3,
		minSize:         0,
		idleTimeout:     time.Hour,
		acquireTimeout:  50 * time.Millisecond,
		sfSlots: []qwpSfSlotLifecycle{
			{state: qwpSfSlotAvailable},
			{state: qwpSfSlotAvailable},
			{state: qwpSfSlotAvailable},
		},
		notify: make(chan struct{}),
	}
	stale := time.Now().Add(-2 * time.Hour)
	// A nil delegate satisfies neither classifier interface, so it classifies
	// cleanly as an idle victim; a zero-value sender does satisfy them and
	// faults when they are called. Ordering the victim first puts the fault
	// after victims have already been counted into the partition's deltas.
	victim := &qwpSenderSlot{slotIndex: 0, idleSince: stale}
	keeper := &qwpSenderSlot{slotIndex: 1, idleSince: time.Now()} // fresh: kept
	faulting := &qwpSenderSlot{slotIndex: 2, idleSince: stale, delegate: &qwpLineSender{}}
	p.all = []*qwpSenderSlot{victim, keeper, faulting}
	p.available = []*qwpSenderSlot{victim, keeper, faulting}

	require.Empty(t, p.selectReapVictims(time.Now()),
		"a faulted classification must select no victims")

	p.mu.Lock()
	for i := range p.sfSlots {
		require.Equal(t, qwpSfSlotAvailable, p.sfSlots[i].state,
			"a fault during classification must not change slot %d", i)
	}
	require.Zero(t, p.pendingLeaseTeardowns,
		"nor as a teardown close() will wait for")

	// The available set must survive the fault intact. Filtering in place and
	// publishing only at the end leaves the backing array rewritten under an
	// unchanged slice header: a kept slot appears twice -- handed to two
	// borrowers at once, two producers on one delegate -- and a victim vanishes
	// from available while staying in p.all, where nothing can ever close it
	// and close() still reports a clean shutdown.
	require.Equal(t, []*qwpSenderSlot{victim, keeper, faulting}, p.available,
		"a fault during classification must leave the available set untouched")
	seen := map[*qwpSenderSlot]int{}
	for _, slot := range p.available {
		seen[slot]++
	}
	for slot, n := range seen {
		require.Equal(t, 1, n, "slot %p appears %d times in p.available", slot, n)
	}
	require.ErrorIs(t, p.poisonedErr, ErrPoolPoisoned)
	p.mu.Unlock()

	// The poison is loud on every producer-facing surface.
	_, err := p.borrow(context.Background())
	require.ErrorIs(t, err, ErrPoolPoisoned, "borrow must refuse on a poisoned pool")
	require.ErrorIs(t, p.close(context.Background()), ErrPoolPoisoned,
		"close must report the poison verdict")

	// A second fault does not overwrite the first recorded error.
	p.mu.Lock()
	first := p.poisonedErr
	p.poisonLocked("second fault", "boom")
	require.Same(t, first, p.poisonedErr)
	p.mu.Unlock()
}

// TestQwpSenderPoolPoisonSurfacesOnLeaseReturn pins that returning a lease to
// a poisoned pool reports the terminal error: the recycle itself still
// completes (the pool's bookkeeping is intact), but the producer must learn
// the pool is dead rather than borrow-and-fail later.
func TestQwpSenderPoolPoisonSurfacesOnLeaseReturn(t *testing.T) {
	p := &qwpSenderPool{
		maxSize:        1,
		acquireTimeout: 50 * time.Millisecond,
		notify:         make(chan struct{}),
	}
	slot := &qwpSenderSlot{}
	p.all = []*qwpSenderSlot{slot}
	gen := slot.generation.Add(1)
	ps := &qwpPooledSender{pool: p, slot: slot, gen: gen}

	p.mu.Lock()
	p.poisonLocked("test", "boom")
	p.mu.Unlock()

	err := p.giveBack(context.Background(), ps, false)
	require.ErrorIs(t, err, ErrPoolPoisoned)
	p.mu.Lock()
	defer p.mu.Unlock()
	require.Equal(t, []*qwpSenderSlot{slot}, p.available,
		"the return itself must still complete")
}

// TestQwpSenderPoolPrewarmFailureReportsRetainedSlotLock pins that a failed
// build does not silently swallow a slot lock it could not release. The pool
// being constructed is dropped unreachable on that path, so the error it
// returns is the caller's only notice that a rebuild on the same sf_dir and
// sender_id may fail naming this process as the holder.
func TestQwpSenderPoolPrewarmFailureReportsRetainedSlotLock(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()

	// Hold the manager worker inside spare creation so the prewarmed slot's
	// close cannot prove quiescence and retains its flock, then fail the build.
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	buildErr := errors.New("injected prewarm failure")
	afterEngineHook := func() error {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			return errors.New("manager did not enter spare creation")
		}
		return buildErr
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	qwpSfTestAfterEngineCreateHook.Store(&afterEngineHook)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfTestAfterEngineCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	_, err := newQwpSenderPool(context.Background(), conf, 1, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, buildErr, "the build failure must survive the join")
	require.ErrorIs(t, err, ErrSfCleanupPending,
		"a slot lock the failed build could not release must reach the caller")

	close(release)
	released = true

	// Wait for the deferred cleanup to finish before the test returns.
	// t.TempDir's RemoveAll otherwise races the retry owner, which is still
	// unmapping and unlinking inside the slot -- on Linux that surfaces as an
	// unlinkat failure in the cleanup rather than as anything about this test.
	// The condition runs on testify's polling goroutine, where t.FailNow is
	// invalid -- it calls runtime.Goexit, the condition never returns, and the
	// real error is replaced by a timeout. Collect the close error and assert
	// it back on the test goroutine.
	slot := filepath.Join(sfDir, qwpSfDefaultSenderId+"-0")
	var closeErr error
	require.Eventually(t, func() bool {
		lock, lockErr := qwpSfAcquireSlotLock(slot)
		if lockErr != nil {
			return false
		}
		closeErr = lock.close()
		return true
	}, 10*time.Second, 10*time.Millisecond, "the retired slot's cleanup must finish")
	require.NoError(t, closeErr)
}

// TestQwpSenderPoolReprobeSurvivesAFaultingSlot pins that a fault while probing
// a retired slot neither strands the pool mutex nor publishes a half-rewritten
// retired list. slotCloseCompleted calls into the delegate, and this runs on
// the housekeeper and on the borrow-at-capacity path — a stranded p.mu
// deadlocks every later borrow, return, reap and repeat close, which is exactly
// the re-probe the ErrSfCleanupPending contract tells callers to keep making.
// The fault is recovered at the operation boundary and poisons the pool: a
// probe that faulted once would fault again, and the lifecycle transitions it
// half-built must never be applied — a repeat probe must not free an index
// whose retired-list entry survived the failed classification.
func TestQwpSenderPoolReprobeSurvivesAFaultingSlot(t *testing.T) {
	p := &qwpSenderPool{
		storeAndForward: true,
		maxSize:         3,
		acquireTimeout:  50 * time.Millisecond,
		sfSlots: []qwpSfSlotLifecycle{
			{state: qwpSfSlotRetired},
			{state: qwpSfSlotRetired},
			{state: qwpSfSlotRetired},
		},
		notify: make(chan struct{}),
	}
	done := &qwpSenderSlot{slotIndex: 0}                               // no reporter: completed
	pending := &qwpSenderSlot{slotIndex: 1, cleanup: &neverDoneSlot{}} // still held
	faulting := &qwpSenderSlot{slotIndex: 2, cleanup: panicOnProbeSlot{}}
	p.retiredSlots = []*qwpSenderSlot{done, pending, faulting}

	p.reprobeRetiredSlots()

	locked := make(chan struct{})
	go func() {
		p.mu.Lock()
		_ = len(p.retiredSlots)
		p.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("the pool lock is still held after the probe faulted")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	require.Equal(t, []*qwpSenderSlot{done, pending, faulting}, p.retiredSlots,
		"a faulting probe must not publish a half-rewritten retired list")
	// Lifecycle publication must be all-or-nothing with the retired list.
	for i := range p.sfSlots {
		require.Equal(t, qwpSfSlotRetired, p.sfSlots[i].state,
			"a faulting probe must not change lifecycle state %d", i)
	}
	require.ErrorIs(t, p.poisonedErr, ErrPoolPoisoned,
		"a faulting probe must record the terminal pool error")
}

// neverDoneSlot reports a cleanup that has not finished.
type neverDoneSlot struct{}

func (*neverDoneSlot) closeCompleted() bool               { return false }
func (*neverDoneSlot) retryCloseIfNeeded() error          { return nil }
func (*neverDoneSlot) ensureCloseRetryOwner(*slog.Logger) {}

// panicOnProbeSlot stands in for a delegate whose state check faults.
type panicOnProbeSlot struct{}

func (panicOnProbeSlot) closeCompleted() bool               { panic("probe boom") }
func (panicOnProbeSlot) retryCloseIfNeeded() error          { return nil }
func (panicOnProbeSlot) ensureCloseRetryOwner(*slog.Logger) {}

// TestQwpSenderPoolConstructionPanicKeepsSlotIndexReserved pins that a panic
// during a pooled build does not put the slot index back into circulation
// while the engine it left behind still holds that directory's flock. The
// build guard installs a retry owner, but the pool's recover sees only an
// error — freeing the index lets a later borrow re-pick it and fail to open a
// slot this process is still holding, which is the reservation bitmap's whole
// purpose.
func TestQwpSenderPoolConstructionPanicKeepsSlotIndexReserved(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()

	// Hold the manager worker so the engine's close cannot complete, then
	// fault the build after the engine exists.
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	boom := func() {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
		}
		panic("pooled build boom")
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	qwpTestAfterSendLoopBuiltHook.Store(&boom)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpTestAfterSendLoopBuiltHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.close(context.Background()) })

	_, err = p.borrow(context.Background())
	require.ErrorContains(t, err, "sender build panicked")

	p.mu.Lock()
	state := p.sfSlots[0].state
	p.mu.Unlock()
	require.Equal(t, qwpSfSlotRetired, state,
		"the slot must stay retired while the engine's retry owner holds its flock")

	close(release)
	released = true
	require.Eventually(t, func() bool {
		p.reprobeRetiredSlots()
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.sfSlots[0].state == qwpSfSlotFree
	}, 10*time.Second, 10*time.Millisecond, "the index must come back once cleanup finishes")
}

// TestQwpSenderPoolPanicAfterSenderBuiltKeepsSlotReserved covers the other two
// construction unwind paths. The reporter used to be carried out only when the
// sender did not exist yet, so a fault after it was built — or inside the
// orphan setup, whose own defer ran first — still reached the pool as a bare
// panic. The pool then freed the slot index while the retry owner held that
// directory's flock, which is the collision the reservation bitmap exists to
// prevent.
func TestQwpSenderPoolPanicAfterSenderBuiltKeepsSlotReserved(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfDir := t.TempDir()

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	createHook := func(path string) {
		if filepath.Base(path) == "sf-initial.sfa" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	// Fault after the sender exists: the transport-swap callback runs there.
	boom := func() {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
		}
		panic("post-sender boom")
	}
	qwpSfTestSegmentCreateHook.Store(&createHook)
	qwpTestAfterSenderBuiltHook.Store(&boom)
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(20 * time.Millisecond)
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpTestAfterSenderBuiltHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
	})

	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;"
	p, err := newQwpSenderPool(context.Background(), conf, 0, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.close(context.Background()) })

	_, err = p.borrow(context.Background())
	require.ErrorContains(t, err, "sender build panicked")

	p.mu.Lock()
	state := p.sfSlots[0].state
	p.mu.Unlock()
	require.Equal(t, qwpSfSlotRetired, state,
		"the slot must stay retired while the flock is held")

	close(release)
	released = true
	require.Eventually(t, func() bool {
		p.reprobeRetiredSlots()
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.sfSlots[0].state == qwpSfSlotFree
	}, 10*time.Second, 10*time.Millisecond, "the index must come back once cleanup finishes")
}
