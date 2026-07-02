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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type ackTableEntry = struct {
	name   string
	seqTxn int64
}

// TestQwpDurableAckGatesWatermark drives a server that OK-acks a batch (which, in
// durable mode, must NOT advance AckedFsn) and only later sends the covering
// STATUS_DURABLE_ACK (which must).
func TestQwpDurableAckGatesWatermark(t *testing.T) {
	ctx := context.Background()
	sendDurable := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(ctx); err != nil { // the flushed batch (wireSeq 0)
			return
		}
		// Settled: OK ack for wireSeq 0 committing trades up to seqTxn 0.
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckOKWithTables(0, ackTableEntry{"trades", 0}))
		<-sendDurable
		// Durable: trades durable up to seqTxn 0 → releases wireSeq 0.
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckDurable(ackTableEntry{"trades", 0}))
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
		WithDurableAckKeepaliveInterval(0), // isolate: no keepalive pings
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(ctx)
	qs := s.(QwpSender)

	if err := s.Table("trades").Int64Column("v", 1).AtNow(ctx); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// After the OK ack, the durable watermark must stay unadvanced: the settled
	// ACK alone does not confirm durability.
	time.Sleep(150 * time.Millisecond)
	if got := qs.AckedFsn(); got >= 0 {
		t.Fatalf("AckedFsn advanced to %d on the OK ACK alone; durable mode must wait for STATUS_DURABLE_ACK", got)
	}

	// Release the durable ack; now the watermark must reach the batch.
	close(sendDurable)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := qs.AwaitAckedFsn(waitCtx, 0); err != nil {
		t.Fatalf("AwaitAckedFsn(0) after durable ack: %v", err)
	}
	if n := qs.TotalDurableAcks(); n < 1 {
		t.Errorf("TotalDurableAcks = %d, want >= 1", n)
	}
	if n := qs.TotalDurableTrimAdvances(); n < 1 {
		t.Errorf("TotalDurableTrimAdvances = %d, want >= 1", n)
	}
}

// newQwpPingGatedDurableServer stands in for a durable-ack primary that flushes
// the covering STATUS_DURABLE_ACK only AFTER it has observed a client ping, not
// on a wall-clock timer. OnPingReceived (driven by conn.Read processing the
// client's WebSocket ping control frames) signals pingSeen; a goroutine
// withholds the durable ack until then. This makes a test's success contingent
// on the keepalive ping actually firing — a regression that stopped emitting
// pings makes AwaitAckedFsn strand and the test fail, rather than passing
// trivially on a sleep. srvCtx bounds the withholding goroutine so a run where
// no ping ever arrives (keepalive disabled) does not leak it.
func newQwpPingGatedDurableServer(srvCtx context.Context) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		pingSeen := make(chan struct{}, 1)
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OnPingReceived: func(context.Context, []byte) bool {
				select {
				case pingSeen <- struct{}{}:
				default:
				}
				return true // still return the pong
			},
		})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		// Read the flushed batch so the OK ACK covers it.
		if _, _, err := conn.Read(srvCtx); err != nil {
			return
		}
		_ = conn.Write(srvCtx, websocket.MessageBinary, buildAckOKWithTables(0, ackTableEntry{"trades", 0}))
		// Release the durable ack only once a ping is observed. A separate
		// goroutine writes it so this handler can stay in the read loop below,
		// which is what processes the client's ping control frames and thus
		// drives OnPingReceived.
		go func() {
			select {
			case <-pingSeen:
				_ = conn.Write(srvCtx, websocket.MessageBinary, buildAckDurable(ackTableEntry{"trades", 0}))
			case <-srvCtx.Done():
			}
		}()
		for {
			if _, _, err := conn.Read(srvCtx); err != nil {
				return
			}
		}
	}))
}

// TestQwpDurableAckKeepaliveDelivers exercises the keepalive-enabled path against
// a server that withholds the durable ack until it observes a client ping. The
// batch becomes durable ONLY because the keepalive ping fires (and elicits the
// ack) — so this proves sendDurableKeepalive/qwpTransport.ping are actually
// exercised, unlike a timer-gated server that would pass even if pings stopped.
func TestQwpDurableAckKeepaliveDelivers(t *testing.T) {
	ctx := context.Background()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	srv := newQwpPingGatedDurableServer(srvCtx)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
		WithDurableAckKeepaliveInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(ctx)
	qs := s.(QwpSender)

	if err := s.Table("trades").Int64Column("v", 1).AtNow(ctx); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := qs.AwaitAckedFsn(waitCtx, 0); err != nil {
		t.Fatalf("AwaitAckedFsn(0) with keepalive enabled: %v", err)
	}
}

// TestQwpDurableAckKeepaliveDisabledStrands is the control for
// TestQwpDurableAckKeepaliveDelivers: same ping-gated server, but the keepalive
// is disabled (interval 0). With no ping to prod the server, the durable ack is
// never sent and AwaitAckedFsn must time out — confirming it is the keepalive
// ping, not some other mechanism, that drives durability in the sibling test.
func TestQwpDurableAckKeepaliveDisabledStrands(t *testing.T) {
	ctx := context.Background()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	srv := newQwpPingGatedDurableServer(srvCtx)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
		WithDurableAckKeepaliveInterval(0), // disabled: no ping will ever fire
		WithCloseFlushTimeout(0),           // fast close: skip the (doomed) drain wait
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(ctx)
	qs := s.(QwpSender)

	if err := s.Table("trades").Int64Column("v", 1).AtNow(ctx); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// The OK ACK arrives, but durable mode does not advance AckedFsn on it, and
	// with the keepalive off nothing elicits the withheld STATUS_DURABLE_ACK.
	waitCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	if err := qs.AwaitAckedFsn(waitCtx, 0); err == nil {
		t.Fatal("AwaitAckedFsn(0) returned nil with the keepalive disabled and the durable ack withheld; " +
			"the watermark must not advance without the durable ack")
	}
}

// newQwpNonAdvertisingServer stands in for a replica / non-primary: it completes
// the WebSocket upgrade WITHOUT the X-QWP-Durable-Ack advertisement, which a
// durable-ack client must treat as a terminal mismatch.
func newQwpNonAdvertisingServer(ctx context.Context) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1") // no X-QWP-Durable-Ack advertisement
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}))
}

// assertDurableMismatchError pins the documented WithRequestDurableAck /
// QwpDurableAckMismatchError contract: the connect fails with a *SenderError of
// category PROTOCOL_VIOLATION, while the underlying *QwpDurableAckMismatchError
// stays reachable via errors.As.
func assertDurableMismatchError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("durable-ack against a non-advertising endpoint should fail to connect")
	}
	var se *SenderError
	if !errors.As(err, &se) {
		t.Fatalf("error = %v, want a *SenderError (per WithRequestDurableAck godoc)", err)
	}
	if se.Category != CategoryProtocolViolation {
		t.Fatalf("category = %v, want PROTOCOL_VIOLATION", se.Category)
	}
	var mismatch *QwpDurableAckMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want the *QwpDurableAckMismatchError cause reachable", err)
	}
}

// TestQwpDurableAckMismatchTerminal pins Hazard A: requesting durable-ack against
// an endpoint that does not advertise it (a replica / non-primary) fails the
// connect terminally rather than silently falling back to OK-only trimming. It
// covers both the default (InitialConnectOff) and the Sync connect paths, which
// historically returned a raw *QwpDurableAckMismatchError instead of the
// documented *SenderError / PROTOCOL_VIOLATION (M1).
func TestQwpDurableAckMismatchTerminal(t *testing.T) {
	ctx := context.Background()
	srv := newQwpNonAdvertisingServer(ctx)
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	t.Run("DefaultOffPath", func(t *testing.T) {
		_, err := NewLineSender(ctx,
			WithQwp(), WithAddress(addr),
			WithRequestDurableAck(true),
		)
		assertDurableMismatchError(t, err)
	})

	t.Run("SyncPath", func(t *testing.T) {
		_, err := LineSenderFromConf(ctx, fmt.Sprintf(
			"ws::addr=%s;request_durable_ack=on;initial_connect_retry=sync;", addr))
		assertDurableMismatchError(t, err)
	})
}

// TestQwpDurableAckReconnectRequiresFreshDurable exercises the reconnect / reset /
// replay / re-confirm path that durable-ack exists to make safe. The primary
// OK-acks a batch (settled) but drops the connection BEFORE the durable upload
// completes — the exact silent-data-loss window. On reconnect the batch is
// replayed and MUST NOT be satisfied by the pre-drop OK or a stale watermark: only
// a fresh STATUS_DURABLE_ACK on the new connection advances AckedFsn.
func TestQwpDurableAckReconnectRequiresFreshDurable(t *testing.T) {
	ctx := context.Background()
	var connCount atomic.Int32
	replayed := make(chan struct{})       // closed once conn 2 has read the replayed batch
	releaseDurable := make(chan struct{}) // test closes this to let conn 2 durably ack

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(ctx); err != nil { // the flushed batch (wireSeq 0)
			return
		}
		if connCount.Add(1) == 1 {
			// Connection 1: settle the batch (OK), then drop WITHOUT a durable
			// ack — the upload never completes on this connection.
			_ = conn.Write(ctx, websocket.MessageBinary, buildAckOKWithTables(0, ackTableEntry{"trades", 0}))
			return
		}
		// Connection 2: the sender has reconnected and replayed the batch (again
		// wireSeq 0 — nextWireSeq resets per connection). Signal the test, wait
		// for the go-ahead, then OK + durably ack it.
		close(replayed)
		<-releaseDurable
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckOKWithTables(0, ackTableEntry{"trades", 0}))
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckDurable(ackTableEntry{"trades", 0}))
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
		WithDurableAckKeepaliveInterval(0),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(ctx)
	qs := s.(QwpSender)

	if err := s.Table("trades").Int64Column("v", 1).AtNow(ctx); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Wait until the reconnected connection has read the replayed batch.
	select {
	case <-replayed:
	case <-time.After(10 * time.Second):
		t.Fatal("sender did not reconnect and replay the un-durably-acked batch")
	}

	// At this point conn 1's OK settled the batch and the connection dropped;
	// conn 2 has replayed it but is blocked before its durable ack. The durable
	// watermark must NOT have advanced — neither the pre-drop OK nor a watermark
	// carried across the reconnect may satisfy the batch.
	if got := qs.AckedFsn(); got >= 0 {
		t.Fatalf("AckedFsn advanced to %d before any durable ack — a settled OK or "+
			"a stale cross-reconnect watermark wrongly satisfied the replayed batch", got)
	}

	// Release the fresh durable ack on the reconnected connection; now — and only
	// now — the watermark reaches the batch.
	close(releaseDurable)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := qs.AwaitAckedFsn(waitCtx, 0); err != nil {
		t.Fatalf("AwaitAckedFsn(0) after fresh durable ack on reconnect: %v", err)
	}
	if n := qs.TotalDurableAcks(); n < 1 {
		t.Errorf("TotalDurableAcks = %d, want >= 1", n)
	}
}

// TestQwpDurableAckEndToEndProgression is the local faithful-fake equivalent of
// the Enterprise durable-ack E2E: a primary that settles (OK) each batch
// immediately, then durably-acks it after an object-storage "upload" delay.
// Across several batches it verifies that the durable watermark, AwaitAckedFsn,
// the progress handler, and the durable counters all track durability, not the
// settled OK.
func TestQwpDurableAckEndToEndProgression(t *testing.T) {
	ctx := context.Background()
	const batches = 5
	const uploadDelay = 25 * time.Millisecond

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var writeMu sync.Mutex
		write := func(b []byte) {
			writeMu.Lock()
			_ = conn.Write(ctx, websocket.MessageBinary, b)
			writeMu.Unlock()
		}
		var seq int64
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
			n := seq
			seq++
			write(buildAckOKWithTables(n, ackTableEntry{"trades", n})) // settled now
			go func(k int64) {                                         // durable after the upload delay
				time.Sleep(uploadDelay)
				write(buildAckDurable(ackTableEntry{"trades", k}))
			}(n)
		}
	}))
	defer srv.Close()

	var progressMu sync.Mutex
	var progress []int64
	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
		WithDurableAckKeepaliveInterval(0),
		WithProgressHandler(func(fsn int64) {
			progressMu.Lock()
			progress = append(progress, fsn)
			progressMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(ctx)
	qs := s.(QwpSender)

	var lastFsn int64
	for i := 0; i < batches; i++ {
		if err := s.Table("trades").Int64Column("v", int64(i)).AtNow(ctx); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		fsn, err := qs.FlushAndGetSequence(ctx)
		if err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
		lastFsn = fsn
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := qs.AwaitAckedFsn(waitCtx, lastFsn); err != nil {
		t.Fatalf("AwaitAckedFsn(%d): %v", lastFsn, err)
	}
	if n := qs.TotalDurableAcks(); n < 1 {
		t.Errorf("TotalDurableAcks = %d, want >= 1", n)
	}
	if n := qs.TotalDurableTrimAdvances(); n < 1 {
		t.Errorf("TotalDurableTrimAdvances = %d, want >= 1", n)
	}

	// Progress is delivered asynchronously, so let it catch up to the durable
	// watermark AwaitAckedFsn already observed.
	deadline := time.After(3 * time.Second)
	for {
		progressMu.Lock()
		reached := len(progress) > 0 && progress[len(progress)-1] == lastFsn
		progressMu.Unlock()
		if reached {
			break
		}
		select {
		case <-deadline:
			progressMu.Lock()
			p := append([]int64(nil), progress...)
			progressMu.Unlock()
			t.Fatalf("progress never reached %d; got %v", lastFsn, p)
		case <-time.After(10 * time.Millisecond):
		}
	}
	progressMu.Lock()
	defer progressMu.Unlock()
	var prev int64 = -1
	for _, fsn := range progress {
		if fsn <= prev {
			t.Fatalf("progress not monotonic: %v", progress)
		}
		prev = fsn
	}
}

// TestQwpDurableAckDropChainsBehindPending pins Hazard D at the send-loop level:
// a DROP_AND_CONTINUE rejection in durable mode enqueues an empty entry that
// chains in FIFO order — the watermark must not pass the dropped frame until
// every preceding OK batch is durable, then release both together.
func TestQwpDurableAckDropChainsBehindPending(t *testing.T) {
	ctx := context.Background()
	sendDurable := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(ctx); err != nil { // batch 0: settled, not durable
			return
		}
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckOKWithTables(0, ackTableEntry{"trades", 0}))
		if _, _, err := conn.Read(ctx); err != nil { // batch 1: rejected → dropped
			return
		}
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckError(QwpStatusParseError, 1, "bad batch"))
		<-sendDurable
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckDurable(ackTableEntry{"trades", 0}))
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
		WithDurableAckKeepaliveInterval(0),
		WithErrorPolicy(CategoryParseError, PolicyDropAndContinue),
		WithErrorHandler(func(*SenderError) {}),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(ctx)
	qs := s.(QwpSender)

	for i := 0; i < 2; i++ {
		if err := s.Table("trades").Int64Column("v", int64(i)).AtNow(ctx); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := qs.FlushAndGetSequence(ctx); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	// Batch 1 was dropped, but its empty entry chains behind the pending OK for
	// batch 0 — the watermark must hold below both.
	time.Sleep(150 * time.Millisecond)
	if got := qs.AckedFsn(); got >= 0 {
		t.Fatalf("AckedFsn = %d after drop; must not advance past a pending OK batch", got)
	}

	close(sendDurable)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := qs.AwaitAckedFsn(waitCtx, 1); err != nil {
		t.Fatalf("AwaitAckedFsn(1) after covering durable ack: %v", err)
	}
}

// TestQwpDurableAckMismatchAsyncConnectLatchesTerminal covers the async-connect
// foreground path: the constructor returns immediately, the background dial
// hits the durable-ack mismatch, and the terminal PROTOCOL_VIOLATION latches so
// the producer observes it — no silent fallback to commit-only trimming.
func TestQwpDurableAckMismatchAsyncConnectLatchesTerminal(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1") // no durable-ack advertisement
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
		WithInitialConnectMode(InitialConnectAsync),
		WithErrorHandler(func(*SenderError) {}),
	)
	if err != nil {
		t.Fatalf("NewLineSender (async): %v", err)
	}
	defer s.Close(ctx)
	qs := s.(QwpSender)

	deadline := time.After(10 * time.Second)
	for {
		se := qs.LastTerminalError()
		if se != nil {
			if se.Category != CategoryProtocolViolation {
				t.Fatalf("terminal category = %s, want %s", se.Category, CategoryProtocolViolation)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("async durable-ack mismatch never latched a terminal error")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
