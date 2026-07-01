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

// TestQwpDurableAckKeepaliveDelivers exercises the keepalive-enabled path: the
// server withholds the durable ack for several keepalive intervals (during which
// pings fire), then confirms. The batch must still become durable without a
// deadlock, proving the keepalive path does not stall the send loop.
func TestQwpDurableAckKeepaliveDelivers(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		_ = conn.Write(ctx, websocket.MessageBinary, buildAckOKWithTables(0, ackTableEntry{"trades", 0}))
		time.Sleep(120 * time.Millisecond) // several 20ms keepalive intervals
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

// TestQwpDurableAckMismatchTerminal pins Hazard A: requesting durable-ack against
// an endpoint that does not advertise it (a replica / non-primary) fails the
// connect terminally rather than silently falling back to OK-only trimming.
func TestQwpDurableAckMismatchTerminal(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	_, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithRequestDurableAck(true),
	)
	if err == nil {
		t.Fatal("durable-ack against a non-advertising endpoint should fail to connect")
	}
	var mismatch *QwpDurableAckMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want *QwpDurableAckMismatchError", err)
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
