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
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// readPersistedDurableWatermark reads the on-disk durable-ack watermark FSN for
// the slot, or (INVALID, false) when the file is absent or not yet stamped with
// the magic. It reads the file bytes directly (rather than re-mmap'ing) so a live
// sender may hold its own mapping of the same file concurrently.
func readPersistedDurableWatermark(t *testing.T, slotDir string) (int64, bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(slotDir, qwpSfAckWatermarkFileName))
	if err != nil || int64(len(b)) < qwpSfAckWatermarkFileSize {
		return qwpSfAckWatermarkInvalid, false
	}
	if binary.LittleEndian.Uint32(b[qwpSfAckWatermarkMagicOffset:qwpSfAckWatermarkMagicOffset+4]) != qwpSfAckWatermarkMagic {
		return qwpSfAckWatermarkInvalid, false
	}
	return int64(binary.LittleEndian.Uint64(b[qwpSfAckWatermarkFsnOffset : qwpSfAckWatermarkFsnOffset+8])), true
}

// TestQwpDurableAckSfCrashRecoveryReplaysTail composes request_durable_ack +
// sf_dir + a process restart to prove the headline durable-ack guarantee
// end-to-end, which no single existing test does: a batch that was only settled
// (OK-acked) but not durably acked must NOT advance the on-disk durable
// watermark, and after a crash the sender resumes from the last DURABLE point —
// replaying only the not-yet-durable tail, never from FSN 0.
//
// The tracker/watermark pieces are unit-tested in isolation
// (TestQwpDurableAckGatesWatermark, the qwp_sf_ack_watermark tests), but the
// restart fuzz suites run against DEDUP tables, where a wrong re-replay from FSN
// 0 would still pass a count() check. This test uses a plain (non-DEDUP) table
// and asserts the exact frame that crosses the wire on recovery, so a regression
// that re-shipped already-durable rows (silent duplicates) fails loudly.
func TestQwpDurableAckSfCrashRecoveryReplaysTail(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	slotDir := filepath.Join(dir, qwpSfDefaultSenderId)

	// --- Session 1: durably-ack batch 0; OK-only (withhold durable) batch 1. ---
	batch1OKed := make(chan struct{})
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		wsConn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer wsConn.CloseNow()
		var n int64
		for {
			if _, _, err := wsConn.Read(ctx); err != nil {
				return
			}
			seq := n // wireSeq == per-table seqTxn == batch index in this run
			n++
			// Distinct per-batch seqTxns so the batch-0 durable ack cannot also
			// cover batch 1.
			_ = wsConn.Write(ctx, websocket.MessageBinary, buildAckOKWithTables(seq, ackTableEntry{"trades", seq}))
			switch seq {
			case 0:
				_ = wsConn.Write(ctx, websocket.MessageBinary, buildAckDurable(ackTableEntry{"trades", 0}))
			case 1:
				close(batch1OKed) // OK only — the un-durable tail.
			}
		}
	}))
	addr1 := strings.TrimPrefix(srv1.URL, "http://")

	s1, err := LineSenderFromConf(ctx, "ws::addr="+addr1+";sf_dir="+dir+
		";request_durable_ack=on;durable_ack_keepalive_interval_millis=0;close_flush_timeout_millis=200;")
	if err != nil {
		srv1.Close()
		t.Fatalf("session 1 LineSenderFromConf: %v", err)
	}
	qs1 := s1.(QwpSender)

	// Batch 0: write, flush, and wait until it is durable.
	if err := s1.Table("trades").Int64Column("v", 0).AtNow(ctx); err != nil {
		t.Fatalf("write batch 0: %v", err)
	}
	fsn0, err := qs1.FlushAndGetSequence(ctx)
	if err != nil {
		t.Fatalf("flush batch 0: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := qs1.AwaitAckedFsn(waitCtx, fsn0); err != nil {
		cancel()
		t.Fatalf("AwaitAckedFsn(%d) for durable batch 0: %v", fsn0, err)
	}
	cancel()

	// Batch 1: write, flush, wait for the server's OK ack (but NOT its durable).
	if err := s1.Table("trades").Int64Column("v", 1).AtNow(ctx); err != nil {
		t.Fatalf("write batch 1: %v", err)
	}
	fsn1, err := qs1.FlushAndGetSequence(ctx)
	if err != nil {
		t.Fatalf("flush batch 1: %v", err)
	}
	if fsn1 <= fsn0 {
		t.Fatalf("batch 1 FSN %d must be > batch 0 FSN %d", fsn1, fsn0)
	}
	select {
	case <-batch1OKed:
	case <-time.After(5 * time.Second):
		t.Fatal("server never OK-acked batch 1")
	}

	// The OK-only batch 1 must NOT advance the durable watermark past batch 0.
	if got := qs1.AckedFsn(); got != fsn0 {
		t.Fatalf("AckedFsn = %d after batch 1's OK ack; want %d — an OK ack alone must not advance durability", got, fsn0)
	}
	// The on-disk durable watermark must reach batch 0 and stop there.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if wm, ok := readPersistedDurableWatermark(t, slotDir); ok && wm == fsn0 {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("on-disk durable watermark = (%d, present=%v); want %d persisted", wm, ok, fsn0)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Give any erroneous advance-to-fsn1 time to surface, then re-check: an
	// OK-only batch must never move the persisted watermark.
	time.Sleep(100 * time.Millisecond)
	if wm, _ := readPersistedDurableWatermark(t, slotDir); wm != fsn0 {
		t.Fatalf("on-disk durable watermark advanced to %d on an OK-only batch; want %d", wm, fsn0)
	}

	// Crash: close session 1. Batch 1 stays on disk, un-trimmed (un-durable).
	_ = s1.Close(ctx)
	srv1.Close()

	// --- Session 2: reopen the SAME slot; only the un-durable tail must replay. ---
	var framesReplayed atomic.Int64
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		wsConn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer wsConn.CloseNow()
		var n int64
		for {
			if _, _, err := wsConn.Read(ctx); err != nil {
				return
			}
			framesReplayed.Add(1)
			seq := n // wireSeq resets to 0 on this fresh connection
			n++
			_ = wsConn.Write(ctx, websocket.MessageBinary, buildAckOKWithTables(seq, ackTableEntry{"trades", seq}))
			_ = wsConn.Write(ctx, websocket.MessageBinary, buildAckDurable(ackTableEntry{"trades", seq}))
		}
	}))
	defer srv2.Close()
	addr2 := strings.TrimPrefix(srv2.URL, "http://")

	s2, err := LineSenderFromConf(ctx, "ws::addr="+addr2+";sf_dir="+dir+
		";request_durable_ack=on;durable_ack_keepalive_interval_millis=0;close_flush_timeout_millis=200;")
	if err != nil {
		t.Fatalf("session 2 LineSenderFromConf: %v", err)
	}
	qs2 := s2.(QwpSender)
	defer s2.Close(ctx)

	// Recovery replays the not-yet-durable tail (FSN fsn1) and the new primary
	// durably-acks it; AckedFsn must reach fsn1 without writing any new rows.
	waitCtx2, cancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel2()
	if err := qs2.AwaitAckedFsn(waitCtx2, fsn1); err != nil {
		t.Fatalf("AwaitAckedFsn(%d) after crash recovery: %v (replayed %d frame(s))",
			fsn1, err, framesReplayed.Load())
	}

	// The crux: batch 0 was durable before the crash, so it must NOT replay.
	// Exactly one frame — the un-durable tail — should have crossed the wire; a
	// count of 2 means recovery restarted from FSN 0 and would have duplicated
	// already-durable rows on a non-DEDUP table.
	if got := framesReplayed.Load(); got != 1 {
		t.Fatalf("replayed %d frames on recovery; want exactly 1 (the un-durable tail). "+
			"2 means recovery resumed from FSN 0, re-shipping durable rows", got)
	}
	if n := qs2.TotalDurableAcks(); n < 1 {
		t.Errorf("session 2 TotalDurableAcks = %d, want >= 1", n)
	}
}
