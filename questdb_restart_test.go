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
	"fmt"
	"testing"
	"time"
)

// facadeWriteRows writes restartFuzz-schema rows through a leased facade sender
// (a LineSender, not a QwpSender), so the restartFuzz HTTP row assertions can
// verify them.
func facadeWriteRows(t *testing.T, s LineSender, idBase int64, count int, tsBaseNanos int64) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		id := idBase + int64(i)
		ts := time.Unix(0, tsBaseNanos+int64(i)*1000).UTC()
		if err := s.Table(restartFuzzTableName).
			Int64Column("id", id).
			Float64Column("val", float64(id)*1.5).
			At(ctx, ts); err != nil {
			t.Fatalf("write row id=%d: %v", id, err)
		}
	}
}

// TestQwpIntegrationFacadeLazyConnectDownThenUp is the facade port of Java's
// QuestDBServerRecoveryTest: with lazy_connect=true the handle builds while the
// server is DOWN (async ingest + read pool min=0), buffers writes meanwhile,
// then once the server is UP the ingest side reconnects and replays, and reads
// connect lazily on the first borrow. Requires a fixture-launched (restartable)
// server; skips against an external QDB_FUZZ_ADDR target.
// TestQwpIntegration* so the qwp-fuzz.yml server-bound -run filters select it (M3).
func TestQwpIntegrationFacadeLazyConnectDownThenUp(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("requires fixture-launched server (cannot pause a QDB_FUZZ_ADDR target)")
	}
	restartFuzzSetup(t, srv)
	t.Cleanup(func() { _ = srv.start() })

	ctx := context.Background()
	baseTs := int64(1_700_000_000_000_000_000)
	const rows = 500

	// Server DOWN before build.
	srv.kill()

	// lazy_connect builds without the server: ingest async, read pool min=0.
	db, err := NewQuestDB(ctx,
		fmt.Sprintf("ws::addr=%s;lazy_connect=true;", srv.wsAddr()),
		WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("lazy build with server down: %v", err)
	}
	defer db.Close(ctx)
	if total, _ := db.queryPool.poolSnapshot(); total != 0 {
		t.Errorf("read pool prewarmed total=%d, want 0 (lazy)", total)
	}

	// Buffer writes while the server is down.
	s, err := db.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("BorrowSender (down): %v", err)
	}
	facadeWriteRows(t, s, 0, rows, baseTs)
	_ = s.Flush(ctx)
	_ = s.Close(ctx)

	// Server UP — the async sender reconnects and replays the buffered rows.
	if err := srv.start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	restartFuzzAssertRowCount(t, srv, rows, 90*time.Second)

	// Reads connect lazily on the first borrow now that the server is up.
	q, err := db.BorrowQuery(ctx)
	if err != nil {
		t.Fatalf("BorrowQuery (lazy connect): %v", err)
	}
	defer q.Close()
	cur := q.Query(ctx, "select count() from "+restartFuzzTableName)
	for _, err := range cur.Batches() {
		if err != nil {
			t.Fatalf("read after recovery: %v", err)
		}
	}
}

// TestQwpIntegrationFacadeSfCrashRecoveryAcrossRestart exercises SF-in-pool crash
// recovery (Hazard A/C/D/H end-to-end): a pooled SF sender writes, the server is
// killed before close so unacked frames stay on disk, then a fresh facade on the
// same sf_dir reattaches to the stranded slot and replays. Requires a
// fixture-launched server; skips against an external target.
func TestQwpIntegrationFacadeSfCrashRecoveryAcrossRestart(t *testing.T) {
	srv := fuzzServer(t)
	if !srv.owns {
		t.Skip("requires fixture-launched server (cannot kill a QDB_FUZZ_ADDR target)")
	}
	restartFuzzSetup(t, srv)
	t.Cleanup(func() { _ = srv.start() })

	ctx := context.Background()
	sfDir := t.TempDir()
	baseTs := int64(1_700_000_000_000_000_000)
	const rows = 2000
	conf := fmt.Sprintf("ws::addr=%s;sf_dir=%s;close_flush_timeout_millis=0;", srv.wsAddr(), sfDir)

	// Epoch 1: pooled SF sender writes; kill the server BEFORE close so
	// genuinely-unacked frames remain durable on disk in the slot dir.
	db1, err := NewQuestDB(ctx, conf, WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("epoch 1 connect: %v", err)
	}
	s, err := db1.BorrowSender(ctx)
	if err != nil {
		t.Fatalf("epoch 1 BorrowSender: %v", err)
	}
	facadeWriteRows(t, s, 0, rows, baseTs)
	_ = s.Flush(ctx)
	srv.kill()
	_ = s.Close(ctx) // best-effort; frames are durable for epoch 2
	_ = db1.Close(ctx)

	// Epoch 2: server back, a fresh facade on the same sf_dir reattaches to the
	// stranded slot (prewarm rebinds <base>-0) and replays its unacked data.
	if err := srv.start(); err != nil {
		t.Fatalf("epoch 2 start server: %v", err)
	}
	db2, err := NewQuestDB(ctx, conf, WithQuestDBConnectionListener(func(SenderConnectionEvent) {}))
	if err != nil {
		t.Fatalf("epoch 2 connect: %v", err)
	}
	defer db2.Close(ctx)
	restartFuzzAssertRowCount(t, srv, rows, 120*time.Second)
}
