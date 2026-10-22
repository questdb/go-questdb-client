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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// These tests pin that a slot recovery refused stays reportable when the
// sender that preserved it is discarded before any caller receives it. The
// copy's only report would otherwise be a best-effort log line, and a retry
// opens the fresh slot under the original name and reports nothing.

// requirePreservedSegments asserts the preserved copy still holds the files
// writeFailClosedSlot created.
func requirePreservedSegments(t *testing.T, preserved string) {
	t.Helper()
	require.FileExists(t, filepath.Join(preserved, "sf-initial.sfa"))
	require.FileExists(t, filepath.Join(preserved, "sf-active.sfa"))
}

func TestQwpSenderBuildFailureNamesQuarantinedSlot(t *testing.T) {
	sfRoot := t.TempDir()
	writeFailClosedSlot(t, filepath.Join(sfRoot, "solo"))

	// Nothing listens on port 1, and the default initial connect makes one
	// pass, so construction fails after the engine preserved the slot.
	_, err := LineSenderFromConf(context.Background(), "ws::addr=127.0.0.1:1;sf_dir="+sfRoot+
		";sender_id=solo;close_flush_timeout_millis=200;")
	require.Error(t, err)
	preserved := filepath.Join(sfRoot, "solo"+qwpSfQuarantineSlotInfix+"0")
	require.Contains(t, err.Error(), preserved)
	requirePreservedSegments(t, preserved)
}

func TestQwpSenderBuildPanicNamesQuarantinedSlot(t *testing.T) {
	boom := func() error { panic("injected construction panic") }
	qwpSfTestAfterEngineCreateHook.Store(&boom)
	t.Cleanup(func() { qwpSfTestAfterEngineCreateHook.Store(nil) })

	sfRoot := t.TempDir()
	writeFailClosedSlot(t, filepath.Join(sfRoot, "solo"))

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = LineSenderFromConf(context.Background(), "ws::addr=127.0.0.1:1;sf_dir="+sfRoot+
			";sender_id=solo;close_flush_timeout_millis=200;")
	}()
	require.NotNil(t, recovered)
	if bp, ok := recovered.(qwpSfBuildPanic); ok {
		require.Eventually(t, bp.reporter.closeCompleted, qwpTestWaitTimeout, time.Millisecond)
	}
	preserved := filepath.Join(sfRoot, "solo"+qwpSfQuarantineSlotInfix+"0")
	require.Contains(t, fmt.Sprint(recovered), preserved)
	requirePreservedSegments(t, preserved)
}

func TestQwpSenderPoolPrewarmFailureNamesQuarantinedSlot(t *testing.T) {
	injected := errors.New("second prewarm build failed")
	var calls int
	fail := func() error {
		calls++
		if calls == 2 {
			return injected
		}
		return nil
	}
	qwpSfTestAfterEngineCreateHook.Store(&fail)
	t.Cleanup(func() { qwpSfTestAfterEngineCreateHook.Store(nil) })

	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfRoot := t.TempDir()
	writeFailClosedSlot(t, filepath.Join(sfRoot, qwpSfDefaultSenderId+"-0"))

	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfRoot + ";close_flush_timeout_millis=200;"
	_, err := newQwpSenderPool(context.Background(), conf, 2, 2,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.ErrorIs(t, err, injected)
	// Slot 1 failed on its own; only the unwind can report the copy slot 0 made.
	preserved := filepath.Join(sfRoot, qwpSfDefaultSenderId+"-0"+qwpSfQuarantineSlotInfix+"0")
	require.Contains(t, err.Error(), preserved)
	requirePreservedSegments(t, preserved)
}

func TestQuestDBQueryPoolFailureNamesQuarantinedSlot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == qwpReadPath {
			w.WriteHeader(http.StatusForbidden) // terminal egress reject: query prewarm fails fast
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
	t.Cleanup(srv.Close)
	sfRoot := t.TempDir()
	writeFailClosedSlot(t, filepath.Join(sfRoot, qwpSfDefaultSenderId+"-0"))

	_, err := NewQuestDB(context.Background(),
		"ws::addr="+strings.TrimPrefix(srv.URL, "http://")+";sf_dir="+sfRoot+
			";sender_pool_min=1;sender_pool_max=1;close_flush_timeout_millis=200;",
		noopConnListener())
	require.Error(t, err)
	preserved := filepath.Join(sfRoot, qwpSfDefaultSenderId+"-0"+qwpSfQuarantineSlotInfix+"0")
	require.Contains(t, err.Error(), preserved)
	requirePreservedSegments(t, preserved)
}

func TestQwpSenderPoolClosingDuringBuildNamesQuarantinedSlot(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	hook := func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	createSlotHook.Store(&hook)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		createSlotHook.Store(nil)
	})

	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	sfRoot := t.TempDir()
	ctx := context.Background()
	conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
		";sf_dir=" + sfRoot + ";close_flush_timeout_millis=200;"
	// The acquire timeout must outlast the held build: a borrower that gives up
	// leaves the build to settleGrowthBuild, which has no caller to report to.
	p, err := newQwpSenderPool(ctx, conf, 0, 1,
		qwpPoolTestUnhurriedAcquire, 0, 0, nil, nil, QwpBackgroundDrainerListener{}, nil)
	require.NoError(t, err)
	// Created only now: present during construction, the slot would be adopted
	// by the startup recovery scan, and borrow would take it without a build.
	writeFailClosedSlot(t, filepath.Join(sfRoot, qwpSfDefaultSenderId+"-0"))

	borrowErr := make(chan error, 1)
	go func() {
		lease, err := p.borrow(ctx)
		if lease != nil {
			_ = lease.Close(ctx)
		}
		borrowErr <- err
	}()
	<-entered
	closeErr := make(chan error, 1)
	// close waits for the slot still being created, so it runs off this goroutine.
	go func() { closeErr <- p.close(ctx) }()
	require.Eventually(t, p.closing.Load, qwpTestWaitTimeout, time.Millisecond)
	releaseOnce.Do(func() { close(release) })

	select {
	case err = <-borrowErr:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("borrow did not return after the build was released")
	}
	require.ErrorIs(t, err, errPoolClosed)
	preserved := filepath.Join(sfRoot, qwpSfDefaultSenderId+"-0"+qwpSfQuarantineSlotInfix+"0")
	require.Contains(t, err.Error(), preserved)
	requirePreservedSegments(t, preserved)
	select {
	case <-closeErr:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("pool close did not return after the build settled")
	}
}
