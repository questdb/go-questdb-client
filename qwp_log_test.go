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
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordCapturingHandler stores every record it handles, including the PC
// slog stamped at the call site, so a test can check source attribution.
type recordCapturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordCapturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordCapturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *recordCapturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordCapturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordCapturingHandler) sourceFiles() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var files []string
	for _, r := range h.records {
		frame, _ := runtime.CallersFrames([]uintptr{r.PC}).Next()
		files = append(files, filepath.Base(frame.File))
	}
	return files
}

// TestQwpLogSourceAttributionSurvivesTheGuard pins that the panic guard lives
// on the handler, not around the call: slog captures the caller's PC before
// the handler runs, so an AddSource-style handler sees the real emitting
// file. A per-call wrapper function would stamp every record with its own
// file instead, pointing all diagnostics at one line of plumbing.
func TestQwpLogSourceAttributionSurvivesTheGuard(t *testing.T) {
	capture := &recordCapturingHandler{}

	// Drive a real production emission: a drainer that cannot open its slot
	// logs the failure from qwp_sf_drainer.go.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfManifestFileName),
		[]byte("too short"), 0o644))
	original := qwpSfManifestQuarantineRename.load()
	t.Cleanup(func() { qwpSfManifestQuarantineRename.store(original) })
	qwpSfManifestQuarantineRename.store(func(string, string) error { return syscall.ENOSPC })

	drainer := qwpSfNewOrphanDrainer(
		dir, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		nil,
		200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond,
	)
	drainer.logger = qwpGuardLogger(slog.New(capture))
	drainer.drainerRun(context.Background())

	files := capture.sourceFiles()
	require.Contains(t, files, "qwp_sf_drainer.go",
		"the record must attribute to the emitting file")
	require.NotContains(t, files, "qwp_log.go",
		"the guard must not claim the emission as its own")
	require.NotContains(t, files, "qwp_sf_engine.go",
		"nor may a wrapper helper")
}

// TestQwpGuardedHandlerRecoversEveryMethod pins the guard itself: all four
// slog.Handler methods on a guarded logger absorb a panicking user handler.
// Enabled answers false, Handle drops the record, WithAttrs and WithGroup
// hand back the receiver unchanged — so no derived logger escapes the guard.
func TestQwpGuardedHandlerRecoversEveryMethod(t *testing.T) {
	l := qwpGuardLogger(slog.New(panicEverythingHandler{}))
	require.NotPanics(t, func() { l.Info("hello", "k", "v") })
	require.NotPanics(t, func() {
		derived := l.With("a", 1).WithGroup("g")
		derived.Error("still guarded")
	})
	// Idempotent wrap: guarding a guarded logger must not stack handlers.
	require.Same(t, l, qwpGuardLogger(l))
}

// panicEverythingHandler panics in every slog.Handler method.
type panicEverythingHandler struct{}

func (panicEverythingHandler) Enabled(context.Context, slog.Level) bool { panic("enabled") }
func (panicEverythingHandler) Handle(context.Context, slog.Record) error {
	panic("handle")
}
func (panicEverythingHandler) WithAttrs([]slog.Attr) slog.Handler { panic("attrs") }
func (panicEverythingHandler) WithGroup(string) slog.Handler      { panic("group") }
