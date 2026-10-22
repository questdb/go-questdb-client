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
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func (h *recordCapturingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	messages := make([]string, 0, len(h.records))
	for _, r := range h.records {
		messages = append(messages, r.Message)
	}
	return messages
}

func TestQwpRecoveryDiagnosticsUseConfiguredLoggerFromEngineOpen(t *testing.T) {
	defaultCapture := &recordCapturingHandler{}
	configuredCapture := &recordCapturingHandler{}
	originalDefault := slog.Default()
	slog.SetDefault(slog.New(defaultCapture))
	t.Cleanup(func() { slog.SetDefault(originalDefault) })

	tests := []struct {
		name        string
		message     string
		prepareSlot func(t *testing.T, slot string)
	}{
		{
			name:    "sanitized residue retry",
			message: "qwp/sf: sealed-segment residue was sanitized; retrying recovery once",
			prepareSlot: func(t *testing.T, slot string) {
				s0 := createRecoverySegment(t, slot, "sf-initial.sfa", 0, "a")
				s1 := createRecoverySegment(t, slot, "sf-0001.sfa", 1, "b")
				createRecoveryManifest(t, slot, 0, 1, s0, s1)
				s0.buf[s0.publishedOffset()+20] = 0x7f
				closeRecoverySegments(t, s0, s1)
			},
		},
		{
			name:    "fail-closed quarantine",
			message: "qwp/sf: recovery failed closed; preserved the slot and starting fresh",
			prepareSlot: func(t *testing.T, slot string) {
				segment, err := qwpSfCreateSegment(filepath.Join(slot, "sf-initial.sfa"), 0, 4096)
				require.NoError(t, err)
				require.NoError(t, segment.markManifestRequired())
				require.NoError(t, segment.close())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			slot := filepath.Join(root, qwpSfDefaultSenderId)
			require.NoError(t, os.MkdirAll(slot, 0o755))
			tc.prepareSlot(t, slot)

			sender, err := NewLineSender(context.Background(),
				WithQwp(),
				WithAddress("127.0.0.1:1"),
				WithSfDir(root),
				WithInitialConnectMode(InitialConnectAsync),
				WithCloseFlushTimeout(0),
				WithLogger(slog.New(configuredCapture)),
			)
			require.NoError(t, err)
			require.NoError(t, sender.Close(context.Background()))

			require.Contains(t, configuredCapture.messages(), tc.message,
				"recovery diagnostics must use the logger configured by the caller")
			require.NotContains(t, defaultCapture.messages(), tc.message,
				"recovery diagnostics must not leak to slog.Default")
		})
	}
}

func TestQwpDrainerRecoveryDiagnosticUsesConfiguredLoggerFromEngineOpen(t *testing.T) {
	defaultCapture := &recordCapturingHandler{}
	configuredCapture := &recordCapturingHandler{}
	originalDefault := slog.Default()
	slog.SetDefault(slog.New(defaultCapture))
	t.Cleanup(func() { slog.SetDefault(originalDefault) })

	slot := t.TempDir()
	s0 := createRecoverySegment(t, slot, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, slot, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, slot, 0, 1, s0, s1)
	s0.buf[s0.publishedOffset()+20] = 0x7f
	closeRecoverySegments(t, s0, s1)

	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	drainer := qwpSfNewOrphanDrainer(
		slot, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv),
		nil,
		200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond,
	)
	drainer.logger = qwpGuardLogger(slog.New(configuredCapture))
	drainer.drainerRun(context.Background())

	const message = "qwp/sf: sealed-segment residue was sanitized; retrying recovery once"
	require.Contains(t, configuredCapture.messages(), message,
		"drainer recovery diagnostics must use the drainer's configured logger")
	require.NotContains(t, defaultCapture.messages(), message,
		"drainer recovery diagnostics must not leak to slog.Default")
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

// Each method panics independently: an Enabled panic would otherwise prevent
// a log call from ever reaching Handle. Check the fallback, not just survival.
func TestQwpGuardedHandlerRecoversEveryMethod(t *testing.T) {
	for _, method := range []string{"Enabled", "Handle", "WithAttrs", "WithGroup"} {
		t.Run(method, func(t *testing.T) {
			capture := &recordCapturingHandler{}
			panics := 0
			h := &methodPanicLogHandler{Handler: capture, method: method, panics: &panics}
			l := qwpGuardLogger(slog.New(h))
			switch method {
			case "Enabled":
				require.False(t, l.Enabled(context.Background(), slog.LevelInfo))
				l.Info("filtered after Enabled panic")
				require.Empty(t, capture.messages())
				require.Equal(t, 2, panics)
			case "Handle":
				l.Info("attempted")
				require.NoError(t, l.Handler().Handle(context.Background(), slog.Record{}),
					"a recovered Handle panic returns nil")
				require.Empty(t, capture.messages())
				require.Equal(t, 2, panics)
			case "WithAttrs":
				derived := l.With("a", 1)
				require.Equal(t, l.Handler(), derived.Handler(), "retain the existing handler")
				derived.Info("still usable")
				require.Equal(t, []string{"still usable"}, capture.messages())
				require.Equal(t, 1, panics)
			case "WithGroup":
				derived := l.WithGroup("g")
				require.Equal(t, l.Handler(), derived.Handler(), "retain the existing handler")
				derived.Info("still usable")
				require.Equal(t, []string{"still usable"}, capture.messages())
				require.Equal(t, 1, panics)
			}
		})
	}
}

func TestQwpGuardedHandlerDerivation(t *testing.T) {
	var out bytes.Buffer
	l := qwpGuardLogger(slog.New(slog.NewJSONHandler(&out, nil)))
	l.Debug("disabled")
	require.Empty(t, out.String(), "preserve normal level filtering")
	derived := l.With("outside", 1).WithGroup("g").With("inside", 2)
	derived.Info("message", "record", 3)
	var record map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &record))
	require.Equal(t, float64(1), record["outside"])
	require.Equal(t, map[string]any{"inside": float64(2), "record": float64(3)}, record["g"])

	// Successful derivation must also guard the handler returned by the user.
	panics := 0
	h := &methodPanicLogHandler{Handler: &recordCapturingHandler{}, method: "Handle", panics: &panics}
	guarded := qwpGuardLogger(slog.New(h))
	guarded.With("a", 1).Info("attributes")
	guarded.WithGroup("g").Info("group")
	require.Equal(t, 2, panics)
}

func TestQwpGuardedHandlerPreservesHandleError(t *testing.T) {
	want := errors.New("handler error")
	h := &methodPanicLogHandler{Handler: &recordCapturingHandler{}, handleErr: want}
	guarded := qwpGuardLogger(slog.New(h))
	require.ErrorIs(t, guarded.Handler().Handle(context.Background(), slog.Record{}), want)
}

func TestQwpGuardLoggerIsIdempotent(t *testing.T) {
	l := qwpGuardLogger(slog.New(&recordCapturingHandler{}))
	require.Same(t, l, qwpGuardLogger(l))
	require.Same(t, l, qwpEffectiveLogger(l))
}

// methodPanicLogHandler is used synchronously by the wrapper unit tests.
// All methods other than the selected one forward to the supplied handler.
type methodPanicLogHandler struct {
	slog.Handler
	method    string
	panics    *int
	handleErr error
}

func (h *methodPanicLogHandler) panicIn(method string) {
	if h.method == method {
		(*h.panics)++
		panic(method)
	}
}

func (h *methodPanicLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	h.panicIn("Enabled")
	return h.Handler.Enabled(ctx, level)
}

func (h *methodPanicLogHandler) Handle(ctx context.Context, rec slog.Record) error {
	h.panicIn("Handle")
	if h.handleErr != nil {
		return h.handleErr
	}
	return h.Handler.Handle(ctx, rec)
}

func (h *methodPanicLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.panicIn("WithAttrs")
	derived := *h
	derived.Handler = h.Handler.WithAttrs(attrs)
	return &derived
}

func (h *methodPanicLogHandler) WithGroup(name string) slog.Handler {
	h.panicIn("WithGroup")
	derived := *h
	derived.Handler = h.Handler.WithGroup(name)
	return &derived
}
