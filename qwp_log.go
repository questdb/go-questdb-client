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
)

// QWP diagnostics use the logger configured by WithLogger, WithQuestDBLogger,
// or WithQwpQueryClientLogger, falling back to slog.Default(). The application
// controls the sink, format, and enabled levels. Resolving a nil logger here
// captures the current default; a component retaining that result does not
// track later calls to slog.SetDefault.

// qwpGuardedHandler recovers synchronous panics from each wrapped
// slog.Handler method. Calls through a resolved logger use this boundary;
// argument evaluation before the call is outside it. The wrapper does not
// stop blocking, make re-entry safe, contain panics on another goroutine, or
// prevent process termination by application code.
//
// Guarding the handler preserves source attribution: slog captures the
// emitting caller's location before invoking the handler.
type qwpGuardedHandler struct{ inner slog.Handler }

// Enabled reports false when the wrapped handler panics: a handler that
// cannot answer safely gets no record.
func (h qwpGuardedHandler) Enabled(ctx context.Context, level slog.Level) (enabled bool) {
	defer func() {
		if recover() != nil {
			enabled = false
		}
	}()
	return h.inner.Enabled(ctx, level)
}

// Handle suppresses a panic and returns nil. It cannot undo any output the
// wrapped handler already produced before panicking.
func (h qwpGuardedHandler) Handle(ctx context.Context, rec slog.Record) (err error) {
	defer func() { _ = recover() }()
	return h.inner.Handle(ctx, rec)
}

// WithAttrs guards the derived handler. If derivation panics, it retains
// the existing guarded handler without the requested attributes.
func (h qwpGuardedHandler) WithAttrs(attrs []slog.Attr) (out slog.Handler) {
	out = h
	defer func() { _ = recover() }()
	inner := h.inner.WithAttrs(attrs)
	return qwpGuardedHandler{inner: inner}
}

// WithGroup guards the derived handler. If derivation panics, it retains
// the existing guarded handler without the requested group.
func (h qwpGuardedHandler) WithGroup(name string) (out slog.Handler) {
	out = h
	defer func() { _ = recover() }()
	inner := h.inner.WithGroup(name)
	return qwpGuardedHandler{inner: inner}
}

// qwpGuardLogger returns a logger whose handler is panic-guarded. nil
// resolves to slog.Default(). An already-guarded logger comes back as-is.
// Option setters use it for non-nil loggers; nil remains unset until a
// downstream call to qwpEffectiveLogger.
func qwpGuardLogger(l *slog.Logger) *slog.Logger {
	if l == nil {
		l = slog.Default()
	}
	if _, ok := l.Handler().(qwpGuardedHandler); ok {
		return l
	}
	return slog.New(qwpGuardedHandler{inner: l.Handler()})
}

// qwpEffectiveLogger resolves the configured logger, substituting
// slog.Default() when the caller registered none. It returns a non-nil
// logger with a guarded handler, including when l did not pass through an
// option setter. Configured, already-guarded loggers are reused.
func qwpEffectiveLogger(l *slog.Logger) *slog.Logger {
	return qwpGuardLogger(l)
}
