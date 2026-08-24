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

// The client's diagnostics go through an injectable *slog.Logger rather than
// the global log package, so an embedding application controls the sink,
// format, and verbosity of everything the QWP transport emits. Register one
// with WithLogger (standalone sender) or WithQuestDBLogger (facade). When
// none is registered the client falls back to slog.Default(), so a
// misconfigured or rejecting server is never silent — the native-client
// "loud defaults" contract — while low-value chatter (replayed rejections,
// transient failover windows, watermark clamps) is emitted at slog.LevelDebug
// and stays hidden until the operator lowers the level. Inject a Discard
// handler to silence everything, or a custom handler to route it into the
// application's logging stack.

// qwpGuardedHandler wraps the application's slog.Handler with a panic
// boundary on all four methods. The handler is user code and free to panic,
// and the step behind a log call is regularly the one that matters — latching
// a fatal error, reporting on a channel, releasing a transport, writing a
// quarantine sentinel. Guarding once at the handler makes every log call in
// the package safe wherever and however the logger is spelled; guarding
// call sites individually can never be complete, because the set of
// expressions that can hold a logger is unbounded.
//
// A handler-level wrapper also preserves source attribution: slog captures
// the caller's location before the handler runs, so an AddSource handler
// reports the real emitting line, not a wrapper's.
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

// Handle drops the record when the wrapped handler panics.
func (h qwpGuardedHandler) Handle(ctx context.Context, rec slog.Record) (err error) {
	defer func() { _ = recover() }()
	return h.inner.Handle(ctx, rec)
}

// WithAttrs returns the receiver unchanged when the wrapped handler panics,
// so a derived logger can never escape the guard.
func (h qwpGuardedHandler) WithAttrs(attrs []slog.Attr) (out slog.Handler) {
	out = h
	defer func() { _ = recover() }()
	inner := h.inner.WithAttrs(attrs)
	return qwpGuardedHandler{inner: inner}
}

// WithGroup returns the receiver unchanged when the wrapped handler panics.
func (h qwpGuardedHandler) WithGroup(name string) (out slog.Handler) {
	out = h
	defer func() { _ = recover() }()
	inner := h.inner.WithGroup(name)
	return qwpGuardedHandler{inner: inner}
}

// qwpGuardLogger returns a logger whose handler is panic-guarded. nil
// resolves to slog.Default(). Idempotent: an already-guarded logger comes
// back as-is, so wrapping at every entry point cannot stack guards. Every
// logger the client stores or resolves goes through here — the option
// setters (WithLogger, WithQuestDBLogger, WithQwpQueryClientLogger) and
// qwpEffectiveLogger.
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
// slog.Default() when the caller registered none, and returns it with the
// panic-guarded handler installed. The result is always non-nil and always
// guarded, so every call site can log directly and unconditionally — even
// when the logger reached its struct field without passing through an option
// setter.
func qwpEffectiveLogger(l *slog.Logger) *slog.Logger {
	return qwpGuardLogger(l)
}
