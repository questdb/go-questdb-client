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
	"log/slog"

	"github.com/coder/websocket"
)

// qwpSfClassify maps a QWP server response status byte to a Category.
// Wire codes are 1:1 with the categories the server distinguishes;
// unknown bytes fall through to CategoryUnknown so the client never
// silently drops a rejection it cannot interpret.
//
// Mirror of Java CursorWebSocketSendLoop.classify (always keep these
// two in sync — categories are part of the public cross-language
// surface).
func qwpSfClassify(status QwpStatusCode) Category {
	switch status {
	case QwpStatusSchemaMismatch:
		return CategorySchemaMismatch
	case QwpStatusParseError:
		return CategoryParseError
	case QwpStatusInternalError:
		return CategoryInternalError
	case QwpStatusSecurityError:
		return CategorySecurityError
	case QwpStatusWriteError:
		return CategoryWriteError
	case QwpStatusNotWritable:
		return CategoryNotWritable
	default:
		return CategoryUnknown
	}
}

// qwpSfDefaultPolicyFor is the default Category → Policy mapping, used
// when the user has not overridden the slot via builder option or
// connect-string. There is no drop policy: a rejection is either
// replayed (RETRIABLE / RETRIABLE_OTHER — the bytes stay in the SF
// log, the connection is recycled, replay resumes from ackedFsn+1) or
// halts the sender loudly (TERMINAL — reserved for rejections that are
// deterministic under byte-identical replay; the bytes stay on disk).
// CategoryProtocolViolation is forced TERMINAL and CategoryUnknown is
// forced RETRIABLE; the resolver enforces both independent of user
// overrides.
func qwpSfDefaultPolicyFor(c Category) Policy {
	switch c {
	case CategoryWriteError, // transient server state (disk pressure, suspended table)
		CategoryInternalError, // transient by definition; deterministic repeats poison-escalate
		CategoryUnknown:       // fail open: status byte from a newer server
		return PolicyRetriable
	case CategoryNotWritable: // read-only replica / demoting primary: rotate endpoints
		return PolicyRetriableOther
	case CategorySchemaMismatch, // deterministic: same bytes, same mismatch
		CategoryParseError,    // deterministic: malformed bytes never parse
		CategorySecurityError: // ACL denial on a writable node (read-only refusals arrive as role-change closes)
		return PolicyTerminal
	case CategoryProtocolViolation:
		return PolicyTerminal
	default:
		return PolicyTerminal
	}
}

// qwpSfIsTerminalCloseCode reports whether a WebSocket close code
// nominally signals a protocol-layer violation per RFC 6455. NO LONGER
// consulted by the policy path: WS close codes carry no policy
// semantics (policy travels only in QWP NACK frames), every close is
// reconnect-eligible, and a frame that deterministically kills the
// connection is caught behaviorally by the poison-frame detector
// (max_frame_rejections) rather than by this code list. Retained for
// diagnostics and tests.
func qwpSfIsTerminalCloseCode(code websocket.StatusCode) bool {
	switch code {
	case websocket.StatusProtocolError,
		websocket.StatusUnsupportedData,
		websocket.StatusInvalidFramePayloadData,
		websocket.StatusPolicyViolation,
		websocket.StatusMessageTooBig,
		websocket.StatusMandatoryExtension:
		return true
	default:
		return false
	}
}

// qwpSfPolicyResolver composes the precedence chain for resolving a
// Category to a concrete Policy:
//
//  1. resolver (programmatic, full control via WithErrorPolicyResolver)
//  2. perCat[c] (builder WithErrorPolicy or connect-string on_*_error)
//  3. global (connect-string on_server_error)
//  4. spec default (qwpSfDefaultPolicyFor)
//
// CategoryProtocolViolation and CategoryUnknown bypass user overrides —
// ProtocolViolation always resolves to PolicyTerminal and Unknown to
// PolicyRetriable (fail open) — silently ignoring user-set slots for
// those two categories.
type qwpSfPolicyResolver struct {
	resolver func(Category) Policy
	perCat   [numCategories]Policy
	global   Policy
	// logger sinks the resolver panic-guard diagnostic. nil ->
	// slog.Default() via qwpEffectiveLogger.
	logger *slog.Logger
}

// callResolver invokes the user-supplied resolver under a panic guard.
// The resolver runs on the receiver goroutine, so a panicking
// WithErrorPolicyResolver callback would otherwise crash the host. A
// panic is treated as a user bug: recover, log, and fall back to the
// spec default for the category. That default is always a concrete
// policy (never PolicyAuto), so resolve's
// `!= PolicyAuto` check short-circuits the rest of the precedence chain
// — a broken resolver yields the safe spec policy rather than silently
// deferring to lower-precedence slots. A clean return is propagated
// verbatim, including PolicyAuto, which lets resolve fall through.
//
// Mirrors the handler panic guard in qwpSfErrorDispatcher.deliver.
func (r *qwpSfPolicyResolver) callResolver(c Category) (pol Policy) {
	defer func() {
		if rec := recover(); rec != nil {
			qwpEffectiveLogger(r.logger).Error("qwp/sf: error policy resolver panicked",
				"category", c, "panic", rec)
			pol = qwpSfDefaultPolicyFor(c)
		}
	}()
	return r.resolver(c)
}

// resolve returns the Policy to apply for the given Category.
// PolicyAuto is never returned — every category resolves to a concrete
// Retriable / RetriableOther / Terminal choice.
func (r *qwpSfPolicyResolver) resolve(c Category) Policy {
	// Forced regardless of user configuration: a poison-escalated /
	// protocol-level violation is deterministic (TERMINAL); an
	// unintelligible status byte fails open to retry so a newer server
	// cannot kill deployed senders (RETRIABLE).
	if c == CategoryProtocolViolation {
		return PolicyTerminal
	}
	if c == CategoryUnknown {
		return PolicyRetriable
	}
	if r != nil {
		if r.resolver != nil {
			if p := r.callResolver(c); p != PolicyAuto {
				return p
			}
		}
		if int(c) < len(r.perCat) {
			if p := r.perCat[c]; p != PolicyAuto {
				return p
			}
		}
		if r.global != PolicyAuto {
			return r.global
		}
	}
	return qwpSfDefaultPolicyFor(c)
}
