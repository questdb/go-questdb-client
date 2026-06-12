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
	"log"

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
	default:
		return CategoryUnknown
	}
}

// qwpSfDefaultPolicyFor is the spec default Policy for each Category,
// used when the user has not overridden the slot via builder option or
// connect-string. CategoryProtocolViolation and CategoryUnknown are
// forced HALT and the resolver enforces this independent of user
// overrides.
//
// Reasoning per the spec § "Default category → policy":
//   - SchemaMismatch / WriteError → DropAndContinue: replay reproduces
//     the same rejection; halting blocks unrelated tables on the same
//     connection.
//   - ParseError → Halt: almost certainly a client bug; halt preserves
//     the on-disk frames for postmortem.
//   - InternalError / SecurityError → Halt: catch-all server fault or
//     misconfig; loud failure wanted, no retryable bit available.
//   - ProtocolViolation / Unknown → Halt: connection is gone or the
//     status byte is not one we can interpret — never silently drop.
func qwpSfDefaultPolicyFor(c Category) Policy {
	switch c {
	case CategorySchemaMismatch, CategoryWriteError:
		return PolicyDropAndContinue
	case CategoryParseError, CategoryInternalError, CategorySecurityError:
		return PolicyHalt
	case CategoryProtocolViolation, CategoryUnknown:
		return PolicyHalt
	default:
		return PolicyHalt
	}
}

// qwpSfIsTerminalCloseCode reports whether a WebSocket close code is
// terminal — replaying the same bytes will produce the same close, so
// reconnect cannot fix it. Translates to CategoryProtocolViolation.
//
// Reserved codes 1004/1005/1006/1015 are deliberately not classified
// terminal: when they arrive in practice they signal abnormal
// disconnect rather than the server's reasoned rejection of payload
// bytes, so reconnect is the right reaction.
//
// Mirror of Java CursorWebSocketSendLoop.isTerminalCloseCode.
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
// CategoryProtocolViolation and CategoryUnknown bypass user overrides
// and always resolve to PolicyHalt — silently ignoring user-set
// non-Halt slots for those two categories.
type qwpSfPolicyResolver struct {
	resolver func(Category) Policy
	perCat   [numCategories]Policy
	global   Policy
}

// callResolver invokes the user-supplied resolver under a panic guard.
// The resolver runs on the receiver goroutine, so a panicking
// WithErrorPolicyResolver callback would otherwise crash the host. A
// panic is treated as a user bug: recover, log, and fall back to the
// spec default for the category. That default is always a concrete
// Halt / DropAndContinue (never PolicyAuto), so resolve's
// `!= PolicyAuto` check short-circuits the rest of the precedence chain
// — a broken resolver yields the safe spec policy rather than silently
// deferring to lower-precedence slots. A clean return is propagated
// verbatim, including PolicyAuto, which lets resolve fall through.
//
// Mirrors the handler panic guard in qwpSfErrorDispatcher.deliver.
func (r *qwpSfPolicyResolver) callResolver(c Category) (pol Policy) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[ERROR] qwp/sf: error policy resolver panicked on category %s: %v", c, rec)
			pol = qwpSfDefaultPolicyFor(c)
		}
	}()
	return r.resolver(c)
}

// resolve returns the Policy to apply for the given Category.
// PolicyAuto is never returned — every category resolves to a concrete
// Halt or DropAndContinue choice.
func (r *qwpSfPolicyResolver) resolve(c Category) Policy {
	// Forced HALT for unknown / protocol-violation regardless of user
	// configuration — silence forbidden, no DROP for the unintelligible.
	if c == CategoryProtocolViolation || c == CategoryUnknown {
		return PolicyHalt
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
