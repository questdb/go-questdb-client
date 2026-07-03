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
	"testing"

	"github.com/coder/websocket"
)

func TestQwpSfClassify(t *testing.T) {
	tests := []struct {
		status QwpStatusCode
		want   Category
	}{
		{QwpStatusSchemaMismatch, CategorySchemaMismatch},
		{QwpStatusParseError, CategoryParseError},
		{QwpStatusInternalError, CategoryInternalError},
		{QwpStatusSecurityError, CategorySecurityError},
		{QwpStatusWriteError, CategoryWriteError},
		{QwpStatusNotWritable, CategoryNotWritable},
		// OK / DurableAck never reach classify in production but they
		// fall through to Unknown defensively.
		{QwpStatusOK, CategoryUnknown},
		{QwpStatusDurableAck, CategoryUnknown},
		// Forward-compat: server adds a new status byte we do not
		// understand.
		{QwpStatusCode(0xFE), CategoryUnknown},
	}
	for _, tc := range tests {
		if got := qwpSfClassify(tc.status); got != tc.want {
			t.Errorf("qwpSfClassify(0x%02X) = %s, want %s",
				byte(tc.status), got, tc.want)
		}
	}
}

func TestQwpSfDefaultPolicyFor(t *testing.T) {
	tests := []struct {
		c    Category
		want Policy
	}{
		{CategoryWriteError, PolicyRetriable},
		{CategoryInternalError, PolicyRetriable},
		{CategoryUnknown, PolicyRetriable},
		{CategoryNotWritable, PolicyRetriableOther},
		{CategorySchemaMismatch, PolicyTerminal},
		{CategoryParseError, PolicyTerminal},
		{CategorySecurityError, PolicyTerminal},
		{CategoryProtocolViolation, PolicyTerminal},
	}
	for _, tc := range tests {
		if got := qwpSfDefaultPolicyFor(tc.c); got != tc.want {
			t.Errorf("qwpSfDefaultPolicyFor(%s) = %s, want %s",
				tc.c, got, tc.want)
		}
	}
}

func TestQwpSfIsTerminalCloseCode(t *testing.T) {
	tests := []struct {
		code websocket.StatusCode
		want bool
		name string
	}{
		{websocket.StatusProtocolError, true, "PROTOCOL_ERROR"},
		{websocket.StatusUnsupportedData, true, "UNSUPPORTED_DATA"},
		{websocket.StatusInvalidFramePayloadData, true, "INVALID_PAYLOAD_DATA"},
		{websocket.StatusPolicyViolation, true, "POLICY_VIOLATION"},
		{websocket.StatusMessageTooBig, true, "MESSAGE_TOO_BIG"},
		{websocket.StatusMandatoryExtension, true, "MANDATORY_EXTENSION"},
		{websocket.StatusNormalClosure, false, "NormalClosure"},
		{websocket.StatusGoingAway, false, "GoingAway"},
		{websocket.StatusAbnormalClosure, false, "AbnormalClosure"},
		{websocket.StatusInternalError, false, "InternalError"},
		{websocket.StatusCode(-1), false, "non-CloseError sentinel"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := qwpSfIsTerminalCloseCode(tc.code); got != tc.want {
				t.Errorf("qwpSfIsTerminalCloseCode(%d) = %v, want %v",
					tc.code, got, tc.want)
			}
		})
	}
}

// TestQwpSfPolicyResolverPrecedence checks the four-layer override
// stack: programmatic resolver > per-category map > global default >
// spec default. ProtocolViolation and Unknown ignore overrides.
func TestQwpSfPolicyResolverPrecedence(t *testing.T) {
	t.Run("nil resolver falls through to spec defaults", func(t *testing.T) {
		var r *qwpSfPolicyResolver
		if got := r.resolve(CategoryWriteError); got != PolicyRetriable {
			t.Errorf("nil resolver WriteError = %s, want Retriable", got)
		}
	})

	t.Run("zero resolver falls through to spec defaults", func(t *testing.T) {
		r := &qwpSfPolicyResolver{}
		if got := r.resolve(CategoryParseError); got != PolicyTerminal {
			t.Errorf("zero resolver ParseError = %s, want Terminal", got)
		}
	})

	t.Run("global override beats spec default", func(t *testing.T) {
		r := &qwpSfPolicyResolver{global: PolicyTerminal}
		if got := r.resolve(CategoryWriteError); got != PolicyTerminal {
			t.Errorf("global=Terminal WriteError = %s, want Terminal", got)
		}
	})

	t.Run("per-category beats global", func(t *testing.T) {
		r := &qwpSfPolicyResolver{global: PolicyTerminal}
		r.perCat[CategorySchemaMismatch] = PolicyRetriable
		if got := r.resolve(CategorySchemaMismatch); got != PolicyRetriable {
			t.Errorf("per-cat beats global = %s, want Retriable", got)
		}
	})

	t.Run("programmatic resolver beats per-category", func(t *testing.T) {
		r := &qwpSfPolicyResolver{}
		r.perCat[CategoryParseError] = PolicyRetriable
		r.resolver = func(c Category) Policy {
			if c == CategoryParseError {
				return PolicyTerminal
			}
			return PolicyAuto
		}
		if got := r.resolve(CategoryParseError); got != PolicyTerminal {
			t.Errorf("programmatic beats per-cat = %s, want Terminal", got)
		}
	})

	t.Run("programmatic resolver returning Auto falls through", func(t *testing.T) {
		r := &qwpSfPolicyResolver{}
		r.perCat[CategoryWriteError] = PolicyTerminal
		r.resolver = func(Category) Policy { return PolicyAuto }
		if got := r.resolve(CategoryWriteError); got != PolicyTerminal {
			t.Errorf("programmatic Auto + per-cat=Halt = %s, want Halt", got)
		}
	})

	t.Run("panicking resolver falls back to spec default", func(t *testing.T) {
		// A per-category override is set, but the panic short-circuits
		// to the spec default rather than falling through to it: a
		// broken resolver must not silently defer to lower-precedence
		// slots. WriteError's spec default is Retriable.
		r := &qwpSfPolicyResolver{}
		r.perCat[CategoryWriteError] = PolicyTerminal
		r.resolver = func(Category) Policy { panic("boom") }
		if got := r.resolve(CategoryWriteError); got != PolicyRetriable {
			t.Errorf("panicking resolver WriteError = %s, want Retriable (spec default)", got)
		}
	})

	t.Run("panicking resolver does not crash the caller", func(t *testing.T) {
		// The receiver goroutine invokes resolve directly; a panic that
		// escapes would take down the host process. ParseError's spec
		// default is Terminal.
		r := &qwpSfPolicyResolver{}
		r.resolver = func(Category) Policy { panic("boom") }
		if got := r.resolve(CategoryParseError); got != PolicyTerminal {
			t.Errorf("panicking resolver ParseError = %s, want Terminal (spec default)", got)
		}
	})

	t.Run("ProtocolViolation forced Terminal regardless", func(t *testing.T) {
		r := &qwpSfPolicyResolver{global: PolicyRetriable}
		r.perCat[CategoryProtocolViolation] = PolicyRetriable
		r.resolver = func(Category) Policy { return PolicyRetriable }
		if got := r.resolve(CategoryProtocolViolation); got != PolicyTerminal {
			t.Errorf("ProtocolViolation = %s, want Terminal (forced)", got)
		}
	})

	t.Run("Unknown forced Retriable regardless", func(t *testing.T) {
		// Fail open: a status byte from a newer server must degrade to
		// retry, not to a dead sender — even a user-configured Terminal
		// is ignored.
		r := &qwpSfPolicyResolver{global: PolicyTerminal}
		r.perCat[CategoryUnknown] = PolicyTerminal
		r.resolver = func(Category) Policy { return PolicyTerminal }
		if got := r.resolve(CategoryUnknown); got != PolicyRetriable {
			t.Errorf("Unknown = %s, want Retriable (forced fail-open)", got)
		}
	})
}
