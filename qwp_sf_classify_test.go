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
		{CategorySchemaMismatch, PolicyDropAndContinue},
		{CategoryWriteError, PolicyDropAndContinue},
		{CategoryParseError, PolicyHalt},
		{CategoryInternalError, PolicyHalt},
		{CategorySecurityError, PolicyHalt},
		{CategoryProtocolViolation, PolicyHalt},
		{CategoryUnknown, PolicyHalt},
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
		if got := r.resolve(CategorySchemaMismatch); got != PolicyDropAndContinue {
			t.Errorf("nil resolver SchemaMismatch = %s, want DropAndContinue", got)
		}
	})

	t.Run("zero resolver falls through to spec defaults", func(t *testing.T) {
		r := &qwpSfPolicyResolver{}
		if got := r.resolve(CategoryParseError); got != PolicyHalt {
			t.Errorf("zero resolver ParseError = %s, want Halt", got)
		}
	})

	t.Run("global override beats spec default", func(t *testing.T) {
		r := &qwpSfPolicyResolver{global: PolicyHalt}
		if got := r.resolve(CategorySchemaMismatch); got != PolicyHalt {
			t.Errorf("global=Halt SchemaMismatch = %s, want Halt", got)
		}
	})

	t.Run("per-category beats global", func(t *testing.T) {
		r := &qwpSfPolicyResolver{global: PolicyHalt}
		r.perCat[CategorySchemaMismatch] = PolicyDropAndContinue
		if got := r.resolve(CategorySchemaMismatch); got != PolicyDropAndContinue {
			t.Errorf("per-cat beats global = %s, want DropAndContinue", got)
		}
	})

	t.Run("programmatic resolver beats per-category", func(t *testing.T) {
		r := &qwpSfPolicyResolver{}
		r.perCat[CategoryParseError] = PolicyDropAndContinue
		r.resolver = func(c Category) Policy {
			if c == CategoryParseError {
				return PolicyHalt
			}
			return PolicyAuto
		}
		if got := r.resolve(CategoryParseError); got != PolicyHalt {
			t.Errorf("programmatic beats per-cat = %s, want Halt", got)
		}
	})

	t.Run("programmatic resolver returning Auto falls through", func(t *testing.T) {
		r := &qwpSfPolicyResolver{}
		r.perCat[CategoryWriteError] = PolicyHalt
		r.resolver = func(Category) Policy { return PolicyAuto }
		if got := r.resolve(CategoryWriteError); got != PolicyHalt {
			t.Errorf("programmatic Auto + per-cat=Halt = %s, want Halt", got)
		}
	})

	t.Run("ProtocolViolation forced Halt regardless", func(t *testing.T) {
		r := &qwpSfPolicyResolver{global: PolicyDropAndContinue}
		r.perCat[CategoryProtocolViolation] = PolicyDropAndContinue
		r.resolver = func(Category) Policy { return PolicyDropAndContinue }
		if got := r.resolve(CategoryProtocolViolation); got != PolicyHalt {
			t.Errorf("ProtocolViolation = %s, want Halt (forced)", got)
		}
	})

	t.Run("Unknown forced Halt regardless", func(t *testing.T) {
		r := &qwpSfPolicyResolver{global: PolicyDropAndContinue}
		r.perCat[CategoryUnknown] = PolicyDropAndContinue
		if got := r.resolve(CategoryUnknown); got != PolicyHalt {
			t.Errorf("Unknown = %s, want Halt (forced)", got)
		}
	})
}
