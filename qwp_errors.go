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
	"fmt"
	"strings"
	"time"
)

// QwpUpgradeRejectError is returned by qwpTransport.connect when the
// server completes the HTTP exchange with a non-101 status. Construction
// captures the response status and the failover-relevant headers so the
// reconnect loop can classify the host without re-parsing strings:
//
//   - StatusCode is the HTTP response status (e.g. 421 for a misdirected
//     request).
//   - Role is the trimmed X-QuestDB-Role header value (empty if absent).
//     The spec admits STANDALONE / PRIMARY / REPLICA / PRIMARY_CATCHUP;
//     unrecognised tokens are surfaced verbatim and classified by the
//     reconnect loop.
//   - Zone is the trimmed X-QuestDB-Zone header value (empty if absent).
//     Used to record host zone tier ahead of any successful upgrade.
//   - RetryAfter is the parsed Retry-After header in seconds (0 if absent
//     or unparseable). Hint only — the failover loop's outage budget
//     still bounds the wait.
//   - Body is up to qwpUpgradeBodySnippetCap bytes of the response body,
//     captured for error formatting. Truncation is signalled by a
//     trailing "…" in the Error() output.
type QwpUpgradeRejectError struct {
	StatusCode int
	Role       string
	Zone       string
	RetryAfter time.Duration
	Body       string
	// cause is the underlying websocket.Dial error. connect builds this
	// type only on a dial failure, so it is non-nil in practice. It is
	// the real reason the upgrade failed when StatusCode is 101: the
	// HTTP exchange reached the handshake-complete status but the
	// WebSocket upgrade itself was rejected (e.g. a bad
	// Sec-WebSocket-Accept). Exposed via Unwrap.
	cause error
}

// qwpUpgradeBodySnippetCap bounds how many response-body bytes the
// transport captures into QwpUpgradeRejectError.Body. Keeps error
// messages bounded when a misconfigured server returns a large HTML
// payload on a 4xx/5xx upgrade rejection.
const qwpUpgradeBodySnippetCap = 512

// Error implements the error interface. The format leads with the
// HTTP status and tag (Role / Zone / Retry-After) so the failover
// loop can include the message verbatim in its budget-exhaustion
// report without losing the structured fields.
func (e *QwpUpgradeRejectError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "qwp: upgrade rejected with HTTP %d", e.StatusCode)
	if e.Role != "" {
		fmt.Fprintf(&b, " (role=%s)", e.Role)
	}
	if e.Zone != "" {
		fmt.Fprintf(&b, " (zone=%s)", e.Zone)
	}
	if e.RetryAfter > 0 {
		fmt.Fprintf(&b, " (retry-after=%s)", e.RetryAfter)
	}
	if e.Body != "" {
		fmt.Fprintf(&b, ": %s", e.Body)
	}
	// A 101 status means the HTTP handshake completed but the WebSocket
	// upgrade was still rejected, so "rejected with HTTP 101" is
	// misleading on its own — surface the underlying dial error that
	// actually explains the failure.
	if e.StatusCode == 101 && e.cause != nil {
		fmt.Fprintf(&b, ": %v", e.cause)
	}
	return b.String()
}

// QwpDurableAckMismatchError is returned by connect() when the client requested
// durable-ack (request_durable_ack=on) but the endpoint did not advertise support
// (X-QWP-Durable-Ack: enabled) on the upgrade — typically because it is a replica,
// not a replication primary. It is terminal: the client must not fall back to
// OK-only trimming, which would drop data the caller believes durable. Surfaces
// to the producer as a *SenderError of category PROTOCOL_VIOLATION on every
// connect path (initial-off, initial-sync, and async reconnect); the underlying
// *QwpDurableAckMismatchError stays reachable through that SenderError via
// errors.As.
type QwpDurableAckMismatchError struct {
	Endpoint string
}

func (e *QwpDurableAckMismatchError) Error() string {
	return fmt.Sprintf("qwp: durable-ack requested but endpoint %q did not advertise support "+
		"(X-QWP-Durable-Ack); a replication primary is required", e.Endpoint)
}

// Unwrap returns the underlying websocket.Dial error so errors.Is /
// errors.As can reach the transport-level cause. Classification keys
// off StatusCode via a top-level type assertion, so unwrapping does
// not affect host-role classification.
func (e *QwpUpgradeRejectError) Unwrap() error {
	return e.cause
}

// IsRoleReject reports whether the upgrade was rejected with the
// failover-spec "topology hint" combination: HTTP 421 plus a non-empty
// X-QuestDB-Role header. The reconnect loop classifies the host as
// TransientReject (Role == PRIMARY_CATCHUP, case-insensitive) or
// TopologyReject (any other non-empty role).
func (e *QwpUpgradeRejectError) IsRoleReject() bool {
	return e.StatusCode == 421 && e.Role != ""
}

// IsCatchupRole reports whether the role tag is PRIMARY_CATCHUP
// (case-insensitive). Only meaningful when IsRoleReject() is true.
func (e *QwpUpgradeRejectError) IsCatchupRole() bool {
	return strings.EqualFold(e.Role, "PRIMARY_CATCHUP")
}

// qwpStatusName returns a human-readable name for a QWP status code.
// Used by (*SenderError).Error() to format the wire-byte component of
// rejection messages.
func qwpStatusName(status QwpStatusCode) string {
	switch status {
	case QwpStatusOK:
		return "OK"
	case QwpStatusDurableAck:
		return "DURABLE_ACK"
	case QwpStatusSchemaMismatch:
		return "SCHEMA_MISMATCH"
	case QwpStatusParseError:
		return "PARSE_ERROR"
	case QwpStatusInternalError:
		return "INTERNAL_ERROR"
	case QwpStatusSecurityError:
		return "SECURITY_ERROR"
	case QwpStatusWriteError:
		return "WRITE_ERROR"
	case QwpStatusNotWritable:
		return "NOT_WRITABLE"
	case qwpStatusCancelled:
		return "CANCELLED"
	case qwpStatusLimitExceeded:
		return "LIMIT_EXCEEDED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", status)
	}
}

// parseAckErrorPayload extracts the status code, cumulative sequence
// number, and server error message from a non-OK ACK frame. Used by
// the SF send loop's receiver to assemble a *SenderError with the
// surrounding FSN-span context.
//
// Precondition: data has already been validated by readAck, which
// guarantees the layout invariants documented on readAck.
func parseAckErrorPayload(data []byte) (status QwpStatusCode, seq int64, msg string) {
	status = QwpStatusCode(data[0])
	if status == QwpStatusOK || status == QwpStatusDurableAck {
		return status, 0, ""
	}
	return status, parseAckSequence(data), parseAckError(data)
}
