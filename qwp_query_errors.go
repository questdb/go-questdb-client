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
)

// QwpQueryError is a server-side error reported during query egress. It
// corresponds to a QUERY_ERROR frame (msg_kind 0x13) and is distinct from
// QwpError, which carries ingress ACK status. CANCELLED and LIMIT_EXCEEDED
// are egress-specific statuses that surface here.
type QwpQueryError struct {
	// RequestId correlates the error with the query that produced it.
	RequestId int64

	// Status is the server-reported egress status byte (e.g.
	// qwpStatusCancelled, qwpStatusLimitExceeded, QwpStatusParseError).
	Status QwpStatusCode

	// Message is the server-supplied UTF-8 description, or empty if the
	// server sent a zero-length message.
	Message string
}

// Error implements the error interface.
func (e *QwpQueryError) Error() string {
	name := qwpStatusName(e.Status)
	if e.Message != "" {
		return fmt.Sprintf("qwp: query error %s (0x%02X): %s",
			name, byte(e.Status), e.Message)
	}
	return fmt.Sprintf("qwp: query error %s (0x%02X)", name, byte(e.Status))
}

// QwpRoleMismatchError is returned by QwpQueryClient construction when
// none of the configured endpoints satisfies the target= role filter.
// The connect walk records the most-recently-observed SERVER_INFO,
// whether any endpoint negotiated v1, and the last underlying transport
// failure so callers can distinguish four failure shapes: "no primary
// available" (LastObserved non-nil; at least one v2 endpoint reported a
// different role), "OSS-only cluster" (SawV1Mismatch true; at least
// one endpoint negotiated v1 and cannot report a role), "all endpoints
// unreachable" (LastTransportError non-nil with both other fields
// zero), and combinations of the above (e.g. one endpoint dialled but
// reported the wrong role while another refused the connection).
type QwpRoleMismatchError struct {
	// Target is the requested role filter ("any", "primary", "replica").
	// Stored as a string for human-readable error formatting; the
	// internal qwpTargetFilter enum is mapped to its name on
	// construction.
	Target string

	// LastObserved is the SERVER_INFO of the most recent endpoint the
	// connect walk reached and that returned a role this filter would
	// reject. Nil if every endpoint refused the connection or only
	// v1 endpoints responded.
	LastObserved *QwpServerInfo

	// SawV1Mismatch is true when at least one endpoint negotiated QWP
	// v1 (no SERVER_INFO frame, role unknown) and was therefore skipped
	// because the target filter requires a role guarantee. Lets callers
	// detect "the cluster is up but it's OSS / v1 and can't supply a
	// role" without parsing the error message.
	SawV1Mismatch bool

	// LastTransportError is the most recent transport-level failure the
	// connect walk hit (TCP/TLS dial, WebSocket upgrade, SERVER_INFO
	// timeout). Populated when at least one endpoint failed before
	// reaching the role-filter step. Nil when every endpoint dialled
	// cleanly but failed only the role / v1 checks. Available via
	// errors.Is / errors.As through Unwrap.
	LastTransportError error

	// Endpoints lists every endpoint the walk attempted, in the order
	// they were tried. Useful for diagnosing why none of them matched.
	Endpoints []string
}

// Error implements the error interface.
func (e *QwpRoleMismatchError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "qwp query: no endpoint matches target=%s", e.Target)
	if e.LastObserved != nil {
		fmt.Fprintf(&b, "; last observed role=%s", e.LastObserved.RoleName())
		if e.LastObserved.NodeId != "" {
			fmt.Fprintf(&b, " on node %q", e.LastObserved.NodeId)
		}
	}
	if e.SawV1Mismatch {
		b.WriteString(
			"; at least one endpoint negotiated v1 and cannot supply a role")
	}
	if e.LastTransportError != nil {
		fmt.Fprintf(&b, "; last transport error: %v", e.LastTransportError)
	}
	if len(e.Endpoints) > 0 {
		fmt.Fprintf(&b, " (tried: %s)", strings.Join(e.Endpoints, ", "))
	}
	return b.String()
}

// Unwrap exposes the underlying transport failure (if any) to
// errors.Is / errors.As so callers can match on both the role-mismatch
// shape and the specific dial / upgrade failure that contributed to it.
// Returns nil when every endpoint reached the role-filter step.
func (e *QwpRoleMismatchError) Unwrap() error {
	return e.LastTransportError
}

// QwpFailoverReset is yielded as a non-fatal error by *QwpQuery.Batches
// when the I/O layer detects a transport-terminal failure mid-query
// and successfully reconnects to another role-matching endpoint to
// replay the request. Subsequent batches arrive with batch_seq
// restarting at 0 on the new node.
//
// Consumer pattern: detect via errors.As, discard any rows accumulated
// from the prior connection, and continue iterating. Consumers that
// don't accumulate (simple "print rows" loops) can ignore the error
// and just continue. Treating it as terminal is also safe — the user
// gets a clear human-readable error and the iterator's deferred
// cleanup tears down the dying generation.
//
// Surfaced only on the Query (SELECT) path. Exec never returns this:
// with replay_exec=off (the default) a transport drop yields the raw
// transport error without reconnecting — so a non-idempotent
// statement the server may already have applied is not silently
// re-executed — and with replay_exec=on Exec replays transparently
// and consumes the reset internally.
type QwpFailoverReset struct {
	// NewNode is the SERVER_INFO of the endpoint the client just
	// rebound to, or nil if the new connection negotiated v1 (no
	// SERVER_INFO emitted).
	NewNode *QwpServerInfo

	// Attempt is the 1-based replay attempt counter. Attempt=1 means
	// the failure happened during the original submission and the
	// first reconnect succeeded; Attempt=N means N transport failures
	// occurred before this reset.
	Attempt int

	// LastError is the underlying transport-terminal error that
	// triggered this reset. Useful for diagnostics; nil only on the
	// rare case of a server-initiated reconnect with no transport
	// fault.
	LastError error
}

// Error implements the error interface.
func (e *QwpFailoverReset) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "qwp query: failover reset (attempt %d)", e.Attempt)
	if e.NewNode != nil {
		fmt.Fprintf(&b, " to %s/%s", e.NewNode.NodeId, e.NewNode.RoleName())
	}
	if e.LastError != nil {
		fmt.Fprintf(&b, ": %v", e.LastError)
	}
	return b.String()
}

// Unwrap exposes the underlying transport error to errors.Is /
// errors.As so callers can match on both the reset event and the
// specific transport failure that triggered it.
func (e *QwpFailoverReset) Unwrap() error {
	return e.LastError
}

// QwpFailoverExhaustedError surfaces from *QwpQuery.Batches and
// (*QwpQueryClient).Exec when the failover budget
// (failover_max_attempts) has been consumed without producing a
// successful query completion. Carries the attempt count and the most
// recent transport-terminal error so callers can distinguish "the
// initial attempt failed" from "every retry within the budget also
// failed", and surface a useful diagnostic without parsing the
// underlying message. Mirrors Java's onError(STATUS_INTERNAL_ERROR,
// "transport failure after N execute attempts ...") shape from
// QwpQueryClient.executeOnce.
type QwpFailoverExhaustedError struct {
	// Attempts is the number of execute attempts (initial submission
	// plus all replays) that failed before the budget was reached.
	// Always equal to the configured failover_max_attempts when the
	// error is constructed by the session orchestrator; preserved as
	// a separate field so a caller-side log line does not need to
	// re-derive it from configuration.
	Attempts int

	// LastError is the most recent transport-terminal error that
	// pushed the count up to the budget. Non-nil. Available via
	// errors.Is / errors.As through Unwrap so callers can match on
	// both the exhaustion shape and the specific underlying cause.
	LastError error
}

// Error implements the error interface.
func (e *QwpFailoverExhaustedError) Error() string {
	failovers := e.Attempts - 1
	if failovers < 0 {
		failovers = 0
	}
	var b strings.Builder
	fmt.Fprintf(&b, "qwp query: failover exhausted after %d execute attempt",
		e.Attempts)
	if e.Attempts != 1 {
		b.WriteByte('s')
	}
	fmt.Fprintf(&b, " (%d failover reconnect", failovers)
	if failovers != 1 {
		b.WriteByte('s')
	}
	b.WriteByte(')')
	if e.LastError != nil {
		fmt.Fprintf(&b, "; last error: %v", e.LastError)
	}
	return b.String()
}

// Unwrap exposes the underlying transport error to errors.Is /
// errors.As so callers can match on both the exhaustion shape and the
// specific transport failure that triggered the final retry.
func (e *QwpFailoverExhaustedError) Unwrap() error {
	return e.LastError
}
