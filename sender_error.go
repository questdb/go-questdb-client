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

// Package questdb provides the QuestDB ingestion clients.
//
// SenderError is the QWP cursor-SF server-error payload. It surfaces in
// two ways. Asynchronously, to a registered SenderErrorHandler:
//
//	opts := []questdb.LineSenderOption{
//		questdb.WithQwp(),
//		questdb.WithErrorHandler(func(e *questdb.SenderError) {
//			log.Printf("server rejected FSN [%d,%d]: %v", e.FromFsn, e.ToFsn, e)
//		}),
//	}
//
// And synchronously, on the next producer-thread API call after a
// terminal rejection has been latched:
//
//	if err := s.Flush(ctx); err != nil {
//		var se *questdb.SenderError
//		if errors.As(err, &se) {
//			// unpack se.Category, se.ServerMessage, se.FromFsn, ...
//		}
//	}
//
// Both paths deliver the same payload. The producer-side typed error is
// the FSN's-eye-view of "what was rejected"; the async handler carries
// retriable rejections too (informational — the batch is replayed, not
// dropped).
package questdb

import (
	"fmt"
	"time"
)

// Category classifies a QWP server-side rejection. Categories align 1:1
// with stable wire status bytes (SchemaMismatch / ParseError /
// InternalError / SecurityError / WriteError / NotWritable) plus
// ProtocolViolation (client-latched, no wire status byte: poison-frame
// escalation, 404/426 upgrade rejection, durable-ack capability
// mismatch) and Unknown (forward-compat for new server status bytes).
type Category byte

const (
	// CategoryUnknown is the zero value and the fallback for any
	// status byte the client does not recognize. Forced RETRIABLE
	// (fail open): a status byte from a newer server must degrade to
	// retry, not to a dead sender.
	CategoryUnknown Category = iota
	// CategorySchemaMismatch: column type incompatible with existing
	// table, missing column, NOT NULL violation, no such table.
	// Wire status 0x03.
	CategorySchemaMismatch
	// CategoryParseError: QWP-level malformed payload — likely a
	// client bug. Wire status 0x05.
	CategoryParseError
	// CategoryInternalError: catch-all server fault (CairoException
	// isCritical, unhandled Throwable). Wire status 0x06.
	CategoryInternalError
	// CategorySecurityError: authentication or authorization failure.
	// Wire status 0x08, also produced by 401/403 on the WebSocket
	// upgrade. Mid-stream it can only mean ACL denial on a writable
	// node — read-only refusals arrive as role-change closes.
	CategorySecurityError
	// CategoryWriteError: non-critical Cairo error, table not
	// accepting writes. Wire status 0x09.
	CategoryWriteError
	// CategoryNotWritable: the node cannot serve writes at all right
	// now (read-only replica, demoting primary). Wire status 0x0C —
	// reserved: current servers signal this state with a
	// reconnect-eligible close instead of a mid-stream NACK, so the
	// category is mapped for forward compatibility with servers that
	// NACK it explicitly.
	CategoryNotWritable
	// CategoryProtocolViolation: a frame the server (or an
	// intermediary) deterministically rejects — the poison-frame
	// detector observed the same head-of-line frame fail
	// max_frame_rejections consecutive times with no ack progress,
	// over an episode at least the reconnect max-duration long — or a
	// 404/426 upgrade rejection / durable-ack capability mismatch.
	// Forced TERMINAL.
	CategoryProtocolViolation

	numCategories // sentinel: must be last
)

// String returns the canonical name of the category. Stable across
// releases — safe to log and grep.
func (c Category) String() string {
	switch c {
	case CategoryUnknown:
		return "UNKNOWN"
	case CategorySchemaMismatch:
		return "SCHEMA_MISMATCH"
	case CategoryParseError:
		return "PARSE_ERROR"
	case CategoryInternalError:
		return "INTERNAL_ERROR"
	case CategorySecurityError:
		return "SECURITY_ERROR"
	case CategoryWriteError:
		return "WRITE_ERROR"
	case CategoryNotWritable:
		return "NOT_WRITABLE"
	case CategoryProtocolViolation:
		return "PROTOCOL_VIOLATION"
	default:
		return fmt.Sprintf("Category(%d)", byte(c))
	}
}

// Policy is the action the SF send loop took when a category fired.
// Resolution precedence (highest first): builder errorPolicyResolver →
// builder per-category errorPolicy → connect-string per-category
// on_*_error → connect-string global on_server_error → spec defaults.
//
// There is no drop policy by design: the client never silently
// discards data. A rejected batch is either replayed (PolicyRetriable
// / PolicyRetriableOther) or halts the sender loudly with the bytes
// preserved on disk (PolicyTerminal).
//
// CategoryProtocolViolation is forced TERMINAL and CategoryUnknown is
// forced RETRIABLE (fail open: a status byte from a newer server must
// degrade to retry, not to a dead sender); user overrides for those
// categories are ignored.
type Policy byte

const (
	// PolicyAuto is the zero value, used as a sentinel meaning
	// "fall through to the next layer of resolution". Never appears
	// on a delivered SenderError — the loop always resolves to a
	// concrete policy before building the error.
	PolicyAuto Policy = iota
	// PolicyRetriable: recycle the connection and replay from the
	// store-and-forward log — reconnect with capped exponential
	// backoff and reposition at ackedFsn+1. No data is dropped and
	// the producer keeps writing; delivery through SenderErrorHandler
	// is informational. A frame that keeps being rejected with no ack
	// progress escalates to PolicyTerminal via the poison-frame
	// detector (max_frame_rejections).
	PolicyRetriable
	// PolicyRetriableOther: same recycle-and-replay semantics as
	// PolicyRetriable, used for a rejection that says this node cannot
	// serve writes at all (read-only replica / demoting primary). The
	// reconnect demotes the current node like any transport failure, so
	// on a multi-endpoint cluster it naturally repicks another endpoint
	// first.
	PolicyRetriableOther
	// PolicyTerminal: latch the error as terminal. The next
	// producer-thread API call returns the SenderError; the sender
	// does not drain further until the caller closes and rebuilds
	// it. The rejected bytes remain in the store-and-forward log on
	// disk — nothing is silently discarded.
	PolicyTerminal
)

// String returns the canonical name of the policy. Stable across
// releases — safe to log and grep.
func (p Policy) String() string {
	switch p {
	case PolicyAuto:
		return "AUTO"
	case PolicyRetriable:
		return "RETRIABLE"
	case PolicyRetriableOther:
		return "RETRIABLE_OTHER"
	case PolicyTerminal:
		return "TERMINAL"
	default:
		return fmt.Sprintf("Policy(%d)", byte(p))
	}
}

// Sentinel field values on SenderError. Use these instead of literal
// numbers so cross-language users see the same intent.
const (
	// NoStatusByte signals SenderError carries no QWP status byte —
	// CategoryProtocolViolation does not come from a server status
	// frame. Stored as int because Go has no nullable byte.
	NoStatusByte = -1
	// NoMessageSequence signals SenderError carries no per-frame
	// sequence number — same case as NoStatusByte.
	NoMessageSequence int64 = -1
)

// SenderError is the immutable description of a server-side rejection
// of an asynchronously published QWP batch. It is delivered to user
// code via the registered SenderErrorHandler (async) and as the typed
// error returned from the next producer-thread API call after a
// terminal latch (sync). Both paths carry the same payload.
//
// SenderError implements the error interface, so it can be passed
// directly through error-returning APIs and unwrapped via errors.As:
//
//	var se *questdb.SenderError
//	if errors.As(err, &se) { ... }
//
// The [FromFsn, ToFsn] span is the load-bearing correlation key —
// join it to whatever the producer logged alongside the value
// returned by FlushAndGetSequence to identify the rejected data.
type SenderError struct {
	// Category is the rejection classification. The recommended
	// switch target.
	Category Category

	// AppliedPolicy is what the loop actually did about the
	// rejection — PolicyRetriable / PolicyRetriableOther means the
	// connection was recycled and the batch is replayed (nothing
	// dropped); PolicyTerminal means a terminal latch is in place
	// with the bytes preserved in the store-and-forward log.
	AppliedPolicy Policy

	// ServerStatusByte is the raw QWP status byte (e.g. 0x03 for
	// SCHEMA_MISMATCH). Set to NoStatusByte for
	// CategoryProtocolViolation. Stored as int to allow the
	// sentinel.
	ServerStatusByte int

	// ServerMessage is the human-readable description provided by
	// the server (≤1024 UTF-8 bytes for QWP error frames, or the
	// WebSocket close reason for protocol violations). Empty if
	// the server provided no text.
	ServerMessage string

	// MessageSequence is the server's per-frame messageSequence as
	// mirrored back in the rejection frame, used for cross-team
	// debugging and to correlate against server-side logs. Set to
	// NoMessageSequence for CategoryProtocolViolation.
	MessageSequence int64

	// FromFsn is the inclusive lower bound of the FSN span for the
	// rejected batch — the correlation key for joining against
	// FlushAndGetSequence values on the producer side.
	FromFsn int64

	// ToFsn is the inclusive upper bound of the FSN span for the
	// rejected batch.
	ToFsn int64

	// TableName is the rejected table name, when the server
	// attributed the error to a single table. Empty string means
	// "unknown" or "multi-table batch" — the server does not
	// attribute multi-table batch errors today.
	TableName string

	// DetectedAt is the wall-clock-independent receipt time on the
	// I/O goroutine. Use for ordering and ops timelines, not for
	// correlation.
	DetectedAt time.Time

	// cause is the underlying transport-level error this SenderError was
	// synthesized from, when there is one — a WebSocket upgrade rejection
	// (*QwpUpgradeRejectError) or a durable-ack mismatch
	// (*QwpDurableAckMismatchError). nil for server-ACK rejections, which
	// carry no wrapped cause. Exposed via Unwrap so errors.As can still
	// reach the typed cause while callers switch on the stable Category.
	cause error
}

// Unwrap returns the underlying transport-level cause when this SenderError
// was synthesized from one (a WebSocket upgrade rejection or a durable-ack
// mismatch), else nil. It lets errors.As reach the typed cause — e.g.
// var m *QwpDurableAckMismatchError; errors.As(err, &m) — while the
// SenderError still carries the release-stable Category the API promises.
func (e *SenderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Error implements the error interface. The format is stable enough
// to grep on but is intended for human consumption; programmatic
// callers should switch on Category, ServerStatusByte, etc.
func (e *SenderError) Error() string {
	if e == nil {
		return "<nil *SenderError>"
	}
	var sb []byte
	sb = append(sb, "qwp: server rejected batch: "...)
	sb = append(sb, e.Category.String()...)
	if e.ServerStatusByte != NoStatusByte {
		sb = append(sb, fmt.Sprintf(" (status=0x%02X %s)",
			byte(e.ServerStatusByte),
			qwpStatusName(QwpStatusCode(e.ServerStatusByte)))...)
	}
	sb = append(sb, fmt.Sprintf(" policy=%s fsn=[%d,%d]",
		e.AppliedPolicy, e.FromFsn, e.ToFsn)...)
	if e.TableName != "" {
		sb = append(sb, fmt.Sprintf(" table=%s", e.TableName)...)
	}
	if e.MessageSequence != NoMessageSequence {
		sb = append(sb, fmt.Sprintf(" seq=%d", e.MessageSequence)...)
	}
	if e.ServerMessage != "" {
		sb = append(sb, " — "...)
		sb = append(sb, e.ServerMessage...)
	}
	return string(sb)
}

// ----------------------------------------------------------------------
// Deprecated v4.2.0 compatibility shim. Delete this whole block in
// v4.4.0 (one minor after the SenderError replacement landed in
// v4.3.0): the QwpError type, its Error method, and the
// (*SenderError).As bridge below exist only so source written against
// v4.2.0's QwpError keeps compiling across the upgrade.
// ----------------------------------------------------------------------

// QwpError was the v4.2.0 QWP server-rejection payload returned from
// Flush and delivered to the async error path. v4.3.0 replaced it with
// SenderError, which additionally carries the [FromFsn, ToFsn]
// correlation span, the applied Policy, table attribution, and a
// release-stable Category.
//
// Deprecated: use SenderError. This shim only keeps v4.2.0 source
// compiling and is scheduled for removal in v4.4.0. The
// (*SenderError).As bridge keeps the historical pattern working:
//
//	var qwpErr *questdb.QwpError
//	if errors.As(err, &qwpErr) { /* still populated, from *SenderError */ }
//
// A type switch `case *questdb.QwpError:` will NOT match anymore —
// Flush now returns *SenderError — so switch on *SenderError (or its
// Category) instead. Field mapping from the old payload:
//
//	QwpError.Status   ← SenderError.ServerStatusByte (Category for the name)
//	QwpError.Sequence ← SenderError.MessageSequence
//	QwpError.Message  ← SenderError.ServerMessage
type QwpError struct {
	// Status is the raw QWP status byte from the server's ACK
	// rejection. Zero (the QwpStatusOK byte) when the underlying
	// SenderError is a CategoryProtocolViolation, which v4.2.0 never
	// surfaced through this type.
	//
	// Deprecated: read SenderError.ServerStatusByte / .Category.
	Status QwpStatusCode

	// Sequence is the server's per-frame message sequence, mirrored
	// back in the rejection frame.
	//
	// Deprecated: read SenderError.MessageSequence.
	Sequence int64

	// Message is the server-supplied error description, or empty if
	// the server sent no text.
	//
	// Deprecated: read SenderError.ServerMessage.
	Message string
}

// Error implements the error interface, preserving the exact v4.2.0
// message format so adopters that grep their logs see no change.
//
// Deprecated: use SenderError.
func (e *QwpError) Error() string {
	name := qwpStatusName(e.Status)
	if e.Message != "" {
		return fmt.Sprintf("qwp: server error %s (0x%02X): %s",
			name, byte(e.Status), e.Message)
	}
	return fmt.Sprintf("qwp: server error %s (0x%02X)",
		name, byte(e.Status))
}

// As bridges the deprecated *QwpError shim onto the SenderError
// payload so the historical errors.As(err, &qwpErr) pattern keeps
// working after the v4.3.0 type replacement. errors.As resolves
// **SenderError by assignability before consulting this method, so the
// only target we handle is **QwpError; everything else falls through
// to the standard walk.
//
// Deprecated: exists solely for the QwpError shim; removed with it.
func (e *SenderError) As(target any) bool {
	if e == nil {
		return false
	}
	qe, ok := target.(**QwpError)
	if !ok {
		return false
	}
	status := QwpStatusCode(0)
	if e.ServerStatusByte != NoStatusByte {
		status = QwpStatusCode(byte(e.ServerStatusByte))
	}
	*qe = &QwpError{
		Status:   status,
		Sequence: e.MessageSequence,
		Message:  e.ServerMessage,
	}
	return true
}
