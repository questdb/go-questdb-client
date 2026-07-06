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
	"log/slog"
)

// SenderConnectionEventKind classifies a QWP ingress connection-state
// transition.
type SenderConnectionEventKind int

const (
	// SenderConnected is the first successful connect of the sender's
	// lifetime — fired once, before any data is sent.
	SenderConnected SenderConnectionEventKind = iota
	// SenderDisconnected fires once per outage when the active wire drops,
	// before the sender either reconnects or (on a poison-frame terminal) halts.
	SenderDisconnected
	// SenderReconnected fires when a reconnect succeeds against the same
	// endpoint that was previously active.
	SenderReconnected
	// SenderFailedOver fires when a reconnect succeeds against a different
	// endpoint than the previously active one. PreviousHost/PreviousPort
	// carry the old endpoint.
	SenderFailedOver
	// SenderEndpointAttemptFailed fires when a single endpoint dial fails
	// during a reconnect walk. May be coalesced under inbox pressure.
	SenderEndpointAttemptFailed
	// SenderAllEndpointsUnreachable fires when a full address-list sweep
	// fails to connect. May be coalesced under inbox pressure.
	SenderAllEndpointsUnreachable
	// SenderAuthFailed is terminal: an auth/upgrade rejection halted the
	// sender. Cause carries the typed error.
	//
	// There is deliberately no reconnect-budget-exhausted kind: a running
	// sender retries transport outages indefinitely (Invariant B) and
	// never terminates on a wall clock.
	SenderAuthFailed
)

// String returns the SCREAMING_SNAKE_CASE name of the kind (e.g. "CONNECTED"),
// or "UNKNOWN(<n>)" for an unrecognised
// value.
func (k SenderConnectionEventKind) String() string {
	switch k {
	case SenderConnected:
		return "CONNECTED"
	case SenderDisconnected:
		return "DISCONNECTED"
	case SenderReconnected:
		return "RECONNECTED"
	case SenderFailedOver:
		return "FAILED_OVER"
	case SenderEndpointAttemptFailed:
		return "ENDPOINT_ATTEMPT_FAILED"
	case SenderAllEndpointsUnreachable:
		return "ALL_ENDPOINTS_UNREACHABLE"
	case SenderAuthFailed:
		return "AUTH_FAILED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(k))
	}
}

// SenderConnectionEvent describes one connection-state transition observed by
// the QWP ingress client. Fields not applicable to a given Kind take their
// zero value (Host=="", Port==0, AttemptNumber==0, Cause==nil) — Go's
// encoding of the "not applicable" sentinels. TimestampMillis is stamped
// at emit time on the sender's I/O goroutine.
type SenderConnectionEvent struct {
	// Kind classifies the connection-state transition this event describes and
	// determines which of the remaining fields are meaningful.
	Kind SenderConnectionEventKind
	// Host/Port is the endpoint this event concerns. Empty/0 for
	// SenderAllEndpointsUnreachable when no last-attempted endpoint is known.
	Host string
	Port int
	// PreviousHost/PreviousPort is the prior endpoint for SenderFailedOver.
	PreviousHost string
	PreviousPort int
	// AttemptNumber is the reconnect-attempt counter at emit time; 0 for the
	// initial connect (not a reconnect).
	AttemptNumber int64
	// RoundNumber is the count of full address-list sweeps; 0 when not
	// applicable.
	RoundNumber int64
	// Cause is the classified failure for endpoint-attempt and terminal kinds;
	// nil for success events (Connected/Reconnected/FailedOver) and for
	// AllEndpointsUnreachable, which aggregates a whole sweep with no single cause.
	Cause error
	// TimestampMillis is the wall-clock emit time (Unix epoch milliseconds).
	TimestampMillis int64
}

// String renders the event as a single-line, human-readable summary for logs,
// omitting fields left at their zero value for the event's Kind.
func (e SenderConnectionEvent) String() string {
	s := "SenderConnectionEvent{kind=" + e.Kind.String()
	if e.Host != "" {
		s += fmt.Sprintf(", endpoint=%s:%d", e.Host, e.Port)
	}
	if e.PreviousHost != "" {
		s += fmt.Sprintf(", previous=%s:%d", e.PreviousHost, e.PreviousPort)
	}
	if e.AttemptNumber != 0 {
		s += fmt.Sprintf(", attempt=%d", e.AttemptNumber)
	}
	if e.RoundNumber != 0 {
		s += fmt.Sprintf(", round=%d", e.RoundNumber)
	}
	if e.Cause != nil {
		s += fmt.Sprintf(", cause=%v", e.Cause)
	}
	return s + "}"
}

// SenderConnectionListener is the user-supplied callback invoked when the QWP
// ingress client observes a connection-state transition (initial connect,
// failover, an endpoint attempt failing, the full address list being
// unreachable, or a terminal auth rejection). Registered via
// WithConnectionListener.
//
// # Threading
//
// Invoked on a dedicated dispatcher goroutine, never on the I/O or producer
// goroutine. A slow listener cannot stall publishing or reconnect; if the
// bounded inbox fills, surplus events are dropped (visible via
// QwpSender.DroppedConnectionNotifications()).
//
// # Delivery
//
// Delivery is best-effort: the bounded inbox is drop-oldest for every payload,
// so under inbox pressure any queued event — success (Connected, FailedOver,
// Reconnected), failure (EndpointAttemptFailed, AllEndpointsUnreachable), or
// terminal (AuthFailed) — may be displaced before it
// is delivered (visible via QwpSender.DroppedConnectionNotifications()). Failure
// events are the likeliest to coalesce, as they can burst during a reconnect
// walk; the sparser success and terminal events survive under normal operation.
// A terminal event is emitted before the producer-side typed error becomes
// observable, so a listener that receives it can react before the producer
// learns via the next API call. A panic from the listener is recovered and
// logged.
//
// # Calling back into the sender
//
// The listener may call Close() or Flush() on the sender — e.g. to shut down
// on SenderAuthFailed — without deadlocking. Because the listener runs on the
// dispatcher goroutine, not the producer goroutine, those calls deliberately
// do NOT touch in-progress producer state: they surface only a latched
// terminal error and will not flush rows the producer has staged but not yet
// flushed itself (those may be mid-assembly on the producer goroutine).
// Close() still tears down the wire, drains already-published frames up to
// close_flush_timeout, and releases resources. Same contract as
// SenderErrorHandler.
type SenderConnectionListener func(SenderConnectionEvent)

// newDefaultSenderConnectionListener builds the loud-not-silent fallback used
// when no listener is registered: every transition is logged, terminal/failure
// kinds loudly (Error for an auth failure, Warn for a disconnect or an
// endpoint/all-endpoints reachability failure, Info otherwise). Loud by design
// so a flapping or unreachable server is never silent. The caller's logger
// controls the sink; nil resolves to slog.Default().
func newDefaultSenderConnectionListener(logger *slog.Logger) SenderConnectionListener {
	l := qwpEffectiveLogger(logger)
	return func(e SenderConnectionEvent) {
		switch e.Kind {
		case SenderAuthFailed:
			l.Error("qwp: connection event", "event", e)
		case SenderDisconnected, SenderEndpointAttemptFailed, SenderAllEndpointsUnreachable:
			l.Warn("qwp: connection event", "event", e)
		default:
			l.Info("qwp: connection event", "event", e)
		}
	}
}

// silentSenderConnectionListener drops every event. Used by background drainers,
// whose outcome is reported through QwpBackgroundDrainerListener; their raw
// connection lifecycle would otherwise reach the loud default with no endpoint
// context and masquerade as the foreground sender flapping.
func silentSenderConnectionListener(SenderConnectionEvent) {}

// newQwpConnDispatcher builds the off-I/O dispatcher that delivers connection
// events to listener (or the loud default when nil).
func newQwpConnDispatcher(listener SenderConnectionListener, capacity int) *qwpDispatcher[*SenderConnectionEvent] {
	if listener == nil {
		listener = newDefaultSenderConnectionListener(nil)
	}
	return newQwpDispatcher(
		func(e *SenderConnectionEvent) { listener(*e) },
		func(e *SenderConnectionEvent) string { return e.String() },
		func(e *SenderConnectionEvent) bool { return e != nil },
		"qwp/conn",
		capacity,
	)
}
