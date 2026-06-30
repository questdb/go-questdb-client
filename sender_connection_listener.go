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
	"log"
)

// SenderConnectionEventKind classifies a QWP ingress connection-state
// transition. Mirrors the Java client's SenderConnectionEvent.Kind.
type SenderConnectionEventKind int

const (
	// SenderConnected is the first successful connect of the sender's
	// lifetime — fired once, before any data is sent.
	SenderConnected SenderConnectionEventKind = iota
	// SenderDisconnected fires once per outage when the active wire drops
	// and the reconnect loop is about to start.
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
	SenderAuthFailed
	// SenderReconnectBudgetExhausted is terminal: the reconnect budget ran
	// out. Cause carries the typed error.
	SenderReconnectBudgetExhausted
)

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
	case SenderReconnectBudgetExhausted:
		return "RECONNECT_BUDGET_EXHAUSTED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(k))
	}
}

// SenderConnectionEvent describes one connection-state transition observed by
// the QWP ingress client. Fields not applicable to a given Kind take their
// zero value (Host=="", Port==0, AttemptNumber==0, Cause==nil) — Go's
// idiomatic encoding of Java's NO_PORT/NO_ATTEMPT_NUMBER/NO_ROUND_NUMBER
// sentinels. TimestampMillis is set by the dispatcher at emit time.
type SenderConnectionEvent struct {
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
	// Cause is the classified failure for failure/terminal kinds; nil for
	// success events (Connected/Reconnected/FailedOver).
	Cause error
	// TimestampMillis is the wall-clock emit time (Unix epoch milliseconds).
	TimestampMillis int64
}

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
// unreachable, or a terminal auth/budget rejection). Registered via
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
// Success events (Connected, FailedOver, Reconnected) fire on each transition.
// Failure events (EndpointAttemptFailed, AllEndpointsUnreachable) may be
// coalesced under inbox pressure. Terminal events (AuthFailed,
// ReconnectBudgetExhausted) fire before the producer-side typed error becomes
// observable, so a listener can react before the producer learns via the next
// API call. A panic from the listener is recovered and logged.
type SenderConnectionListener func(SenderConnectionEvent)

// defaultSenderConnectionListener is the loud-not-silent fallback used when no
// listener is registered: every transition is logged, terminal/failure kinds
// loudly. Per the Java client's DefaultSenderConnectionListener.
func defaultSenderConnectionListener(e SenderConnectionEvent) {
	level := "[INFO]"
	switch e.Kind {
	case SenderAuthFailed, SenderReconnectBudgetExhausted:
		level = "[ERROR]"
	case SenderDisconnected, SenderEndpointAttemptFailed, SenderAllEndpointsUnreachable:
		level = "[WARN]"
	}
	log.Printf("%s qwp: %s", level, e)
}

// newQwpConnDispatcher builds the off-I/O dispatcher that delivers connection
// events to listener (or the loud default when nil).
func newQwpConnDispatcher(listener SenderConnectionListener, capacity int) *qwpDispatcher[*SenderConnectionEvent] {
	if listener == nil {
		listener = defaultSenderConnectionListener
	}
	return newQwpDispatcher(
		func(e *SenderConnectionEvent) { listener(*e) },
		func(e *SenderConnectionEvent) string { return e.String() },
		func(e *SenderConnectionEvent) bool { return e != nil },
		capacity,
	)
}
