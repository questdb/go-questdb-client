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
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// qwpDefaultPort is the port applied to addr= entries that omit one.
// Matches Java QwpQueryClient.DEFAULT_WS_PORT and the live server's
// default HTTP/WebSocket bind. Single source of truth so the live
// integration tests and the connection-string parser cannot drift.
const qwpDefaultPort = 9000

// qwpEndpoint is one address on the connect-walk list. Distinct from a
// raw "host:port" string so callers can stream the same endpoint
// through validate / hostport / debug paths without re-parsing.
type qwpEndpoint struct {
	host string
	port int
}

// String formats the endpoint as host:port, bracketing IPv6 hosts so
// downstream consumers can re-parse the form without ambiguity.
func (e qwpEndpoint) String() string {
	if strings.Contains(e.host, ":") {
		return fmt.Sprintf("[%s]:%d", e.host, e.port)
	}
	return fmt.Sprintf("%s:%d", e.host, e.port)
}

// qwpTargetFilter constrains the connect walk to endpoints whose
// SERVER_INFO.role passes the filter. Mirrors Java QwpQueryClient's
// TARGET_ANY/PRIMARY/REPLICA constants. Zero value is qwpTargetAny so
// tests and config defaults can use the zero-init pattern naturally.
type qwpTargetFilter byte

const (
	// qwpTargetAny accepts any role. The default; matches Java's
	// TARGET_ANY. Used when callers only want any reachable endpoint.
	qwpTargetAny qwpTargetFilter = iota
	// qwpTargetPrimary accepts STANDALONE, PRIMARY, and PRIMARY_CATCHUP.
	// STANDALONE is included so single-node OSS deployments (which do
	// not configure replication) are not accidentally excluded.
	qwpTargetPrimary
	// qwpTargetReplica accepts only REPLICA. Use when read latency is
	// secondary to offloading the primary.
	qwpTargetReplica
)

// Exported aliases for the target-filter constants, so callers of
// WithTarget can name the values without the type being exported
// (mirrors the ProtocolVersion1/2/3 pattern for protocolVersion).
// Equivalent to the connect-string target=any|primary|replica values.
const (
	// QwpTargetAny accepts any reachable endpoint regardless of role.
	// The default; equivalent to target=any (or omitting the key).
	QwpTargetAny = qwpTargetAny
	// QwpTargetPrimary routes only to STANDALONE / PRIMARY /
	// PRIMARY_CATCHUP endpoints; equivalent to target=primary.
	QwpTargetPrimary = qwpTargetPrimary
	// QwpTargetReplica routes only to REPLICA endpoints; equivalent
	// to target=replica.
	QwpTargetReplica = qwpTargetReplica
)

// String returns the connection-string form for diagnostics and error
// messages.
func (t qwpTargetFilter) String() string {
	switch t {
	case qwpTargetAny:
		return "any"
	case qwpTargetPrimary:
		return "primary"
	case qwpTargetReplica:
		return "replica"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

// parseTargetFilter maps the connection-string value to the enum.
// Empty input normalises to qwpTargetAny so parsers that assemble the
// effective config from multiple sources can use absence-as-default
// without a dedicated branch. Mirrors Java's
// QwpQueryClient.fromConfig target validation.
func parseTargetFilter(s string) (qwpTargetFilter, error) {
	switch s {
	case "", "any":
		return qwpTargetAny, nil
	case "primary":
		return qwpTargetPrimary, nil
	case "replica":
		return qwpTargetReplica, nil
	default:
		return 0, fmt.Errorf(
			"qwp query: invalid target %q (expected any, primary, or replica)", s)
	}
}

// accepts reports whether the given role byte passes the filter.
// Mirrors Java QwpQueryClient.matchesTarget exactly: primary accepts
// STANDALONE so OSS deployments (which advertise STANDALONE rather
// than PRIMARY) are treated as primaries for routing purposes.
func (t qwpTargetFilter) accepts(role byte) bool {
	switch t {
	case qwpTargetAny:
		return true
	case qwpTargetPrimary:
		return role == qwpRoleStandalone ||
			role == qwpRolePrimary ||
			role == qwpRolePrimaryCatchup
	case qwpTargetReplica:
		return role == qwpRoleReplica
	default:
		return false
	}
}

// parseEndpointList splits a comma-separated addr= value into typed
// endpoints. Defers per-endpoint validation to splitQwpHostPort and
// the explicit port-range check; rejects the empty string and any
// element that fails parsing. Surfaces errors with the original
// element so a malformed entry in the middle of the list is easy to
// pinpoint.
//
// defaultPort is applied when an entry omits :port. Use
// defaultHttpPort (9000) for the QWP defaults; tests pass an explicit
// number when they need a different default.
func parseEndpointList(s string, defaultPort int) ([]qwpEndpoint, error) {
	if s == "" {
		return nil, fmt.Errorf("qwp query: addr is empty")
	}
	parts := strings.Split(s, ",")
	out := make([]qwpEndpoint, 0, len(parts))
	for _, p := range parts {
		entry := strings.TrimSpace(p)
		if entry == "" {
			return nil, fmt.Errorf("qwp query: empty entry in addr list %q", s)
		}
		host, portStr, err := splitQwpHostPort(entry)
		if err != nil {
			return nil, fmt.Errorf("qwp query: invalid addr %q: %w", entry, err)
		}
		if host == "" {
			return nil, fmt.Errorf("qwp query: invalid addr %q: empty host", entry)
		}
		port := defaultPort
		if portStr != "" {
			n, err := strconv.Atoi(portStr)
			if err != nil {
				return nil, fmt.Errorf(
					"qwp query: invalid addr %q: invalid port %q", entry, portStr)
			}
			if n < 1 || n > 65535 {
				return nil, fmt.Errorf(
					"qwp query: invalid addr %q: port %d out of range [1, 65535]",
					entry, n)
			}
			port = n
		}
		out = append(out, qwpEndpoint{host: host, port: port})
	}
	return out, nil
}

// qwpConnectResult bundles everything connectWalk produces on success:
// a live transport + I/O goroutine pair, the index of the bound
// endpoint in cfg.endpoints, and the SERVER_INFO from the bound
// connection. Returned to the caller (newQwpQueryClient or the
// failover orchestrator) so the client struct can publish all three
// atomically.
type qwpConnectResult struct {
	transport      *qwpTransport
	io             *qwpEgressIO
	endpointIdx    int
	serverInfo     *QwpServerInfo
}

// connectWalk iterates cfg.endpoints in order, attempting one
// transport.connect per endpoint. The first endpoint whose
// SERVER_INFO.role passes cfg.target's filter wins; non-matching
// endpoints are torn down and skipped. v1 servers (no SERVER_INFO)
// satisfy only target=any — qwpTargetPrimary / qwpTargetReplica are
// rejected because the role byte is unknown.
//
// Closes any partially-bound resources before returning on a failure
// path so callers do not have to worry about leaked goroutines or
// half-open sockets. On a successful return the caller takes
// ownership of the transport + I/O.
//
// failedIdx selects between two walk shapes, mirroring Java's
// reconnectSkippingIndex:
//
//   - failedIdx < 0: initial connect. Visits all len(endpoints)
//     entries starting at index 0.
//   - failedIdx >= 0: failover reconnect. Visits the other
//     len(endpoints)-1 entries starting at failedIdx+1 (mod n) and
//     never revisits failedIdx itself. A transport failure is likely
//     to repeat immediately on the same socket, so retrying it would
//     just burn an attempt; the outer failover loop can come back to
//     this endpoint on a subsequent attempt if every other endpoint
//     is also unreachable.
//
// cancelCh, when non-nil, is checked at every endpoint boundary to
// short-circuit the walk if the user has asked to cancel. Cancel()
// closes the session's cancelCh but does not cancel the user's ctx,
// so without this check a slow walk would block on
// serverInfoTimeout × len(endpoints) before honouring the cancel.
// The check is at the loop boundary only; it does NOT preempt an
// in-flight Dial / SERVER_INFO read, so the worst-case wait shrinks
// from the full walk to a single endpoint's timeout. Java has the
// same boundary-only granularity.
func connectWalk(ctx context.Context, cfg *qwpQueryClientConfig, failedIdx int, cancelCh <-chan struct{}) (*qwpConnectResult, error) {
	if len(cfg.endpoints) == 0 {
		return nil, fmt.Errorf("qwp query: no endpoints configured")
	}
	scheme := "ws"
	if cfg.tlsMode != tlsDisabled {
		scheme = "wss"
	}
	endpointStrings := make([]string, len(cfg.endpoints))
	for i, ep := range cfg.endpoints {
		endpointStrings[i] = ep.String()
	}

	var lastObserved *QwpServerInfo
	var lastErr error
	sawV1Mismatch := false
	n := len(cfg.endpoints)
	startIdx := 0
	stepCount := n
	if failedIdx >= 0 {
		startIdx = failedIdx + 1
		stepCount = n - 1
	}
	for offset := 0; offset < stepCount; offset++ {
		if cancelCh != nil {
			select {
			case <-cancelCh:
				return nil, context.Canceled
			default:
			}
		}
		idx := (startIdx + offset) % n
		ep := cfg.endpoints[idx]
		wsURL := scheme + "://" + ep.String()

		tr := &qwpTransport{}
		opts := qwpTransportOpts{
			tlsInsecureSkipVerify: cfg.tlsMode == tlsInsecureSkipVerify,
			endpointPath:          cfg.endpointPath,
			authorization:         cfg.effectiveAuthorization(),
			maxBatchRows:          cfg.maxBatchRows,
			acceptEncoding:        cfg.buildAcceptEncodingHeader(),
			// target != any forces v2; otherwise we still advertise v2
			// so v2 servers know the client can read SERVER_INFO and
			// will emit it.
			maxVersion:        qwpMaxSupportedVersion,
			serverInfoTimeout: cfg.serverInfoTimeout,
		}
		if err := tr.connect(ctx, wsURL, opts); err != nil {
			lastErr = err
			// Try the next endpoint; transport.connect already cleaned
			// up after itself on the failure path.
			continue
		}

		info := tr.serverInfo
		if info == nil && cfg.target != qwpTargetAny {
			// v1 server cannot satisfy a specific role filter — its
			// role is unknown and a "best effort" bind would give the
			// caller a false guarantee. Record this so the final
			// QwpRoleMismatchError can flag SawV1Mismatch and tell the
			// caller "the cluster is up but it's OSS / v1" rather than
			// "all endpoints unreachable".
			sawV1Mismatch = true
			_ = tr.close()
			continue
		}
		if info != nil && !cfg.target.accepts(info.Role) {
			lastObserved = info
			_ = tr.close()
			continue
		}

		// Bound. Stand up the I/O goroutine pair on the heap-stable
		// transport pointer and publish. The atomic pointer in the
		// client struct allows swapping `tr` independently across
		// reconnects without disturbing the IO goroutine's view.
		io := newQwpEgressIO(tr, cfg.bufferPoolSize)
		io.start()
		return &qwpConnectResult{
			transport:   tr,
			io:          io,
			endpointIdx: idx,
			serverInfo:  tr.serverInfo,
		}, nil
	}

	if cfg.target == qwpTargetAny {
		// No matching endpoint and the filter is permissive — every
		// endpoint must have failed the dial. Surface the last
		// underlying error so the user sees a useful diagnostic.
		if lastErr == nil {
			lastErr = fmt.Errorf("qwp query: all endpoints unreachable")
		}
		return nil, fmt.Errorf("qwp query: connect failed (tried %d endpoints): %w",
			stepCount, lastErr)
	}
	// Specific role filter and no match — surface a typed
	// QwpRoleMismatchError carrying the last observed SERVER_INFO, the
	// v1-mismatch flag, and the last transport error so callers can
	// distinguish "no primary available" (LastObserved non-nil),
	// "OSS-only cluster" (SawV1Mismatch true), "all endpoints
	// unreachable" (LastTransportError non-nil with both other fields
	// zero), and any combination thereof.
	return nil, &QwpRoleMismatchError{
		Target:             cfg.target.String(),
		LastObserved:       lastObserved,
		SawV1Mismatch:      sawV1Mismatch,
		LastTransportError: lastErr,
		Endpoints:          endpointStrings,
	}
}

// qwpQuerySession orchestrates a single Query / Exec call: submission,
// event consumption, and transparent failover (reconnect + replay) on
// transport-terminal failure. The session owns the retained
// sql / bindPayload / initialCredit / bindCount so a replay attempt
// can reuse them on the new connection without round-tripping through
// the user goroutine.
//
// One session per Query / Exec; not safe for concurrent reuse. Cancel
// is the only method safe to call from another goroutine.
type qwpQuerySession struct {
	client *QwpQueryClient

	// Retained request fields. Cleared on successful End / ExecDone /
	// Error so a follow-up query on the same client cannot accidentally
	// observe them.
	sql           string
	bindPayload   []byte
	bindCount     int
	initialCredit int64

	// currentRequestId tracks the request_id of the in-flight
	// generation. Updated atomically each time submit is called: a
	// fresh value on the initial submit and on every replay. Cancel
	// reads it to send a CANCEL frame for the right generation.
	currentRequestId atomic.Int64

	// replayable gates whether nextEvent is allowed to
	// reconnect-and-resubmit on a transport-terminal failure. true
	// for Query (SELECT is idempotent — replaying is always safe);
	// for Exec it is cfg.replayExec, false by default so a
	// non-idempotent INSERT/UPDATE/DELETE/DDL that the server may
	// have already applied before the transport drop is never
	// silently re-executed on the new connection. When false,
	// nextEvent surfaces the raw transport error instead of
	// resubmitting (the connection is poisoned; the caller must
	// rebuild and decide whether the statement applied).
	replayable bool

	// attempt counts executeOnce invocations: 1 on the initial
	// submission, 2 after the first replay, etc. Capped by
	// cfg.failoverMaxAttempts.
	attempt int

	// failoverDeadline is the wall-clock cap on this Query/Exec's
	// failover loop, stamped once at session creation (mirrors Java
	// computing the deadline before the attempt loop,
	// QwpQueryClient.java:1517-1528). Zero means no time cap —
	// failover is then bounded only by cfg.failoverMaxAttempts.
	failoverDeadline time.Time

	// cancelCh is closed by requestCancel and selected on at every
	// reconnect-and-replay boundary so the session does not start a
	// fresh attempt after the user has asked for cancellation. A
	// closed channel lets sleepInterruptible wake immediately on
	// Cancel without polling. cancelOnce guards the close.
	cancelCh   chan struct{}
	cancelOnce sync.Once
}

// isCancelled reports whether requestCancel has been called.
func (s *qwpQuerySession) isCancelled() bool {
	select {
	case <-s.cancelCh:
		return true
	default:
		return false
	}
}

// failoverBudgetExpired reports whether the per-Query/Exec wall-clock
// failover budget (failover_max_duration_ms) has elapsed. A zero
// deadline means the budget is disabled — failover is then bounded
// only by cfg.failoverMaxAttempts. Mirrors Java's
// failoverMaxDurationMs == 0 → unbounded (QwpQueryClient.java:1527)
// and the now >= deadline give-up test (QwpQueryClient.java:1541).
func (s *qwpQuerySession) failoverBudgetExpired() bool {
	return !s.failoverDeadline.IsZero() && !time.Now().Before(s.failoverDeadline)
}

// newQwpQuerySession allocates and returns a session bound to client.
// The retained sql / bind payload comes from the supplied req. The
// caller must call submit before nextEvent; submit assigns the initial
// requestId and dispatches the first attempt to the I/O goroutine.
//
// replayable decides whether a transport-terminal failure may be
// recovered by reconnect-and-resubmit: pass true for Query (SELECT is
// idempotent) and cfg.replayExec for Exec (false by default to protect
// non-idempotent statements from double-execution).
func newQwpQuerySession(client *QwpQueryClient, req qwpRequest, replayable bool) *qwpQuerySession {
	s := &qwpQuerySession{
		client:        client,
		sql:           req.sql,
		bindPayload:   req.bindPayload,
		bindCount:     req.bindCount,
		initialCredit: req.initialCredit,
		replayable:    replayable,
		cancelCh:      make(chan struct{}),
	}
	s.currentRequestId.Store(req.requestId)
	// Stamp the failover budget deadline once, before the first
	// submit, mirroring Java computing failoverDeadlineNanos before
	// the attempt loop (QwpQueryClient.java:1517-1528). A zero or
	// negative cap leaves failoverDeadline as the zero Time, which
	// failoverBudgetExpired treats as "no time cap".
	if d := client.cfg.failoverMaxDuration; d > 0 {
		s.failoverDeadline = time.Now().Add(d)
	}
	return s
}

// submit dispatches the current attempt's qwpRequest to the I/O
// goroutine on the bound generation. Returns the same error
// io.submitQuery would have returned (closed I/O, latched ioErr,
// ctx-cancelled wait).
func (s *qwpQuerySession) submit(ctx context.Context) error {
	s.attempt++
	req := qwpRequest{
		sql:           s.sql,
		requestId:     s.currentRequestId.Load(),
		initialCredit: s.initialCredit,
		bindCount:     s.bindCount,
		bindPayload:   s.bindPayload,
	}
	return s.client.io().submitQuery(ctx, req)
}

// requestCancel marks the session cancelled and forwards the cancel
// to the bound I/O goroutine. Safe to call from any goroutine. Closes
// cancelCh first so the failover loop and any in-flight backoff sleep
// short-circuit even if the cancel races a reconnect.
func (s *qwpQuerySession) requestCancel() {
	s.cancelOnce.Do(func() { close(s.cancelCh) })
	s.client.io().requestCancel(s.currentRequestId.Load())
}

// nextEvent returns the next event from the current generation. On
// qwpEventKindTransportError, runs the reconnect-and-replay loop and
// returns a synthesized qwpEventKindFailoverReset event whose
// failoverReset field carries the new generation's QwpServerInfo. The
// caller's iterator (Batches() / Exec() loop) yields the reset to the
// user, who is expected to discard accumulated state and continue.
//
// When failover is disabled (cfg.failoverEnabled == false), or this
// session is not replayable (a non-idempotent Exec with
// replay_exec=off — see s.replayable), the original transport error
// is returned as-is, WITHOUT reconnecting or resubmitting, so the
// caller surfaces it through the usual error path and the
// possibly-already-applied statement is never re-executed. When the
// failover budget is exhausted (s.attempt >= cfg.failoverMaxAttempts,
// or the failover_max_duration_ms wall-clock budget has elapsed), the
// event is wrapped into a *QwpFailoverExhaustedError so callers can
// errors.As against the exhaustion shape and distinguish "we ran out
// of retries" from "first attempt failed".
func (s *qwpQuerySession) nextEvent(ctx context.Context) (qwpEvent, error) {
	ev, err := s.client.io().takeEvent(ctx)
	if err != nil {
		return ev, err
	}
	if ev.kind != qwpEventKindTransportError {
		return ev, nil
	}
	// Transport-terminal failure. Decide whether to retry.
	if s.isCancelled() {
		return ev, nil
	}
	cfg := s.client.cfg
	if !cfg.failoverEnabled {
		return ev, nil
	}
	if !s.replayable {
		// Non-idempotent Exec with replay_exec=off. The server may
		// have already applied the INSERT/UPDATE/DELETE/DDL before the
		// transport dropped, so reconnecting and resubmitting would
		// risk a silent second execution. Surface the raw transport
		// error instead: the connection is poisoned (loadIoErr is
		// latched), the next Query/Exec fails fast, and the caller
		// must rebuild the client and decide whether the statement
		// took effect. Query is always replayable (SELECT is
		// idempotent), so this branch only ever fires for Exec.
		return ev, nil
	}
	if s.attempt >= cfg.failoverMaxAttempts || s.failoverBudgetExpired() {
		// Budget exhausted: the attempt cap was reached or the
		// failover_max_duration_ms wall-clock budget elapsed. Wrap the
		// underlying transport error so callers can errors.As to
		// *QwpFailoverExhaustedError and distinguish "we ran out of
		// retries" from "first attempt failed". Mirrors Java's
		// combined give-up test (attempt >= max || now >= deadline)
		// at QwpQueryClient.java:1541, which emits one exhaustion
		// message for both causes.
		return s.exhaustedEvent(ev), nil
	}
	lastErr := fmt.Errorf("qwp query: %s", ev.errMessage)
	failedIdx := int(s.client.currentEndpointIdx.Load())
	// Backoff (interruptible by ctx and cancel), clamped so the sleep
	// never overshoots the failover budget. Mirrors Java
	// QwpQueryClient.java:1569-1583: after the jittered delay,
	// recompute the remaining budget, give up if it is already spent,
	// and otherwise shrink the sleep to what remains.
	delay := computeBackoff(s.client.cfg, s.attempt)
	if !s.failoverDeadline.IsZero() {
		remaining := time.Until(s.failoverDeadline)
		if remaining <= 0 {
			return s.exhaustedEvent(ev), nil
		}
		if delay > remaining {
			delay = remaining
		}
	}
	if !sleepInterruptible(ctx, s.cancelCh, delay) || s.isCancelled() {
		return ev, nil
	}
	// Re-bind to a different role-matching endpoint and replay. A
	// successful return increments s.attempt (via submit) and
	// publishes the new generation on the client.
	newInfo, replayErr := s.client.reconnectAndReplay(ctx, s, failedIdx)
	if replayErr != nil {
		if s.isCancelled() {
			// Cancel landed during the walk and connectWalk's boundary
			// poll short-circuited it. Surface the original transport
			// error rather than a connect-failed wrap, matching the
			// pre-walk and post-sleep cancel guards above.
			return ev, nil
		}
		// Reconnect failed — surface a transport error wrapping the
		// dial failure and the original cause. The caller's next
		// iteration will see this and either retry (if the budget
		// permits) or surface to the user. Thread the typed replayErr
		// (e.g. *QwpRoleMismatchError) so callers can errors.As
		// against it on a failover-time mismatch, matching the
		// initial-connect path.
		return qwpEvent{
			kind:         qwpEventKindTransportError,
			errMessage:   fmt.Sprintf("%v (after %v)", replayErr, lastErr),
			transportErr: fmt.Errorf("%w (after %w)", replayErr, lastErr),
		}, nil
	}
	return qwpEvent{
		kind:      qwpEventKindFailoverReset,
		requestId: s.currentRequestId.Load(),
		failoverReset: &QwpFailoverReset{
			NewNode:   newInfo,
			Attempt:   s.attempt,
			LastError: lastErr,
		},
	}, nil
}

// exhaustedEvent wraps a terminal transport event into a
// qwpEventKindTransportError event whose typed cause is a
// *QwpFailoverExhaustedError. Used at the point where the failover
// budget has been consumed so the caller can errors.As against the
// exhaustion shape and distinguish it from the first-attempt-failed
// case. Preserves the original event's underlying error (or its
// errMessage when no typed cause was attached) as the LastError so
// errors.Unwrap chains down to the actual transport fault.
func (s *qwpQuerySession) exhaustedEvent(ev qwpEvent) qwpEvent {
	cause := ev.transportErr
	if cause == nil {
		msg := ev.errMessage
		if msg == "" {
			msg = "qwp query: transport-terminal failure"
		}
		cause = errors.New(msg)
	}
	exhausted := &QwpFailoverExhaustedError{
		Attempts:  s.attempt,
		LastError: cause,
	}
	return qwpEvent{
		kind:         qwpEventKindTransportError,
		requestId:    ev.requestId,
		errMessage:   exhausted.Error(),
		transportErr: exhausted,
	}
}

// computeBackoff is the full-jitter exponential schedule from
// QwpQueryClient.java:1557-1568. attempt is the 1-based count of
// completed (failed) attempts at the call site — i.e. attempt=1
// means the initial submission just failed and we are about to
// retry for the first time. The base doubles per step (initial,
// 2*initial, 4*initial, …) until the configured ceiling, then
// full-jitter draws the actual sleep uniformly from [0, base).
// Egress is single-user, so the lowest expected recovery time
// wins over the reconnect-storm damping that equal-jitter buys
// the shared ingress path (failover.md §3.1; ingress jitter in
// qwp_sf_round_walk.go's qwpSfComputeBackoff). attempt < 1,
// initial == 0, or a non-positive cap returns zero (no sleep).
func computeBackoff(cfg *qwpQueryClientConfig, attempt int) time.Duration {
	if attempt < 1 || cfg.failoverBackoffInitial == 0 {
		return 0
	}
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	d := cfg.failoverBackoffInitial << shift
	if d <= 0 || d > cfg.failoverBackoffMax {
		d = cfg.failoverBackoffMax
	}
	if d <= 0 {
		return 0
	}
	// Full-jitter: [0, base). rand.Int63n requires a positive
	// argument; the d > 0 guard above keeps that contract.
	return time.Duration(rand.Int63n(int64(d)))
}

// sleepInterruptible blocks for d, returning early when ctx expires
// or cancelCh is closed. Returns true if the full sleep completed,
// false if interrupted. Zero d returns immediately.
func sleepInterruptible(ctx context.Context, cancelCh <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-cancelCh:
		return false
	}
}
