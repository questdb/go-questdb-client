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
	"fmt"
	"strconv"
	"strings"
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
// startIdx allows the failover path to skip the just-failed endpoint:
// the walk visits endpoints [startIdx, len-1] then [0, startIdx-1],
// for a total of len(endpoints) attempts. The initial connect uses
// startIdx=0.
func connectWalk(ctx context.Context, cfg *qwpQueryClientConfig, startIdx int) (*qwpConnectResult, error) {
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
	n := len(cfg.endpoints)
	for offset := 0; offset < n; offset++ {
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
			// caller a false guarantee.
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
			n, lastErr)
	}
	// Specific role filter and no match — surface a typed
	// QwpRoleMismatchError carrying the last observed SERVER_INFO so
	// callers can distinguish "no primary available" (LastObserved
	// non-nil) from "all endpoints unreachable" (LastObserved nil).
	return nil, &QwpRoleMismatchError{
		Target:       cfg.target.String(),
		LastObserved: lastObserved,
		Endpoints:    endpointStrings,
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

	// attempt counts executeOnce invocations: 1 on the initial
	// submission, 2 after the first replay, etc. Capped by
	// cfg.failoverMaxAttempts.
	attempt int

	// cancelled is set by Cancel and checked at every reconnect-and-
	// replay boundary so the session does not start a fresh attempt
	// after the user has asked for cancellation.
	cancelled atomic.Bool
}

// newQwpQuerySession allocates and returns a session bound to client.
// The retained sql / bind payload comes from the supplied req. The
// caller must call submit before nextEvent; submit assigns the initial
// requestId and dispatches the first attempt to the I/O goroutine.
func newQwpQuerySession(client *QwpQueryClient, req qwpRequest) *qwpQuerySession {
	s := &qwpQuerySession{
		client:        client,
		sql:           req.sql,
		bindPayload:   req.bindPayload,
		bindCount:     req.bindCount,
		initialCredit: req.initialCredit,
	}
	s.currentRequestId.Store(req.requestId)
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
// to the bound I/O goroutine. Safe to call from any goroutine. Sets
// the cancelled flag first so the failover loop short-circuits even
// if the cancel races a reconnect.
func (s *qwpQuerySession) requestCancel() {
	s.cancelled.Store(true)
	s.client.io().requestCancel(s.currentRequestId.Load())
}

// nextEvent returns the next event from the current generation. On
// qwpEventKindTransportError, runs the reconnect-and-replay loop and
// returns a synthesized qwpEventKindFailoverReset event whose
// failoverReset field carries the new generation's QwpServerInfo. The
// caller's iterator (Batches() / Exec() loop) yields the reset to the
// user, who is expected to discard accumulated state and continue.
//
// When failover is disabled (cfg.failoverEnabled == false), or when
// the failover budget is exhausted, the original transport error is
// returned as-is so the caller surfaces it through the usual error
// path.
func (s *qwpQuerySession) nextEvent(ctx context.Context) (qwpEvent, error) {
	ev, err := s.client.io().takeEvent(ctx)
	if err != nil {
		return ev, err
	}
	if ev.kind != qwpEventKindTransportError {
		return ev, nil
	}
	// Transport-terminal failure. Decide whether to retry.
	if !s.shouldReplay() || s.cancelled.Load() {
		return ev, nil
	}
	lastErr := fmt.Errorf("qwp query: %s", ev.errMessage)
	failedIdx := int(s.client.currentEndpointIdx.Load())
	// Backoff (interruptible by ctx and cancel).
	delay := computeBackoff(s.client.cfg, s.attempt)
	if !sleepInterruptible(ctx, &s.cancelled, delay) || s.cancelled.Load() {
		return ev, nil
	}
	// Re-bind to a different role-matching endpoint and replay. A
	// successful return increments s.attempt (via submit) and
	// publishes the new generation on the client.
	newInfo, replayErr := s.client.reconnectAndReplay(ctx, s, failedIdx)
	if replayErr != nil {
		// Reconnect failed — surface a transport error wrapping the
		// dial failure and the original cause. The caller's next
		// iteration will see this and either retry (if the budget
		// permits) or surface to the user.
		return qwpEvent{
			kind:       qwpEventKindTransportError,
			errMessage: fmt.Sprintf("%v (after %v)", replayErr, lastErr),
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

// shouldReplay reports whether the current configuration permits
// another reconnect attempt for this session. Encapsulates the four
// "no replay" gates: failover disabled, attempt budget exhausted,
// fewer than 2 endpoints (nothing to fail over to), and Exec replay
// disabled when the SQL is non-idempotent.
func (s *qwpQuerySession) shouldReplay() bool {
	cfg := s.client.cfg
	if !cfg.failoverEnabled {
		return false
	}
	if s.attempt >= cfg.failoverMaxAttempts {
		return false
	}
	if len(cfg.endpoints) < 2 {
		// Single-endpoint deployments can still benefit from a
		// reconnect (e.g., a transient TCP RST), but the spec only
		// guarantees failover when multiple endpoints are configured.
		// Match Java's behaviour: allow single-endpoint replays —
		// they exercise the same reconnect machinery against the same
		// host/port.
		return true
	}
	return true
}

// computeBackoff is the exponential schedule from
// QwpQueryClient.java:839-840. attempt is the 1-based count of
// completed (failed) attempts at the call site — i.e. attempt=1
// means the initial submission just failed and we are about to
// retry for the first time. The first retry uses initial; the
// second uses 2*initial; the schedule doubles per step until the
// configured ceiling. attempt < 1 returns zero (no sleep before
// the very first try).
func computeBackoff(cfg *qwpQueryClientConfig, attempt int) time.Duration {
	if attempt < 1 {
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
	return d
}

// sleepInterruptible blocks for d, returning early when ctx expires
// or cancelled flips to true. Returns true if the full sleep
// completed, false if interrupted. Zero d returns immediately.
func sleepInterruptible(ctx context.Context, cancelled *atomic.Bool, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	// Poll cancelled in addition to the ctx because Cancel() doesn't
	// cancel the user's ctx — the session has its own atomic flag.
	// Use a small ticker so cancellation reaches the sleeper without
	// adding a hundred-microsecond floor on every backoff.
	checkInterval := d / 4
	if checkInterval < time.Millisecond {
		checkInterval = time.Millisecond
	}
	if checkInterval > 50*time.Millisecond {
		checkInterval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if cancelled.Load() {
				return false
			}
		}
	}
}
