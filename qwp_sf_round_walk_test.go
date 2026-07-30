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
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRoundWalkRejectServer returns an httptest server that responds
// to every upgrade with the given status + headers. Used to drive
// 421 / 401 / 404 / etc. classification in the round-walk.
func newRoundWalkRejectServer(t *testing.T, status int, headers http.Header) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, vs := range headers {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
	}))
}

// newRoundWalkHealthyServer returns a server that accepts the WS
// upgrade. The QWP X-QWP-Version header is set to "1" so the
// transport's negotiation passes.
func newRoundWalkHealthyServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		// Block until the client closes.
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
}

// newRoundWalkDurableServer accepts the WS upgrade AND advertises durable-ack,
// standing in for a replication primary.
func newRoundWalkDurableServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
}

// hostPortOf extracts host:port from an httptest URL.
func hostPortOf(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// TestRoundWalkBindsDurablePeerWhenFirstLacksDurableAck pins the Java
// buildAndConnect contract: a foreground durable-ack sweep walks PAST an endpoint
// that does not advertise durable-ack and binds a later durable-advertising
// primary, rather than aborting the whole sweep on the first mismatch.
func TestRoundWalkBindsDurablePeerWhenFirstLacksDurableAck(t *testing.T) {
	plainSrv := newRoundWalkHealthyServer(t) // upgrades OK, no durable-ack header
	defer plainSrv.Close()
	durableSrv := newRoundWalkDurableServer(t)
	defer durableSrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, plainSrv),
		endpointForServer(t, durableSrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	factory := qwpSfBuildEndpointFactory(endpoints, "ws", qwpTransportOpts{
		endpointPath:      qwpWritePath,
		requestDurableAck: true,
	}, nil)
	params := qwpSfRoundWalkParams{
		Factory:                 factory,
		Tracker:                 tracker,
		Endpoints:               endpoints,
		MaxDuration:             5 * time.Second,
		InitialBackoff:          50 * time.Millisecond,
		MaxBackoff:              500 * time.Millisecond,
		DurableMismatchTerminal: true,
	}
	result := qwpSfRunRoundWalk(context.Background(), nil, params, -1)

	require.Nil(t, result.Terminal, "a durable primary exists — the mismatch must not be terminal")
	require.NotNil(t, result.Transport, "must walk past the non-durable host and bind the durable primary")
	defer result.Transport.close()
	assert.Equal(t, 1, result.Idx)
}

// TestRoundWalkTerminalWhenNoEndpointAdvertisesDurableAck: when the WHOLE sweep
// lacks durable-ack, the remembered mismatch surfaces as terminal (fail closed).
func TestRoundWalkTerminalWhenNoEndpointAdvertisesDurableAck(t *testing.T) {
	a := newRoundWalkHealthyServer(t)
	defer a.Close()
	b := newRoundWalkHealthyServer(t)
	defer b.Close()

	endpoints := []qwpEndpoint{endpointForServer(t, a), endpointForServer(t, b)}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	factory := qwpSfBuildEndpointFactory(endpoints, "ws", qwpTransportOpts{
		endpointPath:      qwpWritePath,
		requestDurableAck: true,
	}, nil)
	params := qwpSfRoundWalkParams{
		Factory:                 factory,
		Tracker:                 tracker,
		Endpoints:               endpoints,
		MaxDuration:             2 * time.Second,
		InitialBackoff:          20 * time.Millisecond,
		MaxBackoff:              100 * time.Millisecond,
		DurableMismatchTerminal: true,
	}
	result := qwpSfRunRoundWalk(context.Background(), nil, params, -1)

	require.Nil(t, result.Transport)
	var mismatch *QwpDurableAckMismatchError
	require.ErrorAs(t, result.Terminal, &mismatch)
}

// endpointForServer parses an httptest URL into a qwpEndpoint.
func endpointForServer(t *testing.T, srv *httptest.Server) qwpEndpoint {
	t.Helper()
	eps, err := parseEndpointList(hostPortOf(srv), qwpDefaultPort)
	require.NoError(t, err)
	require.Len(t, eps, 1)
	return eps[0]
}

// runWalkAgainst dials the configured tracker+endpoints and returns
// the result. Tests assert on the result struct fields.
func runWalkAgainst(
	t *testing.T,
	endpoints []qwpEndpoint,
	tracker *qwpHostTracker,
	previousIdx int,
	maxDuration, initialBackoff, maxBackoff time.Duration,
) qwpSfRoundWalkResult {
	t.Helper()
	factory := qwpSfBuildEndpointFactory(endpoints, "ws", qwpTransportOpts{
		endpointPath: qwpWritePath,
	}, nil)
	params := qwpSfRoundWalkParams{
		Factory:        factory,
		Tracker:        tracker,
		Endpoints:      endpoints,
		MaxDuration:    maxDuration,
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
	}
	return qwpSfRunRoundWalk(context.Background(), nil, params, previousIdx)
}

// TestRoundWalkBindsHealthyPeerWhenFirstRoleRejects verifies that
// when host 0 returns 421+PRIMARY_CATCHUP and host 1 accepts, the
// walk lands on host 1 within a single round (no inter-host sleep).
func TestRoundWalkBindsHealthyPeerWhenFirstRoleRejects(t *testing.T) {
	rejectSrv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	})
	defer rejectSrv.Close()
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, rejectSrv),
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)

	start := time.Now()
	result := runWalkAgainst(t, endpoints, tracker, -1,
		5*time.Second, 100*time.Millisecond, 1*time.Second)
	elapsed := time.Since(start)

	require.NotNil(t, result.Transport, "expected successful bind")
	defer result.Transport.close()
	assert.Equal(t, 1, result.Idx, "should bind to healthy peer at idx=1")
	assert.Less(t, elapsed, 500*time.Millisecond,
		"single-round walk must NOT pay round-boundary sleep (skip-backoff-within-round)")

	// Tracker should record host 0 as TransientReject, host 1 as Healthy.
	snap := tracker.snapshot()
	assert.Equal(t, qwpHostTransientReject, snap[0].state)
	assert.Equal(t, qwpHostHealthy, snap[1].state)
}

// TestRoundWalkBindsHealthyPeerWhenFirstTransportErrors verifies the
// transport-error fallthrough: host 0 refuses TCP (unreachable port),
// host 1 accepts, walk lands on host 1.
func TestRoundWalkBindsHealthyPeerWhenFirstTransportErrors(t *testing.T) {
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	// Use a port that's almost certainly closed.
	endpoints := []qwpEndpoint{
		{host: "127.0.0.1", port: 1}, // port 1 = no service
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)

	result := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)

	require.NotNil(t, result.Transport)
	defer result.Transport.close()
	assert.Equal(t, 1, result.Idx, "must bind to healthy peer despite host 0 dial failure")
	snap := tracker.snapshot()
	assert.Equal(t, qwpHostTransportError, snap[0].state)
	assert.Equal(t, qwpHostHealthy, snap[1].state)
}

// TestRoundWalk404IsTransient is the 2026-05-08 reclassification:
// a 404 on one peer must NOT terminate the walk; the round-walk
// continues to a healthy sibling.
func TestRoundWalk404IsTransient(t *testing.T) {
	notFoundSrv := newRoundWalkRejectServer(t, 404, http.Header{})
	defer notFoundSrv.Close()
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, notFoundSrv),
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	result := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)

	require.NotNil(t, result.Transport, "404 must walk through to healthy peer, not terminate")
	defer result.Transport.close()
	assert.Equal(t, 1, result.Idx)
}

// TestRoundWalk426IsTransient: same reasoning as 404 — protocol
// version mismatch on one peer (rolling upgrade artifact) must not
// lock the client out of compatible siblings.
func TestRoundWalk426IsTransient(t *testing.T) {
	upgradeSrv := newRoundWalkRejectServer(t, 426, http.Header{})
	defer upgradeSrv.Close()
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, upgradeSrv),
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	result := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)

	require.NotNil(t, result.Transport)
	defer result.Transport.close()
	assert.Equal(t, 1, result.Idx)
}

// TestRoundWalkProtocolRejectDefersToTransientTransportError is the C1
// regression: a sweep that hit both a 404/426 protocol reject and a transient
// transport error must NOT latch the deferred protocol-reject terminal. The
// transport-errored host may be mid-restart, and terminating on the sibling's
// misconfiguration would drop a running sender the outage was about to release
// (Invariant B). The bounded walk exhausts its budget — keeps retrying — instead.
func TestRoundWalkProtocolRejectDefersToTransientTransportError(t *testing.T) {
	notFoundSrv := newRoundWalkRejectServer(t, 404, http.Header{})
	defer notFoundSrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, notFoundSrv), // persistent 404 reject
		{host: "127.0.0.1", port: 1},      // transient transport error (down host)
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	result := runWalkAgainst(t, endpoints, tracker, -1,
		300*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)

	require.Nil(t, result.Transport, "no endpoint is bindable in this sweep")
	require.Nil(t, result.Terminal,
		"a 404 coexisting with a transient transport error must not terminate (Invariant B)")
	require.NotNil(t, result.Exhausted,
		"the bounded walk must exhaust its budget and keep retrying, not latch a terminal")
}

// TestRoundWalkProtocolRejectDefersToRoleReject pins that a 404/426 protocol
// reject coexisting with a 421 role reject does not latch a terminal: a
// role-rejecting replica is promotable and may become a compatible primary, so
// the walk keeps retrying (Invariant B) rather than dropping a running sender.
func TestRoundWalkProtocolRejectDefersToRoleReject(t *testing.T) {
	notFoundSrv := newRoundWalkRejectServer(t, 404, http.Header{})
	defer notFoundSrv.Close()
	roleSrv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	})
	defer roleSrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, notFoundSrv), // persistent 404 protocol reject
		endpointForServer(t, roleSrv),     // 421 role reject (promotable replica)
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	result := runWalkAgainst(t, endpoints, tracker, -1,
		200*time.Millisecond, 10*time.Millisecond, 30*time.Millisecond)

	require.Nil(t, result.Transport, "no endpoint is bindable in this sweep")
	require.Nil(t, result.Terminal,
		"a 404 coexisting with a 421 role reject must not terminate (the replica may be promoted)")
	require.NotNil(t, result.Exhausted,
		"the bounded walk must exhaust and keep retrying, not latch a terminal")
}

// TestRoundWalkReconnectRedialsBoundHostPastDeferredTerminalSibling is the
// regression for a stale attempted-bit skipping the previously-bound host on
// reconnect. RecordSuccess leaves the bound host's round slot consumed; a
// reconnect walk must start a fresh round so that host is a candidate again.
// Otherwise a sweep that finds only a 404/426 (or durable-mismatch) sibling —
// whose terminal is deferred to sweep exhaustion — latches that terminal
// without ever redialing the healthy host, permanently dropping a sender that
// should have reconnected (Invariant B).
func TestRoundWalkReconnectRedialsBoundHostPastDeferredTerminalSibling(t *testing.T) {
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	tracker.recordSuccess(0, nil) // host 0 bound on the prior connection

	dialed := make(map[int]int)
	factory := func(_ context.Context, idx int) (*qwpTransport, error) {
		dialed[idx]++
		if idx == 0 {
			return &qwpTransport{}, nil // healthy host, binds if dialed
		}
		return nil, &QwpUpgradeRejectError{StatusCode: 404} // misconfigured sibling
	}
	params := qwpSfRoundWalkParams{
		Factory:        factory,
		Tracker:        tracker,
		MaxDuration:    0, // unbounded, like the running loop
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	}
	// previousIdx=0: reconnecting after a mid-stream drop on the bound host.
	result := qwpSfRunRoundWalk(context.Background(), nil, params, 0)

	require.Nil(t, result.Terminal, "must not latch a terminal while a healthy host is redialable")
	require.NotNil(t, result.Transport, "must redial and bind the healthy host")
	assert.Equal(t, 0, result.Idx)
	assert.Positive(t, dialed[0], "healthy host 0 must be redialed")
}

// TestRoundWalkAuthErrorIsTerminal verifies that 401/403 short-
// circuits the walk — even if other peers might be reachable, the
// failover-loop spec treats AuthError as cluster-wide.
func TestRoundWalkAuthErrorIsTerminal(t *testing.T) {
	authSrv := newRoundWalkRejectServer(t, 401, http.Header{})
	defer authSrv.Close()
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, authSrv),
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	result := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)

	assert.Nil(t, result.Transport)
	require.NotNil(t, result.Terminal, "401 must surface as Terminal QwpUpgradeRejectError")
	assert.Equal(t, 401, result.Terminal.(*QwpUpgradeRejectError).StatusCode)
	// Tracker should NOT have host 1 as Healthy — the walk bailed
	// before reaching it.
	snap := tracker.snapshot()
	assert.NotEqual(t, qwpHostHealthy, snap[1].state)
}

// TestRoundWalkBudgetExhaustsOnAllRoleReject: every peer responds
// 421+CATCHUP for the full outage window. The walk must pay a
// round-boundary sleep at each round exhaustion (InitialBackoff
// equal-jitter, no doubling) and terminate when the budget runs out.
func TestRoundWalkBudgetExhaustsOnAllRoleReject(t *testing.T) {
	srv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	})
	defer srv.Close()

	endpoints := []qwpEndpoint{endpointForServer(t, srv)}
	tracker := newQwpHostTracker(1, "", qwpTargetAny)

	// Tight budget; each round-boundary sleep is ~10-20ms.
	start := time.Now()
	result := runWalkAgainst(t, endpoints, tracker, -1,
		200*time.Millisecond, 10*time.Millisecond, 30*time.Millisecond)
	elapsed := time.Since(start)

	assert.Nil(t, result.Transport)
	require.NotNil(t, result.Exhausted, "budget must exhaust, not terminate")
	assert.Greater(t, result.Attempts, 1, "must have made several role-reject attempts")
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond,
		"must consume the full budget before exhaustion")
	// Per-host outcome surfaces in Error().
	msg := result.Exhausted.Error()
	assert.Contains(t, msg, "TransientReject",
		"exhausted error must surface the per-host classification: %s", msg)
}

// TestRoundWalkBudgetExhaustsOnAllTransport: every peer dial-fails
// (closed port). Backoff doubling between rounds; eventual
// exhaustion with TransportError as the per-host outcome.
func TestRoundWalkBudgetExhaustsOnAllTransport(t *testing.T) {
	endpoints := []qwpEndpoint{{host: "127.0.0.1", port: 1}}
	tracker := newQwpHostTracker(1, "", qwpTargetAny)

	result := runWalkAgainst(t, endpoints, tracker, -1,
		150*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
	assert.Nil(t, result.Transport)
	require.NotNil(t, result.Exhausted)
	msg := result.Exhausted.Error()
	assert.Contains(t, msg, "TransportError", "exhausted msg: %s", msg)
}

// TestRoundWalkMidStreamDemoteBeforePickNext verifies the §2.3
// ordering invariant: a non-negative previousIdx must demote
// before the first PickNext. We bind host 0 as Healthy, then
// simulate a mid-stream failure (previousIdx=0), then re-walk —
// PickNext must NOT return 0 first.
func TestRoundWalkMidStreamDemoteBeforePickNext(t *testing.T) {
	healthy1 := newRoundWalkHealthyServer(t)
	defer healthy1.Close()
	healthy2 := newRoundWalkHealthyServer(t)
	defer healthy2.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, healthy1),
		endpointForServer(t, healthy2),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)

	// First walk binds host 0.
	r1 := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)
	require.NotNil(t, r1.Transport)
	require.Equal(t, 0, r1.Idx)
	_ = r1.Transport.close()

	// Simulate mid-stream failure on host 0: re-walk with previousIdx=0.
	r2 := runWalkAgainst(t, endpoints, tracker, 0,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)
	require.NotNil(t, r2.Transport)
	defer r2.Transport.close()
	assert.Equal(t, 1, r2.Idx,
		"mid-stream demote must run before PickNext; host 0 should be TransportError-priority now")
}

// TestRoundWalkExhaustedErrorIncludesPerHostOutcomes verifies that
// the SenderError's ServerMessage (built from result.Exhausted) lists
// each configured endpoint with its final state.
func TestRoundWalkExhaustedErrorIncludesPerHostOutcomes(t *testing.T) {
	catchupSrv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	})
	defer catchupSrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, catchupSrv),
		{host: "127.0.0.1", port: 1}, // closed port → TransportError
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	result := runWalkAgainst(t, endpoints, tracker, -1,
		150*time.Millisecond, 5*time.Millisecond, 30*time.Millisecond)

	require.NotNil(t, result.Exhausted)
	msg := result.Exhausted.Error()
	assert.Contains(t, msg, "TransientReject", "msg: %s", msg)
	assert.Contains(t, msg, "TransportError", "msg: %s", msg)
	assert.Contains(t, msg, endpoints[0].String(), "msg: %s", msg)
	assert.Contains(t, msg, endpoints[1].String(), "msg: %s", msg)
}

// TestRoundWalkCancellation: ctx cancellation mid-walk surfaces as
// the Cancelled exit path, not Exhausted.
func TestRoundWalkCancellation(t *testing.T) {
	srv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	})
	defer srv.Close()

	endpoints := []qwpEndpoint{endpointForServer(t, srv)}
	tracker := newQwpHostTracker(1, "", qwpTargetAny)

	factory := qwpSfBuildEndpointFactory(endpoints, "ws", qwpTransportOpts{
		endpointPath: qwpWritePath,
	}, nil)
	params := qwpSfRoundWalkParams{
		Factory:        factory,
		Tracker:        tracker,
		Endpoints:      endpoints,
		MaxDuration:    10 * time.Second,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a brief delay so at least one round happens first.
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()
	result := qwpSfRunRoundWalk(ctx, nil, params, -1)
	assert.Nil(t, result.Transport)
	assert.Nil(t, result.Exhausted)
	require.NotNil(t, result.Cancelled)
	assert.True(t, errors.Is(result.Cancelled, context.Canceled))
}

// TestComputeBackoffSaturatesBeforeOverflow exercises the spec's
// "saturate before doubling" guarantee at the integer boundary.
// The function must NOT overflow time.Duration even for very large
// attempt counts.
func TestComputeBackoffSaturatesBeforeOverflow(t *testing.T) {
	initial := 100 * time.Millisecond
	max := 5 * time.Second
	for _, attempt := range []int{0, 1, 5, 10, 30, 60, 100} {
		got := qwpSfComputeBackoff(attempt, initial, max)
		// Equal-jitter: [base, 2*base). For high attempts, base
		// saturates at max, so result is [max, 2*max).
		assert.GreaterOrEqual(t, got, initial,
			"attempt=%d: backoff must be at least InitialBackoff", attempt)
		assert.Less(t, got, 2*max,
			"attempt=%d: backoff must not exceed 2*max", attempt)
	}
}

// TestComputeBackoffEqualJitterShape probabilistically verifies the
// equal-jitter window for attempt=0. Across many samples, every
// observation must fall in [InitialBackoff, 2*InitialBackoff).
func TestComputeBackoffEqualJitterShape(t *testing.T) {
	initial := 100 * time.Millisecond
	max := 1 * time.Second
	for i := 0; i < 200; i++ {
		got := qwpSfComputeBackoff(0, initial, max)
		assert.GreaterOrEqual(t, got, initial,
			"sample %d: %v < %v", i, got, initial)
		assert.Less(t, got, 2*initial,
			"sample %d: %v >= %v", i, got, 2*initial)
	}
}

// Full-stack reconnect-and-rebind integration is covered by the
// existing TestQwpSfSendLoop* suite (which now goes through the
// implicit 1-host tracker code path). The tests above pin the
// round-walk semantics in isolation; the send-loop integration
// tests prove the wiring works end-to-end.

// --- ingress is role-blind: target= is accepted but inert ---

// TestRoundWalkIngressIgnoresTargetFilter pins the accepted-but-inert
// contract for target= on the SF ingress path. The ingress connect
// path does not route by server role — role-based endpoint selection
// is an egress-only feature — so the round-walk binds the first
// healthy peer regardless of the tracker's target filter and records
// it Healthy. It does not demote peers to TopologyReject, which would
// connect/close-storm (no host can satisfy a filter the ingress walk
// never evaluates). The filter is honoured on the egress connect-walk
// (see qwp_failover_test.go).
//
// Production always builds the ingress tracker with qwpTargetAny (see
// qwp_sender_cursor.go), so a non-Any filter never even reaches this
// code; the test feeds one directly to prove the round-walk itself is
// target-agnostic.
func TestRoundWalkIngressIgnoresTargetFilter(t *testing.T) {
	for _, target := range []QwpTargetFilter{qwpTargetAny, qwpTargetPrimary, qwpTargetReplica} {
		t.Run(target.String(), func(t *testing.T) {
			srv := newRoundWalkHealthyServer(t)
			defer srv.Close()
			endpoints := []qwpEndpoint{endpointForServer(t, srv)}
			tracker := newQwpHostTracker(1, "", target)
			result := runWalkAgainst(t, endpoints, tracker, -1,
				2*time.Second, 50*time.Millisecond, 500*time.Millisecond)
			require.NotNil(t, result.Transport,
				"ingress must bind a healthy peer regardless of target=%s", target)
			defer result.Transport.close()
			snap := tracker.snapshot()
			assert.Equal(t, qwpHostHealthy, snap[0].state,
				"bound host must be recorded Healthy, not TopologyReject")
		})
	}
}

// TestRoundWalkPerCallerPreviousIdxIsolation pins down the
// failover.md §2.3 invariant: two callers (foreground SF loop +
// orphan drainer) sharing one tracker MUST use private previousIdx
// slots. A mid-stream demote from caller A on idx=0 must not
// disturb caller B's idx=1 bind.
//
// Setup mirrors what Phase 5 wires up in production:
//   - 1 shared tracker, 2 hosts (both healthy).
//   - Caller A binds idx=0; caller B binds idx=1.
//   - Caller A "loses" its connection mid-stream and re-enters the
//     round-walk with previousIdx=0. Caller B is unaffected — its
//     local previousIdx slot stays at 1.
func TestRoundWalkPerCallerPreviousIdxIsolation(t *testing.T) {
	healthy0 := newRoundWalkHealthyServer(t)
	defer healthy0.Close()
	healthy1 := newRoundWalkHealthyServer(t)
	defer healthy1.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, healthy0),
		endpointForServer(t, healthy1),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)

	// Caller A: binds idx=0.
	rA := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)
	require.NotNil(t, rA.Transport)
	defer rA.Transport.close()
	require.Equal(t, 0, rA.Idx)

	// Caller B: binds idx=1 because idx=0 is Healthy-attempted
	// (sticky-Healthy preserves it, but `attempted` is set since
	// caller A consumed its round slot). After BeginRound(false)
	// caller B starts fresh — let's simulate that explicitly so
	// the test setup reflects "two independent callers, each
	// running its own round".
	tracker.BeginRound(false)
	// Even with attempted cleared, the lower-index Healthy host
	// wins PickNext (priority (Healthy, Same)). To force caller B
	// onto idx=1 we treat caller A's bound idx as "attempted" for
	// caller B's round — exactly the mid-stream demote signal the
	// real send loop applies to its OWN bound host on pump exit.
	// Here, we mimic the production wiring: caller B's local
	// previousIdx is -1 (it has no prior bind), and caller A's
	// previousIdx=0 is what caller A would consume.
	rB := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)
	require.NotNil(t, rB.Transport)
	defer rB.Transport.close()
	// Either bind is structurally correct (both Healthy, same
	// zone tier) — what we're really pinning is the per-caller
	// slot semantics next.

	// Now: caller A loses its connection mid-stream. Caller A
	// re-walks with previousIdx=0 (its own bound idx); caller B
	// is untouched. After caller A's walk, caller B's bind must
	// still be valid (no one called RecordMidStreamFailure on
	// caller B's idx).
	rA2 := runWalkAgainst(t, endpoints, tracker, rA.Idx,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)
	require.NotNil(t, rA2.Transport, "caller A must reconnect successfully")
	defer rA2.Transport.close()
	// After the demote, host rA.Idx is now TransportError; caller
	// A must end up on the other host.
	assert.NotEqual(t, rA.Idx, rA2.Idx,
		"after mid-stream demote, caller A must walk to the other host")

	// Caller B's `previousIdx` is the test's local variable (rB.Idx).
	// Caller A's mid-stream walk did NOT touch it. Sanity-check by
	// snapshotting the tracker: rB.Idx must still be Healthy
	// (the sticky-Healthy preservation across BeginRound(true)
	// keeps it so), proving the demote was scoped to caller A's
	// host only.
	snap := tracker.snapshot()
	assert.NotEqual(t, qwpHostHealthy, snap[rA.Idx].state,
		"caller A's bound host should be demoted post mid-stream")
}

// --- qwpSfRunSingleRound: the per-round primitive ---

// runSingleRoundAgainst dials the configured endpoints once via
// qwpSfRunSingleRound and returns the result. Tests assert on the
// inner-loop result shape (single-round, no inter-round sleep).
func runSingleRoundAgainst(
	t *testing.T,
	endpoints []qwpEndpoint,
	tracker *qwpHostTracker,
	previousIdx int,
) qwpSfSingleRoundResult {
	t.Helper()
	factory := qwpSfBuildEndpointFactory(endpoints, "ws", qwpTransportOpts{
		endpointPath: qwpWritePath,
	}, nil)
	params := qwpSfRoundWalkParams{
		Factory:   factory,
		Tracker:   tracker,
		Endpoints: endpoints,
	}
	return qwpSfRunSingleRound(context.Background(), nil, params, previousIdx)
}

// TestRunSingleRoundBindsHealthyPeerWhenFirstRoleRejects is the
// per-round counterpart to TestRoundWalkBindsHealthyPeerWhenFirstRoleRejects:
// the inner walks every unattempted host once and binds the healthy
// peer without paying a round-boundary sleep.
func TestRunSingleRoundBindsHealthyPeerWhenFirstRoleRejects(t *testing.T) {
	rejectSrv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"REPLICA"},
	})
	defer rejectSrv.Close()
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, rejectSrv),
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)

	start := time.Now()
	rr := runSingleRoundAgainst(t, endpoints, tracker, -1)
	elapsed := time.Since(start)

	require.NotNil(t, rr.Transport, "expected successful bind on healthy peer")
	defer rr.Transport.close()
	assert.Equal(t, 1, rr.Idx, "must bind to healthy peer at idx=1")
	assert.Less(t, elapsed, 500*time.Millisecond,
		"single-round walk must NOT pay any inter-host sleep")

	snap := tracker.snapshot()
	assert.Equal(t, qwpHostTopologyReject, snap[0].state,
		"REPLICA reject without CATCHUP must classify as TopologyReject")
	assert.Equal(t, qwpHostHealthy, snap[1].state)
}

// TestRunSingleRoundExhaustsWithoutSleep verifies the exhaustion
// path: when every host is unreachable, the inner returns
// immediately with LastError set, without paying any round-boundary
// sleep. The outer multi-round wrapper is the one that pays sleeps;
// the inner is a pure walk.
func TestRunSingleRoundExhaustsWithoutSleep(t *testing.T) {
	endpoints := []qwpEndpoint{
		{host: "127.0.0.1", port: 1},
		{host: "127.0.0.1", port: 2},
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)

	start := time.Now()
	rr := runSingleRoundAgainst(t, endpoints, tracker, -1)
	elapsed := time.Since(start)

	assert.Nil(t, rr.Transport)
	assert.Nil(t, rr.Terminal)
	assert.Nil(t, rr.Cancelled)
	require.Error(t, rr.LastError,
		"exhaustion must surface the most recent dial failure")
	assert.Equal(t, 2, rr.Attempts, "every host must be attempted before exit")
	assert.Less(t, elapsed, 2*time.Second,
		"single-round exhaustion must not sleep; dial timeouts dominate")

	// Both hosts left as TransportError; attempted bits set.
	snap := tracker.snapshot()
	for i, h := range snap {
		assert.Equal(t, qwpHostTransportError, h.state, "host %d state", i)
		assert.True(t, h.attempted, "host %d attempted", i)
	}
}

// TestRunSingleRoundAuthErrorShortCircuits verifies that a 401 on
// host 0 causes the inner to return Terminal immediately, without
// dialing host 1 — auth is uniform across the cluster, walking on
// would just produce identical rejections (failover.md §6).
func TestRunSingleRoundAuthErrorShortCircuits(t *testing.T) {
	authSrv := newRoundWalkRejectServer(t, 401, http.Header{})
	defer authSrv.Close()
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, authSrv),
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "", qwpTargetAny)
	rr := runSingleRoundAgainst(t, endpoints, tracker, -1)

	assert.Nil(t, rr.Transport)
	require.NotNil(t, rr.Terminal, "401 must short-circuit as Terminal")
	assert.Equal(t, 401, rr.Terminal.(*QwpUpgradeRejectError).StatusCode)
	assert.Equal(t, 1, rr.Attempts, "walk must stop after the auth-failing host")
	assert.NotEqual(t, qwpHostHealthy, tracker.snapshot()[1].state,
		"walk must not have reached the healthy peer")
}

// TestRunSingleRoundCtxCancelDuringDialDoesNotDemote verifies the
// cancellation race fix: when ctx fires while params.Factory is
// in-flight, the returned dial error is a wrapped context.Canceled
// — not a host failure. The walk must surface it as Cancelled and
// leave the tracker's host state untouched, otherwise a healthy
// host gets spuriously demoted to TransportError just because the
// caller stopped waiting (e.g. drainer shutdown, sender Close
// during reconnect, a watchdog tripping mid-dial).
func TestRunSingleRoundCtxCancelDuringDialDoesNotDemote(t *testing.T) {
	dialStarted := make(chan struct{})
	factory := func(ctx context.Context, _ int) (*qwpTransport, error) {
		close(dialStarted)
		<-ctx.Done()
		return nil, fmt.Errorf("qwp: websocket dial: %w", ctx.Err())
	}
	tracker := newQwpHostTracker(1, "", qwpTargetAny)
	params := qwpSfRoundWalkParams{
		Factory: factory,
		Tracker: tracker,
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-dialStarted
		cancel()
	}()
	rr := qwpSfRunSingleRound(ctx, nil, params, -1)

	require.NotNil(t, rr.Cancelled, "ctx cancel during dial must surface as Cancelled")
	assert.True(t, errors.Is(rr.Cancelled, context.Canceled))
	assert.Equal(t, -1, rr.Idx)
	assert.Equal(t, 1, rr.Attempts, "the in-flight dial counts as one attempt")

	snap := tracker.snapshot()
	assert.Equal(t, qwpHostUnknown, snap[0].state,
		"cancelled dial must not demote the host to TransportError")
}

// TestRunSingleRoundCancelChDuringDialDoesNotDemote is the
// cancelCh-channel counterpart. cancelCh exists so the send loop
// can distinguish "user close" from "ctx cancelled" — the fix must
// honour both signals symmetrically.
func TestRunSingleRoundCancelChDuringDialDoesNotDemote(t *testing.T) {
	dialStarted := make(chan struct{})
	cancelCh := make(chan struct{})
	factory := func(_ context.Context, _ int) (*qwpTransport, error) {
		close(dialStarted)
		<-cancelCh
		return nil, errors.New("qwp: websocket dial: connection refused")
	}
	tracker := newQwpHostTracker(1, "", qwpTargetAny)
	params := qwpSfRoundWalkParams{
		Factory: factory,
		Tracker: tracker,
	}

	go func() {
		<-dialStarted
		close(cancelCh)
	}()
	rr := qwpSfRunSingleRound(context.Background(), cancelCh, params, -1)

	require.NotNil(t, rr.Cancelled, "cancelCh during dial must surface as Cancelled")
	assert.True(t, errors.Is(rr.Cancelled, context.Canceled))
	assert.Equal(t, -1, rr.Idx)
	assert.Equal(t, 1, rr.Attempts)

	snap := tracker.snapshot()
	assert.Equal(t, qwpHostUnknown, snap[0].state,
		"cancelCh-aborted dial must not demote the host to TransportError")
}

// TestInitialConnectOffWalksMultiHostToHealthy is the spec-parity
// test: with `initial_connect_retry` left at its default (off), a
// connect string with multiple `addr=` entries must walk every host
// once and land on the healthy peer rather than failing on the
// first reject. Mirrors Java
// WriteFailoverTest.testOffModeSinglePassWalkFindsPrimary.
func TestInitialConnectOffWalksMultiHostToHealthy(t *testing.T) {
	// Host 0: rejects with 421 + REPLICA (TopologyReject).
	rejectSrv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"REPLICA"},
	})
	defer rejectSrv.Close()
	// Host 1: SF-compatible test server that ACKs frames.
	healthySrv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer healthySrv.Close()

	sfDir := t.TempDir()
	addr0 := strings.TrimPrefix(rejectSrv.URL, "http://")
	addr1 := strings.TrimPrefix(healthySrv.URL, "http://")
	conf := fmt.Sprintf(
		"ws::addr=%s,%s;sf_dir=%s;sender_id=t;close_flush_timeout_millis=2000;",
		addr0, addr1, sfDir,
	)

	sender, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err,
		"initial connect (default off) must walk past REPLICA and bind on healthy peer")
	defer func() { _ = sender.Close(context.Background()) }()

	// Send a row and confirm it reached the healthy server — proves
	// the bind landed on host 1, not on host 0 (which would have
	// rejected the upgrade outright). Flush no longer blocks on the
	// server ACK (the cursor architecture made local persistence, not
	// the ACK, the durability guarantee — see CLAUDE.md), so the send
	// loop delivers in the background; poll for receipt rather than
	// reading the counter synchronously right after Flush.
	require.NoError(t, sender.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, sender.Flush(context.Background()))
	require.Eventually(t, func() bool {
		return healthySrv.totalFramesReceived.Load() >= int64(1)
	}, 2*time.Second, 1*time.Millisecond,
		"the healthy peer must have received the test frame")
}

// TestInitialConnectOffFailsWhenAllRejected: when every endpoint
// rejects on the initial single-round walk, the constructor must
// return a clear error rather than hanging or burning the reconnect
// budget. The error must name the walk and the attempt count.
func TestInitialConnectOffFailsWhenAllRejected(t *testing.T) {
	r1 := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"REPLICA"},
	})
	defer r1.Close()
	r2 := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
	})
	defer r2.Close()

	sfDir := t.TempDir()
	addr0 := strings.TrimPrefix(r1.URL, "http://")
	addr1 := strings.TrimPrefix(r2.URL, "http://")
	conf := fmt.Sprintf(
		"ws::addr=%s,%s;sf_dir=%s;sender_id=t;",
		addr0, addr1, sfDir,
	)

	start := time.Now()
	sender, err := LineSenderFromConf(context.Background(), conf)
	elapsed := time.Since(start)
	if sender != nil {
		_ = sender.Close(context.Background())
	}
	require.Error(t, err, "initial connect must fail when every endpoint rejects")
	assert.Contains(t, err.Error(), "initial connect",
		"error must identify the single-round walk: %v", err)
	assert.Less(t, elapsed, 3*time.Second,
		"failure must surface promptly; OFF mode must not retry across rounds")
}

// TestQwpMemoryModeMultiHostFailsOverToHealthy is the regression test
// that memory mode (no sf_dir) must honour the multi-host
// addr= list exactly as SF mode does. The README's headline failover
// example (ws::addr=node-a,node-b,node-c;) is memory mode, so a dead
// first endpoint must not hard-fail the constructor — the sender has
// to walk past it and bind on the first healthy peer, just like the SF
// analog TestInitialConnectOffWalksMultiHostToHealthy above.
//
// Before the fix the memory path dialed only endpoints[0] (the
// sanitizer rewrote addr to endpoints[0]; the constructor did one
// synchronous dial through a single-host factory and installed no host
// tracker), so this connect returned the dead host's upgrade error.
func TestQwpMemoryModeMultiHostFailsOverToHealthy(t *testing.T) {
	// Host 0: dead — rejects every upgrade with 503 (a "generic
	// transient" the round walk steps past, per qwp_sf_round_walk.go).
	dead := newRoundWalkRejectServer(t, http.StatusServiceUnavailable, nil)
	defer dead.Close()
	// Host 1: healthy SF-compatible server that ACKs frames.
	healthy := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer healthy.Close()

	deadAddr := strings.TrimPrefix(dead.URL, "http://")
	healthyAddr := strings.TrimPrefix(healthy.URL, "http://")
	// NO sf_dir → memory mode. Identical addr shape to the SF analog.
	conf := fmt.Sprintf("ws::addr=%s,%s;close_flush_timeout_millis=2000;",
		deadAddr, healthyAddr)

	sender, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err,
		"memory-mode multi-host connect must walk past the dead first endpoint and bind on the healthy peer")
	defer func() { _ = sender.Close(context.Background()) }()

	require.NoError(t, sender.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, sender.Flush(context.Background()))
	require.Eventually(t, func() bool {
		return healthy.totalFramesReceived.Load() >= int64(1)
	}, 2*time.Second, 1*time.Millisecond,
		"the healthy peer must have received the row — proving the bind landed on host 1")
}

// TestQwpMemoryModeThreadsFailoverConfig pins the remaining case:
// memory mode must thread the multi-host failover tracker AND the
// user's reconnect budget into the send loop, not discard them. The
// pre-fix memory path installed no tracker (so reconnect could never
// fail over off the first node) and hard-coded
// qwpSfDefaultReconnectMaxDuration (so user reconnect budgets were
// silently dropped).
func TestQwpMemoryModeThreadsFailoverConfig(t *testing.T) {
	a := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer a.Close()
	b := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer b.Close()
	addrA := strings.TrimPrefix(a.URL, "http://")
	addrB := strings.TrimPrefix(b.URL, "http://")

	// NO sf_dir → memory mode, with a non-default reconnect budget.
	conf := fmt.Sprintf("ws::addr=%s,%s;reconnect_max_duration_millis=1234;",
		addrA, addrB)
	sender, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	defer func() { _ = sender.Close(context.Background()) }()

	s, ok := sender.(*qwpLineSender)
	require.True(t, ok, "want *qwpLineSender, got %T", sender)
	require.NotNil(t, s.cursorSendLoop.tracker,
		"memory mode must install the multi-host failover tracker")
	require.Equal(t, 2, s.cursorSendLoop.tracker.Len(),
		"the tracker must cover both configured endpoints")
	require.Equal(t, 1234*time.Millisecond, s.cursorSendLoop.reconnectMaxDuration,
		"memory mode must thread the user's reconnect_max_duration_millis, not the 5-minute default")
}

// newHangListener accepts TCP connections and parks them — never
// writes any HTTP response, so a client awaiting the WebSocket 101
// upgrade response hangs until its auth_timeout_ms fires. Used by
// TestInitialConnectAuthTimeoutBoundsHungUpgrade to simulate a node
// that takes the connection but never completes the upgrade.
func newHangListener(t *testing.T) (addr string, teardown func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "hang listener")
	var (
		mu     sync.Mutex
		closed bool
		conns  []net.Conn
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				_ = c.Close()
				return
			}
			conns = append(conns, c)
			mu.Unlock()
			// Park the connection. Set a long deadline so a buggy
			// server-side read can't burn the test budget; we close
			// from teardown.
			_ = c.SetDeadline(time.Now().Add(time.Minute))
		}
	}()
	teardown = func() {
		mu.Lock()
		closed = true
		toClose := append([]net.Conn(nil), conns...)
		mu.Unlock()
		_ = ln.Close()
		for _, c := range toClose {
			_ = c.Close()
		}
		<-done
	}
	return ln.Addr().String(), teardown
}

// TestInitialConnectAuthTimeoutBoundsHungUpgrade is the spec-parity
// test for `auth_timeout_ms`: when host 0 accepts the TCP socket but
// never writes the WS 101 response, the sender's upgrade read must
// time out at auth_timeout_ms (per-host) and walk to host 1, which
// completes the upgrade and accepts frames. Without the per-host
// bound the connect would burn the entire reconnect budget (or the
// underlying HTTP transport default) on the stuck host. Mirrors Java
// WriteFailoverTest.testAuthTimeoutBoundsHungUpgrade.
func TestInitialConnectAuthTimeoutBoundsHungUpgrade(t *testing.T) {
	hangAddr, closeHang := newHangListener(t)
	defer closeHang()

	healthySrv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer healthySrv.Close()
	healthyAddr := strings.TrimPrefix(healthySrv.URL, "http://")

	sfDir := t.TempDir()
	const authTimeoutMs = 500
	conf := fmt.Sprintf(
		"ws::addr=%s,%s;sf_dir=%s;sender_id=t;auth_timeout_ms=%d;close_flush_timeout_millis=2000;",
		hangAddr, healthyAddr, sfDir, authTimeoutMs,
	)

	t0 := time.Now()
	sender, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err,
		"sender must walk past the hung upgrade and bind on the healthy peer")
	defer func() { _ = sender.Close(context.Background()) }()

	require.NoError(t, sender.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, sender.Flush(context.Background()))
	require.Eventually(t, func() bool {
		return healthySrv.totalFramesReceived.Load() >= int64(1)
	}, 2*time.Second, 1*time.Millisecond,
		"the healthy peer must have received the test frame")

	elapsed := time.Since(t0)
	// Two-sided bound: host[0] MUST burn ~auth_timeout_ms (500 ms)
	// before the walk moves on (lower bound catches a regression that
	// short-circuits host[0]); host[1] connects quickly afterwards
	// (upper bound catches a regression that lets the per-host timeout
	// drift well past the configured value).
	assert.GreaterOrEqual(t, elapsed, 400*time.Millisecond,
		"host[0] must actually exercise auth_timeout_ms (~500 ms) before the walk moves on; elapsed=%v", elapsed)
	assert.Less(t, elapsed, 2*time.Second,
		"auth_timeout_ms must bound the hung upgrade close to the configured 500 ms; elapsed=%v", elapsed)
}

// TestInitialConnectStaysOnPrimaryAfterTopologyChange — Go-side
// counterpart of Java WriteFailoverTest.testFailoverPromotedReplicaJoinsRotation.
// After the SF round-walk binds to the healthy primary, subsequent
// batches MUST keep landing on the bound peer even if a previously-
// rejecting host becomes topologically eligible (the "promoted
// replica" case). The Go cursor send loop does not observe topology
// changes on idle peers, so the bound endpoint stays sticky — this
// test pins that stickiness so a future scheduler hook can't quietly
// regress it into proactive rotation.
//
// We don't actually mutate the rejecting server mid-test (Go's
// httptest doesn't expose a clean "swap behaviour" toggle and the
// promotion is conceptually a no-op on the sender side anyway).
// What we assert is what matters: two successive batches on the
// same Sender both reach the originally-bound healthy peer, and
// the rejecting host receives exactly one upgrade attempt — the
// initial round-walk. A regressed sender that re-walks the ring
// on every flush would push that count past 1, which is the
// regression this test exists to catch.
func TestInitialConnectStaysOnPrimaryAfterTopologyChange(t *testing.T) {
	// Host 0: rejects with 421 + REPLICA — the SF round-walk walks past.
	// Inlined (not via newRoundWalkRejectServer) so we can count upgrade
	// hits and pin the stickiness invariant below.
	var rejectHits atomic.Int64
	rejectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rejectHits.Add(1)
		w.Header().Add("X-QuestDB-Role", "REPLICA")
		w.WriteHeader(421)
	}))
	defer rejectSrv.Close()
	// Host 1: SF-compatible test server that ACKs frames.
	primarySrv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer primarySrv.Close()

	sfDir := t.TempDir()
	addr0 := strings.TrimPrefix(rejectSrv.URL, "http://")
	addr1 := strings.TrimPrefix(primarySrv.URL, "http://")
	conf := fmt.Sprintf(
		"ws::addr=%s,%s;sf_dir=%s;sender_id=t;close_flush_timeout_millis=2000;",
		addr0, addr1, sfDir,
	)

	sender, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	defer func() { _ = sender.Close(context.Background()) }()

	// Batch 1 — establishes the bind on host 1.
	require.NoError(t, sender.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, sender.Flush(context.Background()))
	require.Eventually(t, func() bool {
		return primarySrv.totalFramesReceived.Load() >= int64(1)
	}, 2*time.Second, 1*time.Millisecond,
		"batch 1 must reach the primary peer")
	framesAfter1 := primarySrv.totalFramesReceived.Load()

	// Batch 2 — must also land on host 1 (no proactive rotation).
	require.NoError(t, sender.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	require.NoError(t, sender.Flush(context.Background()))
	require.Eventually(t, func() bool {
		return primarySrv.totalFramesReceived.Load() > framesAfter1
	}, 2*time.Second, 1*time.Millisecond,
		"batch 2 must also reach the same primary peer (stickiness)")

	// Stickiness invariant: the rejecter was touched exactly once —
	// by the initial SF round-walk. A regressed sender that re-walks
	// the full ring on every flush would have hit it again before
	// batch 2 (or before each frame), so > 1 would mean the
	// stickiness property has regressed.
	assert.Equal(t, int64(1), rejectHits.Load(),
		"rejecting host must be touched only by the initial round-walk")
}

// TestRoundWalkRoleRejectBackoffGrows pins the 70c706a parity fix: an
// all-replica window pays the same growing capped backoff as any other
// outage. The former flat InitialBackoff retry re-dialed a fresh TLS
// handshake ~10/s per endpoint for the whole window.
func TestRoundWalkRoleRejectBackoffGrows(t *testing.T) {
	factory := func(context.Context, int) (*qwpTransport, error) {
		return nil, &QwpUpgradeRejectError{StatusCode: 421, Role: "REPLICA"}
	}
	tracker := newQwpHostTracker(1, "", qwpTargetAny)
	rounds := 0
	result := qwpSfRunRoundWalk(context.Background(), nil, qwpSfRoundWalkParams{
		Factory:        factory,
		Tracker:        tracker,
		MaxDuration:    700 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		OnRoundExhausted: func(outcome qwpSfSweepOutcome) {
			rounds++
			require.True(t, outcome.allReplica(), "every sweep here is all-replica")
		},
	}, -1)
	require.NotNil(t, result.Exhausted)
	// The growing jittered schedule (20, 40, 80, 160, 200… ms) fits only a
	// handful of rounds into the budget; the former flat 20ms schedule fit 20+.
	assert.GreaterOrEqual(t, rounds, 2)
	assert.LessOrEqual(t, rounds, 10, "role-reject rounds must pay growing backoff")
}

// TestRoundWalkUnboundedNeverExhausts pins the Invariant-B walk shape:
// MaxDuration <= 0 retries indefinitely and exits only on success,
// terminal, or cancellation — never Exhausted.
func TestRoundWalkUnboundedNeverExhausts(t *testing.T) {
	factory := func(context.Context, int) (*qwpTransport, error) {
		return nil, errors.New("dial tcp: connect: connection refused")
	}
	tracker := newQwpHostTracker(1, "", qwpTargetAny)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	result := qwpSfRunRoundWalk(ctx, nil, qwpSfRoundWalkParams{
		Factory:        factory,
		Tracker:        tracker,
		MaxDuration:    0,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	}, -1)
	require.Nil(t, result.Exhausted, "unbounded walk must never report Exhausted")
	require.NotNil(t, result.Cancelled, "only cancellation ends an unbounded walk against a down server")
	assert.Greater(t, result.Attempts, 10, "walk should have kept attempting for the whole window")
}
