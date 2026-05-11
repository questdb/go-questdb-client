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
	"net/http"
	"net/http/httptest"
	"strings"
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

// hostPortOf extracts host:port from an httptest URL.
func hostPortOf(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
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
	assert.Equal(t, 401, result.Terminal.StatusCode)
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

// TestRoundWalkRecordZoneFromRejectHeader: the X-QuestDB-Zone
// header on a 421 reject must feed RecordZone. Setup: client has
// zone=eu-west-1a; reject server returns zone=us-east-1a (Other);
// healthy server doesn't advertise (stays Unknown). After the walk,
// the rejected host's zone tier is Other.
func TestRoundWalkRecordZoneFromRejectHeader(t *testing.T) {
	rejectSrv := newRoundWalkRejectServer(t, 421, http.Header{
		"X-QuestDB-Role": []string{"PRIMARY_CATCHUP"},
		"X-QuestDB-Zone": []string{"us-east-1a"},
	})
	defer rejectSrv.Close()
	healthySrv := newRoundWalkHealthyServer(t)
	defer healthySrv.Close()

	endpoints := []qwpEndpoint{
		endpointForServer(t, rejectSrv),
		endpointForServer(t, healthySrv),
	}
	tracker := newQwpHostTracker(2, "eu-west-1a", qwpTargetAny)
	result := runWalkAgainst(t, endpoints, tracker, -1,
		2*time.Second, 50*time.Millisecond, 500*time.Millisecond)
	require.NotNil(t, result.Transport)
	defer result.Transport.close()

	snap := tracker.snapshot()
	assert.Equal(t, qwpZoneOther, snap[0].zoneTier,
		"reject server's zone=us-east-1a vs client zone=eu-west-1a must classify as Other")
	assert.Equal(t, qwpZoneUnknown, snap[1].zoneTier,
		"healthy server didn't advertise; tier stays Unknown")
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
