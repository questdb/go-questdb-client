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
	"strings"
	"time"
)

// qwpSfRoundWalkResult is returned by qwpSfRunRoundWalk on exit and
// captures everything the caller needs to wrap into the appropriate
// SenderError surface (success, terminal, or budget-exhausted).
type qwpSfRoundWalkResult struct {
	// Transport is non-nil on success; the caller takes ownership and
	// must close it on shutdown.
	Transport *qwpTransport
	// Idx is the host index Transport was bound to, or -1 on
	// failure. Callers should record this back into their per-caller
	// previousIdx slot so the next round-walk (after a mid-stream
	// failure) can demote correctly.
	Idx int
	// Attempts counts dial attempts during this walk (success
	// returns the number of attempts including the successful one).
	Attempts int
	// Terminal is a non-nil typed reject when an Auth-error (401/403)
	// halts the walk per failover.md §6. Callers convert it to a
	// CategorySecurityError SenderError.
	Terminal *QwpUpgradeRejectError
	// Exhausted is non-nil when the wall-clock budget ran out. Wraps
	// the last underlying dial error plus a per-host snapshot for
	// diagnostics.
	Exhausted *qwpSfRoundWalkExhaustedError
	// Cancelled is non-nil when ctx or cancelCh fired during the
	// walk. Holds ctx.Err() so the caller can decide whether to
	// shut down silently or surface the cancellation.
	Cancelled error
}

// qwpSfRoundWalkExhaustedError surfaces a per-outage summary when
// the round-walk runs out of wall-clock budget without binding. The
// per-host outcomes lift the spec §13.4 diagnostics intent into the
// error payload so the user-visible SenderError can name which hosts
// role-rejected vs transport-errored.
type qwpSfRoundWalkExhaustedError struct {
	// Elapsed is the wall-clock time the outage consumed (from the
	// first failed dial to budget exhaustion).
	Elapsed time.Duration
	// Attempts is the total dial attempts during the outage.
	Attempts int
	// LastError is the most recent dial failure, exposed via Unwrap.
	LastError error
	// HostOutcomes is a snapshot of the tracker's per-host entries
	// at exhaustion. The slice index matches the connect-string
	// addr= ordering.
	HostOutcomes []qwpHostEntry
	// Endpoints, when non-nil, is the parallel list of addresses
	// the walk attempted, in addr= order. Lets the error message
	// surface "h1:9000 role-rejected, h2:9000 transport-error".
	// Optional — single-host callers may leave it nil.
	Endpoints []qwpEndpoint
}

// Error implements the error interface. The format is intentionally
// machine-friendly so the SenderError.ServerMessage can carry it
// verbatim and downstream log parsers can pick out the structured
// pieces.
func (e *qwpSfRoundWalkExhaustedError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "reconnect budget exhausted after %s / %d attempts",
		e.Elapsed.Round(time.Millisecond), e.Attempts)
	if len(e.HostOutcomes) > 0 {
		b.WriteString(" (host outcomes:")
		for i, h := range e.HostOutcomes {
			addr := ""
			if i < len(e.Endpoints) {
				addr = " " + e.Endpoints[i].String()
			}
			fmt.Fprintf(&b, " [%d%s state=%s zone=%s]", i, addr, h.state, h.zoneTier)
		}
		b.WriteString(")")
	}
	if e.LastError != nil {
		fmt.Fprintf(&b, ": %v", e.LastError)
	}
	return b.String()
}

// Unwrap exposes the last underlying error so errors.Is / errors.As
// can match on the dial failure beneath the exhaustion wrapper.
func (e *qwpSfRoundWalkExhaustedError) Unwrap() error {
	return e.LastError
}

// qwpSfRoundWalkParams bundles the immutable inputs of the walk so
// the call site stays readable. Built once per logical caller and
// reused across reconnect cycles.
type qwpSfRoundWalkParams struct {
	// Factory dials the host at the given index. Implementations
	// own the idx → URL/auth/TLS mapping (see
	// qwpSfBuildEndpointFactory). May ignore idx for single-host
	// callers that ship a 1-host tracker.
	Factory qwpSfReconnectFactory
	// Tracker is the failover.md §2 host-health tracker. MUST have
	// Len() >= 1; the round-walk does not synthesize an implicit
	// one.
	Tracker *qwpHostTracker
	// Endpoints, when non-nil, is the parallel list of addresses
	// for budget-exhausted error formatting only. The factory owns
	// dial; endpoints[i] is purely diagnostic.
	Endpoints []qwpEndpoint
	// MaxDuration is the wall-clock outage budget
	// (reconnect_max_duration_millis per failover.md §7).
	MaxDuration time.Duration
	// InitialBackoff is the smallest pre-jitter sleep at round
	// exhaustion (reconnect_initial_backoff_millis).
	InitialBackoff time.Duration
	// MaxBackoff caps the pre-jitter sleep (reconnect_max_backoff_millis).
	// Post-jitter sleep may exceed it (equal-jitter shape).
	MaxBackoff time.Duration
	// OnAttempt, when non-nil, fires before each dial so callers
	// can bump observability counters (totalReconnectAttempts,
	// per-attempt status, etc.).
	OnAttempt func()
}

// qwpSfSingleRoundResult is the inner-loop return shape for one walk
// through every unattempted host in the tracker. qwpSfRunRoundWalk
// wraps this in a multi-round backoff loop; the InitialConnectOff
// branch in newQwpCursorLineSenderFromConf calls qwpSfRunSingleRound
// directly so a multi-host config still gets a full sweep on initial
// connect (failover.md §1.2 / §4.2; Java parity with
// QwpWebSocketSender.buildAndConnect).
//
// Exactly one of Transport / Terminal / Cancelled is non-nil on
// non-exhaustion exits. When all three are nil, the round was
// exhausted (every host attempted, no bind) and LastError /
// LastWasRoleReject describe the last dial.
type qwpSfSingleRoundResult struct {
	// Transport is non-nil on success; caller takes ownership.
	Transport *qwpTransport
	// Idx is the bound endpoint index, or -1 on any non-success exit.
	Idx int
	// Attempts is the dial count consumed during this round
	// (success inclusive).
	Attempts int
	// Terminal is set when the walk hits a 401/403 upgrade reject —
	// per failover.md §6, auth errors short-circuit failover.
	Terminal *QwpUpgradeRejectError
	// Cancelled is ctx.Err() (or context.Canceled when cancelCh
	// fired) when the walk was interrupted. Also non-nil for
	// misconfigurations (nil tracker / factory) so callers route
	// both via the same exit branch.
	Cancelled error
	// LastError is the most recent dial failure when the round
	// exhausted. Nil on success / terminal / cancelled exits.
	LastError error
	// LastWasRoleReject indicates the most recent failure was a
	// role-reject (421 + role header, or v2 SERVER_INFO target
	// mismatch). Drives the outer loop's round-boundary backoff
	// selection per §3.2.
	LastWasRoleReject bool
}

// qwpSfRunSingleRound walks every unattempted host in the tracker
// once, dialing each via params.Factory and classifying the outcome.
// Returns on the first of:
//
//   - successful bind (Transport set);
//   - terminal AuthError 401/403 (Terminal set) — failover.md §6;
//   - ctx or cancelCh cancellation (Cancelled set);
//   - round exhaustion (PickNext returns -1, no remaining
//     unattempted hosts).
//
// On exhaustion this function does NOT sleep and does NOT call
// BeginRound — those belong to the multi-round outer loop. Callers
// running a single-round walk (the InitialConnectOff branch) treat
// exhaustion as the terminal "all endpoints unreachable" condition.
//
// previousIdx >= 0 triggers RecordMidStreamFailure before the first
// PickNext (failover.md §2.3 ordering invariant). Pass -1 when no
// prior bind exists (initial connect).
func qwpSfRunSingleRound(
	ctx context.Context,
	cancelCh <-chan struct{},
	params qwpSfRoundWalkParams,
	previousIdx int,
) qwpSfSingleRoundResult {
	if params.Tracker == nil || params.Tracker.Len() == 0 {
		return qwpSfSingleRoundResult{
			Idx:       -1,
			Cancelled: fmt.Errorf("qwp/sf: round-walk requires a non-empty tracker"),
		}
	}
	if params.Factory == nil {
		return qwpSfSingleRoundResult{
			Idx:       -1,
			Cancelled: fmt.Errorf("qwp/sf: round-walk requires a factory"),
		}
	}

	var (
		attempts          int
		lastErr           error
		lastWasRoleReject bool
	)

	// Apply pending mid-stream demote before the first PickNext.
	// failover.md §2.3 normative ordering: reverse this and
	// sticky-Healthy preserves the just-failed host, putting it back
	// at the top of priority.
	if previousIdx >= 0 {
		params.Tracker.RecordMidStreamFailure(previousIdx)
	}

	for {
		if err := ctx.Err(); err != nil {
			return qwpSfSingleRoundResult{Idx: -1, Cancelled: err, Attempts: attempts}
		}
		if cancelCh != nil {
			select {
			case <-cancelCh:
				return qwpSfSingleRoundResult{
					Idx:       -1,
					Cancelled: context.Canceled,
					Attempts:  attempts,
				}
			default:
			}
		}

		idx := params.Tracker.PickNext()
		if idx < 0 {
			return qwpSfSingleRoundResult{
				Idx:               -1,
				Attempts:          attempts,
				LastError:         lastErr,
				LastWasRoleReject: lastWasRoleReject,
			}
		}

		// Dial host[idx].
		if params.OnAttempt != nil {
			params.OnAttempt()
		}
		attempts++
		t, err := params.Factory(ctx, idx)
		if err == nil && t != nil {
			// Post-upgrade classification per failover.md §5:
			//
			//   - v2 with SERVER_INFO: role byte is authoritative.
			//     Mismatch against target= → role-reject (transient if
			//     role==PRIMARY_CATCHUP, topology otherwise — same
			//     transient/topology split as a 421 + role reject).
			//   - v2 with CAP_ZONE: zone_id feeds RecordZone so the
			//     tracker's (state, zone) priority can route within
			//     the configured `zone=` neighbourhood.
			//   - v1 fallback (no SERVER_INFO): target=any binds; any
			//     other target produces TopologyReject because v1
			//     cannot supply the role byte (failover.md §5 wire-v1
			//     row). The operator either upgrades the server to v2
			//     or drops the target= filter.
			if t.serverInfo != nil {
				if t.serverInfo.ZoneId != "" {
					params.Tracker.RecordZone(idx, t.serverInfo.ZoneId)
				}
				if params.Tracker.target != qwpTargetAny &&
					!params.Tracker.target.accepts(t.serverInfo.Role) {
					_ = t.close()
					transient := t.serverInfo.Role == qwpRolePrimaryCatchup
					params.Tracker.RecordRoleReject(idx, transient)
					lastErr = fmt.Errorf(
						"qwp/sf: target=%s rejected peer with SERVER_INFO.role=%s",
						params.Tracker.target, qwpRoleName(t.serverInfo.Role))
					lastWasRoleReject = true
					continue
				}
			} else if params.Tracker.target != qwpTargetAny {
				_ = t.close()
				params.Tracker.RecordRoleReject(idx, false)
				lastErr = fmt.Errorf(
					"qwp/sf: target=%s requires QWP v2+; peer negotiated v1 (no SERVER_INFO available)",
					params.Tracker.target)
				lastWasRoleReject = true
				continue
			}
			params.Tracker.RecordSuccess(idx)
			return qwpSfSingleRoundResult{
				Transport: t,
				Idx:       idx,
				Attempts:  attempts,
			}
		}
		lastErr = err

		// Classify the failure. Typed *QwpUpgradeRejectError carries
		// the precise spec-relevant fields; everything else is a
		// generic transport error.
		var rej *QwpUpgradeRejectError
		if errors.As(err, &rej) {
			// AuthError (401 / 403): terminal per §6. Bypass failover.
			if rej.StatusCode == 401 || rej.StatusCode == 403 {
				return qwpSfSingleRoundResult{
					Idx:      -1,
					Attempts: attempts,
					Terminal: rej,
				}
			}
			// Record zone if the reject carried X-QuestDB-Zone.
			if rej.Zone != "" {
				params.Tracker.RecordZone(idx, rej.Zone)
			}
			// 421 + non-empty role: role-reject (transient or topology).
			// 421 without role, 404, 426, 503, etc.: generic transient.
			if rej.IsRoleReject() {
				params.Tracker.RecordRoleReject(idx, rej.IsCatchupRole())
				lastWasRoleReject = true
				continue
			}
			params.Tracker.RecordTransportError(idx)
			lastWasRoleReject = false
			continue
		}

		// Non-upgrade-reject failure: TCP/TLS dial error,
		// response-header timeout, etc. — all transient.
		params.Tracker.RecordTransportError(idx)
		lastWasRoleReject = false
	}
}

// qwpSfRunRoundWalk drives the failover.md §13.6 multi-round walk:
// each round calls qwpSfRunSingleRound; on exhaustion it pays one
// round-boundary sleep (equal-jitter exponential for transport
// rounds, flat InitialBackoff for role-reject rounds per §3.2),
// clamped to the remaining budget, then BeginRound(true) and
// retries. Returns on success, terminal AuthError, budget
// exhaustion, or cancellation.
//
// The result enum tells the caller which exit path was taken; only
// one of Transport / Terminal / Exhausted / Cancelled is non-nil.
// ctx is the master context; cancelCh, when non-nil, provides a
// secondary cancellation channel (used to distinguish "user close"
// from "ctx cancelled").
func qwpSfRunRoundWalk(
	ctx context.Context,
	cancelCh <-chan struct{},
	params qwpSfRoundWalkParams,
	previousIdx int,
) qwpSfRoundWalkResult {
	outageStart := time.Now()
	backoffAttempt := 0
	totalAttempts := 0
	enteringPreviousIdx := previousIdx

	for {
		rr := qwpSfRunSingleRound(ctx, cancelCh, params, enteringPreviousIdx)
		// previousIdx only demotes on the first inner call. Subsequent
		// rounds enter with -1 so a stale slot doesn't double-demote.
		enteringPreviousIdx = -1
		totalAttempts += rr.Attempts

		if rr.Transport != nil {
			return qwpSfRoundWalkResult{
				Transport: rr.Transport,
				Idx:       rr.Idx,
				Attempts:  totalAttempts,
			}
		}
		if rr.Terminal != nil {
			return qwpSfRoundWalkResult{
				Idx:      -1,
				Attempts: totalAttempts,
				Terminal: rr.Terminal,
			}
		}
		if rr.Cancelled != nil {
			return qwpSfRoundWalkResult{
				Idx:       -1,
				Cancelled: rr.Cancelled,
				Attempts:  totalAttempts,
			}
		}

		// Round exhausted. Pay one round-boundary sleep or terminate
		// if the budget is gone.
		elapsed := time.Since(outageStart)
		if elapsed >= params.MaxDuration {
			return qwpSfRoundWalkResult{
				Idx:      -1,
				Attempts: totalAttempts,
				Exhausted: buildExhaustedError(
					params.Tracker, params.Endpoints, elapsed, totalAttempts, rr.LastError),
			}
		}
		var sleep time.Duration
		if rr.LastWasRoleReject {
			// Role-reject: no exponential doubling. ComputeBackoff(0)
			// surfaces as EqualJitter(InitialBackoff). Reset the
			// counter so a subsequent transport-only round doesn't
			// inherit a stale attempt count.
			sleep = qwpSfComputeBackoff(0, params.InitialBackoff, params.MaxBackoff)
			backoffAttempt = 0
		} else {
			sleep = qwpSfComputeBackoff(backoffAttempt, params.InitialBackoff, params.MaxBackoff)
			backoffAttempt++
		}
		remaining := params.MaxDuration - elapsed
		if remaining <= 0 {
			return qwpSfRoundWalkResult{
				Idx:      -1,
				Attempts: totalAttempts,
				Exhausted: buildExhaustedError(
					params.Tracker, params.Endpoints, elapsed, totalAttempts, rr.LastError),
			}
		}
		if sleep > remaining {
			sleep = remaining
		}
		// Sleep interruptible by ctx + cancelCh.
		if !qwpSfSleepInterruptible(ctx, cancelCh, sleep) {
			return qwpSfRoundWalkResult{
				Idx:       -1,
				Cancelled: context.Canceled,
				Attempts:  totalAttempts,
			}
		}
		params.Tracker.BeginRound(true)
	}
}

// buildExhaustedError snapshots the tracker and packages the
// per-host outcomes into a typed *qwpSfRoundWalkExhaustedError.
// Pure formatter; no I/O.
func buildExhaustedError(
	tracker *qwpHostTracker,
	endpoints []qwpEndpoint,
	elapsed time.Duration,
	attempts int,
	lastErr error,
) *qwpSfRoundWalkExhaustedError {
	if lastErr == nil {
		lastErr = errors.New("no dial attempts succeeded")
	}
	return &qwpSfRoundWalkExhaustedError{
		Elapsed:      elapsed,
		Attempts:     attempts,
		LastError:    lastErr,
		HostOutcomes: tracker.snapshot(),
		Endpoints:    endpoints,
	}
}

// qwpSfComputeBackoff implements the failover.md §3 backoff
// function: doubling InitialBackoff up to MaxBackoff with
// saturate-before-overflow, then equal-jitter `[base, 2·base)`.
// The post-jitter sleep is NOT clamped to MaxBackoff — once base
// saturates the cap, the actual sleep lands in [max, 2·max), per
// the SF spec's intent that the post-jitter window stays positive.
//
// attempt is 0-based; ComputeBackoff(0) returns
// EqualJitter(InitialBackoff). The function is pure; callers
// supply the deadline check separately.
func qwpSfComputeBackoff(attempt int, initial, max time.Duration) time.Duration {
	if initial <= 0 {
		return 0
	}
	base := initial
	for i := 0; i < attempt && base < max; i++ {
		if base > max/2 {
			base = max
			break
		}
		base *= 2
	}
	if base > max {
		base = max
	}
	if base <= 0 {
		return 0
	}
	// Equal-jitter: [base, 2*base). rand.Int63n requires a positive
	// argument; the base > 0 guard above keeps that contract.
	return base + time.Duration(rand.Int63n(int64(base)))
}

// qwpSfSleepInterruptible blocks for d, returning early when ctx
// expires or cancelCh fires. Returns true if the full sleep
// completed, false if interrupted. Zero d returns immediately.
func qwpSfSleepInterruptible(ctx context.Context, cancelCh <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	if cancelCh == nil {
		select {
		case <-t.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-cancelCh:
		return false
	}
}
