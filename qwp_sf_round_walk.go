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
	Terminal error
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
	// (reconnect_max_duration_millis per failover.md §7). <= 0 means
	// unbounded: the walk retries until success, terminal error, or
	// cancellation, and never returns Exhausted. The running send loop,
	// async initial connect, and background drainers use unbounded mode
	// (Invariant B: a store-and-forward client never gives up on a
	// transient outage); only the blocking sync initial connect passes
	// a positive budget.
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
	// OnEndpointFailed, when non-nil, fires after a dial fails for a real
	// host reason (not a cancellation), with the failed endpoint index and
	// the dial error. Drives the SenderEndpointAttemptFailed event.
	OnEndpointFailed func(idx int, err error)
	// OnRoundExhausted, when non-nil, fires once each time a full
	// address-list sweep fails to bind any host, with the sweep's
	// aggregate failure classes. Drives the
	// SenderAllEndpointsUnreachable event and the drainer's per-sweep
	// durable-ack settle accounting.
	OnRoundExhausted func(outcome qwpSfSweepOutcome)
	// DurableMismatchTerminal controls how a *QwpDurableAckMismatchError is
	// classified: true (foreground) makes it terminal (fail loud, no failover);
	// false (background drainer, whose source data is pinned) treats it as a
	// transient dial failure so the reconnect budget keeps retrying — a primary
	// that is briefly unreachable must not permanently fail the orphan slot.
	DurableMismatchTerminal bool
	// Background marks a walk owned by a background drainer: attempted
	// hosts are tracked in a walk-local qwpRoundCursor instead of the
	// shared tracker's round slots, and the round boundary resets the
	// cursor rather than calling the shared BeginRound. Host state /
	// zone classifications are still read from — and dial outcomes
	// still recorded into — shared state.
	Background bool
	// cursor carries the Background walk's attempted bits across its
	// rounds. qwpSfRunRoundWalk seeds it; direct qwpSfRunSingleRound
	// callers leave it nil (the single round synthesizes one when
	// Background is set).
	cursor *qwpRoundCursor
}

// qwpSfSweepOutcome aggregates the failure classes one exhausted
// address-list sweep observed, so round-boundary hooks can classify
// the sweep as a whole rather than by its (ordering-dependent) final
// dial. A durable-ack capability gap and a protocol reject are
// persistent classes distinct from a transient transport error: an
// all-replica window requires every real failure to be a role reject,
// and a deferred durable-mismatch / protocol-reject terminal fires
// only when no transient transport error coexisted (Invariant B: an
// outage of the intended host must not be escalated to a terminal
// because a different peer is misconfigured or mid-upgrade).
type qwpSfSweepOutcome struct {
	// SawDurableMismatch: at least one dial reached an endpoint that
	// does not advertise durable-ack (*QwpDurableAckMismatchError).
	SawDurableMismatch bool
	// SawRoleReject: at least one dial was 421-rejected with a role
	// header (replica or catching-up primary).
	SawRoleReject bool
	// SawProtocolReject: at least one dial was rejected with a 404
	// (wrong endpoint) or 426 (upgrade required) — a persistent
	// misconfiguration or a peer mid-upgrade, not a transient
	// transport error.
	SawProtocolReject bool
	// SawTransportError: at least one dial failed for a transient real
	// reason (TCP/TLS error, response-header timeout, 421-without-role,
	// 503).
	SawTransportError bool
}

// allReplica reports whether every real failure in the sweep was a
// role reject — the all-replica failover window. The window is
// transient topology churn (a replica gets promoted, a primary
// reappears), so callers reset settle budgets and fire
// OnPrimaryUnavailable rather than treating it as an outage.
func (o qwpSfSweepOutcome) allReplica() bool {
	return o.SawRoleReject && !o.SawDurableMismatch && !o.SawTransportError && !o.SawProtocolReject
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
// exhausted (every host attempted, no bind) and LastError / Outcome
// describe the sweep.
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
	Terminal error
	// Cancelled is ctx.Err() (or context.Canceled when cancelCh
	// fired) when the walk was interrupted. Also non-nil for
	// misconfigurations (nil tracker / factory) so callers route
	// both via the same exit branch.
	Cancelled error
	// LastError is the most recent dial failure when the round
	// exhausted. Nil on success / terminal / cancelled exits.
	LastError error
	// Outcome aggregates the exhausted sweep's failure classes for
	// the round-boundary hooks (all-replica detection, durable-ack
	// settle accounting). Zero value on non-exhaustion exits.
	Outcome qwpSfSweepOutcome
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
	cursor := params.cursor
	if params.Background && cursor == nil {
		cursor = newQwpRoundCursor(params.Tracker.Len())
	}

	var (
		attempts              int
		lastErr               error
		outcome               qwpSfSweepOutcome
		pendingMismatch       *QwpDurableAckMismatchError
		pendingProtocolReject *QwpUpgradeRejectError
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

		idx := params.Tracker.pickNext(cursor)
		if idx < 0 {
			// Sweep exhausted. A deferred durable-ack mismatch or protocol-level
			// reject (404 wrong endpoint, 426 upgrade required) becomes terminal
			// only when it was the sweep's *sole* failure class: a single peer
			// rejecting must not lock the walk out of a compatible sibling, and a
			// sweep that found nothing but rejects fails loud — replaying the
			// handshake meets the same reject and waiting cannot fix a
			// misconfigured path, a pre-QWP server, or an all-replica cluster.
			// A coexisting transient transport error (a healthy endpoint
			// mid-restart) or a 421 role reject (a promotable replica that may
			// become a compatible primary) means the reject is not yet proven
			// permanent, so keep retrying with backoff rather than latching a
			// terminal that would drop a running sender the window was about to
			// release (Invariant B).
			if !outcome.SawTransportError && !outcome.SawRoleReject {
				if pendingMismatch != nil {
					return qwpSfSingleRoundResult{Idx: -1, Attempts: attempts, Terminal: pendingMismatch}
				}
				if pendingProtocolReject != nil {
					return qwpSfSingleRoundResult{Idx: -1, Attempts: attempts, Terminal: pendingProtocolReject}
				}
			}
			return qwpSfSingleRoundResult{
				Idx:       -1,
				Attempts:  attempts,
				LastError: lastErr,
				Outcome:   outcome,
			}
		}

		// Dial host[idx].
		if params.OnAttempt != nil {
			params.OnAttempt()
		}
		attempts++
		t, err := params.Factory(ctx, idx)
		if err == nil && t != nil {
			// A successful upgrade binds unconditionally. The ingress
			// endpoint sends no SERVER_INFO frame and the client never
			// expects one (per the wire spec, ingress is role- and
			// zone-blind), so this path has no server role to filter on
			// — and needs none: the server itself 421-rejects an ingress
			// upgrade to a REPLICA or PRIMARY_CATCHUP node (with
			// X-QuestDB-Role), so any node that completes the upgrade is
			// write-eligible. Those 421s are classified as role rejects
			// below; a clean upgrade means bind. target= (like zone=) is
			// thus accepted at config time but inert here — re-rejecting
			// a node the server already accepted would only
			// connect/close-storm until the reconnect budget expired.
			// Ingress trackers are built with qwpTargetAny regardless,
			// so this path never observes a non-Any filter.
			params.Tracker.recordSuccess(idx, cursor)
			return qwpSfSingleRoundResult{
				Transport: t,
				Idx:       idx,
				Attempts:  attempts,
			}
		}
		lastErr = err

		// Cancellation race: ctx (or cancelCh) may have fired while
		// the dial was in flight, in which case err is just a
		// wrapped context.Canceled / context.DeadlineExceeded — not
		// a host failure. Recording it as a transport error would
		// falsely demote a healthy host the caller simply stopped
		// waiting for. Bail out before classification.
		if cerr := ctx.Err(); cerr != nil {
			return qwpSfSingleRoundResult{
				Idx:       -1,
				Cancelled: cerr,
				Attempts:  attempts,
			}
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

		// Durable-ack mismatch: walk the rest of the sweep before deciding, since
		// a later endpoint may be a durable-advertising primary, so keep
		// walking and only fail if the whole sweep found none. For a foreground sender the remembered mismatch becomes terminal
		// at sweep exhaustion (OK-only fallback would drop believed-durable data);
		// a drainer leaves it unremembered and just keeps retrying within its
		// budget (§5.8). Either way, demote the host so the sweep can terminate.
		var mismatch *QwpDurableAckMismatchError
		if errors.As(err, &mismatch) {
			if params.DurableMismatchTerminal {
				pendingMismatch = mismatch
			}
			if params.OnEndpointFailed != nil {
				params.OnEndpointFailed(idx, err)
			}
			params.Tracker.recordTransportError(idx, cursor)
			outcome.SawDurableMismatch = true
			continue
		}

		// Classify the failure. Typed *QwpUpgradeRejectError carries
		// the precise spec-relevant fields; everything else is a
		// generic transport error.
		var rej *QwpUpgradeRejectError
		if errors.As(err, &rej) {
			// AuthError (401 / 403): terminal per §6. Bypass failover.
			// Suppress OnEndpointFailed: a terminal auth reject must
			// surface as AUTH_FAILED only, not preceded by a transient
			// endpoint-failure event.
			if rej.StatusCode == 401 || rej.StatusCode == 403 {
				return qwpSfSingleRoundResult{
					Idx:      -1,
					Attempts: attempts,
					Terminal: rej,
				}
			}
			if params.OnEndpointFailed != nil {
				params.OnEndpointFailed(idx, err)
			}
			// Protocol-level rejects (404 wrong endpoint, 426 upgrade
			// required): remember for the sweep-exhaustion terminal above,
			// demote the host so a compatible sibling can still bind, and
			// classify the sweep as a protocol reject — NOT a transient
			// transport error. A mixed sweep that also hit a real transport
			// outage then keeps retrying; the deferred terminal fires only
			// once the outage clears and the reject is the sole failure class
			// (Invariant B). Mirrors the sanctioned non-421-reject terminal.
			if rej.StatusCode == 404 || rej.StatusCode == 426 {
				pendingProtocolReject = rej
				params.Tracker.recordTransportError(idx, cursor)
				outcome.SawProtocolReject = true
				continue
			}
			// X-QuestDB-Zone on a 421 reject is intentionally ignored
			// on the SF-ingest path: the ingress walk does not route by
			// zone, and the tracker is constructed with clientZone="" so
			// every host stays Same anyway. The egress connect-walk
			// consumes the same header in qwp_query_failover.go.
			// 421 + non-empty role: role-reject (transient or topology).
			// 421 without role, 503, and other statuses: generic transient
			// (an LB or proxy blip during a restart must not kill an SF
			// sender).
			if rej.IsRoleReject() {
				params.Tracker.recordRoleReject(idx, rej.IsCatchupRole(), cursor)
				outcome.SawRoleReject = true
				continue
			}
			params.Tracker.recordTransportError(idx, cursor)
			outcome.SawTransportError = true
			continue
		}

		// Non-upgrade-reject failure: TCP/TLS dial error,
		// response-header timeout, etc. — all transient.
		if params.OnEndpointFailed != nil {
			params.OnEndpointFailed(idx, err)
		}
		params.Tracker.recordTransportError(idx, cursor)
		outcome.SawTransportError = true
	}
}

// qwpSfRunRoundWalk drives the failover.md §13.6 multi-round walk:
// each round calls qwpSfRunSingleRound; on exhaustion it pays one
// equal-jitter exponential round-boundary sleep — role-reject rounds
// included: an all-replica window must grow toward MaxBackoff like
// any other outage, or every retry re-dials a fresh TLS handshake at
// a fixed ~10/s per endpoint — clamped to the remaining budget when
// one exists, then begins a fresh round (shared BeginRound(true) for
// foreground walks, a walk-local cursor reset for Background walks)
// and retries. Returns on success, terminal AuthError, budget
// exhaustion (bounded mode only), or cancellation.
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

	// Floor the backoff knobs: an unbounded walk with a non-positive
	// InitialBackoff would redial the whole address list with no pause and
	// busy-spin. Current callers always supply them; this guards a future one.
	if params.InitialBackoff <= 0 {
		params.InitialBackoff = qwpSfDefaultReconnectInitialBackoff
	}
	if params.MaxBackoff <= 0 {
		params.MaxBackoff = qwpSfDefaultReconnectMaxBackoff
	}

	if params.Tracker != nil && params.Tracker.Len() > 0 {
		if params.Background {
			// A background walk starts with a fresh walk-local cursor —
			// inherently clean of any other walker's round slots.
			if params.cursor == nil {
				params.cursor = newQwpRoundCursor(params.Tracker.Len())
			}
		} else {
			// A foreground (re)connect always starts a fresh round.
			// RecordSuccess from a prior bind leaves that host's attempted
			// slot consumed, so a partially-attempted round (bound host
			// attempted, siblings not) would skip the bound host here: a
			// reconnect after a drop on it could then sweep only the OTHER
			// hosts and — if one is a 404/426 or durable-mismatch peer whose
			// terminal is deferred to sweep exhaustion — latch that terminal
			// without ever redialing the healthy host. BeginRound(true) clears
			// the stale slots (keeping the sticky-Healthy pin); the §2.3
			// mid-stream demote inside the first single round still downgrades
			// the just-failed host so it is tried last, not first.
			params.Tracker.BeginRound(true)
		}
	}

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
			// A terminal result short-circuits the multi-round reconnect
			// budget: no round-boundary backoff, no BeginRound(true)
			// re-sweep. For a 401/403 AuthError this is unambiguous. For a
			// durable-ack mismatch (DurableMismatchTerminal foreground
			// senders) it is a deliberate fail-closed choice: even when the
			// durable primary is only transiently down and the sweep merely
			// found a write-eligible NON-durable node, we stop here rather
			// than keep retrying the primary within reconnect_max_duration.
			// Continuing would risk silently downgrading believed-durable
			// data to OK-only delivery during a primary-failover window, so
			// durable senders HALT and rely on close+rebuild (SF replays
			// from the durable watermark; memory mode loses the in-RAM tail —
			// the accepted cost of the fail-closed guarantee). Applying this
			// HALT on reconnect (not just the initial connect) is a deliberate
			// fail-closed choice: retrying the primary while the only blocker
			// is a transient transport error would reopen the silent-downgrade
			// window described above.
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

		// Round exhausted: a full address-list sweep bound no host.
		if params.OnRoundExhausted != nil {
			params.OnRoundExhausted(rr.Outcome)
		}
		// Pay one round-boundary sleep or terminate if the budget is gone.
		bounded := params.MaxDuration > 0
		elapsed := time.Since(outageStart)
		if bounded && elapsed >= params.MaxDuration {
			return qwpSfRoundWalkResult{
				Idx:      -1,
				Attempts: totalAttempts,
				Exhausted: buildExhaustedError(
					params.Tracker, params.Endpoints, elapsed, totalAttempts, rr.LastError),
			}
		}
		sleep := qwpSfComputeBackoff(backoffAttempt, params.InitialBackoff, params.MaxBackoff)
		backoffAttempt++
		if bounded {
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
		}
		// Sleep interruptible by ctx + cancelCh.
		if !qwpSfSleepInterruptible(ctx, cancelCh, sleep) {
			return qwpSfRoundWalkResult{
				Idx:       -1,
				Cancelled: context.Canceled,
				Attempts:  totalAttempts,
			}
		}
		if params.Background {
			params.cursor.reset()
		} else {
			params.Tracker.BeginRound(true)
		}
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
