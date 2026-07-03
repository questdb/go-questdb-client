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
	"strings"
	"sync"
)

// qwpHostState classifies a host's last-observed connect outcome.
// Lower state-priority values win in PickNext's lexicographic
// comparison (see failover.md §2).
type qwpHostState byte

const (
	// qwpHostHealthy: last connect to this host succeeded. Priority 1.
	qwpHostHealthy qwpHostState = iota
	// qwpHostUnknown: never tried this round, or just reset by
	// BeginRound(forgetClassifications=true). Priority 2.
	qwpHostUnknown
	// qwpHostTransientReject: server returned 421 +
	// X-QuestDB-Role: PRIMARY_CATCHUP. Likely to recover; priority 3.
	qwpHostTransientReject
	// qwpHostTransportError: TCP/TLS/handshake error during connect,
	// or mid-stream send/recv failure recorded via
	// RecordMidStreamFailure. Priority 4.
	qwpHostTransportError
	// qwpHostTopologyReject: server returned 421 + X-QuestDB-Role
	// other than PRIMARY_CATCHUP (or `target=` mismatch on the role
	// table). Will not become writable without a topology change.
	// Priority 5 (worst).
	qwpHostTopologyReject
)

// statePriority returns the spec-defined priority of a state.
// Lower is better. Unrecognized states return a sentinel that loses
// every comparison.
func (s qwpHostState) priority() int {
	switch s {
	case qwpHostHealthy:
		return 1
	case qwpHostUnknown:
		return 2
	case qwpHostTransientReject:
		return 3
	case qwpHostTransportError:
		return 4
	case qwpHostTopologyReject:
		return 5
	}
	return 99
}

// String returns the spec-doc name of the state for diagnostics.
func (s qwpHostState) String() string {
	switch s {
	case qwpHostHealthy:
		return "Healthy"
	case qwpHostUnknown:
		return "Unknown"
	case qwpHostTransientReject:
		return "TransientReject"
	case qwpHostTransportError:
		return "TransportError"
	case qwpHostTopologyReject:
		return "TopologyReject"
	}
	return "Invalid"
}

// qwpZoneTier classifies a host's zone relative to the client's
// configured `zone=` value. Assignment happens via RecordZone, fed
// from either SERVER_INFO.zone_id (post-upgrade) or X-QuestDB-Zone
// (upgrade reject).
type qwpZoneTier byte

const (
	// qwpZoneSame: server zone equals client zone (case-insensitive),
	// OR client zone is unset, OR target=primary (writers must follow
	// the master regardless of geography). Priority 1.
	qwpZoneSame qwpZoneTier = iota
	// qwpZoneUnknown: server did not advertise a zone (no CAP_ZONE,
	// no X-QuestDB-Zone header, or the client did not consume
	// SERVER_INFO). Priority 2.
	qwpZoneUnknown
	// qwpZoneOther: server advertised a zone that differs from the
	// client's `zone=`. Priority 3 (worst). Only reachable when the
	// client has an explicit zone and target != primary.
	qwpZoneOther
)

// priority returns the spec-defined zone tier priority. Lower is
// better; ordering is `Same` < `Unknown` < `Other`.
func (z qwpZoneTier) priority() int {
	switch z {
	case qwpZoneSame:
		return 1
	case qwpZoneUnknown:
		return 2
	case qwpZoneOther:
		return 3
	}
	return 99
}

// String returns the spec-doc name of the tier for diagnostics.
func (z qwpZoneTier) String() string {
	switch z {
	case qwpZoneSame:
		return "Same"
	case qwpZoneUnknown:
		return "Unknown"
	case qwpZoneOther:
		return "Other"
	}
	return "Invalid"
}

// qwpHostEntry is the per-host tracker slot. The `attempted` bit is
// reset at every BeginRound; state and zoneTier persist across rounds
// unless explicitly cleared (BeginRound(forgetClassifications=true)).
type qwpHostEntry struct {
	state     qwpHostState
	zoneTier  qwpZoneTier
	attempted bool
}

// qwpRoundCursor holds walk-local attempted bits for a background round
// walk (orphan drainers). Background walkers consult the shared
// tracker's host classifications (state / zone tier) read-only for
// prioritization and still record dial outcomes into shared state, but
// they must not consume the shared round slots nor clear them via
// BeginRound: sweep exhaustion is load-bearing for terminal decisions
// (durable-ack mismatch, protocol upgrade rejects), so a walker whose
// attempted bits were contaminated by a concurrent walker could go
// terminal without ever dialing a healthy sibling. Owned by a single
// walk goroutine; not safe for concurrent use.
type qwpRoundCursor struct {
	attempted []bool
}

func newQwpRoundCursor(numHosts int) *qwpRoundCursor {
	return &qwpRoundCursor{attempted: make([]bool, numHosts)}
}

// reset clears the cursor's attempted bits — the background walk's
// round boundary, standing in for the shared tracker's BeginRound.
func (c *qwpRoundCursor) reset() {
	for i := range c.attempted {
		c.attempted[i] = false
	}
}

// markAttempted consumes the round slot for idx: the cursor's when the
// walk is background (cursor non-nil), the shared entry's otherwise.
// Callers hold t.mu.
func (c *qwpRoundCursor) markAttempted(idx int, shared *qwpHostEntry) {
	if c == nil {
		shared.attempted = true
		return
	}
	c.attempted[idx] = true
}

// isAttempted reports whether idx's round slot is consumed, from the
// cursor when non-nil, from the shared entry otherwise. Callers hold
// t.mu.
func (c *qwpRoundCursor) isAttempted(idx int, shared *qwpHostEntry) bool {
	if c == nil {
		return shared.attempted
	}
	return c.attempted[idx]
}

// qwpHostTracker implements the failover.md §2 host-health model:
// each configured `addr=` entry carries a `(state, zone_tier)`
// classification and a per-round `attempted` bit. PickNext returns
// the lexicographically-best unattempted entry.
//
// The tracker is shared across loops (foreground SF I/O thread,
// orphan drainers, etc.); per-caller demotion state (e.g. the
// `previousIdx` slot used to drive RecordMidStreamFailure on the
// next iteration) lives on the *caller*, not on the tracker. See
// failover.md §2.3 "Per-caller previousIdx, not shared". The shared
// `attempted` round slots belong to foreground walks alone (at most
// one runs at a time); background walkers bring their own
// qwpRoundCursor — see its doc for why.
//
// All methods are safe for concurrent use; a single internal mutex
// serializes every operation. The public API is not required to be
// re-entrant.
type qwpHostTracker struct {
	mu sync.Mutex

	// hosts is the per-endpoint slot table. len(hosts) matches the
	// configured addr= list and never changes for the tracker's
	// lifetime.
	hosts []qwpHostEntry

	// clientZone is the trimmed, lowercased value of the
	// connect-string `zone=` key. Empty when the user did not
	// configure a zone (including whitespace-only values, which
	// collapse to "" after the constructor's TrimSpace).
	clientZone string

	// target collapses zone tiers to Same when set to
	// qwpTargetPrimary (writers must follow the master regardless
	// of geography). Other target values leave zone-tier assignment
	// to RecordZone.
	target QwpTargetFilter
}

// newQwpHostTracker constructs a tracker for `numHosts` configured
// endpoints. The initial state of every host is `Unknown` (never
// observed); the initial zone tier depends on the client config:
//
//   - Same when target=primary or the client zone is unset. No zone
//     observation is needed in these cases — the tier collapses for
//     all hosts.
//   - Unknown otherwise. RecordZone fills in Same/Other once the
//     transport observes a server zone for the host.
//
// clientZone is case-insensitive and whitespace-insensitive (stored
// trimmed + lowercased); pass "" when the user did not configure
// one. A whitespace-only value collapses to "" here so the
// zone-blind shortcut applies — symmetric with RecordZone, which
// trims server-side zone observations. numHosts must be > 0; the
// caller is responsible for validation (sanitizeQwpConf rejects an
// empty endpoint list before reaching this point).
func newQwpHostTracker(numHosts int, clientZone string, target QwpTargetFilter) *qwpHostTracker {
	t := &qwpHostTracker{
		hosts:      make([]qwpHostEntry, numHosts),
		clientZone: strings.ToLower(strings.TrimSpace(clientZone)),
		target:     target,
	}
	initialZone := qwpZoneUnknown
	if t.zoneCollapsedToSame() {
		initialZone = qwpZoneSame
	}
	for i := range t.hosts {
		t.hosts[i] = qwpHostEntry{
			state:    qwpHostUnknown,
			zoneTier: initialZone,
		}
	}
	return t
}

// zoneCollapsedToSame reports whether every host's zone tier
// collapses to Same regardless of observed server zone. Holds when
// target=primary (writers follow the master) or the client did not
// configure a zone (zone-blind). Does not require the lock; reads
// only immutable fields set at construction.
func (t *qwpHostTracker) zoneCollapsedToSame() bool {
	return t.target == qwpTargetPrimary || t.clientZone == ""
}

// Len returns the number of hosts the tracker manages. Exposed
// mainly so callers can size their own per-caller previousIdx slots
// to match the addr= list.
func (t *qwpHostTracker) Len() int {
	return len(t.hosts)
}

// PickNext returns the index of the highest-priority unattempted
// host, or -1 if the round is exhausted. Selection is
// lexicographic on (state_priority, zone_priority); ties go to the
// lower index (i.e. the order in which the user supplied addr=).
//
// Calling PickNext twice without an intervening BeginRound is
// permitted on a non-exhausted tracker — the result is deterministic
// and idempotent because PickNext does not mutate `attempted`. The
// caller is responsible for invoking the appropriate Record* method
// before the next selection so the same host isn't returned again.
func (t *qwpHostTracker) PickNext() int {
	return t.pickNext(nil)
}

// pickNext is PickNext with an optional round cursor: a non-nil cursor
// supplies the attempted bits (background walk) while state / zone
// classifications still come from shared state.
func (t *qwpHostTracker) pickNext(cursor *qwpRoundCursor) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	best := -1
	bestStatePri := 0
	bestZonePri := 0
	for i := range t.hosts {
		h := &t.hosts[i]
		if cursor.isAttempted(i, h) {
			continue
		}
		sp := h.state.priority()
		zp := h.zoneTier.priority()
		if best == -1 || sp < bestStatePri || (sp == bestStatePri && zp < bestZonePri) {
			best = i
			bestStatePri = sp
			bestZonePri = zp
		}
	}
	return best
}

// IsRoundExhausted reports whether every host has been attempted in
// the current round. The reconnect loop calls this between
// PickNext == -1 and BeginRound to confirm the exhaustion path —
// useful for diagnostics; correctness only requires the PickNext
// return value.
func (t *qwpHostTracker) IsRoundExhausted() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range t.hosts {
		if !t.hosts[i].attempted {
			return false
		}
	}
	return true
}

// RecordSuccess marks host idx as Healthy and consumes its round
// slot. Previously-Healthy hosts (at other indices) are NOT
// implicitly demoted — the sticky-Healthy effect emerges at
// BeginRound(forgetClassifications=true). Out-of-range idx is a
// silent no-op so callers can pass a stored previousIdx without a
// defensive bounds check.
func (t *qwpHostTracker) RecordSuccess(idx int) {
	t.recordSuccess(idx, nil)
}

// recordSuccess is RecordSuccess with an optional round cursor: the
// classification update always lands in shared state, while a non-nil
// cursor absorbs the attempted bit (background walk).
func (t *qwpHostTracker) recordSuccess(idx int, cursor *qwpRoundCursor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if idx < 0 || idx >= len(t.hosts) {
		return
	}
	t.hosts[idx].state = qwpHostHealthy
	cursor.markAttempted(idx, &t.hosts[idx])
}

// RecordRoleReject classifies a 421 + role response. When
// transient is true (role == PRIMARY_CATCHUP), the host enters
// TransientReject and gets another chance on the next
// BeginRound(forgetClassifications=true); when false (any other
// non-empty role), it enters TopologyReject and stays at the
// lowest priority until the operator confirms cluster health.
// Both outcomes consume the round slot.
func (t *qwpHostTracker) RecordRoleReject(idx int, transient bool) {
	t.recordRoleReject(idx, transient, nil)
}

// recordRoleReject is RecordRoleReject with an optional round cursor;
// see recordSuccess for the split.
func (t *qwpHostTracker) recordRoleReject(idx int, transient bool, cursor *qwpRoundCursor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if idx < 0 || idx >= len(t.hosts) {
		return
	}
	if transient {
		t.hosts[idx].state = qwpHostTransientReject
	} else {
		t.hosts[idx].state = qwpHostTopologyReject
	}
	cursor.markAttempted(idx, &t.hosts[idx])
}

// RecordTransportError marks a host as TransportError after a
// TCP/TLS/handshake failure during connect. Consumes the round
// slot. Mid-stream send/recv failures (after a successful upgrade)
// go through RecordMidStreamFailure instead.
func (t *qwpHostTracker) RecordTransportError(idx int) {
	t.recordTransportError(idx, nil)
}

// recordTransportError is RecordTransportError with an optional round
// cursor; see recordSuccess for the split.
func (t *qwpHostTracker) recordTransportError(idx int, cursor *qwpRoundCursor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if idx < 0 || idx >= len(t.hosts) {
		return
	}
	t.hosts[idx].state = qwpHostTransportError
	cursor.markAttempted(idx, &t.hosts[idx])
}

// RecordMidStreamFailure demotes a Healthy host to TransportError
// after the receive or send pump throws past a successful upgrade.
// Does NOT touch `attempted` — the caller passes its private
// previousIdx slot and we want the next PickNext to consider the
// newly-demoted host as one of the candidates in the same round.
// Non-Healthy entries are left alone; if a drainer already
// observed a TopologyReject on this index, foreground's mid-stream
// failure should not undo that classification.
//
// The reconnect-loop ordering invariant (failover.md §2.3) is:
// call RecordMidStreamFailure BEFORE the next PickNext / BeginRound.
// Reversing the order makes sticky-Healthy preserve the just-failed
// host as priority pick, which then receives the first reconnect
// attempt and fails again.
func (t *qwpHostTracker) RecordMidStreamFailure(idx int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if idx < 0 || idx >= len(t.hosts) {
		return
	}
	if t.hosts[idx].state == qwpHostHealthy {
		t.hosts[idx].state = qwpHostTransportError
	}
}

// RecordZone updates a host's zone tier from an observed server
// zone identifier. Inputs follow the spec:
//
//   - zoneId == "" (or whitespace-only): no-op; the existing tier
//     is preserved. This covers servers that did not emit a zone
//     header (servers without CAP_ZONE, or a 421 reject without
//     X-QuestDB-Zone). The tracker's initial tier remains in effect.
//   - zoneId == client zone (case-insensitive): tier becomes Same.
//   - target=primary or client zone unset: tier becomes Same
//     regardless of the zoneId value (the spec collapses zone tiers
//     in these modes; writers must follow the master).
//   - otherwise: tier becomes Other.
//
// Does NOT touch state or attempted — zone observation is
// orthogonal to state classification and may happen on the same
// connect attempt that also records success / role-reject /
// transport-error.
func (t *qwpHostTracker) RecordZone(idx int, zoneId string) {
	trimmed := strings.TrimSpace(zoneId)
	if trimmed == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if idx < 0 || idx >= len(t.hosts) {
		return
	}
	if t.zoneCollapsedToSame() {
		t.hosts[idx].zoneTier = qwpZoneSame
		return
	}
	if strings.EqualFold(trimmed, t.clientZone) {
		t.hosts[idx].zoneTier = qwpZoneSame
	} else {
		t.hosts[idx].zoneTier = qwpZoneOther
	}
}

// BeginRound clears the per-round attempted flags. When
// forgetClassifications is true, additionally:
//
//   - Resets every non-Healthy state to Unknown so stale
//     TransientReject / TopologyReject / TransportError entries get
//     another chance.
//   - Preserves the LAST Healthy entry whose zone tier is Same as
//     the sticky-Healthy pin. Any earlier same-zone Healthy entry,
//     and any cross-zone (Other) Healthy entry, is reset to Unknown
//     — a sticky pin in another zone would otherwise lock the
//     client out of probing local hosts after they recover.
//   - Zone tiers are NOT cleared — once observed, they persist for
//     the host's lifetime in this client until re-observed.
//
// forgetClassifications=true is the between-outages reset; false
// is the within-outage reset (same round bits cleared,
// classifications preserved).
func (t *qwpHostTracker) BeginRound(forgetClassifications bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range t.hosts {
		t.hosts[i].attempted = false
	}
	if !forgetClassifications {
		return
	}
	// Find the LAST Healthy entry with Same zone tier — preserve that
	// one and only that one. A later same-zone Healthy supersedes any
	// earlier one; cross-zone (Other) Healthy entries are not
	// preserved at all.
	stickyIdx := -1
	for i := range t.hosts {
		if t.hosts[i].state == qwpHostHealthy && t.hosts[i].zoneTier == qwpZoneSame {
			stickyIdx = i
		}
	}
	for i := range t.hosts {
		if i == stickyIdx {
			continue
		}
		// Reset every non-Unknown state to Unknown. This covers:
		//   - All Healthy entries that aren't the sticky (earlier
		//     same-zone Healthy, or cross-zone Healthy).
		//   - All TransientReject / TopologyReject / TransportError
		//     entries (give them another chance next round).
		if t.hosts[i].state != qwpHostUnknown {
			t.hosts[i].state = qwpHostUnknown
		}
	}
}

// snapshot returns a copy of the host-entry slice. Test-only;
// callers must not mutate the returned slice (it shares no memory
// with the tracker, but the contract is "observation, not
// influence").
func (t *qwpHostTracker) snapshot() []qwpHostEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]qwpHostEntry, len(t.hosts))
	copy(out, t.hosts)
	return out
}
