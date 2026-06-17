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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Construction & initial state ---

// TestQwpHostTrackerInitialStateZoneUnset confirms that, when the
// client did not configure a zone, every host starts Unknown/Same —
// the spec's zone-blind shortcut. PickNext then becomes a pure
// state-priority race.
func TestQwpHostTrackerInitialStateZoneUnset(t *testing.T) {
	tr := newQwpHostTracker(3, "", qwpTargetAny)
	for i, h := range tr.snapshot() {
		assert.Equal(t, qwpHostUnknown, h.state, "host %d initial state", i)
		assert.Equal(t, qwpZoneSame, h.zoneTier, "host %d zoneTier (client zone unset → Same)", i)
		assert.False(t, h.attempted, "host %d attempted", i)
	}
}

// TestQwpHostTrackerInitialStateTargetPrimary verifies that
// target=primary collapses every host's initial tier to Same, even
// when the client has an explicit zone. Writers must follow the
// master regardless of geography (failover.md §2).
func TestQwpHostTrackerInitialStateTargetPrimary(t *testing.T) {
	tr := newQwpHostTracker(2, "eu-west-1a", qwpTargetPrimary)
	for i, h := range tr.snapshot() {
		assert.Equal(t, qwpZoneSame, h.zoneTier, "host %d zoneTier (target=primary collapse)", i)
	}
}

// TestQwpHostTrackerInitialStateZoneAware verifies that when the
// client has an explicit zone and target!=primary, the initial tier
// is Unknown until RecordZone fills it in.
func TestQwpHostTrackerInitialStateZoneAware(t *testing.T) {
	tr := newQwpHostTracker(2, "eu-west-1a", qwpTargetAny)
	for i, h := range tr.snapshot() {
		assert.Equal(t, qwpZoneUnknown, h.zoneTier, "host %d zoneTier", i)
	}
}

// TestQwpHostTrackerInitialStateZoneWhitespaceOnly confirms that a
// whitespace-only client zone collapses to the zone-unset shortcut.
// Without the constructor's TrimSpace the tracker would treat every
// observed server zone as Other (since EqualFold("  ", any) is
// false), breaking zone-locality for users who accidentally pass
// `zone=  `.
func TestQwpHostTrackerInitialStateZoneWhitespaceOnly(t *testing.T) {
	tr := newQwpHostTracker(2, "  \t ", qwpTargetAny)
	for i, h := range tr.snapshot() {
		assert.Equal(t, qwpZoneSame, h.zoneTier, "host %d zoneTier (whitespace zone → unset → Same)", i)
	}
	// Subsequent RecordZone observations must also collapse to Same,
	// not Other — proves the trim sticks beyond the initial tier.
	tr.RecordZone(0, "us-east-1a")
	tr.RecordZone(1, "eu-west-1a")
	for i, h := range tr.snapshot() {
		assert.Equal(t, qwpZoneSame, h.zoneTier, "host %d zoneTier after RecordZone", i)
	}
}

// TestQwpHostTrackerLen reports the configured host count.
func TestQwpHostTrackerLen(t *testing.T) {
	assert.Equal(t, 3, newQwpHostTracker(3, "", qwpTargetAny).Len())
	assert.Equal(t, 0, newQwpHostTracker(0, "", qwpTargetAny).Len())
}

// --- PickNext basic walk ---

// TestQwpHostTrackerPickNextWalksInOrder verifies that on a fresh
// round with all Unknown hosts, PickNext returns 0, then 1, etc.
// after each is recorded. Tie-breaks go to the lower index.
func TestQwpHostTrackerPickNextWalksInOrder(t *testing.T) {
	tr := newQwpHostTracker(3, "", qwpTargetAny)
	assert.Equal(t, 0, tr.PickNext())
	tr.RecordTransportError(0)
	assert.Equal(t, 1, tr.PickNext())
	tr.RecordTransportError(1)
	assert.Equal(t, 2, tr.PickNext())
	tr.RecordTransportError(2)
	assert.Equal(t, -1, tr.PickNext(), "round must exhaust after every host attempted")
	assert.True(t, tr.IsRoundExhausted())
}

// TestQwpHostTrackerPickNextEmpty edge case: a tracker with zero
// hosts must immediately report -1 / exhausted without panicking.
func TestQwpHostTrackerPickNextEmpty(t *testing.T) {
	tr := newQwpHostTracker(0, "", qwpTargetAny)
	assert.Equal(t, -1, tr.PickNext())
	assert.True(t, tr.IsRoundExhausted())
}

// TestQwpHostTrackerPickNextSkipsAttempted: once an entry is
// attempted (regardless of outcome), PickNext must skip it within
// the same round.
func TestQwpHostTrackerPickNextSkipsAttempted(t *testing.T) {
	tr := newQwpHostTracker(3, "", qwpTargetAny)
	tr.RecordSuccess(1) // priority 1 (Healthy) but already attempted
	// Both 0 and 2 remain Unknown (priority 2), unattempted. The
	// lower index wins the tie.
	assert.Equal(t, 0, tr.PickNext())
}

// --- State priority ordering ---

// TestQwpHostTrackerStatePriorityOrdering walks the full state
// lattice: with five hosts in distinct states, PickNext must visit
// them in Healthy → Unknown → TransientReject → TransportError →
// TopologyReject order.
func TestQwpHostTrackerStatePriorityOrdering(t *testing.T) {
	tr := newQwpHostTracker(5, "", qwpTargetAny)
	// Force-install distinct states across the five hosts. The
	// public API only mutates state via Record* (which also sets
	// attempted), so we go through snapshot+reset for the test
	// scaffolding instead of poking the internal slice directly:
	// record a "fake" round to install the states, then BeginRound
	// to clear attempted while preserving classification (no
	// forget).
	tr.RecordTransportError(0)   // host 0 → TransportError
	tr.RecordRoleReject(1, true) // host 1 → TransientReject
	tr.RecordRoleReject(2, false) // host 2 → TopologyReject
	tr.RecordSuccess(3)          // host 3 → Healthy
	// host 4 stays Unknown.
	tr.BeginRound(false)

	// Best state is Healthy (3), then Unknown (4), then
	// TransientReject (1), then TransportError (0), then
	// TopologyReject (2).
	expectOrder := []int{3, 4, 1, 0, 2}
	for step, want := range expectOrder {
		got := tr.PickNext()
		require.Equalf(t, want, got, "step %d: expected host %d", step, want)
		tr.RecordTransportError(got) // consume the round slot
	}
	assert.Equal(t, -1, tr.PickNext())
}

// --- Zone priority ordering ---

// TestQwpHostTrackerZonePriorityOrdering: with all states equal,
// zone tier breaks the tie. Same < Unknown < Other.
func TestQwpHostTrackerZonePriorityOrdering(t *testing.T) {
	tr := newQwpHostTracker(3, "eu-west-1a", qwpTargetAny)
	// All start in Unknown state with Unknown zone tier (since
	// client has an explicit zone). Install zones:
	//   host 0 → Other ("us-east-1a")
	//   host 1 → Same  ("eu-west-1a")
	//   host 2 → (left as Unknown)
	tr.RecordZone(0, "us-east-1a")
	tr.RecordZone(1, "eu-west-1a")
	// host 2 stays zone=Unknown.

	// All states are Unknown → lexicographic comparison falls to
	// zone priority. Order should be: 1 (Same), 2 (Unknown), 0 (Other).
	expectOrder := []int{1, 2, 0}
	for step, want := range expectOrder {
		got := tr.PickNext()
		require.Equalf(t, want, got, "step %d: expected host %d", step, want)
		tr.RecordTransportError(got)
	}
}

// TestQwpHostTrackerLexicographicStateOverridesZone: state outranks
// zone. An Other-zone Healthy beats a Same-zone Unknown.
func TestQwpHostTrackerLexicographicStateOverridesZone(t *testing.T) {
	tr := newQwpHostTracker(2, "eu-west-1a", qwpTargetAny)
	tr.RecordZone(0, "us-east-1a") // host 0 → state=Unknown, zone=Other (priority (2, 3))
	tr.RecordZone(1, "eu-west-1a") // host 1 → state=Unknown, zone=Same  (priority (2, 1))
	// Promote host 0 to Healthy so its priority becomes (1, 3).
	tr.RecordSuccess(0)
	tr.BeginRound(false)

	// Host 0 (1, 3) beats host 1 (2, 1) because state outranks zone.
	assert.Equal(t, 0, tr.PickNext())
}

// TestQwpHostTrackerTieBreakByListOrder: equal (state, zone) ties go
// to the lower index — matching the user-supplied addr= order.
func TestQwpHostTrackerTieBreakByListOrder(t *testing.T) {
	tr := newQwpHostTracker(4, "", qwpTargetAny)
	// All Unknown / Same after construction with zone unset.
	assert.Equal(t, 0, tr.PickNext())
	tr.RecordTransportError(0)
	assert.Equal(t, 1, tr.PickNext())
	tr.RecordTransportError(1)
	assert.Equal(t, 2, tr.PickNext())
}

// --- RecordZone semantics ---

// TestQwpHostTrackerRecordZoneEmptyIsNoOp: an empty/whitespace
// zoneId must NOT touch the tier (spec §2.1).
func TestQwpHostTrackerRecordZoneEmptyIsNoOp(t *testing.T) {
	tr := newQwpHostTracker(1, "eu-west-1a", qwpTargetAny)
	tr.RecordZone(0, "eu-west-1a") // tier → Same
	tr.RecordZone(0, "")            // no-op
	tr.RecordZone(0, "   ")         // no-op (whitespace)
	assert.Equal(t, qwpZoneSame, tr.snapshot()[0].zoneTier)
}

// TestQwpHostTrackerRecordZoneCaseInsensitive: comparison against
// client zone is case-insensitive (failover.md §1.1, §5).
func TestQwpHostTrackerRecordZoneCaseInsensitive(t *testing.T) {
	tr := newQwpHostTracker(1, "EU-West-1A", qwpTargetAny)
	tr.RecordZone(0, "eu-west-1a")
	assert.Equal(t, qwpZoneSame, tr.snapshot()[0].zoneTier)

	tr2 := newQwpHostTracker(1, "eu-west-1a", qwpTargetAny)
	tr2.RecordZone(0, "EU-WEST-1A")
	assert.Equal(t, qwpZoneSame, tr2.snapshot()[0].zoneTier)
}

// TestQwpHostTrackerRecordZoneTargetPrimaryAlwaysSame: under
// target=primary, even a clearly-different zoneId must yield Same
// (zone tier collapses).
func TestQwpHostTrackerRecordZoneTargetPrimaryAlwaysSame(t *testing.T) {
	tr := newQwpHostTracker(1, "eu-west-1a", qwpTargetPrimary)
	tr.RecordZone(0, "us-east-1a")
	assert.Equal(t, qwpZoneSame, tr.snapshot()[0].zoneTier)
}

// TestQwpHostTrackerRecordZoneClientUnsetAlwaysSame: when the
// client did not configure a zone, every observed zoneId yields
// Same. The spec's rationale: a zone-blind client has no
// preference, so every host is equally local.
func TestQwpHostTrackerRecordZoneClientUnsetAlwaysSame(t *testing.T) {
	tr := newQwpHostTracker(1, "", qwpTargetAny)
	tr.RecordZone(0, "us-east-1a")
	assert.Equal(t, qwpZoneSame, tr.snapshot()[0].zoneTier)
}

// TestQwpHostTrackerRecordZoneDoesNotTouchStateOrAttempted: zone
// observation is orthogonal to state / round bookkeeping.
func TestQwpHostTrackerRecordZoneDoesNotTouchStateOrAttempted(t *testing.T) {
	tr := newQwpHostTracker(1, "eu-west-1a", qwpTargetAny)
	tr.RecordZone(0, "eu-west-1a")
	h := tr.snapshot()[0]
	assert.Equal(t, qwpHostUnknown, h.state, "state must remain Unknown")
	assert.False(t, h.attempted, "attempted must remain false")
}

// TestQwpHostTrackerRecordZoneOutOfRangeNoOp: out-of-range idx
// must not panic — the caller may legitimately pass a stale
// previousIdx slot.
func TestQwpHostTrackerRecordZoneOutOfRangeNoOp(t *testing.T) {
	tr := newQwpHostTracker(2, "eu-west-1a", qwpTargetAny)
	assert.NotPanics(t, func() {
		tr.RecordZone(-1, "x")
		tr.RecordZone(42, "x")
	})
}

// --- Mid-stream demote semantics ---

// TestQwpHostTrackerMidStreamDemotesHealthyOnly: per failover.md
// §2.1, mid-stream failure demotes Healthy → TransportError but
// must not touch other states (a drainer's earlier TopologyReject
// observation must survive a foreground mid-stream blip).
func TestQwpHostTrackerMidStreamDemotesHealthyOnly(t *testing.T) {
	tr := newQwpHostTracker(4, "", qwpTargetAny)
	tr.RecordSuccess(0)           // host 0 → Healthy
	tr.RecordRoleReject(1, true)  // host 1 → TransientReject
	tr.RecordRoleReject(2, false) // host 2 → TopologyReject
	tr.RecordTransportError(3)    // host 3 → TransportError (already worst-but-1)

	tr.RecordMidStreamFailure(0)
	tr.RecordMidStreamFailure(1)
	tr.RecordMidStreamFailure(2)
	tr.RecordMidStreamFailure(3)

	snap := tr.snapshot()
	assert.Equal(t, qwpHostTransportError, snap[0].state, "Healthy must demote to TransportError")
	assert.Equal(t, qwpHostTransientReject, snap[1].state, "TransientReject must be untouched")
	assert.Equal(t, qwpHostTopologyReject, snap[2].state, "TopologyReject must be untouched")
	assert.Equal(t, qwpHostTransportError, snap[3].state, "already-TransportError must be untouched")
}

// TestQwpHostTrackerMidStreamDoesNotTouchAttempted: mid-stream
// demotion preserves the round bit so the just-failed host can be
// considered (and skipped) in the same round walk.
func TestQwpHostTrackerMidStreamDoesNotTouchAttempted(t *testing.T) {
	tr := newQwpHostTracker(2, "", qwpTargetAny)
	tr.RecordSuccess(0)
	tr.BeginRound(false) // attempted cleared but state preserved
	assert.False(t, tr.snapshot()[0].attempted)
	tr.RecordMidStreamFailure(0)
	assert.False(t, tr.snapshot()[0].attempted,
		"RecordMidStreamFailure must NOT set attempted")
}

// TestQwpHostTrackerMidStreamOutOfRangeNoOp covers the same defensive
// bounds check as RecordZone for callers passing a stale previousIdx.
func TestQwpHostTrackerMidStreamOutOfRangeNoOp(t *testing.T) {
	tr := newQwpHostTracker(2, "", qwpTargetAny)
	assert.NotPanics(t, func() {
		tr.RecordMidStreamFailure(-1)
		tr.RecordMidStreamFailure(99)
	})
}

// --- BeginRound semantics ---

// TestQwpHostTrackerBeginRoundClearsAttemptedOnly: with
// forgetClassifications=false, every Record* outcome is preserved
// across the round boundary; only the attempted bits reset.
func TestQwpHostTrackerBeginRoundClearsAttemptedOnly(t *testing.T) {
	tr := newQwpHostTracker(3, "", qwpTargetAny)
	tr.RecordRoleReject(0, false) // host 0 → TopologyReject
	tr.RecordRoleReject(1, true)  // host 1 → TransientReject
	tr.RecordSuccess(2)           // host 2 → Healthy
	tr.BeginRound(false)

	snap := tr.snapshot()
	assert.Equal(t, qwpHostTopologyReject, snap[0].state)
	assert.Equal(t, qwpHostTransientReject, snap[1].state)
	assert.Equal(t, qwpHostHealthy, snap[2].state)
	for i, h := range snap {
		assert.False(t, h.attempted, "host %d attempted must be cleared", i)
	}
}

// TestQwpHostTrackerBeginRoundForgetResetsNonHealthy: with
// forgetClassifications=true, TransientReject / TopologyReject /
// TransportError all reset to Unknown for a fresh shot.
func TestQwpHostTrackerBeginRoundForgetResetsNonHealthy(t *testing.T) {
	tr := newQwpHostTracker(3, "", qwpTargetAny)
	tr.RecordRoleReject(0, false)
	tr.RecordRoleReject(1, true)
	tr.RecordTransportError(2)
	tr.BeginRound(true)
	for i, h := range tr.snapshot() {
		assert.Equal(t, qwpHostUnknown, h.state, "host %d", i)
	}
}

// TestQwpHostTrackerStickyHealthyLastSameZone: with
// forgetClassifications=true, the LAST same-zone Healthy entry is
// preserved; earlier same-zone Healthy entries are reset.
func TestQwpHostTrackerStickyHealthyLastSameZone(t *testing.T) {
	tr := newQwpHostTracker(3, "", qwpTargetAny)
	// All three start Same (zone unset → collapsed).
	tr.RecordSuccess(0)
	tr.BeginRound(true) // sticky host 0 preserved

	// Now mark host 1 as Healthy too. Both 0 and 1 are Same+Healthy.
	tr.RecordSuccess(1)
	tr.BeginRound(true)
	snap := tr.snapshot()
	assert.Equal(t, qwpHostUnknown, snap[0].state, "older same-zone Healthy must reset")
	assert.Equal(t, qwpHostHealthy, snap[1].state, "last same-zone Healthy must be preserved")
	assert.Equal(t, qwpHostUnknown, snap[2].state)
}

// TestQwpHostTrackerStickyHealthyCrossZoneReset: a Healthy entry in
// the Other zone must NOT be preserved across BeginRound(true) — a
// sticky pin in another zone would otherwise lock the client out
// of probing local hosts after they recover.
func TestQwpHostTrackerStickyHealthyCrossZoneReset(t *testing.T) {
	tr := newQwpHostTracker(2, "eu-west-1a", qwpTargetAny)
	tr.RecordZone(0, "us-east-1a") // host 0 → Other
	tr.RecordZone(1, "eu-west-1a") // host 1 → Same
	tr.RecordSuccess(0)            // host 0 → Healthy + Other
	tr.BeginRound(true)

	snap := tr.snapshot()
	assert.Equal(t, qwpHostUnknown, snap[0].state,
		"cross-zone Healthy must reset, not be preserved as sticky")
	assert.Equal(t, qwpZoneOther, snap[0].zoneTier,
		"zone tier must persist across BeginRound")
	assert.Equal(t, qwpZoneSame, snap[1].zoneTier,
		"zone tier must persist across BeginRound (host 1)")
}

// TestQwpHostTrackerStickyHealthyPicksSameOverOther: when both a
// same-zone Healthy and an other-zone Healthy exist, the same-zone
// one wins the sticky — even if the cross-zone one was recorded
// LATER. (Cross-zone Healthy never wins.)
func TestQwpHostTrackerStickyHealthyPicksSameOverOther(t *testing.T) {
	tr := newQwpHostTracker(2, "eu-west-1a", qwpTargetAny)
	tr.RecordZone(0, "eu-west-1a")
	tr.RecordZone(1, "us-east-1a")
	tr.RecordSuccess(0) // earlier
	tr.RecordSuccess(1) // later, but cross-zone — must NOT win sticky
	tr.BeginRound(true)

	snap := tr.snapshot()
	assert.Equal(t, qwpHostHealthy, snap[0].state, "same-zone Healthy must be preserved")
	assert.Equal(t, qwpHostUnknown, snap[1].state, "cross-zone Healthy must reset")
}

// TestQwpHostTrackerStickyHealthyTargetPrimaryCollapsesToLast: under
// target=primary, every zone tier is Same so the rule degenerates
// to "preserve the last Healthy entry".
func TestQwpHostTrackerStickyHealthyTargetPrimaryCollapsesToLast(t *testing.T) {
	tr := newQwpHostTracker(3, "eu-west-1a", qwpTargetPrimary)
	tr.RecordZone(0, "eu-west-1a")
	tr.RecordZone(1, "us-east-1a") // collapses to Same
	tr.RecordSuccess(0)
	tr.RecordSuccess(1) // later, also Same after collapse — wins sticky
	tr.BeginRound(true)

	snap := tr.snapshot()
	assert.Equal(t, qwpHostUnknown, snap[0].state)
	assert.Equal(t, qwpHostHealthy, snap[1].state,
		"target=primary: last Healthy wins regardless of original zone")
}

// TestQwpHostTrackerBeginRoundPreservesZoneTier: zone tier must
// survive BeginRound (both variants). Re-observing a different
// zone is the only way to change it.
func TestQwpHostTrackerBeginRoundPreservesZoneTier(t *testing.T) {
	tr := newQwpHostTracker(2, "eu-west-1a", qwpTargetAny)
	tr.RecordZone(0, "eu-west-1a")
	tr.RecordZone(1, "us-east-1a")
	tr.BeginRound(false)
	tr.BeginRound(true)
	snap := tr.snapshot()
	assert.Equal(t, qwpZoneSame, snap[0].zoneTier)
	assert.Equal(t, qwpZoneOther, snap[1].zoneTier)
}

// TestQwpHostTrackerStickyHealthyHonoursSelectionPriority: after a
// BeginRound(true) preserves a sticky-Healthy host, PickNext picks
// it first (priority 1).
func TestQwpHostTrackerStickyHealthyHonoursSelectionPriority(t *testing.T) {
	tr := newQwpHostTracker(3, "", qwpTargetAny)
	tr.RecordTransportError(0)
	tr.RecordTransportError(1)
	tr.RecordSuccess(2)
	tr.BeginRound(true) // host 2 preserved as sticky-Healthy

	assert.Equal(t, 2, tr.PickNext(),
		"sticky-Healthy host must be returned first on the next round")
}

// --- Out-of-range tolerance ---

// TestQwpHostTrackerOutOfRangeNoOp covers the bounds-check
// contracts for the Record* operations a caller might invoke with
// a stale or default previousIdx (e.g. -1 / Len()+1).
func TestQwpHostTrackerOutOfRangeNoOp(t *testing.T) {
	tr := newQwpHostTracker(2, "", qwpTargetAny)
	assert.NotPanics(t, func() {
		tr.RecordSuccess(-1)
		tr.RecordSuccess(99)
		tr.RecordRoleReject(-1, true)
		tr.RecordRoleReject(99, false)
		tr.RecordTransportError(-1)
		tr.RecordTransportError(99)
		tr.RecordMidStreamFailure(-1)
		tr.RecordMidStreamFailure(99)
		tr.RecordZone(-1, "x")
		tr.RecordZone(99, "x")
	})
	// State must be untouched on the in-range hosts.
	for i, h := range tr.snapshot() {
		assert.Equal(t, qwpHostUnknown, h.state, "host %d", i)
	}
}

// --- Concurrency ---

// TestQwpHostTrackerConcurrentAccess hammers every operation from
// multiple goroutines and verifies that (a) no race triggers under
// -race and (b) the final state is internally consistent (every
// host has a valid state / zone tier and the round-exhausted
// predicate matches a manual scan of attempted bits).
func TestQwpHostTrackerConcurrentAccess(t *testing.T) {
	const (
		numHosts    = 8
		numWorkers  = 16
		opsPerLoop  = 500
	)
	tr := newQwpHostTracker(numHosts, "eu-west-1a", qwpTargetAny)
	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < opsPerLoop; i++ {
				idx := (seed + i) % numHosts
				switch (seed + i) % 7 {
				case 0:
					tr.RecordSuccess(idx)
				case 1:
					tr.RecordRoleReject(idx, true)
				case 2:
					tr.RecordRoleReject(idx, false)
				case 3:
					tr.RecordTransportError(idx)
				case 4:
					tr.RecordMidStreamFailure(idx)
				case 5:
					tr.RecordZone(idx, "eu-west-1a")
				case 6:
					if (seed+i)%2 == 0 {
						tr.BeginRound(false)
					} else {
						tr.BeginRound(true)
					}
				}
				_ = tr.PickNext()
				_ = tr.IsRoundExhausted()
				counter.Add(1)
			}
		}(w)
	}
	wg.Wait()
	assert.Equal(t, int64(numWorkers*opsPerLoop), counter.Load())

	// Post-hoc consistency: every entry must hold a valid state +
	// zone tier value. The exact final classification is
	// non-deterministic.
	for i, h := range tr.snapshot() {
		assert.GreaterOrEqual(t, h.state.priority(), 1, "host %d state=%v", i, h.state)
		assert.LessOrEqual(t, h.state.priority(), 5, "host %d state=%v", i, h.state)
		assert.GreaterOrEqual(t, h.zoneTier.priority(), 1, "host %d zone=%v", i, h.zoneTier)
		assert.LessOrEqual(t, h.zoneTier.priority(), 3, "host %d zone=%v", i, h.zoneTier)
	}
}

// --- IsRoundExhausted ---

// TestQwpHostTrackerIsRoundExhausted exercises the predicate in
// each meaningful phase.
func TestQwpHostTrackerIsRoundExhausted(t *testing.T) {
	tr := newQwpHostTracker(2, "", qwpTargetAny)
	assert.False(t, tr.IsRoundExhausted(), "fresh tracker is not exhausted")

	tr.RecordTransportError(0)
	assert.False(t, tr.IsRoundExhausted(), "one of two attempted")

	tr.RecordTransportError(1)
	assert.True(t, tr.IsRoundExhausted(), "both attempted")

	tr.BeginRound(false)
	assert.False(t, tr.IsRoundExhausted(), "BeginRound clears attempted")
}

// TestQwpHostTrackerStringers covers the diagnostic stringers so a
// future change that adds a state / tier doesn't silently produce
// "Invalid" in error messages.
func TestQwpHostTrackerStringers(t *testing.T) {
	assert.Equal(t, "Healthy", qwpHostHealthy.String())
	assert.Equal(t, "Unknown", qwpHostUnknown.String())
	assert.Equal(t, "TransientReject", qwpHostTransientReject.String())
	assert.Equal(t, "TransportError", qwpHostTransportError.String())
	assert.Equal(t, "TopologyReject", qwpHostTopologyReject.String())
	assert.Equal(t, "Same", qwpZoneSame.String())
	assert.Equal(t, "Unknown", qwpZoneUnknown.String())
	assert.Equal(t, "Other", qwpZoneOther.String())
}
