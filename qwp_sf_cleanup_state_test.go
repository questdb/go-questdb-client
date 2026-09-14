/******************************************************************************
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
 *****************************************************************************/

package questdb

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func cleanupManagerTornDown(e *qwpSfCursorEngine) bool {
	s := e.cleanup.snapshot()
	switch s.phase {
	case qwpSfCleanupManagerOwned, qwpSfCleanupReady, qwpSfCleanupClaimed,
		qwpSfCleanupRunning, qwpSfCleanupRetryable, qwpSfCleanupComplete:
		return true
	default:
		return false
	}
}

func (e *qwpSfCursorEngine) engineTryClaimTerminalCleanup() bool {
	_, ok := e.cleanup.tryClaim(qwpSfCleanupOwnerClose)
	return ok
}

func (e *qwpSfCursorEngine) engineCloseRetryable() bool {
	_, ok := e.cleanup.tryClaim(qwpSfCleanupOwnerClose)
	return ok
}

func (e *qwpSfCursorEngine) engineFinishClaimedClose() error {
	e.cleanup.mu.Lock()
	s := e.cleanup.state
	e.cleanup.mu.Unlock()
	if s.phase != qwpSfCleanupClaimed {
		return nil
	}
	token := qwpSfCleanupToken{phase: s.phase, owner: s.owner, generation: s.generation}
	e.appendMu.Lock()
	defer e.appendMu.Unlock()
	return e.engineFinishCloseGuarded(token)
}

func (e *qwpSfCursorEngine) engineCloseNeedsRedrive() bool {
	return e.cleanup.needsRedrive()
}

// Cleanup ownership used to be inferred from independently sampled atomics.
// A claimant could observe managerTornDown before a rollback, then observe
// deferredCleanupOwned after that rollback, and incorrectly enter terminal
// cleanup after manager quiescence had been revoked. Keep the old marker names
// out of the engine: one cleanup record must own phase, token and close inputs.
func TestQwpSfEngineCleanupHasSingleOwnershipRecord(t *testing.T) {
	typ := reflect.TypeOf(qwpSfCursorEngine{})
	for _, name := range []string{
		"closeCompleted",
		"terminalCleanupClaimed",
		"managerTornDown",
		"closeInFlight",
		"deferredCleanupOwned",
		"deferredFullyDrained",
		"deferredLeakSegments",
	} {
		_, exists := typ.FieldByName(name)
		require.Falsef(t, exists, "cleanup ownership still split across %s", name)
	}
	_, exists := typ.FieldByName("cleanup")
	require.True(t, exists, "engine must carry one cleanup state record")
}

func TestQwpSfCleanupTransitionTable(t *testing.T) {
	legal := map[qwpSfCleanupPhase]map[qwpSfCleanupPhase]bool{
		qwpSfCleanupOpen: {
			qwpSfCleanupStoppingManager: true,
		},
		qwpSfCleanupStoppingManager: {
			qwpSfCleanupOpen:         true,
			qwpSfCleanupManagerOwned: true,
			qwpSfCleanupReady:        true,
		},
		qwpSfCleanupManagerOwned: {
			qwpSfCleanupOpen:  true,
			qwpSfCleanupReady: true,
		},
		qwpSfCleanupReady: {
			qwpSfCleanupClaimed: true,
		},
		qwpSfCleanupClaimed: {
			qwpSfCleanupRunning:   true,
			qwpSfCleanupRetryable: true,
		},
		qwpSfCleanupRunning: {
			qwpSfCleanupRetryable: true,
			qwpSfCleanupComplete:  true,
		},
		qwpSfCleanupRetryable: {
			qwpSfCleanupClaimed: true,
		},
		qwpSfCleanupComplete: {},
	}
	for from, tos := range legal {
		for to := range tos {
			from, to := from, to
			t.Run(from.String()+"-to-"+to.String(), func(t *testing.T) {
				c := qwpSfCleanupControl{state: qwpSfCleanupState{phase: from}}
				require.NotPanics(t, func() { c.transitionLocked(to, qwpSfCleanupOwnerClose) })
			})
		}
	}
	for from := range legal {
		for to := range legal {
			if legal[from][to] {
				continue
			}
			from, to := from, to
			t.Run(from.String()+"-rejects-"+to.String(), func(t *testing.T) {
				c := qwpSfCleanupControl{state: qwpSfCleanupState{phase: from}}
				require.Panics(t, func() { c.transitionLocked(to, qwpSfCleanupOwnerClose) })
			})
		}
	}
}

func TestQwpSfCleanupAbortManagerHandoff(t *testing.T) {
	var c qwpSfCleanupControl
	c.markReadersQuiesced(false)
	stopping, action := c.begin(false, qwpSfCleanupOwnerClose)
	require.Equal(t, qwpSfCleanupDriveManager, action)
	handoff, ok := c.prepareManagerHandoff(stopping)
	require.True(t, ok)

	beforeStale := c.snapshot()
	require.False(t, c.abortManagerHandoff(stopping))
	require.Equal(t, beforeStale, c.snapshot(), "a stale token must not mutate cleanup state")

	require.True(t, c.abortManagerHandoff(handoff))
	aborted := c.snapshot()
	require.Equal(t, qwpSfCleanupOpen, aborted.phase)
	require.Equal(t, qwpSfCleanupOwnerNone, aborted.owner)
	require.Greater(t, aborted.generation, handoff.generation)

	beforeReuse := c.snapshot()
	require.False(t, c.abortManagerHandoff(handoff), "the transition must invalidate its handoff token")
	require.Equal(t, beforeReuse, c.snapshot())
}

func TestQwpSfCleanupManagerHandoffDeclinedAndClaim(t *testing.T) {
	var c qwpSfCleanupControl
	c.markReadersQuiesced(false)
	stopping, action := c.begin(false, qwpSfCleanupOwnerClose)
	require.Equal(t, qwpSfCleanupDriveManager, action)
	require.True(t, c.recordDrain(stopping, true))
	handoff, ok := c.prepareManagerHandoff(stopping)
	require.True(t, ok)

	beforeStale := c.snapshot()
	_, ok = c.managerHandoffDeclinedAndClaim(stopping, qwpSfCleanupOwnerClose)
	require.False(t, ok)
	require.Equal(t, beforeStale, c.snapshot(), "a stale token must not mutate cleanup state")

	claim, ok := c.managerHandoffDeclinedAndClaim(handoff, qwpSfCleanupOwnerRetry)
	require.True(t, ok)
	claimed := c.snapshot()
	require.Equal(t, qwpSfCleanupClaimed, claimed.phase)
	require.Equal(t, qwpSfCleanupOwnerRetry, claimed.owner)
	require.Equal(t, handoff.generation+2, claimed.generation,
		"the ownerless ready generation must invalidate the manager handoff")
	require.Equal(t, qwpSfCleanupToken{
		phase:      claimed.phase,
		owner:      claimed.owner,
		generation: claimed.generation,
	}, claim)

	beforeReuse := c.snapshot()
	_, ok = c.managerHandoffDeclinedAndClaim(handoff, qwpSfCleanupOwnerClose)
	require.False(t, ok)
	require.Equal(t, beforeReuse, c.snapshot())
}

func TestQwpSfCleanupAbandonClaim(t *testing.T) {
	var c qwpSfCleanupControl
	c.markReadersQuiesced(false)
	stopping, action := c.begin(false, qwpSfCleanupOwnerClose)
	require.Equal(t, qwpSfCleanupDriveManager, action)
	require.True(t, c.recordDrain(stopping, false))
	claim, ok := c.managerReadyAndClaim(stopping, qwpSfCleanupOwnerClose)
	require.True(t, ok)

	firstErr := errors.New("first close failure")
	beforeStale := c.snapshot()
	require.False(t, c.abandonClaim(stopping, firstErr))
	require.Equal(t, beforeStale, c.snapshot(), "a stale token must not mutate cleanup state")

	require.True(t, c.abandonClaim(claim, firstErr))
	retryable := c.snapshot()
	require.Equal(t, qwpSfCleanupRetryable, retryable.phase)
	require.Equal(t, qwpSfCleanupOwnerNone, retryable.owner)
	require.Equal(t, claim.generation+1, retryable.generation)
	require.Same(t, firstErr, retryable.firstErr)

	retryClaim, ok := c.tryClaim(qwpSfCleanupOwnerRetry)
	require.True(t, ok)
	beforeOldClaim := c.snapshot()
	secondErr := errors.New("later close failure")
	require.False(t, c.abandonClaim(claim, secondErr), "an earlier claimed generation must stay invalid")
	require.Equal(t, beforeOldClaim, c.snapshot())

	require.True(t, c.abandonClaim(retryClaim, secondErr))
	require.Same(t, firstErr, c.snapshot().firstErr, "the first cleanup error must remain latched")
}

func TestQwpSfCleanupRetryOwnerPanicked(t *testing.T) {
	t.Run("releases exclusive retry ownership", func(t *testing.T) {
		var c qwpSfCleanupControl
		require.True(t, c.startRetryOwner())
		require.False(t, c.startRetryOwner(), "only one retry owner may run")

		before := c.snapshot()
		require.True(t, c.retryOwnerPanicked())
		after := c.snapshot()
		expected := before
		expected.retryOwnerStarted = false
		require.Equal(t, expected, after)

		require.True(t, c.startRetryOwner(), "a replacement retry owner must be allowed after panic")
		require.False(t, c.startRetryOwner(), "replacement ownership must remain exclusive")
	})

	t.Run("completed cleanup cannot be restarted", func(t *testing.T) {
		var c qwpSfCleanupControl
		c.markReadersQuiesced(false)
		stopping, action := c.begin(false, qwpSfCleanupOwnerClose)
		require.Equal(t, qwpSfCleanupDriveManager, action)
		require.True(t, c.recordDrain(stopping, true))
		claim, ok := c.managerReadyAndClaim(stopping, qwpSfCleanupOwnerClose)
		require.True(t, ok)
		run, _, _, _, _, _, ok := c.startTerminal(claim)
		require.True(t, ok)
		require.True(t, c.startRetryOwner())
		require.True(t, c.complete(run))

		completed := c.snapshot()
		require.False(t, c.retryOwnerPanicked(), "completion wins over retry-owner recovery")
		require.Equal(t, completed, c.snapshot(), "completed state must not be mutated")
		require.False(t, c.startRetryOwner(), "completed cleanup cannot acquire another retry owner")
	})
}

func TestQwpSfCleanupStaleTokenCannotComplete(t *testing.T) {
	c := qwpSfCleanupControl{}
	// begin refuses until the owner of the send loop has said the readers are
	// done, which engineCloseInternal does before it asks for any cleanup.
	c.markReadersQuiesced(false)
	token, action := c.begin(false, qwpSfCleanupOwnerClose)
	require.Equal(t, qwpSfCleanupDriveManager, action)
	require.True(t, c.recordDrain(token, false))
	claim, ok := c.managerReadyAndClaim(token, qwpSfCleanupOwnerClose)
	require.True(t, ok)
	run, _, _, _, _, _, ok := c.startTerminal(claim)
	require.True(t, ok)
	require.True(t, c.complete(run))
	require.False(t, c.complete(run), "a terminal generation must complete at most once")
	require.False(t, c.retry(run, nil), "a stale completed token must not reopen cleanup")
}
