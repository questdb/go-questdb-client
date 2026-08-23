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
	_, ok := e.cleanup.tryClaim(qwpSfCleanupOwnerClose, false)
	return ok
}

func (e *qwpSfCursorEngine) engineCloseRetryable() bool {
	_, ok := e.cleanup.tryClaim(qwpSfCleanupOwnerClose, false)
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
	legal := map[qwpSfCleanupPhase][]qwpSfCleanupPhase{
		qwpSfCleanupOpen:            {qwpSfCleanupStoppingManager},
		qwpSfCleanupStoppingManager: {qwpSfCleanupOpen, qwpSfCleanupManagerOwned, qwpSfCleanupReady},
		qwpSfCleanupManagerOwned:    {qwpSfCleanupOpen, qwpSfCleanupReady},
		qwpSfCleanupReady:           {qwpSfCleanupClaimed},
		qwpSfCleanupClaimed:         {qwpSfCleanupRunning, qwpSfCleanupRetryable},
		qwpSfCleanupRunning:         {qwpSfCleanupRetryable, qwpSfCleanupComplete},
		qwpSfCleanupRetryable:       {qwpSfCleanupClaimed},
		qwpSfCleanupComplete:        nil,
	}
	for from, tos := range legal {
		for _, to := range tos {
			from, to := from, to
			t.Run(from.String()+"-to-"+to.String(), func(t *testing.T) {
				c := qwpSfCleanupControl{state: qwpSfCleanupState{phase: from}}
				require.NotPanics(t, func() { c.transitionLocked(to, qwpSfCleanupOwnerClose) })
			})
		}
	}
	for from := range legal {
		for to := range legal {
			if qwpSfCleanupTransitionAllowed(from, to) {
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

func TestQwpSfCleanupStaleTokenCannotComplete(t *testing.T) {
	c := qwpSfCleanupControl{}
	token, action := c.begin(false, qwpSfCleanupOwnerClose, false)
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
