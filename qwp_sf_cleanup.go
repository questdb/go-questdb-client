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

import "sync"

// qwpSfCleanupPhase is the complete cleanup-ownership protocol for one cursor
// engine. Unlike the former collection of atomics, a phase is observed and
// changed as one record, so a rollback cannot be spliced together with an
// earlier manager-quiescence observation.
type qwpSfCleanupPhase uint8

const (
	qwpSfCleanupOpen qwpSfCleanupPhase = iota
	qwpSfCleanupStoppingManager
	qwpSfCleanupManagerOwned
	qwpSfCleanupReady
	qwpSfCleanupClaimed
	qwpSfCleanupRunning
	qwpSfCleanupRetryable
	qwpSfCleanupComplete
)

func (p qwpSfCleanupPhase) String() string {
	switch p {
	case qwpSfCleanupOpen:
		return "open"
	case qwpSfCleanupStoppingManager:
		return "stopping-manager"
	case qwpSfCleanupManagerOwned:
		return "manager-owned"
	case qwpSfCleanupReady:
		return "ready"
	case qwpSfCleanupClaimed:
		return "claimed"
	case qwpSfCleanupRunning:
		return "running"
	case qwpSfCleanupRetryable:
		return "retryable"
	case qwpSfCleanupComplete:
		return "complete"
	default:
		return "unknown"
	}
}

type qwpSfCleanupOwner uint8

const (
	qwpSfCleanupOwnerNone qwpSfCleanupOwner = iota
	qwpSfCleanupOwnerClose
	qwpSfCleanupOwnerManager
	qwpSfCleanupOwnerRetry
)

// qwpSfCleanupToken is an unforgeable-in-practice claim on one generation of
// cleanup work. Every helper verifies all three fields while holding the state
// mutex. A token from before a rollback, handoff, retry or completion is stale
// and cannot close resources.
type qwpSfCleanupToken struct {
	phase      qwpSfCleanupPhase
	owner      qwpSfCleanupOwner
	generation uint64
}

type qwpSfCleanupState struct {
	phase               qwpSfCleanupPhase
	owner               qwpSfCleanupOwner
	generation          uint64
	drainKnown          bool
	fullyDrained        bool
	barriersCommitted   bool
	leakMappings        bool
	resourcesClosed     bool
	drainedFilesPending bool
	firstErr            error
	retryOwnerStarted   bool

	// readersQuiesced says that whoever owned the send loop has stopped it, or
	// decided to leave its mappings alone. Terminal cleanup unmaps the segment
	// files, and the send loop reads them through slice headers it took
	// earlier, so a claim taken before this is published can pull memory out
	// from under a live reader. The engine cannot work this out for itself:
	// only the caller that owns the loop knows.
	readersQuiesced bool
}

// qwpSfCleanupControl owns the state and its dedicated mutex. Callers hold the
// mutex only for the helpers below; manager waits, appendMu acquisition, file
// IO and resource closure always happen after the helper returns.
type qwpSfCleanupControl struct {
	mu    sync.Mutex
	state qwpSfCleanupState
}

type qwpSfCleanupAction uint8

const (
	qwpSfCleanupNoAction qwpSfCleanupAction = iota
	qwpSfCleanupDriveManager
	qwpSfCleanupFinish
)

func qwpSfCleanupTransitionAllowed(from, to qwpSfCleanupPhase) bool {
	switch from {
	case qwpSfCleanupOpen:
		return to == qwpSfCleanupStoppingManager
	case qwpSfCleanupStoppingManager:
		return to == qwpSfCleanupOpen || to == qwpSfCleanupManagerOwned || to == qwpSfCleanupReady
	case qwpSfCleanupManagerOwned:
		return to == qwpSfCleanupOpen || to == qwpSfCleanupReady
	case qwpSfCleanupReady:
		return to == qwpSfCleanupClaimed
	case qwpSfCleanupClaimed:
		return to == qwpSfCleanupRunning || to == qwpSfCleanupRetryable
	case qwpSfCleanupRunning:
		return to == qwpSfCleanupRetryable || to == qwpSfCleanupComplete
	case qwpSfCleanupRetryable:
		return to == qwpSfCleanupClaimed
	case qwpSfCleanupComplete:
		return false
	default:
		return false
	}
}

func (c *qwpSfCleanupControl) transitionLocked(to qwpSfCleanupPhase, owner qwpSfCleanupOwner) qwpSfCleanupToken {
	from := c.state.phase
	if !qwpSfCleanupTransitionAllowed(from, to) {
		panic("qwp/sf: illegal cleanup transition " + from.String() + " -> " + to.String())
	}
	c.state.generation++
	c.state.phase = to
	c.state.owner = owner
	return qwpSfCleanupToken{phase: to, owner: owner, generation: c.state.generation}
}

func (c *qwpSfCleanupControl) tokenCurrentLocked(token qwpSfCleanupToken, phase qwpSfCleanupPhase) bool {
	return token.phase == phase && token.owner == c.state.owner &&
		token.generation == c.state.generation && c.state.phase == phase
}

// begin latches the mapping-safety input before making any ownership decision.
// A fresh or rolled-back close drives manager teardown. A manager-quiescent
// ownerless close claims terminal cleanup directly. Every other phase already
// has an owner or is complete.
func (c *qwpSfCleanupControl) begin(leakMappings bool, owner qwpSfCleanupOwner) (qwpSfCleanupToken, qwpSfCleanupAction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if leakMappings {
		c.state.leakMappings = true
	}
	// No claimant runs terminal cleanup before the owner of the send loop has
	// said the readers are done. engineCloseInternal publishes that as its
	// first act, so an owner passes here immediately; everyone else -- the
	// retry goroutine, a pool re-probe, a repeated Close -- has to wait for it.
	// Keeping the rule here rather than at each entry point means a new
	// claimant inherits it instead of having to remember it.
	if !c.state.readersQuiesced {
		return qwpSfCleanupToken{}, qwpSfCleanupNoAction
	}
	switch c.state.phase {
	case qwpSfCleanupOpen:
		return c.transitionLocked(qwpSfCleanupStoppingManager, owner), qwpSfCleanupDriveManager
	case qwpSfCleanupReady, qwpSfCleanupRetryable:
		return c.transitionLocked(qwpSfCleanupClaimed, owner), qwpSfCleanupFinish
	default:
		return qwpSfCleanupToken{}, qwpSfCleanupNoAction
	}
}

// markReadersQuiesced is published by the one caller that owns the send loop,
// before it asks for any cleanup of its own. It is one-way: a slot never gains
// new readers once its loop has stopped.
//
// leakMappings travels with it because the two are one fact, not two. A loop
// that was abandoned mid-read is quiesced only in the sense that nobody will
// wait for it; its mappings must still be left alone. Published separately,
// there is a window where the record says the readers are done but not that
// their mappings are untouchable, and a claim taken in that window unmaps
// memory a live reader is still walking.
func (c *qwpSfCleanupControl) markReadersQuiesced(leakMappings bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if leakMappings {
		c.state.leakMappings = true
	}
	c.state.readersQuiesced = true
}

// beginRepeatClose is begin for a caller that did not stop the send loop
// itself, which is what a repeated public Close is. It refuses until the owner
// of the loop has published quiescence, and refuses again once a retry
// goroutine owns the work. Reading those facts and taking the claim under one
// lock is the point: apart, a caller can act on a phase that has moved on
// between the two reads.
func (c *qwpSfCleanupControl) beginRepeatClose(owner qwpSfCleanupOwner) (qwpSfCleanupToken, qwpSfCleanupAction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.state.readersQuiesced || c.state.retryOwnerStarted {
		return qwpSfCleanupToken{}, qwpSfCleanupNoAction
	}
	switch c.state.phase {
	case qwpSfCleanupOpen:
		return c.transitionLocked(qwpSfCleanupStoppingManager, owner), qwpSfCleanupDriveManager
	case qwpSfCleanupReady, qwpSfCleanupRetryable:
		return c.transitionLocked(qwpSfCleanupClaimed, owner), qwpSfCleanupFinish
	default:
		return qwpSfCleanupToken{}, qwpSfCleanupNoAction
	}
}

func (c *qwpSfCleanupControl) recordDrain(token qwpSfCleanupToken, fullyDrained bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupStoppingManager) {
		return false
	}
	c.state.drainKnown = true
	c.state.fullyDrained = fullyDrained
	return true
}

// rollbackStopping invalidates a close owner that panicked before it could
// prove and publish manager quiescence. A retry must drive manager teardown
// again; retaining the captured drain/leak inputs is safe because closed has
// already fenced new appends.
func (c *qwpSfCleanupControl) rollbackStopping(token qwpSfCleanupToken) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupStoppingManager) {
		return false
	}
	c.transitionLocked(qwpSfCleanupOpen, qwpSfCleanupOwnerNone)
	return true
}

// managerReadyAndClaim publishes quiescence, then returns a new terminal owner
// token. The stopping-manager token is invalid before this helper returns.
func (c *qwpSfCleanupControl) managerReadyAndClaim(token qwpSfCleanupToken, owner qwpSfCleanupOwner) (qwpSfCleanupToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupStoppingManager) {
		return qwpSfCleanupToken{}, false
	}
	c.transitionLocked(qwpSfCleanupReady, qwpSfCleanupOwnerNone)
	return c.transitionLocked(qwpSfCleanupClaimed, owner), true
}

func (c *qwpSfCleanupControl) prepareManagerHandoff(token qwpSfCleanupToken) (qwpSfCleanupToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupStoppingManager) {
		return qwpSfCleanupToken{}, false
	}
	return c.transitionLocked(qwpSfCleanupManagerOwned, qwpSfCleanupOwnerManager), true
}

// abortManagerHandoff is used only when registration panics before it can
// return an ownership verdict. The manager helpers contain no user callbacks
// and install their callback as their final mutation; the guarded caller fires
// fault-injection hooks before that mutation. Manager quiescence must be
// re-proven on the retry.
func (c *qwpSfCleanupControl) abortManagerHandoff(token qwpSfCleanupToken) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupManagerOwned) {
		return false
	}
	c.transitionLocked(qwpSfCleanupOpen, qwpSfCleanupOwnerNone)
	return true
}

func (c *qwpSfCleanupControl) managerHandoffDeclinedAndClaim(token qwpSfCleanupToken, owner qwpSfCleanupOwner) (qwpSfCleanupToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupManagerOwned) {
		return qwpSfCleanupToken{}, false
	}
	c.transitionLocked(qwpSfCleanupReady, qwpSfCleanupOwnerNone)
	return c.transitionLocked(qwpSfCleanupClaimed, owner), true
}

// managerClaim is called only from the manager's callback after the worker is
// past its loop or the affected ring's service pass. That callback is the new
// proof of quiescence; a stale handoff token cannot publish it.
func (c *qwpSfCleanupControl) managerClaim(token qwpSfCleanupToken) (qwpSfCleanupToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupManagerOwned) {
		return qwpSfCleanupToken{}, false
	}
	c.transitionLocked(qwpSfCleanupReady, qwpSfCleanupOwnerNone)
	return c.transitionLocked(qwpSfCleanupClaimed, qwpSfCleanupOwnerManager), true
}

// tryClaim takes an already-ownerless terminal claim without driving the
// manager teardown first. It carries the same quiescence rule as begin and
// beginRepeatClose: every door into a terminal claim asks the same question, so
// wiring this one to a new caller cannot reopen the hole the others close.
func (c *qwpSfCleanupControl) tryClaim(owner qwpSfCleanupOwner) (qwpSfCleanupToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.state.readersQuiesced {
		return qwpSfCleanupToken{}, false
	}
	if c.state.phase != qwpSfCleanupReady && c.state.phase != qwpSfCleanupRetryable {
		return qwpSfCleanupToken{}, false
	}
	return c.transitionLocked(qwpSfCleanupClaimed, owner), true
}

// startTerminal is the required last transition before any terminal resource
// close. It consumes the claimed generation after appendMu has been acquired
// and returns a distinct running token. The same claim cannot start twice, and
// no caller can validate one field before a rollback and another after it.
func (c *qwpSfCleanupControl) startTerminal(token qwpSfCleanupToken) (qwpSfCleanupToken, bool, bool, bool, bool, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupClaimed) || !c.state.drainKnown {
		return qwpSfCleanupToken{}, false, false, false, false, false, false
	}
	runToken := c.transitionLocked(qwpSfCleanupRunning, token.owner)
	return runToken, c.state.fullyDrained, c.state.barriersCommitted,
		c.state.leakMappings, c.state.resourcesClosed, c.state.drainedFilesPending, true
}

func (c *qwpSfCleanupControl) abandonClaim(token qwpSfCleanupToken, err error) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupClaimed) {
		return false
	}
	if err != nil && c.state.firstErr == nil {
		c.state.firstErr = err
	}
	c.transitionLocked(qwpSfCleanupRetryable, qwpSfCleanupOwnerNone)
	return true
}

// checkpointDrainBarriers records that the fully-drained durability barriers
// (watermark sync and the collapsed manifest update) are committed. A retry
// generation skips them: they write through side files a partially completed
// pass may already have closed, and re-running them then fails spuriously.
func (c *qwpSfCleanupControl) checkpointDrainBarriers(token qwpSfCleanupToken) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupRunning) {
		return false
	}
	c.state.barriersCommitted = true
	return true
}

func (c *qwpSfCleanupControl) checkpointResourcesClosed(token qwpSfCleanupToken, drainedFilesPending bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupRunning) {
		return false
	}
	c.state.resourcesClosed = true
	c.state.drainedFilesPending = drainedFilesPending
	return true
}

func (c *qwpSfCleanupControl) markDrainedFilesComplete(token qwpSfCleanupToken) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupRunning) {
		return false
	}
	c.state.drainedFilesPending = false
	return true
}

func (c *qwpSfCleanupControl) retry(token qwpSfCleanupToken, err error) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupRunning) {
		return false
	}
	if err != nil && c.state.firstErr == nil {
		c.state.firstErr = err
	}
	c.transitionLocked(qwpSfCleanupRetryable, qwpSfCleanupOwnerNone)
	return true
}

func (c *qwpSfCleanupControl) complete(token qwpSfCleanupToken) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tokenCurrentLocked(token, qwpSfCleanupRunning) {
		return false
	}
	c.transitionLocked(qwpSfCleanupComplete, qwpSfCleanupOwnerNone)
	return true
}

func (c *qwpSfCleanupControl) completed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.phase == qwpSfCleanupComplete
}

func (c *qwpSfCleanupControl) needsRedrive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.phase == qwpSfCleanupOpen && !c.state.retryOwnerStarted
}

func (c *qwpSfCleanupControl) startRetryOwner() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state.phase == qwpSfCleanupComplete || c.state.retryOwnerStarted {
		return false
	}
	c.state.retryOwnerStarted = true
	return true
}

func (c *qwpSfCleanupControl) retryOwnerPanicked() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state.phase == qwpSfCleanupComplete {
		return false
	}
	c.state.retryOwnerStarted = false
	return true
}

func (c *qwpSfCleanupControl) snapshot() qwpSfCleanupState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}
