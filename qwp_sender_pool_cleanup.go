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
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Keep failed pools alive until process exit, along with any resources that
// could not be safely released. This list does not retry cleanup or decide
// which slots can be reused.
var qwpFailedSenderPools struct {
	sync.Mutex
	pools []*qwpSenderPool
}

// Tests use this hook to panic during a slot-state update, after another
// slot's state has already changed.
var qwpTestPoolTransitionHook atomic.Pointer[func(*qwpSenderPool, int, qwpSfSlotState, qwpSfSlotState)]

// withLock runs fn with the pool mutex held and always unlocks it, even after
// a panic. Pass any slots that fn might remove from the pool's lists so a
// failure cannot lose track of their senders, files, or locks.
func (p *qwpSenderPool) withLock(op string, slots []*qwpSenderSlot, fn func()) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			p.failSlotsLocked(op, r, slots)
			err = p.poisonedErr
		}
	}()
	if p.failureDone == nil {
		p.failureDone = make(chan struct{})
	}
	fn()
	return nil
}

func (p *qwpSenderPool) failSlotsLocked(op string, cause any, slots []*qwpSenderSlot) {
	for _, slot := range slots {
		if slot != nil && !slot.cleanupFailed {
			slot.cleanupFailed = true
			p.failedSlots = append(p.failedSlots, slot)
		}
	}
	p.poisonLocked(op, cause)
}

// prepareSlotCloseLocked marks a sender as closing and counts its pending
// cleanup. Check and change the slot's state before removing it from p.all.
// The caller must then keep the sender and arrange for it to be closed.
func (p *qwpSenderPool) prepareSlotCloseLocked(slot *qwpSenderSlot, from qwpSfSlotState) bool {
	if slot.cleanupFailed {
		return false
	}
	p.transitionSfSlotLocked(slot.slotIndex, from, qwpSfSlotClosing)
	p.pendingLeaseTeardowns++
	p.removeFromAllLocked(slot)
	return true
}

func (p *qwpSenderPool) removeAvailableLocked(slot *qwpSenderSlot) {
	for i, available := range p.available {
		if available == slot {
			p.available = append(p.available[:i], p.available[i+1:]...)
			return
		}
	}
}

// closeReturnedSlot keeps a reference to the sender until its Close call ends.
// It runs in the background without the pool lock, so returning a borrowed
// sender does not have to wait for files or sockets to close.
func (p *qwpSenderPool) closeReturnedSlot(slot *qwpSenderSlot) {
	err := p.closeSlotTask(slot, nil)
	p.finishSlotClose(slot, err)
	if err != nil {
		qwpEffectiveLogger(p.logger).Warn("qwp pool: returned sender cleanup failed", "error", err)
	}
}

// Catch panics from the test hook and error checks as well as from Close.
func (p *qwpSenderPool) closeSlotTask(slot *qwpSenderSlot, before func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &qwpCleanupPanicError{phase: "pool sender close", cause: r, stack: debug.Stack()}
			p.noteCloseFailure(slot, err)
		}
	}()
	if before != nil {
		before()
	}
	err = closeSlotGuarded(context.Background(), slot.delegate)
	p.noteCloseFailure(slot, err)
	return err
}

// Report internal failure immediately, even if another sender is still closing.
func (p *qwpSenderPool) noteCloseFailure(slot *qwpSenderSlot, err error) {
	if errors.Is(err, ErrCleanupFailed) {
		p.withLock("sender cleanup failure", []*qwpSenderSlot{slot}, func() {
			p.failSlotsLocked("sender close", err, []*qwpSenderSlot{slot})
		})
	}
}

// finishSlotClose saves one sender's close result. If updating its pool entry
// panics, callers can still process the results for the other senders.
func (p *qwpSenderPool) finishSlotClose(slot *qwpSenderSlot, err error) {
	p.withLock("sender cleanup result", []*qwpSenderSlot{slot}, func() {
		p.pendingLeaseTeardowns--
		p.closeTeardownErr = qwpAppendCloseError(p.closeTeardownErr, err)
		p.reclaimSlotLocked(slot, err)
		p.broadcastLocked()
	})
}

func (p *qwpSenderPool) cleanupWaitResult(ctx context.Context, done, failed <-chan struct{}) error {
	select {
	case <-done:
		return p.currentCloseResult()
	default:
	}
	select {
	case <-done:
		return p.currentCloseResult()
	case <-failed:
		return p.currentCloseResult()
	case <-ctx.Done():
		result := p.currentCloseResult()
		if errors.Is(result, ErrCleanupFailed) {
			return result
		}
		select {
		case <-done:
			// Shutdown may have finished since we last checked. Read the result
			// again so we do not miss an error or report finished work as pending.
			return p.currentCloseResult()
		default:
			return errors.Join(result, ErrCleanupPending, ctx.Err())
		}
	}
}
