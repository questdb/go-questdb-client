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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin that a sender pool's saved close errors stay the same size
// however many senders fail, so walking them never overflows the stack.

// qwpTestErrorDepth returns the number of error values on the longest path
// through Unwrap() error and Unwrap() []error, counting the starting error
// and the leaf.
func qwpTestErrorDepth(err error) int {
	if err == nil {
		return 0
	}
	deepest := 0
	switch u := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range u.Unwrap() {
			deepest = max(deepest, qwpTestErrorDepth(child))
		}
	case interface{ Unwrap() error }:
		deepest = qwpTestErrorDepth(u.Unwrap())
	}
	return 1 + deepest
}

// poolCloseOnlyCleanup reports close completion but no later cleanup results,
// so finishSlotClose saves its close error directly.
type poolCloseOnlyCleanup struct{}

func (poolCloseOnlyCleanup) closeCompleted() bool { return true }

// poolResultCleanup reports a completed close and a cleanup result, which the
// pool collects when it reclaims or reprobes the slot.
type poolResultCleanup struct{ result error }

func (c *poolResultCleanup) closeCompleted() bool { return true }
func (c *poolResultCleanup) cleanupResult() error { return c.result }

func TestQwpBoundedCloseErrorsStaysBounded(t *testing.T) {
	const total = 100_000
	errs := make([]error, total)
	for i := range errs {
		errs[i] = fmt.Errorf("close error %d", i+1)
	}
	failure := fmt.Errorf("%w: close error 50000", ErrCleanupFailed)
	errs[49_999] = failure

	var acc error
	var afterTen error
	var afterTenText string
	for i, err := range errs {
		acc = qwpAppendBoundedCloseError(acc, err)
		if i == 9 {
			afterTen, afterTenText = acc, acc.Error()
		}
	}

	// The retained failure wraps ErrCleanupFailed, so the longest path is
	// record -> failure -> ErrCleanupFailed.
	require.LessOrEqual(t, qwpTestErrorDepth(acc), 3)
	text := acc.Error()
	require.Less(t, len(text), 1024)
	require.Contains(t, text, "close error 1\n")
	require.Contains(t, text, "close error 100000")
	require.Contains(t, text, "99997 more")
	require.ErrorIs(t, acc, errs[0])
	require.ErrorIs(t, acc, errs[total-1])
	require.ErrorIs(t, acc, ErrCleanupFailed)
	require.ErrorIs(t, acc, failure)
	require.Equal(t, 99_997, acc.(*qwpBoundedCloseErrors).omitted)

	// A result already handed out does not change as more errors arrive.
	require.Equal(t, 8, afterTen.(*qwpBoundedCloseErrors).omitted)
	require.Equal(t, afterTenText, afterTen.Error())
}

func TestQwpBoundedCloseErrorsEdgeRules(t *testing.T) {
	first := errors.New("first")
	require.Nil(t, qwpAppendBoundedCloseError(nil, nil))
	require.True(t, qwpAppendBoundedCloseError(first, nil) == first)
	require.True(t, qwpAppendBoundedCloseError(nil, first) == first)

	// A first error that already matches ErrCleanupFailed needs no second copy.
	failedFirst := fmt.Errorf("%w: first", ErrCleanupFailed)
	acc := qwpAppendBoundedCloseError(failedFirst, fmt.Errorf("%w: second", ErrCleanupFailed))
	acc = qwpAppendBoundedCloseError(acc, errors.New("third"))
	rec := acc.(*qwpBoundedCloseErrors)
	require.Nil(t, rec.failed)
	require.Equal(t, 1, rec.omitted)

	// While the failure is also the last error, it is listed once.
	failure := fmt.Errorf("%w: second", ErrCleanupFailed)
	acc = qwpAppendBoundedCloseError(first, failure)
	require.Len(t, acc.(*qwpBoundedCloseErrors).Unwrap(), 2)
	require.Equal(t, 1, strings.Count(acc.Error(), "second"))
}

func TestQwpSenderPoolFinishSlotCloseErrorsStayBounded(t *testing.T) {
	const closes = 10_000
	p := &qwpSenderPool{notify: make(chan struct{})}
	p.pendingLeaseTeardowns = closes
	for i := 1; i <= closes; i++ {
		slot := &qwpSenderSlot{cleanup: poolCloseOnlyCleanup{}, slotIndex: -1}
		p.finishSlotClose(slot, fmt.Errorf("close error %d", i))
	}
	require.LessOrEqual(t, qwpTestErrorDepth(p.closeTeardownErr), 2)
	result := p.stableCloseResult().Error()
	require.Contains(t, result, "close error 1\n")
	require.Contains(t, result, fmt.Sprintf("close error %d", closes))
}

func TestQwpSenderPoolReclaimedCleanupResultsStayBounded(t *testing.T) {
	const closes = 10_000
	p := &qwpSenderPool{notify: make(chan struct{})}
	p.pendingLeaseTeardowns = closes
	for i := 1; i <= closes; i++ {
		cleanup := &poolResultCleanup{result: fmt.Errorf("cleanup result %d", i)}
		slot := &qwpSenderSlot{cleanup: cleanup, slotIndex: -1}
		p.finishSlotClose(slot, cleanup.result)
	}
	require.LessOrEqual(t, qwpTestErrorDepth(p.closeTeardownErr), 2)
	result := p.stableCloseResult().Error()
	require.Contains(t, result, "cleanup result 1\n")
	require.Contains(t, result, fmt.Sprintf("cleanup result %d", closes))
}

func TestQwpSenderPoolRetiredCleanupResultsStayBounded(t *testing.T) {
	const closes, batch = 10_000, 100
	p := &qwpSenderPool{notify: make(chan struct{})}
	for start := 1; start <= closes; start += batch {
		p.mu.Lock()
		for i := start; i < start+batch; i++ {
			cleanup := &poolResultCleanup{result: fmt.Errorf("retired result %d", i)}
			p.retiredSlots = append(p.retiredSlots, &qwpSenderSlot{cleanup: cleanup, slotIndex: -1})
		}
		require.Equal(t, batch, p.reprobeRetiredSlotsLocked())
		p.mu.Unlock()
	}
	require.Empty(t, p.retiredSlots)
	require.LessOrEqual(t, qwpTestErrorDepth(p.closeTeardownErr), 2)
	result := p.stableCloseResult().Error()
	require.Contains(t, result, "retired result 1\n")
	require.Contains(t, result, fmt.Sprintf("retired result %d", closes))
}
