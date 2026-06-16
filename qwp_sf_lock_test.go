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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfSlotLockAcquireCreatesDirAndLockFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "child", "slot")
	l, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	defer func() { _ = l.close() }()

	// Directory was auto-created.
	st, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, st.IsDir())

	// .lock file exists and is empty — the locked range on Windows
	// would otherwise prevent a contender from reading the PID.
	lockBody, err := os.ReadFile(filepath.Join(dir, qwpSfLockFileName))
	require.NoError(t, err)
	assert.Empty(t, lockBody)

	// .lock.pid sidecar holds our PID.
	pidBody, err := os.ReadFile(filepath.Join(dir, qwpSfLockPidFileName))
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBody)))
	require.NoError(t, err)
	assert.Equal(t, os.Getpid(), pid)
}

func TestQwpSfSlotLockContentionReportsHolder(t *testing.T) {
	dir := t.TempDir()
	l1, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	defer func() { _ = l1.close() }()

	_, err = qwpSfAcquireSlotLock(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "slot already in use")
	assert.Contains(t, err.Error(), fmt.Sprintf("pid=%d", os.Getpid()))
}

func TestQwpSfSlotLockReleaseAllowsReacquire(t *testing.T) {
	dir := t.TempDir()
	l1, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, l1.close())

	// Stale .lock file should still exist (we never unlink) but the
	// flock is gone, so a fresh acquire succeeds.
	_, err = os.Stat(filepath.Join(dir, qwpSfLockFileName))
	require.NoError(t, err)

	l2, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	require.NoError(t, l2.close())
}

func TestQwpSfSlotLockEmptyDirIsRejected(t *testing.T) {
	_, err := qwpSfAcquireSlotLock("")
	require.Error(t, err)
}

func TestQwpSfSlotLockReportsSlotPath(t *testing.T) {
	dir := t.TempDir()
	l, err := qwpSfAcquireSlotLock(dir)
	require.NoError(t, err)
	defer func() { _ = l.close() }()
	assert.Equal(t, dir, l.slotPath())
}
