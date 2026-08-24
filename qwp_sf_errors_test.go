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
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQwpSfBoundaryErrorTaxonomy(t *testing.T) {
	cause := syscall.ENOSPC
	failClosed := qwpSfFailClosed("manifest boundaries disagree")
	durability := qwpSfDurabilityError("write manifest", "/slot/sf-manifest.bin", cause)
	combined := errors.Join(failClosed, durability)
	cleanupPending := fmt.Errorf("close snapshot: %w", ErrSfCleanupPending)
	poolPoison := fmt.Errorf("classification panic: %w", ErrPoolPoisoned)

	tests := []struct {
		name                string
		err                 error
		wantDurability      bool
		wantFailClosed      bool
		wantCleanupPending  bool
		wantPoolPoison      bool
		wantFilesystemCause bool
	}{
		{name: "durability", err: durability, wantDurability: true, wantFilesystemCause: true},
		{name: "fail_closed", err: failClosed, wantFailClosed: true},
		{name: "combined", err: combined, wantDurability: true, wantFailClosed: true, wantFilesystemCause: true},
		{name: "cleanup_pending", err: cleanupPending, wantCleanupPending: true},
		{name: "pool_poison", err: poolPoison, wantPoolPoison: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantDurability, errors.Is(tc.err, ErrSfDurability))
			require.Equal(t, tc.wantFailClosed, errors.Is(tc.err, qwpSfErrRecoveryFailClosed))
			require.Equal(t, tc.wantCleanupPending, errors.Is(tc.err, ErrSfCleanupPending))
			require.Equal(t, tc.wantPoolPoison, errors.Is(tc.err, ErrPoolPoisoned))
			require.Equal(t, tc.wantFilesystemCause, errors.Is(tc.err, cause))
		})
	}
}

func TestQwpSfFailClosedQuarantineFailurePreservesBothErrorClasses(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender")
	require.NoError(t, os.MkdirAll(slot, 0o755))
	segment, err := qwpSfCreateSegment(filepath.Join(slot, "sf-initial.sfa"), 0, 4096)
	require.NoError(t, err)
	require.NoError(t, segment.markManifestRequired())
	require.NoError(t, segment.close())

	injected := syscall.EIO
	originalDirSync := qwpSfTestDirSyncHook.Load()
	t.Cleanup(func() { qwpSfTestDirSyncHook.Store(originalDirSync) })
	dirSync := func(dir string) error {
		if dir == root {
			return injected
		}
		return nil
	}
	qwpSfTestDirSyncHook.Store(&dirSync)

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.Nil(t, engine)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.ErrorIs(t, err, ErrSfDurability)
	require.ErrorIs(t, err, injected)
}
