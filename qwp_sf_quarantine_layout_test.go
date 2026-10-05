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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover the quarantine layout this client shares with the Java
// client: a refused slot is preserved as a sibling under the reserved
// .unreplayable- namespace, participating clients exclude that namespace from
// adoption by name, and the transition is serialised by the parent-anchored
// logical slot lock. The layout fixtures were taken from Java revision
// 981bdb02a471f3b290c89b8e78cbc422610e329e; they are evidence about names and
// files, not live cross-client integration proof.

// javaReferenceIsQuarantinedSlotName copies the inspected Java
// OrphanScanner.isQuarantinedSlotName predicate independently of the production
// Go constant, so changing the Go spelling cannot make the layout fixture pass
// by changing both sides of one assertion.
func javaReferenceIsQuarantinedSlotName(name string) bool {
	return strings.Contains(filepath.Base(name), ".unreplayable-")
}

// qwpSfTestRefusedSlot writes a slot whose recovery fails closed, plus a marker
// file whose bytes the quarantine must carry over untouched.
func qwpSfTestRefusedSlot(t *testing.T, slot string) map[string][]byte {
	t.Helper()
	writeFailClosedSlot(t, slot)
	payload := []byte("bytes the operator has to be able to find")
	require.NoError(t, os.WriteFile(filepath.Join(slot, "evidence.txt"), payload, 0o644))
	contents := map[string][]byte{}
	entries, err := os.ReadDir(slot)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, readErr := os.ReadFile(filepath.Join(slot, e.Name()))
		require.NoError(t, readErr)
		contents[e.Name()] = b
	}
	return contents
}

func qwpSfRequireSameFiles(t *testing.T, dir string, want map[string][]byte) {
	t.Helper()
	for name, payload := range want {
		got, err := os.ReadFile(filepath.Join(dir, name))
		require.NoErrorf(t, err, "preserved copy must still hold %s", name)
		require.Equalf(t, payload, got, "preserved copy must hold %s byte for byte", name)
	}
}

func qwpSfTestSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink privilege is unavailable: %v", err)
		}
		require.NoError(t, err)
	}
}

// TestQwpSfQuarantinePreservesSlotAsSibling pins the destination layout, the
// fresh start, the reported path, and the best-effort marker.
func TestQwpSfQuarantinePreservesSlotAsSibling(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, engine.engineClose()) }()

	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	require.Equal(t, preserved, engine.engineQuarantinedSlotPath())
	require.DirExists(t, preserved)
	require.NoDirExists(t, filepath.Join(root, qwpSfLegacyQuarantineDirName),
		"the nested container must not be recreated")
	qwpSfRequireSameFiles(t, preserved, contents)
	assert.False(t, engine.engineWasRecoveredFromDisk(), "the sender continues on a fresh slot")
	assert.Equal(t, int64(-1), engine.enginePublishedFsn())

	marker, err := os.ReadFile(filepath.Join(preserved, qwpSfFailedSentinelName))
	require.NoError(t, err, "a writable copy gets the current reason recorded")
	assert.NotEmpty(t, strings.TrimSpace(string(marker)))
	assert.False(t, qwpSfIsCandidateOrphan(preserved))
}

// TestQwpSfQuarantineSkipsOccupiedDestinations pins that selection advances
// past anything already at a candidate name and never overwrites it. A file, a
// directory and a dangling symlink all reserve their destination: only a
// confirmed missing path is free.
func TestQwpSfQuarantineSkipsOccupiedDestinations(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	occupiedFile := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	require.NoError(t, os.WriteFile(occupiedFile, []byte("older evidence"), 0o644))
	occupiedDir := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"1")
	require.NoError(t, os.MkdirAll(occupiedDir, 0o755))
	dangling := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"2")
	qwpSfTestSymlink(t, filepath.Join(root, "nothing-here"), dangling)

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, engine.engineClose()) }()

	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"3")
	require.Equal(t, preserved, engine.engineQuarantinedSlotPath())
	qwpSfRequireSameFiles(t, preserved, contents)

	kept, err := os.ReadFile(occupiedFile)
	require.NoError(t, err)
	assert.Equal(t, []byte("older evidence"), kept)
	require.DirExists(t, occupiedDir)
	target, err := os.Readlink(dangling)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(root, "nothing-here"), target)
}

// TestQwpSfQuarantineNamespaceExhaustionRefusesConstruction pins the policy
// choice the client makes at 64 destinations: it refuses, says an operator has
// to act, and leaves every preserved copy and the source slot untouched.
// Retrying the unchanged operation cannot clear it.
func TestQwpSfQuarantineNamespaceExhaustionRefusesConstruction(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)
	for i := 0; i < qwpSfMaxQuarantineSlotAttempts; i++ {
		occupied := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+strconv.Itoa(i))
		require.NoError(t, os.MkdirAll(occupied, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(occupied, "keep"), []byte(strconv.Itoa(i)), 0o644))
	}

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.Error(t, err)
	require.ErrorIs(t, err, qwpSfErrQuarantineNamespaceFull)
	require.NotErrorIs(t, err, ErrSfDurability,
		"operator-owned namespace exhaustion is not a transient storage fault")
	assert.Contains(t, err.Error(), "move or remove")

	qwpSfRequireSameFiles(t, slot, contents)
	for i := 0; i < qwpSfMaxQuarantineSlotAttempts; i++ {
		kept, readErr := os.ReadFile(filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+strconv.Itoa(i), "keep"))
		require.NoError(t, readErr)
		require.Equal(t, strconv.Itoa(i), string(kept))
	}

	// The same call again is refused the same way: nothing was freed.
	engine, err = qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.ErrorIs(t, err, qwpSfErrQuarantineNamespaceFull)
}

// TestQwpSfQuarantineNameTooLongPreservesTheSlot pins the other refusal: a
// legal sender_id long enough to push the destination past the bounded name
// length is not truncated into another sender's namespace.
func TestQwpSfQuarantineNameTooLongPreservesTheSlot(t *testing.T) {
	root := t.TempDir()
	longName := strings.Repeat("s", qwpSfQuarantineNameMaxLen)
	slot := filepath.Join(root, longName)
	contents := qwpSfTestRefusedSlot(t, slot)

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSfDurability,
		"a deterministic naming refusal must stay distinct from transient I/O")
	assert.Contains(t, err.Error(), "shorter sender_id")
	qwpSfRequireSameFiles(t, slot, contents)

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, qwpSfIsQuarantinedSlotName(e.Name()),
			"a refused destination must not have been created under a truncated name")
	}
}

// TestQwpSfQuarantineKeepsAnExistingFailedEntry pins the deliberate difference
// from the Java client, which rewrites the marker: an entry already at that
// name is diagnostic evidence this client does not own, so its contents, and a
// symlink's target, stay untouched. Exclusion never depended on the marker.
func TestQwpSfQuarantineKeepsAnExistingFailedEntry(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		root := t.TempDir()
		slot := filepath.Join(root, "sender-a")
		qwpSfTestRefusedSlot(t, slot)
		require.NoError(t, os.WriteFile(filepath.Join(slot, qwpSfFailedSentinelName),
			[]byte("an older reason"), 0o644))

		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		engine, err := qwpSfNewCursorEngineWithOptions(slot, 4096, qwpSfUnlimitedTotalBytes, 0, qwpSfEngineOpenOptions{
			logger: logger, recoverForeground: true,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, engine.engineClose()) }()

		preserved := engine.engineQuarantinedSlotPath()
		require.NotEmpty(t, preserved)
		kept, err := os.ReadFile(filepath.Join(preserved, qwpSfFailedSentinelName))
		require.NoError(t, err)
		assert.Equal(t, "an older reason", string(kept))
		assert.Contains(t, logs.String(), preserved)
		assert.Contains(t, logs.String(), "recovery failed closed",
			"the current reason is diagnosed even though the older marker is preserved")
	})

	t.Run("existing-symlink", func(t *testing.T) {
		root := t.TempDir()
		slot := filepath.Join(root, "sender-a")
		qwpSfTestRefusedSlot(t, slot)
		outside := filepath.Join(root, "not-to-be-written")
		qwpSfTestSymlink(t, outside, filepath.Join(slot, qwpSfFailedSentinelName))

		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		engine, err := qwpSfNewCursorEngineWithOptions(slot, 4096, qwpSfUnlimitedTotalBytes, 0, qwpSfEngineOpenOptions{
			logger: logger, recoverForeground: true,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, engine.engineClose()) }()

		preserved := engine.engineQuarantinedSlotPath()
		require.NotEmpty(t, preserved)
		target, err := os.Readlink(filepath.Join(preserved, qwpSfFailedSentinelName))
		require.NoError(t, err)
		assert.Equal(t, outside, target, "an existing entry must not be followed or replaced")
		require.NoFileExists(t, outside, "nothing may be written through the symlink")
		assert.False(t, qwpSfIsCandidateOrphan(preserved))
		assert.Contains(t, logs.String(), preserved)
		assert.Contains(t, logs.String(), "recovery failed closed")
	})
}

// TestQwpSfQuarantineMarkerFailuresAreDiagnosedButNonFatal injects each marker
// operation independently. The rename stays complete and name-based exclusion
// stays effective; the controlled sink receives both destination and cause.
func TestQwpSfQuarantineMarkerFailuresAreDiagnosedButNonFatal(t *testing.T) {
	for _, tc := range []struct {
		name string
		arm  func(t *testing.T, fault error)
	}{
		{
			name: "create",
			arm: func(t *testing.T, fault error) {
				original := qwpSfFailedMarkerOpen.load()
				qwpSfFailedMarkerOpen.store(func(string) (*os.File, error) { return nil, fault })
				t.Cleanup(func() { qwpSfFailedMarkerOpen.store(original) })
			},
		},
		{
			name: "write",
			arm: func(t *testing.T, fault error) {
				original := qwpSfFailedMarkerWrite.load()
				qwpSfFailedMarkerWrite.store(func(*os.File, string) (int, error) { return 0, fault })
				t.Cleanup(func() { qwpSfFailedMarkerWrite.store(original) })
			},
		},
		{
			name: "close",
			arm: func(t *testing.T, fault error) {
				original := qwpSfFailedMarkerClose.load()
				qwpSfFailedMarkerClose.store(func(f *os.File) error {
					return errors.Join(original(f), fault)
				})
				t.Cleanup(func() { qwpSfFailedMarkerClose.store(original) })
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			slot := filepath.Join(root, "sender-a")
			contents := qwpSfTestRefusedSlot(t, slot)
			fault := errors.New("injected marker " + tc.name + " failure")
			tc.arm(t, fault)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))

			preserved, err := qwpSfQuarantineSlot(slot, "current recovery reason", logger)
			require.NoError(t, err)
			require.Equal(t, filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0"), preserved)
			qwpSfRequireSameFiles(t, preserved, contents)
			assert.False(t, qwpSfIsCandidateOrphan(preserved))
			assert.Contains(t, logs.String(), preserved)
			assert.Contains(t, logs.String(), fault.Error())
		})
	}
}

// TestQwpSfQuarantineMarkerFailureStillPreserves pins that a marker this client
// cannot write does not fail an otherwise complete preservation, does not undo
// the rename, and does not change the exclusion, which rests on the name. The
// transition is driven directly because the fixture's read-only directory would
// otherwise stop the engine before it ever reached the quarantine.
func TestQwpSfQuarantineMarkerFailureStillPreserves(t *testing.T) {
	if !qwpTestCanEnforceOwnerPermissions() {
		t.Skip("this platform or user cannot enforce the directory permissions this fixture relies on")
	}
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)
	// The slot moves as a whole, so a read-only slot directory follows it to
	// the destination and refuses the marker there.
	require.NoError(t, os.Chmod(slot, 0o555))
	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	t.Cleanup(func() { _ = os.Chmod(preserved, 0o755) })

	disabledLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.Level(100)}))
	got, err := qwpSfQuarantineSlot(slot, "recovery refused the slot", disabledLogger)
	require.NoError(t, err, "a best-effort marker must not fail a completed preservation")
	require.Equal(t, preserved, got)
	qwpSfRequireSameFiles(t, preserved, contents)
	require.NoFileExists(t, filepath.Join(preserved, qwpSfFailedSentinelName))
	assert.False(t, qwpSfIsCandidateOrphan(preserved),
		"exclusion must not depend on a marker the client could not write")
}

// TestQwpSfQuarantineCandidateInspectionFailurePreservesTheSource pins that an
// uninspectable destination is not treated as free.
func TestQwpSfQuarantineCandidateInspectionFailurePreservesTheSource(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	inspectionFault := errors.New("candidate inspection refused")
	original := qwpSfQuarantineLstat.load()
	qwpSfQuarantineLstat.store(func(path string) (os.FileInfo, error) {
		if path == filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0") {
			return nil, inspectionFault
		}
		return original(path)
	})
	t.Cleanup(func() { qwpSfQuarantineLstat.store(original) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.ErrorIs(t, err, inspectionFault)
	require.ErrorIs(t, err, ErrSfDurability)
	assert.Empty(t, qwpSfQuarantineDestination(err))
	qwpSfRequireSameFiles(t, slot, contents)
}

// TestQwpSfQuarantineRenameFailurePreservesTheSource pins the last boundary
// before ownership changes. A failed rename reports its syscall cause but no
// destination, and leaves the source bytes in place.
func TestQwpSfQuarantineRenameFailurePreservesTheSource(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	renameFault := errors.New("rename refused")
	original := qwpSfQuarantineRename.load()
	qwpSfQuarantineRename.store(func(oldPath, newPath string) error {
		if oldPath == slot {
			return renameFault
		}
		return original(oldPath, newPath)
	})
	t.Cleanup(func() { qwpSfQuarantineRename.store(original) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.ErrorIs(t, err, renameFault)
	require.ErrorIs(t, err, ErrSfDurability)
	assert.Empty(t, qwpSfQuarantineDestination(err))
	qwpSfRequireSameFiles(t, slot, contents)
	require.NoDirExists(t, filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0"))
}

// TestQwpSfQuarantineBarrierFailureReportsTheDestination pins the honest
// reporting of a partly completed transition: the rename moved the bytes, the
// directory barrier then failed, and the error still names where the bytes are
// even though no engine is returned and nothing was logged.
func TestQwpSfQuarantineBarrierFailureReportsTheDestination(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	barrierFault := errors.New("barrier refused")
	hook := func(dir string) error {
		if filepath.Clean(dir) == filepath.Clean(root) {
			return barrierFault
		}
		return nil
	}
	qwpSfTestDirSyncHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestDirSyncHook.Store(nil) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.Error(t, err)
	require.ErrorIs(t, err, barrierFault, "the underlying cause stays matchable")

	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	assert.Equal(t, preserved, qwpSfQuarantineDestination(err))
	assert.Contains(t, err.Error(), preserved)
	qwpSfRequireSameFiles(t, preserved, contents)
	require.NoDirExists(t, slot, "a completed rename is not rolled back")
}

// TestQwpSfFreshSlotFailureAfterRenameReportsTheDestination pins the other
// partial transition: the rename completed and the fresh slot could not be
// created. No sender comes back, so the error has to carry the destination.
func TestQwpSfFreshSlotFailureAfterRenameReportsTheDestination(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	// A full disk refuses the fresh slot's first segment. Recovery of the
	// refused slot reads what is already there, so this bites only after the
	// preservation.
	originalReserve := qwpSfReserveNewBlocksFn.load()
	qwpSfReserveNewBlocksFn.store(func(*os.File, int64, int64) error { return syscall.ENOSPC })
	t.Cleanup(func() { qwpSfReserveNewBlocksFn.store(originalReserve) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOSPC, "the underlying cause stays matchable")

	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	assert.Equal(t, preserved, qwpSfQuarantineDestination(err))
	assert.Contains(t, err.Error(), preserved)
	qwpSfRequireSameFiles(t, preserved, contents)
}

// TestQwpSfFailedFreshBuildAndLogicalReleaseKeepEveryCause pins the combined
// failure path: the old slot was preserved, fresh construction failed, and the
// still-held transition lock could not be released immediately. The returned
// reporter owns that lock while every cause and the completed destination stay
// observable.
func TestQwpSfFailedFreshBuildAndLogicalReleaseKeepEveryCause(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	originalReserve := qwpSfReserveNewBlocksFn.load()
	qwpSfReserveNewBlocksFn.store(func(*os.File, int64, int64) error { return syscall.ENOSPC })
	t.Cleanup(func() { qwpSfReserveNewBlocksFn.store(originalReserve) })

	releaseFault := errors.New("logical release refused")
	var allowRelease atomic.Bool
	hook := func(*qwpSfSlotLock) error {
		if !allowRelease.Load() {
			return releaseFault
		}
		return nil
	}
	qwpSfTestBeforeLogicalLockReleaseHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestBeforeLogicalLockReleaseHook.Store(nil) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorIs(t, err, releaseFault)
	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	require.Equal(t, preserved, qwpSfQuarantineDestination(err))
	qwpSfRequireSameFiles(t, preserved, contents)

	var buildErr *qwpSfBuildCleanupError
	require.ErrorAs(t, err, &buildErr)
	_, busy := qwpSfAcquireLogicalSlotLock(slot)
	require.ErrorIs(t, busy, qwpSfErrLockBusy)
	allowRelease.Store(true)
	require.Eventually(t, buildErr.closeCompleted, qwpTestWaitTimeout, 10*time.Millisecond)
}

// TestQwpSfQuarantineErrorReportsEveryCompletedDestination pins the structured
// reporting used if a transition ever completes more than one preservation.
// The public sender path still points to the first/original copy.
func TestQwpSfQuarantineErrorReportsEveryCompletedDestination(t *testing.T) {
	cause := errors.New("fresh transition failed")
	err := &qwpSfQuarantineError{
		destination:            "/sf/sender.unreplayable-0",
		additionalDestinations: []string{"/sf/sender.unreplayable-1"},
		cause:                  cause,
	}
	require.ErrorIs(t, err, cause)
	assert.Equal(t, "/sf/sender.unreplayable-0", qwpSfQuarantineDestination(err))
	assert.Equal(t, []string{"/sf/sender.unreplayable-0", "/sf/sender.unreplayable-1"},
		qwpSfQuarantineDestinations(err))
	assert.Contains(t, err.Error(), "original evidence preserved")
	assert.Contains(t, err.Error(), "/sf/sender.unreplayable-1")
}

// TestQwpSfQuarantinedNamesAreNeverAdopted pins the exclusion every adoption
// route shares. The preserved copy holds exactly what a live orphan holds and
// carries no marker; only its name keeps it out.
func TestQwpSfQuarantinedNamesAreNeverAdopted(t *testing.T) {
	root := t.TempDir()
	ordinary := filepath.Join(root, "orphan-1")
	writeFailClosedSlot(t, ordinary)
	require.True(t, qwpSfIsCandidateOrphan(ordinary))

	preserved := filepath.Join(root, "orphan-2"+qwpSfQuarantineSlotInfix+"7")
	writeFailClosedSlot(t, preserved)
	require.NoFileExists(t, filepath.Join(preserved, qwpSfFailedSentinelName))

	assert.False(t, qwpSfIsCandidateOrphan(preserved))
	require.ErrorIs(t, qwpSfSlotDisqualifiedForAdoption(preserved), qwpSfErrSlotNotAdoptable)
	assert.Equal(t, []string{ordinary}, qwpSfScanOrphans(root, nil))

	// A layout fixture in the Java client's shape is excluded the same way:
	// the infix, not the numeric suffix or the payload, is what matters.
	javaShaped := filepath.Join(root, "default"+qwpSfQuarantineSlotInfix+"0")
	require.NoError(t, os.MkdirAll(javaShaped, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(javaShaped, "sf-initial.sfa"), []byte("x"), 0o644))
	assert.False(t, qwpSfIsCandidateOrphan(javaShaped))
	assert.True(t, qwpSfIsQuarantinedSlotName(javaShaped))
	assert.True(t, javaReferenceIsQuarantinedSlotName(javaShaped),
		"the new Go destination must satisfy the predicate inspected in the pinned Java revision")
	assert.True(t, qwpSfIsQuarantinedSlotName(filepath.Join(root, "sender"+qwpSfQuarantineSlotInfix+"nonsense")))
	assert.False(t, qwpSfIsQuarantinedSlotName(filepath.Join(root, "sender-unreplayable-0")))
}

// TestQwpSfDrainerSkipsASlotPreservedAfterTheScan pins the queued-adopter case
// the scan alone cannot settle: the drainer revalidates under the logical lock
// and leaves the preserved copy alone.
func TestQwpSfDrainerSkipsASlotPreservedAfterTheScan(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	root := t.TempDir()
	slot := filepath.Join(root, "orphan-1")
	writeFailClosedSlot(t, slot)
	queued := qwpSfScanOrphans(root, nil)
	require.Equal(t, []string{slot}, queued)

	// The foreground sender sets the slot aside between the scan and the run.
	preserved, err := qwpSfQuarantineSlot(slot, "test preservation", nil)
	require.NoError(t, err)

	stale := qwpSfNewOrphanDrainer(queued[0], 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv), nil, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	stale.drainerRun(context.Background())
	assert.Equal(t, qwpSfDrainOutcomeLockedByOther, stale.drainerOutcome())
	require.NoDirExists(t, queued[0],
		"the originally queued pathname must not be recreated merely to inspect it")

	// A direct adopter handed the preserved path is independently stopped by
	// the reserved name, even without relying on the stale-path observation.
	drainer := qwpSfNewOrphanDrainer(preserved, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv), nil, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	drainer.drainerRun(context.Background())
	assert.Equal(t, qwpSfDrainOutcomeLockedByOther, drainer.drainerOutcome())
	require.NoFileExists(t, filepath.Join(preserved, ".lock"),
		"a skipped preserved slot must not even be opened")
	require.FileExists(t, filepath.Join(preserved, "sf-initial.sfa"))
}

// TestQwpSfLegacyQuarantineContainerIsNotReinterpreted covers the collision the
// old layout left behind: `quarantined` is a legal sender_id, and a container
// holding an older client's copies must not be opened as a live slot. The
// distinction between an absent path, an ambiguous directory and a failed
// inspection is the point.
func TestQwpSfLegacyQuarantineContainerIsNotReinterpreted(t *testing.T) {
	t.Run("absent-path-is-usable", func(t *testing.T) {
		root := t.TempDir()
		engine, err := qwpSfNewCursorEngine(filepath.Join(root, "quarantined"), 4096, qwpSfUnlimitedTotalBytes, 0)
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	})

	t.Run("empty-directory-is-usable", func(t *testing.T) {
		root := t.TempDir()
		slot := filepath.Join(root, "quarantined")
		require.NoError(t, os.MkdirAll(slot, 0o755))
		engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
		require.NoError(t, err)
		require.NoError(t, engine.engineClose())
	})

	t.Run("ordinary-slot-files-are-usable", func(t *testing.T) {
		root := t.TempDir()
		slot := filepath.Join(root, "quarantined")
		require.NoError(t, os.MkdirAll(slot, 0o755))
		s0 := createRecoverySegment(t, slot, "sf-initial.sfa", 0, "a")
		createRecoveryManifest(t, slot, 0, 0, s0)
		closeRecoverySegments(t, s0)
		engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
		require.NoError(t, err)
		assert.True(t, engine.engineWasRecoveredFromDisk())
		require.NoError(t, engine.engineClose())
	})

	for _, name := range []string{"quarantined", "Quarantined", "QUARANTINED"} {
		t.Run("nested-evidence-refused-"+name, func(t *testing.T) {
			root := t.TempDir()
			slot := filepath.Join(root, name)
			nested := filepath.Join(slot, "sender-a-1699999999")
			require.NoError(t, os.MkdirAll(nested, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(nested, "sf-initial.sfa"), []byte("old copy"), 0o644))
			// A mixed container, with root-level slot files as well, is refused
			// just the same.
			require.NoError(t, os.WriteFile(filepath.Join(slot, "sf-initial.sfa"), []byte("live?"), 0o644))

			engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
			require.Nil(t, engine)
			require.ErrorIs(t, err, qwpSfErrLegacyQuarantineContainer)
			assert.Contains(t, err.Error(), "separate the preserved copies")

			kept, readErr := os.ReadFile(filepath.Join(nested, "sf-initial.sfa"))
			require.NoError(t, readErr)
			assert.Equal(t, "old copy", string(kept), "the refusal must not touch the evidence")
			require.NoFileExists(t, filepath.Join(slot, qwpSfFailedSentinelName),
				"a refusal to interpret a directory is not a corruption verdict")
			assert.False(t, qwpSfIsCandidateOrphan(slot))
			assert.Empty(t, qwpSfScanOrphans(root, nil))
		})
	}

	t.Run("symlink-refused", func(t *testing.T) {
		root := t.TempDir()
		elsewhere := filepath.Join(root, "elsewhere")
		require.NoError(t, os.MkdirAll(elsewhere, 0o755))
		slot := filepath.Join(root, "quarantined")
		qwpSfTestSymlink(t, elsewhere, slot)

		engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
		require.Nil(t, engine)
		require.ErrorIs(t, err, qwpSfErrLegacyQuarantineContainer)
		entries, readErr := os.ReadDir(elsewhere)
		require.NoError(t, readErr)
		assert.Empty(t, entries, "the symlink target must not be touched")
	})

	t.Run("inspection-failure-is-operational", func(t *testing.T) {
		if !qwpTestCanEnforceOwnerPermissions() {
			t.Skip("this platform or user cannot enforce the directory permissions this fixture relies on")
		}
		root := t.TempDir()
		slot := filepath.Join(root, "quarantined")
		require.NoError(t, os.MkdirAll(slot, 0o755))
		require.NoError(t, os.Chmod(slot, 0o000))
		t.Cleanup(func() { _ = os.Chmod(slot, 0o755) })

		err := qwpSfInspectLegacyQuarantinePath(slot)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSfDurability)
		assert.NotErrorIs(t, err, qwpSfErrLegacyQuarantineContainer,
			"an inspection that could not complete is not proof of an ambiguous layout")
		require.NoFileExists(t, filepath.Join(slot, qwpSfFailedSentinelName))

		adoptionErr := qwpSfSlotDisqualifiedForAdoption(slot)
		require.ErrorIs(t, adoptionErr, qwpSfErrSlotNotAdoptable)
		require.ErrorIs(t, adoptionErr, ErrSfDurability,
			"the adoption wrapper must preserve the operational classification")
		var logs bytes.Buffer
		assert.Empty(t, qwpSfScanOrphansWithLogger(root, nil,
			slog.New(slog.NewTextHandler(&logs, nil))))
		assert.Contains(t, logs.String(), "could not inspect an orphan candidate")
		assert.Contains(t, logs.String(), slot)

		// A later successful inspection can proceed.
		require.NoError(t, os.Chmod(slot, 0o755))
		require.NoError(t, qwpSfInspectLegacyQuarantinePath(slot))
	})
}

// TestQwpSfLegacyQuarantineEvidenceIsLeftAlone pins that a root still holding
// the old nested container works normally for other slots, and that nothing
// flattens, renames, or resumes the old copies.
func TestQwpSfLegacyQuarantineEvidenceIsLeftAlone(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "quarantined", "sender-a-1699999999")
	require.NoError(t, os.MkdirAll(legacy, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "sf-initial.sfa"), []byte("old copy"), 0o644))

	slot := filepath.Join(root, "sender-a")
	qwpSfTestRefusedSlot(t, slot)
	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, engine.engineClose()) }()

	assert.Equal(t, filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0"),
		engine.engineQuarantinedSlotPath())
	kept, err := os.ReadFile(filepath.Join(legacy, "sf-initial.sfa"))
	require.NoError(t, err)
	assert.Equal(t, "old copy", string(kept))
	assert.Empty(t, qwpSfScanOrphans(root, func(name string) bool { return name == "sender-a" }))
}

// TestQwpSfSenderIdQuarantinedThroughConfiguration pins the collision fix on
// both configuration paths, not just the internal engine call.
func TestQwpSfSenderIdQuarantinedThroughConfiguration(t *testing.T) {
	srv := newQwpTestServer(t)
	t.Cleanup(srv.Close)
	ctx := context.Background()
	addr := strings.TrimPrefix(srv.URL, "http://")

	for _, id := range []string{"quarantined", "Quarantined", "QUARANTINED"} {
		for _, precreate := range []bool{false, true} {
			state := "absent"
			if precreate {
				state = "existing-empty"
			}
			t.Run("conf-string-"+id+"-"+state, func(t *testing.T) {
				sfRoot := t.TempDir()
				if precreate {
					require.NoError(t, os.MkdirAll(filepath.Join(sfRoot, id), 0o755))
				}
				sender, err := LineSenderFromConf(ctx, fmt.Sprintf(
					"ws::addr=%s;sf_dir=%s;sender_id=%s;close_flush_timeout_millis=200;", addr, sfRoot, id))
				require.NoError(t, err)
				require.NoError(t, sender.Close(ctx))
			})

			t.Run("programmatic-"+id+"-"+state, func(t *testing.T) {
				sfRoot := t.TempDir()
				if precreate {
					require.NoError(t, os.MkdirAll(filepath.Join(sfRoot, id), 0o755))
				}
				sender, err := NewLineSender(ctx,
					WithQwp(), WithAddress(addr), WithSfDir(sfRoot), WithSenderId(id),
					WithCloseFlushTimeout(200*time.Millisecond))
				require.NoError(t, err)
				require.NoError(t, sender.Close(ctx))
			})
		}
	}

	t.Run("refuses-a-container", func(t *testing.T) {
		sfRoot := t.TempDir()
		nested := filepath.Join(sfRoot, "quarantined", "sender-a-1699999999")
		require.NoError(t, os.MkdirAll(nested, 0o755))
		_, err := LineSenderFromConf(ctx, fmt.Sprintf(
			"ws::addr=%s;sf_dir=%s;sender_id=quarantined;close_flush_timeout_millis=200;", addr, sfRoot))
		require.ErrorIs(t, err, qwpSfErrLegacyQuarantineContainer)
		require.DirExists(t, nested)
	})
}
