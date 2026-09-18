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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// qwpSfFailedSentinelName is the per-slot file that disqualifies a
// slot from auto-drain. Drainers drop it on genuine terminals only —
// auth failure, durable-ack settle-budget exhaustion, corrupt
// recovery, a wedged no-progress connection — then human-in-the-loop.
// Transport outages and all-replica windows never drop it (Invariant
// B: they are retried indefinitely).
const qwpSfFailedSentinelName = ".failed"

// qwpSfErrSlotNotAdoptable reports that a slot an earlier scan listed is not
// adoptable any more: it was preserved aside, marked failed, drained away, or
// is otherwise not a candidate now. It is an ordinary lifecycle outcome, not a
// storage fault and not evidence about the slot's bytes, so it must never
// quarantine a slot or write a .failed marker.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrSlotNotAdoptable = errors.New("qwp/sf: slot is no longer an adoption candidate")

// qwpSfErrSlotAlreadyDrained is the successful subset of a stale adoption:
// another owner removed all queued work before this adopter obtained the
// logical lock. The path must not be opened, but the drainer did not fail.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrSlotAlreadyDrained = errors.New("qwp/sf: slot no longer has queued data")

// qwpSfScanOrphans walks the group root sfDir and returns every
// child directory that:
//   - is not excluded by the exclude predicate (a standalone sender
//     excludes its own slot; the QwpSender pool fences its whole
//     in-range slot set so live siblings are never adopted)
//   - contains at least one *.sfa segment file
//   - does NOT contain the .failed sentinel
//
// exclude is called with each child directory's base name; nil
// excludes nothing.
//
// Lock state is intentionally not part of the candidate filter —
// testing it requires actually opening + flocking the lock file,
// which races with concurrent drainers/senders. The drainer pool
// attempts to acquire each candidate's lock in turn and skips ones
// that fail; this keeps the scanner pure and read-only.
//
// Returns an empty list if sfDir doesn't exist or is empty.
func qwpSfScanOrphans(sfDir string, exclude func(name string) bool) []string {
	return qwpSfScanOrphansWithLogger(sfDir, exclude, nil)
}

// qwpSfScanOrphansWithLogger is the production scanner. The test helper above
// remains side-effect-free; production callers supply their effective logger
// so an inspection failure is reported as the operational fault it is instead
// of being indistinguishable from an ordinary exclusion. A missing root is
// expected and stays quiet; other root inspection failures are logged.
func qwpSfScanOrphansWithLogger(sfDir string, exclude func(name string) bool, logger *slog.Logger) []string {
	if sfDir == "" {
		return nil
	}
	if _, err := os.Stat(sfDir); err != nil {
		if logger != nil && !errors.Is(err, os.ErrNotExist) {
			qwpEffectiveLogger(logger).Error("qwp/sf: could not scan orphan root",
				"root", sfDir, "error", err)
		}
		return nil
	}
	entries, err := os.ReadDir(sfDir)
	if err != nil {
		if logger != nil && !errors.Is(err, os.ErrNotExist) {
			qwpEffectiveLogger(logger).Error("qwp/sf: could not scan orphan root",
				"root", sfDir, "error", err)
		}
		return nil
	}
	var orphans []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name == "." || name == ".." {
			continue
		}
		if exclude != nil && exclude(name) {
			continue
		}
		slotPath := filepath.Join(sfDir, name)
		candidate, candidateErr := qwpSfCandidateOrphan(slotPath)
		if candidateErr != nil {
			if logger != nil && errors.Is(candidateErr, ErrSfDurability) {
				qwpEffectiveLogger(logger).Error("qwp/sf: could not inspect an orphan candidate; leaving it eligible for a later scan",
					"slot", slotPath, "error", candidateErr)
			}
			continue
		}
		if candidate {
			orphans = append(orphans, slotPath)
		}
	}
	return orphans
}

// qwpSfIsCandidateOrphan reports whether slotPath looks like a slot
// dir with unacked data and no failure sentinel. Visible for tests.
//
// A slot preserved under the reserved quarantine namespace is never a
// candidate, whatever it contains: it is operator-owned evidence, and its
// .failed marker is best-effort, so the name is what the exclusion rests on.
// A directory whose name matches the legacy quarantine container is excluded
// as well when it holds, or might hold, nested evidence.
func qwpSfIsCandidateOrphan(slotPath string) bool {
	candidate, _ := qwpSfCandidateOrphan(slotPath)
	return candidate
}

// qwpSfCandidateOrphan is the error-preserving form used by production scans.
// A bool alone is enough for selection, but not for diagnostics: an unreadable
// directory is operationally different from a recognized quarantine name.
func qwpSfCandidateOrphan(slotPath string) (bool, error) {
	if err := qwpSfSlotDisqualifiedForAdoption(slotPath); err != nil {
		return false, err
	}
	entries, err := os.ReadDir(slotPath)
	if err != nil {
		return false, qwpSfDurabilityError("inspect orphan candidate", slotPath, err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sfa") {
			return true, nil
		}
	}
	_, err = os.Stat(filepath.Join(slotPath, qwpSfManifestFileName))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, qwpSfDurabilityError("inspect orphan manifest", slotPath, err)
}

// qwpSfRequireCandidateForAdoption re-runs the complete candidate predicate for
// an adopter that already queued slotPath. The earlier scan is only a hint: an
// intervening owner can drain the slot, mark it failed, or preserve it aside.
// Every such change abandons adoption before the engine creates or opens files.
func qwpSfRequireCandidateForAdoption(slotPath string) error {
	candidate, err := qwpSfCandidateOrphan(slotPath)
	if err != nil {
		if errors.Is(err, qwpSfErrSlotNotAdoptable) {
			return err
		}
		return errors.Join(qwpSfErrSlotNotAdoptable, err)
	}
	if !candidate {
		return errors.Join(qwpSfErrSlotNotAdoptable,
			fmt.Errorf("%w [slot=%s]", qwpSfErrSlotAlreadyDrained, slotPath))
	}
	return nil
}

// qwpSfSlotDisqualifiedForAdoption reports why slotPath must not be adopted
// automatically, or nil when nothing disqualifies it. It says nothing about
// whether the slot holds work: an empty, fully drained slot is not disqualified
// by this helper, but qwpSfRequireCandidateForAdoption still rejects it because
// there is no work left to adopt.
//
// A scan and the open that follows it are separated in time, so an adopter
// re-runs the complete candidate predicate under the logical slot lock instead
// of trusting the scan: the slot may have been preserved aside, marked failed,
// or drained away since.
func qwpSfSlotDisqualifiedForAdoption(slotPath string) error {
	// The reserved name alone is authoritative. Do not require a readable path,
	// a marker, a valid numeric suffix or any recognizable payload before
	// excluding operator-owned evidence.
	if qwpSfIsQuarantinedSlotName(slotPath) {
		return fmt.Errorf("%w: the slot was set aside for an operator [slot=%s]", qwpSfErrSlotNotAdoptable, slotPath)
	}
	if filepath.Base(filepath.Clean(slotPath)) == qwpSfLogicalLockDirName {
		return fmt.Errorf("%w: the path is internal logical-lock metadata [slot=%s]", qwpSfErrSlotNotAdoptable, slotPath)
	}
	if _, err := os.Stat(slotPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: slot path disappeared [slot=%s]", qwpSfErrSlotNotAdoptable, slotPath)
		}
		return errors.Join(qwpSfErrSlotNotAdoptable,
			qwpSfDurabilityError("inspect adoption candidate", slotPath, err))
	}
	if qwpSfIsLegacyQuarantineName(filepath.Base(filepath.Clean(slotPath))) {
		// Ambiguous layout, or an inspection that could not complete. Neither
		// authorises adoption; an operational fault leaves the path eligible
		// for a later scan.
		if err := qwpSfInspectLegacyQuarantinePath(slotPath); err != nil {
			// Preserve both classifications. Ambiguity is an ordinary refusal;
			// an I/O failure stays matchable as ErrSfDurability so the drainer
			// reports it instead of silently calling it a lifecycle race.
			return errors.Join(qwpSfErrSlotNotAdoptable, err)
		}
	}
	marker := filepath.Join(slotPath, qwpSfFailedSentinelName)
	if _, err := os.Lstat(marker); err == nil {
		// Any entry reserves the marker name, including a directory or dangling
		// symlink. Following it could both miss the latter and inspect bytes that
		// are not owned by this slot.
		return fmt.Errorf("%w: the slot carries a %s marker [slot=%s]",
			qwpSfErrSlotNotAdoptable, qwpSfFailedSentinelName, slotPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.Join(qwpSfErrSlotNotAdoptable,
			qwpSfDurabilityError("inspect adoption failure marker", marker, err))
	}
	return nil
}

// qwpSfMarkSlotFailed drops a .failed file in slotPath with the
// given reason as content. Idempotent — overwrites on each call so
// the latest reason is recorded. Best-effort.
func qwpSfMarkSlotFailed(slotPath, reason string) {
	path := filepath.Join(slotPath, qwpSfFailedSentinelName)
	body := reason
	if body == "" {
		body = "drainer failed"
	}
	_ = os.WriteFile(path, []byte(body), 0o644)
}
