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
	if sfDir == "" {
		return nil
	}
	if _, err := os.Stat(sfDir); err != nil {
		return nil
	}
	entries, err := os.ReadDir(sfDir)
	if err != nil {
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
		if qwpSfIsCandidateOrphan(slotPath) {
			orphans = append(orphans, slotPath)
		}
	}
	return orphans
}

// qwpSfIsCandidateOrphan reports whether slotPath looks like a slot
// dir with unacked data and no failure sentinel. Visible for tests.
func qwpSfIsCandidateOrphan(slotPath string) bool {
	if _, err := os.Stat(slotPath); err != nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(slotPath, qwpSfFailedSentinelName)); err == nil {
		return false
	}
	return qwpSfHasAnySegmentFile(slotPath)
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

// qwpSfHasAnySegmentFile reports whether slotPath contains at least
// one *.sfa file.
func qwpSfHasAnySegmentFile(slotPath string) bool {
	entries, err := os.ReadDir(slotPath)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sfa") {
			return true
		}
	}
	return false
}
