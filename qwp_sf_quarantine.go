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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// qwpSfQuarantineSlotInfix names a whole slot directory the client refused and
// set aside for an operator. The layout is a sibling of the original slot:
//
//	<sf_dir>/<sender_id>.unreplayable-<n>
//
// It matches the Java client's reserved namespace, so a participating client of
// either language excludes these copies from adoption by name alone. A valid
// sender_id cannot contain a dot, so the namespace cannot collide with a
// configured slot name.
const qwpSfQuarantineSlotInfix = ".unreplayable-"

// qwpSfMaxQuarantineSlotAttempts bounds the destinations one slot name may
// occupy, matching the Java client's 0..63 selection. This is a policy choice,
// not a format requirement: preserved copies are operator-owned, and a client
// that kept minting new ones would quietly fill the filesystem instead of
// asking for human attention. Exhaustion refuses construction; it never deletes
// evidence to free a name.
const qwpSfMaxQuarantineSlotAttempts = 64

// qwpSfErrQuarantineNamespaceFull reports that every destination for a slot
// name is occupied. Retrying the unchanged operation cannot clear it: an
// operator has to move or remove the preserved copies.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrQuarantineNamespaceFull = errors.New("qwp/sf: every quarantine destination for this slot name is occupied")

// qwpSfLegacyQuarantineDirName is the container older versions of this client
// created for whole-slot copies (<sf_dir>/quarantined/<copy>/). Those copies are
// left exactly where they are: nothing flattens, renames, scans, or resumes
// them. The name is also a legal sender_id, so a slot configured with it has to
// be told apart from an evidence container.
const qwpSfLegacyQuarantineDirName = "quarantined"

// qwpSfErrLegacyQuarantineContainer refuses to interpret a directory that could
// be an older client's quarantine container as a live slot. It is a refusal to
// guess, not a corruption verdict and not permission to move anything.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrLegacyQuarantineContainer = errors.New("qwp/sf: slot path looks like a legacy quarantine container")

// Cold-path filesystem seams make the preservation boundaries deterministic
// in tests. Production values are the direct os operations.
var qwpSfQuarantineLstat = qwpSfSwappable(os.Lstat)
var qwpSfQuarantineRename = qwpSfSwappable(os.Rename)
var qwpSfFailedMarkerOpen = qwpSfSwappable(func(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
})
var qwpSfFailedMarkerWrite = qwpSfSwappable(func(f *os.File, body string) (int, error) {
	return f.WriteString(body)
})
var qwpSfFailedMarkerClose = qwpSfSwappable(func(f *os.File) error { return f.Close() })

// qwpSfIsQuarantinedSlotName reports whether path's last element names a
// preserved copy. It deliberately ignores the .failed marker, the numeric
// suffix, and the payload: marker creation is best-effort and the exact
// condition that makes a slot unreplayable (a full or read-only disk) is the
// condition that makes writing a marker fail. Name-based exclusion is what
// keeps the copy out of adoption regardless.
func qwpSfIsQuarantinedSlotName(path string) bool {
	if path == "" {
		return false
	}
	name := filepath.Base(filepath.Clean(path))
	return strings.Contains(name, qwpSfQuarantineSlotInfix)
}

// qwpSfQuarantineError reports a quarantine transition that did not complete,
// while still naming any destination the attempt did produce. A rename followed
// by a failed barrier is not a rollback: the bytes are at the destination, and
// the caller must be able to say so without depending on a returned sender or
// on log delivery.
type qwpSfQuarantineError struct {
	// destination is the first completed rename target, or "" when the source
	// was never moved. additionalDestinations records later completed moves
	// without changing QuarantinedSlotPath's first-copy rule.
	destination            string
	additionalDestinations []string
	cause                  error
}

func (e *qwpSfQuarantineError) Error() string {
	if e.destination == "" {
		return e.cause.Error()
	}
	if len(e.additionalDestinations) == 0 {
		return fmt.Sprintf("%s [preserved at %s]", e.cause.Error(), e.destination)
	}
	return fmt.Sprintf("%s [original evidence preserved at %s; additional completed destinations: %s]",
		e.cause.Error(), e.destination, strings.Join(e.additionalDestinations, ", "))
}

func (e *qwpSfQuarantineError) Unwrap() error { return e.cause }

// qwpSfQuarantineDestination returns the preserved destination recorded in err,
// or "" when no rename completed.
func qwpSfQuarantineDestination(err error) string {
	destinations := qwpSfQuarantineDestinations(err)
	if len(destinations) == 0 {
		return ""
	}
	return destinations[0]
}

// qwpSfQuarantineDestinations returns every completed preservation destination
// carried by err, in transition order. The first remains the original evidence
// reported by QuarantinedSlotPath; later entries can hold partial fresh slots.
func qwpSfQuarantineDestinations(err error) []string {
	var qErr *qwpSfQuarantineError
	if !errors.As(err, &qErr) || qErr.destination == "" {
		return nil
	}
	out := make([]string, 1, 1+len(qErr.additionalDestinations))
	out[0] = qErr.destination
	return append(out, qErr.additionalDestinations...)
}

// qwpSfQuarantineSlot preserves a whole slot directory under the reserved
// sibling namespace and returns the destination.
//
// Preservation means this operation does not overwrite, delete, replay, or
// reclaim the bytes it moves. It is not a backup, it does not repair damage
// recovery already found, and it cannot protect the copy from later storage
// failure or from an operator. A successful return means the rename and the
// directory barrier completed; every other outcome returns a
// *qwpSfQuarantineError that names whatever destination did complete.
//
// The .failed marker inside the new copy is best-effort and is written only
// when no entry of that name exists: an older marker is diagnostic evidence
// this client does not own. Exclusion from adoption depends on the destination
// name, never on the marker.
func qwpSfQuarantineSlot(slotDir, reason string, logger *slog.Logger) (string, error) {
	clean := filepath.Clean(slotDir)
	parent, base := filepath.Dir(clean), filepath.Base(clean)
	target, err := qwpSfSelectQuarantineTarget(parent, base)
	if err != nil {
		return "", &qwpSfQuarantineError{cause: err}
	}
	if err := qwpSfQuarantineRename.load()(clean, target); err != nil {
		return "", &qwpSfQuarantineError{
			cause: qwpSfDurabilityError("quarantine slot as "+target, clean, err),
		}
	}
	// From here the bytes live at target. Every later failure reports that
	// destination rather than claiming the transition did not happen.
	if err := qwpSfSyncSlotDir(parent); err != nil {
		return "", &qwpSfQuarantineError{
			destination: target,
			cause:       qwpSfDurabilityError("sync slot parent after quarantine", parent, err),
		}
	}
	created, markerErr := qwpSfCreateFailedMarker(target, reason)
	if markerErr != nil {
		// A marker-only failure must not fail an otherwise complete
		// preservation, and must not be repaired by removing evidence.
		qwpEffectiveLogger(logger).Error(
			"qwp/sf: could not record the .failed marker in a preserved slot; the copy is still excluded from adoption by name",
			"quarantined", target, "error", markerErr)
	} else if !created {
		qwpEffectiveLogger(logger).Warn(
			"qwp/sf: preserved slot already carried a .failed entry; keeping it unchanged",
			"quarantined", target, "reason", reason)
	}
	return target, nil
}

// qwpSfSelectQuarantineTarget picks the first unoccupied destination for base
// under parent. Only a confirmed missing path is free: a file, a directory, and
// a symlink (including a dangling one) all reserve their candidate, and any
// other inspection failure is an operational error rather than evidence that
// the name may be taken.
func qwpSfSelectQuarantineTarget(parent, base string) (string, error) {
	for i := 0; i < qwpSfMaxQuarantineSlotAttempts; i++ {
		name := base + qwpSfQuarantineSlotInfix + strconv.Itoa(i)
		if len(name) > qwpSfQuarantineNameMaxLen {
			// Truncating the stem would aim the rename at a name another
			// sender_id can legitimately own, so refuse instead. The slot is
			// untouched and an operator can move it by hand.
			return "", fmt.Errorf(
				"qwp/sf: quarantine destination name would exceed %d bytes [slot=%s]; "+
					"move or remove the slot directory by hand, or configure a shorter sender_id",
				qwpSfQuarantineNameMaxLen, filepath.Join(parent, base))
		}
		candidate := filepath.Join(parent, name)
		_, err := qwpSfQuarantineLstat.load()(candidate)
		if err == nil {
			continue
		}
		if errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		}
		return "", qwpSfDurabilityError("inspect quarantine destination", candidate, err)
	}
	return "", fmt.Errorf(
		"%w [slot=%s, destinations=%d]; the preserved copies are operator-owned: move or remove them, "+
			"then retry. Nothing is deleted automatically to free a destination",
		qwpSfErrQuarantineNamespaceFull, filepath.Join(parent, base), qwpSfMaxQuarantineSlotAttempts)
}

// qwpSfCreateFailedMarker writes reason to <slotPath>/.failed only when no
// entry of that name exists, reporting whether it created one.
//
// Exclusive creation is what keeps an existing marker's contents, and an
// existing symlink's target, untouched: O_EXCL fails on any existing entry and
// never follows one. Unlike qwpSfMarkSlotFailed, which the background drainer
// uses to record its latest reason, every failure here is observed and
// reported.
func qwpSfCreateFailedMarker(slotPath, reason string) (bool, error) {
	path := filepath.Join(slotPath, qwpSfFailedSentinelName)
	f, err := qwpSfFailedMarkerOpen.load()(path)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, qwpSfDurabilityError("create failure marker", path, err)
	}
	body := reason
	if body == "" {
		body = "slot set aside by the sender"
	}
	payload := body + "\n"
	n, writeErr := qwpSfFailedMarkerWrite.load()(f, payload)
	if writeErr == nil && n != len(payload) {
		writeErr = io.ErrShortWrite
	}
	closeErr := qwpSfFailedMarkerClose.load()(f)
	if writeErr != nil || closeErr != nil {
		return true, qwpSfDurabilityError("write failure marker", path, errors.Join(writeErr, closeErr))
	}
	return true, nil
}

// qwpSfIsLegacyQuarantineName reports whether name is the container older
// clients created. The comparison is ASCII case-insensitive on every platform:
// a case-sensitive filesystem can hold `Quarantined` beside `quarantined`, and
// refusing both is the conservative reading. No filesystem probing is done.
func qwpSfIsLegacyQuarantineName(name string) bool {
	return strings.EqualFold(name, qwpSfLegacyQuarantineDirName)
}

// qwpSfInspectLegacyQuarantinePath decides whether slotDir, whose name matches
// the legacy container, may be used as a live slot.
//
// It distinguishes three outcomes the caller must not conflate:
//
//   - a confirmed missing path, or an existing directory holding nothing but
//     ordinary slot files, is usable; the name alone is not prohibited;
//   - a directory holding child directories or symlinks, or a symlink at the
//     slot path itself, is ambiguous: it may hold an older client's evidence,
//     so this refuses to interpret it. That refusal is not a corruption verdict
//     and never authorises moving or deleting anything;
//   - a permission or I/O failure is operational and is returned as such. An
//     inspection that could not complete is not proof of either safety or
//     ambiguity.
//
// Nothing here follows a symlink to decide that its target is safe, and nothing
// creates the slot path merely to inspect it.
func qwpSfInspectLegacyQuarantinePath(slotDir string) error {
	info, err := os.Lstat(slotDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return qwpSfDurabilityError("inspect legacy quarantine container", slotDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf(
			"%w: the slot path is a symlink, so its contents cannot be attributed [slot=%s]. "+
				"Stop every owner of this sf_dir and separate any preserved evidence from live slot data before reusing this name",
			qwpSfErrLegacyQuarantineContainer, slotDir)
	}
	if !info.IsDir() {
		return fmt.Errorf(
			"%w: the slot path is not a directory [slot=%s]. Move it aside by hand before using this sender_id",
			qwpSfErrLegacyQuarantineContainer, slotDir)
	}
	entries, err := os.ReadDir(slotDir)
	if err != nil {
		return qwpSfDurabilityError("inspect legacy quarantine container", slotDir, err)
	}
	for _, e := range entries {
		// A child directory or symlink is how the old layout stored a whole
		// preserved slot. Refusing an unrelated child of that shape is an
		// accepted false positive: it asks for a human look instead of
		// reinterpreting evidence as a live slot.
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf(
				"%w: it holds %q, which may be an older client's preserved slot [slot=%s]. "+
					"Stop every owner of this sf_dir and separate the preserved copies from live slot data; "+
					"do not delete the container wholesale",
				qwpSfErrLegacyQuarantineContainer, e.Name(), slotDir)
		}
	}
	return nil
}
