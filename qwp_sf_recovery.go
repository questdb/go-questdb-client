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
	"sort"
	"strings"
	"time"
)

// No recovery path, failed or otherwise, destroys a frame the committed
// boundaries still require. Recovery does mutate the slot before it knows it
// will succeed -- it zeroes bytes the boundaries prove dead, flags chain
// headers as manifest-required, creates or removes the manifest, installs a
// clean segment where a torn one held no recoverable frame, and removes files
// proven stale -- but every file that holds a frame the manifest still accounts
// for is either left exactly as it was or preserved under another name.
// TestQwpSfFailedRecoveryPreservesEveryRequiredFrame pins the failed half. On
// the success path the committed head is what licenses a removal:
// qwpSfDiscardOpened unlinks a segment only below that boundary, where the
// manifest proves its frames delivered, and quarantines anything at or above
// it that still carries frames or a torn tail.
var (
	//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
	qwpSfErrRecoveryFailClosed = errors.New("qwp/sf: recovery failed closed")
	//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
	qwpSfErrSanitizedResidue = errors.New("qwp/sf: sanitized sealed-segment residue; retry recovery once")
)

func qwpSfOpenRing(sfDir string, maxBytesPerSegment int64) (*qwpSfSegmentRing, error) {
	ring, _, err := qwpSfRecoverRing(sfDir, maxBytesPerSegment)
	return ring, err
}

func qwpSfRecoverRing(sfDir string, maxBytesPerSegment int64) (_ *qwpSfSegmentRing, _ *qwpSfManifest, retErr error) {
	if _, err := os.Stat(sfDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("qwp/sf: stat %s: %w", sfDir, err)
	}
	entries, err := os.ReadDir(sfDir)
	if err != nil {
		return nil, nil, fmt.Errorf("qwp/sf: read %s: %w", sfDir, err)
	}

	var all []*qwpSfSegment
	var corruptPaths []string
	var manifest *qwpSfManifest
	success := false
	defer func() {
		if success {
			return
		}
		for _, seg := range all {
			_ = seg.close()
		}
		if manifest != nil {
			_ = manifest.close()
		}
	}()

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sfa") {
			continue
		}
		path := filepath.Join(sfDir, entry.Name())
		seg, openErr := qwpSfOpenSegment(path)
		if openErr != nil {
			if errors.Is(openErr, qwpSfErrSegmentCorrupt) {
				corruptPaths = append(corruptPaths, path)
				qwpSfLogGuarded(nil, slog.LevelWarn, "qwp/sf: deferring corrupt segment quarantine until recovery boundaries validate", "path", path, "error", openErr)
				continue
			}
			return nil, nil, fmt.Errorf("qwp/sf: open segment %s during recovery: %w", path, openErr)
		}
		all = append(all, seg)
	}
	// A corrupt file's identity is unknown -- segment names carry a generation,
	// not a base -- so wherever the tail of the chain has to be proved, one that
	// could be carrying frames blocks the proof. One that provably carries none
	// does not: it cannot be the missing tail, whatever position it holds.
	var framefulCorrupt []string
	for _, path := range corruptPaths {
		if qwpSfCorruptMayHoldFrames(path) {
			framefulCorrupt = append(framefulCorrupt, path)
		}
	}

	manifest, err = qwpSfManifestOpen(sfDir)
	if err != nil {
		return nil, nil, err
	}
	if len(all) == 0 {
		// With every file unreadable, one that could be carrying frames is the
		// whole chain as far as recovery can tell -- manifest or not, nothing
		// here can show its frames delivered, so the slot fails closed and the
		// caller preserves it.
		if len(framefulCorrupt) > 0 {
			return nil, nil, qwpSfFailClosed("every SF segment file is unreadable and one of them may carry frames: %s", strings.Join(framefulCorrupt, ", "))
		}
		if manifest != nil && manifest.headBase != manifest.activeBase {
			return nil, nil, qwpSfFailClosed("sf-manifest.bin references durable data but no segment file carries frames")
		}
		if manifest != nil {
			qwpSfLogGuarded(nil, slog.LevelWarn, "qwp/sf: removing collapsed manifest with no segment files", "dir", sfDir)
			if err := manifest.close(); err != nil {
				return nil, nil, err
			}
			manifest = nil
			if !qwpSfManifestRemove(sfDir) {
				return nil, nil, fmt.Errorf("qwp/sf: remove collapsed manifest in %s", sfDir)
			}
		}
		// What is left provably carries no frames -- the residue of a crash
		// during segment creation -- so the bytes are preserved aside for
		// forensics and the slot starts fresh.
		qwpSfQuarantinePaths(corruptPaths)
		success = true
		return nil, nil, nil
	}

	data := make([]*qwpSfSegment, 0, len(all))
	requiresManifest := false
	for _, seg := range all {
		if seg.segmentFrameCount() > 0 {
			data = append(data, seg)
		}
		requiresManifest = requiresManifest || seg.segmentManifestRequired()
	}
	sort.Slice(data, func(i, j int) bool {
		return uint64(data[i].segmentBaseSeq()) < uint64(data[j].segmentBaseSeq())
	})
	if manifest == nil && requiresManifest {
		return nil, nil, qwpSfFailClosed("new-format SF segment exists but sf-manifest.bin is missing")
	}

	var chain []*qwpSfSegment
	var activeSeg *qwpSfSegment
	var preserve map[*qwpSfSegment]struct{}
	if manifest != nil {
		head, active := manifest.headBase, manifest.activeBase
		activeSeg = qwpSfFindActive(all, active)
		for _, seg := range data {
			base := seg.segmentBaseSeq()
			end := base + seg.segmentFrameCount()
			if base < head {
				if end > head {
					return nil, nil, qwpSfFailClosed("segment overlaps committed SF head boundary")
				}
				continue
			}
			if base > active {
				return nil, nil, qwpSfFailClosed("segment exists beyond committed SF active boundary")
			}
			if base == active && seg != activeSeg {
				// A duplicate at the committed active base that the chain does
				// not adopt. Rotation always assigns a strictly greater base,
				// so a frameful one is not reachable today; preserving it
				// rather than unlinking it keeps the no-frame-destroyed
				// guarantee at the top of this file resting on the code
				// instead of on that argument.
				if seg.segmentFrameCount() > 0 {
					if preserve == nil {
						preserve = make(map[*qwpSfSegment]struct{}, 1)
					}
					preserve[seg] = struct{}{}
				}
				continue
			}
			chain = append(chain, seg)
		}
		if len(chain) > 0 {
			if err := qwpSfValidateContiguous(chain); err != nil {
				return nil, nil, err
			}
			if chain[0].segmentBaseSeq() != head {
				return nil, nil, qwpSfFailClosed("missing expected SF head segment at base %d", head)
			}
		}
		if activeSeg == nil {
			if len(chain) == 0 && head == active && len(framefulCorrupt) == 0 {
				if err := qwpSfDiscardOpened(all, nil, nil); err != nil {
					return nil, nil, err
				}
				all = nil
				if err := manifest.close(); err != nil {
					return nil, nil, err
				}
				manifest = nil
				if !qwpSfManifestRemove(sfDir) {
					return nil, nil, fmt.Errorf("qwp/sf: remove clean-drain manifest in %s", sfDir)
				}
				success = true
				return nil, nil, nil
			}
			return nil, nil, qwpSfFailClosed("missing expected SF active segment at base %d", active)
		}
		if len(chain) == 0 {
			if head != active || activeSeg.segmentFrameCount() != 0 || len(framefulCorrupt) > 0 {
				suffix := ""
				if len(framefulCorrupt) > 0 {
					suffix = " (a corrupt segment prevents proving the empty state)"
				}
				return nil, nil, qwpSfFailClosed("missing SF chain between committed boundaries%s", suffix)
			}
			chain = append(chain, activeSeg)
		} else if chain[len(chain)-1] != activeSeg {
			last := chain[len(chain)-1]
			chainEnd := last.segmentBaseSeq() + last.segmentFrameCount()
			if len(framefulCorrupt) == 0 && activeSeg.segmentFrameCount() == 0 && activeSeg.segmentBaseSeq() == chainEnd {
				chain = append(chain, activeSeg)
			} else {
				return nil, nil, qwpSfFailClosed("missing expected SF active/tail segment at base %d", active)
			}
		}
		if path, err := qwpSfSanitizeSealedResidue(chain); err != nil {
			return nil, nil, err
		} else if path != "" {
			return nil, nil, fmt.Errorf("%w: %s", qwpSfErrSanitizedResidue, path)
		}
		// A frame-zero tear leaves no recoverable frame in the active segment,
		// but the bytes remain useful forensic evidence. Preserve them under the
		// established .corrupt name and put a clean active at the same
		// manifest-committed base instead of zeroing the only copy in place.
		replacedTornActive := false
		if activeSeg.segmentFrameCount() == 0 && activeSeg.segmentTornTailBytes() > 0 {
			torn := activeSeg
			path := torn.segmentPath()
			if err := torn.close(); err != nil {
				return nil, nil, err
			}
			replacement, err := qwpSfReplaceTornActive(path, active, maxBytesPerSegment)
			if err != nil {
				return nil, nil, err
			}
			for i, seg := range all {
				if seg == torn {
					all[i] = replacement
					break
				}
			}
			chain[len(chain)-1] = replacement
			activeSeg = replacement
			replacedTornActive = true
		}
		for _, seg := range chain {
			if err := seg.markManifestRequired(); err != nil {
				return nil, nil, err
			}
		}
		if replacedTornActive {
			if err := qwpSfSyncSlotDir(sfDir); err != nil {
				return nil, nil, fmt.Errorf("qwp/sf: sync torn-active replacement directory %s: %w", sfDir, err)
			}
		}
	} else {
		// A legacy slot has no committed boundaries, so the files themselves are
		// the only evidence of the chain's extent and a corrupt segment that may
		// carry frames could be its head, an interior link, or the unsent tail.
		// Nothing here can show its frames already delivered, so the whole legacy
		// branch fails closed instead of quarantining the file and migrating a
		// chain that may be missing rows. A file proven frameless holds no
		// position in the chain, so it does not block the migration; the common
		// tail preserves it aside.
		if len(framefulCorrupt) > 0 {
			return nil, nil, qwpSfFailClosed("cannot migrate the legacy SF chain: a corrupt segment of unknown identity could belong to it")
		}
		if len(data) > 0 {
			start := data[0].segmentBaseSeq()
			if start != 0 {
				for _, seg := range all {
					if seg.segmentFrameCount() == 0 && seg.segmentTornTailBytes() > 0 && seg.segmentBaseSeq() < start {
						return nil, nil, qwpSfFailClosed("cannot migrate the legacy SF chain based at %d: segment at base %d lost its frames to a torn write and sits below that head, so its range cannot be shown already-acked", start, seg.segmentBaseSeq())
					}
				}
			}
			if err := qwpSfValidateContiguous(data); err != nil {
				return nil, nil, err
			}
			chain = append(chain, data...)
			activeSeg = chain[len(chain)-1]
			if _, err := qwpSfSanitizeSealedResidue(chain); err != nil {
				return nil, nil, err
			}
		} else {
			activeSeg = qwpSfChooseEmptyInitial(all)
			if activeSeg == nil {
				if err := qwpSfDiscardOpened(all, nil, nil); err != nil {
					return nil, nil, err
				}
				all = nil
				success = true
				return nil, nil, nil
			}
			chain = append(chain, activeSeg)
		}
		head := chain[0].segmentBaseSeq()
		manifest, err = qwpSfManifestCreate(sfDir, head, activeSeg.segmentBaseSeq())
		if err != nil {
			return nil, nil, err
		}
		for _, seg := range chain {
			if err := seg.markManifestRequired(); err != nil {
				return nil, nil, err
			}
		}
	}

	keep := make(map[*qwpSfSegment]struct{}, len(chain))
	for _, seg := range chain {
		keep[seg] = struct{}{}
	}
	if err := qwpSfDiscardOpened(all, keep, preserve); err != nil {
		return nil, nil, err
	}
	qwpSfQuarantinePaths(corruptPaths)
	if err := activeSeg.sanitizeTornTail(); err != nil {
		return nil, nil, err
	}

	ring := qwpSfNewSegmentRing(activeSeg, maxBytesPerSegment)
	ring.sealedSegments = append(ring.sealedSegments, chain[:len(chain)-1]...)
	// The ring constructor derives publishedFsn from the active segment alone.
	// Recovery may retain an empty active segment after a completed rotation and
	// full trim, with no sealed segments left. Its positive base still records
	// the historical sequence frontier, so derive publishedFsn from nextSeq for
	// every recovered chain. A genuinely fresh base-zero ring remains at -1.
	ring.publishedFsn.Store(ring.nextSeq.Load() - 1)
	ring.manifest = manifest
	// Ownership of chain and manifest transferred to the ring.
	all = nil
	success = true
	return ring, manifest, nil
}

func qwpSfFailClosed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", qwpSfErrRecoveryFailClosed, fmt.Sprintf(format, args...))
}

func qwpSfValidateContiguous(chain []*qwpSfSegment) error {
	for i := 1; i < len(chain); i++ {
		prev, curr := chain[i-1], chain[i]
		expected := prev.segmentBaseSeq() + prev.segmentFrameCount()
		if curr.segmentBaseSeq() != expected {
			return qwpSfFailClosed("FSN gap in recovered segments: prev baseSeq=%d frameCount=%d expected next baseSeq=%d but got %d", prev.segmentBaseSeq(), prev.segmentFrameCount(), expected, curr.segmentBaseSeq())
		}
	}
	return nil
}

func qwpSfFindActive(all []*qwpSfSegment, activeBase int64) *qwpSfSegment {
	var torn, clean *qwpSfSegment
	for _, seg := range all {
		if seg.segmentBaseSeq() != activeBase {
			continue
		}
		if seg.segmentFrameCount() > 0 {
			return seg
		}
		if seg.segmentTornTailBytes() > 0 {
			if torn == nil {
				torn = seg
			}
			continue
		}
		if clean == nil {
			clean = seg
		}
	}
	if clean != nil {
		return clean
	}
	return torn
}

func qwpSfChooseEmptyInitial(all []*qwpSfSegment) *qwpSfSegment {
	var first *qwpSfSegment
	for _, seg := range all {
		if seg.segmentFrameCount() != 0 || seg.segmentTornTailBytes() != 0 {
			continue
		}
		if filepath.Base(seg.segmentPath()) == "sf-initial.sfa" {
			return seg
		}
		if first == nil {
			first = seg
		}
	}
	return first
}

func qwpSfSanitizeSealedResidue(chain []*qwpSfSegment) (string, error) {
	first := ""
	for _, seg := range chain[:len(chain)-1] {
		if seg.segmentTornTailBytes() == 0 {
			continue
		}
		if first == "" {
			first = seg.segmentPath()
		}
		if err := seg.sanitizeTornTail(); err != nil {
			return "", err
		}
	}
	return first, nil
}

// qwpSfDiscardOpened releases every opened segment outside keep. A file below
// the committed head is unlinked: the manifest proves its frames delivered. One
// that still carries bytes the boundaries do not account for -- a torn tail, or
// a member of preserve -- is quarantined under a .corrupt name instead, so no
// recovery path destroys bytes it cannot prove delivered.
func qwpSfDiscardOpened(all []*qwpSfSegment, keep, preserve map[*qwpSfSegment]struct{}) error {
	for _, seg := range all {
		if _, ok := keep[seg]; ok {
			continue
		}
		path := seg.segmentPath()
		_, wanted := preserve[seg]
		wanted = wanted || seg.segmentTornTailBytes() > 0
		if err := seg.close(); err != nil {
			return err
		}
		if wanted {
			qwpSfQuarantinePaths([]string{path})
		} else if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			qwpSfLogGuarded(nil, slog.LevelWarn, "qwp/sf: could not remove validated extra segment", "path", path, "error", err)
		}
	}
	return nil
}

// qwpSfCorruptMayHoldFrames reports whether a segment file that failed to open
// could still be carrying frames. A file too short to hold a header, or one
// holding nothing but zero bytes, provably carries none: the frame scan stops
// at the first zero length prefix, so a frame always leaves a non-zero byte
// behind. A zero-filled file is the ordinary residue of a crash during
// qwpSfCreateSegment, whose header lives in the mapping until a later flush,
// and the manager mints a spare on roughly every rotation.
//
// Anything else -- and any file this cannot read -- is treated as a possible
// frame carrier, since recovery must not talk itself out of a slot's evidence
// on the strength of a failed syscall.
func qwpSfCorruptMayHoldFrames(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return true
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return true
	}
	if st.Size() < qwpSfHeaderSize {
		return false
	}
	buf := make([]byte, 64*1024)
	for {
		n, err := f.Read(buf)
		for _, b := range buf[:n] {
			if b != 0 {
				return true
			}
		}
		if err != nil {
			return !errors.Is(err, io.EOF)
		}
	}
}

func qwpSfQuarantinePaths(paths []string) {
	for _, path := range paths {
		if _, err := qwpSfQuarantinePath(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			qwpSfLogGuarded(nil, slog.LevelWarn, "qwp/sf: could not quarantine corrupt segment", "path", path, "error", err)
		}
	}
}

// qwpSfTornActiveTempSuffix names the half-built replacement for a torn active
// segment. It deliberately does not end in .sfa, so no directory scan —
// recovery, the segment manager, the orphan sweep — can mistake a partial file
// for a segment. A leftover from a crash mid-replacement is truncated and
// reused by the next attempt.
const qwpSfTornActiveTempSuffix = ".replacing"

// Filesystem seams for the torn-active swap. Production always holds os.Link
// and os.Rename; tests replace one to reach a failure exit that no real
// filesystem can be talked into on demand. Every rename on this path goes
// through the seam -- the install, the no-hard-link fallback that moves the
// torn file aside, and that fallback's rollback -- so a test can fail exactly
// one of them by looking at the source path.
var (
	qwpSfTornActiveLink   = qwpSfSwappable(os.Link)
	qwpSfTornActiveRename = qwpSfSwappable(os.Rename)
)

// qwpSfReplaceTornActive preserves the bytes of a torn active segment under the
// established .corrupt name and puts a clean, empty segment at the same
// manifest-committed base in its place. Returns the segment now at path.
//
// Failure exits aim to leave a segment file at path, because the next startup
// refuses a slot whose committed active segment is missing and neither
// .corrupt nor .replacing ends in .sfa for a directory scan to find. Two steps
// arrange that. The replacement is built at a temporary path and only swapped
// in once it exists, so a full disk -- the obvious way to fail here, and the
// obvious reason the slot is being recovered at all -- leaves the torn file
// under its own name and the next recovery simply tries again. And the
// preserved copy is made by hard-linking the torn file aside, so the install
// rename replaces path's directory entry over a name that is occupied
// throughout, and a crash between the two steps costs at most a stray link.
//
// Where hard links are unavailable the fallback renames the torn file aside
// and rolls that rename back if the install fails. That is the one exit that
// can leave the committed active base unoccupied: if both the install rename
// and the rollback rename fail, the torn bytes survive under the preserved
// name and the returned error says so. What the next recovery makes of the
// emptied slot follows the committed boundaries -- a manifest that committed
// frames fails closed, one whose head equals its active collapses and starts
// fresh -- and either way the preserved copy is the record of the bytes.
func qwpSfReplaceTornActive(path string, baseSeq, maxBytesPerSegment int64) (*qwpSfSegment, error) {
	tmp := path + qwpSfTornActiveTempSuffix
	replacement, err := qwpSfCreateSegment(tmp, baseSeq, maxBytesPerSegment)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: build replacement for torn active %s: %w", path, err)
	}
	// The replacement's header must be durable before its name is installed
	// over the committed active base. The install rename below can reach the
	// disk first otherwise, and a crash in that window leaves a durable
	// directory entry over a zero-filled file: the next recovery reads it as
	// corrupt, qwpSfCorruptMayHoldFrames says it holds no frames, no active
	// segment survives, and the whole slot -- including the sealed segments'
	// undelivered rows, intact on disk -- is quarantined.
	if err := replacement.syncHeader(); err != nil {
		_ = replacement.close()
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("qwp/sf: build replacement for torn active %s: %w", path, err)
	}
	// Closed before the swap so the reopen below owns the only mapping, and so
	// the segment records the path it ends up at rather than the temporary name
	// every later diagnostic would then report.
	if err := replacement.close(); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("qwp/sf: build replacement for torn active %s: %w", path, err)
	}
	preserved, err := qwpSfQuarantineTargetPath(path)
	if err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	linked := true
	if err := qwpSfTornActiveLink.load()(path, preserved); err != nil {
		linked = false
		if err := qwpSfTornActiveRename.load()(path, preserved); err != nil {
			_ = os.Remove(tmp)
			return nil, fmt.Errorf("qwp/sf: quarantine segment %s: %w", path, err)
		}
	}
	if err := qwpSfTornActiveRename.load()(tmp, path); err != nil {
		_ = os.Remove(tmp)
		if linked {
			_ = os.Remove(preserved)
		} else if rollbackErr := qwpSfTornActiveRename.load()(preserved, path); rollbackErr != nil {
			return nil, fmt.Errorf(
				"qwp/sf: install replacement for torn active %s: %w (the torn segment is preserved at %s and no file is left at the committed active base)",
				path, err, preserved)
		}
		return nil, fmt.Errorf("qwp/sf: install replacement for torn active %s: %w", path, err)
	}
	return qwpSfOpenSegment(path)
}

func qwpSfQuarantinePath(path string) (string, error) {
	target, err := qwpSfQuarantineTargetPath(path)
	if err != nil {
		return "", err
	}
	if err := os.Rename(path, target); err != nil {
		return "", fmt.Errorf("qwp/sf: quarantine segment %s: %w", path, err)
	}
	return target, nil
}

// qwpSfQuarantineTargetPath picks a free .corrupt name for path, probing for a
// numbered suffix so an earlier quarantine's evidence is never overwritten.
func qwpSfQuarantineTargetPath(path string) (string, error) {
	target := path + ".corrupt"
	for suffix := 1; ; suffix++ {
		_, err := os.Stat(target)
		if errors.Is(err, os.ErrNotExist) {
			return target, nil
		}
		if err != nil {
			return "", fmt.Errorf("qwp/sf: inspect segment quarantine target %s: %w", target, err)
		}
		target = fmt.Sprintf("%s.corrupt-%d", path, suffix)
	}
}

func qwpSfQuarantineSlot(slotDir string) (string, error) {
	parent := filepath.Dir(slotDir)
	quarantineDir := filepath.Join(parent, "quarantined")
	if err := os.MkdirAll(quarantineDir, 0o755); err != nil {
		return "", fmt.Errorf("qwp/sf: create quarantine directory %s: %w", quarantineDir, err)
	}
	target := filepath.Join(quarantineDir, fmt.Sprintf("%s-%d", filepath.Base(slotDir), time.Now().UnixNano()))
	if err := os.Rename(slotDir, target); err != nil {
		return "", fmt.Errorf("qwp/sf: quarantine slot %s as %s: %w", slotDir, target, err)
	}
	if err := qwpSfSyncSlotDir(parent); err != nil {
		return "", fmt.Errorf("qwp/sf: fsync slot parent after quarantine: %w", err)
	}
	if err := qwpSfSyncSlotDir(quarantineDir); err != nil {
		return "", fmt.Errorf("qwp/sf: fsync quarantine directory: %w", err)
	}
	return target, nil
}
