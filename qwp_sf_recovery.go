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
	"sort"
	"strings"
	"time"
)

// Failed recovery never mutates committed chain bytes. The only durable
// mutations before success are zeroing bytes already proved to be dead by the
// committed boundaries, or preserve-by-rename quarantine.
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
				qwpEffectiveLogger(nil).Warn("qwp/sf: deferring corrupt segment quarantine until recovery boundaries validate", "path", path, "error", openErr)
				continue
			}
			return nil, nil, fmt.Errorf("qwp/sf: open segment %s during recovery: %w", path, openErr)
		}
		all = append(all, seg)
	}

	manifest, err = qwpSfManifestOpen(sfDir)
	if err != nil {
		return nil, nil, err
	}
	if len(all) == 0 {
		if len(corruptPaths) > 0 {
			if manifest != nil {
				return nil, nil, qwpSfFailClosed("every SF segment is corrupt but sf-manifest.bin references durable data")
			}
			qwpSfQuarantinePaths(corruptPaths)
			success = true
			return nil, nil, nil
		}
		if manifest != nil && manifest.headBase != manifest.activeBase {
			return nil, nil, qwpSfFailClosed("sf-manifest.bin references durable data but no segment files exist")
		}
		if manifest != nil {
			qwpEffectiveLogger(nil).Warn("qwp/sf: removing collapsed manifest with no segment files", "dir", sfDir)
			if err := manifest.close(); err != nil {
				return nil, nil, err
			}
			manifest = nil
			if !qwpSfManifestRemove(sfDir) {
				return nil, nil, fmt.Errorf("qwp/sf: remove collapsed manifest in %s", sfDir)
			}
		}
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
			if len(chain) == 0 && head == active && len(corruptPaths) == 0 {
				if err := qwpSfDiscardOpened(all, nil); err != nil {
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
			if head != active || activeSeg.segmentFrameCount() != 0 || len(corruptPaths) > 0 {
				suffix := ""
				if len(corruptPaths) > 0 {
					suffix = " (a corrupt segment prevents proving the empty state)"
				}
				return nil, nil, qwpSfFailClosed("missing SF chain between committed boundaries%s", suffix)
			}
			chain = append(chain, activeSeg)
		} else if chain[len(chain)-1] != activeSeg {
			last := chain[len(chain)-1]
			chainEnd := last.segmentBaseSeq() + last.segmentFrameCount()
			if len(corruptPaths) == 0 && activeSeg.segmentFrameCount() == 0 && activeSeg.segmentBaseSeq() == chainEnd {
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
		for _, seg := range chain {
			if err := seg.markManifestRequired(); err != nil {
				return nil, nil, err
			}
		}
	} else {
		if len(data) > 0 {
			start := data[0].segmentBaseSeq()
			if start != 0 {
				if len(corruptPaths) > 0 {
					return nil, nil, qwpSfFailClosed("cannot migrate the legacy SF chain based at %d: a corrupt segment of unknown identity could be its head", start)
				}
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
				if err := qwpSfDiscardOpened(all, nil); err != nil {
					return nil, nil, err
				}
				all = nil
				qwpSfQuarantinePaths(corruptPaths)
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
	if err := qwpSfDiscardOpened(all, keep); err != nil {
		return nil, nil, err
	}
	qwpSfQuarantinePaths(corruptPaths)
	if err := activeSeg.sanitizeTornTail(); err != nil {
		return nil, nil, err
	}

	ring := qwpSfNewSegmentRing(activeSeg, maxBytesPerSegment)
	ring.sealedSegments = append(ring.sealedSegments, chain[:len(chain)-1]...)
	// The ring constructor derives publishedFsn from the active segment alone.
	// Recovery may retain an empty active tail after a completed rotation, in
	// which case nextSeq is still the FSN immediately after the sealed chain.
	if len(ring.sealedSegments) > 0 {
		ring.publishedFsn.Store(ring.nextSeq.Load() - 1)
	}
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
		if seg.segmentTornTailBytes() > 0 && torn == nil {
			torn = seg
		} else if clean == nil {
			clean = seg
		}
	}
	if torn != nil {
		return torn
	}
	return clean
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

func qwpSfDiscardOpened(all []*qwpSfSegment, keep map[*qwpSfSegment]struct{}) error {
	for _, seg := range all {
		if _, ok := keep[seg]; ok {
			continue
		}
		path := seg.segmentPath()
		torn := seg.segmentTornTailBytes() > 0
		if err := seg.close(); err != nil {
			return err
		}
		if torn {
			qwpSfQuarantinePaths([]string{path})
		} else if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			qwpEffectiveLogger(nil).Warn("qwp/sf: could not remove validated extra segment", "path", path, "error", err)
		}
	}
	return nil
}

func qwpSfQuarantinePaths(paths []string) {
	for _, path := range paths {
		_ = os.Remove(path + ".corrupt")
		if err := os.Rename(path, path+".corrupt"); err != nil && !errors.Is(err, os.ErrNotExist) {
			qwpEffectiveLogger(nil).Warn("qwp/sf: could not quarantine corrupt segment", "path", path, "error", err)
		}
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
	if err := qwpSfSyncDir(parent); err != nil {
		return "", fmt.Errorf("qwp/sf: fsync slot parent after quarantine: %w", err)
	}
	if err := qwpSfSyncDir(quarantineDir); err != nil {
		return "", fmt.Errorf("qwp/sf: fsync quarantine directory: %w", err)
	}
	return target, nil
}
