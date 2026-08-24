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
	"hash/crc32"
	"io"
	"log/slog"
	"math"
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
// the success path the committed head is what licenses a removal. The recovery
// plan selects unlink only below that boundary, and revalidateUnlink repeats
// the proof immediately before os.Remove.
//
//lint:ignore ST1012 The qwpSf prefix groups internal store-and-forward errors.
var qwpSfErrSanitizedResidue = errors.New("qwp/sf: sanitized sealed-segment residue; retry recovery once")

type qwpSfManifestProvenance uint8

const (
	qwpSfManifestMissing qwpSfManifestProvenance = iota
	qwpSfManifestCommitted
	qwpSfManifestSynthesized
	qwpSfManifestInvalid
)

type qwpSfRecoveryAction uint8

const (
	qwpSfRecoveryKeep qwpSfRecoveryAction = iota
	qwpSfRecoverySanitizeSealed
	qwpSfRecoverySanitizeActive
	qwpSfRecoveryQuarantine
	qwpSfRecoveryUnlink
	qwpSfRecoveryReplace
	qwpSfRecoveryFailClosed
)

// qwpSfRecoveryFilePlan is the immutable evidence and selected action for one
// directory entry. Recovery never infers an unlink from the state left behind
// by an earlier mutation: the base/frame/torn/format facts are captured while
// inspecting the slot, and the action carries the fact that licenses it.
type qwpSfRecoveryFilePlan struct {
	path             string
	segment          *qwpSfSegment
	baseSeq          int64
	validFrames      int64
	tornTailBytes    int64
	manifestRequired bool
	mayHoldFrames    bool
	action           qwpSfRecoveryAction
	license          string
}

// qwpSfRecoveryPlan separates recovery's read-only classification from its
// namespace and mapped-file mutations. manifestProvenance describes the
// boundary record observed during inspection; it changes to synthesized only
// after apply durably creates that record.
type qwpSfRecoveryPlan struct {
	sfDir                  string
	maxBytesPerSegment     int64
	manifestProvenance     qwpSfManifestProvenance
	manifest               *qwpSfManifest
	invalidManifestPath    string
	headBase               int64
	activeBase             int64
	all                    []*qwpSfSegment
	files                  []qwpSfRecoveryFilePlan
	chain                  []*qwpSfSegment
	activeSeg              *qwpSfSegment
	synthesizeManifest     bool
	removeManifest         bool
	collapsed              bool
	retryAfterSanitizePath string
	failClosedErr          error
}

// qwpSfRecoveryContext provides the logger used while scanning and recovering
// a slot.
type qwpSfRecoveryContext struct {
	logger *slog.Logger
}

func qwpSfOpenRing(sfDir string, maxBytesPerSegment int64) (*qwpSfSegmentRing, error) {
	ring, _, err := qwpSfRecoverRing(sfDir, maxBytesPerSegment)
	return ring, err
}

func qwpSfRecoverRing(sfDir string, maxBytesPerSegment int64) (*qwpSfSegmentRing, *qwpSfManifest, error) {
	return qwpSfRecoverRingWithContext(sfDir, maxBytesPerSegment, qwpSfRecoveryContext{})
}

func qwpSfRecoverRingWithContext(sfDir string, maxBytesPerSegment int64, recoveryContext qwpSfRecoveryContext) (*qwpSfSegmentRing, *qwpSfManifest, error) {
	if _, err := os.Stat(sfDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, qwpSfDurabilityError("stat recovery slot", sfDir, err)
	}
	entries, err := os.ReadDir(sfDir)
	if err != nil {
		return nil, nil, qwpSfDurabilityError("read recovery slot", sfDir, err)
	}

	var all []*qwpSfSegment
	var manifest *qwpSfManifest
	var plan *qwpSfRecoveryPlan
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

	files := make([]qwpSfRecoveryFilePlan, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sfa") {
			continue
		}
		path := filepath.Join(sfDir, entry.Name())
		seg, openErr := qwpSfOpenSegment(path)
		if openErr != nil {
			if errors.Is(openErr, qwpSfErrSegmentCorrupt) {
				mayHoldFrames := qwpSfCorruptMayHoldFrames(path)
				files = append(files, qwpSfRecoveryFilePlan{
					path:          path,
					mayHoldFrames: mayHoldFrames,
					action:        qwpSfRecoveryQuarantine,
					license:       "the unreadable bytes are preserved under a non-segment name",
				})
				qwpEffectiveLogger(recoveryContext.logger).Warn("qwp/sf: deferring corrupt segment quarantine until recovery boundaries validate", "path", path, "error", openErr)
				continue
			}
			return nil, nil, fmt.Errorf("qwp/sf: open segment %s during recovery: %w", path, openErr)
		}
		all = append(all, seg)
		files = append(files, qwpSfRecoveryFilePlan{
			path:             path,
			segment:          seg,
			baseSeq:          seg.segmentBaseSeq(),
			validFrames:      seg.segmentFrameCount(),
			tornTailBytes:    seg.segmentTornTailBytes(),
			manifestRequired: seg.segmentManifestRequired(),
			mayHoldFrames:    seg.segmentFrameCount() > 0 || seg.segmentTornTailBytes() > 0,
			action:           qwpSfRecoveryKeep,
			license:          "selected recovery-chain member",
		})
	}

	var invalidManifest bool
	manifest, invalidManifest, err = qwpSfManifestInspect(sfDir)
	if err != nil {
		return nil, nil, err
	}
	plan = qwpSfBuildRecoveryPlan(sfDir, maxBytesPerSegment, all, files, manifest, invalidManifest)
	ring, recoveredManifest, err := qwpSfApplyRecoveryPlan(plan)
	manifest = plan.manifest
	if err != nil {
		return nil, nil, err
	}
	// Ownership of the retained chain and manifest transferred to the ring. All
	// other segments were closed by the action executor.
	all = nil
	success = true
	return ring, recoveredManifest, nil
}

func qwpSfBuildRecoveryPlan(
	sfDir string,
	maxBytesPerSegment int64,
	all []*qwpSfSegment,
	files []qwpSfRecoveryFilePlan,
	manifest *qwpSfManifest,
	invalidManifest bool,
) *qwpSfRecoveryPlan {
	p := &qwpSfRecoveryPlan{
		sfDir:              sfDir,
		maxBytesPerSegment: maxBytesPerSegment,
		manifest:           manifest,
		all:                all,
		files:              files,
	}
	switch {
	case manifest != nil:
		p.manifestProvenance = qwpSfManifestCommitted
		p.headBase = manifest.headBase
		p.activeBase = manifest.activeBase
	case invalidManifest:
		p.manifestProvenance = qwpSfManifestInvalid
		p.invalidManifestPath = filepath.Join(sfDir, qwpSfManifestFileName)
	default:
		p.manifestProvenance = qwpSfManifestMissing
	}

	framefulCorrupt := p.framefulCorruptPaths()
	if len(all) == 0 {
		if len(framefulCorrupt) > 0 {
			return p.failClosed(qwpSfFailClosed(
				"every SF segment file is unreadable and one of them may carry frames: %s",
				strings.Join(framefulCorrupt, ", ")))
		}
		if manifest != nil && manifest.headBase != manifest.activeBase {
			return p.failClosed(qwpSfFailClosed(
				"sf-manifest.bin references durable data but no segment file carries frames"))
		}
		p.collapsed = true
		p.removeManifest = manifest != nil
		return p
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
		if data[i].segmentBaseSeq() != data[j].segmentBaseSeq() {
			return data[i].segmentBaseSeq() < data[j].segmentBaseSeq()
		}
		return data[i].segmentPath() < data[j].segmentPath()
	})
	if manifest == nil && requiresManifest {
		return p.failClosed(qwpSfFailClosed(
			"new-format SF segment exists but sf-manifest.bin is missing"))
	}

	var preserve map[*qwpSfSegment]struct{}
	if manifest != nil {
		if err := p.planCommittedChain(data, framefulCorrupt, &preserve); err != nil {
			return p.failClosed(err)
		}
	} else {
		if err := p.planLegacyChain(data, framefulCorrupt); err != nil {
			return p.failClosed(err)
		}
	}
	if p.failClosedErr != nil {
		return p
	}

	keep := make(map[*qwpSfSegment]struct{}, len(p.chain))
	for _, seg := range p.chain {
		keep[seg] = struct{}{}
		p.setSegmentAction(seg, qwpSfRecoveryKeep, "selected member of the validated contiguous chain")
	}
	if err := p.planDiscardActions(keep, preserve); err != nil {
		return p.failClosed(err)
	}
	p.planSanitizationActions()
	return p
}

func (p *qwpSfRecoveryPlan) planCommittedChain(
	data []*qwpSfSegment,
	framefulCorrupt []string,
	preserve *map[*qwpSfSegment]struct{},
) error {
	head, active := p.headBase, p.activeBase
	p.activeSeg = qwpSfFindActive(p.all, active)
	for _, seg := range data {
		base := seg.segmentBaseSeq()
		end, err := qwpSfSegmentEnd(seg)
		if err != nil {
			return err
		}
		if base < head {
			if end > head {
				return qwpSfFailClosed("segment overlaps committed SF head boundary")
			}
			continue
		}
		if base > active {
			return qwpSfFailClosed("segment exists beyond committed SF active boundary")
		}
		if base == active && seg != p.activeSeg {
			if seg.segmentFrameCount() > 0 {
				if *preserve == nil {
					*preserve = make(map[*qwpSfSegment]struct{}, 1)
				}
				(*preserve)[seg] = struct{}{}
			}
			continue
		}
		p.chain = append(p.chain, seg)
	}
	if len(p.chain) > 0 {
		if err := qwpSfValidateContiguous(p.chain); err != nil {
			return err
		}
		if p.chain[0].segmentBaseSeq() != head {
			return qwpSfFailClosed("missing expected SF head segment at base %d", head)
		}
	}
	if p.activeSeg == nil {
		if len(p.chain) == 0 && head == active && len(framefulCorrupt) == 0 {
			p.collapsed = true
			p.removeManifest = true
			return nil
		}
		return qwpSfFailClosed("missing expected SF active segment at base %d", active)
	}
	if len(p.chain) == 0 {
		if head != active || p.activeSeg.segmentFrameCount() != 0 || len(framefulCorrupt) > 0 {
			suffix := ""
			if len(framefulCorrupt) > 0 {
				suffix = " (a corrupt segment prevents proving the empty state)"
			}
			return qwpSfFailClosed("missing SF chain between committed boundaries%s", suffix)
		}
		p.chain = append(p.chain, p.activeSeg)
	} else if p.chain[len(p.chain)-1] != p.activeSeg {
		last := p.chain[len(p.chain)-1]
		chainEnd, err := qwpSfSegmentEnd(last)
		if err != nil {
			return err
		}
		if len(framefulCorrupt) == 0 && p.activeSeg.segmentFrameCount() == 0 && p.activeSeg.segmentBaseSeq() == chainEnd {
			p.chain = append(p.chain, p.activeSeg)
		} else {
			return qwpSfFailClosed("missing expected SF active/tail segment at base %d", active)
		}
	}
	return nil
}

func (p *qwpSfRecoveryPlan) planLegacyChain(data []*qwpSfSegment, framefulCorrupt []string) error {
	if len(framefulCorrupt) > 0 {
		return qwpSfFailClosed(
			"cannot migrate the legacy SF chain: a corrupt segment of unknown identity could belong to it")
	}
	if len(data) > 0 {
		if err := qwpSfValidateContiguous(data); err != nil {
			return err
		}
		p.chain = append(p.chain, data...)
		p.activeSeg = p.chain[len(p.chain)-1]
	} else {
		p.activeSeg = qwpSfChooseEmptyInitial(p.all)
		if p.activeSeg == nil {
			p.collapsed = true
			return nil
		}
		p.chain = append(p.chain, p.activeSeg)
	}
	p.headBase = p.chain[0].segmentBaseSeq()
	p.activeBase = p.activeSeg.segmentBaseSeq()
	if p.headBase != 0 {
		for _, seg := range p.all {
			if seg.segmentFrameCount() == 0 && seg.segmentTornTailBytes() > 0 &&
				seg.segmentBaseSeq() < p.headBase {
				return qwpSfFailClosed(
					"cannot migrate the legacy SF chain based at %d: segment at base %d lost its frames to a torn write and sits below that head, so its range cannot be shown already-acked",
					p.headBase, seg.segmentBaseSeq())
			}
		}
	}
	p.synthesizeManifest = true
	return nil
}

func (p *qwpSfRecoveryPlan) planDiscardActions(
	keep, preserve map[*qwpSfSegment]struct{},
) error {
	if !p.collapsed && len(keep) > 0 {
		anchored := false
		for seg := range keep {
			if seg.segmentBaseSeq() == p.headBase {
				anchored = true
				break
			}
		}
		if !anchored {
			return qwpSfFailClosed(
				"head %d matches the base of no kept segment; refusing to release any file under an unverified boundary",
				p.headBase)
		}
	}
	for _, seg := range p.all {
		if _, ok := keep[seg]; ok {
			continue
		}
		file := p.fileForSegment(seg)
		if file == nil {
			return errors.New("qwp/sf: recovery plan lost an opened segment")
		}
		end, err := qwpSfSegmentEnd(seg)
		if err != nil {
			return err
		}
		_, explicitlyPreserved := preserve[seg]
		mustPreserve := explicitlyPreserved || (file.tornTailBytes > 0 &&
			(file.baseSeq >= p.headBase || end > p.headBase))
		if mustPreserve {
			file.action = qwpSfRecoveryQuarantine
			file.license = "bytes are not proven delivered; preserve them outside the segment namespace"
			continue
		}
		if file.mayHoldFrames && p.manifestProvenance != qwpSfManifestCommitted {
			return qwpSfFailClosed(
				"segment at base %d may hold frames and no committed boundary licenses its deletion",
				file.baseSeq)
		}
		if file.mayHoldFrames && (file.baseSeq >= p.headBase || end > p.headBase) {
			return qwpSfFailClosed(
				"segment at base %d is not wholly below committed head %d; refusing deletion",
				file.baseSeq, p.headBase)
		}
		file.action = qwpSfRecoveryUnlink
		if file.mayHoldFrames {
			file.license = "committed head proves the complete segment delivered"
		} else {
			file.license = "validated scan proves the file holds no frame or torn tail"
		}
	}
	return nil
}

func (p *qwpSfRecoveryPlan) planSanitizationActions() {
	if len(p.chain) == 0 {
		return
	}
	for _, seg := range p.chain[:len(p.chain)-1] {
		if seg.segmentTornTailBytes() == 0 {
			continue
		}
		p.setSegmentAction(seg, qwpSfRecoverySanitizeSealed,
			"the validated frame boundary makes the sealed tail append-ineligible residue")
		if p.retryAfterSanitizePath == "" {
			p.retryAfterSanitizePath = seg.segmentPath()
		}
	}
	if p.activeSeg.segmentFrameCount() == 0 && p.activeSeg.segmentTornTailBytes() > 0 &&
		p.manifestProvenance == qwpSfManifestCommitted {
		p.setSegmentAction(p.activeSeg, qwpSfRecoveryReplace,
			"committed active base requires a clean appendable segment while torn bytes remain preserved")
	} else if p.activeSeg.segmentTornTailBytes() > 0 {
		p.setSegmentAction(p.activeSeg, qwpSfRecoverySanitizeActive,
			"validated active cursor licenses descriptor-first zeroing of its appendable tail")
	}
}

func (p *qwpSfRecoveryPlan) failClosed(err error) *qwpSfRecoveryPlan {
	p.failClosedErr = err
	for i := range p.files {
		p.files[i].action = qwpSfRecoveryFailClosed
		p.files[i].license = err.Error()
	}
	return p
}

func (p *qwpSfRecoveryPlan) framefulCorruptPaths() []string {
	var paths []string
	for i := range p.files {
		file := &p.files[i]
		if file.segment == nil && file.mayHoldFrames {
			paths = append(paths, file.path)
		}
	}
	return paths
}

func (p *qwpSfRecoveryPlan) fileForSegment(seg *qwpSfSegment) *qwpSfRecoveryFilePlan {
	for i := range p.files {
		if p.files[i].segment == seg {
			return &p.files[i]
		}
	}
	return nil
}

func (p *qwpSfRecoveryPlan) setSegmentAction(seg *qwpSfSegment, action qwpSfRecoveryAction, license string) {
	if file := p.fileForSegment(seg); file != nil {
		file.action = action
		file.license = license
	}
}

func qwpSfSegmentEnd(seg *qwpSfSegment) (int64, error) {
	base, frames := seg.segmentBaseSeq(), seg.segmentFrameCount()
	if frames > math.MaxInt64-base {
		return 0, qwpSfFailClosed(
			"segment range overflows FSN space: baseSeq=%d frameCount=%d", base, frames)
	}
	return base + frames, nil
}

func qwpSfApplyRecoveryPlan(p *qwpSfRecoveryPlan) (*qwpSfSegmentRing, *qwpSfManifest, error) {
	if p.failClosedErr != nil {
		return nil, nil, p.failClosedErr
	}
	if p.invalidManifestPath != "" {
		if err := qwpSfQuarantineCreationDebris(p.invalidManifestPath); err != nil {
			return nil, nil, err
		}
		p.invalidManifestPath = ""
	}
	if p.synthesizeManifest {
		manifest, err := qwpSfManifestCreate(p.sfDir, p.headBase, p.activeBase)
		if err != nil {
			return nil, nil, err
		}
		p.manifest = manifest
		p.manifestProvenance = qwpSfManifestSynthesized
	}

	if p.retryAfterSanitizePath != "" {
		if err := p.markChainManifestRequired(); err != nil {
			return nil, nil, err
		}
		for i := range p.files {
			file := &p.files[i]
			if file.action == qwpSfRecoverySanitizeSealed {
				if err := file.segment.sanitizeTornTail(); err != nil {
					return nil, nil, err
				}
			}
		}
		return nil, nil, fmt.Errorf("%w: %s", qwpSfErrSanitizedResidue, p.retryAfterSanitizePath)
	}

	if err := p.applyReplacement(); err != nil {
		return nil, nil, err
	}
	if err := p.markChainManifestRequired(); err != nil {
		return nil, nil, err
	}
	if err := p.applyDirectoryActions(); err != nil {
		return nil, nil, err
	}
	if p.activeSeg != nil {
		if file := p.fileForSegment(p.activeSeg); file != nil && file.action == qwpSfRecoverySanitizeActive {
			if err := p.activeSeg.sanitizeTornTail(); err != nil {
				return nil, nil, err
			}
		}
	}

	if p.removeManifest {
		if err := qwpSfSyncSlotDir(p.sfDir); err != nil {
			return nil, nil, qwpSfDurabilityError("sync recovery cleanup directory", p.sfDir, err)
		}
		if err := p.manifest.close(); err != nil {
			return nil, nil, err
		}
		p.manifest = nil
		if err := qwpSfRemoveManifestAndSyncDir(p.sfDir); err != nil {
			return nil, nil, err
		}
	}
	if p.collapsed || p.activeSeg == nil {
		return nil, p.manifest, nil
	}
	// Recovery may be retrying after a prior process installed a replacement or
	// completed another namespace mutation whose directory barrier failed. The
	// clean files alone cannot reveal that pending epoch. Commit the slot
	// namespace unconditionally before exposing any recovered mmap for append.
	if err := qwpSfSyncSlotDir(p.sfDir); err != nil {
		return nil, nil, qwpSfDurabilityError("sync recovered slot before exposing ring", p.sfDir, err)
	}

	ring := qwpSfNewSegmentRing(p.activeSeg, p.maxBytesPerSegment)
	ring.sealedSegments = append(ring.sealedSegments, p.chain[:len(p.chain)-1]...)
	ring.publishedFsn.Store(ring.nextSeq.Load() - 1)
	ring.manifest = p.manifest
	return ring, p.manifest, nil
}

func (p *qwpSfRecoveryPlan) applyReplacement() error {
	if p.activeSeg == nil {
		return nil
	}
	file := p.fileForSegment(p.activeSeg)
	if file == nil || file.action != qwpSfRecoveryReplace {
		return nil
	}
	torn := p.activeSeg
	if err := torn.close(); err != nil {
		return err
	}
	replacement, err := qwpSfReplaceTornActive(file.path, p.activeBase, p.maxBytesPerSegment)
	if err != nil {
		return err
	}
	for i, seg := range p.all {
		if seg == torn {
			p.all[i] = replacement
			break
		}
	}
	for i, seg := range p.chain {
		if seg == torn {
			p.chain[i] = replacement
			break
		}
	}
	file.segment = replacement
	file.baseSeq = replacement.segmentBaseSeq()
	file.validFrames = replacement.segmentFrameCount()
	file.tornTailBytes = replacement.segmentTornTailBytes()
	file.manifestRequired = replacement.segmentManifestRequired()
	file.mayHoldFrames = false
	file.action = qwpSfRecoveryKeep
	file.license = "clean replacement installed at the committed active base"
	p.activeSeg = replacement
	return nil
}

func (p *qwpSfRecoveryPlan) markChainManifestRequired() error {
	for _, seg := range p.chain {
		if err := seg.markManifestRequired(); err != nil {
			return err
		}
	}
	return nil
}

func (p *qwpSfRecoveryPlan) applyDirectoryActions() error {
	for i := range p.files {
		file := &p.files[i]
		switch file.action {
		case qwpSfRecoveryUnlink:
			if file.segment != nil {
				if err := file.segment.close(); err != nil {
					return err
				}
			}
			if err := p.revalidateUnlink(file); err != nil {
				return err
			}
			if err := os.Remove(file.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return qwpSfDurabilityError("apply recovery unlink", file.path, err)
			}
		case qwpSfRecoveryQuarantine:
			if file.segment != nil {
				if err := file.segment.close(); err != nil {
					return err
				}
			}
			if _, err := qwpSfQuarantinePath(file.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return qwpSfDurabilityError("apply recovery quarantine", file.path, err)
			}
		}
	}
	return nil
}

// revalidateUnlink runs immediately before os.Remove. It deliberately repeats
// the licensing check instead of trusting the planner: a future planner branch
// can select a bad action, but it still cannot turn that mistake into deletion.
func (p *qwpSfRecoveryPlan) revalidateUnlink(file *qwpSfRecoveryFilePlan) error {
	if file.action != qwpSfRecoveryUnlink {
		return qwpSfFailClosed("recovery delete-site received a non-unlink action for %s", file.path)
	}
	if !file.mayHoldFrames {
		return nil
	}
	if p.manifestProvenance != qwpSfManifestCommitted || p.manifest == nil ||
		p.manifest.headBase != p.headBase || p.manifest.activeBase != p.activeBase {
		return qwpSfFailClosed("committed recovery boundary changed before deleting %s", file.path)
	}
	if !p.collapsed {
		anchored := false
		for _, seg := range p.chain {
			if seg.segmentBaseSeq() == p.headBase {
				anchored = true
				break
			}
		}
		if !anchored {
			return qwpSfFailClosed("head %d matches no kept segment immediately before deleting %s", p.headBase, file.path)
		}
	}
	end, err := qwpSfSegmentEnd(file.segment)
	if err != nil {
		return err
	}
	if file.baseSeq >= p.headBase || end > p.headBase {
		return qwpSfFailClosed("segment %s is not wholly below committed head %d", file.path, p.headBase)
	}
	return nil
}

func qwpSfValidateContiguous(chain []*qwpSfSegment) error {
	for i, seg := range chain {
		end, err := qwpSfSegmentEnd(seg)
		if err != nil {
			return err
		}
		if i+1 < len(chain) && chain[i+1].segmentBaseSeq() != end {
			return qwpSfFailClosed("FSN gap in recovered segments: prev baseSeq=%d frameCount=%d expected next baseSeq=%d but got %d", seg.segmentBaseSeq(), seg.segmentFrameCount(), end, chain[i+1].segmentBaseSeq())
		}
	}
	return nil
}

func qwpSfFindActive(all []*qwpSfSegment, activeBase int64) *qwpSfSegment {
	var frames, torn, clean *qwpSfSegment
	for _, seg := range all {
		if seg.segmentBaseSeq() != activeBase {
			continue
		}
		if seg.segmentFrameCount() > 0 {
			if frames == nil || seg.segmentPath() < frames.segmentPath() {
				frames = seg
			}
			continue
		}
		if seg.segmentTornTailBytes() > 0 {
			if torn == nil || seg.segmentPath() < torn.segmentPath() {
				torn = seg
			}
			continue
		}
		if clean == nil || seg.segmentPath() < clean.segmentPath() {
			clean = seg
		}
	}
	if frames != nil {
		return frames
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
		return nil, qwpSfDurabilityError("build replacement for torn active", path, err)
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
		return nil, qwpSfDurabilityError("sync replacement for torn active", path, err)
	}
	// Closed before the swap so the reopen below owns the only mapping, and so
	// the segment records the path it ends up at rather than the temporary name
	// every later diagnostic would then report.
	if err := replacement.close(); err != nil {
		_ = os.Remove(tmp)
		return nil, qwpSfDurabilityError("close replacement for torn active", path, err)
	}
	preserved, err := qwpSfQuarantineTargetPath(path)
	if err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	linked := true
	if linkErr := qwpSfTornActiveLink.load()(path, preserved); linkErr != nil {
		linked = false
		if renameErr := qwpSfTornActiveRename.load()(path, preserved); renameErr != nil {
			_ = os.Remove(tmp)
			return nil, errors.Join(
				qwpSfDurabilityError("hard-link torn active into quarantine", path, linkErr),
				qwpSfDurabilityError("rename torn active into quarantine", path, renameErr),
			)
		}
	}
	// The torn bytes need a durable name before the clean replacement may take
	// over the manifest-committed active path. Link/rename preservation and the
	// install therefore belong to separate namespace epochs.
	if err := qwpSfSyncSlotDir(filepath.Dir(path)); err != nil {
		_ = os.Remove(tmp)
		if linked {
			_ = os.Remove(preserved)
		} else if rollbackErr := qwpSfTornActiveRename.load()(preserved, path); rollbackErr != nil {
			return nil, errors.Join(
				qwpSfDurabilityError("sync preserved torn-active segment", preserved, err),
				qwpSfDurabilityError("restore torn active after failed preservation barrier", path, rollbackErr),
			)
		}
		return nil, qwpSfDurabilityError("sync preserved torn-active segment", preserved, err)
	}
	if err := qwpSfTornActiveRename.load()(tmp, path); err != nil {
		_ = os.Remove(tmp)
		if linked {
			_ = os.Remove(preserved)
		} else if rollbackErr := qwpSfTornActiveRename.load()(preserved, path); rollbackErr != nil {
			return nil, errors.Join(
				qwpSfDurabilityError("install replacement for torn active", path, err),
				qwpSfDurabilityError(
					"restore torn active after failed replacement install; preserved copy remains and no file is left at the committed active base",
					preserved,
					rollbackErr,
				),
			)
		}
		return nil, qwpSfDurabilityError("install replacement for torn active", path, err)
	}
	if err := qwpSfSyncSlotDir(filepath.Dir(path)); err != nil {
		return nil, qwpSfDurabilityError("sync installed torn-active replacement", path, err)
	}
	return qwpSfOpenSegment(path)
}

func qwpSfQuarantinePath(path string) (string, error) {
	target, err := qwpSfQuarantineTargetPath(path)
	if err != nil {
		return "", err
	}
	if err := os.Rename(path, target); err != nil {
		return "", qwpSfDurabilityError("quarantine segment", path, err)
	}
	return target, nil
}

// qwpSfQuarantineTargetPath picks a free .corrupt name for path, probing for a
// numbered suffix so an earlier quarantine's evidence is never overwritten.
func qwpSfQuarantineTargetPath(path string) (string, error) {
	target := qwpSfBoundedSuffixPath(path, ".corrupt")
	for suffix := 1; ; suffix++ {
		_, err := os.Stat(target)
		if errors.Is(err, os.ErrNotExist) {
			return target, nil
		}
		if err != nil {
			return "", qwpSfDurabilityError("inspect segment quarantine target", target, err)
		}
		target = qwpSfBoundedSuffixPath(path, fmt.Sprintf(".corrupt-%d", suffix))
	}
}

// qwpSfQuarantineNameMaxLen bounds the file-name component of a quarantine
// target, with margin under the 255-byte limit common across filesystems, so
// the quarantine rename cannot fail with ENAMETOOLONG over a name the client
// itself formed.
const qwpSfQuarantineNameMaxLen = 200

// qwpSfBoundedSuffixPath appends suffix to path's file name, truncating the
// stem when the combined component would exceed qwpSfQuarantineNameMaxLen
// bytes. A truncated stem is tagged with a checksum of the full name so two
// long names that share a prefix cannot collapse onto the same target.
func qwpSfBoundedSuffixPath(path, suffix string) string {
	dir, name := filepath.Split(path)
	if len(name)+len(suffix) <= qwpSfQuarantineNameMaxLen {
		return dir + name + suffix
	}
	tag := fmt.Sprintf("-%08x", crc32.ChecksumIEEE([]byte(name)))
	keep := qwpSfQuarantineNameMaxLen - len(suffix) - len(tag)
	if keep < 0 {
		keep = 0
	}
	return dir + name[:keep] + tag + suffix
}

func qwpSfQuarantineSlot(slotDir string) (string, error) {
	parent := filepath.Dir(slotDir)
	quarantineDir := filepath.Join(parent, "quarantined")
	if err := os.MkdirAll(quarantineDir, 0o755); err != nil {
		return "", qwpSfDurabilityError("create quarantine directory", quarantineDir, err)
	}
	target := filepath.Join(quarantineDir, fmt.Sprintf("%s-%d", filepath.Base(slotDir), time.Now().UnixNano()))
	if err := os.Rename(slotDir, target); err != nil {
		return "", qwpSfDurabilityError("quarantine slot as "+target, slotDir, err)
	}
	if err := qwpSfSyncSlotDir(parent); err != nil {
		return "", qwpSfDurabilityError("sync slot parent after quarantine", parent, err)
	}
	if err := qwpSfSyncSlotDir(quarantineDir); err != nil {
		return "", qwpSfDurabilityError("sync quarantine directory", quarantineDir, err)
	}
	return target, nil
}
