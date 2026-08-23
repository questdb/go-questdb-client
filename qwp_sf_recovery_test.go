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
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createRecoverySegment(t *testing.T, dir, name string, base int64, payloads ...string) *qwpSfSegment {
	t.Helper()
	seg, err := qwpSfCreateSegment(filepath.Join(dir, name), base, 4096)
	require.NoError(t, err)
	for _, payload := range payloads {
		_, err := seg.tryAppend([]byte(payload))
		require.NoError(t, err)
	}
	return seg
}

func createRecoveryManifest(t *testing.T, dir string, head, active int64, segments ...*qwpSfSegment) {
	t.Helper()
	m, err := qwpSfManifestCreate(dir, head, active)
	require.NoError(t, err)
	require.NoError(t, m.close())
	for _, seg := range segments {
		require.NoError(t, seg.markManifestRequired())
	}
}

func closeRecoverySegments(t *testing.T, segments ...*qwpSfSegment) {
	t.Helper()
	for _, seg := range segments {
		require.NoError(t, seg.close())
	}
}

// openTornEmptySegment builds a segment holding no frames whose tail carries an
// attempted-but-failed write, then reopens it so recovery's torn-tail detection
// has run.
func openTornEmptySegment(t *testing.T, dir, name string, base int64) *qwpSfSegment {
	t.Helper()
	path := filepath.Join(dir, name)
	seg, err := qwpSfCreateSegment(path, base, 4096)
	require.NoError(t, err)
	require.NoError(t, seg.close())
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+20)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	reopened, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	require.Zero(t, reopened.segmentFrameCount())
	require.NotZero(t, reopened.segmentTornTailBytes())
	return reopened
}

func reopenEmptySegment(t *testing.T, dir, name string, base int64) *qwpSfSegment {
	t.Helper()
	path := filepath.Join(dir, name)
	seg, err := qwpSfCreateSegment(path, base, 4096)
	require.NoError(t, err)
	require.NoError(t, seg.close())
	reopened, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	require.Zero(t, reopened.segmentTornTailBytes())
	return reopened
}

func TestQwpSfFindActivePrefersCleanSegmentOverTorn(t *testing.T) {
	const base int64 = 5
	dir := t.TempDir()
	torn := openTornEmptySegment(t, dir, "sf-torn.sfa", base)
	clean := reopenEmptySegment(t, dir, "sf-clean.sfa", base)
	defer closeRecoverySegments(t, torn, clean)

	// A clean empty segment at the committed base can be appended to as it
	// stands, while a torn one first has to be quarantined and replaced. Which
	// of the two the directory listing happened to return first must not decide
	// it.
	assert.Same(t, clean, qwpSfFindActive([]*qwpSfSegment{torn, clean}, base))
	assert.Same(t, clean, qwpSfFindActive([]*qwpSfSegment{clean, torn}, base))

	// A torn one is still adopted when it is the only candidate: recovery then
	// preserves its bytes and puts a replacement at the same base.
	assert.Same(t, torn, qwpSfFindActive([]*qwpSfSegment{torn}, base))

	// A segment that actually holds frames outranks both.
	withFrames := createRecoverySegment(t, dir, "sf-frames.sfa", base, "a")
	defer closeRecoverySegments(t, withFrames)
	assert.Same(t, withFrames, qwpSfFindActive([]*qwpSfSegment{torn, clean, withFrames}, base))

	assert.Nil(t, qwpSfFindActive([]*qwpSfSegment{torn, clean}, base+1),
		"a segment at another base is not a candidate")

	// A second torn candidate is not a clean one. Recording it as the clean
	// choice would hand back a torn segment for the preserve-and-replace dance
	// while the usable clean file at the same base is removed as a stray.
	torn2 := openTornEmptySegment(t, dir, "sf-torn2.sfa", base)
	defer closeRecoverySegments(t, torn2)
	assert.Same(t, clean, qwpSfFindActive([]*qwpSfSegment{torn, torn2, clean}, base))
	assert.Same(t, torn, qwpSfFindActive([]*qwpSfSegment{torn, torn2}, base))
}

// TestQwpSfRecoveryRefusesMissingChainBetweenCommittedBoundaries pins that a
// slot whose committed frames are simply not on disk is refused rather than
// opened. Coming up regardless would drop those rows silently, which is the one
// thing the manifest's committed boundaries exist to prevent.
func TestQwpSfRecoveryRefusesMissingChainBetweenCommittedBoundaries(t *testing.T) {
	dir := t.TempDir()
	// The manifest commits a chain from base 0 up to an active at base 5, but
	// the only file present is the empty active. Everything in between is gone.
	active := createRecoverySegment(t, dir, "sf-active.sfa", 5)
	createRecoveryManifest(t, dir, 0, 5, active)
	closeRecoverySegments(t, active)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "missing SF chain between committed boundaries")
}

// TestQwpSfRecoveryRefusesEmptyChainWhileASegmentIsCorrupt covers the same
// refusal in the case where the boundaries themselves say the slot is empty. A
// corrupt file holding data could be the missing chain, so the empty state
// cannot be proved.
func TestQwpSfRecoveryRefusesEmptyChainWhileASegmentIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", 5)
	createRecoveryManifest(t, dir, 5, 5, active)
	closeRecoverySegments(t, active)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-bad.sfa"), bytes.Repeat([]byte{0xab}, 4096), 0o644))

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "a corrupt segment prevents proving the empty state")
}

// TestQwpSfRecoveryAcceptsEmptyChainBesideAFramelessStray is the other half of
// that rule. A file that provably holds no frame -- shorter than a header, or
// nothing but zeros -- cannot be the missing chain, so it takes nothing away
// from the proof and the slot recovers. Both shapes are ordinary crash residue:
// qwpSfCreateSegment leaves the header in the mapping until a later flush, and
// the manager mints a spare on roughly every rotation.
func TestQwpSfRecoveryAcceptsEmptyChainBesideAFramelessStray(t *testing.T) {
	for _, stray := range [][]byte{{}, make([]byte, 4096)} {
		name := "zero-length"
		if len(stray) > 0 {
			name = "zero-filled"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			active := createRecoverySegment(t, dir, "sf-active.sfa", 5)
			createRecoveryManifest(t, dir, 5, 5, active)
			closeRecoverySegments(t, active)
			strayPath := filepath.Join(dir, "sf-stray.sfa")
			require.NoError(t, os.WriteFile(strayPath, stray, 0o644))

			ring, _, err := qwpSfRecoverRing(dir, 4096)
			require.NoError(t, err)
			require.NotNil(t, ring)
			defer ring.segmentRingClose()
			assert.Equal(t, int64(5), ring.getActiveSegment().segmentBaseSeq())
			_, statErr := os.Stat(strayPath + ".corrupt")
			require.NoError(t, statErr, "the stray is still quarantined, just not fatal")
		})
	}
}

func TestQwpSfRecoveryFailsClosedWhenManifestNewestIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, dir, 0, 1, s0, s1)
	closeRecoverySegments(t, s0, s1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-0001.sfa"), make([]byte, 4096), 0o644))

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "active")
}

func TestQwpSfRecoveryFailsClosedWhenManifestOldestIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, dir, 0, 1, s0, s1)
	closeRecoverySegments(t, s0, s1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-initial.sfa"), make([]byte, 4096), 0o644))

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "head")
}

func TestQwpSfRecoveryRejectsFlaggedSegmentWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	require.NoError(t, seg.markManifestRequired())
	require.NoError(t, seg.close())

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "manifest.bin is missing")
}

func TestQwpSfRecoveryManifestOnlyBoundaries(t *testing.T) {
	t.Run("data-boundaries-fail", func(t *testing.T) {
		dir := t.TempDir()
		m, err := qwpSfManifestCreate(dir, 0, 1)
		require.NoError(t, err)
		require.NoError(t, m.close())
		_, _, err = qwpSfRecoverRing(dir, 4096)
		require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	})
	t.Run("collapsed-is-empty", func(t *testing.T) {
		dir := t.TempDir()
		m, err := qwpSfManifestCreate(dir, 4, 4)
		require.NoError(t, err)
		require.NoError(t, m.close())
		ring, manifest, err := qwpSfRecoverRing(dir, 4096)
		require.NoError(t, err)
		assert.Nil(t, ring)
		assert.Nil(t, manifest)
		_, err = os.Stat(filepath.Join(dir, qwpSfManifestFileName))
		assert.True(t, os.IsNotExist(err))
	})
}

func TestQwpSfRecoveryCollapsedManifestWithCorruptActiveFailsClosed(t *testing.T) {
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", 4, "unacked")
	createRecoveryManifest(t, dir, 4, 4, active)
	closeRecoverySegments(t, active)
	path := filepath.Join(dir, "sf-active.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0}, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.ErrorContains(t, err, "may carry frames")
	_, statErr := os.Stat(path)
	require.NoError(t, statErr, "fail-closed recovery must preserve the corrupt active")
}

// writeFramefulCorrupt writes an unreadable segment file that recovery cannot
// prove frameless: it is long enough to hold a header and carries non-zero
// bytes, so its identity in the chain stays unknown.
func writeFramefulCorrupt(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	body := make([]byte, 4096)
	for i := range body {
		body[i] = 0xab
	}
	require.NoError(t, os.WriteFile(path, body, 0o644))
	return path
}

// writeFramelessResidue writes what a crash during qwpSfCreateSegment leaves
// behind: a full-size file whose header never reached the disk, so every byte
// in it is zero.
func writeFramelessResidue(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0o644))
	return path
}

// A legacy slot carries no committed boundaries, but a file proven to hold no
// frames cannot be a link in its chain, so it does not block the migration.
func TestQwpSfRecoveryMigratesLegacyChainPastFramelessResidue(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a", "b")
	closeRecoverySegments(t, seg)
	residue := writeFramelessResidue(t, dir, "sf-spare.sfa")

	ring, manifest, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	require.NotNil(t, manifest)
	defer func() { require.NoError(t, ring.segmentRingClose()) }()
	assert.Equal(t, int64(2), ring.getActiveSegment().segmentFrameCount())

	_, statErr := os.Stat(residue)
	assert.True(t, os.IsNotExist(statErr), "frameless residue must not stay under an .sfa name")
	_, statErr = os.Stat(residue + ".corrupt")
	assert.NoError(t, statErr, "frameless residue is preserved aside")
}

// Every file unreadable and one of them possibly carrying frames is a slot
// whose rows cannot be shown delivered, with or without a manifest to bound it.
func TestQwpSfRecoveryAllUnreadableWithoutManifestFailsClosed(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "unacked")
	closeRecoverySegments(t, seg)
	path := filepath.Join(dir, "sf-initial.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0}, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.ErrorContains(t, err, "may carry frames")
	_, statErr := os.Stat(path)
	require.NoError(t, statErr, "fail-closed recovery must preserve the unreadable segment")
}

// Nothing but frameless residue is an empty slot: it starts fresh and the bytes
// are preserved aside rather than fed to a chain.
func TestQwpSfRecoveryFramelessResidueOnlyIsEmptySlot(t *testing.T) {
	dir := t.TempDir()
	residue := writeFramelessResidue(t, dir, "sf-initial.sfa")

	ring, manifest, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	assert.Nil(t, ring)
	assert.Nil(t, manifest)
	_, statErr := os.Stat(residue)
	assert.True(t, os.IsNotExist(statErr))
	_, statErr = os.Stat(residue + ".corrupt")
	assert.NoError(t, statErr)
}

func TestQwpSfRecoveryMigratesLegacyAndStampsManifestFlag(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	require.NoError(t, seg.close())

	ring, manifest, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	require.NotNil(t, manifest)
	assert.True(t, ring.getActiveSegment().segmentManifestRequired())
	require.NoError(t, ring.segmentRingClose())

	ring, _, err = qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	assert.Equal(t, int64(0), ring.getActiveSegment().segmentBaseSeq())
}

func TestQwpSfRecoveryRejectsOverflowingLegacyTerminalRangeBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-initial.sfa")
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", math.MaxInt64, "a")
	require.NoError(t, seg.close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	ring, manifest, err := qwpSfRecoverRing(dir, 4096)
	if ring != nil {
		_ = ring.segmentRingClose()
	}
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.Nil(t, ring)
	require.Nil(t, manifest)
	require.ErrorContains(t, err, "segment range overflows FSN space")
	after, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, before, after, "range validation must precede header stamping or sanitation")
	_, statErr := os.Stat(filepath.Join(dir, qwpSfManifestFileName))
	require.True(t, os.IsNotExist(statErr), "range validation must precede manifest synthesis")
}

// TestQwpSfRecoveryResyncsMigratedHeadersOncePerOpen pins the deliberate
// restart cost of cross-process durability validation. A mapped flag may come
// from a process that died after pwrite but before fsync, so every reopened
// flagged segment receives one barrier before recovery trusts it. Repeated
// calls on the same segment object remain free.
func TestQwpSfRecoveryResyncsMigratedHeadersOncePerOpen(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	require.NoError(t, s0.close())
	require.NoError(t, s1.close())

	// The first recovery migrates the legacy slot and stamps every segment.
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NoError(t, ring.segmentRingClose())

	var flushes atomic.Int64
	hook := func(string) { flushes.Add(1) }
	originalHook := qwpSfTestSegmentSyncHeaderHook.Load()
	qwpSfTestSegmentSyncHeaderHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestSegmentSyncHeaderHook.Store(originalHook) })
	ring, _, err = qwpSfRecoverRing(dir, 4096)
	qwpSfTestSegmentSyncHeaderHook.Store(originalHook)
	require.NoError(t, err)
	require.NoError(t, ring.segmentRingClose())

	assert.Equal(t, int64(2), flushes.Load(),
		"each reopened flagged segment must be resynced exactly once")
}

func TestQwpSfRecoverySanitizesSealedResidueThenRetries(t *testing.T) {
	dir := t.TempDir()
	s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
	createRecoveryManifest(t, dir, 0, 1, s0, s1)
	off := s0.publishedOffset()
	s0.buf[off+20] = 0x7f // all-zero bad header with non-zero payload farther on
	closeRecoverySegments(t, s0, s1)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrSanitizedResidue)

	b, err := os.ReadFile(filepath.Join(dir, "sf-initial.sfa"))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, len(b)-int(off)), b[off:])

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	assert.Equal(t, int64(1), ring.segmentRingPublishedFsn())
}

func TestQwpSfRecoverySanitizesSealedResidueEquivalently(t *testing.T) {
	for _, manifestBacked := range []bool{false, true} {
		name := "legacy"
		if manifestBacked {
			name = "manifest"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
			active := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
			if manifestBacked {
				createRecoveryManifest(t, dir, 0, 1, sealed, active)
			}
			off := sealed.publishedOffset()
			sealed.buf[off+20] = 0x7f
			closeRecoverySegments(t, sealed, active)

			_, _, err := qwpSfRecoverRing(dir, 4096)
			require.ErrorIs(t, err, qwpSfErrSanitizedResidue,
				"both provenance paths must stop after applying the same sealed-tail sanitization action")

			onDisk, err := os.ReadFile(filepath.Join(dir, "sf-initial.sfa"))
			require.NoError(t, err)
			assert.Equal(t, make([]byte, len(onDisk)-int(off)), onDisk[off:])

			ring, recoveredManifest, err := qwpSfRecoverRing(dir, 4096)
			require.NoError(t, err)
			require.NotNil(t, ring)
			defer ring.segmentRingClose()
			require.NotNil(t, recoveredManifest,
				"legacy recovery must have synthesized the same durable boundary before applying mutations")
			assert.Equal(t, int64(1), ring.segmentRingPublishedFsn())
		})
	}
}

func TestQwpSfRecoveryActionPlanIsIndependentOfFileOrder(t *testing.T) {
	dir := t.TempDir()
	stale := createRecoverySegment(t, dir, "sf-stale.sfa", 0, "f0", "f1")
	sealed := createRecoverySegment(t, dir, "sf-sealed.sfa", 2, "f2")
	sealedTail := sealed.publishedOffset()
	sealed.buf[sealedTail+20] = 1
	require.NoError(t, sealed.close())
	sealed, err := qwpSfOpenSegment(sealed.segmentPath())
	require.NoError(t, err)
	active := createRecoverySegment(t, dir, "sf-active.sfa", 3, "f3")
	duplicate := createRecoverySegment(t, dir, "sf-duplicate.sfa", 3, "duplicate")
	torn := openTornEmptySegment(t, dir, "sf-torn.sfa", 3)
	empty := createRecoverySegment(t, dir, "sf-empty.sfa", 4)
	segments := []*qwpSfSegment{stale, sealed, active, duplicate, torn, empty}
	t.Cleanup(func() { closeRecoverySegments(t, segments...) })

	expected := map[string]qwpSfRecoveryAction{
		"sf-stale.sfa":     qwpSfRecoveryUnlink,
		"sf-sealed.sfa":    qwpSfRecoverySanitizeSealed,
		"sf-active.sfa":    qwpSfRecoveryKeep,
		"sf-duplicate.sfa": qwpSfRecoveryQuarantine,
		"sf-torn.sfa":      qwpSfRecoveryQuarantine,
		"sf-empty.sfa":     qwpSfRecoveryUnlink,
	}
	manifest := &qwpSfManifest{generation: 1, headBase: 2, activeBase: 3}

	var visitPermutations func(int)
	permuted := append([]*qwpSfSegment(nil), segments...)
	seen := 0
	visitPermutations = func(at int) {
		if at == len(permuted) {
			seen++
			facts := make([]qwpSfRecoveryFilePlan, 0, len(permuted))
			for _, seg := range permuted {
				facts = append(facts, qwpSfRecoveryFilePlan{
					path:             seg.segmentPath(),
					segment:          seg,
					baseSeq:          seg.segmentBaseSeq(),
					validFrames:      seg.segmentFrameCount(),
					tornTailBytes:    seg.segmentTornTailBytes(),
					manifestRequired: seg.segmentManifestRequired(),
					mayHoldFrames:    seg.segmentFrameCount() > 0 || seg.segmentTornTailBytes() > 0,
					action:           qwpSfRecoveryKeep,
				})
			}
			plan := qwpSfBuildRecoveryPlan(dir, 4096, permuted, facts, manifest, false)
			require.NoError(t, plan.failClosedErr)
			require.Equal(t, qwpSfManifestCommitted, plan.manifestProvenance)
			require.Equal(t, filepath.Join(dir, "sf-sealed.sfa"), plan.retryAfterSanitizePath)
			for _, file := range plan.files {
				require.Equal(t, expected[filepath.Base(file.path)], file.action,
					"unexpected action for %s in permutation %d", file.path, seen)
				require.NotEmpty(t, file.license)
			}
			return
		}
		for i := at; i < len(permuted); i++ {
			permuted[at], permuted[i] = permuted[i], permuted[at]
			visitPermutations(at + 1)
			permuted[at], permuted[i] = permuted[i], permuted[at]
		}
	}
	visitPermutations(0)
	require.Equal(t, 720, seen)
}

func TestQwpSfRecoveryLegacyPositiveHeadWithCorruptUnknownFailsClosed(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-0002.sfa", 2, "a")
	require.NoError(t, seg.close())
	writeFramefulCorrupt(t, dir, "sf-unknown.sfa")

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.True(t, errors.Is(err, qwpSfErrRecoveryFailClosed))
	assert.Contains(t, err.Error(), "cannot migrate")
}

func TestQwpSfRecoveryManifestBoundaryExtras(t *testing.T) {
	t.Run("stale-below-head-is-removed", func(t *testing.T) {
		dir := t.TempDir()
		stale := createRecoverySegment(t, dir, "sf-stale.sfa", 0, "old")
		active := createRecoverySegment(t, dir, "sf-active.sfa", 1, "live")
		createRecoveryManifest(t, dir, 1, 1, stale, active)
		closeRecoverySegments(t, stale, active)

		ring, _, err := qwpSfRecoverRing(dir, 4096)
		require.NoError(t, err)
		require.NotNil(t, ring)
		defer ring.segmentRingClose()
		_, err = os.Stat(filepath.Join(dir, "sf-stale.sfa"))
		assert.True(t, os.IsNotExist(err))
		assert.Equal(t, int64(1), ring.getActiveSegment().segmentBaseSeq())
	})

	t.Run("overlap-head-fails", func(t *testing.T) {
		dir := t.TempDir()
		overlap := createRecoverySegment(t, dir, "sf-overlap.sfa", 0, "a", "b")
		active := createRecoverySegment(t, dir, "sf-active.sfa", 1, "live")
		createRecoveryManifest(t, dir, 1, 1, overlap, active)
		closeRecoverySegments(t, overlap, active)
		_, _, err := qwpSfRecoverRing(dir, 4096)
		require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
		assert.Contains(t, err.Error(), "overlaps")
	})

	t.Run("beyond-active-fails", func(t *testing.T) {
		dir := t.TempDir()
		active := createRecoverySegment(t, dir, "sf-active.sfa", 0, "a")
		beyond := createRecoverySegment(t, dir, "sf-beyond.sfa", 1, "b")
		createRecoveryManifest(t, dir, 0, 0, active, beyond)
		closeRecoverySegments(t, active, beyond)
		_, _, err := qwpSfRecoverRing(dir, 4096)
		require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
		assert.Contains(t, err.Error(), "beyond")
	})
}

func TestQwpSfRecoveryRejectsMissingManifestHead(t *testing.T) {
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", 1, "live")
	createRecoveryManifest(t, dir, 0, 1, active)
	closeRecoverySegments(t, active)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "missing expected SF head segment")
}

func TestQwpSfRecoveryCleanDrainCrashWindowRemovesStaleFiles(t *testing.T) {
	dir := t.TempDir()
	stale := createRecoverySegment(t, dir, "sf-stale.sfa", 0, "old")
	createRecoveryManifest(t, dir, 2, 2, stale)
	closeRecoverySegments(t, stale)

	ring, manifest, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	assert.Nil(t, ring)
	assert.Nil(t, manifest)
	_, err = os.Stat(filepath.Join(dir, "sf-stale.sfa"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, qwpSfManifestFileName))
	assert.True(t, os.IsNotExist(err))
}

func TestQwpSfRecoveryRotationCrashWindow(t *testing.T) {
	// The active segment sits one base past the end of a chain the manifest
	// fully accounts for, so the tail is proved by the files that are readable.
	// Only a stray that could itself be carrying frames puts that proof in
	// doubt; a zero-length one -- what a crash inside qwpSfCreateSegment leaves
	// behind -- does not, and must not cost the slot.
	for _, tc := range []struct {
		name       string
		stray      []byte
		failClosed bool
	}{
		{name: "clean"},
		{name: "corrupt-unknown", stray: bytes.Repeat([]byte{0xab}, 4096), failClosed: true},
		{name: "zero-length-stray", stray: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
			active := createRecoverySegment(t, dir, "sf-active.sfa", 1)
			createRecoveryManifest(t, dir, 0, 1, sealed, active)
			closeRecoverySegments(t, sealed, active)
			if tc.stray != nil {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-unknown.sfa"), tc.stray, 0o644))
			}
			ring, _, err := qwpSfRecoverRing(dir, 4096)
			if tc.failClosed {
				require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ring)
			defer ring.segmentRingClose()
			assert.Equal(t, int64(1), ring.getActiveSegment().segmentBaseSeq())
			assert.Equal(t, 1, ring.sealedSegmentCount())
			assert.Equal(t, int64(0), ring.segmentRingPublishedFsn())
		})
	}
}

func TestQwpSfRecoveryFullyTrimmedEmptyActiveKeepsSequenceDomain(t *testing.T) {
	const (
		baseSeq     int64 = 7
		segmentSize int64 = 4096
	)
	dir := t.TempDir()
	activePath := filepath.Join(dir, "sf-active.sfa")
	active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
	createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
	closeRecoverySegments(t, active)

	engine, err := qwpSfNewCursorEngine(dir, segmentSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()
	require.Equal(t, baseSeq-1, engine.enginePublishedFsn())
	require.Equal(t, baseSeq-1, engine.engineAckedFsn())
	require.Equal(t, baseSeq, engine.ring.nextSeqHint())

	require.Eventually(t, func() bool {
		return engine.ring.hotSpare.Load() != nil
	}, time.Second, time.Millisecond)

	// Fill the recovered active almost to its end, then append once more to
	// rotate it into the sealed chain.
	filler := make([]byte, segmentSize-qwpSfHeaderSize-qwpSfFrameHeaderSize-4)
	fsn, err := engine.engineAppendBlocking(context.Background(), filler)
	require.NoError(t, err)
	require.Equal(t, baseSeq, fsn)
	fsn, err = engine.engineAppendBlocking(context.Background(), []byte("next"))
	require.NoError(t, err)
	require.Equal(t, baseSeq+1, fsn)

	engine.engineAcknowledge(baseSeq)
	require.Equal(t, baseSeq, engine.engineAckedFsn())
	awaitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sender := &qwpLineSender{cursorEngine: engine, cursorSendLoop: &qwpSfSendLoop{}}
	require.NoError(t, sender.AwaitAckedFsn(awaitCtx, baseSeq))
	require.Eventually(t, func() bool {
		_, err := os.Stat(activePath)
		return os.IsNotExist(err)
	}, time.Second, time.Millisecond, "the acknowledged segment at the recovered base must trim")
}

func TestQwpSfRecoveryLegacyBaseZeroRefusesCorruptStray(t *testing.T) {
	// A legacy slot has no committed boundaries, so nothing shows the corrupt
	// file's frames already delivered — not even when the surviving chain
	// starts at base 0 and therefore owns the head. Recovery refuses to migrate
	// and leaves the bytes exactly where they are.
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	require.NoError(t, seg.close())
	stray := writeFramefulCorrupt(t, dir, "sf-stray.sfa")

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "corrupt segment of unknown identity")
	_, statErr := os.Stat(stray)
	require.NoError(t, statErr)
	_, statErr = os.Stat(stray + ".corrupt")
	require.True(t, os.IsNotExist(statErr))
}

func TestQwpSfRecoveryRejectsLegacyTornEmptyBelowPositiveHead(t *testing.T) {
	dir := t.TempDir()
	torn := createRecoverySegment(t, dir, "sf-torn.sfa", 0)
	torn.buf[qwpSfHeaderSize+20] = 1
	data := createRecoverySegment(t, dir, "sf-data.sfa", 2, "a")
	closeRecoverySegments(t, torn, data)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	assert.Contains(t, err.Error(), "lost its frames")
}

func TestQwpSfRecoverySanitizesActiveTail(t *testing.T) {
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	createRecoveryManifest(t, dir, 0, 0, active)
	off := active.publishedOffset()
	active.buf[off+20] = 1
	require.NoError(t, active.close())

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	assert.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
	b, err := os.ReadFile(filepath.Join(dir, "sf-initial.sfa"))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, len(b)-int(off)), b[off:])
}

func TestQwpSfRecoveryQuarantinesTornEmptyActiveBeforeReplacement(t *testing.T) {
	const baseSeq int64 = 7
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
	createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
	closeRecoverySegments(t, active)
	path := filepath.Join(dir, "sf-active.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+20)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	require.Equal(t, baseSeq, ring.getActiveSegment().segmentBaseSeq())
	require.Zero(t, ring.getActiveSegment().segmentFrameCount())
	require.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
	require.Equal(t, baseSeq-1, ring.segmentRingPublishedFsn())

	corrupt, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, byte(1), corrupt[qwpSfHeaderSize+20], "quarantine must preserve the torn bytes")
}

func TestQwpSfRecoveryRetryCommitsInstalledTornActiveNamespace(t *testing.T) {
	const baseSeq int64 = 7
	dir, path := tornActiveSlot(t, baseSeq)
	originalDirSync := qwpSfTestDirSyncHook.Load()
	t.Cleanup(func() { qwpSfTestDirSyncHook.Store(originalDirSync) })

	injectedInstall := errors.New("injected installed-replacement barrier failure")
	barriers := 0
	failSecond := func(string) error {
		barriers++
		if barriers == 2 {
			return injectedInstall
		}
		return nil
	}
	qwpSfTestDirSyncHook.Store(&failSecond)
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	if ring != nil {
		_ = ring.segmentRingClose()
	}
	require.ErrorIs(t, err, injectedInstall)
	require.Equal(t, 2, barriers, "fault must follow the replacement install rename")
	require.FileExists(t, path, "the clean replacement is visible after the failed barrier")
	require.FileExists(t, path+".corrupt", "the torn bytes remain preserved")

	injectedRetry := errors.New("injected retry namespace barrier failure")
	retryBarriers := 0
	failRetry := func(string) error {
		retryBarriers++
		return injectedRetry
	}
	qwpSfTestDirSyncHook.Store(&failRetry)
	ring, _, err = qwpSfRecoverRing(dir, 4096)
	if ring != nil {
		_ = ring.segmentRingClose()
	}
	require.ErrorIs(t, err, injectedRetry,
		"retry must not expose the installed inode until its namespace is durable")
	require.Equal(t, 1, retryBarriers)

	qwpSfTestDirSyncHook.Store(originalDirSync)
	ring, _, err = qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer func() { _ = ring.segmentRingClose() }()
	require.Equal(t, baseSeq, ring.getActiveSegment().segmentBaseSeq())
	require.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
}

// TestQwpSfRecoveryKeepsTornActiveWhenReplacementCannotBeCreated pins that a
// full disk during the replacement costs nothing permanent. Quarantining the
// torn file first would leave the slot with no segment at its committed active
// base, which the next startup refuses — so a disk that fills up and empties
// again would cost the whole slot.
func TestQwpSfRecoveryKeepsTornActiveWhenReplacementCannotBeCreated(t *testing.T) {
	const baseSeq int64 = 7
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
	createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
	closeRecoverySegments(t, active)
	path := filepath.Join(dir, "sf-active.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+20)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	originalReserve := qwpSfReserveNewBlocksFn.load()
	qwpSfReserveNewBlocksFn.store(func(*os.File, int64, int64) error { return syscall.ENOSPC })
	_, _, err = qwpSfRecoverRing(dir, 4096)
	qwpSfReserveNewBlocksFn.store(originalReserve)
	require.ErrorIs(t, err, syscall.ENOSPC)

	// The torn file is still where the manifest says the active segment is, and
	// nothing was filed away as evidence yet.
	torn, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte(1), torn[qwpSfHeaderSize+20])
	_, err = os.Stat(path + ".corrupt")
	require.True(t, os.IsNotExist(err), "nothing may be quarantined until the replacement exists")
	_, err = os.Stat(path + qwpSfTornActiveTempSuffix)
	require.True(t, os.IsNotExist(err), "the half-built replacement must not be left behind")

	// With space back, the same slot recovers.
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	require.Equal(t, baseSeq, ring.getActiveSegment().segmentBaseSeq())
	require.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
	corrupt, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, byte(1), corrupt[qwpSfHeaderSize+20], "quarantine must preserve the torn bytes")
}

// TestQwpSfRecoveryKeepsTornActiveWhenInstallRenameFails covers the last exit
// from the swap. Once the torn file has been set aside, a failed install leaves
// the committed active base empty unless the function puts something back --
// and neither .corrupt nor .replacing ends in .sfa, so no later directory scan
// would ever find those bytes again. The slot would be refused, quarantined or
// marked failed, over a rename.
//
// Both preservation strategies are exercised: the hard link, where path is
// never vacated at all, and the rename fallback for filesystems without links,
// which has to roll its rename back.
func TestQwpSfRecoveryKeepsTornActiveWhenInstallRenameFails(t *testing.T) {
	for _, tc := range []struct {
		name      string
		linkFails bool
	}{
		{name: "hard-link"},
		{name: "rename-fallback", linkFails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const baseSeq int64 = 7
			dir := t.TempDir()
			active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
			createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
			closeRecoverySegments(t, active)
			path := filepath.Join(dir, "sf-active.sfa")
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			require.NoError(t, err)
			_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+20)
			require.NoError(t, err)
			require.NoError(t, f.Sync())
			require.NoError(t, f.Close())

			// Fail only the install rename, identified by its source: the
			// fallback's move-aside and its rollback go through the same seam.
			originalRename := qwpSfTornActiveRename.load()
			qwpSfTornActiveRename.store(func(from, to string) error {
				if strings.HasSuffix(from, qwpSfTornActiveTempSuffix) {
					return syscall.EIO
				}
				return originalRename(from, to)
			})
			originalLink := qwpSfTornActiveLink.load()
			if tc.linkFails {
				qwpSfTornActiveLink.store(func(string, string) error { return syscall.EPERM })
			}
			_, _, err = qwpSfRecoverRing(dir, 4096)
			qwpSfTornActiveRename.store(originalRename)
			qwpSfTornActiveLink.store(originalLink)
			require.ErrorIs(t, err, syscall.EIO)

			torn, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, byte(1), torn[qwpSfHeaderSize+20],
				"the committed active base must still hold the torn segment")
			_, err = os.Stat(path + ".corrupt")
			require.True(t, os.IsNotExist(err), "a failed swap leaves no half-filed evidence")
			_, err = os.Stat(path + qwpSfTornActiveTempSuffix)
			require.True(t, os.IsNotExist(err), "the unused replacement must not be left behind")

			// The next recovery simply tries again.
			ring, _, err := qwpSfRecoverRing(dir, 4096)
			require.NoError(t, err)
			require.NotNil(t, ring)
			defer ring.segmentRingClose()
			require.Equal(t, baseSeq, ring.getActiveSegment().segmentBaseSeq())
			require.Zero(t, ring.getActiveSegment().segmentTornTailBytes())
			corrupt, err := os.ReadFile(path + ".corrupt")
			require.NoError(t, err)
			require.Equal(t, byte(1), corrupt[qwpSfHeaderSize+20], "quarantine must preserve the torn bytes")
		})
	}
}

// TestQwpSfRecoveryTornActiveMoveAsideFailure covers the exit taken when the
// filesystem has no hard links AND the move-aside rename fails: nothing has
// been touched yet, so the torn segment must still be at the committed active
// base and the unused replacement must be gone.
func TestQwpSfRecoveryTornActiveMoveAsideFailure(t *testing.T) {
	const baseSeq int64 = 3
	dir, path := tornActiveSlot(t, baseSeq)

	originalLink := qwpSfTornActiveLink.load()
	originalRename := qwpSfTornActiveRename.load()
	// Restore through t.Cleanup: a fault inside qwpSfRecoverRing would
	// otherwise leave both seams returning errors for the rest of the package
	// run, failing every later recovery test for the wrong reason.
	t.Cleanup(func() {
		qwpSfTornActiveLink.store(originalLink)
		qwpSfTornActiveRename.store(originalRename)
	})
	qwpSfTornActiveLink.store(func(string, string) error { return syscall.EPERM })
	qwpSfTornActiveRename.store(func(from, to string) error {
		if strings.HasSuffix(from, ".sfa") {
			return syscall.EIO
		}
		return originalRename(from, to)
	})
	_, _, err := qwpSfRecoverRing(dir, 4096)
	qwpSfTornActiveLink.store(originalLink)
	qwpSfTornActiveRename.store(originalRename)
	require.ErrorIs(t, err, syscall.EIO)

	torn, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte(1), torn[qwpSfHeaderSize+20],
		"the committed active base must still hold the torn segment")
	_, err = os.Stat(path + qwpSfTornActiveTempSuffix)
	require.True(t, os.IsNotExist(err), "the unused replacement must not be left behind")
}

// TestQwpSfRecoveryTornActiveRollbackFailure covers the one exit that cannot
// put a file back at the committed active base: no hard links, the install
// rename fails, and so does the rollback. The torn bytes survive under the
// preserved name and the error has to say so, because the next recovery fails
// closed on a missing active segment.
func TestQwpSfRecoveryTornActiveRollbackFailure(t *testing.T) {
	const baseSeq int64 = 3
	dir, path := tornActiveSlot(t, baseSeq)

	originalLink := qwpSfTornActiveLink.load()
	originalRename := qwpSfTornActiveRename.load()
	// Restore through t.Cleanup for the same reason as the sibling test above.
	t.Cleanup(func() {
		qwpSfTornActiveLink.store(originalLink)
		qwpSfTornActiveRename.store(originalRename)
	})
	qwpSfTornActiveLink.store(func(string, string) error { return syscall.EPERM })
	qwpSfTornActiveRename.store(func(from, to string) error {
		// The move-aside is the only rename allowed through. A .sfa.replacing
		// source is the install, which must fail; so must the rollback.
		if strings.HasSuffix(from, ".sfa") {
			return originalRename(from, to)
		}
		return syscall.EIO
	})
	_, _, err := qwpSfRecoverRing(dir, 4096)
	qwpSfTornActiveLink.store(originalLink)
	qwpSfTornActiveRename.store(originalRename)
	require.ErrorIs(t, err, syscall.EIO)
	require.Contains(t, err.Error(), "no file is left at the committed active base")

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "the rollback failed, so nothing is at the active base")
	corrupt, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, byte(1), corrupt[qwpSfHeaderSize+20],
		"the torn bytes must survive under the preserved name the error names")

	// The committed boundaries decide what the next recovery makes of the
	// emptied slot. Here head == active, so it committed no frames and the slot
	// collapses to a fresh one; the preserved copy stays the record of the bytes.
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	if ring != nil {
		ring.segmentRingClose()
	}
	_, err = os.Stat(path + ".corrupt")
	require.NoError(t, err, "the preserved copy must survive the fresh start")
}

// tornActiveSlot builds a one-segment manifest-backed slot whose committed
// active segment carries a torn tail, and returns the slot dir and that
// segment's path.
func tornActiveSlot(t *testing.T, baseSeq int64) (string, string) {
	t.Helper()
	dir := t.TempDir()
	active := createRecoverySegment(t, dir, "sf-active.sfa", baseSeq)
	createRecoveryManifest(t, dir, baseSeq, baseSeq, active)
	closeRecoverySegments(t, active)
	path := filepath.Join(dir, "sf-active.sfa")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+20)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	return dir, path
}

func TestQwpSfQuarantinePathPreservesEarlierEvidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-active.sfa")
	require.NoError(t, os.WriteFile(path, []byte("new evidence"), 0o644))
	require.NoError(t, os.WriteFile(path+".corrupt", []byte("old evidence"), 0o644))

	target, err := qwpSfQuarantinePath(path)
	require.NoError(t, err)
	require.Equal(t, path+".corrupt-1", target)
	oldEvidence, err := os.ReadFile(path + ".corrupt")
	require.NoError(t, err)
	require.Equal(t, []byte("old evidence"), oldEvidence)
	newEvidence, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, []byte("new evidence"), newEvidence)
}

// TestQwpSfForegroundStartsFreshOverUnreadableSegmentVersion pins that a
// segment this build cannot read costs the sender nothing permanent. Nothing
// later can make the file readable here, so leaving the slot in place would
// mean every NewLineSender on it fails forever with no way back. Preserving the
// slot whole keeps the bytes for a client that does understand them, and the
// sender comes up on a fresh slot and keeps ingesting.
func TestQwpSfForegroundStartsFreshOverUnreadableSegmentVersion(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	require.NoError(t, os.MkdirAll(slot, 0o755))
	seg := createRecoverySegment(t, slot, "sf-initial.sfa", 0, "a")
	closeRecoverySegments(t, seg)
	path := filepath.Join(slot, "sf-initial.sfa")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[4] = qwpSfSegmentVersion + 1
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	require.NotNil(t, engine)
	assert.False(t, engine.engineWasRecoveredFromDisk())
	require.NoError(t, engine.engineClose())

	entries, err := os.ReadDir(filepath.Join(root, "quarantined"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	preserved, err := os.ReadFile(filepath.Join(root, "quarantined", entries[0].Name(), "sf-initial.sfa"))
	require.NoError(t, err)
	assert.Equal(t, qwpSfSegmentVersion+1, preserved[4],
		"the unreadable segment must be preserved byte-for-byte, not rewritten or renamed as corruption")
}

func TestQwpSfForegroundFailClosedQuarantinesAndStartsFresh(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	require.NoError(t, os.MkdirAll(slot, 0o755))
	s0 := createRecoverySegment(t, slot, "sf-initial.sfa", 0, "a")
	s1 := createRecoverySegment(t, slot, "sf-active.sfa", 1, "b")
	createRecoveryManifest(t, slot, 0, 1, s0, s1)
	closeRecoverySegments(t, s0, s1)
	require.NoError(t, os.WriteFile(filepath.Join(slot, "sf-active.sfa"), make([]byte, 4096), 0o644))

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	require.NotNil(t, engine)
	assert.False(t, engine.engineWasRecoveredFromDisk())
	require.NoError(t, engine.engineClose())

	quarantineRoot := filepath.Join(root, "quarantined")
	entries, err := os.ReadDir(quarantineRoot)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.False(t, qwpSfIsCandidateOrphan(quarantineRoot))

	// Nothing reclaims that copy, so the sender has to be able to say where it
	// put it — reading the log must not be the only way to find the rows.
	assert.Equal(t, filepath.Join(quarantineRoot, entries[0].Name()), engine.engineQuarantinedSlotPath())
}

// TestQwpSfEngineQuarantinedSlotPathEmptyWithoutQuarantine pins the other
// answer: a slot that recovered normally set nothing aside.
func TestQwpSfEngineQuarantinedSlotPathEmptyWithoutQuarantine(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	require.NotNil(t, engine)
	defer func() { _ = engine.engineClose() }()
	assert.Equal(t, "", engine.engineQuarantinedSlotPath())
}

// snapshotFrameBearingSegments maps the content hash of every readable .sfa
// file in dir that holds at least one frame to its name.
func snapshotFrameBearingSegments(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sfa") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		seg, openErr := qwpSfOpenSegment(path)
		if openErr != nil {
			continue
		}
		frames := seg.segmentFrameCount()
		require.NoError(t, seg.close())
		if frames == 0 {
			continue
		}
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		out[fmt.Sprintf("%x", sha256.Sum256(raw))] = entry.Name()
	}
	return out
}

// snapshotAllContents hashes every regular file in dir, whatever its name, so
// a file that recovery preserved under a different one still counts.
func snapshotAllContents(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		out[fmt.Sprintf("%x", sha256.Sum256(raw))] = entry.Name()
	}
	return out
}

// TestQwpSfFailedRecoveryPreservesEveryRequiredFrame pins the invariant this
// file opens with. Recovery mutates the slot well before it knows it will
// succeed -- zeroing dead bytes, flagging headers, creating and removing the
// manifest, removing files it proves stale -- so "we failed closed" is only
// safe if none of that can reach a frame the boundaries still account for. A
// fail-closed slot is handed to an operator or to a newer client, and both are
// reading the bytes that are left.
//
// Each case is a slot that fails closed for a different reason. The check is
// content, not names: preserving a file by renaming it to .corrupt is allowed,
// losing or rewriting its bytes is not.
func TestQwpSfFailedRecoveryPreservesEveryRequiredFrame(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T, dir string)
	}{
		{
			name: "active-segment-unreadable",
			build: func(t *testing.T, dir string) {
				s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
				s1 := createRecoverySegment(t, dir, "sf-0001.sfa", 1, "b")
				createRecoveryManifest(t, dir, 0, 1, s0, s1)
				closeRecoverySegments(t, s0, s1)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-0001.sfa"),
					bytes.Repeat([]byte{0xab}, 4096), 0o644))
			},
		},
		{
			name: "frames-beyond-the-committed-active-boundary",
			build: func(t *testing.T, dir string) {
				s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
				beyond := createRecoverySegment(t, dir, "sf-0009.sfa", 9, "later")
				createRecoveryManifest(t, dir, 0, 0, s0, beyond)
				closeRecoverySegments(t, s0, beyond)
			},
		},
		{
			name: "legacy-slot-with-a-corrupt-stray",
			build: func(t *testing.T, dir string) {
				seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
				closeRecoverySegments(t, seg)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-stray.sfa"),
					bytes.Repeat([]byte{0xcd}, 4096), 0o644))
			},
		},
		{
			name: "empty-chain-beside-a-corrupt-file-that-could-hold-frames",
			build: func(t *testing.T, dir string) {
				s0 := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
				active := createRecoverySegment(t, dir, "sf-0005.sfa", 5)
				createRecoveryManifest(t, dir, 5, 5, s0, active)
				closeRecoverySegments(t, s0, active)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-bad.sfa"),
					bytes.Repeat([]byte{0xab}, 4096), 0o644))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.build(t, dir)
			before := snapshotFrameBearingSegments(t, dir)
			require.NotEmpty(t, before, "the fixture must contain frames worth preserving")

			_, _, err := qwpSfRecoverRing(dir, 4096)
			require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)

			after := snapshotAllContents(t, dir)
			for hash, name := range before {
				require.Contains(t, after, hash,
					"failed recovery lost the bytes of %s, which the committed boundaries still account for", name)
			}
		})
	}
}

// TestQwpSfCollapsedSlotQuarantinesCorruptFiles pins that the two exits which
// collapse a slot and start fresh still set the corrupt files aside. The fresh
// slot is built in the same directory, and qwpSfCreateSegment opens
// sf-initial.sfa with O_TRUNC — so a corrupt file left under that name is
// destroyed in place, which is the one thing recovery promises never to do.
func TestQwpSfCollapsedSlotQuarantinesCorruptFiles(t *testing.T) {
	dir := t.TempDir()
	// A slot whose committed boundaries meet above every segment on disk: the
	// sealed segment is below head, so the chain is empty and no active segment
	// is found -- the collapse exit. A provably frameless corrupt file sits
	// alongside it, on the name the fresh slot is about to create with O_TRUNC.
	sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a", "b")
	createRecoveryManifest(t, dir, 2, 2, sealed)
	closeRecoverySegments(t, sealed)
	// A separate corrupt file, so the slot still has one openable segment and
	// recovery reaches the collapse exit rather than the all-unreadable one.
	corrupt := filepath.Join(dir, "sf-00000001.sfa")
	require.NoError(t, os.WriteFile(corrupt, make([]byte, 4096), 0o644))

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	if ring != nil {
		ring.segmentRingClose()
	}

	preserved, err := os.ReadFile(corrupt + ".corrupt")
	require.NoError(t, err, "the corrupt file must be set aside before the slot restarts")
	require.Len(t, preserved, 4096)
	_, err = os.Stat(corrupt)
	require.True(t, os.IsNotExist(err), "it must not stay under its .sfa name")
}

// TestQwpSfStaleTornSegmentIsUnlinkedNotQuarantined pins that a torn tail
// below the committed head is unlinked. The manifest proves every frame in
// such a segment delivered, so its tail bytes are accounted for — preserving
// them leaves a segment-sized .corrupt file that nothing ever reclaims and
// that sf_max_total_bytes does not count, so a slot that repeatedly tears
// mid-append starves the budget meant to bound it.
func TestQwpSfStaleTornSegmentIsUnlinkedNotQuarantined(t *testing.T) {
	dir := t.TempDir()
	stale := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a", "b")
	active := createRecoverySegment(t, dir, "sf-active.sfa", 2, "c")
	createRecoveryManifest(t, dir, 2, 2, stale, active)
	closeRecoverySegments(t, stale, active)

	// Tear the stale segment's tail. It is entirely below the committed head.
	stalePath := filepath.Join(dir, "sf-initial.sfa")
	f, err := os.OpenFile(stalePath, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{1}, qwpSfHeaderSize+2048)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()

	_, err = os.Stat(stalePath)
	require.True(t, os.IsNotExist(err), "a delivered segment must be unlinked")
	_, err = os.Stat(stalePath + ".corrupt")
	require.True(t, os.IsNotExist(err),
		"and must not leave debris outside the sf_max_total_bytes budget")
}

// TestQwpSfTornSegmentAtTheCommittedHeadIsPreserved pins the boundary that
// decides whether recovery may unlink a torn tail. head is always some
// segment's base, so a segment AT the head is the start of what the slot still
// owes — not something the manifest has accounted for. And frameCount stops at
// the first bad CRC, so a torn segment whose first frame is damaged reports
// zero: judging it on base+frameCount alone puts it at base and destroys any
// valid frames sitting physically behind the damage.
func TestQwpSfTornSegmentAtTheCommittedHeadIsPreserved(t *testing.T) {
	dir := t.TempDir()
	// A clean empty active at the committed base, plus a torn frameless
	// segment at the same base. qwpSfFindActive prefers the clean one, so the
	// torn file goes to qwpSfDiscardOpened.
	active := createRecoverySegment(t, dir, "sf-active.sfa", 2)
	createRecoveryManifest(t, dir, 2, 2, active)
	closeRecoverySegments(t, active)
	torn := openTornEmptySegment(t, dir, "sf-torn.sfa", 2)
	require.NoError(t, torn.markManifestRequired())
	closeRecoverySegments(t, torn)
	tornPath := filepath.Join(dir, "sf-torn.sfa")

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()

	_, statErr := os.Stat(tornPath + ".corrupt")
	require.NoError(t, statErr,
		"a torn segment at the committed head must be preserved, not unlinked")
}

// TestQwpSfLegacyMigrationRefusesATornSegmentBelowASynthesizedHead pins the
// legacy path's most dangerous shape. segmentFrameCount stops at the first bad
// CRC, so a segment whose frame 0 is damaged reports zero frames while
// physically holding rows. If the migration commits a head above it, every
// later recovery treats it as delivered and unlinks it — silently, with the
// open reporting success.
//
// The guard has to run against the FINAL head. When no segment survives with
// frames, the head comes from qwpSfChooseEmptyInitial, which skips torn
// candidates and so can land above the damaged file — the case a guard scoped
// to the frameful branch never sees.
func TestQwpSfLegacyMigrationRefusesATornSegmentBelowASynthesizedHead(t *testing.T) {
	dir := t.TempDir()
	// Damaged and frameless, at base 0. No manifest: a legacy slot.
	torn := openTornEmptySegment(t, dir, "sf-initial.sfa", 0)
	closeRecoverySegments(t, torn)
	// A clean empty segment at a higher base, which the head selection prefers
	// because it skips torn candidates.
	spare := reopenEmptySegment(t, dir, "sf-0000000000000001.sfa", 3)
	closeRecoverySegments(t, spare)

	_, _, err := qwpSfRecoverRing(dir, 4096)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed,
		"a torn segment below the synthesized head must fail the migration closed")

	_, statErr := os.Stat(filepath.Join(dir, "sf-initial.sfa"))
	require.NoError(t, statErr, "and its bytes must still be on disk")
}

// TestQwpSfFramefulDuplicateAtTheActiveBaseIsPreserved pins the preserve set,
// the one branch where a segment carrying frames at or above the committed
// head survives without being adopted into the chain. Two independent
// mutations — never consulting the set, and never populating it — deleted such
// a file with the whole suite green, so the branch whose comment says it keeps
// the no-frame-destroyed guarantee "resting on the code" was resting on
// nothing.
func TestQwpSfFramefulDuplicateAtTheActiveBaseIsPreserved(t *testing.T) {
	dir := t.TempDir()
	sealed := createRecoverySegment(t, dir, "sf-a.sfa", 0, "f0", "f1", "f2")
	active := createRecoverySegment(t, dir, "sf-b.sfa", 3, "f3")
	dup := createRecoverySegment(t, dir, "sf-c.sfa", 3, "dup3") // frameful, same base
	createRecoveryManifest(t, dir, 0, 3, sealed, active, dup)
	closeRecoverySegments(t, sealed, active, dup)

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()

	dupPath := filepath.Join(dir, "sf-c.sfa")
	_, statErr := os.Stat(dupPath + ".corrupt")
	require.NoError(t, statErr,
		"a frameful duplicate at the committed active base must be preserved")
	_, statErr = os.Stat(dupPath)
	require.True(t, os.IsNotExist(statErr), "and moved out of the .sfa namespace")
}

// TestQwpSfQuarantineTargetPathBoundsName pins that a quarantine target's
// file-name component stays under qwpSfQuarantineNameMaxLen bytes, so the
// quarantine rename cannot fail with ENAMETOOLONG over a name the client
// itself formed, and that two long names differing late in the stem still get
// distinct targets.
func TestQwpSfQuarantineTargetPathBoundsName(t *testing.T) {
	dir := t.TempDir()
	longA := strings.Repeat("a", 240) + ".sfa"
	longB := strings.Repeat("a", 239) + "b.sfa"
	require.NoError(t, os.WriteFile(filepath.Join(dir, longA), []byte("A"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, longB), []byte("B"), 0o644))

	targetA, err := qwpSfQuarantineTargetPath(filepath.Join(dir, longA))
	require.NoError(t, err)
	targetB, err := qwpSfQuarantineTargetPath(filepath.Join(dir, longB))
	require.NoError(t, err)

	require.LessOrEqual(t, len(filepath.Base(targetA)), qwpSfQuarantineNameMaxLen)
	require.LessOrEqual(t, len(filepath.Base(targetB)), qwpSfQuarantineNameMaxLen)
	require.NotEqual(t, targetA, targetB,
		"long names differing only late in the stem must not collide")

	require.NoError(t, os.Rename(filepath.Join(dir, longA), targetA),
		"the bounded target must be a legal name on the filesystem")

	// The probe still finds a free numbered name once the first is taken.
	require.NoError(t, os.WriteFile(filepath.Join(dir, longA), []byte("A2"), 0o644))
	next, err := qwpSfQuarantineTargetPath(filepath.Join(dir, longA))
	require.NoError(t, err)
	require.NotEqual(t, targetA, next)
	require.LessOrEqual(t, len(filepath.Base(next)), qwpSfQuarantineNameMaxLen)
	require.NoError(t, os.Rename(filepath.Join(dir, longA), next))
}

// TestQwpSfDiscardRefusesAnUnanchoredHead pins the executor's independent
// boundary verification. A planner bug may select unlink under a head that
// matches no retained segment, but the delete site must still refuse it.
func TestQwpSfDiscardRefusesAnUnanchoredHead(t *testing.T) {
	dir := t.TempDir()
	kept := createRecoverySegment(t, dir, "sf-kept.sfa", 2, "c")
	extra := createRecoverySegment(t, dir, "sf-extra.sfa", 0, "a")
	defer closeRecoverySegments(t, kept, extra)

	manifest := &qwpSfManifest{headBase: 1, activeBase: 2}
	file := &qwpSfRecoveryFilePlan{
		path:          extra.segmentPath(),
		segment:       extra,
		baseSeq:       extra.segmentBaseSeq(),
		validFrames:   extra.segmentFrameCount(),
		mayHoldFrames: true,
		action:        qwpSfRecoveryUnlink,
	}
	plan := &qwpSfRecoveryPlan{
		manifestProvenance: qwpSfManifestCommitted,
		manifest:           manifest,
		headBase:           1,
		activeBase:         2,
		chain:              []*qwpSfSegment{kept},
	}
	err := plan.revalidateUnlink(file)
	require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed,
		"a head matching no kept segment base must be refused, not acted on")

	_, statErr := os.Stat(filepath.Join(dir, "sf-kept.sfa"))
	require.NoError(t, statErr)
	_, statErr = os.Stat(filepath.Join(dir, "sf-extra.sfa"))
	require.NoError(t, statErr, "no file may be removed under a refused boundary")
	_, statErr = os.Stat(filepath.Join(dir, "sf-extra.sfa.corrupt"))
	require.True(t, os.IsNotExist(statErr), "nor renamed")
}

// TestQwpSfDiscardRefusesTornDeletionUnderAnUncommittedHead pins the second
// discard-site verification. A head read from a committed manifest proves
// every frame below it delivered, so a torn file wholly below it may be
// unlinked. A head the legacy migration synthesized from surviving files
// proves nothing — segmentFrameCount stops at the first bad CRC, so a torn
// file below such a head can still hold undelivered rows — and the delete
// site refuses that shape even when the branch that computed the head fails
// to.
func TestQwpSfDiscardRefusesTornDeletionUnderAnUncommittedHead(t *testing.T) {
	t.Run("uncommitted-head-refuses", func(t *testing.T) {
		dir := t.TempDir()
		kept := createRecoverySegment(t, dir, "sf-kept.sfa", 3, "c")
		torn := openTornEmptySegment(t, dir, "sf-torn.sfa", 0)
		defer closeRecoverySegments(t, kept, torn)

		file := &qwpSfRecoveryFilePlan{
			path:          torn.segmentPath(),
			segment:       torn,
			baseSeq:       torn.segmentBaseSeq(),
			tornTailBytes: torn.segmentTornTailBytes(),
			mayHoldFrames: true,
			action:        qwpSfRecoveryUnlink,
		}
		plan := &qwpSfRecoveryPlan{
			manifestProvenance: qwpSfManifestSynthesized,
			headBase:           3,
			activeBase:         3,
			chain:              []*qwpSfSegment{kept},
		}
		err := plan.revalidateUnlink(file)
		require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
		_, statErr := os.Stat(filepath.Join(dir, "sf-torn.sfa"))
		require.NoError(t, statErr, "the torn file must stay exactly where it was")
	})

	t.Run("committed-head-unlinks", func(t *testing.T) {
		dir := t.TempDir()
		kept := createRecoverySegment(t, dir, "sf-kept.sfa", 3, "c")
		torn := openTornEmptySegment(t, dir, "sf-torn.sfa", 0)
		defer closeRecoverySegments(t, kept)

		manifest := &qwpSfManifest{headBase: 3, activeBase: 3}
		files := []qwpSfRecoveryFilePlan{{
			path:          torn.segmentPath(),
			segment:       torn,
			baseSeq:       torn.segmentBaseSeq(),
			tornTailBytes: torn.segmentTornTailBytes(),
			mayHoldFrames: true,
			action:        qwpSfRecoveryUnlink,
		}}
		plan := &qwpSfRecoveryPlan{
			manifestProvenance: qwpSfManifestCommitted,
			manifest:           manifest,
			headBase:           3,
			activeBase:         3,
			chain:              []*qwpSfSegment{kept},
			files:              files,
		}
		require.NoError(t, plan.applyDirectoryActions())
		_, statErr := os.Stat(filepath.Join(dir, "sf-torn.sfa"))
		require.True(t, os.IsNotExist(statErr),
			"a committed head proves the torn file delivered, so it is unlinked")
	})
}
