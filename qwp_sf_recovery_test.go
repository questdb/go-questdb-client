package questdb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

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

func TestQwpSfRecoveryLegacyPositiveHeadWithCorruptUnknownFailsClosed(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-0002.sfa", 2, "a")
	require.NoError(t, seg.close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-unknown.sfa"), []byte("bad"), 0o644))

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
	for _, corrupt := range []bool{false, true} {
		name := "clean"
		if corrupt {
			name = "corrupt-unknown"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
			active := createRecoverySegment(t, dir, "sf-active.sfa", 1)
			createRecoveryManifest(t, dir, 0, 1, sealed, active)
			closeRecoverySegments(t, sealed, active)
			if corrupt {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sf-unknown.sfa"), []byte("bad"), 0o644))
			}
			ring, _, err := qwpSfRecoverRing(dir, 4096)
			if corrupt {
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

func TestQwpSfRecoveryLegacyBaseZeroQuarantinesCorruptStray(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "a")
	require.NoError(t, seg.close())
	stray := filepath.Join(dir, "sf-stray.sfa")
	require.NoError(t, os.WriteFile(stray, []byte("bad"), 0o644))

	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NoError(t, err)
	require.NotNil(t, ring)
	defer ring.segmentRingClose()
	_, err = os.Stat(stray + ".corrupt")
	require.NoError(t, err)
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
}
