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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type qwpSfCrashFileOpKind uint8

const (
	qwpSfCrashCreate qwpSfCrashFileOpKind = iota
	qwpSfCrashRemove
	qwpSfCrashLink
	qwpSfCrashRename
	qwpSfCrashReplace
)

type qwpSfCrashFileOp struct {
	kind qwpSfCrashFileOpKind
	from string
	to   string
	file qwpSfCrashNamespaceFile
}

type qwpSfCrashFileEvent struct {
	op      *qwpSfCrashFileOp
	commit  *qwpSfCrashFileOp
	barrier bool
}

func qwpSfCloneCrashImage(src map[string]qwpSfCrashNamespaceFile) map[string]qwpSfCrashNamespaceFile {
	dst := make(map[string]qwpSfCrashNamespaceFile, len(src))
	for name, file := range src {
		dst[name] = qwpSfCrashNamespaceFile{data: append([]byte(nil), file.data...), mode: file.mode}
	}
	return dst
}

func qwpSfApplyCrashFileOp(image map[string]qwpSfCrashNamespaceFile, op qwpSfCrashFileOp) bool {
	switch op.kind {
	case qwpSfCrashCreate:
		image[op.to] = qwpSfCrashNamespaceFile{data: append([]byte(nil), op.file.data...), mode: op.file.mode}
	case qwpSfCrashRemove:
		delete(image, op.from)
	case qwpSfCrashLink:
		file, ok := image[op.from]
		if !ok {
			return false
		}
		image[op.to] = qwpSfCrashNamespaceFile{data: append([]byte(nil), file.data...), mode: file.mode}
	case qwpSfCrashRename:
		file, ok := image[op.from]
		if !ok {
			return false
		}
		delete(image, op.from)
		image[op.to] = file
	case qwpSfCrashReplace:
		if _, ok := image[op.to]; !ok {
			return false
		}
		image[op.to] = qwpSfCrashNamespaceFile{data: append([]byte(nil), op.file.data...), mode: op.file.mode}
	default:
		panic("unknown crash file operation")
	}
	return true
}

func qwpSfMaterializeCrashImage(t *testing.T, image map[string]qwpSfCrashNamespaceFile) string {
	t.Helper()
	dir := t.TempDir()
	for name, file := range image {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), file.data, file.mode))
	}
	return dir
}

func qwpSfAssertCrashImageRecovers(t *testing.T, image map[string]qwpSfCrashNamespaceFile) {
	t.Helper()
	dir := qwpSfMaterializeCrashImage(t, image)
	ring, _, err := qwpSfRecoverRing(dir, 4096)
	require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.NoError(t, err)
	if ring != nil {
		require.NoError(t, ring.segmentRingClose())
	}
	require.NoFileExists(t, filepath.Join(dir, qwpSfFailedSentinelName))
}

func qwpSfVisitCrashFileOpOrders(ops []qwpSfCrashFileOp, visit func([]qwpSfCrashFileOp)) {
	for subset := 0; subset < 1<<len(ops); subset++ {
		selected := make([]qwpSfCrashFileOp, 0, len(ops))
		for i, op := range ops {
			if subset&(1<<i) != 0 {
				selected = append(selected, op)
			}
		}
		var permute func(int)
		permute = func(at int) {
			if at == len(selected) {
				visit(append([]qwpSfCrashFileOp(nil), selected...))
				return
			}
			for i := at; i < len(selected); i++ {
				selected[at], selected[i] = selected[i], selected[at]
				permute(at + 1)
				selected[at], selected[i] = selected[i], selected[at]
			}
		}
		permute(0)
	}
}

func qwpSfAssertCrashFileEpochs(
	t *testing.T,
	initial map[string]qwpSfCrashNamespaceFile,
	events []qwpSfCrashFileEvent,
) {
	t.Helper()
	committed := qwpSfCloneCrashImage(initial)
	epoch := make([]qwpSfCrashFileOp, 0, 4)
	caseNo := 0
	visitEpoch := func() {
		qwpSfVisitCrashFileOpOrders(epoch, func(order []qwpSfCrashFileOp) {
			image := qwpSfCloneCrashImage(committed)
			valid := true
			for _, op := range order {
				valid = valid && qwpSfApplyCrashFileOp(image, op)
			}
			if valid {
				t.Run(fmt.Sprintf("state-%03d", caseNo), func(t *testing.T) {
					qwpSfAssertCrashImageRecovers(t, image)
				})
			}
			caseNo++
		})
	}
	for _, event := range events {
		if event.op != nil {
			epoch = append(epoch, *event.op)
			visitEpoch()
			continue
		}
		if event.commit != nil {
			// A successful file fsync commits that file's new bytes, but it
			// does not commit pending directory entries. Keep namespace work in
			// the epoch so, without its directory barrier, a later manifest
			// commit can be tested with the dependent segment name absent.
			require.True(t, qwpSfApplyCrashFileOp(committed, *event.commit))
			visitEpoch()
			continue
		}
		require.True(t, event.barrier)
		for _, op := range epoch {
			require.True(t, qwpSfApplyCrashFileOp(committed, op))
		}
		epoch = epoch[:0]
		t.Run(fmt.Sprintf("state-%03d-after-barrier", caseNo), func(t *testing.T) {
			qwpSfAssertCrashImageRecovers(t, committed)
		})
		caseNo++
	}
	require.Empty(t, epoch)
}

type qwpSfCrashTraceRecorder struct {
	t       *testing.T
	dir     string
	durable map[string]qwpSfCrashNamespaceFile
	events  []qwpSfCrashFileEvent
}

func qwpSfNewCrashTraceRecorder(t *testing.T, dir string) *qwpSfCrashTraceRecorder {
	t.Helper()
	return &qwpSfCrashTraceRecorder{t: t, dir: dir, durable: qwpSfSnapshotCrashNamespace(t, dir)}
}

func (r *qwpSfCrashTraceRecorder) recordDirBarrier(dir string) error {
	r.t.Helper()
	require.Equal(r.t, r.dir, dir)
	current := qwpSfSnapshotCrashNamespace(r.t, r.dir)
	names := make([]string, 0, len(r.durable)+len(current))
	seen := make(map[string]struct{}, len(r.durable)+len(current))
	for name := range r.durable {
		if strings.HasSuffix(name, ".sfa") {
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	for name := range current {
		if !strings.HasSuffix(name, ".sfa") {
			continue
		}
		if _, ok := seen[name]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		before, hadBefore := r.durable[name]
		after, hasAfter := current[name]
		switch {
		case !hadBefore && hasAfter:
			op := qwpSfCrashFileOp{kind: qwpSfCrashCreate, to: name, file: after}
			r.events = append(r.events, qwpSfCrashFileEvent{op: &op})
			r.durable[name] = after
		case hadBefore && !hasAfter:
			op := qwpSfCrashFileOp{kind: qwpSfCrashRemove, from: name}
			r.events = append(r.events, qwpSfCrashFileEvent{op: &op})
			delete(r.durable, name)
		default:
			_ = before
		}
	}
	r.events = append(r.events, qwpSfCrashFileEvent{barrier: true})
	return nil
}

func (r *qwpSfCrashTraceRecorder) recordSegmentHeaderCommit(path string) {
	r.t.Helper()
	name := filepath.Base(path)
	// markManifestRequired flushes a brand-new spare before its name is
	// committed. The following directory barrier captures both. A later call
	// is rotation's rebase flush and therefore commits new bytes for an
	// already-durable name.
	if _, ok := r.durable[name]; !ok {
		return
	}
	current := qwpSfSnapshotCrashNamespace(r.t, r.dir)
	file, ok := current[name]
	require.True(r.t, ok)
	op := qwpSfCrashFileOp{kind: qwpSfCrashReplace, to: name, file: file}
	r.events = append(r.events, qwpSfCrashFileEvent{commit: &op})
	r.durable[name] = file
}

func (r *qwpSfCrashTraceRecorder) recordManifestCommit(f *os.File) error {
	r.t.Helper()
	if err := qwpSfFsync(f); err != nil {
		return err
	}
	name := filepath.Base(f.Name())
	current := qwpSfSnapshotCrashNamespace(r.t, r.dir)
	file, ok := current[name]
	require.True(r.t, ok)
	op := qwpSfCrashFileOp{kind: qwpSfCrashReplace, to: name, file: file}
	r.events = append(r.events, qwpSfCrashFileEvent{commit: &op})
	r.durable[name] = file
	return nil
}

func (r *qwpSfCrashTraceRecorder) install() func() {
	originalDirSync := qwpSfTestDirSyncHook.Load()
	originalHeaderSync := qwpSfTestSegmentSyncHeaderHook.Load()
	originalManifestSync := qwpSfManifestSync.Load()
	dirSync := r.recordDirBarrier
	headerSync := r.recordSegmentHeaderCommit
	manifestSync := r.recordManifestCommit
	qwpSfTestDirSyncHook.Store(&dirSync)
	qwpSfTestSegmentSyncHeaderHook.Store(&headerSync)
	qwpSfManifestSync.Store(&manifestSync)
	return func() {
		qwpSfTestDirSyncHook.Store(originalDirSync)
		qwpSfTestSegmentSyncHeaderHook.Store(originalHeaderSync)
		qwpSfManifestSync.Store(originalManifestSync)
	}
}

// This test drives the actual manager and producer paths while recording their
// namespace operations, file commits, and named directory barriers. The crash
// model then enumerates every subset and ordering the filesystem may persist
// inside each directory epoch.
func TestQwpSfSpareRotationAndTrimCrashEpochsRecover(t *testing.T) {
	const segmentSize int64 = 72
	dir := t.TempDir()
	active, err := qwpSfCreateSegment(filepath.Join(dir, "sf-initial.sfa"), 0, segmentSize)
	require.NoError(t, err)
	require.NoError(t, active.markManifestRequired())
	manifest, err := qwpSfManifestCreate(dir, 0, 0)
	require.NoError(t, err)
	ring := qwpSfNewSegmentRing(active, segmentSize)
	ring.manifest = manifest
	defer func() { require.NoError(t, ring.segmentRingClose()) }()

	payload := make([]byte, 16) // two frames exactly fill a 72-byte segment
	require.Equal(t, int64(0), ring.appendOrFsn(payload))
	require.Equal(t, int64(1), ring.appendOrFsn(payload))

	manager, err := qwpSfNewSegmentManager(segmentSize, time.Hour, qwpSfUnlimitedTotalBytes)
	require.NoError(t, err)
	entry, err := manager.segmentManagerRegisterWithWatermark(ring, dir, nil)
	require.NoError(t, err)
	defer manager.segmentManagerDeregister(ring)

	t.Run("spare-rotation", func(t *testing.T) {
		initial := qwpSfSnapshotCrashNamespace(t, dir)
		recorder := qwpSfNewCrashTraceRecorder(t, dir)
		restore := recorder.install()
		t.Cleanup(restore)
		manager.serviceRing(entry)
		require.NotNil(t, ring.hotSpare.Load())
		require.Equal(t, int64(2), ring.appendOrFsn(payload))
		restore()

		qwpSfAssertCrashFileEpochs(t, initial, recorder.events)
		require.Len(t, recorder.events, 4, "create, directory barrier, header commit, manifest commit")
		require.Equal(t, qwpSfCrashCreate, recorder.events[0].op.kind)
		require.True(t, recorder.events[1].barrier)
		require.Equal(t, qwpSfCrashReplace, recorder.events[2].commit.kind)
		require.NotEqual(t, qwpSfManifestFileName, recorder.events[2].commit.to)
		require.Equal(t, qwpSfManifestFileName, recorder.events[3].commit.to)
	})

	// Provision the next spare before the trim trace so the trim pass cannot
	// mix an unrelated create epoch into the operations under proof.
	manager.serviceRing(entry)
	require.NotNil(t, ring.hotSpare.Load())
	ring.acknowledge(1)

	t.Run("trim", func(t *testing.T) {
		initial := qwpSfSnapshotCrashNamespace(t, dir)
		recorder := qwpSfNewCrashTraceRecorder(t, dir)
		restore := recorder.install()
		t.Cleanup(restore)
		manager.serviceRing(entry)
		restore()

		qwpSfAssertCrashFileEpochs(t, initial, recorder.events)
		require.Len(t, recorder.events, 4, "pre-trim barrier, manifest commit, unlink, post-trim barrier")
		require.True(t, recorder.events[0].barrier)
		require.Equal(t, qwpSfManifestFileName, recorder.events[1].commit.to)
		require.Equal(t, qwpSfCrashRemove, recorder.events[2].op.kind)
		require.True(t, recorder.events[3].barrier)
	})
}

func TestQwpSfTornActiveReplacementCrashEpochsRecover(t *testing.T) {
	for _, tc := range []struct {
		name           string
		forceFallback  bool
		preserveOpKind qwpSfCrashFileOpKind
	}{
		{name: "hard-link", preserveOpKind: qwpSfCrashLink},
		{name: "rename-fallback", forceFallback: true, preserveOpKind: qwpSfCrashRename},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, path := tornActiveSlot(t, 7)
			initial := qwpSfSnapshotCrashNamespace(t, dir)
			var events []qwpSfCrashFileEvent
			recordedTemp := false

			recordTemp := func() {
				if recordedTemp {
					return
				}
				tmp := path + qwpSfTornActiveTempSuffix
				data, err := os.ReadFile(tmp)
				require.NoError(t, err)
				info, err := os.Stat(tmp)
				require.NoError(t, err)
				op := qwpSfCrashFileOp{kind: qwpSfCrashCreate, to: filepath.Base(tmp), file: qwpSfCrashNamespaceFile{data: data, mode: info.Mode().Perm()}}
				events = append(events, qwpSfCrashFileEvent{op: &op})
				recordedTemp = true
			}

			originalLink := qwpSfTornActiveLink.load()
			originalRename := qwpSfTornActiveRename.load()
			qwpSfTornActiveLink.store(func(from, to string) error {
				recordTemp()
				if tc.forceFallback {
					return os.ErrPermission
				}
				if err := originalLink(from, to); err != nil {
					return err
				}
				op := qwpSfCrashFileOp{kind: qwpSfCrashLink, from: filepath.Base(from), to: filepath.Base(to)}
				events = append(events, qwpSfCrashFileEvent{op: &op})
				return nil
			})
			qwpSfTornActiveRename.store(func(from, to string) error {
				if err := originalRename(from, to); err != nil {
					return err
				}
				op := qwpSfCrashFileOp{kind: qwpSfCrashRename, from: filepath.Base(from), to: filepath.Base(to)}
				events = append(events, qwpSfCrashFileEvent{op: &op})
				return nil
			})
			barrier := func(string) error {
				events = append(events, qwpSfCrashFileEvent{barrier: true})
				return nil
			}
			qwpSfTestDirSyncHook.Store(&barrier)
			restore := func() {
				qwpSfTornActiveLink.store(originalLink)
				qwpSfTornActiveRename.store(originalRename)
				qwpSfTestDirSyncHook.Store(nil)
			}
			t.Cleanup(restore)

			ring, _, err := qwpSfRecoverRing(dir, 4096)
			require.NoError(t, err)
			require.NotNil(t, ring)
			require.NoError(t, ring.segmentRingClose())
			restore()
			require.True(t, recordedTemp)
			require.Len(t, events, 6, "create, preserve, barrier, install, barrier, pre-exposure barrier")
			require.Equal(t, tc.preserveOpKind, events[1].op.kind)
			require.True(t, events[2].barrier)
			require.Equal(t, qwpSfCrashRename, events[3].op.kind)
			require.True(t, events[4].barrier)
			require.True(t, events[5].barrier)

			qwpSfAssertCrashFileEpochs(t, initial, events)
		})
	}
}

func TestQwpSfManifestQuarantineCrashEpochRecovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfManifestFileName)
	require.NoError(t, os.WriteFile(path, []byte("invalid"), 0o644))
	initial := qwpSfSnapshotCrashNamespace(t, dir)
	var events []qwpSfCrashFileEvent
	originalRename := qwpSfManifestQuarantineRename.load()
	qwpSfManifestQuarantineRename.store(func(from, to string) error {
		if err := originalRename(from, to); err != nil {
			return err
		}
		op := qwpSfCrashFileOp{kind: qwpSfCrashRename, from: filepath.Base(from), to: filepath.Base(to)}
		events = append(events, qwpSfCrashFileEvent{op: &op})
		return nil
	})
	barrier := func(string) error {
		events = append(events, qwpSfCrashFileEvent{barrier: true})
		return nil
	}
	qwpSfTestDirSyncHook.Store(&barrier)
	t.Cleanup(func() {
		qwpSfManifestQuarantineRename.store(originalRename)
		qwpSfTestDirSyncHook.Store(nil)
	})

	require.NoError(t, qwpSfQuarantineCreationDebris(path))
	qwpSfManifestQuarantineRename.store(originalRename)
	qwpSfTestDirSyncHook.Store(nil)
	require.Len(t, events, 2, "rename, barrier")
	require.True(t, events[1].barrier)
	qwpSfAssertCrashFileEpochs(t, initial, events)
}
