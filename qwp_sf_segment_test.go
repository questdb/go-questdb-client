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
	"encoding/binary"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfSegmentCreateRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-test.sfa")

	const segSize int64 = 4096
	seg, err := qwpSfCreateSegment(path, 100, segSize)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	assert.Equal(t, int64(100), seg.segmentBaseSeq())
	assert.Equal(t, int64(0), seg.segmentFrameCount())
	assert.Equal(t, qwpSfHeaderSize, seg.publishedOffset())
	assert.Equal(t, segSize, seg.segmentSize())
	assert.False(t, seg.isFull())
	assert.Equal(t, int64(0), seg.segmentTornTailBytes())

	// On-disk header must be readable and well-formed even before any
	// frames are appended.
	f, err := os.Open(path)
	require.NoError(t, err)
	hdr := make([]byte, qwpSfHeaderSize)
	_, err = f.Read(hdr)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	assert.Equal(t, qwpSfFileMagic, binary.LittleEndian.Uint32(hdr[0:4]))
	assert.Equal(t, qwpSfSegmentVersion, hdr[4])
	assert.Equal(t, byte(0), hdr[5])
	assert.Equal(t, uint16(0), binary.LittleEndian.Uint16(hdr[6:8]))
	assert.Equal(t, uint64(100), binary.LittleEndian.Uint64(hdr[8:16]))
}

func TestQwpSfSegmentTryAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-append.sfa")

	seg, err := qwpSfCreateSegment(path, 0, 4096)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	payload := []byte("hello qwp sf")
	off, err := seg.tryAppend(payload)
	require.NoError(t, err)
	assert.Equal(t, qwpSfHeaderSize, off)
	assert.Equal(t, int64(1), seg.segmentFrameCount())
	expectedPub := qwpSfHeaderSize + qwpSfFrameHeaderSize + int64(len(payload))
	assert.Equal(t, expectedPub, seg.publishedOffset())

	// Verify on-disk frame layout: [crc32c | u32 len | payload].
	buf := seg.address()
	storedLen := binary.LittleEndian.Uint32(buf[off+4 : off+8])
	assert.Equal(t, uint32(len(payload)), storedLen)
	storedCrc := binary.LittleEndian.Uint32(buf[off : off+4])

	expectedCrc := crc32.Update(0, qwpSfCrcTable, buf[off+4:off+8])
	expectedCrc = crc32.Update(expectedCrc, qwpSfCrcTable, payload)
	assert.Equal(t, expectedCrc, storedCrc)
	assert.Equal(t, payload, buf[off+qwpSfFrameHeaderSize:off+qwpSfFrameHeaderSize+int64(len(payload))])
}

func TestQwpSfSegmentTryAppendUntilFull(t *testing.T) {
	const segSize int64 = 256
	seg, err := qwpSfCreateInMemorySegment(0, segSize)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	payload := []byte("abcdefgh") // 8 bytes
	want := int64(0)
	for {
		_, err := seg.tryAppend(payload)
		if errors.Is(err, qwpSfErrSegmentFull) {
			break
		}
		require.NoError(t, err)
		want++
	}
	assert.Equal(t, want, seg.segmentFrameCount())
	assert.True(t, seg.isFull())
	// Subsequent attempts keep returning the sentinel without
	// corrupting state.
	_, err = seg.tryAppend(payload)
	assert.ErrorIs(t, err, qwpSfErrSegmentFull)
	assert.Equal(t, want, seg.segmentFrameCount())
}

func TestQwpSfSegmentInMemoryHasNoFile(t *testing.T) {
	seg, err := qwpSfCreateInMemorySegment(42, 4096)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	assert.True(t, seg.memoryBacked)
	assert.Equal(t, "", seg.segmentPath())
	assert.Nil(t, seg.file)
	// Header must still be readable from the malloc'd buffer.
	buf := seg.address()
	assert.Equal(t, qwpSfFileMagic, binary.LittleEndian.Uint32(buf[0:4]))
	assert.Equal(t, uint64(42), binary.LittleEndian.Uint64(buf[8:16]))
}

func TestQwpSfSegmentRebaseSeq(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-rebase.sfa")
	seg, err := qwpSfCreateSegment(path, 0, 4096)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	require.NoError(t, seg.rebaseSeq(7777))
	assert.Equal(t, int64(7777), seg.segmentBaseSeq())
	// Header on disk must reflect the rebase.
	buf := seg.address()
	assert.Equal(t, uint64(7777), binary.LittleEndian.Uint64(buf[8:16]))

	// Once a frame is appended, rebase must reject.
	_, err = seg.tryAppend([]byte{1, 2, 3})
	require.NoError(t, err)
	err = seg.rebaseSeq(9999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot rebase")
}

func TestQwpSfSegmentRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-recover.sfa")

	{
		seg, err := qwpSfCreateSegment(path, 50, 4096)
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			_, err := seg.tryAppend([]byte{byte(i), byte(i + 1), byte(i + 2)})
			require.NoError(t, err)
		}
		require.NoError(t, seg.close())
	}

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	assert.Equal(t, int64(50), seg.segmentBaseSeq())
	assert.Equal(t, int64(3), seg.segmentFrameCount())
	// publishedOffset should point past the third frame.
	expectedPub := qwpSfHeaderSize + 3*(qwpSfFrameHeaderSize+3)
	assert.Equal(t, expectedPub, seg.publishedOffset())
	assert.Equal(t, int64(0), seg.segmentTornTailBytes())
}

func TestQwpSfSegmentRecoveryRejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-badmagic.sfa")

	// Create a file with a wrong magic.
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0o644))
	seg, err := qwpSfOpenSegment(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad magic")
	// Bad content classifies as skippable for recovery.
	assert.ErrorIs(t, err, qwpSfErrSegmentCorrupt)
	assert.Nil(t, seg)
}

func TestQwpSfSegmentRecoveryRejectsBadVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-badver.sfa")

	{
		seg, err := qwpSfCreateSegment(path, 0, 4096)
		require.NoError(t, err)
		// Poke a bad version byte before close.
		seg.address()[4] = 99
		require.NoError(t, seg.close())
	}

	seg, err := qwpSfOpenSegment(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported segment version")
	assert.NotErrorIs(t, err, qwpSfErrSegmentCorrupt,
		"a newer client may have written intact frames; do not quarantine them as corruption")
	assert.ErrorIs(t, err, qwpSfErrRecoveryFailClosed,
		"no later attempt by this build can read the file, so the slot must be preserved whole rather than retried forever")
	assert.Nil(t, seg)
}

func TestQwpSfSegmentRecoveryHandlesTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-torntail.sfa")

	{
		seg, err := qwpSfCreateSegment(path, 0, 4096)
		require.NoError(t, err)
		_, err = seg.tryAppend([]byte("good frame"))
		require.NoError(t, err)
		// Simulate a torn write: corrupt the bytes immediately past
		// the last good frame so detectTornTail flags it. We write
		// non-zero garbage into what looks like a frame header.
		buf := seg.address()
		off := seg.publishedOffset()
		binary.LittleEndian.PutUint32(buf[off:off+4], 0xDEADBEEF)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], 0x1000) // claims a 4 KiB payload
		require.NoError(t, seg.close())
	}

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	assert.Equal(t, int64(1), seg.segmentFrameCount())
	assert.Greater(t, seg.segmentTornTailBytes(), int64(0))
	// publishedOffset must land at the start of the broken frame so
	// future appends overwrite it.
	expected := qwpSfHeaderSize + qwpSfFrameHeaderSize + int64(len("good frame"))
	assert.Equal(t, expected, seg.publishedOffset())
}

func writeTornSegment(t *testing.T, path string, size int64) {
	t.Helper()
	seg, err := qwpSfCreateSegment(path, 0, size)
	require.NoError(t, err)
	_, err = seg.tryAppend([]byte("good frame"))
	require.NoError(t, err)
	buf := seg.address()
	off := seg.publishedOffset()
	binary.LittleEndian.PutUint32(buf[off:off+4], 0xDEADBEEF)
	binary.LittleEndian.PutUint32(buf[off+4:off+8], 0x1000)
	require.NoError(t, seg.close())
}

// TestQwpSfFsyncReportsFailure pins that the platform fsync helper both works
// on a healthy file and reports a failure rather than returning nil. The darwin
// build calls the syscall directly, where an unchecked errno would silently
// turn every durability barrier in the slot into a no-op.
func TestQwpSfFsyncReportsFailure(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fsync")
	require.NoError(t, err)
	_, err = f.WriteString("durable")
	require.NoError(t, err)
	require.NoError(t, qwpSfFsync(f))

	require.NoError(t, f.Close())
	err = qwpSfFsync(f)
	require.Error(t, err, "a closed file must report, not silently succeed")
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr, "failures keep os.File.Sync's error shape")
	require.Equal(t, f.Name(), pathErr.Path)
}

func TestQwpSfSegmentMarkManifestRequiredSkipsRedundantFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-flag.sfa")
	seg, err := qwpSfCreateSegment(path, 0, 4096)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	var flushes atomic.Int64
	hook := func(string) { flushes.Add(1) }
	qwpSfTestSegmentSyncHeaderHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestSegmentSyncHeaderHook.Store(nil) })

	require.NoError(t, seg.markManifestRequired())
	require.True(t, seg.segmentManifestRequired())
	require.Equal(t, int64(1), flushes.Load(), "setting the flag must reach the disk")

	require.NoError(t, seg.markManifestRequired())
	require.Equal(t, int64(1), flushes.Load(),
		"a flag already on disk must not cost a second flush")
}

func TestQwpSfSegmentMarkManifestRequiredWritesDescriptorBeforeMapping(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*os.File, []byte, int64) (int, error)
		cause error
	}{
		{
			name: "enospc",
			write: func(*os.File, []byte, int64) (int, error) {
				return 0, syscall.ENOSPC
			},
			cause: syscall.ENOSPC,
		},
		{
			name: "short-write",
			write: func(*os.File, []byte, int64) (int, error) {
				return 0, nil
			},
			cause: io.ErrShortWrite,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sf-flag.sfa")
			seg, err := qwpSfCreateSegment(path, 0, 4096)
			require.NoError(t, err)
			defer func() { _ = seg.close() }()
			mappedBefore := append([]byte(nil), seg.buf[:qwpSfHeaderSize]...)
			diskBefore, err := os.ReadFile(path)
			require.NoError(t, err)

			original := qwpSfSegmentWriteAt.load()
			called := false
			qwpSfSegmentWriteAt.store(func(f *os.File, p []byte, off int64) (int, error) {
				called = true
				require.Equal(t, int64(5), off)
				require.Equal(t, []byte{qwpSfManifestRequiredFlag}, p)
				return tc.write(f, p, off)
			})
			t.Cleanup(func() { qwpSfSegmentWriteAt.store(original) })

			err = seg.markManifestRequired()
			require.ErrorIs(t, err, tc.cause)
			require.ErrorIs(t, err, ErrSfDurability)
			require.True(t, called, "the recovery metadata mutation must go through the descriptor seam")
			assert.Equal(t, mappedBefore, seg.buf[:qwpSfHeaderSize],
				"a failed descriptor write must not mutate the mapped page")
			diskAfter, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			assert.Equal(t, diskBefore, diskAfter, "the injected failure must preserve the on-disk bytes")
		})
	}
}

func TestQwpSfSegmentMarkManifestRequiredRetriesAfterSyncFailureAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sf-flag.sfa")
	seg, err := qwpSfCreateSegment(path, 0, 4096)
	require.NoError(t, err)

	injected := errors.New("injected header sync failure")
	failSync := func(string) error { return injected }
	qwpSfTestSegmentSyncHeaderErrorHook.Store(&failSync)
	t.Cleanup(func() { qwpSfTestSegmentSyncHeaderErrorHook.Store(nil) })

	err = seg.markManifestRequired()
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, err, ErrSfDurability)
	require.False(t, seg.segmentManifestRequired(),
		"failed durability must restore the backed shared mapping so any reopen retries")
	require.NoError(t, seg.close())

	seg, err = qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()
	require.False(t, seg.segmentManifestRequired(),
		"the same-host page-cache view must not preserve an unsynced success marker")

	var syncAttempts atomic.Int64
	countSync := func(string) { syncAttempts.Add(1) }
	qwpSfTestSegmentSyncHeaderHook.Store(&countSync)
	t.Cleanup(func() { qwpSfTestSegmentSyncHeaderHook.Store(nil) })
	qwpSfTestSegmentSyncHeaderErrorHook.Store(nil)

	require.NoError(t, seg.markManifestRequired())
	require.Equal(t, int64(1), syncAttempts.Load(),
		"a reopened segment must retry the descriptor write and durability barrier")
}

func TestQwpSfSegmentMarkManifestRequiredResyncsFlagAfterProcessDeath(t *testing.T) {
	const helperEnv = "QWP_SF_TEST_DIE_AFTER_MANIFEST_FLAG_WRITE"
	if path := os.Getenv(helperEnv); path != "" {
		seg, err := qwpSfOpenSegment(path)
		if err != nil {
			os.Exit(70)
		}
		dieBeforeSync := func(string) error {
			os.Exit(71)
			return nil
		}
		qwpSfTestSegmentSyncHeaderErrorHook.Store(&dieBeforeSync)
		_ = seg.markManifestRequired()
		os.Exit(72)
	}

	path := filepath.Join(t.TempDir(), "sf-flag-process-death.sfa")
	seg, err := qwpSfCreateSegment(path, 0, 4096)
	require.NoError(t, err)
	require.NoError(t, seg.close())

	cmd := exec.Command(os.Args[0], "-test.run=^TestQwpSfSegmentMarkManifestRequiredResyncsFlagAfterProcessDeath$")
	cmd.Env = append(os.Environ(), helperEnv+"="+path)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "helper unexpectedly succeeded: %s", output)
	require.Equal(t, 71, exitErr.ExitCode(), "helper missed the post-write/pre-sync crash point: %s", output)

	seg, err = qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()
	require.True(t, seg.segmentManifestRequired(),
		"the next process must exercise the page-cache-visible unsynced flag")
	var syncs atomic.Int64
	countSync := func(string) { syncs.Add(1) }
	qwpSfTestSegmentSyncHeaderHook.Store(&countSync)
	t.Cleanup(func() { qwpSfTestSegmentSyncHeaderHook.Store(nil) })

	require.NoError(t, seg.markManifestRequired())
	require.Equal(t, int64(1), syncs.Load(),
		"a reopened flagged header must be resynced before recovery trusts it")
}

func TestQwpSfRecoverySegmentBufferMutationsAreCentralized(t *testing.T) {
	for _, path := range []string{
		"qwp_sf_recovery.go",
		"qwp_sf_recovered_dict.go",
		"qwp_sf_segment.go",
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		for _, violation := range qwpSfTestRecoveryBufferMutationViolations(fset, file, path) {
			t.Errorf("%s:%d: %s mutates a segment mmap buffer directly; use writeRecoveryAt",
				violation.path, violation.line, violation.function)
		}
	}
}

func TestQwpSfRecoverySegmentBufferMutationGuardDetectsAliases(t *testing.T) {
	const source = `package questdb
func bad(segment *qwpSfSegment) {
	buf := segment.address()
	clear(buf[1:])
	buf[0] = 1
}
func writeHeader(segment *qwpSfSegment) {
	segment.buf[4] = 1
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "guard_fixture.go", source, 0)
	require.NoError(t, err)
	violations := qwpSfTestRecoveryBufferMutationViolations(fset, file, "guard_fixture.go")
	require.Len(t, violations, 3,
		"aliases and a same-named non-method must not bypass the approved receiver methods")
}

type qwpSfTestBufferMutationViolation struct {
	path     string
	function string
	line     int
}

func qwpSfTestRecoveryBufferMutationViolations(
	fset *token.FileSet,
	file *ast.File,
	path string,
) []qwpSfTestBufferMutationViolation {
	var violations []qwpSfTestBufferMutationViolation
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || qwpSfTestApprovedSegmentMutationMethod(path, fn) {
			continue
		}
		aliases := make(map[string]bool)
		record := func(node ast.Node) {
			violations = append(violations, qwpSfTestBufferMutationViolation{
				path: path, function: fn.Name.Name, line: fset.Position(node.Pos()).Line,
			})
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if qwpSfTestWritesIntoBuf(lhs, aliases) {
						record(lhs)
					}
				}
				for i, rhs := range n.Rhs {
					if !qwpSfTestExprAliasesSegmentBuf(rhs, aliases) {
						continue
					}
					if i < len(n.Lhs) {
						if ident, ok := n.Lhs[i].(*ast.Ident); ok {
							aliases[ident.Name] = true
						}
					}
				}
			case *ast.ValueSpec:
				for i, value := range n.Values {
					if i < len(n.Names) && qwpSfTestExprAliasesSegmentBuf(value, aliases) {
						aliases[n.Names[i].Name] = true
					}
				}
			case *ast.CallExpr:
				if len(n.Args) == 0 || !qwpSfTestExprAliasesSegmentBuf(n.Args[0], aliases) {
					break
				}
				mutates := false
				switch called := n.Fun.(type) {
				case *ast.Ident:
					mutates = called.Name == "copy" || called.Name == "clear"
				case *ast.SelectorExpr:
					mutates = strings.HasPrefix(called.Sel.Name, "Put")
				}
				if mutates {
					record(n)
				}
			}
			return true
		})
	}
	return violations
}

func qwpSfTestApprovedSegmentMutationMethod(path string, fn *ast.FuncDecl) bool {
	if filepath.Base(path) != "qwp_sf_segment.go" || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	receiver := fn.Recv.List[0].Type
	if pointer, ok := receiver.(*ast.StarExpr); ok {
		receiver = pointer.X
	}
	name, ok := receiver.(*ast.Ident)
	if !ok || name.Name != "qwpSfSegment" {
		return false
	}
	switch fn.Name.Name {
	case "writeHeader", "rebaseSeq", "tryAppend", "writeRecoveryAt", "restoreRecoveryMappingAfterFailedSync":
		return true
	default:
		return false
	}
}

func qwpSfTestWritesIntoBuf(expr ast.Expr, aliases map[string]bool) bool {
	switch expr.(type) {
	case *ast.IndexExpr, *ast.SliceExpr:
		return qwpSfTestExprAliasesSegmentBuf(expr, aliases)
	default:
		return false
	}
}

func qwpSfTestExprAliasesSegmentBuf(expr ast.Expr, aliases map[string]bool) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.Ident:
			if aliases[n.Name] {
				found = true
				return false
			}
		case *ast.SelectorExpr:
			if n.Sel.Name == "buf" {
				found = true
				return false
			}
		case *ast.CallExpr:
			if selector, ok := n.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "address" {
				found = true
				return false
			}
		}
		if found {
			found = true
			return false
		}
		return !found
	})
	return found
}

func TestQwpSfSegmentSanitizeTornTailReservesBlocksThroughDescriptor(t *testing.T) {
	// The tail may sit over a hole on a filesystem where qwpSfAllocate
	// took its sparse fallback. Zeroing it through the descriptor
	// allocates the blocks and turns a full disk into an ENOSPC error;
	// a store through the mapping would raise SIGBUS instead, which no
	// recover() can catch.
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-torntail-reserve.sfa")
	const segSize int64 = 4096
	writeTornSegment(t, path, segSize)

	original := qwpSfSegmentWriteAt.load()
	var covered []int64
	qwpSfSegmentWriteAt.store(func(f *os.File, p []byte, off int64) (int, error) {
		covered = append(covered, off, off+int64(len(p)))
		return original(f, p, off)
	})
	t.Cleanup(func() { qwpSfSegmentWriteAt.store(original) })

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()
	cursor := seg.publishedOffset()
	require.Greater(t, seg.segmentTornTailBytes(), int64(0))

	require.NoError(t, seg.sanitizeTornTail())

	require.Equal(t, []int64{cursor, segSize, cursor, cursor + 1}, covered,
		"the descriptor writes must cover the appendable tail before clearing its retry marker")
	assert.Zero(t, seg.segmentTornTailBytes())
	onDisk, err := os.ReadFile(path)
	require.NoError(t, err)
	for i := cursor; i < segSize; i++ {
		require.Zero(t, onDisk[i], "byte %d of the sanitized tail is not zero", i)
	}
}

func TestQwpSfSegmentSanitizeTornTailSurfacesFullDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-torntail-enospc.sfa")
	writeTornSegment(t, path, 4096)

	original := qwpSfSegmentWriteAt.load()
	qwpSfSegmentWriteAt.store(func(*os.File, []byte, int64) (int, error) {
		return 0, syscall.ENOSPC
	})
	t.Cleanup(func() { qwpSfSegmentWriteAt.store(original) })

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	err = seg.sanitizeTornTail()
	require.ErrorIs(t, err, syscall.ENOSPC)
	assert.Greater(t, seg.segmentTornTailBytes(), int64(0),
		"a tail that could not be zeroed stays flagged so the next recovery retries it")
}

func TestQwpSfSegmentSanitizeTornTailPreservesRetryEvidenceAfterPartialWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-torntail-partial.sfa")
	const segSize = qwpSfHeaderSize + 3*qwpSfZeroFillChunk
	writeTornSegment(t, path, segSize)

	original := qwpSfSegmentWriteAt.load()
	tailWrites := 0
	qwpSfSegmentWriteAt.store(func(f *os.File, p []byte, off int64) (int, error) {
		tailWrites++
		if tailWrites == 2 {
			return 0, syscall.ENOSPC
		}
		return original(f, p, off)
	})
	t.Cleanup(func() { qwpSfSegmentWriteAt.store(original) })

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	require.Greater(t, seg.segmentTornTailBytes(), int64(qwpSfZeroFillChunk))
	err = seg.sanitizeTornTail()
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.GreaterOrEqual(t, tailWrites, 2, "fault must occur after an earlier chunk succeeded")
	require.NoError(t, seg.close())
	qwpSfSegmentWriteAt.store(original)

	seg, err = qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()
	require.Greater(t, seg.segmentTornTailBytes(), int64(0),
		"reopen must retain a durable sanitation-pending signal after partial zeroing")
	require.NoError(t, seg.sanitizeTornTail(), "the preserved signal must make sanitation retryable")
	require.Zero(t, seg.segmentTornTailBytes())
}

func TestQwpSfSegmentSanitizeTornTailRestoresMarkerAfterFinalSyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sf-torntail-final-sync.sfa")
	writeTornSegment(t, path, 4096)

	injected := errors.New("injected final sanitation sync failure")
	failFinal := func(_ string, phase string) error {
		if phase == "cleared-marker" {
			return injected
		}
		return nil
	}
	qwpSfTestSegmentSanitizeTailSyncErrorHook.Store(&failFinal)
	t.Cleanup(func() { qwpSfTestSegmentSanitizeTailSyncErrorHook.Store(nil) })

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	err = seg.sanitizeTornTail()
	require.ErrorIs(t, err, injected)
	require.Greater(t, seg.segmentTornTailBytes(), int64(0))
	require.NotZero(t, seg.buf[seg.appendCursor],
		"a failed final barrier must restore the shared retry marker")
	require.NoError(t, seg.close())
	qwpSfTestSegmentSanitizeTailSyncErrorHook.Store(nil)

	seg, err = qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()
	require.Greater(t, seg.segmentTornTailBytes(), int64(0),
		"reopen must retry after the failed marker-clear barrier")
	require.NoError(t, seg.sanitizeTornTail())
	require.Zero(t, seg.segmentTornTailBytes())
}

func TestQwpSfSegmentRecoveryHandlesCleanPartialFill(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-clean.sfa")

	{
		seg, err := qwpSfCreateSegment(path, 0, 4096)
		require.NoError(t, err)
		_, err = seg.tryAppend([]byte("partial fill"))
		require.NoError(t, err)
		require.NoError(t, seg.close())
	}

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	// Trailing zero bytes are NOT a torn tail.
	assert.Equal(t, int64(0), seg.segmentTornTailBytes())
}

func TestQwpSfSegmentRecoveryRejectsNegativeBaseSeq(t *testing.T) {
	// FSNs are non-negative by construction. A negative baseSeq on disk
	// means bit-rot or a hand-edited file; recovery must refuse it
	// rather than feeding the bad value into the unsigned-comparison
	// sort and contiguity check, which would place the segment last
	// and trip the FSN-gap error.
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-badbase.sfa")
	{
		seg, err := qwpSfCreateSegment(path, 0, 4096)
		require.NoError(t, err)
		require.NoError(t, seg.close())
	}
	// Rewrite the on-disk baseSeq field at offset 8 to a negative
	// value (sign bit set).
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var bad [8]byte
	binary.LittleEndian.PutUint64(bad[:], 0xFFFFFFFFFFFFFFFF) // int64(-1)
	_, err = f.WriteAt(bad[:], 8)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	seg, err := qwpSfOpenSegment(path)
	require.Error(t, err)
	assert.Nil(t, seg)
	assert.Contains(t, err.Error(), "bad baseSeq")
	assert.ErrorIs(t, err, qwpSfErrSegmentCorrupt)
}

func TestQwpSfSegmentRecoveryRejectsOversizedLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-bad.sfa")

	{
		seg, err := qwpSfCreateSegment(path, 0, 256)
		require.NoError(t, err)
		// Write a frame that claims a payload larger than the file.
		buf := seg.address()
		binary.LittleEndian.PutUint32(buf[qwpSfHeaderSize:qwpSfHeaderSize+4], 0xAAAAAAAA)
		binary.LittleEndian.PutUint32(buf[qwpSfHeaderSize+4:qwpSfHeaderSize+8], 0xFFFFFFFF)
		require.NoError(t, seg.close())
	}

	seg, err := qwpSfOpenSegment(path)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	// Corrupt frame is treated as a torn tail; recovery stops at the
	// header position, so frameCount is 0 and lastGood == HEADER_SIZE.
	assert.Equal(t, int64(0), seg.segmentFrameCount())
	assert.Equal(t, qwpSfHeaderSize, seg.publishedOffset())
}

func TestQwpSfSegmentMsync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sf-msync.sfa")
	seg, err := qwpSfCreateSegment(path, 0, 4096)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	_, err = seg.tryAppend([]byte("durable"))
	require.NoError(t, err)
	require.NoError(t, seg.msync())
}

func TestQwpSfSegmentMsyncMemoryBackedIsNoop(t *testing.T) {
	seg, err := qwpSfCreateInMemorySegment(0, 4096)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	_, err = seg.tryAppend([]byte("ram"))
	require.NoError(t, err)
	require.NoError(t, seg.msync())
}

func TestQwpSfSegmentTooSmallSize(t *testing.T) {
	_, err := qwpSfCreateInMemorySegment(0, qwpSfHeaderSize)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too small")
}

func TestQwpSfFlockExclusive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".lock")

	f1, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer func() { _ = f1.Close() }()
	require.NoError(t, qwpSfFlockExclusive(f1))

	f2, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer func() { _ = f2.Close() }()
	err = qwpSfFlockExclusive(f2)
	assert.ErrorIs(t, err, qwpSfErrLockBusy)

	require.NoError(t, f1.Close())
	// Re-acquire on f2 now that f1 has released.
	require.NoError(t, qwpSfFlockExclusive(f2))
}

// TestQwpSfSegmentGoldenFileJavaConformance is the Java<->Go .sfa
// golden-file conformance guard for CLAUDE.md's on-disk compatibility
// claim: a segment file written by either client must be byte-readable
// by the other. The "golden" is a canonical .sfa image laid out by hand
// from the format documented on the Java MmapSegment.java (FILE_MAGIC,
// HEADER_SIZE, FRAME_HEADER_SIZE, VERSION, baseSeq, CRC32C over
// (payloadLen, payload)) — built independently of the production
// qwpSfSegment codec so it pins all three directions of drift:
//
//  1. The format constants still equal the Java MmapSegment literals.
//  2. The Go reader (qwpSfOpenSegment) recovers a hand-built image.
//  3. The Go writer (qwpSfCreateSegment + tryAppend) reproduces the
//     image byte-for-byte, except the non-deterministic createdMicros
//     header field.
//
// CRC32C (Castagnoli) is a standardised checksum, so the in-test stdlib
// crc32 and the Java client's Crc32c necessarily agree on the same
// bytes; the conformance therefore rests on the byte layout, which this
// test pins explicitly. A switch to a different polynomial or endianness
// on either side trips the reader or writer sub-test.
func TestQwpSfSegmentGoldenFileJavaConformance(t *testing.T) {
	// 1. Format constants must equal the Java MmapSegment.java literals.
	assert.Equal(t, uint32(0x31304653), qwpSfFileMagic, "'SF01' little-endian")
	assert.Equal(t, int64(24), qwpSfHeaderSize)
	assert.Equal(t, int64(8), qwpSfFrameHeaderSize)
	assert.Equal(t, byte(1), qwpSfSegmentVersion)

	// Canonical input: a non-zero baseSeq and two frames of differing
	// length so a length-handling drift is visible.
	const goldenBaseSeq = int64(7)
	// A fixed createdMicros keeps the golden image deterministic; the
	// production writer stamps time.Now(), checked separately below.
	const goldenCreatedMicros = int64(1_700_000_000_000_000)
	goldenFrames := [][]byte{[]byte("hello"), []byte("QWP!")}

	crcTable := crc32.MakeTable(crc32.Castagnoli)

	// Build the golden .sfa image by hand from the documented layout.
	golden := make([]byte, qwpSfHeaderSize)
	binary.LittleEndian.PutUint32(golden[0:4], 0x31304653) // magic 'SF01'
	golden[4] = 1                                          // version
	golden[5] = 0                                          // flags
	binary.LittleEndian.PutUint16(golden[6:8], 0)          // reserved
	binary.LittleEndian.PutUint64(golden[8:16], uint64(goldenBaseSeq))
	binary.LittleEndian.PutUint64(golden[16:24], uint64(goldenCreatedMicros))
	for _, p := range goldenFrames {
		frame := make([]byte, qwpSfFrameHeaderSize+int64(len(p)))
		binary.LittleEndian.PutUint32(frame[4:8], uint32(len(p)))
		copy(frame[8:], p)
		// CRC32C covers (payloadLen, payload) — frame[4:] here.
		crc := crc32.Update(0, crcTable, frame[4:])
		binary.LittleEndian.PutUint32(frame[0:4], crc)
		golden = append(golden, frame...)
	}

	// 2. Reader: a hand-built (cross-impl) image must be recovered intact.
	t.Run("Go reader accepts the golden image", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "golden.sfa")
		require.NoError(t, os.WriteFile(path, golden, 0o644))

		seg, err := qwpSfOpenSegment(path)
		require.NoError(t, err)
		defer func() { _ = seg.close() }()

		assert.Equal(t, goldenBaseSeq, seg.segmentBaseSeq())
		assert.Equal(t, int64(len(goldenFrames)), seg.segmentFrameCount())
		assert.Equal(t, int64(0), seg.segmentTornTailBytes(),
			"a clean golden image must report no torn tail")
		assert.Equal(t, int64(len(golden)), seg.publishedOffset(),
			"recovery must position the cursor just past the last valid frame")

		// Walk the frames back out of the mapping and confirm payloads.
		buf := seg.address()
		off := qwpSfHeaderSize
		for i, p := range goldenFrames {
			payloadLen := int64(binary.LittleEndian.Uint32(buf[off+4 : off+8]))
			require.Equalf(t, int64(len(p)), payloadLen, "frame %d payloadLen", i)
			got := buf[off+qwpSfFrameHeaderSize : off+qwpSfFrameHeaderSize+payloadLen]
			assert.Equalf(t, p, got, "frame %d payload", i)
			off += qwpSfFrameHeaderSize + payloadLen
		}
	})

	// 3. Writer: the production writer must reproduce the golden image,
	//    modulo the non-deterministic createdMicros header field.
	t.Run("Go writer reproduces the golden image", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "written.sfa")
		const segSize int64 = 4096

		seg, err := qwpSfCreateSegment(path, goldenBaseSeq, segSize)
		require.NoError(t, err)
		for _, p := range goldenFrames {
			_, err := seg.tryAppend(p)
			require.NoError(t, err)
		}
		require.NoError(t, seg.close())

		written, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, int(segSize), len(written),
			"create pre-allocates the full segment size")

		// Header: everything except createdMicros[16:24] is deterministic.
		assert.Equal(t, golden[0:16], written[0:16],
			"magic/version/flags/reserved/baseSeq must match the golden header")
		gotMicros := int64(binary.LittleEndian.Uint64(written[16:24]))
		assert.Greaterf(t, gotMicros, int64(1_600_000_000_000_000),
			"createdMicros must be a plausible recent timestamp, got %d", gotMicros)

		// Frames must be byte-identical to the golden image (CRC + len +
		// payload). This is what a Java reader would parse.
		assert.Equal(t, golden[qwpSfHeaderSize:], written[qwpSfHeaderSize:len(golden)],
			"frame bytes (crc + len + payload) must match the golden image")

		// The pre-allocated tail past the last frame is zero-filled.
		tail := written[len(golden):]
		assert.Equal(t, make([]byte, len(tail)), tail,
			"the reserved tail beyond the written frames must be zero-filled")
	})
}
