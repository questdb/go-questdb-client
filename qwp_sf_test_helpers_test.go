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
	"context"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These are failure guards, not latency assertions. Disk-backed setup can need
// several fsyncs and a scheduled manager pass even for tiny test segments.
// Keep intentional backpressure, cancellation and retry intervals local to the
// tests that exercise them rather than replacing them with these budgets.
const (
	qwpTestWaitTimeout   = 10 * time.Second
	qwpTestAppendTimeout = 30 * time.Second
)

// qwpTestSubprocess gives multi-step recovery/ownership fixtures room for slow
// storage and race instrumentation. The child's watchdog dumps stacks before
// the parent's hard stop; neither timeout is a cleanup correctness assertion.
func qwpTestSubprocess(t *testing.T, testName string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	return exec.CommandContext(ctx, exe, "-test.run=^"+regexp.QuoteMeta(testName)+"$", "-test.v", "-test.timeout=2m")
}

func (s *qwpSfSwappableVar[T]) store(value T) { s.v.Store(&value) }

// createRecoverySegment writes a segment fixture. The Windows cleanup build
// compiles qwp_sf_cleanup_release_test.go without the recovery suite, so
// these fixtures live with the shared helpers.
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

func qwpSfNewCursorEngineForDrainer(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	return qwpSfNewCursorEngineWithOptions(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadline, qwpSfEngineOpenOptions{})
}

func qwpSfOpenRing(sfDir string, maxBytesPerSegment int64) (*qwpSfSegmentRing, error) {
	ring, _, err := qwpSfRecoverRing(sfDir, maxBytesPerSegment)
	return ring, err
}

func qwpSfRecoverRing(sfDir string, maxBytesPerSegment int64) (*qwpSfSegmentRing, *qwpSfManifest, error) {
	return qwpSfRecoverRingWithContext(sfDir, maxBytesPerSegment, qwpSfRecoveryContext{})
}

// qwpSfManifestOpen combines inspection and quarantine for manifest tests.
// Production recovery plans the quarantine before changing the directory.
func qwpSfManifestOpen(dir string) (*qwpSfManifest, error) {
	manifest, invalid, err := qwpSfManifestInspect(dir)
	if err != nil || !invalid {
		return manifest, err
	}
	if err := qwpSfQuarantineCreationDebris(filepath.Join(dir, qwpSfManifestFileName)); err != nil {
		return nil, err
	}
	return nil, nil
}

func qwpSfAckWatermarkOpen(slotDir string) *qwpSfAckWatermark {
	w, _ := qwpSfAckWatermarkOpenRequired(slotDir)
	return w
}

// qwpSfAckWatermarkOpenRequired exercises the file layer with the
// preserve-anything startup policy: no recovered history is supplied, so every
// record the existing format accepts is kept. Startup-decision coverage lives
// in qwp_sf_ack_watermark_startup_test.go, which passes real histories.
func qwpSfAckWatermarkOpenRequired(slotDir string) (*qwpSfAckWatermark, error) {
	return qwpSfAckWatermarkOpenPrepared(slotDir, qwpSfAckWatermarkStartup{publishedFsn: math.MaxInt64}, nil)
}
