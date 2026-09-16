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
	"path/filepath"
	"time"
)

func (s *qwpSfSwappableVar[T]) store(value T) { s.v.Store(&value) }

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

func qwpSfAckWatermarkOpenRequired(slotDir string) (*qwpSfAckWatermark, error) {
	return qwpSfAckWatermarkOpenRequiredWithLogger(slotDir, nil)
}
