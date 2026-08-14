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
	"fmt"
)

// qwpSfRecoveredDictAnalysis is the engine-construction-time fold of every
// surviving SF frame. symbols contains the persisted dictionary prefix followed
// by the contiguous suffix the frames themselves still define. Both the
// producer and the send loop seed from this exact slice so they cannot assign
// different strings to the same recovered id.
type qwpSfRecoveredDictAnalysis struct {
	symbols             []string
	maxReplayDeltaStart int
}

// qwpSfAnalyzeRecoveredDict reconstructs as much of a recovered slot's global
// symbol dictionary as its surviving frames prove. The segment CRC scan has
// already validated every payload; this pass validates the QWP delta structure
// and folds only the previously-uncovered tail of each range.
//
// A gap in a frame that will replay is unrecoverable: ids below deltaStart were
// introduced by frames that have already been trimmed and no longer exist in
// either the persisted prefix or the surviving log. A gap confined to already-
// ACKed frames is harmless when no later replay frame depends on it; a later
// self-sufficient delta starting at zero begins a new provable epoch.
func qwpSfAnalyzeRecoveredDict(
	ring *qwpSfSegmentRing,
	ackedFsn int64,
	prefix []string,
) (qwpSfRecoveredDictAnalysis, error) {
	baseline := len(prefix)
	a := qwpSfRecoveredDictAnalysis{
		symbols: append([]string(nil), prefix...),
	}
	coverage := baseline
	gap := false
	gapAffectsReplay := false

	err := qwpSfWalkRecoveredFrames(ring, func(fsn int64, payload []byte) error {
		if !qwpIsDeltaFrame(payload) {
			return nil
		}
		deltaStart, deltaCount, entries, ok := qwpParseDeltaDict(payload)
		if !ok {
			gap = true
			if fsn > ackedFsn {
				gapAffectsReplay = true
			}
			return nil
		}
		if fsn > ackedFsn && deltaStart > a.maxReplayDeltaStart {
			a.maxReplayDeltaStart = deltaStart
		}

		if gap {
			// Only a self-sufficient frame can reset an already-ACKed gap.
			// An unacked gapped frame still reaches the fresh server first, so
			// a later reset cannot make that replay order safe.
			if deltaStart != 0 || gapAffectsReplay {
				if fsn > ackedFsn {
					gapAffectsReplay = true
				}
				return nil
			}
			gap = false
			coverage = baseline
			a.symbols = a.symbols[:baseline]
		}

		deltaEnd := deltaStart + deltaCount // qwpParseDeltaDict overflow-checks.
		if deltaStart > coverage {
			gap = true
			if fsn > ackedFsn {
				gapAffectsReplay = true
			}
			return nil
		}
		if deltaEnd <= coverage {
			return nil
		}

		// Skip the overlap already supplied by the side-file or an earlier
		// frame, then append the one contiguous unseen tail. The parser has
		// validated the whole entry region, but keep the local checks so this
		// fold remains fail-closed if that helper's contract changes.
		p := entries
		for skip := coverage - deltaStart; skip > 0; skip-- {
			entryLen, n, err := qwpReadVarint(p)
			if err != nil || entryLen > uint64(len(p)-n) {
				return fmt.Errorf("qwp/sf: malformed recovered symbol dictionary overlap at fsn %d", fsn)
			}
			p = p[n+int(entryLen):]
		}
		for id := coverage; id < deltaEnd; id++ {
			entryLen, n, err := qwpReadVarint(p)
			if err != nil || entryLen > uint64(len(p)-n) {
				return fmt.Errorf("qwp/sf: malformed recovered symbol dictionary suffix at fsn %d", fsn)
			}
			p = p[n:]
			a.symbols = append(a.symbols, string(p[:int(entryLen)]))
			p = p[int(entryLen):]
		}
		coverage = deltaEnd
		return nil
	})
	if err != nil {
		return qwpSfRecoveredDictAnalysis{}, err
	}
	if gapAffectsReplay {
		return qwpSfRecoveredDictAnalysis{}, fmt.Errorf(
			"qwp/sf: recovered symbol dictionary is incomplete: surviving unacked frames reference ids below their delta start; resend required")
	}
	return a, nil
}

// qwpSfWalkRecoveredFrames visits every CRC-validated frame in FSN order. It
// runs before the ring is registered with the manager or exposed to a producer,
// so direct access to the sealed list and active segment is race-free.
func qwpSfWalkRecoveredFrames(ring *qwpSfSegmentRing, visit func(fsn int64, payload []byte) error) error {
	if ring == nil {
		return nil
	}
	segments := make([]*qwpSfSegment, 0, len(ring.getSealedSegments())+1)
	segments = append(segments, ring.getSealedSegments()...)
	if active := ring.getActiveSegment(); active != nil {
		segments = append(segments, active)
	}
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		buf := segment.address()
		limit := segment.publishedOffset()
		pos := qwpSfHeaderSize
		for frame := int64(0); frame < segment.segmentFrameCount(); frame++ {
			if pos+qwpSfFrameHeaderSize > limit {
				return fmt.Errorf("qwp/sf: recovered frame envelope is truncated [baseSeq=%d, frame=%d]",
					segment.segmentBaseSeq(), frame)
			}
			payloadLen := int64(binary.LittleEndian.Uint32(buf[pos+4 : pos+8]))
			payloadStart := pos + qwpSfFrameHeaderSize
			payloadEnd := payloadStart + payloadLen
			if payloadEnd < payloadStart || payloadEnd > limit {
				return fmt.Errorf("qwp/sf: recovered frame payload is truncated [baseSeq=%d, frame=%d, payloadLen=%d]",
					segment.segmentBaseSeq(), frame, payloadLen)
			}
			if err := visit(segment.segmentBaseSeq()+frame, buf[payloadStart:payloadEnd]); err != nil {
				return err
			}
			pos = payloadEnd
		}
	}
	return nil
}
