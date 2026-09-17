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
)

// qwpSfRecoveredDictAnalysis is what the engine constructor learns by reading
// every surviving SF frame. symbols holds the side-file's trusted entries
// followed by the further ids the frames themselves spell out. The producer and
// the send loop both start from this one slice, so a recovered id cannot end up
// with two different names.
type qwpSfRecoveredDictAnalysis struct {
	symbols             []string
	maxReplayDeltaStart int
}

// qwpSfAnalyzeRecoveredDict rebuilds the symbol dictionary from the saved
// dictionary file and the frames that are still on disk. It checks that every
// repeated id has the same name, then adds any new ids found in the frames.
//
// If two sources give the same id different names, recovery stops and preserves
// the slot. Replaying it could associate rows with the wrong symbol.
//
// A frame that still has to be sent and starts above the known ids is a dead
// end: the ids below its start came from frames that were ACKed and trimmed
// away, and neither the side-file nor the remaining frames hold them any more.
// The same hole in an already-ACKed frame does no harm as long as no frame
// waiting to be sent needs it, and a later frame that carries the dictionary
// from id 0 starts the count over from something known to be complete.
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
			// Only a frame carrying the dictionary from id 0 clears a hole,
			// and only when the hole was in an already-ACKed frame. If a
			// frame still waiting to be sent has the hole, it reaches the
			// fresh server ahead of any such reset, so the reset cannot
			// rescue it.
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

		// Check names for ids we already know. Matching repeats are normal, but
		// different names for the same id mean the saved dictionary and frames
		// disagree. The server cannot detect this because there is no missing id,
		// so stop recovery rather than replaying rows with the wrong symbols.
		// qwpParseDeltaDict has already checked the entry data; keep the bounds
		// checks here in case that changes.
		p := entries
		for id := deltaStart; id < deltaEnd && id < coverage; id++ {
			entryLen, n, err := qwpReadVarint(p)
			if err != nil || entryLen > uint64(len(p)-n) {
				return qwpSfFailClosed("malformed recovered symbol dictionary overlap at fsn %d", fsn)
			}
			p = p[n:]
			if string(p[:int(entryLen)]) != a.symbols[id] {
				return qwpSfFailClosed(
					"recovered symbol dictionary disagrees on symbol id %d: %q already recovered, frame at fsn %d carries %q",
					id, a.symbols[id], fsn, string(p[:int(entryLen)]))
			}
			p = p[int(entryLen):]
		}
		if deltaEnd <= coverage {
			return nil
		}

		// Take the ids this frame adds on top of what is already known.
		for id := coverage; id < deltaEnd; id++ {
			entryLen, n, err := qwpReadVarint(p)
			if err != nil || entryLen > uint64(len(p)-n) {
				return qwpSfFailClosed("malformed recovered symbol dictionary suffix at fsn %d", fsn)
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
		return qwpSfRecoveredDictAnalysis{}, qwpSfFailClosed(
			"recovered symbol dictionary is incomplete: surviving unacked frames reference ids below their delta start; resend required")
	}
	return a, nil
}

// qwpSfWalkRecoveredFrames visits every CRC-validated frame in FSN order. It
// runs before the ring is registered with the manager or handed to a producer,
// so reading the sealed list and the active segment directly cannot race with
// anything.
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
				return qwpSfFailClosed("recovered frame envelope is truncated [baseSeq=%d, frame=%d]",
					segment.segmentBaseSeq(), frame)
			}
			payloadLen := int64(binary.LittleEndian.Uint32(buf[pos+4 : pos+8]))
			payloadStart := pos + qwpSfFrameHeaderSize
			payloadEnd := payloadStart + payloadLen
			if payloadEnd < payloadStart || payloadEnd > limit {
				return qwpSfFailClosed("recovered frame payload is truncated [baseSeq=%d, frame=%d, payloadLen=%d]",
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
