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

import "fmt"

// QwpServerInfo is the decoded SERVER_INFO frame delivered by a v2 QWP
// egress server as the first WebSocket frame after the upgrade
// handshake. v1 servers do not emit it, in which case the
// QwpQueryClient.ServerInfo() accessor returns nil.
//
// All fields are populated from a single decode pass; the struct is
// immutable from the user's perspective and safe to share across
// goroutines once published.
type QwpServerInfo struct {
	// Role is the server's replication role byte. Compare against the
	// qwpRole* constants or feed to RoleName for a human-readable form.
	// Drives target= filtering on multi-endpoint connections.
	Role byte
	// Epoch is a monotonic counter that advances across role
	// transitions on the same node (replica → primary, primary →
	// replica). Clients tracking a specific primary use it to refuse a
	// stale reconnect that lands on a node which no longer believes it
	// is primary at the current cluster epoch. 0 on releases without
	// fencing wired up; treat as a hint.
	Epoch uint64
	// Capabilities is the server capability bitfield from SERVER_INFO.
	// The only bit currently defined is CAP_ZONE (qwpCapZone): when
	// set, the frame carries a zone_id trailer after node_id.
	Capabilities uint32
	// ServerWallNs is the server wall-clock at the time SERVER_INFO was
	// emitted, in nanoseconds since the Unix epoch.
	ServerWallNs int64
	// ClusterId is a free-form identifier supplied by the server
	// operator. Surfaced in error messages and diagnostics.
	ClusterId string
	// NodeId is a free-form per-node identifier supplied by the server
	// operator. Distinct nodes in the same cluster carry distinct
	// values; surfaced in error messages and diagnostics.
	NodeId string
	// ZoneId is the server's zone identifier, populated when
	// Capabilities & qwpCapZone is set (failover.md §2). The
	// comparison against the client's configured zone= is
	// case-insensitive. Empty when the server did not opt into
	// CAP_ZONE; in that case the host's tracker tier stays Unknown.
	ZoneId string
}

// RoleName returns the human-readable name for the role byte. Unknown
// values surface as "UNKNOWN(n)" so diagnostics never lose information.
// Mirrors Java QwpServerInfo.roleName.
func (s *QwpServerInfo) RoleName() string {
	return qwpRoleName(s.Role)
}

// qwpRoleName is the package-internal helper that powers RoleName and
// the role-mismatch error formatter. Lives at package scope so callers
// without a populated *QwpServerInfo (e.g. role-mismatch error paths
// that have only the byte) can reuse the same names.
func qwpRoleName(role byte) string {
	switch role {
	case qwpRoleStandalone:
		return "STANDALONE"
	case qwpRolePrimary:
		return "PRIMARY"
	case qwpRoleReplica:
		return "REPLICA"
	case qwpRolePrimaryCatchup:
		return "PRIMARY_CATCHUP"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", role)
	}
}

// String returns a human-readable summary of the SERVER_INFO contents.
// Used by diagnostics and error messages; not parsed.
func (s *QwpServerInfo) String() string {
	return fmt.Sprintf(
		"QwpServerInfo{role=%s, epoch=%d, clusterId=%q, nodeId=%q, capabilities=0x%X, serverWallNs=%d}",
		s.RoleName(), s.Epoch, s.ClusterId, s.NodeId, s.Capabilities, s.ServerWallNs,
	)
}

// decodeServerInfo parses the SERVER_INFO frame off the wire. The
// payload is the full QWP message (12-byte header + msg_kind + body)
// as delivered by the WebSocket transport. The decoder validates the
// magic / version / msg_kind, then reads the body fields little-endian
// with bounds checks on every length-prefixed string so a hostile
// u16 length cannot drag bytes from outside the frame.
//
// negotiatedVersion is the QWP wire-protocol version selected by the
// HTTP upgrade handshake. The frame's header version byte must equal
// it exactly — spec §3 forbids a version-byte mismatch on any
// server-to-client frame.
//
// Mirrors Java QwpServerInfoDecoder.decode.
func decodeServerInfo(payload []byte, negotiatedVersion byte) (*QwpServerInfo, error) {
	if len(payload) < qwpHeaderSize+1 {
		return nil, newQwpDecodeError(fmt.Sprintf(
			"SERVER_INFO frame too short: %d bytes (need >= %d)",
			len(payload), qwpHeaderSize+1))
	}
	// Validate the QWP header before trusting any of the body bytes.
	// Mirrors parseFrameHeader's guards in qwp_query_decoder.go but
	// avoids the decoder's per-frame state writes (deltaOn / gorillaOn
	// / zstdOn) since SERVER_INFO carries none of those flags.
	if magic := uint32(payload[0]) | uint32(payload[1])<<8 |
		uint32(payload[2])<<16 | uint32(payload[3])<<24; magic != qwpMagic {
		return nil, newQwpDecodeError(fmt.Sprintf(
			"SERVER_INFO bad magic 0x%08X", magic))
	}
	if payload[4] != negotiatedVersion {
		return nil, newQwpDecodeError(fmt.Sprintf(
			"SERVER_INFO frame version %d does not match negotiated version %d",
			payload[4], negotiatedVersion))
	}
	// Spec §4: table_count is 0 on every non-RESULT_BATCH frame.
	tableCount := uint16(payload[qwpHeaderOffsetTableCount]) |
		uint16(payload[qwpHeaderOffsetTableCount+1])<<8
	if tableCount != 0 {
		return nil, newQwpDecodeError(fmt.Sprintf(
			"SERVER_INFO frame table_count = %d, expected 0", tableCount))
	}

	br := qwpByteReader{}
	br.reset(payload[qwpHeaderSize:])
	kindByte, err := br.readByte()
	if err != nil {
		return nil, err
	}
	if qwpMsgKind(kindByte) != qwpMsgKindServerInfo {
		return nil, newQwpDecodeError(fmt.Sprintf(
			"expected SERVER_INFO msg_kind 0x%02X, got 0x%02X",
			byte(qwpMsgKindServerInfo), kindByte))
	}
	role, err := br.readByte()
	if err != nil {
		return nil, err
	}
	epoch, err := br.readUint64LE()
	if err != nil {
		return nil, err
	}
	capabilities, err := br.readUint32LE()
	if err != nil {
		return nil, err
	}
	serverWallNs, err := br.readInt64LE()
	if err != nil {
		return nil, err
	}
	clusterId, err := readUtf8U16(&br, "cluster_id")
	if err != nil {
		return nil, err
	}
	nodeId, err := readUtf8U16(&br, "node_id")
	if err != nil {
		return nil, err
	}
	// Optional zone_id, gated by CAP_ZONE in capabilities. Servers
	// that haven't opted into CAP_ZONE end the frame at node_id;
	// servers that have opted in append a u16-length-prefixed UTF-8
	// zone identifier (failover.md §5). The reader's bounds checks
	// in readUtf8U16 guard against a hostile length declaring more
	// bytes than the frame contains.
	var zoneId string
	if capabilities&qwpCapZone != 0 {
		zoneId, err = readUtf8U16(&br, "zone_id")
		if err != nil {
			return nil, err
		}
	}
	return &QwpServerInfo{
		Role:         role,
		Epoch:        epoch,
		Capabilities: capabilities,
		ServerWallNs: serverWallNs,
		ClusterId:    clusterId,
		NodeId:       nodeId,
		ZoneId:       zoneId,
	}, nil
}

// readUtf8U16 reads a u16-length-prefixed UTF-8 string from the
// reader. The length is bounds-checked against the reader's remaining
// bytes before allocation so a hostile length cannot trigger an
// out-of-bounds slice. fieldName is woven into the error for
// diagnostic clarity.
func readUtf8U16(br *qwpByteReader, fieldName string) (string, error) {
	n, err := br.readUint16LE()
	if err != nil {
		return "", wrapQwpDecodeError(
			fmt.Sprintf("SERVER_INFO truncated reading %s length", fieldName),
			err)
	}
	if int(n) > br.remaining() {
		return "", newQwpDecodeError(fmt.Sprintf(
			"SERVER_INFO %s length %d exceeds frame remainder %d",
			fieldName, n, br.remaining()))
	}
	if n == 0 {
		return "", nil
	}
	bytes, err := br.slice(int(n))
	if err != nil {
		return "", err
	}
	// Copy out of the aliasing slice so the returned string survives
	// the recv buffer's lifecycle.
	return string(bytes), nil
}
