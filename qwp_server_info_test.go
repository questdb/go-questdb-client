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
	"strings"
	"testing"
)

func TestQwpServerInfoRoleName(t *testing.T) {
	cases := []struct {
		role byte
		want string
	}{
		{qwpRoleStandalone, "STANDALONE"},
		{qwpRolePrimary, "PRIMARY"},
		{qwpRoleReplica, "REPLICA"},
		{qwpRolePrimaryCatchup, "PRIMARY_CATCHUP"},
		{0xFF, "UNKNOWN(0xFF)"},
		{0x42, "UNKNOWN(0x42)"},
	}
	for _, tc := range cases {
		got := qwpRoleName(tc.role)
		if got != tc.want {
			t.Errorf("qwpRoleName(0x%02X) = %q, want %q", tc.role, got, tc.want)
		}
		s := &QwpServerInfo{Role: tc.role}
		if s.RoleName() != tc.want {
			t.Errorf("(*QwpServerInfo).RoleName() = %q, want %q", s.RoleName(), tc.want)
		}
	}
}

// buildServerInfoFrame produces a full SERVER_INFO QWP message (12-byte
// header + body) for tests. flagBits is OR-ed onto the header flags so
// negative tests can craft hostile shapes; pass 0 for the conformant
// frame v2 servers actually emit.
func buildServerInfoFrame(version byte, flagBits byte, role byte, epoch uint64, capabilities uint32, serverWallNs int64, clusterId, nodeId string) []byte {
	body := []byte{}
	body = append(body, byte(qwpMsgKindServerInfo))
	body = append(body, role)
	body = appendUint64LE(body, epoch)
	body = appendUint32LE(body, capabilities)
	body = appendInt64LE(body, serverWallNs)
	body = appendUint16LE(body, uint16(len(clusterId)))
	body = append(body, clusterId...)
	body = appendUint16LE(body, uint16(len(nodeId)))
	body = append(body, nodeId...)

	header := make([]byte, qwpHeaderSize)
	// magic
	magic := uint32(qwpMagic)
	header[0] = byte(magic)
	header[1] = byte(magic >> 8)
	header[2] = byte(magic >> 16)
	header[3] = byte(magic >> 24)
	header[4] = version
	header[qwpHeaderOffsetFlags] = flagBits
	// tableCount (uint16 LE) at offset 6. Spec §4 mandates 0 on every
	// non-RESULT_BATCH kind, including SERVER_INFO; leaving the bytes
	// zero satisfies that.
	// payloadLen (uint32 LE) at offset 8.
	payloadLen := uint32(len(body))
	header[qwpHeaderOffsetPayloadLen] = byte(payloadLen)
	header[qwpHeaderOffsetPayloadLen+1] = byte(payloadLen >> 8)
	header[qwpHeaderOffsetPayloadLen+2] = byte(payloadLen >> 16)
	header[qwpHeaderOffsetPayloadLen+3] = byte(payloadLen >> 24)
	return append(header, body...)
}

func appendUint16LE(buf []byte, v uint16) []byte {
	return append(buf, byte(v), byte(v>>8))
}

func appendUint32LE(buf []byte, v uint32) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func appendUint64LE(buf []byte, v uint64) []byte {
	return append(buf,
		byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

func appendInt64LE(buf []byte, v int64) []byte {
	return appendUint64LE(buf, uint64(v))
}

func TestQwpServerInfoDecodeHappyPath(t *testing.T) {
	frame := buildServerInfoFrame(
		qwpMaxSupportedVersion, 0,
		qwpRolePrimary, 7, 0, 1_700_000_000_000_000_000,
		"cluster-A", "node-1",
	)
	info, err := decodeServerInfo(frame, qwpMaxSupportedVersion)
	if err != nil {
		t.Fatalf("decodeServerInfo: %v", err)
	}
	if info.Role != qwpRolePrimary {
		t.Errorf("Role = 0x%02X, want PRIMARY", info.Role)
	}
	if info.Epoch != 7 {
		t.Errorf("Epoch = %d, want 7", info.Epoch)
	}
	if info.Capabilities != 0 {
		t.Errorf("Capabilities = %d, want 0", info.Capabilities)
	}
	if info.ServerWallNs != 1_700_000_000_000_000_000 {
		t.Errorf("ServerWallNs = %d", info.ServerWallNs)
	}
	if info.ClusterId != "cluster-A" {
		t.Errorf("ClusterId = %q", info.ClusterId)
	}
	if info.NodeId != "node-1" {
		t.Errorf("NodeId = %q", info.NodeId)
	}
}

func TestQwpServerInfoDecodeEmptyIdentifiers(t *testing.T) {
	frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
		qwpRoleStandalone, 0, 0, 0, "", "")
	info, err := decodeServerInfo(frame, qwpMaxSupportedVersion)
	if err != nil {
		t.Fatalf("decodeServerInfo: %v", err)
	}
	if info.ClusterId != "" {
		t.Errorf("ClusterId = %q, want empty", info.ClusterId)
	}
	if info.NodeId != "" {
		t.Errorf("NodeId = %q, want empty", info.NodeId)
	}
}

func TestQwpServerInfoDecodeRejectsVersionMismatch(t *testing.T) {
	// Spec §3 requires the SERVER_INFO header version byte to equal
	// the version negotiated during the HTTP upgrade. A v1-stamped
	// frame on a v2-negotiated connection (and vice versa) must be
	// rejected, even though both versions are individually supported.
	cases := []struct {
		name              string
		frameVersion      byte
		negotiatedVersion byte
	}{
		{"v1_frame_v2_connection", 0x01, qwpMaxSupportedVersion},
		{"v2_frame_v1_connection", qwpMaxSupportedVersion, 0x01},
		{"too_new_frame", 0xFF, qwpMaxSupportedVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame := buildServerInfoFrame(tc.frameVersion, 0,
				qwpRoleStandalone, 0, 0, 0, "", "")
			_, err := decodeServerInfo(frame, tc.negotiatedVersion)
			if err == nil {
				t.Fatalf("decoder accepted version=0x%02X on a "+
					"v0x%02X-negotiated connection",
					tc.frameVersion, tc.negotiatedVersion)
			}
			if !strings.Contains(err.Error(), "does not match negotiated version") {
				t.Errorf("error = %v, want version-mismatch message", err)
			}
		})
	}
}

func TestQwpServerInfoDecodeRejectsBadMagic(t *testing.T) {
	frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
		qwpRoleStandalone, 0, 0, 0, "", "")
	frame[0] = 0x00 // corrupt magic
	_, err := decodeServerInfo(frame, qwpMaxSupportedVersion)
	if err == nil {
		t.Fatal("decoder accepted bad magic")
	}
	if !strings.Contains(err.Error(), "bad magic") {
		t.Errorf("error = %v, want bad magic", err)
	}
}

func TestQwpServerInfoDecodeRejectsNonZeroTableCount(t *testing.T) {
	// Spec §4 mandates table_count = 0 on every non-RESULT_BATCH
	// frame. SERVER_INFO is no exception — a server that smuggles a
	// non-zero value here is malformed and must be rejected before any
	// body bytes are trusted.
	frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
		qwpRoleStandalone, 0, 0, 0, "", "")
	frame[qwpHeaderOffsetTableCount] = 1
	_, err := decodeServerInfo(frame, qwpMaxSupportedVersion)
	if err == nil {
		t.Fatal("decoder accepted non-zero table_count")
	}
	if !strings.Contains(err.Error(), "table_count") {
		t.Errorf("error = %v, want table_count", err)
	}
}

func TestQwpServerInfoDecodeRejectsWrongMsgKind(t *testing.T) {
	frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
		qwpRoleStandalone, 0, 0, 0, "", "")
	frame[qwpHeaderSize] = byte(qwpMsgKindResultBatch)
	_, err := decodeServerInfo(frame, qwpMaxSupportedVersion)
	if err == nil {
		t.Fatal("decoder accepted wrong msg_kind")
	}
	if !strings.Contains(err.Error(), "expected SERVER_INFO msg_kind") {
		t.Errorf("error = %v, want expected SERVER_INFO msg_kind", err)
	}
}

func TestQwpServerInfoDecodeRejectsTruncatedFrame(t *testing.T) {
	// Try truncating at every offset from 0 through one short of full
	// frame length; every truncation should produce a decode error.
	full := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
		qwpRolePrimary, 5, 0, 1234, "abc", "n1")
	for cut := 0; cut < len(full); cut++ {
		_, err := decodeServerInfo(full[:cut], qwpMaxSupportedVersion)
		if err == nil {
			t.Errorf("truncated frame of length %d decoded without error", cut)
		}
	}
}

func TestQwpServerInfoDecodeRejectsOversizedClusterId(t *testing.T) {
	// Hand-craft a frame whose cluster_id u16 length claims more
	// bytes than the frame contains.
	frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
		qwpRolePrimary, 0, 0, 0, "abc", "node")
	// cluster_id length lives at qwpHeaderSize + 1 (kind) + 1 (role)
	// + 8 (epoch) + 4 (caps) + 8 (wallNs).
	clusterLenOffset := qwpHeaderSize + 1 + 1 + 8 + 4 + 8
	frame[clusterLenOffset] = 0xFF
	frame[clusterLenOffset+1] = 0xFF
	_, err := decodeServerInfo(frame, qwpMaxSupportedVersion)
	if err == nil {
		t.Fatal("decoder accepted oversized cluster_id length")
	}
	if !strings.Contains(err.Error(), "exceeds frame remainder") {
		t.Errorf("error = %v, want exceeds frame remainder", err)
	}
}

func TestQwpServerInfoDecodeRejectsOversizedNodeId(t *testing.T) {
	frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
		qwpRolePrimary, 0, 0, 0, "abc", "node")
	// node_id length lives right after cluster_id bytes. cluster_id
	// is "abc" (3 bytes) so node_id length offset = clusterLenOffset
	// + 2 + 3.
	nodeLenOffset := qwpHeaderSize + 1 + 1 + 8 + 4 + 8 + 2 + 3
	frame[nodeLenOffset] = 0xFF
	frame[nodeLenOffset+1] = 0xFF
	_, err := decodeServerInfo(frame, qwpMaxSupportedVersion)
	if err == nil {
		t.Fatal("decoder accepted oversized node_id length")
	}
	if !strings.Contains(err.Error(), "exceeds frame remainder") {
		t.Errorf("error = %v, want exceeds frame remainder", err)
	}
}

func TestQwpServerInfoStringContainsKeyFields(t *testing.T) {
	info := &QwpServerInfo{
		Role:         qwpRolePrimary,
		Epoch:        42,
		Capabilities: 0xCAFE,
		ServerWallNs: 1234567890,
		ClusterId:    "alpha",
		NodeId:       "beta",
	}
	s := info.String()
	for _, want := range []string{"PRIMARY", "epoch=42", "alpha", "beta", "0xCAFE"} {
		if !strings.Contains(s, want) {
			t.Errorf("String() = %q missing %q", s, want)
		}
	}
}
