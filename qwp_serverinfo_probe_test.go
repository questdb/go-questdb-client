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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQwpServerInfoIsEgressOnly pins a wire-protocol invariant: the
// server delivers an unsolicited SERVER_INFO frame as the first frame
// only on the egress endpoint (/read/v1). The ingest endpoint
// (/write/v4) sends no SERVER_INFO and the client never expects one —
// it sends data right after the upgrade and the first inbound frame is
// an ACK. This is why the ingest path leaves serverInfoTimeout=0 and
// readAck() reads the first frame as an ACK without skipping a
// SERVER_INFO.
//
// The test opts into a synchronous first-frame read (serverInfoTimeout
// > 0) on each endpoint:
//   - egress MUST return a SERVER_INFO frame (control: proves the probe
//     and server are healthy);
//   - ingest MUST time out the post-upgrade read (the server sends
//     nothing until the client speaks).
//
// If the ingest assertion ever fails, the server has started emitting
// SERVER_INFO on /write/v4, and the ingest read path must be changed to
// consume and discard it before the ACK loop. Source of truth:
// connect/wire-protocols/qwp-{ingress,egress}-websocket.md.
//
// Run against a live server, e.g.:
//
//	QDB_FUZZ_ADDR=localhost:9000 go test -v -run TestQwpServerInfoIsEgressOnly .
func TestQwpServerInfoIsEgressOnly(t *testing.T) {
	qwpEnsureServer(t)

	probe := func(label, path string) (*QwpServerInfo, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var tr qwpTransport
		err := tr.connect(ctx, "ws://"+qwpTestAddr, qwpTransportOpts{
			endpointPath:      path,
			maxVersion:        qwpVersion,
			serverInfoTimeout: 3 * time.Second,
		})
		defer tr.close()

		t.Logf("[%-7s %s]: serverInfo=%v err=%v", label, path, tr.serverInfo, err)
		return tr.serverInfo, err
	}

	// Control: egress must deliver SERVER_INFO as the first frame.
	egInfo, egErr := probe("egress", qwpReadPath)
	require.NoError(t, egErr, "egress control: SERVER_INFO read should succeed on /read/v1")
	require.NotNil(t, egInfo,
		"egress control: expected a SERVER_INFO frame on /read/v1 (if nil, the probe itself is broken)")

	// Invariant: ingest must NOT deliver SERVER_INFO. The upgrade
	// succeeds, then the first-frame read times out because the server
	// sends nothing until the client does. "SERVER_INFO read failed" in
	// the error confirms the upgrade completed and it is the post-upgrade
	// read that timed out (an upgrade reject would surface a different
	// error before this point).
	inInfo, inErr := probe("ingest", qwpWritePath)
	require.Nil(t, inInfo,
		"ingest must NOT receive SERVER_INFO (spec: ingress is role/zone-blind). "+
			"If non-nil, the server now emits SERVER_INFO on /write/v4 and the ingest "+
			"read path must consume/discard it before the ACK loop.")
	require.Error(t, inErr, "ingest: the post-upgrade first-frame read must time out")
	require.Contains(t, inErr.Error(), "SERVER_INFO read failed",
		"ingest: upgrade should succeed, then the first-frame read should time out")
}
