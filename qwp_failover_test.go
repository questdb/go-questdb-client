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
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// mockClusterNode is one entry in a multi-server failover test
// fixture. Each node has its own httptest.Server and tags itself with
// a role / nodeId / clusterId that flow into the SERVER_INFO frame
// it emits to incoming clients.
type mockClusterNode struct {
	t *testing.T
	// srv is the underlying httptest.Server.
	srv *httptest.Server
	// role is the SERVER_INFO.role byte. PRIMARY / REPLICA / etc.
	role byte
	// nodeId / clusterId are echoed in SERVER_INFO. nodeId is unique
	// per node so observeConnectedIdx can match the binding back to
	// the node.
	nodeId    string
	clusterId string

	// alive gates whether the server accepts new connections.
	alive atomic.Bool
	// onConnectCount counts successful upgrades for diagnostics.
	onConnectCount atomic.Int64
	// suppressServerInfo, when true, completes the WebSocket upgrade
	// but never writes the SERVER_INFO frame, so the client's
	// SERVER_INFO read times out at serverInfoTimeout. Used by tests
	// that need a slow but reachable endpoint.
	suppressServerInfo atomic.Bool
}

// addr returns the host:port for connection-string assembly.
func (n *mockClusterNode) addr() string {
	return strings.TrimPrefix(n.srv.URL, "http://")
}

// mockCluster aggregates N httptest.Server fakes — one per simulated
// QuestDB node. Use newMockCluster to build the cluster, then access
// nodes[i] to drive selective failure / role assertions.
type mockCluster struct {
	t     *testing.T
	nodes []*mockClusterNode
}

// addrList joins the node host:port pairs for use in the addr= conf
// string or WithQwpQueryEndpoints option. Honours the order passed to
// newMockCluster so target-filter tests can assert which node bound.
func (c *mockCluster) addrList() string {
	parts := make([]string, 0, len(c.nodes))
	for _, n := range c.nodes {
		parts = append(parts, n.addr())
	}
	return strings.Join(parts, ",")
}

// newMockCluster spins up n in-process WebSocket servers, each tagged
// with a role / nodeId / clusterId provided by tag(). The returned
// cluster is automatically torn down via t.Cleanup; tests can also
// kill individual nodes mid-test via node.kill().
//
// Each node's handler is responsible for the post-SERVER_INFO
// choreography. Nil handler defaults to "send a QUERY_ERROR(internal)
// to every QUERY_REQUEST" — useful for transport-failure simulations
// that don't otherwise produce events.
func newMockCluster(t *testing.T, n int, tag func(idx int) (role byte, nodeId, clusterId string), handler func(idx int, m *qwpMockEgressConn)) *mockCluster {
	t.Helper()
	cluster := &mockCluster{t: t, nodes: make([]*mockClusterNode, 0, n)}
	for i := 0; i < n; i++ {
		role, nodeId, clusterId := tag(i)
		mn := &mockClusterNode{
			t:         t,
			role:      role,
			nodeId:    nodeId,
			clusterId: clusterId,
		}
		mn.alive.Store(true)
		idx := i
		mn.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !mn.alive.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set(qwpHeaderVersion, fmt.Sprintf("%d", qwpMaxSupportedVersion))
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				t.Logf("mock node %d: accept: %v", idx, err)
				return
			}
			defer conn.CloseNow()
			mn.onConnectCount.Add(1)
			if mn.suppressServerInfo.Load() {
				// Hold the upgraded connection open without writing
				// SERVER_INFO so the client's read times out.
				<-r.Context().Done()
				return
			}
			frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
				mn.role, uint64(idx+1), 0, time.Now().UnixNano(),
				mn.clusterId, mn.nodeId)
			if err := conn.Write(r.Context(), websocket.MessageBinary, frame); err != nil {
				t.Logf("mock node %d: SERVER_INFO write: %v", idx, err)
				return
			}
			// Stamp v2 on every frame the mock writes — the cluster
			// advertises qwpMaxSupportedVersion in X-QWP-Version
			// (see above), and the decoder's strict-equality version
			// check rejects frames whose header version byte does not
			// match the negotiated version.
			mc := &qwpMockEgressConn{t: t, conn: conn, version: qwpMaxSupportedVersion}
			if handler != nil {
				handler(idx, mc)
			} else {
				// Default: stay alive until the connection drops.
				for {
					if _, _, err := conn.Read(r.Context()); err != nil {
						return
					}
				}
			}
		}))
		cluster.nodes = append(cluster.nodes, mn)
	}
	t.Cleanup(func() {
		for _, n := range cluster.nodes {
			n.alive.Store(false)
			n.srv.Close()
		}
	})
	return cluster
}

// rolesPrimaryReplicaReplica produces the standard tag closure for
// failover tests where the first node is the primary and the rest
// are replicas. Mirrors the typical QuestDB cluster topology.
func rolesPrimaryReplicaReplica() func(int) (byte, string, string) {
	return func(idx int) (byte, string, string) {
		if idx == 0 {
			return qwpRolePrimary, fmt.Sprintf("node-%d", idx), "test-cluster"
		}
		return qwpRoleReplica, fmt.Sprintf("node-%d", idx), "test-cluster"
	}
}

// rolesAllReplicas tags every node REPLICA — used to test
// QwpRoleMismatchError when target=primary cannot find a match.
func rolesAllReplicas() func(int) (byte, string, string) {
	return func(idx int) (byte, string, string) {
		return qwpRoleReplica, fmt.Sprintf("replica-%d", idx), "test-cluster"
	}
}

// --- Tests ---

// TestQwpClientConnectsToFirstMatchingTarget verifies that the
// connect walk binds to the first endpoint whose role passes the
// filter. With target=primary and a primary-then-replicas cluster,
// the client picks node 0.
func TestQwpClientConnectsToFirstMatchingTarget(t *testing.T) {
	cluster := newMockCluster(t, 3, rolesPrimaryReplicaReplica(), nil)

	cfg := qwpQueryDefaultConfig()
	eps, err := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	if err != nil {
		t.Fatalf("parseEndpointList: %v", err)
	}
	cfg.endpoints = eps
	cfg.target = qwpTargetPrimary
	cfg.serverInfoTimeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	if c.CurrentEndpoint() != cluster.nodes[0].addr() {
		t.Errorf("currentEndpoint = %s, want %s",
			c.CurrentEndpoint(), cluster.nodes[0].addr())
	}
	info := c.ServerInfo()
	if info == nil {
		t.Fatal("ServerInfo nil after v2 connect")
	}
	if info.Role != qwpRolePrimary {
		t.Errorf("role = %s, want PRIMARY", info.RoleName())
	}
	if info.NodeId != "node-0" {
		t.Errorf("nodeId = %q, want node-0", info.NodeId)
	}
}

// TestQwpClientWalksPastReplicasToPrimary verifies that the walk
// skips role-mismatched endpoints and lands on the first matching
// one further down the list.
func TestQwpClientWalksPastReplicasToPrimary(t *testing.T) {
	// Two replicas first, then a primary at index 2.
	cluster := newMockCluster(t, 3, func(idx int) (byte, string, string) {
		role := qwpRoleReplica
		if idx == 2 {
			role = qwpRolePrimary
		}
		return role, fmt.Sprintf("node-%d", idx), "test-cluster"
	}, nil)

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetPrimary
	cfg.serverInfoTimeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	if c.CurrentEndpoint() != cluster.nodes[2].addr() {
		t.Errorf("bound to %s, want %s (node-2 is the primary)",
			c.CurrentEndpoint(), cluster.nodes[2].addr())
	}
}

// TestQwpClientRoleMismatchSurfacesTypedError verifies that the walk
// returns *QwpRoleMismatchError with the last observed SERVER_INFO
// when target=primary but every endpoint reports REPLICA.
func TestQwpClientRoleMismatchSurfacesTypedError(t *testing.T) {
	cluster := newMockCluster(t, 2, rolesAllReplicas(), nil)

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetPrimary
	cfg.serverInfoTimeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := newQwpQueryClient(ctx, cfg)
	if err == nil {
		t.Fatal("expected QwpRoleMismatchError")
	}
	var rme *QwpRoleMismatchError
	if !errors.As(err, &rme) {
		t.Fatalf("err = %v (%T), want *QwpRoleMismatchError", err, err)
	}
	if rme.Target != "primary" {
		t.Errorf("Target = %q, want primary", rme.Target)
	}
	if rme.LastObserved == nil {
		t.Fatal("LastObserved should be populated")
	}
	if rme.LastObserved.Role != qwpRoleReplica {
		t.Errorf("LastObserved.Role = %s, want REPLICA",
			rme.LastObserved.RoleName())
	}
	if !strings.Contains(rme.Error(), "primary") {
		t.Errorf("Error string %q missing target", rme.Error())
	}
}

// TestQwpClientV1MismatchSurfacesSawV1MismatchFlag verifies that when
// every endpoint negotiates QWP v1 (no SERVER_INFO frame) and the
// caller asks for target=primary, the typed error reports
// SawV1Mismatch=true with a LastObserved=nil. Without this flag the
// caller cannot distinguish "you pointed me at an OSS / v1 cluster"
// from "all endpoints unreachable".
func TestQwpClientV1MismatchSurfacesSawV1MismatchFlag(t *testing.T) {
	// Two v1-only endpoints: each echoes X-QWP-Version=1 on upgrade
	// and never emits a SERVER_INFO frame, mirroring an OSS server.
	v1Server := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(qwpHeaderVersion, "1")
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			for {
				if _, _, err := conn.Read(r.Context()); err != nil {
					return
				}
			}
		}))
	}
	srvA := v1Server()
	defer srvA.Close()
	srvB := v1Server()
	defer srvB.Close()
	addrList := strings.TrimPrefix(srvA.URL, "http://") + "," +
		strings.TrimPrefix(srvB.URL, "http://")

	cfg := qwpQueryDefaultConfig()
	eps, err := parseEndpointList(addrList, qwpDefaultPort)
	if err != nil {
		t.Fatalf("parseEndpointList: %v", err)
	}
	cfg.endpoints = eps
	cfg.target = qwpTargetPrimary
	cfg.serverInfoTimeout = 500 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = newQwpQueryClient(ctx, cfg)
	if err == nil {
		t.Fatal("expected QwpRoleMismatchError")
	}
	var rme *QwpRoleMismatchError
	if !errors.As(err, &rme) {
		t.Fatalf("err = %v (%T), want *QwpRoleMismatchError", err, err)
	}
	if !rme.SawV1Mismatch {
		t.Errorf("SawV1Mismatch = false, want true")
	}
	if rme.LastObserved != nil {
		t.Errorf("LastObserved = %+v, want nil (no v2 endpoint reported a role)",
			rme.LastObserved)
	}
	if rme.Target != "primary" {
		t.Errorf("Target = %q, want primary", rme.Target)
	}
	if !strings.Contains(rme.Error(), "negotiated v1") {
		t.Errorf("Error string %q missing v1 hint", rme.Error())
	}
}

// TestQwpClientRoleMismatchPreservesTransportError verifies that when
// the connect walk encounters a mix of transport failures and other
// non-matching outcomes (e.g. v1 endpoints) under target=primary, the
// returned QwpRoleMismatchError carries both the v1 flag and the last
// underlying transport error so callers can tell network problems from
// pure role mismatch and reach the dial error via errors.As / Unwrap.
func TestQwpClientRoleMismatchPreservesTransportError(t *testing.T) {
	// Endpoint A: refuses the WebSocket upgrade with 503 — generates a
	// transport-level dial error.
	srvFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srvFail.Close()
	// Endpoint B: negotiates QWP v1 — accepted at the transport layer
	// but skipped by the role filter because v1 has no SERVER_INFO.
	srvV1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer srvV1.Close()

	addrList := strings.TrimPrefix(srvFail.URL, "http://") + "," +
		strings.TrimPrefix(srvV1.URL, "http://")
	cfg := qwpQueryDefaultConfig()
	eps, err := parseEndpointList(addrList, qwpDefaultPort)
	if err != nil {
		t.Fatalf("parseEndpointList: %v", err)
	}
	cfg.endpoints = eps
	cfg.target = qwpTargetPrimary
	cfg.serverInfoTimeout = 500 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = newQwpQueryClient(ctx, cfg)
	if err == nil {
		t.Fatal("expected QwpRoleMismatchError")
	}
	var rme *QwpRoleMismatchError
	if !errors.As(err, &rme) {
		t.Fatalf("err = %v (%T), want *QwpRoleMismatchError", err, err)
	}
	if !rme.SawV1Mismatch {
		t.Errorf("SawV1Mismatch = false, want true (v1 endpoint was visited)")
	}
	if rme.LastTransportError == nil {
		t.Fatal("LastTransportError = nil, want the dial failure from the 503 endpoint")
	}
	if !errors.Is(err, rme.LastTransportError) {
		t.Errorf("errors.Is(err, LastTransportError) = false, want true via Unwrap")
	}
	if !strings.Contains(rme.Error(), "last transport error") {
		t.Errorf("Error string %q missing transport-error hint", rme.Error())
	}
	if !strings.Contains(rme.Error(), "negotiated v1") {
		t.Errorf("Error string %q missing v1 hint", rme.Error())
	}
}

// TestQwpClientPrimaryAcceptsStandalone verifies the OSS-friendly
// rule that target=primary also accepts STANDALONE — the role v1
// servers report when replication is not configured. Without this,
// every single-node OSS deployment would refuse target=primary.
func TestQwpClientPrimaryAcceptsStandalone(t *testing.T) {
	cluster := newMockCluster(t, 1, func(int) (byte, string, string) {
		return qwpRoleStandalone, "solo", "oss"
	}, nil)
	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetPrimary
	cfg.serverInfoTimeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)
	if c.ServerInfo().Role != qwpRoleStandalone {
		t.Errorf("role = %s, want STANDALONE", c.ServerInfo().RoleName())
	}
}

// TestQwpFailoverYieldsResetThenResumes drives the full transparent-
// failover happy path: the first server emits a transport failure
// mid-query, the client reconnects to the second server and replays
// the QUERY_REQUEST, and the iterator yields *QwpFailoverReset
// followed by the new generation's batches.
func TestQwpFailoverYieldsResetThenResumes(t *testing.T) {
	type nodeState struct {
		failOnce atomic.Bool
	}
	states := make([]*nodeState, 2)
	for i := range states {
		states[i] = &nodeState{}
	}
	// Node 0 fails the first connection's query (closes the conn
	// after reading QUERY_REQUEST). Node 1 serves successfully.
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, frame, err := m.conn.Read(ctx)
			_ = frame
			if err != nil {
				return
			}
			if idx == 0 && states[0].failOnce.CompareAndSwap(false, true) {
				// Force a transport-terminal failure.
				m.conn.Close(websocket.StatusInternalError, "simulated fault")
				return
			}
			// Node 1: respond with one batch then RESULT_END.
			frameBytes := buildOneRowInt64Batch(t, 1, 0, "v", 99)
			m.sendBinary(ctx, frameBytes)
			m.sendBinary(ctx, writeQwpFrame(0,
				buildResultEndBody(1, 0, 1)))
			// Hold open so client can close cleanly.
			for {
				if _, _, err := m.conn.Read(ctx); err != nil {
					return
				}
			}
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 2 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 3
	cfg.failoverBackoffInitial = 1 * time.Millisecond
	cfg.failoverBackoffMax = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "select v from t")
	defer q.Close()

	var (
		gotReset bool
		gotBatch bool
	)
	for batch, err := range q.Batches() {
		if err != nil {
			var reset *QwpFailoverReset
			if errors.As(err, &reset) {
				gotReset = true
				if reset.NewNode == nil || reset.NewNode.NodeId != "node-1" {
					t.Errorf("reset.NewNode = %+v, want node-1", reset.NewNode)
				}
				continue
			}
			t.Fatalf("unexpected error: %v", err)
		}
		gotBatch = true
		if got := batch.Int64(0, 0); got != 99 {
			t.Errorf("batch value = %d, want 99", got)
		}
	}

	if !gotReset {
		t.Error("expected *QwpFailoverReset yield, got none")
	}
	if !gotBatch {
		t.Error("expected to receive a batch from the new generation")
	}
	if c.CurrentEndpoint() != cluster.nodes[1].addr() {
		t.Errorf("after failover bound to %s, want %s",
			c.CurrentEndpoint(), cluster.nodes[1].addr())
	}
}

// TestQwpFailoverSkipsJustFailedEndpoint verifies that on reconnect
// the connect walk does not revisit the endpoint that just failed,
// matching Java's reconnectSkippingIndex. With three endpoints where
// only the middle one passes the role filter, the reconnect must skip
// the failed primary (rather than rebind to it and trip the same
// fault) and surface a role-mismatch error instead.
func TestQwpFailoverSkipsJustFailedEndpoint(t *testing.T) {
	// idx=0 REPLICA, idx=1 PRIMARY, idx=2 REPLICA. Only the primary
	// passes target=primary, so initial bind lands on idx=1.
	cluster := newMockCluster(t, 3, func(idx int) (byte, string, string) {
		role := qwpRoleReplica
		if idx == 1 {
			role = qwpRolePrimary
		}
		return role, fmt.Sprintf("node-%d", idx), "test-cluster"
	},
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Drain the QUERY_REQUEST then close the socket to simulate
			// a transport-terminal fault.
			_, _, _ = m.conn.Read(ctx)
			m.conn.Close(websocket.StatusInternalError, "simulated fault")
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetPrimary
	cfg.serverInfoTimeout = 2 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 5
	cfg.failoverBackoffInitial = 1 * time.Millisecond
	cfg.failoverBackoffMax = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	if c.CurrentEndpoint() != cluster.nodes[1].addr() {
		t.Fatalf("initial bind = %s, want %s (the only primary)",
			c.CurrentEndpoint(), cluster.nodes[1].addr())
	}

	q := c.Query(ctx, "select v from t")
	defer q.Close()

	var sawErr bool
	for _, err := range q.Batches() {
		if err == nil {
			t.Errorf("unexpected non-error batch from a poisoned connection")
			continue
		}
		var reset *QwpFailoverReset
		if errors.As(err, &reset) {
			t.Errorf("unexpected failover reset; reconnect should fail role filter")
			continue
		}
		// The failover-time role mismatch must surface as a typed
		// *QwpRoleMismatchError so callers can errors.As against it,
		// matching the initial-connect path.
		var rme *QwpRoleMismatchError
		if !errors.As(err, &rme) {
			t.Errorf("err = %v (%T), want errors.As to match *QwpRoleMismatchError",
				err, err)
		} else if rme.Target != "primary" {
			t.Errorf("rme.Target = %q, want primary", rme.Target)
		}
		if !strings.Contains(err.Error(), "no endpoint matches target=primary") {
			t.Errorf("err = %v, want role-mismatch text", err)
		}
		sawErr = true
	}
	if !sawErr {
		t.Error("expected reconnect to surface a transport error")
	}

	// The failed primary must be connected exactly once — the initial
	// bind. Without the skip, the reconnect walk would wrap around to
	// idx=1 again and the count would be 2.
	if got := cluster.nodes[1].onConnectCount.Load(); got != 1 {
		t.Errorf("primary at idx=1 connected %d times, want 1 (no rebind)", got)
	}
}

// TestQwpFailoverDisabledSurfacesTransportError verifies that with
// failoverEnabled=false, a transport-terminal failure mid-query
// surfaces directly through Batches() instead of triggering replay.
func TestQwpFailoverDisabledSurfacesTransportError(t *testing.T) {
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, _ = m.conn.Read(ctx)
			m.conn.Close(websocket.StatusInternalError, "simulated fault")
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 2 * time.Second
	cfg.failoverEnabled = false

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "select 1")
	defer q.Close()

	var sawErr bool
	for _, err := range q.Batches() {
		if err != nil {
			var reset *QwpFailoverReset
			if errors.As(err, &reset) {
				t.Errorf("got reset with failover disabled")
				continue
			}
			sawErr = true
		}
	}
	if !sawErr {
		t.Error("expected transport error to surface, got none")
	}
}

// TestQwpFailoverRespectsMaxAttempts verifies that after exhausting
// failoverMaxAttempts the iterator surfaces a typed
// *QwpFailoverExhaustedError rather than the underlying transport
// error and rather than looping forever. The exhaustion error must
// carry the attempt count and unwrap to the most recent transport
// failure so callers can errors.As against both shapes.
func TestQwpFailoverRespectsMaxAttempts(t *testing.T) {
	// Both nodes always fail; max_attempts = 3 means we get 3
	// connect attempts total before giving up.
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, _ = m.conn.Read(ctx)
			m.conn.Close(websocket.StatusInternalError, "always fail")
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 1 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 3
	cfg.failoverBackoffInitial = 1 * time.Millisecond
	cfg.failoverBackoffMax = 5 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "select 1")
	defer q.Close()

	var (
		resets       int
		terminalErrs []error
	)
	for _, err := range q.Batches() {
		if err == nil {
			continue
		}
		var reset *QwpFailoverReset
		if errors.As(err, &reset) {
			resets++
			continue
		}
		terminalErrs = append(terminalErrs, err)
	}
	if len(terminalErrs) != 1 {
		t.Fatalf("terminalErrors = %d, want 1: %v", len(terminalErrs), terminalErrs)
	}
	// Resets should be < failoverMaxAttempts because the budget
	// includes the initial submission.
	if resets >= cfg.failoverMaxAttempts {
		t.Errorf("resets = %d, expected < failoverMaxAttempts (%d)",
			resets, cfg.failoverMaxAttempts)
	}
	// Exhaustion must surface as a typed *QwpFailoverExhaustedError
	// so callers can distinguish "ran out of retries" from "first
	// attempt failed". The message MUST identify exhaustion and
	// SHOULD carry the attempt count and the most recent
	// transport-failure message — assert all three.
	terminalErr := terminalErrs[0]
	var exhausted *QwpFailoverExhaustedError
	if !errors.As(terminalErr, &exhausted) {
		t.Fatalf("terminal err = %v (%T), want errors.As to match *QwpFailoverExhaustedError",
			terminalErr, terminalErr)
	}
	if exhausted.Attempts != cfg.failoverMaxAttempts {
		t.Errorf("exhausted.Attempts = %d, want %d (failoverMaxAttempts)",
			exhausted.Attempts, cfg.failoverMaxAttempts)
	}
	if exhausted.LastError == nil {
		t.Error("exhausted.LastError = nil, want the underlying transport error")
	}
	if !strings.Contains(terminalErr.Error(), "failover exhausted") {
		t.Errorf("terminal err = %q, want it to identify failover exhaustion",
			terminalErr.Error())
	}
	if !strings.Contains(terminalErr.Error(), "last error:") {
		t.Errorf("terminal err = %q, want it to include the last transport-failure message",
			terminalErr.Error())
	}
}

// TestQwpFailoverRespectsMaxDuration verifies that the wall-clock
// failover budget (failover_max_duration_ms) ends the loop even when
// failoverMaxAttempts is set high enough that the attempt cap would
// never fire. Exhaustion must still surface as a typed
// *QwpFailoverExhaustedError, and the attempt count must be far below
// the attempt cap — proving the duration budget, not the attempt cap,
// was the binding constraint. Mirrors Java's combined give-up test
// (attempt >= max || now >= deadline) at QwpQueryClient.java:1541.
func TestQwpFailoverRespectsMaxDuration(t *testing.T) {
	// Both nodes always fail; the attempt cap is set absurdly high so
	// only the wall-clock budget can end the loop.
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, _ = m.conn.Read(ctx)
			m.conn.Close(websocket.StatusInternalError, "always fail")
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 1 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 100000 // never the binding constraint
	cfg.failoverBackoffInitial = 5 * time.Millisecond
	cfg.failoverBackoffMax = 20 * time.Millisecond
	cfg.failoverMaxDuration = 80 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "select 1")
	defer q.Close()

	start := time.Now()
	var terminalErrs []error
	for _, err := range q.Batches() {
		if err == nil {
			continue
		}
		var reset *QwpFailoverReset
		if errors.As(err, &reset) {
			continue
		}
		terminalErrs = append(terminalErrs, err)
	}
	elapsed := time.Since(start)

	if len(terminalErrs) != 1 {
		t.Fatalf("terminalErrors = %d, want 1: %v", len(terminalErrs), terminalErrs)
	}
	terminalErr := terminalErrs[0]
	var exhausted *QwpFailoverExhaustedError
	if !errors.As(terminalErr, &exhausted) {
		t.Fatalf("terminal err = %v (%T), want errors.As to match *QwpFailoverExhaustedError",
			terminalErr, terminalErr)
	}
	// The duration cap, not the attempt cap, must have ended the loop:
	// attempts must be >= 1 and nowhere near failoverMaxAttempts.
	if exhausted.Attempts < 1 || exhausted.Attempts >= cfg.failoverMaxAttempts {
		t.Errorf("exhausted.Attempts = %d, want in [1, %d) — duration budget should bind first",
			exhausted.Attempts, cfg.failoverMaxAttempts)
	}
	if exhausted.LastError == nil {
		t.Error("exhausted.LastError = nil, want the underlying transport error")
	}
	if !strings.Contains(terminalErr.Error(), "failover exhausted") {
		t.Errorf("terminal err = %q, want it to identify failover exhaustion",
			terminalErr.Error())
	}
	if !strings.Contains(terminalErr.Error(), "last error:") {
		t.Errorf("terminal err = %q, want it to include the last transport-failure message",
			terminalErr.Error())
	}
	// Sanity: giving up on the wall-clock budget must be prompt, not a
	// run through 100000 attempts. Generous bound to stay non-flaky on
	// loaded CI while still catching a broken/missing deadline check.
	if elapsed > 3*time.Second {
		t.Errorf("failover took %v, want prompt give-up on the ~80ms budget", elapsed)
	}
}

// TestQwpQueryErrorIsNotRetried verifies the kind-split contract:
// a server-emitted QUERY_ERROR (e.g. a SQL parse error) surfaces
// directly to the user without any failover attempt, even with
// failover enabled. Only client-side transport-terminal events
// trigger the reconnect path.
func TestQwpQueryErrorIsNotRetried(t *testing.T) {
	connectCount := atomic.Int64{}
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			connectCount.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, frame, err := m.conn.Read(ctx)
			_ = frame
			if err != nil {
				return
			}
			// Send QUERY_ERROR with status=ParseError. The kind-
			// split routes this to the user, not to the failover
			// loop.
			body := []byte{byte(qwpMsgKindQueryError)}
			body = appendInt64LE(body, 1) // requestId
			body = append(body, byte(QwpStatusParseError))
			msg := "syntax error"
			body = appendUint16LE(body, uint16(len(msg)))
			body = append(body, msg...)
			m.sendBinary(ctx, writeQwpFrame(0, body))
			// Hold open.
			for {
				if _, _, err := m.conn.Read(ctx); err != nil {
					return
				}
			}
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 2 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 5

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "select bogus")
	defer q.Close()

	var qe *QwpQueryError
	var resetCount int
	for _, err := range q.Batches() {
		if err == nil {
			continue
		}
		var r *QwpFailoverReset
		if errors.As(err, &r) {
			resetCount++
			continue
		}
		errors.As(err, &qe)
	}
	// Initial connect = 1, no replay attempts.
	if got := connectCount.Load(); got != 1 {
		t.Errorf("connectCount = %d, want 1 (no failover for QUERY_ERROR)", got)
	}
	if resetCount != 0 {
		t.Errorf("resetCount = %d, want 0", resetCount)
	}
	if qe == nil {
		t.Fatal("expected *QwpQueryError, got none")
	}
	if qe.Status != QwpStatusParseError {
		t.Errorf("status = 0x%02X, want PARSE_ERROR", byte(qe.Status))
	}
}

// TestQwpExecDefaultDoesNotReplayOnTransportDrop verifies that with
// replayExec=false (the default), a transport drop mid-Exec does NOT
// reconnect-and-resubmit: the (possibly already-applied) statement is
// never silently re-executed on a fresh connection. The caller gets a
// raw transport error (not *QwpFailoverReset) and the second node is
// never contacted.
func TestQwpExecDefaultDoesNotReplayOnTransportDrop(t *testing.T) {
	first := atomic.Bool{}
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, _ = m.conn.Read(ctx)
			if idx == 0 && first.CompareAndSwap(false, true) {
				// Simulate the server having committed the INSERT, then
				// the transport dropping before the EXEC_DONE ack lands.
				m.conn.Close(websocket.StatusInternalError, "fault")
				return
			}
			// Reaching any node other than node 0's first connection
			// means the client reconnected and re-sent the INSERT —
			// exactly the silent double-execution replay_exec=off must
			// prevent. Fail loudly from the server goroutine.
			t.Errorf("node %d received a connection: Exec replayed a "+
				"non-idempotent statement with replay_exec=off", idx)
			for {
				if _, _, err := m.conn.Read(ctx); err != nil {
					return
				}
			}
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 2 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 3
	cfg.failoverBackoffInitial = 1 * time.Millisecond
	cfg.failoverBackoffMax = 5 * time.Millisecond
	cfg.replayExec = false

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	_, err = c.Exec(ctx, "INSERT INTO t VALUES (1)")
	if err == nil {
		t.Fatal("expected a transport error from Exec with replayExec=false")
	}
	// The error must NOT be a failover reset: surfacing one would imply
	// a successful reconnect-and-replay happened.
	var reset *QwpFailoverReset
	if errors.As(err, &reset) {
		t.Fatalf("err is *QwpFailoverReset (%v): replay_exec=off must "+
			"not reconnect-and-replay a non-idempotent Exec", err)
	}
	// Nor a failover-exhausted error: we must bail before any retry
	// budget is consumed, not after exhausting it.
	var exhausted *QwpFailoverExhaustedError
	if errors.As(err, &exhausted) {
		t.Fatalf("err is *QwpFailoverExhaustedError (%v): replay_exec=off "+
			"must not enter the retry loop at all", err)
	}
	// Proof the statement was not re-sent: node 0 was connected exactly
	// once (initial connect, then faulted) and node 1 was never reached.
	if got := cluster.nodes[0].onConnectCount.Load(); got != 1 {
		t.Errorf("node 0 connectCount = %d, want 1 (single submit, no replay)", got)
	}
	if got := cluster.nodes[1].onConnectCount.Load(); got != 0 {
		t.Errorf("node 1 connectCount = %d, want 0 (no reconnect)", got)
	}
}

// TestQwpExecOptInReplaysTransparently verifies that with
// replayExec=true, Exec retries transparently on transport drop and
// returns the new generation's ExecResult to the caller.
func TestQwpExecOptInReplaysTransparently(t *testing.T) {
	first := atomic.Bool{}
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, _ = m.conn.Read(ctx)
			if idx == 0 && first.CompareAndSwap(false, true) {
				m.conn.Close(websocket.StatusInternalError, "fault")
				return
			}
			body := []byte{byte(qwpMsgKindExecDone)}
			body = appendInt64LE(body, 2) // replay requestId
			body = append(body, 0)        // op_type
			body = append(body, 0)        // rowsAffected varint = 0
			m.sendBinary(ctx, writeQwpFrame(0, body))
			for {
				if _, _, err := m.conn.Read(ctx); err != nil {
					return
				}
			}
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 2 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 3
	cfg.failoverBackoffInitial = 1 * time.Millisecond
	cfg.failoverBackoffMax = 5 * time.Millisecond
	cfg.replayExec = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	res, err := c.Exec(ctx, "INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatalf("Exec failed unexpectedly: %v", err)
	}
	_ = res
}

// TestQwpFailoverCancelDuringBackoff verifies that Cancel during the
// failover backoff sleep aborts the replay rather than completing
// the wait. Uses a small but non-trivial backoff so the cancel
// observably interrupts the sleep.
func TestQwpFailoverCancelDuringBackoff(t *testing.T) {
	cluster := newMockCluster(t, 2, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, _ = m.conn.Read(ctx)
			m.conn.Close(websocket.StatusInternalError, "always fail")
		})

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 1 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 5
	cfg.failoverBackoffInitial = 200 * time.Millisecond
	cfg.failoverBackoffMax = 200 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "select 1")
	defer q.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Wait for the first reset to occur, then cancel.
		time.Sleep(50 * time.Millisecond)
		q.Cancel()
	}()

	start := time.Now()
	for _, err := range q.Batches() {
		_ = err
	}
	elapsed := time.Since(start)
	wg.Wait()

	// Without cancel interruption, the test would burn through the
	// full failover budget (5 * 200ms = 1s+). With interruption it
	// should exit much faster.
	if elapsed > 800*time.Millisecond {
		t.Errorf("elapsed = %v, expected fast cancel exit", elapsed)
	}
}

// TestQwpFailoverCancelDuringWalk verifies that Cancel during the
// reconnect's connectWalk phase short-circuits at the next endpoint
// boundary instead of burning a full timeout per remaining endpoint.
// Node 0 succeeds initially and then drops the connection on the
// query; nodes 1..3 hang at SERVER_INFO so each attempted bind costs
// one serverInfoTimeout. Without the boundary cancel poll the walk
// would cost 3 × serverInfoTimeout; with it, the walk exits after one
// timeout once the cancel flag is observed.
func TestQwpFailoverCancelDuringWalk(t *testing.T) {
	cluster := newMockCluster(t, 4, rolesPrimaryReplicaReplica(),
		func(idx int, m *qwpMockEgressConn) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if idx == 0 {
				// Drain the QUERY_REQUEST then close to simulate a
				// transport-terminal fault.
				_, _, _ = m.conn.Read(ctx)
				m.conn.Close(websocket.StatusInternalError, "simulated fault")
				return
			}
			// Nodes 1..3 never reach the handler — suppressServerInfo
			// holds them at the upgrade barrier. Defensive idle loop.
			for {
				if _, _, err := m.conn.Read(ctx); err != nil {
					return
				}
			}
		})
	for i := 1; i < 4; i++ {
		cluster.nodes[i].suppressServerInfo.Store(true)
	}

	cfg := qwpQueryDefaultConfig()
	eps, _ := parseEndpointList(cluster.addrList(), qwpDefaultPort)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 500 * time.Millisecond
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 3
	cfg.failoverBackoffInitial = 1 * time.Millisecond
	cfg.failoverBackoffMax = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "select 1")
	defer q.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Cancel well before the first slow endpoint's timeout fires
		// so the boundary poll has the flag set when the walk
		// progresses to the second slow endpoint.
		time.Sleep(100 * time.Millisecond)
		q.Cancel()
	}()

	start := time.Now()
	for _, err := range q.Batches() {
		_ = err
	}
	elapsed := time.Since(start)
	wg.Wait()

	// Without the boundary poll the first walk visits all three slow
	// endpoints (3 × 500ms = 1.5s); with it the walk exits after the
	// first endpoint's timeout (~500ms) plus negligible overhead. Use
	// 1s as the threshold to give CI machines headroom while still
	// distinguishing the two regimes.
	if elapsed > 1*time.Second {
		t.Errorf("elapsed = %v, expected boundary cancel after one endpoint timeout", elapsed)
	}
}

// TestQwpComputeBackoffFullJitter verifies the egress backoff is
// full-jitter [0, base) per failover.md §3.1 (Java reference
// QwpQueryClient.java:1557-1568): the 1-based double-on-each-step
// schedule, capped at max, sets the ceiling; the returned sleep is
// drawn uniformly below it so co-tenants don't dial in lockstep.
// Sampling-based — it asserts the [0, base) envelope and that the
// draw genuinely spans it, which rules out a regression to a
// deterministic schedule (old behaviour: always == base) or to the
// ingress equal-jitter shape [base, 2·base).
func TestQwpComputeBackoffFullJitter(t *testing.T) {
	cfg := &qwpQueryClientConfig{
		failoverBackoffInitial: 50 * time.Millisecond,
		failoverBackoffMax:     1 * time.Second,
	}
	// base is the pre-jitter ceiling: initial doubled per step,
	// capped at max. computeBackoff must return a draw in [0, base).
	bases := []struct {
		attempt int
		base    time.Duration
	}{
		{1, 50 * time.Millisecond},
		{2, 100 * time.Millisecond},
		{3, 200 * time.Millisecond},
		{4, 400 * time.Millisecond},
		{5, 800 * time.Millisecond},
		{6, 1 * time.Second},  // capped
		{20, 1 * time.Second}, // capped
	}
	const samples = 4000
	for _, tc := range bases {
		minSeen := tc.base
		maxSeen := time.Duration(-1)
		for i := 0; i < samples; i++ {
			got := computeBackoff(cfg, tc.attempt)
			if got < 0 || got >= tc.base {
				t.Fatalf("computeBackoff(attempt=%d) = %v, want [0, %v)",
					tc.attempt, got, tc.base)
			}
			if got < minSeen {
				minSeen = got
			}
			if got > maxSeen {
				maxSeen = got
			}
		}
		// Full-jitter spans [0, base): across thousands of draws the
		// minimum must dip below base/2 and the maximum must rise
		// above it. This is the signature that separates full-jitter
		// from a deterministic return (min==max==base, also caught by
		// the envelope check) and from ingress equal-jitter
		// [base, 2·base) (every draw would be >= base). P(all draws
		// land on one side of base/2) ≈ 2·2^-4000, so neither bound
		// is flaky.
		half := tc.base / 2
		if minSeen >= half {
			t.Errorf("attempt=%d: min sample %v >= base/2 %v; "+
				"expected full-jitter to dip into [0, base/2)",
				tc.attempt, minSeen, half)
		}
		if maxSeen < half {
			t.Errorf("attempt=%d: max sample %v < base/2 %v; "+
				"expected full-jitter to reach into [base/2, base)",
				tc.attempt, maxSeen, half)
		}
	}

	// attempt < 1 means "no sleep before the very first try" — the
	// caller has not yet failed an attempt, so there is nothing to
	// back off from. Zero, never jittered.
	for _, attempt := range []int{0, -1, -100} {
		if got := computeBackoff(cfg, attempt); got != 0 {
			t.Errorf("computeBackoff(attempt=%d) = %v, want 0 "+
				"(no pre-first-try sleep)", attempt, got)
		}
	}

	// initial=0 disables backoff entirely, mirroring Java's
	// `if (failoverInitialBackoffMs > 0L)` guard. Without the
	// early return, the `d <= 0` overflow branch would fall
	// through to max for every attempt >= 1.
	zeroInitial := &qwpQueryClientConfig{
		failoverBackoffInitial: 0,
		failoverBackoffMax:     1 * time.Second,
	}
	for _, attempt := range []int{0, 1, 2, 5, 100} {
		if got := computeBackoff(zeroInitial, attempt); got != 0 {
			t.Errorf("computeBackoff(initial=0, attempt=%d) = %v, want 0",
				attempt, got)
		}
	}

	// A non-positive cap collapses the schedule before the jitter
	// draw: rand.Int63n(0) panics, so the d <= 0 guard must
	// short-circuit to zero. With initial>0 but max=0 the doubling
	// result always exceeds max, forcing d to the non-positive cap.
	zeroMax := &qwpQueryClientConfig{
		failoverBackoffInitial: 50 * time.Millisecond,
		failoverBackoffMax:     0,
	}
	for _, attempt := range []int{1, 2, 5, 100} {
		if got := computeBackoff(zeroMax, attempt); got != 0 {
			t.Errorf("computeBackoff(max=0, attempt=%d) = %v, want 0",
				attempt, got)
		}
	}
}

// gatedQwpServer stands up an httptest WebSocket server that negotiates
// qwpMaxSupportedVersion and emits SERVER_INFO only after `release` is
// closed. onReached is closed (once) the moment a connection has been
// upgraded and is parked waiting for the gate — i.e. the client is now
// blocked inside transport.connect()'s SERVER_INFO read, which (on the
// failover path) means reconnectAndReplay is inside connectWalk holding
// c.genMu. After the gate opens it answers every QUERY_REQUEST with a
// RESULT_END so the consumer terminates cleanly, then signals onClosed
// (once) when the client tears the connection down. The onClosed signal
// is the leak probe: only a generation that something calls shutdown()
// on ever closes its WebSocket.
func gatedQwpServer(t *testing.T, nodeId string, release <-chan struct{},
	onReached, onClosed *sync.Once, reached, closed chan struct{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, fmt.Sprintf("%d", qwpMaxSupportedVersion))
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		onReached.Do(func() { close(reached) })
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		info := buildServerInfoFrame(qwpMaxSupportedVersion, 0, qwpRolePrimary,
			2, 0, time.Now().UnixNano(), "test-cluster", nodeId)
		if err := conn.Write(r.Context(), websocket.MessageBinary, info); err != nil {
			onClosed.Do(func() { close(closed) })
			return
		}
		for {
			typ, frame, err := conn.Read(r.Context())
			if err != nil {
				// Client tore the connection down — the generation that
				// owns this socket had shutdown() called on it.
				onClosed.Do(func() { close(closed) })
				return
			}
			if typ != websocket.MessageBinary || len(frame) < 9 ||
				frame[0] != byte(qwpMsgKindQueryRequest) {
				continue
			}
			reqId := int64(binary.LittleEndian.Uint64(frame[1:9]))
			end := writeQwpFrame(0, buildResultEndBody(reqId, 0, 0))
			end[4] = qwpMaxSupportedVersion // match negotiated version
			if err := conn.Write(r.Context(), websocket.MessageBinary, end); err != nil {
				onClosed.Do(func() { close(closed) })
				return
			}
		}
	}))
}

// TestQwpQueryCloseRacingFailoverDoesNotLeakGeneration is a regression
// test for the close-vs-reconnect leak: Close() running while
// reconnectAndReplay is mid connectWalk used to consume closeOnce
// against the dying generation, after which reconnectAndReplay
// published a fresh generation (reader + dispatcher + waiter goroutines
// + a live WebSocket) that nothing ever called shutdown() on — leaked
// for the process lifetime.
//
// Node A binds initially then drops the connection on the query,
// forcing failover. Node B is the only other candidate and gates its
// SERVER_INFO write, so the test can call Close() with the failover
// provably parked inside connectWalk (holding c.genMu). The fix makes
// Close take c.genMu to set closed + snapshot the bound pair, and makes
// reconnectAndReplay refuse to publish (and self-tear-down) a
// generation built while closing. Either way the failover target's
// WebSocket must end up closed by the client; pre-fix it never was.
func TestQwpQueryCloseRacingFailoverDoesNotLeakGeneration(t *testing.T) {
	var (
		bReleaseGate           = make(chan struct{})
		bReached               = make(chan struct{})
		bClosed                = make(chan struct{})
		bReachedOnce, bClosed1 sync.Once
	)

	// Node A: v2 SERVER_INFO, read the QUERY_REQUEST, then drop the
	// socket to simulate a transport-terminal fault and trigger failover.
	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, fmt.Sprintf("%d", qwpMaxSupportedVersion))
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		info := buildServerInfoFrame(qwpMaxSupportedVersion, 0, qwpRolePrimary,
			1, 0, time.Now().UnixNano(), "test-cluster", "node-a")
		if err := conn.Write(r.Context(), websocket.MessageBinary, info); err != nil {
			return
		}
		_, _, _ = conn.Read(r.Context()) // the QUERY_REQUEST
		conn.Close(websocket.StatusInternalError, "simulated fault")
	}))
	defer nodeA.Close()

	nodeB := gatedQwpServer(t, "node-b", bReleaseGate,
		&bReachedOnce, &bClosed1, bReached, bClosed)
	defer nodeB.Close()

	cfg := qwpQueryDefaultConfig()
	eps, err := parseEndpointList(
		strings.TrimPrefix(nodeA.URL, "http://")+","+
			strings.TrimPrefix(nodeB.URL, "http://"), qwpDefaultPort)
	if err != nil {
		t.Fatalf("parseEndpointList: %v", err)
	}
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 5 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 3
	cfg.failoverBackoffInitial = 1 * time.Millisecond
	cfg.failoverBackoffMax = 5 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	if err != nil {
		t.Fatalf("newQwpQueryClient: %v", err)
	}
	defer c.Close(ctx)

	if c.CurrentEndpoint() != strings.TrimPrefix(nodeA.URL, "http://") {
		t.Fatalf("initial bind = %s, want node A", c.CurrentEndpoint())
	}

	var qwg sync.WaitGroup
	qwg.Add(1)
	go func() {
		defer qwg.Done()
		qctx, qcancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer qcancel()
		q := c.Query(qctx, "select 1")
		defer q.Close()
		for _, err := range q.Batches() {
			if err == nil {
				continue
			}
			var reset *QwpFailoverReset
			if errors.As(err, &reset) {
				continue // consume the new generation's frames
			}
			// Any terminal error (incl. the close-during-failover
			// transport error) ends iteration — that is expected here.
			break
		}
	}()

	// Wait until the failover reconnect is provably parked inside
	// connectWalk on node B (holding c.genMu), then Close from another
	// goroutine — the exact interleaving that used to leak.
	select {
	case <-bReached:
	case <-time.After(10 * time.Second):
		t.Fatal("failover did not reach node B")
	}

	closeDone := make(chan error, 1)
	go func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		closeDone <- c.Close(cctx)
	}()
	// Best-effort nudge so Close() is blocked on c.genMu while
	// reconnectAndReplay still holds it (the most interesting
	// interleaving). Not a correctness requirement — every interleaving
	// is leak-free post-fix.
	time.Sleep(75 * time.Millisecond)
	close(bReleaseGate)

	// The leak probe: post-fix the freshly built generation is torn
	// down (by Close's snapshot, or by reconnectAndReplay's self-
	// teardown), so node B's WebSocket is closed by the client. Pre-fix
	// nothing ever calls shutdown() on it and this never fires.
	select {
	case <-bClosed:
	case <-time.After(6 * time.Second):
		t.Fatal("regression: failover-target connection was never closed " +
			"by the client — reconnectAndReplay published a generation " +
			"that Close() leaked")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close returned %v, want nil", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Close did not return")
	}

	done := make(chan struct{})
	go func() { qwg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("query goroutine did not unwind after Close")
	}

	if !c.closed.Load() {
		t.Error("client closed flag not set after Close")
	}
	q := c.Query(ctx, "select 1")
	var sawClosed bool
	for _, err := range q.Batches() {
		if err != nil && strings.Contains(err.Error(), "closed") {
			sawClosed = true
		}
	}
	q.Close()
	if !sawClosed {
		t.Error("Query after Close did not surface a closed-client error")
	}
}
