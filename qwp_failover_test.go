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
			frame := buildServerInfoFrame(qwpMaxSupportedVersion, 0,
				mn.role, uint64(idx+1), 0, time.Now().UnixNano(),
				mn.clusterId, mn.nodeId)
			if err := conn.Write(r.Context(), websocket.MessageBinary, frame); err != nil {
				t.Logf("mock node %d: SERVER_INFO write: %v", idx, err)
				return
			}
			mc := &qwpMockEgressConn{t: t, conn: conn}
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
// failoverMaxAttempts the iterator surfaces the underlying
// transport error rather than looping forever.
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

	var resets, terminalErrors int
	for _, err := range q.Batches() {
		if err == nil {
			continue
		}
		var reset *QwpFailoverReset
		if errors.As(err, &reset) {
			resets++
			continue
		}
		terminalErrors++
	}
	if terminalErrors != 1 {
		t.Errorf("terminalErrors = %d, want 1", terminalErrors)
	}
	// Resets should be < failoverMaxAttempts because the budget
	// includes the initial submission.
	if resets >= cfg.failoverMaxAttempts {
		t.Errorf("resets = %d, expected < failoverMaxAttempts (%d)",
			resets, cfg.failoverMaxAttempts)
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
			body = append(body, byte(qwpStatusParseError))
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
	if qe.Status != qwpStatusParseError {
		t.Errorf("status = 0x%02X, want PARSE_ERROR", byte(qe.Status))
	}
}

// TestQwpExecDefaultSurfacesFailoverReset verifies that with
// replayExec=false (the default), Exec returns *QwpFailoverReset
// when a transport drop triggers a successful reconnect — the
// caller sees the reset and decides whether to retry.
func TestQwpExecDefaultSurfacesFailoverReset(t *testing.T) {
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
			// Node 1 ack with EXEC_DONE. With replayExec=false, the
			// client never consumes this — Exec returns the
			// *QwpFailoverReset error before observing the new
			// generation's response. Best-effort write so a closed
			// conn after the test returned does not flag the test
			// as failed.
			body := []byte{byte(qwpMsgKindExecDone)}
			body = appendInt64LE(body, 2)
			body = append(body, 0)
			body = append(body, 0)
			_ = m.conn.Write(ctx, websocket.MessageBinary,
				writeQwpFrame(0, body))
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
		t.Fatal("expected *QwpFailoverReset error from Exec with replayExec=false")
	}
	var reset *QwpFailoverReset
	if !errors.As(err, &reset) {
		t.Fatalf("err = %v (%T), want *QwpFailoverReset", err, err)
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

// TestQwpComputeBackoffMonotonic pins the schedule against the Java
// reference: 1-based attempts, double-on-each-step, capped at max.
func TestQwpComputeBackoffMonotonic(t *testing.T) {
	cfg := &qwpQueryClientConfig{
		failoverBackoffInitial: 50 * time.Millisecond,
		failoverBackoffMax:     1 * time.Second,
	}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 0},
		{1, 50 * time.Millisecond},
		{2, 100 * time.Millisecond},
		{3, 200 * time.Millisecond},
		{4, 400 * time.Millisecond},
		{5, 800 * time.Millisecond},
		{6, 1 * time.Second},  // capped
		{20, 1 * time.Second}, // capped
	}
	for _, tc := range cases {
		got := computeBackoff(cfg, tc.attempt)
		if got != tc.want {
			t.Errorf("computeBackoff(attempt=%d) = %v, want %v",
				tc.attempt, got, tc.want)
		}
	}

	// initial=0 disables backoff entirely, mirroring Java's
	// `if (failoverInitialBackoffMs > 0L)` guard. Without the
	// early return, the `d <= 0` overflow branch would fall
	// through to max for every attempt >= 1.
	zeroCfg := &qwpQueryClientConfig{
		failoverBackoffInitial: 0,
		failoverBackoffMax:     1 * time.Second,
	}
	for _, attempt := range []int{0, 1, 2, 5, 100} {
		if got := computeBackoff(zeroCfg, attempt); got != 0 {
			t.Errorf("computeBackoff(initial=0, attempt=%d) = %v, want 0",
				attempt, got)
		}
	}
}
