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
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// connect_timeout is a COMMON key: the parser accepts it on every schema so a
// shared connect string ports from the Java client. It is wired on HTTP and
// QWP; the TCP ILP dial leaves it inert (matching the Java client, where only
// HttpClient.connect / WebSocketClient.doConnect honour it).
func TestConnectTimeoutParsedOnAllSchemas(t *testing.T) {
	for _, schema := range []string{"http", "https", "tcp", "tcps", "ws", "wss"} {
		conf := schema + "::addr=localhost:9000;connect_timeout=2500;"
		c, err := confFromStr(conf)
		if err != nil {
			t.Fatalf("%s: confFromStr: %v", schema, err)
		}
		if c.connectTimeoutMs != 2500 {
			t.Errorf("%s: connectTimeoutMs=%d, want 2500", schema, c.connectTimeoutMs)
		}
	}
}

func TestConnectTimeoutRejectsNonPositive(t *testing.T) {
	for _, v := range []string{"0", "-1", "abc", "5.5"} {
		if _, err := confFromStr("ws::addr=a:9000;connect_timeout=" + v + ";"); err == nil {
			t.Errorf("connect_timeout=%q: expected error, got nil", v)
		}
	}
}

func TestConnectTimeoutOptionIngest(t *testing.T) {
	c := newLineSenderConfig(qwpSenderType)
	WithConnectTimeout(750 * time.Millisecond)(c)
	if c.connectTimeoutMs != 750 {
		t.Errorf("connectTimeoutMs=%d, want 750", c.connectTimeoutMs)
	}
}

func TestConnectTimeoutEgressConf(t *testing.T) {
	c, err := parseQwpQueryConf("ws::addr=a:9000;connect_timeout=1200;")
	if err != nil {
		t.Fatalf("parseQwpQueryConf: %v", err)
	}
	if c.connectTimeoutMs != 1200 {
		t.Errorf("connectTimeoutMs=%d, want 1200", c.connectTimeoutMs)
	}
	for _, v := range []string{"0", "-1", "abc"} {
		if _, err := parseQwpQueryConf("ws::addr=a:9000;connect_timeout=" + v + ";"); err == nil {
			t.Errorf("connect_timeout=%q: expected error, got nil", v)
		}
	}
}

func TestConnectTimeoutEgressOption(t *testing.T) {
	c := qwpQueryDefaultConfig()
	WithQwpQueryConnectTimeout(900 * time.Millisecond)(c)
	if c.connectTimeoutMs != 900 {
		t.Errorf("connectTimeoutMs=%d, want 900", c.connectTimeoutMs)
	}
}

// blackholeAddr is TEST-NET-1 (RFC 5737): on a normal network the SYN is
// silently dropped, so a TCP connect stalls until our connect_timeout fires —
// long before the OS connect timeout (60-120s). When the runner has no route,
// the connect fast-fails instead and the test skips rather than flakes, mirroring
// Java's NetConnectTimeoutTest / QwpQueryClientConnectTimeoutTest.
const blackholeAddr = "192.0.2.1:9009"

// assertBoundedConnectFailure skips on a fast no-route fail (the connect_timeout
// path was not exercised) and otherwise asserts the dial bailed well inside the
// budget instead of riding the OS connect timeout.
func assertBoundedConnectFailure(t *testing.T, elapsed time.Duration, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected connect to a black-hole host to fail")
	}
	if elapsed < 200*time.Millisecond {
		t.Skipf("TEST-NET-1 fast-failed (no black-hole route) in %v: %v", elapsed, err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("connect_timeout not honored: bailed after %v (want < 5s): %v", elapsed, err)
	}
}

func TestConnectTimeoutQwpIngestBlackhole(t *testing.T) {
	// Default initial-connect mode is OFF: the constructor dials once and
	// surfaces the failure, so connect_timeout bounds that single dial.
	start := time.Now()
	_, err := LineSenderFromConf(context.Background(),
		"ws::addr="+blackholeAddr+";connect_timeout=500;")
	assertBoundedConnectFailure(t, time.Since(start), err)
}

func TestConnectTimeoutQwpQueryBlackhole(t *testing.T) {
	start := time.Now()
	_, err := QwpQueryClientFromConf(context.Background(),
		"ws::addr="+blackholeAddr+";connect_timeout=500;auth_timeout_ms=15000;failover=off;target=any;")
	elapsed := time.Since(start)
	assertBoundedConnectFailure(t, elapsed, err)
	// A connect-phase timeout must read as a connect failure, never be relabeled
	// as an auth_timeout overage (Java QwpQueryClientConnectTimeoutTest).
	if strings.Contains(err.Error(), "auth_timeout") {
		t.Fatalf("connect-phase timeout misreported as auth_timeout: %v", err)
	}
}

// TestConnectTimeoutHttpUsesPrivateTransport covers the HTTP connect_timeout
// branch deterministically: when connect_timeout is set, the sender must build
// a private transport (carrying the net.Dialer timeout) rather than share the
// global one — the global dialer must never be mutated for a single sender.
// (protocol_version=2 skips the version-detect request, so this touches no
// network; the dialer-bounds-connect behavior itself is proven by the QWP
// black-hole tests, which share the same net.Dialer{Timeout} mechanism.)
func TestConnectTimeoutHttpUsesPrivateTransport(t *testing.T) {
	c, err := confFromStr("http::addr=192.0.2.1:9009;protocol_version=2;connect_timeout=500;")
	if err != nil {
		t.Fatalf("conf: %v", err)
	}
	ls, err := newHttpLineSender(context.Background(), c)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer ls.Close(context.Background())
	v2, ok := ls.(*httpLineSenderV2)
	if !ok {
		t.Fatalf("unexpected sender type %T", ls)
	}
	if v2.globalTransport != nil {
		t.Error("a connect_timeout HTTP sender must use a private transport, not the shared global")
	}
}

// TestConnectTimeoutHttpCustomTransportNotMutated covers the http_sender.go
// guard (`&& conf.httpTransport == nil`): a caller-supplied transport is owned by
// the caller, so connect_timeout must NOT overwrite its DialContext even when set
// — doing so would silently replace the caller's custom dialer. We prove the
// custom DialContext survives by invoking it after construction: the sentinel
// fires only if the field was left intact (a net.Dialer would have replaced it).
func TestConnectTimeoutHttpCustomTransportNotMutated(t *testing.T) {
	var dialed atomic.Bool
	sentinel := errors.New("sentinel dialer")
	custom := newHttpTransport()
	custom.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dialed.Store(true)
		return nil, sentinel
	}

	// protocol_version=2 skips the version-detect request, so no network is
	// touched at build time. httpTransport != nil + connect_timeout set is the
	// guarded path.
	c, err := confFromStr("http::addr=192.0.2.1:9009;protocol_version=2;connect_timeout=500;")
	if err != nil {
		t.Fatalf("conf: %v", err)
	}
	c.httpTransport = custom

	ls, err := newHttpLineSender(context.Background(), c)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer ls.Close(context.Background())

	v2, ok := ls.(*httpLineSenderV2)
	if !ok {
		t.Fatalf("unexpected sender type %T", ls)
	}
	tr, ok := v2.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", v2.client.Transport)
	}
	if tr != custom {
		t.Fatal("sender did not use the caller-supplied custom transport")
	}
	// The sender must not own (and later close) a caller-supplied transport.
	if v2.ownedTransport != nil {
		t.Error("a caller-supplied transport must not be adopted as ownedTransport")
	}
	// Invoke the transport's dialer: it must still be the caller's sentinel, not a
	// connect_timeout net.Dialer that replaced it.
	if _, err := tr.DialContext(context.Background(), "tcp", "127.0.0.1:9"); !errors.Is(err, sentinel) {
		t.Errorf("DialContext err=%v, want sentinel — connect_timeout overwrote the custom dialer", err)
	}
	if !dialed.Load() {
		t.Error("custom DialContext was not invoked — connect_timeout replaced it")
	}
}
