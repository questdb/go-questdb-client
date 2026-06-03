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
	"encoding/base64"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

// qwpQueryCleanupDrainTimeout bounds the drain that happens on
// close-path cleanup (QwpQuery.Close, iterator break-out, Exec-on-
// SELECT misuse). Deliberately independent of the caller's context so
// the dispatcher returns to idle and the client stays usable for a
// follow-up Query/Exec even when the caller's ctx has already expired
// by the time cleanup runs. 5s matches the Java client's
// shutdownJoinMs default.
const qwpQueryCleanupDrainTimeout = 5 * time.Second

// QwpQueryClient is a QuestDB query-side (egress) client. It opens one
// WebSocket connection to /read/v1, runs a dedicated I/O goroutine
// pair (reader + dispatcher), and streams result batches to the caller
// via Query/Exec. The I/O goroutines read and decode ahead of the
// consumer up to the configured buffer-pool depth.
//
// Thread safety: not safe for concurrent Query or Exec calls on the
// same client. Open one client per query-issuing goroutine. Cancel
// (on the returned *QwpQuery) and Close are safe to call from other
// goroutines.
type QwpQueryClient struct {
	cfg *qwpQueryClientConfig

	// transportPtr and ioPtr are atomically replaced by the failover
	// orchestrator on reconnect. The session reads through the
	// transport() / io() accessors so a swap mid-Query is observed
	// as a clean generation boundary. Both pointers are set during
	// construction (newQwpQueryClient) and never nil while the
	// client is live.
	transportPtr atomic.Pointer[qwpTransport]
	ioPtr        atomic.Pointer[qwpEgressIO]

	// genMu serialises generation lifecycle transitions: the
	// destroy-old / build-new pair in reconnectAndReplay, and Close's
	// set-closed + snapshot of the bound (transport, io) pair. nextEvent
	// reads the atomic pointers under no lock; reconnect and Close grab
	// this mutex so a transport fault cannot publish a fresh generation
	// that a concurrent Close would never observe (and so leak forever),
	// and so Close always tears down a consistent generation pair rather
	// than a torn read straddling publishGeneration. Held only across the
	// reconnect critical section and Close's flag-set+snapshot — never
	// across a user-facing wait, since the I/O shutdown in both runs
	// after the mutex is released.
	genMu sync.Mutex

	// hostTracker is the failover.md §2 host-health / zone tracker
	// shared by the initial connect and every failover reconnect. It
	// drives endpoint selection via the (state, zone) priority lattice
	// — the `zone=` locality hint is effective here (the SF ingress
	// tracker is zone-blind by contrast). Constructed once in
	// newQwpQueryClient and never replaced; its state (sticky-Healthy,
	// topology classifications) deliberately persists across
	// reconnects for the client's lifetime. Thread-safe internally.
	hostTracker *qwpHostTracker

	// currentEndpointIdx tracks the index in cfg.endpoints currently
	// bound. -1 before construction completes, set by connectWalk and
	// updated by reconnectAndReplay. Read by the failover orchestrator
	// to feed RecordMidStreamFailure with the just-failed index before
	// the reconnect walk.
	currentEndpointIdx atomic.Int32
	// serverInfo holds the SERVER_INFO from the bound generation.
	// Nil on v1 connections. Written by connectWalk and
	// reconnectAndReplay; read via the public ServerInfo() accessor.
	serverInfo atomic.Pointer[QwpServerInfo]

	// nextRequestId is the monotonic client-assigned request id
	// handed to the I/O goroutine on each submit. Assigned from the
	// user goroutine inside Query/Exec; not accessed from other
	// goroutines (one query at a time).
	nextRequestId int64

	// binds is the reusable typed bind-parameter sink. Populated on
	// the user goroutine by the QwpBindFunc passed to Query / Exec.
	// buildRequest copies the encoded bytes into a fresh per-request
	// slice before handing the request to the I/O goroutine, so a
	// follow-up query's reset + re-encode cannot race the dispatcher.
	binds QwpBinds

	// closed guards Close against double-close and later Query/Exec.
	closed atomic.Bool
	// closeOnce ensures the teardown side effects (I/O shutdown,
	// transport close) run at most once even under concurrent Close
	// callers.
	closeOnce sync.Once
}

// transport returns the bound generation's transport. Callers should
// re-load on every use rather than caching, since the pointer is
// swapped atomically on transparent failover. Never returns nil for
// a live client; Close stores nil but the closed flag short-circuits
// any subsequent call before transport() is read.
func (c *QwpQueryClient) transport() *qwpTransport {
	return c.transportPtr.Load()
}

// io returns the bound generation's I/O goroutine pair. See transport().
func (c *QwpQueryClient) io() *qwpEgressIO {
	return c.ioPtr.Load()
}

// publishGeneration atomically swaps the bound transport + I/O + the
// connect-walk metadata. Used by both the initial connect path and
// the failover reconnect path so the publish ordering stays
// consistent across both. Holds genMu so two concurrent transport
// faults cannot both spawn a new generation.
func (c *QwpQueryClient) publishGeneration(r *qwpConnectResult) {
	c.transportPtr.Store(r.transport)
	c.ioPtr.Store(r.io)
	c.currentEndpointIdx.Store(int32(r.endpointIdx))
	c.serverInfo.Store(r.serverInfo)
}

// ServerInfo returns the SERVER_INFO frame consumed during the bound
// generation's WebSocket handshake, or nil if the negotiated version
// is v1 (no SERVER_INFO emitted). The returned pointer is owned by
// the client and is replaced atomically on each transparent failover
// reconnect; callers that need to retain a value across a possible
// reconnect should copy out the fields.
func (c *QwpQueryClient) ServerInfo() *QwpServerInfo {
	return c.serverInfo.Load()
}

// CurrentEndpoint returns the host:port string of the endpoint the
// client is currently bound to. Updated atomically on each transparent
// failover reconnect. Returns the empty string before the constructor
// has completed.
func (c *QwpQueryClient) CurrentEndpoint() string {
	idx := int(c.currentEndpointIdx.Load())
	if idx < 0 || idx >= len(c.cfg.endpoints) {
		return ""
	}
	return c.cfg.endpoints[idx].String()
}

// QwpBindFunc populates the typed bind parameters for a single Query
// or Exec call. The function is invoked on the caller's goroutine
// before the query is submitted. Setters must be invoked in strictly
// ascending index order starting at 0; the latched error on QwpBinds
// is surfaced as the query's first result.
type QwpBindFunc func(*QwpBinds)

// QueryOption is a functional option for Query / Exec that attaches
// per-call settings — currently just bind parameters.
type QueryOption func(*qwpQueryOptions)

// qwpQueryOptions collects the effective settings for a single Query
// or Exec invocation. Private so the public surface is the option
// constructors, not the struct itself.
type qwpQueryOptions struct {
	bindFn QwpBindFunc
}

// WithQueryBinds attaches a bind-parameter setter to a Query or Exec call.
// The setter runs on the caller's goroutine and receives a reusable
// *QwpBinds sink. Placeholders in the SQL text are $1, $2, ...; the
// corresponding setter calls use 0-based indexes. Setters must be
// invoked in strictly ascending index order with no gaps; a duplicate
// or out-of-order index surfaces the error through the query result.
func WithQueryBinds(fn QwpBindFunc) QueryOption {
	return func(o *qwpQueryOptions) { o.bindFn = fn }
}

// QwpQueryClientOption is a functional option for NewQwpQueryClient.
// Deliberately a distinct type from LineSenderOption — the two clients
// share no transport code above qwpTransport, and using a different
// option type prevents misuse (e.g. passing an ingest option to the
// query constructor).
type QwpQueryClientOption func(*qwpQueryClientConfig)

// WithQwpQueryAddress overrides the default "localhost:9000" server
// address. Accepts a single "host:port" or a comma-separated list of
// endpoints; the latter is equivalent to WithQwpQueryEndpoints. The
// connect walk uses the first endpoint matching the target= filter.
// Errors during parsing are deferred to validate(), so a malformed
// addr surfaces from the client constructor.
func WithQwpQueryAddress(addr string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) {
		eps, err := parseEndpointList(addr, qwpDefaultPort)
		if err != nil {
			// Stash a sentinel single-entry list with the bad address
			// so validate() surfaces a useful error from the
			// originating field; the err itself is not wired through
			// the options API. Keep at least one entry so validate's
			// "no endpoints" path is not also tripped.
			c.endpoints = []qwpEndpoint{{host: addr, port: 0}}
			return
		}
		c.endpoints = eps
	}
}

// WithQwpQueryEndpoints sets the ordered list of endpoints the connect
// walk attempts. Each entry is a "host[:port]" string; missing port
// defaults to qwpDefaultPort. Errors during parsing are deferred to
// validate() so the client constructor surfaces them. Use this option
// when the configured endpoints are typed at the call site (e.g., a
// service-discovery layer); WithQwpQueryAddress with a comma-separated
// list is equivalent.
func WithQwpQueryEndpoints(addrs ...string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) {
		joined := strings.Join(addrs, ",")
		eps, err := parseEndpointList(joined, qwpDefaultPort)
		if err != nil {
			c.endpoints = []qwpEndpoint{{host: joined, port: 0}}
			return
		}
		c.endpoints = eps
	}
}

// WithQwpQueryEndpointPath overrides the default "/read/v1" WebSocket
// upgrade path. Rarely needed — present for parity with Java's
// withEndpointPath and to support reverse-proxy rewrites.
func WithQwpQueryEndpointPath(path string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.endpointPath = path }
}

// WithQwpQueryAuth sets the raw Authorization HTTP header value sent
// on the WebSocket upgrade. Mutually exclusive with
// WithQwpQueryBasicAuth and WithQwpQueryBearerToken.
func WithQwpQueryAuth(authHeader string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.authorization = authHeader }
}

// WithQwpQueryBasicAuth enables HTTP Basic authentication. The server
// validates against the same user store that the Postgres wire
// protocol uses — a user created via CREATE USER ... WITH PASSWORD ...
// works unchanged.
func WithQwpQueryBasicAuth(username, password string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) {
		c.httpUser = username
		c.httpPass = password
	}
}

// WithQwpQueryBearerToken enables HTTP Bearer authentication with an
// OIDC access token. The server verifies the token via its configured
// OIDC provider.
func WithQwpQueryBearerToken(token string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.httpToken = token }
}

// WithQwpQueryClientID overrides the default X-QWP-Client-Id header
// sent on the WebSocket upgrade. Empty uses the module default.
func WithQwpQueryClientID(id string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.clientID = id }
}

// WithQwpQueryBufferPoolSize overrides the decode buffer pool depth.
// Larger pools let the dispatcher decode further ahead of a slow
// consumer; smaller pools reduce memory but stall the dispatcher
// sooner. Must be >= 1.
func WithQwpQueryBufferPoolSize(size int) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.bufferPoolSize = size }
}

// WithQwpQueryMaxBatchRows asks the server to cap each RESULT_BATCH
// at the given row count. 0 omits the header and lets the server use
// its own cap. Useful for latency-sensitive streaming consumers that
// want the first rows sooner.
func WithQwpQueryMaxBatchRows(rows int) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.maxBatchRows = rows }
}

// WithQwpQueryInitialCredit opts the next query into credit-based
// egress flow control with the given initial byte budget. The server
// streams at most `bytes` of result payload before pausing; the
// client auto-replenishes by the size of each batch after the
// consumer releases it. 0 (the default) disables flow control.
func WithQwpQueryInitialCredit(bytes int64) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.initialCredit = bytes }
}

// WithQwpQueryCompression selects the compression codec advertised to
// the server on the WebSocket upgrade. Accepted values: "raw" (default,
// no compression, accept-encoding header omitted), "zstd" (demand zstd,
// fall back to raw if the server cannot), "auto" (advertise both and
// let the server pick). Anything else surfaces as an error from the
// constructor. Matches Java QwpQueryClient.withCompression's
// preference argument.
func WithQwpQueryCompression(preference string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.compression = preference }
}

// WithQwpQueryCompressionLevel overrides the zstd compression level
// hint the client sends in the accept-encoding header. Ignored when
// the compression preference is "raw". Accepts [1, 22] matching
// Java; the server clamps down to its own supported range.
func WithQwpQueryCompressionLevel(level int) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.compressionLevel = level }
}

// WithQwpQueryTls enables TLS with full certificate validation against
// the system cert pool.
func WithQwpQueryTls() QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.tlsMode = tlsEnabled }
}

// WithQwpQueryTarget restricts the connect walk to endpoints whose
// SERVER_INFO.role passes the given filter. Accepts "any" (default,
// matches any role), "primary" (STANDALONE | PRIMARY |
// PRIMARY_CATCHUP), or "replica" (REPLICA only). Mirrors Java's
// withTarget. An invalid value is deferred to validate(): the client
// constructor surfaces the error.
//
// target=primary or replica forces v2 negotiation: a v1 server has
// no SERVER_INFO and cannot satisfy a role-specific filter.
func WithQwpQueryTarget(target string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) {
		t, err := parseTargetFilter(target)
		if err != nil {
			// Stash an out-of-range sentinel; validate() turns this
			// into a typed error from the client constructor.
			c.target = qwpTargetFilter(255)
			return
		}
		c.target = t
	}
}

// WithQwpQueryFailover toggles transparent reconnect-and-replay on
// transport-terminal failure mid-query. Default true; matches Java's
// failover=on default. When false, transport errors surface directly
// through Batches() / Exec().
func WithQwpQueryFailover(enabled bool) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.failoverEnabled = enabled }
}

// WithQwpQueryFailoverMaxAttempts caps the number of executeOnce
// invocations per Query / Exec call. Counts the initial attempt plus
// every reconnect retry. Must be >= 1; the default
// (qwpDefaultFailoverMaxAttempts = 8) matches Java.
func WithQwpQueryFailoverMaxAttempts(n int) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.failoverMaxAttempts = n }
}

// WithQwpQueryFailoverBackoff sets the exponential backoff between
// reconnect attempts. initial is the first sleep (doubled per retry);
// max is the ceiling. Defaults match Java
// (qwpDefaultFailoverInitialBackoff = 50ms,
// qwpDefaultFailoverMaxBackoff = 1s).
func WithQwpQueryFailoverBackoff(initial, max time.Duration) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) {
		c.failoverBackoffInitial = initial
		c.failoverBackoffMax = max
	}
}

// WithQwpQueryFailoverMaxDuration caps the total wall-clock time the
// per-Query / Exec failover loop spends reconnecting and replaying.
// Whichever of this or WithQwpQueryFailoverMaxAttempts fires first
// ends the loop. 0 disables the time cap (failover then bounded only
// by attempts). Must be >= 0; the default
// (qwpDefaultFailoverMaxDuration = 30s) matches Java's
// DEFAULT_FAILOVER_MAX_DURATION_MS.
func WithQwpQueryFailoverMaxDuration(d time.Duration) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.failoverMaxDuration = d }
}

// WithQwpQueryServerInfoTimeout overrides the SERVER_INFO read
// deadline applied during each WebSocket upgrade. Default
// qwpDefaultServerInfoTimeout (5s) matches Java's
// DEFAULT_SERVER_INFO_TIMEOUT_MS. Must be > 0: the server always emits
// SERVER_INFO as the first post-upgrade frame, so skipping the
// synchronous drain would leave that frame in the recv buffer where
// the I/O loop would later misread it as a query response.
func WithQwpQueryServerInfoTimeout(d time.Duration) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.serverInfoTimeout = d }
}

// WithQwpQueryZone sets the client's opaque, case-insensitive
// locality hint (failover.md §1.1). When set and target != primary,
// the connect/reconnect walk prefers endpoints whose server-advertised
// zone (SERVER_INFO.zone_id under CAP_ZONE, or the X-QuestDB-Zone
// header on a 421 reject) matches, via the (state, zone) priority
// lattice. Empty (the default) is zone-blind. Mirrors the ingest
// WithQwpZone / zone= key so a connect string can be shared verbatim.
func WithQwpQueryZone(zone string) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.zone = zone }
}

// WithQwpQueryAuthTimeout overrides the per-host upgrade-response-read
// bound (failover.md §1.1). It bounds only the wait between writing
// the WebSocket upgrade request and reading the response headers — not
// TCP connect, TLS handshake, or the SERVER_INFO read (see
// WithQwpQueryServerInfoTimeout). Must be > 0; the default
// (qwpDefaultAuthTimeoutMs = 15s) matches the ingest client and Java.
// Sub-millisecond durations round down and are rejected by validate().
func WithQwpQueryAuthTimeout(d time.Duration) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) {
		c.authTimeoutMs = int(d.Milliseconds())
	}
}

// WithQwpQueryReplayExec opts Exec into transparent replay on
// transport-terminal failure. Default false because non-idempotent
// statements (INSERT / UPDATE / DELETE / DDL) might double-execute
// if the server applied the statement before the transport drop was
// detected. Callers that know their statements are idempotent can
// opt in to match Java's transparent replay behaviour.
func WithQwpQueryReplayExec(enabled bool) QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.replayExec = enabled }
}

// WithQwpQueryTlsInsecureSkipVerify enables TLS but skips certificate
// validation. Intended for testing only.
func WithQwpQueryTlsInsecureSkipVerify() QwpQueryClientOption {
	return func(c *qwpQueryClientConfig) { c.tlsMode = tlsInsecureSkipVerify }
}

// NewQwpQueryClient constructs a QwpQueryClient from functional options
// and opens the WebSocket connection. Matches Java
// QwpQueryClient.newPlainText + connect(), but bundled into one call
// since Go does not usually separate construction from connection.
func NewQwpQueryClient(ctx context.Context, opts ...QwpQueryClientOption) (*QwpQueryClient, error) {
	cfg := qwpQueryDefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return newQwpQueryClient(ctx, cfg)
}

// QwpQueryClientFromConf constructs a QwpQueryClient from a ws:: /
// wss:: config string and opens the WebSocket connection. See
// parseQwpQueryConf for the full key reference.
func QwpQueryClientFromConf(ctx context.Context, conf string) (*QwpQueryClient, error) {
	cfg, err := parseQwpQueryConf(conf)
	if err != nil {
		return nil, err
	}
	return newQwpQueryClient(ctx, cfg)
}

// newQwpQueryClient is the internal factory shared by both public
// entry points. It performs validation, runs the multi-endpoint
// connect walk, and spawns the I/O goroutines for the bound
// generation. The walk applies the target= role filter against the
// SERVER_INFO frame each endpoint emits.
func newQwpQueryClient(ctx context.Context, cfg *qwpQueryClientConfig) (*QwpQueryClient, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// Early probe: if we told the server we can accept zstd, round-
	// trip a transient decoder so any klauspost/compress init failure
	// surfaces here on the user goroutine rather than mid-stream on
	// the first compressed batch. Matches Java's probeZstdAvailable
	// in intent; cheaper in pure Go since there is no JNI library to
	// load. Run before the dial so a misbehaving zstd binding does
	// not leak a half-open WebSocket.
	if cfg.compression != qwpCompressionRaw {
		if err := probeZstdAvailable(); err != nil {
			return nil, err
		}
	}

	c := &QwpQueryClient{
		cfg:           cfg,
		nextRequestId: 1, // match Java's QwpQueryClient.nextRequestId initial value
		// Fresh tracker: every host starts Unknown with attempted=false,
		// so the first PickNext sweep walks the addr= list in order
		// (failover.md §2 selection priority — ties break on the
		// user-supplied order). zone= and target= shape the (state,
		// zone) lattice from here on. Mirrors Java connect()'s
		// hostTracker==null branch (no BeginRound on a fresh tracker).
		hostTracker: newQwpHostTracker(len(cfg.endpoints), cfg.zone, cfg.target),
	}
	c.currentEndpointIdx.Store(-1)

	// allowFallthroughReset=false: initial connect probes each endpoint
	// exactly once (Java connect() parity), no re-sweep on a uniformly
	// rejecting cluster.
	result, err := connectWalk(ctx, cfg, c.hostTracker, nil, false)
	if err != nil {
		return nil, err
	}
	c.publishGeneration(result)
	return c, nil
}

// errClosedDuringFailover is the typed cause surfaced to the in-flight
// query when Close races a reconnect: the client is shutting down, so
// the failover loop must terminate rather than bind a fresh generation
// nothing will ever tear down. Distinct from the "client is closed"
// string returned by Query/Exec at submit time so logs can tell a
// close-before-submit apart from a close-mid-failover.
var errClosedDuringFailover = errors.New(
	"qwp query: client closed during failover")

// reconnectAndReplay tears down the current generation, demotes the
// just-failed endpoint and walks the host tracker by (state, zone)
// priority (failover.md §2; the demoted host drops to TransportError
// so a healthier or same-zone peer is preferred, but it stays a
// candidate and is retried if nothing better binds — including the
// n=1 case), publishes the new generation, and resubmits the
// in-flight query with a fresh requestId. Returns the new
// generation's QwpServerInfo (nil for v1) or a non-nil error if the
// walk fails. Holds c.genMu for the duration of the swap so two
// concurrent transport faults serialise and so a concurrent Close
// cannot interleave with the swap.
//
// Close coordination: Close sets c.closed and snapshots the bound
// generation under c.genMu. Because this function holds c.genMu for
// its whole body, c.closed cannot change underneath it, so a single
// check before any work decides the outcome:
//
//   - closed already set (Close won the lock first): Close has
//     already snapshotted and owns teardown of the bound generation.
//     Bail before touching it (a second teardown here would race
//     Close's unlocked tr.close()) and before standing up a fresh
//     generation Close could never reach.
//
//   - closed set only after this returns (Close is blocked on
//     c.genMu): we publish normally; Close then snapshots and tears
//     down the generation we just published.
//
// The post-connectWalk re-check is belt-and-suspenders: with closed
// written under c.genMu it is unreachable, but it keeps this function
// locally correct (no leaked generation) even if a future closed-
// setter forgoes the lock.
//
// Mirrors the high-level shape of Java's reconnectViaTracker +
// executeOnce composition.
func (c *QwpQueryClient) reconnectAndReplay(ctx context.Context, s *qwpQuerySession, failedIdx int) (*QwpServerInfo, error) {
	c.genMu.Lock()
	defer c.genMu.Unlock()

	if c.closed.Load() {
		return nil, errClosedDuringFailover
	}

	// Tear down the dying generation. Use the cleanup-bounded ctx
	// independent of the user's so the dispatcher's exit waits a
	// fixed budget regardless of what the caller's deadline says.
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), qwpQueryCleanupDrainTimeout)
	defer cancel()
	if oldIO := c.io(); oldIO != nil {
		_ = oldIO.shutdown(cleanupCtx)
	}
	if oldTr := c.transport(); oldTr != nil {
		_ = oldTr.close()
	}

	// Demote the just-failed endpoint, then open a fresh round. Order
	// is normative (failover.md §2.3): RecordMidStreamFailure must run
	// BEFORE the round reset, else sticky-Healthy would preserve the
	// just-failed host as the priority pick and hand it the first
	// reconnect attempt again. RecordMidStreamFailure only demotes a
	// still-Healthy slot and leaves `attempted` untouched; the
	// subsequent BeginRound(forgetClassifications=false) clears the
	// per-round bits but keeps topology classifications observed in
	// prior Executes (wire-egress.md §11.9.2 "lazy forget"). The one
	// fall-through BeginRound(true) lives inside connectWalk. n=1
	// degenerates cleanly: the lone host is demoted to TransportError,
	// PickNext still returns it, and the walk retries the same host —
	// the only candidate — instead of failing for lack of an
	// alternative.
	c.hostTracker.RecordMidStreamFailure(failedIdx)
	c.hostTracker.BeginRound(false)
	// Pass s.cancelCh so the walk short-circuits at endpoint
	// boundaries when the user calls Cancel mid-failover.
	// allowFallthroughReset=true: one BeginRound(true) re-sweep so a
	// long-lived client recovers from a topology change (Java
	// reconnectViaTracker parity).
	result, err := connectWalk(ctx, c.cfg, c.hostTracker, s.cancelCh, true)
	if err != nil {
		return nil, err
	}
	if c.closed.Load() {
		// Defensive: see the doc comment. connectWalk already spawned
		// the new generation's I/O goroutines + WebSocket, so tear them
		// down here rather than publish an orphan nothing will shut down.
		_ = result.io.shutdown(cleanupCtx)
		_ = result.transport.close()
		return nil, errClosedDuringFailover
	}
	c.publishGeneration(result)

	// Allocate a fresh requestId for the replay attempt. Matches
	// Java's nextRequestId++ on each executeOnce: the server treats
	// each attempt as a distinct query (the prior server's request
	// is now orphaned by the dropped connection).
	newReqID := c.nextRequestId
	c.nextRequestId++
	s.currentRequestId.Store(newReqID)
	if err := s.submit(ctx); err != nil {
		return nil, fmt.Errorf("qwp query: replay submit failed: %w", err)
	}
	// Re-issue the cancel if Cancel landed during the reconnect.
	// session.requestCancel reads (currentRequestId, c.io()) without
	// a lock, so a Cancel racing this function can pick up either
	// the OLD request_id paired with the NEW io (the window between
	// publishGeneration and currentRequestId.Store above — the new
	// dispatcher's top-of-loop CAS then clears the OLD id as a stale
	// "prior-query" cancel) or the OLD request_id paired with the OLD
	// io (the window before publishGeneration — the cancel atomic is
	// set on a torn-down dispatcher that will never read it). In both
	// cases the user's Cancel intent is silently dropped and the
	// replay runs to completion. Re-issuing here against the now-
	// stable (newReqID, c.io()) pair lands one CANCEL frame on the
	// wire: the dispatcher either matches newReqID in its CAS loop
	// (no clear) or picks it up via drainPendingCancel in
	// receiveLoop.
	if s.isCancelled() {
		c.io().requestCancel(newReqID)
	}
	return result.serverInfo, nil
}

// probeZstdAvailable allocates and immediately closes a zstd decoder
// so init-time failures (allocation pressure, bundled-library issues)
// surface synchronously at construction time. The Go port is simpler
// than Java's because klauspost/compress is pure Go — there is no
// native library to be missing. The probe still serves as a small
// sanity gate and matches Java's ordering (init after upgrade so
// transport errors surface first).
func probeZstdAvailable() error {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("qwp query: zstd decoder init failed: %w", err)
	}
	dec.Close()
	return nil
}

// effectiveAuthorization computes the Authorization header value
// from the config, resolving the three mutually-exclusive auth modes
// into a single header string.
func (c *qwpQueryClientConfig) effectiveAuthorization() string {
	if c.authorization != "" {
		return c.authorization
	}
	if c.httpUser != "" && c.httpPass != "" {
		creds := c.httpUser + ":" + c.httpPass
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))
	}
	if c.httpToken != "" {
		return "Bearer " + c.httpToken
	}
	return ""
}

// Close shuts down the I/O goroutines, sends a WebSocket close frame,
// and releases the underlying connection. Safe to call more than
// once; subsequent calls return nil. Safe to call from a goroutine
// other than the one driving Query/Exec, including while a Batches()
// iteration or Exec() is mid transparent-failover reconnect.
//
// Calling Close while a *QwpQuery.Batches() loop body is still using
// the batch's aliased []byte slices is undefined: the transport may
// free buffers the caller is still reading. The right way to unblock
// an in-flight iterator from another goroutine is Cancel (or cancel
// the Query/Exec context); Close then races at most the generation
// teardown, never the buffer aliasing.
func (c *QwpQueryClient) Close(ctx context.Context) error {
	var firstErr error
	c.closeOnce.Do(func() {
		// Set closed and snapshot the bound (io, transport) pair under
		// genMu. This is what makes Close safe against a concurrent
		// reconnectAndReplay: it holds genMu across its whole destroy-
		// old / build-new / publish swap, so under the lock we observe
		// exactly one consistent generation — never a torn pair half-
		// way through publishGeneration — and reconnectAndReplay
		// observes our closed flag and self-tears-down (or skips
		// building) any generation we are not the one tearing down.
		// See reconnectAndReplay's doc for the full interleaving table.
		// The shutdown/close run after Unlock so genMu is never held
		// across a user-facing wait.
		c.genMu.Lock()
		c.closed.Store(true)
		io := c.io()
		tr := c.transport()
		c.genMu.Unlock()

		if io != nil {
			if err := io.shutdown(ctx); err != nil {
				firstErr = err
			}
		}
		if tr != nil {
			if err := tr.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	})
	return firstErr
}

// Query submits a SELECT-style statement and returns a cursor over its
// result batches. The server-side execution begins immediately; the
// cursor drains events lazily as the caller ranges over Batches().
//
// Per-call options are supplied via the variadic opts list — see
// WithQueryBinds for attaching typed bind parameters. Repeating the same
// SQL text across calls hits the server's SQL-text-keyed factory cache;
// interpolating values into the SQL string defeats that reuse, use
// WithQueryBinds instead.
//
// Query never returns an error directly: any failure raised at submit
// time (closed client, bind setter error, ctx-cancelled submit) is
// latched on the returned *QwpQuery and yielded as the first element of
// Batches(). Callers MUST iterate Batches() to observe submit failures;
// dropping the cursor without ranging it discards the latched error
// silently. Use Exec for statements where the synchronous error
// signature is more natural.
//
// Err on a wrong statement kind also surfaces through the first
// Batches() yield: if the server sends EXEC_DONE (non-SELECT
// statement), the iterator yields (nil, error) and terminates. Use
// Exec for statements that do not produce a result set.
//
// Breaking out of the range loop early sends a CANCEL frame to the
// server and drains the remaining events until a terminal frame
// arrives. Always defer (*QwpQuery).Close() to guarantee cleanup on
// any path.
func (c *QwpQueryClient) Query(ctx context.Context, sql string, opts ...QueryOption) *QwpQuery {
	q := &QwpQuery{
		client: c,
		ctx:    ctx,
		sql:    sql,
	}
	if c.closed.Load() {
		q.pendingErr = errors.New("qwp query: client is closed")
		q.state.Store(qwpQueryStateDone)
		return q
	}
	req, err := c.buildRequest(sql, opts)
	if err != nil {
		q.pendingErr = err
		q.state.Store(qwpQueryStateDone)
		return q
	}
	q.requestId = req.requestId
	// SELECT is idempotent: transparent reconnect-and-replay on a
	// transport drop is always safe, so the session is replayable
	// regardless of replay_exec (which only governs Exec).
	q.session = newQwpQuerySession(c, req, true)
	if err := q.session.submit(ctx); err != nil {
		q.pendingErr = err
		q.state.Store(qwpQueryStateDone)
	}
	return q
}

// Exec runs a non-SELECT statement (DDL / INSERT / UPDATE / ...) and
// blocks until the server returns EXEC_DONE or a terminal error. On
// success returns the ExecResult (op type + rows affected). On a
// QUERY_ERROR frame the returned error is a *QwpQueryError; on a
// transport or decode failure it is a plain error.
//
// Per-call options are supplied via the variadic opts list — see
// WithQueryBinds for attaching typed bind parameters.
//
// Calling Exec on a SELECT statement returns an error — SELECT sends
// RESULT_BATCH + RESULT_END, which Exec does not expect. Use Query
// for SELECTs.
func (c *QwpQueryClient) Exec(ctx context.Context, sql string, opts ...QueryOption) (ExecResult, error) {
	if c.closed.Load() {
		return ExecResult{}, errors.New("qwp query: client is closed")
	}
	req, err := c.buildRequest(sql, opts)
	if err != nil {
		return ExecResult{}, err
	}
	reqId := req.requestId

	// Exec replays on a transport drop only when the caller opted in
	// via replay_exec=on. Default off: a non-idempotent statement the
	// server may already have applied must not be silently re-executed
	// on the reconnect — nextEvent surfaces the raw transport error
	// instead (see qwpQuerySession.replayable).
	session := newQwpQuerySession(c, req, c.cfg.replayExec)
	if err := session.submit(ctx); err != nil {
		return ExecResult{}, err
	}

	for {
		ev, err := session.nextEvent(ctx)
		if err != nil {
			// ctx expired or I/O terminated before we saw a terminal
			// event. Cancel + drain on a cleanup ctx so the dispatcher
			// returns to idle; otherwise the next Query/Exec on this
			// client blocks on the single-slot requests channel.
			// Route through the session so cancel targets the live
			// generation's request_id even after a transparent failover
			// reconnect (where the session's currentRequestId diverges
			// from reqId).
			session.requestCancel()
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(), qwpQueryCleanupDrainTimeout)
			_ = drainUntilTerminal(cleanupCtx, c.io())
			cleanupCancel()
			return ExecResult{}, err
		}
		switch ev.kind {
		case qwpEventKindExecDone:
			return ev.execResult, nil
		case qwpEventKindError:
			return ExecResult{}, eventToError(ev, reqId)
		case qwpEventKindTransportError:
			// The session has already exhausted its replay budget (or
			// failover was disabled). Surface the underlying transport
			// error so callers can errors.Is / errors.As against the
			// cause without picking up *QwpQueryError (which carries
			// server-status bytes that are meaningless for client-
			// side faults).
			return ExecResult{}, transportEventError(ev)
		case qwpEventKindFailoverReset:
			// Only reachable when this Exec opted into replay
			// (replay_exec=on): the session passes c.cfg.replayExec as
			// its replayable flag, and nextEvent emits this event only
			// for a replayable session — a non-idempotent Exec with
			// replay_exec=off is short-circuited to a raw transport
			// error before any reconnect, so it never double-executes.
			// Here the session already reconnected and resubmitted
			// transparently; the reset is informational. Consume the
			// new generation's terminal event on the next iteration.
		case qwpEventKindBatch:
			// Server streamed a result batch for what we asked for as
			// an exec. Release the buffer, send a CANCEL so the
			// server stops streaming the rest of the result set, and
			// drain to a terminal frame on a cleanup-bounded context
			// so the dispatcher returns to idle regardless of the
			// caller's ctx. Then surface the type-mismatch. Cancel
			// routes through the session so it targets the live
			// generation's request_id even after a transparent
			// failover reconnect.
			c.io().releaseBuffer(ev.batch)
			session.requestCancel()
			cleanupCtx, cancel := context.WithTimeout(
				context.Background(), qwpQueryCleanupDrainTimeout)
			_ = drainUntilTerminal(cleanupCtx, c.io())
			cancel()
			return ExecResult{}, fmt.Errorf(
				"qwp query: Exec called on a SELECT-style statement; use Query instead")
		case qwpEventKindEnd:
			// Bare RESULT_END with no preceding RESULT_BATCH — same
			// misuse as above (user ran a SELECT via Exec).
			return ExecResult{}, fmt.Errorf(
				"qwp query: Exec called on a SELECT-style statement; use Query instead")
		default:
			return ExecResult{}, fmt.Errorf("qwp query: unexpected event kind %d", ev.kind)
		}
	}
}

// buildRequest assembles the qwpRequest for a Query / Exec call. The
// bind setter runs on the caller's goroutine against the client's
// reusable QwpBinds scratch; the encoded bytes are then copied into a
// fresh per-request slice so the dispatcher's read of bindPayload is
// always against a request-owned buffer, independent of what the
// caller does with the scratch afterwards.
func (c *QwpQueryClient) buildRequest(sql string, opts []QueryOption) (qwpRequest, error) {
	if len(sql) > qwpMaxSqlTextBytes {
		return qwpRequest{}, fmt.Errorf(
			"qwp query: SQL text length %d exceeds %d-byte limit",
			len(sql), qwpMaxSqlTextBytes)
	}
	var settings qwpQueryOptions
	for _, opt := range opts {
		opt(&settings)
	}
	c.binds.reset()
	if settings.bindFn != nil {
		settings.bindFn(&c.binds)
		if err := c.binds.Err(); err != nil {
			return qwpRequest{}, err
		}
	}
	var bindPayload []byte
	if src := c.binds.bufferBytes(); len(src) > 0 {
		bindPayload = append([]byte(nil), src...)
	}
	reqId := c.nextRequestId
	c.nextRequestId++
	return qwpRequest{
		sql:           sql,
		requestId:     reqId,
		initialCredit: c.cfg.initialCredit,
		bindCount:     c.binds.Count(),
		bindPayload:   bindPayload,
	}, nil
}

// drainUntilTerminal reads and discards events until a terminal one
// (End / ExecDone / Error / TransportError) arrives. Releases any
// batch buffers along the way. Returns a transport/context error if
// takeEvent fails. Includes TransportError because a poisoned
// connection's pending events will be one of these — looping past
// would block forever waiting for an End the I/O goroutine will
// never emit.
func drainUntilTerminal(ctx context.Context, io *qwpEgressIO) error {
	for {
		ev, err := io.takeEvent(ctx)
		if err != nil {
			return err
		}
		switch ev.kind {
		case qwpEventKindBatch:
			io.releaseBuffer(ev.batch)
		case qwpEventKindEnd, qwpEventKindExecDone, qwpEventKindError,
			qwpEventKindTransportError:
			return nil
		}
	}
}

// eventToError converts a qwpEventKindError event into the most
// specific Go error type available. Server-sent QUERY_ERROR frames
// (status > 0) become *QwpQueryError; synthesized client-side errors
// (status == 0, set by emitError) stay as plain errors.
func eventToError(ev qwpEvent, reqId int64) error {
	if ev.errStatus != 0 {
		id := ev.requestId
		if id == 0 {
			id = reqId
		}
		return &QwpQueryError{
			RequestId: id,
			Status:    ev.errStatus,
			Message:   ev.errMessage,
		}
	}
	if ev.errMessage != "" {
		return errors.New(ev.errMessage)
	}
	return errors.New("qwp query: unspecified error")
}

// transportEventError converts a qwpEventKindTransportError into a
// caller-facing error. When transportErr is set (failover orchestrator
// path), wraps with %w so errors.As can match the underlying typed
// cause (e.g. *QwpRoleMismatchError from a failed reconnect walk).
// Falls back to a plain string-formatted error for I/O-goroutine
// emissions that only carry errMessage.
func transportEventError(ev qwpEvent) error {
	if ev.transportErr != nil {
		return fmt.Errorf("qwp query: %w", ev.transportErr)
	}
	return fmt.Errorf("qwp query: %s", ev.errMessage)
}

// Query lifecycle states. Transitions are linear: Idle → Iterating →
// Done, or Idle → Done (if Close runs before Batches is entered, or
// submit failed so the query is Done from construction). Coordination
// between Close and Batches is done via CAS on this state — see the
// per-method comments for the exact handshake.
const (
	qwpQueryStateIdle int32 = iota
	qwpQueryStateIterating
	qwpQueryStateDone
)

// QwpQuery is a streaming cursor over a SELECT result set returned by
// (*QwpQueryClient).Query. It is single-use: once the range over
// Batches() terminates (by End, Error, or break), the cursor is done
// and must not be iterated again.
//
// Thread safety: Batches and the buffers it yields are single-consumer
// — do not share the cursor across goroutines. Cancel is safe to call
// from other goroutines at any time. Close is safe to call from other
// goroutines too, but is a no-op while a Batches iteration is in
// flight: the iterator runs its own cancel+drain on every exit path,
// so a concurrent Close would only race it for the dispatcher's
// single terminal event. To unblock a hung iterator from another
// goroutine, use Cancel (or cancel the context passed to Query).
type QwpQuery struct {
	client *QwpQueryClient
	ctx    context.Context
	sql    string

	// session orchestrates submission and event consumption,
	// including transparent reconnect-and-replay on transport-
	// terminal failure. Owns the in-flight requestId across replays;
	// the requestId field below is the *initial* attempt's id and is
	// used only for diagnostics (RequestId accessor).
	session *qwpQuerySession

	// requestId is the initial (first-attempt) client-assigned id.
	// Surfaced via RequestId for log correlation; on replay the
	// session's currentRequestId diverges. Cancel routes through the
	// session so it always targets the live generation.
	requestId int64

	// totalRows is set when a RESULT_END frame arrives. Read via
	// TotalRows(). Default 0 on a query that never reached End
	// (cancelled, errored, or still running). Atomic because the
	// iterator goroutine in Batches() writes it while a sibling
	// goroutine (e.g. cancel/observer) may call TotalRows().
	totalRows atomic.Int64

	// pendingErr holds an error surfaced at submit time (closed
	// client, submit blocked on ctx cancel). Yielded on the first
	// iteration of Batches() so callers discover it naturally.
	pendingErr error

	// state is the lifecycle phase (see qwpQueryState* constants).
	// Batches() enters via CAS(Idle→Iterating); Close() takes
	// ownership of cleanup only via CAS(Idle→Done). Either defer
	// flips to Done on exit. A failed CAS in Close means an iterator
	// is active (and will clean up itself) or the query is already
	// done — both cases are no-ops.
	state atomic.Int32

	// cancelled records whether Cancel() has been invoked. Used to
	// avoid emitting a synthesized "cancelled by caller" error on top
	// of the server's QUERY_ERROR(status=CANCELLED) echo.
	cancelled atomic.Bool
}

// Batches returns a range-over-func iterator that yields each
// RESULT_BATCH frame as a *QwpColumnBatch, along with an optional
// error. The iterator terminates on RESULT_END (clean end), a
// QUERY_ERROR from the server (yielded as the last element's error),
// a transport/decode failure (same), or the caller breaking out of
// the range loop (sends CANCEL to the server, drains remaining
// events).
//
// The yielded *QwpColumnBatch is only valid inside the body of the
// current iteration — its slices alias the pool-owned decode buffer
// and will be reused for the next batch. Use batch.CopyAll() to
// retain data across iterations.
func (q *QwpQuery) Batches() iter.Seq2[*QwpColumnBatch, error] {
	return func(yield func(*QwpColumnBatch, error) bool) {
		// CAS Idle→Iterating grabs the iteration slot and also locks
		// out a concurrent Close from running its own drain. On
		// failure the query is already Done (either Close won the
		// race, a prior iteration ran, or submit failed) — surface
		// pendingErr once and stop.
		if !q.state.CompareAndSwap(qwpQueryStateIdle, qwpQueryStateIterating) {
			if q.pendingErr != nil {
				yield(nil, q.pendingErr)
				q.pendingErr = nil
			}
			return
		}
		defer q.state.Store(qwpQueryStateDone)

		for {
			ev, err := q.session.nextEvent(q.ctx)
			if err != nil {
				// takeEvent returned before a terminal frame (most
				// often q.ctx expired while we were waiting on the
				// server). The dispatcher is still parked in
				// receiveLoop for this query, so cancel + drain on a
				// cleanup ctx before returning — symmetrical to the
				// !keepGoing break-out below. The caller's deferred
				// Close() sees state=Done (flipped by the defer on
				// this function) and becomes a no-op; without this
				// drain the dispatcher would stay stuck and strand
				// the client for follow-up Query/Exec.
				yield(nil, err)
				q.cancelAndDrainOnCleanupCtx()
				return
			}
			switch ev.kind {
			case qwpEventKindBatch:
				keepGoing := false
				func() {
					// Release the buffer even if the caller's yield
					// body panics. Without this, a single panic with
					// bufferPoolSize=1 permanently starves the pool,
					// and the dispatcher — still parked in receiveLoop
					// for this query — blocks the next Query/Exec.
					// On panic we also run the cancel+drain before
					// rethrowing: the outer `defer q.state.Store(Done)`
					// has already flipped the state, so the caller's
					// defer q.Close() would otherwise be a no-op and
					// leave the dispatcher stranded.
					defer func() {
						q.client.io().releaseBuffer(ev.batch)
						if r := recover(); r != nil {
							q.cancelAndDrainOnCleanupCtx()
							panic(r)
						}
					}()
					keepGoing = yield(&ev.batch.batch, nil)
				}()
				if !keepGoing {
					// User broke out — request cancel and drain the
					// remaining events until a terminal frame so the
					// dispatcher returns to idle and the next query
					// can submit cleanly. Drain uses a bounded cleanup
					// ctx independent of q.ctx because a common reason
					// to break out is exactly that q.ctx has expired.
					q.cancelAndDrainOnCleanupCtx()
					return
				}
			case qwpEventKindEnd:
				q.totalRows.Store(ev.totalRows)
				return
			case qwpEventKindError:
				// A server-sent cancellation echo (status=Cancelled)
				// in response to our own Cancel call is not an error
				// the caller needs to see — yielding it would make a
				// clean "I broke out of the loop" look like a
				// failure. Swallow that one case.
				if q.cancelled.Load() && ev.errStatus == qwpStatusCancelled {
					return
				}
				yield(nil, eventToError(ev, q.requestId))
				return
			case qwpEventKindTransportError:
				// Synthesized client-side transport-terminal failure
				// — the connection is poisoned and cannot serve more
				// frames. Surface as a plain error; the session
				// orchestrator (qwp_query_failover.go) intercepts
				// this case before it reaches Batches when failover
				// is enabled and replay succeeds.
				yield(nil, transportEventError(ev))
				return
			case qwpEventKindFailoverReset:
				// Emitted by the session orchestrator after a
				// successful reconnect-and-replay. Yield as a
				// non-fatal error so the caller can detect via
				// errors.As and discard accumulated state, then
				// continue iterating to consume the new generation's
				// batches. ev.failoverReset is always non-nil for
				// this kind.
				if !yield(nil, ev.failoverReset) {
					q.cancelAndDrainOnCleanupCtx()
					return
				}
			case qwpEventKindExecDone:
				// Wrong statement kind: user ran a non-SELECT via
				// Query. Surface with a typed error so they can
				// switch to Exec.
				yield(nil, fmt.Errorf(
					"qwp query: Query called on a non-SELECT statement; use Exec instead"))
				return
			default:
				yield(nil, fmt.Errorf("qwp query: unexpected event kind %d", ev.kind))
				return
			}
		}
	}
}

// TotalRows returns the server-reported total-row count from the
// RESULT_END frame, or 0 if the query did not reach End (cancelled,
// errored, or still running). Safe to call from any goroutine.
func (q *QwpQuery) TotalRows() int64 {
	return q.totalRows.Load()
}

// RequestId returns the client-assigned id for this query. Exposed
// mainly for test instrumentation and cross-correlating logs with
// server-side request ids.
func (q *QwpQuery) RequestId() int64 {
	return q.requestId
}

// Cancel asks the server to abort the current query. Safe to call
// from any goroutine, including before the first Batches() iteration
// or while another goroutine is ranging over Batches(). A no-op if
// the query has already reached a terminal state.
//
// The cancel is asynchronous: Batches() keeps yielding whatever the
// server has already buffered before it reacts to the CANCEL. The
// server eventually responds with QUERY_ERROR(status=CANCELLED),
// which Batches() swallows silently so a caller-initiated Cancel
// produces a clean end of iteration.
func (q *QwpQuery) Cancel() {
	if q.state.Load() == qwpQueryStateDone {
		return
	}
	if q.cancelled.CompareAndSwap(false, true) {
		// Route through the session so cancel targets the live
		// generation's request_id even after a transparent failover
		// reconnect (where the session's currentRequestId diverges
		// from q.requestId).
		if q.session != nil {
			q.session.requestCancel()
		} else {
			q.client.io().requestCancel(q.requestId)
		}
	}
}

// Close finalizes the cursor. Drains any pending events to a
// terminal frame so the underlying I/O dispatcher returns to idle —
// required before the next Query or Exec on the same client. Safe
// to defer even on already-finished queries; the second call is a
// no-op.
//
// Close is also a no-op while a Batches() iteration is in flight on
// another goroutine: the iterator performs its own cancel+drain on
// every exit path, and a concurrent Close would only race it for the
// dispatcher's single terminal event. Use Cancel (or cancel q.ctx)
// to unblock an in-flight iterator from another goroutine.
//
// Does not close the client itself. Call (*QwpQueryClient).Close
// to release the underlying WebSocket connection.
func (q *QwpQuery) Close() {
	// CAS Idle→Done claims exclusive cleanup ownership. Failure means
	// either a Batches() iteration is running (state=Iterating — it
	// will clean up on exit) or the cursor is already Done (prior
	// iteration, Close, or submit failure). Both are no-ops here.
	if !q.state.CompareAndSwap(qwpQueryStateIdle, qwpQueryStateDone) {
		return
	}
	q.cancelAndDrainOnCleanupCtx()
}

// cancelAndDrainOnCleanupCtx sends a CANCEL for this query's
// requestId (unless one is already in flight) and drains pending
// events until a terminal frame arrives, so the dispatcher returns
// to idle regardless of q.ctx's state. Uses a fresh bounded context
// because every caller either runs after q.ctx has already been
// observed done (iterator break-out, takeEvent-error) or inside a
// user-driven Close which has no meaningful ctx of its own.
func (q *QwpQuery) cancelAndDrainOnCleanupCtx() {
	if q.cancelled.CompareAndSwap(false, true) {
		if q.session != nil {
			q.session.requestCancel()
		} else {
			q.client.io().requestCancel(q.requestId)
		}
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), qwpQueryCleanupDrainTimeout)
	defer cancel()
	_ = drainUntilTerminal(cleanupCtx, q.client.io())
}
