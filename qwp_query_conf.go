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
	"fmt"
	"strconv"
	"strings"
	"time"
)

// qwpQueryClientConfig is the internal configuration of QwpQueryClient.
// Populated either by functional options (NewQwpQueryClient) or by the
// ws:: / wss:: config-string parser (QwpQueryClientFromConf). The
// options surface is deliberately smaller than the ingest LineSender's
// — QWP egress has its own concerns (buffer pool depth, max batch
// rows) and does not inherit ILP-era knobs.
type qwpQueryClientConfig struct {
	// endpoints is the ordered list of WebSocket endpoints the connect
	// walk attempts. The first matching the target= filter wins;
	// transient failures during the walk skip to the next entry. The
	// failover orchestrator reuses the same list for reconnect.
	// Default is one entry pointing at defaultHttpAddress.
	endpoints []qwpEndpoint
	// endpointPath is the HTTP path used for the WebSocket upgrade.
	// Default "/read/v1".
	endpointPath string
	// httpUser / httpPass populate an HTTP Basic Authorization header
	// at connect time. Mutually exclusive with token.
	httpUser string
	httpPass string
	// httpToken populates a Bearer Authorization header at connect
	// time. Mutually exclusive with user/pass.
	httpToken string
	// clientID overrides the default X-QWP-Client-Id header. Empty
	// uses the module default (qwpClientId).
	clientID string
	// bufferPoolSize is the depth of the decode buffer pool. Default
	// qwpDefaultEgressBufferPoolSize. Must be >= 1.
	bufferPoolSize int
	// maxBatchRows caps per-batch row count the server emits. 0 omits
	// the X-QWP-Max-Batch-Rows header and lets the server use its cap.
	maxBatchRows int
	// initialCredit is the egress flow-control budget. 0 = unbounded
	// (no CREDIT bookkeeping). A positive value streams at most N
	// bytes before the server parks; the client auto-replenishes as
	// the consumer releases each batch.
	initialCredit int64
	// compression is the preference the client advertises on the
	// upgrade handshake. One of "raw", "zstd", "auto". Default "raw"
	// matches Java's library default — no compression, no handshake
	// header, no server-side encode cost. "zstd" asks for zstd first
	// and falls back to raw; "auto" advertises both and lets the
	// server pick.
	compression string
	// compressionLevel is the zstd level hint sent via the accept-
	// encoding header. Ignored when compression == "raw". Clamped
	// server-side to [1, 9]; client accepts [1, 22] matching Java.
	compressionLevel int
	// tlsMode mirrors lineSenderConfig's three-valued TLS state.
	tlsMode tlsMode

	// target constrains the connect walk by SERVER_INFO.role. Default
	// is qwpTargetAny, which accepts any role and so needs no role
	// byte. qwpTargetPrimary and qwpTargetReplica do: if the client
	// does not consume SERVER_INFO (serverInfoTimeout disabled) or the
	// server sends no parseable frame, the role is unknown and the
	// filter cannot be evaluated.
	target QwpTargetFilter
	// zone is the client's opaque, case-insensitive locality hint
	// (failover.md §1.1). When set and target != primary, the host
	// tracker prefers endpoints whose server-advertised zone_id
	// (SERVER_INFO.zone_id under CAP_ZONE, or the X-QuestDB-Zone
	// header on a 421 reject) matches, via the (state, zone) priority
	// lattice. Empty (the default) collapses every host to the Same
	// tier, i.e. zone-blind selection. Shared verbatim with the
	// ingest connect string, where it is accepted-but-inert (the
	// ingestion path does not route by zone).
	zone string
	// authTimeoutMs is the failover.md §1.1 per-host upper bound on
	// the HTTP upgrade response read (the wait between writing the
	// upgrade request and reading the response headers). It does NOT
	// cover TCP connect, TLS handshake, or the post-upgrade
	// SERVER_INFO frame read (that uses serverInfoTimeout). Default
	// qwpDefaultAuthTimeoutMs (15_000); must be > 0.
	authTimeoutMs int
	// failoverEnabled toggles transparent reconnect-and-replay on
	// transport-terminal failure mid-query. Default true; matches
	// Java's failover=on default. When false, transport errors
	// surface directly through Batches() / Exec().
	failoverEnabled bool
	// failoverMaxAttempts caps the number of executeOnce invocations
	// per Query / Exec. Counts the initial attempt plus every
	// reconnect retry. Default qwpDefaultFailoverMaxAttempts.
	failoverMaxAttempts int
	// failoverBackoffInitial is the initial sleep between reconnect
	// attempts. Doubled on each subsequent attempt up to
	// failoverBackoffMax. Default qwpDefaultFailoverInitialBackoff.
	failoverBackoffInitial time.Duration
	// failoverBackoffMax caps the exponential backoff. Default
	// qwpDefaultFailoverMaxBackoff.
	failoverBackoffMax time.Duration
	// failoverMaxDuration is the total wall-clock cap on the per-
	// Query/Exec failover loop. Whichever of this or
	// failoverMaxAttempts fires first ends the loop. 0 disables the
	// time cap (failover then bounded only by attempts). Default
	// qwpDefaultFailoverMaxDuration; matches Java's
	// DEFAULT_FAILOVER_MAX_DURATION_MS.
	failoverMaxDuration time.Duration
	// serverInfoTimeout bounds the synchronous read of SERVER_INFO
	// after each upgrade. The server always emits SERVER_INFO as the
	// first post-upgrade frame, so the drain is mandatory on egress;
	// must be > 0. Default qwpDefaultServerInfoTimeout.
	serverInfoTimeout time.Duration
	// replayExec opts Exec into transparent replay on transport-
	// terminal failures. Default false — non-idempotent statements
	// (INSERT/UPDATE/DELETE/DDL) might double-execute on a transport
	// drop after the server applied the statement. Callers that know
	// their statements are idempotent can opt in via
	// WithQwpQueryReplayExec(true).
	replayExec bool
}

// qwpCompressionRaw / qwpCompressionZstd / qwpCompressionAuto are the
// three valid values for qwpQueryClientConfig.compression. "raw" is
// the library default: omits the accept-encoding header entirely so
// servers that do not know about compression see an unchanged
// handshake.
const (
	qwpCompressionRaw  = "raw"
	qwpCompressionZstd = "zstd"
	qwpCompressionAuto = "auto"
)

// qwpDefaultCompressionLevel matches Java QwpQueryClient's compression
// level default (Sender.java compressionLevel field = 1; see also
// connect-string.md §Query client keys: "Default `1` — the cheapest
// server-side CPU"). Only relevant when compression != "raw".
const qwpDefaultCompressionLevel = 1

// qwpDefaultEgressBufferPoolSize is the I/O decode pool depth when the
// caller hasn't overridden it. Matches the Java client default
// (DEFAULT_IO_BUFFER_POOL_SIZE = 4): four slots let the dispatcher keep
// decoding ~4 batches ahead of a slow consumer before the buffer pool
// drains and back-pressures the WebSocket via the TCP window.
const qwpDefaultEgressBufferPoolSize = 4

// Failover defaults — match Java QwpQueryClient.DEFAULT_FAILOVER_*.
const (
	// qwpDefaultFailoverMaxAttempts is the cap on executeOnce
	// invocations per Query/Exec call. Counts the initial attempt
	// plus every reconnect retry. Java's DEFAULT_FAILOVER_MAX_ATTEMPTS
	// = 8.
	qwpDefaultFailoverMaxAttempts = 8
	// qwpDefaultFailoverInitialBackoff is the initial sleep between
	// reconnect attempts; doubled per retry up to
	// qwpDefaultFailoverMaxBackoff. Java's
	// DEFAULT_FAILOVER_INITIAL_BACKOFF_MS = 50.
	qwpDefaultFailoverInitialBackoff = 50 * time.Millisecond
	// qwpDefaultFailoverMaxBackoff caps the exponential backoff.
	// Java's DEFAULT_FAILOVER_MAX_BACKOFF_MS = 1000.
	qwpDefaultFailoverMaxBackoff = 1 * time.Second
	// qwpDefaultFailoverMaxDuration is the total wall-clock cap on the
	// per-Query/Exec failover loop; 0 would disable the cap. Java's
	// DEFAULT_FAILOVER_MAX_DURATION_MS = 30_000.
	qwpDefaultFailoverMaxDuration = 30 * time.Second
	// qwpDefaultServerInfoTimeout bounds the synchronous SERVER_INFO
	// read after the upgrade. Java's DEFAULT_SERVER_INFO_TIMEOUT_MS =
	// 5000.
	qwpDefaultServerInfoTimeout = 5 * time.Second
	// qwpDefaultAuthTimeoutMs is the per-host upgrade-response-read
	// bound when the caller hasn't overridden it. failover.md §1.1
	// default (15_000); matches the ingest sender default and Java's
	// DEFAULT_AUTH_TIMEOUT_MS so a shared connect string behaves
	// identically on both clients.
	qwpDefaultAuthTimeoutMs = 15_000
)

// qwpQueryDefaultConfig returns the zero-arg default config. Used as
// the seed for both the functional-options path and the config-string
// path. Seeds endpoints with a single entry pointing at the local
// QuestDB default; functional options or addr= override it.
func qwpQueryDefaultConfig() *qwpQueryClientConfig {
	return &qwpQueryClientConfig{
		endpoints:              []qwpEndpoint{{host: "127.0.0.1", port: qwpDefaultPort}},
		endpointPath:           qwpReadPath, // "/read/v1"
		bufferPoolSize:         qwpDefaultEgressBufferPoolSize,
		compression:            qwpCompressionRaw,
		compressionLevel:       qwpDefaultCompressionLevel,
		target:                 qwpTargetAny,
		failoverEnabled:        true,
		failoverMaxAttempts:    qwpDefaultFailoverMaxAttempts,
		failoverBackoffInitial: qwpDefaultFailoverInitialBackoff,
		failoverBackoffMax:     qwpDefaultFailoverMaxBackoff,
		failoverMaxDuration:    qwpDefaultFailoverMaxDuration,
		serverInfoTimeout:      qwpDefaultServerInfoTimeout,
		authTimeoutMs:          qwpDefaultAuthTimeoutMs,
	}
}

// buildAcceptEncodingHeader translates the user's compression
// preference into the X-QWP-Accept-Encoding header value. "raw"
// returns an empty string so the transport omits the header entirely
// (Java parity — servers that pre-date egress compression see an
// unchanged handshake). "zstd" and "auto" both advertise
// "zstd;level=N,raw"; the server picks one. Mirrors Java's
// QwpQueryClient.buildAcceptEncodingHeader.
func (c *qwpQueryClientConfig) buildAcceptEncodingHeader() string {
	if c.compression == qwpCompressionRaw {
		return ""
	}
	return fmt.Sprintf("zstd;level=%d,raw", c.compressionLevel)
}

// validate is the single-source sanity gate shared by both config entry
// points. Runs after options/conf-string parsing and before any network
// I/O. Mirrors Java QwpQueryClient.fromConfig's final cross-field
// checks (mutually-exclusive auth modes, TLS-only roots keys, bufferPool
// >= 1) plus the host-required check pushed into the Go parser.
func (c *qwpQueryClientConfig) validate() error {
	if len(c.endpoints) == 0 {
		return fmt.Errorf("qwp query: no endpoints configured")
	}
	for i, ep := range c.endpoints {
		if ep.host == "" {
			return fmt.Errorf("qwp query: endpoint %d has empty host", i)
		}
		if ep.port < 1 || ep.port > 65535 {
			return fmt.Errorf("qwp query: endpoint %d port %d out of range [1, 65535]",
				i, ep.port)
		}
	}
	if c.endpointPath == "" {
		return fmt.Errorf("qwp query: endpoint path is empty")
	}
	if c.bufferPoolSize < 1 {
		return fmt.Errorf("qwp query: buffer pool size must be >= 1, got %d", c.bufferPoolSize)
	}
	if c.maxBatchRows < 0 {
		return fmt.Errorf("qwp query: max batch rows must be >= 0, got %d", c.maxBatchRows)
	}
	if c.maxBatchRows > qwpMaxRowsPerBatch {
		return fmt.Errorf("qwp query: max batch rows %d exceeds client cap %d",
			c.maxBatchRows, qwpMaxRowsPerBatch)
	}
	if c.initialCredit < 0 {
		return fmt.Errorf("qwp query: initial credit must be >= 0, got %d", c.initialCredit)
	}
	switch c.compression {
	case qwpCompressionRaw, qwpCompressionZstd, qwpCompressionAuto:
		// ok
	default:
		return fmt.Errorf(
			"qwp query: unsupported compression %q (expected raw, zstd, or auto)",
			c.compression)
	}
	if c.compressionLevel < 1 || c.compressionLevel > 22 {
		return fmt.Errorf(
			"qwp query: compression level must be in [1, 22], got %d",
			c.compressionLevel)
	}
	basicSet := c.httpUser != "" || c.httpPass != ""
	if basicSet && c.httpToken != "" {
		return fmt.Errorf("qwp query: username/password and token are mutually exclusive")
	}
	if basicSet && (c.httpUser == "" || c.httpPass == "") {
		return fmt.Errorf("qwp query: both username and password must be provided together")
	}
	if c.failoverMaxAttempts < 1 {
		return fmt.Errorf(
			"qwp query: failover_max_attempts must be >= 1, got %d", c.failoverMaxAttempts)
	}
	if c.failoverBackoffInitial < 0 {
		return fmt.Errorf(
			"qwp query: failover_backoff_initial must be >= 0, got %v",
			c.failoverBackoffInitial)
	}
	if c.failoverBackoffMax < 0 {
		return fmt.Errorf(
			"qwp query: failover_backoff_max must be >= 0, got %v",
			c.failoverBackoffMax)
	}
	if c.failoverBackoffMax < c.failoverBackoffInitial {
		return fmt.Errorf(
			"qwp query: failover_backoff_max (%v) must be >= failover_backoff_initial (%v)",
			c.failoverBackoffMax, c.failoverBackoffInitial)
	}
	if c.failoverMaxDuration < 0 {
		return fmt.Errorf(
			"qwp query: failover_max_duration must be >= 0, got %v",
			c.failoverMaxDuration)
	}
	if c.serverInfoTimeout <= 0 {
		return fmt.Errorf(
			"qwp query: server_info_timeout must be > 0, got %v", c.serverInfoTimeout)
	}
	if c.authTimeoutMs <= 0 {
		return fmt.Errorf(
			"qwp query: auth_timeout_ms must be > 0, got %d", c.authTimeoutMs)
	}
	if c.target > qwpTargetReplica {
		return fmt.Errorf("qwp query: invalid target %d (expected any, primary, or replica)",
			byte(c.target))
	}
	return nil
}

// addressString returns a comma-joined "host:port,..." form of the
// configured endpoints. Used by error messages and tests; not part of
// the public API.
func (c *qwpQueryClientConfig) addressString() string {
	parts := make([]string, 0, len(c.endpoints))
	for _, ep := range c.endpoints {
		parts = append(parts, ep.String())
	}
	return strings.Join(parts, ",")
}

// splitQwpHostPort splits a single host[:port] entry. Returns the host
// (with surrounding brackets stripped, if any), the port string (empty
// when no port was supplied), and a structural error for malformed
// bracketed forms. The port string is returned untrimmed so the caller
// can produce a useful error message; numeric validation happens in
// parseEndpointList.
//
// Forms accepted:
//   - "host"             — bare host, port defaults to qwpDefaultPort
//   - "host:port"        — explicit port; validated against [1, 65535]
//   - "[ipv6]:port"      — bracketed IPv6 with port
//   - "[ipv6]"           — bracketed IPv6 without port
//   - "ipv6::with::colons" — bare IPv6 (>=2 colons unbracketed)
//
// Rejected (by parseEndpointList using these errors):
//   - empty string
//   - empty bracketed host: "[]:port"
//   - missing closing ']': "[::1:9000"
//   - trailing garbage after ']': "[::1]9000"
//   - port out of [1, 65535]
//   - non-numeric port
func splitQwpHostPort(s string) (host, port string, err error) {
	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return "", "", fmt.Errorf("missing closing ']' in IPv6 address")
		}
		host = s[1:end]
		rest := s[end+1:]
		switch {
		case rest == "":
			return host, "", nil
		case rest[0] == ':':
			return host, rest[1:], nil
		default:
			return "", "", fmt.Errorf("expected ':' after ']' in IPv6 address")
		}
	}
	// No brackets: count colons.
	colons := strings.Count(s, ":")
	switch colons {
	case 0:
		return s, "", nil
	case 1:
		i := strings.IndexByte(s, ':')
		return s[:i], s[i+1:], nil
	default:
		// Multi-colon, unbracketed → bare IPv6 host without port.
		// A custom port on IPv6 requires brackets.
		return s, "", nil
	}
}

// parseQwpQueryConf parses a ws:: / wss:: config string into a
// qwpQueryClientConfig. The supported key set mirrors Java
// QwpQueryClient.fromConfig, except tls_roots / tls_roots_password,
// which aren't supported.
func parseQwpQueryConf(conf string) (*qwpQueryClientConfig, error) {
	data, err := parseConfigStr(conf)
	if err != nil {
		return nil, err
	}
	cfg := qwpQueryDefaultConfig()
	switch data.Schema {
	case "ws", "qwpws":
		// connect-string.md §Protocols and transports: qwpws /
		// qwpwss are long-form aliases for ws / wss.
		cfg.tlsMode = tlsDisabled
	case "wss", "qwpwss":
		cfg.tlsMode = tlsEnabled
	default:
		return nil, NewInvalidConfigStrError(
			"invalid schema %q, expected ws, wss, qwpws, or qwpwss", data.Schema)
	}
	tlsVerifySet := false

	for k, v := range data.KeyValuePairs {
		switch k {
		case "addr":
			eps, err := parseEndpointList(v, qwpDefaultPort)
			if err != nil {
				return nil, NewInvalidConfigStrError("%v", err)
			}
			cfg.endpoints = eps
		case "path":
			cfg.endpointPath = v
		case "username":
			cfg.httpUser = v
		case "password":
			cfg.httpPass = v
		case "token":
			cfg.httpToken = v
		case "client_id":
			cfg.clientID = v
		case "buffer_pool_size":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid buffer_pool_size %q: %v", v, err)
			}
			cfg.bufferPoolSize = n
		case "max_batch_rows":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid max_batch_rows %q: %v", v, err)
			}
			cfg.maxBatchRows = n
		case "initial_credit":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid initial_credit %q: %v", v, err)
			}
			cfg.initialCredit = n
		case "compression":
			switch v {
			case qwpCompressionRaw, qwpCompressionZstd, qwpCompressionAuto:
				cfg.compression = v
			default:
				return nil, NewInvalidConfigStrError(
					"invalid compression %q, expected raw, zstd, or auto", v)
			}
		case "compression_level":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid compression_level %q: %v", v, err)
			}
			cfg.compressionLevel = n
		case "tls_verify":
			switch v {
			case "on":
				cfg.tlsMode = tlsEnabled
			case "unsafe_off":
				cfg.tlsMode = tlsInsecureSkipVerify
			default:
				return nil, NewInvalidConfigStrError(
					"invalid tls_verify %q, expected on or unsafe_off", v)
			}
			tlsVerifySet = true
		case "tls_roots":
			return nil, NewInvalidConfigStrError("tls_roots is not available in the go client")
		case "tls_roots_password":
			return nil, NewInvalidConfigStrError("tls_roots_password is not available in the go client")
		case "target":
			t, err := parseTargetFilter(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("%v", err)
			}
			cfg.target = t
		case "zone":
			// Opaque locality hint (failover.md §1.1). Stored verbatim;
			// the host tracker lowercases for case-insensitive compare.
			// Accepted here so a single connect string can be shared
			// with the ingest client (where the same key is
			// accepted-but-inert).
			cfg.zone = v
		case "auth_timeout_ms":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid auth_timeout_ms %q: %v", v, err)
			}
			if n <= 0 {
				return nil, NewInvalidConfigStrError(
					"auth_timeout_ms must be > 0, got %d", n)
			}
			cfg.authTimeoutMs = n
		case "failover":
			switch v {
			case "on":
				cfg.failoverEnabled = true
			case "off":
				cfg.failoverEnabled = false
			default:
				return nil, NewInvalidConfigStrError(
					"invalid failover %q, expected on or off", v)
			}
		case "failover_max_attempts":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid failover_max_attempts %q: %v", v, err)
			}
			if n < 1 {
				return nil, NewInvalidConfigStrError(
					"failover_max_attempts must be >= 1, got %d", n)
			}
			cfg.failoverMaxAttempts = n
		case "failover_backoff_initial_ms":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid failover_backoff_initial_ms %q: %v", v, err)
			}
			if n < 0 {
				return nil, NewInvalidConfigStrError(
					"failover_backoff_initial_ms must be >= 0, got %d", n)
			}
			cfg.failoverBackoffInitial = time.Duration(n) * time.Millisecond
		case "failover_backoff_max_ms":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid failover_backoff_max_ms %q: %v", v, err)
			}
			if n < 0 {
				return nil, NewInvalidConfigStrError(
					"failover_backoff_max_ms must be >= 0, got %d", n)
			}
			cfg.failoverBackoffMax = time.Duration(n) * time.Millisecond
		case "failover_max_duration_ms":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid failover_max_duration_ms %q: %v", v, err)
			}
			if n < 0 {
				return nil, NewInvalidConfigStrError(
					"failover_max_duration_ms must be >= 0, got %d", n)
			}
			cfg.failoverMaxDuration = time.Duration(n) * time.Millisecond
		case "server_info_timeout_ms":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid server_info_timeout_ms %q: %v", v, err)
			}
			if n <= 0 {
				return nil, NewInvalidConfigStrError(
					"server_info_timeout_ms must be > 0, got %d", n)
			}
			cfg.serverInfoTimeout = time.Duration(n) * time.Millisecond
		case "replay_exec":
			switch v {
			case "on":
				cfg.replayExec = true
			case "off":
				cfg.replayExec = false
			default:
				return nil, NewInvalidConfigStrError(
					"invalid replay_exec %q, expected on or off", v)
			}
		default:
			if ingressOnlyKeys[k] {
				// Silently accepted on egress so a single ws:: / wss::
				// connect string can drive both Sender and
				// QwpQueryClient. The QwpQueryClient does not
				// interpret the value — range/enum/type checks run on
				// the ingress side (conf_parse.go).
				// connect-string.md §16-20 is the load-bearing spec
				// text.
				continue
			}
			return nil, NewInvalidConfigStrError("unsupported option %q", k)
		}
	}

	// tls_verify gates the TLS handshake; only meaningful on wss/qwpwss.
	if tlsVerifySet && (data.Schema == "ws" || data.Schema == "qwpws") {
		return nil, NewInvalidConfigStrError("tls_verify requires the wss:: schema")
	}

	// Wrap validate's plain errors as *InvalidConfigStrError so a caller
	// that came in via the conf-string path sees one consistent error
	// type — both the per-key parse errors above and the cross-field
	// validation errors below. The functional-options path
	// (NewQwpQueryClient) calls validate() directly and keeps the plain
	// fmt.Errorf form, where "config string" framing would be wrong.
	if err := cfg.validate(); err != nil {
		return nil, NewInvalidConfigStrError("%v", err)
	}
	return cfg, nil
}
