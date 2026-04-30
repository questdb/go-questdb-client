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
)

// qwpQueryClientConfig is the internal configuration of QwpQueryClient.
// Populated either by functional options (NewQwpQueryClient) or by the
// ws:: / wss:: config-string parser (QwpQueryClientFromConf). The
// options surface is deliberately smaller than the ingest LineSender's
// — QWP egress has its own concerns (buffer pool depth, max batch
// rows) and does not inherit ILP-era knobs.
type qwpQueryClientConfig struct {
	// address is "host:port". Default "localhost:9000".
	address string
	// endpointPath is the HTTP path used for the WebSocket upgrade.
	// Default "/read/v1".
	endpointPath string
	// authorization, when non-empty, is sent verbatim as the
	// Authorization HTTP header. Mutually exclusive with user/pass and
	// token.
	authorization string
	// httpUser / httpPass populate an HTTP Basic Authorization header
	// at connect time. Mutually exclusive with authorization and token.
	httpUser string
	httpPass string
	// httpToken populates a Bearer Authorization header at connect
	// time. Mutually exclusive with authorization and user/pass.
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
// level default. Only relevant when compression != "raw".
const qwpDefaultCompressionLevel = 3

// qwpDefaultEgressBufferPoolSize is the I/O decode pool depth when the
// caller hasn't overridden it. Matches the Java client default
// (DEFAULT_IO_BUFFER_POOL_SIZE = 4): four slots let the dispatcher keep
// decoding ~4 batches ahead of a slow consumer before the buffer pool
// drains and back-pressures the WebSocket via the TCP window.
const qwpDefaultEgressBufferPoolSize = 4

// qwpQueryDefaultConfig returns the zero-arg default config. Used as
// the seed for both the functional-options path and the config-string
// path.
func qwpQueryDefaultConfig() *qwpQueryClientConfig {
	return &qwpQueryClientConfig{
		address:          defaultHttpAddress, // "localhost:9000"
		endpointPath:     qwpReadPath,        // "/read/v1"
		bufferPoolSize:   qwpDefaultEgressBufferPoolSize,
		compression:      qwpCompressionRaw,
		compressionLevel: qwpDefaultCompressionLevel,
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
	if c.address == "" {
		return fmt.Errorf("qwp query: address is empty")
	}
	if err := validateQwpAddr(c.address); err != nil {
		return err
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
	authModes := 0
	if c.authorization != "" {
		authModes++
	}
	if basicSet {
		authModes++
	}
	if c.httpToken != "" {
		authModes++
	}
	if authModes > 1 {
		return fmt.Errorf("qwp query: auth, username/password, and token are mutually exclusive")
	}
	if basicSet && (c.httpUser == "" || c.httpPass == "") {
		return fmt.Errorf("qwp query: both username and password must be provided together")
	}
	return nil
}

// validateQwpAddr checks that an addr= value is a well-formed
// host[:port] (or bracketed IPv6) form. It enforces the port-range
// [1, 65535] when present and rejects malformed bracketed IPv6 inputs
// up front so callers see a parser-level error rather than an opaque
// dial failure later. Multi-address (comma-separated) entries are not
// supported in the Go client; an embedded comma in addr is rejected
// here so the user sees an actionable error rather than a "host not
// found" downstream.
//
// Forms accepted:
//   - "host"             — bare host, port defaults to the URL scheme's
//   - "host:port"        — explicit port; validated against [1, 65535]
//   - "[ipv6]:port"      — bracketed IPv6 with port
//   - "[ipv6]"           — bracketed IPv6 without port
//   - "ipv6::with::colons" — bare IPv6 (>=2 colons unbracketed)
//
// Rejected:
//   - empty string (caught earlier in validate())
//   - empty bracketed host: "[]:port"
//   - missing closing ']': "[::1:9000"
//   - trailing garbage after ']': "[::1]9000"
//   - port out of [1, 65535]
//   - non-numeric port
//   - comma-separated multi-address (Go client doesn't support failover)
func validateQwpAddr(s string) error {
	if strings.Contains(s, ",") {
		return fmt.Errorf(
			"qwp query: invalid addr %q: multi-address (comma-separated) is not supported", s)
	}
	host, port, err := splitQwpHostPort(s)
	if err != nil {
		return fmt.Errorf("qwp query: invalid addr %q: %w", s, err)
	}
	if host == "" {
		return fmt.Errorf("qwp query: invalid addr %q: empty host", s)
	}
	if port == "" {
		return nil
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("qwp query: invalid addr %q: invalid port %q", s, port)
	}
	if n < 1 || n > 65535 {
		return fmt.Errorf("qwp query: invalid addr %q: port %d out of range [1, 65535]", s, n)
	}
	return nil
}

// splitQwpHostPort splits a single host[:port] entry. Returns the host
// (with surrounding brackets stripped, if any), the port string (empty
// when no port was supplied), and a structural error for malformed
// bracketed forms. The port string is returned untrimmed so the caller
// can produce a useful error message; numeric validation happens in
// validateQwpAddr.
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
	case "ws":
		cfg.tlsMode = tlsDisabled
	case "wss":
		cfg.tlsMode = tlsEnabled
	default:
		return nil, NewInvalidConfigStrError("invalid schema %q, expected ws or wss", data.Schema)
	}
	tlsVerifySet := false

	for k, v := range data.KeyValuePairs {
		switch k {
		case "addr":
			cfg.address = v
		case "path":
			cfg.endpointPath = v
		case "auth":
			cfg.authorization = v
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
		default:
			return nil, NewInvalidConfigStrError("unsupported option %q", k)
		}
	}

	if tlsVerifySet && data.Schema == "ws" {
		return nil, NewInvalidConfigStrError("tls_verify requires the wss:: schema")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
