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
	// tlsMode mirrors lineSenderConfig's three-valued TLS state.
	tlsMode tlsMode
}

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
		address:        defaultHttpAddress, // "localhost:9000"
		endpointPath:   qwpReadPath,        // "/read/v1"
		bufferPoolSize: qwpDefaultEgressBufferPoolSize,
	}
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

// parseQwpQueryConf parses a ws:: / wss:: config string into a
// qwpQueryClientConfig. The supported key set mirrors Java
// QwpQueryClient.fromConfig (subset: compression_level / compression /
// tls_roots are intentionally omitted here; compression lands with
// step 9, and tls_roots is rejected by the Go module as a whole).
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
