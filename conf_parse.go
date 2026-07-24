/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
	"math"
	"strconv"
	"strings"
	"time"
)

type configData struct {
	Schema        string
	KeyValuePairs map[string]string
}

// egressOnlyKeys lists connect-string keys defined by the spec for the
// QwpQueryClient (egress) only. The ingress LineSender silently
// accepts them when the schema is ws:: / wss:: so that one connect
// string can drive both Sender and QwpQueryClient — per
// connect-string.md §16-20 ("Each direction reads the keys relevant
// to it and ignores keys meant only for the other direction") and
// §Query client keys ("The Sender (ingress) silently consumes the
// same keys ... the Sender does not interpret the values"). Range,
// enum, and type checks for these keys happen on the egress side
// only.
var egressOnlyKeys = map[string]bool{
	"buffer_pool_size":            true,
	"client_id":                   true,
	"compression":                 true,
	"compression_level":           true,
	"failover":                    true,
	"failover_backoff_initial_ms": true,
	"failover_backoff_max_ms":     true,
	"failover_max_attempts":       true,
	"failover_max_duration_ms":    true,
	"initial_credit":              true,
	"max_batch_rows":              true,
	"path":                        true,
	"replay_exec":                 true,
	"server_info_timeout_ms":      true,
}

// ingressOnlyKeys lists connect-string keys the ingress LineSender
// interprets and the egress QwpQueryClient silently accepts, so a
// shared ws:: / wss:: connect string works in both directions. The set
// is the connect-string.md §Key index ingress keys plus the
// ingress-only keys the Java client (Sender.java) accepts that the doc
// Key index omits: transaction and connection_listener_inbox_capacity.
// The user / pass auth aliases are shared keys, canonicalized to
// username / password by parseConfigStr. The UDP-only keys
// max_datagram_size and multicast_ttl are deliberately absent: QWP is
// the WebSocket transport, so both QWP parsers reject them. Same SSOT as
// egressOnlyKeys.
var ingressOnlyKeys = map[string]bool{
	"auto_flush":                            true,
	"auto_flush_bytes":                      true,
	"auto_flush_interval":                   true,
	"auto_flush_rows":                       true,
	"close_flush_timeout_millis":            true,
	"connection_listener_inbox_capacity":    true,
	"drain_orphans":                         true,
	"durable_ack_keepalive_interval_millis": true,
	"error_inbox_capacity":                  true,
	"init_buf_size":                         true,
	"initial_connect_retry":                 true,
	"max_background_drainers":               true,
	"max_buf_size":                          true,
	"max_name_len":                          true,
	"on_internal_error":                     true,
	"on_parse_error":                        true,
	"on_schema_error":                       true,
	"on_security_error":                     true,
	"on_server_error":                       true,
	"on_write_error":                        true,
	"reconnect_initial_backoff_millis":      true,
	"reconnect_max_backoff_millis":          true,
	"reconnect_max_duration_millis":         true,
	"request_durable_ack":                   true,
	"sender_id":                             true,
	"sf_append_deadline_millis":             true,
	"sf_dir":                                true,
	"sf_durability":                         true,
	"sf_max_segment_bytes":                          true,
	"sf_max_total_bytes":                    true,
	"transaction":                           true,
}

func confFromStr(conf string) (*lineSenderConfig, error) {
	var senderConf *lineSenderConfig

	data, err := parseConfigStr(conf)
	if err != nil {
		return nil, err
	}

	switch data.Schema {
	case "http":
		senderConf = newLineSenderConfig(httpSenderType)
	case "https":
		senderConf = newLineSenderConfig(httpSenderType)
		senderConf.tlsMode = tlsEnabled
	case "tcp":
		senderConf = newLineSenderConfig(tcpSenderType)
	case "tcps":
		senderConf = newLineSenderConfig(tcpSenderType)
		senderConf.tlsMode = tlsEnabled
	case "ws", "qwpws":
		// connect-string.md §Protocols and transports: qwpws is a
		// long-form alias for ws. Same TLS mode (disabled), same
		// transport selection.
		senderConf = newLineSenderConfig(qwpSenderType)
	case "wss", "qwpwss":
		senderConf = newLineSenderConfig(qwpSenderType)
		senderConf.tlsMode = tlsEnabled
	default:
		return nil, fmt.Errorf("invalid schema: %s", data.Schema)
	}

	// Resolve auto_flush before the key loop below. That loop ranges over
	// a map, whose iteration order Go randomizes, so a connect string that
	// pairs auto_flush=off (which zeroes every trigger, including QWP's
	// autoFlushBytes) with an explicit auto_flush_rows / _interval / _bytes
	// must not let map order pick the winner. Applying off here first and
	// letting the explicit trigger keys overwrite in the loop makes an
	// explicit trigger deterministically win over off — the only sensible
	// resolution of a self-contradictory config.
	if v, ok := data.KeyValuePairs["auto_flush"]; ok {
		switch v {
		case "off":
			senderConf.autoFlushRows = 0
			senderConf.autoFlushInterval = 0
			senderConf.autoFlushBytes = 0
		case "on":
			// The default; explicit triggers (if any) apply in the loop.
		default:
			return nil, NewInvalidConfigStrError("invalid %s value, %q is not 'on' or 'off'", "auto_flush", v)
		}
	}

	for k, v := range data.KeyValuePairs {
		switch k {
		case "addr":
			senderConf.address = v
		case "username", "user":
			// user is the deprecated alias of username (Sender.java).
			switch senderConf.senderType {
			case httpSenderType, qwpSenderType:
				senderConf.httpUser = v
			case tcpSenderType:
				senderConf.tcpKeyId = v
			default:
				panic("add a case for " + k)
			}
		case "password", "pass":
			// pass is the deprecated alias of password (Sender.java).
			if senderConf.senderType != httpSenderType && senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for HTTP and QWP senders", k)
			}
			senderConf.httpPass = v
		case "token":
			switch senderConf.senderType {
			case httpSenderType, qwpSenderType:
				senderConf.httpToken = v
			case tcpSenderType:
				senderConf.tcpKey = v
			default:
				panic("add a case for " + k)
			}
		case "token_x", "token_y":
			// TCP ILP public-key auth components (ECDSA P-256 X/Y). They
			// are not part of the QWP connect-string vocabulary, so reject
			// them on QWP for symmetry with the egress QwpQueryClient, which
			// also rejects them. On the legacy ILP transports this client
			// does not need the public key, so the values are ignored.
			if senderConf.senderType == qwpSenderType {
				return nil, NewInvalidConfigStrError("unsupported option %q", k)
			}
			continue
		case "auto_flush":
			// Resolved in the deterministic pre-pass above so map
			// iteration order can't decide auto_flush=off vs. an explicit
			// auto_flush_* trigger.
		case "auto_flush_rows":
			if v == "off" {
				senderConf.autoFlushRows = 0
				continue
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			senderConf.autoFlushRows = parsedVal
		case "auto_flush_interval":
			if v == "off" {
				senderConf.autoFlushInterval = 0
				continue
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			senderConf.autoFlushInterval = time.Duration(parsedVal) * time.Millisecond
		case "auto_flush_bytes":
			senderConf.autoFlushBytesSet = true
			if v == "off" {
				senderConf.autoFlushBytes = 0
				continue
			}
			parsedVal, err := parseSizeBytes(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q: %v", k, v, err)
			}
			senderConf.autoFlushBytes = int(parsedVal)
		case "init_buf_size", "max_buf_size":
			// Size-typed (connect-string.md §Size suffixes); accept
			// JVM-style k/kb/m/mb/g/gb/t/tb suffixes alongside bare
			// bytes.
			parsedVal, err := parseSizeBytes(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q: %v", k, v, err)
			}
			if k == "init_buf_size" {
				senderConf.initBufSize = int(parsedVal)
			} else {
				senderConf.maxBufSize = int(parsedVal)
			}
		case "request_min_throughput", "max_name_len":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			if k == "request_min_throughput" {
				senderConf.minThroughput = parsedVal
			} else {
				senderConf.fileNameLimit = parsedVal
			}
		case "request_timeout", "retry_timeout":
			timeout, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			timeoutDur := time.Duration(timeout) * time.Millisecond

			switch k {
			case "request_timeout":
				senderConf.requestTimeout = timeoutDur
			case "retry_timeout":
				senderConf.retryTimeout = timeoutDur
			default:
				panic("add a case for " + k)
			}
		case "tls_verify":
			switch v {
			case "on":
				senderConf.tlsMode = tlsEnabled
			case "unsafe_off":
				senderConf.tlsMode = tlsInsecureSkipVerify
			default:
				return nil, NewInvalidConfigStrError("invalid tls_verify value, %q is not 'on' or 'unsafe_off", v)
			}
		case "tls_roots":
			return nil, NewInvalidConfigStrError("tls_roots is not available in the go client")
		case "tls_roots_password":
			return nil, NewInvalidConfigStrError("tls_roots_password is not available in the go client")
		case "protocol_version":
			if senderConf.senderType == qwpSenderType {
				// protocol_version is not part of the QWP connect-string
				// vocabulary -- the version is negotiated at the WebSocket
				// upgrade. Reject any value (including "auto") so a ws:: string
				// carrying it is surfaced as malformed, matching the egress
				// QwpQueryClient and the other language clients.
				return nil, NewInvalidConfigStrError(
					"protocol_version is not supported for QWP; the protocol version is negotiated during the WebSocket upgrade")
			}
			if v != "auto" {
				version, err := strconv.Atoi(v)
				if err != nil {
					return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
				}
				pVersion := protocolVersion(version)
				if pVersion < ProtocolVersion1 || pVersion > ProtocolVersion3 {
					return nil, NewInvalidConfigStrError("current client only supports protocol version 1 (text format for all datatypes), 2 (binary format for part datatypes), 3 (decimals) or explicitly unset")
				}
				senderConf.protocolVersion = pVersion
			}
		case "close_timeout":
			// Java client never accepted close_timeout — only
			// close_flush_timeout_millis (Sender.java §3071). The
			// legacy Go-only key was a v4.0-era memory-mode knob;
			// the cursor architecture (CLAUDE.md) unified memory and
			// SF paths onto close_flush_timeout_millis. Reject with
			// a migration hint rather than silently dropping or
			// going through the generic "unsupported option" path.
			return nil, NewInvalidConfigStrError(
				"close_timeout is no longer supported; use close_flush_timeout_millis instead")
		case "sf_dir":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			senderConf.sfDir = v
		case "sender_id":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			if err := validateSenderId(v); err != nil {
				return nil, err
			}
			senderConf.senderId = v
		case "sf_max_segment_bytes":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			// 0 is the "use the default segment size" sentinel, resolved
			// at construction (qwpSfDefaultMaxBytes), matching the
			// WithSfMaxSegmentBytes option path. parseSizeBytes already rejects
			// negative and non-numeric input.
			parsedVal, err := parseSizeBytes(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a non-negative size", k, v)
			}
			senderConf.sfMaxSegmentBytes = parsedVal
		case "sf_max_total_bytes":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			// 0 is the "use the default total cap" sentinel, resolved at
			// construction (qwpSfDefaultMaxTotalBytes, or the memory-mode
			// variant), matching the WithSfMaxTotalBytes option path.
			parsedVal, err := parseSizeBytes(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a non-negative size", k, v)
			}
			senderConf.sfMaxTotalBytes = parsedVal
		case "sf_durability":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			if err := validateSfDurability(v); err != nil {
				return nil, err
			}
			senderConf.sfDurability = v
		case "sf_append_deadline_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.sfAppendDeadlineMillis = parsedVal
		case "auth_timeout_ms":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.authTimeoutMs = parsedVal
		case "connect_timeout":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := parseConnectTimeoutMillis(v)
			if err != nil {
				return nil, err
			}
			senderConf.connectTimeoutMs = parsedVal
		case "zone":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			// Egress consumes this via the (state, zone) priority
			// lattice (failover.md §2); the ingestion path does not
			// route by zone, so the value never reaches the SF tracker.
			// Silently accepted on both so a single connect string works
			// across ingress and egress clients without per-startup
			// noise.
			senderConf.zone = v
		case "target":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			// Egress consumes this as the connect-walk role filter,
			// matching against the server's advertised role. The
			// ingestion path does not route by role (role-based
			// endpoint selection is egress-only), so target is accepted
			// but inert on ingestion, symmetric with zone above. Parsed
			// here so a malformed value is still rejected on both paths.
			t, err := parseTargetFilter(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("%v", err)
			}
			senderConf.target = t
		case "reconnect_max_duration_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal < 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a non-negative int (milliseconds)", k, v)
			}
			senderConf.reconnectMaxDurationMillis = parsedVal
			senderConf.reconnectMaxDurationMillisSet = true
		case "reconnect_initial_backoff_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.reconnectInitialBackoffMillis = parsedVal
			senderConf.reconnectInitialBackoffMillisSet = true
		case "reconnect_max_backoff_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.reconnectMaxBackoffMillis = parsedVal
			senderConf.reconnectMaxBackoffMillisSet = true
		case "initial_connect_retry":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			switch v {
			case "on", "true", "sync":
				senderConf.initialConnectMode = InitialConnectSync
			case "off", "false":
				senderConf.initialConnectMode = InitialConnectOff
			case "async":
				senderConf.initialConnectMode = InitialConnectAsync
			default:
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not 'on' / 'off' / 'true' / 'false' / 'sync' / 'async'", k, v)
			}
			senderConf.initialConnectModeSet = true
		case "close_flush_timeout_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int (milliseconds)", k, v)
			}
			senderConf.closeFlushTimeoutSet = true
			senderConf.closeFlushTimeoutMillis = parsedVal
		case "drain_orphans":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			switch v {
			case "on", "true":
				senderConf.drainOrphans = true
			case "off", "false":
				senderConf.drainOrphans = false
			default:
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not 'on' / 'off' / 'true' / 'false'", k, v)
			}
		case "max_background_drainers":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal < 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a non-negative int", k, v)
			}
			senderConf.maxBackgroundDrainers = parsedVal
		case "on_server_error":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			pol, err := parseErrorPolicyValue(k, v, true)
			if err != nil {
				return nil, err
			}
			senderConf.errorPolicyGlobal = pol
		case "on_schema_error":
			if err := setPerCategoryPolicy(senderConf, k, v, CategorySchemaMismatch); err != nil {
				return nil, err
			}
		case "on_parse_error":
			if err := setPerCategoryPolicy(senderConf, k, v, CategoryParseError); err != nil {
				return nil, err
			}
		case "on_internal_error":
			if err := setPerCategoryPolicy(senderConf, k, v, CategoryInternalError); err != nil {
				return nil, err
			}
		case "on_security_error":
			if err := setPerCategoryPolicy(senderConf, k, v, CategorySecurityError); err != nil {
				return nil, err
			}
		case "on_write_error":
			if err := setPerCategoryPolicy(senderConf, k, v, CategoryWriteError); err != nil {
				return nil, err
			}
		case "error_inbox_capacity":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			// 0 is the "use the default capacity" sentinel, resolved at
			// construction (qwpSfDefaultErrorInboxCapacity), matching the
			// WithErrorInboxCapacity option path. Any other sub-floor value
			// is a user-supplied undersized inbox and is rejected.
			if parsedVal != 0 && parsedVal < qwpSfMinErrorInboxCapacity {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %d: must be >= %d",
					k, parsedVal, qwpSfMinErrorInboxCapacity)
			}
			senderConf.errorInboxCapacity = parsedVal
		case "request_durable_ack":
			if senderConf.senderType != qwpSenderType {
				// sf-client.md §4.6 mandates rejecting
				// request_durable_ack=on on non-WebSocket transports.
				// QWP (ws/wss) is the only WebSocket transport here, so
				// a non-QWP sender can never honour it -- reject the key
				// outright, consistent with every other SF key.
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			switch v {
			case "off", "false":
				// The default. Non-durable, OK-driven trim is fully
				// conformant (sf-client.md §9.2 / §19); nothing to wire.
			case "on", "true":
				// Durable-ack mode (sf-client.md §4.3 / §8.1 / §9.3 /
				// §10 / §11) is a deferred opt-in, EE-only QoS feature:
				// the cursor send loop OK-trims and silently ignores
				// DURABLE_ACK frames (qwp_sf_send_loop.go). §19 makes
				// the key normative so we accept it, but opting in is
				// rejected with a clear deferred-feature message rather
				// than the generic "unsupported option", mirroring
				// sf_durability=flush.
				return nil, NewInvalidConfigStrError(
					"request_durable_ack=%s is not yet supported: durable-ack mode is not implemented in this client (deferred follow-up; use request_durable_ack=off)", v)
			default:
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not 'on' / 'off' / 'true' / 'false'", k, v)
			}
		case "durable_ack_keepalive_interval_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			// Accepted for connect-string portability (sf-client.md
			// §4.3 / §19) but inert: it only paces keepalive PINGs in
			// durable-ack mode, which this client does not implement
			// (see request_durable_ack). Validate the shape so a typo
			// still errors helpfully; 0 / negative mean "disabled" per
			// spec, so any int is in range.
			if _, err := strconv.Atoi(v); err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not a valid int (milliseconds)", k, v)
			}
		case "transaction":
			// Transactional ingestion is a WebSocket-only ingress feature
			// (Sender.java). This client does not implement it, so an
			// explicit opt-in must fail instead of silently producing
			// ordinary writes. Rejected on the legacy ILP transports,
			// which never carry it.
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			switch v {
			case "off":
				// The default; this client has no transactional mode to
				// disable.
			case "on":
				return nil, NewInvalidConfigStrError(
					"transaction=on is not yet supported: transactional ingestion is not implemented in this client (use transaction=off)")
			default:
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not 'on' or 'off'", k, v)
			}
		case "connection_listener_inbox_capacity":
			// WebSocket-only ingress key (Sender.java). This client exposes
			// no connection-listener inbox; accepted on QWP as a validated
			// no-op for connect-string portability, rejected on legacy ILP.
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			if _, err := strconv.Atoi(v); err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not a valid int", k, v)
			}
		case "max_datagram_size", "multicast_ttl":
			// UDP-only ingress keys (Sender.java accepts them on every
			// transport). QWP is the WebSocket transport, where these keys
			// never apply, so reject them on a ws:: / wss:: connect string,
			// mirroring the egress QwpQueryClient and the protocol_version /
			// retry_timeout rejects. On the legacy ILP transports this client
			// has no UDP path either, so the key is inert there; the value is
			// shape-validated so a typo still errors, and the key is accepted
			// so a connect string shared across those transports parses.
			if senderConf.senderType == qwpSenderType {
				return nil, NewInvalidConfigStrError(
					"%s is not supported for QWP; it applies to the UDP transport only", k)
			}
			if _, err := strconv.Atoi(v); err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not a valid int", k, v)
			}
		default:
			if senderConf.senderType == qwpSenderType && egressOnlyKeys[k] {
				// Silently accepted on ingress so a single ws:: / wss::
				// connect string can configure both Sender and
				// QwpQueryClient. The Sender does not interpret the
				// value — range/enum/type checks run on the egress side
				// (qwp_query_conf.go). connect-string.md §16-20 and
				// §Query client keys are the load-bearing spec text.
				continue
			}
			return nil, NewInvalidConfigStrError("unsupported option %q", k)
		}
	}

	return senderConf, nil
}

// parseErrorPolicyValue parses a connect-string Policy value. When
// allowAuto is true, "auto" is accepted (used by the global
// on_server_error key whose default semantic is "use the per-category
// table"); per-category keys reject "auto" because the sentinel is
// only meaningful at the global layer.
func parseErrorPolicyValue(k, v string, allowAuto bool) (Policy, error) {
	switch v {
	case "halt":
		return PolicyHalt, nil
	case "drop":
		return PolicyDropAndContinue, nil
	case "auto":
		if allowAuto {
			return PolicyAuto, nil
		}
	}
	if allowAuto {
		return PolicyAuto, NewInvalidConfigStrError(
			"invalid %s value, %q is not 'auto' / 'halt' / 'drop'", k, v)
	}
	return PolicyAuto, NewInvalidConfigStrError(
		"invalid %s value, %q is not 'halt' / 'drop'", k, v)
}

// setPerCategoryPolicy parses v as a Policy and stores it on the
// per-category override slot for c, gating to QWP and setting the
// per-category-set flag for sanitizer routing.
func setPerCategoryPolicy(conf *lineSenderConfig, k, v string, c Category) error {
	if conf.senderType != qwpSenderType {
		return NewInvalidConfigStrError("%s is only supported for QWP senders", k)
	}
	pol, err := parseErrorPolicyValue(k, v, false)
	if err != nil {
		return err
	}
	conf.errorPolicyPerCat[c] = pol
	conf.errorPolicyPerCatSet = true
	return nil
}

// validateSfDurability checks an sf_durability value. The empty
// string means "unset" (defaults to memory at construction); only
// "memory" is currently honoured. "flush" / "append" are reserved
// for a deferred follow-up and rejected with a pointer to the
// supported value. Shared by the connect-string parser and
// sanitizeQwpConf so the WithSfDurability functional-option path
// rejects identically — single source of truth for the value space.
func validateSfDurability(v string) error {
	switch v {
	case "", "memory":
		return nil
	case "flush", "append":
		return NewInvalidConfigStrError(
			"sf_durability=%s is not yet supported (deferred follow-up; use sf_durability=memory)", v)
	default:
		return NewInvalidConfigStrError(
			"invalid sf_durability value, %q is not 'memory' (other values reserved for future use)", v)
	}
}

// parseSizeBytes parses a size-typed connect-string value: a non-
// negative decimal integer optionally followed by a JVM-style 1024-
// based size suffix. connect-string.md §Size suffixes: suffixes are
// case-insensitive (k / kb / m / mb / g / gb / t / tb). Plain
// integers (no suffix) are parsed as bytes. Returns an error for
// empty input, non-numeric prefixes, unknown suffixes, negative
// values, or int64 overflow.
//
// The longest known suffix wins ("kb" before "k"), so "1kb" is 1024
// and not 1 followed by an unparsed "kb".
func parseSizeBytes(v string) (int64, error) {
	if v == "" {
		return 0, fmt.Errorf("empty size value")
	}
	s := strings.ToLower(v)
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "kb"):
		mult, s = 1<<10, s[:len(s)-2]
	case strings.HasSuffix(s, "mb"):
		mult, s = 1<<20, s[:len(s)-2]
	case strings.HasSuffix(s, "gb"):
		mult, s = 1<<30, s[:len(s)-2]
	case strings.HasSuffix(s, "tb"):
		mult, s = 1<<40, s[:len(s)-2]
	case strings.HasSuffix(s, "k"):
		mult, s = 1<<10, s[:len(s)-1]
	case strings.HasSuffix(s, "m"):
		mult, s = 1<<20, s[:len(s)-1]
	case strings.HasSuffix(s, "g"):
		mult, s = 1<<30, s[:len(s)-1]
	case strings.HasSuffix(s, "t"):
		mult, s = 1<<40, s[:len(s)-1]
	}
	if s == "" {
		return 0, fmt.Errorf("no number before size suffix in %q", v)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %v", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("size %q must be non-negative", v)
	}
	if mult > 1 && n > 0 && n > (1<<62)/mult {
		return 0, fmt.Errorf("size %q overflows int64", v)
	}
	return n * mult, nil
}

func parseConnectTimeoutMillis(value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, NewInvalidConfigStrError(
			"invalid connect_timeout value, %q must be a positive int (milliseconds)", value)
	}
	const maxDurationMillis = math.MaxInt64 / int64(time.Millisecond)
	if int64(parsed) > maxDurationMillis {
		return 0, NewInvalidConfigStrError(
			"invalid connect_timeout value, %q exceeds the maximum of %d milliseconds",
			value, maxDurationMillis)
	}
	return parsed, nil
}

// validateSenderId enforces the same character set the Java client
// allows for sender_id: ASCII letters, digits, '-', '_'. Matches
// Sender.java validateSenderId (no '.', no path separators, no
// spaces) and the connect-string spec at §Store-and-forward "Allowed
// characters: letters, digits, `_`, `-`". The value is used as a path
// segment under sf_dir; '.' is excluded to keep slot names stable
// across filesystems and avoid '..' surprises.
func validateSenderId(id string) error {
	if id == "" {
		return NewInvalidConfigStrError("sender_id must not be empty")
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_'
		if !ok {
			return NewInvalidConfigStrError(
				"sender_id contains invalid character: %q (allowed: letters, digits, _ -)",
				string(c))
		}
	}
	return nil
}

func parseConfigStr(conf string) (configData, error) {
	var (
		key    = &strings.Builder{}
		value  = &strings.Builder{}
		isKey  = true
		result = configData{
			KeyValuePairs: map[string]string{},
		}
		seenKeys = map[string]bool{}

		nextRune   rune
		isEscaping bool
	)

	schemaStr, conf, found := strings.Cut(conf, "::")
	if !found {
		return result, NewInvalidConfigStrError("no schema separator found '::'")
	}

	result.Schema = schemaStr

	if len(conf) == 0 {
		return result, NewInvalidConfigStrError("'addr' key not found")
	}

	if !strings.HasSuffix(conf, ";") {
		conf += ";"
	}

	keyValueStr := []rune(conf)
	for idx, rune := range keyValueStr {
		if idx < len(conf)-1 {
			nextRune = keyValueStr[idx+1]
		} else {
			nextRune = 0
		}
		switch rune {
		case ';':
			if isKey {
				if nextRune == 0 {
					return result, NewInvalidConfigStrError("unexpected end of string")
				}
				return result, NewInvalidConfigStrError("invalid key character ';'")
			}

			if !isEscaping && nextRune == ';' {
				isEscaping = true
				continue
			}

			if isEscaping {
				value.WriteRune(rune)
				isEscaping = false
				continue
			}

			if value.Len() == 0 {
				return result, NewInvalidConfigStrError("empty value for key %q", key)
			}

			// Reject duplicate raw keys (case-sensitive) for parity with Rust
			// and the per-field checks in Java. Deprecated aliases are
			// canonicalized after this raw-duplicate check, so an
			// alias/canonical pair is allowed and resolves last-write-wins.
			// `addr` is the documented exception: the failover spec (§1)
			// allows `addr=h1;addr=h2` as an alternative spelling of
			// `addr=h1,h2`. Both forms accumulate into a single
			// comma-joined value so downstream parsers see one shape.
			rawKey := key.String()
			if seenKeys[rawKey] && rawKey != "addr" {
				return result, NewInvalidConfigStrError("duplicate key %q", rawKey)
			}
			seenKeys[rawKey] = true

			keyStr := canonicalConfigKey(rawKey)
			if existing, exists := result.KeyValuePairs[keyStr]; exists && keyStr == "addr" {
				result.KeyValuePairs[keyStr] = existing + "," + value.String()
			} else {
				result.KeyValuePairs[keyStr] = value.String()
			}

			key.Reset()
			value.Reset()
			isKey = true
		case '=':
			if isKey {
				isKey = false
			} else {
				value.WriteRune(rune)
			}
		default:
			if isKey {
				key.WriteRune(rune)
			} else {
				value.WriteRune(rune)
			}
		}
	}

	if isEscaping {
		return result, NewInvalidConfigStrError("unescaped ';'")
	}

	return result, nil
}

func canonicalConfigKey(key string) string {
	switch key {
	case "user":
		return "username"
	case "pass":
		return "password"
	default:
		return key
	}
}
