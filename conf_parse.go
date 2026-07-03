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
	"compression":                 true,
	"compression_level":           true,
	"failover":                    true,
	"failover_backoff_initial_ms": true,
	"failover_backoff_max_ms":     true,
	"failover_max_attempts":       true,
	"failover_max_duration_ms":    true,
	"initial_credit":              true,
	"max_batch_rows":              true,
	// Egress query keys the ingress parser ignores so a shared ws:: / wss::
	// string (notably the facade's) validates through both parsers.
	"auth":                   true,
	"client_id":              true,
	"path":                   true,
	"query_close_timeout_ms": true,
	"replay_exec":            true,
	"server_info_timeout_ms": true,
}

// ingressOnlyKeys lists connect-string keys defined by the spec for
// the ingress LineSender only. The egress QwpQueryClient silently
// accepts them so a shared connect string works in both directions.
// Same SSOT as egressOnlyKeys; the lists are kept in sync with
// connect-string.md §Key index.
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
	"max_frame_rejections":                  true,
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
	"sf_max_bytes":                          true,
	"sf_max_total_bytes":                    true,
	// QWP ingest keys the egress parser ignores so a shared ws:: / wss::
	// string (notably the facade's) validates through both parsers.
	// gorilla / in_flight_window are the documented Java-parity knobs;
	// token_x / token_y are legacy public-key fields the ingest client
	// accepts-but-ignores. (The HTTP-only protocol_version / request_timeout
	// / retry_timeout / request_min_throughput are deliberately absent: the
	// QWP ingest client rejects them in sanitizeQwpConf, so both parsers
	// rejecting them is the correct, symmetric behaviour.)
	"gorilla":          true,
	"in_flight_window": true,
	"token_x":          true,
	"token_y":          true,
}

// poolKeys are the facade-owned (Side.POOL) connect-string keys. They are
// meaningful only to the QuestDB facade (questdb.go); both standalone clients
// accept-but-ignore them on ws/wss so one cluster config validates through the
// ingest and egress parsers alike. Kept in sync with QuestDB facade options.
var poolKeys = map[string]bool{
	"lazy_connect":            true,
	"sender_pool_min":         true,
	"sender_pool_max":         true,
	"query_pool_min":          true,
	"query_pool_max":          true,
	"acquire_timeout_ms":      true,
	"idle_timeout_ms":         true,
	"max_lifetime_ms":         true,
	"housekeeper_interval_ms": true,
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
		case "username":
			switch senderConf.senderType {
			case httpSenderType, qwpSenderType:
				senderConf.httpUser = v
			case tcpSenderType:
				senderConf.tcpKeyId = v
			default:
				panic("add a case for " + k)
			}
		case "password":
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
		case "token_x":
		case "token_y":
			// Some clients require public key.
			// But since Go sender doesn't need it, we ignore the values.
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
		case "connect_timeout":
			// COMMON key (Java parity): accepted on every schema so a shared
			// connect string ports. Wired on HTTP and QWP; inert on TCP.
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.connectTimeoutMs = parsedVal
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
		case "in_flight_window":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			senderConf.inFlightWindow = parsedVal
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
		case "gorilla":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			switch v {
			case "on":
				senderConf.gorillaDisabled = false
			case "off":
				senderConf.gorillaDisabled = true
			default:
				return nil, NewInvalidConfigStrError("invalid gorilla value, %q is not 'on' or 'off'", v)
			}
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
		case "sf_max_bytes":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			// 0 is the "use the default segment size" sentinel, resolved
			// at construction (qwpSfDefaultMaxBytes), matching the
			// WithSfMaxBytes option path. parseSizeBytes already rejects
			// negative and non-numeric input.
			parsedVal, err := parseSizeBytes(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a non-negative size", k, v)
			}
			senderConf.sfMaxBytes = parsedVal
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
		case "max_frame_rejections":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal < 1 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be an int >= 1", k, v)
			}
			senderConf.maxFrameRejections = parsedVal
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
			// WithErrorInboxCapacity option path. Any other out-of-range value
			// is rejected: a sub-floor inbox is undersized, and an over-ceiling
			// one would panic make(chan) at construction.
			if parsedVal != 0 && (parsedVal < qwpSfMinErrorInboxCapacity || parsedVal > qwpSfMaxErrorInboxCapacity) {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %d: must be in [%d, %d]",
					k, parsedVal, qwpSfMinErrorInboxCapacity, qwpSfMaxErrorInboxCapacity)
			}
			senderConf.errorInboxCapacity = parsedVal
		case "connection_listener_inbox_capacity":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			// 0 is the "use the default capacity" sentinel; any other out-of-range
			// value is rejected (sub-floor undersized, over-ceiling would panic
			// make(chan) at construction).
			if parsedVal != 0 && (parsedVal < qwpSfMinErrorInboxCapacity || parsedVal > qwpSfMaxErrorInboxCapacity) {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %d: must be in [%d, %d]",
					k, parsedVal, qwpSfMinErrorInboxCapacity, qwpSfMaxErrorInboxCapacity)
			}
			senderConf.connectionListenerInboxCapacity = parsedVal
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
				// The default. Non-durable, OK-driven trim (sf-client.md §9.2).
			case "on", "true":
				// Durable-ack mode: the cursor send loop trims / replays / awaits
				// on STATUS_DURABLE_ACK instead of the OK ACK
				// (design/qwp-cursor-durability.md).
				senderConf.requestDurableAck = true
			default:
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not 'on' / 'off' / 'true' / 'false'", k, v)
			}
		case "durable_ack_keepalive_interval_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			// Paces the durable-ack keepalive ping; 0 / negative disable it.
			ms, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %q is not a valid int (milliseconds)", k, v)
			}
			senderConf.durableAckKeepaliveMillis = ms
			senderConf.durableAckKeepaliveMillisSet = true
		default:
			if senderConf.senderType == qwpSenderType && (egressOnlyKeys[k] || poolKeys[k]) {
				// Silently accepted on ingress so a single ws:: / wss::
				// connect string can configure both Sender and
				// QwpQueryClient. The Sender does not interpret the
				// value — range/enum/type checks run on the egress side
				// (qwp_query_conf.go). connect-string.md §16-20 and
				// §Query client keys are the load-bearing spec text.
				// poolKeys are facade-owned (Side.POOL): both standalone
				// clients accept-but-ignore them so the QuestDB facade can
				// validate one cluster config through both parsers.
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
	case "terminal":
		return PolicyTerminal, nil
	case "retriable":
		return PolicyRetriable, nil
	case "retriable_other":
		return PolicyRetriableOther, nil
	case "halt", "drop":
		// NACK policy v2 removed the drop policy (no silent data loss)
		// and renamed halt; fail loudly with a migration hint instead of
		// silently reinterpreting an old config.
		return PolicyAuto, NewInvalidConfigStrError(
			"invalid %s value: %q was removed by NACK policy v2 — use 'terminal', 'retriable', or 'retriable_other'", k, v)
	case "auto":
		if allowAuto {
			return PolicyAuto, nil
		}
	}
	if allowAuto {
		return PolicyAuto, NewInvalidConfigStrError(
			"invalid %s value, %q is not 'auto' / 'terminal' / 'retriable' / 'retriable_other'", k, v)
	}
	return PolicyAuto, NewInvalidConfigStrError(
		"invalid %s value, %q is not 'terminal' / 'retriable' / 'retriable_other'", k, v)
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

			// Reject duplicate keys (case-sensitive) for parity with Rust and
			// the per-field checks in Java; otherwise dups would silently LWW.
			// `addr` is the documented exception: the failover spec (§1)
			// allows `addr=h1;addr=h2` as an alternative spelling of
			// `addr=h1,h2`. Both forms accumulate into a single
			// comma-joined value so downstream parsers see one shape.
			keyStr := key.String()
			if existing, exists := result.KeyValuePairs[keyStr]; exists {
				if keyStr == "addr" {
					result.KeyValuePairs[keyStr] = existing + "," + value.String()
				} else {
					return result, NewInvalidConfigStrError("duplicate key %q", keyStr)
				}
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
