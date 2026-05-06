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
	case "ws":
		senderConf = newLineSenderConfig(qwpSenderType)
	case "wss":
		senderConf = newLineSenderConfig(qwpSenderType)
		senderConf.tlsMode = tlsEnabled
	default:
		return nil, fmt.Errorf("invalid schema: %s", data.Schema)
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
			if v == "off" {
				senderConf.autoFlushRows = 0
				senderConf.autoFlushInterval = 0
				senderConf.autoFlushBytes = 0
			} else if v != "on" {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not 'on' or 'off'", k, v)
			}
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
			if v == "off" {
				senderConf.autoFlushBytes = 0
				continue
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			senderConf.autoFlushBytes = parsedVal
		case "request_min_throughput", "init_buf_size", "max_buf_size", "max_name_len":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}

			switch k {
			case "request_min_throughput":
				senderConf.minThroughput = parsedVal
			case "init_buf_size":
				senderConf.initBufSize = parsedVal
			case "max_buf_size":
				senderConf.maxBufSize = parsedVal
			case "max_name_len":
				senderConf.fileNameLimit = parsedVal
			default:
				panic("add a case for " + k)
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
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int (milliseconds)", k, v)
			}
			senderConf.closeTimeout = time.Duration(parsedVal) * time.Millisecond
		case "max_schemas_per_connection":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			senderConf.maxSchemasPerConnection = parsedVal
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
			parsedVal, err := strconv.ParseInt(v, 10, 64)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int", k, v)
			}
			senderConf.sfMaxBytes = parsedVal
		case "sf_max_total_bytes":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.ParseInt(v, 10, 64)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int", k, v)
			}
			senderConf.sfMaxTotalBytes = parsedVal
		case "sf_durability":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			switch v {
			case "memory":
				senderConf.sfDurability = v
			case "flush", "append":
				return nil, NewInvalidConfigStrError(
					"sf_durability=%s is not yet supported (deferred follow-up; use sf_durability=memory)", v)
			default:
				return nil, NewInvalidConfigStrError(
					"invalid sf_durability value, %q is not 'memory' (other values reserved for future use)", v)
			}
		case "sf_append_deadline_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.sfAppendDeadlineMillis = parsedVal
		case "reconnect_max_duration_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal < 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a non-negative int (milliseconds)", k, v)
			}
			senderConf.reconnectMaxDurationMillis = parsedVal
		case "reconnect_initial_backoff_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.reconnectInitialBackoffMillis = parsedVal
		case "reconnect_max_backoff_millis":
			if senderConf.senderType != qwpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for QWP senders", k)
			}
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal <= 0 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q must be a positive int (milliseconds)", k, v)
			}
			senderConf.reconnectMaxBackoffMillis = parsedVal
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
			if parsedVal < qwpSfMinErrorInboxCapacity {
				return nil, NewInvalidConfigStrError(
					"invalid %s value, %d: must be >= %d",
					k, parsedVal, qwpSfMinErrorInboxCapacity)
			}
			senderConf.errorInboxCapacity = parsedVal
		default:
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

// validateSenderId enforces the same character set the Java client
// allows for sender_id: ASCII letters, digits, '-', '_', '.'. The
// value is used as a path segment under sf_dir; permitting '/' or
// '\\' would let users traverse out of the slot dir.
func validateSenderId(id string) error {
	if id == "" {
		return NewInvalidConfigStrError("sender_id must not be empty")
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
		if !ok {
			return NewInvalidConfigStrError("sender_id contains invalid character: %q", string(c))
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
			keyStr := key.String()
			if _, exists := result.KeyValuePairs[keyStr]; exists {
				return result, NewInvalidConfigStrError("duplicate key %q", keyStr)
			}
			result.KeyValuePairs[keyStr] = value.String()

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
