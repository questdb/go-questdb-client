/*******************************************************************************
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
	default:
		return nil, fmt.Errorf("invalid schema: %s", data.Schema)
	}

	for k, v := range data.KeyValuePairs {
		switch k {
		case "addr":
			senderConf.address = v
		case "username":
			switch senderConf.senderType {
			case httpSenderType:
				senderConf.httpUser = v
			case tcpSenderType:
				senderConf.tcpKeyId = v
			default:
				panic("add a case for " + k)
			}
		case "password":
			if senderConf.senderType != httpSenderType {
				return nil, NewInvalidConfigStrError("%s is only supported for HTTP sender", k)
			}
			senderConf.httpPass = v
		case "token":
			switch senderConf.senderType {
			case httpSenderType:
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
		case "request_min_throughput", "init_buf_size", "max_buf_size":
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
				if pVersion < ProtocolVersion1 || pVersion > ProtocolVersion2 {
					return nil, NewInvalidConfigStrError("current client only supports protocol version 1 (text format for all datatypes), 2 (binary format for part datatypes) or explicitly unset")
				}
				senderConf.protocolVersion = pVersion
			}
		default:
			return nil, NewInvalidConfigStrError("unsupported option %q", k)
		}
	}

	return senderConf, nil
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

			result.KeyValuePairs[key.String()] = value.String()

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
