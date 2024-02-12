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

package v3

import (
	"strconv"
	"strings"
	"time"
)

type schemaType string

const (
	schemaHttp  schemaType = "http"
	schemaHttps schemaType = "https"
	schemaTcp   schemaType = "tcp"
	schemaTcps  schemaType = "tcps"
)

func parseConfigString(conf string) ([]LineSenderOption, error) {
	var (
		key, value        *strings.Builder
		lastRune          rune
		isEscaping        bool
		isKey             bool
		opts              []LineSenderOption
		user, pass, token string
	)

	splitStr := strings.SplitAfterN(string(conf), "::", 1)
	if len(splitStr) < 2 {
		return opts, NewConfigStrParseError("no schema separator found '::'")
	}

	schema := schemaType(splitStr[0])
	switch schema {
	case schemaHttp:
		opts = append(opts, WithHttp())
	case schemaHttps:
		opts = append(opts, WithHttp(), WithTls())
	case schemaTcp:
		opts = append(opts, WithTcp())
	case schemaTcps:
		opts = append(opts, WithTcp(), WithTls())
	default:
		return opts, NewConfigStrParseError("invalid schema %q", schema)
	}

	keyValueStr := []rune(splitStr[1])
	for idx, rune := range keyValueStr {
		if idx > 0 {
			lastRune = keyValueStr[idx-1]
		}
		switch rune {
		case ';':
			if isKey {
				key.WriteRune(rune)
				continue
			}

			if isEscaping {
				value.WriteRune(rune)
				isEscaping = false
				continue
			}

			if !isEscaping && lastRune == ';' {
				isEscaping = true
				continue
			}

			switch strings.ToLower(key.String()) {
			case "addr":
				opts = append(opts, WithAddress(value.String()))
			case "user":
				user = value.String()
				if user != "" && pass != "" {
					opts = append(opts, WithBasicAuth(user, pass))
				}
				if token != "" && user != "" {
					opts = append(opts, WithAuth(user, token))
				}
			case "pass":
				pass = value.String()
				if user != "" && pass != "" {
					opts = append(opts, WithBasicAuth(user, pass))
				}
			case "token":
				token = value.String()
				if token != "" && user != "" {
					opts = append(opts, WithAuth(user, token))
				}
			case "auto_flush", "auto_flush_rows", "auto_flush_bytes":
				if value.String() == "on" {
					return opts, NewConfigStrParseError("auto_flush option not available for this client")
				}
			case "min_throughput", "init_buf_size", "max_buf_size":
				parsedVal, err := strconv.Atoi(value.String())
				if err != nil {
					return opts, NewConfigStrParseError("invalid %s value, %q is not a valid int", key, value.String())

				}
				switch key.String() {
				case "min_throughput":
					opts = append(opts, WithMinThroughput(parsedVal))
				case "init_buf_size":
					opts = append(opts, WithInitBufferSize(parsedVal))
				case "max_buf_size":
					opts = append(opts, WithBufferCapacity(parsedVal))
				default:
					panic("add a case for " + key.String())
				}

			case "grace_timeout", "retry_timeout":
				timeout, err := time.ParseDuration(value.String())
				if err != nil {
					return opts, NewConfigStrParseError("invalid %s value, %q is not a valid duration", key, value)
				}
				switch key.String() {
				case "grace_timeout":
					opts = append(opts, WithGraceTimeout(timeout))
				case "retry_timeout":
					opts = append(opts, WithRetryTimeout(timeout))
				default:
					panic("add a case for " + key.String())
				}
			case "tls_verify":
				switch value.String() {
				case "on":
					opts = append(opts, WithTls())
				case "unsafe_off":
					opts = append(opts, WithTlsInsecureSkipVerify())
				default:
					return opts, NewConfigStrParseError("invalid tls_verify value, %q is not 'on' or 'unsafe_off", value)
				}
			case "tls_roots":
				return opts, NewConfigStrParseError("tls_roots is not available in the go client")
			case "tls_roots_password":
				return opts, NewConfigStrParseError("tls_roots_password is not available in the go client")
			default:
				return opts, NewConfigStrParseError("unsupported config key %q", key)
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
	return opts, nil
}
