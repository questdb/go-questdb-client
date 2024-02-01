package questdb

import (
	"fmt"
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

func parseConfigString(conf string, s *LineSender) error {
	var (
		address    string
		key, value *strings.Builder
		lastRune   rune
		isEscaping bool
		isKey      bool
	)

	splitStr := strings.SplitAfterN(string(conf), "::", 1)
	if len(splitStr) < 2 {
		return NewConfigStrParseError("no schema separator found '::'")
	}

	schema := schemaType(splitStr[0])
	switch schema {
	case schemaHttp, schemaHttps, schemaTcp, schemaTcps:
		address += fmt.Sprintf("%s://", address)
	default:
		return NewConfigStrParseError("invalid schema %q", schema)
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

			processConfigKeyValuePair(key.String(), value.String(), s)
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
}

func processConfigKeyValuePair(key, value string, s *LineSender) error {
	switch strings.ToLower(key) {
	case "addr":
		s.address = value
	case "user":
		s.user = value
	case "pass":
		s.pass = value
	case "token":
		s.token = value
	case "auto_flush", "auto_flush_rows", "auto_flush_bytes":
		if value == "on" {
			return NewConfigStrParseError("auto_flush option not available for this client")
		}
	case "min_throughput", "init_buf_size", "max_buf_size":
		parsedVal, err := strconv.Atoi(value)
		if err != nil {
			return NewConfigStrParseError("invalid %s value, %q is not a valid int", key, value)
		}
		switch key {
		case "min_throughput":
			s.minThroughputBytesPerSecond = parsedVal
		case "init_buf_size":
			s.initBufSizeBytes = parsedVal
		case "max_buf_size":
			s.maxBufSizeBytes = parsedVal
		default:
			panic("add a case for " + key)
		}

	case "grace_timeout", "retry_timeout":
		timeout, err := time.ParseDuration(value)
		if err != nil {
			return NewConfigStrParseError("invalid %s value, %q is not a valid duration", key, value)
		}
		switch key {
		case "grace_timeout":
			s.graceTimeout = timeout
		case "retry_timeout":
			s.retryTimeout = timeout
		default:
			panic("add a case for " + key)
		}
	case "tls_verify":
		switch value {
		case "on":
			s.tlsMode = tlsEnabled
		case "unsafe_off":
			s.tlsMode = tlsDisabled
		default:
			return NewConfigStrParseError("invalid tls_verify value, %q is not 'on' or 'unsafe_off", value)
		}
	case "tls_roots":
		s.tlsRoots = value
	case "tls_roots_password":
		return NewConfigStrParseError("tls_roots_password is not available in the go client")
	default:
		return NewConfigStrParseError("unsupported config key %q", key)
	}
}
