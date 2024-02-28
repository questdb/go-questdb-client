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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type HttpLineSender struct {
	buffer

	address                     string
	retryTimeout                time.Duration
	tlsMode                     tlsMode
	minThroughputBytesPerSecond int
	graceTimeout                time.Duration

	user  string
	pass  string
	token string

	client http.Client
}

type HttpLineSenderOption func(s *HttpLineSender)

func NewHttpLineSender(opts ...HttpLineSenderOption) (*HttpLineSender, error) {
	s := &HttpLineSender{
		address:                     "127.0.0.1:9000",
		minThroughputBytesPerSecond: 100 * 1024,
		graceTimeout:                5 * time.Second,
		retryTimeout:                10 * time.Second,

		buffer: buffer{
			bufCap:        defaultBufferCapacity,
			fileNameLimit: defaultFileNameLimit,
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.address == "" {
		s.address = "127.0.0.1:9000"
	}

	if s.tlsMode == tlsEnabled && tlsClientConfig.InsecureSkipVerify {
		return nil, errors.New("once InsecureSkipVerify is used, it must be enforced for all subsequent HttpLineSenders")
	}

	if s.tlsMode == tlsInsecureSkipVerify {
		tlsClientConfig.InsecureSkipVerify = true
	}

	s.client = http.Client{
		Transport: &globalTransport,
		Timeout:   0, // todo: add a global maximum timeout?
	}

	clientCt.Add(1)

	return s, nil
}

var (
	tlsClientConfig tls.Config = tls.Config{}
	clientCt        atomic.Int64

	globalTransport http.Transport = http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxConnsPerHost:       0,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tlsClientConfig,
	}
)

func WithBasicAuth(user, pass string) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.user = user
		s.pass = pass
	}
}

func WithBearerToken(token string) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.token = token
	}
}

func WithGraceTimeout(timeout time.Duration) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.graceTimeout = timeout
	}
}

func WithRetryTimeout(t time.Duration) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.retryTimeout = t
	}
}

func WithMinThroughput(bytesPerSecond int) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.minThroughputBytesPerSecond = bytesPerSecond
	}
}

func WithHttpInitBufferSize(sizeInBytes int) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.initBufSizeBytes = sizeInBytes
	}
}

func WithHttpAddress(addr string) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.address = addr
	}
}

// WithHttpTls enables TLS connection encryption.
func WithHttpTls() HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.tlsMode = tlsEnabled
	}
}

// WithTlsInsecureSkipVerify enables TLS connection encryption,
// but skips server certificate verification. Useful in test
// environments with self-signed certificates. Do not use in
// production environments.
func WithHttpTlsInsecureSkipVerify() HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.tlsMode = tlsInsecureSkipVerify
	}
}

// WithHttpBufferCapacity sets desired buffer capacity in bytes to
// be used when sending ILP messages. Defaults to 128KB.
//
// This setting is a soft limit, i.e. the underlying buffer may
// grow larger than the provided value, but will shrink on a
// At, AtNow, or Flush call.
func WithHttpBufferCapacity(capacity int) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		if capacity > 0 {
			s.bufCap = capacity
		}
	}
}

func HttpLineSenderFromConf(ctx context.Context, conf string) (*HttpLineSender, error) {
	var (
		user, pass, token string
	)

	data, err := parseConfigString(conf)
	if err != nil {
		return nil, err
	}

	if data.schema != "http" && data.schema != "https" {
		return nil, fmt.Errorf("invalid schema: %s", data.schema)
	}

	opts := make([]HttpLineSenderOption, 0)
	for k, v := range data.keyValuePairs {

		switch strings.ToLower(k) {
		case "addr":
			opts = append(opts, WithHttpAddress(v))
		case "user":
			user = v
			if user != "" && pass != "" {
				opts = append(opts, WithBasicAuth(user, pass))
			}
		case "pass":
			pass = v
			if user != "" && pass != "" {
				opts = append(opts, WithBasicAuth(user, pass))
			}
		case "token":
			token = v
			opts = append(opts, WithBearerToken(token))
		case "auto_flush":
			if v == "on" {
				return nil, NewConfigStrParseError("auto_flush option is not supported")
			}
		case "auto_flush_rows", "auto_flush_bytes":
			return nil, NewConfigStrParseError("auto_flush option is not supported")
		case "min_throughput", "init_buf_size", "max_buf_size":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)

			}
			switch k {
			case "min_throughput":
				opts = append(opts, WithMinThroughput(parsedVal))
			case "init_buf_size":
				opts = append(opts, WithHttpInitBufferSize(parsedVal))
			case "max_buf_size":
				opts = append(opts, WithHttpBufferCapacity(parsedVal))
			default:
				panic("add a case for " + k)
			}

		case "grace_timeout", "retry_timeout":
			timeout, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)
			}

			timeoutDur := time.Duration(timeout * int(time.Millisecond))

			switch k {
			case "grace_timeout":
				opts = append(opts, WithGraceTimeout(timeoutDur))
			case "retry_timeout":
				opts = append(opts, WithRetryTimeout(timeoutDur))
			default:
				panic("add a case for " + k)
			}
		case "tls_verify":
			switch v {
			case "on":
				opts = append(opts, WithHttpTls())
			case "unsafe_off":
				opts = append(opts, WithHttpTlsInsecureSkipVerify())
			default:
				return nil, NewConfigStrParseError("invalid tls_verify value, %q is not 'on' or 'unsafe_off", v)
			}
		case "tls_roots":
			return nil, NewConfigStrParseError("tls_roots is not available in the go client")
		case "tls_roots_password":
			return nil, NewConfigStrParseError("tls_roots_password is not available in the go client")
		default:
			return nil, NewConfigStrParseError("unsupported option %q", k)
		}

	}
	return NewHttpLineSender(opts...)
}

func (s *HttpLineSender) Flush(ctx context.Context) error {
	var (
		err           error
		req           *http.Request
		retryInterval time.Duration

		maxRetryInterval = time.Second
	)
	uri := "http"
	if s.tlsMode > 0 {
		uri += "s"
	}
	uri += fmt.Sprintf("://%s/write", s.address)

	// timeout = ( request.len() / min_throughput ) + grace
	// Conversion from int to time.Duration is in milliseconds and grace is in millis
	timeout := time.Duration(s.Len()/s.minThroughputBytesPerSecond)*time.Second + s.graceTimeout
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err = http.NewRequestWithContext(
		reqCtx,
		http.MethodPost,
		uri,
		s,
	)
	if err != nil {
		return err
	}

	if s.user != "" && s.pass != "" {
		req.SetBasicAuth(s.user, s.pass)
	}

	if s.token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", s.token))
	}

	resp, err := s.client.Do(req)
	if err == nil {
		defer resp.Body.Close()

		// If the requests succeeds with a non-error status code, flush has succeeded
		if !isRetryableError(resp.StatusCode) {
			return nil
		}
		// Otherwise, we will retry until the timeout
		err = fmt.Errorf("Non-OK Status Code %d: %s", resp.StatusCode, resp.Status)
	}

	if s.retryTimeout > 0 {
		retryInterval = 10 * time.Millisecond
		for err != nil {
			jitter := time.Duration(rand.Intn(10)) * time.Millisecond
			time.Sleep(retryInterval + jitter)

			resp, retryErr := s.client.Do(req)
			if retryErr == nil {
				defer resp.Body.Close()

				if !isRetryableError(resp.StatusCode) {
					return nil
				}
				retryErr = fmt.Errorf("Non-OK Status Code %d: %s", resp.StatusCode, resp.Status)
			}

			var urlErr *url.Error
			if errors.As(retryErr, &urlErr) {
				if urlErr.Timeout() {
					return err
				}
			}

			err = retryErr

			retryInterval = retryInterval * 2
			if retryInterval > maxRetryInterval {
				retryInterval = maxRetryInterval
			}
		}
	}

	return err
}

// Table sets the table name (metric) for a new ILP message. Should be
// called before any Symbol or Column method.
//
// Table name cannot contain any of the following characters:
// '\n', '\r', '?', ',', ”', '"', '\', '/', ':', ')', '(', '+', '*',
// '%', '~', starting '.', trailing '.', or a non-printable char.
func (s *HttpLineSender) Table(name string) *HttpLineSender {
	s.buffer.table(name)
	return s
}

// Symbol adds a symbol column value to the ILP message. Should be called
// before any Column method.
//
// Symbol name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *HttpLineSender) Symbol(name, val string) *HttpLineSender {
	s.buffer.symbol(name, val)
	return s
}

// Int64Column adds a 64-bit integer (long) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *HttpLineSender) Int64Column(name string, val int64) *HttpLineSender {
	s.buffer.int64Column(name, val)
	return s
}

// Long256Column adds a 256-bit unsigned integer (long256) column
// value to the ILP message.
//
// Only non-negative numbers that fit into 256-bit unsigned integer are
// supported and any other input value would lead to an error.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *HttpLineSender) Long256Column(name string, val *big.Int) *HttpLineSender {
	s.buffer.long256Column(name, val)
	return s
}

// TimestampColumn adds a timestamp column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *HttpLineSender) TimestampColumn(name string, ts time.Time) *HttpLineSender {
	s.buffer.timestampColumn(name, ts)
	return s
}

// Float64Column adds a 64-bit float (double) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *HttpLineSender) Float64Column(name string, val float64) *HttpLineSender {
	s.buffer.float64Column(name, val)
	return s
}

// StringColumn adds a string column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *HttpLineSender) StringColumn(name, val string) *HttpLineSender {
	s.stringColumn(name, val)
	return s
}

// BoolColumn adds a boolean column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *HttpLineSender) BoolColumn(name string, val bool) *HttpLineSender {
	s.buffer.boolColumn(name, val)
	return s
}

func (s *HttpLineSender) Close() {
	newCt := clientCt.Add(-1)
	if newCt == 0 {
		globalTransport.CloseIdleConnections()
	}
}

// AtNow omits the timestamp and finalizes the ILP message.
// The server will insert each message using the system clock
// as the row timestamp.
//
// If the underlying buffer reaches configured capacity, this
// method also sends the accumulated messages.
func (s *HttpLineSender) AtNow(ctx context.Context) error {
	err := s.at(time.Time{}, false)
	if err != nil {
		return err
	}
	if s.Len() > s.bufCap {
		return s.Flush(ctx)
	}
	return nil
}

// At sets the timestamp in Epoch nanoseconds and finalizes
// the ILP message.
//
// If the underlying buffer reaches configured capacity, this
// method also sends the accumulated messages.
func (s *HttpLineSender) At(ctx context.Context, ts time.Time) error {
	err := s.at(ts, true)
	if err != nil {
		return err
	}
	if s.Len() > s.bufCap {
		return s.Flush(ctx)
	}
	return nil
}

func isRetryableError(statusCode int) bool {
	switch statusCode {
	case 500, // Internal Server Error
		503, // Service Unavailable
		504, // Gateway Timeout
		507, // Insufficient Storage
		509, // Bandwidth Limit Exceeded
		523, // Origin is Unreachable
		524, // A Timeout Occurred
		529, // Site is overloaded
		599: // Network Connect Timeout Error
		return true
	default:
		return false
	}
}
