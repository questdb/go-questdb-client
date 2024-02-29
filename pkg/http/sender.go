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

package http

import (
	"bytes"
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

	"github.com/questdb/go-questdb-client/v3/pkg/buffer"
	"github.com/questdb/go-questdb-client/v3/pkg/conf"
)

type tlsMode int64

const (
	tlsDisabled           tlsMode = 0
	tlsEnabled            tlsMode = 1
	tlsInsecureSkipVerify tlsMode = 2
)

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages over HTTP(S).
//
// Each sender corresponds to a single HTTP client. All senders
// utilize a global transport for connection pooling. A sender
// should not be called concurrently by multiple goroutines.
type LineSender struct {
	buffer.Buffer

	address                     string
	retryTimeout                time.Duration
	minThroughputBytesPerSecond int
	graceTimeout                time.Duration
	tlsMode                     tlsMode

	user  string
	pass  string
	token string

	client http.Client
}

// LineSenderOption defines line sender option.
type LineSenderOption func(s *LineSender)

func NewLineSender(opts ...LineSenderOption) (*LineSender, error) {
	s := &LineSender{
		address:                     "127.0.0.1:9000",
		minThroughputBytesPerSecond: 100 * 1024,
		graceTimeout:                5 * time.Second,
		retryTimeout:                10 * time.Second,

		Buffer: *buffer.NewBuffer(),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.address == "" {
		s.address = "127.0.0.1:9000"
	}

	if s.tlsMode != tlsInsecureSkipVerify && tlsClientConfig.InsecureSkipVerify {
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

// WithTls enables TLS connection encryption.
func WithTls() LineSenderOption {
	return func(s *LineSender) {
		s.tlsMode = tlsEnabled
	}
}

// WithBasicAuth sets a Basic authentication header for
// ILP requests over HTTP
func WithBasicAuth(user, pass string) LineSenderOption {
	return func(s *LineSender) {
		s.user = user
		s.pass = pass
	}
}

// WithBearerToken sets a Bearer token Authentication header for
// ILP requests
func WithBearerToken(token string) LineSenderOption {
	return func(s *LineSender) {
		s.token = token
	}
}

// WithGraceTimeout is used in combination with min throughput
// to set the timeout of an ILP request. Defaults to 5 seconds
//
// timeout = (request.len() / min_throughput) + grace
func WithGraceTimeout(timeout time.Duration) LineSenderOption {
	return func(s *LineSender) {
		s.graceTimeout = timeout
	}
}

// WithMinThroughput is used in combination with grace timeout
// to set the timeout of an ILP request. Defaults to 100KiB/s
//
// timeout = (request.len() / min_throughput) + grace
func WithMinThroughput(bytesPerSecond int) LineSenderOption {
	return func(s *LineSender) {
		s.minThroughputBytesPerSecond = bytesPerSecond
	}
}

// WithRetryTimeout is the cumulative maximum duration spend in
// retries. Defaults to 10 seconds.
//
// Only network-related errors and QuestDB-specific 5xx response
// codes are retryable.
func WithRetryTimeout(t time.Duration) LineSenderOption {
	return func(s *LineSender) {
		s.retryTimeout = t
	}
}

// WithInitBuffer size sets the desired initial buffer capacity
// in bytes to be used when sending ILP messages. Defaults to 0.
func WithInitBufferSize(sizeInBytes int) LineSenderOption {
	return func(s *LineSender) {
		s.InitBufSizeBytes = sizeInBytes
	}
}

// WithAddress sets address to connect to. Should be in the
// "host:port" format. Defaults to "127.0.0.1:9000".
func WithAddress(addr string) LineSenderOption {
	return func(s *LineSender) {
		s.address = addr
	}
}

// WithTlsInsecureSkipVerify enables TLS connection encryption,
// but skips server certificate verification. Useful in test
// environments with self-signed certificates. Do not use in
// production environments.
func WithTlsInsecureSkipVerify() LineSenderOption {
	return func(s *LineSender) {
		s.tlsMode = tlsInsecureSkipVerify
	}
}

// WithBufferCapacity sets desired buffer capacity in bytes to
// be used when sending ILP messages. Defaults to 128KB.
//
// This setting is a soft limit, i.e. the underlying buffer may
// grow larger than the provided value, but will shrink on a
// At, AtNow, or Flush call.
func WithBufferCapacity(capacity int) LineSenderOption {
	return func(s *LineSender) {
		if capacity > 0 {
			s.BufCap = capacity
		}
	}
}

// LineSenderFromConf creates a LineSender using the QuestDB config string format.
func LineSenderFromConf(ctx context.Context, config string) (*LineSender, error) {
	var (
		user, pass, token string
	)

	data, err := conf.ParseConfigString(config)
	if err != nil {
		return nil, err
	}

	if data.Schema != "http" && data.Schema != "https" {
		return nil, fmt.Errorf("invalid schema: %s", data.Schema)
	}

	opts := make([]LineSenderOption, 0)
	for k, v := range data.KeyValuePairs {

		switch strings.ToLower(k) {
		case "addr":
			opts = append(opts, WithAddress(v))
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
				return nil, conf.NewConfigStrParseError("auto_flush option is not supported")
			}
		case "auto_flush_rows", "auto_flush_bytes":
			return nil, conf.NewConfigStrParseError("auto_flush option is not supported")
		case "min_throughput", "init_buf_size", "max_buf_size":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, conf.NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)

			}
			switch k {
			case "min_throughput":
				opts = append(opts, WithMinThroughput(parsedVal))
			case "init_buf_size":
				opts = append(opts, WithInitBufferSize(parsedVal))
			case "max_buf_size":
				opts = append(opts, WithBufferCapacity(parsedVal))
			default:
				panic("add a case for " + k)
			}

		case "grace_timeout", "retry_timeout":
			timeout, err := strconv.Atoi(v)
			if err != nil {
				return nil, conf.NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)
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
				opts = append(opts, WithTls())
			case "unsafe_off":
				opts = append(opts, WithTlsInsecureSkipVerify())
			default:
				return nil, conf.NewConfigStrParseError("invalid tls_verify value, %q is not 'on' or 'unsafe_off", v)
			}
		case "tls_roots":
			return nil, conf.NewConfigStrParseError("tls_roots is not available in the go client")
		case "tls_roots_password":
			return nil, conf.NewConfigStrParseError("tls_roots_password is not available in the go client")
		default:
			return nil, conf.NewConfigStrParseError("unsupported option %q", k)
		}

	}
	return NewLineSender(opts...)
}

// Flush flushes the accumulated messages to the underlying HTTP
// client. Should be called periodically to make sure that
// all messages are sent to the server.
//
// For optimal performance, this method should not be called after
// each ILP message. Instead, the messages should be written in
// batches followed by a Flush call. Optimal batch size may vary
// from 100 to 1,000 messages depending on the message size and
// configured buffer capacity.
func (s *LineSender) Flush(ctx context.Context) error {
	var (
		req           *http.Request
		retryInterval time.Duration

		maxRetryInterval = time.Second
	)

	err := s.LastErr()
	s.ClearLastErr()
	if err != nil {
		s.DiscardPendingMsg()
		return err
	}
	if s.HasTable() {
		s.DiscardPendingMsg()
		return errors.New("pending ILP message must be finalized with At or AtNow before calling Flush")
	}

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
			// bytes.Buffer grows as 2*cap+n, so we use 3x as the threshold.
			if s.Cap() > 3*s.BufCap {
				// Shrink the buffer back to desired capacity.
				s.Buffer.Buffer = *bytes.NewBuffer(make([]byte, s.InitBufSizeBytes, s.BufCap))
			}
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
					// bytes.Buffer grows as 2*cap+n, so we use 3x as the threshold.
					if s.Cap() > 3*s.BufCap {
						// Shrink the buffer back to desired capacity.
						s.Buffer.Buffer = *bytes.NewBuffer(make([]byte, s.InitBufSizeBytes, s.BufCap))
					}
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
func (s *LineSender) Table(name string) *LineSender {
	s.Buffer.Table(name)
	return s
}

// Symbol adds a symbol column value to the ILP message. Should be called
// before any Column method.
//
// Symbol name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Symbol(name, val string) *LineSender {
	s.Buffer.Symbol(name, val)
	return s
}

// Int64Column adds a 64-bit integer (long) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Int64Column(name string, val int64) *LineSender {
	s.Buffer.Int64Column(name, val)
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
func (s *LineSender) Long256Column(name string, val *big.Int) *LineSender {
	s.Buffer.Long256Column(name, val)
	return s
}

// TimestampColumn adds a timestamp column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) TimestampColumn(name string, ts time.Time) *LineSender {
	s.Buffer.TimestampColumn(name, ts)
	return s
}

// Float64Column adds a 64-bit float (double) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Float64Column(name string, val float64) *LineSender {
	s.Buffer.Float64Column(name, val)
	return s
}

// StringColumn adds a string column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) StringColumn(name, val string) *LineSender {
	s.Buffer.StringColumn(name, val)
	return s
}

// BoolColumn adds a boolean column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) BoolColumn(name string, val bool) *LineSender {
	s.Buffer.BoolColumn(name, val)
	return s
}

// Close closes the underlying HTTP client. If no clients remain open,
// the global http.Transport will close all idle connections.
func (s *LineSender) Close() {
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
func (s *LineSender) AtNow(ctx context.Context) error {
	err := s.Buffer.At(time.Time{}, false)
	if err != nil {
		return err
	}
	if s.Len() > s.BufCap {
		return s.Flush(ctx)
	}
	return nil
}

// At sets the timestamp in Epoch nanoseconds and finalizes
// the ILP message.
//
// If the underlying buffer reaches configured capacity, this
// method also sends the accumulated messages.
func (s *LineSender) At(ctx context.Context, ts time.Time) error {
	err := s.Buffer.At(ts, true)
	if err != nil {
		return err
	}
	if s.Len() > s.BufCap {
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
