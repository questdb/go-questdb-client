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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	// We use a shared http transport to pool connections
	// across HttpLineSenders
	globalTransport http.Transport = http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxConnsPerHost:       0,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{},
	}

	// clientCt is used to track the number of open HttpLineSenders
	// If the clientCt reaches 0, meaning all HttpLineSenders have been
	// closed, the globalTransport closes all idle connections to
	// free up resources
	clientCt atomic.Int64
)

// HttpLineSender allows you to insert rows into QuestDB by sending ILP
// messages over HTTP(S).
//
// Each sender corresponds to a single HTTP client. All senders
// utilize a global transport for connection pooling. A sender
// should not be called concurrently by multiple goroutines.
type httpLineSender struct {
	buf buffer

	address string

	// Retry/timeout-related fields
	retryTimeout                time.Duration
	minThroughputBytesPerSecond int
	requestTimeout              time.Duration

	// Auto-flush fields
	autoFlushRows     int
	autoFlushInterval time.Duration
	flushDeadline     time.Time

	// Authentication-related fields
	user    string
	pass    string
	token   string
	tlsMode tlsMode

	client    http.Client
	uri       string
	closed    bool
	transport *http.Transport
}

// HttpLineSenderOption defines line sender option.
type HttpLineSenderOption func(s *httpLineSender)

// WithTls enables TLS connection encryption.
func WithHttpTls() HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.tlsMode = tlsEnabled
	}
}

// WithBasicAuth sets a Basic authentication header for
// ILP requests over HTTP.
func WithHttpBasicAuth(user, pass string) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.user = user
		s.pass = pass
	}
}

// WithBearerToken sets a Bearer token Authentication header for
// ILP requests.
func WithHttpBearerToken(token string) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.token = token
	}
}

// WithRequestTimeout is used in combination with min_throughput
// to set the timeout of an ILP request. Defaults to 10 seconds.
//
// timeout = (request.len() / min_throughput) + request_timeout
func WithHttpRequestTimeout(timeout time.Duration) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.requestTimeout = timeout
	}
}

// WithMinThroughput is used in combination with request_timeout
// to set the timeout of an ILP request. Defaults to 100KiB/s.
//
// timeout = (request.len() / min_throughput) + request_timeout
func WithHttpMinThroughput(bytesPerSecond int) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.minThroughputBytesPerSecond = bytesPerSecond
	}
}

// WithRetryTimeout is the cumulative maximum duration spend in
// retries. Defaults to 10 seconds.
//
// Only network-related errors and certain 5xx response
// codes are retryable.
func WithHttpRetryTimeout(t time.Duration) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.retryTimeout = t
	}
}

// WithInitBuffer size sets the desired initial buffer capacity
// in bytes to be used when sending ILP messages. Defaults to 128KB.
func WithHttpInitBufferSize(sizeInBytes int) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.buf.BufCap = sizeInBytes
	}
}

// WithAddress sets address to connect to. Should be in the
// "host:port" format. Defaults to "127.0.0.1:9000".
func WithHttpAddress(addr string) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.address = addr
	}
}

// WithTlsInsecureSkipVerify enables TLS connection encryption,
// but skips server certificate verification. Useful in test
// environments with self-signed certificates. Do not use in
// production environments.
//
// If using the global transport (the default), once
// WithTlsInsecureSkipVerify is used for any client,
// all subsequent clients may not use the WithTls option.
// To avoid this, use WithHttpTransport to add a custom
// transport to the Sender's http client.
func WithHttpTlsInsecureSkipVerify() HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.tlsMode = tlsInsecureSkipVerify
	}
}

// WithAutoFlushDisabled turns off auto-flushing behavior.
// To send ILP messages, the user must call Flush().
func WithHttpAutoFlushDisabled() HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.autoFlushRows = 0
		s.autoFlushInterval = 0
	}
}

// WithAutoFlushRows sets the number of buffered rows that
// must be breached in order to trigger an auto-flush.
// Defaults to 75000.
func WithHttpAutoFlushRows(rows int) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.autoFlushRows = rows
	}
}

// WithAutoFlushInterval the interval at which the Sender
// automatically flushes its buffer. Defaults to 1 second.
func WithHttpAutoFlushInterval(interval time.Duration) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.autoFlushInterval = interval
	}
}

// WithHttpTransport sets the client's http transport to the
// passed pointer instead of the global transport. This can be
// used for customizing the http transport used by the HttpLineSender.
func WithHttpTransport(t *http.Transport) HttpLineSenderOption {
	return func(s *httpLineSender) {
		s.transport = t
	}
}

// NewHttpLineSender creates a new InfluxDB Line Protocol (ILP) sender. Each
// sender corresponds to a single HTTP client. Sender should
// not be called concurrently by multiple goroutines.
func NewHttpLineSender(opts ...HttpLineSenderOption) (*httpLineSender, error) {
	s := &httpLineSender{
		address:                     "127.0.0.1:9000",
		minThroughputBytesPerSecond: 100 * 1024,
		requestTimeout:              10 * time.Second,
		retryTimeout:                10 * time.Second,
		autoFlushRows:               75000,
		autoFlushInterval:           time.Second,

		buf: newBuffer(),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.address == "" {
		s.address = "127.0.0.1:9000"
	}

	if s.transport == nil {
		s.transport = &globalTransport
	}

	if s.transport == &globalTransport {
		if s.tlsMode == tlsEnabled && globalTransport.TLSClientConfig.InsecureSkipVerify {
			return nil, errors.New("once InsecureSkipVerify is used with the default global transport, the WithTls option may not be used unless in combination with WithHttpTransport")
		}
	}

	if s.tlsMode == tlsInsecureSkipVerify {
		// TODO(puzpuzpuz): this code is racy
		if s.transport.TLSClientConfig == nil {
			s.transport.TLSClientConfig = &tls.Config{}
		}
		s.transport.TLSClientConfig.InsecureSkipVerify = true
	}

	s.client = http.Client{
		Transport: s.transport,
		Timeout:   0, // todo: add a global maximum timeout?
	}

	if s.transport == &globalTransport {
		clientCt.Add(1)
	}

	s.uri = "http"
	if s.tlsMode > 0 {
		s.uri += "s"
	}
	s.uri += fmt.Sprintf("://%s/write", s.address)

	s.buf.Buffer = *bytes.NewBuffer(make([]byte, 0, s.buf.BufCap))

	return s, nil
}

// HttpLineSenderFromConf creates a HttpLineSender using the QuestDB config string format.
//
// Example config string: "http::addr=localhost;username=joe;password=123;"
//
// QuestDB ILP clients use a common key-value configuration string format across all
// implementations. We opted for this config over a URL because it reduces the amount
// of character escaping required for paths and base64-encoded param values.
//
// The config string format is as follows:
//
// schema::key1=value1;key2=value2;key3=value3;
//
// Schemas supported are "http", "https", "tcp", "tcps"
//
// Supported parameter values for http(s):
//
// addr:      hostname/port of QuestDB HTTP endpoint
// username:  for basic authentication
// password:  for basic authentication
// token:     bearer token auth (used instead of basic authentication)
//
// auto_flush:       determines if auto-flushing is enabled (values "on" or "off", defaults to "on")
// auto_flush_rows:  auto-flushing is triggered above this row count (defaults to 75000). If set, explicitly implies auto_flush=on
//
// request_min_throughput: bytes per second, used to calculate each request's timeout (defaults to 100KiB/s)
// request_timeout:        minimum request timeout in milliseconds (defaults to 10 seconds)
// retry_timeout:          cumulative maximum millisecond duration spent in retries (defaults to 10 seconds)
//
// tls_verify: determines if TLS certificates should be validated (defaults to "on", can be set to "unsafe_off")
func HttpLineSenderFromConf(ctx context.Context, config string) (*httpLineSender, error) {
	var (
		user, pass, token string
	)

	data, err := ParseConfigString(config)
	if err != nil {
		return nil, err
	}

	if data.Schema != "http" && data.Schema != "https" {
		return nil, fmt.Errorf("invalid schema: %s", data.Schema)
	}

	opts := make([]HttpLineSenderOption, 0)
	for k, v := range data.KeyValuePairs {
		switch strings.ToLower(k) {
		case "addr":
			opts = append(opts, WithHttpAddress(v))
		case "user":
			user = v
			if user != "" && pass != "" {
				opts = append(opts, WithHttpBasicAuth(user, pass))
			}
		case "pass":
			pass = v
			if user != "" && pass != "" {
				opts = append(opts, WithHttpBasicAuth(user, pass))
			}
		case "token":
			token = v
			opts = append(opts, WithHttpBearerToken(token))
		case "auto_flush":
			if v == "off" {
				opts = append(opts, WithHttpAutoFlushDisabled())
			} else if v != "on" {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not 'on' or 'off'", k, v)
			}
		case "auto_flush_rows":
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal < 1 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a positive int", k, v)
			}
			opts = append(opts, WithHttpAutoFlushRows(parsedVal))
		case "auto_flush_interval":
			parsedVal, err := strconv.Atoi(v)
			if err != nil || parsedVal < 1 {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a positive int", k, v)
			}
			opts = append(opts, WithHttpAutoFlushInterval(time.Duration(parsedVal)))
		case "min_throughput", "init_buf_size":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}
			switch k {
			case "min_throughput":
				opts = append(opts, WithHttpMinThroughput(parsedVal))
			case "init_buf_size":
				opts = append(opts, WithHttpInitBufferSize(parsedVal))
			default:
				panic("add a case for " + k)
			}
		case "request_timeout", "retry_timeout":
			timeout, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)
			}

			timeoutDur := time.Duration(timeout * int(time.Millisecond))

			switch k {
			case "request_timeout":
				opts = append(opts, WithHttpRequestTimeout(timeoutDur))
			case "retry_timeout":
				opts = append(opts, WithHttpRetryTimeout(timeoutDur))
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
				return nil, NewInvalidConfigStrError("invalid tls_verify value, %q is not 'on' or 'unsafe_off", v)
			}
		case "tls_roots":
			return nil, NewInvalidConfigStrError("tls_roots is not available in the go client")
		case "tls_roots_password":
			return nil, NewInvalidConfigStrError("tls_roots_password is not available in the go client")
		default:
			return nil, NewInvalidConfigStrError("unsupported option %q", k)
		}

	}
	return NewHttpLineSender(opts...)
}

func (s *httpLineSender) Flush(ctx context.Context) error {
	return s.flush0(ctx, false)
}

func (s *httpLineSender) flush0(ctx context.Context, closing bool) error {
	var (
		req           *http.Request
		retryInterval time.Duration

		maxRetryInterval = time.Second
	)

	if s.closed {
		return errors.New("cannot flush a closed LineSender")
	}

	err := s.buf.LastErr()
	s.buf.ClearLastErr()
	if err != nil {
		s.buf.DiscardPendingMsg()
		return err
	}
	if s.buf.HasTable() {
		s.buf.DiscardPendingMsg()
		return errors.New("pending ILP message must be finalized with At or AtNow before calling Flush")
	}

	if s.buf.msgCount == 0 {
		return nil
	}

	// We rely on the following HTTP client implicit behavior:
	// s.buf implements WriteTo method which is used by the client.
	req, err = http.NewRequest(
		http.MethodPost,
		s.uri,
		&s.buf,
	)
	if err != nil {
		return err
	}

	if s.user != "" && s.pass != "" {
		req.SetBasicAuth(s.user, s.pass)
	} else if s.token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", s.token))
	}

	retry, err := s.makeRequest(ctx, req)
	if !retry {
		s.refreshFlushDeadline(err)
		return err
	}

	if !closing && s.retryTimeout > 0 {
		retryStartTime := time.Now()

		retryInterval = 10 * time.Millisecond
		for err != nil {
			if time.Since(retryStartTime) > s.retryTimeout {
				return NewRetryTimeoutError(s.retryTimeout, err)
			}

			jitter := time.Duration(rand.Intn(10)) * time.Millisecond
			time.Sleep(retryInterval + jitter)

			retry, err = s.makeRequest(ctx, req)
			if !retry {
				s.refreshFlushDeadline(err)
				return err
			}

			// Retry with exponentially-increasing timeout
			// up to a global maximum (1 second)
			retryInterval = retryInterval * 2
			if retryInterval > maxRetryInterval {
				retryInterval = maxRetryInterval
			}
		}
	}

	s.refreshFlushDeadline(err)
	return err
}

func (s *httpLineSender) refreshFlushDeadline(err error) {
	if s.autoFlushInterval > 0 {
		if err != nil {
			s.flushDeadline = time.Time{}
		} else {
			s.flushDeadline = time.Now().Add(s.autoFlushInterval)
		}
	}
}

func (s *httpLineSender) Table(name string) *httpLineSender {
	s.buf.Table(name)
	return s
}

func (s *httpLineSender) Symbol(name, val string) *httpLineSender {
	s.buf.Symbol(name, val)
	return s
}

func (s *httpLineSender) Int64Column(name string, val int64) *httpLineSender {
	s.buf.Int64Column(name, val)
	return s
}

func (s *httpLineSender) Long256Column(name string, val *big.Int) *httpLineSender {
	s.buf.Long256Column(name, val)
	return s
}

func (s *httpLineSender) TimestampColumn(name string, ts time.Time) *httpLineSender {
	s.buf.TimestampColumn(name, ts)
	return s
}

func (s *httpLineSender) Float64Column(name string, val float64) *httpLineSender {
	s.buf.Float64Column(name, val)
	return s
}

func (s *httpLineSender) StringColumn(name, val string) *httpLineSender {
	s.buf.StringColumn(name, val)
	return s
}

func (s *httpLineSender) BoolColumn(name string, val bool) *httpLineSender {
	s.buf.BoolColumn(name, val)
	return s
}

func (s *httpLineSender) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}

	var err error

	if s.autoFlushRows > 0 {
		err = s.flush0(ctx, true)
	}

	s.closed = true

	if s.transport == &globalTransport {
		newCt := clientCt.Add(-1)
		if newCt == 0 {
			globalTransport.CloseIdleConnections()
		}
	}

	return err
}

func (s *httpLineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

func (s *httpLineSender) At(ctx context.Context, ts time.Time) error {
	if s.closed {
		return errors.New("cannot queue new messages on a closed LineSender")
	}

	sendTs := true
	if ts.IsZero() {
		sendTs = false
	}
	err := s.buf.At(ts, sendTs)
	if err != nil {
		return err
	}

	// Check row count-based auto flush.
	if s.buf.msgCount == s.autoFlushRows {
		return s.Flush(ctx)
	}
	// Check time-based auto flush.
	if s.autoFlushInterval > 0 {
		if s.flushDeadline.IsZero() {
			s.flushDeadline = time.Now().Add(s.autoFlushInterval)
		} else if time.Now().After(s.flushDeadline) {
			return s.Flush(ctx)
		}
	}

	return nil
}

// makeRequest returns a boolean if we need to retry the request
func (s *httpLineSender) makeRequest(ctx context.Context, req *http.Request) (bool, error) {
	// reqTimeout = ( request.len() / min_throughput ) + request_timeout
	// nb: conversion from int to time.Duration is in milliseconds
	reqTimeout := time.Duration(s.buf.Len()/s.minThroughputBytesPerSecond)*time.Second + s.requestTimeout
	reqCtx, cancel := context.WithTimeout(ctx, reqTimeout)
	defer cancel()

	req = req.WithContext(reqCtx)
	resp, err := s.client.Do(req)
	if err != nil {
		return true, err
	}

	defer resp.Body.Close()

	// Don't retry on successful responses
	if resp.StatusCode < 300 {
		return false, nil
	}

	// Retry on known 500-related errors
	if isRetryableError(resp.StatusCode) {
		return true, fmt.Errorf("%d: %s", resp.StatusCode, resp.Status)
	}

	// For all other response codes, attempt to parse the body
	// as a JSON error message from the QuestDB server.
	// If this fails at any point, just return the status message
	// and body contents (if any)

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("%d: %s", resp.StatusCode, resp.Status)
	}
	httpErr := &HttpError{
		httpStatus: resp.StatusCode,
	}
	err = json.Unmarshal(buf, httpErr)
	if err != nil {
		return false, fmt.Errorf("%d: %s -- %s", resp.StatusCode, resp.Status, buf)
	}

	return false, httpErr

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

// Messages returns a copy of accumulated ILP messages that are not
// flushed to the TCP connection yet. Useful for debugging purposes.
func (s *httpLineSender) Messages() string {
	return s.buf.Messages()
}

// MsgCount returns the number of buffered messages
func (s *httpLineSender) MsgCount() int {
	return s.buf.msgCount
}

// BufLen returns the number of bytes written to the buffer.
func (s *httpLineSender) BufLen() int {
	return s.buf.Len()
}