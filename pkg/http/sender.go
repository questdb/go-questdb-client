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

	"github.com/questdb/go-questdb-client/v3/pkg/buffer"
	"github.com/questdb/go-questdb-client/v3/pkg/conf"
)

type tlsMode int64

const (
	tlsDisabled           tlsMode = 0
	tlsEnabled            tlsMode = 1
	tlsInsecureSkipVerify tlsMode = 2
)

var (
	// We use a shared http transport to pool connections
	// across LineSenders
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

	// clientCt is used to track the number of open LineSenders
	// If the clientCt reaches 0, meaning all LineSenders have been
	// closed, the globalTransport closes all idle connections to
	// free up resources
	clientCt atomic.Int64

	// Since we use a global http.Transport, we also use a global
	// tls config. By default, this is set to verify all server certs,
	// but can be changed by the WithTlsInsecureVerify setting.
	tlsClientConfig tls.Config = tls.Config{}
)

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages over HTTP(S).
//
// Each sender corresponds to a single HTTP client. All senders
// utilize a global transport for connection pooling. A sender
// should not be called concurrently by multiple goroutines.
type LineSender struct {
	buffer.Buffer

	address string

	// Retry/timeout-related fields
	retryTimeout                time.Duration
	minThroughputBytesPerSecond int
	requestTimeout              time.Duration

	// Auto-flush fields
	autoFlushRows int

	// Authentication-related fields
	user    string
	pass    string
	token   string
	tlsMode tlsMode

	client http.Client
	uri    string
	closed bool
}

// LineSenderOption defines line sender option.
type LineSenderOption func(s *LineSender)

// WithTls enables TLS connection encryption.
func WithTls() LineSenderOption {
	return func(s *LineSender) {
		s.tlsMode = tlsEnabled
	}
}

// WithBasicAuth sets a Basic authentication header for
// ILP requests over HTTP.
func WithBasicAuth(user, pass string) LineSenderOption {
	return func(s *LineSender) {
		s.user = user
		s.pass = pass
	}
}

// WithBearerToken sets a Bearer token Authentication header for
// ILP requests.
func WithBearerToken(token string) LineSenderOption {
	return func(s *LineSender) {
		s.token = token
	}
}

// WithRequestTimeout is used in combination with min_throughput
// to set the timeout of an ILP request. Defaults to 10 seconds.
//
// timeout = (request.len() / min_throughput) + request_timeout
func WithRequestTimeout(timeout time.Duration) LineSenderOption {
	return func(s *LineSender) {
		s.requestTimeout = timeout
	}
}

// WithMinThroughput is used in combination with request_timeout
// to set the timeout of an ILP request. Defaults to 100KiB/s.
//
// timeout = (request.len() / min_throughput) + request_timeout
func WithMinThroughput(bytesPerSecond int) LineSenderOption {
	return func(s *LineSender) {
		s.minThroughputBytesPerSecond = bytesPerSecond
	}
}

// WithRetryTimeout is the cumulative maximum duration spend in
// retries. Defaults to 10 seconds.
//
// Only network-related errors and certain 5xx response
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

// WithAutoFlushDisabled turns off auto-flushing behavior.
// To send ILP messages, the user must call Flush().
func WithAutoFlushDisabled() LineSenderOption {
	return func(s *LineSender) {
		s.autoFlushRows = 0
	}
}

// WithAutoFlushRows sets the number of buffered rows that
// must be breached in order to trigger an auto-flush.
// Defaults to 75000.
func WithAutoFlushRows(rows int) LineSenderOption {
	return func(s *LineSender) {
		s.autoFlushRows = rows
	}
}

// NewLineSender creates a new InfluxDB Line Protocol (ILP) sender. Each
// sender corresponds to a single HTTP client. Sender should
// not be called concurrently by multiple goroutines.
func NewLineSender(opts ...LineSenderOption) (*LineSender, error) {
	s := &LineSender{
		address:                     "127.0.0.1:9000",
		minThroughputBytesPerSecond: 100 * 1024,
		requestTimeout:              10 * time.Second,
		retryTimeout:                10 * time.Second,
		autoFlushRows:               75000,

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

	s.uri = "http"
	if s.tlsMode > 0 {
		s.uri += "s"
	}
	s.uri += fmt.Sprintf("://%s/write", s.address)

	return s, nil
}

// LineSenderFromConf creates a LineSender using the QuestDB config string format.
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
		case "min_throughput", "init_buf_size":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, conf.NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)

			}
			switch k {
			case "min_throughput":
				opts = append(opts, WithMinThroughput(parsedVal))
			case "init_buf_size":
				opts = append(opts, WithInitBufferSize(parsedVal))
			default:
				panic("add a case for " + k)
			}

		case "request_timeout", "retry_timeout":
			timeout, err := strconv.Atoi(v)
			if err != nil {
				return nil, conf.NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)
			}

			timeoutDur := time.Duration(timeout * int(time.Millisecond))

			switch k {
			case "request_timeout":
				opts = append(opts, WithRequestTimeout(timeoutDur))
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

	if s.closed {
		return errors.New("cannot flush a closed LineSender")
	}

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

	req, err = http.NewRequest(
		http.MethodPost,
		s.uri,
		s,
	)
	if err != nil {
		return err
	}

	if s.user != "" && s.pass != "" {
		req.SetBasicAuth(s.user, s.pass)
	}

	if s.token != "" {
		if req.Header.Get("Authorization") != "" {
			return errors.New("cannot use both Basic and Token Authorization")
		}
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", s.token))
	}

	retry, err := s.makeRequest(ctx, req)
	if !retry {
		return err
	}

	if s.retryTimeout > 0 {
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
func (s *LineSender) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}

	if s.autoFlushRows > 0 && s.Buffer.Len() > 0 {
		err := s.Flush(ctx)
		if err != nil {
			return err
		}
	}

	s.closed = true

	newCt := clientCt.Add(-1)
	if newCt == 0 {
		globalTransport.CloseIdleConnections()
	}

	return nil
}

// AtNow omits the timestamp and finalizes the ILP message.
// The server will insert each message using the system clock
// as the row timestamp.
//
// If the underlying buffer reaches configured capacity or the
// number of buffered messages exceeds the auto-flush trigger, this
// method also sends the accumulated messages.
func (s *LineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

// At sets the timestamp in Epoch nanoseconds and finalizes
// the ILP message.
//
// If the underlying buffer reaches configured capacity or the
// number of buffered messages exceeds the auto-flush trigger, this
// method also sends the accumulated messages.
//
// If ts.IsZero(), no timestamp is sent to the server.
func (s *LineSender) At(ctx context.Context, ts time.Time) error {
	if s.closed {
		return errors.New("cannot queue new messages on a closed LineSender")
	}

	sendTs := true
	if ts.IsZero() {
		sendTs = false
	}
	err := s.Buffer.At(ts, sendTs)
	if err != nil {
		return err
	}

	if s.MsgCount() == s.autoFlushRows {
		return s.Flush(ctx)
	}

	return nil
}

// makeRequest returns a boolean if we need to retry the request
func (s *LineSender) makeRequest(ctx context.Context, req *http.Request) (bool, error) {
	// reqTimeout = ( request.len() / min_throughput ) + request_timeout
	// nb: conversion from int to time.Duration is in milliseconds
	reqTimeout := time.Duration(s.Len()/s.minThroughputBytesPerSecond)*time.Second + s.requestTimeout
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
