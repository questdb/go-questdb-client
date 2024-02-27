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
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type HttpLineSender struct {
	buffer

	address      string
	timeout      time.Duration
	retryTimeout time.Duration
	tlsMode      tlsMode

	user  string
	pass  string
	token string

	client http.Client
}

type HttpLineSenderOption func(s *HttpLineSender)

func NewHttpLineSender(opts ...HttpLineSenderOption) *HttpLineSender {
	s := &HttpLineSender{
		address:      "127.0.0.1:9000",
		timeout:      30 * time.Second,
		retryTimeout: 10 * time.Second,
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.address == "" {
		s.address = "127.0.0.1:9000"
	}

	s.client = http.Client{
		Transport: &globalTransport,
		Timeout:   s.timeout,
	}

	clientCt.Add(1)

	return s
}

var globalTransport http.Transport = http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: defaultTransportDialContext(&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}),
	MaxConnsPerHost:       0,
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

var clientCt atomic.Int64

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

func WithHttpTimeout(t time.Duration) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.timeout = t
	}
}

// WithHttpTls enables TLS connection encryption.
func WithHttpTls() HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.tlsMode = tlsEnabled
	}
}

func WithHttpRetry(t time.Duration) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.retryTimeout = t
	}
}

func WithHttpInitBufferSize(sizeInBytes int) HttpLineSenderOption {
	return func(s *HttpLineSender) {
		s.initBufSizeBytes = sizeInBytes
	}
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

	req, err = http.NewRequestWithContext(
		ctx,
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
		if !isRetryableError(resp.StatusCode) {
			return nil
		}
		err = fmt.Errorf("Non-OK Status Code %d: %s", resp.StatusCode, resp.Status)
	}
	defer resp.Body.Close()

	if s.retryTimeout > 0 {
		retryInterval = 10 * time.Millisecond
		for {
			jitter := time.Duration(rand.Intn(10)) * time.Millisecond
			time.Sleep(retryInterval + jitter)

			resp, retryErr := s.client.Do(req)
			if retryErr == nil {
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

func defaultTransportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return dialer.DialContext
}
