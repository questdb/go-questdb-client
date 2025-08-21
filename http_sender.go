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
	"sync/atomic"
	"time"
)

type globalHttpTransport struct {
	transport *http.Transport
	// clientCt is used to track the number of open httpLineSenders
	// If the clientCt reaches 0, meaning all senders have been
	// closed, the global transport closes all idle connections to
	// free up resources
	clientCt atomic.Int64
}

func (t *globalHttpTransport) ClientCount() int64 {
	return t.clientCt.Load()
}

func (t *globalHttpTransport) RegisterClient() {
	t.clientCt.Add(1)
}

func (t *globalHttpTransport) UnregisterClient() {
	newCt := t.clientCt.Add(-1)
	if newCt == 0 {
		t.transport.CloseIdleConnections()
	}
}

var (
	// We use a shared http transport to pool connections
	// across HttpLineSenders
	globalTransport *globalHttpTransport = &globalHttpTransport{transport: newHttpTransport()}
)

func newHttpTransport() *http.Transport {
	return &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxConnsPerHost:     0,
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     120 * time.Second,
		TLSHandshakeTimeout: defaultRequestTimeout,
		TLSClientConfig:     &tls.Config{},
	}
}

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
	user  string
	pass  string
	token string

	client http.Client
	uri    string
	closed bool

	// Global transport is used unless a custom transport was provided.
	globalTransport *globalHttpTransport
}

type httpLineSenderV2 struct {
	httpLineSender
}

func newHttpLineSender(ctx context.Context, conf *lineSenderConfig) (LineSender, error) {
	var transport *http.Transport
	s := &httpLineSender{
		address:                     conf.address,
		minThroughputBytesPerSecond: conf.minThroughput,
		requestTimeout:              conf.requestTimeout,
		retryTimeout:                conf.retryTimeout,
		autoFlushRows:               conf.autoFlushRows,
		autoFlushInterval:           conf.autoFlushInterval,
		user:                        conf.httpUser,
		pass:                        conf.httpPass,
		token:                       conf.httpToken,

		buf: newBuffer(conf.initBufSize, conf.maxBufSize, conf.fileNameLimit),
	}

	if conf.httpTransport != nil {
		// Use custom transport.
		transport = conf.httpTransport
	} else if conf.tlsMode == tlsInsecureSkipVerify {
		// We can't use the global transport in case of skipped TLS verification.
		// Instead, create a single-time transport with disabled keep-alives.
		transport = newHttpTransport()
		transport.DisableKeepAlives = true
		transport.TLSClientConfig.InsecureSkipVerify = true
	} else {
		// Otherwise, use the global transport.
		s.globalTransport = globalTransport
		transport = globalTransport.transport
	}

	s.client = http.Client{
		Transport: transport,
		Timeout:   0,
	}

	if s.globalTransport != nil {
		s.globalTransport.RegisterClient()
	}

	// auto detect server line protocol version
	pVersion := conf.protocolVersion
	if pVersion == protocolVersionUnset {
		var err error
		pVersion, err = s.detectProtocolVersion(ctx, conf)
		if err != nil {
			return nil, err
		}
	}

	s.uri = "http"
	if conf.tlsMode != tlsDisabled {
		s.uri += "s"
	}
	s.uri += fmt.Sprintf("://%s/write", s.address)

	if pVersion == ProtocolVersion1 {
		return s, nil
	} else {
		return &httpLineSenderV2{
			*s,
		}, nil
	}
}

func (s *httpLineSender) Flush(ctx context.Context) error {
	return s.flush0(ctx, false)
}

func (s *httpLineSender) flush0(ctx context.Context, closing bool) error {
	var (
		retryInterval    time.Duration
		maxRetryInterval = time.Second
	)

	if s.closed {
		return errClosedSenderFlush
	}

	err := s.buf.LastErr()
	s.buf.ClearLastErr()
	if err != nil {
		s.buf.DiscardPendingMsg()
		return err
	}
	if s.buf.HasTable() {
		s.buf.DiscardPendingMsg()
		return errFlushWithPendingMessage
	}

	if s.buf.msgCount == 0 {
		return nil
	}
	// Always reset the buffer at the end of flush.
	defer s.buf.Reset()

	retry, err := s.makeRequest(ctx)
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

			retry, err = s.makeRequest(ctx)
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

func (s *httpLineSender) Table(name string) LineSender {
	s.buf.Table(name)
	return s
}

func (s *httpLineSender) Symbol(name, val string) LineSender {
	s.buf.Symbol(name, val)
	return s
}

func (s *httpLineSender) Int64Column(name string, val int64) LineSender {
	s.buf.Int64Column(name, val)
	return s
}

func (s *httpLineSender) Long256Column(name string, val *big.Int) LineSender {
	s.buf.Long256Column(name, val)
	return s
}

func (s *httpLineSender) TimestampColumn(name string, ts time.Time) LineSender {
	s.buf.TimestampColumn(name, ts)
	return s
}

func (s *httpLineSender) Float64Column(name string, val float64) LineSender {
	s.buf.Float64Column(name, val)
	return s
}

func (s *httpLineSender) StringColumn(name, val string) LineSender {
	s.buf.StringColumn(name, val)
	return s
}

func (s *httpLineSender) BoolColumn(name string, val bool) LineSender {
	s.buf.BoolColumn(name, val)
	return s
}

func (s *httpLineSender) Float64Array1DColumn(name string, values []float64) LineSender {
	s.buf.SetLastErr(errors.New("current protocol version does not support double-array"))
	return s
}

func (s *httpLineSender) Float64Array2DColumn(name string, values [][]float64) LineSender {
	s.buf.SetLastErr(errors.New("current protocol version does not support double-array"))
	return s
}

func (s *httpLineSender) Float64Array3DColumn(name string, values [][][]float64) LineSender {
	s.buf.SetLastErr(errors.New("current protocol version does not support double-array"))
	return s
}

func (s *httpLineSender) Float64ArrayNDColumn(name string, values *NdArray[float64]) LineSender {
	s.buf.SetLastErr(errors.New("current protocol version does not support double-array"))
	return s
}

func (s *httpLineSender) Close(ctx context.Context) error {
	if s.closed {
		return errDoubleSenderClose
	}

	var err error

	if s.autoFlushRows > 0 {
		err = s.flush0(ctx, true)
	}

	s.closed = true

	if s.globalTransport != nil {
		s.globalTransport.UnregisterClient()
	}

	return err
}

func (s *httpLineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

func (s *httpLineSender) At(ctx context.Context, ts time.Time) error {
	if s.closed {
		return errClosedSenderAt
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
func (s *httpLineSender) makeRequest(ctx context.Context) (bool, error) {
	req, err := http.NewRequest(
		http.MethodPost,
		s.uri,
		bytes.NewReader(s.buf.Bytes()),
	)
	if err != nil {
		return false, err
	}
	req.ContentLength = int64(s.BufLen())

	if s.user != "" && s.pass != "" {
		req.SetBasicAuth(s.user, s.pass)
	} else if s.token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", s.token))
	}

	// reqTimeout = ( request.len() / request_min_throughput ) + request_timeout
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

func (s *httpLineSender) detectProtocolVersion(ctx context.Context, conf *lineSenderConfig) (protocolVersion, error) {
	scheme := "http"
	if conf.tlsMode != tlsDisabled {
		scheme = "https"
	}
	settingsUri := fmt.Sprintf("%s://%s/settings", scheme, s.address)

	req, err := http.NewRequest(http.MethodGet, settingsUri, nil)
	if err != nil {
		return protocolVersionUnset, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	req = req.WithContext(reqCtx)

	resp, err := s.client.Do(req)
	if err != nil {
		return protocolVersionUnset, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 404:
		return ProtocolVersion1, nil
	case 200:
		return parseServerSettings(resp, conf)
	default:
		buf, _ := io.ReadAll(resp.Body)
		if len(buf) > 1024 {
			buf = append(buf[:1024], []byte("...")...)
		}
		return protocolVersionUnset, fmt.Errorf("failed to detect server line protocol version [http-status=%d, http-message=%s]",
			resp.StatusCode, string(buf))
	}
}

func parseServerSettings(resp *http.Response, conf *lineSenderConfig) (protocolVersion, error) {
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return protocolVersionUnset, fmt.Errorf("%d: %s", resp.StatusCode, resp.Status)
	}

	var settings struct {
		Config struct {
			LineProtoSupportVersions []int `json:"line.proto.support.versions"`
			MaxFileNameLength        int   `json:"cairo.max.file.name.length"`
		} `json:"config"`
	}

	if err := json.Unmarshal(buf, &settings); err != nil {
		return ProtocolVersion1, nil
	}

	// Update file name limit if provided by server
	if settings.Config.MaxFileNameLength != 0 {
		conf.fileNameLimit = settings.Config.MaxFileNameLength
	}

	// Determine protocol version based on server support
	versions := settings.Config.LineProtoSupportVersions
	if len(versions) == 0 {
		return ProtocolVersion1, nil
	}

	hasProtocolVersion1 := false
	for _, version := range versions {
		if version == 2 {
			return ProtocolVersion2, nil
		}
		if version == 1 {
			hasProtocolVersion1 = true
		}
	}
	if hasProtocolVersion1 {
		return ProtocolVersion1, nil
	}

	return protocolVersionUnset, errors.New("server does not support current client")
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

// Messages returns the accumulated ILP messages that are not
// flushed to the TCP connection yet. Useful for debugging purposes.
func (s *httpLineSender) Messages() []byte {
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

func (s *httpLineSenderV2) Table(name string) LineSender {
	s.buf.Table(name)
	return s
}

func (s *httpLineSenderV2) Symbol(name, val string) LineSender {
	s.buf.Symbol(name, val)
	return s
}

func (s *httpLineSenderV2) Int64Column(name string, val int64) LineSender {
	s.buf.Int64Column(name, val)
	return s
}

func (s *httpLineSenderV2) Long256Column(name string, val *big.Int) LineSender {
	s.buf.Long256Column(name, val)
	return s
}

func (s *httpLineSenderV2) TimestampColumn(name string, ts time.Time) LineSender {
	s.buf.TimestampColumn(name, ts)
	return s
}

func (s *httpLineSenderV2) StringColumn(name, val string) LineSender {
	s.buf.StringColumn(name, val)
	return s
}

func (s *httpLineSenderV2) BoolColumn(name string, val bool) LineSender {
	s.buf.BoolColumn(name, val)
	return s
}

func (s *httpLineSenderV2) Float64Column(name string, val float64) LineSender {
	s.buf.Float64ColumnBinary(name, val)
	return s
}

func (s *httpLineSenderV2) Float64Array1DColumn(name string, values []float64) LineSender {
	s.buf.Float64Array1DColumn(name, values)
	return s
}

func (s *httpLineSenderV2) Float64Array2DColumn(name string, values [][]float64) LineSender {
	s.buf.Float64Array2DColumn(name, values)
	return s
}

func (s *httpLineSenderV2) Float64Array3DColumn(name string, values [][][]float64) LineSender {
	s.buf.Float64Array3DColumn(name, values)
	return s
}

func (s *httpLineSenderV2) Float64ArrayNDColumn(name string, values *NdArray[float64]) LineSender {
	s.buf.Float64ArrayNDColumn(name, values)
	return s
}
