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
	"bufio"
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	mathRand "math/rand"
	"net"
	"net/http"
	"net/url"
	"time"
)

// ErrInvalidMsg indicates a failed attempt to construct an ILP
// message, e.g. duplicate calls to Table method or illegal
// chars found in table or column name.
var ErrInvalidMsg = errors.New("invalid message")

const (
	defaultBufferCapacity = 128 * 1024
	defaultFileNameLimit  = 127
)

type tlsMode int64

const (
	tlsDisabled           tlsMode = 0
	tlsEnabled            tlsMode = 1
	tlsInsecureSkipVerify tlsMode = 2
)

type transportProtocol string

const (
	protocolHttp transportProtocol = "http"
	protocolTcp  transportProtocol = "tcp"
)

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages over HTTP(S)/TCP(S).
//
// Each sender corresponds to a single TCP connection. A sender
// should not be called concurrently by multiple goroutines.
type LineSender struct {
	buffer

	address           string
	transportProtocol transportProtocol

	// Authentication-related fields
	tlsMode tlsMode

	keyId string // Erased once auth is done.
	key   string // Erased once auth is done.
	user  string // Erased once auth is done.
	pass  string // Erased once auth is done.
	token string // Erased once auth is done.

	minThroughputBytesPerSecond int
	graceTimeout                time.Duration
	retryTimeout                time.Duration
	initBufSizeBytes            int

	tcpConn    net.Conn
	httpClient http.Client
}

// LineSenderOption defines line sender option.
type LineSenderOption func(*LineSender)

// WithAddress sets address to connect to. Should be in the
// "host:port" format. Defaults to "127.0.0.1:9009".
func WithAddress(address string) LineSenderOption {
	return func(s *LineSender) {
		s.address = address
	}
}

// WithAuth sets token (private key) used for ILP authentication.
func WithAuth(tokenId, token string) LineSenderOption {
	return func(s *LineSender) {
		s.keyId = tokenId
		s.key = token
	}
}

// WithTls enables TLS connection encryption.
func WithTls() LineSenderOption {
	return func(s *LineSender) {
		s.tlsMode = tlsEnabled
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
			s.bufCap = capacity
		}
	}
}

// WithFileNameLimit sets maximum file name length in chars
// allowed by the server. Affects maximum table and column name
// lengths accepted by the sender. Should be set to the same value
// as on the server. Defaults to 127.
func WithFileNameLimit(limit int) LineSenderOption {
	return func(s *LineSender) {
		if limit > 0 {
			s.fileNameLimit = limit
		}
	}
}

func WithGraceTimeout(timeout time.Duration) LineSenderOption {
	return func(s *LineSender) {
		s.graceTimeout = timeout
	}
}

func WithMinThroughput(bytesPerSecond int) LineSenderOption {
	return func(s *LineSender) {
		s.minThroughputBytesPerSecond = bytesPerSecond
	}
}

func WithInitBufferSize(sizeInBytes int) LineSenderOption {
	return func(s *LineSender) {
		s.initBufSizeBytes = sizeInBytes
	}
}

func WithRetryTimeout(timeout time.Duration) LineSenderOption {
	return func(s *LineSender) {
		s.retryTimeout = timeout
	}
}

func WithBasicAuth(user, pass string) LineSenderOption {
	return func(s *LineSender) {
		s.user = user
		s.pass = pass
	}
}

func WithHttp() LineSenderOption {
	return func(s *LineSender) {
		s.transportProtocol = protocolHttp
	}
}

func WithTcp() LineSenderOption {
	return func(s *LineSender) {
		s.transportProtocol = protocolTcp
	}
}

func WithBearerToken(token string) LineSenderOption {
	return func(s *LineSender) {
		s.token = token
	}
}

func FromConf(ctx context.Context, conf string) (*LineSender, error) {
	opts, err := parseConfigString(conf)
	if err != nil {
		return nil, err
	}
	return NewLineSender(ctx, opts...)
}

// NewLineSender creates new InfluxDB Line Protocol (ILP) sender. Each
// sender corresponds to a single HTTP/TCP connection. Sender should
// not be called concurrently by multiple goroutines.
func NewLineSender(ctx context.Context, opts ...LineSenderOption) (*LineSender, error) {
	var (
		d    net.Dialer
		key  *ecdsa.PrivateKey
		conn net.Conn
		err  error
	)

	s := &LineSender{
		address:                     "127.0.0.1:9009",
		tlsMode:                     tlsDisabled,
		minThroughputBytesPerSecond: 100 * 1024,
		graceTimeout:                time.Second * 5,
		retryTimeout:                time.Second * 10,

		buffer: buffer{
			bufCap:        defaultBufferCapacity,
			fileNameLimit: defaultFileNameLimit,
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	// Default to tcp
	if s.transportProtocol == "" {
		s.transportProtocol = protocolTcp
	}

	switch s.transportProtocol {
	case protocolTcp:
		// If the user does not specify an address, default to port 9009
		if s.address == "" {
			s.address = "127.0.0.1:9009"
		}

		// Process tcp args in the same exact way that we do in v2
		if s.keyId != "" && s.key != "" {
			keyRaw, err := base64.RawURLEncoding.DecodeString(s.key)
			if err != nil {
				return nil, fmt.Errorf("failed to decode auth key: %v", err)
			}
			key = new(ecdsa.PrivateKey)
			key.PublicKey.Curve = elliptic.P256()
			key.PublicKey.X, key.PublicKey.Y = key.PublicKey.Curve.ScalarBaseMult(keyRaw)
			key.D = new(big.Int).SetBytes(keyRaw)
		}

		if s.tlsMode == tlsDisabled {
			conn, err = d.DialContext(ctx, "tcp", s.address)
		} else {
			config := &tls.Config{}
			if s.tlsMode == tlsInsecureSkipVerify {
				config.InsecureSkipVerify = true
			}
			conn, err = tls.DialWithDialer(&d, "tcp", s.address, config)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to connect to server: %v", err)
		}

		if key != nil {
			if deadline, ok := ctx.Deadline(); ok {
				conn.SetDeadline(deadline)
			}

			_, err = conn.Write([]byte(s.keyId + "\n"))
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to write key id: %v", err)
			}

			reader := bufio.NewReader(conn)
			raw, err := reader.ReadBytes('\n')
			if len(raw) < 2 {
				conn.Close()
				return nil, fmt.Errorf("empty challenge response from server: %v", err)
			}
			// Remove the `\n` in the last position.
			raw = raw[:len(raw)-1]
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to read challenge response from server: %v", err)
			}

			// Hash the challenge with sha256.
			hash := crypto.SHA256.New()
			hash.Write(raw)
			hashed := hash.Sum(nil)

			stdSig, err := ecdsa.SignASN1(rand.Reader, key, hashed)
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to sign challenge using auth key: %v", err)
			}
			_, err = conn.Write([]byte(base64.StdEncoding.EncodeToString(stdSig) + "\n"))
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to write signed challenge: %v", err)
			}

			// Reset the deadline.
			conn.SetDeadline(time.Time{})
			// Erase the key values since we don't need them anymore.
			s.key = ""
			s.keyId = ""
		}

		s.tcpConn = conn
	case protocolHttp:
		// Default to localhost at port 9000
		if s.address == "" {
			s.address = "127.0.0.1:9000"
		}

		// Transport copied from http.DefaultTransport
		tr := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: defaultTransportDialContext(&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		switch s.tlsMode {
		case tlsInsecureSkipVerify:
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		case tlsDisabled:
			tr.TLSClientConfig = nil
		}

		s.httpClient = http.Client{
			Transport: tr,
		}

	default:
		panic("unsupported protocol " + s.transportProtocol)
	}

	s.buffer.Buffer = *bytes.NewBuffer(make([]byte, s.initBufSizeBytes, s.bufCap))

	return s, nil

}

// Close closes the underlying TCP connection. Does not flush
// in-flight messages, so make sure to call Flush first.
func (s *LineSender) Close() error {
	if s.transportProtocol == protocolTcp {
		return s.tcpConn.Close()
	}

	return nil
}

// Table sets the table name (metric) for a new ILP message. Should be
// called before any Symbol or Column method.
//
// Table name cannot contain any of the following characters:
// '\n', '\r', '?', ',', ”', '"', '\', '/', ':', ')', '(', '+', '*',
// '%', '~', starting '.', trailing '.', or a non-printable char.
func (s *LineSender) Table(name string) *LineSender {
	s.buffer.table(name)
	return s
}

// Symbol adds a symbol column value to the ILP message. Should be called
// before any Column method.
//
// Symbol name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Symbol(name, val string) *LineSender {
	s.buffer.symbol(name, val)
	return s
}

// Int64Column adds a 64-bit integer (long) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Int64Column(name string, val int64) *LineSender {
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
func (s *LineSender) Long256Column(name string, val *big.Int) *LineSender {
	s.buffer.long256Column(name, val)
	return s
}

// TimestampColumn adds a timestamp column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) TimestampColumn(name string, ts time.Time) *LineSender {
	s.buffer.timestampColumn(name, ts)
	return s
}

// Float64Column adds a 64-bit float (double) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Float64Column(name string, val float64) *LineSender {
	s.buffer.float64Column(name, val)
	return s
}

// StringColumn adds a string column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) StringColumn(name, val string) *LineSender {
	s.stringColumn(name, val)
	return s
}

// BoolColumn adds a boolean column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) BoolColumn(name string, val bool) *LineSender {
	s.buffer.boolColumn(name, val)
	return s
}

// AtNow omits the timestamp and finalizes the ILP message.
// The server will insert each message using the system clock
// as the row timestamp.
//
// If the underlying buffer reaches configured capacity, this
// method also sends the accumulated messages.
func (s *LineSender) AtNow(ctx context.Context) error {
	return s.at(ctx, time.Time{}, false)
}

// At sets the timestamp in Epoch nanoseconds and finalizes
// the ILP message.
//
// If the underlying buffer reaches configured capacity, this
// method also sends the accumulated messages.
func (s *LineSender) At(ctx context.Context, ts time.Time) error {
	return s.at(ctx, ts, true)
}

func (s *LineSender) at(ctx context.Context, ts time.Time, sendTs bool) error {
	err := s.lastErr
	s.lastErr = nil
	if err != nil {
		s.discardPendingMsg()
		return err
	}
	if !s.hasTable {
		s.discardPendingMsg()
		return fmt.Errorf("table name was not provided: %w", ErrInvalidMsg)
	}
	if !s.hasTags && !s.hasFields {
		s.discardPendingMsg()
		return fmt.Errorf("no symbols or columns were provided: %w", ErrInvalidMsg)
	}

	if sendTs {
		s.WriteByte(' ')
		s.WriteInt(ts.UnixNano())
	}
	s.WriteByte('\n')

	s.lastMsgPos = s.Len()
	s.resetMsgFlags()

	if s.Len() > s.bufCap {
		return s.Flush(ctx)
	}
	return nil
}

// Flush flushes the accumulated messages to the underlying TCP
// connection or HTTP client. Should be called periodically to make sure that
// all messages are sent to the server.
//
// For optimal performance, this method should not be called after
// each ILP message. Instead, the messages should be written in
// batches followed by a Flush call. Optimal batch size may vary
// from 100 to 1,000 messages depending on the message size and
// configured buffer capacity.
func (s *LineSender) Flush(ctx context.Context) error {
	switch s.transportProtocol {
	case protocolTcp:
		return s.flushTcp(ctx)
	case protocolHttp:
		return s.flushHttp(ctx)
	default:
		panic("invalid protocol " + s.transportProtocol)
	}
}

func (s *LineSender) flushTcp(ctx context.Context) error {
	err := s.lastErr
	s.lastErr = nil
	if err != nil {
		s.discardPendingMsg()
		return err
	}
	if s.hasTable {
		s.discardPendingMsg()
		return errors.New("pending ILP message must be finalized with At or AtNow before calling Flush")
	}

	if err = ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		s.tcpConn.SetWriteDeadline(deadline)
	} else {
		s.tcpConn.SetWriteDeadline(time.Time{})
	}

	n, err := s.WriteTo(s.tcpConn)
	if err != nil {
		s.lastMsgPos -= int(n)
		return err
	}

	// bytes.Buffer grows as 2*cap+n, so we use 3x as the threshold.
	if s.Cap() > 3*s.bufCap {
		// Shrink the buffer back to desired capacity.
		s.buffer.Buffer = *bytes.NewBuffer(make([]byte, s.initBufSizeBytes, s.bufCap))
	}
	s.lastMsgPos = 0

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

func (s *LineSender) flushHttp(ctx context.Context) error {
	var (
		err           error
		req           *http.Request
		retryInterval time.Duration

		maxRetryInterval = time.Second
	)
	uri := string(s.transportProtocol)
	if s.tlsMode > 0 {
		uri += "s"
	}
	uri += fmt.Sprintf("://%s/write", s.address)

	// timeout = ( request.len() / min_throughput ) + grace
	// Conversion from int to time.Duration is in milliseconds and grace is in millis
	timeout := time.Duration(s.Len()/s.minThroughputBytesPerSecond)*time.Second + s.graceTimeout
	reqCtx, cancelFunc := context.WithTimeout(ctx, timeout)
	defer cancelFunc()

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

	resp, err := s.httpClient.Do(req)
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
			jitter := time.Duration(mathRand.Intn(10)) * time.Millisecond
			time.Sleep(retryInterval + jitter)

			resp, retryErr := s.httpClient.Do(req)
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

func defaultTransportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return dialer.DialContext
}
