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
	"net"
	"strconv"
	"strings"
	"time"
)

type tcpTlsMode int64

const (
	tlsDisabled           tcpTlsMode = 0
	tlsEnabled            tcpTlsMode = 1
	tlsInsecureSkipVerify tcpTlsMode = 2
)

// TcpLineSender allows you to insert rows into QuestDB by sending ILP
// messages over TCP(S).
//
// Each sender corresponds to a single TCP connection. A sender
// should not be called concurrently by multiple goroutines.
type tcpLineSender struct {
	buf buffer

	address string

	// Authentication-related fields
	tlsMode tcpTlsMode
	keyId   string // Erased once auth is done.
	key     string // Erased once auth is done.

	conn net.Conn
}

// LineSenderOption defines line sender option.
type TcpLineSenderOption func(*tcpLineSender)

// WithAddress sets address to connect to. Should be in the
// "host:port" format. Defaults to "127.0.0.1:9009".
func WithTcpAddress(address string) TcpLineSenderOption {
	return func(s *tcpLineSender) {
		s.address = address
	}
}

// WithAuth sets token (private key) used for ILP authentication.
func WithTcpAuth(tokenId, token string) TcpLineSenderOption {
	return func(s *tcpLineSender) {
		s.keyId = tokenId
		s.key = token
	}
}

// WithTls enables TLS connection encryption.
func WithTcpTls() TcpLineSenderOption {
	return func(s *tcpLineSender) {
		s.tlsMode = tlsEnabled
	}
}

// WithTlsInsecureSkipVerify enables TLS connection encryption,
// but skips server certificate verification. Useful in test
// environments with self-signed certificates. Do not use in
// production environments.
func WithTcpTlsInsecureSkipVerify() TcpLineSenderOption {
	return func(s *tcpLineSender) {
		s.tlsMode = tlsInsecureSkipVerify
	}
}

// WithBufferCapacity sets desired buffer capacity in bytes to
// be used when sending ILP messages. Defaults to 128KB.
//
// This setting is a soft limit, i.e. the underlying buffer may
// grow larger than the provided value, but will shrink on a
// At, AtNow, or Flush call.
func WithTcpBufferCapacity(capacity int) TcpLineSenderOption {
	return func(s *tcpLineSender) {
		if capacity > 0 {
			s.buf.BufCap = capacity
		}
	}
}

// WithFileNameLimit sets maximum file name length in chars
// allowed by the server. Affects maximum table and column name
// lengths accepted by the sender. Should be set to the same value
// as on the server. Defaults to 127.
func WithTcpFileNameLimit(limit int) TcpLineSenderOption {
	return func(s *tcpLineSender) {
		if limit > 0 {
			s.buf.FileNameLimit = limit
		}
	}
}

// NewTcpLineSender creates new InfluxDB Line Protocol (ILP) sender. Each
// sender corresponds to a single TCP connection. Sender should
// not be called concurrently by multiple goroutines.
func NewTcpLineSender(ctx context.Context, opts ...TcpLineSenderOption) (*tcpLineSender, error) {
	var (
		d    net.Dialer
		key  *ecdsa.PrivateKey
		conn net.Conn
		err  error
	)

	s := &tcpLineSender{
		address: "127.0.0.1:9009",
		tlsMode: tlsDisabled,

		buf: newBuffer(),
	}

	for _, opt := range opts {
		opt(s)
	}

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

	s.conn = conn
	s.buf.Buffer = *bytes.NewBuffer(make([]byte, 0, s.buf.BufCap))

	return s, nil
}

func optsFromConf(config string) ([]TcpLineSenderOption, error) {
	var (
		user, token string
	)

	data, err := ParseConfigString(config)
	if err != nil {
		return nil, err
	}

	if data.Schema != "tcp" && data.Schema != "tcps" {
		return nil, fmt.Errorf("invalid schema: %s", data.Schema)
	}

	opts := make([]TcpLineSenderOption, 0)

	for k, v := range data.KeyValuePairs {
		switch strings.ToLower(k) {
		case "addr":
			opts = append(opts, WithTcpAddress(v))
		case "user":
			user = v
			if token != "" && user != "" {
				opts = append(opts, WithTcpAuth(user, token))
			}
		case "token":
			token = v
			opts = append(opts, WithTcpAuth(user, token))
		case "auto_flush":
			if v == "on" {
				return nil, NewInvalidConfigStrError("auto_flush option is not supported")
			}
		case "auto_flush_rows", "auto_flush_bytes":
			return nil, NewInvalidConfigStrError("auto_flush option is not supported")
		case "init_buf_size", "max_buf_size":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewInvalidConfigStrError("invalid %s value, %q is not a valid int", k, v)

			}
			switch k {
			case "max_buf_size":
				opts = append(opts, WithTcpBufferCapacity(parsedVal))
			default:
				panic("add a case for " + k)
			}

		case "tls_verify":
			switch v {
			case "on":
				opts = append(opts, WithTcpTls())
			case "unsafe_off":
				opts = append(opts, WithTcpTlsInsecureSkipVerify())
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

	return opts, nil
}

// TcpLineSenderFromConf creates a TcpLineSender using the QuestDB config string format.
//
// Example config string: "tcp::addr=localhost;username=joe;token=123;auto_flush_rows=1000;"
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
// Supported parameter values for tcp(s):
//
// addr:      hostname/port of QuestDB HTTP endpoint
// username:  for basic authentication
// password:  for basic authentication
// token:     bearer token auth (used instead of basic authentication)
//
// auto_flush:       determines if auto-flushing is enabled (values "on" or "off", defaults to "on")
// auto_flush_rows:  auto-flushing is triggered above this row count (defaults to 75000). If set, explicitly implies auto_flush=on
//
// init_buf_size:  initial growable ILP buffer size in bytes (defaults to 64KiB)
// max_buf_size:   buffer growth limit in bytes. client errors if breached (default is 100MiB)
//
// tls_verify: determines if TLS certificates should be validated (defaults to "on", can be set to "unsafe_off")
func TcpLineSenderFromConf(ctx context.Context, config string) (*tcpLineSender, error) {
	opts, err := optsFromConf(config)
	if err != nil {
		return nil, err
	}
	return NewTcpLineSender(ctx, opts...)
}

func (s *tcpLineSender) Close(_ context.Context) error {
	return s.conn.Close()
}

func (s *tcpLineSender) Table(name string) *tcpLineSender {
	s.buf.Table(name)
	return s
}

func (s *tcpLineSender) Symbol(name, val string) *tcpLineSender {
	s.buf.Symbol(name, val)
	return s
}

func (s *tcpLineSender) Int64Column(name string, val int64) *tcpLineSender {
	s.buf.Int64Column(name, val)
	return s
}

func (s *tcpLineSender) Long256Column(name string, val *big.Int) *tcpLineSender {
	s.buf.Long256Column(name, val)
	return s
}

func (s *tcpLineSender) TimestampColumn(name string, ts time.Time) *tcpLineSender {
	s.buf.TimestampColumn(name, ts)
	return s
}

func (s *tcpLineSender) Float64Column(name string, val float64) *tcpLineSender {
	s.buf.Float64Column(name, val)
	return s
}

func (s *tcpLineSender) StringColumn(name, val string) *tcpLineSender {
	s.buf.StringColumn(name, val)
	return s
}

func (s *tcpLineSender) BoolColumn(name string, val bool) *tcpLineSender {
	s.buf.BoolColumn(name, val)
	return s
}

func (s *tcpLineSender) Flush(ctx context.Context) error {
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

	if err = ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		s.conn.SetWriteDeadline(deadline)
	} else {
		s.conn.SetWriteDeadline(time.Time{})
	}

	if _, err := s.buf.WriteTo(s.conn); err != nil {
		return err
	}

	// bytes.Buffer grows as 2*cap+n, so we use 3x as the threshold.
	if s.buf.Cap() > 3*s.buf.BufCap {
		// Shrink the buffer back to desired capacity.
		s.buf.Buffer = *bytes.NewBuffer(make([]byte, 0, s.buf.BufCap))
	}
	return nil
}

func (s *tcpLineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

func (s *tcpLineSender) At(ctx context.Context, ts time.Time) error {
	sendTs := true
	if ts.IsZero() {
		sendTs = false
	}

	err := s.buf.At(ts, sendTs)
	if err != nil {
		return err
	}

	if s.buf.Len() > s.buf.BufCap {
		return s.Flush(ctx)
	}
	return nil
}

// Messages returns a copy of accumulated ILP messages that are not
// flushed to the TCP connection yet. Useful for debugging purposes.
func (s *tcpLineSender) Messages() string {
	return s.buf.Messages()
}

// MsgCount returns the number of buffered messages
func (s *tcpLineSender) MsgCount() int {
	return s.buf.msgCount
}
