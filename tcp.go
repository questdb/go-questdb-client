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

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages over HTTP(S)/TCP(S).
//
// Each sender corresponds to a single TCP connection. A sender
// should not be called concurrently by multiple goroutines.
type LineSender struct {
	buffer

	address string

	// Authentication-related fields
	tlsMode tlsMode

	keyId string // Erased once auth is done.
	key   string // Erased once auth is done.

	conn net.Conn
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

func WithInitBufferSize(sizeInBytes int) LineSenderOption {
	return func(s *LineSender) {
		s.initBufSizeBytes = sizeInBytes
	}
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
		address: "127.0.0.1:9009",
		tlsMode: tlsDisabled,

		buffer: buffer{
			bufCap:        defaultBufferCapacity,
			fileNameLimit: defaultFileNameLimit,
		},
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

	s.buffer.Buffer = *bytes.NewBuffer(make([]byte, s.initBufSizeBytes, s.bufCap))

	return s, nil

}

func LineSenderFromConf(ctx context.Context, conf string) (*LineSender, error) {
	var (
		user, token string
	)

	data, err := parseConfigString(conf)
	if err != nil {
		return nil, err
	}

	if data.schema != "tcp" && data.schema != "tcps" {
		return nil, fmt.Errorf("invalid schema: %s", data.schema)
	}

	opts := make([]LineSenderOption, 0)

	for k, v := range data.keyValuePairs {
		switch strings.ToLower(k) {
		case "addr":
			opts = append(opts, WithAddress(v))
		case "user":
			user = v
			if token != "" && user != "" {
				opts = append(opts, WithAuth(user, token))
			}
		case "token":
			token = v
			opts = append(opts, WithAuth(user, token))
		case "auto_flush":
			if v == "on" {
				return nil, NewConfigStrParseError("auto_flush option is not supported")
			}
		case "auto_flush_rows", "auto_flush_bytes":
			return nil, NewConfigStrParseError("auto_flush option is not supported")
		case "init_buf_size", "max_buf_size":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)

			}
			switch k {
			case "init_buf_size":
				opts = append(opts, WithInitBufferSize(parsedVal))
			case "max_buf_size":
				opts = append(opts, WithBufferCapacity(parsedVal))
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

	return NewLineSender(ctx, opts...)
}

// Close closes the underlying TCP connection. Does not flush
// in-flight messages, so make sure to call Flush first.
func (s *LineSender) Close() error {
	return s.conn.Close()
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
		s.conn.SetWriteDeadline(deadline)
	} else {
		s.conn.SetWriteDeadline(time.Time{})
	}

	n, err := s.WriteTo(s.conn)
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

// AtNow omits the timestamp and finalizes the ILP message.
// The server will insert each message using the system clock
// as the row timestamp.
//
// If the underlying buffer reaches configured capacity, this
// method also sends the accumulated messages.
func (s *LineSender) AtNow(ctx context.Context) error {
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
func (s *LineSender) At(ctx context.Context, ts time.Time) error {
	err := s.at(ts, true)
	if err != nil {
		return err
	}
	if s.Len() > s.bufCap {
		return s.Flush(ctx)
	}
	return nil
}
