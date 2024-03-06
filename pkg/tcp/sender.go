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

package tcp

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
// messages over TCP(S).
//
// Each sender corresponds to a single TCP connection. A sender
// should not be called concurrently by multiple goroutines.
type LineSender struct {
	buffer.Buffer

	address string

	// Authentication-related fields
	tlsMode tlsMode
	keyId   string // Erased once auth is done.
	key     string // Erased once auth is done.

	// Auto-flush fields
	autoFlush     bool
	autoFlushRows int
	msgCount      int

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
			s.BufCap = capacity
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
			s.FileNameLimit = limit
		}
	}
}

// WithInitBuffer size sets the desired initial buffer capacity
// in bytes to be used when sending ILP messages. Defaults to 0.
func WithInitBufferSize(sizeInBytes int) LineSenderOption {
	return func(s *LineSender) {
		s.InitBufSizeBytes = sizeInBytes
	}
}

// WithAutoFlushDisabled turns off auto-flushing behavior.
// To send ILP messages, the user either must call Flush().
func WithAutoFlushDisabled() LineSenderOption {
	return func(s *LineSender) {
		s.autoFlush = false
	}
}

// WithAutoFlushRows sets the number of buffered rows that
// must be breached in order to trigger an auto-flush.
// Defaults to 75000.
func WithAutoFlushRows(rows int) LineSenderOption {
	return func(s *LineSender) {
		s.autoFlush = true
		s.autoFlushRows = rows
	}
}

// NewLineSender creates new InfluxDB Line Protocol (ILP) sender. Each
// sender corresponds to a single TCP connection. Sender should
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

		autoFlush:     true,
		autoFlushRows: 75000,

		Buffer: *buffer.NewBuffer(),
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

	s.Buffer.Buffer = *bytes.NewBuffer(make([]byte, s.InitBufSizeBytes, s.BufCap))

	return s, nil

}

func optsFromConf(config string) ([]LineSenderOption, error) {
	var (
		user, token string
	)

	data, err := conf.ParseConfigString(config)
	if err != nil {
		return nil, err
	}

	if data.Schema != "tcp" && data.Schema != "tcps" {
		return nil, fmt.Errorf("invalid schema: %s", data.Schema)
	}

	opts := make([]LineSenderOption, 0)

	for k, v := range data.KeyValuePairs {
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
				return nil, conf.NewConfigStrParseError("auto_flush option is not supported")
			}
		case "auto_flush_rows", "auto_flush_bytes":
			return nil, conf.NewConfigStrParseError("auto_flush option is not supported")
		case "init_buf_size", "max_buf_size":
			parsedVal, err := strconv.Atoi(v)
			if err != nil {
				return nil, conf.NewConfigStrParseError("invalid %s value, %q is not a valid int", k, v)

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

	return opts, nil
}

// LineSenderFromConf creates a LineSender using the QuestDB config string format.
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
func LineSenderFromConf(ctx context.Context, config string) (*LineSender, error) {
	opts, err := optsFromConf(config)
	if err != nil {
		return nil, err
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

// Flush flushes the accumulated messages to the underlying TCP
// connection. Should be called periodically to make sure that
// all messages are sent to the server.
//
// For optimal performance, this method should not be called after
// each ILP message. Instead, the messages should be written in
// batches followed by a Flush call. Optimal batch size may vary
// from 100 to 1,000 messages depending on the message size and
// configured buffer capacity.
func (s *LineSender) Flush(ctx context.Context) error {

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

	if err = ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		s.conn.SetWriteDeadline(deadline)
	} else {
		s.conn.SetWriteDeadline(time.Time{})
	}

	if _, err := s.Buffer.WriteTo(s.conn); err != nil {
		return err
	}

	// bytes.Buffer grows as 2*cap+n, so we use 3x as the threshold.
	if s.Cap() > 3*s.BufCap {
		// Shrink the buffer back to desired capacity.
		s.Buffer.Buffer = *bytes.NewBuffer(make([]byte, s.InitBufSizeBytes, s.BufCap))
	}

	s.msgCount = 0

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
	sendTs := true
	if ts.IsZero() {
		sendTs = false
	}
	err := s.Buffer.At(ts, sendTs)
	if err != nil {
		return err
	}

	s.msgCount++

	if s.Len() > s.BufCap {
		return s.Flush(ctx)
	}

	if s.autoFlush && s.msgCount > s.autoFlushRows {
		return s.Flush(ctx)
	}

	return nil
}
