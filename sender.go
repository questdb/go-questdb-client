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
	"math"
	"math/big"
	"net"
	"strconv"
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

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages.
//
// Each sender corresponds to a single TCP connection. A sender
// should not be called concurrently by multiple goroutines.
type LineSender struct {
	address       string
	tlsMode       tlsMode
	keyId         string // Erased once auth is done.
	key           string // Erased once auth is done.
	bufCap        int
	fileNameLimit int
	conn          net.Conn
	buf           *buffer
	lastMsgPos    int
	lastErr       error
	hasTable      bool
	hasTags       bool
	hasFields     bool
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

// WithTls enables TLS connection encryption, but skips server
// certificate verification. Useful in test environments with
// self-signed certificates. Do not use in production
// environments.
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
		address:       "127.0.0.1:9009",
		bufCap:        defaultBufferCapacity,
		fileNameLimit: defaultFileNameLimit,
		tlsMode:       tlsDisabled,
	}
	for _, opt := range opts {
		opt(s)
	}

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
	s.buf = newBuffer(s.bufCap)
	return s, nil
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
	if s.lastErr != nil {
		return s
	}
	if s.hasTable {
		s.lastErr = fmt.Errorf("table name already provided: %w", ErrInvalidMsg)
		return s
	}
	s.lastErr = s.writeTableName(name)
	if s.lastErr != nil {
		return s
	}
	s.hasTable = true
	return s
}

// Symbol adds a symbol column value to the ILP message. Should be called
// before any Column method.
//
// Symbol name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Symbol(name, val string) *LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("table name was not provided: %w", ErrInvalidMsg)
		return s
	}
	if s.hasFields {
		s.lastErr = fmt.Errorf("symbols have to be written before any other column: %w", ErrInvalidMsg)
		return s
	}
	s.buf.WriteByte(',')
	s.lastErr = s.writeColumnName(name)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('=')
	s.lastErr = s.writeStrValue(val, false)
	if s.lastErr != nil {
		return s
	}
	s.hasTags = true
	return s
}

// Int64Column adds a 64-bit integer (long) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Int64Column(name string, val int64) *LineSender {
	if !s.prepareForField(name) {
		return s
	}
	s.lastErr = s.writeColumnName(name)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('=')
	s.buf.WriteInt(val)
	s.buf.WriteByte('i')
	s.hasFields = true
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
	if val.Sign() < 0 {
		if s.lastErr != nil {
			return s
		}
		s.lastErr = fmt.Errorf("long256 cannot be negative: %s", val.String())
		return s
	}
	if val.BitLen() > 256 {
		if s.lastErr != nil {
			return s
		}
		s.lastErr = fmt.Errorf("long256 cannot be larger than 256-bit: %v", val.BitLen())
		return s
	}
	if !s.prepareForField(name) {
		return s
	}
	s.lastErr = s.writeColumnName(name)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('=')
	s.buf.WriteByte('0')
	s.buf.WriteByte('x')
	s.buf.WriteBigInt(val)
	s.buf.WriteByte('i')
	if s.lastErr != nil {
		return s
	}
	s.hasFields = true
	return s
}

// TimestampColumn adds a timestamp column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) TimestampColumn(name string, ts time.Time) *LineSender {
	if !s.prepareForField(name) {
		return s
	}
	s.lastErr = s.writeColumnName(name)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('=')
	s.buf.WriteInt(ts.UnixMicro())
	s.buf.WriteByte('t')
	s.hasFields = true
	return s
}

// Float64Column adds a 64-bit float (double) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) Float64Column(name string, val float64) *LineSender {
	if !s.prepareForField(name) {
		return s
	}
	s.lastErr = s.writeColumnName(name)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('=')
	s.buf.WriteFloat(val)
	s.hasFields = true
	return s
}

// StringColumn adds a string column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) StringColumn(name, val string) *LineSender {
	if !s.prepareForField(name) {
		return s
	}
	s.lastErr = s.writeColumnName(name)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('=')
	s.buf.WriteByte('"')
	s.lastErr = s.writeStrValue(val, true)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('"')
	s.hasFields = true
	return s
}

// BoolColumn adds a boolean column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (s *LineSender) BoolColumn(name string, val bool) *LineSender {
	if !s.prepareForField(name) {
		return s
	}
	s.lastErr = s.writeColumnName(name)
	if s.lastErr != nil {
		return s
	}
	s.buf.WriteByte('=')
	if val {
		s.buf.WriteByte('t')
	} else {
		s.buf.WriteByte('f')
	}
	s.hasFields = true
	return s
}

func (s *LineSender) writeTableName(str string) error {
	if str == "" {
		return fmt.Errorf("table name cannot be empty: %w", ErrInvalidMsg)
	}
	// We use string length in bytes as an approximation. That's to
	// avoid calculating the number of runes.
	if len(str) > s.fileNameLimit {
		return fmt.Errorf("table name length exceeds the limit: %w", ErrInvalidMsg)
	}
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		b := str[i]
		switch b {
		case ' ':
			s.buf.WriteByte('\\')
		case '=':
			s.buf.WriteByte('\\')
		case '.':
			if i == 0 || i == len(str)-1 {
				return fmt.Errorf("table name contains '.' char at the start or end: %s: %w", str, ErrInvalidMsg)
			}
		default:
			if illegalTableNameChar(b) {
				return fmt.Errorf("table name contains an illegal char: "+
					"'\\n', '\\r', '?', ',', ''', '\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: %s: %w",
					str, ErrInvalidMsg)
			}
		}
		s.buf.WriteByte(b)
	}
	return nil
}

func illegalTableNameChar(ch byte) bool {
	switch ch {
	case '\n':
		return true
	case '\r':
		return true
	case '?':
		return true
	case ',':
		return true
	case '\'':
		return true
	case '"':
		return true
	case '\\':
		return true
	case '/':
		return true
	case ':':
		return true
	case ')':
		return true
	case '(':
		return true
	case '+':
		return true
	case '*':
		return true
	case '%':
		return true
	case '~':
		return true
	case '\u0000':
		return true
	case '\u0001':
		return true
	case '\u0002':
		return true
	case '\u0003':
		return true
	case '\u0004':
		return true
	case '\u0005':
		return true
	case '\u0006':
		return true
	case '\u0007':
		return true
	case '\u0008':
		return true
	case '\u0009':
		return true
	case '\u000b':
		return true
	case '\u000c':
		return true
	case '\u000e':
		return true
	case '\u000f':
		return true
	case '\u007f':
		return true
	}
	return false
}

func (s *LineSender) writeColumnName(str string) error {
	if str == "" {
		return fmt.Errorf("column name cannot be empty: %w", ErrInvalidMsg)
	}
	// We use string length in bytes as an approximation. That's to
	// avoid calculating the number of runes.
	if len(str) > s.fileNameLimit {
		return fmt.Errorf("column name length exceeds the limit: %w", ErrInvalidMsg)
	}
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		b := str[i]
		switch b {
		case ' ':
			s.buf.WriteByte('\\')
		case '=':
			s.buf.WriteByte('\\')
		default:
			if illegalColumnNameChar(b) {
				return fmt.Errorf("column name contains an illegal char: "+
					"'\\n', '\\r', '?', '.', ',', ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: %s: %w",
					str, ErrInvalidMsg)
			}
		}
		s.buf.WriteByte(b)
	}
	return nil
}

func illegalColumnNameChar(ch byte) bool {
	switch ch {
	case '\n':
		return true
	case '\r':
		return true
	case '?':
		return true
	case '.':
		return true
	case ',':
		return true
	case '\'':
		return true
	case '"':
		return true
	case '\\':
		return true
	case '/':
		return true
	case ':':
		return true
	case ')':
		return true
	case '(':
		return true
	case '+':
		return true
	case '-':
		return true
	case '*':
		return true
	case '%':
		return true
	case '~':
		return true
	case '\u0000':
		return true
	case '\u0001':
		return true
	case '\u0002':
		return true
	case '\u0003':
		return true
	case '\u0004':
		return true
	case '\u0005':
		return true
	case '\u0006':
		return true
	case '\u0007':
		return true
	case '\u0008':
		return true
	case '\u0009':
		return true
	case '\u000b':
		return true
	case '\u000c':
		return true
	case '\u000e':
		return true
	case '\u000f':
		return true
	case '\u007f':
		return true
	}
	return false
}

func (s *LineSender) writeStrValue(str string, quoted bool) error {
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		b := str[i]
		switch b {
		case ' ':
			if !quoted {
				s.buf.WriteByte('\\')
			}
		case ',':
			if !quoted {
				s.buf.WriteByte('\\')
			}
		case '=':
			if !quoted {
				s.buf.WriteByte('\\')
			}
		case '"':
			if quoted {
				s.buf.WriteByte('\\')
			}
		case '\n':
			s.buf.WriteByte('\\')
		case '\r':
			s.buf.WriteByte('\\')
		case '\\':
			s.buf.WriteByte('\\')
		}
		s.buf.WriteByte(b)
	}
	return nil
}

func (s *LineSender) prepareForField(name string) bool {
	if s.lastErr != nil {
		return false
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("table name was not provided: %w", ErrInvalidMsg)
		return false
	}
	if !s.hasFields {
		s.buf.WriteByte(' ')
	} else {
		s.buf.WriteByte(',')
	}
	return true
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
		s.buf.WriteByte(' ')
		s.buf.WriteInt(ts.UnixNano())
	}
	s.buf.WriteByte('\n')

	s.lastMsgPos = s.buf.Len()
	s.resetMsgFlags()

	if s.buf.Len() > s.bufCap {
		return s.Flush(ctx)
	}
	return nil
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

	n, err := s.buf.WriteTo(s.conn)
	if err != nil {
		s.lastMsgPos -= int(n)
		return err
	}

	// bytes.Buffer grows as 2*cap+n, so we use 3x as the threshold.
	if s.buf.Cap() > 3*s.bufCap {
		// Shrink the buffer back to desired capacity.
		s.buf = newBuffer(s.bufCap)
	}
	s.lastMsgPos = 0

	return nil
}

func (s *LineSender) discardPendingMsg() {
	s.buf.Truncate(s.lastMsgPos)
	s.resetMsgFlags()
}

func (s *LineSender) resetMsgFlags() {
	s.hasTable = false
	s.hasTags = false
	s.hasFields = false
}

// Messages returns a copy of accumulated ILP messages that are not
// flushed to the TCP connection yet. Useful for debugging purposes.
func (s *LineSender) Messages() string {
	return s.buf.String()
}

// buffer is a wrapper on top of bytes.buffer. It extends the
// original struct with methods for writing int64 and float64
// numbers without unnecessary allocations.
type buffer struct {
	bytes.Buffer
}

func newBuffer(cap int) *buffer {
	return &buffer{*bytes.NewBuffer(make([]byte, 0, cap))}
}

func (b *buffer) WriteInt(i int64) {
	// We need up to 20 bytes to fit an int64, including a sign.
	var a [20]byte
	s := strconv.AppendInt(a[0:0], i, 10)
	b.Write(s)
}

func (b *buffer) WriteFloat(f float64) {
	if math.IsNaN(f) {
		b.WriteString("NaN")
		return
	} else if math.IsInf(f, -1) {
		b.WriteString("-Infinity")
		return
	} else if math.IsInf(f, 1) {
		b.WriteString("Infinity")
		return
	}
	// We need up to 24 bytes to fit a float64, including a sign.
	var a [24]byte
	s := strconv.AppendFloat(a[0:0], f, 'G', -1, 64)
	b.Write(s)
}

func (b *buffer) WriteBigInt(i *big.Int) {
	// We need up to 64 bytes to fit an unsigned 256-bit number.
	var a [64]byte
	s := i.Append(a[0:0], 16)
	b.Write(s)
}
