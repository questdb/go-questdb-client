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
	"math/big"
	"net/http"
	"time"
)

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages.
//
// Each sender corresponds to a single client-server connection.
// A sender should not be called concurrently by multiple goroutines.
type LineSender interface {
	// Table sets the table name (metric) for a new ILP message. Should be
	// called before any Symbol or Column method.
	//
	// Table name cannot contain any of the following characters:
	// '\n', '\r', '?', ',', ”', '"', '\', '/', ':', ')', '(', '+', '*',
	// '%', '~', starting '.', trailing '.', or a non-printable char.
	Table(name string) LineSender

	// Symbol adds a symbol column value to the ILP message. Should be called
	// before any Column method.
	//
	// Symbol name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Symbol(name, val string) LineSender

	// Int64Column adds a 64-bit integer (long) column value to the ILP
	// message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Int64Column(name string, val int64) LineSender

	// Long256Column adds a 256-bit unsigned integer (long256) column
	// value to the ILP message.
	//
	// Only non-negative numbers that fit into 256-bit unsigned integer are
	// supported and any other input value would lead to an error.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Long256Column(name string, val *big.Int) LineSender

	// TimestampColumn adds a timestamp column value to the ILP
	// message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	TimestampColumn(name string, ts time.Time) LineSender

	// Float64Column adds a 64-bit float (double) column value to the ILP
	// message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Float64Column(name string, val float64) LineSender

	// StringColumn adds a string column value to the ILP message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	StringColumn(name, val string) LineSender

	// BoolColumn adds a boolean column value to the ILP message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	BoolColumn(name string, val bool) LineSender

	// At sets the timestamp in Epoch nanoseconds and finalizes
	// the ILP message.
	//
	// If the underlying buffer reaches configured capacity or the
	// number of buffered messages exceeds the auto-flush trigger, this
	// method also sends the accumulated messages.
	//
	// If ts.IsZero(), no timestamp is sent to the server.
	At(ctx context.Context, ts time.Time) error

	// AtNow omits the timestamp and finalizes the ILP message.
	// The server will insert each message using the system clock
	// as the row timestamp.
	//
	// If the underlying buffer reaches configured capacity or the
	// number of buffered messages exceeds the auto-flush trigger, this
	// method also sends the accumulated messages.
	AtNow(ctx context.Context) error

	// Flush sends the accumulated messages via the underlying
	// connection. Should be called periodically to make sure that
	// all messages are sent to the server.
	//
	// For optimal performance, this method should not be called after
	// each ILP message. Instead, the messages should be written in
	// batches followed by a Flush call. The optimal batch size may
	// vary from one thousand to few thousand messages depending on
	// the message size.
	Flush(ctx context.Context) error

	// Close closes the underlying HTTP client.
	//
	// If auto-flush is enabled, the client will flush any remaining buffered
	// messages before closing itself.
	Close(ctx context.Context) error
}

type lineSenderConfig struct {
	address        string
	initBufferSize int
	fileNameLimit  int
	httpTransport  *http.Transport

	// Retry/timeout-related fields
	retryTimeout                time.Duration
	minThroughputBytesPerSecond int
	requestTimeout              time.Duration

	// Authentication-related fields
	tcpTlsMode tcpTlsMode
	tcpKeyId   string
	tcpKey     string
	httpUser   string
	httpPass   string
	httpToken  string

	// Auto-flush fields
	autoFlushRows     int
	autoFlushInterval time.Duration
}

// LineSenderOption defines line sender config option.
type LineSenderOption func(*lineSenderConfig)

// WithTls enables TLS connection encryption.
func WithTls() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.tcpTlsMode = tlsEnabled
	}
}

// WithAuth sets token (private key) used for ILP authentication.
//
// Only available for the TCP sender.
func WithAuth(tokenId, token string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.tcpKeyId = tokenId
		s.tcpKey = token
	}
}

// WithBasicAuth sets a Basic authentication header for
// ILP requests over HTTP.
//
// Only available for the HTTP sender.
func WithBasicAuth(user, pass string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.httpUser = user
		s.httpPass = pass
	}
}

// WithBearerToken sets a Bearer token Authentication header for
// ILP requests.
//
// Only available for the HTTP sender.
func WithBearerToken(token string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.httpToken = token
	}
}

// WithRequestTimeout is used in combination with min_throughput
// to set the timeout of an ILP request. Defaults to 10 seconds.
//
// timeout = (request.len() / min_throughput) + request_timeout
//
// Only available for the HTTP sender.
func WithRequestTimeout(timeout time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.requestTimeout = timeout
	}
}

// WithMinThroughput is used in combination with request_timeout
// to set the timeout of an ILP request. Defaults to 100KiB/s.
//
// timeout = (request.len() / min_throughput) + request_timeout
//
// Only available for the HTTP sender.
func WithMinThroughput(bytesPerSecond int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.minThroughputBytesPerSecond = bytesPerSecond
	}
}

// WithRetryTimeout is the cumulative maximum duration spend in
// retries. Defaults to 10 seconds.
//
// Only network-related errors and certain 5xx response
// codes are retryable.
//
// Only available for the HTTP sender.
func WithRetryTimeout(t time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.retryTimeout = t
	}
}

// WithInitBufferSize sets the desired initial buffer capacity
// in bytes to be used when sending ILP messages. Defaults to 128KB.
//
// This setting is a soft limit, i.e. the underlying buffer may
// grow larger than the provided value.
func WithInitBufferSize(sizeInBytes int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.initBufferSize = sizeInBytes
	}
}

// WithFileNameLimit sets maximum file name length in chars
// allowed by the server. Affects maximum table and column name
// lengths accepted by the sender. Should be set to the same value
// as on the server. Defaults to 127.
func WithFileNameLimit(limit int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.fileNameLimit = limit
	}
}

// WithAddress sets address to connect to. Should be in the
// "host:port" format. Defaults to "127.0.0.1:9000" in case
// of HTTP and "127.0.0.1:9009" in case of TCP.
func WithAddress(addr string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.address = addr
	}
}

// WithTlsInsecureSkipVerify enables TLS connection encryption,
// but skips server certificate verification. Useful in test
// environments with self-signed certificates. Do not use in
// production environments.
//
// Only available for the TCP sender.
//
// For the HTTP sender, use WithHttpTransport.
func WithTlsInsecureSkipVerify() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.tcpTlsMode = tlsInsecureSkipVerify
	}
}

// WithHttpTransport sets the client's http transport to the
// passed pointer instead of the global transport. This can be
// used for customizing the http transport used by the HttpLineSender.
//
// Only available for the HTTP sender.
func WithHttpTransport(t *http.Transport) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.httpTransport = t
	}
}

// WithAutoFlushDisabled turns off auto-flushing behavior.
// To send ILP messages, the user must call Flush().
//
// Only available for the HTTP sender.
func WithAutoFlushDisabled() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.autoFlushRows = 0
		s.autoFlushInterval = 0
	}
}

// WithAutoFlushRows sets the number of buffered rows that
// must be breached in order to trigger an auto-flush.
// Defaults to 75000.
//
// Only available for the HTTP sender.
func WithAutoFlushRows(rows int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.autoFlushRows = rows
	}
}

// WithAutoFlushInterval the interval at which the Sender
// automatically flushes its buffer. Defaults to 1 second.
//
// Only available for the HTTP sender.
func WithAutoFlushInterval(interval time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.autoFlushInterval = interval
	}
}
