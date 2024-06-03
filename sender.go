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
	"net/http"
	"os"
	"strings"
	"time"
)

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages over HTTP or TCP protocol.
//
// Each sender corresponds to a single client-server connection.
// A sender should not be called concurrently by multiple goroutines.
//
// HTTP senders reuse connections from a global pool by default. You can
// customize the HTTP transport by passing a custom http.Transport to the
// WithHttpTransport option.
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

const (
	defaultHttpAddress = "127.0.0.1:9000"
	defaultTcpAddress  = "127.0.0.1:9009"

	defaultInitBufferSize = 128 * 1024        // 128KB
	defaultMaxBufferSize  = 100 * 1024 * 1024 // 100MB
	defaultFileNameLimit  = 127

	defaultAutoFlushRows     = 75000
	defaultAutoFlushInterval = time.Second

	defaultMinThroughput  = 100 * 1024 // 100KB/s
	defaultRetryTimeout   = 10 * time.Second
	defaultRequestTimeout = 10 * time.Second
)

type senderType int64

const (
	noSenderType   senderType = 0
	httpSenderType senderType = 1
	tcpSenderType  senderType = 2
)

type tlsMode int64

const (
	tlsDisabled           tlsMode = 0
	tlsEnabled            tlsMode = 1
	tlsInsecureSkipVerify tlsMode = 2
)

type lineSenderConfig struct {
	senderType    senderType
	address       string
	initBufSize   int
	maxBufSize    int
	fileNameLimit int
	httpTransport *http.Transport

	// Retry/timeout-related fields
	retryTimeout   time.Duration
	minThroughput  int
	requestTimeout time.Duration

	// Authentication-related fields
	tlsMode   tlsMode
	tcpKeyId  string
	tcpKey    string
	httpUser  string
	httpPass  string
	httpToken string

	// Auto-flush fields
	autoFlushRows     int
	autoFlushInterval time.Duration
}

// LineSenderOption defines line sender config option.
type LineSenderOption func(*lineSenderConfig)

// WithHttp enables ingestion over HTTP protocol.
func WithHttp() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.senderType = httpSenderType
	}
}

// WithTcp enables ingestion over TCP protocol.
func WithTcp() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.senderType = tcpSenderType
	}
}

// WithTls enables TLS connection encryption.
func WithTls() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.tlsMode = tlsEnabled
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
		s.minThroughput = bytesPerSecond
	}
}

// WithRetryTimeout is the cumulative maximum duration spend in
// retries. Defaults to 10 seconds. Retries work great when
// used in combination with server-side data deduplication.
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
		s.initBufSize = sizeInBytes
	}
}

// WithMaxBufferSize sets the maximum buffer capacity
// in bytes to be used when sending ILP messages. The sender will
// return an error if the limit is reached. Defaults to 100MB.
//
// Only available for the HTTP sender.
func WithMaxBufferSize(sizeInBytes int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.maxBufSize = sizeInBytes
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
func WithTlsInsecureSkipVerify() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.tlsMode = tlsInsecureSkipVerify
	}
}

// WithHttpTransport sets the client's http transport to the
// passed pointer instead of the global transport. This can be
// used for customizing the http transport used by the LineSender.
// For example to set custom timeouts, TLS settings, etc.
// WithTlsInsecureSkipVerify is ignored when this option is in use.
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

// LineSenderFromEnv creates a LineSender with a config string defined by the QDB_CLIENT_CONF
// environment variable. See LineSenderFromConf for the config string format.
//
// This is a convenience method suitable for Cloud environments.
func LineSenderFromEnv(ctx context.Context) (LineSender, error) {
	conf := strings.TrimSpace(os.Getenv("QDB_CLIENT_CONF"))
	if conf == "" {
		return nil, errors.New("QDB_CLIENT_CONF environment variable is not set")
	}
	c, err := confFromStr(conf)
	if err != nil {
		return nil, err
	}
	return newLineSender(ctx, c)
}

// LineSenderFromConf creates a LineSender using the QuestDB config string format.
//
// Example config string: "http::addr=localhost;username=joe;password=123;auto_flush_rows=1000;"
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
// Options:
// http(s) and tcp(s):
// -------------------
// addr:           hostname/port of QuestDB endpoint
// init_buf_size:  initial growable ILP buffer size in bytes (defaults to 128KiB)
// tls_verify:     determines if TLS certificates should be validated (defaults to "on", can be set to "unsafe_off")
//
// http(s)-only
// ------------
// username:               for basic authentication
// password:               for basic authentication
// token:                  bearer token auth (used instead of basic authentication)
// auto_flush:             determines if auto-flushing is enabled (values "on" or "off", defaults to "on")
// auto_flush_rows:        auto-flushing is triggered above this row count (defaults to 75000). If set, explicitly implies auto_flush=on
// request_min_throughput: bytes per second, used to calculate each request's timeout (defaults to 100KiB/s)
// request_timeout:        minimum request timeout in milliseconds (defaults to 10 seconds)
// retry_timeout:          cumulative maximum millisecond duration spent in retries (defaults to 10 seconds)
// max_buf_size:           buffer growth limit in bytes. Client errors if breached (default is 100MiB)
//
// tcp(s)-only
// -----------
// username:  KID (key ID) for ECDSA authentication
// token:     Secret K (D) for ECDSA authentication
func LineSenderFromConf(ctx context.Context, conf string) (LineSender, error) {
	c, err := confFromStr(conf)
	if err != nil {
		return nil, err
	}
	return newLineSender(ctx, c)
}

// NewLineSender creates new InfluxDB Line Protocol (ILP) sender. Each
// sender corresponds to a single client connection. LineSender should
// not be called concurrently by multiple goroutines.
func NewLineSender(ctx context.Context, opts ...LineSenderOption) (LineSender, error) {
	conf := &lineSenderConfig{}
	for _, opt := range opts {
		opt(conf)
	}
	return newLineSender(ctx, conf)
}

func newLineSender(ctx context.Context, conf *lineSenderConfig) (LineSender, error) {
	switch conf.senderType {
	case tcpSenderType:
		err := sanitizeTcpConf(conf)
		if err != nil {
			return nil, err
		}
		return newTcpLineSender(ctx, conf)
	case httpSenderType:
		err := sanitizeHttpConf(conf)
		if err != nil {
			return nil, err
		}
		return newHttpLineSender(conf)
	}
	return nil, errors.New("sender type is not specified: use WithHttp or WithTcp")
}

func sanitizeTcpConf(conf *lineSenderConfig) error {
	err := validateConf(conf)
	if err != nil {
		return err
	}

	// validate tcp-specific settings
	if conf.requestTimeout != 0 {
		return errors.New("requestTimeout setting is not available in the TCP client")
	}
	if conf.retryTimeout != 0 {
		return errors.New("retryTimeout setting is not available in the TCP client")
	}
	if conf.minThroughput != 0 {
		return errors.New("minThroughput setting is not available in the TCP client")
	}
	if conf.autoFlushRows != 0 {
		return errors.New("autoFlushRows setting is not available in the TCP client")
	}
	if conf.autoFlushInterval != 0 {
		return errors.New("autoFlushInterval setting is not available in the TCP client")
	}
	if conf.maxBufSize != 0 {
		return errors.New("maxBufferSize setting is not available in the TCP client")
	}
	if conf.tcpKey == "" && conf.tcpKeyId != "" {
		return errors.New("tcpKey is empty and tcpKeyId is not. both (or none) must be provided")
	}
	if conf.tcpKeyId == "" && conf.tcpKey != "" {
		return errors.New("tcpKeyId is empty and tcpKey is not. both (or none) must be provided")
	}

	// Set defaults
	if conf.address == "" {
		conf.address = defaultTcpAddress
	}
	if conf.initBufSize == 0 {
		conf.initBufSize = defaultInitBufferSize
	}
	if conf.fileNameLimit == 0 {
		conf.fileNameLimit = defaultFileNameLimit
	}

	return nil
}

func sanitizeHttpConf(conf *lineSenderConfig) error {
	err := validateConf(conf)
	if err != nil {
		return err
	}

	// validate http-specific settings
	if (conf.httpUser != "" || conf.httpPass != "") && conf.httpToken != "" {
		return errors.New("both basic and token authentication cannot be used")
	}

	// Set defaults
	if conf.address == "" {
		conf.address = defaultHttpAddress
	}
	if conf.requestTimeout == 0 {
		conf.requestTimeout = defaultRequestTimeout
	}
	if conf.retryTimeout == 0 {
		conf.retryTimeout = defaultRetryTimeout
	}
	if conf.minThroughput == 0 {
		conf.minThroughput = defaultMinThroughput
	}
	if conf.autoFlushRows == 0 {
		conf.autoFlushRows = defaultAutoFlushRows
	}
	if conf.autoFlushInterval == 0 {
		conf.autoFlushInterval = defaultAutoFlushInterval
	}
	if conf.initBufSize == 0 {
		conf.initBufSize = defaultInitBufferSize
	}
	if conf.maxBufSize == 0 {
		conf.maxBufSize = defaultMaxBufferSize
	}
	if conf.fileNameLimit == 0 {
		conf.fileNameLimit = defaultFileNameLimit
	}

	return nil
}

func validateConf(conf *lineSenderConfig) error {
	if conf.initBufSize < 0 {
		return fmt.Errorf("initial buffer size is negative: %d", conf.initBufSize)
	}
	if conf.maxBufSize < 0 {
		return fmt.Errorf("max buffer size is negative: %d", conf.maxBufSize)
	}

	if conf.fileNameLimit < 0 {
		return fmt.Errorf("file name limit is negative: %d", conf.fileNameLimit)
	}

	if conf.retryTimeout < 0 {
		return fmt.Errorf("retry timeout is negative: %d", conf.retryTimeout)
	}
	if conf.requestTimeout < 0 {
		return fmt.Errorf("request timeout is negative: %d", conf.requestTimeout)
	}
	if conf.minThroughput < 0 {
		return fmt.Errorf("min throughput is negative: %d", conf.minThroughput)
	}

	if conf.autoFlushRows < 0 {
		return fmt.Errorf("auto flush rows is negative: %d", conf.autoFlushRows)
	}
	if conf.autoFlushInterval < 0 {
		return fmt.Errorf("auto flush interval is negative: %d", conf.autoFlushInterval)
	}

	return nil
}
