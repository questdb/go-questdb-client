/*+*****************************************************************************
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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	errClosedSenderFlush       = errors.New("cannot flush a closed LineSender")
	errFlushWithPendingMessage = errors.New("pending ILP message must be finalized with At or AtNow before calling Flush")
	errClosedSenderAt          = errors.New("cannot queue new messages on a closed LineSender")
	errDoubleSenderClose       = errors.New("double sender close")
	errDecimalNotSupported     = errors.New("current protocol version does not support decimal")
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
	// Should be used only for non-designated timestamp column.
	// Designated timestamp column values should be passed to At/AtNow.
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

	// DecimalColumnFromString adds a decimal column value to the ILP message.
	//
	// Serializes the decimal value using the text representation.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	DecimalColumnFromString(name string, val string) LineSender

	// DecimalColumn adds a decimal column value to the ILP message.
	//
	// Serializes the decimal value using the binary representation.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	DecimalColumn(name string, val Decimal) LineSender

	// DecimalColumnShopspring adds a decimal column value to the ILP message.
	//
	// Serializes the decimal value using the binary representation.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	DecimalColumnShopspring(name string, val ShopspringDecimal) LineSender

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

	// Float64Array1DColumn adds an array of 64-bit floats (double array) to the ILP message.
	//
	// A nil values slice yields a NULL array; a non-nil empty slice yields
	// a distinct, non-null empty array (cardinality 0).
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', "', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Float64Array1DColumn(name string, values []float64) LineSender

	// Float64Array2DColumn adds a 2D array of 64-bit floats (double 2D array) to the ILP message.
	//
	// A nil values slice yields a NULL array; a non-nil empty slice yields
	// a distinct, non-null empty array (cardinality 0).
	//
	// The values parameter must have a regular (rectangular) shape - all rows must have
	// exactly the same length. If the array has irregular shape, this method returns an error.
	//
	// Example of valid input:
	//   values := [][]float64{{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}}  // 3x2 regular shape
	//
	// Example of invalid input:
	//   values := [][]float64{{1.0, 2.0}, {3.0}, {4.0, 5.0, 6.0}}   // irregular shape - returns error
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', "', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Float64Array2DColumn(name string, values [][]float64) LineSender

	// Float64Array3DColumn adds a 3D array of 64-bit floats (double 3D array) to the ILP message.
	//
	// A nil values slice yields a NULL array; a non-nil empty slice yields
	// a distinct, non-null empty array (cardinality 0).
	//
	// The values parameter must have a regular (cuboid) shape - all dimensions must have
	// consistent sizes throughout. If the array has irregular shape, this method returns an error.
	//
	// Example of valid input:
	//   values := [][][]float64{
	//     {{1.0, 2.0}, {3.0, 4.0}},    // 2x2 matrix
	//     {{5.0, 6.0}, {7.0, 8.0}},    // 2x2 matrix (same shape)
	//   }  // 2x2x2 regular shape
	//
	// Example of invalid input:
	//   values := [][][]float64{
	//     {{1.0, 2.0}, {3.0, 4.0}},    // 2x2 matrix
	//     {{5.0}, {6.0, 7.0, 8.0}},    // irregular matrix - returns error
	//   }
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', "', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Float64Array3DColumn(name string, values [][][]float64) LineSender

	// Float64ArrayNDColumn adds an n-dimensional array of 64-bit floats (double n-D array) to the ILP message.
	//
	// A nil value yields a NULL array; a non-nil array with a zero-length
	// dimension yields a distinct, non-null empty array (cardinality 0).
	//
	// Example usage:
	//   // Create a 2x3x4 array
	//   arr, _ := questdb.NewNDArray[float64](2, 3, 4)
	//   arr.Fill(1.5)
	//   sender.Float64ArrayNDColumn("ndarray_col", arr)
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', "', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Float64ArrayNDColumn(name string, values *NdArray[float64]) LineSender

	// At sets the designated timestamp value and finalizes the ILP
	// message.
	//
	// If the underlying buffer reaches configured capacity or the
	// number of buffered messages exceeds the auto-flush trigger, this
	// method also sends the accumulated messages.
	//
	// If ts.IsZero(), no timestamp is sent to the server.
	At(ctx context.Context, ts time.Time) error

	// AtNow omits designated timestamp value and finalizes the ILP
	// message. The server will insert each message using the system
	// clock as the row timestamp.
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
	qwpSenderType  senderType = 3
)

type tlsMode int64

const (
	tlsDisabled           tlsMode = 0
	tlsEnabled            tlsMode = 1
	tlsInsecureSkipVerify tlsMode = 2
)

type protocolVersion int64

const (
	protocolVersionUnset protocolVersion = 0
	ProtocolVersion1     protocolVersion = 1
	ProtocolVersion2     protocolVersion = 2
	ProtocolVersion3     protocolVersion = 3
)

// InitialConnectMode controls how the QWP sender treats failures of
// its very first connect attempt. Mirrors the Java client's
// `initial_connect_retry` enum.
type InitialConnectMode byte

const (
	// InitialConnectOff (the default) makes any failure on the first
	// connect terminal — typically a misconfig, retrying just hides
	// it. The constructor surfaces the dial error directly.
	InitialConnectOff InitialConnectMode = iota
	// InitialConnectSync runs the same retry-with-backoff loop as
	// reconnect on the calling goroutine, blocking the constructor
	// until either the connection comes up or the reconnect budget
	// (reconnect_max_duration_millis) is exhausted. Auth/upgrade
	// failures stay terminal.
	InitialConnectSync
	// InitialConnectAsync defers the dial to the I/O goroutine and
	// returns from the constructor immediately with an unconnected
	// sender. The producer goroutine can call Table()/At()/Flush()
	// right away; rows accumulate in the cursor SF engine until the
	// connection comes up. Connect-budget exhaustion or terminal
	// upgrade failure is delivered through the configured
	// SenderErrorHandler (and surfaced from any subsequent producer
	// API call as a typed error).
	InitialConnectAsync
)

type lineSenderConfig struct {
	senderType    senderType
	address       string
	initBufSize   int
	maxBufSize    int
	fileNameLimit int
	httpTransport *http.Transport

	// Multi-host failover (failover.md §1 / §2). For QWP, sanitizeQwpConf
	// populates endpoints from address (which may be a comma-joined
	// list); downstream consumers walk endpoints rather than address.
	// Non-QWP transports leave endpoints nil and continue using address
	// directly — sanitizeHttp/sanitizeTcp reject comma-form addr at
	// validation time since neither transport supports multi-host yet.
	endpoints     []qwpEndpoint
	authTimeoutMs int             // QWP-only; 0 -> 15000 (15s) at sanitize time
	zone          string          // QWP-only; honoured on egress, inert on ingest (no zone routing)
	target        QwpTargetFilter // QWP-only; zero value = QwpTargetAny

	// connectTimeoutMs bounds the TCP connect (ms). COMMON key: parsed on
	// every schema for Java connect-string parity, but wired only on HTTP and
	// QWP — the TCP ILP dial leaves it inert, matching the Java client. 0 keeps
	// the OS connect timeout.
	connectTimeoutMs int

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
	autoFlushBytes    int // QWP-only; 0 disables the byte-size trigger
	// autoFlushBytesSet records whether the user explicitly set
	// auto_flush_bytes (vs. the seeded qwpDefaultAutoFlushBytes).
	// sanitizeQwpConf uses it to reject only a user-written
	// auto_flush_bytes > sf_max_bytes contradiction; a defaulted trigger
	// over a smaller user-chosen segment is left for the runtime clamp.
	autoFlushBytesSet bool

	protocolVersion protocolVersion

	// QWP-specific fields
	inFlightWindow  int       // retained for config compatibility; a no-op in the cursor architecture (see WithInFlightWindow). Seeded to qwpDefaultInFlightWindow by newLineSenderConfig
	dumpWriter      io.Writer // if set, record outgoing bytes (unexported)
	gorillaDisabled bool      // false (default) = Gorilla timestamp encoding enabled

	// QWP store-and-forward (cursor) fields. Setting sfDir selects
	// disk-backed segments: flushed batches are persisted to mmap'd
	// files under <sfDir>/<senderId>/ and the send loop replays from
	// disk on reconnect / restart. When sfDir is empty, segments are
	// memory-backed; both modes run on the same cursor engine + send
	// loop.
	sfDir                         string
	senderId                      string // empty -> "default" at construction
	sfMaxBytes                    int64  // per-segment size (bytes); 0 -> 4 MiB
	sfMaxTotalBytes               int64  // total cap (bytes); 0 -> 10 GiB
	sfDurability                  string // empty / "memory" only; reserved future "flush" / "append"
	sfAppendDeadlineMillis        int    // 0 -> 30000
	reconnectMaxDurationMillis    int    // 0 -> 300000 (5 min)
	reconnectInitialBackoffMillis int    // 0 -> 100
	reconnectMaxBackoffMillis     int    // 0 -> 5000
	// Per-key explicit-set flags for the three reconnect_* knobs.
	// Used by sanitizeQwpConf to implement the implicit promotion of
	// initial_connect_retry to "on" when the user tuned any reconnect
	// budget without choosing a connect mode (matches Java's behaviour
	// — see Sender.java's actualInitialConnectMode resolution).
	reconnectMaxDurationMillisSet    bool
	reconnectInitialBackoffMillisSet bool
	reconnectMaxBackoffMillisSet     bool
	initialConnectMode               InitialConnectMode // default InitialConnectOff
	initialConnectModeSet            bool               // true if user explicitly chose a mode (gates the reconnect_*-driven promotion)
	closeFlushTimeoutMillis          int                // 0 -> 5000; -1 / negative -> fast close (skip drain)
	closeFlushTimeoutSet             bool               // true if user explicitly set the value (so 0 means "fast close" rather than "use default")
	drainOrphans                     bool               // default false (Phase 6)
	maxBackgroundDrainers            int                // 0 -> 4 (Phase 6)
	// orphanDrainExclude, when non-nil, fences additional slot names from
	// orphan adoption beyond the sender's own slot. The QwpSender pool sets it
	// to its in-range slot set so a pooled sender never adopts a live sibling's
	// dir (Hazard G). nil for standalone senders. Internal; no connect-string key.
	orphanDrainExclude func(slotName string) bool

	// QWP server-error API (Phase 5). All fields are QWP-only.
	errorHandler         SenderErrorHandler    // nil -> default loud handler
	errorPolicyResolver  func(Category) Policy // nil -> per-category map / global / spec defaults
	errorPolicyPerCat    [numCategories]Policy // PolicyAuto = unset; cleared at construction
	errorPolicyPerCatSet bool                  // tracks whether *any* per-category override was set
	errorPolicyGlobal    Policy                // PolicyAuto = unset
	errorInboxCapacity   int                   // 0 -> qwpSfDefaultErrorInboxCapacity; sanitizer floors at qwpSfMinErrorInboxCapacity

	// QWP connection-event listener (all fields QWP-only).
	connectionListener              SenderConnectionListener // nil -> default loud listener
	connectionListenerInboxCapacity int                      // 0 -> qwpSfDefaultErrorInboxCapacity
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

// WithQwp enables ingestion over the QWP WebSocket protocol.
func WithQwp() LineSenderOption {
	return func(s *lineSenderConfig) {
		s.senderType = qwpSenderType
	}
}

// WithInFlightWindow is retained for backward compatibility but is a
// no-op. In the QWP cursor architecture, backpressure is governed by
// the engine's segment ring and the append deadline, not by a fixed
// in-flight batch count. Flush never waits for the server ACK, so
// there is no synchronous mode to opt into. Connect strings carrying
// in_flight_window still parse; the value is ignored.
//
// Only available for the QWP sender.
//
// Deprecated: the in-flight window has no effect and there is no
// replacement — backpressure is automatic. To confirm server ACKs,
// pair FlushAndGetSequence with AwaitAckedFsn.
func WithInFlightWindow(window int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.inFlightWindow = window
	}
}

// WithCloseTimeout sets the time Close() waits for the I/O goroutine
// to finish draining published batches to the server before
// force-cancelling. Defaults to 5 seconds. Because Flush() never waits
// for the server ACK, this close-time drain — not Flush() — is the
// sender's last chance to get buffered data confirmed; rows still
// unacked when the timeout expires may be lost (memory mode) or left
// on disk for replay (store-and-forward).
//
// Deprecated: use WithCloseFlushTimeout instead. WithCloseTimeout is
// preserved as an alias so v4.0–v4.5 code keeps compiling — it
// routes through the same close_flush_timeout_millis path the spec
// (connect-string.md §Ingress reconnect) defines. d <= 0 is treated
// as "no override" (default 5s) to match the legacy semantics; to
// skip the drain entirely, use WithCloseFlushTimeout, where 0 /
// negative means "fast close".
func WithCloseTimeout(d time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		if d >= time.Millisecond {
			s.closeFlushTimeoutSet = true
			s.closeFlushTimeoutMillis = int(d / time.Millisecond)
		}
	}
}

// WithErrorHandler registers a callback invoked asynchronously when
// the SF send loop observes a server-side batch rejection. The
// handler runs on a dedicated dispatcher goroutine; slow handlers
// cannot stall publishing. If the bounded inbox fills up, surplus
// notifications are dropped (visible via
// QwpSender.DroppedErrorNotifications()).
//
// Passing nil reverts to the default loud-not-silent handler that
// logs ERROR for HALT and WARN for DROP.
//
// The handler may call Close() or Flush() on the sender (e.g. to shut
// down on a HALT) without deadlocking — see SenderErrorHandler for the
// re-entrancy contract.
//
// Only available for the QWP sender.
func WithErrorHandler(h SenderErrorHandler) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.errorHandler = h
	}
}

// WithConnectionListener registers a callback invoked asynchronously when the
// QWP sender observes a connection-state transition (connect, disconnect,
// reconnect, failover, terminal auth/budget failure). The listener runs on a
// dedicated dispatcher goroutine; a slow listener cannot stall publishing, and
// surplus events are dropped when the bounded inbox fills (visible via
// QwpSender.DroppedConnectionNotifications()). Passing nil reverts to the
// default loud listener. See SenderConnectionListener for the contract.
//
// Only available for the QWP sender.
func WithConnectionListener(l SenderConnectionListener) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.connectionListener = l
	}
}

// WithConnectionListenerInboxCapacity sets the bounded inbox depth for the
// connection-event dispatcher. Zero uses the default (256); any other value
// outside [16, 1<<20] is rejected at construction. Equivalent to the
// connect-string connection_listener_inbox_capacity key.
//
// Only available for the QWP sender.
func WithConnectionListenerInboxCapacity(capacity int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.connectionListenerInboxCapacity = capacity
	}
}

// WithErrorPolicy sets the Policy applied for one Category. Per-
// category overrides take precedence over the connect-string global
// on_server_error and the spec defaults; a programmatic resolver
// registered via WithErrorPolicyResolver still wins over both.
//
// PolicyAuto removes any prior override (falls through to next
// layer). CategoryProtocolViolation and CategoryUnknown are always
// HALT: an override for either is ignored and not recorded, matching
// the connect-string form, which has no on_protocol_violation_error /
// on_unknown_error key and rejects those outright.
//
// Only available for the QWP sender.
func WithErrorPolicy(c Category, p Policy) LineSenderOption {
	return func(s *lineSenderConfig) {
		// PROTOCOL_VIOLATION and UNKNOWN are never user-configurable.
		// Refusing the override here keeps the per-category slot from
		// ever holding a latent non-HALT policy, so the forced HALT does
		// not depend on resolve() checking these two categories first.
		if int(c) >= len(s.errorPolicyPerCat) ||
			c == CategoryProtocolViolation || c == CategoryUnknown {
			return
		}
		s.errorPolicyPerCat[c] = p
		s.errorPolicyPerCatSet = false
		for _, q := range s.errorPolicyPerCat {
			if q != PolicyAuto {
				s.errorPolicyPerCatSet = true
				break
			}
		}
	}
}

// WithErrorPolicyResolver registers a programmatic resolver invoked
// for every Category before any per-category map or global default.
// Returning PolicyAuto from the resolver falls through to the next
// layer (per-category map, then global, then spec default).
//
// CategoryProtocolViolation and CategoryUnknown are forced HALT and
// bypass the resolver entirely.
//
// Only available for the QWP sender.
func WithErrorPolicyResolver(r func(Category) Policy) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.errorPolicyResolver = r
	}
}

// WithErrorInboxCapacity sets the size of the bounded inbox between
// the I/O goroutine and the dispatcher goroutine. Larger values
// tolerate slower handlers at the cost of memory; smaller values
// surface backpressure (drop counter) sooner. Defaults to 256;
// minimum is 16 (sanitized at construction).
//
// Only available for the QWP sender.
func WithErrorInboxCapacity(n int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.errorInboxCapacity = n
	}
}

// WithSfDir activates the store-and-forward cursor path against
// the given group root. The sender's slot lives at
// `<sfDir>/<senderId>/`; flushed batches are persisted there and
// replayed on reconnect / restart. Setting an empty string is a
// no-op (memory mode).
//
// Only available for the QWP sender.
func WithSfDir(dir string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.sfDir = dir
	}
}

// WithSenderId sets the sub-directory name under sfDir that
// uniquely identifies this sender's slot. Defaults to "default";
// multi-sender deployments must set distinct IDs to avoid lock
// collisions on the same slot. Only meaningful when sf_dir is set.
//
// Only available for the QWP sender.
func WithSenderId(id string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.senderId = id
	}
}

// WithSfMaxBytes sets the per-segment cap (bytes) for the cursor
// engine. Defaults to 4 MiB. Lower values rotate segments more
// aggressively; higher values amortize the rotation overhead.
//
// Only available for the QWP sender.
func WithSfMaxBytes(n int64) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.sfMaxBytes = n
	}
}

// WithSfMaxTotalBytes caps the total cursor allocation (active +
// hot spare + sealed segments) for this sender. The producer is
// backpressured when an append would exceed the cap. Defaults to
// 10 GiB.
//
// Only available for the QWP sender.
func WithSfMaxTotalBytes(n int64) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.sfMaxTotalBytes = n
	}
}

// WithReconnectPolicy configures the per-outage reconnect cap and
// backoff policy. maxDuration bounds the total time spent
// reconnecting before the loop gives up; initialBackoff and
// maxBackoff bound a backoff sleep between attempts (with jitter).
// A zero or negative argument is treated as "leave the default" for
// that knob — it does not register as an explicit user choice and so
// does not trigger the initial_connect_retry promotion.
//
// Only available for the QWP sender.
func WithReconnectPolicy(maxDuration, initialBackoff, maxBackoff time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		if maxDuration > 0 {
			s.reconnectMaxDurationMillis = int(maxDuration / time.Millisecond)
			s.reconnectMaxDurationMillisSet = true
		}
		if initialBackoff > 0 {
			s.reconnectInitialBackoffMillis = int(initialBackoff / time.Millisecond)
			s.reconnectInitialBackoffMillisSet = true
		}
		if maxBackoff > 0 {
			s.reconnectMaxBackoffMillis = int(maxBackoff / time.Millisecond)
			s.reconnectMaxBackoffMillisSet = true
		}
	}
}

// WithInitialConnectRetry, when true, applies the same
// retry-with-backoff policy to the initial connect attempt as is
// applied on reconnect. By default an initial connect failure is
// terminal — useful for catching misconfig early.
//
// Equivalent to WithInitialConnectMode(InitialConnectSync) when
// retry is true, or WithInitialConnectMode(InitialConnectOff) when
// retry is false. Use WithInitialConnectMode directly to select
// InitialConnectAsync.
//
// Only available for the QWP sender.
func WithInitialConnectRetry(retry bool) LineSenderOption {
	return func(s *lineSenderConfig) {
		if retry {
			s.initialConnectMode = InitialConnectSync
		} else {
			s.initialConnectMode = InitialConnectOff
		}
		s.initialConnectModeSet = true
	}
}

// WithInitialConnectMode configures whether the QWP sender's first
// connection attempt may retry on failure, and if so whether the
// retry runs synchronously on the calling thread or asynchronously
// on the I/O goroutine. See InitialConnectMode for value semantics.
//
// Only available for the QWP sender.
func WithInitialConnectMode(mode InitialConnectMode) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.initialConnectMode = mode
		s.initialConnectModeSet = true
	}
}

// WithCloseFlushTimeout bounds Close()'s wait for the cursor
// engine's ackedFsn to catch up to publishedFsn. A zero or
// negative duration skips the drain entirely (fast close).
// Defaults to 5 seconds.
//
// Only meaningful for the QWP sender in cursor mode (sf_dir set).
func WithCloseFlushTimeout(d time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.closeFlushTimeoutSet = true
		s.closeFlushTimeoutMillis = int(d / time.Millisecond)
	}
}

// WithGorilla enables or disables Gorilla delta-of-delta encoding for
// timestamp columns. Defaults to enabled. When disabled, FLAG_GORILLA
// is cleared on every message and timestamp columns are sent as raw
// int64 little-endian values with no encoding-flag prefix.
//
// Mirrors QwpWebSocketSender.setGorillaEnabled in the Java client
// (default true there as well). Only available for the QWP sender.
func WithGorilla(enabled bool) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.gorillaDisabled = !enabled
	}
}

// WithQwpDumpWriter returns an option that records all outgoing TCP
// bytes to w. When no server address is configured, an in-process
// fake WebSocket acceptor is used so the dump includes the full HTTP
// upgrade and WebSocket framing — replayable via "cat dump.bin | nc
// host port".
//
// Only available for the QWP sender.
func WithQwpDumpWriter(w io.Writer) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.dumpWriter = w
	}
}

// WithAuthTimeout bounds how long the QWP transport waits for the
// HTTP-upgrade response (the per-host upper bound from failover.md
// §7). A zero or negative duration falls back to the 15s default at
// construction. Equivalent to the connect-string auth_timeout_ms key.
//
// Only available for the QWP sender.
func WithAuthTimeout(d time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.authTimeoutMs = int(d / time.Millisecond)
	}
}

// WithConnectTimeout bounds the TCP connect to a QuestDB endpoint, so a
// black-holed or firewalled host is abandoned within d instead of riding the
// much longer OS connect timeout. It clamps only the TCP connect; the TLS
// handshake and the QWP upgrade response stay under WithAuthTimeout. A zero or
// negative duration keeps the OS connect timeout. Honoured by the HTTP and QWP
// senders; accepted but inert on TCP, and ignored when a custom http.Transport
// is supplied (that transport is the caller's to configure). Equivalent to the
// connect-string connect_timeout key.
func WithConnectTimeout(d time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		ms := int(d / time.Millisecond)
		// A positive sub-millisecond budget must not truncate to 0, which means
		// "keep the OS default" — floor it to 1ms so a tight budget stays tight.
		if d > 0 && ms == 0 {
			ms = 1
		}
		s.connectTimeoutMs = ms
	}
}

// WithZone sets the failover zone hint used for endpoint locality.
// It is silently stored but inert on the ingestion path, which is
// zone-blind — it never receives SERVER_INFO. The egress (query) path
// consults it to prefer same-zone endpoints. Equivalent to the
// connect-string zone key.
//
// Only available for the QWP sender.
func WithZone(zone string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.zone = zone
	}
}

// WithTarget constrains failover endpoint selection to servers whose
// advertised role passes the filter (QwpTargetAny / QwpTargetPrimary
// / QwpTargetReplica). Defaults to QwpTargetAny. Equivalent to the
// connect-string target=any|primary|replica key.
//
// The filter is honoured on the query (egress) path, which reads the
// server's role from the SERVER_INFO frame. The ingestion path never
// receives SERVER_INFO (it is role-blind by the wire-protocol spec),
// so the value is accepted but inert there — the server's own role
// reject keeps writes off replicas. Symmetric with WithZone.
//
// Only available for the QWP sender.
func WithTarget(target QwpTargetFilter) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.target = target
	}
}

// WithSfDurability selects the store-and-forward cursor durability
// mode. Only "memory" (the default when unset) is currently honoured;
// "flush" and "append" are reserved for a deferred follow-up and are
// rejected at construction. Requires sf_dir to be set. Equivalent to
// the connect-string sf_durability key.
//
// Only available for the QWP sender.
func WithSfDurability(mode string) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.sfDurability = mode
	}
}

// WithSfAppendDeadline bounds how long a producer call blocks waiting
// to append a batch into the store-and-forward cursor engine before
// it returns a backpressure error that wraps ErrBackpressureTimeout
// (match with errors.Is). A zero or negative duration falls back to
// the 30s default at construction. Requires sf_dir to be set.
// Equivalent to the connect-string sf_append_deadline_millis key.
//
// Only available for the QWP sender.
func WithSfAppendDeadline(d time.Duration) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.sfAppendDeadlineMillis = int(d / time.Millisecond)
	}
}

// WithDrainOrphans enables adoption and draining of orphaned
// store-and-forward slots left behind by a crashed or superseded
// sender sharing the same sf_dir group root. Defaults to disabled.
// Requires sf_dir to be set. Equivalent to the connect-string
// drain_orphans key.
//
// Only available for the QWP sender.
func WithDrainOrphans(enabled bool) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.drainOrphans = enabled
	}
}

// WithMaxBackgroundDrainers caps the number of concurrent
// orphan-drainer goroutines. Defaults to 4. Only meaningful when
// drain_orphans is enabled. Equivalent to the connect-string
// max_background_drainers key.
//
// Only available for the QWP sender.
func WithMaxBackgroundDrainers(n int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.maxBackgroundDrainers = n
	}
}

// WithServerErrorPolicy sets the global fallback Policy applied to a
// server-side batch rejection when no higher-precedence layer
// resolves it. Resolution precedence (highest first): the
// WithErrorPolicyResolver resolver → the WithErrorPolicy per-category
// override → the connect-string per-category on_*_error → this global
// policy (connect-string on_server_error) → spec defaults.
//
// PolicyAuto (the zero value) leaves the global layer unset, falling
// through to the spec defaults. CategoryProtocolViolation and
// CategoryUnknown are always HALT regardless of this setting.
//
// Only available for the QWP sender.
func WithServerErrorPolicy(p Policy) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.errorPolicyGlobal = p
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

// WithRequestTimeout is used in combination with request_min_throughput
// to set the timeout of an ILP request. Defaults to 10 seconds.
//
// timeout = (request.len() / request_min_throughput) + request_timeout
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
// timeout = (request.len() / request_min_throughput) + request_timeout
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
// as on the server. Must be at least 16. Defaults to 127.
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

// WithAutoFlushBytes triggers an auto-flush once the cumulative size
// of all buffered table data exceeds the given byte threshold. A value
// of 0 disables the byte-based trigger. Defaults to 0.
//
// Only available for the QWP sender. Distinct from WithMaxBufferSize:
// this is a soft trigger, while WithMaxBufferSize is a hard cap.
func WithAutoFlushBytes(bytes int) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.autoFlushBytes = bytes
		s.autoFlushBytesSet = true
	}
}

// WithProtocolVersion sets the ingestion protocol version.
//
//   - HTTP transport automatically negotiates the protocol version by default(unset, STRONGLY RECOMMENDED).
//     You can explicitly configure the protocol version to avoid the slight latency cost at connection time.
//   - TCP transport does not negotiate the protocol version and uses [ProtocolVersion1] by
//     default. You must explicitly set [ProtocolVersion2] in order to ingest
//     arrays.
//   - [ProtocolVersion3] enables decimal binary encoding (ILP v3).
//
// NOTE: QuestDB server version 9.0.0 or later is required for [ProtocolVersion2].
// For [ProtocolVersion3], make sure the server advertises ILP v3 support via /settings.
func WithProtocolVersion(version protocolVersion) LineSenderOption {
	return func(s *lineSenderConfig) {
		s.protocolVersion = version
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
// Example config string: "http::addr=localhost;username=admin;password=quest;auto_flush_rows=1000;"
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
//
// -------------------
//
// addr:           hostname/port of QuestDB endpoint
//
// init_buf_size:  initial growable ILP buffer size in bytes (defaults to 128KiB)
//
// tls_verify:     determines if TLS certificates should be validated (defaults to "on", can be set to "unsafe_off")
//
// http(s)-only
//
// ------------
//
// username:               for basic authentication
//
// password:               for basic authentication
//
// token:                  bearer token auth (used instead of basic authentication)
//
// auto_flush:             determines if auto-flushing is enabled (values "on" or "off", defaults to "on")
//
// auto_flush_rows:        auto-flushing is triggered above this row count (defaults to 75000). If set, explicitly implies auto_flush=on. Set to 'off' to disable.
//
// auto_flush_interval:    auto-flushing is triggered above this time, in milliseconds (defaults to 1000 milliseconds). If set, explicitly implies auto_flush=on. Set to 'off' to disable.
//
// request_min_throughput: bytes per second, used to calculate each request's timeout (defaults to 100KiB/s)
//
// request_timeout:        minimum request timeout in milliseconds (defaults to 10 seconds)
//
// retry_timeout:          cumulative maximum millisecond duration spent in retries (defaults to 10 seconds)
//
// max_buf_size:           buffer growth limit in bytes. Client errors if breached (default is 100MiB)
//
// tcp(s)-only
//
// -----------
//
// username:  KID (key ID) for ECDSA authentication
//
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
	// First pass: discover the sender type and reject conflicting
	// transport options. We must iterate every option (not stop at the
	// first transport) so that mixing e.g. WithHttp + WithQwp is caught
	// before pass 2 seeds HTTP defaults onto a QWP config.
	tmp := newLineSenderConfig(noSenderType)
	sType := noSenderType
	for _, opt := range opts {
		opt(tmp)
		if tmp.senderType == noSenderType {
			continue
		}
		if sType == noSenderType {
			sType = tmp.senderType
		} else if tmp.senderType != sType {
			return nil, errors.New("conflicting transport options: use only one of WithHttp, WithTcp, or WithQwp")
		}
	}

	if sType == noSenderType {
		return nil, errors.New("sender type is not specified: use WithHttp, WithTcp, or WithQwp")
	}

	conf := newLineSenderConfig(sType)
	for _, opt := range opts {
		opt(conf)
	}
	return newLineSender(ctx, conf)
}

func newLineSenderConfig(t senderType) *lineSenderConfig {
	switch t {
	case tcpSenderType:
		return &lineSenderConfig{
			senderType:    t,
			address:       defaultTcpAddress,
			initBufSize:   defaultInitBufferSize,
			fileNameLimit: defaultFileNameLimit,
		}
	case qwpSenderType:
		// retryTimeout deliberately not seeded for QWP: connect-
		// string.md does not list retry_timeout as a QWP key
		// (it's HTTP-only), and Sender.java rejects it on the
		// WebSocket protocol. Leaving the zero value lets
		// sanitizeQwpConf detect "user set it" and reject.
		// reconnect_max_duration_millis is the QWP analogue.
		return &lineSenderConfig{
			senderType:        t,
			address:           defaultHttpAddress,
			autoFlushRows:     qwpDefaultAutoFlushRows,
			autoFlushInterval: qwpDefaultAutoFlushInterval,
			autoFlushBytes:    qwpDefaultAutoFlushBytes,
			inFlightWindow:    qwpDefaultInFlightWindow,
			initBufSize:       defaultInitBufferSize,
			maxBufSize:        defaultMaxBufferSize,
			fileNameLimit:     defaultFileNameLimit,
			// failover.md §7: 15s upper bound on the HTTP upgrade
			// response read. Parser overrides on explicit value.
			authTimeoutMs: 15_000,
		}
	default:
		return &lineSenderConfig{
			senderType:        t,
			address:           defaultHttpAddress,
			requestTimeout:    defaultRequestTimeout,
			retryTimeout:      defaultRetryTimeout,
			minThroughput:     defaultMinThroughput,
			autoFlushRows:     defaultAutoFlushRows,
			autoFlushInterval: defaultAutoFlushInterval,
			initBufSize:       defaultInitBufferSize,
			maxBufSize:        defaultMaxBufferSize,
			fileNameLimit:     defaultFileNameLimit,
		}
	}
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
		return newHttpLineSender(ctx, conf)
	case qwpSenderType:
		err := sanitizeQwpConf(conf)
		if err != nil {
			return nil, err
		}
		return newQwpLineSenderFromConf(ctx, conf)
	}
	return nil, errors.New("sender type is not specified: use WithHttp, WithTcp, or WithQwp")
}

func sanitizeTcpConf(conf *lineSenderConfig) error {
	err := validateConf(conf)
	if err != nil {
		return err
	}

	if strings.Contains(conf.address, ",") {
		return errors.New("multi-host addr is not supported for TCP")
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
	if conf.autoFlushBytes != 0 {
		return errors.New("autoFlushBytes setting is not available in the TCP client")
	}
	if conf.maxBufSize != 0 {
		return errors.New("maxBufferSize setting is not available in the TCP client")
	}
	if err := rejectQwpOnlyOptions(conf); err != nil {
		return err
	}
	if conf.tcpKey == "" && conf.tcpKeyId != "" {
		return errors.New("tcpKey is empty and tcpKeyId is not. both (or none) must be provided")
	}
	if conf.tcpKeyId == "" && conf.tcpKey != "" {
		return errors.New("tcpKeyId is empty and tcpKey is not. both (or none) must be provided")
	}

	return nil
}

func sanitizeQwpConf(conf *lineSenderConfig) error {
	err := validateConf(conf)
	if err != nil {
		return err
	}

	// QWP does not support HTTP-specific settings.
	if conf.requestTimeout != 0 {
		return errors.New("requestTimeout setting is not available in the QWP client")
	}
	if conf.minThroughput != 0 {
		return errors.New("minThroughput setting is not available in the QWP client")
	}
	if conf.retryTimeout != 0 {
		// connect-string.md does not list retry_timeout as a QWP key
		// (it's HTTP-only) and Sender.java rejects it on the
		// WebSocket protocol. The QWP analogue is the per-outage
		// reconnect budget; point the user there.
		return errors.New(
			"retry_timeout is not supported for QWP; use reconnect_max_duration_millis for the per-outage budget")
	}
	if conf.httpTransport != nil {
		return errors.New("httpTransport setting is not available in the QWP client")
	}
	if conf.tcpKeyId != "" || conf.tcpKey != "" {
		return errors.New("TCP auth settings (tcpKeyId/tcpKey) are not available in the QWP client")
	}
	// QWP auth: either Basic (user+pass) or Bearer (token), not both.
	if (conf.httpUser != "" || conf.httpPass != "") && conf.httpToken != "" {
		return errors.New("both basic and token authentication cannot be used")
	}
	if conf.inFlightWindow < 0 {
		return fmt.Errorf("in-flight window is negative: %d", conf.inFlightWindow)
	}
	if conf.protocolVersion != protocolVersionUnset {
		return errors.New("protocol_version setting is not available in the QWP client")
	}
	// Multi-host failover (failover.md §1 / §2). The parser populates
	// conf.endpoints for connect-string callers; functional-option
	// callers go through WithAddress, which writes only conf.address.
	// Back-fill endpoints from a single-host conf.address here so the
	// downstream code paths can rely on len(endpoints) >= 1.
	if len(conf.endpoints) == 0 && conf.address != "" {
		eps, err := parseEndpointList(conf.address, qwpDefaultPort)
		if err != nil {
			return err
		}
		conf.endpoints = eps
		conf.address = eps[0].String()
	}
	if conf.authTimeoutMs <= 0 {
		conf.authTimeoutMs = 15_000
	}
	// Implicit promotion of initial_connect_retry. When the user tuned
	// any reconnect_* knob but did not pick an initial-connect mode,
	// promote to sync — the reconnect budget they wrote should also
	// cover the *first* connect attempt. Otherwise the knob name reads
	// as a generic retry budget but the underlying path only governs
	// reconnects from an established connection, and the budget is
	// silently dropped at startup. Mirrors the Java client's
	// actualInitialConnectMode resolution in Sender.java.
	//
	// An explicit user choice (any value of initial_connect_retry, or
	// either of the With* setters) wins unconditionally — including
	// "off" paired with a tuned reconnect budget for users who want
	// fail-fast on startup misconfig but a generous post-connect budget.
	if !conf.initialConnectModeSet &&
		(conf.reconnectMaxDurationMillisSet ||
			conf.reconnectInitialBackoffMillisSet ||
			conf.reconnectMaxBackoffMillisSet) {
		conf.initialConnectMode = InitialConnectSync
	}
	// Cursor / store-and-forward validation. sf_dir activates cursor
	// mode; the sf_*, sender_id, drain_orphans, max_background_drainers
	// knobs are only meaningful when cursor mode is on.
	if conf.sfDir == "" {
		if conf.senderId != "" {
			return errors.New("sender_id requires sf_dir to be set")
		}
		if conf.sfMaxBytes != 0 || conf.sfMaxTotalBytes != 0 || conf.sfDurability != "" || conf.sfAppendDeadlineMillis != 0 {
			return errors.New("sf_max_bytes / sf_max_total_bytes / sf_durability / sf_append_deadline_millis require sf_dir to be set")
		}
		if conf.drainOrphans || conf.maxBackgroundDrainers != 0 {
			return errors.New("drain_orphans / max_background_drainers require sf_dir to be set")
		}
	}
	// Validate the sf_durability value space for the functional-option
	// path (WithSfDurability). The connect-string parser already
	// rejected flush/append/bogus, so this is a harmless re-check
	// there; it is the only gate on the option path.
	if err := validateSfDurability(conf.sfDurability); err != nil {
		return err
	}
	// Validate the sender_id charset for the functional-option path
	// (WithSenderId). The connect-string parser gates the parser path
	// (TestSfConfRejectsBadSenderId); this is the only gate on the
	// option path. Empty is the "use default" sentinel and resolves
	// to qwpSfDefaultSenderId downstream — skip validateSenderId's
	// strict non-empty rule for that case. Critical: senderId is used
	// unmodified as a path segment under sfDir at slotPath
	// construction (qwp_sender_cursor.go), so '.', '/' or '\' would
	// escape the sf_dir root.
	if conf.senderId != "" {
		if err := validateSenderId(conf.senderId); err != nil {
			return err
		}
	}
	// 0 is the use-default sentinel for both (resolved to
	// qwpSfDefaultMaxBytes / qwpSfDefaultMaxTotalBytes at construction),
	// so only a negative value is rejected here.
	if conf.sfMaxBytes < 0 {
		return fmt.Errorf("sf_max_bytes must be >= 0: %d", conf.sfMaxBytes)
	}
	if conf.sfMaxTotalBytes < 0 {
		return fmt.Errorf("sf_max_total_bytes must be >= 0: %d", conf.sfMaxTotalBytes)
	}
	if conf.sfMaxBytes > 0 && conf.sfMaxTotalBytes > 0 && conf.sfMaxTotalBytes < conf.sfMaxBytes {
		return fmt.Errorf("sf_max_total_bytes (%d) must be >= sf_max_bytes (%d)",
			conf.sfMaxTotalBytes, conf.sfMaxBytes)
	}
	// Reject an explicit auto_flush_bytes that exceeds an explicit
	// sf_max_bytes. The byte trigger would let a batch grow until its
	// encoded frame can no longer fit a single segment, and such a frame
	// can never be flushed — it is dropped at the flush boundary. Gated
	// on autoFlushBytesSet so a *defaulted* 8 MiB trigger over a smaller
	// user-chosen segment is left to the runtime clamp (which lowers the
	// effective trigger to fit); only a user-written contradiction is a
	// hard error. sf_max_bytes is the per-segment cap, so the frame must
	// actually fit in slightly less than this (header overhead), but the
	// trigger clamp already keeps the encoded frame under the segment;
	// this check just rejects the self-evidently impossible pairing up front.
	if conf.autoFlushBytesSet && conf.sfMaxBytes > 0 && int64(conf.autoFlushBytes) > conf.sfMaxBytes {
		return fmt.Errorf(
			"auto_flush_bytes (%d) must not exceed sf_max_bytes (%d): a batch that fills the byte trigger could not fit in a single segment",
			conf.autoFlushBytes, conf.sfMaxBytes)
	}
	if conf.maxBackgroundDrainers < 0 {
		return fmt.Errorf("max_background_drainers must be >= 0: %d", conf.maxBackgroundDrainers)
	}
	// Server-error API knobs (Phase 5). User-supplied
	// errorInboxCapacity must be in [qwpSfMinErrorInboxCapacity (16),
	// qwpSfMaxErrorInboxCapacity]; 0 falls back to the default at construction.
	// The ceiling guards make(chan, capacity) from a "makechan: size out of
	// range" panic on a pathological value.
	if conf.errorInboxCapacity != 0 && (conf.errorInboxCapacity < qwpSfMinErrorInboxCapacity ||
		conf.errorInboxCapacity > qwpSfMaxErrorInboxCapacity) {
		return fmt.Errorf("error_inbox_capacity must be in [%d, %d]: %d",
			qwpSfMinErrorInboxCapacity, qwpSfMaxErrorInboxCapacity, conf.errorInboxCapacity)
	}
	// Same bounds for the connection listener inbox; the connect-string parser
	// enforces the floor but WithConnectionListenerInboxCapacity reaches here
	// directly.
	if conf.connectionListenerInboxCapacity != 0 && (conf.connectionListenerInboxCapacity < qwpSfMinErrorInboxCapacity ||
		conf.connectionListenerInboxCapacity > qwpSfMaxErrorInboxCapacity) {
		return fmt.Errorf("connection_listener_inbox_capacity must be in [%d, %d]: %d",
			qwpSfMinErrorInboxCapacity, qwpSfMaxErrorInboxCapacity, conf.connectionListenerInboxCapacity)
	}

	return nil
}

func sanitizeHttpConf(conf *lineSenderConfig) error {
	err := validateConf(conf)
	if err != nil {
		return err
	}

	if strings.Contains(conf.address, ",") {
		return errors.New("multi-host addr is not supported for HTTP")
	}
	// validate http-specific settings
	if (conf.httpUser != "" || conf.httpPass != "") && conf.httpToken != "" {
		return errors.New("both basic and token authentication cannot be used")
	}
	if conf.autoFlushBytes != 0 {
		return errors.New("autoFlushBytes setting is not available in the HTTP client")
	}
	if err := rejectQwpOnlyOptions(conf); err != nil {
		return err
	}

	return nil
}

// rejectQwpOnlyOptions surfaces an error when a QWP-only option was
// set on a non-QWP sender. The connect-string parser already rejects
// each of these keys on non-ws/wss schemas; this mirrors the gate
// for callers that build the config programmatically via With*.
func rejectQwpOnlyOptions(conf *lineSenderConfig) error {
	if conf.errorHandler != nil || conf.errorPolicyResolver != nil ||
		conf.errorPolicyPerCatSet || conf.errorPolicyGlobal != PolicyAuto ||
		conf.errorInboxCapacity != 0 {
		return errors.New("server-error API settings are only available in the QWP client")
	}
	if conf.connectionListener != nil || conf.connectionListenerInboxCapacity != 0 {
		return errors.New("connection listener settings are only available in the QWP client")
	}
	var name string
	switch {
	case conf.sfDir != "":
		name = "sf_dir"
	case conf.senderId != "":
		name = "sender_id"
	case conf.sfMaxBytes != 0:
		name = "sf_max_bytes"
	case conf.sfMaxTotalBytes != 0:
		name = "sf_max_total_bytes"
	case conf.sfDurability != "":
		name = "sf_durability"
	case conf.sfAppendDeadlineMillis != 0:
		name = "sf_append_deadline_millis"
	case conf.drainOrphans:
		name = "drain_orphans"
	case conf.maxBackgroundDrainers != 0:
		name = "max_background_drainers"
	case conf.reconnectMaxDurationMillisSet,
		conf.reconnectInitialBackoffMillisSet,
		conf.reconnectMaxBackoffMillisSet:
		name = "reconnect_*"
	case conf.initialConnectModeSet:
		name = "initial_connect_retry"
	case conf.closeFlushTimeoutSet:
		name = "close_flush_timeout_millis"
	case conf.gorillaDisabled:
		name = "gorilla"
	case conf.dumpWriter != nil:
		name = "QWP dump writer"
	case conf.inFlightWindow != 0:
		name = "in_flight_window"
	case conf.authTimeoutMs != 0:
		name = "auth_timeout_ms"
	case conf.zone != "":
		name = "zone"
	case conf.target != qwpTargetAny:
		name = "target"
	default:
		return nil
	}
	return fmt.Errorf("%s is only available in the QWP client", name)
}

func newQwpLineSenderFromConf(ctx context.Context, conf *lineSenderConfig) (LineSender, error) {
	opts := qwpTransportOpts{
		tlsInsecureSkipVerify: conf.tlsMode == tlsInsecureSkipVerify,
		endpointPath:          qwpWritePath,
		authTimeoutMs:         conf.authTimeoutMs,
		connectTimeoutMs:      conf.connectTimeoutMs,
		// QWP has a single protocol version; advertise it.
		// serverInfoTimeout stays zero: the ingest endpoint sends no
		// SERVER_INFO frame and the client never expects one — it sends
		// data right after the upgrade and reads ACKs back. Ingest does
		// not route by role or zone, so target= and zone= are accepted
		// but inert on ingestion and honoured on the egress connect-walk
		// instead.
		maxVersion: qwpVersion,
	}
	// QWP auth: Basic (username:password) or Bearer (token).
	// Matches the Java client's buildWebSocketAuthHeader().
	if conf.httpUser != "" && conf.httpPass != "" {
		creds := conf.httpUser + ":" + conf.httpPass
		opts.authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))
	} else if conf.httpToken != "" {
		opts.authorization = "Bearer " + conf.httpToken
	}

	// Both memory mode (no sf_dir) and store-and-forward (sf_dir set)
	// run on the cursor engine + send loop, and both must honour the
	// multi-host addr= list, the initial_connect_retry mode, and the
	// reconnect_* budgets — per the README "Multi-host failover"
	// section, those failover knobs apply whether or not sf_dir is set.
	// The two modes differ only in the cursor engine's backing store
	// (RAM vs mmapped files) and a couple of defaults, which
	// newQwpCursorLineSenderFromConf resolves from conf.sfDir.
	return newQwpCursorLineSenderFromConf(ctx, conf, opts)
}

func validateConf(conf *lineSenderConfig) error {
	if conf.initBufSize < 0 {
		return fmt.Errorf("initial buffer size is negative: %d", conf.initBufSize)
	}
	if conf.maxBufSize < 0 {
		return fmt.Errorf("max buffer size is negative: %d", conf.maxBufSize)
	}

	if conf.fileNameLimit < 16 {
		return fmt.Errorf("max_name_len must be at least 16 bytes: %d", conf.fileNameLimit)
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
	if conf.autoFlushBytes < 0 {
		return fmt.Errorf("auto flush bytes is negative: %d", conf.autoFlushBytes)
	}
	if conf.protocolVersion < protocolVersionUnset || conf.protocolVersion > ProtocolVersion3 {
		return errors.New("current client only supports protocol version 1 (text format for all datatypes), " +
			"2 (binary format for floats/arrays), 3 (binary decimals) or explicitly unset")
	}

	return nil
}
