[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/questdb/go-questdb-client/v4)

# go-questdb-client

Golang client for [QuestDB](https://questdb.io), documented in the
[QuestDB Go client guide](https://questdb.io/docs/connect/clients/go/). It
**ingests** data and runs **streaming SQL queries** over QuestDB's transports:

- **QWP — QuestDB Wire Protocol** (`ws` / `wss`): a binary *columnar* protocol
  over WebSocket. The only transport that exposes the full QuestDB type system
  and the query side, with store-and-forward durability, multi-host failover,
  and connection pooling via the [`QuestDB` handle](#the-questdb-handle).
- **InfluxDB Line Protocol (ILP)** over **HTTP/HTTPS** and **TCP/TCPS**: the
  legacy ingestion-only transports, kept for backward compatibility.

The library requires Go 1.23 or newer.

Features:

- [Context](https://www.digitalocean.com/community/tutorials/how-to-use-contexts-in-go)-aware
  API, optimized for batch writes.
- A pooled, goroutine-safe [`QuestDB` handle](#the-questdb-handle) that owns
  ingest and query connection pools over one cluster config.
- Full QuestDB type system over QWP (`byte`, `short`, `int`, `float`, `char`,
  `date`, nanosecond timestamps, `uuid`, `geohash`, `int64` arrays, decimals).
- Store-and-forward durability, automatic reconnect, and multi-host failover.
- TLS encryption and authentication on every transport.

New in v4:

- **QWP WebSocket transport** for both ingestion and querying, with a typed
  server-error API and multi-host failover.
- **The `QuestDB` handle** — a facade that pools QWP ingest and query
  connections, with `lazy_connect` to tolerate a down server at startup.
- **N-dimensional arrays** of doubles (QuestDB server 9.0.0 and up).
- **Fixed-width decimal columns** (QuestDB server 9.2.0 and up).

ILP over HTTP/TCP is compatible with QuestDB 7.3.10 and newer. The QWP
transport, arrays, and decimals require the newer server versions noted above.

API reference: [pkg.go.dev](https://pkg.go.dev/github.com/questdb/go-questdb-client/v4).

## Installation

```bash
go get github.com/questdb/go-questdb-client/v4
```

## Quick start

The recommended entry point is the [`QuestDB`](#the-questdb-handle) handle: one
`ws`/`wss` config drives a pool for each direction. Construct it once, share it
across goroutines, and borrow a sender to ingest and a query session to read.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()

	db, err := qdb.Connect(ctx, "ws::addr=localhost:9000;")
	if err != nil {
		panic(err)
	}
	defer func() {
		// Return all borrowed handles before closing db.
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.Close(closeCtx); err != nil {
			log.Printf("questdb close: %v", err) // see "QWP shutdown and ownership"
		}
	}()

	// Ingest: borrow a sender, write rows, Close returns it to the pool.
	sender, err := db.BorrowSender(ctx)
	if err != nil {
		panic(err)
	}
	if err := sender.
		Table("trades").
		Symbol("symbol", "ETH-USD").
		Symbol("side", "sell").
		Float64Column("price", 2615.54).
		Float64Column("amount", 0.00044).
		AtNow(ctx); err != nil {
		panic(err)
	}
	if err := sender.Close(ctx); err != nil { // flush + return to pool
		panic(err)
	}

	// Query: borrow a session, run a SELECT, iterate its result batches.
	query, err := db.BorrowQuery(ctx)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := query.Close(); err != nil {
			log.Printf("query close: %v", err)
		}
	}()

	cursor := query.Query(ctx, "SELECT symbol, price FROM trades LIMIT 10")
	defer cursor.Close()
	for batch, err := range cursor.Batches() {
		if err != nil {
			panic(err)
		}
		for row := 0; row < batch.RowCount(); row++ {
			fmt.Println(batch.String(0, row), batch.Float64(1, row))
		}
	}
}
```

A runnable version lives at
[`examples/qwp/pool/main.go`](examples/qwp/pool/main.go).

> **Ingestion errors are asynchronous.** Over QWP, `Flush` returning `nil` does
> **not** mean the server accepted the rows — schema, parse, and write
> rejections are delivered out of band. Register an error handler in any
> non-trivial producer. See [Error handling](#error-handling).

## The QuestDB handle

`QuestDB` is a goroutine-safe handle for a QuestDB deployment. It owns an
elastic pool of each client type — senders for ingestion and query sessions for
SQL — plus a background housekeeper that closes idle and over-age connections.

| Method | Returns | Purpose |
|--------|---------|---------|
| `qdb.Connect(ctx, conf)` | `*QuestDB` | Open a handle with default pool sizing. One `ws`/`wss` string for both directions. |
| `qdb.NewQuestDB(ctx, conf, opts...)` | `*QuestDB` | Same, with pool-tuning options. |
| `db.BorrowSender(ctx)` | `LineSender` | Lease a sender; `Close` flushes and returns it to the pool. |
| `db.BorrowQuery(ctx)` | `*Query` | Lease a query session; `Close` returns it. |
| `db.Close(ctx)` | `error` | Shut down both pools. See [QWP shutdown and ownership](#qwp-shutdown-and-ownership) for what it waits for, what errors mean, and when to call it again. |

The schema must be `ws` or `wss` — the pooled facade is QWP-only. A borrowed
sender or query session is single-threaded; the handle itself is safe to share.
Calling `Close` on a borrowed sender or query session returns it to the pool.
A healthy connection stays open for reuse. The pool disconnects it when removing
it or shutting down.

Pool sizing and behavior are tunable through options (an explicit option wins
over the matching connect-string key) or the equivalent connect-string keys:

```go
db, err := qdb.NewQuestDB(ctx, "ws::addr=localhost:9000;",
	qdb.WithSenderPoolMax(8),
	qdb.WithQueryPoolMax(16),
	qdb.WithAcquireTimeout(10*time.Second),
	qdb.WithQuestDBErrorHandler(func(e *qdb.SenderError) { /* async rejections */ }),
	qdb.WithQuestDBConnectionListener(func(e qdb.SenderConnectionEvent) { /* events */ }))
```

| Connect-string key | Option | Default | Effect |
|---|---|---|---|
| `sender_pool_min` / `sender_pool_max` | `WithSenderPoolMin` / `WithSenderPoolMax` | `1` / `4` | Ingest pool size bounds. |
| `query_pool_min` / `query_pool_max` | `WithQueryPoolMin` / `WithQueryPoolMax` | `1` / `4` | Query pool size bounds. |
| `acquire_timeout_ms` | `WithAcquireTimeout` | `5000` | How long a borrow waits when the pool is at `max`. |
| `idle_timeout_ms` | `WithIdleTimeout` | `60000` | Idle connection reap, never below `min` (`0` = never). |
| `max_lifetime_ms` | `WithMaxLifetime` | `1800000` | Max age before an idle connection above `min` is recycled (`0` = no limit). |
| `housekeeper_interval_ms` | `WithHousekeeperInterval` | `5000` | Reaper sweep interval. |
| `lazy_connect` | `WithLazyConnect` | `off` | Tolerate a down server at startup (see below). |
| `connect_timeout` | `WithConnectTimeout` | OS default | Per-dial TCP connect timeout in milliseconds, common to ingest and query (`0` = keep the OS default). |
| `query_close_timeout_ms` | `WithQwpQueryCloseTimeout` | `5000` | How long a query lease's `Close` waits to drain an abandoned statement's cursor before evicting the connection. |
| `connection_listener_inbox_capacity` | `WithConnectionListenerInboxCapacity` | `256` | Bounded inbox depth for the `SenderConnectionListener` event stream (floor `16`). |

### Tolerating a down server at startup

By default the pool prewarms `min` connections eagerly, so `Connect` fails fast
if the server is unreachable. Set `lazy_connect=true` to build anyway: ingest
connects asynchronously (writes buffer until the wire is up) and the query pool
connects lazily on first borrow.

```go
db, err := qdb.Connect(ctx, "ws::addr=localhost:9000;lazy_connect=true;")
```

`lazy_connect` (or the `WithLazyConnect` option) is facade-only; standalone
clients accept but ignore the key.

## QWP shutdown and ownership

**Stop using a handle before you close it.** The `QuestDB` handle is safe to
share: different goroutines may borrow, return and close separate handles at
the same time. Each sender, query session or cursor still needs one user at a
time, and that user must be done with it before it is closed. For a running
query, cancel it, wait for the code reading results to stop, and stop using
slices that point into result batches. Callbacks must not close or change a
handle; see [Error handling](#error-handling).

**The context limits the wait, not the cleanup.** `Close(ctx)` starts shutdown
even when the context is already cancelled. When the context expires, Close
returns and cleanup continues in the background.

**Closing a sender sends what it can.** Close discards a row not finished with
`At`, `AtNow` or `AtNano`, queues the completed rows for sending (each frame
limited by the append timeout), then waits up to `close_flush_timeout_millis`
for server acknowledgements. The context does not cut either step short. Rows
held only in memory are lost if the process exits before they are delivered;
with store-and-forward, queued rows are replayed after a restart.

| Method | Behavior |
|---|---|
| [`QuestDB.Close(ctx)`](questdb.go) | Shuts down both pools, including background drainers. Nil means every resource is released. Calling it again, or concurrently, waits for the same shutdown; a later call with a fresh context checks progress. |
| [`QwpQueryClient.Close(ctx)`](qwp_query_client.go) | The same rules, for a standalone query client. |
| Standalone QWP `LineSender.Close(ctx)` | Call once; a second call returns a double-close error. A nil result can leave cleanup running in the background. Before reopening a disk-backed sender's slot, check [`SlotLockReleased`](qwp_sender.go). |
| Borrowed sender `Close(ctx)` | Returns the sender to its pool. It queues rows using the append timeout instead of `ctx` and does not wait for acknowledgements. If completed rows can't be queued (`ErrBackpressureTimeout` or `ErrSfDurability`), Close drops them and returns the error; to keep them, call `Flush` again before Close. |
| Borrowed query session `Close()` | Finishes reading an open response, for up to `query_close_timeout_ms`, and returns the session. |

Cleanup errors that happen after a handle is returned are logged and reported by
`QuestDB.Close`.

**Reading a Close error.**

- `ErrCleanupFailed`: check this first. An internal failure left resources that
  cannot safely be released. Calling Close again won't repair it, and an
  affected store-and-forward slot may stay locked until the process restarts.
- `ErrCleanupPending` (and `ErrSfCleanupPending` for store-and-forward):
  cleanup has not finished. Return any borrowed handles, check the storage, and
  call Close again with a fresh deadline.
- Any other error: rows could not be queued or delivered. It stays in the
  result even after all resources are released.

Callbacks may still be running when Close returns, and queued notifications may
be dropped. The Go docs of [`QuestDB.Close`](questdb.go),
[`LineSender.Close`](sender.go) and the [cleanup errors](qwp_errors.go) have
the full details.

## Other ways to connect

### Standalone clients

If you do not want pooling — a one-shot ETL job, or the ILP transports the
facade does not expose — construct the underlying clients directly:

```go
// Ingestion (any transport, inferred from the schema).
sender, err := qdb.LineSenderFromConf(ctx, "ws::addr=localhost:9000;")

// Ingestion via the options API (the only way to register callbacks on a
// standalone sender). NewLineSender needs exactly one transport option.
sender, err := qdb.NewLineSender(ctx,
	qdb.WithQwp(),
	qdb.WithAddress("localhost:9000"),
	qdb.WithErrorHandler(func(e *qdb.SenderError) { /* ... */ }),
	qdb.WithConnectionListener(func(e qdb.SenderConnectionEvent) { /* ... */ }))

// Querying.
client, err := qdb.QwpQueryClientFromConf(ctx, "ws::addr=localhost:9000;")
```

### Legacy ILP over HTTP / TCP

The InfluxDB Line Protocol transports are ingestion-only. HTTP is recommended
over TCP:

```go
sender, err := qdb.LineSenderFromConf(ctx, "http::addr=localhost:9000;")
sender, err := qdb.LineSenderFromConf(ctx, "tcp::addr=localhost:9009;")
```

The row-building API below is identical across all transports. QWP is a
distinct binary protocol rather than a version of ILP, so the
`protocol_version` key does not apply to `ws`/`wss`. For the full list of
connect-string keys, see the
[connect string reference](https://questdb.io/docs/connect/clients/connect-string/).

### Authentication

Basic auth and bearer tokens work the same way across every transport — pass
them in the connect string (use `wss`/`https`/`tcps` so credentials travel over
TLS), or via the `WithBasicAuth` / `WithBearerToken` options:

```go
db, err := qdb.Connect(ctx, "wss::addr=host:9000;username=admin;password=secret;")
db, err := qdb.Connect(ctx, "wss::addr=host:9000;token=<bearer>;")
```

## Ingestion

A `LineSender` (pooled or standalone) builds rows with a fluent API:

1. `Table(name)` selects the table.
2. column setters add values: `Symbol`, `StringColumn`, `BoolColumn`,
   `Int64Column`, `Float64Column`, `TimestampColumn`, `Long256Column`,
   the array and decimal setters, and the QWP-only setters below.
3. `At(ctx, ts)` or `AtNow(ctx)` finalizes the row.
4. `Flush(ctx)` sends buffered rows; `Close(ctx)` does a final flush.

The fluent methods do not return errors — the first error is latched and
surfaces from `At`, `AtNow`, or `Flush`, so always check that return value.
Tables and columns are created automatically if they do not exist.

```go
err = sender.
	Table("trades").
	Symbol("symbol", "BTC-USD").
	Symbol("side", "sell").
	Float64Column("price", 39269.98).
	Float64Column("amount", 0.001).
	At(ctx, time.Now()) // or AtNow(ctx) for a server-assigned timestamp
```

To store a NULL, omit that column's setter for the row: on commit, every column
not set is gap-filled with NULL.

### QWP-only column types

QWP exposes types ILP does not. Type-assert the sender to `qdb.QwpSender` — a
borrowed sender from the [`QuestDB` handle](#the-questdb-handle) always
implements it (an HTTP or TCP sender does not):

```go
qs, ok := sender.(qdb.QwpSender)
if !ok {
	log.Fatal("not a QWP sender")
}

// Table and Symbol return LineSender, so call them first; then chain the
// QWP-only setters (which return QwpSender) into AtNano.
qs.Table("sensors")
qs.Symbol("site", "roof")
err = qs.
	ByteColumn("status_code", 3).
	ShortColumn("battery", 4812).
	Int32Column("sample_count", 120_000).
	Float32Column("temperature", 21.7).
	CharColumn("grade", 'A').
	DateColumn("calibrated", time.Now()).
	TimestampNanosColumn("captured", time.Now()).
	UuidColumn("device_id", 0x0123456789abcdef, 0xfedcba9876543210).
	GeohashColumn("location", 0x1fb9, 15).
	Int64Array1DColumn("raw_counts", []int64{10, 20, 30}).
	Decimal64Column("voltage", qdb.NewDecimalFromInt64(12345, 4)).
	AtNano(ctx, time.Now()) // nanosecond designated timestamp; At uses microseconds
```

`QwpSender` adds `ByteColumn`, `ShortColumn`, `Int32Column`, `Float32Column`,
`CharColumn`, `DateColumn`, `TimestampNanosColumn`, `UuidColumn`,
`GeohashColumn`, `Int64Array1DColumn` / `2D` / `3D`, `Decimal64Column` /
`Decimal128Column` / `Decimal256Column`, and `AtNano`, plus acknowledgement and
diagnostic methods such as `FlushAndGetSequence` and `AwaitAckedFsn`; see the
[Go docs](https://pkg.go.dev/github.com/questdb/go-questdb-client/v4#QwpSender).

### N-dimensional arrays

QuestDB 9.0.0+ supports n-dimensional arrays of doubles. For 1D/2D/3D, pass a
Go slice directly:

```go
err = sender.Table("book").
	Float64Array1DColumn("levels", []float64{1.0842, 1.0843, 1.0841}).
	AtNow(ctx)

err = sender.Table("matrix_data").
	Float64Array2DColumn("matrix", [][]float64{{1.1, 2.2}, {3.3, 4.4}}).
	AtNow(ctx)
```

For higher dimensions, build an `NdArray` and reuse it:

```go
arr, err := qdb.NewNDArray[float64](2, 3, 4)
if err != nil {
	log.Fatal(err)
}
arr.Fill(1.5)
arr.Set(42.0, 0, 1, 2) // set 42.0 at coordinates [0,1,2]

err = sender.Table("ndarray_data").
	Float64ArrayNDColumn("features", arr).
	AtNow(ctx)
```

Over ILP, arrays use protocol version 2, auto-negotiated on HTTP(S) or set with
`protocol_version=2` on TCP(S). QWP carries them natively.

### Decimal columns

QuestDB 9.2.0+ supports fixed-width decimal columns. Construct a `qdb.Decimal`
from an `int64`, a `*big.Int`, or a string literal:

```go
price := qdb.NewDecimalFromInt64(12345, 2) // 123.45, scale 2
commission, err := qdb.NewDecimal(big.NewInt(-750), 4) // -0.0750, scale 4
if err != nil {
	log.Fatal(err)
}

err = sender.Table("trades").
	Symbol("symbol", "ETH-USD").
	DecimalColumn("price", price).
	DecimalColumn("commission", commission).
	AtNow(ctx)
```

`DecimalColumn` (on every sender) serializes a 256-bit value; the width-specific
`Decimal64Column` /
`Decimal128Column` / `Decimal256Column` (on `QwpSender`) target the matching
width. `DecimalColumnFromString` emits a validated string literal, and
`DecimalColumnShopspring` accepts
[github.com/shopspring/decimal](https://github.com/shopspring/decimal) values.

### Flushing and backpressure

The sender batches rows and auto-flushes when a threshold is reached:

| Trigger | WebSocket (QWP) | HTTP |
|---|---|---|
| Row count (`auto_flush_rows`) | 1,000 | 75,000 |
| Interval (`auto_flush_interval`) | 100 ms | 1,000 ms |
| Byte size (`auto_flush_bytes`) | 8 MiB | disabled |

`Flush` and `FlushAndGetSequence` over QWP **never wait for the server ACK**:
they return once the batch is queued for sending (in memory, or on disk with
store-and-forward), and the sender delivers it in the background. A returned
`Flush` therefore does not mean the server has the rows; without
store-and-forward, a process exit before they are sent loses them. To wait for
confirmation, pair `FlushAndGetSequence` (which returns the batch's sequence
number) with `AwaitAckedFsn`.

A server ACK means WAL commit by default. For Enterprise durable delivery,
`request_durable_ack=on` (or `qdb.WithRequestDurableAck(true)`) advances the
acknowledged watermark only after the batch is durably uploaded to object
storage. It is QWP-only and requires a replication primary — the connect fails
terminally against a replica.

### Error handling

When the server rejects a published QWP batch, the rejection surfaces as a
`*qdb.SenderError` carrying a stable `Category` (`SCHEMA_MISMATCH`,
`PARSE_ERROR`, `INTERNAL_ERROR`, `SECURITY_ERROR`, `WRITE_ERROR`,
`NOT_WRITABLE`, `PROTOCOL_VIOLATION`, `UNKNOWN`), the server message, and the
`[FromFsn, ToFsn]` span — join that span against `FlushAndGetSequence` to
identify the rejected rows. There are two delivery paths, same payload:

```go
// Async handler — register on the QuestDB handle (applied to every pooled
// sender) or on a standalone sender via qdb.WithErrorHandler.
db, err := qdb.NewQuestDB(ctx, "ws::addr=localhost:9000;",
	qdb.WithQuestDBErrorHandler(func(e *qdb.SenderError) {
		log.Printf("rejected fsn=[%d,%d] %s: %s",
			e.FromFsn, e.ToFsn, e.Category, e.ServerMessage)
	}))

// Sync — after a terminal rejection, the typed error surfaces on the next
// producer call.
if err := sender.Flush(ctx); err != nil {
	var se *qdb.SenderError
	if errors.As(err, &se) {
		// inspect se.Category, se.ServerMessage, se.FromFsn, ...
	}
}
```

> **Callbacks only report events.** From a callback, don't call `Flush`,
> `Close`, row-building methods or `QuestDB.Close`; signal your code through a
> channel or a context instead.

Nothing is ever silently dropped. Each `Category` resolves to a `Policy`:

- `RETRIABLE` / `RETRIABLE_OTHER` — recycle the connection and replay from the
  local buffer (in RAM for memory mode, on disk when `sf_dir` is set); nothing
  is dropped and the producer keeps writing; the handler is only informed.
  `RETRIABLE_OTHER` (`NOT_WRITABLE`) additionally rotates to the
  next endpoint. A frame rejected repeatedly with no ack progress escalates to
  `TERMINAL` via the poison-frame detector (`max_frame_rejections`, default 4).
- `TERMINAL` — latch the error; the next producer call returns it and the sender
  stops draining until you close and rebuild it. The rejected bytes remain in
  the local buffer (on disk when `sf_dir` is set).

Resolution precedence, highest first: `WithErrorPolicyResolver` →
`WithErrorPolicy(category, policy)` → connect-string `on_<category>_error` →
`on_server_error` → defaults. `PROTOCOL_VIOLATION` is always `TERMINAL` and
`UNKNOWN` is always `RETRIABLE`; overrides for those two are ignored. Connect-string equivalents take `terminal` / `retriable` /
`retriable_other` (and `auto` for the global key):

```text
ws::addr=localhost:9000;on_server_error=retriable;on_schema_error=terminal;
```

Background drainers (`drain_orphans=on`) apply the same policy to the slots
they drain. When a rejection there resolves to `TERMINAL`, the drainer gives
up on that slot and writes [`.failed`](#quarantined-slots). Drainer
rejections are logged and are not passed to your error handler.

### Store-and-forward

QWP supports an opt-in **store-and-forward** (SF) mode: outgoing batches are
written to disk segments before they are sent, and the sender replays them
after a disconnect or a process restart. Activate it by setting `sf_dir` on a
`ws`/`wss` connection:

```go
sender, err := qdb.LineSenderFromConf(ctx,
	"ws::addr=localhost:9000;sf_dir=/var/lib/questdb-sf;sender_id=my-app;")
```

Each sender owns a slot directory, `<sf_dir>/<sender_id>/`, which it locks so
that two senders never share it. The [`QuestDB` handle](#the-questdb-handle)
gives each pooled sender its own slot. Without `sf_dir`, unacknowledged rows
live in process memory and are lost if the process dies; the sender still
reconnects through transient outages.

SF reserves disk blocks natively when it creates its files, which is supported
on Linux, macOS and Windows. Where the platform or filesystem cannot do that,
creating a slot fails with `ErrSfDurability`. Memory-backed senders are not
affected.

| Key | Default | Effect |
|---|---|---|
| `sf_dir` | unset | Root directory for slots. Setting it turns SF on. |
| `sender_id` | `default` | Slot name: ASCII letters, digits, `-` and `_`. |
| `sf_max_segment_bytes` | 4 MiB | Size of each segment file. |
| `sf_max_total_bytes` | 10 GiB | Disk space one slot may use, including its `.corrupt` files. When it is used up, the producer waits. |
| `sf_append_deadline_millis` | 30000 | How long `At` / `AtNow` / `Flush` wait for space before failing with `ErrBackpressureTimeout`. |
| `sf_durability` | `memory` | The only supported value; see the crash guarantees below. |
| `reconnect_max_duration_millis` | 300000 | Time limit for a blocking initial connect. A running sender retries outages indefinitely. The same value sets how long one frame may keep being rejected, and how long an adopted orphan slot may stay connected without making progress, before the client gives up on it, so leave it at the default unless you mean that. |
| `reconnect_initial_backoff_millis` | 100 | First retry delay, with jitter. |
| `reconnect_max_backoff_millis` | 5000 | Longest retry delay. |
| `initial_connect_retry` | `off` | `off`: fail if the first connect fails. `on` / `sync`: retry, blocking the constructor. `async`: retry in the background while the constructor returns. |
| `close_flush_timeout_millis` | 5000 | How long Close waits for server acknowledgements; see [shutdown](#qwp-shutdown-and-ownership). |
| `drain_orphans` | `off` | When `on`, find other slots under `sf_dir` that still hold unsent rows and send them in the background. Closing the sender stops this. |
| `max_background_drainers` | 4 | How many orphan slots are sent at once. |
| `max_frame_rejections` | 4 | How many times in a row the server may reject the same frame before the sender stops with a terminal error. |
| `request_durable_ack` | `off` | Wait for object-storage upload before acknowledging; see [Flushing and backpressure](#flushing-and-backpressure). |
| `durable_ack_keepalive_interval_millis` | 200 | Idle ping that asks for pending durable acknowledgements; `<= 0` disables it, and an idle producer can then stall `AwaitAckedFsn`. |

The same options are available programmatically: `WithSfDir`, `WithSenderId`,
`WithSfMaxSegmentBytes`, `WithSfMaxTotalBytes`, `WithReconnectPolicy`,
`WithInitialConnectRetry`, `WithInitialConnectMode`, `WithCloseFlushTimeout`,
`WithMaxFrameRejections`, `WithRequestDurableAck`,
`WithDurableAckKeepaliveInterval`.

**What an OS crash or power loss costs.** Frames reach the disk through the
kernel's normal writeback, not an `fsync` per frame. Each time a segment fills
up, the sender makes it durable before moving to the next one. After an OS
crash or power loss, recovery keeps every full segment and the readable
beginning of the one being written; only rows at its end that the kernel had not
written yet can be lost. When the disk is slower than the producer, the sender
waits for it at each new segment. On macOS, `fsync` does not flush the drive's
own cache, so a power cut can still lose cached data. On Windows, SF does not
guarantee that file creations, renames and deletions survive a host crash in
order.

**Delivery is at least once.** After a restart, replay starts after the last
acknowledgement recorded in `.ack-watermark`, or at the oldest surviving segment
when that record is missing or does not fit the recovered frames. Either way the
server can receive rows it had already acknowledged.

#### Local errors from the SF path

Two errors mean the rows could not be queued yet, not that the sender has
failed. Rows that were not queued stay pending, and calling `Flush` again
retries them without resending rows that were. When `At` or `AtNow` returns one
of these errors, it came from an auto-flush, and the row that call finished is
already buffered: don't build it again, or the server receives it twice.

| Error | Meaning |
|---|---|
| `qdb.ErrBackpressureTimeout` | No space within `sf_append_deadline_millis`: the server is not keeping up or is unreachable, or `sf_max_total_bytes` is too small. Memory-backed senders return it too, after waiting 30 seconds with their 128 MiB buffer full. |
| `qdb.ErrSfDurability` | Local storage failed, usually because the disk is full, read-only or failing. Creating a sender fails with it too when its slot cannot be written. |

A borrowed sender's `Close` drops the rows instead; see
[shutdown](#qwp-shutdown-and-ownership). Match these errors with `errors.Is`.
Other errors can reach the producer as well, and an error that matches neither
is not necessarily terminal.

#### Recovery and damaged tails

After a crash, recovery keeps the readable beginning of the segment that was
being written and discards the rest, even if later bytes hold intact rows.
Missing segments or gaps in the saved queue make recovery refuse the slot
instead; see [Quarantined slots](#quarantined-slots). A local I/O error during
recovery is retried later; it is not treated as damage.

Remove segment files only together with the whole slot directory. In some states
recovery cannot tell a deleted segment from a fully delivered slot, and it
starts the slot empty without reporting the lost rows.

#### Quarantined slots

If recovery finds a slot inconsistent, the sender does not delete or repair it.
It moves the whole directory aside to `<sf_dir>/<sender_id>.unreplayable-<n>/`
and starts on a fresh slot, so ingestion continues. The unsent rows are in that
copy:

```go
if qs, ok := sender.(qdb.QwpSender); ok {
	if path := qs.QuarantinedSlotPath(); path != "" {
		log.Printf("unsent rows preserved at %s", path)
	}
}
```

What to know about these copies:

- They are the only copy of those rows. The client never deletes them, and they
  don't count against `sf_max_total_bytes`, so cleaning them up is up to you. A
  crash loop can leave one copy per restart.
- `<n>` runs from 0 to 63. When all 64 names are taken, the sender refuses to
  start until you move or remove some copies.
- Orphan draining skips them. The Java client uses the same names.
- With a `QuestDB` pool, check each borrowed sender, or list
  `<sf_dir>/*.unreplayable-*`: a slot set aside by a pool build that no caller
  receives is reported only in the log.
- A copy is not a backup, and storage faults can still damage it. Don't rename
  it back into a slot to make a client replay it.

Other files you may find in a slot:

- `<name>.sfa.corrupt`: a segment whose header could not be read, renamed in
  place. It counts against `sf_max_total_bytes`; if such files fill the limit,
  the producer gets `ErrBackpressureTimeout`. Deleting them frees the space
  within about a second.
- `.failed`: the reason a background drainer gave up on the slot, for example
  failed authentication, a server rejection whose error policy is `TERMINAL`,
  durable acknowledgements that never arrive, a connection that makes no
  progress, or a slot that recovery found inconsistent. No drainer adopts that
  slot again. A local I/O error while opening a slot leaves no `.failed`, so
  the slot is tried again on the next scan. Quarantined copies also get a
  `.failed`, when it can be written.

**Sharing an `sf_dir`.** Senders coordinate opening, quarantining and draining
slots through advisory locks under `<sf_dir>/.slot-locks/`. Share an `sf_dir`
only between clients that take those locks, on a filesystem where advisory locks
and `rename` work. Older clients and other programs are not protected: before
upgrading, stop older clients that write to or drain the root, or give them
their own roots; turning off orphan draining is not enough. Don't delete the lock
files, and don't move, rename or remove slot, quarantine or lock paths while any
client is running.

Accepted limits of quarantine and slot locking are listed in
[docs/qwp-limits-and-invariants.md](docs/qwp-limits-and-invariants.md).

## Querying

The query side streams columnar result batches over the same WebSocket
protocol. Borrow a session from the [`QuestDB` handle](#the-questdb-handle) (or
construct a standalone `QwpQueryClient`): `Query` returns a streaming cursor for
SELECTs, `Exec` runs DDL/DML. Both block until the statement completes.

```go
query, err := db.BorrowQuery(ctx)
if err != nil {
	log.Fatal(err)
}
defer query.Close()

// DDL / DML via Exec.
if _, err := query.Exec(ctx,
	"CREATE TABLE example (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"); err != nil {
	log.Fatal(err)
}

// SELECT returns a *QwpQuery; range over its Batches iterator.
cursor := query.Query(ctx, "SELECT ts, v FROM example")
defer cursor.Close()

var sum int64
for batch, err := range cursor.Batches() {
	if err != nil {
		log.Fatal(err)
	}
	vCol := batch.Column(1) // column 1 is `v` (LONG)
	for r := 0; r < vCol.RowCount(); r++ {
		sum += vCol.Int64(r)
	}
}
```

A borrowed query session runs **one query at a time** and is **not** safe for
concurrent `Query` / `Exec`. To run queries in parallel, borrow one session per
goroutine (the query pool's `max` caps concurrency). `Cancel` (on the cursor)
is safe from another goroutine. Stop reading results before closing a cursor
or session; see [shutdown and ownership](#qwp-shutdown-and-ownership).

### Reading result batches

A `*QwpColumnBatch` is valid **only during its loop iteration** — never store
the batch. Its accessors take `(col, row)`; the cached `batch.Column(i)`
(`QwpColumn`) accessors take `(row)`. Most scalar accessors return values;
`Str` and `Binary` alias the receive buffer (clone before the loop advances),
while `String` allocates a fresh Go string. Use `batch.CopyAll()` for a
retainable snapshot.

For tight column sweeps, `Int64Range` / `Float64Range` decode a row range into a
caller-owned slice in one shot (a single `memmove` on a no-null column):

```go
buf := make([]int64, 0, 1024)
for batch, err := range cursor.Batches() {
	if err != nil {
		log.Fatal(err)
	}
	buf = batch.Column(1).Int64Range(0, batch.RowCount(), buf[:0])
	for _, v := range buf {
		sum += v
	}
}
```

`TIMESTAMP` / `timestamp_ns` / `DATE` come back as `int64` (micro/nano/milli
since epoch); `UUID` as `UuidHi`/`UuidLo` halves; decimals as the unscaled
integer plus `DecimalScale(col)`. A typed accessor on a NULL cell returns the
zero value — call `IsNull(col, row)` when NULL is meaningful.

### Bind parameters

Bind parameters use `$1`, `$2`, … placeholders, passed via
`qdb.WithQwpQueryBinds`. Setters take 0-based indexes and must be called in
strictly ascending order (index `0` maps to `$1`):

```go
cursor := query.Query(ctx,
	"SELECT ts, v FROM example WHERE v > $1",
	qdb.WithQwpQueryBinds(func(b *qdb.QwpBinds) {
		b.LongBind(0, 100)
	}))
```

Setters include `BooleanBind`, `ByteBind`, `ShortBind`, `IntBind`, `LongBind`,
`FloatBind`, `DoubleBind`, `CharBind`, `DateBind`, `TimestampMicrosBind`,
`TimestampNanosBind`, `VarcharBind`, `UuidBind`, `Long256Bind`, `GeohashBind`,
and `DecimalBind` (plus `Null...Bind` variants). Use `VarcharBind` for symbol
parameters. `Exec` results expose `RowsAffected` and `OpType`. A server-side
query failure surfaces as a `*qdb.QwpQueryError` from `Batches()` or `Exec`,
carrying a numeric `Status`, the server `Message`, and the client-assigned
`RequestId`.

A runnable example is at
[`examples/qwp/basic-query/main.go`](examples/qwp/basic-query/main.go).

## Multi-host failover

> **Note:** Multi-host failover with automatic reconnect requires QuestDB
> Enterprise.

`addr` accepts a comma-separated list for transparent failover; the client
walks it in priority order on connect and reconnect (it does not load-balance):

```go
qdb.Connect(ctx, "ws::addr=node-a:9000,node-b:9000,node-c:9000;")
```

`target` constrains acceptable endpoints by replicated-cluster role: `any`
(default), `primary` (writers — also standalone OSS servers), or `replica`.
`zone` is an opaque locality identifier the client prefers when set. Both are
**query-side** features: ingestion always lands on the primary (replicas reject
write connections), so `target` / `zone` are accepted but inert for ingest.

Watch connection-state transitions with `WithQuestDBConnectionListener`
(facade) or `WithConnectionListener` (standalone): the
`SenderConnectionEvent.Kind` is one of `SenderConnected`, `SenderDisconnected`,
`SenderReconnected`, `SenderFailedOver`, `SenderEndpointAttemptFailed`,
`SenderAllEndpointsUnreachable`, or `SenderAuthFailed`. There is deliberately no
budget-exhausted kind: a running sender retries transport outages
indefinitely. On the query side, a mid-stream reconnect yields a non-fatal
`*QwpFailoverReset` (discard accumulated rows and continue); an exhausted
failover budget yields `*QwpFailoverExhaustedError`.

For full configuration, see the
[client failover guide](https://questdb.io/docs/high-availability/client-failover/configuration/).

## Legacy: pooled HTTP senders

> **Experimental, HTTP-only.** For QWP pooling use the
> [`QuestDB` handle](#the-questdb-handle) instead.

`LineSenderPool` pools previously-used HTTP `LineSender`s so they can be reused
without reallocating. It is thread-safe; acquire a sender per goroutine and
`Close` it (returns it to the pool) when done:

```go
pool, err := qdb.PoolFromConf("http::addr=localhost:9000;")
if err != nil {
	panic(err)
}
defer pool.Close(ctx)

sender, err := pool.Sender(ctx)
if err != nil {
	panic(err)
}
sender.Table("prices").Symbol("ticker", "AAPL").Float64Column("price", 123.45).AtNow(ctx)
if err := sender.Close(ctx); err != nil { // returns the sender to the pool
	panic(err)
}
```

`LineSenderPool` rejects TCP and QWP configs — it is for the stateless HTTP
transport only.

## Community

If you need help, have questions, or want to give feedback, join our
[Community Forum](https://community.questdb.io/). You can also
[sign up to our mailing list](https://questdb.io/contributors/) to get notified
of new releases.
