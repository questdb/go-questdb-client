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

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()

	db, err := qdb.Connect(ctx, "ws::addr=localhost:9000;")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

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
	defer query.Close()

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
| `db.Close(ctx)` | `error` | Shut down both pools and disconnect every underlying client. Idempotent. |

The schema must be `ws` or `wss` — the pooled facade is QWP-only. A borrowed
sender or query session is single-threaded; the handle itself is safe to share.
`Close` on a borrowed lease flushes/returns it to the pool — the real
disconnect happens only at `db.Close`.

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
| `max_lifetime_ms` | `WithMaxLifetime` | `1800000` | Max connection age before recycling (`0` = no limit). |
| `housekeeper_interval_ms` | `WithHousekeeperInterval` | `5000` | Reaper sweep interval. |
| `lazy_connect` | — | `off` | Tolerate a down server at startup (see below). |

Unlike the Java client's facade, the Go `QuestDB` handle accepts the ingest
error handler and connection listener directly via
`WithQuestDBErrorHandler` / `WithQuestDBConnectionListener`, so you do not need
a standalone sender to observe rejections or connection-state transitions.

### Tolerating a down server at startup

By default the pool prewarms `min` connections eagerly, so `Connect` fails fast
if the server is unreachable. Set `lazy_connect=true` to build anyway: ingest
connects asynchronously (writes buffer until the wire is up) and the query pool
connects lazily on first borrow.

```go
db, err := qdb.Connect(ctx, "ws::addr=localhost:9000;lazy_connect=true;")
```

`lazy_connect` is a facade-only key; standalone clients accept but ignore it.
`connect_timeout` (milliseconds) bounds the TCP connect on each dial and is
common to both directions.

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
`Decimal128Column` / `Decimal256Column`, and `AtNano`, plus the
acknowledgement and observability accessors (`AwaitAckedFsn`,
`FlushAndGetSequence`, `TotalReconnectAttempts`, `LastTerminalError`, …).

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
they return once the batch is published into the cursor engine (in RAM for
memory mode, on disk for store-and-forward), after which a dedicated I/O
goroutine delivers and replays it. A returned `Flush` therefore means the batch
is *published*, not server-confirmed; in memory mode a process exit before the
background send completes can still lose unacked rows. For server-ACK
confirmation, pair `FlushAndGetSequence` (returns the published FSN) with
`AwaitAckedFsn`.

Backpressure is governed by the engine's segment ring and the append deadline
(`sf_append_deadline_millis`), not a fixed in-flight count.
`in_flight_window` / `qdb.WithInFlightWindow(n)` is retained for compatibility
but is a **no-op**.

### Error handling

When the server rejects a published QWP batch, the rejection surfaces as a
`*qdb.SenderError` carrying a stable `Category` (`SCHEMA_MISMATCH`,
`PARSE_ERROR`, `INTERNAL_ERROR`, `SECURITY_ERROR`, `WRITE_ERROR`,
`PROTOCOL_VIOLATION`, `UNKNOWN`), the server message, and the
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

// Sync — after a HALT, the typed error surfaces on the next producer call.
if err := sender.Flush(ctx); err != nil {
	var se *qdb.SenderError
	if errors.As(err, &se) {
		// inspect se.Category, se.ServerMessage, se.FromFsn, ...
	}
}
```

Each `Category` resolves to a `Policy` — `HALT` (latch the error; close and
rebuild the sender to continue) or `DROP_AND_CONTINUE` (drop the rejected span
and keep going). Resolution precedence, highest first: `WithErrorPolicyResolver`
→ `WithErrorPolicy(category, policy)` → connect-string `on_<category>_error` →
`on_server_error` → spec defaults. `PROTOCOL_VIOLATION` and `UNKNOWN` are always
`HALT`. Connect-string equivalents take `halt` / `drop` (and `auto` for the
global key):

```text
ws::addr=localhost:9000;on_server_error=halt;on_schema_error=drop;on_write_error=drop;
```

### Store-and-forward

QWP supports an opt-in **store-and-forward** (SF) mode: outgoing batches are
persisted to mmap'd disk segments before they leave the wire, and the I/O loop
replays from disk on transient disconnects or process restarts. Activate it by
setting `sf_dir` on a `ws`/`wss` connection:

```go
sender, err := qdb.LineSenderFromConf(ctx,
	"ws::addr=localhost:9000;sf_dir=/var/lib/questdb-sf;sender_id=my-app;")
```

The slot lives at `<sf_dir>/<sender_id>/`, guarded by an advisory `flock` so two
senders never share a slot. When the [`QuestDB` handle](#the-questdb-handle)
runs in SF mode it assigns each pooled sender its own slot automatically.

| Key | Default | Effect |
|---|---|---|
| `sf_dir` | unset | Group root. Setting it activates SF. |
| `sender_id` | `default` | Per-sender slot name; ASCII letters / digits / `-_.` only. |
| `sf_max_bytes` | 4 MiB | Per-segment file size. |
| `sf_max_total_bytes` | 10 GiB | Total cap; producer is backpressured when reached. |
| `sf_append_deadline_millis` | 30000 | How long `At` / `AtNow` block on backpressure before failing. |
| `reconnect_max_duration_millis` | 300000 | Per-outage cap on reconnect retries. |
| `reconnect_initial_backoff_millis` | 100 | Initial backoff with jitter. |
| `reconnect_max_backoff_millis` | 5000 | Backoff cap. |
| `initial_connect_retry` | `off` | `off` = terminal on first failure; `on`/`sync` = retry, blocking the constructor; `async` = retry on the I/O goroutine, constructor returns immediately. |
| `close_flush_timeout_millis` | 5000 | `Close` waits this long for ACKs; `0` / `-1` skips the drain. |
| `drain_orphans` | `off` | When `on`, scan `<sf_dir>/*` and adopt sibling slots holding unacked data. |
| `max_background_drainers` | 4 | Cap on concurrent orphan drainers. |

The same options are available programmatically: `WithSfDir`, `WithSenderId`,
`WithSfMaxBytes`, `WithSfMaxTotalBytes`, `WithReconnectPolicy`,
`WithInitialConnectRetry`, `WithInitialConnectMode`, `WithCloseFlushTimeout`.

Without `sf_dir`, unacknowledged data lives in process memory and is lost if the
process dies; the reconnect loop still spans transient outages.

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
and `Close` are safe to call from another goroutine.

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
`SenderAllEndpointsUnreachable`, `SenderAuthFailed`, or
`SenderReconnectBudgetExhausted`. On the query side, a mid-stream reconnect
yields a non-fatal `*QwpFailoverReset` (discard accumulated rows and continue);
an exhausted failover budget yields `*QwpFailoverExhaustedError`.

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
