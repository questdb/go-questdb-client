[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/questdb/go-questdb-client/v4)

# go-questdb-client

Golang client for QuestDB's [Influx Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/)
(ILP) over HTTP and TCP. This library makes it easy to insert data into
[QuestDB](https://questdb.io).

The library requires Go 1.23 or newer.

Features:
* [Context](https://www.digitalocean.com/community/tutorials/how-to-use-contexts-in-go)-aware API.
* Optimized for batch writes.
* Three transports: ILP over HTTP and TCP, plus QWP (QuestDB's binary
  columnar protocol) over WebSocket.
* Supports TLS encryption and authentication.
* Automatic write retries and connection reuse for ILP over HTTP;
  store-and-forward, reconnect, and multi-host failover for QWP.

New in v4:
* QWP WebSocket transport exposing the full QuestDB type system, with a
  typed server-error API and multi-host failover.
* N-dimensional arrays of doubles (QuestDB server 9.0.0 and up).
* Fixed-width decimal columns (QuestDB server 9.2.0 and up).

ILP over HTTP/TCP is compatible with QuestDB 7.3.10 and newer. The QWP
transport, arrays, and decimals require the newer server versions noted
above.

Documentation is available [here](https://pkg.go.dev/github.com/questdb/go-questdb-client/v4).

## Quickstart

```go
package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()
	// Connect to QuestDB running locally.
	sender, err := qdb.LineSenderFromConf(ctx, "http::addr=localhost:9000;")
	if err != nil {
		log.Fatal(err)
	}
	// Make sure to close the sender on exit to release resources.
	defer sender.Close(ctx)
	// Send a few ILP messages.
	err = sender.
		Table("trades").
		Symbol("symbol", "ETH-USD").
		Symbol("side", "sell").
		Float64Column("price", 2615.54).
		Float64Column("amount", 0.00044).
		AtNow(ctx) // timestamp will be set at the server side

	tradedTs, err := time.Parse(time.RFC3339, "2022-08-06T15:04:05.123456Z")
	if err != nil {
		log.Fatal(err)
	}

	// You can pass a timestamp, rather than using the AtNow call
	err = sender.
		Table("trades").
		Symbol("symbol", "BTC-USD").
		Symbol("side", "sell").
		Float64Column("price", 39269.98).
		Float64Column("amount", 0.001).
		At(ctx, tradedTs)
	if err != nil {
		log.Fatal(err)
	}

	tradedTs, err = time.Parse(time.RFC3339, "2022-08-06T15:04:06.987654Z")
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades_go").
		Symbol("pair", "GBPJPY").
		Symbol("type", "sell").
		Float64Column("traded_price", 135.97).
		Float64Column("limit_price", 0.84).
		Int64Column("qty", 400).
		At(ctx, tradedTs)
	if err != nil {
		log.Fatal(err)
	}

	// Make sure that the messages are sent over the network.
	err = sender.Flush(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
```

HTTP is the recommended transport to use. To connect via TCP, set the configuration string to:
```go
	// ...
	sender, err := qdb.LineSenderFromConf(ctx, "tcp::addr=localhost:9009;")
	// ...
```

## QuestDB Wire Protocol (QWP) over WebSocket

QWP is QuestDB's binary *columnar* wire protocol. Compared to ILP, it
offers higher throughput for wide rows and exposes the full QuestDB type
system — including `byte`, `short`, `int`, `float`, `char`, `date`,
nanosecond timestamps, `uuid`, `geohash`, `int64` arrays, and
fixed-width decimals.

Switch the Quickstart to QWP by changing the schema to `ws` (plain) or
`wss` (TLS):

```go
sender, err := qdb.LineSenderFromConf(ctx, "ws::addr=localhost:9000;")
```

The full fluent API shown in the Quickstart (`Table`, `Symbol`,
`Float64Column`, `Int64Column`, `At`, `AtNow`, `Flush`, `Close`) works
unchanged, as do the array and decimal methods shown below. QWP is a
distinct binary protocol rather than a version of ILP, so the
`protocol_version` configuration key does not apply.

### QWP-only column types

To access types that ILP does not expose, type-assert the sender to
`qdb.QwpSender`:

```go
sender, err := qdb.LineSenderFromConf(ctx, "ws::addr=localhost:9000;")
if err != nil {
    log.Fatal(err)
}
defer sender.Close(ctx)
qwp := sender.(qdb.QwpSender)

err = qwp.
    Table("sensors").
    Symbol("site", "roof").
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
    AtNano(ctx, time.Now())
```

`QwpSender` adds: `ByteColumn`, `ShortColumn`, `Int32Column`,
`Float32Column`, `CharColumn`, `DateColumn`, `TimestampNanosColumn`,
`UuidColumn`, `GeohashColumn`, `Int64Array1DColumn`,
`Int64Array2DColumn`, `Int64Array3DColumn`, `Decimal64Column`,
`Decimal128Column`, `Decimal256Column`, and `AtNano` (nanosecond-
resolution designated timestamp; `At` uses microseconds).

### Flush semantics and backpressure

The QWP sender always pipelines encoding with transmission: a dedicated
I/O goroutine drains a cursor engine to the WebSocket and owns reconnect
and replay. You do not configure a pipeline depth — backpressure is
governed by the engine's segment ring and the append deadline
(`sf_append_deadline_millis` in store-and-forward mode), not by a
fixed in-flight count.

There is no in-flight-window knob: the cursor architecture governs
backpressure on its own, so connect strings have no pipeline-depth key
and an unrecognized one is rejected as an unsupported option.

`Flush` and `FlushAndGetSequence` **never wait for the server ACK**.
They return once the batch is published into the cursor engine — in
RAM for memory mode, on disk for store-and-forward — after which the
I/O goroutine delivers and replays it in the background. A returned
`Flush` therefore means the batch is durably *published*, not that the
server has confirmed it: in memory mode, a process exit before the
background send completes can still lose unacked rows. Auto-flush
(triggered by row/byte/interval thresholds) follows the same
publish-only path. For server-ACK confirmation, `FlushAndGetSequence`
returns the published FSN (the upper bound of any `SenderError.ToFsn`
for that batch); pair it with `AwaitAckedFsn` to wait for the server
to confirm that FSN.

### Authentication

Basic auth and bearer tokens work the same way as for HTTP:

```go
qdb.LineSenderFromConf(ctx, "wss::addr=host:9000;username=admin;password=secret;")
qdb.LineSenderFromConf(ctx, "wss::addr=host:9000;token=<bearer>;")
```

`LineSenderPool` is HTTP-only and cannot be used with QWP — QWP's
cursor engine already pipelines transmission from a single sender.

### Error handling

When the server rejects a published QWP batch, the rejection surfaces
as a `*qdb.SenderError` carrying a stable `Category`
(`SCHEMA_MISMATCH`, `PARSE_ERROR`, `INTERNAL_ERROR`, `SECURITY_ERROR`,
`WRITE_ERROR`, `PROTOCOL_VIOLATION`, `UNKNOWN`), the server message,
and the `[FromFsn, ToFsn]` span — join that span against the value
returned by `FlushAndGetSequence` to identify exactly which rows were
rejected.

There are two delivery paths, both carrying the same payload:

```go
sender, err := qdb.NewLineSender(ctx,
    qdb.WithQwp(),
    qdb.WithAddress("localhost:9000"),
    // Async: dead-letter channel for DROP_AND_CONTINUE batches.
    qdb.WithErrorHandler(func(e *qdb.SenderError) {
        log.Printf("rejected fsn=[%d,%d] %s: %s",
            e.FromFsn, e.ToFsn, e.Category, e.ServerMessage)
    }),
)
// ...

// Sync: after a HALT, the typed error surfaces on the next
// producer-thread call (At / AtNow / Flush).
if err := sender.Flush(ctx); err != nil {
    var se *qdb.SenderError
    if errors.As(err, &se) {
        // inspect se.Category, se.ServerMessage, se.FromFsn, ...
    }
}
```

Each `Category` resolves to a `Policy` — `HALT` (latch the error;
the sender does not drain further until you close and rebuild it) or
`DROP_AND_CONTINUE` (drop the rejected span from the store and keep
going; recover the data via the async handler). Resolution precedence,
highest first: `WithErrorPolicyResolver` → `WithErrorPolicy(category,
policy)` → connect-string `on_<category>_error` → connect-string
`on_server_error` → spec defaults. `PROTOCOL_VIOLATION` and `UNKNOWN`
are always `HALT` and cannot be overridden.

The connect-string equivalents take `halt` / `drop` (and `auto` for
the global key):

```go
qdb.LineSenderFromConf(ctx,
    "ws::addr=localhost:9000;"+
    "on_server_error=halt;"+        // global default
    "on_schema_error=drop;"+        // per-category override
    "on_write_error=drop;")
```

Per-category keys are `on_schema_error`, `on_parse_error`,
`on_internal_error`, `on_security_error`, and `on_write_error`.

### Multi-host failover

`addr` accepts a comma-separated list (or repeated `addr=` keys) for
transparent failover. The client walks the list in priority order on
connect and reconnect; it does not shuffle or load-balance — that is
the server-side coordinator's job.

```go
qdb.LineSenderFromConf(ctx,
    "ws::addr=node-a:9000,node-b:9000,node-c:9000;")
```

`target` constrains which endpoints are acceptable by replicated-cluster
role: `any` (default), `primary` (writers only — also accepts
standalone OSS servers), or `replica`. `zone` is an opaque,
case-insensitive locality identifier (e.g. `eu-west-1a`); when set, the
client prefers same-zone endpoints. Both `target` and `zone` are
effective on the query side; for ingestion they are silently accepted
but have no effect — the ingestion path does not route by server role
or zone (role/zone-aware endpoint selection is a query-side feature).

```go
qdb.LineSenderFromConf(ctx,
    "ws::addr=node-a:9000,node-b:9000;target=primary;zone=eu-west-1a;")
```

The reconnect budget and backoff that govern how long failover persists
through an outage are the `reconnect_*` and `initial_connect_retry`
knobs documented under [QWP store-and-forward](#qwp-store-and-forward-sf)
— they apply whether or not `sf_dir` is set.

### Querying with `QwpQueryClient`

QWP also supports the query side: streaming columnar result batches
from the server back to the client over the same WebSocket protocol.
Use `QwpQueryClient` to run SELECT and DML statements:

```go
client, err := qdb.NewQwpQueryClient(ctx,
    qdb.WithQwpQueryAddress("localhost:9000"),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close(ctx)

// Non-SELECT statements use Exec.
if _, err := client.Exec(ctx,
    "CREATE TABLE example (ts TIMESTAMP, v LONG) TIMESTAMP(ts)"); err != nil {
    log.Fatal(err)
}

// SELECT returns a *QwpQuery; range over its Batches iterator.
q := client.Query(ctx, "SELECT ts, v FROM example")
defer q.Close()

var sum int64
for batch, err := range q.Batches() {
    if err != nil {
        log.Fatal(err)
    }
    vCol := batch.Column(1) // column 1 is `v` (LONG)
    for r := 0; r < vCol.RowCount(); r++ {
        sum += vCol.Int64(r)
    }
}
```

For tight column sweeps you can decode a row range into a caller-owned
slice in one shot. On a no-null column this lowers to a single
`memmove`, after which the inner loop is branch-free and vectorizable:

```go
buf := make([]int64, 0, 1024)
for batch, err := range q.Batches() {
    if err != nil {
        log.Fatal(err)
    }
    buf = batch.Column(1).Int64Range(0, batch.RowCount(), buf[:0])
    for _, v := range buf {
        sum += v
    }
}
```

Bind parameters are passed via `qdb.WithQwpQueryBinds` and use `$1`, `$2`,
... placeholders. Setters take 0-based indexes and must be called in
ascending order:

```go
q := client.Query(ctx,
    "SELECT ts, v FROM example WHERE v > $1",
    qdb.WithQwpQueryBinds(func(b *qdb.QwpBinds) {
        b.LongBind(0, 100)
    }),
)
```

Configuration via a config string is also supported:

```go
client, err := qdb.QwpQueryClientFromConf(ctx,
    "ws::addr=localhost:9000;username=admin;password=secret;")
```

`QwpQueryClient` is **not** safe for concurrent `Query` or `Exec` calls —
open one client per query-issuing goroutine. `Cancel` (on `*QwpQuery`)
and `Close` (on the client) are safe to call from any goroutine,
including from within an in-flight iterator.

A complete runnable example is at
[`examples/qwp/basic-query/main.go`](examples/qwp/basic-query/main.go).

## N-dimensional arrays

QuestDB server version 9.0.0 and newer supports n-dimensional arrays of double precision floating point numbers. 
The Go client provides several methods to send arrays to QuestDB:

### 1D Arrays

```go
// Send a 1D array of doubles
values1D := []float64{1.1, 2.2, 3.3, 4.4}
err = sender.
    Table("measurements").
    Symbol("sensor", "temp_probe_1").
    Float64Array1DColumn("readings", values1D).
    AtNow(ctx)
```

### 2D Arrays

```go
// Send a 2D array of doubles (must be rectangular)
values2D := [][]float64{
    {1.1, 2.2, 3.3},
    {4.4, 5.5, 6.6},
    {7.7, 8.8, 9.9},
}
err = sender.
    Table("matrix_data").
    Symbol("experiment", "test_001").
    Float64Array2DColumn("matrix", values2D).
    AtNow(ctx)
```

### 3D Arrays

```go
// Send a 3D array of doubles (must be regular cuboid shape)
values3D := [][][]float64{
    {{1.0, 2.0}, {3.0, 4.0}},
    {{5.0, 6.0}, {7.0, 8.0}},
}
err = sender.
    Table("tensor_data").
    Symbol("model", "neural_net_v1").
    Float64Array3DColumn("weights", values3D).
    AtNow(ctx)
```

### N-dimensional Arrays

For higher dimensions, use the `NewNDArray` function:

```go
// Create a 2x3x4 array
arr, err := qdb.NewNDArray[float64](2, 3, 4)
if err != nil {
    log.Fatal(err)
}

// Fill with values
arr.Fill(1.5)

// Or set individual values
arr.Set([]uint{0, 1, 2}, 42.0)

err = sender.
    Table("ndarray_data").
    Symbol("dataset", "training_batch_1").
    Float64ArrayNDColumn("features", arr).
    AtNow(ctx)
```

The array data is sent over a new protocol version (2) that is auto-negotiated
when using HTTP(s), or can be specified explicitly via the ``protocol_version=2``
parameter when using TCP(s).

We recommend using HTTP(s), but here is an TCP example, should you need it:

```go
sender, err := qdb.NewLineSender(ctx, 
    qdb.WithTcp(), 
    qdb.WithProtocolVersion(qdb.ProtocolVersion2))
```

When using ``protocol_version=2`` (with either TCP(s) or HTTP(s)), the sender
will now also serialize ``float64`` (double-precision) columns as binary.
You might see a performance uplift if this is a dominant data type in your
ingestion workload.

## Decimal columns

QuestDB server version 9.2.0 and newer supports decimal columns with arbitrary precision and scale.
The Go client converts supported decimal values to QuestDB's text/binary wire format automatically:

- `DecimalColumn`: `questdb.Decimal`, including helpers like `questdb.NewDecimalFromInt64` and `questdb.NewDecimal`.
- `DecimalColumnShopspring`: `github.com/shopspring/decimal.Decimal` values or pointers.
- `DecimalColumnFromString`: `string` literals representing decimal values (validated at runtime).

```go
price := qdb.NewDecimalFromInt64(12345, 2) // 123.45 with scale 2
commission := qdb.NewDecimal(big.NewInt(-750), 4) // -0.0750 with scale 4

err = sender.
    Table("trades").
    Symbol("symbol", "ETH-USD").
    DecimalColumn("price", price).
    DecimalColumn("commission", commission).
    AtNow(ctx)
```

To emit textual decimals, pass a validated string literal:

```go
err = sender.
    Table("quotes").
    DecimalColumnFromString("mid", "1.23456").
    AtNow(ctx)
```

## Pooled Line Senders

**Warning: Experimental feature designed for use with HTTP senders ONLY**

Version 3 of the client introduces a `LineSenderPool`, which provides a mechanism
to pool previously-used `LineSender`s so they can be reused without having
to allocate and instantiate new senders.

A LineSenderPool is thread-safe and can be used to concurrently obtain senders
across multiple goroutines.

Since `LineSender`s must be used in a single-threaded context, a typical pattern is to Acquire
a sender from a `LineSenderPool` at the beginning of a goroutine and use a deferred
execution block to Close the sender at the end of the goroutine.

Here is an example of the `LineSenderPool` Acquire, Release, and Close semantics:

```go
package main

import (
	"context"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()

	pool := qdb.PoolFromConf("http::addr=localhost:9000")
	defer func() {
		err := pool.Close(ctx)
		if err != nil {
			panic(err)
		}
	}()

	sender, err := pool.Sender(ctx)
	if err != nil {
		panic(err)
	}

	sender.Table("prices").
		Symbol("ticker", "AAPL").
		Float64Column("price", 123.45).
		AtNow(ctx)

	// Close call returns the sender back to the pool
	if err := sender.Close(ctx); err != nil {
		panic(err)
	}
}
```

## QWP store-and-forward (SF)

QuestDB's WebSocket transport (`ws::` / `wss::`, see Java client docs)
supports an opt-in **store-and-forward** mode: outgoing batches are
persisted to mmap'd disk segments before they leave the wire, and the
I/O loop replays from disk on transient disconnects or process
restarts. User code does not see brief outages; an unrecoverable
failure surfaces on the next `At` / `AtNow` / `Flush` call.

Activate SF by setting `sf_dir` (the parent directory under which the
sender's slot is created) on a `ws::` / `wss::` connection string:

```go
sender, err := qdb.LineSenderFromConf(ctx,
    "ws::addr=localhost:9000;"+
    "sf_dir=/var/lib/questdb-sf;"+
    "sender_id=my-app;"+
    "close_flush_timeout_millis=5000;")
```

The slot lives at `<sf_dir>/<sender_id>/`. An advisory exclusive
`flock` on `<slot>/.lock` prevents two senders from sharing a slot;
the lock releases automatically when the process exits.

### Connect-string knobs (QWP only)

| Key | Default | Effect |
|---|---|---|
| `sf_dir` | unset | Group root. Setting it activates SF. |
| `sender_id` | `default` | Per-sender slot name; ASCII letters / digits / `-_.` only. |
| `sf_max_segment_bytes` | 4 MiB | Per-segment file size. |
| `sf_max_total_bytes` | 10 GiB | Total cap; producer is backpressured when reached. |
| `sf_durability` | `memory` | Reserved; `flush` / `append` are deferred follow-ups. |
| `sf_append_deadline_millis` | 30000 | How long `At` / `AtNow` block on backpressure before failing. |
| `reconnect_max_duration_millis` | 300000 | Per-outage cap on reconnect retries. |
| `reconnect_initial_backoff_millis` | 100 | Initial backoff with jitter. |
| `reconnect_max_backoff_millis` | 5000 | Backoff cap. |
| `initial_connect_retry` | `off` | `off`/`false` = terminal on first failure; `on`/`true`/`sync` = same retry loop as reconnect, blocking the constructor; `async` = same retry loop on the I/O goroutine, constructor returns immediately and producers experience backpressure until the wire comes up. |
| `close_flush_timeout_millis` | 5000 | `Close` waits this long for ACKs; `0` / `-1` skips the drain. |
| `drain_orphans` | `off` | When `on`, scan `<sf_dir>/*` and adopt sibling slots that hold unacked data. |
| `max_background_drainers` | 4 | Cap on concurrent orphan drainers. |

The same options are available programmatically:
`WithSfDir`, `WithSenderId`, `WithSfMaxSegmentBytes`, `WithSfMaxTotalBytes`,
`WithReconnectPolicy`, `WithInitialConnectRetry`,
`WithInitialConnectMode`, `WithCloseFlushTimeout`.

### Failure semantics

- **Transient disconnect**: caught by the I/O loop, transparent to user code.
- **Auth rejection (HTTP 401/403)** on connect or reconnect: terminal — surfaced on the next user-thread call.
- **Server rejected a frame** (e.g. schema mismatch): terminal; replay would just rebound, so the loop stops and reports the rejection. Bytes stay on disk for inspection.
- **Reconnect cap exhausted**: terminal; restart the process to resume from disk.
- **Disk cap full**: `At` / `AtNow` block up to `sf_append_deadline_millis`, then fail with a "wire path is not draining" error.

### Crash recovery

On startup with the same `sf_dir` + `sender_id`, the sender opens
existing segment files, validates per-frame CRC32C, recovers any torn
tail at the active segment's last good frame, and resumes sending
where the prior session left off.

If a previous sender process crashed and left its slot dir behind,
turning on `drain_orphans=on` will scan sibling slots under `sf_dir`
and adopt them on a separate connection: the foreground sender is
unaffected, and a `.failed` sentinel is dropped if a drainer can't
make progress (auth rejection, exhausted reconnect cap, etc.).

## Community

If you need help, have additional questions or want to provide feedback, you
may find in our [Community Forum](https://community.questdb.io/).
You can also [sign up to our mailing list](https://questdb.io/contributors/)
to get notified of new releases.
