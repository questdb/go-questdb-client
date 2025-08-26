[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/questdb/go-questdb-client/v4)

# go-questdb-client

Golang client for QuestDB's [Influx Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/)
(ILP) over HTTP and TCP. This library makes it easy to insert data into
[QuestDB](https://questdb.io).

The library requires Go 1.19 or newer.

Features:
* [Context](https://www.digitalocean.com/community/tutorials/how-to-use-contexts-in-go)-aware API.
* Optimized for batch writes.
* Supports TLS encryption and ILP authentication.
* Automatic write retries and connection reuse for ILP over HTTP.
* Tested against QuestDB 7.3.10 and newer versions.

New in v4:
* Supports n-dimensional arrays of doubles for QuestDB servers 9.0.0 and up

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

## Community

If you need help, have additional questions or want to provide feedback, you
may find in our [Community Forum](https://community.questdb.io/).
You can also [sign up to our mailing list](https://questdb.io/contributors/)
to get notified of new releases.
