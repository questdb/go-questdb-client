[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/questdb/go-questdb-client/v3)

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

New in v3:
* Supports ILP over HTTP using the same client semantics

Documentation is available [here](https://pkg.go.dev/github.com/questdb/go-questdb-client/v3).

## Quickstart

```go
package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
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

	qdb "github.com/questdb/go-questdb-client/v3"
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

## Migration from v2

v2 code example:
```go
package main

import (
	"context"

	qdb "github.com/questdb/go-questdb-client/v2"
)

func main() {
	// Connect to QuestDB running on 127.0.0.1:9009 (ILP/TCP)
	sender, err := qdb.NewLineSender(context.TODO())
	// ...
	defer sender.Close()
	// ...
}
```

Migrated v3 code:
```go
package main

import (
	"context"

	qdb "github.com/questdb/go-questdb-client/v3"
)

func main() {
	// Connect to QuestDB running on 127.0.0.1:9000 (ILP/HTTP)
	sender, err := qdb.NewLineSender(context.TODO(), qdb.WithHTTP())
	// Alternatively, you can use the LineSenderFromConf function:
	// sender, err := qdb.LineSenderFromConf(ctx, "http::addr=localhost:9000;")
	// ...
	// or you can export the "http::addr=localhost:9000;" config string to
	// the QDB_CLIENT_CONF environment variable and use the LineSenderFromEnv function:
	// sender, err := qdb.LineSenderFromEnv(ctx)
	// ...
	defer sender.Close(context.TODO())
	// ...
}
```

Note that the migrated code uses the HTTP sender instead of the TCP one.

## Community

If you need help, have additional questions or want to provide feedback, you
may find in our [Community Forum](https://community.questdb.io/).
You can also [sign up to our mailing list](https://questdb.io/contributors/)
to get notified of new releases.
