[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/questdb/go-questdb-client/v3)

# go-questdb-client

Golang client for QuestDB's [Influx Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/)
(ILP) over HTTP and TCP. This library makes it easy to insert data into
[QuestDB](https://questdb.io).

The library requires Go 1.19 or newer.

Features:
* Context-aware API.
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
	"fmt"
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
	defer sender.Close()
	// Send a few ILP messages.
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Float64Column("value", 12.4).
		AtNow(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp2").
		Float64Column("value", 11.4).
		At(ctx, time.Now().UnixNano())
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

To connect via TCP, set the configuration string to:
```go
	// ...
	sender, err := qdb.LineSenderFromConf(ctx, "tcp::addr=localhost:9009;")
	// ...
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
	defer sender.Close(context.TODO())
	// ...
}
```

Note that the migrated code uses the HTTP sender instead of the TCP one.

## Community

If you need help, have additional questions or want to provide feedback, you
may find us on [Slack](https://slack.questdb.io).
You can also [sign up to our mailing list](https://questdb.io/community/)
to get notified of new releases.
