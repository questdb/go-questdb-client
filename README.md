[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/questdb/go-questdb-client/v3)

# go-questdb-client

Golang client for QuestDB's Influx Line Protocol over TCP and HTTP.

Features:
* Context-aware API.
* Optimized for batch writes.
* Supports TLS encryption and [ILP authentication](https://questdb.io/docs/reference/api/ilp/authenticate).
* Tested against QuestDB 7.3.9 and newer versions.

New in v3:
* Supports ILP over HTTP using the same client semantics

Documentation is available [here](https://pkg.go.dev/github.com/questdb/go-questdb-client/v3).

## Usage

### TCP

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
	// Connect to QuestDB running on 127.0.0.1:9009
	sender, err := qdb.NewLineSender(ctx)
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

### HTTP

Now supported in v3. For more information, please visit https://questdb.io/docs/reference/api/ilp/overview/#http-and-tcp-overview

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3/pkg/http"
)

func main() {
	ctx := context.TODO()
	// Connect to QuestDB running on 127.0.0.1:9000
	sender, err := qdb.NewLineSender()
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