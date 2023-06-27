package main

import (
	"context"
	qdb "github.com/questdb/go-questdb-client"
	"log"
	"sync"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	ctx := context.TODO()
	// Connect to QuestDB running on 127.0.0.1:9009
	sender, err := qdb.NewLineSender(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Make sure to close the sender on exit to release resources.
	defer sender.Close()

	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Millisecond * 500)
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					defer wg.Done()
					// Send a few ILP messages.
					err = sender.
						Table("trades").
						Symbol("name", "test_ilp1").
						Float64Column("value", 12.4).
						At(ctx, time.Now().UnixNano())
					if err != nil {
						log.Printf("error sending message: %v", err)
					}

					// Make sure that the messages are sent over the network.
					err = sender.Flush(ctx)
					if err != nil {
						log.Printf("error Flush message: %v", err)
					} else {
						log.Printf("Flush message success")
					}
				}()
			}
			log.Printf("waiting for messages to be sent")
			wg.Wait()
		}
	}

	log.Printf("done")
}
