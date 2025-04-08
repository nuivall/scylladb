package main

import (
	"fmt"
	"sync"

	//"strings"
	"net"
	"time"

	"sync/atomic"

	"github.com/gocql/gocql"
)

const (
	scyllaHost = "127.0.0.1"
	numGoroutines = 10000
	loopCount    = 300
)

var oks int64 = 0

func connectAndQuery(id int) {
	for i := 0; i < loopCount; i++ {
		if ((id * i) % 1000 == 0) {
			fmt.Printf("Oks conns %d\n", atomic.LoadInt64(&oks))
		}
		cluster := gocql.NewCluster(scyllaHost)
		if false {
			cluster.Authenticator = gocql.PasswordAuthenticator{
				Username: "cassandra",
				Password: "cassandra",
			}
		}
		cluster.Dialer = &net.Dialer{
			Timeout:   2 * time.Second, // Set a timeout for the connection
			KeepAlive: 0,               // Disable TCP KeepAlive
		}
		session, err := cluster.CreateSession()
		if err != nil {
			//fmt.Printf("Goroutine %d, iteration %d: failed to connect: %v\n", id, i, err)

			// if (!strings.Contains(fmt.Sprint(err), "Unknown type of response to startup frame: gocql.errorFrame")) {
			// 	fmt.Printf("Goroutine %d, iteration %d: failed to connect: %v", id, i, err)
			// }
			continue
		}
		atomic.AddInt64(&oks, 1)
		session.Close()
	}
}

func main() {
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			connectAndQuery(id)
		}(i)
	}
	wg.Wait()
	fmt.Printf("All goroutines finished execution, oks %d\n", oks)
}
