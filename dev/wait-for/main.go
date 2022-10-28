package main

import (
	"log"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalln("Usage:", os.Args[0], "<addr1> [addr2, ...]")
	}

    for _, addr := range os.Args[1:] {
        ok := waitForPort(addr, time.Second, 30)
        if !ok {
            log.Fatalln("Couldn't connect to", addr)
        }
    }
}

func waitForPort(addr string, sleep time.Duration, limit int) bool {
	for i := 0; i < limit; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return true
		}

		if conn != nil {
			_ = conn.Close()
		}

		time.Sleep(sleep)
	}

	return false
}
