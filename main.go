package main

import (
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var lg *log.Logger

func main() {
	counter := map[string]int32{}
	syncer := sync.Mutex{}
	lg = log.New(os.Stdout, "", log.Ltime)

	lg.Printf("sfsadja %s", "sda")

	server, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: 6543,
	})
	if err != nil {
		panic(err)
	}

	go func() {
		ticker := time.Tick(5 * time.Second)
		select {
		case <-ticker:
			syncer.Lock()
			for key, val := range counter {
				val -= 1000

				if val < 0 {
					delete(counter, key)
					continue
				}

				counter[key] = val
			}
			syncer.Unlock()
		}
	}()

	for {
		conn, err := server.AcceptTCP()
		if err != nil {
			continue
		}
		conn.SetNoDelay(true)
		lg.Printf("Incoming connection from %s", conn.RemoteAddr().String())

		go func() {
			addr := conn.RemoteAddr()
			ip := strings.Split(addr.String(), ":")[0]

			allowed := true
			syncer.Lock()
			v, ok := counter[ip]
			if !ok {
				v = 0
			}
			lg.Printf("Current count for %s is %v", addr.String(), v)
			v++
			if v > 1000 {
				allowed = false
			}
			counter[ip] = v
			syncer.Unlock()

			if allowed {
				forwardConnection(conn)
			}

			conn.Close()
		}()
	}
}

func forwardConnection(src net.Conn) {
	dst, err := net.Dial("tcp", ":888")
	if err != nil {
		return
	}

	done := make(chan struct{})

	lg.Printf("starting relay from %s to %s", src.RemoteAddr().String(), dst.RemoteAddr().String())

	go func() {
		defer src.Close()
		defer dst.Close()
		io.Copy(dst, src)
		done <- struct{}{}
	}()

	go func() {
		defer src.Close()
		defer dst.Close()
		io.Copy(src, dst)
		done <- struct{}{}
	}()

	<-done
	<-done
}
