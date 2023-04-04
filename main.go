package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var lg *log.Logger
var lim string
var portString string
var threshold int32

func main() {
	flag.StringVar(&lim, "lim", "200", "count of conns per 5 sec")
	flag.StringVar(&portString, "port", "6543", "port")
	flag.Parse()

	threshold = 100
	limInt, err := strconv.Atoi(lim)
	if err != nil {
		panic(err)
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		panic(err)
	}

	counter := map[string]int32{}
	blackList := map[string]struct{}{}
	blackList["198.244.191.140"] = struct{}{}
	syncer := sync.Mutex{}
	totMu := sync.Mutex{}
	totalCounter := 0
	lg = log.New(os.Stdout, "", log.Ltime)

	lg.Printf("starting service with lim %v", limInt)

	server, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: port,
	})
	if err != nil {
		panic(err)
	}

	go func() {
		ticker := time.Tick(5 * time.Second)
		for {
			select {
			case <-ticker:
				syncer.Lock()
				for key, val := range counter {

					val -= int32(limInt)

					if val < 0 {
						delete(counter, key)
						continue
					}

					counter[key] = val
				}
				syncer.Unlock()
			}
		}
	}()

	for {
		conn, err := server.AcceptTCP()
		if err != nil {
			continue
		}
		addr := conn.RemoteAddr()
		ip := strings.Split(addr.String(), ":")[0]

		if _, is := blackList[ip]; is {
			conn.Close()
			continue
		}

		go func() {
			lg.Printf("Incoming connection from [%s]", addr.String())
			conn.SetNoDelay(true)

			allowed := true
			syncer.Lock()
			v, ok := counter[ip]
			if !ok {
				v = 0
			}

			if v > threshold {
				lg.Printf("blacklisted %s", ip)
				blackList[ip] = struct{}{}
				allowed = false
			} else {
				lg.Printf("current count for [%s] is [%v]", ip, v)
				v++
				if v > int32(limInt) {
					allowed = false
				}
				counter[ip] = v
			}
			syncer.Unlock()

			if allowed {
				totMu.Lock()
				totalCounter++
				lg.Printf("total connection count: [%v]", totalCounter)
				totMu.Unlock()

				forwardConnection(conn)

				lg.Printf("closing connection for [%s]", ip)
				totMu.Lock()
				totalCounter--
				totMu.Unlock()
			}
			conn.Close()
		}()
	}
}

func forwardConnection(src *net.TCPConn) {
	dst, err := net.DialTCP("tcp", nil, &net.TCPAddr{Port: 888})
	if err != nil {
		return
	}

	src.SetKeepAlive(true)
	src.SetKeepAlivePeriod(5 * time.Second)

	dst.SetNoDelay(true)

	done := make(chan struct{})

	lg.Printf("starting relay from [%s] to [%s]", src.RemoteAddr().String(), dst.RemoteAddr().String())

	go func() {
		defer src.Close()
		defer dst.Close()
		_, err := io.Copy(dst, src)
		if err != nil {
			lg.Printf("Forward from dst to src stopped with [%s]", err)
		}
		done <- struct{}{}
	}()

	go func() {
		defer src.Close()
		defer dst.Close()
		_, err := io.Copy(src, dst)
		if err != nil {
			lg.Printf("Forward from src to dst stopped with [%s]", err)
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}
