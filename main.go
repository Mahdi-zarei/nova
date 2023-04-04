package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var lg *log.Logger
var lim string
var portString string
var threshold int32
var forwPortString string
var dstPort int
var thString string
var connPoolSize string

func main() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	flag.StringVar(&lim, "lim", "200", "count of conns per 5 sec")
	flag.StringVar(&portString, "port", "6543", "port")
	flag.StringVar(&forwPortString, "frw", "888", "forwarding port")
	flag.StringVar(&thString, "th", "100", "threshold")
	flag.StringVar(&connPoolSize, "pool", "100", "pool size")
	flag.Parse()

	tmp, err := strconv.Atoi(thString)
	if err != nil {
		panic(err)
	}

	threshold = int32(tmp)

	poolSize, err := strconv.Atoi(connPoolSize)
	if err != nil {
		panic(err)
	}

	limInt, err := strconv.Atoi(lim)
	if err != nil {
		panic(err)
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		panic(err)
	}

	dstPort, err = strconv.Atoi(forwPortString)
	if err != nil {
		panic(err)
	}

	counter := map[string]int32{}
	blackList := map[string]struct{}{}
	blackList["198.244.191.140"] = struct{}{}
	syncer := sync.Mutex{}
	totMu := sync.Mutex{}
	var pool []*net.TCPConn
	poolSyncer := sync.Mutex{}
	totalCounter := 0
	lg = log.New(os.Stdout, "", log.Ltime)

	for i := 0; i < poolSize; i++ {
		cn, err := net.DialTCP("tcp", nil, &net.TCPAddr{Port: dstPort})
		if err != nil {
			panic(err)
		}

		pool = append(pool, cn)
	}

	go func() {
		<-c
		fmt.Println(blackList)
		os.Exit(0)
	}()

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

				can := true
				dst := &net.TCPConn{}
				poolSyncer.Lock()
				if len(pool) == 0 {
					lg.Printf("pool is fool!")
					can = false
				} else {
					dst = pool[0]
					pool = pool[1:]
				}
				poolSyncer.Unlock()
				if can {
					totMu.Lock()
					totalCounter++
					lg.Printf("total connection count: [%v]", totalCounter)
					totMu.Unlock()

					forwardConnection(conn, dst)

					poolSyncer.Lock()
					pool = append(pool, dst)
					poolSyncer.Unlock()

					lg.Printf("closing connection for [%s]", ip)
					totMu.Lock()
					totalCounter--
					totMu.Unlock()
				}

			}
			conn.Close()
		}()
	}
}

func forwardConnection(src *net.TCPConn, dst *net.TCPConn) {
	src.SetKeepAlive(true)
	src.SetKeepAlivePeriod(5 * time.Second)

	dst.SetNoDelay(true)

	done := make(chan struct{})

	lg.Printf("starting relay from [%s] to [%s]", src.RemoteAddr().String(), dst.RemoteAddr().String())

	go func() {
		defer src.Close()
		_, err := io.Copy(dst, src)
		if err != nil {
			lg.Printf("Forward from dst to src stopped with [%s]", err)
		}
		done <- struct{}{}
	}()

	go func() {
		defer src.Close()
		_, err := io.Copy(src, dst)
		if err != nil {
			lg.Printf("Forward from src to dst stopped with [%s]", err)
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}
