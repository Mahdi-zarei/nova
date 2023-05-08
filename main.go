package main

import (
	"errors"
	"github.com/google/uuid"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ender []byte
var linker map[string]map[int]*net.TCPConn
var locker sync.Mutex
var destIP string
var destPort int
var logger *log.Logger
var connCount int
var bufferSize int
var totalCount atomic.Int32

func main() {
	destIP = "127.0.1.1"
	destPort = 1194
	logger = log.New(os.Stdout, "", log.LstdFlags)
	bufferSize = 512 * 1024
	connCount = 4
	linker = make(map[string]map[int]*net.TCPConn)
	var tmp []string
	tmp = []string{"\000", "\002", "\004", "\000"}
	for _, x := range tmp {
		ender = append(ender, []byte(x)...)
	}

	srv, err := net.ListenTCP("tcp", &net.TCPAddr{Port: 1195})
	if err != nil {
		panic(err)
	}

	for {
		conn, err := srv.AcceptTCP()
		if err != nil {
			logger.Printf("error in accepting connection: %s", err)
			continue
		}
		conn.SetNoDelay(true)
		conn.SetKeepAlive(true)
		conn.SetKeepAlivePeriod(1 * time.Second)

		uid, seq, err := getIdentifier(conn)
		if err != nil {
			logger.Printf("error in getting identifying data: %s", err)
			conn.Close()
			continue
		}

		if mapConnection(uid, seq, conn) == connCount {
			logger.Printf("connections are all here, starting to forward")
			go handleConnections(uid, linker[uid])
		}
	}
}

func handleConnections(uid string, conns map[int]*net.TCPConn) {
	upstream, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   net.ParseIP(destIP),
		Port: destPort,
	})
	if err != nil {
		cleanUpConnections(uid)
		logger.Printf("error dialing the upstream server: %s", err)
		return
	}
	upstream.SetNoDelay(true)
	upstream.SetKeepAlive(true)
	upstream.SetKeepAlivePeriod(1 * time.Second)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer cleanUpConnections(uid)
		defer upstream.Close()
		handleManyToOneForward(upstream, conns)
		wg.Done()
	}()

	go func() {
		defer cleanUpConnections(uid)
		defer upstream.Close()
		handleOneToManyForward(upstream, conns)
		wg.Done()
	}()

	totalCount.Add(1)
	logger.Printf("starting forward for new client, count: %v", totalCount.Load())

	wg.Wait()
	totalCount.Add(-1)
	logger.Printf("forward stopped, client count: %v", totalCount.Load())
}

func handleManyToOneForward(dest *net.TCPConn, conns map[int]*net.TCPConn) {
	buffer := make([]byte, bufferSize)
	cnt := 0
	for {
		nr, err := conns[cnt].Read(buffer)
		if nr > 0 {

			if isComplete(buffer, nr) {
				nr = nr - 4
				cnt++
				cnt %= connCount
			}

			logger.Println("read ", cnt)
			_, err2 := dest.Write(buffer[:nr])
			if err2 != nil {
				logger.Printf("error in writing to openvpn connection: %s", err2)
				return
			}

		}

		if err != nil {
			logger.Printf("error in reading from mux connections: %s", err)
			return
		}
	}
}

func handleOneToManyForward(src *net.TCPConn, conns map[int]*net.TCPConn) {
	buffer := make([]byte, connCount*bufferSize)
	cnt := 0
	for {
		nr, err := src.Read(buffer)

		if nr > 0 {

			buffer, nr = appendToBuffer(buffer, nr)

			logger.Println("write ", cnt)
			_, err2 := conns[cnt].Write(buffer[:nr])
			if err2 != nil {
				logger.Printf("error in writing to mux connections: %s", err2)
				return
			}

			cnt++
			cnt %= connCount

		}

		if err != nil {
			logger.Printf("error in reading from openvpn connection: %s", err)
			return
		}
	}
}

func cleanUpConnections(uid string) {
	locker.Lock()
	defer locker.Unlock()
	conns, ok := linker[uid]
	if !ok {
		return
	}

	for _, conn := range conns {
		conn.Close()
	}
	delete(linker, uid)
}

func getIdentifier(conn *net.TCPConn) (string, int, error) {
	data := ""
	buffer := make([]byte, 1024)
	fullRead := 0

	for {
		nr, err := conn.Read(buffer)
		fullRead += nr
		if nr > 0 {
			logger.Printf("read [%s]", string(buffer[:nr]))
			if isComplete(buffer, nr) {
				data += convertToString(buffer, nr-4)
				spl := strings.Split(data, "#")
				uid, err := uuid.Parse(spl[0])
				if err != nil {
					return "", 0, err
				}
				seq, err := strconv.Atoi(spl[1])
				if err != nil {
					return "", 0, err
				}
				return uid.String(), seq, nil
			}
			data += convertToString(buffer, nr)
		}
		if err != nil {
			return "", 0, err
		}
		if fullRead > 150 {
			return "", 0, errors.New("unknown init packet format")
		}
	}
}

func convertToString(buffer []byte, nr int) string {
	res := ""
	for idx, b := range buffer {
		if idx >= nr {
			break
		}

		res += string(b)
	}
	return res
}

func isComplete(buffer []byte, nr int) bool {
	if nr >= len(ender) {

		for i := 0; i < len(ender); i++ {
			if buffer[nr-len(ender)+i] != ender[i] {
				return false
			}
		}

		return true
	}
	return false
}

func appendToBuffer(buffer []byte, nr int) ([]byte, int) {
	for i := 0; i < len(ender); i++ {
		buffer[nr+i] = ender[i]
	}

	return buffer, nr + len(ender)
}

func mapConnection(uid string, seq int, conn *net.TCPConn) int {
	locker.Lock()
	defer locker.Unlock()
	if _, ok := linker[uid]; !ok {
		linker[uid] = make(map[int]*net.TCPConn)
	}
	linker[uid][seq] = conn

	return len(linker[uid])
}
