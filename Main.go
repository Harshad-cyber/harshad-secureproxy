package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct {
	URL   string
	Alive bool
	mux   sync.RWMutex
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mux.RLock()
	alive := b.Alive
	b.mux.RUnlock()
	return alive
}

var backendPool = []*Backend{
	{URL: "127.0.0.1:8081", Alive: true},
	{URL: "127.0.0.1:8082", Alive: true},
	{URL: "127.0.0.1:8083", Alive: true},
}

var requestCounter uint64

func getNextPeer() *Backend {
	n := uint64(len(backendPool))
	for i := uint64(0); i < n; i++ {
		idx := atomic.AddUint64(&requestCounter, 1) % n
		if backendPool[idx].IsAlive() {
			return backendPool[idx]
		}
	}
	return nil
}

type RateLimiter struct {
	sync.Mutex
	lastRequest time.Time
	rate        time.Duration
}

var limiter = &RateLimiter{
	rate: 500 * time.Millisecond,
}

func (rl *RateLimiter) Allow() bool {
	rl.Lock()
	defer rl.Unlock()
	
	now := time.Now()
	if now.Sub(rl.lastRequest) < rl.rate {
		return false
	}
	rl.lastRequest = now
	return true
}

func startHealthCheck() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		for _, b := range backendPool {
			conn, err := net.DialTimeout("tcp", b.URL, 1*time.Second)
			if err != nil {
				if b.IsAlive() {
					b.SetAlive(false)
					fmt.Printf("🔴 CRITICAL ALERT: Server Offline -> %s\n", b.URL)
				}
			} else {
				conn.Close()
				if !b.IsAlive() {
					b.SetAlive(true)
					fmt.Printf("🟢 SYSTEM RECOVERY: Server Resumed -> %s\n", b.URL)
				}
			}
		}
	}
}

func handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	if !limiter.Allow() {
		fmt.Printf("⚠️ SECURITY WARNING: Rate Limit Exceeded! Dropping connection.\n")
		fmt.Fprint(clientConn, "HTTP/1.1 429 Too Many Requests\r\n\r\nSecurity Alert: Too many requests! Connection blocked.")
		return
	}

	targetBackend := getNextPeer()
	if targetBackend == nil {
		fmt.Fprint(clientConn, "HTTP/1.1 503 Service Unavailable\r\n\r\nInfrastructure Alert: No backend routing available.")
		return
	}

	backendConn, err := net.DialTimeout("tcp", targetBackend.URL, 2*time.Second)
	if err != nil {
		fmt.Printf("❌ INFRASTRUCTURE ERROR: Failed to route to %s: %v\n", targetBackend.URL, err)
		targetBackend.SetAlive(false)
		return
	}
	defer backendConn.Close()

	go io.Copy(backendConn, clientConn)
	io.Copy(clientConn, backendConn)
}

func main() {
	go startHealthCheck()

	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		panic(err)
	}
	fmt.Println("🛡️ Harshad-SecureProxy Core Engine successfully active on Port 8000...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn)
	}
}
