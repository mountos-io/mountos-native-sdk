package client

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func dialPipe(t testing.TB, s *pipeServer, o Options) *Client {
	t.Helper()
	if o.Logger == nil {
		o.Logger = &bufLogger{}
	}
	cl, err := Dial(s.path, o)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

// waitSeen waits until the server received n requests.
func waitSeen(t *testing.T, s *pipeServer, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, seen := s.stats(); seen >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	_, _, seen := s.stats()
	t.Fatalf("server saw %d requests, want %d", seen, n)
}

// statAll runs one Stat per path on its own goroutine and checks each result.
func statAll(t *testing.T, cl *Client, paths []string) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, len(paths))
	for _, p := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := cl.Stat(p)
			switch {
			case err != nil:
				errs <- fmt.Errorf("stat %s: %w", p, err)
			case info.GetSize() != int64(len(p)):
				errs <- fmt.Errorf("stat %s: size %d, got another caller's reply", p, info.GetSize())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func distinctPaths(n int) []string {
	paths := make([]string, n)
	for i := range paths {
		paths[i] = "/" + strings.Repeat("a", i+1)
	}
	return paths
}

// Calls on one connection are in flight together. The server holds every reply
// until it has seen all of them, so a client that sends one at a time stalls.
func TestPipelinedCallsOverlap(t *testing.T) {
	const n = 8
	s := newPipeServer(t, true, 32)
	s.gate(n, false)
	cl := dialPipe(t, s, Options{PoolSize: 1})
	start := time.Now()
	statAll(t, cl, distinctPaths(n))
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("calls took %v, they did not overlap", d)
	}
	if peak, _, _ := s.stats(); peak != n {
		t.Fatalf("peak requests in flight = %d, want %d", peak, n)
	}
	if s.connCount() != 1 {
		t.Fatalf("connections = %d, want 1", s.connCount())
	}
}

// The server answers in reverse arrival order. Each caller still gets its own reply.
func TestPipelinedRepliesOutOfOrder(t *testing.T) {
	const n = 6
	s := newPipeServer(t, true, 32)
	s.gate(n, true)
	cl := dialPipe(t, s, Options{PoolSize: 1})
	statAll(t, cl, distinctPaths(n))
}

// A call that times out fails alone. Its late reply is discarded, other calls
// are not affected, and the connection stays up.
func TestLateReplyAfterTimeoutDiscarded(t *testing.T) {
	s := newPipeServer(t, true, 32)
	s.slow["/slow"] = 500 * time.Millisecond
	cl := dialPipe(t, s, Options{PoolSize: 1, CallTimeout: 150 * time.Millisecond})
	events, _ := collect(cl)
	slowErr := make(chan error, 1)
	go func() { _, err := cl.Stat("/slow"); slowErr <- err }()
	waitSeen(t, s, 1)
	if _, err := cl.Stat("/ok"); err != nil {
		t.Fatalf("a fast call beside a slow one = %v", err)
	}
	if err := <-slowErr; !isTimeout(err) {
		t.Fatalf("slow call = %v, want a timeout", err)
	}
	sk := cl.conns[0].cur.Load()
	abandoned := func() int {
		sk.pmu.Lock()
		defer sk.pmu.Unlock()
		return len(sk.abandoned)
	}
	if n := abandoned(); n != 1 {
		t.Fatalf("abandoned ids = %d, want 1", n)
	}
	deadline := time.Now().Add(3 * time.Second)
	for abandoned() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := abandoned(); n != 0 {
		t.Fatalf("late reply left %d abandoned ids", n)
	}
	for i := 0; i < 3; i++ {
		if info, err := cl.Stat("/later"); err != nil || info.GetSize() != int64(len("/later")) {
			t.Fatalf("call %d after the late reply = %v", i, err)
		}
	}
	if s.connCount() != 1 {
		t.Fatalf("connections = %d, the timeout must keep the connection", s.connCount())
	}
	select {
	case e := <-events:
		t.Fatalf("event %+v after a timeout that keeps the connection", e)
	default:
	}
}

// More abandoned ids than the cap drop the connection. The next call dials again.
func TestAbandonedOverflowRedials(t *testing.T) {
	s := newPipeServer(t, true, 2)
	s.hang["/h"] = true
	cl := dialPipe(t, s, Options{PoolSize: 1, CallTimeout: 30 * time.Millisecond})
	events, _ := collect(cl)
	sk := cl.conns[0].cur.Load()
	if sk.abandonCap != 2*abandonFactor {
		t.Fatalf("abandon cap = %d", sk.abandonCap)
	}
	for i := 0; i < sk.abandonCap; i++ {
		if _, err := cl.Stat("/h"); !isTimeout(err) {
			t.Fatalf("call %d = %v", i, err)
		}
	}
	if s.connCount() != 1 || sk.isDead() {
		t.Fatalf("connection lost within the abandon budget")
	}
	if _, err := cl.Stat("/h"); !isTimeout(err) {
		t.Fatalf("overflow call = %v", err)
	}
	if e := recvEv(t, events); e.Kind != EventConnLost {
		t.Fatalf("event = %+v", e)
	}
	if _, err := cl.Stat("/ok"); err != nil {
		t.Fatalf("call after the redial = %v", err)
	}
	if s.connCount() != 2 {
		t.Fatalf("connections = %d, want a redial", s.connCount())
	}
}

// Without the capability, with max_inflight 1, or with Options.MaxInflight 1,
// the server never sees two requests at once. A higher limit is respected.
func TestInflightLimit(t *testing.T) {
	cases := []struct {
		name         string
		advertise    bool
		serverMax    uint32
		optMax       int
		wantPeak     int
		wantAtLeast2 bool
	}{
		{"no capability", false, 8, 0, 1, false},
		{"max_inflight 1", true, 1, 0, 1, false},
		{"max_inflight 0", true, 0, 0, 1, false},
		{"client limit 1", true, 8, 1, 1, false},
		{"client limit 3", true, 8, 3, 3, true},
		{"server limit 2", true, 2, 0, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newPipeServer(t, tc.advertise, tc.serverMax)
			s.slow["/s"] = 20 * time.Millisecond
			cl := dialPipe(t, s, Options{PoolSize: 1, MaxInflight: tc.optMax})
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 5; i++ {
						if _, err := cl.Stat("/s"); err != nil {
							t.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			peak, _, _ := s.stats()
			if peak > tc.wantPeak {
				t.Fatalf("peak in flight = %d, limit %d", peak, tc.wantPeak)
			}
			if tc.wantAtLeast2 && peak < 2 {
				t.Fatalf("peak in flight = %d, calls did not overlap", peak)
			}
		})
	}
}

// A lost transport fails every call outstanding on it with the connection-lost
// error, and the next call dials again.
func TestTransportErrorFailsAllOutstanding(t *testing.T) {
	const n = 5
	s := newPipeServer(t, true, 32)
	s.hang["/h"] = true
	cl := dialPipe(t, s, Options{PoolSize: 1})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() { _, err := cl.Stat("/h"); errs <- err }()
	}
	waitSeen(t, s, n)
	s.dropConns()
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if !isConnLost(err) {
				t.Fatalf("outstanding call = %v, want a connection-lost error", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("outstanding call still waiting after the transport closed")
		}
	}
	if _, err := cl.Stat("/ok"); err != nil {
		t.Fatalf("call after the redial = %v", err)
	}
}

// Close with calls outstanding returns them all and leaves no goroutine.
func TestCloseWithOutstandingLeavesNoGoroutines(t *testing.T) {
	base := runtime.NumGoroutine()
	s := newPipeServer(t, true, 32)
	s.hang["/h"] = true
	cl, err := Dial(s.path, Options{PoolSize: 2, Logger: &bufLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	const n = 6
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() { _, err := cl.Stat("/h"); errs <- err }()
	}
	waitSeen(t, s, n)
	cl.Close()
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("call succeeded on a closed client")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("call still waiting after Close")
		}
	}
	if _, err := cl.Stat("/x"); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("call after Close = %v", err)
	}
	s.close()
	settleGoroutines(t, base)
}

// With several connections the pool sends each call to the connection with the
// fewest outstanding calls.
func TestPoolPicksLeastLoaded(t *testing.T) {
	const n = 6
	s := newPipeServer(t, true, 32)
	s.gate(n, false)
	cl := dialPipe(t, s, Options{PoolSize: 2})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cl.Stat("/a"); err != nil {
				t.Error(err)
			}
		}()
		waitSeen(t, s, i+1)
	}
	wg.Wait()
	s.mu.Lock()
	a, b := s.perConn[0], s.perConn[1]
	s.mu.Unlock()
	if a != n/2 || b != n/2 {
		t.Fatalf("calls per connection = %d and %d, want %d each", a, b, n/2)
	}
}

// Different Files run on one connection at the same time.
func TestFilesOverlapOnOneConnection(t *testing.T) {
	const n = 4
	s := newPipeServer(t, true, 32)
	cl := dialPipe(t, s, Options{PoolSize: 1})
	files := make([]*File, n)
	for i := range files {
		f, err := cl.Open(fmt.Sprintf("/f%d", i), rw, create)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = f
	}
	s.gate(n, true)
	var wg sync.WaitGroup
	for _, f := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 16)
			if got, err := f.ReadAt(buf, 0); err != nil || got != 16 {
				t.Errorf("read = %d, %v", got, err)
			}
		}()
	}
	wg.Wait()
	if peak, _, _ := s.stats(); peak != n {
		t.Fatalf("peak requests in flight = %d, want %d", peak, n)
	}
}

// Many goroutines mix path calls and File calls on a small pool. Every result must be its own.
func TestPipelinedMixedStress(t *testing.T) {
	s := newPipeServer(t, true, 8)
	cl := dialPipe(t, s, Options{PoolSize: 2, MaxInflight: 6})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/g%d", g)
			for i := 0; i < 100; i++ {
				switch i % 3 {
				case 0:
					info, err := cl.Stat(path)
					if err != nil || info.GetSize() != int64(len(path)) {
						t.Errorf("stat %s = %v", path, err)
						return
					}
				case 1:
					f, err := cl.Open(path, rw, create)
					if err != nil {
						t.Errorf("open = %v", err)
						return
					}
					if n, err := f.ReadAt(make([]byte, 64), 0); err != nil || n != 64 {
						t.Errorf("read = %d, %v", n, err)
					}
					if err := f.Close(); err != nil {
						t.Errorf("close = %v", err)
					}
				default:
					if err := cl.Ping(); err != nil {
						t.Errorf("ping = %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

// A request that opens a handle or a watch can succeed on the server after the
// caller gave up. The late reply is discarded and the client releases the
// resource, so timeouts do not leak handles on a connection that stays up.
func TestLateOpenReplyReleasesServerResource(t *testing.T) {
	cases := []struct {
		name string
		call func(cl *Client) error
		want string
	}{
		{"open", func(cl *Client) error { _, err := cl.Open("/f", rw, create); return err }, "close:1"},
		{"opendir", func(cl *Client) error { _, err := cl.Opendir("/d"); return err }, "closedir:1"},
		{"watch", func(cl *Client) error { _, err := cl.Watch("/d", true, 0); return err }, "unwatch:77"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newPipeServer(t, true, 32)
			s.slowMake = 300 * time.Millisecond
			cl := dialPipe(t, s, Options{PoolSize: 1, CallTimeout: 100 * time.Millisecond})
			if err := tc.call(cl); !isTimeout(err) {
				t.Fatalf("call = %v, want a timeout", err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				s.mu.Lock()
				got := append([]string(nil), s.undone...)
				s.mu.Unlock()
				if len(got) == 1 && got[0] == tc.want {
					if s.connCount() != 1 {
						t.Fatalf("connections = %d, want 1", s.connCount())
					}
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatalf("server never got %s", tc.want)
		})
	}
}
