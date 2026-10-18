package client

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// pipeServer reads frames without waiting for replies and serves each request
// on its own goroutine, so replies can come in any order. It answers Ping,
// Stat (size is the path length), Open, Fstat, ReadAt (count bytes of zeros,
// at most 4096) and Close.
type pipeServer struct {
	t    testing.TB
	ln   net.Listener
	path string
	wg   sync.WaitGroup

	advertise   bool   // report the pipelining capability in Hello
	maxInflight uint32 // max_inflight in Hello

	mu       sync.Mutex
	conns    []net.Conn
	out      map[int]int // requests received and not yet answered, per connection
	peak     int         // most requests outstanding at once on one connection
	peakAll  int         // most requests outstanding at once on all connections
	seen     int         // requests received, not counting Hello
	perConn  map[int]int // requests received per connection
	slow     map[string]time.Duration
	hang     map[string]bool // paths that never get a reply
	gateN    int             // Stat and ReadAt wait until gateN of them arrived
	gateSeen int
	gateOpen chan struct{}
	reverse  bool // after the gate, the last arrival answers first
	nextH    uint64
	slowMake time.Duration // pause before Open, Opendir and Watch reply
	undone   []string      // "close:N", "closedir:N", "unwatch:N" received
	quit     chan struct{} // closed by close, releases requests that never get a reply
	quitOnce sync.Once
}

func newPipeServer(t testing.TB, advertise bool, maxInflight uint32) *pipeServer {
	t.Helper()
	dir, err := os.MkdirTemp("", "mfp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := &pipeServer{
		t: t, path: filepath.Join(dir, "p.sock"), advertise: advertise, maxInflight: maxInflight,
		quit: make(chan struct{}), out: map[int]int{}, perConn: map[int]int{}, slow: map[string]time.Duration{}, hang: map[string]bool{},
	}
	if s.ln, err = net.Listen("unix", s.path); err != nil {
		t.Fatal(err)
	}
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(s.close)
	return s
}

func (s *pipeServer) accept() {
	defer s.wg.Done()
	for i := 0; ; i++ {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, c)
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(c, i)
	}
}

// close stops the listener, closes every connection and waits for every goroutine.
func (s *pipeServer) close() {
	s.quitOnce.Do(func() { close(s.quit) })
	s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.t.Errorf("pipe server goroutines still running")
	}
}

func (s *pipeServer) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// dropConns closes every accepted connection.
func (s *pipeServer) dropConns() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		c.Close()
	}
}

func (s *pipeServer) stats() (peak, peakAll, seen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak, s.peakAll, s.seen
}

// gate makes the next n Stat or ReadAt requests wait until all n arrived.
func (s *pipeServer) gate(n int, reverse bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gateN, s.gateSeen, s.gateOpen, s.reverse = n, 0, make(chan struct{}), reverse
}

func (s *pipeServer) serve(c net.Conn, idx int) {
	defer s.wg.Done()
	defer c.Close()
	var wmu sync.Mutex
	for {
		f, err := frame.Read(c, limits)
		if err != nil {
			return
		}
		if f.Method == mHello {
			caps := []string{"bulk_io", "ls_stat", "remove_all", "watch", "warm"}
			if s.advertise {
				caps = append(caps, capPipelining)
			}
			s.write(c, &wmu, f, &wire.HelloResponse{Version: protoVersion, MaxInflight: s.maxInflight, Capabilities: caps}, nil)
			continue
		}
		s.mu.Lock()
		s.seen++
		s.perConn[idx]++
		s.out[idx]++
		s.peak = max(s.peak, s.out[idx])
		all := 0
		for _, n := range s.out {
			all += n
		}
		s.peakAll = max(s.peakAll, all)
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.answer(c, &wmu, f)
			s.mu.Lock()
			s.out[idx]--
			s.mu.Unlock()
		}()
	}
}

func (s *pipeServer) write(c net.Conn, wmu *sync.Mutex, f frame.Frame, m proto.Message, data []byte) {
	meta, _ := proto.Marshal(m)
	wmu.Lock()
	defer wmu.Unlock()
	frame.Write(c, frame.Frame{Flags: frame.FlagResponse, Method: f.Method, RequestID: f.RequestID, Meta: meta, Payload: data}, limits)
}

func (s *pipeServer) answer(c net.Conn, wmu *sync.Mutex, f frame.Frame) {
	switch f.Method {
	case mStat:
		var r wire.StatRequest
		proto.Unmarshal(f.Meta, &r)
		s.mu.Lock()
		hang, pause := s.hang[r.Path], s.slow[r.Path]
		s.mu.Unlock()
		if hang {
			<-s.quit
			return
		}
		s.passGate()
		time.Sleep(pause)
		s.write(c, wmu, f, &wire.StatResponse{Info: &wire.FileInfo{Name: r.Path, Size: int64(len(r.Path))}}, nil)
	case mReadAt:
		var r wire.ReadAtRequest
		proto.Unmarshal(f.Meta, &r)
		s.passGate()
		n := min(int(r.Length), 4096)
		s.write(c, wmu, f, &wire.ReadAtResponse{Count: uint32(n)}, make([]byte, n))
	case mOpen, mOpendir:
		s.mu.Lock()
		s.nextH++
		h, pause := s.nextH, s.slowMake
		s.mu.Unlock()
		time.Sleep(pause)
		if f.Method == mOpen {
			s.write(c, wmu, f, &wire.OpenResponse{Handle: h}, nil)
		} else {
			s.write(c, wmu, f, &wire.OpendirResponse{Handle: h}, nil)
		}
	case mWatch:
		s.mu.Lock()
		pause := s.slowMake
		s.mu.Unlock()
		time.Sleep(pause)
		s.write(c, wmu, f, &wire.WatchResponse{WatchId: 77}, nil)
	case mClose:
		var r wire.CloseRequest
		proto.Unmarshal(f.Meta, &r)
		s.noteUndone(fmt.Sprintf("close:%d", r.Handle))
		s.write(c, wmu, f, &wire.Empty{}, nil)
	case mClosedir:
		var r wire.ClosedirRequest
		proto.Unmarshal(f.Meta, &r)
		s.noteUndone(fmt.Sprintf("closedir:%d", r.Handle))
		s.write(c, wmu, f, &wire.Empty{}, nil)
	case mUnwatch:
		var r wire.UnwatchRequest
		proto.Unmarshal(f.Meta, &r)
		s.noteUndone(fmt.Sprintf("unwatch:%d", r.WatchId))
		s.write(c, wmu, f, &wire.Empty{}, nil)
	case mFstat:
		s.write(c, wmu, f, &wire.FstatResponse{Info: &wire.FileInfo{Inode: 7}}, nil)
	default:
		s.write(c, wmu, f, &wire.Empty{}, nil)
	}
}

func (s *pipeServer) noteUndone(what string) {
	s.mu.Lock()
	s.undone = append(s.undone, what)
	s.mu.Unlock()
}

// passGate blocks while a gate waits for more requests.
func (s *pipeServer) passGate() {
	s.mu.Lock()
	if s.gateN == 0 {
		s.mu.Unlock()
		return
	}
	s.gateSeen++
	idx, n, open, rev := s.gateSeen, s.gateN, s.gateOpen, s.reverse
	if idx == n {
		close(open)
	}
	s.mu.Unlock()
	select {
	case <-open:
	case <-time.After(3 * time.Second):
		return
	}
	if rev {
		time.Sleep(time.Duration(n-idx) * 15 * time.Millisecond)
	}
}
