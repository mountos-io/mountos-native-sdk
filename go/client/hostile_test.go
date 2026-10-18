package client

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// hostileServer completes Hello on every connection. The first connection
// then misbehaves through act on each later request. Any later connection
// answers every request with an empty success, so a redialed client recovers.
type hostileServer struct {
	t    *testing.T
	ln   net.Listener
	path string
	wg   sync.WaitGroup

	mu    sync.Mutex
	conns int
}

// act handles one request on the hostile connection. It returns false when the
// connection is done: the server then waits for the client to close it.
type act func(c net.Conn, f frame.Frame) bool

func newHostileServer(t *testing.T, a act) *hostileServer {
	t.Helper()
	dir, err := os.MkdirTemp("", "mfh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := &hostileServer{t: t, path: filepath.Join(dir, "h.sock")}
	if s.ln, err = net.Listen("unix", s.path); err != nil {
		t.Fatal(err)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := s.ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			n := s.conns
			s.conns++
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(c, n == 0, a)
			}()
		}
	}()
	return s
}

func (s *hostileServer) serve(c net.Conn, hostile bool, a act) {
	defer c.Close()
	for {
		f, err := frame.Read(c, limits)
		if err != nil {
			return
		}
		if f.Method == mHello {
			sendOK(c, f, &wire.HelloResponse{Version: protoVersion, MaxInflight: 1})
			continue
		}
		if !hostile {
			sendOK(c, f, &wire.Empty{})
			continue
		}
		if !a(c, f) {
			io.Copy(io.Discard, c) // hold the socket until the client closes it
			return
		}
	}
}

// close stops the listener and waits for every server goroutine. A goroutine
// still blocked means the client left a socket open.
func (s *hostileServer) close() {
	s.ln.Close()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		s.t.Errorf("server connection still open after client Close: client leaked a socket")
	}
}

func sendOK(c net.Conn, f frame.Frame, m proto.Message) {
	meta, _ := proto.Marshal(m)
	frame.Write(c, frame.Frame{Flags: frame.FlagResponse, Method: f.Method, RequestID: f.RequestID, Meta: meta}, limits)
}

// header builds a raw frame header with arbitrary section lengths.
func rawHeader(flags uint8, method uint16, id uint64, metaLen, payloadLen uint32) []byte {
	b := make([]byte, 0, frame.HeaderSize)
	b = append(b, 'M', 'F', 'S', '1', frame.Version, flags, byte(method>>8), byte(method))
	for s := 56; s >= 0; s -= 8 {
		b = append(b, byte(id>>uint(s)))
	}
	for _, n := range []uint32{metaLen, payloadLen} {
		b = append(b, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return b
}

// hostileCase is one misbehavior and the client call that meets it.
type hostileCase struct {
	act  act
	call func(cl *Client) error
	// wantErr checks the typed error. A nil wantErr means the call may succeed or fail.
	wantErr func(err error) bool
	name    string
	timeout time.Duration // Options.CallTimeout
	// retries is how many calls after the hostile one may fail before the client works again.
	retries int
}

func ping(cl *Client) error { return cl.Ping() }

func isProto(err error) bool { var pe *ProtocolError; return errors.As(err, &pe) }
func isConnLost(err error) bool {
	return isProto(err) && errors.Is(err, ErrConnLost)
}
func isTimeout(err error) bool {
	var te *TimeoutError
	return errors.As(err, &te) && te.Code() == CodeDeadlineExceeded
}

func hostileCases() []hostileCase {
	garbage := make([]byte, 128)
	rand.New(rand.NewSource(7)).Read(garbage)
	copy(garbage, "XXXX") // never a valid magic
	reply := func(flags uint8, method uint16, id uint64, meta []byte) []byte {
		return append(rawHeader(flags, method, id, uint32(len(meta)), 0), meta...)
	}
	good := func(f frame.Frame) []byte {
		return reply(frame.FlagResponse, f.Method, f.RequestID, nil)
	}
	return []hostileCase{
		{name: "garbage bytes", call: ping, wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool { c.Write(garbage); return false }},
		{name: "bad version", call: ping, wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool {
				h := rawHeader(frame.FlagResponse, f.Method, f.RequestID, 0, 0)
				h[4] = 9
				c.Write(h)
				return false
			}},
		{name: "claim over the limit", call: ping, wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(rawHeader(frame.FlagResponse, f.Method, f.RequestID, 0, maxPayloadBytes+1))
				return false
			}},
		{name: "huge claim then stall", call: ping, timeout: 300 * time.Millisecond, wantErr: isTimeout,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(rawHeader(frame.FlagResponse, f.Method, f.RequestID, 0, maxPayloadBytes))
				c.Write(make([]byte, 1000))
				return false
			}},
		{name: "huge meta claim then stall", call: ping, timeout: 300 * time.Millisecond, wantErr: isTimeout,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(rawHeader(frame.FlagResponse, f.Method, f.RequestID, maxMetaBytes, 0))
				return false
			}},
		{name: "truncated header then close", call: ping, wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(rawHeader(frame.FlagResponse, f.Method, f.RequestID, 0, 0)[:11])
				c.Close()
				return false
			}},
		{name: "truncated body then close", call: ping, wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(rawHeader(frame.FlagResponse, f.Method, f.RequestID, 40, 0))
				c.Write([]byte("short"))
				c.Close()
				return false
			}},
		{name: "wrong request id", call: ping, wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(reply(frame.FlagResponse, f.Method, f.RequestID+1, nil))
				return false
			}},
		{name: "request id zero", call: ping, wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(reply(frame.FlagResponse, f.Method, 0, nil))
				return false
			}},
		{name: "reply twice", call: ping, retries: 2,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(append(good(f), good(f)...))
				return false
			}},
		{name: "wrong method id", call: ping, wantErr: isProto,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(reply(frame.FlagResponse, f.Method+1, f.RequestID, nil))
				return false
			}},
		{name: "missing response flag", call: ping, wantErr: isProto,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(reply(0, f.Method, f.RequestID, nil))
				return false
			}},
		{name: "error frame with undecodable meta", call: ping, wantErr: isProto,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(reply(frame.FlagResponse|frame.FlagError, f.Method, f.RequestID, []byte{0xff, 0xff, 0xff, 0xff, 0xff}))
				return false
			}},
		{name: "response with undecodable meta", call: func(cl *Client) error { _, err := cl.Stat("/x"); return err }, wantErr: isProto,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(reply(frame.FlagResponse, f.Method, f.RequestID, []byte{0x0a, 0xff, 0xff, 0xff, 0x0f}))
				return false
			}},
		{name: "notice with unknown kind", call: ping,
			act: func(c net.Conn, f frame.Frame) bool {
				meta, _ := proto.Marshal(&wire.Notice{Kind: 9999, Sequence: 1})
				c.Write(reply(frame.FlagNotify, 62, f.RequestID, meta))
				c.Write(reply(frame.FlagNotify, 62, f.RequestID, []byte{0xff, 0xff, 0xff}))
				c.Write(good(f))
				return true
			}},
		{name: "close in StatBatch", call: func(cl *Client) error { _, err := cl.StatBatch([]string{"/a", "/b"}); return err },
			wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool { c.Close(); return false }},
		{name: "close in ReadFile body", call: func(cl *Client) error { _, err := cl.ReadFile("/a"); return err },
			wantErr: isConnLost,
			act: func(c net.Conn, f frame.Frame) bool {
				c.Write(rawHeader(frame.FlagResponse, f.Method, f.RequestID, 0, 1<<20))
				c.Write(make([]byte, 100<<10))
				c.Close()
				return false
			}},
		{name: "close in ReadBatch", wantErr: isConnLost,
			call: func(cl *Client) error {
				f, err := cl.Open("/a", rw, create)
				if err != nil {
					return err
				}
				_, err = f.ReadBatch([]Range{{0, 10}, {10, 10}})
				return err
			},
			act: func(c net.Conn, f frame.Frame) bool {
				if f.Method == mOpen {
					sendOK(c, f, &wire.OpenResponse{Handle: 1})
					return true
				}
				c.Close()
				return false
			}},
		{name: "never replies", call: ping, timeout: 200 * time.Millisecond, wantErr: isTimeout,
			act: func(c net.Conn, f frame.Frame) bool { return false }},
		{name: "never replies to bulk", call: func(cl *Client) error { _, err := cl.StatBatch([]string{"/a"}); return err },
			timeout: 200 * time.Millisecond, wantErr: isTimeout,
			act: func(c net.Conn, f frame.Frame) bool { return false }},
	}
}

func TestHostileServer(t *testing.T) {
	for _, tc := range hostileCases() {
		t.Run(tc.name, func(t *testing.T) {
			s := newHostileServer(t, tc.act)
			base := runtime.NumGoroutine()
			log := &bufLogger{}
			cl, err := Dial(s.path, Options{PoolSize: 1, Logger: log, CallTimeout: tc.timeout})
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			err = within(t, 5*time.Second, func() error { return tc.call(cl) })
			if tc.wantErr != nil {
				if err == nil {
					t.Fatal("call succeeded against a hostile server")
				}
				if !tc.wantErr(err) {
					t.Fatalf("untyped error %T: %v", err, err)
				}
				if !strings.HasPrefix(err.Error(), "mountos: ") {
					t.Fatalf("message lacks the package prefix: %q", err)
				}
			}
			ok := false
			for i := 0; i <= tc.retries && !ok; i++ {
				ok = within(t, 5*time.Second, func() error { return cl.Ping() }) == nil
			}
			if !ok {
				t.Fatal("client did not recover on a healthy connection")
			}
			cl.Close()
			s.close()
			settleGoroutines(t, base)
		})
	}
}

// A notice of an unknown kind or with undecodable meta is logged and dropped.
// It never reaches a listener and never fails the call.
func TestUnknownNoticeIsLoggedNotDelivered(t *testing.T) {
	var cs []hostileCase
	for _, c := range hostileCases() {
		if c.name == "notice with unknown kind" {
			cs = append(cs, c)
		}
	}
	s := newHostileServer(t, cs[0].act)
	defer s.close()
	log := &bufLogger{}
	cl, err := Dial(s.path, Options{PoolSize: 1, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ch, _ := collect(cl)
	if err := cl.Ping(); err != nil {
		t.Fatalf("ping = %v", err)
	}
	log.waitFor(t, "unknown notice kind 9999")
	log.waitFor(t, "bad notice frame")
	select {
	case e := <-ch:
		t.Fatalf("listener got %+v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

// within runs fn and fails the test when it does not return in d.
func within(t *testing.T, d time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		buf := make([]byte, 1<<16)
		t.Fatalf("call did not return in %v\n%s", d, buf[:runtime.Stack(buf, true)])
		return nil
	}
}

// settleGoroutines fails when goroutines started by the scenario outlive it.
func settleGoroutines(t *testing.T, base int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines: %d at start, %d after Close\n%s", base, n, buf[:runtime.Stack(buf, true)])
	}
}
