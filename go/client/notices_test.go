package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

type unwatchRec struct {
	conn int
	id   uint64
}

// fakeNotify holds warm ids, watch ids and notice state. Callers hold fakeServer.mu.
type fakeNotify struct {
	warmConn   map[uint64]int
	watchConn  map[uint64]int
	unwatches  []unwatchRec
	nextWarm   uint64
	nextWatch  uint64
	seq        uint64
	eager      int  // CHANGE notices sent right after a Watch reply
	pingNotice int  // notices sent before every Ping reply
	full       bool // WarmOpen and WarmRead answer EAGAIN
}

// handleNotify serves the warm, Watch and Unwatch methods. ok is false for other methods.
func (s *fakeServer) handleNotify(f frame.Frame, idx int) (resp proto.Message, werr *wire.WireError, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := &s.nt
	if n.warmConn == nil {
		n.warmConn, n.watchConn = map[uint64]int{}, map[uint64]int{}
	}
	switch f.Method {
	case mClone, mCloneDir, mRemoveDir, mOpCancel:
		resp, werr = s.handleOps(f, idx)
		return resp, werr, true
	case mWarmOpen, mWarmRead:
		if n.full {
			return nil, errno("EAGAIN"), true
		}
		n.nextWarm++
		n.warmConn[n.nextWarm] = idx
		if f.Method == mWarmOpen {
			return &wire.WarmOpenResponse{WarmId: n.nextWarm}, nil, true
		}
		return &wire.WarmReadResponse{WarmId: n.nextWarm}, nil, true
	case mWatch:
		n.nextWatch++
		n.watchConn[n.nextWatch] = idx
		return &wire.WatchResponse{WatchId: n.nextWatch}, nil, true
	case mUnwatch:
		var r wire.UnwatchRequest
		proto.Unmarshal(f.Meta, &r)
		if c, found := n.watchConn[r.WatchId]; !found || c != idx {
			return nil, errno("EBADF"), true
		}
		delete(n.watchConn, r.WatchId)
		n.unwatches = append(n.unwatches, unwatchRec{idx, r.WatchId})
		return &wire.Empty{}, nil, true
	}
	return nil, nil, false
}

// afterWatch runs after a Watch reply: an overflow for an old replay point, then eager changes.
func (s *fakeServer) afterWatch(idx int, f frame.Frame, resp proto.Message) {
	var r wire.WatchRequest
	proto.Unmarshal(f.Meta, &r)
	id := resp.(*wire.WatchResponse).WatchId
	s.mu.Lock()
	seq, eager := s.nt.seq, s.nt.eager
	s.mu.Unlock()
	if r.SinceSequence != 0 && r.SinceSequence < seq {
		s.notify(idx, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: id})
	}
	for i := 0; i < eager; i++ {
		s.notify(idx, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CHANGE, RefId: id,
			ChangeKind: wire.WatchEventKind_WATCH_EVENT_KIND_WRITE, Path: fmt.Sprintf("/e%d", i)})
	}
}

// notify pushes a notice frame on connection idx at any time.
func (s *fakeServer) notify(idx int, n *wire.Notice) {
	s.mu.Lock()
	c := s.connByIdx[idx]
	if n.Sequence == 0 {
		s.nt.seq++
		n.Sequence = s.nt.seq
	}
	s.mu.Unlock()
	if c == nil {
		s.t.Errorf("notify: no connection %d", idx)
		return
	}
	meta, _ := proto.Marshal(n)
	s.wmu.Lock()
	defer s.wmu.Unlock()
	frame.Write(c, frame.Frame{Flags: frame.FlagNotify, Method: mNotice, RequestID: 1, Meta: meta}, s.lim)
}

func (s *fakeServer) warmConnOf(id uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.nt.warmConn[id]
	if !ok {
		s.t.Fatalf("unknown warm id %d", id)
	}
	return c
}

func (s *fakeServer) watchConnOf(id uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.nt.watchConn[id]
	if !ok {
		s.t.Fatalf("unknown watch id %d", id)
	}
	return c
}

func (s *fakeServer) warmDone(id uint64) {
	s.notify(s.warmConnOf(id), &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_WARM_DONE, RefId: id})
}

func (s *fakeServer) warmFailed(id uint64, code, msg, path string) {
	s.notify(s.warmConnOf(id), &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_WARM_FAILED, RefId: id, Code: code, Message: msg, Path: path})
}

func (s *fakeServer) change(watch uint64, kind wire.WatchEventKind, path, old string) {
	s.notify(s.watchConnOf(watch), &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CHANGE, RefId: watch, ChangeKind: kind, Path: path, OldPath: old})
}

func (s *fakeServer) connIndexes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int
	for i := range s.connByIdx {
		out = append(out, i)
	}
	return out
}

func (s *fakeServer) watchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.nt.watchConn)
}

func (s *fakeServer) unwatchLog() []unwatchRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]unwatchRec(nil), s.nt.unwatches...)
}

// bufLogger captures log lines.
type bufLogger struct {
	b  bytes.Buffer
	mu sync.Mutex
}

func (l *bufLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.b, format+"\n", args...)
}

func (l *bufLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func (l *bufLogger) waitFor(t *testing.T, sub string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s := l.String(); strings.Contains(s, sub) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("log lacks %q, has %q", sub, l.String())
	return ""
}

func dialLog(t *testing.T, s *fakeServer, pool int) (*Client, *bufLogger) {
	t.Helper()
	l := &bufLogger{}
	cl, err := Dial(s.path, Options{PoolSize: pool, Logger: l})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl, l
}

func collect(cl *Client) (<-chan Event, func()) {
	ch := make(chan Event, 8192)
	rm := cl.AddListener(func(e Event) { ch <- e })
	return ch, rm
}

func recvEv(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("no event")
	}
	return Event{}
}

func noEvent(t *testing.T, ch <-chan Event) {
	t.Helper()
	select {
	case e := <-ch:
		t.Fatalf("unexpected event %+v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestResponseMatchingWithNotices(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/a"] = []byte("x")
	cl := dial(t, s, 3)
	ch, _ := collect(cl)
	s.mu.Lock()
	s.nt.pingNotice = 3
	s.mu.Unlock()
	const workers, rounds = 4, 40
	var wg sync.WaitGroup
	var bad atomic.Int32
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if cl.Ping() != nil {
					bad.Add(1)
				}
				if info, err := cl.Stat("/a"); err != nil || info.GetSize() != 1 {
					bad.Add(1)
				}
			}
		}()
	}
	var pushed atomic.Int32
	stop := make(chan struct{})
	pdone := make(chan struct{})
	go func() {
		defer close(pdone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, i := range s.connIndexes() {
				s.notify(i, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING})
				pushed.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(stop)
	<-pdone
	if bad.Load() != 0 {
		t.Fatalf("%d calls failed or returned wrong data", bad.Load())
	}
	want := int(pushed.Load()) + workers*rounds*3
	deadline := time.After(3 * time.Second)
	got := 0
	for got < want {
		select {
		case <-ch:
			got++
		case <-deadline:
			t.Fatalf("got %d notices, want %d", got, want)
		}
	}
}

func TestNoticeOnIdleConn(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	ch, _ := collect(cl)
	s.notify(1, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: 7})
	e := recvEv(t, ch)
	if e.Kind != EventOverflow || e.RefID != 7 || e.Sequence == 0 || e.Time.IsZero() {
		t.Fatalf("event = %+v", e)
	}
}

func TestWarmEventsReachAllListeners(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1) // one connection keeps notice order fixed
	a, _ := collect(cl)
	b, _ := collect(cl)
	ok, err := cl.WarmOpen("/ok")
	if err != nil {
		t.Fatal(err)
	}
	bad, err := cl.WarmRead("/gone", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.warmDone(ok)
	s.warmFailed(bad, "ENOENT", "no such file", "/gone")
	for _, ch := range []<-chan Event{a, b} {
		e1, e2 := recvEv(t, ch), recvEv(t, ch)
		if e1.Kind != EventWarmDone || e1.RefID != ok || e1.Err != nil {
			t.Fatalf("e1 = %+v", e1)
		}
		var we *WireError
		if e2.Kind != EventWarmFailed || e2.RefID != bad || e2.Path != "/gone" || !errors.As(e2.Err, &we) || we.Code != "ENOENT" || we.Message != "no such file" {
			t.Fatalf("e2 = %+v", e2)
		}
	}
}

func TestWarmFullQueue(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	s.mu.Lock()
	s.nt.full = true
	s.mu.Unlock()
	var we *WireError
	if _, err := cl.WarmOpen("/x"); !errors.As(err, &we) || we.Code != "EAGAIN" {
		t.Fatalf("WarmOpen = %v", err)
	}
}

func TestNoListenerLogs(t *testing.T) {
	s := newFakeServer(t, 0)
	cl, l := dialLog(t, s, 1)
	id, _ := cl.WarmOpen("/gone")
	s.warmFailed(id, "ENOENT", "no such file", "/gone")
	out := l.waitFor(t, "warm_failed")
	if !strings.Contains(out, `path="/gone"`) || !strings.Contains(out, fmt.Sprintf("ref=%d", id)) || !strings.Contains(out, "ENOENT") {
		t.Fatalf("log = %q", out)
	}
}

func TestNoListenerDefaultsToStderr(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	s := newFakeServer(t, 0)
	cl, err := Dial(s.path, Options{PoolSize: 1})
	os.Stderr = old
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: 9})
	got := make(chan string, 1)
	go func() {
		buf := make([]byte, 512)
		n, _ := r.Read(buf)
		got <- string(buf[:n])
	}()
	select {
	case line := <-got:
		if !strings.Contains(line, "mountos: ") || !strings.Contains(line, "overflow") || !strings.Contains(line, "ref=9") {
			t.Fatalf("stderr = %q", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nothing on stderr")
	}
	w.Close()
	io.Copy(io.Discard, r)
	r.Close()
}

func TestRemovedListenerLogsAgain(t *testing.T) {
	s := newFakeServer(t, 0)
	cl, l := dialLog(t, s, 1)
	ch, rm := collect(cl)
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: 1})
	recvEv(t, ch)
	if strings.Contains(l.String(), "overflow") {
		t.Fatalf("logged with a listener: %q", l.String())
	}
	rm()
	rm()
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: 2})
	l.waitFor(t, "ref=2")
}

func TestConnLostEvent(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	ch, _ := collect(cl)
	s.dropAll()
	e := recvEv(t, ch)
	var we *WireError
	if e.Kind != EventConnLost || e.Err == nil || errors.As(e.Err, &we) || !errors.Is(e.Err, ErrConnLost) {
		t.Fatalf("event = %+v", e)
	}
	if err := cl.Ping(); err != nil {
		t.Fatalf("ping after redial = %v", err)
	}
}

func TestClientCloseNoConnLost(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	ch, _ := collect(cl)
	cl.Close()
	noEvent(t, ch)
}

func TestClosingEvent(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	ch, _ := collect(cl)
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING})
	if e := recvEv(t, ch); e.Kind != EventClosing {
		t.Fatalf("event = %+v", e)
	}
}

func TestKindStrings(t *testing.T) {
	for k, want := range map[EventKind]string{EventWarmDone: "warm_done", EventWarmFailed: "warm_failed", EventChange: "change", EventOverflow: "overflow", EventClosing: "closing", EventConnLost: "conn_lost"} {
		if k.String() != want {
			t.Fatalf("%d = %s", k, k)
		}
	}
	for k, want := range map[ChangeKind]string{Create: "create", Remove: "remove", Write: "write", Rename: "rename", Attrib: "attrib"} {
		if k.String() != want {
			t.Fatalf("%d = %s", k, k)
		}
	}
}

func TestListenerPanicKeepsDispatcher(t *testing.T) {
	s := newFakeServer(t, 0)
	cl, l := dialLog(t, s, 1)
	cl.AddListener(func(e Event) {
		if e.RefID == 1 {
			panic("boom")
		}
	})
	ch, _ := collect(cl)
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: 1})
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: 2})
	if e := recvEv(t, ch); e.RefID != 1 {
		t.Fatalf("first = %+v", e)
	}
	if e := recvEv(t, ch); e.RefID != 2 {
		t.Fatalf("second = %+v", e)
	}
	if out := l.waitFor(t, "panic"); !strings.Contains(out, "boom") {
		t.Fatalf("log = %q", out)
	}
}

func TestSlowListenerDropsOldest(t *testing.T) {
	s := newFakeServer(t, 0)
	cl, l := dialLog(t, s, 1)
	gate := make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(release) // runs before the client closes, so a failed test cannot block Close
	var mu sync.Mutex
	var got []Event
	cl.AddListener(func(e Event) {
		if e.RefID == 1 {
			<-gate
		}
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
	})
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING, RefId: 1})
	time.Sleep(50 * time.Millisecond) // the dispatcher blocks on event 1
	const total = queueLimit + 500
	for i := 0; i < total; i++ {
		s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING, RefId: uint64(100 + i)})
	}
	time.Sleep(200 * time.Millisecond) // the reader finishes queueing
	release()
	l.waitFor(t, "dropped")
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= queueLimit+1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	overflow := 0
	var last uint64
	for _, e := range got {
		if e.Kind == EventOverflow && e.RefID == 0 {
			overflow++
		}
		if e.Kind == EventClosing {
			last = e.RefID
		}
	}
	if overflow < 1 {
		t.Fatalf("no local overflow among %d events", len(got))
	}
	if last != uint64(100+total-1) {
		t.Fatalf("newest event lost: last = %d", last)
	}
	if len(got) > queueLimit+3 {
		t.Fatalf("delivered %d events, queue limit %d", len(got), queueLimit)
	}
}

func TestWaitWarm(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	ctx := context.Background()

	id, _ := cl.WarmOpen("/a")
	go func() {
		time.Sleep(30 * time.Millisecond)
		s.warmDone(id)
	}()
	if err := cl.WaitWarm(ctx, id); err != nil {
		t.Fatalf("done = %v", err)
	}

	bad, _ := cl.WarmOpen("/gone")
	go func() {
		time.Sleep(30 * time.Millisecond)
		s.warmFailed(bad, "ENOENT", "no such file", "/gone")
	}()
	var we *WireError
	if err := cl.WaitWarm(ctx, bad); !errors.As(err, &we) || we.Code != "ENOENT" {
		t.Fatalf("failed = %v", err)
	}

	early, _ := cl.WarmOpen("/early")
	ch, _ := collect(cl)
	s.warmDone(early)
	recvEv(t, ch) // the notice is processed
	if err := cl.WaitWarm(ctx, early); err != nil {
		t.Fatalf("already finished = %v", err)
	}
	if err := cl.WaitWarm(ctx, bad); !errors.As(err, &we) {
		t.Fatalf("already failed = %v", err)
	}

	pending, _ := cl.WarmOpen("/slow")
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := cl.WaitWarm(cctx, pending); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx = %v", err)
	}
	// a late result after the cancelled wait is still kept
	s.warmDone(pending)
	recvEv(t, ch)
	if err := cl.WaitWarm(ctx, pending); err != nil {
		t.Fatalf("after cancel = %v", err)
	}
}

func TestWaitWarmCacheLimit(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	ch, _ := collect(cl)
	const n = warmKeep + 50
	for i := 0; i < n; i++ {
		s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_WARM_DONE, RefId: uint64(i + 1)})
	}
	for i := 0; i < n; i++ {
		recvEv(t, ch)
	}
	if err := cl.WaitWarm(context.Background(), n); err != nil {
		t.Fatalf("newest = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := cl.WaitWarm(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("evicted id = %v", err)
	}
}

func TestWaitWarmClientClose(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	id, _ := cl.WarmOpen("/a")
	done := make(chan error, 1)
	go func() { done <- cl.WaitWarm(context.Background(), id) }()
	time.Sleep(30 * time.Millisecond)
	cl.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("WaitWarm did not return")
	}
}

func TestWaitWarmConcurrent(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 3)
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 20; i++ {
		id, err := cl.WarmOpen("/p")
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- cl.WaitWarm(context.Background(), id)
			}()
		}
		if i%2 == 0 {
			s.warmDone(id)
		} else {
			go s.warmDone(id)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientCloseLeavesNoGoroutines(t *testing.T) {
	s := newFakeServer(t, 0)
	// warm up lazily started runtime goroutines
	c0, _ := Dial(s.path, Options{PoolSize: 1, Logger: &bufLogger{}})
	c0.Close()
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()

	cl, _ := dialLog(t, s, 4)
	cl.AddListener(func(Event) {})
	w1, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := cl.Watch("/e", false, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.change(w1.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_WRITE, "/d/x", "")
	id, _ := cl.WarmOpen("/a")
	go cl.WaitWarm(context.Background(), id)
	time.Sleep(50 * time.Millisecond)
	w2.Close()
	cl.Close()

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		buf := make([]byte, 1<<16)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("goroutines: before %d, after %d\n%s", before, after, buf)
	}
	if _, ok := <-w1.Events(); ok {
		// one buffered change may remain, the channel must still end
		for range w1.Events() {
		}
	}
}

func TestCloseFromListener(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	closed := make(chan struct{})
	cl.AddListener(func(e Event) {
		if e.Kind == EventClosing {
			cl.Close()
			close(closed)
		}
	})
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING})
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close called from a listener did not return")
	}
	select {
	case <-cl.hub.done:
	case <-time.After(3 * time.Second):
		t.Fatal("dispatcher still running after the listener returned")
	}
}

func TestWireErrorEAGAIN(t *testing.T) {
	err := &WireError{Code: "EAGAIN"}
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatal("EAGAIN does not match syscall.EAGAIN")
	}
}

func TestMetaLimitMatchesServer(t *testing.T) {
	if limits.MaxMetaBytes != 16<<20 {
		t.Fatalf("meta limit = %d", limits.MaxMetaBytes)
	}
}
