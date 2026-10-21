package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

func dialOpts(t *testing.T, s *fakeServer, o Options) *Client {
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

// A call takes the first connection that frees up, not a fixed one.
func TestAcquireTakesFirstFreeConn(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/slow"], s.files["/fast"] = []byte("x"), []byte("x")
	s.delays = map[string]time.Duration{"/slow": 600 * time.Millisecond, "/fast": 60 * time.Millisecond}
	cl := dial(t, s, 2)
	for round := 0; round < 4; round++ {
		s.slowSeen.Store(0)
		go cl.Stat("/slow")
		go cl.Stat("/fast")
		for s.slowSeen.Load() < 2 {
			time.Sleep(time.Millisecond)
		}
		start := time.Now()
		if err := cl.Ping(); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 400*time.Millisecond {
			t.Fatalf("round %d: ping waited %v behind the slow call", round, d)
		}
		time.Sleep(700 * time.Millisecond)
	}
}

func TestCallTimeoutDropsConnection(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dialOpts(t, s, Options{PoolSize: 1, CallTimeout: 150 * time.Millisecond})
	f, err := cl.Open("/t", rw, create)
	if err != nil {
		t.Fatal(err)
	}
	ch, _ := collect(cl)
	s.stall.Store(1)
	err = cl.Ping()
	var te *TimeoutError
	if !errors.As(err, &te) || te.Code() != "DEADLINE_EXCEEDED" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ping = %v", err)
	}
	if e := recvEv(t, ch); e.Kind != EventConnLost {
		t.Fatalf("event = %+v", e)
	}
	if err := cl.Ping(); err != nil {
		t.Fatalf("ping after redial = %v", err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrConnLost) {
		t.Fatalf("file on the dropped connection = %v", err)
	}
	if s.connCount() != 2 {
		t.Fatalf("connections = %d, want a redial", s.connCount())
	}
}

func TestNoTimeoutByDefault(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	s.stall.Store(1)
	start := time.Now()
	if err := cl.Ping(); err != nil || time.Since(start) < 900*time.Millisecond {
		t.Fatalf("ping = %v after %v", err, time.Since(start))
	}
}

// A request too large to send fails that call only. Nothing was written, so the
// socket and the Files on it stay valid.
func TestOversizeRequestKeepsConnection(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, err := cl.Open("/o", rw, create)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cl.Stat(strings.Repeat("a", maxMetaBytes+1))
	var pe *ProtocolError
	if !errors.As(err, &pe) || !errors.Is(err, frame.ErrTooLarge) {
		t.Fatalf("stat = %v", err)
	}
	if _, err := f.WriteAt([]byte("ok"), 0); err != nil {
		t.Fatalf("file after oversize = %v", err)
	}
	if s.connCount() != 1 {
		t.Fatalf("connections = %d, want 1", s.connCount())
	}
}

// A local failure on a connection that hosts a watch ends the watch and
// reaches the listeners, like a lost socket.
func TestLocalFailureEndsWatch(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dialOpts(t, s, Options{PoolSize: 1, CallTimeout: 150 * time.Millisecond})
	ch, _ := collect(cl)
	w, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.stall.Store(1)
	if err := cl.Ping(); err == nil {
		t.Fatal("ping succeeded")
	}
	select {
	case _, open := <-w.Events():
		if open {
			t.Fatal("unexpected event")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watch still open after the connection failed")
	}
	if e := recvEv(t, ch); e.Kind != EventConnLost || e.Err == nil {
		t.Fatalf("event = %+v", e)
	}
}

func TestStatBatchRunsInParallel(t *testing.T) {
	s := newFakeServer(t, 0)
	s.batchDelay = 80 * time.Millisecond
	const n = 4096*4 + 5
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("/p%d", i)
		s.files[paths[i]] = make([]byte, i)
	}
	cl := dial(t, s, 4)
	res, err := cl.StatBatch(paths)
	if err != nil || len(res) != n {
		t.Fatalf("len = %d, %v", len(res), err)
	}
	for i, r := range res {
		if r.Err != nil || r.Info.GetSize() != int64(i) {
			t.Fatalf("item %d = %+v", i, r)
		}
	}
	if p := s.peak.Load(); p < 2 {
		t.Fatalf("peak requests in flight = %d", p)
	}
}

func TestStatChunksRespectMetaBudget(t *testing.T) {
	long := strings.Repeat("a", 1<<20)
	paths := make([]string, 20)
	for i := range paths {
		paths[i] = long
	}
	chunks := statChunks(paths)
	total := 0
	for _, c := range chunks {
		if len(c)*(len(long)+pathOverhead) > maxMetaBytes/2+len(long) {
			t.Fatalf("chunk of %d paths exceeds the metadata budget", len(c))
		}
		total += len(c)
	}
	if len(chunks) < 2 || total != len(paths) {
		t.Fatalf("chunks = %d, total %d", len(chunks), total)
	}
	if got := statChunks([]string{strings.Repeat("b", maxMetaBytes)}); len(got) != 1 || len(got[0]) != 1 {
		t.Fatal("a single oversize path must still form a chunk")
	}
}

func TestReadBatchParallelAcrossConns(t *testing.T) {
	old := fanoutMinBytes
	fanoutMinBytes = 0
	t.Cleanup(func() { fanoutMinBytes = old })
	s := newFakeServer(t, 1000)
	s.batchDelay = 80 * time.Millisecond
	data := pattern(6000, 3)
	s.files["/r"] = data
	cl := dial(t, s, 3)
	f := openRW(t, cl, "/r")
	ranges := []Range{{Offset: 0, Length: 2500}, {Offset: 2500, Length: 2500}, {Offset: 5000, Length: 2000}, {Offset: 100, Length: 10}}
	res, err := f.ReadBatch(ranges)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{data[:2500], data[2500:5000], data[5000:], data[100:110]}
	for i, r := range res {
		if r.Err != nil || !bytes.Equal(r.Data, want[i]) {
			t.Fatalf("range %d: %d bytes, %v", i, len(r.Data), r.Err)
		}
	}
	if p := s.peak.Load(); p < 2 {
		t.Fatalf("peak requests in flight = %d", p)
	}
	s.mu.Lock()
	open := len(s.handles)
	s.mu.Unlock()
	if open != 1 {
		t.Fatalf("%d handles open, the extra read handles must be closed", open)
	}
}

func TestReadBatchStopsAfterShortPart(t *testing.T) {
	s := newFakeServer(t, 1000)
	s.files["/short"] = pattern(1200, 9)
	cl := dial(t, s, 1)
	f := openRW(t, cl, "/short")
	res, err := f.ReadBatch([]Range{{Offset: 0, Length: 3000}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res[0].Data) != 1200 {
		t.Fatalf("got %d bytes", len(res[0].Data))
	}
}

func TestWriteBatchParallelAcrossConns(t *testing.T) {
	s := newFakeServer(t, 1000)
	s.batchDelay = 80 * time.Millisecond
	cl := dial(t, s, 3)
	f := openRW(t, cl, "/w")
	a, b, c := pattern(2500, 1), pattern(2500, 50), pattern(1800, 90)
	res, err := f.WriteBatch([]WriteItem{{Offset: 0, Data: a}, {Offset: 2500, Data: b}, {Offset: 5000, Data: c}})
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range []int{2500, 2500, 1800} {
		if res[i].N != n || res[i].Err != nil {
			t.Fatalf("item %d = %+v", i, res[i])
		}
	}
	s.mu.Lock()
	got := append([]byte(nil), s.files["/w"]...)
	open := len(s.handles)
	s.mu.Unlock()
	if !bytes.Equal(got, append(append(append([]byte(nil), a...), b...), c...)) {
		t.Fatal("file content differs")
	}
	if p := s.peak.Load(); p < 2 {
		t.Fatalf("peak requests in flight = %d", p)
	}
	if open != 1 {
		t.Fatalf("%d handles open, the extra write handles must be closed", open)
	}
}

// Overlapping items keep their order, so the batch stays on one connection.
func TestWriteBatchOverlapStaysSerial(t *testing.T) {
	s := newFakeServer(t, 1000)
	s.batchDelay = 30 * time.Millisecond
	cl := dial(t, s, 3)
	f := openRW(t, cl, "/ov")
	first, second := bytes.Repeat([]byte{'a'}, 2500), bytes.Repeat([]byte{'b'}, 1500)
	if _, err := f.WriteBatch([]WriteItem{{Offset: 0, Data: first}, {Offset: 500, Data: second}}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	got := append([]byte(nil), s.files["/ov"]...)
	s.mu.Unlock()
	want := append(append([]byte(nil), first[:500]...), second...)
	want = append(want, first[2000:]...)
	if !bytes.Equal(got, want) {
		t.Fatal("later item must win over the earlier one")
	}
	if p := s.peak.Load(); p != 1 {
		t.Fatalf("peak requests in flight = %d, want 1", p)
	}
}

// Without an inode the extra handle cannot be tied to the open file.
func TestBatchWithoutInodeStaysSerial(t *testing.T) {
	s := newFakeServer(t, 1000)
	s.noInode = true
	s.batchDelay = 30 * time.Millisecond
	s.files["/n"] = pattern(4000, 1)
	cl := dial(t, s, 3)
	f := openRW(t, cl, "/n")
	if _, err := f.ReadBatch([]Range{{Offset: 0, Length: 4000}}); err != nil {
		t.Fatal(err)
	}
	if p := s.peak.Load(); p != 1 {
		t.Fatalf("peak requests in flight = %d, want 1", p)
	}
}

func TestMissingCapabilityFailsLocally(t *testing.T) {
	s := newFakeServer(t, 0)
	s.caps = []string{"ls_stat", "remove_all"}
	s.files["/c"] = []byte("x")
	s.dirs["/"] = nil
	cl := dial(t, s, 2)
	s.mu.Lock()
	asked := len(s.helloCaps)
	s.mu.Unlock()
	if asked != 8 {
		t.Fatalf("hello asked for %d capabilities", asked)
	}
	f := openRW(t, cl, "/c")
	calls := map[string]func() error{
		"watch":      func() error { _, err := cl.Watch("/", true, 0); return err },
		"warm open":  func() error { _, err := cl.WarmOpen("/c"); return err },
		"warm read":  func() error { _, err := cl.WarmRead("/c", 0, 0); return err },
		"stat batch": func() error { _, err := cl.StatBatch([]string{"/c"}); return err },
		"read batch": func() error { _, err := f.ReadBatch([]Range{{Offset: 0, Length: 1}}); return err },
		"write batch": func() error {
			_, err := f.WriteBatch([]WriteItem{{Offset: 0, Data: []byte("y")}})
			return err
		},
		"read file":  func() error { _, err := cl.ReadFile("/c"); return err },
		"write file": func() error { return cl.WriteFile("/c", []byte("z"), true) },
	}
	for name, call := range calls {
		var we *WireError
		if err := call(); !errors.As(err, &we) || we.Code != "UNSUPPORTED_OPERATION" {
			t.Fatalf("%s = %v", name, err)
		}
	}
	if s.connCount() != 2 {
		t.Fatalf("a local refusal must keep the connections, have %d", s.connCount())
	}
	if _, _, err := cl.Ls("/", 10, ""); err != nil {
		t.Fatalf("ls is supported: %v", err)
	}
	if _, err := f.WriteAt([]byte("y"), 0); err != nil {
		t.Fatalf("plain write = %v", err)
	}
}

func TestNoReportedCapabilitiesAllowsAll(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	if _, err := cl.StatBatch([]string{"/none"}); err != nil {
		t.Fatalf("stat batch = %v", err)
	}
	w, err := cl.Watch("/", true, 0)
	if err != nil {
		t.Fatalf("watch = %v", err)
	}
	w.Close()
}

// A listener that does not return must not hold back watch events.
func TestSlowListenerDoesNotDelayWatch(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	cl.AddListener(func(Event) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-block
	})
	defer close(block)
	w, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_CREATE, "/d/a", "")
	<-started
	for i := 0; i < 10; i++ {
		s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_WRITE, fmt.Sprintf("/d/%d", i), "")
	}
	for i := 0; i < 11; i++ {
		select {
		case <-w.Events():
		case <-time.After(2 * time.Second):
			t.Fatalf("watch event %d held back by the blocked listener", i)
		}
	}
}

func TestWalkDir(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/d"] = []string{"a", "sub", "z"}
	s.dirs["/d/sub"] = []string{"x", "deep"}
	s.dirs["/d/sub/deep"] = []string{"y"}
	cl := dial(t, s, 1)
	var seen []string
	err := cl.WalkDir("/d", func(p string, e *DirEntryWithInfo) error {
		if e.Info == nil {
			t.Errorf("%s: no attributes", p)
		}
		seen = append(seen, p)
		return nil
	})
	want := "/d/a /d/sub /d/sub/x /d/sub/deep /d/sub/deep/y /d/z"
	if err != nil || strings.Join(seen, " ") != want {
		t.Fatalf("seen %v, %v", seen, err)
	}
}

func TestWalkDirPages(t *testing.T) {
	s := newFakeServer(t, 0)
	var names []string
	for i := 0; i < walkPage*2+7; i++ {
		names = append(names, fmt.Sprintf("n%05d", i))
	}
	s.dirs["/big"] = names
	cl := dial(t, s, 1)
	count := 0
	if err := cl.WalkDir("/big", func(string, *DirEntryWithInfo) error { count++; return nil }); err != nil || count != len(names) {
		t.Fatalf("count %d of %d, %v", count, len(names), err)
	}
	all, err := cl.ReadDirAll("/big")
	if err != nil || len(all) != len(names) || all[len(all)-1].GetName() != names[len(names)-1] {
		t.Fatalf("ReadDirAll = %d entries, %v", len(all), err)
	}
}

func TestWalkDirStopsOnCallbackError(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/d"] = []string{"a", "b", "c"}
	cl := dial(t, s, 1)
	stop := errors.New("stop")
	n := 0
	err := cl.WalkDir("/d", func(p string, _ *DirEntryWithInfo) error {
		n++
		if p == "/d/b" {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || n != 2 {
		t.Fatalf("err %v after %d entries", err, n)
	}
}

func TestWalkDirSkipDir(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/d"] = []string{"skip", "keep"}
	s.dirs["/d/skip"] = []string{"hidden"}
	s.dirs["/d/keep"] = []string{"seen"}
	cl := dial(t, s, 1)
	var seen []string
	err := cl.WalkDir("/d", func(p string, _ *DirEntryWithInfo) error {
		seen = append(seen, p)
		if p == "/d/skip" {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil || strings.Join(seen, " ") != "/d/skip /d/keep /d/keep/seen" {
		t.Fatalf("seen %v, %v", seen, err)
	}
}

// A directory that cannot be listed ends the walk with its error.
func TestWalkDirReportsListError(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	if err := cl.WalkDir("/missing", func(string, *DirEntryWithInfo) error { return nil }); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("walk of a missing root = %v", err)
	}
}

// An append handle ignores offsets, so the batch runs on that handle alone and in item order.
func TestWriteBatchAppendStaysSerial(t *testing.T) {
	s := newFakeServer(t, 1000)
	s.batchDelay = 30 * time.Millisecond
	cl := dial(t, s, 3)
	f, err := cl.Open("/ap", wire.OpenFlag_OPEN_FLAG_READ_WRITE|wire.OpenFlag_OPEN_FLAG_APPEND|wire.OpenFlag_OPEN_FLAG_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	items := []WriteItem{{Offset: 0, Data: pattern(2500, 1)}, {Offset: 2500, Data: pattern(2500, 50)}, {Offset: 5000, Data: pattern(1800, 90)}}
	if _, err := f.WriteBatch(items); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	opens := len(s.openFlags)
	s.mu.Unlock()
	if p := s.peak.Load(); p != 1 || opens != 1 {
		t.Fatalf("peak requests in flight = %d, opens = %d, want 1 and 1", p, opens)
	}
}

// Every handle that fanout opens keeps the append flag of the File.
func TestFanoutKeepsAppendFlag(t *testing.T) {
	old := fanoutMinBytes
	fanoutMinBytes = 0
	t.Cleanup(func() { fanoutMinBytes = old })
	s := newFakeServer(t, 1000)
	s.files["/af"] = pattern(6000, 3)
	cl := dial(t, s, 3)
	f, err := cl.Open("/af", wire.OpenFlag_OPEN_FLAG_READ_WRITE|wire.OpenFlag_OPEN_FLAG_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if _, err := f.ReadBatch([]Range{{Offset: 0, Length: 2500}, {Offset: 2500, Length: 2500}, {Offset: 5000, Length: 1000}}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.openFlags) < 2 {
		t.Fatalf("%d opens, fanout did not run", len(s.openFlags))
	}
	for i, fl := range s.openFlags {
		if fl&uint32(wire.OpenFlag_OPEN_FLAG_APPEND) == 0 {
			t.Fatalf("open %d dropped the append flag: %#x", i, fl)
		}
	}
}
