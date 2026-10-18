package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"syscall"
	"testing"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

const (
	rw     = wire.OpenFlag_OPEN_FLAG_READ_WRITE | wire.OpenFlag_OPEN_FLAG_CREATE
	create = 0o644
)

func dial(t *testing.T, s *fakeServer, pool int) *Client {
	t.Helper()
	cl, err := Dial(s.path, Options{PoolSize: pool, Logger: &bufLogger{}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func TestHandshakeOpensPool(t *testing.T) {
	s := newFakeServer(t, 0)
	dial(t, s, 0)
	if got := s.connCount(); got != DefaultPoolSize {
		t.Fatalf("conns = %d, want %d", got, DefaultPoolSize)
	}
	dial(t, s, 2)
}

func TestDialBadPath(t *testing.T) {
	s := newFakeServer(t, 0)
	if _, err := Dial(s.path+"x", Options{PoolSize: 1}); err == nil {
		t.Fatal("expected dial error")
	} else {
		var de *DialError
		if !errors.As(err, &de) {
			t.Fatalf("got %T, want *DialError", err)
		}
	}
}

func TestPingStat(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/a"] = []byte("hello")
	cl := dial(t, s, 2)
	if err := cl.Ping(); err != nil {
		t.Fatal(err)
	}
	info, err := cl.Stat("/a")
	if err != nil || info.GetSize() != 5 {
		t.Fatalf("stat = %v, %v", info, err)
	}
}

func TestErrorMapping(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	_, err := cl.Stat("/missing")
	var we *WireError
	if !errors.As(err, &we) || we.Code != "ENOENT" {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(err, fs.ErrNotExist) || !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("ENOENT does not match fs.ErrNotExist: %v", err)
	}
	if errors.Is(err, fs.ErrExist) {
		t.Fatal("ENOENT matched ErrExist")
	}
	if err := cl.Ping(); err != nil {
		t.Fatalf("conn unusable after wire error: %v", err)
	}
	if _, err := cl.Open("/x", 3, 0); err == nil {
		t.Fatal("access mode 3 accepted")
	}
}

func TestSparseWriteRead(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, err := cl.Open("/sparse", rw, create)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.WriteAt([]byte("tail"), 100); n != 4 || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	buf := make([]byte, 104)
	if n, err := f.ReadAt(buf, 0); n != 104 || err != nil {
		t.Fatalf("read = %d, %v", n, err)
	}
	if !bytes.Equal(buf[:100], make([]byte, 100)) || string(buf[100:]) != "tail" {
		t.Fatalf("hole not zero: % x", buf)
	}
}

func TestReadAtEOF(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, _ := cl.Open("/e", rw, create)
	f.WriteAt([]byte("12345"), 0)
	buf := make([]byte, 10)
	n, err := f.ReadAt(buf, 0)
	if n != 5 || err != io.EOF {
		t.Fatalf("short read = %d, %v", n, err)
	}
	if n, err = f.ReadAt(buf[:5], 0); n != 5 || err != nil {
		t.Fatalf("exact read = %d, %v", n, err)
	}
	if n, err = f.ReadAt(buf, 5); n != 0 || err != io.EOF {
		t.Fatalf("at end = %d, %v", n, err)
	}
}

func TestSequentialReadWriteSeek(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, _ := cl.Open("/seq", rw, create)
	io.WriteString(f, "hello ")
	io.WriteString(f, "world")
	if pos, err := f.Seek(0, io.SeekEnd); pos != 11 || err != nil {
		t.Fatalf("seek end = %d, %v", pos, err)
	}
	f.Seek(-5, io.SeekCurrent)
	got, err := io.ReadAll(f)
	if err != nil || string(got) != "world" {
		t.Fatalf("readall = %q, %v", got, err)
	}
	if _, err := f.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative seek accepted")
	}
}

func TestWriteSplitByServerLimit(t *testing.T) {
	const limit = 1024
	s := newFakeServer(t, limit)
	cl := dial(t, s, 1)
	f, _ := cl.Open("/big", rw, create)
	data := bytes.Repeat([]byte("abcdefg"), 1000)
	if n, err := f.WriteAt(data, 0); n != len(data) || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	s.mu.Lock()
	writes, seen := s.writes, s.maxSeen
	s.mu.Unlock()
	if seen > limit || writes != (len(data)+limit-1)/limit {
		t.Fatalf("frames = %d, max payload = %d", writes, seen)
	}
	got := make([]byte, len(data))
	if n, err := f.ReadAt(got, 0); n != len(data) || err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back = %d, %v", n, err)
	}
}

func TestWriteSplitDefaultLimit(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, _ := cl.Open("/huge", rw, create)
	data := bytes.Repeat([]byte{7}, maxPayloadBytes+10)
	if n, err := f.WriteAt(data, 0); n != len(data) || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	s.mu.Lock()
	writes := s.writes
	s.mu.Unlock()
	if writes != 2 {
		t.Fatalf("frames = %d, want 2", writes)
	}
}

func TestHandleIsolation(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 3)
	var files []*File
	for i := 0; i < 6; i++ {
		f, err := cl.Open(fmt.Sprintf("/f%d", i), rw, create)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	for i, f := range files {
		msg := []byte(fmt.Sprintf("file-%d", i))
		f.WriteAt(msg, 0)
		cl.Ping()
		got := make([]byte, len(msg))
		if _, err := f.ReadAt(got, 0); err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("file %d: %q, %v", i, got, err)
		}
	}
	conns := map[*conn]bool{}
	for _, f := range files {
		conns[f.sk.c] = true
	}
	if len(conns) < 2 {
		t.Fatalf("files spread over %d conns", len(conns))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.violations != 0 {
		t.Fatalf("%d handles used on the wrong connection", s.violations)
	}
}

func TestParallelDistinctFiles(t *testing.T) {
	const n = 4
	s := newFakeServer(t, 0)
	s.barrierN = n
	s.release = make(chan struct{})
	cl := dial(t, s, n)
	files := make([]*File, n)
	for i := range files {
		f, err := cl.Open(fmt.Sprintf("/p%d", i), rw, create)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteAt([]byte("data"), 0)
		files[i] = f
	}
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for _, f := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4)
			if _, err := f.ReadAt(buf, 0); err != nil || string(buf) != "data" {
				errs <- fmt.Errorf("read %q: %v", buf, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.arrived != n {
		t.Fatalf("arrived = %d", s.arrived)
	}
	select {
	case <-s.release:
	default:
		t.Fatal("reads never overlapped on the server")
	}
}

func TestSameFileSerialized(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	f, _ := cl.Open("/same", rw, create)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.WriteAt([]byte{byte(i)}, int64(i))
			f.Stat()
		}()
	}
	wg.Wait()
	buf := make([]byte, 8)
	if _, err := f.ReadAt(buf, 0); err != nil || !bytes.Equal(buf, []byte{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("buf = %v, %v", buf, err)
	}
}

func TestDoubleClose(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, _ := cl.Open("/c", rw, create)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); !errors.Is(err, ErrFileClosed) {
		t.Fatalf("second close = %v", err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrFileClosed) {
		t.Fatalf("read after close = %v", err)
	}
}

func TestConnLossAndRedial(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, _ := cl.Open("/l", rw, create)
	ch, _ := collect(cl)
	s.dropAll()
	if e := recvEv(t, ch); e.Kind != EventConnLost {
		t.Fatalf("event = %+v", e)
	}
	if err := cl.Ping(); err != nil {
		t.Fatalf("ping after redial = %v", err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrConnLost) {
		t.Fatalf("stale file read = %v", err)
	}
}

func TestClientClosed(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f, _ := cl.Open("/z", rw, create)
	cl.Close()
	if err := cl.Ping(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("ping = %v", err)
	}
	if _, err := f.Stat(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("stat = %v", err)
	}
}
