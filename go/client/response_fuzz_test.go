package client

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// fuzzReply is what the decode fuzz server answers to every method except
// Hello, Open and Opendir.
type fuzzReply struct {
	flags   uint8
	meta    []byte
	payload []byte
	notice  []byte // sent as a notice frame before the reply when non-nil
}

// fuzzServer answers each request with the current fuzzReply.
type fuzzServer struct {
	path  string
	ln    net.Listener
	reply atomic.Pointer[fuzzReply]
	wg    sync.WaitGroup
}

func newFuzzServer(tb testing.TB) *fuzzServer {
	dir, err := os.MkdirTemp("", "mff")
	if err != nil {
		tb.Fatal(err)
	}
	s := &fuzzServer{path: filepath.Join(dir, "f.sock")}
	s.reply.Store(&fuzzReply{})
	if s.ln, err = net.Listen("unix", s.path); err != nil {
		tb.Fatal(err)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := s.ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() { defer s.wg.Done(); s.serve(c) }()
		}
	}()
	tb.Cleanup(func() {
		s.ln.Close()
		s.wg.Wait()
		os.RemoveAll(dir)
	})
	return s
}

func (s *fuzzServer) serve(c net.Conn) {
	defer c.Close()
	for {
		f, err := frame.Read(c, limits)
		if err != nil {
			return
		}
		switch f.Method {
		case mHello:
			sendOK(c, f, &wire.HelloResponse{Version: protoVersion, MaxInflight: 1})
		case mOpen:
			sendOK(c, f, &wire.OpenResponse{Handle: 1})
		case mOpendir:
			sendOK(c, f, &wire.OpendirResponse{Handle: 1})
		default:
			r := s.reply.Load()
			if r.notice != nil {
				frame.Write(c, frame.Frame{Flags: frame.FlagNotify, Method: mNotice, RequestID: f.RequestID, Meta: r.notice}, limits)
			}
			frame.Write(c, frame.Frame{Flags: frame.FlagResponse | r.flags, Method: f.Method, RequestID: f.RequestID, Meta: r.meta, Payload: r.payload}, limits)
		}
	}
}

// fuzzOps runs one client operation against files opened for this execution.
var fuzzOps = []struct {
	name string
	run  func(cl *Client, f *File, d *Dir) error
}{
	{"Ping", func(cl *Client, _ *File, _ *Dir) error { return cl.Ping() }},
	{"Stat", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.Stat("/a"); return err }},
	{"Lstat", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.Lstat("/a"); return err }},
	{"FileStat", func(_ *Client, f *File, _ *Dir) error { _, err := f.Stat(); return err }},
	{"Mkdir", func(cl *Client, _ *File, _ *Dir) error { return cl.Mkdir("/d", 0o755) }},
	{"Rename", func(cl *Client, _ *File, _ *Dir) error { return cl.Rename("/a", "/b") }},
	{"Readlink", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.Readlink("/l"); return err }},
	{"Statfs", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.Statfs("/"); return err }},
	{"RemoveAll", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.RemoveAll("/d"); return err }},
	{"WarmOpen", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.WarmOpen("/a"); return err }},
	{"WarmRead", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.WarmRead("/a", 0, 0); return err }},
	{"Utimens", func(cl *Client, _ *File, _ *Dir) error { n := time.Unix(1, 0); return cl.Utimens("/a", &n, &n) }},
	{"Ls", func(cl *Client, _ *File, _ *Dir) error { _, _, err := cl.Ls("/d", 100, ""); return err }},
	{"ReadDirAll", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.ReadDirAll("/d"); return err }},
	{"StatBatch", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.StatBatch([]string{"/a", "/b", "/c"}); return err }},
	{"ReadFile", func(cl *Client, _ *File, _ *Dir) error { _, err := cl.ReadFile("/a"); return err }},
	{"WriteFile", func(cl *Client, _ *File, _ *Dir) error { return cl.WriteFile("/a", []byte("data"), true) }},
	{"ReadAt", func(_ *Client, f *File, _ *Dir) error {
		p := make([]byte, 64)
		n, err := f.ReadAt(p, 0)
		if n < 0 || n > len(p) {
			return errors.New("fuzz oracle: ReadAt count out of range")
		}
		return err
	}},
	{"Read", func(_ *Client, f *File, _ *Dir) error {
		p := make([]byte, 64)
		n, err := f.Read(p)
		if n < 0 || n > len(p) {
			return errors.New("fuzz oracle: Read count out of range")
		}
		return err
	}},
	{"WriteAt", func(_ *Client, f *File, _ *Dir) error {
		n, err := f.WriteAt([]byte("0123456789"), 0)
		if n < 0 || n > 10 {
			return errors.New("fuzz oracle: WriteAt count out of range")
		}
		return err
	}},
	{"SeekEnd", func(_ *Client, f *File, _ *Dir) error { _, err := f.Seek(0, io.SeekEnd); return err }},
	{"Sync", func(_ *Client, f *File, _ *Dir) error { return f.Sync() }},
	{"Flush", func(_ *Client, f *File, _ *Dir) error { return f.Flush() }},
	{"FileTruncate", func(_ *Client, f *File, _ *Dir) error { return f.Truncate(5) }},
	{"ReadBatch", func(_ *Client, f *File, _ *Dir) error {
		rs, err := f.ReadBatch([]Range{{0, 8}, {8, 8}, {100, 8}})
		if err == nil {
			if len(rs) != 3 {
				return errors.New("fuzz oracle: ReadBatch result count")
			}
			for i, r := range rs {
				if r.Err == nil && len(r.Data) > 8 {
					return errors.New("fuzz oracle: ReadBatch item longer than range " + string(rune('0'+i)))
				}
			}
		}
		return err
	}},
	{"WriteBatch", func(_ *Client, f *File, _ *Dir) error {
		rs, err := f.WriteBatch([]WriteItem{{0, []byte("abcd")}, {10, []byte("ef")}})
		if err == nil {
			if len(rs) != 2 {
				return errors.New("fuzz oracle: WriteBatch result count")
			}
			for _, r := range rs {
				if r.Err == nil && (r.N < 0 || r.N > 4) {
					return errors.New("fuzz oracle: WriteBatch count out of range")
				}
			}
		}
		return err
	}},
	{"Readdir", func(_ *Client, _ *File, d *Dir) error { _, _, err := d.Readdir(10); return err }},
	{"FileClose", func(_ *Client, f *File, _ *Dir) error { return f.Close() }},
	{"DirClose", func(_ *Client, _ *File, d *Dir) error { return d.Close() }},
	{"Watch", func(cl *Client, _ *File, _ *Dir) error {
		w, err := cl.Watch("/d", true, 0)
		if err == nil {
			w.Close()
		}
		return err
	}},
	{"WaitWarm", func(cl *Client, _ *File, _ *Dir) error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		id, err := cl.WarmOpen("/a")
		if err != nil {
			return err
		}
		if err := cl.WaitWarm(ctx, id); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return nil
	}},
}

// typedClientError reports whether err is a documented failure of the client.
func typedClientError(err error) bool {
	var we *WireError
	var pe *ProtocolError
	var te *TimeoutError
	var de *DialError
	return errors.As(err, &we) || errors.As(err, &pe) || errors.As(err, &te) || errors.As(err, &de) ||
		errors.Is(err, ErrConnLost) || errors.Is(err, ErrFileClosed) || errors.Is(err, ErrClientClosed) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrShortWrite)
}

// FuzzResponseDecode feeds random bytes as the reply meta, payload and flags of
// every client operation. Each call returns within the timeout and returns nil
// or a typed error. Counts never exceed what the caller asked for.
func FuzzResponseDecode(f *testing.F) {
	s := newFuzzServer(f)
	cl, err := Dial(s.path, Options{PoolSize: 1, Logger: discardLogger{}, CallTimeout: 2 * time.Second})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { cl.Close() })

	wm := func(m proto.Message) []byte { b, _ := proto.Marshal(m); return b }
	f.Add(uint8(0), uint8(0), uint8(0), []byte{})
	f.Add(uint8(1), uint8(0), uint8(0), wm(&wire.StatResponse{Info: &wire.FileInfo{Name: "a", Size: 3}}))
	f.Add(uint8(14), uint8(0), uint8(2), append(wm(&wire.ReadAtResponse{Count: 2}), 'x', 'y'))
	f.Add(uint8(20), uint8(0), uint8(0), wm(&wire.ReadBatchResponse{Counts: []uint32{1, 2, 3}}))
	f.Add(uint8(20), uint8(0), uint8(0), wm(&wire.ReadBatchResponse{Counts: []uint32{1, 2}, Errors: []*wire.BatchItemError{{Index: 99}}}))
	f.Add(uint8(14), uint8(1), uint8(0), wm(&wire.WireError{Code: "ENOENT", Message: "x"}))
	f.Add(uint8(14), uint8(1), uint8(0), []byte{0xff, 0xff, 0xff})
	f.Add(uint8(0), uint8(2), uint8(0), wm(&wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_WARM_DONE, RefId: 1}))
	f.Add(uint8(0), uint8(2), uint8(0), []byte{0xff, 0xff})

	f.Fuzz(func(t *testing.T, opSel, mode, split uint8, data []byte) {
		op := fuzzOps[int(opSel)%len(fuzzOps)]
		cut := len(data) * int(split) / 256
		r := &fuzzReply{meta: data[:cut], payload: data[cut:]}
		if mode&1 != 0 {
			r.flags = frame.FlagError
		}
		if mode&2 != 0 {
			r.notice = data
		}
		// Open the handles with a valid reply, then swap in the fuzzed one.
		s.reply.Store(&fuzzReply{})
		file, ferr := cl.Open("/a", rw, create)
		dir, derr := cl.Opendir("/d")
		if ferr != nil || derr != nil {
			t.Fatalf("setup open: %v %v", ferr, derr)
		}
		s.reply.Store(r)
		done := make(chan error, 1)
		go func() { done <- op.run(cl, file, dir) }()
		select {
		case err := <-done:
			if err != nil && !typedClientError(err) {
				t.Fatalf("%s: untyped error %T: %v", op.name, err, err)
			}
			if err != nil && err.Error() == "" {
				t.Fatalf("%s: empty error message", op.name)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: call did not return", op.name)
		}
	})
}
