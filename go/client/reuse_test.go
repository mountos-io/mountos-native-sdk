package client

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// emptyInfoServer answers Hello and Open, and every other request with an
// empty success reply that carries no info message.
func emptyInfoServer(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "mfs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "api.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					f, err := frame.Read(c, limits)
					if err != nil {
						return
					}
					var m proto.Message = &wire.Empty{}
					switch f.Method {
					case mHello:
						m = &wire.HelloResponse{Version: 1, MaxInflight: 1}
					case mOpen:
						m = &wire.OpenResponse{Handle: 1}
					}
					meta, _ := proto.Marshal(m)
					frame.Write(c, frame.Frame{Flags: frame.FlagResponse, Method: f.Method, RequestID: f.RequestID, Meta: meta}, limits)
				}
			}()
		}
	}()
	return path
}

func TestStatWithoutInfoIsProtocolError(t *testing.T) {
	cl, err := Dial(emptyInfoServer(t), Options{PoolSize: 1, Logger: &bufLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	f, err := cl.Open("/f", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]func() (*wire.FileInfo, error){
		"Stat":      func() (*wire.FileInfo, error) { return cl.Stat("/f") },
		"Lstat":     func() (*wire.FileInfo, error) { return cl.Lstat("/f") },
		"File.Stat": f.Stat,
	}
	for name, call := range calls {
		info, err := call()
		var pe *ProtocolError
		if info != nil || !errors.As(err, &pe) {
			t.Fatalf("%s = %v, %v, want nil and a protocol error", name, info, err)
		}
	}
	// The reply decoded cleanly, so the connection stays usable.
	if err := cl.Ping(); err != nil {
		t.Fatalf("ping after missing info: %v", err)
	}
}

// ReadBatch results share one reply buffer. An append to one result must not
// overwrite the next one.
func TestReadBatchResultsDoNotAlias(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f := openRW(t, cl, "/f")
	if _, err := f.WriteBatch([]WriteItem{{Offset: 0, Data: []byte("aaaabbbbcccc")}}); err != nil {
		t.Fatal(err)
	}
	rr, err := f.ReadBatch([]Range{{Offset: 0, Length: 4}, {Offset: 4, Length: 4}, {Offset: 8, Length: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if cap(rr[0].Data) != len(rr[0].Data) {
		t.Fatalf("cap %d > len %d, an append would reach the neighbor", cap(rr[0].Data), len(rr[0].Data))
	}
	_ = append(rr[0].Data, 'X', 'X', 'X', 'X')
	if string(rr[1].Data) != "bbbb" || string(rr[2].Data) != "cccc" {
		t.Fatalf("neighbors changed: %q %q", rr[1].Data, rr[2].Data)
	}
}

// A range split across several frames still assembles in order.
func TestReadBatchSplitRangeAssembles(t *testing.T) {
	s := newFakeServer(t, 4096)
	cl := dial(t, s, 1)
	f := openRW(t, cl, "/f")
	want := bytes.Repeat([]byte("0123456789"), 1000)
	if _, err := f.WriteBatch([]WriteItem{{Offset: 0, Data: want}}); err != nil {
		t.Fatal(err)
	}
	rr, err := f.ReadBatch([]Range{{Offset: 0, Length: 10000}, {Offset: 0, Length: 10}})
	if err != nil || !bytes.Equal(rr[0].Data, want) || string(rr[1].Data) != "0123456789" {
		t.Fatalf("read = %v", err)
	}
}

// After a timeout drops the socket, the reused request slot serves the next
// call without a stale reply.
func TestCallAfterTimeoutUsesCleanSlot(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/f"] = []byte("x")
	cl, err := Dial(s.path, Options{PoolSize: 1, CallTimeout: 200 * time.Millisecond, Logger: &bufLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	s.stall.Store(1)
	if err := cl.Ping(); !isTimeout(err) {
		t.Fatalf("ping = %v, want timeout", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := cl.Stat("/f"); err != nil {
			t.Fatalf("stat %d after timeout: %v", i, err)
		}
	}
}
