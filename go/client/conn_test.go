package client

import (
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

// A reply that names no request in flight fails the call instead of hanging it.
func TestUnmatchedReplyFailsCall(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		for {
			f, rerr := frame.Read(c, limits)
			if rerr != nil {
				return
			}
			out := frame.Frame{Flags: frame.FlagResponse, Method: f.Method, RequestID: f.RequestID}
			if f.Method == mHello {
				out.Meta, _ = proto.Marshal(&wire.HelloResponse{Version: protoVersion})
			} else {
				out.RequestID += 100
			}
			frame.Write(c, out, limits)
		}
	}()
	cl, err := Dial(sock, Options{PoolSize: 1, Logger: discardLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	done := make(chan error, 1)
	go func() { done <- cl.Ping() }()
	select {
	case err := <-done:
		var pe *ProtocolError
		if !errors.As(err, &pe) || !errors.Is(err, ErrConnLost) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call hung on an unmatched reply")
	}
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// A socket published after the Close snapshot is dropped, so Hello never waits on it.
func TestCloseDuringDialDoesNotHang(t *testing.T) {
	dir, err := os.MkdirTemp("", "mfs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "silent.sock")
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
			t.Cleanup(func() { c.Close() })
		}
	}()
	cl := &Client{path: path, hub: newHub(&bufLogger{}), closedCh: make(chan struct{}), free: make(chan struct{}, 1), maxInflight: 1}
	c := &conn{cl: cl, path: path}
	cl.conns = []*conn{c}
	c.afterDial = func() {
		go cl.Close()
		for !cl.isClosed() {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond) // lets Close pass its shutdown snapshot
	}
	done := make(chan error, 1)
	go func() { _, err := c.session(); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("err = %v, want ErrClientClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session hangs after Close")
	}
}
