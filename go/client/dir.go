package client

import (
	"errors"
	"sync"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// Dir is an open directory handle. It is bound to one connection. Calls on a Dir are serialized.
type Dir struct {
	cl     *Client
	c      *conn
	gen    uint64
	handle uint64

	mu     sync.Mutex
	closed bool
}

// Readdir returns up to max entries and whether the listing reached its end.
func (d *Dir) Readdir(max uint32) ([]*DirEntry, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var r wire.ReaddirResponse
	if err := d.do(mReaddir, &wire.ReaddirRequest{Handle: d.handle, MaxEntries: max}, &r); err != nil {
		return nil, false, err
	}
	return r.GetEntries(), r.GetEof(), nil
}

// Close releases the handle. A second Close returns ErrFileClosed.
func (d *Dir) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.do(mClosedir, &wire.ClosedirRequest{Handle: d.handle}, &wire.Empty{})
	if !errors.Is(err, ErrFileClosed) {
		d.closed = true
	}
	return err
}

// do runs one request on the Dir's connection. Caller holds d.mu.
func (d *Dir) do(method uint16, req, resp proto.Message) error {
	if d.closed {
		return ErrFileClosed
	}
	if d.cl.isClosed() {
		return ErrClientClosed
	}
	d.c.mu.Lock()
	defer d.c.mu.Unlock()
	if !d.c.alive() || d.c.gen != d.gen {
		return ErrConnLost
	}
	_, err := d.c.call(method, req, resp, nil)
	return err
}
