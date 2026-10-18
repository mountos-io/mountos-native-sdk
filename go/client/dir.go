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
	sk     *sock
	handle uint64

	mu     sync.Mutex
	closed bool
}

// Readdir returns up to limit entries and whether the listing reached its end.
func (d *Dir) Readdir(limit uint32) ([]*DirEntry, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var r wire.ReaddirResponse
	if err := d.do(mReaddir, &wire.ReaddirRequest{Handle: d.handle, MaxEntries: limit}, &r); err != nil {
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
	_, err := d.sk.callWait(method, req, resp, nil)
	return err
}
