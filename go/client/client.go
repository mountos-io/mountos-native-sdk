// Package client is a Go client for the mountOS native filesystem API.
package client

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// DefaultPoolSize is the number of connections used when Options.PoolSize is 0.
const DefaultPoolSize = 4

// Options configures a Client.
type Options struct {
	PoolSize int // connections to open, DefaultPoolSize when 0

	// CallTimeout bounds every request. 0 means no limit. A request that gets no
	// reply in time returns a *TimeoutError, and its connection is closed and
	// dialed again on the next call. A multi-frame call, such as a large
	// File.ReadAt, applies the limit to each frame.
	CallTimeout time.Duration

	// Logger receives diagnostic lines: events that no listener took, dropped
	// events, and listener panics. Pass your own, for example a *log.Logger.
	// When nil, lines go to os.Stderr with the prefix "mountos: ".
	Logger Logger
}

// Client holds a pool of connections to one API socket. It is safe for concurrent use.
type Client struct {
	path        string
	hub         *hub
	conns       []*conn
	next        atomic.Uint64
	free        chan struct{} // a token per released connection, wakes acquire
	callTimeout time.Duration
	caps        atomic.Pointer[map[string]struct{}] // capabilities the server reported. nil or empty means all.
	mu          sync.Mutex
	closed      bool
	closedCh    chan struct{}
	closeOnce   sync.Once
}

// Dial connects to the unix socket at path. It opens every pooled connection
// and completes Hello on each, so a bad socket fails here.
func Dial(path string, opts Options) (*Client, error) {
	n := opts.PoolSize
	if n <= 0 {
		n = DefaultPoolSize
	}
	cl := &Client{
		path: path, hub: newHub(opts.Logger), closedCh: make(chan struct{}),
		free: make(chan struct{}, n), callTimeout: opts.CallTimeout,
	}
	for i := 0; i < n; i++ {
		c := &conn{cl: cl, path: path}
		cl.conns = append(cl.conns, c)
		c.lock()
		err := c.ensure()
		c.unlock()
		if err != nil {
			cl.Close()
			return nil, err
		}
	}
	return cl, nil
}

// Close stops every reader, closes every connection and Watch, and stops the
// event dispatcher. Open Files become unusable. Close waits for the dispatcher
// unless a listener is running. Then the dispatcher exits when that listener
// returns, so a listener may call Close. Close is safe to call more than once.
func (cl *Client) Close() error {
	cl.closeOnce.Do(func() {
		cl.mu.Lock()
		cl.closed = true
		cl.mu.Unlock()
		close(cl.closedCh)
		for _, c := range cl.conns {
			c.shutdown()
		}
		for _, c := range cl.conns {
			c.lock()
			c.drop()
			c.unlock()
		}
		cl.hub.closeWatches()
		cl.hub.shutdown()
	})
	return nil
}

func (cl *Client) isClosed() bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.closed
}

// acquire locks the first free connection, scanning from a rotating start.
// When all are busy it waits until any connection is released, so a call never
// queues behind a slow request while another connection is free.
func (cl *Client) acquire() *conn {
	n := uint64(len(cl.conns))
	for {
		start := cl.next.Add(1)
		for i := uint64(0); i < n; i++ {
			if c := cl.conns[(start+i)%n]; c.mu.TryLock() {
				return c
			}
		}
		select {
		case <-cl.free:
		case <-cl.closedCh:
			c := cl.conns[start%n]
			c.lock()
			return c
		}
	}
}

// setCaps records the capabilities from Hello.
func (cl *Client) setCaps(list []string) {
	set := make(map[string]struct{}, len(list))
	for _, name := range list {
		set[name] = struct{}{}
	}
	cl.caps.Store(&set)
}

// methodCapability maps the methods that need a capability to its name.
var methodCapability = map[uint16]string{
	mLs: "ls_stat", mRemoveAll: "remove_all", mWatch: "watch", mUnwatch: "watch",
	mStatBatch: "bulk_io", mReadBatch: "bulk_io", mWriteBatch: "bulk_io", mReadFile: "bulk_io", mWriteFile: "bulk_io",
	mWarmOpen: "warm", mWarmRead: "warm",
}

// need fails a call locally when the server reported capabilities and lacks the
// one method needs. A server that reported none (an older server) allows all.
func (cl *Client) need(method uint16) error {
	name, ok := methodCapability[method]
	set := cl.caps.Load()
	if !ok || set == nil || len(*set) == 0 {
		return nil
	}
	if _, has := (*set)[name]; has {
		return nil
	}
	return &WireError{Code: CodeUnsupportedOperation, Message: "server lacks capability " + name}
}

// run sends one request on a pooled connection.
func (cl *Client) run(method uint16, req, resp proto.Message) error {
	if cl.isClosed() {
		return ErrClientClosed
	}
	c := cl.acquire()
	defer c.unlock()
	if err := c.ensure(); err != nil {
		return err
	}
	_, err := c.call(method, req, resp, nil)
	return err
}

// Open opens path. flags are wire.OpenFlag values; the low two bits pick the access mode.
// The returned File stays on the connection that opened it.
func (cl *Client) Open(path string, flags wire.OpenFlag, mode uint32) (*File, error) {
	if flags&3 == 3 {
		return nil, errors.New("mountos: invalid access mode in open flags")
	}
	if cl.isClosed() {
		return nil, ErrClientClosed
	}
	c := cl.acquire()
	defer c.unlock()
	return cl.openLocked(c, path, flags, mode)
}

// openLocked opens path on c. Caller holds c's lock.
func (cl *Client) openLocked(c *conn, path string, flags wire.OpenFlag, mode uint32) (*File, error) {
	if err := c.ensure(); err != nil {
		return nil, err
	}
	var r wire.OpenResponse
	if _, err := c.call(mOpen, &wire.OpenRequest{Path: path, Flags: uint32(flags), Mode: mode}, &r, nil); err != nil {
		return nil, err
	}
	return &File{cl: cl, c: c, gen: c.gen, max: c.maxData, handle: r.GetHandle(), path: path, flags: flags}, nil
}

// DirEntry is one directory entry. Info is set when the server returns attributes.
type DirEntry = wire.DirEntry

// DirEntryWithInfo is a DirEntry from Ls, which always carries Info.
type DirEntryWithInfo = wire.DirEntry

// Utimens sets access and modification times. A nil time stays unchanged.
func (cl *Client) Utimens(path string, atime, mtime *time.Time) error {
	req := &wire.UtimensRequest{Path: path}
	if atime != nil {
		req.AtimeNs, req.SetAtime = atime.UnixNano(), true
	}
	if mtime != nil {
		req.MtimeNs, req.SetMtime = mtime.UnixNano(), true
	}
	return cl.run(mUtimens, req, &wire.Empty{})
}

// Ls lists up to maxEntries entries of path with attributes. Pass the returned
// cursor to continue; an empty cursor means the listing is complete.
func (cl *Client) Ls(path string, maxEntries uint32, cursor string) ([]*DirEntryWithInfo, string, error) {
	var r wire.LsResponse
	if err := cl.run(mLs, &wire.LsRequest{Path: path, MaxEntries: maxEntries, Cursor: cursor}, &r); err != nil {
		return nil, "", err
	}
	return r.GetEntries(), r.GetNextCursor(), nil
}

// Opendir opens a directory handle. The returned Dir stays on the connection that opened it.
func (cl *Client) Opendir(path string) (*Dir, error) {
	if cl.isClosed() {
		return nil, ErrClientClosed
	}
	c := cl.acquire()
	defer c.unlock()
	if err := c.ensure(); err != nil {
		return nil, err
	}
	var r wire.OpendirResponse
	if _, err := c.call(mOpendir, &wire.OpendirRequest{Path: path}, &r, nil); err != nil {
		return nil, err
	}
	return &Dir{cl: cl, c: c, gen: c.gen, handle: r.GetHandle()}, nil
}
