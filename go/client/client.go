// Package client is a Go client for the mountOS native filesystem API.
package client

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// DefaultPoolSize is the number of connections used when Options.PoolSize is 0.
const DefaultPoolSize = 4

// maxInflightCeiling bounds Options.MaxInflight.
const maxInflightCeiling = 1024

// DefaultMaxInflight is the most requests kept outstanding on one connection
// when Options.MaxInflight is 0.
const DefaultMaxInflight = 32

// Options configures a Client.
type Options struct {
	PoolSize int // connections to open, DefaultPoolSize when 0

	// MaxInflight is the most requests kept outstanding on one connection,
	// DefaultMaxInflight when 0. The limit in force is the lower of this and
	// the server's max_inflight. A server that does not report the "pipelining"
	// capability gets one request at a time, and so does a value of 1.
	MaxInflight int

	// CallTimeout bounds every request. 0 means no limit. A request that gets no
	// reply in time returns a *TimeoutError. When the connection allows several
	// requests in flight, only that call fails and the late reply is discarded.
	// Otherwise the connection is closed and dialed again on the next call. A
	// multi-frame call, such as a large File.ReadAt, applies the limit to each
	// frame.
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
	free        chan struct{} // a token per released slot, wakes acquire
	maxInflight int
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
	inflight := min(opts.MaxInflight, maxInflightCeiling)
	if inflight <= 0 {
		inflight = DefaultMaxInflight
	}
	cl := &Client{
		path: path, hub: newHub(opts.Logger), closedCh: make(chan struct{}),
		free: make(chan struct{}, n*inflight), maxInflight: inflight, callTimeout: opts.CallTimeout,
	}
	for i := 0; i < n; i++ {
		c := &conn{cl: cl, path: path}
		cl.conns = append(cl.conns, c)
		c.mu.Lock()
		_, err := c.ensure()
		c.mu.Unlock()
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
			c.mu.Lock()
			c.drop()
			c.mu.Unlock()
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

// acquire reserves one in-flight slot on the least loaded connection and
// returns its socket. Ties go to a rotating start. When every connection is at
// its limit it waits until any slot frees, so a call never queues behind a
// slow request while another connection has room. The caller calls release.
func (cl *Client) acquire() (*sock, error) {
	n := uint64(len(cl.conns))
	for {
		if cl.isClosed() {
			return nil, ErrClientClosed
		}
		start := cl.next.Add(1)
		var best *conn
		bestLoad := 0
		for i := uint64(0); i < n; i++ {
			c := cl.conns[(start+i)%n]
			if load, room := c.load(); room && (best == nil || load < bestLoad) {
				best, bestLoad = c, load
			}
		}
		if best == nil {
			select {
			case <-cl.free:
			case <-cl.closedCh:
			}
			continue
		}
		sk, err := best.session()
		if err != nil {
			return nil, err
		}
		select {
		case sk.slots <- struct{}{}:
			return sk, nil
		default: // another caller took the room
		}
	}
}

// inflightLimit is the requests allowed at once on a connection, from the Hello reply.
func (cl *Client) inflightLimit(resp *wire.HelloResponse) int {
	if !slices.Contains(resp.GetCapabilities(), capPipelining) {
		return 1
	}
	return int(max(1, min(uint32(cl.maxInflight), resp.GetMaxInflight())))
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
	sk, err := cl.acquire()
	if err != nil {
		return err
	}
	defer sk.release()
	_, err = sk.call(method, req, resp, nil)
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
	sk, err := cl.acquire()
	if err != nil {
		return nil, err
	}
	defer sk.release()
	return cl.openOn(sk, path, flags, mode)
}

// openOn opens path on sk. The caller holds a slot of sk.
func (cl *Client) openOn(sk *sock, path string, flags wire.OpenFlag, mode uint32) (*File, error) {
	var r wire.OpenResponse
	if _, err := sk.call(mOpen, &wire.OpenRequest{Path: path, Flags: uint32(flags), Mode: mode}, &r, nil); err != nil {
		return nil, err
	}
	return &File{cl: cl, sk: sk, max: sk.maxData, handle: r.GetHandle(), path: path, flags: flags}, nil
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
	sk, err := cl.acquire()
	if err != nil {
		return nil, err
	}
	defer sk.release()
	var r wire.OpendirResponse
	if _, err := sk.call(mOpendir, &wire.OpendirRequest{Path: path}, &r, nil); err != nil {
		return nil, err
	}
	return &Dir{cl: cl, sk: sk, handle: r.GetHandle()}, nil
}
