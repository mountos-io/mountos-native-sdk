package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

const (
	protoVersion    = 1
	maxPayloadBytes = 8 << 20
	maxMetaBytes    = 16 << 20
	dialTimeout     = 10 * time.Second
	readBufBytes    = 32 << 10
	coalesceBytes   = 32 << 10 // largest payload copied into one buffer with its header
	maxKeptBuf      = 1 << 20  // largest encode buffer a socket keeps between requests
	abandonFactor   = 4        // abandoned ids a socket keeps per allowed in-flight request
	capPipelining   = "pipelining"
)

var limits = frame.Limits{MaxMetaBytes: maxMetaBytes, MaxPayloadBytes: maxPayloadBytes}

// helloCapabilities are the capabilities this client asks for in Hello.
var helloCapabilities = []string{"bulk_io", "ls_stat", "remove_all", "watch", "warm", capPipelining}

// waiter is the reply slot of one request. The reader sends at most one frame.
type waiter struct{ ch chan frame.Frame }

// A waiter goes back to the pool only when it is out of the pending map and
// its channel is empty.
var waiterPool = sync.Pool{New: func() any { return &waiter{ch: make(chan frame.Frame, 1)} }}

// sock is one live socket, its reader goroutine, and the state of the requests
// on it. The ids, the pending calls and the slots belong to the socket, so a
// redial starts clean and a File or Watch can tell that its socket is gone.
// dead is closed when the reader exits, for any reason. err is set before dead
// is closed.
type sock struct {
	nc   net.Conn
	err  error
	dead chan struct{}
	c    *conn

	slots chan struct{} // a token per outstanding request, capacity limit

	// Guarded by pmu: pend, abandoned, nextID.
	abandoned map[uint64]uint16 // timed out ids and their methods. A late reply is discarded.
	pend      map[uint64]*waiter

	// Guarded by wmu: wbuf, mbuf.
	mbuf []byte // reused request metadata buffer
	wbuf []byte // reused encode buffer for small frames

	// slots, maxData, abandonCap and limit are set once before ready becomes
	// true. Read them only after ready.
	maxData    int // largest payload per frame, from Hello
	abandonCap int // abandoned ids kept before the socket is dropped
	limit      int // requests outstanding at once
	nextID     uint64
	errOnce    sync.Once
	pmu        sync.Mutex
	wmu        sync.Mutex // serializes request writes
	ready      atomic.Bool
	closing    atomic.Bool // set when this side closes the socket on purpose
}

// setErr records why the socket ended. The first reason wins.
func (s *sock) setErr(err error) { s.errOnce.Do(func() { s.err = err }) }

func (s *sock) isDead() bool {
	select {
	case <-s.dead:
		return true
	default:
		return false
	}
}

// lostErr is the error for a request on a socket that ended. Call it only after dead is closed.
func (s *sock) lostErr() error {
	return &ProtocolError{Op: "read frame", Err: fmt.Errorf("%w: %v", ErrConnLost, s.err)} //nolint:errorlint // The cause stays text so callers cannot match transport errors through it.
}

// conn is one pooled connection. mu serializes dial and drop. Requests do not
// take it: they go through the sock, which allows limit of them at once. A
// broken conn is redialed by the next caller. Files opened on the old socket
// fail cleanly because that socket is dead.
type conn struct {
	cl   *Client
	sk   *sock // nil when not dialed. Changed under mu.
	cur  atomic.Pointer[sock]
	path string
	mu   sync.Mutex
}

// session returns the live socket, dialing first when there is none.
func (c *conn) session() (*sock, error) {
	if sk := c.cur.Load(); sk != nil && sk.ready.Load() && !sk.isDead() {
		return sk, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ensure()
}

// load reports the requests outstanding on the live socket and whether it has
// room for one more. A conn without a live socket reports no load, since the
// next caller dials it.
func (c *conn) load() (n int, room bool) {
	sk := c.cur.Load()
	if sk == nil || !sk.ready.Load() || sk.isDead() {
		return 0, true
	}
	n = len(sk.slots)
	return n, n < sk.limit
}

// ensure dials and handshakes when there is no live socket. Caller holds mu.
func (c *conn) ensure() (*sock, error) {
	if c.cl.isClosed() {
		return nil, ErrClientClosed
	}
	if sk := c.sk; sk != nil && sk.ready.Load() && !sk.isDead() {
		return sk, nil
	}
	c.drop()
	nc, err := net.DialTimeout("unix", c.path, dialTimeout)
	if err != nil {
		return nil, &DialError{Path: c.path, Err: err}
	}
	sk := &sock{c: c, nc: nc, dead: make(chan struct{}), pend: map[uint64]*waiter{}, abandoned: map[uint64]uint16{}}
	c.sk = sk
	c.cur.Store(sk)
	go c.readLoop(sk)
	var resp wire.HelloResponse
	err = sk.roundTrip(mHello, &wire.HelloRequest{
		ClientName: "mountos-native-sdk-go", MinVersion: protoVersion, MaxVersion: protoVersion,
		Capabilities: helloCapabilities,
	}, nil, &resp, nil)
	if err == nil && resp.GetVersion() != protoVersion {
		err = fmt.Errorf("server chose version %d", resp.GetVersion())
	}
	if err != nil {
		c.drop()
		return nil, &DialError{Path: c.path, Err: err}
	}
	c.cl.setCaps(resp.GetCapabilities())
	sk.maxData = maxPayloadBytes
	if m := int(resp.GetMaxPayloadBytes()); m > 0 && m < sk.maxData {
		sk.maxData = m
	}
	sk.limit = c.cl.inflightLimit(&resp)
	if sk.limit > 1 {
		sk.abandonCap = sk.limit * abandonFactor
	}
	sk.slots = make(chan struct{}, sk.limit)
	sk.ready.Store(true)
	return sk, nil
}

// drop closes the socket on purpose and waits for its reader to exit. Caller holds mu.
func (c *conn) drop() {
	sk := c.sk
	if sk == nil {
		return
	}
	c.sk = nil
	sk.closing.Store(true)
	_ = sk.nc.Close() // Teardown; the reader goroutine ends on the close.
	<-sk.dead
}

// shutdown closes the socket without taking mu, so a call blocked on it
// fails at once. Client.Close uses it, then drops under mu.
func (c *conn) shutdown() {
	if sk := c.cur.Load(); sk != nil {
		sk.closing.Store(true)
		_ = sk.nc.Close() // Teardown; the reader goroutine ends on the close.
	}
}

// fail closes a socket that broke or lost sync and waits for its reader to
// exit. Unlike a drop it reports the loss to watches and listeners, with err
// as the reason. Every request still outstanding on the socket fails with the
// connection-lost error.
func (s *sock) fail(err error) {
	s.setErr(err)
	_ = s.nc.Close() // The socket already failed; err is the reason.
	<-s.dead
}

// readLoop reads every frame of one socket. Responses go to the waiting call.
// Notices go to the client. An unexpected exit reports a lost connection.
func (c *conn) readLoop(sk *sock) {
	br := bufio.NewReaderSize(sk.nc, readBufBytes)
	for {
		f, err := frame.Read(br, limits)
		if err != nil {
			sk.setErr(err)
			break
		}
		if f.Flags&frame.FlagNotify != 0 {
			c.cl.hub.notice(sk, f)
			continue
		}
		if !sk.deliver(f) {
			// A reply that names no pending or abandoned request breaks the contract.
			sk.setErr(fmt.Errorf("unexpected reply: method %d id %d", f.Method, f.RequestID))
			break
		}
	}
	_ = sk.nc.Close() // The read loop ended; the stored error is the reason.
	close(sk.dead)
	if !sk.closing.Load() {
		c.cl.hub.connLost(sk)
	}
}

// deliver hands a reply to its waiter, or discards it when the request was
// abandoned. It reports false for an id that is neither.
func (s *sock) deliver(f frame.Frame) bool {
	s.pmu.Lock()
	w, ok := s.pend[f.RequestID]
	var method uint16
	var abandoned bool
	if ok {
		delete(s.pend, f.RequestID)
	} else if method, abandoned = s.abandoned[f.RequestID]; abandoned {
		delete(s.abandoned, f.RequestID)
	}
	s.pmu.Unlock()
	switch {
	case ok:
		w.ch <- f // capacity 1 and one send per waiter: never blocks
	case abandoned:
		s.releaseLate(method, f)
	default:
		return false
	}
	return true
}

// releaseLate frees the server resource that an abandoned request created. A
// request that opened a handle or a watch succeeded on the server after the
// caller gave up, and nobody holds its id. The undo runs on its own goroutine,
// since the reader must not wait for a reply. It ends with the socket.
func (s *sock) releaseLate(method uint16, f frame.Frame) {
	if f.Flags&frame.FlagResponse == 0 || f.Flags&frame.FlagError != 0 || f.Method != method {
		return
	}
	var undo uint16
	var req proto.Message
	switch method {
	case mOpen:
		var r wire.OpenResponse
		if proto.Unmarshal(f.Meta, &r) == nil {
			undo, req = mClose, &wire.CloseRequest{Handle: r.GetHandle()}
		}
	case mOpendir:
		var r wire.OpendirResponse
		if proto.Unmarshal(f.Meta, &r) == nil {
			undo, req = mClosedir, &wire.ClosedirRequest{Handle: r.GetHandle()}
		}
	case mWatch:
		var r wire.WatchResponse
		if proto.Unmarshal(f.Meta, &r) == nil {
			undo, req = mUnwatch, &wire.UnwatchRequest{WatchId: r.GetWatchId()}
		}
	}
	if req == nil {
		return
	}
	go func() { _, _ = s.callWait(undo, req, &wire.Empty{}, nil) }() // Best-effort undo of an abandoned request.
}

// hold takes one in-flight slot, waiting while the socket is at its limit. It
// reports false when the socket is dead. Pair it with release.
func (s *sock) hold() bool {
	if s.isDead() {
		return false
	}
	select {
	case s.slots <- struct{}{}:
		return true
	case <-s.dead:
		return false
	}
}

// release returns a slot and wakes a caller that waits for any free slot.
func (s *sock) release() {
	<-s.slots
	select {
	case s.c.cl.free <- struct{}{}:
	default:
	}
}

// callWait runs one request on the socket of a File, Dir or Watch. It waits
// for a slot, then calls.
func (s *sock) callWait(method uint16, req, resp proto.Message, payload []byte) ([]byte, error) {
	if !s.hold() {
		if s.c.cl.isClosed() {
			return nil, ErrClientClosed
		}
		return nil, ErrConnLost
	}
	defer s.release()
	return s.call(method, req, resp, payload)
}

// call runs one request. The caller holds a slot. A transport or protocol
// failure closes the socket and fails every request outstanding on it. A
// WireError keeps it, and so does a request that failed before any byte was
// written, and a timeout while the abandoned set has room.
func (s *sock) call(method uint16, req, resp proto.Message, payload []byte) ([]byte, error) {
	if err := s.c.cl.need(method); err != nil {
		return nil, err
	}
	var out []byte
	err := s.roundTrip(method, req, payload, resp, &out)
	if err != nil && !keepsSocket(err) {
		s.fail(err)
	}
	return out, err
}

// keepsSocket reports whether err leaves the socket in sync with the server.
func keepsSocket(err error) bool {
	var we *WireError
	var pe *ProtocolError
	var te *TimeoutError
	return errors.As(err, &we) || (errors.As(err, &pe) && pe.unsent) || (errors.As(err, &te) && te.kept)
}

// write sends f with one system call. A small frame is copied into the
// socket's reused buffer. A frame with a large payload goes out as a vectored
// write, so the payload is not copied. Caller holds wmu.
func (s *sock) write(f frame.Frame) error {
	if len(f.Payload) > coalesceBytes {
		return frame.WriteBuffers(s.nc, f, limits)
	}
	buf, err := frame.Append(s.wbuf[:0], f, limits)
	if err != nil {
		return err
	}
	if cap(buf) <= maxKeptBuf {
		s.wbuf = buf
	}
	_, err = s.nc.Write(buf)
	return err
}

// send registers w under a new id and writes the request. Registration comes
// first, so the reply cannot arrive before the waiter exists. On an error the
// waiter is out of the pending map.
func (s *sock) send(w *waiter, method uint16, req proto.Message, payload []byte) (uint64, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	meta, err := proto.MarshalOptions{}.MarshalAppend(s.mbuf[:0], req)
	if err != nil {
		return 0, &ProtocolError{Op: "encode request", Err: err, unsent: true}
	}
	if cap(meta) <= maxKeptBuf {
		s.mbuf = meta
	}
	s.pmu.Lock()
	s.nextID++
	id := s.nextID
	s.pend[id] = w
	s.pmu.Unlock()
	d := s.c.cl.callTimeout
	if d > 0 {
		_ = s.nc.SetWriteDeadline(time.Now().Add(d)) // A failed deadline set surfaces as a write error below.
	}
	if err = s.write(frame.Frame{Method: method, RequestID: id, Meta: meta, Payload: payload}); err != nil {
		s.reclaim(id, w, false, method)
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return 0, &TimeoutError{After: d}
		}
		return 0, &ProtocolError{Op: "write frame", Err: err, unsent: errors.Is(err, frame.ErrTooLarge)}
	}
	return id, nil
}

// reclaim takes request id out of the pending map. When the reader already
// took it, the reply is on its way and reclaim returns it with delivered set.
// With abandon set and room in the abandoned set, the id joins that set with
// its method, so the late reply is discarded, and kept is true.
func (s *sock) reclaim(id uint64, w *waiter, abandon bool, method uint16) (f frame.Frame, delivered, kept bool) {
	s.pmu.Lock()
	_, pending := s.pend[id]
	if pending {
		delete(s.pend, id)
		if abandon && len(s.abandoned) < s.abandonCap {
			s.abandoned[id] = method
			kept = true
		}
	}
	s.pmu.Unlock()
	if !pending {
		f, delivered = <-w.ch, true
	}
	return f, delivered, kept
}

// await waits for the reply to request id and returns w to the pool.
func (s *sock) await(w *waiter, id uint64, method uint16) (frame.Frame, error) {
	defer waiterPool.Put(w)
	var expired <-chan time.Time
	d := s.c.cl.callTimeout
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		expired = t.C
	}
	select {
	case f := <-w.ch:
		return f, nil
	case <-expired:
		f, delivered, kept := s.reclaim(id, w, true, method)
		if delivered {
			return f, nil
		}
		return frame.Frame{}, &TimeoutError{After: d, kept: kept}
	case <-s.dead:
		f, delivered, _ := s.reclaim(id, w, false, method)
		if delivered {
			return f, nil
		}
		return frame.Frame{}, s.lostErr()
	}
}

// roundTrip sends one request and decodes its reply. It never closes the socket.
func (s *sock) roundTrip(method uint16, req proto.Message, payload []byte, resp proto.Message, out *[]byte) error {
	if s.isDead() {
		return s.lostErr()
	}
	w := waiterPool.Get().(*waiter)
	id, err := s.send(w, method, req, payload)
	if err != nil {
		waiterPool.Put(w)
		return err
	}
	f, err := s.await(w, id, method)
	if err != nil {
		return err
	}
	if f.Flags&frame.FlagResponse == 0 || f.Method != method {
		return &ProtocolError{Op: "read frame", Err: fmt.Errorf("bad response flags %#x method %d", f.Flags, f.Method)}
	}
	if f.Flags&frame.FlagError != 0 {
		var we wire.WireError
		if err := proto.Unmarshal(f.Meta, &we); err != nil {
			return &ProtocolError{Op: "decode error frame", Err: err}
		}
		return &WireError{Code: we.GetCode(), Message: we.GetMessage()}
	}
	if err := proto.Unmarshal(f.Meta, resp); err != nil {
		return &ProtocolError{Op: "decode response", Err: err}
	}
	if out != nil {
		*out = f.Payload
	}
	return nil
}
