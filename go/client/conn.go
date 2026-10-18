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
	maxKeptBuf      = 1 << 20  // largest encode buffer a connection keeps between requests
)

var limits = frame.Limits{MaxMetaBytes: maxMetaBytes, MaxPayloadBytes: maxPayloadBytes}

// sock is one live socket and its reader goroutine. dead is closed when the
// reader exits, for any reason. err is set before dead is closed.
type sock struct {
	nc      net.Conn
	closing atomic.Bool // set when this side closes the socket on purpose
	dead    chan struct{}
	errOnce sync.Once
	err     error
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

// pending is the one request a conn waits on. The reader fills ch.
type pending struct {
	id uint64
	ch chan frame.Frame
}

// helloCapabilities are the capabilities this client asks for in Hello.
var helloCapabilities = []string{"bulk_io", "ls_stat", "remove_all", "watch", "warm"}

// conn is one connection. mu serializes requests: one request is in flight at
// a time. A reader goroutine reads every frame, so notices arrive on idle
// connections too. A broken conn is redialed by the next caller, and gen
// changes so Files opened on the old socket fail cleanly.
type conn struct {
	mu      sync.Mutex
	cl      *Client
	path    string
	sk      *sock // nil when not dialed. Changed under mu.
	cur     atomic.Pointer[sock]
	nextID  uint64
	gen     uint64
	maxData int    // largest payload per frame, from Hello
	wbuf    []byte // reused encode buffer for small frames. Guarded by mu.
	mbuf    []byte // reused request metadata buffer. Guarded by mu.
	slot    pending
	timer   *time.Timer // reused call timer. Guarded by mu.

	rmu  sync.Mutex // guards pend
	pend *pending   // &slot while a request waits, else nil
}

// lock takes the connection for one request. unlock releases it and wakes a
// caller that waits for any free connection.
func (c *conn) lock() { c.mu.Lock() }

func (c *conn) unlock() {
	c.mu.Unlock()
	select {
	case c.cl.free <- struct{}{}:
	default:
	}
}

// alive reports whether the socket is open and its reader still runs. Caller holds mu.
func (c *conn) alive() bool { return c.sk != nil && !c.sk.isDead() }

// ensure dials and handshakes when there is no live socket. Caller holds mu.
func (c *conn) ensure() error {
	if c.cl.isClosed() {
		return ErrClientClosed
	}
	if c.alive() {
		return nil
	}
	c.drop()
	nc, err := net.DialTimeout("unix", c.path, dialTimeout)
	if err != nil {
		return &DialError{Path: c.path, Err: err}
	}
	sk := &sock{nc: nc, dead: make(chan struct{})}
	c.sk, c.gen, c.nextID = sk, c.gen+1, 0
	c.cur.Store(sk)
	go c.readLoop(sk)
	var resp wire.HelloResponse
	err = c.roundTrip(mHello, &wire.HelloRequest{
		ClientName: "mountos-native-sdk-go", MinVersion: protoVersion, MaxVersion: protoVersion,
		Capabilities: helloCapabilities,
	}, nil, &resp, nil)
	if err != nil {
		c.drop()
		return &DialError{Path: c.path, Err: err}
	}
	if resp.GetVersion() != protoVersion {
		c.drop()
		return &DialError{Path: c.path, Err: fmt.Errorf("server chose version %d", resp.GetVersion())}
	}
	c.cl.setCaps(resp.GetCapabilities())
	c.maxData = maxPayloadBytes
	if m := int(resp.GetMaxPayloadBytes()); m > 0 && m < c.maxData {
		c.maxData = m
	}
	return nil
}

// drop closes the socket on purpose and waits for its reader to exit. Caller holds mu.
func (c *conn) drop() { c.release(true) }

// fail closes a socket that broke or lost sync. Unlike drop it reports the
// loss to watches and listeners, with err as the reason. Caller holds mu.
func (c *conn) fail(err error) {
	if c.sk != nil {
		c.sk.setErr(err)
	}
	c.release(false)
}

func (c *conn) release(onPurpose bool) {
	sk := c.sk
	if sk == nil {
		return
	}
	c.sk = nil
	if onPurpose {
		sk.closing.Store(true)
	}
	sk.nc.Close()
	<-sk.dead
}

// shutdown closes the socket without taking mu, so a call blocked on it
// fails at once. Client.Close uses it, then drops under mu.
func (c *conn) shutdown() {
	if sk := c.cur.Load(); sk != nil {
		sk.closing.Store(true)
		sk.nc.Close()
	}
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
		c.rmu.Lock()
		p := c.pend
		match := p != nil && p.id == f.RequestID
		if match {
			c.pend = nil
			p.ch <- f
		}
		c.rmu.Unlock()
		if !match {
			// One request is in flight, so any other reply breaks the contract.
			sk.setErr(fmt.Errorf("unexpected reply: method %d id %d", f.Method, f.RequestID))
			break
		}
	}
	sk.nc.Close()
	close(sk.dead)
	if !sk.closing.Load() {
		c.cl.hub.connLost(sk)
	}
}

// call runs one request on the live socket. Caller holds mu and has called ensure.
// A transport or protocol failure drops the socket. A WireError keeps it, and so
// does a request that failed before any byte was written.
func (c *conn) call(method uint16, req, resp proto.Message, payload []byte) ([]byte, error) {
	if err := c.cl.need(method); err != nil {
		return nil, err
	}
	var out []byte
	err := c.roundTrip(method, req, payload, resp, &out)
	if err != nil && !keepsSocket(err) {
		c.fail(err)
	}
	return out, err
}

// keepsSocket reports whether err leaves the socket in sync with the server.
func keepsSocket(err error) bool {
	var we *WireError
	var pe *ProtocolError
	return errors.As(err, &we) || (errors.As(err, &pe) && pe.unsent)
}

// write sends f with one system call. A small frame is copied into the
// connection's reused buffer. A frame with a large payload goes out as a
// vectored write, so the payload is not copied. Caller holds mu.
func (c *conn) write(nc net.Conn, f frame.Frame) error {
	if len(f.Payload) > coalesceBytes {
		return frame.WriteBuffers(nc, f, limits)
	}
	buf, err := frame.Append(c.wbuf[:0], f, limits)
	if err != nil {
		return err
	}
	if cap(buf) <= maxKeptBuf {
		c.wbuf = buf
	}
	_, err = nc.Write(buf)
	return err
}

// arm makes slot the waiting request id and clears any reply a dead socket
// left behind. One request is in flight per conn, so one slot and its channel
// serve every call. Caller holds mu.
func (c *conn) arm(id uint64) *pending {
	p := &c.slot
	if p.ch == nil {
		p.ch = make(chan frame.Frame, 1)
	}
	select {
	case <-p.ch:
	default:
	}
	c.rmu.Lock()
	p.id = id
	c.pend = p
	c.rmu.Unlock()
	return p
}

// disarm clears the waiting request. Caller holds mu.
func (c *conn) disarm() {
	c.rmu.Lock()
	c.pend = nil
	c.rmu.Unlock()
}

// callTimer returns the reused timer set to fire after d. Caller holds mu and
// calls stopTimer when the call ends.
func (c *conn) callTimer(d time.Duration) *time.Timer {
	if c.timer == nil {
		c.timer = time.NewTimer(d)
	} else {
		c.timer.Reset(d)
	}
	return c.timer
}

func (c *conn) stopTimer() {
	if c.timer != nil {
		c.timer.Stop()
	}
}

func (c *conn) roundTrip(method uint16, req proto.Message, payload []byte, resp proto.Message, out *[]byte) error {
	meta, err := proto.MarshalOptions{}.MarshalAppend(c.mbuf[:0], req)
	if err != nil {
		return &ProtocolError{Op: "encode request", Err: err, unsent: true}
	}
	if cap(meta) <= maxKeptBuf {
		c.mbuf = meta
	}
	sk := c.sk
	c.nextID++
	p := c.arm(c.nextID)
	defer c.disarm()
	var expired <-chan time.Time
	if d := c.cl.callTimeout; d > 0 {
		expired = c.callTimer(d).C
		defer c.stopTimer()
		sk.nc.SetWriteDeadline(time.Now().Add(d))
		defer sk.nc.SetWriteDeadline(time.Time{})
	}
	err = c.write(sk.nc, frame.Frame{Method: method, RequestID: p.id, Meta: meta, Payload: payload})
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return &TimeoutError{After: c.cl.callTimeout}
		}
		return &ProtocolError{Op: "write frame", Err: err, unsent: errors.Is(err, frame.ErrTooLarge)}
	}
	var f frame.Frame
	select {
	case f = <-p.ch:
	case <-expired:
		return &TimeoutError{After: c.cl.callTimeout}
	case <-sk.dead:
		select {
		case f = <-p.ch: // the reply arrived just before the socket died
		default:
			return &ProtocolError{Op: "read frame", Err: fmt.Errorf("%w: %v", ErrConnLost, sk.err)}
		}
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
