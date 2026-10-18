package client

import (
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
)

var limits = frame.Limits{MaxMetaBytes: maxMetaBytes, MaxPayloadBytes: maxPayloadBytes}

// sock is one live socket and its reader goroutine. dead is closed when the
// reader exits, for any reason. err is set before dead is closed.
type sock struct {
	nc      net.Conn
	closing atomic.Bool // set when this side closes the socket on purpose
	dead    chan struct{}
	err     error
}

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
	maxData int // largest payload per frame, from Hello

	rmu  sync.Mutex // guards pend
	pend *pending
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
	}, nil, &resp, nil)
	if err != nil {
		c.drop()
		return &DialError{Path: c.path, Err: err}
	}
	if resp.GetVersion() != protoVersion {
		c.drop()
		return &DialError{Path: c.path, Err: fmt.Errorf("server chose version %d", resp.GetVersion())}
	}
	c.maxData = maxPayloadBytes
	if m := int(resp.GetMaxPayloadBytes()); m > 0 && m < c.maxData {
		c.maxData = m
	}
	return nil
}

// drop closes the socket and waits for its reader to exit. Caller holds mu.
func (c *conn) drop() {
	sk := c.sk
	if sk == nil {
		return
	}
	c.sk = nil
	sk.closing.Store(true)
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
	for {
		f, err := frame.Read(sk.nc, limits)
		if err != nil {
			sk.err = err
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
			sk.err = fmt.Errorf("unexpected reply: method %d id %d", f.Method, f.RequestID)
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
// A transport or protocol failure drops the socket. A WireError keeps it.
func (c *conn) call(method uint16, req, resp proto.Message, payload []byte) ([]byte, error) {
	var out []byte
	err := c.roundTrip(method, req, payload, resp, &out)
	if _, ok := err.(*WireError); !ok && err != nil {
		c.drop()
	}
	return out, err
}

func (c *conn) roundTrip(method uint16, req proto.Message, payload []byte, resp proto.Message, out *[]byte) error {
	meta, err := proto.Marshal(req)
	if err != nil {
		return &ProtocolError{Op: "encode request", Err: err}
	}
	sk := c.sk
	c.nextID++
	p := &pending{id: c.nextID, ch: make(chan frame.Frame, 1)}
	c.rmu.Lock()
	c.pend = p
	c.rmu.Unlock()
	defer func() {
		c.rmu.Lock()
		if c.pend == p {
			c.pend = nil
		}
		c.rmu.Unlock()
	}()
	if err := frame.Write(sk.nc, frame.Frame{Method: method, RequestID: p.id, Meta: meta, Payload: payload}, limits); err != nil {
		return &ProtocolError{Op: "write frame", Err: err}
	}
	var f frame.Frame
	select {
	case f = <-p.ch:
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
