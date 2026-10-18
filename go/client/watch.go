package client

import (
	"errors"
	"sync"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

const watchBuffer = 256

// Watch is one server-side watch. It lives on the connection that made the
// Watch call, and ends when that connection closes. Events is a filtered
// view of the client events for this watch.
type Watch struct {
	cl  *Client
	c   *conn
	sk  *sock
	ch  chan Event
	gen uint64

	mu      sync.Mutex
	id      uint64
	known   bool    // id is set. Before that, events of the connection wait in early.
	early   []Event // events that arrived before the Watch call returned
	dropped bool    // an event was dropped and no OVERFLOW went out yet
	closed  bool
}

// Watch starts watching path. With recursive the whole subtree is watched.
// A non-zero since asks for a replay point. Events are not stored, so an older
// sequence yields one EventOverflow. The watch uses one pooled connection.
// The caller must call Close.
func (cl *Client) Watch(path string, recursive bool, since uint64) (*Watch, error) {
	if cl.isClosed() {
		return nil, ErrClientClosed
	}
	c := cl.acquire()
	defer c.mu.Unlock()
	if err := c.ensure(); err != nil {
		return nil, err
	}
	w := &Watch{cl: cl, c: c, sk: c.sk, gen: c.gen, ch: make(chan Event, watchBuffer)}
	cl.hub.addWatch(w)
	var r wire.WatchResponse
	if _, err := c.call(mWatch, &wire.WatchRequest{Path: path, Recursive: recursive, SinceSequence: since}, &r, nil); err != nil {
		cl.hub.removeWatch(w)
		return nil, err
	}
	w.setID(r.GetWatchId())
	if cl.isClosed() {
		w.finish()
		return nil, ErrClientClosed
	}
	return w, nil
}

// ID returns the server watch id.
func (w *Watch) ID() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.id
}

// Events returns the change events of this watch. The channel holds 256
// events. When the reader is slower, events are dropped and an EventOverflow
// follows as soon as the channel has room. The channel closes on Close, when
// the connection is lost, and when the Client closes.
func (w *Watch) Events() <-chan Event { return w.ch }

// Close sends Unwatch on the connection that made the Watch call, then closes
// the Events channel. A connection that is already gone is not an error. Close
// is safe to call more than once.
func (w *Watch) Close() error {
	var err error
	if !w.cl.isClosed() {
		w.c.mu.Lock()
		if w.c.sk == w.sk && w.c.alive() {
			_, err = w.c.call(mUnwatch, &wire.UnwatchRequest{WatchId: w.ID()}, &wire.Empty{}, nil)
		}
		w.c.mu.Unlock()
	}
	w.finish()
	var we *WireError
	if errors.Is(err, ErrConnLost) || (errors.As(err, &we) && we.Code == "EBADF") {
		return nil
	}
	return err
}

// finish closes the channel and unregisters the watch.
func (w *Watch) finish() {
	w.cl.hub.removeWatch(w)
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()
}

// setID learns the watch id and replays the events held back until now.
func (w *Watch) setID(id uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.id, w.known = id, true
	early := w.early
	w.early = nil
	for _, ev := range early {
		if w.matches(ev) {
			w.push(ev)
		}
	}
}

// matches reports whether ev is for this watch. Caller holds mu and id is known.
// An overflow without a watch id applies to every watch of its connection, and
// a local overflow applies to every watch.
func (w *Watch) matches(ev Event) bool {
	switch ev.Kind {
	case EventChange:
		return ev.src == w.sk && ev.RefID == w.id
	case EventOverflow:
		return ev.src == nil || (ev.src == w.sk && (ev.RefID == w.id || ev.RefID == 0))
	}
	return false
}

// offer is called by the dispatcher for every event. It reports whether the
// watch took the event.
func (w *Watch) offer(ev Event) bool {
	if ev.Kind == EventConnLost && ev.src == w.sk {
		w.finish()
		return false
	}
	if ev.Kind != EventChange && ev.Kind != EventOverflow {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	if !w.known {
		if ev.src != w.sk && ev.src != nil {
			return false
		}
		if len(w.early) >= watchBuffer {
			w.early = w.early[1:]
		}
		w.early = append(w.early, ev)
		return true
	}
	if !w.matches(ev) {
		return false
	}
	w.push(ev)
	return true
}

// push sends ev without blocking. Caller holds mu.
func (w *Watch) push(ev Event) {
	if w.dropped {
		select {
		case w.ch <- Event{Kind: EventOverflow, RefID: w.id, Time: time.Now()}:
			w.dropped = false
		default:
		}
	}
	select {
	case w.ch <- ev:
	default:
		w.dropped = true
	}
}
