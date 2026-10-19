package client

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// Logger receives the client's diagnostic lines. *log.Logger satisfies it.
// Options.Logger sets one. When it is nil the client writes to os.Stderr with
// the prefix "mountos: ".
type Logger interface {
	Printf(format string, args ...any)
}

func defaultLogger() Logger { return log.New(os.Stderr, "mountos: ", log.LstdFlags) }

// EventKind is the type of a server notice or a local connection event.
type EventKind int

// Event kinds.
const (
	EventWarmDone   EventKind = iota // A warm request finished. RefID is the warm id.
	EventWarmFailed                  // A warm request failed. Err is a *WireError.
	EventChange                      // A watched path changed. RefID is the watch id.
	EventOverflow                    // Events were dropped. List watched paths again.
	EventClosing                     // The server is about to close this connection.
	EventConnLost                    // A connection dropped unexpectedly. Err says why.
)

func (k EventKind) String() string {
	switch k {
	case EventWarmDone:
		return "warm_done"
	case EventWarmFailed:
		return "warm_failed"
	case EventChange:
		return "change"
	case EventOverflow:
		return "overflow"
	case EventClosing:
		return "closing"
	case EventConnLost:
		return "conn_lost"
	}
	return fmt.Sprintf("EventKind(%d)", int(k))
}

// ChangeKind is the type of change in an EventChange.
type ChangeKind int

// Change kinds.
const (
	Create ChangeKind = iota
	Remove
	Write
	Rename // Path is the new name, OldPath the old one.
	Attrib
)

func (k ChangeKind) String() string {
	switch k {
	case Create:
		return "create"
	case Remove:
		return "remove"
	case Write:
		return "write"
	case Rename:
		return "rename"
	case Attrib:
		return "attrib"
	}
	return fmt.Sprintf("ChangeKind(%d)", int(k))
}

// Event is one server notice or local connection event.
type Event struct {
	Time    time.Time
	Err     error // *WireError for EventWarmFailed, a plain error for EventConnLost.
	src     *sock // connection the event came from. nil for events made by the client.
	Path    string
	OldPath string

	Kind     EventKind
	Sequence uint64
	RefID    uint64     // Warm id or watch id. 0 when it does not apply.
	Change   ChangeKind // Set for EventChange.
}

const (
	queueLimit   = 1024
	warmKeep     = 1024
	listenerNote = "no listener registered"
)

type listener struct {
	fn func(Event)
	id uint64
}

type warmResult struct{ err error }

// hub owns the dispatcher goroutine, the listener list, the watch registry
// and the cache of finished warms.
type hub struct {
	log Logger

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	// Guarded by wmu: warmed, waiters, order, instance, instanceKnown.
	waiters map[uint64][]chan error
	warmed  map[uint64]warmResult

	// Guarded by lmu: watches, listeners, nextLID.
	watches   map[*Watch]struct{}
	listeners []listener

	// Guarded by qmu: q, dropped, stopped.
	q []Event

	order    []uint64 // warm ids in cache, oldest first
	instance uint64   // server instance of the last Hello. 0 when that server reports none.
	dropped  int
	nextLID  uint64
	qmu      sync.Mutex
	lmu      sync.Mutex
	wmu      sync.Mutex
	inCall   atomic.Bool // a listener is running, possibly the caller of shutdown
	stopped  bool
	// instanceKnown is true once a Hello has been seen, so a first connect
	// differs from a reconnect to a server that reports no id.
	instanceKnown bool
}

func newHub(l Logger) *hub {
	if l == nil {
		l = defaultLogger()
	}
	h := &hub{
		log: l, watches: map[*Watch]struct{}{},
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		warmed: map[uint64]warmResult{}, waiters: map[uint64][]chan error{},
	}
	go h.run()
	return h
}

// shutdown stops the dispatcher and waits for it. Queued events are dropped.
// While a listener runs it may be the caller (Close from inside a listener), so
// shutdown does not wait. The dispatcher then exits when the listener returns.
func (h *hub) shutdown() {
	h.qmu.Lock()
	first := !h.stopped
	h.stopped = true
	h.qmu.Unlock()
	if first {
		close(h.stop)
	}
	if !h.inCall.Load() {
		<-h.done
	}
}

// AddListener registers fn for every event. Events arrive in order from one
// goroutine per Client, so fn must return soon. A slow fn does not delay Watch
// events. When fn is slow and 1024 events queue up, the oldest are dropped and
// fn gets one EventOverflow in their place. With no listener registered, the client logs each event.
// Call the returned function to remove fn.
func (cl *Client) AddListener(fn func(Event)) (remove func()) {
	h := cl.hub
	h.lmu.Lock()
	h.nextLID++
	id := h.nextLID
	h.listeners = append(h.listeners[:len(h.listeners):len(h.listeners)], listener{id: id, fn: fn})
	h.lmu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			h.lmu.Lock()
			defer h.lmu.Unlock()
			next := make([]listener, 0, len(h.listeners))
			for _, l := range h.listeners {
				if l.id != id {
					next = append(next, l)
				}
			}
			h.listeners = next
		})
	}
}

// notice decodes a notice frame and delivers it. Warm results also reach the
// warm cache at once, so WaitWarm never depends on the listener queue.
func (h *hub) notice(sk *sock, f frame.Frame) {
	var n wire.Notice
	if err := proto.Unmarshal(f.Meta, &n); err != nil {
		h.log.Printf("bad notice frame on request %d: %v", f.RequestID, err)
		return
	}
	ev := Event{Sequence: n.GetSequence(), RefID: n.GetRefId(), Path: n.GetPath(), Time: time.Now(), src: sk}
	switch n.GetKind() {
	case wire.NoticeKind_NOTICE_KIND_WARM_DONE:
		ev.Kind = EventWarmDone
		h.warmSettled(ev.RefID, nil)
	case wire.NoticeKind_NOTICE_KIND_WARM_FAILED:
		ev.Kind = EventWarmFailed
		ev.Err = &WireError{Code: n.GetCode(), Message: n.GetMessage()}
		h.warmSettled(ev.RefID, ev.Err)
	case wire.NoticeKind_NOTICE_KIND_CHANGE:
		if n.GetChangeKind() == wire.WatchEventKind_WATCH_EVENT_KIND_OVERFLOW {
			ev.Kind = EventOverflow
			break
		}
		ev.Kind = EventChange
		ev.Change = ChangeKind(n.GetChangeKind())
		ev.OldPath = n.GetOldPath()
	case wire.NoticeKind_NOTICE_KIND_OVERFLOW:
		ev.Kind = EventOverflow
	case wire.NoticeKind_NOTICE_KIND_CLOSING:
		ev.Kind = EventClosing
	default:
		h.log.Printf("unknown notice kind %d on request %d", int32(n.GetKind()), f.RequestID)
		return
	}
	h.emit(ev)
}

// connLost reports a connection that dropped without the client asking for it.
func (h *hub) connLost(sk *sock) {
	h.emit(Event{Kind: EventConnLost, Err: fmt.Errorf("%w: %v", ErrConnLost, sk.err), Time: time.Now(), src: sk}) //nolint:errorlint // The cause stays text so callers cannot match transport errors through it.
}

// emit gives ev to the watches at once, from the caller's goroutine, so a slow
// listener never delays them. The listeners get it through the bounded queue.
// An event nobody takes is logged.
func (h *hub) emit(ev Event) {
	taken := h.offerWatches(ev)
	h.lmu.Lock()
	listening := len(h.listeners) > 0
	h.lmu.Unlock()
	if !listening {
		if !taken {
			h.log.Printf("%s: %s", listenerNote, describe(ev))
		}
		return
	}
	h.enqueue(ev)
}

// offerWatches offers ev to every watch and reports whether one took it.
func (h *hub) offerWatches(ev Event) bool {
	h.lmu.Lock()
	ws := make([]*Watch, 0, len(h.watches))
	for w := range h.watches {
		ws = append(ws, w)
	}
	h.lmu.Unlock()
	taken := false
	for _, w := range ws {
		if w.offer(ev) {
			taken = true
		}
	}
	return taken
}

// enqueue adds ev to the bounded listener queue. When the queue is full the
// oldest event goes.
func (h *hub) enqueue(ev Event) {
	h.qmu.Lock()
	if h.stopped {
		h.qmu.Unlock()
		return
	}
	if len(h.q) >= queueLimit {
		h.q = h.q[1:]
		h.dropped++
	}
	h.q = append(h.q, ev)
	h.qmu.Unlock()
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *hub) run() {
	defer close(h.done)
	for {
		select {
		case <-h.wake:
		case <-h.stop:
			return
		}
		h.qmu.Lock()
		batch, dropped := h.q, h.dropped
		h.q, h.dropped = nil, 0
		h.qmu.Unlock()
		if dropped > 0 {
			h.log.Printf("event queue full, dropped %d oldest events", dropped)
			h.deliver(Event{Kind: EventOverflow, Time: time.Now()})
		}
		for _, ev := range batch {
			select {
			case <-h.stop:
				return
			default:
			}
			h.deliver(ev)
		}
	}
}

// deliver gives ev to every listener.
func (h *hub) deliver(ev Event) {
	h.lmu.Lock()
	ls := h.listeners
	h.lmu.Unlock()
	for _, l := range ls {
		h.call(l.fn, ev)
	}
}

func (h *hub) call(fn func(Event), ev Event) {
	h.inCall.Store(true)
	defer func() {
		h.inCall.Store(false)
		if r := recover(); r != nil {
			h.log.Printf("event listener panic on %s: %v", describe(ev), r)
		}
	}()
	fn(ev)
}

func describe(ev Event) string {
	return fmt.Sprintf("event %s ref=%d path=%q err=%v", ev.Kind, ev.RefID, ev.Path, ev.Err)
}

func (h *hub) addWatch(w *Watch) {
	h.lmu.Lock()
	h.watches[w] = struct{}{}
	h.lmu.Unlock()
}

func (h *hub) removeWatch(w *Watch) {
	h.lmu.Lock()
	delete(h.watches, w)
	h.lmu.Unlock()
}

// closeWatches ends every open watch without a server call.
func (h *hub) closeWatches() {
	h.lmu.Lock()
	ws := make([]*Watch, 0, len(h.watches))
	for w := range h.watches {
		ws = append(ws, w)
	}
	h.lmu.Unlock()
	for _, w := range ws {
		w.finish()
	}
}

// warmSettled records a finished warm and wakes its waiters.
func (h *hub) warmSettled(id uint64, err error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	if _, ok := h.warmed[id]; !ok {
		h.order = append(h.order, id)
		if len(h.order) > warmKeep {
			delete(h.warmed, h.order[0])
			h.order = h.order[1:]
		}
	}
	h.warmed[id] = warmResult{err}
	for _, ch := range h.waiters[id] {
		ch <- err
	}
	delete(h.waiters, id)
}

// instanceSeen records the server instance a Hello reported. Warm ids restart
// per server instance, so a different instance drops the finished warm results
// and fails every waiter with ErrServerRestarted. This includes a change from or
// to 0 (a server that reports no id). A reconnect between two servers that both
// report 0 cannot be told apart and keeps the results.
func (h *hub) instanceSeen(id uint64) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	known := h.instanceKnown
	h.instanceKnown = true
	if known && h.instance == id {
		return
	}
	h.instance = id
	if !known {
		return
	}
	h.warmed, h.order = map[uint64]warmResult{}, nil
	for wid, chs := range h.waiters {
		for _, ch := range chs {
			ch <- ErrServerRestarted
		}
		delete(h.waiters, wid)
	}
}

// WaitWarm waits for the warm request id returned by WarmOpen or WarmRead.
// It returns nil when the server reports WARM_DONE and the *WireError when it
// reports WARM_FAILED. A result that arrived before the call is kept for the
// last 1024 warms. It does not call the server. It returns ctx.Err() when ctx
// ends first and ErrClientClosed when the client closes. When a reconnect
// shows a new server instance, finished results are dropped and waiters get
// ErrServerRestarted. If the connection
// that holds the request drops, no result will come, so pass a ctx with a
// deadline or watch for EventConnLost.
func (cl *Client) WaitWarm(ctx context.Context, id uint64) error {
	h := cl.hub
	h.wmu.Lock()
	if r, ok := h.warmed[id]; ok {
		h.wmu.Unlock()
		return r.err
	}
	ch := make(chan error, 1)
	h.waiters[id] = append(h.waiters[id], ch)
	h.wmu.Unlock()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
	case <-cl.closedCh:
	}
	h.wmu.Lock()
	ws := h.waiters[id]
	for i, w := range ws {
		if w == ch {
			h.waiters[id] = append(ws[:i:i], ws[i+1:]...)
			break
		}
	}
	if len(h.waiters[id]) == 0 {
		delete(h.waiters, id)
	}
	h.wmu.Unlock()
	select {
	case err := <-ch: // settled while giving up
		return err
	default:
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrClientClosed
}
