package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// fakeOps holds the Clone, CloneDir, RemoveDir and OpCancel state. Callers
// hold fakeServer.mu.
type fakeOps struct {
	conn     map[uint64]int // op id -> connection index
	ended    map[uint64]bool
	cloneErr string
	cancels  []uint64
	clones   []*wire.CloneRequest
	dirs     []*wire.CloneDirRequest
	removes  []string
	eager    []*wire.Notice
	next     uint64
	seen     int // requests of the four methods received
	outcome  wire.CloneOutcome
	full     bool // CloneDir and RemoveDir answer EAGAIN
}

// handleOps serves the four operation methods. Caller holds s.mu.
func (s *fakeServer) handleOps(f frame.Frame, idx int) (proto.Message, *wire.WireError) {
	o := &s.ops
	if o.conn == nil {
		o.conn, o.ended = map[uint64]int{}, map[uint64]bool{}
	}
	o.seen++
	start := func() (uint64, *wire.WireError) {
		if o.full {
			return 0, errno("EAGAIN")
		}
		o.next++
		o.conn[o.next] = idx
		return o.next, nil
	}
	switch f.Method {
	case mClone:
		var r wire.CloneRequest
		proto.Unmarshal(f.Meta, &r)
		o.clones = append(o.clones, &r)
		if o.cloneErr != "" {
			return nil, errno(o.cloneErr)
		}
		return &wire.CloneResponse{Outcome: o.outcome}, nil
	case mCloneDir:
		var r wire.CloneDirRequest
		proto.Unmarshal(f.Meta, &r)
		o.dirs = append(o.dirs, &r)
		id, e := start()
		if e != nil {
			return nil, e
		}
		return &wire.CloneDirResponse{OpId: id}, nil
	case mRemoveDir:
		var r wire.RemoveDirRequest
		proto.Unmarshal(f.Meta, &r)
		o.removes = append(o.removes, r.Path)
		id, e := start()
		if e != nil {
			return nil, e
		}
		return &wire.RemoveDirResponse{OpId: id}, nil
	}
	var r wire.OpCancelRequest
	proto.Unmarshal(f.Meta, &r)
	if c, ok := o.conn[r.OpId]; !ok || c != idx || o.ended[r.OpId] {
		return nil, errno("ENOENT")
	}
	o.cancels = append(o.cancels, r.OpId)
	return &wire.Empty{}, nil
}

// afterOpStart pushes the queued eager notices right after a start reply,
// stamped with the new op id.
func (s *fakeServer) afterOpStart(idx int, resp proto.Message) {
	var id uint64
	switch r := resp.(type) {
	case *wire.CloneDirResponse:
		id = r.OpId
	case *wire.RemoveDirResponse:
		id = r.OpId
	default:
		return
	}
	s.mu.Lock()
	eager := s.ops.eager
	s.mu.Unlock()
	for _, n := range eager {
		n = proto.Clone(n).(*wire.Notice)
		if n.Kind != wire.NoticeKind_NOTICE_KIND_OVERFLOW || n.RefId != 0 {
			n.RefId = id
		}
		s.opNotice(id, n)
	}
}

func (s *fakeServer) opConnOf(id uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.ops.conn[id]
	if !ok {
		s.t.Fatalf("unknown op id %d", id)
	}
	return c
}

// opNotice sends n on the connection that owns op id. OP_DONE ends the op.
func (s *fakeServer) opNotice(id uint64, n *wire.Notice) {
	if n.Kind == wire.NoticeKind_NOTICE_KIND_OP_DONE {
		s.mu.Lock()
		s.ops.ended[id] = true
		s.mu.Unlock()
	}
	s.notify(s.opConnOf(id), n)
}

func (s *fakeServer) opDone(id uint64, n *wire.Notice) {
	n.Kind, n.RefId = wire.NoticeKind_NOTICE_KIND_OP_DONE, id
	s.opNotice(id, n)
}

func (s *fakeServer) opFailed(id uint64, path, code, msg string) {
	s.opNotice(id, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OP_FAILED, RefId: id, Path: path, Code: code, Message: msg})
}

func (s *fakeServer) opsSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ops.seen
}

func waitCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCloneOutcomes(t *testing.T) {
	cases := []struct {
		opts    *CloneOptions
		wantOpt *wire.CloneOptions
		name    string
		want    wire.CloneOutcome
	}{
		{nil, nil, "created", CloneCreated},
		{&CloneOptions{Overwrite: true, Preserve: []wire.PreserveKind{PreserveTimes, PreserveACL}},
			&wire.CloneOptions{Overwrite: true, Preserve: []wire.PreserveKind{PreserveTimes, PreserveACL}}, "overwritten", CloneOverwritten},
		{&CloneOptions{}, &wire.CloneOptions{}, "skipped", CloneSkipped},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newFakeServer(t, 0)
			cl := dial(t, s, 1)
			s.mu.Lock()
			s.ops.outcome = tc.want
			s.mu.Unlock()
			got, err := cl.Clone("a/src", "b/dst", tc.opts)
			if err != nil || got != tc.want {
				t.Fatalf("Clone = %v, %v, want %v", got, err, tc.want)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			req := s.ops.clones[0]
			if req.From != "a/src" || req.To != "b/dst" || !proto.Equal(req.Options, tc.wantOpt) {
				t.Fatalf("request = %v, want options %v", req, tc.wantOpt)
			}
		})
	}
}

func TestCloneErrorsAreWireErrors(t *testing.T) {
	for _, code := range []string{"EISDIR", "EEXIST", "EPERM", "EXDEV"} {
		s := newFakeServer(t, 0)
		cl := dial(t, s, 1)
		s.mu.Lock()
		s.ops.cloneErr = code
		s.mu.Unlock()
		_, err := cl.Clone("a", "b", nil)
		var we *WireError
		if !errors.As(err, &we) || we.Code != code {
			t.Fatalf("Clone error = %v, want code %s", err, code)
		}
	}
}

func TestCloneDirCounts(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, err := cl.CloneDir("src", "dst", &CloneOptions{Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	if len(s.ops.dirs) != 1 || s.ops.dirs[0].From != "src" || !s.ops.dirs[0].Options.Overwrite {
		t.Fatalf("request = %v", s.ops.dirs)
	}
	s.mu.Unlock()
	s.opFailed(op.ID(), "dir/x", "EACCES", "denied")
	s.opDone(op.ID(), &wire.Notice{Cloned: 7, Overwritten: 2, Skipped: 3, Failed: 1, Protected: 4})
	counts, failures, err := op.Wait(waitCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	want := OpCounts{Cloned: 7, Overwritten: 2, Skipped: 3, Failed: 1, Protected: 4}
	if counts != want {
		t.Fatalf("counts = %+v, want %+v", counts, want)
	}
	if len(failures) != 1 || failures[0].Path != "dir/x" || failures[0].Code != "EACCES" || failures[0].Message != "denied" {
		t.Fatalf("failures = %+v", failures)
	}
	again, _, err := op.Wait(waitCtx(t))
	if err != nil || again != want {
		t.Fatalf("second Wait = %+v, %v", again, err)
	}
}

func TestRemoveDirCounts(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, err := cl.RemoveDir("tree")
	if err != nil {
		t.Fatal(err)
	}
	s.opDone(op.ID(), &wire.Notice{Removed: 12, Protected: 1})
	counts, failures, err := op.Wait(waitCtx(t))
	if err != nil || counts.Removed != 12 || counts.Protected != 1 || len(failures) != 0 {
		t.Fatalf("Wait = %+v %v %v", counts, failures, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Equal(s.ops.removes, []string{"tree"}) {
		t.Fatalf("removes = %v", s.ops.removes)
	}
}

// Notices that follow the start reply at once reach the Operation even when
// the reader handles them before the start call returns.
func TestOpNoticesBeforeRegistration(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	s.mu.Lock()
	s.ops.eager = []*wire.Notice{
		{Kind: wire.NoticeKind_NOTICE_KIND_OP_FAILED, Path: "p", Code: "EIO", Message: "m"},
		{Kind: wire.NoticeKind_NOTICE_KIND_OP_DONE, Failed: 1, Cloned: 5},
	}
	s.mu.Unlock()
	for range 50 {
		op, err := cl.CloneDir("a", "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		counts, failures, err := op.Wait(waitCtx(t))
		if err != nil || counts.Cloned != 5 || counts.Failed != 1 || len(failures) != 1 || failures[0].Code != "EIO" {
			t.Fatalf("Wait = %+v %+v %v", counts, failures, err)
		}
	}
	cl.hub.opmu.Lock()
	defer cl.hub.opmu.Unlock()
	if len(cl.hub.ops) != 0 || cl.hub.opOrphans != 0 {
		t.Fatalf("hub keeps %d states, %d unclaimed", len(cl.hub.ops), cl.hub.opOrphans)
	}
}

func TestManyFailuresAreCapped(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, err := cl.RemoveDir("tree")
	if err != nil {
		t.Fatal(err)
	}
	const total = MaxOpFailures + 36
	for i := range total {
		s.opFailed(op.ID(), fmt.Sprintf("f%d", i), "EBUSY", "busy")
	}
	s.opDone(op.ID(), &wire.Notice{Failed: total, Removed: 3})
	counts, failures, err := op.Wait(waitCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != MaxOpFailures || failures[0].Path != "f0" || failures[MaxOpFailures-1].Path != fmt.Sprintf("f%d", MaxOpFailures-1) {
		t.Fatalf("kept %d failures, first %q", len(failures), failures[0].Path)
	}
	if counts.Failed != total {
		t.Fatalf("Failed = %d, want the server total %d", counts.Failed, total)
	}
}

func TestOverflowMakesStateUnknown(t *testing.T) {
	t.Run("connection level", func(t *testing.T) {
		s := newFakeServer(t, 0)
		cl := dial(t, s, 1)
		op, err := cl.RemoveDir("tree")
		if err != nil {
			t.Fatal(err)
		}
		s.notify(s.opConnOf(op.ID()), &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW})
		_, _, err = op.Wait(waitCtx(t))
		if !errors.Is(err, ErrOperationStateUnknown) {
			t.Fatalf("Wait error = %v, want state unknown", err)
		}
	})
	t.Run("named operation", func(t *testing.T) {
		s := newFakeServer(t, 0)
		cl := dial(t, s, 1)
		a, _ := cl.RemoveDir("a")
		b, _ := cl.RemoveDir("b")
		s.notify(s.opConnOf(a.ID()), &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW, RefId: a.ID()})
		if _, _, err := a.Wait(waitCtx(t)); !errors.Is(err, ErrOperationStateUnknown) {
			t.Fatalf("a: %v", err)
		}
		s.opDone(b.ID(), &wire.Notice{Removed: 1})
		if counts, _, err := b.Wait(waitCtx(t)); err != nil || counts.Removed != 1 {
			t.Fatalf("b: %+v %v", counts, err)
		}
	})
	t.Run("sent right after the start reply", func(t *testing.T) {
		s := newFakeServer(t, 0)
		cl := dial(t, s, 1)
		s.mu.Lock()
		s.ops.eager = []*wire.Notice{{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW}}
		s.mu.Unlock()
		for range 30 {
			op, err := cl.RemoveDir("tree")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := op.Wait(waitCtx(t)); !errors.Is(err, ErrOperationStateUnknown) {
				t.Fatalf("Wait error = %v, want state unknown", err)
			}
		}
	})
	t.Run("other connection is not affected", func(t *testing.T) {
		s := newFakeServer(t, 0)
		cl := dial(t, s, 2)
		var ops []*Operation
		for len(ops) < 8 {
			op, err := cl.RemoveDir("tree")
			if err != nil {
				t.Fatal(err)
			}
			ops = append(ops, op)
			if s.opConnOf(op.ID()) != s.opConnOf(ops[0].ID()) {
				break
			}
		}
		first, last := ops[0], ops[len(ops)-1]
		if s.opConnOf(first.ID()) == s.opConnOf(last.ID()) {
			t.Skip("the pool kept every operation on one connection")
		}
		s.notify(s.opConnOf(first.ID()), &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW})
		if _, _, err := first.Wait(waitCtx(t)); !errors.Is(err, ErrOperationStateUnknown) {
			t.Fatalf("first: %v", err)
		}
		s.opDone(last.ID(), &wire.Notice{Removed: 9})
		if counts, _, err := last.Wait(waitCtx(t)); err != nil || counts.Removed != 9 {
			t.Fatalf("last: %+v %v", counts, err)
		}
	})
}

// An operation that ended before the overflow keeps its result.
func TestOverflowKeepsEndedOperation(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, _ := cl.RemoveDir("tree")
	s.opDone(op.ID(), &wire.Notice{Removed: 4})
	if _, _, err := op.Wait(waitCtx(t)); err != nil {
		t.Fatal(err)
	}
	s.notify(s.opConnOf(op.ID()), &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OVERFLOW})
	counts, _, err := op.Wait(waitCtx(t))
	if err != nil || counts.Removed != 4 {
		t.Fatalf("Wait after overflow = %+v %v", counts, err)
	}
}

func TestCancel(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, err := cl.CloneDir("src", "dst", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = op.Cancel(); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	if !slices.Equal(s.ops.cancels, []uint64{op.ID()}) {
		t.Fatalf("cancels = %v", s.ops.cancels)
	}
	s.mu.Unlock()
	s.opDone(op.ID(), &wire.Notice{Cloned: 2, Skipped: 1})
	counts, _, err := op.Wait(waitCtx(t))
	if err != nil || counts.Cloned != 2 {
		t.Fatalf("Wait after cancel = %+v %v", counts, err)
	}
	var we *WireError
	if err = op.Cancel(); !errors.As(err, &we) || we.Code != "ENOENT" {
		t.Fatalf("Cancel of an ended operation = %v, want ENOENT", err)
	}
}

// Cancel goes to the connection that started the operation, whatever the pool
// would pick for a new call.
func TestCancelUsesOwningConnection(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 3)
	var ops []*Operation
	for range 6 {
		op, err := cl.RemoveDir("tree")
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, op)
	}
	for _, op := range ops {
		if err := op.Cancel(); err != nil {
			t.Fatalf("Cancel of op %d: %v", op.ID(), err)
		}
	}
}

func TestWaitContext(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, _ := cl.RemoveDir("tree")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := op.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait error = %v, want deadline exceeded", err)
	}
	s.opDone(op.ID(), &wire.Notice{Removed: 1})
	if counts, _, err := op.Wait(waitCtx(t)); err != nil || counts.Removed != 1 {
		t.Fatalf("Wait after timeout = %+v %v", counts, err)
	}
}

func TestConnectionCloseFailsWaiters(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, _ := cl.RemoveDir("tree")
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := op.Wait(waitCtx(t))
			errs <- err
		}()
	}
	time.Sleep(30 * time.Millisecond)
	s.dropAll()
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrConnLost) {
			t.Fatalf("Wait error = %v, want connection lost", err)
		}
	}
	if _, _, err := op.Wait(waitCtx(t)); !errors.Is(err, ErrConnLost) {
		t.Fatalf("Wait on a lost operation = %v", err)
	}
}

func TestClientCloseFailsWaiters(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	op, _ := cl.RemoveDir("tree")
	done := make(chan error, 1)
	go func() {
		_, _, err := op.Wait(waitCtx(t))
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cl.Close()
	if err := <-done; !errors.Is(err, ErrClientClosed) {
		t.Fatalf("Wait error = %v, want client closed", err)
	}
}

func TestOperationLimitIsEAGAIN(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	s.mu.Lock()
	s.ops.full = true
	s.mu.Unlock()
	for name, call := range map[string]func() error{
		"CloneDir":  func() error { _, err := cl.CloneDir("a", "b", nil); return err },
		"RemoveDir": func() error { _, err := cl.RemoveDir("a"); return err },
	} {
		var we *WireError
		if err := call(); !errors.As(err, &we) || we.Code != "EAGAIN" {
			t.Fatalf("%s error = %v, want EAGAIN", name, err)
		}
	}
	cl.hub.opmu.Lock()
	defer cl.hub.opmu.Unlock()
	if len(cl.hub.ops) != 0 {
		t.Fatalf("a refused start left %d states", len(cl.hub.ops))
	}
}

func TestCapabilitiesAbsent(t *testing.T) {
	s := newFakeServer(t, 0)
	s.caps = []string{"bulk_io", "warm"}
	cl := dial(t, s, 1)
	check := func(name string, err error) {
		t.Helper()
		var we *WireError
		if !errors.As(err, &we) || we.Code != CodeUnsupportedOperation {
			t.Fatalf("%s error = %v, want UNSUPPORTED_OPERATION", name, err)
		}
	}
	_, err := cl.Clone("a", "b", nil)
	check("Clone", err)
	_, err = cl.CloneDir("a", "b", nil)
	check("CloneDir", err)
	_, err = cl.RemoveDir("a")
	check("RemoveDir", err)
	if n := s.opsSeen(); n != 0 {
		t.Fatalf("server saw %d operation requests, want none", n)
	}
}

func TestCapabilitiesPresentAndAsked(t *testing.T) {
	s := newFakeServer(t, 0)
	s.caps = []string{"clone", "ops"}
	cl := dial(t, s, 1)
	if _, err := cl.Clone("a", "b", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.CloneDir("a", "b", nil); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range []string{"clone", "ops"} {
		if !slices.Contains(s.helloCaps, c) {
			t.Fatalf("Hello asked for %v, lacks %s", s.helloCaps, c)
		}
	}
}

func TestRemoveAllStillWorks(t *testing.T) {
	s := newFakeServer(t, 0)
	s.caps = []string{"remove_all"}
	s.files["/t/a"] = []byte("x")
	s.files["/t/b"] = []byte("y")
	cl := dial(t, s, 1)
	n, err := cl.RemoveAll("/t")
	if err != nil || n != 2 {
		t.Fatalf("RemoveAll = %d, %v", n, err)
	}
}

func TestOpEventsReachListeners(t *testing.T) {
	s := newFakeServer(t, 0)
	cl, log := dialLog(t, s, 1)
	ch, _ := collect(cl)
	op, _ := cl.RemoveDir("tree")
	s.opFailed(op.ID(), "x", "EIO", "bad")
	s.opDone(op.ID(), &wire.Notice{Removed: 2, Failed: 1})
	f := recvEv(t, ch)
	var we *WireError
	if f.Kind != EventOpFailed || f.RefID != op.ID() || f.Path != "x" || !errors.As(f.Err, &we) || we.Code != "EIO" {
		t.Fatalf("failed event = %+v", f)
	}
	d := recvEv(t, ch)
	if d.Kind != EventOpDone || d.Counts.Removed != 2 || d.Counts.Failed != 1 {
		t.Fatalf("done event = %+v", d)
	}
	if got := log.String(); got != "" {
		t.Fatalf("logged %q", got)
	}
}

// With no listener an operation notice is not logged: the Operation keeps it.
func TestOpNoticesNotLoggedWithoutListener(t *testing.T) {
	s := newFakeServer(t, 0)
	cl, log := dialLog(t, s, 1)
	op, _ := cl.RemoveDir("tree")
	for range 5 {
		s.opFailed(op.ID(), "x", "EIO", "bad")
	}
	s.opDone(op.ID(), &wire.Notice{Failed: 5})
	if _, _, err := op.Wait(waitCtx(t)); err != nil {
		t.Fatal(err)
	}
	if got := log.String(); got != "" {
		t.Fatalf("logged %q", got)
	}
}

func TestOrphanStatesAreBounded(t *testing.T) {
	h := newHub(&bufLogger{})
	defer h.shutdown()
	sk := &sock{dead: make(chan struct{})}
	for id := uint64(1); id <= opOrphanLimit+50; id++ {
		h.opNotice(sk, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OP_FAILED, RefId: id})
	}
	if h.opOrphans != opOrphanLimit || len(h.ops) != opOrphanLimit {
		t.Fatalf("held %d states, %d unclaimed, want %d", len(h.ops), h.opOrphans, opOrphanLimit)
	}
	h.opsLost(sk, ErrConnLost)
	if h.opOrphans != 0 || len(h.ops) != 0 {
		t.Fatalf("after loss: %d states, %d unclaimed", len(h.ops), h.opOrphans)
	}
}

// A duplicate or late OP_DONE does not change a result that is final.
func TestFinalResultIsStable(t *testing.T) {
	h := newHub(&bufLogger{})
	defer h.shutdown()
	sk := &sock{dead: make(chan struct{})}
	st := h.opRegister(sk, 1, 0)
	h.opNotice(sk, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OP_DONE, RefId: 1, Cloned: 1})
	h.opNotice(sk, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OP_DONE, RefId: 1, Cloned: 99})
	h.opNotice(sk, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_OP_FAILED, RefId: 1, Path: "late"})
	counts, failures, err := h.opResult(st)
	if err != nil || counts.Cloned != 1 || len(failures) != 0 {
		t.Fatalf("result = %+v %v %v", counts, failures, err)
	}
}
