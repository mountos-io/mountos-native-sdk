package client

import (
	"context"
	"errors"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// MaxOpFailures is the most failures an Operation keeps. The server sends at
// most this many OP_FAILED notices per operation. Counts.Failed can be larger.
const MaxOpFailures = 64

// ErrOperationStateUnknown is returned by Operation.Wait when notices were lost
// and the end of the operation cannot be known. The server may still run it.
var ErrOperationStateUnknown = errors.New("mountos: operation state unknown, notices were lost")

// Preserve values for CloneOptions.Preserve.
const (
	PreserveTimes = wire.PreserveKind_PRESERVE_KIND_TIMES
	PreserveXattr = wire.PreserveKind_PRESERVE_KIND_XATTR
	PreserveACL   = wire.PreserveKind_PRESERVE_KIND_ACL
)

// Outcomes of Clone.
const (
	CloneCreated     = wire.CloneOutcome_CLONE_OUTCOME_CLONED
	CloneOverwritten = wire.CloneOutcome_CLONE_OUTCOME_OVERWRITTEN
	CloneSkipped     = wire.CloneOutcome_CLONE_OUTCOME_SKIPPED
)

// CloneOptions tune Clone and CloneDir. The zero value clones with the current
// time on new entries, without extended attributes or ACLs, and fails on a
// destination that differs.
type CloneOptions struct {
	_ struct{} // Callers must name fields. New fields can be added.
	// Preserve lists the extra properties to copy from the source.
	Preserve []wire.PreserveKind
	// Overwrite replaces the content of a destination that differs.
	Overwrite bool
}

func (o *CloneOptions) wire() *wire.CloneOptions {
	if o == nil {
		return nil
	}
	return &wire.CloneOptions{Overwrite: o.Overwrite, Preserve: o.Preserve}
}

// OpCounts are the totals an operation reports when it ends.
type OpCounts struct {
	_           struct{} // Callers must name fields. New fields can be added.
	Cloned      uint64
	Overwritten uint64
	Skipped     uint64
	Failed      uint64 // can exceed the number of failures kept
	Removed     uint64
	Protected   uint64
}

// OpFailure is one entry that failed inside an operation.
type OpFailure struct {
	_       struct{} // Callers must name fields. New fields can be added.
	Path    string   // relative to the mount root. Empty when the name is not valid UTF-8.
	Code    string
	Message string
}

// Clone copies one entry that is not a directory (a file, a symlink, or a
// special file) from one path to another and returns what happened to the
// destination. The server needs the "clone" capability. Without it the call
// fails with UNSUPPORTED_OPERATION. opts may be nil.
func (cl *Client) Clone(from, to string, opts *CloneOptions) (wire.CloneOutcome, error) {
	var r wire.CloneResponse
	if err := cl.run(mClone, &wire.CloneRequest{From: from, To: to, Options: opts.wire()}, &r); err != nil {
		return 0, err
	}
	return r.GetOutcome(), nil
}

// CloneDir starts copying the tree at from to to in the background and returns
// at once. The server needs the "ops" capability. It fails with EAGAIN when the
// server runs too many operations. opts may be nil.
func (cl *Client) CloneDir(from, to string, opts *CloneOptions) (*Operation, error) {
	var r wire.CloneDirResponse
	return cl.startOp(mCloneDir, &wire.CloneDirRequest{From: from, To: to, Options: opts.wire()}, &r, r.GetOpId)
}

// RemoveDir starts removing path and everything below it in the background and
// returns at once. Unlike RemoveAll it keeps going after a failed entry and
// reports each failure. The server needs the "ops" capability. It fails with
// EAGAIN when the server runs too many operations.
func (cl *Client) RemoveDir(path string) (*Operation, error) {
	var r wire.RemoveDirResponse
	return cl.startOp(mRemoveDir, &wire.RemoveDirRequest{Path: path}, &r, r.GetOpId)
}

// startOp sends a start request on one pooled connection. The reader registers
// the operation while it hands over the reply, so no notice of the operation
// can arrive before the registration. idOf reads the op id from resp.
func (cl *Client) startOp(method uint16, req, resp proto.Message, idOf func() uint64) (*Operation, error) {
	if cl.isClosed() {
		return nil, ErrClientClosed
	}
	sk, err := cl.acquire()
	if err != nil {
		return nil, err
	}
	defer sk.release()
	st, err := sk.callOp(method, req, resp, idOf)
	if err != nil {
		return nil, err
	}
	return &Operation{cl: cl, sk: sk, id: st.key.id, st: st}, nil
}

// callOp runs a start request. The state is set only on success, and the read
// loop wrote it before the reply reached this goroutine.
func (s *sock) callOp(method uint16, req, resp proto.Message, idOf func() uint64) (*opState, error) {
	if err := s.c.cl.need(method); err != nil {
		return nil, err
	}
	var st *opState
	hook := func(f frame.Frame) {
		if f.Flags&frame.FlagResponse == 0 || f.Flags&frame.FlagError != 0 || f.Method != method {
			return
		}
		if proto.Unmarshal(f.Meta, resp) == nil {
			st = s.c.cl.hub.opRegister(s, idOf())
		}
	}
	err := s.roundTripHook(method, req, nil, resp, nil, hook)
	if err != nil && !keepsSocket(err) {
		s.fail(err)
	}
	if err == nil && st == nil {
		return nil, missingResult(opName(method), "op_id")
	}
	return st, err
}

func opName(method uint16) string {
	if method == mCloneDir {
		return "CloneDir"
	}
	return "RemoveDir"
}

// Operation is a running or ended CloneDir or RemoveDir. It lives on the
// connection that started it.
type Operation struct {
	cl *Client
	sk *sock
	st *opState
	id uint64
}

// ID returns the operation id the server gave. It matches Event.RefID.
func (o *Operation) ID() uint64 { return o.id }

// Wait waits for the end of the operation. A nil error means the server
// reported the end, and the counts and failures say how it went. Failures keeps
// at most MaxOpFailures entries. It returns ErrOperationStateUnknown when
// notices were lost, a connection error when the connection dropped, and
// ctx.Err() when ctx ends first. After the first two errors the counts and
// failures hold what arrived. Wait can be called again and from several goroutines.
func (o *Operation) Wait(ctx context.Context) (OpCounts, []OpFailure, error) {
	select {
	case <-o.st.done:
	case <-ctx.Done():
		select {
		case <-o.st.done: // ended while giving up
		default:
			return OpCounts{}, nil, ctx.Err()
		}
	}
	return o.cl.hub.opResult(o.st)
}

// Cancel asks the server to stop the operation. The operation still ends with
// OP_DONE, so call Wait for the partial counts. An operation that already ended
// fails with ENOENT.
func (o *Operation) Cancel() error {
	if o.cl.isClosed() {
		return ErrClientClosed
	}
	_, err := o.sk.callWait(mOpCancel, &wire.OpCancelRequest{OpId: o.id}, &wire.Empty{}, nil)
	return err
}

type opKey struct {
	sk *sock
	id uint64
}

// opState is the shared record of one operation. Guarded by hub.opmu, except
// done, which is closed once.
type opState struct {
	key      opKey
	done     chan struct{}
	err      error
	failures []OpFailure
	counts   OpCounts
	finished bool
}

// opNotice records an OP_DONE or OP_FAILED notice for a registered operation.
func (h *hub) opNotice(sk *sock, n *wire.Notice) {
	h.opmu.Lock()
	defer h.opmu.Unlock()
	st := h.ops[opKey{sk, n.GetRefId()}]
	if st == nil {
		return
	}
	if n.GetKind() == wire.NoticeKind_NOTICE_KIND_OP_FAILED {
		if len(st.failures) < MaxOpFailures {
			st.failures = append(st.failures, OpFailure{Path: n.GetPath(), Code: n.GetCode(), Message: n.GetMessage()})
		}
		return
	}
	st.counts = countsOf(n)
	h.opFinish(st, nil)
}

// countsOf reads the count fields of an OP_DONE notice.
func countsOf(n *wire.Notice) OpCounts {
	return OpCounts{
		Cloned: n.GetCloned(), Overwritten: n.GetOverwritten(), Skipped: n.GetSkipped(),
		Failed: n.GetFailed(), Removed: n.GetRemoved(), Protected: n.GetProtected(),
	}
}

// opFinish ends st with err and forgets it. Caller holds opmu.
func (h *hub) opFinish(st *opState, err error) {
	if st.finished {
		return
	}
	st.finished, st.err = true, err
	close(st.done)
	delete(h.ops, st.key)
}

// opRegister records the operation id on sk. The read loop calls it while it
// hands over the start reply, so every later notice of the connection finds
// the state. The loop ends the state when the connection ends.
func (h *hub) opRegister(sk *sock, id uint64) *opState {
	h.opmu.Lock()
	defer h.opmu.Unlock()
	key := opKey{sk, id}
	st := &opState{key: key, done: make(chan struct{})}
	h.ops[key] = st
	return st
}

// opResult returns the counts and failures of an ended state.
func (h *hub) opResult(st *opState) (OpCounts, []OpFailure, error) {
	h.opmu.Lock()
	defer h.opmu.Unlock()
	return st.counts, append([]OpFailure(nil), st.failures...), st.err
}

// opsOverflow handles an OVERFLOW notice from sk. Reference 0 names the whole
// connection. Another reference names one operation. Pending operations end
// with ErrOperationStateUnknown.
func (h *hub) opsOverflow(sk *sock, ref uint64) {
	h.opmu.Lock()
	defer h.opmu.Unlock()
	for key, st := range h.ops {
		if key.sk == sk && (ref == 0 || key.id == ref) {
			h.opFinish(st, ErrOperationStateUnknown)
		}
	}
}

// opsLost ends every pending operation of sk with err, because its connection
// ended.
func (h *hub) opsLost(sk *sock, err error) {
	h.opmu.Lock()
	defer h.opmu.Unlock()
	for key, st := range h.ops {
		if key.sk == sk {
			h.opFinish(st, err)
		}
	}
}
