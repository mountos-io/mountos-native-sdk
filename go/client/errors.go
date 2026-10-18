package client

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"
)

// WireError is a failure reported by the server. Code is a POSIX name such as ENOENT.
type WireError struct {
	Code    string
	Message string
}

func (e *WireError) Error() string {
	if e.Message == "" {
		return "mountos: " + e.Code
	}
	return fmt.Sprintf("mountos: %s: %s", e.Code, e.Message)
}

// Is matches the syscall.Errno for Code, so errors.Is(err, fs.ErrNotExist) works for ENOENT.
func (e *WireError) Is(target error) bool {
	if errno, ok := errnoByName[e.Code]; ok {
		return errors.Is(errno, target)
	}
	return false
}

var errnoByName = map[string]syscall.Errno{
	"EPERM": syscall.EPERM, "ENOENT": syscall.ENOENT, "EIO": syscall.EIO,
	"EBADF": syscall.EBADF, "EACCES": syscall.EACCES, "EBUSY": syscall.EBUSY,
	"EEXIST": syscall.EEXIST, "EXDEV": syscall.EXDEV, "ENOTDIR": syscall.ENOTDIR,
	"EISDIR": syscall.EISDIR, "EINVAL": syscall.EINVAL, "EMFILE": syscall.EMFILE,
	"EFBIG": syscall.EFBIG, "ENOSPC": syscall.ENOSPC, "EROFS": syscall.EROFS,
	"ENAMETOOLONG": syscall.ENAMETOOLONG, "ENOTEMPTY": syscall.ENOTEMPTY,
	"ELOOP": syscall.ELOOP, "EAGAIN": syscall.EAGAIN, "EMLINK": syscall.EMLINK,
	"EDQUOT": syscall.EDQUOT, "ESTALE": syscall.ESTALE,
}

// DialError reports a failure to connect or to complete the Hello handshake.
type DialError struct {
	Path string
	Err  error
}

func (e *DialError) Error() string { return fmt.Sprintf("mountos: dial %s: %v", e.Path, e.Err) }
func (e *DialError) Unwrap() error { return e.Err }

// ProtocolError reports a malformed or unexpected frame, or a broken connection.
type ProtocolError struct {
	Op  string
	Err error

	unsent bool // raised before any byte was written, so the socket is still in sync
}

func (e *ProtocolError) Error() string { return fmt.Sprintf("mountos: %s: %v", e.Op, e.Err) }
func (e *ProtocolError) Unwrap() error { return e.Err }

var (
	// ErrClientClosed is returned after Client.Close.
	ErrClientClosed = errors.New("mountos: client closed")
	// ErrFileClosed is returned for calls on a closed File.
	ErrFileClosed = errors.New("mountos: file closed")
	// ErrConnLost is returned for a File whose connection broke after it was opened.
	ErrConnLost = errors.New("mountos: connection lost, handle invalid")
)

// Error codes the client raises itself. They match the spec names of the same codes.
const (
	CodeDeadlineExceeded     = "DEADLINE_EXCEEDED"
	CodeUnsupportedOperation = "UNSUPPORTED_OPERATION"
)

// TimeoutError reports a call that did not finish within Options.CallTimeout.
// The client drops the connection, so a late reply cannot match another call.
// A File opened on that connection returns ErrConnLost afterward.
type TimeoutError struct {
	After time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("mountos: %s: no reply within %v", CodeDeadlineExceeded, e.After)
}

// Code returns CodeDeadlineExceeded.
func (e *TimeoutError) Code() string { return CodeDeadlineExceeded }

// Timeout reports true, so the error satisfies net.Error.
func (e *TimeoutError) Timeout() bool { return true }

// Temporary reports true. A call may be tried again.
func (e *TimeoutError) Temporary() bool { return true }

// Is matches context.DeadlineExceeded.
func (e *TimeoutError) Is(target error) bool { return target == context.DeadlineExceeded }

// missingResult reports a success reply that lacks the message result op needs.
// The reply decoded cleanly, so the socket stays in sync.
func missingResult(op, field string) error {
	return &ProtocolError{Op: op, Err: fmt.Errorf("reply carries no %s", field)}
}
