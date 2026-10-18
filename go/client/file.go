package client

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// File is an open server handle. It is bound to one connection. Calls on a File
// are serialized. Calls on different Files run in parallel, on one connection
// when the server allows several requests in flight.
type File struct {
	cl     *Client
	sk     *sock
	max    int // payload bytes per frame
	handle uint64
	path   string        // as opened, for parallel batches
	flags  wire.OpenFlag // as opened

	mu     sync.Mutex
	pos    int64
	closed bool
}

// do runs one request on the File's connection. Caller holds f.mu.
func (f *File) do(method uint16, req, resp proto.Message, payload []byte) ([]byte, error) {
	if f.closed {
		return nil, ErrFileClosed
	}
	if f.cl.isClosed() {
		return nil, ErrClientClosed
	}
	return f.sk.callWait(method, req, resp, payload)
}

func (f *File) chunk() int { return f.max }

// Close releases the handle. A second Close returns ErrFileClosed.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.do(mClose, &wire.CloseRequest{Handle: f.handle}, &wire.Empty{}, nil)
	if !errors.Is(err, ErrFileClosed) {
		f.closed = true
	}
	return err
}

// ReadAt reads len(p) bytes at off. It returns io.EOF when fewer bytes are available.
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readAt(p, off)
}

func (f *File) readAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("mountos: negative offset")
	}
	total := 0
	for total < len(p) {
		want := min(len(p)-total, f.chunk())
		var r wire.ReadAtResponse
		data, err := f.do(mReadAt, &wire.ReadAtRequest{Handle: f.handle, Offset: off + int64(total), Length: uint32(want)}, &r, nil)
		if err != nil {
			return total, err
		}
		if int(r.GetCount()) != len(data) || len(data) > want {
			return total, &ProtocolError{Op: "read", Err: fmt.Errorf("count %d, payload %d, asked %d", r.GetCount(), len(data), want)}
		}
		total += copy(p[total:], data)
		if len(data) < want {
			return total, io.EOF
		}
	}
	return total, nil
}

// Read reads at the current position.
func (f *File) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	n, err := f.readAt(p, f.pos)
	f.pos += int64(n)
	if n > 0 && errors.Is(err, io.EOF) {
		err = nil
	}
	return n, err
}

// WriteAt writes all of p at off, split across frames as needed.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeAt(p, off)
}

func (f *File) writeAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("mountos: negative offset")
	}
	total := 0
	for total < len(p) {
		part := p[total:min(len(p), total+f.chunk())]
		var r wire.WriteAtResponse
		if _, err := f.do(mWriteAt, &wire.WriteAtRequest{Handle: f.handle, Offset: off + int64(total)}, &r, part); err != nil {
			return total, err
		}
		n := int(r.GetCount())
		if n > len(part) {
			return total, &ProtocolError{Op: "write", Err: fmt.Errorf("count %d exceeds sent %d", n, len(part))}
		}
		total += n
		if n < len(part) {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// Write writes at the current position.
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.writeAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

// Seek sets the position for Read and Write. io.SeekEnd asks the server for the size.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = f.pos
	case io.SeekEnd:
		info, err := f.fstat()
		if err != nil {
			return 0, err
		}
		base = info.GetSize()
	default:
		return 0, errors.New("mountos: invalid whence")
	}
	if base+offset < 0 {
		return 0, errors.New("mountos: negative position")
	}
	f.pos = base + offset
	return f.pos, nil
}

// Sync flushes data and metadata to stable storage.
func (f *File) Sync() error { return f.fsync(false) }

// SyncData flushes file data only.
func (f *File) SyncData() error { return f.fsync(true) }

func (f *File) fsync(dataOnly bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.do(mFsync, &wire.FsyncRequest{Handle: f.handle, DataOnly: dataOnly}, &wire.Empty{}, nil)
	return err
}

func (f *File) fstat() (*wire.FileInfo, error) {
	var r wire.FstatResponse
	if _, err := f.do(mFstat, &wire.FstatRequest{Handle: f.handle}, &r, nil); err != nil {
		return nil, err
	}
	return r.GetInfo(), nil
}

var (
	_ io.ReadWriteSeeker = (*File)(nil)
	_ io.ReaderAt        = (*File)(nil)
	_ io.WriterAt        = (*File)(nil)
	_ io.Closer          = (*File)(nil)
)
