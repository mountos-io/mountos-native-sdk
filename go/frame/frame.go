// Package frame encodes and decodes the native API wire frame. Each frame is a
// fixed 24-byte header, a Protobuf metadata section, and a raw payload section.
// Payload bytes are never Protobuf-encoded, so large reads and writes copy once.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// Magic identifies a native API frame stream.
var Magic = [4]byte{'M', 'F', 'S', '1'}

// Version is the frame layout version. It changes only on a wire break, which
// is a major API version bump.
const Version uint8 = 1

// HeaderSize is the fixed header length in bytes.
const HeaderSize = 24

// Flags carried in the header.
const (
	FlagResponse  uint8 = 1 << iota // Frame is a response to request RequestID.
	FlagError                       // Metadata holds a wire error, not the result.
	FlagStreamEnd                   // Final frame of a server stream.
	FlagNotify                      // Server-initiated notice. RequestID names the call it answers.
)

// Frame is one decoded message.
type Frame struct {
	Meta      []byte
	Payload   []byte
	RequestID uint64
	Method    uint16
	Flags     uint8
}

// Limits bounds frame sizes. A reader rejects frames over these before it
// allocates. A peer that sends a larger frame gets a protocol error.
type Limits struct {
	MaxMetaBytes    uint32
	MaxPayloadBytes uint32
}

// ErrBadMagic reports a stream that does not start with a native API frame.
var ErrBadMagic = errors.New("frame: bad magic")

// ErrUnsupportedVersion reports a frame layout version this reader does not know.
var ErrUnsupportedVersion = errors.New("frame: unsupported version")

// ErrTooLarge reports a frame section that exceeds the configured limit.
var ErrTooLarge = errors.New("frame: section exceeds limit")

// header validates f against lim and encodes its header.
func header(f Frame, lim Limits) ([HeaderSize]byte, error) {
	var hdr [HeaderSize]byte
	if uint64(len(f.Meta)) > uint64(lim.MaxMetaBytes) {
		return hdr, fmt.Errorf("%w: meta %d > %d", ErrTooLarge, len(f.Meta), lim.MaxMetaBytes)
	}
	if uint64(len(f.Payload)) > uint64(lim.MaxPayloadBytes) {
		return hdr, fmt.Errorf("%w: payload %d > %d", ErrTooLarge, len(f.Payload), lim.MaxPayloadBytes)
	}
	copy(hdr[0:4], Magic[:])
	hdr[4] = Version
	hdr[5] = f.Flags
	binary.BigEndian.PutUint16(hdr[6:8], f.Method)
	binary.BigEndian.PutUint64(hdr[8:16], f.RequestID)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(f.Meta)))
	binary.BigEndian.PutUint32(hdr[20:24], uint32(len(f.Payload)))
	return hdr, nil
}

// Write encodes f to w as one header followed by the metadata and payload.
// It writes the header and both sections in order with no extra buffering.
func Write(w io.Writer, f Frame, lim Limits) error {
	hdr, err := header(f, lim)
	if err != nil {
		return err
	}
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Meta) > 0 {
		if _, err := w.Write(f.Meta); err != nil {
			return err
		}
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return err
		}
	}
	return nil
}

// Append appends the encoded frame to dst and returns the extended slice. A
// caller that reuses dst sends a small frame with one Write and no allocation.
func Append(dst []byte, f Frame, lim Limits) ([]byte, error) {
	hdr, err := header(f, lim)
	if err != nil {
		return dst, err
	}
	dst = append(dst, hdr[:]...)
	dst = append(dst, f.Meta...)
	return append(dst, f.Payload...), nil
}

// WriteBuffers writes the same bytes as Write. On a net.Conn the header and
// both sections leave in one vectored write, so a frame costs one system call.
// On another writer it falls back to one Write per section.
func WriteBuffers(w io.Writer, f Frame, lim Limits) error {
	hdr, err := header(f, lim)
	if err != nil {
		return err
	}
	bufs := make(net.Buffers, 1, 3)
	bufs[0] = hdr[:]
	if len(f.Meta) > 0 {
		bufs = append(bufs, f.Meta)
	}
	if len(f.Payload) > 0 {
		bufs = append(bufs, f.Payload)
	}
	_, err = bufs.WriteTo(w)
	return err
}

// Read decodes one frame from r. It validates magic, version, and section sizes
// before reading the body. io.EOF is returned only when r ends cleanly at a
// frame boundary.
func Read(r io.Reader, lim Limits) (Frame, error) {
	return ReadAlloc(r, lim, nil)
}

// ReadAlloc decodes one frame like Read, but takes the payload buffer from
// alloc. alloc(n) returns a slice with capacity of at least n; ReadAlloc uses
// its first n bytes and overwrites them in full, so the slice need not be
// zeroed. A slice with less capacity is replaced by a fresh allocation. alloc is
// asked only for sizes the received bytes justify: a payload up to readChunk
// asks once for its size, a larger one asks for readChunk first and then for
// each growth step after the earlier steps filled. A call after the first means
// the earlier slice has been copied and the caller may reuse it once alloc is
// called again. The final payload is the slice from the last call. A nil alloc
// allocates with make, as Read does. The metadata section is never taken from
// alloc.
func ReadAlloc(r io.Reader, lim Limits, alloc func(n int) []byte) (Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	if [4]byte(hdr[0:4]) != Magic {
		return Frame{}, ErrBadMagic
	}
	if hdr[4] != Version {
		return Frame{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, hdr[4])
	}
	metaLen := binary.BigEndian.Uint32(hdr[16:20])
	payloadLen := binary.BigEndian.Uint32(hdr[20:24])
	if metaLen > lim.MaxMetaBytes {
		return Frame{}, fmt.Errorf("%w: meta %d > %d", ErrTooLarge, metaLen, lim.MaxMetaBytes)
	}
	if payloadLen > lim.MaxPayloadBytes {
		return Frame{}, fmt.Errorf("%w: payload %d > %d", ErrTooLarge, payloadLen, lim.MaxPayloadBytes)
	}
	f := Frame{
		Flags:     hdr[5],
		Method:    binary.BigEndian.Uint16(hdr[6:8]),
		RequestID: binary.BigEndian.Uint64(hdr[8:16]),
	}
	var err error
	if f.Meta, err = readSection(r, metaLen, nil); err != nil {
		return Frame{}, unexpectedEOF(err)
	}
	if f.Payload, err = readSection(r, payloadLen, alloc); err != nil {
		return Frame{}, unexpectedEOF(err)
	}
	return f, nil
}

// readChunk is the first step of a large section. The buffer is sized by the
// bytes that arrived, never by the length the header claims, so a peer that
// claims a large section and stalls holds at most one chunk plus the growth
// its received bytes earned.
const readChunk = 64 << 10

// growFactor multiplies the received size when a full buffer needs room.
const growFactor = 4

// readSection reads exactly n bytes from r. It returns nil for n == 0.
// A non-nil alloc supplies the buffers.
func readSection(r io.Reader, n uint32, alloc func(n int) []byte) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	if n <= readChunk {
		b := sized(alloc, int(n))[:n]
		_, err := io.ReadFull(r, b)
		return b, err
	}
	buf := sized(alloc, readChunk)[:readChunk]
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	for uint32(len(buf)) < n {
		// A full buffer grows once to growFactor times what has arrived, capped
		// at the claimed size, so a large section costs a few copies.
		if len(buf) == cap(buf) {
			grown := make([]byte, len(buf), min(int64(n), int64(len(buf))*growFactor))
			copy(grown, buf)
			buf = grown
		}
		end := min(cap(buf), int(n))
		got, err := io.ReadFull(r, buf[len(buf):end])
		buf = buf[:len(buf)+got]
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// sized returns a slice with capacity of at least n from alloc, or from make
// when alloc is nil or returns too little.
func sized(alloc func(n int) []byte, n int) []byte {
	if alloc != nil {
		if b := alloc(n); cap(b) >= n {
			return b[:n]
		}
	}
	return make([]byte, n)
}

// unexpectedEOF turns a mid-frame EOF into io.ErrUnexpectedEOF, so callers can
// tell a clean close from a truncated frame.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
