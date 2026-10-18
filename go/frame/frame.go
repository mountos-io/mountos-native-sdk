// Package frame encodes and decodes the native API wire frame. Each frame is a
// fixed 24-byte header, a Protobuf metadata section, and a raw payload section.
// Payload bytes are never Protobuf-encoded, so large reads and writes copy once.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	Flags     uint8
	Method    uint16
	RequestID uint64
	Meta      []byte
	Payload   []byte
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

// Write encodes f to w as one header followed by the metadata and payload.
// It writes the header and both sections in order with no extra buffering.
func Write(w io.Writer, f Frame, lim Limits) error {
	if uint64(len(f.Meta)) > uint64(lim.MaxMetaBytes) {
		return fmt.Errorf("%w: meta %d > %d", ErrTooLarge, len(f.Meta), lim.MaxMetaBytes)
	}
	if uint64(len(f.Payload)) > uint64(lim.MaxPayloadBytes) {
		return fmt.Errorf("%w: payload %d > %d", ErrTooLarge, len(f.Payload), lim.MaxPayloadBytes)
	}
	var hdr [HeaderSize]byte
	copy(hdr[0:4], Magic[:])
	hdr[4] = Version
	hdr[5] = f.Flags
	binary.BigEndian.PutUint16(hdr[6:8], f.Method)
	binary.BigEndian.PutUint64(hdr[8:16], f.RequestID)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(f.Meta)))
	binary.BigEndian.PutUint32(hdr[20:24], uint32(len(f.Payload)))
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

// Read decodes one frame from r. It validates magic, version, and section sizes
// before reading the body. io.EOF is returned only when r ends cleanly at a
// frame boundary.
func Read(r io.Reader, lim Limits) (Frame, error) {
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
	if metaLen > 0 {
		f.Meta = make([]byte, metaLen)
		if _, err := io.ReadFull(r, f.Meta); err != nil {
			return Frame{}, unexpectedEOF(err)
		}
	}
	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return Frame{}, unexpectedEOF(err)
		}
	}
	return f, nil
}

// unexpectedEOF turns a mid-frame EOF into io.ErrUnexpectedEOF, so callers can
// tell a clean close from a truncated frame.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
