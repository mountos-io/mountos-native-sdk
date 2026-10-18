package frame

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

var testLimits = Limits{MaxMetaBytes: 1 << 16, MaxPayloadBytes: 1 << 20}

func encode(t *testing.T, f Frame) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Write(&buf, f, testLimits); err != nil {
		t.Fatalf("write: %v", err)
	}
	return buf.Bytes()
}

func TestRoundTrip(t *testing.T) {
	cases := map[string]Frame{
		"empty":        {Method: 2, RequestID: 1},
		"meta only":    {Method: 10, RequestID: 7, Meta: []byte("meta")},
		"payload only": {Method: 13, RequestID: 8, Payload: []byte("raw bytes")},
		"both":         {Flags: FlagResponse, Method: 14, RequestID: 1 << 40, Meta: []byte{1, 2}, Payload: bytes.Repeat([]byte{0xAB}, 4096)},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := Read(bytes.NewReader(encode(t, want)), testLimits)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if got.Flags != want.Flags || got.Method != want.Method || got.RequestID != want.RequestID {
				t.Fatalf("header mismatch: got %+v want %+v", got, want)
			}
			if !bytes.Equal(got.Meta, want.Meta) || !bytes.Equal(got.Payload, want.Payload) {
				t.Fatalf("body mismatch")
			}
		})
	}
}

func TestHeaderLayoutIsFixed(t *testing.T) {
	raw := encode(t, Frame{Flags: FlagError, Method: 0x0102, RequestID: 0x0304, Meta: []byte{9}, Payload: []byte{8, 7}})
	if len(raw) != HeaderSize+1+2 {
		t.Fatalf("frame length %d, want %d", len(raw), HeaderSize+3)
	}
	want := []byte{'M', 'F', 'S', '1', Version, FlagError, 0x01, 0x02}
	if !bytes.Equal(raw[:8], want) {
		t.Fatalf("prefix % x, want % x", raw[:8], want)
	}
}

func TestReadStreamOfFrames(t *testing.T) {
	var buf bytes.Buffer
	for i := uint64(1); i <= 3; i++ {
		if err := Write(&buf, Frame{Method: 2, RequestID: i}, testLimits); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(1); i <= 3; i++ {
		f, err := Read(&buf, testLimits)
		if err != nil || f.RequestID != i {
			t.Fatalf("frame %d: got %+v, err %v", i, f, err)
		}
	}
	if _, err := Read(&buf, testLimits); err != io.EOF {
		t.Fatalf("clean end: got %v, want io.EOF", err)
	}
}

func TestReadRejects(t *testing.T) {
	good := encode(t, Frame{Method: 2, RequestID: 1, Meta: []byte("m")})

	badMagic := append([]byte{}, good...)
	badMagic[0] = 'X'

	badVersion := append([]byte{}, good...)
	badVersion[4] = Version + 1

	tooBigMeta := append([]byte{}, good...)
	tooBigMeta[16], tooBigMeta[17], tooBigMeta[18], tooBigMeta[19] = 0xFF, 0xFF, 0xFF, 0xFF

	cases := map[string]struct {
		in   []byte
		want error
	}{
		"bad magic":        {badMagic, ErrBadMagic},
		"bad version":      {badVersion, ErrUnsupportedVersion},
		"meta over limit":  {tooBigMeta, ErrTooLarge},
		"truncated header": {good[:10], io.ErrUnexpectedEOF},
		"truncated body":   {good[:HeaderSize], io.ErrUnexpectedEOF},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Read(bytes.NewReader(tc.in), testLimits)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestWriteRejectsOverLimit(t *testing.T) {
	var buf bytes.Buffer
	err := Write(&buf, Frame{Payload: make([]byte, testLimits.MaxPayloadBytes+1)}, testLimits)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("wrote %d bytes for a rejected frame", buf.Len())
	}
}
