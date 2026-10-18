package frame

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
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

func TestReadClaimedSectionDoesNotPreallocate(t *testing.T) {
	lim := Limits{MaxMetaBytes: 16 << 20, MaxPayloadBytes: 8 << 20}
	var buf bytes.Buffer
	hdr := [HeaderSize]byte{'M', 'F', 'S', '1', Version}
	hdr[16], hdr[17] = 0x01, 0x00 // meta length 16 MiB
	buf.Write(hdr[:])
	buf.Write(make([]byte, 100)) // the peer sends 100 bytes and stops
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := Read(&buf, lim)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want unexpected EOF", err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Fatalf("allocated %d bytes for a stalled 16 MiB claim", got)
	}
}

func TestReadStalledAfterChunksStaysBounded(t *testing.T) {
	lim := Limits{MaxMetaBytes: 16 << 20, MaxPayloadBytes: 8 << 20}
	for _, sent := range []int{readChunk, readChunk + 1, 3 * readChunk, 5 * readChunk} {
		var buf bytes.Buffer
		hdr := [HeaderSize]byte{'M', 'F', 'S', '1', Version}
		hdr[21] = 0x80 // payload length 8 MiB
		buf.Write(hdr[:])
		buf.Write(make([]byte, sent))
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := Read(&buf, lim)
		runtime.ReadMemStats(&after)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("sent %d: err = %v, want unexpected EOF", sent, err)
		}
		// Total allocation stays within a small multiple of the bytes that arrived.
		if got := after.TotalAlloc - before.TotalAlloc; got > uint64(growFactor*sent+2*readChunk)*2 {
			t.Fatalf("sent %d: allocated %d bytes for a stalled 8 MiB claim", sent, got)
		}
	}
}

func TestReadLargePayloadAllocations(t *testing.T) {
	lim := Limits{MaxMetaBytes: 1 << 10, MaxPayloadBytes: 8 << 20}
	payload := bytes.Repeat([]byte{7}, 1<<20)
	var enc bytes.Buffer
	if err := Write(&enc, Frame{Payload: payload}, lim); err != nil {
		t.Fatal(err)
	}
	raw := enc.Bytes()
	allocs := testing.AllocsPerRun(20, func() {
		f, err := Read(bytes.NewReader(raw), lim)
		if err != nil || len(f.Payload) != len(payload) {
			t.Fatalf("read failed: %v", err)
		}
	})
	if allocs > 5 {
		t.Fatalf("1 MiB read made %v allocations, want at most 5", allocs)
	}
}

func TestReadLargeSectionsRoundTrip(t *testing.T) {
	lim := Limits{MaxMetaBytes: 1 << 20, MaxPayloadBytes: 1 << 20}
	meta := bytes.Repeat([]byte{1, 2, 3}, 70_000)
	payload := bytes.Repeat([]byte{9, 8}, 200_001)
	var buf bytes.Buffer
	if err := Write(&buf, Frame{Meta: meta, Payload: payload}, lim); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf, lim)
	if err != nil || !bytes.Equal(got.Meta, meta) || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestWriteBuffersMatchesWrite(t *testing.T) {
	cases := []Frame{
		{Method: 2, RequestID: 1},
		{Method: 10, RequestID: 7, Meta: []byte("meta")},
		{Method: 13, RequestID: 8, Payload: []byte("raw bytes")},
		{Flags: FlagResponse, Method: 14, RequestID: 1 << 40, Meta: []byte{1, 2}, Payload: bytes.Repeat([]byte{0xAB}, 4096)},
	}
	for _, f := range cases {
		var got bytes.Buffer
		if err := WriteBuffers(&got, f, testLimits); err != nil {
			t.Fatalf("WriteBuffers: %v", err)
		}
		if !bytes.Equal(got.Bytes(), encode(t, f)) {
			t.Fatalf("WriteBuffers bytes differ from Write for %+v", f)
		}
	}
}

func TestWriteBuffersRejectsOverLimit(t *testing.T) {
	var buf bytes.Buffer
	f := Frame{Meta: make([]byte, testLimits.MaxMetaBytes+1)}
	if err := WriteBuffers(&buf, f, testLimits); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("meta over limit: got %v, want ErrTooLarge", err)
	}
	f = Frame{Payload: make([]byte, testLimits.MaxPayloadBytes+1)}
	if err := WriteBuffers(&buf, f, testLimits); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("payload over limit: got %v, want ErrTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("rejected frame wrote %d bytes", buf.Len())
	}
}

func TestWriteBuffersOverTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback: %v", err)
	}
	defer ln.Close()
	want := Frame{Flags: FlagResponse, Method: 14, RequestID: 9, Meta: []byte("m"), Payload: bytes.Repeat([]byte{7}, 200<<10)}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = WriteBuffers(c, want, testLimits)
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := Read(c, testLimits)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.RequestID != want.RequestID || !bytes.Equal(got.Meta, want.Meta) || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatal("frame differs after TCP round trip")
	}
}

func TestAppendMatchesWrite(t *testing.T) {
	cases := []Frame{
		{Method: 2, RequestID: 1},
		{Method: 10, RequestID: 7, Meta: []byte("meta")},
		{Flags: FlagResponse, Method: 14, RequestID: 1 << 40, Meta: []byte{1, 2}, Payload: bytes.Repeat([]byte{0xAB}, 4096)},
	}
	prefix := []byte("keep")
	for _, f := range cases {
		got, err := Append(prefix, f, testLimits)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if !bytes.HasPrefix(got, prefix) || !bytes.Equal(got[len(prefix):], encode(t, f)) {
			t.Fatalf("Append bytes differ from Write for %+v", f)
		}
	}
}

func TestAppendRejectsOverLimitAndKeepsDst(t *testing.T) {
	dst := []byte("keep")
	got, err := Append(dst, Frame{Payload: make([]byte, testLimits.MaxPayloadBytes+1)}, testLimits)
	if !errors.Is(err, ErrTooLarge) || string(got) != "keep" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestReadAllocMatchesRead(t *testing.T) {
	lim := Limits{MaxMetaBytes: 1 << 20, MaxPayloadBytes: 8 << 20}
	for _, n := range []int{0, 1, readChunk, readChunk + 1, 300_000, 5 << 20} {
		payload := bytes.Repeat([]byte{byte(n), 5, 9}, n/3+1)[:n]
		var enc bytes.Buffer
		if err := Write(&enc, Frame{Method: 3, RequestID: 8, Meta: []byte("m"), Payload: payload}, lim); err != nil {
			t.Fatal(err)
		}
		var asked []int
		got, err := ReadAlloc(bytes.NewReader(enc.Bytes()), lim, func(sz int) []byte {
			asked = append(asked, sz)
			// Dirty, oversized slices: ReadAlloc must neither trust the contents nor the length.
			b := bytes.Repeat([]byte{0xAA}, sz+17)
			return b[:3]
		})
		if err != nil || !bytes.Equal(got.Payload, payload) || got.Method != 3 || got.RequestID != 8 || string(got.Meta) != "m" {
			t.Fatalf("n=%d: ReadAlloc = %+v, %v", n, got, err)
		}
		if n == 0 && len(asked) != 0 {
			t.Fatalf("empty payload asked for %v", asked)
		}
		if n > 0 && n <= readChunk && (len(asked) != 1 || asked[0] != n) {
			t.Fatalf("n=%d: asked %v", n, asked)
		}
		if n > readChunk && asked[0] != readChunk {
			t.Fatalf("n=%d: first ask %d, want %d", n, asked[0], readChunk)
		}
	}
}

func TestReadAllocSmallCapacityFallsBack(t *testing.T) {
	lim := Limits{MaxMetaBytes: 1 << 10, MaxPayloadBytes: 1 << 20}
	payload := bytes.Repeat([]byte{4}, 200_000)
	var enc bytes.Buffer
	if err := Write(&enc, Frame{Payload: payload}, lim); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAlloc(&enc, lim, func(int) []byte { return make([]byte, 1) })
	if err != nil || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("fallback read failed: %v", err)
	}
}

func TestReadAllocStalledClaimAsksForReceivedSizeOnly(t *testing.T) {
	lim := Limits{MaxMetaBytes: 1 << 10, MaxPayloadBytes: 8 << 20}
	var buf bytes.Buffer
	hdr := [HeaderSize]byte{'M', 'F', 'S', '1', Version}
	hdr[20], hdr[21] = 0x00, 0x80 // payload length 8 MiB
	buf.Write(hdr[:])
	buf.Write(make([]byte, 100)) // the peer sends 100 bytes and stops
	var asked []int
	_, err := ReadAlloc(&buf, lim, func(n int) []byte { asked = append(asked, n); return make([]byte, n) })
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want unexpected EOF", err)
	}
	if len(asked) != 1 || asked[0] != readChunk {
		t.Fatalf("asked %v, want only %d", asked, readChunk)
	}
}
