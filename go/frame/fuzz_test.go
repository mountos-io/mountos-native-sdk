package frame

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"runtime"
	"testing"
)

var fuzzLimits = Limits{MaxMetaBytes: 256, MaxPayloadBytes: 1024}

// typed reports whether err is one of the errors Read documents.
func typed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, ErrBadMagic) || errors.Is(err, ErrUnsupportedVersion) || errors.Is(err, ErrTooLarge)
}

// FuzzRead feeds arbitrary bytes to Read with small limits. Read never
// panics, returns a typed error, and never returns a section over its limit.
func FuzzRead(f *testing.F) {
	valid, _ := Append(nil, Frame{Flags: FlagResponse, Method: 14, RequestID: 9, Meta: []byte("m"), Payload: []byte("pp")}, fuzzLimits)
	f.Add(valid)
	f.Add(valid[:HeaderSize])
	f.Add(valid[:len(valid)-1])
	f.Add([]byte{})
	f.Add(append([]byte("MFS1\x01"), make([]byte, 40)...))
	f.Add(append(bytes.Clone(valid), valid...))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		for {
			fr, err := Read(r, fuzzLimits)
			if err != nil {
				if !typed(err) {
					t.Fatalf("untyped error %T: %v", err, err)
				}
				return
			}
			if len(fr.Meta) > int(fuzzLimits.MaxMetaBytes) || len(fr.Payload) > int(fuzzLimits.MaxPayloadBytes) {
				t.Fatalf("section over limit: meta %d payload %d", len(fr.Meta), len(fr.Payload))
			}
		}
	})
}

// FuzzRoundTrip writes a frame built from the inputs and reads back the same frame.
func FuzzRoundTrip(f *testing.F) {
	f.Add(uint8(1), uint16(2), uint64(3), []byte("meta"), []byte("payload"))
	f.Add(uint8(0xff), uint16(0xffff), ^uint64(0), []byte{}, []byte{})
	f.Fuzz(func(t *testing.T, flags uint8, method uint16, id uint64, meta, payload []byte) {
		want := Frame{Flags: flags, Method: method, RequestID: id, Meta: meta, Payload: payload}
		var buf bytes.Buffer
		err := Write(&buf, want, fuzzLimits)
		if len(meta) > int(fuzzLimits.MaxMetaBytes) || len(payload) > int(fuzzLimits.MaxPayloadBytes) {
			if !errors.Is(err, ErrTooLarge) || buf.Len() != 0 {
				t.Fatalf("oversize write: err %v, wrote %d bytes", err, buf.Len())
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		appended, err := Append(nil, want, fuzzLimits)
		if err != nil || !bytes.Equal(appended, buf.Bytes()) {
			t.Fatalf("Append differs from Write: %v", err)
		}
		got, err := Read(&buf, fuzzLimits)
		if err != nil {
			t.Fatal(err)
		}
		if got.Flags != flags || got.Method != method || got.RequestID != id ||
			!bytes.Equal(got.Meta, meta) || !bytes.Equal(got.Payload, payload) {
			t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
		}
		if _, err := Read(&buf, fuzzLimits); err != io.EOF {
			t.Fatalf("after last frame: %v, want io.EOF", err)
		}
	})
}

// A fixed seed run of the round trip property with a frame stream, so the
// property holds on every plain test run without the fuzz engine.
func TestRoundTripStreamRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(0x6d667331))
	var buf bytes.Buffer
	var want []Frame
	for i := 0; i < 2000; i++ {
		fr := Frame{
			Flags: uint8(rng.Intn(256)), Method: uint16(rng.Intn(1 << 16)), RequestID: rng.Uint64(),
			Meta: randBytes(rng, rng.Intn(int(fuzzLimits.MaxMetaBytes)+1)), Payload: randBytes(rng, rng.Intn(int(fuzzLimits.MaxPayloadBytes)+1)),
		}
		if err := Write(&buf, fr, fuzzLimits); err != nil {
			t.Fatal(err)
		}
		want = append(want, fr)
	}
	for i, w := range want {
		got, err := Read(&buf, fuzzLimits)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if got.Flags != w.Flags || got.Method != w.Method || got.RequestID != w.RequestID ||
			!bytes.Equal(got.Meta, w.Meta) || !bytes.Equal(got.Payload, w.Payload) {
			t.Fatalf("frame %d mismatch", i)
		}
	}
	if _, err := Read(&buf, fuzzLimits); err != io.EOF {
		t.Fatalf("tail: %v, want io.EOF", err)
	}
}

func randBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	rng.Read(b)
	return b
}

// A header that claims a section at the limit and then ends must not make
// Read allocate the claimed size. Memory follows the bytes that arrived.
func TestStalledClaimAllocatesLittle(t *testing.T) {
	lim := Limits{MaxMetaBytes: 1 << 20, MaxPayloadBytes: 1 << 30}
	hdr, err := Append(nil, Frame{Method: 1, RequestID: 1}, lim)
	if err != nil {
		t.Fatal(err)
	}
	hdr[20], hdr[21], hdr[22], hdr[23] = 0x3f, 0xff, 0xff, 0xff // payload claim just under 1 GiB
	body := append(hdr, make([]byte, 100<<10)...)               // 100 KiB arrives, then EOF
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = Read(bytes.NewReader(body), lim)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Fatalf("allocated %d bytes for a 100 KiB body with a 1 GiB claim", grew)
	}
}

// Every prefix of a valid frame stream fails with a typed error, and only the
// empty prefix and the full stream end cleanly.
func TestTruncatedPrefixes(t *testing.T) {
	var buf bytes.Buffer
	Write(&buf, Frame{Flags: FlagResponse, Method: 3, RequestID: 5, Meta: []byte("abc"), Payload: []byte("defgh")}, fuzzLimits)
	full := buf.Bytes()
	for n := 0; n < len(full); n++ {
		_, err := Read(bytes.NewReader(full[:n]), fuzzLimits)
		switch {
		case n == 0 && err == io.EOF:
		case n > 0 && errors.Is(err, io.ErrUnexpectedEOF):
		default:
			t.Fatalf("prefix %d: err = %v", n, err)
		}
	}
}
