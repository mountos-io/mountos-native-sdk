package client

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%251)
	}
	return b
}

func TestStatBatchFailingItem(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/a"] = []byte("abc")
	s.files["/c"] = []byte("abcde")
	cl := dial(t, s, 1)
	res, err := cl.StatBatch([]string{"/a", "/missing", "/c"})
	if err != nil || len(res) != 3 {
		t.Fatalf("res = %v, %v", res, err)
	}
	if res[0].Err != nil || res[0].Info.GetSize() != 3 || res[2].Err != nil || res[2].Info.GetSize() != 5 {
		t.Fatalf("good items wrong: %+v %+v", res[0], res[2])
	}
	if res[1].Info != nil || !errors.Is(res[1].Err, fs.ErrNotExist) {
		t.Fatalf("failed item = %+v", res[1])
	}
	var we *WireError
	if !errors.As(res[1].Err, &we) || we.Code != "ENOENT" {
		t.Fatalf("not a WireError: %v", res[1].Err)
	}
	if out, err := cl.StatBatch(nil); err != nil || len(out) != 0 {
		t.Fatalf("empty = %v, %v", out, err)
	}
}

func TestStatBatchSplits(t *testing.T) {
	s := newFakeServer(t, 0)
	const n = 4096*2 + 10
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("/f%d", i)
		if i%7 != 0 {
			s.files[paths[i]] = make([]byte, i)
		}
	}
	cl := dial(t, s, 1)
	res, err := cl.StatBatch(paths)
	if err != nil || len(res) != n {
		t.Fatalf("len = %d, %v", len(res), err)
	}
	for i, r := range res {
		if i%7 == 0 {
			if r.Err == nil || r.Info != nil {
				t.Fatalf("item %d should fail: %+v", i, r)
			}
		} else if r.Err != nil || r.Info.GetSize() != int64(i) {
			t.Fatalf("item %d = %+v", i, r)
		}
	}
	s.mu.Lock()
	sizes := append([]int(nil), s.statSizes...)
	s.mu.Unlock()
	if fmt.Sprint(sizes) != "[4096 4096 10]" {
		t.Fatalf("request sizes = %v", sizes)
	}
}

func openRW(t *testing.T, cl *Client, path string) *File {
	t.Helper()
	f, err := cl.Open(path, rw, create)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func TestBatchRoundTripWithHole(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f := openRW(t, cl, "/f")
	wr, err := f.WriteBatch([]WriteItem{{Offset: 0, Data: []byte("head")}, {Offset: 100, Data: []byte("tail")}})
	if err != nil || len(wr) != 2 || wr[0].N != 4 || wr[1].N != 4 || wr[0].Err != nil || wr[1].Err != nil {
		t.Fatalf("write = %+v, %v", wr, err)
	}
	rr, err := f.ReadBatch([]Range{{0, 4}, {4, 4}, {100, 4}, {102, 50}, {500, 8}})
	if err != nil || len(rr) != 5 {
		t.Fatalf("read = %+v, %v", rr, err)
	}
	if string(rr[0].Data) != "head" || !bytes.Equal(rr[1].Data, make([]byte, 4)) || string(rr[2].Data) != "tail" {
		t.Fatalf("data wrong: %q %q %q", rr[0].Data, rr[1].Data, rr[2].Data)
	}
	if string(rr[3].Data) != "il" || len(rr[4].Data) != 0 {
		t.Fatalf("short ranges: %q %q", rr[3].Data, rr[4].Data)
	}
	for i, r := range rr {
		if r.Err != nil {
			t.Fatalf("range %d err = %v", i, r.Err)
		}
	}
	if len(s.files["/f"]) != 104 {
		t.Fatalf("size = %d", len(s.files["/f"]))
	}
}

func TestBatchItemErrors(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f := openRW(t, cl, "/f")
	if _, err := f.WriteBatch([]WriteItem{{Offset: -1, Data: []byte("x")}}); err == nil {
		t.Fatal("negative offset accepted")
	}
	wr, err := f.WriteBatch([]WriteItem{{Offset: 0, Data: []byte("data")}, {Offset: badOffset, Data: []byte("x")}, {Offset: 4, Data: []byte("!")}})
	if err != nil || wr[0].N != 4 || wr[2].N != 1 || wr[1].N != 0 {
		t.Fatalf("write = %+v, %v", wr, err)
	}
	var we *WireError
	if !errors.As(wr[1].Err, &we) || we.Code != "EIO" || wr[0].Err != nil || wr[2].Err != nil {
		t.Fatalf("write errors = %+v", wr)
	}
	rr, err := f.ReadBatch([]Range{{0, 4}, {badOffset, 2}, {1, 2}})
	if err != nil || string(rr[0].Data) != "data" || string(rr[2].Data) != "at" || len(rr[1].Data) != 0 {
		t.Fatalf("read = %+v, %v", rr, err)
	}
	if !errors.As(rr[1].Err, &we) || we.Code != "EIO" || rr[0].Err != nil || rr[2].Err != nil {
		t.Fatalf("read errors = %+v", rr)
	}
}

func TestBatchSplitsByPayload(t *testing.T) {
	s := newFakeServer(t, 1024)
	cl := dial(t, s, 1)
	f := openRW(t, cl, "/f")
	// Five items of 400 bytes: two fit per request. One item of 2500 bytes needs three pieces.
	var items []WriteItem
	want := make([]byte, 0, 4500)
	for i := 0; i < 5; i++ {
		d := pattern(400, byte(i))
		items = append(items, WriteItem{Offset: int64(i * 400), Data: d})
		want = append(want, d...)
	}
	big := pattern(2500, 9)
	items = append(items, WriteItem{Offset: 2000, Data: big})
	want = append(want[:2000], big...)
	wr, err := f.WriteBatch(items)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range wr {
		if r.Err != nil || r.N != len(items[i].Data) {
			t.Fatalf("write %d = %+v", i, r)
		}
	}
	if s.maxSeen != 0 && s.maxSeen > 1024 {
		t.Fatalf("payload over limit: %d", s.maxSeen)
	}
	if !bytes.Equal(s.files["/f"], want) {
		t.Fatal("file content differs after split write")
	}
	if len(s.writeSizes) < 3 {
		t.Fatalf("expected several write requests, got %v", s.writeSizes)
	}
	rr, err := f.ReadBatch([]Range{{0, 400}, {2000, 2500}, {400, 400}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rr[0].Data, want[:400]) || !bytes.Equal(rr[1].Data, big) || !bytes.Equal(rr[2].Data, want[400:800]) {
		t.Fatal("read data differs after split read")
	}
	if len(s.readSizes) < 3 {
		t.Fatalf("expected several read requests, got %v", s.readSizes)
	}
}

func TestBatchSplitsByCount(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	f := openRW(t, cl, "/f")
	const n = 4096 + 5
	items := make([]WriteItem, n)
	ranges := make([]Range, n)
	for i := range items {
		items[i] = WriteItem{Offset: int64(i), Data: []byte{byte(i)}}
		ranges[i] = Range{Offset: int64(i), Length: 1}
	}
	wr, err := f.WriteBatch(items)
	if err != nil || len(wr) != n {
		t.Fatalf("write: %d, %v", len(wr), err)
	}
	rr, err := f.ReadBatch(ranges)
	if err != nil || len(rr) != n {
		t.Fatalf("read: %d, %v", len(rr), err)
	}
	for i, r := range rr {
		if len(r.Data) != 1 || r.Data[0] != byte(i) {
			t.Fatalf("item %d = %v", i, r.Data)
		}
	}
	if fmt.Sprint(s.writeSizes) != "[4096 5]" || fmt.Sprint(s.readSizes) != "[4096 5]" {
		t.Fatalf("sizes: write %v read %v", s.writeSizes, s.readSizes)
	}
}

func TestReadFileLooping(t *testing.T) {
	s := newFakeServer(t, 1000)
	small := pattern(300, 1)
	large := pattern(3500, 2)
	exact := pattern(1000, 3)
	s.files["/small"], s.files["/large"], s.files["/exact"] = small, large, exact
	cl := dial(t, s, 1)
	for name, want := range map[string][]byte{"/small": small, "/large": large, "/exact": exact} {
		got, err := cl.ReadFile(name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: len %d (want %d), %v", name, len(got), len(want), err)
		}
	}
	if _, err := cl.ReadFile("/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing = %v", err)
	}
}

func TestWriteFile(t *testing.T) {
	s := newFakeServer(t, 1000)
	cl := dial(t, s, 1)
	if err := cl.WriteFile("/n", []byte("hello"), true); err != nil || string(s.files["/n"]) != "hello" {
		t.Fatalf("small create: %q, %v", s.files["/n"], err)
	}
	if err := cl.WriteFile("/n", []byte("HE"), false); err != nil || string(s.files["/n"]) != "HEllo" {
		t.Fatalf("no truncate: %q, %v", s.files["/n"], err)
	}
	if err := cl.WriteFile("/n", []byte("x"), true); err != nil || string(s.files["/n"]) != "x" {
		t.Fatalf("truncate: %q, %v", s.files["/n"], err)
	}
	big := pattern(3500, 5)
	if err := cl.WriteFile("/big", big, true); err != nil || !bytes.Equal(s.files["/big"], big) {
		t.Fatalf("large create: len %d, %v", len(s.files["/big"]), err)
	}
	if s.maxSeen > 1000 {
		t.Fatalf("payload over limit: %d", s.maxSeen)
	}
	s.files["/over"] = pattern(5000, 1)
	if err := cl.WriteFile("/over", big, false); err != nil || len(s.files["/over"]) != 5000 || !bytes.Equal(s.files["/over"][:3500], big) {
		t.Fatalf("large no truncate: len %d, %v", len(s.files["/over"]), err)
	}
	if err := cl.WriteFile("/over", big, true); err != nil || len(s.files["/over"]) != 3500 {
		t.Fatalf("large truncate: len %d, %v", len(s.files["/over"]), err)
	}
}

func nextChange(t *testing.T, w *Watch) Event {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			t.Fatal("watch channel closed")
		}
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("no event")
	}
	return Event{}
}

func TestWatchEvents(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	ch, _ := collect(cl)
	w, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.ID() == 0 {
		t.Fatal("zero watch id")
	}
	s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_CREATE, "/d/a", "")
	s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_RENAME, "/d/b", "/d/a")
	e1, e2 := nextChange(t, w), nextChange(t, w)
	if e1.Kind != EventChange || e1.Change != Create || e1.Path != "/d/a" || e1.RefID != w.ID() || e1.Sequence != 1 {
		t.Fatalf("e1 = %+v", e1)
	}
	if e2.Change != Rename || e2.Path != "/d/b" || e2.OldPath != "/d/a" || e2.Sequence != 2 {
		t.Fatalf("e2 = %+v", e2)
	}
	if l1, l2 := recvEv(t, ch), recvEv(t, ch); l1.Change != Create || l2.OldPath != "/d/a" {
		t.Fatalf("listener events = %+v, %+v", l1, l2)
	}
}

func TestWatchFiltersById(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	w1, _ := cl.Watch("/a", true, 0)
	w2, _ := cl.Watch("/b", true, 0)
	defer w1.Close()
	defer w2.Close()
	s.change(w2.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_WRITE, "/b/x", "")
	s.change(w1.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_WRITE, "/a/x", "")
	if e := nextChange(t, w1); e.Path != "/a/x" {
		t.Fatalf("w1 got %+v", e)
	}
	if e := nextChange(t, w2); e.Path != "/b/x" {
		t.Fatalf("w2 got %+v", e)
	}
	select {
	case e := <-w1.Events():
		t.Fatalf("w1 extra %+v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWatchEarlyChanges(t *testing.T) {
	s := newFakeServer(t, 0)
	s.nt.eager = 5
	cl := dial(t, s, 1)
	w, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 5; i++ {
		if e := nextChange(t, w); e.Path != fmt.Sprintf("/e%d", i) {
			t.Fatalf("event %d = %+v", i, e)
		}
	}
}

func TestWatchOldReplayPointOverflows(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING}) // seq 1
	s.notify(0, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING}) // seq 2
	time.Sleep(20 * time.Millisecond)
	w, err := cl.Watch("/d", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if e := nextChange(t, w); e.Kind != EventOverflow || e.RefID != w.ID() {
		t.Fatalf("event = %+v", e)
	}
}

func TestWatchChangeNoticeOfKindOverflowIsOverflow(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	w, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_OVERFLOW, "", "")
	if e := nextChange(t, w); e.Kind != EventOverflow || e.RefID != w.ID() {
		t.Fatalf("event = %+v", e)
	}
}

func TestWatchCloseSendsUnwatchOnSameConn(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 4)
	w, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	owner := s.watchConnOf(w.ID())
	if s.watchCount() != 1 {
		t.Fatalf("watch count = %d", s.watchCount())
	}
	// other connections stay busy so a wrong pick would show
	for i := 0; i < 8; i++ {
		cl.Ping()
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	log := s.unwatchLog()
	if len(log) != 1 || log[0].conn != owner || log[0].id != w.ID() {
		t.Fatalf("unwatch log = %+v, owner conn %d", log, owner)
	}
	if _, ok := <-w.Events(); ok {
		t.Fatal("channel still open")
	}
	if s.watchCount() != 0 {
		t.Fatalf("server still has %d watches", s.watchCount())
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
}

func TestWatchCloseAfterConnGone(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	w, err := cl.Watch("/d", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.dropAll()
	deadline := time.After(3 * time.Second)
	for open := true; open; {
		select {
		case _, open = <-w.Events():
		case <-deadline:
			t.Fatal("channel not closed after connection loss")
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close = %v", err)
	}
}

func TestWatchUnwatchOnWrongConnIsEBADF(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	w, _ := cl.Watch("/d", true, 0)
	defer w.Close()
	for i := 0; i < 4; i++ {
		var we *WireError
		err := cl.run(mUnwatch, &wire.UnwatchRequest{WatchId: w.ID()}, &wire.Empty{})
		if err != nil && !errors.As(err, &we) {
			t.Fatal(err)
		}
		if err != nil && we.Code != "EBADF" {
			t.Fatalf("code = %s", we.Code)
		}
	}
}

func TestWatchSlowConsumerOverflow(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	count := make(chan struct{}, 4096)
	cl.AddListener(func(e Event) { count <- struct{}{} })
	w, _ := cl.Watch("/d", true, 0)
	defer w.Close()
	const total = watchBuffer + 100
	for i := 0; i < total; i++ {
		s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_WRITE, fmt.Sprintf("/d/%d", i), "")
	}
	for i := 0; i < total; i++ {
		select {
		case <-count:
		case <-time.After(3 * time.Second):
			t.Fatal("listener missed events")
		}
	}
	for i := 0; i < watchBuffer; i++ {
		if e := nextChange(t, w); e.Kind != EventChange || e.Path != fmt.Sprintf("/d/%d", i) {
			t.Fatalf("event %d = %+v", i, e)
		}
	}
	s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_WRITE, "/d/last", "")
	if e := nextChange(t, w); e.Kind != EventOverflow || e.RefID != w.ID() {
		t.Fatalf("want overflow, got %+v", e)
	}
	if e := nextChange(t, w); e.Path != "/d/last" {
		t.Fatalf("after overflow = %+v", e)
	}
}

func TestWatchAfterClientClose(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	cl.Close()
	if _, err := cl.Watch("/d", true, 0); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("watch = %v", err)
	}
}

func TestClientCloseEndsWatches(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 2)
	w1, _ := cl.Watch("/a", true, 0)
	w2, _ := cl.Watch("/b", true, 0)
	cl.Close()
	for _, w := range []*Watch{w1, w2} {
		select {
		case _, ok := <-w.Events():
			if ok {
				t.Fatal("unexpected event")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("channel not closed")
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close after client close = %v", err)
		}
	}
}

func TestWatchRace(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 3)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				w, err := cl.Watch("/a", true, 0)
				if err != nil {
					return
				}
				s.change(w.ID(), wire.WatchEventKind_WATCH_EVENT_KIND_ATTRIB, "/a", "")
				cl.Ping()
				w.Close()
			}
		}()
	}
	wg.Wait()
}
