package client

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

// pagedLister serves a directory of total entries (or endless when total < 0) in pages and
// records the largest page and the number of Ls calls.
type pagedLister struct {
	total int
	calls int
}

func (l *pagedLister) ls(path string, limit uint32, cursor string) ([]*DirEntryWithInfo, string, error) {
	l.calls++
	pos := 0
	if cursor != "" {
		fmt.Sscanf(cursor, "c%d", &pos)
	}
	end := pos + int(limit)
	if l.total >= 0 && end > l.total {
		end = l.total
	}
	out := make([]*DirEntryWithInfo, 0, end-pos)
	for i := pos; i < end; i++ {
		out = append(out, &DirEntryWithInfo{Name: fmt.Sprintf("n%d", i)})
	}
	next := ""
	if l.total < 0 || end < l.total {
		next = fmt.Sprintf("c%d", end)
	}
	return out, next, nil
}

func TestReadDirAllLimitStopsGrowth(t *testing.T) {
	l := &pagedLister{total: -1}
	_, err := readDirAll(context.Background(), l.ls, "/huge", 3000)
	var te *TooManyEntriesError
	if !errors.Is(err, ErrTooManyEntries) || !errors.As(err, &te) || te.Limit != 3000 {
		t.Fatalf("err = %v", err)
	}
	if l.calls > 3000/walkPage+1 {
		t.Fatalf("%d Ls calls for a limit of 3000", l.calls)
	}
}

func TestReadDirAllAtLimit(t *testing.T) {
	l := &pagedLister{total: 2500}
	all, err := readDirAll(context.Background(), l.ls, "/d", 2500)
	if err != nil || len(all) != 2500 {
		t.Fatalf("%d entries, %v", len(all), err)
	}
	l = &pagedLister{total: 2501}
	if _, err := readDirAll(context.Background(), l.ls, "/d", 2500); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("err = %v", err)
	}
}

func TestReadDirAllOptionOverride(t *testing.T) {
	s := newFakeServer(t, 0)
	var names []string
	for i := 0; i < 40; i++ {
		names = append(names, fmt.Sprintf("n%02d", i))
	}
	s.dirs["/d"] = names
	cl := dial(t, s, 1)
	if _, err := cl.ReadDirAll("/d", WithMaxEntries(10)); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("err = %v", err)
	}
	if all, err := cl.ReadDirAll("/d", WithMaxEntries(40)); err != nil || len(all) != 40 {
		t.Fatalf("%d entries, %v", len(all), err)
	}
	if all, err := cl.ReadDirAll("/d"); err != nil || len(all) != 40 {
		t.Fatalf("%d entries, %v", len(all), err)
	}
}

// Every directory of the synthetic tree has one subdirectory, so the tree is endless.
func deepLister(path string, limit uint32, cursor string) ([]*DirEntryWithInfo, string, error) {
	return []*DirEntryWithInfo{{Name: "d", Type: wire.FileType_FILE_TYPE_DIRECTORY}}, "", nil
}

func TestWalkDirDepthLimit(t *testing.T) {
	seen := 0
	err := walkDir(context.Background(), deepLister, "/", 0, 5, func(string, *DirEntryWithInfo) error { seen++; return nil })
	var de *WalkTooDeepError
	if !errors.Is(err, ErrWalkTooDeep) || !errors.As(err, &de) || de.Limit != 5 {
		t.Fatalf("err = %v", err)
	}
	if seen != 6 {
		t.Fatalf("seen %d entries, want 6", seen)
	}
}

func TestWalkDirHoldsOnePagePerLevel(t *testing.T) {
	// A flat directory of many pages: entries delivered equal the total while each Ls call
	// returns at most walkPage entries, so the walk holds one page at a time.
	l := &pagedLister{total: walkPage*5 + 3}
	count, biggest := 0, 0
	ls := func(p string, m uint32, c string) ([]*DirEntryWithInfo, string, error) {
		e, n, err := l.ls(p, m, c)
		biggest = max(biggest, len(e))
		return e, n, err
	}
	if err := walkDir(context.Background(), ls, "/f", 0, 4, func(string, *DirEntryWithInfo) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != walkPage*5+3 || biggest != walkPage || l.calls != 6 {
		t.Fatalf("count %d biggest %d calls %d", count, biggest, l.calls)
	}
}

func TestWalkDirOptionDepth(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/d"] = []string{"a"}
	s.dirs["/d/a"] = []string{"b"}
	s.dirs["/d/a/b"] = []string{"c"}
	cl := dial(t, s, 1)
	nop := func(string, *DirEntryWithInfo) error { return nil }
	if err := cl.WalkDir("/d", nop, WithMaxDepth(1)); !errors.Is(err, ErrWalkTooDeep) {
		t.Fatalf("err = %v", err)
	}
	if err := cl.WalkDir("/d", nop, WithMaxDepth(2)); err != nil {
		t.Fatal(err)
	}
	if err := cl.WalkDir("/d", nop); err != nil {
		t.Fatal(err)
	}
}

func TestReadDirAllRejectsRepeatedCursor(t *testing.T) {
	calls := 0
	ls := func(string, uint32, string) ([]*DirEntryWithInfo, string, error) {
		calls++
		return nil, "same", nil
	}
	var pe *ProtocolError
	if _, err := readDirAll(context.Background(), ls, "/d", 10); !errors.As(err, &pe) {
		t.Fatalf("err = %v", err)
	}
	if calls > 3 {
		t.Fatalf("%d Ls calls for a repeated cursor", calls)
	}
}

func TestWalkDirBoundsEmptyPages(t *testing.T) {
	calls := 0
	ls := func(_ string, _ uint32, cursor string) ([]*DirEntryWithInfo, string, error) {
		calls++
		return nil, cursor + "x", nil
	}
	var pe *ProtocolError
	err := walkDir(context.Background(), ls, "/d", 0, 4, func(string, *DirEntryWithInfo) error { return nil })
	if !errors.As(err, &pe) || calls != maxEmptyPages+1 {
		t.Fatalf("err = %v after %d calls", err, calls)
	}
}

func TestDirHelpersHonourContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := &pagedLister{total: -1}
	ls := func(p string, m uint32, c string) ([]*DirEntryWithInfo, string, error) {
		if l.calls == 2 {
			cancel()
		}
		return l.ls(p, m, c)
	}
	if _, err := readDirAll(ctx, ls, "/huge", 1<<30); !errors.Is(err, context.Canceled) {
		t.Fatalf("readDirAll err = %v", err)
	}
	cancel()
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	l = &pagedLister{total: -1}
	err := walkDir(ctx, ls, "/huge", 0, 4, func(string, *DirEntryWithInfo) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("walkDir err = %v", err)
	}
}
