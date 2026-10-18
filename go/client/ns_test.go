package client

import (
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

func TestSymlinkReadlinkLstat(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/a"] = []byte("hello")
	cl := dial(t, s, 2)
	if err := cl.Symlink("/a", "/l"); err != nil {
		t.Fatal(err)
	}
	if got, err := cl.Readlink("/l"); err != nil || got != "/a" {
		t.Fatalf("readlink = %q, %v", got, err)
	}
	info, err := cl.Lstat("/l")
	if err != nil || info.GetType() != wire.FileType_FILE_TYPE_SYMLINK || info.GetSize() != 2 {
		t.Fatalf("lstat = %v, %v", info, err)
	}
	if _, err := cl.Readlink("/a"); err == nil {
		t.Fatal("readlink on a regular file succeeded")
	}
	if _, err := cl.Lstat("/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lstat missing = %v", err)
	}
	if err := cl.Symlink("/a", "/l"); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("duplicate symlink = %v", err)
	}
}

func TestLink(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/a"] = []byte("data")
	cl := dial(t, s, 1)
	if err := cl.Link("/a", "/b"); err != nil {
		t.Fatal(err)
	}
	info, err := cl.Stat("/b")
	if err != nil || info.GetSize() != 4 || info.GetNlink() != 2 {
		t.Fatalf("stat = %v, %v", info, err)
	}
	if err := cl.Link("/nope", "/c"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("link missing = %v", err)
	}
}

func TestUtimens(t *testing.T) {
	s := newFakeServer(t, 0)
	s.files["/a"] = nil
	cl := dial(t, s, 1)
	at := time.Unix(1000, 5)
	mt := time.Unix(2000, 7)
	if err := cl.Utimens("/a", &at, &mt); err != nil {
		t.Fatal(err)
	}
	info, _ := cl.Stat("/a")
	if info.GetAtimeNs() != at.UnixNano() || info.GetMtimeNs() != mt.UnixNano() {
		t.Fatalf("times = %d, %d", info.GetAtimeNs(), info.GetMtimeNs())
	}
	// A nil time leaves that field unchanged.
	mt2 := time.Unix(3000, 0)
	if err := cl.Utimens("/a", nil, &mt2); err != nil {
		t.Fatal(err)
	}
	info, _ = cl.Stat("/a")
	if info.GetAtimeNs() != at.UnixNano() || info.GetMtimeNs() != mt2.UnixNano() {
		t.Fatalf("after nil atime: %d, %d", info.GetAtimeNs(), info.GetMtimeNs())
	}
	if err := cl.Utimens("/a", nil, nil); err != nil {
		t.Fatal(err)
	}
	info, _ = cl.Stat("/a")
	if info.GetAtimeNs() != at.UnixNano() || info.GetMtimeNs() != mt2.UnixNano() {
		t.Fatalf("after nil,nil: %d, %d", info.GetAtimeNs(), info.GetMtimeNs())
	}
	if err := cl.Utimens("/missing", &at, nil); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing = %v", err)
	}
}

func TestStatfs(t *testing.T) {
	cl := dial(t, newFakeServer(t, 0), 1)
	got, err := cl.Statfs("/")
	want := StatfsInfo{BlockSize: 4096, Blocks: 1000, BlocksFree: 600, BlocksAvail: 500, Files: 100, FilesFree: 90, NameMax: 255}
	if err != nil || *got != want {
		t.Fatalf("statfs = %+v, %v", got, err)
	}
}

func TestOpendirReaddirPaging(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/d"] = []string{"a", "b", "c", "d", "e"}
	cl := dial(t, s, 2)
	d, err := cl.Opendir("/d")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	pages := 0
	for {
		ents, eof, err := d.Readdir(2)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, e := range ents {
			names = append(names, e.GetName())
		}
		if eof {
			break
		}
	}
	if pages != 3 || len(names) != 5 || names[0] != "a" || names[4] != "e" {
		t.Fatalf("pages = %d, names = %v", pages, names)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); !errors.Is(err, ErrFileClosed) {
		t.Fatalf("second close = %v", err)
	}
	if _, _, err := d.Readdir(1); !errors.Is(err, ErrFileClosed) {
		t.Fatalf("readdir after close = %v", err)
	}
	if _, err := cl.Opendir("/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("opendir missing = %v", err)
	}
	if s.violations != 0 {
		t.Fatalf("violations = %d", s.violations)
	}
}

func TestDirConnLost(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/d"] = []string{"a"}
	cl := dial(t, s, 1)
	d, err := cl.Opendir("/d")
	if err != nil {
		t.Fatal(err)
	}
	s.dropAll()
	if _, _, err := d.Readdir(1); err == nil {
		t.Fatal("readdir after drop succeeded")
	}
	if _, _, err := d.Readdir(1); !errors.Is(err, ErrConnLost) {
		t.Fatalf("second readdir = %v, want ErrConnLost", err)
	}
}

func TestLsCursor(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/d"] = []string{"a", "b", "c"}
	s.files["/d/a"] = []byte("x")
	cl := dial(t, s, 1)
	ents, next, err := cl.Ls("/d", 2, "")
	if err != nil || len(ents) != 2 || next == "" || ents[0].GetInfo().GetSize() != 1 {
		t.Fatalf("page1 = %v, %q, %v", ents, next, err)
	}
	ents, next, err = cl.Ls("/d", 2, next)
	if err != nil || len(ents) != 1 || ents[0].GetName() != "c" || next != "" {
		t.Fatalf("page2 = %v, %q, %v", ents, next, err)
	}
	if _, _, err := cl.Ls("/missing", 2, ""); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ls missing = %v", err)
	}
}

func TestRemoveAll(t *testing.T) {
	s := newFakeServer(t, 0)
	s.dirs["/t"] = []string{"a", "b"}
	s.files["/t/a"] = nil
	s.files["/t/b"] = nil
	s.files["/tx"] = nil
	cl := dial(t, s, 1)
	n, err := cl.RemoveAll("/t")
	if err != nil || n != 3 {
		t.Fatalf("removed = %d, %v", n, err)
	}
	if _, err := cl.Stat("/tx"); err != nil {
		t.Fatalf("sibling removed: %v", err)
	}
	if _, err := cl.RemoveAll("/t"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("second = %v", err)
	}
}
