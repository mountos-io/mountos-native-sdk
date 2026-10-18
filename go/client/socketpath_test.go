package client

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func setHome(t *testing.T, home string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Setenv("USERPROFILE", home)
		return
	}
	t.Setenv("HOME", home)
}

func TestSocketPathGolden(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("golden paths use a Unix home")
	}
	setHome(t, "/home/u")
	cases := map[string]string{
		"/mnt/vol":          "mountos_api_9bbffa0eba37045f.sock",
		"/Volumes/data one": "mountos_api_87774d7d94a7fe60.sock",
		"/mnt/ü/x":          "mountos_api_bc8db74b980a9567.sock",
	}
	for mount, name := range cases {
		got, err := SocketPath(mount)
		if err != nil {
			t.Fatal(err)
		}
		if want := filepath.Join("/home/u/.mountOS/sockets", name); got != want {
			t.Errorf("%s: got %s want %s", mount, got, want)
		}
	}
}

func TestSocketPathCleansHome(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	setHome(t, "/home//u/")
	got, _ := SocketPath("/mnt/vol")
	if want := "/home/u/.mountOS/sockets/mountos_api_9bbffa0eba37045f.sock"; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestSocketPathLongHomeFallsBack(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	setHome(t, "/home/"+strings.Repeat("x", 80))
	got, err := SocketPath("/mnt/vol")
	if err != nil {
		t.Fatal(err)
	}
	want := "/tmp/mountos-" + strconv.Itoa(os.Getuid()) + "/mountos_api_9bbffa0eba37045f.sock"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestSocketPathLimitIsInclusive(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	const name = "mountos_api_9bbffa0eba37045f.sock"
	fit := "/" + strings.Repeat("h", maxUnixSocketPath-len(name)-len("/.mountOS/sockets/")-1)
	setHome(t, fit)
	got, _ := SocketPath("/mnt/vol")
	if len(got) != maxUnixSocketPath || !strings.HasPrefix(got, fit) {
		t.Fatalf("at limit: %s (%d)", got, len(got))
	}
	setHome(t, fit+"h")
	got, _ = SocketPath("/mnt/vol")
	if !strings.HasPrefix(got, "/tmp/mountos-") {
		t.Fatalf("over limit: %s", got)
	}
}

func TestSocketPathResolvesSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink resolution is Unix-only")
	}
	setHome(t, "/home/u")
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	want, err := SocketPath(target)
	if err != nil {
		t.Fatal(err)
	}
	got, err := SocketPath(link)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("via symlink %s, want %s", got, want)
	}
}

func TestSocketPathMissingPathKeepsSpelling(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("golden paths use a Unix home")
	}
	setHome(t, "/home/u")
	missing := filepath.Join(t.TempDir(), "missing")
	sum := sha256.Sum256([]byte(missing))
	want := filepath.Join("/home/u/.mountOS/sockets", "mountos_api_"+hex.EncodeToString(sum[:8])+".sock")
	got, err := SocketPath(missing)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestSocketPathEmptyRejected(t *testing.T) {
	if _, err := SocketPath(""); err == nil {
		t.Fatal("empty mount path accepted")
	}
}
