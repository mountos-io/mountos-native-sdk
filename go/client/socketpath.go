package client

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
)

// maxUnixSocketPath is the longest usable AF_UNIX path. macOS allows 104 bytes with the NUL,
// Linux 108.
const maxUnixSocketPath = 103

// SocketPath computes the native API socket path of the mount at mountPath, the path the mount
// was started with. It matches the path the mount listens on. Nothing is created or checked on
// disk.
//
// The name is mountos_api_ plus the first 8 bytes of the SHA-256 of mountPath in hex plus .sock.
// The directory is <home>/.mountOS/sockets. The home comes from the environment, then the
// account database, then the temporary directory. When the path is longer than the AF_UNIX
// limit, the socket is in /tmp/mountos-<uid> instead. On Windows the fallback is
// %LOCALAPPDATA%\mountOS\sockets.
func SocketPath(mountPath string) (string, error) {
	if mountPath == "" {
		return "", errors.New("mountos: SocketPath: empty mount path")
	}
	// Hash the symlink-resolved path, as the mount does. A path that does not exist keeps its spelling.
	if runtime.GOOS != "windows" {
		if resolved, err := filepath.EvalSymlinks(mountPath); err == nil {
			mountPath = resolved
		}
	}
	sum := sha256.Sum256([]byte(mountPath))
	name := "mountos_api_" + hex.EncodeToString(sum[:8]) + ".sock"
	if preferred := filepath.Join(socketsDir(), name); len(preferred) <= maxUnixSocketPath {
		return preferred, nil
	}
	return filepath.Join(fallbackSocketsDir(), name), nil
}

func socketsDir() string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".mountOS", "sockets")
	}
	if u, err := user.Current(); err == nil && u.HomeDir != "" {
		return filepath.Join(u.HomeDir, ".mountOS", "sockets")
	}
	return filepath.Join(os.TempDir(), ".mountOS", "sockets")
}

func fallbackSocketsDir() string {
	if runtime.GOOS == "windows" {
		base := os.Getenv("LOCALAPPDATA")
		if base == "" {
			base = os.TempDir()
		}
		return filepath.Join(base, "mountOS", "sockets")
	}
	return filepath.Join("/tmp", "mountos-"+strconv.Itoa(os.Getuid()))
}
