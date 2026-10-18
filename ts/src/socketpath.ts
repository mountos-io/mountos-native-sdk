import { createHash } from "node:crypto";
import { homedir, tmpdir, userInfo } from "node:os";
import { join } from "node:path";

// Longest usable AF_UNIX path. macOS allows 104 bytes with the NUL, Linux 108.
const maxUnixSocketPath = 103;

/**
 * Computes the native API socket path of the mount at `mountPath`, the path the mount was
 * started with. It matches the path the mount listens on. Nothing is created or checked on disk.
 *
 * The name is `mountos_api_` plus the first 8 bytes of the SHA-256 of the mount path in hex plus
 * `.sock`. The directory is `<home>/.mountOS/sockets`. The home comes from the environment, then
 * the account database, then the temporary directory. When the path is longer than the AF_UNIX
 * limit, the socket is in `/tmp/mountos-<uid>` instead. On Windows the fallback is
 * `%LOCALAPPDATA%\mountOS\sockets`.
 */
export function socketPath(mountPath: string): string {
  if (mountPath === "") {
    throw new Error("socketPath: empty mount path");
  }
  const sum = createHash("sha256").update(mountPath, "utf8").digest("hex");
  const name = `mountos_api_${sum.slice(0, 16)}.sock`;
  const preferred = join(socketsDir(), name);
  if (Buffer.byteLength(preferred) <= maxUnixSocketPath) {
    return preferred;
  }
  return join(fallbackSocketsDir(), name);
}

function socketsDir(): string {
  return join(homeDir(), ".mountOS", "sockets");
}

function homeDir(): string {
  const fromEnv = homedir();
  if (fromEnv) {
    return fromEnv;
  }
  try {
    const h = userInfo().homedir;
    if (h) {
      return h;
    }
  } catch {
    // No account entry. Use the temporary directory.
  }
  return tmpdir();
}

function fallbackSocketsDir(): string {
  if (process.platform === "win32") {
    const base = process.env["LOCALAPPDATA"] || tmpdir();
    return join(base, "mountOS", "sockets");
  }
  return join("/tmp", `mountos-${process.getuid?.() ?? 0}`);
}
