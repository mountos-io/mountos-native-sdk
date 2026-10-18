use std::ffi::OsString;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Longest usable AF_UNIX path. macOS allows 104 bytes with the NUL, Linux 108.
const MAX_UNIX_SOCKET_PATH: usize = 103;

extern "C" {
  fn geteuid() -> u32;
}

/// Computes the native API socket path of the mount at `mount_path`. `mount_path` is the path the
/// mount was started with. The result is the same path the mount listens on. Nothing is created
/// or checked on disk.
///
/// The name is `mountos_api_` plus the first 8 bytes of the SHA-256 of the mount path in hex plus
/// `.sock`. It sits in `~/.mountOS/sockets`, or in `/tmp/mountos-<euid>` when that path is longer
/// than the AF_UNIX limit. The home directory comes from `$HOME`, then the passwd entry, then the
/// temporary directory.
pub fn socket_path(mount_path: impl AsRef<Path>) -> Result<PathBuf> {
  let mount_path = mount_path.as_ref();
  if mount_path.as_os_str().is_empty() {
    return Err(Error::Io(io::Error::new(
      io::ErrorKind::InvalidInput,
      "socket_path: empty mount path",
    )));
  }
  // Hash the symlink-resolved path, as the mount does. A path that does not exist keeps its spelling.
  let resolved = std::fs::canonicalize(mount_path).ok();
  let mount_path = resolved.as_deref().unwrap_or(mount_path);
  let name = socket_name(mount_path.as_os_str().as_bytes());
  let preferred = clean(&sockets_dir().join(&name));
  if preferred.as_os_str().len() <= MAX_UNIX_SOCKET_PATH {
    return Ok(preferred);
  }
  // SAFETY: geteuid has no arguments and cannot fail.
  let euid = unsafe { geteuid() };
  Ok(Path::new("/tmp").join(format!("mountos-{euid}")).join(name))
}

fn socket_name(mount_path: &[u8]) -> String {
  use std::fmt::Write;
  let sum = sha256(mount_path);
  let mut name = String::from("mountos_api_");
  for b in &sum[..8] {
    let _ = write!(name, "{b:02x}");
  }
  name.push_str(".sock");
  name
}

fn sockets_dir() -> PathBuf {
  let home = std::env::var_os("HOME")
    .filter(|h| !h.is_empty())
    .or_else(passwd_home)
    .map(PathBuf::from)
    .unwrap_or_else(std::env::temp_dir);
  home.join(".mountOS").join("sockets")
}

/// Home directory of the real user from /etc/passwd.
fn passwd_home() -> Option<OsString> {
  // SAFETY: getuid has no arguments and cannot fail.
  let uid = unsafe { getuid() }.to_string();
  let data = std::fs::read(Path::new("/etc/passwd")).ok()?;
  for line in data.split(|&b| b == b'\n') {
    let f: Vec<&[u8]> = line.split(|&b| b == b':').collect();
    if f.len() >= 6 && f[2] == uid.as_bytes() && !f[5].is_empty() {
      return Some(OsString::from_vec(f[5].to_vec()));
    }
  }
  None
}

extern "C" {
  fn getuid() -> u32;
}

/// Lexical clean: drops repeated separators and `.` components.
fn clean(p: &Path) -> PathBuf {
  p.components().collect()
}

fn sha256(data: &[u8]) -> [u8; 32] {
  const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
    0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
    0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
    0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2,
  ];
  let mut h: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ];
  let mut msg = data.to_vec();
  msg.push(0x80);
  while msg.len() % 64 != 56 {
    msg.push(0);
  }
  msg.extend_from_slice(&((data.len() as u64) * 8).to_be_bytes());
  for block in msg.chunks_exact(64) {
    let mut w = [0u32; 64];
    for i in 0..16 {
      w[i] = u32::from_be_bytes(block[i * 4..i * 4 + 4].try_into().unwrap());
    }
    for i in 16..64 {
      let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
      let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
      w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
    }
    let mut v = h;
    for i in 0..64 {
      let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
      let ch = (v[4] & v[5]) ^ (!v[4] & v[6]);
      let t1 = v[7]
        .wrapping_add(s1)
        .wrapping_add(ch)
        .wrapping_add(K[i])
        .wrapping_add(w[i]);
      let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
      let maj = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
      let t2 = s0.wrapping_add(maj);
      v = [
        t1.wrapping_add(t2),
        v[0],
        v[1],
        v[2],
        v[3].wrapping_add(t1),
        v[4],
        v[5],
        v[6],
      ];
    }
    for i in 0..8 {
      h[i] = h[i].wrapping_add(v[i]);
    }
  }
  let mut out = [0u8; 32];
  for (i, x) in h.iter().enumerate() {
    out[i * 4..i * 4 + 4].copy_from_slice(&x.to_be_bytes());
  }
  out
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Mutex;

  static ENV: Mutex<()> = Mutex::new(());

  fn with_home<T>(home: &str, f: impl FnOnce() -> T) -> T {
    let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let saved = std::env::var_os("HOME");
    std::env::set_var("HOME", home);
    let out = f();
    match saved {
      Some(v) => std::env::set_var("HOME", v),
      None => std::env::remove_var("HOME"),
    }
    out
  }

  fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
  }

  #[test]
  fn sha256_known_vectors() {
    assert_eq!(
      hex(&sha256(b"abc")),
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
      hex(&sha256(b"")),
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    let long = vec![b'a'; 1000];
    assert_eq!(
      hex(&sha256(&long)),
      "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"
    );
  }

  #[test]
  fn golden_names() {
    let cases = [
      ("/mnt/vol", "mountos_api_9bbffa0eba37045f.sock"),
      ("/Volumes/data one", "mountos_api_87774d7d94a7fe60.sock"),
      ("/mnt/\u{fc}/x", "mountos_api_bc8db74b980a9567.sock"),
    ];
    for (mount, name) in cases {
      let got = with_home("/home/u", || socket_path(mount)).unwrap();
      assert_eq!(got, Path::new("/home/u/.mountOS/sockets").join(name), "{mount}");
    }
  }

  #[test]
  fn home_is_cleaned() {
    let got = with_home("/home//u/", || socket_path("/mnt/vol")).unwrap();
    assert_eq!(
      got,
      Path::new("/home/u/.mountOS/sockets/mountos_api_9bbffa0eba37045f.sock")
    );
  }

  #[test]
  fn long_home_falls_back_to_tmp() {
    let home = format!("/home/{}", "x".repeat(80));
    let got = with_home(&home, || socket_path("/mnt/vol")).unwrap();
    let euid = unsafe { geteuid() };
    assert_eq!(
      got,
      PathBuf::from(format!("/tmp/mountos-{euid}/mountos_api_9bbffa0eba37045f.sock"))
    );
  }

  #[test]
  fn limit_is_inclusive_at_103() {
    let name_len = "mountos_api_9bbffa0eba37045f.sock".len();
    let suffix = "/.mountOS/sockets/".len();
    let fit = format!("/{}", "h".repeat(MAX_UNIX_SOCKET_PATH - name_len - suffix - 1));
    let got = with_home(&fit, || socket_path("/mnt/vol")).unwrap();
    assert_eq!(got.as_os_str().len(), MAX_UNIX_SOCKET_PATH);
    assert!(got.starts_with(&fit));
    let over = format!("{fit}h");
    let got = with_home(&over, || socket_path("/mnt/vol")).unwrap();
    assert!(got.starts_with("/tmp"));
  }

  #[test]
  fn symlink_resolves_before_hashing() {
    let root = std::env::temp_dir().join(format!("mos-sock-{}", std::process::id()));
    let target = root.join("target");
    let link = root.join("link");
    std::fs::create_dir_all(&target).unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let want = with_home("/home/u", || socket_path(&target)).unwrap();
    let got = with_home("/home/u", || socket_path(&link)).unwrap();
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(got, want);
  }

  #[test]
  fn missing_path_keeps_spelling() {
    let missing = "/nonexistent-mountos-sock/vol";
    let got = with_home("/home/u", || socket_path(missing)).unwrap();
    let name = socket_name(missing.as_bytes());
    assert_eq!(got, Path::new("/home/u/.mountOS/sockets").join(name));
  }

  #[test]
  fn empty_mount_path_is_rejected() {
    assert!(socket_path("").is_err());
  }
}
