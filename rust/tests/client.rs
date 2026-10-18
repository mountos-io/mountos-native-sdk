//! Integration tests against an in-process fake server on a Unix socket.

use mountos_native::frame::{self, Frame, FLAG_ERROR, FLAG_RESPONSE};
use mountos_native::wire::*;
use mountos_native::{open_flag, Client, Error, Options};
use prost::Message;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static SOCK_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct Shared {
  files: Mutex<HashMap<String, Vec<u8>>>,
  write_sizes: Mutex<Vec<usize>>,
  inflight: AtomicUsize,
  max_inflight: AtomicUsize,
  read_delay_ms: AtomicUsize,
  hellos: AtomicUsize,
  closes: AtomicUsize,
  links: Mutex<HashMap<String, String>>,
  times: Mutex<Vec<UtimensRequest>>,
  names: Mutex<Vec<String>>,
  dir_closes: AtomicUsize,
}

struct Server {
  path: PathBuf,
  shared: Arc<Shared>,
}

impl Drop for Server {
  fn drop(&mut self) {
    let _ = std::fs::remove_file(&self.path);
  }
}

type Reply = Result<(Vec<u8>, Vec<u8>), (&'static str, &'static str)>;

fn ok(m: impl Message) -> Reply {
  Ok((m.encode_to_vec(), vec![]))
}

fn start(max_payload: u32) -> Server {
  let path = PathBuf::from(format!(
    "/tmp/mn-test-{}-{}.sock",
    std::process::id(),
    SOCK_SEQ.fetch_add(1, SeqCst)
  ));
  let _ = std::fs::remove_file(&path);
  let l = UnixListener::bind(&path).unwrap();
  let shared = Arc::new(Shared::default());
  let sh = shared.clone();
  std::thread::spawn(move || {
    for s in l.incoming() {
      let (s, sh) = (s.unwrap(), sh.clone());
      std::thread::spawn(move || serve(s, sh, max_payload));
    }
  });
  Server { path, shared }
}

fn serve(mut s: UnixStream, sh: Arc<Shared>, max_payload: u32) {
  let mut handles: HashMap<u64, String> = HashMap::new();
  let mut next_handle = 1u64;
  let mut first = true;
  while let Ok(Some(req)) = frame::read(&mut s) {
    if first && req.method != method::HELLO {
      return;
    }
    first = false;
    let reply = handle(&req, &sh, &mut handles, &mut next_handle, max_payload);
    let (flags, meta, payload) = match reply {
      Ok((m, p)) => (FLAG_RESPONSE, m, p),
      Err((code, message)) => (
        FLAG_RESPONSE | FLAG_ERROR,
        WireError {
          code: code.into(),
          message: message.into(),
        }
        .encode_to_vec(),
        vec![],
      ),
    };
    let f = Frame::new(flags, req.method, req.request_id, meta, payload);
    if frame::write(&mut s, &f).is_err() {
      return;
    }
  }
}

fn handle(req: &Frame, sh: &Shared, handles: &mut HashMap<u64, String>, next: &mut u64, max_payload: u32) -> Reply {
  let m = &req.meta[..];
  match req.method {
    method::HELLO => {
      sh.hellos.fetch_add(1, SeqCst);
      ok(HelloResponse {
        version: 1,
        capabilities: vec![],
        max_payload_bytes: max_payload,
        max_inflight: 1,
      })
    }
    method::PING => ok(Empty {}),
    method::STAT => {
      let r = StatRequest::decode(m).unwrap();
      let files = sh.files.lock().unwrap();
      let f = files.get(&r.path).ok_or(("ENOENT", "no such file"))?;
      ok(StatResponse {
        info: Some(FileInfo {
          name: r.path.clone(),
          size: f.len() as i64,
          mode: 0o644,
          ..Default::default()
        }),
      })
    }
    method::OPEN => {
      let r = OpenRequest::decode(m).unwrap();
      if r.flags & 3 == 3 {
        return Err(("EINVAL", "bad access mode"));
      }
      let mut files = sh.files.lock().unwrap();
      let exists = files.contains_key(&r.path);
      if r.flags & open_flag::CREATE != 0 {
        if exists && r.flags & open_flag::EXCL != 0 {
          return Err(("EEXIST", "exists"));
        }
        files.entry(r.path.clone()).or_default();
      } else if !exists {
        return Err(("ENOENT", "no such file"));
      }
      if r.flags & open_flag::TRUNC != 0 {
        files.get_mut(&r.path).unwrap().clear();
      }
      let h = *next;
      *next += 1;
      handles.insert(h, r.path);
      ok(OpenResponse { handle: h })
    }
    method::CLOSE => {
      let r = CloseRequest::decode(m).unwrap();
      handles.remove(&r.handle).ok_or(("EBADF", "bad handle"))?;
      sh.closes.fetch_add(1, SeqCst);
      ok(Empty {})
    }
    method::FSTAT => {
      let r = FstatRequest::decode(m).unwrap();
      let path = handles.get(&r.handle).ok_or(("EBADF", "bad handle"))?;
      let size = sh.files.lock().unwrap()[path].len() as i64;
      ok(StatResponse {
        info: Some(FileInfo {
          size,
          ..Default::default()
        }),
      })
    }
    method::READ_AT => {
      let r = ReadAtRequest::decode(m).unwrap();
      let path = handles.get(&r.handle).ok_or(("EBADF", "bad handle"))?;
      if r.length > max_payload || r.offset < 0 {
        return Err(("EINVAL", "bad range"));
      }
      let now = sh.inflight.fetch_add(1, SeqCst) + 1;
      sh.max_inflight.fetch_max(now, SeqCst);
      let delay = sh.read_delay_ms.load(SeqCst) as u64;
      if delay > 0 {
        std::thread::sleep(Duration::from_millis(delay));
      }
      sh.inflight.fetch_sub(1, SeqCst);
      let files = sh.files.lock().unwrap();
      let data = &files[path];
      let start = (r.offset as usize).min(data.len());
      let end = (start + r.length as usize).min(data.len());
      Ok((
        ReadAtResponse {
          count: (end - start) as u32,
        }
        .encode_to_vec(),
        data[start..end].to_vec(),
      ))
    }
    method::WRITE_AT => {
      let r = WriteAtRequest::decode(m).unwrap();
      let path = handles.get(&r.handle).ok_or(("EBADF", "bad handle"))?;
      if req.payload.len() > max_payload as usize || r.offset < 0 {
        return Err(("EINVAL", "bad write"));
      }
      sh.write_sizes.lock().unwrap().push(req.payload.len());
      let mut files = sh.files.lock().unwrap();
      let data = files.get_mut(path).unwrap();
      let end = r.offset as usize + req.payload.len();
      if data.len() < end {
        data.resize(end, 0);
      }
      data[r.offset as usize..end].copy_from_slice(&req.payload);
      ok(WriteAtResponse {
        count: req.payload.len() as u32,
      })
    }
    method::LSTAT => {
      let r = LstatRequest::decode(m).unwrap();
      let is_link = sh.links.lock().unwrap().contains_key(&r.path);
      let files = sh.files.lock().unwrap();
      if !is_link && !files.contains_key(&r.path) {
        return Err(("ENOENT", "no such file"));
      }
      ok(StatResponse {
        info: Some(FileInfo {
          name: r.path.clone(),
          r#type: if is_link { FileType::Symlink } else { FileType::Regular } as i32,
          ..Default::default()
        }),
      })
    }
    method::SYMLINK => {
      let r = SymlinkRequest::decode(m).unwrap();
      let mut links = sh.links.lock().unwrap();
      if links.contains_key(&r.link_path) {
        return Err(("EEXIST", "exists"));
      }
      links.insert(r.link_path, r.target);
      ok(Empty {})
    }
    method::READLINK => {
      let r = ReadlinkRequest::decode(m).unwrap();
      let links = sh.links.lock().unwrap();
      let target = links.get(&r.path).ok_or(("EINVAL", "not a symlink"))?;
      ok(ReadlinkResponse { target: target.clone() })
    }
    method::LINK => {
      let r = LinkRequest::decode(m).unwrap();
      let mut files = sh.files.lock().unwrap();
      let data = files.get(&r.existing).ok_or(("ENOENT", "no such file"))?.clone();
      files.insert(r.new_path, data);
      ok(Empty {})
    }
    method::UTIMENS => {
      sh.times.lock().unwrap().push(UtimensRequest::decode(m).unwrap());
      ok(Empty {})
    }
    method::STATFS => {
      let r = StatfsRequest::decode(m).unwrap();
      if r.path != "/" {
        return Err(("ENOENT", "no such path"));
      }
      ok(StatfsResponse {
        block_size: 4096,
        blocks: 1000,
        blocks_free: 600,
        blocks_avail: 500,
        files: 100,
        files_free: 40,
        name_max: 255,
      })
    }
    method::OPENDIR => {
      let r = OpendirRequest::decode(m).unwrap();
      if r.path != "/d" {
        return Err(("ENOENT", "no such directory"));
      }
      let h = *next;
      *next += 1;
      // Directory handles store the next entry index after a marker prefix.
      handles.insert(h, "dir:0".into());
      ok(OpendirResponse { handle: h })
    }
    method::READDIR => {
      let r = ReaddirRequest::decode(m).unwrap();
      let pos: usize = handles
        .get(&r.handle)
        .and_then(|v| v.strip_prefix("dir:"))
        .ok_or(("EBADF", "bad handle"))?
        .parse()
        .unwrap();
      let names = sh.names.lock().unwrap();
      let end = (pos + r.max_entries as usize).min(names.len());
      let entries = names[pos..end]
        .iter()
        .map(|n| DirEntry {
          name: n.clone(),
          ..Default::default()
        })
        .collect();
      handles.insert(r.handle, format!("dir:{end}"));
      ok(ReaddirResponse {
        entries,
        eof: end == names.len(),
      })
    }
    method::CLOSEDIR => {
      let r = ClosedirRequest::decode(m).unwrap();
      handles.remove(&r.handle).ok_or(("EBADF", "bad handle"))?;
      sh.dir_closes.fetch_add(1, SeqCst);
      ok(Empty {})
    }
    method::LS => {
      let r = LsRequest::decode(m).unwrap();
      let pos: usize = if r.cursor.is_empty() {
        0
      } else {
        r.cursor.parse().unwrap()
      };
      let names = sh.names.lock().unwrap();
      let end = (pos + r.max_entries as usize).min(names.len());
      let entries = names[pos..end]
        .iter()
        .map(|n| DirEntry {
          name: n.clone(),
          info: Some(FileInfo {
            name: n.clone(),
            size: n.len() as i64,
            ..Default::default()
          }),
          ..Default::default()
        })
        .collect();
      ok(LsResponse {
        entries,
        next_cursor: if end < names.len() {
          end.to_string()
        } else {
          String::new()
        },
      })
    }
    method::REMOVE_ALL => {
      let r = RemoveAllRequest::decode(m).unwrap();
      let prefix = format!("{}/", r.path);
      let mut files = sh.files.lock().unwrap();
      let before = files.len();
      files.retain(|k, _| *k != r.path && !k.starts_with(&prefix));
      ok(RemoveAllResponse {
        removed: (before - files.len()) as u64,
      })
    }
    _ => Err(("ENOSYS", "not implemented")),
  }
}

fn client(srv: &Server, pool: usize) -> Client {
  Client::connect_with(&srv.path, Options::default().with_pool_size(pool)).unwrap()
}

const RW_CREATE: u32 = open_flag::READ_WRITE | open_flag::CREATE;

#[test]
fn handshake_and_ping() {
  let srv = start(8 << 20);
  let c = Client::connect(&srv.path).unwrap();
  assert_eq!(srv.shared.hellos.load(SeqCst), 1);
  c.ping().unwrap();
  c.ping().unwrap();
}

#[test]
fn connect_fails_on_missing_socket() {
  assert!(matches!(
    Client::connect("/tmp/mn-test-nonexistent.sock"),
    Err(Error::Io(_))
  ));
}

#[test]
fn stat_and_error_mapping() {
  let srv = start(8 << 20);
  srv.shared.files.lock().unwrap().insert("/a".into(), vec![1; 7]);
  let c = client(&srv, 2);
  assert_eq!(c.stat("/a").unwrap().size, 7);
  let e = c.stat("/missing").unwrap_err();
  assert_eq!(e.code(), Some("ENOENT"));
  assert_eq!(e.to_string(), "ENOENT: no such file");
  assert_eq!(
    c.open("/missing", open_flag::READ_ONLY, 0).err().unwrap().code(),
    Some("ENOENT")
  );
  assert_eq!(c.open("/a", 3, 0).err().unwrap().code(), Some("EINVAL"));
  assert_eq!(
    c.open("/a", RW_CREATE | open_flag::EXCL, 0o644).err().unwrap().code(),
    Some("EEXIST")
  );
  assert_eq!(c.mkdir("/d", 0o755).unwrap_err().code(), Some("ENOSYS"));
  let io: std::io::Error = c.stat("/missing").unwrap_err().into();
  assert_eq!(io.kind(), std::io::ErrorKind::NotFound);
  // A server error keeps the connection usable.
  c.ping().unwrap();
}

#[test]
fn sparse_write_reads_zeros_in_hole() {
  let srv = start(8 << 20);
  let c = client(&srv, 2);
  let f = c.open("/s", RW_CREATE, 0o644).unwrap();
  assert_eq!(f.write_at(10, b"abc").unwrap(), 3);
  let got = f.read_at(0, 13).unwrap();
  assert_eq!(&got[..10], &[0u8; 10]);
  assert_eq!(&got[10..], b"abc");
  f.close().unwrap();
}

#[test]
fn read_eof_semantics() {
  let srv = start(8 << 20);
  srv.shared.files.lock().unwrap().insert("/e".into(), b"hello".to_vec());
  let c = client(&srv, 2);
  let f = c.open("/e", open_flag::READ_ONLY, 0).unwrap();
  assert_eq!(f.read_at(0, 100).unwrap(), b"hello");
  assert_eq!(f.read_at(3, 10).unwrap(), b"lo");
  assert!(f.read_at(5, 10).unwrap().is_empty());
  assert!(f.read_at(1000, 10).unwrap().is_empty());
  let mut all = Vec::new();
  (&f).read_to_end(&mut all).unwrap();
  assert_eq!(all, b"hello");
}

#[test]
fn write_splits_over_payload_limit() {
  let srv = start(1024);
  let c = client(&srv, 2);
  let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
  let f = c.open("/big", RW_CREATE, 0o644).unwrap();
  assert_eq!(f.write_at(0, &data).unwrap(), 5000);
  let sizes = srv.shared.write_sizes.lock().unwrap().clone();
  assert_eq!(sizes, vec![1024, 1024, 1024, 1024, 904]);
  assert_eq!(f.read_at(0, 5000).unwrap(), data);
  assert_eq!(f.read_at(4990, 100).unwrap(), &data[4990..]);
}

#[test]
fn handles_are_per_connection() {
  let srv = start(8 << 20);
  srv.shared.files.lock().unwrap().insert("/a".into(), b"AAAA".to_vec());
  srv.shared.files.lock().unwrap().insert("/b".into(), b"BBBB".to_vec());
  let c = client(&srv, 2);
  // Both opens get handle 1 on their own connection, so each file must stay on its own.
  let a = c.open("/a", open_flag::READ_ONLY, 0).unwrap();
  let b = c.open("/b", open_flag::READ_ONLY, 0).unwrap();
  assert_eq!(a.read_at(0, 4).unwrap(), b"AAAA");
  assert_eq!(b.read_at(0, 4).unwrap(), b"BBBB");
  a.close().unwrap();
  assert_eq!(b.read_at(0, 4).unwrap(), b"BBBB");
  assert_eq!(srv.shared.hellos.load(SeqCst), 2);
}

#[test]
fn parallel_reads_on_separate_connections() {
  let srv = start(8 << 20);
  srv.shared.files.lock().unwrap().insert("/a".into(), b"AAAA".to_vec());
  srv.shared.files.lock().unwrap().insert("/b".into(), b"BBBB".to_vec());
  srv.shared.read_delay_ms.store(200, SeqCst);
  let c = client(&srv, 4);
  let a = c.open("/a", open_flag::READ_ONLY, 0).unwrap();
  let b = c.open("/b", open_flag::READ_ONLY, 0).unwrap();
  std::thread::scope(|s| {
    let ha = s.spawn(|| a.read_at(0, 4).unwrap());
    let hb = s.spawn(|| b.read_at(0, 4).unwrap());
    assert_eq!(ha.join().unwrap(), b"AAAA");
    assert_eq!(hb.join().unwrap(), b"BBBB");
  });
  assert_eq!(srv.shared.max_inflight.load(SeqCst), 2);
}

#[test]
fn same_file_calls_serialize() {
  let srv = start(8 << 20);
  srv.shared.files.lock().unwrap().insert("/a".into(), b"AAAA".to_vec());
  srv.shared.read_delay_ms.store(50, SeqCst);
  let c = client(&srv, 4);
  let a = c.open("/a", open_flag::READ_ONLY, 0).unwrap();
  std::thread::scope(|s| {
    for _ in 0..3 {
      s.spawn(|| assert_eq!(a.read_at(0, 4).unwrap(), b"AAAA"));
    }
  });
  assert_eq!(srv.shared.max_inflight.load(SeqCst), 1);
}

#[test]
fn double_close_and_use_after_close_fail() {
  let srv = start(8 << 20);
  let c = client(&srv, 2);
  let f = c.open("/x", RW_CREATE, 0o644).unwrap();
  f.close().unwrap();
  assert_eq!(f.close().unwrap_err().code(), Some("EBADF"));
  assert_eq!(f.read_at(0, 1).unwrap_err().code(), Some("EBADF"));
  assert_eq!(f.write_at(0, b"x").unwrap_err().code(), Some("EBADF"));
}

#[test]
fn drop_closes_handle() {
  let srv = start(8 << 20);
  let c = client(&srv, 1);
  drop(c.open("/x", RW_CREATE, 0o644).unwrap());
  assert_eq!(srv.shared.closes.load(SeqCst), 1);
  let f = c.open("/x", open_flag::READ_ONLY, 0).unwrap();
  f.close().unwrap();
  assert_eq!(srv.shared.closes.load(SeqCst), 2);
}

#[test]
fn io_traits_use_file_cursor() {
  let srv = start(8 << 20);
  let c = client(&srv, 2);
  let mut f = c.open("/io", RW_CREATE, 0o644).unwrap();
  f.write_all(b"hello ").unwrap();
  f.write_all(b"world").unwrap();
  assert_eq!(f.stream_position().unwrap(), 11);
  assert_eq!(f.seek(SeekFrom::Start(6)).unwrap(), 6);
  let mut s = String::new();
  f.read_to_string(&mut s).unwrap();
  assert_eq!(s, "world");
  assert_eq!(f.seek(SeekFrom::End(-5)).unwrap(), 6);
  assert_eq!(f.seek(SeekFrom::End(0)).unwrap(), 11);
  assert!(f.seek(SeekFrom::Current(-100)).is_err());
  assert_eq!(f.stat().unwrap().size, 11);
  // The explicit-offset call does not move the cursor.
  f.seek(SeekFrom::Start(0)).unwrap();
  assert_eq!(f.read_at(6, 5).unwrap(), b"world");
  let mut b = [0u8; 5];
  f.read_exact(&mut b).unwrap();
  assert_eq!(&b, b"hello");
}

#[test]
fn lstat_reports_symlink_type() {
  let srv = start(8 << 20);
  srv.shared.files.lock().unwrap().insert("/a".into(), vec![1; 3]);
  let c = client(&srv, 2);
  c.symlink("/a", "/l").unwrap();
  assert_eq!(c.lstat("/l").unwrap().r#type, FileType::Symlink as i32);
  assert_eq!(c.lstat("/a").unwrap().r#type, FileType::Regular as i32);
  assert_eq!(c.lstat("/missing").unwrap_err().code(), Some("ENOENT"));
}

#[test]
fn readlink_after_symlink() {
  let srv = start(8 << 20);
  let c = client(&srv, 2);
  c.symlink("../target", "/l").unwrap();
  assert_eq!(c.readlink("/l").unwrap(), "../target");
  assert_eq!(c.readlink("/nolink").unwrap_err().code(), Some("EINVAL"));
  assert_eq!(c.symlink("x", "/l").unwrap_err().code(), Some("EEXIST"));
}

#[test]
fn link_creates_second_name() {
  let srv = start(8 << 20);
  srv.shared.files.lock().unwrap().insert("/a".into(), b"data".to_vec());
  let c = client(&srv, 2);
  c.link("/a", "/b").unwrap();
  assert_eq!(c.stat("/b").unwrap().size, 4);
  assert_eq!(c.link("/missing", "/c").unwrap_err().code(), Some("ENOENT"));
}

#[test]
fn utimens_sets_flags_only_for_some() {
  let srv = start(8 << 20);
  let c = client(&srv, 2);
  c.utimens("/a", Some(11), Some(22)).unwrap();
  c.utimens("/a", None, Some(33)).unwrap();
  c.utimens("/a", Some(44), None).unwrap();
  c.utimens("/a", None, None).unwrap();
  let t = srv.shared.times.lock().unwrap();
  let got: Vec<_> = t
    .iter()
    .map(|r| (r.atime_ns, r.mtime_ns, r.set_atime, r.set_mtime))
    .collect();
  assert_eq!(
    got,
    vec![
      (11, 22, true, true),
      (0, 33, false, true),
      (44, 0, true, false),
      (0, 0, false, false),
    ]
  );
}

#[test]
fn statfs_returns_all_fields() {
  let srv = start(8 << 20);
  let c = client(&srv, 2);
  let s = c.statfs("/").unwrap();
  assert_eq!(
    (s.block_size, s.blocks, s.blocks_free, s.blocks_avail),
    (4096, 1000, 600, 500)
  );
  assert_eq!((s.files, s.files_free, s.name_max), (100, 40, 255));
  assert_eq!(c.statfs("/nope").unwrap_err().code(), Some("ENOENT"));
}

fn set_names(srv: &Server, n: usize) {
  *srv.shared.names.lock().unwrap() = (0..n).map(|i| format!("f{i}")).collect();
}

#[test]
fn opendir_readdir_pages_then_close() {
  let srv = start(8 << 20);
  set_names(&srv, 5);
  let c = client(&srv, 2);
  let d = c.opendir("/d").unwrap();
  let (a, eof) = d.readdir(2).unwrap();
  assert_eq!(
    (a.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(), eof),
    (vec!["f0", "f1"], false)
  );
  let (b, eof) = d.readdir(2).unwrap();
  assert_eq!((b.len(), eof), (2, false));
  let (e, eof) = d.readdir(2).unwrap();
  assert_eq!((e[0].name.as_str(), e.len(), eof), ("f4", 1, true));
  d.close().unwrap();
  assert_eq!(srv.shared.dir_closes.load(SeqCst), 1);
  assert_eq!(d.close().unwrap_err().code(), Some("EBADF"));
  assert_eq!(d.readdir(1).unwrap_err().code(), Some("EBADF"));
  assert_eq!(c.opendir("/missing").err().unwrap().code(), Some("ENOENT"));
}

#[test]
fn dir_drop_closes_handle() {
  let srv = start(8 << 20);
  let c = client(&srv, 1);
  drop(c.opendir("/d").unwrap());
  assert_eq!(srv.shared.dir_closes.load(SeqCst), 1);
}

#[test]
fn dir_is_bound_to_its_connection() {
  let srv = start(8 << 20);
  set_names(&srv, 3);
  let c = client(&srv, 2);
  let d1 = c.opendir("/d").unwrap();
  let d2 = c.opendir("/d").unwrap();
  // Both handles are 1 on their own connection. Paging one must not move the other.
  assert_eq!(d1.readdir(2).unwrap().0.len(), 2);
  assert_eq!(d2.readdir(1).unwrap().0[0].name, "f0");
  assert_eq!(d1.readdir(2).unwrap().0[0].name, "f2");
}

#[test]
fn ls_pages_with_cursor() {
  let srv = start(8 << 20);
  set_names(&srv, 5);
  let c = client(&srv, 2);
  let (a, cur) = c.ls("/d", 2, "").unwrap();
  assert_eq!(a.len(), 2);
  assert_eq!(a[0].info.as_ref().unwrap().size, 2);
  assert_eq!(cur, "2");
  let (b, cur) = c.ls("/d", 2, &cur).unwrap();
  assert_eq!(b[0].name, "f2");
  assert_eq!(cur, "4");
  let (e, cur) = c.ls("/d", 2, &cur).unwrap();
  assert_eq!((e.len(), cur.as_str()), (1, ""));
}

#[test]
fn remove_all_counts_removed() {
  let srv = start(8 << 20);
  {
    let mut f = srv.shared.files.lock().unwrap();
    for p in ["/t", "/t/a", "/t/b/c", "/tx"] {
      f.insert(p.into(), vec![]);
    }
  }
  let c = client(&srv, 2);
  assert_eq!(c.remove_all("/t").unwrap(), 3);
  assert_eq!(c.remove_all("/t").unwrap(), 0);
  assert!(c.stat("/tx").is_ok());
}

#[test]
fn unsolicited_reply_closes_idle_connection() {
  let path = PathBuf::from(format!(
    "/tmp/mn-test-{}-{}.sock",
    std::process::id(),
    SOCK_SEQ.fetch_add(1, SeqCst)
  ));
  let listener = UnixListener::bind(&path).unwrap();
  let (ready_tx, ready_rx) = std::sync::mpsc::channel();
  let (send_tx, send_rx) = std::sync::mpsc::channel();
  let peer = std::thread::spawn(move || {
    let (mut s, _) = listener.accept().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let req = frame::read(&mut s).unwrap().unwrap();
    let reply = Frame::new(
      FLAG_RESPONSE,
      req.method,
      req.request_id,
      HelloResponse {
        version: 1,
        max_payload_bytes: 1024,
        max_inflight: 1,
        ..Default::default()
      }
      .encode_to_vec(),
      vec![],
    );
    frame::write(&mut s, &reply).unwrap();
    ready_tx.send(()).unwrap();
    send_rx.recv().unwrap();
    // No request is pending; even a duplicate of the valid Hello must be rejected.
    frame::write(&mut s, &reply).unwrap();
    let mut b = [0];
    assert_eq!(s.read(&mut b).unwrap(), 0);
  });
  let c = Client::connect(&path).unwrap();
  ready_rx.recv().unwrap();
  send_tx.send(()).unwrap();
  peer.join().unwrap();
  // Joining the reader during shutdown must not block on delivering an error.
  c.close();
  std::fs::remove_file(path).unwrap();
}

#[test]
fn mismatched_handshake_reply_is_rejected() {
  for wrong_method in [false, true] {
    let path = PathBuf::from(format!(
      "/tmp/mn-test-{}-{}.sock",
      std::process::id(),
      SOCK_SEQ.fetch_add(1, SeqCst)
    ));
    let listener = UnixListener::bind(&path).unwrap();
    let peer = std::thread::spawn(move || {
      let (mut s, _) = listener.accept().unwrap();
      s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
      let req = frame::read(&mut s).unwrap().unwrap();
      frame::write(
        &mut s,
        &Frame::new(
          FLAG_RESPONSE,
          if wrong_method { method::WATCH } else { req.method },
          req.request_id + u64::from(!wrong_method),
          WatchResponse { watch_id: 7 }.encode_to_vec(),
          vec![],
        ),
      )
      .unwrap();
      let mut b = [0];
      assert_eq!(s.read(&mut b).unwrap(), 0);
    });
    assert!(matches!(Client::connect(&path), Err(Error::Protocol(_))));
    peer.join().unwrap();
    std::fs::remove_file(path).unwrap();
  }
}
