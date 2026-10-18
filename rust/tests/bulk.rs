//! Bulk call tests against an in-process fake server on a Unix socket.

use mountos_native::frame::{self, Frame, FLAG_ERROR, FLAG_RESPONSE};
use mountos_native::wire::*;
use mountos_native::{open_flag, Client, Error, Options, Range, WriteItem};
use prost::Message;
use std::collections::HashMap;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

static SOCK_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct Shared {
  files: Mutex<HashMap<String, Vec<u8>>>,
  /// (method, item count) of each batch request.
  batches: Mutex<Vec<(u16, usize)>>,
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
    "/tmp/mn-bw-{}-{}.sock",
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

fn send(out: &Mutex<UnixStream>, f: &Frame) {
  let _ = frame::write(&mut *out.lock().unwrap(), f);
}

fn serve(s: UnixStream, sh: Arc<Shared>, max_payload: u32) {
  let mut rd = s.try_clone().unwrap();
  let out = Arc::new(Mutex::new(s));
  let mut handles: HashMap<u64, String> = HashMap::new();
  let mut next_handle = 1u64;
  while let Ok(Some(req)) = frame::read(&mut rd) {
    let f = match handle(&req, &sh, &mut handles, &mut next_handle, max_payload) {
      Ok((m, p)) => resp(&req, m, 0).with_payload(p),
      Err((c, m)) => error_frame(&req, c, m),
    };
    send(&out, &f);
  }
}

trait WithPayload {
  fn with_payload(self, p: Vec<u8>) -> Frame;
}

impl WithPayload for Frame {
  fn with_payload(mut self, p: Vec<u8>) -> Frame {
    self.payload = p;
    self
  }
}

fn resp(req: &Frame, meta: Vec<u8>, extra: u8) -> Frame {
  Frame {
    flags: FLAG_RESPONSE | extra,
    method: req.method,
    request_id: req.request_id,
    meta,
    payload: vec![],
  }
}

fn error_frame(req: &Frame, code: &str, message: &str) -> Frame {
  resp(
    req,
    WireError {
      code: code.into(),
      message: message.into(),
    }
    .encode_to_vec(),
    FLAG_ERROR,
  )
}

fn item_errors(n: usize, bad: impl Fn(usize) -> bool) -> Vec<BatchItemError> {
  (0..n)
    .filter(|&i| bad(i))
    .map(|i| BatchItemError {
      index: i as u32,
      code: "ENOENT".into(),
      message: format!("item {i}"),
    })
    .collect()
}

fn handle(req: &Frame, sh: &Shared, handles: &mut HashMap<u64, String>, next: &mut u64, max_payload: u32) -> Reply {
  let m = &req.meta[..];
  match req.method {
    method::HELLO => ok(HelloResponse {
      version: 1,
      capabilities: vec![],
      max_payload_bytes: max_payload,
      max_inflight: 1,
    }),
    method::PING => ok(Empty {}),
    method::STAT_BATCH => {
      let r = StatBatchRequest::decode(m).unwrap();
      sh.batches.lock().unwrap().push((req.method, r.paths.len()));
      if r.paths.len() > 4096 {
        return Err(("EINVAL", "too many paths"));
      }
      let files = sh.files.lock().unwrap();
      let mut infos = vec![];
      let mut errors = vec![];
      for (i, p) in r.paths.iter().enumerate() {
        match files.get(p) {
          Some(d) => infos.push(FileInfo {
            name: p.clone(),
            size: d.len() as i64,
            ..Default::default()
          }),
          None => {
            infos.push(FileInfo::default());
            errors.push(BatchItemError {
              index: i as u32,
              code: "ENOENT".into(),
              message: p.clone(),
            });
          }
        }
      }
      ok(StatBatchResponse { infos, errors })
    }
    method::OPEN => {
      let r = OpenRequest::decode(m).unwrap();
      let mut files = sh.files.lock().unwrap();
      if r.flags & open_flag::CREATE != 0 {
        files.entry(r.path.clone()).or_default();
      } else if !files.contains_key(&r.path) {
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
      ok(Empty {})
    }
    method::READ_AT => {
      let r = ReadAtRequest::decode(m).unwrap();
      let path = handles.get(&r.handle).ok_or(("EBADF", "bad handle"))?;
      if r.length > max_payload || r.offset < 0 {
        return Err(("EINVAL", "bad range"));
      }
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
      write_at(
        sh.files.lock().unwrap().get_mut(path).unwrap(),
        r.offset as usize,
        &req.payload,
      );
      ok(WriteAtResponse {
        count: req.payload.len() as u32,
      })
    }
    method::READ_BATCH => {
      let r = ReadBatchRequest::decode(m).unwrap();
      let path = handles.get(&r.handle).ok_or(("EBADF", "bad handle"))?;
      sh.batches.lock().unwrap().push((req.method, r.ranges.len()));
      let total: u64 = r.ranges.iter().map(|x| x.length as u64).sum();
      if r.ranges.len() > 4096 || total > max_payload as u64 {
        return Err(("EINVAL", "batch too large"));
      }
      let files = sh.files.lock().unwrap();
      let data = &files[path];
      let (mut counts, mut payload) = (vec![], vec![]);
      for x in &r.ranges {
        if x.offset < 0 {
          counts.push(0);
          continue;
        }
        let start = (x.offset as usize).min(data.len());
        let end = (start + x.length as usize).min(data.len());
        counts.push((end - start) as u32);
        payload.extend_from_slice(&data[start..end]);
      }
      let errors = item_errors(r.ranges.len(), |i| r.ranges[i].offset < 0);
      Ok((ReadBatchResponse { counts, errors }.encode_to_vec(), payload))
    }
    method::WRITE_BATCH => {
      let r = WriteBatchRequest::decode(m).unwrap();
      let path = handles.get(&r.handle).ok_or(("EBADF", "bad handle"))?;
      sh.batches.lock().unwrap().push((req.method, r.ranges.len()));
      let total: u64 = r.ranges.iter().map(|x| x.length as u64).sum();
      if r.ranges.len() > 4096 || total != req.payload.len() as u64 || total > max_payload as u64 {
        return Err(("EINVAL", "bad batch"));
      }
      let mut files = sh.files.lock().unwrap();
      let data = files.get_mut(path).unwrap();
      let (mut counts, mut pos) = (vec![], 0usize);
      for x in &r.ranges {
        let chunk = &req.payload[pos..pos + x.length as usize];
        pos += x.length as usize;
        if x.offset < 0 {
          counts.push(0);
          continue;
        }
        write_at(data, x.offset as usize, chunk);
        counts.push(x.length);
      }
      let errors = item_errors(r.ranges.len(), |i| r.ranges[i].offset < 0);
      ok(WriteBatchResponse { counts, errors })
    }
    method::READ_FILE => {
      let r = ReadFileRequest::decode(m).unwrap();
      let files = sh.files.lock().unwrap();
      let d = files.get(&r.path).ok_or(("ENOENT", "no such file"))?;
      let limit = if r.max_bytes == 0 {
        max_payload as usize
      } else {
        (r.max_bytes as usize).min(max_payload as usize)
      };
      let data = d[..d.len().min(limit)].to_vec();
      Ok((
        ReadFileResponse {
          count: data.len() as u32,
        }
        .encode_to_vec(),
        data,
      ))
    }
    method::WRITE_FILE => {
      let r = WriteFileRequest::decode(m).unwrap();
      if req.payload.len() > max_payload as usize {
        return Err(("EINVAL", "payload too large"));
      }
      let mut files = sh.files.lock().unwrap();
      let d = files.entry(r.path).or_default();
      if r.truncate {
        d.clear();
      }
      write_at(d, 0, &req.payload);
      ok(WriteFileResponse {
        count: req.payload.len() as u32,
      })
    }
    _ => Err(("ENOSYS", "unsupported")),
  }
}

fn write_at(data: &mut Vec<u8>, off: usize, buf: &[u8]) {
  if data.len() < off + buf.len() {
    data.resize(off + buf.len(), 0);
  }
  data[off..off + buf.len()].copy_from_slice(buf);
}

fn client(srv: &Server, pool: usize) -> Client {
  Client::connect_with(
    &srv.path,
    Options {
      pool_size: pool,
      ..Options::default()
    },
  )
  .unwrap()
}

fn pattern(n: usize) -> Vec<u8> {
  (0..n).map(|i| (i % 251) as u8).collect()
}

fn code(e: &Error) -> &str {
  e.code().unwrap_or("none")
}

#[test]
fn stat_batch_maps_failed_items() {
  let srv = start(1 << 20);
  srv.shared.files.lock().unwrap().insert("/a".into(), vec![0; 5]);
  srv.shared.files.lock().unwrap().insert("/c".into(), vec![0; 7]);
  let c = client(&srv, 1);
  let paths: Vec<String> = ["/a", "/missing", "/c"].iter().map(|s| s.to_string()).collect();
  let r = c.stat_batch(&paths).unwrap();
  assert_eq!(r.len(), 3);
  assert_eq!(r[0].as_ref().unwrap().size, 5);
  assert_eq!(code(r[1].as_ref().unwrap_err()), "ENOENT");
  assert_eq!(r[2].as_ref().unwrap().size, 7);
  assert!(c.stat_batch(&[]).unwrap().is_empty());
}

#[test]
fn stat_batch_splits_over_4096_paths() {
  let srv = start(1 << 20);
  for i in 0..9000 {
    if i % 1000 != 7 {
      srv
        .shared
        .files
        .lock()
        .unwrap()
        .insert(format!("/f{i}"), vec![0; i % 50]);
    }
  }
  let c = client(&srv, 1);
  let paths: Vec<String> = (0..9000).map(|i| format!("/f{i}")).collect();
  let r = c.stat_batch(&paths).unwrap();
  assert_eq!(r.len(), 9000);
  for (i, x) in r.iter().enumerate() {
    if i % 1000 == 7 {
      assert_eq!(code(x.as_ref().unwrap_err()), "ENOENT", "index {i}");
    } else {
      assert_eq!(x.as_ref().unwrap().size, (i % 50) as i64, "index {i}");
    }
  }
  let sizes: Vec<usize> = srv.shared.batches.lock().unwrap().iter().map(|b| b.1).collect();
  assert_eq!(sizes, vec![4096, 4096, 808]);
}

#[test]
fn batch_round_trip_with_hole() {
  let srv = start(1 << 20);
  let c = client(&srv, 1);
  let f = c.open("/b", open_flag::READ_WRITE | open_flag::CREATE, 0o644).unwrap();
  let w = f
    .write_batch(&[
      WriteItem {
        offset: 0,
        data: b"abc".to_vec(),
      },
      WriteItem {
        offset: 10,
        data: b"xyz".to_vec(),
      },
    ])
    .unwrap();
  assert_eq!(w.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<_>>(), vec![3, 3]);
  let r = f
    .read_batch(&[
      Range { offset: 0, length: 3 },
      Range { offset: 3, length: 7 },
      Range { offset: 10, length: 3 },
      Range { offset: 11, length: 10 },
      Range { offset: 100, length: 4 },
      Range { offset: -1, length: 4 },
    ])
    .unwrap();
  assert_eq!(r[0].as_ref().unwrap(), b"abc");
  assert_eq!(r[1].as_ref().unwrap(), &[0u8; 7]);
  assert_eq!(r[2].as_ref().unwrap(), b"xyz");
  assert_eq!(r[3].as_ref().unwrap(), b"yz");
  assert!(r[4].as_ref().unwrap().is_empty());
  assert_eq!(code(r[5].as_ref().unwrap_err()), "ENOENT");
  assert_eq!(f.read_at(0, 100).unwrap().len(), 13);
}

#[test]
fn write_batch_reports_failed_item() {
  let srv = start(1 << 20);
  let c = client(&srv, 1);
  let f = c.open("/w", open_flag::READ_WRITE | open_flag::CREATE, 0o644).unwrap();
  let w = f
    .write_batch(&[
      WriteItem {
        offset: 0,
        data: b"aa".to_vec(),
      },
      WriteItem {
        offset: u64::MAX,
        data: b"bb".to_vec(),
      },
      WriteItem {
        offset: 2,
        data: b"cc".to_vec(),
      },
    ])
    .unwrap();
  assert_eq!(*w[0].as_ref().unwrap(), 2);
  assert_eq!(code(w[1].as_ref().unwrap_err()), "EINVAL");
  assert_eq!(*w[2].as_ref().unwrap(), 2);
  assert_eq!(f.read_at(0, 10).unwrap(), b"aacc");
}

#[test]
fn batches_split_by_payload_size() {
  let srv = start(100);
  let c = client(&srv, 1);
  let f = c.open("/s", open_flag::READ_WRITE | open_flag::CREATE, 0o644).unwrap();
  let items: Vec<WriteItem> = (0..10)
    .map(|i| WriteItem {
      offset: i * 40,
      data: vec![i as u8 + 1; 40],
    })
    .collect();
  let w = f.write_batch(&items).unwrap();
  assert!(w.iter().all(|x| *x.as_ref().unwrap() == 40));
  // 40 byte items under a 100 byte limit: two per request.
  let wr: Vec<usize> = srv
    .shared
    .batches
    .lock()
    .unwrap()
    .iter()
    .filter(|b| b.0 == method::WRITE_BATCH)
    .map(|b| b.1)
    .collect();
  assert_eq!(wr, vec![2; 5]);
  srv.shared.batches.lock().unwrap().clear();
  let ranges: Vec<Range> = (0..10)
    .map(|i| Range {
      offset: i * 40,
      length: 40,
    })
    .collect();
  let r = f.read_batch(&ranges).unwrap();
  for (i, x) in r.iter().enumerate() {
    assert_eq!(x.as_ref().unwrap(), &vec![i as u8 + 1; 40]);
  }
  let rd: Vec<usize> = srv
    .shared
    .batches
    .lock()
    .unwrap()
    .iter()
    .filter(|b| b.0 == method::READ_BATCH)
    .map(|b| b.1)
    .collect();
  assert_eq!(rd, vec![2; 5]);
}

#[test]
fn oversize_item_falls_back_to_chunked_io() {
  let srv = start(100);
  let c = client(&srv, 1);
  let f = c.open("/o", open_flag::READ_WRITE | open_flag::CREATE, 0o644).unwrap();
  let big = pattern(250);
  let w = f
    .write_batch(&[
      WriteItem {
        offset: 0,
        data: b"hd".to_vec(),
      },
      WriteItem {
        offset: 2,
        data: big.clone(),
      },
      WriteItem {
        offset: 300,
        data: b"tl".to_vec(),
      },
    ])
    .unwrap();
  assert_eq!(
    w.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<_>>(),
    vec![2, 250, 2]
  );
  let r = f
    .read_batch(&[
      Range { offset: 0, length: 2 },
      Range { offset: 2, length: 250 },
      Range { offset: 300, length: 2 },
    ])
    .unwrap();
  assert_eq!(r[0].as_ref().unwrap(), b"hd");
  assert_eq!(r[1].as_ref().unwrap(), &big);
  assert_eq!(r[2].as_ref().unwrap(), b"tl");
}

#[test]
fn read_file_larger_than_one_payload() {
  let srv = start(100);
  let data = pattern(350);
  srv.shared.files.lock().unwrap().insert("/big".into(), data.clone());
  srv
    .shared
    .files
    .lock()
    .unwrap()
    .insert("/small".into(), data[..60].to_vec());
  srv
    .shared
    .files
    .lock()
    .unwrap()
    .insert("/exact".into(), data[..200].to_vec());
  let c = client(&srv, 1);
  assert_eq!(c.read_file("/big").unwrap(), data);
  assert_eq!(c.read_file("/small").unwrap(), &data[..60]);
  assert_eq!(c.read_file("/exact").unwrap(), &data[..200]);
  assert_eq!(code(&c.read_file("/none").unwrap_err()), "ENOENT");
}

#[test]
fn write_file_larger_than_one_payload() {
  let srv = start(100);
  let c = client(&srv, 1);
  let data = pattern(350);
  c.write_file("/w", &data, true).unwrap();
  assert_eq!(srv.shared.files.lock().unwrap()["/w"], data);
  c.write_file("/w", b"short", true).unwrap();
  assert_eq!(srv.shared.files.lock().unwrap()["/w"], b"short");
  // Without truncate the tail of a longer file stays.
  c.write_file("/w", b"AB", false).unwrap();
  assert_eq!(srv.shared.files.lock().unwrap()["/w"], b"ABort");
  let big2 = pattern(220);
  c.write_file("/w", &big2, true).unwrap();
  assert_eq!(srv.shared.files.lock().unwrap()["/w"], big2);
}
