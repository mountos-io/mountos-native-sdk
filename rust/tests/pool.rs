//! Pool scheduling, call timeout, local size limits, capability checks, parallel bulk calls and
//! the walk helper, against an in-process fake server whose behavior each test sets.

use mountos_native::frame::{self, Frame, FLAG_ERROR, FLAG_RESPONSE};
use mountos_native::wire::*;
use mountos_native::{open_flag, Client, Error, FileType, Options, WriteItem};
use prost::Message;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

static SOCK_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct Shared {
  capabilities: Mutex<Vec<String>>,
  /// Methods received, in arrival order, with the connection index.
  log: Mutex<Vec<(usize, u16)>>,
  hellos: AtomicUsize,
  inflight: AtomicUsize,
  max_inflight: AtomicUsize,
  conns: AtomicUsize,
  /// Server-side sleep for a Stat of this path, milliseconds.
  stat_delay: Mutex<HashMap<String, u64>>,
  /// Sleep before every ReadAt and StatBatch reply.
  slow_ms: AtomicUsize,
  /// A WriteAt from this call number on fails with ENOSPC. Zero means never.
  fail_write_from: AtomicUsize,
  write_calls: AtomicUsize,
  tree: Mutex<HashMap<String, Vec<(String, bool)>>>,
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

fn start(max_payload: u32) -> Server {
  let path = PathBuf::from(format!(
    "/tmp/mn-pool-{}-{}.sock",
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
      let idx = sh.conns.fetch_add(1, SeqCst);
      std::thread::spawn(move || serve(s, sh, idx, max_payload));
    }
  });
  Server { path, shared }
}

fn err(req: &Frame, code: &str) -> Frame {
  Frame {
    flags: FLAG_RESPONSE | FLAG_ERROR,
    method: req.method,
    request_id: req.request_id,
    meta: WireError {
      code: code.into(),
      message: "x".into(),
    }
    .encode_to_vec(),
    payload: vec![],
  }
}

fn serve(mut s: UnixStream, sh: Arc<Shared>, idx: usize, max_payload: u32) {
  while let Ok(Some(req)) = frame::read(&mut s) {
    sh.log.lock().unwrap().push((idx, req.method));
    let cur = sh.inflight.fetch_add(1, SeqCst) + 1;
    sh.max_inflight.fetch_max(cur, SeqCst);
    let slow = sh.slow_ms.load(SeqCst) as u64;
    let mut payload = vec![];
    let meta = req.meta.clone();
    let out: Result<Vec<u8>, &str> = match req.method {
      method::HELLO => {
        sh.hellos.fetch_add(1, SeqCst);
        Ok(
          HelloResponse {
            version: 1,
            capabilities: sh.capabilities.lock().unwrap().clone(),
            max_payload_bytes: max_payload,
            max_inflight: 1,
          }
          .encode_to_vec(),
        )
      }
      method::STAT => {
        let r = StatRequest::decode(&meta[..]).unwrap();
        let d = sh.stat_delay.lock().unwrap().get(&r.path).copied().unwrap_or(0);
        std::thread::sleep(Duration::from_millis(d));
        if d >= 1000 {
          // A reply that comes after the client gave up.
          std::thread::sleep(Duration::from_millis(300));
        }
        Ok(
          StatResponse {
            info: Some(FileInfo {
              name: r.path,
              ..Default::default()
            }),
          }
          .encode_to_vec(),
        )
      }
      method::PING => Ok(Empty {}.encode_to_vec()),
      method::WRITE_BATCH => {
        let r = WriteBatchRequest::decode(&meta[..]).unwrap();
        let counts = r.ranges.iter().map(|g| g.length).collect();
        Ok(WriteBatchResponse { counts, errors: vec![] }.encode_to_vec())
      }
      method::OPEN => Ok(OpenResponse { handle: 1 }.encode_to_vec()),
      method::READ_AT => {
        std::thread::sleep(Duration::from_millis(slow));
        let r = ReadAtRequest::decode(&meta[..]).unwrap();
        payload = vec![7u8; r.length as usize];
        Ok(ReadAtResponse { count: r.length }.encode_to_vec())
      }
      method::WRITE_AT => {
        let n = sh.write_calls.fetch_add(1, SeqCst) + 1;
        let from = sh.fail_write_from.load(SeqCst);
        if from != 0 && n >= from {
          Err("ENOSPC")
        } else {
          Ok(
            WriteAtResponse {
              count: req.payload.len() as u32,
            }
            .encode_to_vec(),
          )
        }
      }
      method::STAT_BATCH => {
        std::thread::sleep(Duration::from_millis(slow));
        let r = StatBatchRequest::decode(&meta[..]).unwrap();
        let infos = r
          .paths
          .iter()
          .map(|p| FileInfo {
            name: p.clone(),
            size: p[1..].parse().unwrap(),
            ..Default::default()
          })
          .collect();
        Ok(StatBatchResponse { infos, errors: vec![] }.encode_to_vec())
      }
      method::LS => {
        let r = LsRequest::decode(&meta[..]).unwrap();
        let key = format!("/{}", r.path.trim_start_matches('/'));
        let all = sh.tree.lock().unwrap().get(&key).cloned().unwrap_or_default();
        let from: usize = r.cursor.parse().unwrap_or(0);
        let page: Vec<DirEntry> = all
          .iter()
          .skip(from)
          .take(r.max_entries as usize)
          .map(|(n, dir)| DirEntry {
            name: n.clone(),
            r#type: if *dir { FileType::Directory } else { FileType::Regular } as i32,
            info: None,
          })
          .collect();
        let end = from + page.len();
        let next_cursor = if end < all.len() {
          end.to_string()
        } else {
          String::new()
        };
        Ok(
          LsResponse {
            entries: page,
            next_cursor,
          }
          .encode_to_vec(),
        )
      }
      method::WARM_OPEN => Ok(WarmOpenResponse { warm_id: 1 }.encode_to_vec()),
      method::WATCH => Ok(WatchResponse { watch_id: 1 }.encode_to_vec()),
      _ => Err("ENOSYS"),
    };
    sh.inflight.fetch_sub(1, SeqCst);
    let f = match out {
      Ok(meta) => Frame {
        flags: FLAG_RESPONSE,
        method: req.method,
        request_id: req.request_id,
        meta,
        payload,
      },
      Err(code) => err(&req, code),
    };
    if frame::write(&mut s, &f).is_err() {
      return;
    }
  }
}

fn client(srv: &Server, pool: usize, call_timeout: Duration) -> Client {
  Client::connect_with(
    &srv.path,
    Options {
      pool_size: pool,
      call_timeout,
      ..Default::default()
    },
  )
  .unwrap()
}

fn delay(srv: &Server, path: &str, ms: u64) {
  srv.shared.stat_delay.lock().unwrap().insert(path.into(), ms);
}

#[test]
fn call_takes_the_first_free_connection() {
  let srv = start(1 << 20);
  delay(&srv, "/long", 600);
  delay(&srv, "/short", 100);
  let c = client(&srv, 2, Duration::ZERO);
  c.ping().unwrap();
  let (a, b) = (c.clone(), c.clone());
  let t1 = std::thread::spawn(move || a.stat("/long").unwrap());
  std::thread::sleep(Duration::from_millis(80));
  let t2 = std::thread::spawn(move || b.stat("/short").unwrap());
  std::thread::sleep(Duration::from_millis(80));
  let t0 = Instant::now();
  c.stat("/quick").unwrap();
  let took = t0.elapsed();
  assert!(took < Duration::from_millis(350), "waited {took:?} for a connection");
  t2.join().unwrap();
  t1.join().unwrap();
}

#[test]
fn large_read_lets_other_calls_in_between_requests() {
  let srv = start(1000);
  srv.shared.slow_ms.store(40, SeqCst);
  let c = client(&srv, 1, Duration::ZERO);
  let f = c.open("/f", open_flag::READ_ONLY, 0).unwrap();
  let reader = std::thread::spawn(move || f.read_at(0, 20_000).unwrap().len());
  std::thread::sleep(Duration::from_millis(150));
  c.stat("/x").unwrap();
  assert_eq!(reader.join().unwrap(), 20_000);
  let log = srv.shared.log.lock().unwrap();
  let stat_at = log.iter().position(|e| e.1 == method::STAT).unwrap();
  let last_read = log.iter().rposition(|e| e.1 == method::READ_AT).unwrap();
  assert!(stat_at < last_read, "stat ran after the whole read");
}

#[test]
fn timeout_drops_the_connection_and_a_late_reply_cannot_leak() {
  let srv = start(1 << 20);
  delay(&srv, "/hang", 1000);
  let c = client(&srv, 1, Duration::from_millis(200));
  let e = c.stat("/hang").unwrap_err();
  assert!(matches!(e, Error::Timeout(_)), "{e:?}");
  assert_eq!(e.code(), Some("DEADLINE_EXCEEDED"));
  assert_eq!(std::io::Error::from(e).kind(), ErrorKind::TimedOut);
  // The late reply of /hang arrives about 1.3 s after the request.
  let info = c.stat("/next").unwrap();
  assert_eq!(info.name, "/next");
  std::thread::sleep(Duration::from_millis(1300));
  assert_eq!(c.stat("/after").unwrap().name, "/after");
  assert_eq!(
    srv.shared.hellos.load(SeqCst),
    2,
    "the timed out connection was redialed"
  );
}

#[test]
fn timeout_zero_means_no_limit() {
  let srv = start(1 << 20);
  delay(&srv, "/slowish", 300);
  let c = client(&srv, 1, Duration::ZERO);
  assert_eq!(c.stat("/slowish").unwrap().name, "/slowish");
}

#[test]
fn oversize_request_fails_alone_and_keeps_the_connection() {
  let srv = start(1 << 20);
  let c = client(&srv, 1, Duration::ZERO);
  let f = c.open("/f", open_flag::READ_ONLY, 0).unwrap();
  let huge = "p".repeat(17 << 20);
  let e = c.stat(&huge).unwrap_err();
  assert!(matches!(e, Error::TooLarge(_)), "{e:?}");
  assert_eq!(f.read_at(0, 4).unwrap(), vec![7; 4]);
  assert_eq!(srv.shared.hellos.load(SeqCst), 1);
}

#[test]
fn missing_capability_fails_locally() {
  let srv = start(1 << 20);
  *srv.shared.capabilities.lock().unwrap() = vec!["warm".into()];
  let c = client(&srv, 2, Duration::ZERO);
  for e in [
    c.stat_batch(&["/1".to_string()]).map(drop).unwrap_err(),
    c.watch("/", false, 0).map(drop).unwrap_err(),
    c.read_file("/a").map(drop).unwrap_err(),
    c.ls("/", 10, "").map(drop).unwrap_err(),
  ] {
    assert_eq!(e.code(), Some("UNSUPPORTED_OPERATION"), "{e:?}");
    assert_eq!(std::io::Error::from(e).kind(), ErrorKind::Unsupported);
  }
  assert_eq!(c.warm_open("/a").unwrap(), 1);
  let sent: Vec<u16> = srv.shared.log.lock().unwrap().iter().map(|e| e.1).collect();
  for m in [method::STAT_BATCH, method::WATCH, method::READ_FILE, method::LS] {
    assert!(!sent.contains(&m), "method {m} was sent");
  }
  c.ping().unwrap();
}

#[test]
fn no_reported_capabilities_allows_everything() {
  let srv = start(1 << 20);
  let c = client(&srv, 1, Duration::ZERO);
  assert_eq!(c.stat_batch(&["/3".to_string()]).unwrap()[0].as_ref().unwrap().size, 3);
  assert!(c.watch("/", false, 0).is_ok());
}

#[test]
fn stat_batch_chunks_run_in_parallel_and_keep_order() {
  let srv = start(1 << 20);
  srv.shared.slow_ms.store(150, SeqCst);
  let c = client(&srv, 3, Duration::ZERO);
  let paths: Vec<String> = (0..9000).map(|i| format!("/{i}")).collect();
  let r = c.stat_batch(&paths).unwrap();
  assert_eq!(r.len(), 9000);
  assert!(r.iter().enumerate().all(|(i, x)| x.as_ref().unwrap().size == i as i64));
  assert!(srv.shared.max_inflight.load(SeqCst) >= 2, "chunks ran one by one");
}

#[test]
fn write_batch_big_item_failure_reports_bytes_written() {
  let srv = start(1000);
  srv.shared.fail_write_from.store(3, SeqCst);
  let c = client(&srv, 1, Duration::ZERO);
  let f = c.open("/w", open_flag::READ_WRITE, 0).unwrap();
  let r = f
    .write_batch(&[
      WriteItem {
        offset: 0,
        data: vec![1; 3500],
      },
      WriteItem {
        offset: 0,
        data: vec![1; 10],
      },
    ])
    .unwrap();
  match &r[0] {
    Err(Error::Partial { written, source }) => {
      assert_eq!(*written, 2000);
      assert_eq!(source.code(), Some("ENOSPC"));
    }
    other => panic!("{other:?}"),
  }
  assert_eq!(*r[1].as_ref().unwrap(), 10);
}

fn tree(srv: &Server) {
  let mut t = srv.shared.tree.lock().unwrap();
  t.insert("/".into(), vec![("a".into(), true), ("f1".into(), false)]);
  t.insert(
    "/a".into(),
    vec![("b".into(), true), ("f2".into(), false), ("f3".into(), false)],
  );
  t.insert("/a/b".into(), vec![("f4".into(), false)]);
}

#[test]
fn walk_visits_every_entry_depth_first() {
  let srv = start(1 << 20);
  tree(&srv);
  let c = client(&srv, 1, Duration::ZERO);
  let mut seen = Vec::new();
  c.walk("/", |dir, e| {
    seen.push(format!("{dir}:{}", e.name));
    Ok(())
  })
  .unwrap();
  assert_eq!(seen, ["/:a", "/:f1", "/a:b", "/a:f2", "/a:f3", "/a/b:f4"]);
  assert_eq!(c.read_dir_all("/a").unwrap().len(), 3);
}

#[test]
fn walk_stops_on_callback_error_and_reports_ls_errors() {
  let srv = start(1 << 20);
  tree(&srv);
  let c = client(&srv, 1, Duration::ZERO);
  let mut n = 0;
  let e = c
    .walk("/", |_, _| {
      n += 1;
      if n == 3 {
        Err(Error::Server {
          code: "EINTR".into(),
          message: "stop".into(),
        })
      } else {
        Ok(())
      }
    })
    .unwrap_err();
  assert_eq!(e.code(), Some("EINTR"));
  assert_eq!(n, 3);
  *srv.shared.capabilities.lock().unwrap() = vec!["bulk_io".into()];
  let c2 = client(&srv, 1, Duration::ZERO);
  assert_eq!(
    c2.walk("/", |_, _| Ok(())).unwrap_err().code(),
    Some("UNSUPPORTED_OPERATION")
  );
}
