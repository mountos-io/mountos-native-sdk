//! Pipelining tests against an in-process fake server. The server reads frames on one thread and
//! answers each request on its own thread, so it sees every request the client has sent.

#[macro_use]
mod common;

use mountos_native::frame::{self, Frame, FLAG_RESPONSE};
use mountos_native::wire::*;
use mountos_native::{open_flag, Client, Error, EventKind, Options};
use prost::Message;
use std::collections::HashMap;
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

static SOCK_SEQ: AtomicUsize = AtomicUsize::new(0);

type Writer = Arc<Mutex<UnixStream>>;

#[derive(Default)]
struct Shared {
  /// Offer the pipelining capability in the Hello reply.
  pipelining: bool,
  max_inflight: u32,
  /// Hold Stat replies until this many are held, then send them all. Zero means reply at once.
  hold: usize,
  reverse: bool,
  /// Close the connection instead of releasing the held replies.
  drop_when_held: bool,
  /// Sleep before every Stat reply, milliseconds.
  stat_delay_ms: AtomicU64,
  /// Sleep before this Stat path replies, milliseconds.
  path_delay: Mutex<HashMap<String, u64>>,
  open_delay_ms: AtomicU64,
  held: Mutex<Vec<(Writer, Frame)>>,
  hellos: AtomicUsize,
  /// Capabilities the client listed in its last Hello.
  client_caps: Mutex<Vec<String>>,
  /// Requests received and not yet answered, and the highest value seen.
  outstanding: AtomicUsize,
  max_out: AtomicUsize,
  conns: AtomicUsize,
  /// Connection index of each Stat.
  stat_conns: Mutex<Vec<usize>>,
  /// Handles the client closed.
  closed: Mutex<Vec<u64>>,
}

struct Server {
  path: PathBuf,
  sh: Arc<Shared>,
}

impl Drop for Server {
  fn drop(&mut self) {
    let _ = std::fs::remove_file(&self.path);
  }
}

fn start(cfg: impl FnOnce(&mut Shared)) -> Server {
  let path = PathBuf::from(format!(
    "/tmp/mn-pipe-{}-{}.sock",
    std::process::id(),
    SOCK_SEQ.fetch_add(1, SeqCst)
  ));
  let _ = std::fs::remove_file(&path);
  let l = UnixListener::bind(&path).unwrap();
  let mut sh = Shared {
    pipelining: true,
    max_inflight: 32,
    ..Default::default()
  };
  cfg(&mut sh);
  let sh = Arc::new(sh);
  let s2 = sh.clone();
  std::thread::spawn(move || {
    for s in l.incoming() {
      let (s, sh) = (s.unwrap(), s2.clone());
      let idx = sh.conns.fetch_add(1, SeqCst);
      std::thread::spawn(move || serve(s, sh, idx));
    }
  });
  Server { path, sh }
}

fn reply(req: &Frame, meta: Vec<u8>) -> Frame {
  Frame::new(FLAG_RESPONSE, req.method, req.request_id, meta, vec![])
}

fn send(w: &Writer, f: &Frame) {
  let _ = frame::write(&mut *w.lock().unwrap(), f);
}

fn serve(s: UnixStream, sh: Arc<Shared>, idx: usize) {
  let w: Writer = Arc::new(Mutex::new(s.try_clone().unwrap()));
  let mut r = s;
  while let Ok(Some(req)) = frame::read(&mut r) {
    if req.method == method::HELLO {
      sh.hellos.fetch_add(1, SeqCst);
      *sh.client_caps.lock().unwrap() = HelloRequest::decode(&req.meta[..]).unwrap().capabilities;
      let caps = if sh.pipelining {
        vec!["pipelining".to_string()]
      } else {
        vec![]
      };
      let m = build!(HelloResponse {
        version: 1,
        capabilities: caps,
        max_payload_bytes: 1 << 20,
        max_inflight: sh.max_inflight,
      });
      send(&w, &reply(&req, m.encode_to_vec()));
      continue;
    }
    let cur = sh.outstanding.fetch_add(1, SeqCst) + 1;
    sh.max_out.fetch_max(cur, SeqCst);
    let (sh, w) = (sh.clone(), w.clone());
    std::thread::spawn(move || {
      let done = answer(&sh, &w, idx, req);
      sh.outstanding.fetch_sub(done, SeqCst);
    });
  }
}

/// Answers one request. Returns how many requests are complete: a held reply counts when it
/// is released, and a request that never gets a reply never counts.
fn answer(sh: &Shared, w: &Writer, idx: usize, req: Frame) -> usize {
  match req.method {
    method::STAT => {
      let p = StatRequest::decode(&req.meta[..]).unwrap().path;
      sh.stat_conns.lock().unwrap().push(idx);
      if p.starts_with("/never") {
        return 0;
      }
      let d = sh.path_delay.lock().unwrap().get(&p).copied().unwrap_or(0) + sh.stat_delay_ms.load(SeqCst);
      std::thread::sleep(Duration::from_millis(d));
      let info = build!(FileInfo { name: p });
      let f = reply(&req, build!(StatResponse { info: Some(info) }).encode_to_vec());
      if sh.hold == 0 {
        send(w, &f);
        return 1;
      }
      let mut held = sh.held.lock().unwrap();
      held.push((w.clone(), f));
      if held.len() < sh.hold {
        return 0;
      }
      let mut all = std::mem::take(&mut *held);
      drop(held);
      let n = all.len();
      if sh.drop_when_held {
        let _ = w.lock().unwrap().shutdown(Shutdown::Both);
        return n;
      }
      if sh.reverse {
        all.reverse();
      }
      for (w, f) in all {
        send(&w, &f);
      }
      n
    }
    method::OPEN => {
      std::thread::sleep(Duration::from_millis(sh.open_delay_ms.load(SeqCst)));
      send(w, &reply(&req, build!(OpenResponse { handle: 42 }).encode_to_vec()));
      1
    }
    method::CLOSE => {
      sh.closed
        .lock()
        .unwrap()
        .push(CloseRequest::decode(&req.meta[..]).unwrap().handle);
      send(w, &reply(&req, Empty::default().encode_to_vec()));
      1
    }
    method::PING => {
      send(w, &reply(&req, Empty::default().encode_to_vec()));
      1
    }
    m => panic!("unexpected method {m}"),
  }
}

fn client(srv: &Server, pool: usize, timeout_ms: u64, max_inflight: usize) -> Client {
  Client::connect_with(
    &srv.path,
    Options::default()
      .with_pool_size(pool)
      .with_call_timeout(Duration::from_millis(timeout_ms))
      .with_max_inflight(max_inflight),
  )
  .unwrap()
}

fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
  let end = Instant::now() + Duration::from_secs(5);
  while !ok() {
    assert!(Instant::now() < end, "timed out waiting for {what}");
    std::thread::sleep(Duration::from_millis(5));
  }
}

#[test]
fn calls_overlap_and_out_of_order_replies_reach_their_callers() {
  let srv = start(|s| {
    s.hold = 8;
    s.reverse = true;
  });
  let c = client(&srv, 1, 5000, 32);
  assert_eq!(*srv.sh.client_caps.lock().unwrap(), ["pipelining"]);
  std::thread::scope(|sc| {
    for i in 0..8 {
      let c = &c;
      sc.spawn(move || {
        let want = format!("/p{i}");
        assert_eq!(c.stat(&want).unwrap().name, want);
      });
    }
  });
  assert_eq!(srv.sh.max_out.load(SeqCst), 8);
  assert_eq!(srv.sh.hellos.load(SeqCst), 1);
}

#[test]
fn late_reply_after_a_timeout_is_discarded() {
  let srv = start(|_| {});
  srv.sh.path_delay.lock().unwrap().insert("/slow".into(), 400);
  let c = client(&srv, 1, 150, 32);
  let lost = Arc::new(AtomicUsize::new(0));
  let l2 = lost.clone();
  c.add_listener(move |e| {
    if e.kind == EventKind::ConnectionLost {
      l2.fetch_add(1, SeqCst);
    }
  });
  let e = c.stat("/slow").unwrap_err();
  assert!(matches!(e, Error::Timeout(_)), "{e:?}");
  assert_eq!(e.code(), Some("DEADLINE_EXCEEDED"));
  assert_eq!(c.stat("/fast").unwrap().name, "/fast");
  // The late reply for /slow arrives about 250 ms after the call starts. It must not reach another call.
  std::thread::scope(|sc| {
    sc.spawn(|| {
      std::thread::sleep(Duration::from_millis(200));
      assert_eq!(c.stat("/mid").unwrap().name, "/mid");
    });
  });
  std::thread::sleep(Duration::from_millis(300));
  assert_eq!(c.stat("/after").unwrap().name, "/after");
  assert_eq!(srv.sh.hellos.load(SeqCst), 1, "the connection was kept");
  assert_eq!(lost.load(SeqCst), 0);
}

#[test]
fn too_many_abandoned_ids_drop_the_connection() {
  let srv = start(|_| {});
  // Client limit 2 allows 8 abandoned ids.
  let c = client(&srv, 1, 60, 2);
  for i in 0..8 {
    let e = c.stat(&format!("/never{i}")).unwrap_err();
    assert!(matches!(e, Error::Timeout(_)), "{e:?}");
  }
  c.ping().unwrap();
  assert_eq!(srv.sh.hellos.load(SeqCst), 1, "eight abandoned ids fit");
  let e = c.stat("/never8").unwrap_err();
  assert!(matches!(e, Error::Timeout(_)), "{e:?}");
  c.ping().unwrap();
  assert_eq!(srv.sh.hellos.load(SeqCst), 2, "the ninth redialed");
}

#[test]
fn server_without_the_capability_sees_one_request_at_a_time() {
  let srv = start(|s| s.pipelining = false);
  srv.sh.stat_delay_ms.store(10, SeqCst);
  let c = client(&srv, 1, 5000, 32);
  std::thread::scope(|sc| {
    for t in 0..6 {
      let c = &c;
      sc.spawn(move || {
        for i in 0..3 {
          let want = format!("/t{t}-{i}");
          assert_eq!(c.stat(&want).unwrap().name, want);
        }
      });
    }
  });
  assert_eq!(srv.sh.max_out.load(SeqCst), 1);
}

#[test]
fn in_flight_count_follows_the_lower_limit() {
  // (server max_inflight, client max_inflight, expected most outstanding)
  for (server, client_max, want) in [(3u32, 32usize, 3usize), (32, 2, 2), (0, 32, 1), (32, 1, 1)] {
    let srv = start(|s| s.max_inflight = server);
    srv.sh.stat_delay_ms.store(30, SeqCst);
    let c = client(&srv, 1, 5000, client_max);
    std::thread::scope(|sc| {
      for t in 0..12 {
        let c = &c;
        sc.spawn(move || {
          for i in 0..2 {
            c.stat(&format!("/t{t}-{i}")).unwrap();
          }
        });
      }
    });
    assert_eq!(srv.sh.max_out.load(SeqCst), want, "server {server} client {client_max}");
  }
}

#[test]
fn transport_error_fails_every_outstanding_call() {
  let srv = start(|s| {
    s.hold = 4;
    s.drop_when_held = true;
  });
  let c = client(&srv, 1, 5000, 32);
  let errs: Vec<Error> = std::thread::scope(|sc| {
    let hs: Vec<_> = (0..4)
      .map(|i| {
        let c = &c;
        sc.spawn(move || c.stat(&format!("/p{i}")).unwrap_err())
      })
      .collect();
    hs.into_iter().map(|h| h.join().unwrap()).collect()
  });
  for e in &errs {
    assert!(matches!(e, Error::Io(_)), "{e:?}");
  }
  c.ping().unwrap();
  assert_eq!(srv.sh.hellos.load(SeqCst), 2, "the connection was dialed again");
}

#[test]
fn close_with_outstanding_calls_leaves_nothing_running() {
  let srv = start(|s| s.hold = 100);
  let c = client(&srv, 2, 0, 32);
  let probe = c.thread_probe();
  std::thread::scope(|sc| {
    let hs: Vec<_> = (0..3)
      .map(|i| {
        let c = &c;
        sc.spawn(move || c.stat(&format!("/p{i}")).unwrap_err())
      })
      .collect();
    wait_for("three requests at the server", || srv.sh.outstanding.load(SeqCst) == 3);
    c.close();
    for h in hs {
      assert!(matches!(h.join().unwrap(), Error::Io(_)));
    }
  });
  assert_eq!(probe.load(SeqCst), 0, "threads left after close");
  assert!(c.ping().is_err());
}

#[test]
fn late_open_reply_releases_the_handle() {
  let srv = start(|_| {});
  srv.sh.open_delay_ms.store(400, SeqCst);
  let c = client(&srv, 1, 150, 32);
  let probe = c.thread_probe();
  let e = c.open("/f", open_flag::READ_ONLY, 0).map(drop).unwrap_err();
  assert!(matches!(e, Error::Timeout(_)), "{e:?}");
  wait_for("the release Close", || *srv.sh.closed.lock().unwrap() == [42]);
  assert_eq!(srv.sh.hellos.load(SeqCst), 1);
  c.close();
  assert_eq!(probe.load(SeqCst), 0, "threads left after close");
}

#[test]
fn the_connection_with_fewest_outstanding_calls_gets_the_next_one() {
  let srv = start(|_| {});
  srv.sh.stat_delay_ms.store(250, SeqCst);
  let c = client(&srv, 2, 5000, 32);
  std::thread::scope(|sc| {
    sc.spawn(|| c.stat("/a").unwrap());
    wait_for("the first request", || srv.sh.stat_conns.lock().unwrap().len() == 1);
    sc.spawn(|| c.stat("/b").unwrap());
  });
  let conns = srv.sh.stat_conns.lock().unwrap().clone();
  assert_eq!(conns.len(), 2);
  assert_ne!(conns[0], conns[1], "both calls used one connection");
}
