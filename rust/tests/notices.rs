//! Notice, listener, watch and warm tests against an in-process fake server that can push
//! notice frames at any time on any connection.

#[macro_use]
mod common;

use mountos_native::frame::{self, Frame, FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE};
use mountos_native::wire::*;
use mountos_native::{ChangeKind, Client, Error, Event, EventKind, Options, Watch};
use prost::Message;
use std::collections::HashMap;
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

static SOCK_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct Shared {
  /// Write side of every accepted connection, in accept order.
  conns: Mutex<Vec<Arc<Mutex<UnixStream>>>>,
  /// Watch and warm ids to the index of the connection that made the call.
  owners: Mutex<HashMap<u64, usize>>,
  /// (connection index, watch id) of each Unwatch call.
  unwatches: Mutex<Vec<(usize, u64)>>,
  next_id: AtomicU64,
  /// Frames sent before the reply of the next Ping, 15 ms apart.
  pre_reply: Mutex<Vec<Frame>>,
  methods: Mutex<Vec<u16>>,
  closed_conns: AtomicUsize,
  /// instance_id the Hello reply reports. 0 reports none, like an older server.
  instance: AtomicU64,
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

fn start() -> Server {
  let path = PathBuf::from(format!(
    "/tmp/mn-notice-{}-{}.sock",
    std::process::id(),
    SOCK_SEQ.fetch_add(1, SeqCst)
  ));
  let _ = std::fs::remove_file(&path);
  let l = UnixListener::bind(&path).unwrap();
  let shared = Arc::new(Shared::default());
  shared.next_id.store(100, SeqCst);
  let sh = shared.clone();
  std::thread::spawn(move || {
    for s in l.incoming() {
      let (s, sh) = (s.unwrap(), sh.clone());
      std::thread::spawn(move || serve(s, sh));
    }
  });
  Server { path, shared }
}

fn send(out: &Mutex<UnixStream>, f: &Frame) {
  let _ = frame::write(&mut *out.lock().unwrap(), f);
}

fn reply(req: &Frame, meta: Vec<u8>) -> Frame {
  Frame::new(FLAG_RESPONSE, req.method, req.request_id, meta, vec![])
}

fn error_reply(req: &Frame, code: &str, message: &str) -> Frame {
  let mut f = reply(
    req,
    build!(WireError {
      code: code.into(),
      message: message.into(),
    })
    .encode_to_vec(),
  );
  f.flags |= FLAG_ERROR;
  f
}

fn serve(s: UnixStream, sh: Arc<Shared>) {
  let mut rd = s.try_clone().unwrap();
  let out = Arc::new(Mutex::new(s));
  let idx = {
    let mut c = sh.conns.lock().unwrap();
    c.push(out.clone());
    c.len() - 1
  };
  while let Ok(Some(req)) = frame::read(&mut rd) {
    sh.methods.lock().unwrap().push(req.method);
    let meta = &req.meta[..];
    let f = match req.method {
      method::HELLO => reply(
        &req,
        build!(HelloResponse {
          version: 1,
          capabilities: vec![],
          max_payload_bytes: 1 << 20,
          max_inflight: 1,
          instance_id: sh.instance.load(SeqCst),
        })
        .encode_to_vec(),
      ),
      method::PING => {
        let pre: Vec<Frame> = sh.pre_reply.lock().unwrap().drain(..).collect();
        for p in pre {
          send(&out, &p);
          std::thread::sleep(Duration::from_millis(15));
        }
        reply(&req, Empty::default().encode_to_vec())
      }
      method::WATCH => {
        let w = WatchRequest::decode(meta).unwrap();
        if w.path == "/denied" {
          error_reply(&req, "EACCES", "denied")
        } else {
          let id = sh.next_id.fetch_add(1, SeqCst);
          sh.owners.lock().unwrap().insert(id, idx);
          let r = reply(&req, build!(WatchResponse { watch_id: id }).encode_to_vec());
          if w.path == "/burst" {
            // Changes sent right behind the reply, before the client has returned from the call.
            send(&out, &r);
            for i in 1..=5 {
              send(
                &out,
                &nframe(change(id, WatchEventKind::Create, &format!("/burst/{i}"), "", i)),
              );
            }
            continue;
          }
          r
        }
      }
      method::UNWATCH => {
        let u = UnwatchRequest::decode(meta).unwrap();
        let known = sh.owners.lock().unwrap().get(&u.watch_id) == Some(&idx);
        sh.unwatches.lock().unwrap().push((idx, u.watch_id));
        if known {
          reply(&req, Empty::default().encode_to_vec())
        } else {
          error_reply(&req, "EBADF", "unknown watch")
        }
      }
      method::WARM_OPEN => {
        let id = sh.next_id.fetch_add(1, SeqCst);
        sh.owners.lock().unwrap().insert(id, idx);
        reply(&req, build!(WarmOpenResponse { warm_id: id }).encode_to_vec())
      }
      _ => error_reply(&req, "ENOSYS", "unsupported"),
    };
    send(&out, &f);
  }
  sh.closed_conns.fetch_add(1, SeqCst);
}

impl Server {
  fn push(&self, conn: usize, f: &Frame) {
    let c = self.shared.conns.lock().unwrap()[conn].clone();
    send(&c, f);
  }

  /// Pushes to the connection that owns `ref_id`.
  fn push_owner(&self, ref_id: u64, n: Notice) {
    let idx = self.shared.owners.lock().unwrap()[&ref_id];
    self.push(idx, &nframe(n));
  }

  fn drop_conn(&self, conn: usize) {
    let c = self.shared.conns.lock().unwrap()[conn].clone();
    let _ = c.lock().unwrap().shutdown(Shutdown::Both);
  }
}

fn notice(kind: NoticeKind, ref_id: u64, seq: u64) -> Notice {
  build!(Notice {
    kind: kind as i32,
    sequence: seq,
    ref_id,
  })
}

fn change(watch: u64, kind: WatchEventKind, path: &str, old: &str, seq: u64) -> Notice {
  build!(from notice(NoticeKind::Change, watch, seq); change_kind: kind as i32,
    path: path.into(),
    old_path: old.into())
}

fn nframe(n: Notice) -> Frame {
  Frame::new(FLAG_NOTIFY, method::NOTICE, n.ref_id, n.encode_to_vec(), vec![])
}

fn wait_for(what: &str, f: impl Fn() -> bool) {
  for _ in 0..500 {
    if f() {
      return;
    }
    std::thread::sleep(Duration::from_millis(10));
  }
  panic!("timed out waiting for {what}");
}

type Log = Arc<Mutex<Vec<String>>>;

fn client_with(srv: &Server, pool: usize) -> (Client, Log) {
  let log: Log = Arc::default();
  let l = log.clone();
  let c = Client::connect_with(
    &srv.path,
    Options::default()
      .with_pool_size(pool)
      .with_logger(Arc::new(move |line: &str| l.lock().unwrap().push(line.to_string()))),
  )
  .unwrap();
  (c, log)
}

fn client(srv: &Server, pool: usize) -> Client {
  client_with(srv, pool).0
}

type Events = Arc<Mutex<Vec<Event>>>;

fn collect(c: &Client) -> Events {
  let ev: Events = Arc::default();
  let e = ev.clone();
  c.add_listener(move |x| e.lock().unwrap().push(x.clone()));
  ev
}

fn count(ev: &Events) -> usize {
  ev.lock().unwrap().len()
}

fn t(secs: u64) -> Duration {
  Duration::from_secs(secs)
}

#[test]
fn responses_match_while_notices_arrive() {
  let srv = start();
  let c = client(&srv, 1);
  let ev = collect(&c);
  // Notices between the request and its reply.
  *srv.shared.pre_reply.lock().unwrap() = (1..=3).map(|i| nframe(notice(NoticeKind::WarmDone, i, i))).collect();
  c.ping().unwrap();
  // Notices from another thread while calls run back to back.
  let sh = srv.shared.clone();
  let spam = std::thread::spawn(move || {
    let out = sh.conns.lock().unwrap()[0].clone();
    for i in 4..=203u64 {
      send(&out, &nframe(notice(NoticeKind::WarmDone, i, i)));
    }
  });
  for _ in 0..50 {
    c.ping().unwrap();
  }
  spam.join().unwrap();
  wait_for("203 events", || count(&ev) == 203);
  let seqs: Vec<u64> = ev.lock().unwrap().iter().map(|e| e.sequence).collect();
  assert_eq!(seqs, (1..=203).collect::<Vec<_>>());
}

#[test]
fn idle_connection_notices_are_delivered() {
  let srv = start();
  let c = client(&srv, 2);
  let ev = collect(&c);
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 9, 1)));
  wait_for("event", || count(&ev) == 1);
  let e = ev.lock().unwrap()[0].clone();
  assert_eq!((e.kind, e.ref_id, e.sequence), (EventKind::WarmDone, 9, 1));
  assert!(e.error.is_none());
}

#[test]
fn warm_results_reach_every_listener() {
  let srv = start();
  let c = client(&srv, 1);
  let (a, b) = (collect(&c), collect(&c));
  let id = c.warm_open("/a").unwrap();
  let id2 = c.warm_open("/b").unwrap();
  srv.push_owner(id, notice(NoticeKind::WarmDone, id, 1));
  srv.push_owner(
    id2,
    build!(from notice(NoticeKind::WarmFailed, id2, 2); path: "/b".into(),
      code: "ENOENT".into(),
      message: "no such file".into()),
  );
  for ev in [&a, &b] {
    wait_for("two events", || count(ev) == 2);
    let v = ev.lock().unwrap();
    assert_eq!((v[0].kind, v[0].ref_id), (EventKind::WarmDone, id));
    assert_eq!(
      (v[1].kind, v[1].ref_id, v[1].path.as_str()),
      (EventKind::WarmFailed, id2, "/b")
    );
    match v[1].error.as_ref().unwrap() {
      Error::Server { code, message } => assert_eq!((code.as_str(), message.as_str()), ("ENOENT", "no such file")),
      e => panic!("unexpected error {e}"),
    }
  }
}

#[test]
fn no_listener_logs_the_event() {
  let srv = start();
  let (c, log) = client_with(&srv, 1);
  srv.push(
    0,
    &nframe(build!(from notice(NoticeKind::WarmFailed, 5, 1); path: "/x".into(),
      code: "EIO".into(),
      message: "boom".into())),
  );
  wait_for("log line", || !log.lock().unwrap().is_empty());
  {
    let l = log.lock().unwrap();
    assert!(l[0].contains("WarmFailed"), "{}", l[0]);
    assert!(l[0].contains("ref_id=5"), "{}", l[0]);
    assert!(l[0].contains("\"/x\""), "{}", l[0]);
    assert!(l[0].contains("EIO"), "{}", l[0]);
  }
  // With a listener the event goes to it and not to the logger.
  let ev = collect(&c);
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 6, 2)));
  wait_for("event", || count(&ev) == 1);
  assert_eq!(log.lock().unwrap().len(), 1);
}

#[test]
fn connection_lost_event_and_reconnect() {
  let srv = start();
  let c = client(&srv, 1);
  let ev = collect(&c);
  srv.drop_conn(0);
  wait_for("event", || count(&ev) == 1);
  let e = ev.lock().unwrap()[0].clone();
  assert_eq!(e.kind, EventKind::ConnectionLost);
  assert!(matches!(e.error, Some(Error::Io(_))));
  // The next call reconnects.
  c.ping().unwrap();
  assert_eq!(srv.shared.conns.lock().unwrap().len(), 2);
}

#[test]
fn closing_event_then_no_connection_lost() {
  let srv = start();
  let c = client(&srv, 1);
  let ev = collect(&c);
  srv.push(0, &nframe(notice(NoticeKind::Closing, 0, 1)));
  srv.drop_conn(0);
  wait_for("closing", || count(&ev) == 1);
  wait_for("server sees close", || srv.shared.closed_conns.load(SeqCst) == 1);
  std::thread::sleep(Duration::from_millis(100));
  let v = ev.lock().unwrap();
  assert_eq!(v.len(), 1);
  assert_eq!(v[0].kind, EventKind::Closing);
}

fn recv(w: &Watch) -> Event {
  w.recv_timeout(t(2)).expect("watch event")
}

#[test]
fn watch_events_reach_listener_and_iterator() {
  let srv = start();
  let c = client(&srv, 2);
  let ev = collect(&c);
  let w = c.watch("/d", true, 0).unwrap();
  let other = c.watch("/e", false, 0).unwrap();
  assert_ne!(w.id(), other.id());
  srv.push_owner(w.id(), change(w.id(), WatchEventKind::Create, "/d/a", "", 1));
  srv.push_owner(w.id(), change(w.id(), WatchEventKind::Rename, "/d/b", "/d/a", 2));
  srv.push_owner(other.id(), change(other.id(), WatchEventKind::Write, "/e/f", "", 3));
  let e1 = recv(&w);
  assert_eq!(
    (e1.kind, e1.change, e1.path.as_str(), e1.sequence),
    (EventKind::Change, Some(ChangeKind::Create), "/d/a", 1)
  );
  assert_eq!(e1.ref_id, w.id());
  let e2 = recv(&w);
  assert_eq!(
    (e2.change, e2.path.as_str(), e2.old_path.as_str()),
    (Some(ChangeKind::Rename), "/d/b", "/d/a")
  );
  // The other watch's event is not in this view.
  assert!(w.recv_timeout(Duration::from_millis(100)).is_none());
  let e3 = recv(&other);
  assert_eq!(e3.change, Some(ChangeKind::Write));
  wait_for("listener sees all", || count(&ev) == 3);
  let mut w = w;
  srv.push_owner(w.id(), change(w.id(), WatchEventKind::Attrib, "/d/b", "", 4));
  let e4 = w.next().unwrap();
  assert_eq!(e4.change, Some(ChangeKind::Attrib));
}

#[test]
fn changes_right_behind_the_watch_reply_are_kept() {
  for _ in 0..20 {
    let srv = start();
    let c = client(&srv, 1);
    let w = c.watch("/burst", true, 0).unwrap();
    let seqs: Vec<u64> = (0..5).map(|_| recv(&w).sequence).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
  }
}

#[test]
fn watch_error_is_returned() {
  let srv = start();
  let c = client(&srv, 1);
  assert_eq!(c.watch("/denied", false, 0).err().unwrap().code(), Some("EACCES"));
}

#[test]
fn watch_close_sends_unwatch_on_owning_connection() {
  let srv = start();
  let c = client(&srv, 3);
  // Spread the pool so the watch is not on connection 0 by accident.
  for _ in 0..3 {
    c.ping().unwrap();
  }
  let ws: Vec<Watch> = (0..6).map(|i| c.watch(&format!("/w{i}"), false, 0).unwrap()).collect();
  let owners: Vec<usize> = ws.iter().map(|w| srv.shared.owners.lock().unwrap()[&w.id()]).collect();
  let ids: Vec<u64> = ws.iter().map(Watch::id).collect();
  for w in &ws {
    w.close().unwrap();
    w.close().unwrap();
  }
  let got = srv.shared.unwatches.lock().unwrap().clone();
  let want: Vec<(usize, u64)> = owners.into_iter().zip(ids).collect();
  assert_eq!(got, want);
  assert!(got.iter().map(|g| g.0).collect::<std::collections::HashSet<_>>().len() > 1);
}

#[test]
fn watch_drop_sends_unwatch() {
  let srv = start();
  let c = client(&srv, 1);
  let w = c.watch("/d", true, 0).unwrap();
  let id = w.id();
  drop(w);
  assert_eq!(*srv.shared.unwatches.lock().unwrap(), vec![(0, id)]);
}

#[test]
fn close_ends_blocked_iterator() {
  let srv = start();
  let c = client(&srv, 1);
  let w = Arc::new(c.watch("/d", true, 0).unwrap());
  let w2 = w.clone();
  let h = std::thread::spawn(move || (&*w2).count());
  std::thread::sleep(Duration::from_millis(100));
  w.close().unwrap();
  assert_eq!(h.join().unwrap(), 0);
}

#[test]
fn close_after_connection_gone_is_ok() {
  let srv = start();
  let c = client(&srv, 1);
  let ev = collect(&c);
  let w = c.watch("/d", true, 0).unwrap();
  srv.drop_conn(0);
  wait_for("lost", || count(&ev) == 1);
  // The iterator ended with the connection.
  assert!(w.recv_timeout(t(2)).is_none());
  w.close().unwrap();
  assert!(srv.shared.unwatches.lock().unwrap().is_empty());
}

#[test]
fn unwatch_ebadf_is_ignored() {
  let srv = start();
  let c = client(&srv, 1);
  let w = c.watch("/d", true, 0).unwrap();
  srv.shared.owners.lock().unwrap().remove(&w.id());
  w.close().unwrap();
  assert_eq!(srv.shared.unwatches.lock().unwrap().len(), 1);
}

#[test]
fn slow_watch_consumer_gets_overflow() {
  let srv = start();
  let c = client(&srv, 1);
  let ev = collect(&c);
  let w = c.watch("/d", true, 0).unwrap();
  for i in 1..=300u64 {
    srv.push_owner(w.id(), change(w.id(), WatchEventKind::Write, "/d/f", "", i));
  }
  wait_for("all published", || count(&ev) == 300);
  let first = recv(&w);
  assert_eq!((first.kind, first.ref_id), (EventKind::Overflow, w.id()));
  let rest: Vec<Event> = std::iter::from_fn(|| w.recv_timeout(Duration::from_millis(100))).collect();
  assert_eq!(rest.len(), 256);
  assert_eq!(rest.last().unwrap().sequence, 300);
  assert_eq!(rest[0].sequence, 45);
}

#[test]
fn slow_listener_queue_drops_oldest_and_reports_overflow() {
  let srv = start();
  let (c, log) = client_with(&srv, 1);
  let gate = Arc::new((Mutex::new(false), Condvar::new()));
  let g = gate.clone();
  let ev: Events = Arc::default();
  let e = ev.clone();
  c.add_listener(move |x| {
    let (m, cv) = &*g;
    let mut open = m.lock().unwrap();
    while !*open {
      open = cv.wait(open).unwrap();
    }
    e.lock().unwrap().push(x.clone());
  });
  for i in 1..=1100u64 {
    srv.push(0, &nframe(notice(NoticeKind::Closing, 0, i)));
    if i % 100 == 0 {
      std::thread::sleep(Duration::from_millis(5));
    }
  }
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 7777, 2000)));
  c.wait_warm(7777, t(5)).unwrap();
  {
    let (m, cv) = &*gate;
    *m.lock().unwrap() = true;
    cv.notify_all();
  }
  wait_for("sentinel", || ev.lock().unwrap().iter().any(|e| e.sequence == 2000));
  let v = ev.lock().unwrap();
  let overflows = v
    .iter()
    .filter(|e| e.kind == EventKind::Overflow && e.ref_id == 0)
    .count();
  assert!(overflows >= 1, "no local overflow event");
  let server: Vec<u64> = v.iter().filter(|e| e.sequence != 0).map(|e| e.sequence).collect();
  assert!(server.len() < 1101, "{} events", server.len());
  assert!(server.windows(2).all(|w| w[0] < w[1]), "order kept");
  assert_eq!(*server.last().unwrap(), 2000);
  assert!(log.lock().unwrap().iter().any(|l| l.contains("queue full")));
}

#[test]
fn wait_warm_success_failure_finished_and_timeout() {
  let srv = start();
  let c = client(&srv, 1);
  let ev = collect(&c);
  let a = c.warm_open("/a").unwrap();
  let b = c.warm_open("/b").unwrap();
  let done = c.warm_open("/done").unwrap();
  let pending = c.warm_open("/never").unwrap();
  // Finished before anyone waits.
  srv.push_owner(done, notice(NoticeKind::WarmDone, done, 1));
  wait_for("event", || count(&ev) == 1);
  c.wait_warm(done, Duration::from_millis(10)).unwrap();
  // Success while waiting.
  let c2 = c.clone();
  let h = std::thread::spawn(move || c2.wait_warm(a, t(5)));
  std::thread::sleep(Duration::from_millis(50));
  srv.push_owner(a, notice(NoticeKind::WarmDone, a, 2));
  h.join().unwrap().unwrap();
  // Failure while waiting.
  let c2 = c.clone();
  let h = std::thread::spawn(move || c2.wait_warm(b, t(5)));
  std::thread::sleep(Duration::from_millis(50));
  srv.push_owner(
    b,
    build!(from notice(NoticeKind::WarmFailed, b, 3); code: "EIO".into(),
      message: "read failed".into(),
      path: "/b".into()),
  );
  match h.join().unwrap() {
    Err(Error::Server { code, message }) => assert_eq!((code.as_str(), message.as_str()), ("EIO", "read failed")),
    other => panic!("unexpected {other:?}"),
  }
  // Timeout.
  let start = Instant::now();
  match c.wait_warm(pending, Duration::from_millis(150)) {
    Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
    other => panic!("unexpected {other:?}"),
  }
  assert!(start.elapsed() >= Duration::from_millis(140));
  // The client never asked the server about warm state.
  let m = srv.shared.methods.lock().unwrap();
  assert!(m.iter().all(|&x| x == method::HELLO || x == method::WARM_OPEN), "{m:?}");
}

#[test]
fn wait_warm_after_close_fails_even_for_a_finished_warm() {
  let srv = start();
  let c = client(&srv, 1);
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 41, 1)));
  c.wait_warm(41, t(5)).unwrap();
  c.close();
  match c.wait_warm(41, Duration::from_millis(50)) {
    Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionAborted),
    other => panic!("{other:?}"),
  }
}

#[test]
fn warm_cache_keeps_the_last_1024() {
  let srv = start();
  let c = client(&srv, 1);
  for i in 1..=1100u64 {
    srv.push(0, &nframe(notice(NoticeKind::WarmDone, i, i)));
  }
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 5000, 5000)));
  c.wait_warm(5000, t(5)).unwrap();
  c.wait_warm(1100, Duration::from_millis(10)).unwrap();
  c.wait_warm(80, Duration::from_millis(10)).unwrap();
  assert!(c.wait_warm(10, Duration::from_millis(30)).is_err());
}

#[test]
fn panicking_listener_does_not_stop_others() {
  let srv = start();
  let (c, log) = client_with(&srv, 1);
  c.add_listener(|_| panic!("listener bug"));
  let ev = collect(&c);
  for i in 1..=3u64 {
    srv.push(0, &nframe(notice(NoticeKind::WarmDone, i, i)));
  }
  wait_for("events", || count(&ev) == 3);
  assert!(
    log
      .lock()
      .unwrap()
      .iter()
      .filter(|l| l.contains("listener panicked"))
      .count()
      >= 3
  );
}

#[test]
fn remove_listener_stops_delivery() {
  let srv = start();
  let c = client(&srv, 1);
  let a = collect(&c);
  let n = Arc::new(AtomicUsize::new(0));
  let n2 = n.clone();
  let id = c.add_listener(move |_| {
    n2.fetch_add(1, SeqCst);
  });
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 1, 1)));
  wait_for("first", || n.load(SeqCst) == 1);
  c.remove_listener(id);
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 2, 2)));
  wait_for("second", || count(&a) == 2);
  assert_eq!(n.load(SeqCst), 1);
}

#[test]
fn drop_stops_every_thread() {
  let srv = start();
  let c = client(&srv, 3);
  let token = Arc::new(());
  let t2 = token.clone();
  c.add_listener(move |_| {
    let _ = &t2;
  });
  // Connect the whole pool and start two watches.
  let ws: Vec<_> = (0..3).map(|i| c.watch(&format!("/w{i}"), false, 0).unwrap()).collect();
  let probe = c.thread_probe();
  assert!(probe.load(SeqCst) >= 2, "{}", probe.load(SeqCst));
  let c2 = c.clone();
  let h = std::thread::spawn(move || (&ws[0]).count());
  drop(c);
  // A clone keeps the client open.
  c2.ping().unwrap();
  assert!(probe.load(SeqCst) >= 2);
  drop(c2);
  assert_eq!(h.join().unwrap(), 0);
  assert_eq!(probe.load(SeqCst), 0);
  assert_eq!(Arc::strong_count(&token), 1);
  let n = srv.shared.conns.lock().unwrap().len();
  wait_for("server sees closes", || srv.shared.closed_conns.load(SeqCst) == n);
}

#[test]
fn close_fails_later_calls_and_ends_watches() {
  let srv = start();
  let c = client(&srv, 1);
  let w = c.watch("/d", true, 0).unwrap();
  let probe = c.thread_probe();
  c.close();
  c.close();
  assert_eq!(probe.load(SeqCst), 0);
  assert!(w.recv_timeout(Duration::from_millis(10)).is_none());
  match c.ping() {
    Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionAborted),
    other => panic!("unexpected {other:?}"),
  }
  assert!(c.wait_warm(1, Duration::from_millis(10)).is_err());
  w.close().unwrap();
}

#[test]
fn overflow_without_watch_id_reaches_every_watch_of_its_connection() {
  let srv = start();
  let c = client(&srv, 2);
  let a = c.watch("/a", true, 0).unwrap();
  let b = c.watch("/b", true, 0).unwrap();
  let owner = |w: &Watch| srv.shared.owners.lock().unwrap()[&w.id()];
  assert_ne!(owner(&a), owner(&b));
  srv.push_owner(a.id(), notice(NoticeKind::Overflow, 0, 5));
  let got = recv(&a);
  assert_eq!((got.kind, got.ref_id, got.sequence), (EventKind::Overflow, 0, 5));
  assert!(b.recv_timeout(Duration::from_millis(150)).is_none());
}

/// Pings until the client has seen the dropped connection and dialed a new one.
fn reconnect(c: &Client) {
  wait_for("reconnect", || c.ping().is_ok());
}

#[test]
fn wait_warm_rejects_result_of_replaced_server() {
  let srv = start();
  srv.shared.instance.store(11, SeqCst);
  let c = client(&srv, 1);
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 1, 1)));
  c.wait_warm(1, t(5)).unwrap();
  srv.shared.instance.store(22, SeqCst);
  srv.drop_conn(0);
  reconnect(&c);
  // The old completion of warm id 1 is gone. The new server has not finished its warm id 1.
  match c.wait_warm(1, Duration::from_millis(100)) {
    Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
    other => panic!("unexpected {other:?}"),
  }
  srv.push(1, &nframe(notice(NoticeKind::WarmDone, 1, 1)));
  c.wait_warm(1, t(5)).unwrap();
}

#[test]
fn wait_warm_fails_pending_waiter_of_replaced_server() {
  let srv = start();
  srv.shared.instance.store(11, SeqCst);
  let c = client(&srv, 1);
  let c2 = c.clone();
  let h = std::thread::spawn(move || c2.wait_warm(1, t(5)));
  std::thread::sleep(Duration::from_millis(50));
  srv.shared.instance.store(22, SeqCst);
  srv.drop_conn(0);
  reconnect(&c);
  match h.join().unwrap() {
    Err(Error::Io(e)) => assert!(e.to_string().contains("server restarted"), "{e}"),
    other => panic!("unexpected {other:?}"),
  }
}

#[test]
fn wait_warm_keeps_result_when_server_reports_no_instance() {
  let srv = start();
  let c = client(&srv, 1);
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 1, 1)));
  c.wait_warm(1, t(5)).unwrap();
  srv.drop_conn(0);
  reconnect(&c);
  c.wait_warm(1, Duration::from_millis(50)).unwrap();
}

#[test]
fn wait_warm_rejects_result_of_server_that_gained_an_instance() {
  let srv = start();
  let c = client(&srv, 1);
  srv.push(0, &nframe(notice(NoticeKind::WarmDone, 1, 1)));
  c.wait_warm(1, t(5)).unwrap();
  let c2 = c.clone();
  let h = std::thread::spawn(move || c2.wait_warm(2, t(5)));
  std::thread::sleep(Duration::from_millis(50));
  srv.shared.instance.store(22, SeqCst);
  srv.drop_conn(0);
  reconnect(&c);
  match h.join().unwrap() {
    Err(Error::Io(e)) => assert!(e.to_string().contains("server restarted"), "{e}"),
    other => panic!("unexpected {other:?}"),
  }
  match c.wait_warm(1, Duration::from_millis(100)) {
    Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
    other => panic!("unexpected {other:?}"),
  }
  srv.push(1, &nframe(notice(NoticeKind::WarmDone, 1, 1)));
  c.wait_warm(1, t(5)).unwrap();
}
