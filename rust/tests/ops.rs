//! Clone, clone_dir, remove_dir and Operation tests against an in-process fake server that
//! answers the four operation methods and pushes notices at any time.

#[macro_use]
mod common;

use mountos_native::frame::{self, Frame, FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE};
use mountos_native::wire::*;
use mountos_native::{
  Client, CloneOptions, CloneOutcome, Error, Event, EventKind, OpCounts, Operation, Options, Preserve, MAX_OP_FAILURES,
};
use prost::Message;
use std::collections::HashMap;
use std::io;
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static SOCK_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct Shared {
  conns: Mutex<Vec<Arc<Mutex<UnixStream>>>>,
  /// Op id to (connection index, ended).
  ops: Mutex<HashMap<u64, (usize, bool)>>,
  next_op: AtomicU64,
  /// Capabilities the Hello reply reports. Empty reports none, like an older server.
  caps: Mutex<Vec<String>>,
  hello_caps: Mutex<Vec<String>>,
  outcome: AtomicI32,
  clone_err: Mutex<String>,
  full: AtomicBool,
  /// Notices sent right after each start reply. The flag stamps the new op id as ref_id.
  eager: Mutex<Vec<(Notice, bool)>>,
  clones: Mutex<Vec<CloneRequest>>,
  dirs: Mutex<Vec<CloneDirRequest>>,
  removes: Mutex<Vec<String>>,
  cancels: Mutex<Vec<u64>>,
  op_requests: AtomicUsize,
  seq: AtomicU64,
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
    "/tmp/mn-ops-{}-{}.sock",
    std::process::id(),
    SOCK_SEQ.fetch_add(1, SeqCst)
  ));
  let _ = std::fs::remove_file(&path);
  let l = UnixListener::bind(&path).unwrap();
  let shared = Arc::new(Shared::default());
  shared.next_op.store(1, SeqCst);
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

fn error_reply(req: &Frame, code: &str) -> Frame {
  let mut f = reply(
    req,
    build!(WireError {
      code: code.into(),
      message: format!("fake {code}"),
    })
    .encode_to_vec(),
  );
  f.flags |= FLAG_ERROR;
  f
}

fn nframe(n: Notice) -> Frame {
  Frame::new(FLAG_NOTIFY, method::NOTICE, 0, n.encode_to_vec(), vec![])
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
    let meta = &req.meta[..];
    let mut start_id = None;
    let f = match req.method {
      method::HELLO => {
        *sh.hello_caps.lock().unwrap() = HelloRequest::decode(meta).unwrap().capabilities;
        reply(
          &req,
          build!(HelloResponse {
            version: 1,
            capabilities: sh.caps.lock().unwrap().clone(),
            max_payload_bytes: 1 << 20,
            max_inflight: 1,
          })
          .encode_to_vec(),
        )
      }
      method::PING => reply(&req, Empty::default().encode_to_vec()),
      method::CLONE => {
        sh.op_requests.fetch_add(1, SeqCst);
        sh.clones.lock().unwrap().push(CloneRequest::decode(meta).unwrap());
        let err = sh.clone_err.lock().unwrap().clone();
        if err.is_empty() {
          reply(
            &req,
            build!(CloneResponse {
              outcome: sh.outcome.load(SeqCst),
            })
            .encode_to_vec(),
          )
        } else {
          error_reply(&req, &err)
        }
      }
      method::CLONE_DIR | method::REMOVE_DIR => {
        sh.op_requests.fetch_add(1, SeqCst);
        if req.method == method::CLONE_DIR {
          sh.dirs.lock().unwrap().push(CloneDirRequest::decode(meta).unwrap());
        } else {
          sh.removes
            .lock()
            .unwrap()
            .push(RemoveDirRequest::decode(meta).unwrap().path);
        }
        if sh.full.load(SeqCst) {
          error_reply(&req, "EAGAIN")
        } else {
          let id = sh.next_op.fetch_add(1, SeqCst);
          sh.ops.lock().unwrap().insert(id, (idx, false));
          start_id = Some(id);
          let body = if req.method == method::CLONE_DIR {
            build!(CloneDirResponse { op_id: id }).encode_to_vec()
          } else {
            build!(RemoveDirResponse { op_id: id }).encode_to_vec()
          };
          reply(&req, body)
        }
      }
      method::OP_CANCEL => {
        sh.op_requests.fetch_add(1, SeqCst);
        let id = OpCancelRequest::decode(meta).unwrap().op_id;
        let ok = sh
          .ops
          .lock()
          .unwrap()
          .get(&id)
          .is_some_and(|&(c, ended)| c == idx && !ended);
        if ok {
          sh.cancels.lock().unwrap().push(id);
          reply(&req, Empty::default().encode_to_vec())
        } else {
          error_reply(&req, "ENOENT")
        }
      }
      method::REMOVE_ALL => reply(&req, build!(RemoveAllResponse { removed: 2 }).encode_to_vec()),
      _ => error_reply(&req, "ENOSYS"),
    };
    send(&out, &f);
    if let Some(id) = start_id {
      let eager = sh.eager.lock().unwrap().clone();
      for (mut n, stamp) in eager {
        if stamp {
          n.ref_id = id;
        }
        send(&out, &nframe(n));
      }
    }
  }
}

impl Server {
  fn owner(&self, op: u64) -> Arc<Mutex<UnixStream>> {
    let idx = self.shared.ops.lock().unwrap()[&op].0;
    self.shared.conns.lock().unwrap()[idx].clone()
  }

  fn seq(&self) -> u64 {
    self.shared.seq.fetch_add(1, SeqCst) + 1
  }

  /// Sends a notice for op on its connection. An OP_DONE ends the op.
  fn op_notice(&self, op: u64, kind: NoticeKind, fill: impl FnOnce(&mut Notice)) {
    let mut n = Notice::default();
    n.kind = kind as i32;
    n.ref_id = op;
    n.sequence = self.seq();
    fill(&mut n);
    if kind == NoticeKind::OpDone {
      self.shared.ops.lock().unwrap().get_mut(&op).unwrap().1 = true;
    }
    send(&self.owner(op), &nframe(n));
  }

  fn done(&self, op: u64, fill: impl FnOnce(&mut Notice)) {
    self.op_notice(op, NoticeKind::OpDone, fill);
  }

  fn failed(&self, op: u64, path: &str, code: &str, message: &str) {
    self.op_notice(op, NoticeKind::OpFailed, |n| {
      n.path = path.into();
      n.code = code.into();
      n.message = message.into();
    });
  }

  fn owner_index(&self, op: u64) -> usize {
    self.shared.ops.lock().unwrap()[&op].0
  }

  fn caps(&self, caps: &[&str]) {
    *self.shared.caps.lock().unwrap() = caps.iter().map(|c| (*c).to_string()).collect();
  }
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

fn wait_for(what: &str, f: impl Fn() -> bool) {
  for _ in 0..500 {
    if f() {
      return;
    }
    std::thread::sleep(Duration::from_millis(10));
  }
  panic!("timed out waiting for {what}");
}

fn t() -> Duration {
  Duration::from_secs(3)
}

fn is_server(e: &Error, want: &str) -> bool {
  matches!(e, Error::Server { code, .. } if code == want)
}

fn is_aborted(e: &Error) -> bool {
  matches!(e, Error::Io(e) if e.kind() == io::ErrorKind::ConnectionAborted)
}

#[test]
fn hello_asks_for_every_capability() {
  let srv = start();
  let _c = client(&srv, 1);
  let asked = srv.shared.hello_caps.lock().unwrap().clone();
  for want in [
    "bulk_io",
    "ls_stat",
    "remove_all",
    "watch",
    "warm",
    "pipelining",
    "clone",
    "ops",
  ] {
    assert!(asked.iter().any(|c| c == want), "Hello lacks {want}: {asked:?}");
  }
}

#[test]
fn clone_entry_returns_each_outcome_and_sends_options() {
  let srv = start();
  let c = client(&srv, 1);
  let cases = [
    (WireOutcome::Cloned, CloneOutcome::Cloned),
    (WireOutcome::Overwritten, CloneOutcome::Overwritten),
    (WireOutcome::Skipped, CloneOutcome::Skipped),
  ];
  for (wire, want) in cases {
    srv.shared.outcome.store(wire as i32, SeqCst);
    assert_eq!(c.clone_entry("a/src", "b/dst", &CloneOptions::default()).unwrap(), want);
  }
  let opts =
    CloneOptions::default()
      .with_overwrite(true)
      .with_preserve(vec![Preserve::Times, Preserve::Xattr, Preserve::Acl]);
  c.clone_entry("a", "b", &opts).unwrap();
  let reqs = srv.shared.clones.lock().unwrap();
  assert_eq!((reqs[3].from.as_str(), reqs[3].to.as_str()), ("a", "b"));
  let o = reqs[3].options.as_ref().unwrap();
  assert!(o.overwrite);
  assert_eq!(
    o.preserve,
    vec![
      PreserveKind::Times as i32,
      PreserveKind::Xattr as i32,
      PreserveKind::Acl as i32
    ]
  );
  assert!(!reqs[0].options.as_ref().unwrap().overwrite);
}

type WireOutcome = mountos_native::wire::CloneOutcome;

#[test]
fn clone_entry_errors_carry_the_server_code() {
  let srv = start();
  let c = client(&srv, 1);
  for code in ["EISDIR", "EEXIST", "EPERM", "EXDEV"] {
    *srv.shared.clone_err.lock().unwrap() = code.into();
    let e = c.clone_entry("a", "b", &CloneOptions::default()).unwrap_err();
    assert!(is_server(&e, code), "{e}");
  }
}

#[test]
fn clone_dir_waits_for_counts_and_failures() {
  let srv = start();
  let c = client(&srv, 1);
  let op = c
    .clone_dir("src", "dst", &CloneOptions::default().with_overwrite(true))
    .unwrap();
  assert_eq!(srv.shared.dirs.lock().unwrap()[0].from, "src");
  assert!(srv.shared.dirs.lock().unwrap()[0].options.as_ref().unwrap().overwrite);
  srv.failed(op.id(), "dir/x", "EACCES", "denied");
  srv.done(op.id(), |n| {
    n.cloned = 7;
    n.overwritten = 2;
    n.skipped = 3;
    n.failed = 1;
    n.protected = 4;
  });
  let r = op.wait(t()).unwrap();
  let mut want = OpCounts::default();
  want.cloned = 7;
  want.overwritten = 2;
  want.skipped = 3;
  want.failed = 1;
  want.protected = 4;
  assert_eq!(r.counts, want);
  assert_eq!(r.failures.len(), 1);
  assert_eq!(
    (
      r.failures[0].path.as_str(),
      r.failures[0].code.as_str(),
      r.failures[0].message.as_str()
    ),
    ("dir/x", "EACCES", "denied")
  );
  assert_eq!(op.wait(t()).unwrap(), r);
}

#[test]
fn remove_dir_reports_removed_and_protected() {
  let srv = start();
  let c = client(&srv, 1);
  let op = c.remove_dir("tree").unwrap();
  assert_eq!(srv.shared.removes.lock().unwrap().as_slice(), ["tree"]);
  srv.done(op.id(), |n| {
    n.removed = 12;
    n.protected = 1;
  });
  let r = op.wait(t()).unwrap();
  assert_eq!((r.counts.removed, r.counts.protected), (12, 1));
  assert!(r.failures.is_empty());
}

#[test]
fn notices_right_behind_the_start_reply_are_kept() {
  let srv = start();
  let c = client(&srv, 1);
  let mut failed = Notice::default();
  failed.kind = NoticeKind::OpFailed as i32;
  failed.path = "p".into();
  failed.code = "EIO".into();
  failed.message = "m".into();
  let mut done = Notice::default();
  done.kind = NoticeKind::OpDone as i32;
  done.failed = 1;
  done.cloned = 5;
  *srv.shared.eager.lock().unwrap() = vec![(failed, true), (done, true)];
  for _ in 0..30 {
    let op = c.clone_dir("a", "b", &CloneOptions::default()).unwrap();
    let r = op.wait(t()).unwrap();
    assert_eq!((r.counts.cloned, r.counts.failed), (5, 1));
    assert_eq!(r.failures.len(), 1);
    assert_eq!(r.failures[0].code, "EIO");
  }
}

#[test]
fn failures_are_capped_and_the_server_total_is_kept() {
  let srv = start();
  let c = client(&srv, 1);
  let op = c.remove_dir("tree").unwrap();
  let total = MAX_OP_FAILURES + 36;
  for i in 0..total {
    srv.failed(op.id(), &format!("f{i}"), "EBUSY", "busy");
  }
  srv.done(op.id(), |n| n.failed = total as u64);
  let r = op.wait(t()).unwrap();
  assert_eq!(r.failures.len(), MAX_OP_FAILURES);
  assert_eq!(r.failures[0].path, "f0");
  assert_eq!(
    r.failures[MAX_OP_FAILURES - 1].path,
    format!("f{}", MAX_OP_FAILURES - 1)
  );
  assert_eq!(r.counts.failed, total as u64);
}

fn overflow(srv: &Server, op: u64, reference: u64) {
  let mut n = Notice::default();
  n.kind = NoticeKind::Overflow as i32;
  n.ref_id = reference;
  send(&srv.owner(op), &nframe(n));
}

fn is_unknown(r: &mountos_native::Result<mountos_native::OpResult>) -> bool {
  matches!(r, Err(Error::OperationStateUnknown))
}

#[test]
fn connection_level_overflow_makes_the_state_unknown() {
  let srv = start();
  let c = client(&srv, 1);
  let op = c.remove_dir("tree").unwrap();
  overflow(&srv, op.id(), 0);
  assert!(is_unknown(&op.wait(t())));
  assert!(is_unknown(&op.wait(t())));
}

#[test]
fn an_overflow_that_names_one_operation_spares_the_others() {
  let srv = start();
  let c = client(&srv, 1);
  let a = c.remove_dir("a").unwrap();
  let b = c.remove_dir("b").unwrap();
  overflow(&srv, a.id(), a.id());
  assert!(is_unknown(&a.wait(t())));
  srv.done(b.id(), |n| n.removed = 1);
  assert_eq!(b.wait(t()).unwrap().counts.removed, 1);
}

#[test]
fn an_overflow_right_behind_the_start_reply_is_not_lost() {
  let srv = start();
  let c = client(&srv, 1);
  let mut n = Notice::default();
  n.kind = NoticeKind::Overflow as i32;
  *srv.shared.eager.lock().unwrap() = vec![(n, false)];
  for _ in 0..20 {
    let op = c.remove_dir("tree").unwrap();
    assert!(is_unknown(&op.wait(t())));
  }
}

#[test]
fn an_overflow_on_one_connection_spares_operations_on_another() {
  let srv = start();
  let c = client(&srv, 2);
  let mut ops: Vec<Operation> = Vec::new();
  while ops.len() < 8 {
    let op = c.remove_dir("tree").unwrap();
    let other = srv.owner_index(op.id()) != srv.owner_index(ops.first().map_or(op.id(), Operation::id));
    ops.push(op);
    if other {
      break;
    }
  }
  let (first, last) = (&ops[0], &ops[ops.len() - 1]);
  if srv.owner_index(first.id()) == srv.owner_index(last.id()) {
    return;
  }
  overflow(&srv, first.id(), 0);
  assert!(is_unknown(&first.wait(t())));
  srv.done(last.id(), |n| n.removed = 9);
  assert_eq!(last.wait(t()).unwrap().counts.removed, 9);
}

#[test]
fn a_change_overflow_of_a_watch_never_touches_an_operation() {
  let srv = start();
  let c = client(&srv, 1);
  let op = c.remove_dir("tree").unwrap();
  let mut n = Notice::default();
  n.kind = NoticeKind::Change as i32;
  n.change_kind = WatchEventKind::Overflow as i32;
  send(&srv.owner(op.id()), &nframe(n));
  assert!(matches!(
    op.wait(Duration::from_millis(100)),
    Err(Error::Io(e)) if e.kind() == io::ErrorKind::TimedOut
  ));
  srv.done(op.id(), |n| n.removed = 2);
  assert_eq!(op.wait(t()).unwrap().counts.removed, 2);
}

#[test]
fn an_ended_operation_keeps_its_result_after_an_overflow() {
  let srv = start();
  let c = client(&srv, 1);
  let op = c.remove_dir("tree").unwrap();
  srv.done(op.id(), |n| n.removed = 4);
  op.wait(t()).unwrap();
  overflow(&srv, op.id(), 0);
  std::thread::sleep(Duration::from_millis(50));
  assert_eq!(op.wait(t()).unwrap().counts.removed, 4);
}

#[test]
fn cancel_goes_to_the_owning_connection_and_the_operation_still_ends() {
  let srv = start();
  let c = client(&srv, 3);
  let ops: Vec<Operation> = (0..6)
    .map(|_| c.clone_dir("a", "b", &CloneOptions::default()).unwrap())
    .collect();
  for op in &ops {
    op.cancel().unwrap();
  }
  let ids: Vec<u64> = ops.iter().map(Operation::id).collect();
  assert_eq!(*srv.shared.cancels.lock().unwrap(), ids);
  srv.done(ops[0].id(), |n| {
    n.cloned = 2;
    n.skipped = 1;
  });
  assert_eq!(ops[0].wait(t()).unwrap().counts.cloned, 2);
  assert!(is_server(&ops[0].cancel().unwrap_err(), "ENOENT"));
}

#[test]
fn wait_times_out_and_can_be_called_again() {
  let srv = start();
  let c = client(&srv, 1);
  let op = c.remove_dir("tree").unwrap();
  assert!(matches!(
    op.wait(Duration::from_millis(30)),
    Err(Error::Io(e)) if e.kind() == io::ErrorKind::TimedOut
  ));
  srv.done(op.id(), |n| n.removed = 1);
  assert_eq!(op.wait(t()).unwrap().counts.removed, 1);
}

#[test]
fn a_lost_connection_fails_every_waiter() {
  let srv = start();
  let c = client(&srv, 1);
  let op = Arc::new(c.remove_dir("tree").unwrap());
  let waiters: Vec<_> = (0..3)
    .map(|_| {
      let op = op.clone();
      std::thread::spawn(move || op.wait(Duration::from_secs(5)))
    })
    .collect();
  std::thread::sleep(Duration::from_millis(50));
  let _ = srv.owner(op.id()).lock().unwrap().shutdown(Shutdown::Both);
  for w in waiters {
    let e = w.join().unwrap().unwrap_err();
    assert!(is_aborted(&e), "{e}");
  }
  assert!(is_aborted(&op.wait(t()).unwrap_err()));
}

#[test]
fn closing_the_client_fails_pending_waiters() {
  let srv = start();
  let c = client(&srv, 1);
  let op = Arc::new(c.remove_dir("tree").unwrap());
  let o = op.clone();
  let w = std::thread::spawn(move || o.wait(Duration::from_secs(5)));
  std::thread::sleep(Duration::from_millis(50));
  c.close();
  let e = w.join().unwrap().unwrap_err();
  assert!(is_aborted(&e), "{e}");
}

#[test]
fn the_operation_limit_is_eagain() {
  let srv = start();
  let c = client(&srv, 1);
  srv.shared.full.store(true, SeqCst);
  assert!(is_server(
    &c.clone_dir("a", "b", &CloneOptions::default()).err().unwrap(),
    "EAGAIN"
  ));
  assert!(is_server(&c.remove_dir("a").err().unwrap(), "EAGAIN"));
}

#[test]
fn a_missing_capability_fails_locally() {
  let srv = start();
  srv.caps(&["bulk_io", "warm"]);
  let c = client(&srv, 1);
  assert!(is_server(
    &c.clone_entry("a", "b", &CloneOptions::default()).unwrap_err(),
    "UNSUPPORTED_OPERATION"
  ));
  assert!(is_server(
    &c.clone_dir("a", "b", &CloneOptions::default()).err().unwrap(),
    "UNSUPPORTED_OPERATION"
  ));
  assert!(is_server(&c.remove_dir("a").err().unwrap(), "UNSUPPORTED_OPERATION"));
  assert_eq!(srv.shared.op_requests.load(SeqCst), 0);
}

#[test]
fn remove_all_still_works() {
  let srv = start();
  srv.caps(&["remove_all"]);
  let c = client(&srv, 1);
  assert_eq!(c.remove_all("/t").unwrap(), 2);
}

#[test]
fn listeners_get_op_events_and_nothing_is_logged() {
  let srv = start();
  let (c, log) = client_with(&srv, 1);
  let ev: Arc<Mutex<Vec<Event>>> = Arc::default();
  let e = ev.clone();
  c.add_listener(move |x| e.lock().unwrap().push(x.clone()));
  let op = c.remove_dir("tree").unwrap();
  srv.failed(op.id(), "x", "EIO", "bad");
  srv.done(op.id(), |n| {
    n.removed = 2;
    n.failed = 1;
  });
  wait_for("two events", || ev.lock().unwrap().len() == 2);
  let v = ev.lock().unwrap();
  assert_eq!((v[0].kind, v[0].path.as_str()), (EventKind::OpFailed, "x"));
  assert!(matches!(&v[0].error, Some(Error::Server { code, .. }) if code == "EIO"));
  assert_eq!(v[1].kind, EventKind::OpDone);
  let counts = v[1].counts.unwrap();
  assert_eq!((counts.removed, counts.failed), (2, 1));
  assert!(log.lock().unwrap().is_empty());
}

#[test]
fn op_notices_are_not_logged_without_a_listener() {
  let srv = start();
  let (c, log) = client_with(&srv, 1);
  let op = c.remove_dir("tree").unwrap();
  for _ in 0..5 {
    srv.failed(op.id(), "x", "EIO", "bad");
  }
  srv.done(op.id(), |n| n.failed = 5);
  op.wait(t()).unwrap();
  std::thread::sleep(Duration::from_millis(50));
  assert!(log.lock().unwrap().is_empty(), "{:?}", log.lock().unwrap());
}

#[test]
fn ended_and_lost_operations_leave_no_record() {
  let srv = start();
  let c = client(&srv, 1);
  let a = c.remove_dir("a").unwrap();
  let b = c.remove_dir("b").unwrap();
  let d = c.remove_dir("d").unwrap();
  assert_eq!(c.pending_ops(), 3);
  srv.done(a.id(), |n| n.removed = 1);
  a.wait(t()).unwrap();
  assert_eq!(c.pending_ops(), 2);
  overflow(&srv, b.id(), b.id());
  assert!(is_unknown(&b.wait(t())));
  assert_eq!(c.pending_ops(), 1);
  let _ = srv.owner(d.id()).lock().unwrap().shutdown(Shutdown::Both);
  assert!(d.wait(t()).is_err());
  assert_eq!(c.pending_ops(), 0);
}
