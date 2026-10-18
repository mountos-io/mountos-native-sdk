//! Hostile server tests. After Hello the first connection misbehaves. A call must fail with a
//! typed error in bounded time, and the client must work again on a healthy connection.

use mountos_native::frame::{self, Frame, FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE};
use mountos_native::wire::*;
use mountos_native::{open_flag, Client, Error, Options, Range};
use prost::Message;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

static SOCK_SEQ: AtomicUsize = AtomicUsize::new(0);

/// Returns false when the connection is done. The server then waits for the client to close it.
type Act = fn(&mut UnixStream, &Frame) -> bool;

struct Server {
  path: PathBuf,
  accepted: Arc<AtomicUsize>,
  finished: Arc<AtomicUsize>,
}

impl Drop for Server {
  fn drop(&mut self) {
    let _ = std::fs::remove_file(&self.path);
  }
}

fn header(flags: u8, method: u16, id: u64, meta_len: u32, payload_len: u32) -> Vec<u8> {
  let mut h = Vec::with_capacity(24);
  h.extend_from_slice(b"MFS1");
  h.push(1);
  h.push(flags);
  h.extend_from_slice(&method.to_be_bytes());
  h.extend_from_slice(&id.to_be_bytes());
  h.extend_from_slice(&meta_len.to_be_bytes());
  h.extend_from_slice(&payload_len.to_be_bytes());
  h
}

fn reply_bytes(flags: u8, method: u16, id: u64, meta: &[u8]) -> Vec<u8> {
  let mut b = header(flags, method, id, meta.len() as u32, 0);
  b.extend_from_slice(meta);
  b
}

fn good(f: &Frame) -> Vec<u8> {
  reply_bytes(FLAG_RESPONSE, f.method, f.request_id, &[])
}

fn send_ok(s: &mut UnixStream, f: &Frame, meta: Vec<u8>) {
  let _ = frame::write(s, &Frame::new(FLAG_RESPONSE, f.method, f.request_id, meta, vec![]));
}

fn start(act: Act) -> Server {
  let path = PathBuf::from(format!(
    "/tmp/mn-hostile-{}-{}.sock",
    std::process::id(),
    SOCK_SEQ.fetch_add(1, SeqCst)
  ));
  let _ = std::fs::remove_file(&path);
  let l = UnixListener::bind(&path).unwrap();
  let accepted = Arc::new(AtomicUsize::new(0));
  let finished = Arc::new(AtomicUsize::new(0));
  let (a, fin) = (accepted.clone(), finished.clone());
  std::thread::spawn(move || {
    for s in l.incoming() {
      let mut s = s.unwrap();
      let hostile = a.fetch_add(1, SeqCst) == 0;
      let fin = fin.clone();
      std::thread::spawn(move || {
        serve(&mut s, hostile, act);
        let _ = s.shutdown(std::net::Shutdown::Both);
        fin.fetch_add(1, SeqCst);
      });
    }
  });
  Server {
    path,
    accepted,
    finished,
  }
}

fn serve(s: &mut UnixStream, hostile: bool, act: Act) {
  while let Ok(Some(f)) = frame::read(s) {
    if f.method == method::HELLO {
      let m = HelloResponse {
        version: 1,
        capabilities: vec![],
        max_payload_bytes: 1 << 20,
        max_inflight: 1,
      };
      send_ok(s, &f, m.encode_to_vec());
    } else if !hostile {
      send_ok(s, &f, vec![]);
    } else if !act(s, &f) {
      // Hold the socket until the client closes it.
      let mut sink = [0u8; 1024];
      while matches!(s.read(&mut sink), Ok(n) if n > 0) {}
      return;
    }
  }
}

fn logged() -> (Options, Arc<Mutex<Vec<String>>>) {
  let lines = Arc::new(Mutex::new(Vec::new()));
  let l = lines.clone();
  let opts = Options::default()
    .with_pool_size(1)
    .with_logger(Arc::new(move |m: &str| l.lock().unwrap().push(m.to_string())));
  (opts, lines)
}

/// Runs a call on its own thread. A hang or a panic fails the test.
fn within<T: Send + 'static>(name: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
  let (tx, rx) = mpsc::channel();
  std::thread::spawn(move || {
    let _ = tx.send(f());
  });
  match rx.recv_timeout(Duration::from_secs(5)) {
    Ok(v) => v,
    Err(mpsc::RecvTimeoutError::Timeout) => panic!("{name}: call did not return in 5 s"),
    Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{name}: call panicked"),
  }
}

#[derive(Clone, Copy)]
enum Want {
  /// Success is allowed.
  Any,
  Protocol,
  Io,
  Timeout,
}

fn matches_want(w: Want, e: &Error) -> bool {
  matches!(
    (w, e),
    (Want::Any, _)
      | (Want::Protocol, Error::Protocol(_))
      | (Want::Io, Error::Io(_))
      | (Want::Timeout, Error::Timeout(_))
  )
}

struct Case {
  name: &'static str,
  act: Act,
  call: fn(&Client) -> Result<(), Error>,
  timeout: Duration,
  want: Want,
  /// Calls after the hostile one that may fail before the client works again.
  retries: usize,
}

fn ping(c: &Client) -> Result<(), Error> {
  c.ping()
}

const NO_LIMIT: Duration = Duration::ZERO;
const SHORT: Duration = Duration::from_millis(300);

fn garbage(s: &mut UnixStream, _: &Frame) -> bool {
  let _ = s.write_all(&[0x5a; 128]);
  false
}

fn bad_version(s: &mut UnixStream, f: &Frame) -> bool {
  let mut h = header(FLAG_RESPONSE, f.method, f.request_id, 0, 0);
  h[4] = 9;
  let _ = s.write_all(&h);
  false
}

fn claim_over_limit(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&header(
    FLAG_RESPONSE,
    f.method,
    f.request_id,
    0,
    frame::MAX_PAYLOAD_BYTES + 1,
  ));
  false
}

fn claim_then_stall(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&header(
    FLAG_RESPONSE,
    f.method,
    f.request_id,
    0,
    frame::MAX_PAYLOAD_BYTES,
  ));
  let _ = s.write_all(&[0u8; 1000]);
  false
}

fn meta_claim_then_stall(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&header(FLAG_RESPONSE, f.method, f.request_id, frame::MAX_META_BYTES, 0));
  false
}

fn truncated_header(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&header(FLAG_RESPONSE, f.method, f.request_id, 0, 0)[..11]);
  let _ = s.shutdown(std::net::Shutdown::Both);
  false
}

fn truncated_body(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&header(FLAG_RESPONSE, f.method, f.request_id, 40, 0));
  let _ = s.write_all(b"short");
  let _ = s.shutdown(std::net::Shutdown::Both);
  false
}

fn wrong_id(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&reply_bytes(FLAG_RESPONSE, f.method, f.request_id + 1, &[]));
  false
}

fn id_zero(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&reply_bytes(FLAG_RESPONSE, f.method, 0, &[]));
  false
}

fn reply_twice(s: &mut UnixStream, f: &Frame) -> bool {
  let mut b = good(f);
  b.extend(good(f));
  let _ = s.write_all(&b);
  false
}

fn wrong_method(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&reply_bytes(FLAG_RESPONSE, f.method + 1, f.request_id, &[]));
  false
}

fn no_response_flag(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&reply_bytes(0, f.method, f.request_id, &[]));
  false
}

fn bad_error_meta(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&reply_bytes(
    FLAG_RESPONSE | FLAG_ERROR,
    f.method,
    f.request_id,
    &[0xff; 5],
  ));
  false
}

fn bad_response_meta(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&reply_bytes(
    FLAG_RESPONSE,
    f.method,
    f.request_id,
    &[0x0a, 0xff, 0xff, 0xff, 0x0f],
  ));
  false
}

fn empty_ok(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&good(f));
  true
}

fn unknown_notice(s: &mut UnixStream, f: &Frame) -> bool {
  let n = Notice {
    kind: 9999,
    sequence: 1,
    ..Notice::default()
  };
  let mut b = reply_bytes(FLAG_NOTIFY, 62, f.request_id, &n.encode_to_vec());
  b.extend(reply_bytes(FLAG_NOTIFY, 62, f.request_id, &[0xff, 0xff, 0xff]));
  b.extend(good(f));
  let _ = s.write_all(&b);
  true
}

fn close_now(s: &mut UnixStream, _: &Frame) -> bool {
  let _ = s.shutdown(std::net::Shutdown::Both);
  false
}

fn close_in_body(s: &mut UnixStream, f: &Frame) -> bool {
  let _ = s.write_all(&header(FLAG_RESPONSE, f.method, f.request_id, 0, 1 << 20));
  let _ = s.write_all(&vec![0u8; 100 << 10]);
  let _ = s.shutdown(std::net::Shutdown::Both);
  false
}

fn open_then_close(s: &mut UnixStream, f: &Frame) -> bool {
  if f.method == method::OPEN {
    send_ok(s, f, OpenResponse { handle: 1 }.encode_to_vec());
    return true;
  }
  let _ = s.shutdown(std::net::Shutdown::Both);
  false
}

fn silent(_: &mut UnixStream, _: &Frame) -> bool {
  false
}

fn cases() -> Vec<Case> {
  let mk = |name, act, call, timeout, want, retries| Case {
    name,
    act,
    call,
    timeout,
    want,
    retries,
  };
  vec![
    mk("garbage bytes", garbage, ping, NO_LIMIT, Want::Protocol, 0),
    mk("bad version", bad_version, ping, NO_LIMIT, Want::Protocol, 0),
    mk(
      "claim over the limit",
      claim_over_limit,
      ping,
      NO_LIMIT,
      Want::Protocol,
      0,
    ),
    mk("huge claim then stall", claim_then_stall, ping, SHORT, Want::Timeout, 0),
    mk(
      "huge meta claim then stall",
      meta_claim_then_stall,
      ping,
      SHORT,
      Want::Timeout,
      0,
    ),
    mk(
      "truncated header then close",
      truncated_header,
      ping,
      NO_LIMIT,
      Want::Io,
      0,
    ),
    mk("truncated body then close", truncated_body, ping, NO_LIMIT, Want::Io, 0),
    mk("wrong request id", wrong_id, ping, NO_LIMIT, Want::Protocol, 0),
    mk("request id zero", id_zero, ping, NO_LIMIT, Want::Protocol, 0),
    mk("reply twice", reply_twice, ping, NO_LIMIT, Want::Any, 2),
    mk("wrong method id", wrong_method, ping, NO_LIMIT, Want::Protocol, 0),
    mk(
      "missing response flag",
      no_response_flag,
      ping,
      NO_LIMIT,
      Want::Protocol,
      0,
    ),
    mk(
      "error frame with undecodable meta",
      bad_error_meta,
      ping,
      NO_LIMIT,
      Want::Protocol,
      0,
    ),
    mk(
      "response with undecodable meta",
      bad_response_meta,
      |c| c.stat("/x").map(|_| ()),
      NO_LIMIT,
      Want::Protocol,
      0,
    ),
    mk(
      "stat response without info",
      empty_ok,
      |c| c.stat("/x").map(|_| ()),
      NO_LIMIT,
      Want::Protocol,
      0,
    ),
    mk("notice with unknown kind", unknown_notice, ping, NO_LIMIT, Want::Any, 0),
    mk(
      "close in StatBatch",
      close_now,
      |c| c.stat_batch(&["/a".into(), "/b".into()]).map(|_| ()),
      NO_LIMIT,
      Want::Io,
      0,
    ),
    mk(
      "close in ReadFile body",
      close_in_body,
      |c| c.read_file("/a").map(|_| ()),
      NO_LIMIT,
      Want::Io,
      0,
    ),
    mk(
      "close in ReadBatch",
      open_then_close,
      |c| {
        let f = c.open("/a", open_flag::READ_ONLY, 0)?;
        f.read_batch(&[Range::new(0, 10), Range::new(10, 10)]).map(|_| ())
      },
      NO_LIMIT,
      Want::Io,
      0,
    ),
    mk(
      "never replies",
      silent,
      ping,
      Duration::from_millis(200),
      Want::Timeout,
      0,
    ),
    mk(
      "never replies to bulk",
      silent,
      |c| c.stat_batch(&["/a".into()]).map(|_| ()),
      Duration::from_millis(200),
      Want::Timeout,
      0,
    ),
  ]
}

#[test]
fn hostile_server_cases() {
  for tc in cases() {
    let srv = start(tc.act);
    let (mut opts, _lines) = logged();
    opts.call_timeout = tc.timeout;
    let c = Client::connect_with(&srv.path, opts).unwrap();
    let probe = c.thread_probe();
    let started = Instant::now();
    let call = tc.call;
    let cc = c.clone();
    let res = within(tc.name, move || call(&cc));
    assert!(started.elapsed() < Duration::from_secs(5), "{}: too slow", tc.name);
    if let Err(e) = &res {
      assert!(matches_want(tc.want, e), "{}: unexpected error {e:?}", tc.name);
      assert!(!e.to_string().is_empty(), "{}: empty message", tc.name);
    } else {
      assert!(
        matches!(tc.want, Want::Any),
        "{}: call succeeded against a hostile server",
        tc.name
      );
    }
    let mut recovered = false;
    for _ in 0..=tc.retries {
      let cc = c.clone();
      if within(tc.name, move || cc.ping()).is_ok() {
        recovered = true;
        break;
      }
    }
    assert!(recovered, "{}: client did not recover on a healthy connection", tc.name);
    c.close();
    assert_eq!(probe.load(SeqCst), 0, "{}: threads left after close", tc.name);
    let n = srv.accepted.load(SeqCst);
    let end = Instant::now() + Duration::from_secs(3);
    while srv.finished.load(SeqCst) < n && Instant::now() < end {
      std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
      srv.finished.load(SeqCst),
      n,
      "{}: server connection still open after close",
      tc.name
    );
  }
}

#[test]
fn unknown_notice_is_logged_and_dropped() {
  let srv = start(unknown_notice);
  let (opts, lines) = logged();
  let c = Client::connect_with(&srv.path, opts).unwrap();
  let seen = Arc::new(AtomicUsize::new(0));
  let s2 = seen.clone();
  c.add_listener(move |_| {
    s2.fetch_add(1, SeqCst);
  });
  c.ping().unwrap();
  std::thread::sleep(Duration::from_millis(50));
  assert_eq!(seen.load(SeqCst), 0);
  let l = lines.lock().unwrap().join("|");
  assert!(l.contains("unknown kind"), "{l}");
  assert!(l.contains("undecodable notice"), "{l}");
}
