use crate::dir::Dir;
use crate::error::{Error, Result};
use crate::events::{lock, Chan, Event, EventKind, Hub, Logger};
use crate::file::File;
use crate::frame::{self, Frame, FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE, MAX_PAYLOAD_BYTES};
use crate::ops::OpState;
use crate::wire::*;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Client settings. Start from `Options::default()` and chain the `with_` methods. New fields
/// can be added in a minor version, so a struct literal does not compile outside this crate.
#[derive(Clone)]
#[non_exhaustive]
pub struct Options {
  /// Number of connections. Calls on different files can run in parallel up to this number.
  pub pool_size: usize,
  pub client_name: String,
  /// Receives log lines: events nobody listens for, listener panics and dropped events.
  /// The line has no prefix and no newline. When `None`, lines go to stderr with the prefix
  /// "mountos: ". Pass your own function to route them elsewhere.
  pub logger: Option<Logger>,
  /// Longest a single request may take, from the end of the send to the reply. `Duration::ZERO`
  /// (the default) means no limit. A call that runs over fails with `Error::Timeout`. When the
  /// connection pipelines, only that call fails: its id is marked abandoned and the late reply is
  /// dropped. Otherwise, or when too many ids are abandoned, the connection is dropped and dialed
  /// again on the next call, and its files and watches are gone. A call that sends several
  /// requests (a large read or write) gets the limit for each request.
  pub call_timeout: Duration,
  /// Most requests outstanding on one connection when the server pipelines. The server limit
  /// (`max_inflight` in its Hello reply) also applies. Values are held between 1 and 1024. A
  /// server without the pipelining capability gets one request at a time.
  pub max_inflight: usize,
}

/// Default for `Options::max_inflight`.
pub const DEFAULT_MAX_INFLIGHT: usize = 32;
const MAX_INFLIGHT_CEILING: usize = 1024;

/// Alias of `Options`.
pub type ClientOptions = Options;

impl Default for Options {
  fn default() -> Options {
    Options {
      pool_size: 4,
      client_name: concat!("mountos-native-rust/", env!("CARGO_PKG_VERSION")).into(),
      logger: None,
      call_timeout: Duration::ZERO,
      max_inflight: DEFAULT_MAX_INFLIGHT,
    }
  }
}

impl Options {
  /// Sets `pool_size`.
  pub fn with_pool_size(mut self, pool_size: usize) -> Options {
    self.pool_size = pool_size;
    self
  }

  /// Sets `logger`.
  pub fn with_logger(mut self, logger: Logger) -> Options {
    self.logger = Some(logger);
    self
  }

  /// Sets `call_timeout`.
  pub fn with_call_timeout(mut self, call_timeout: Duration) -> Options {
    self.call_timeout = call_timeout;
    self
  }

  /// Sets `max_inflight`.
  pub fn with_max_inflight(mut self, max_inflight: usize) -> Options {
    self.max_inflight = max_inflight;
    self
  }
}

impl fmt::Debug for Options {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Options")
      .field("pool_size", &self.pool_size)
      .field("client_name", &self.client_name)
      .field("logger", &self.logger.as_ref().map(|_| "<fn>"))
      .field("call_timeout", &self.call_timeout)
      .field("max_inflight", &self.max_inflight)
      .finish()
  }
}

fn aborted(msg: &str) -> Error {
  Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, msg.to_string()))
}

/// Joins a thread unless it is the calling one, which would wait for itself.
fn join_other(h: JoinHandle<()>) {
  if h.thread().id() != std::thread::current().id() {
    let _ = h.join();
  }
}

/// Capability the server must report for a method, if any.
fn required_capability(m: u16) -> Option<&'static str> {
  match m {
    method::LS => Some("ls_stat"),
    method::STAT_BATCH | method::READ_BATCH | method::WRITE_BATCH | method::READ_FILE | method::WRITE_FILE => {
      Some("bulk_io")
    }
    method::REMOVE_ALL => Some("remove_all"),
    method::WATCH => Some("watch"),
    method::WARM_OPEN | method::WARM_READ => Some("warm"),
    method::CLONE => Some("clone"),
    method::CLONE_DIR | method::REMOVE_DIR | method::OP_CANCEL => Some("ops"),
    _ => None,
  }
}

/// Capabilities asked for in Hello. The server answers with the ones it offers.
const CLIENT_CAPABILITIES: [&str; 8] = [
  "bulk_io",
  "ls_stat",
  "remove_all",
  "watch",
  "warm",
  "pipelining",
  "clone",
  "ops",
];

/// Turns a socket write that hit the send timeout into `Error::Timeout`.
fn write_timeout(e: Error) -> Error {
  match e {
    Error::Io(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
      Error::Timeout(format!("send: {e}"))
    }
    e => e,
  }
}

static CONN_SEQ: AtomicU64 = AtomicU64::new(1);

/// One request in flight. The caller waits on it, the reader fills it.
struct Waiter {
  method: u16,
  /// Queue that receives the notices of a watch started by this request.
  watch: Option<Arc<Chan>>,
  /// Record of the operation started by this request.
  op: Option<Arc<OpState>>,
  out: Mutex<Option<Result<Frame>>>,
  cv: Condvar,
}

impl Waiter {
  fn set(&self, r: Result<Frame>) {
    *lock(&self.out) = Some(r);
    self.cv.notify_one();
  }

  /// Waits for the result. `None` means the deadline passed first.
  fn wait(&self, deadline: Option<Instant>) -> Option<Result<Frame>> {
    let mut g = lock(&self.out);
    loop {
      if let Some(r) = g.take() {
        return Some(r);
      }
      g = match deadline {
        None => self.cv.wait(g).unwrap_or_else(|e| e.into_inner()),
        Some(d) => {
          let left = d.saturating_duration_since(Instant::now());
          if left.is_zero() {
            return None;
          }
          self.cv.wait_timeout(g, left).unwrap_or_else(|e| e.into_inner()).0
        }
      };
    }
  }
}

#[derive(Default)]
struct Core {
  pending: HashMap<u64, Arc<Waiter>>,
  /// Ids whose caller gave up, with the method. A late reply is dropped (an Open, Opendir or
  /// Watch reply also releases what it created).
  abandoned: HashMap<u64, u16>,
  next_id: u64,
  /// Why the connection ended. Set once.
  end: Option<Error>,
}

/// What the Hello reply settled.
struct Info {
  max_payload: usize,
  /// Requests that may be outstanding at once.
  limit: usize,
  /// Capabilities the server reported. `None` when it reported none (an older server), which
  /// allows every call.
  caps: Option<HashSet<String>>,
  /// Server instance id. 0 from an older server.
  instance: u64,
}

/// One handshaken connection. Calls from many threads overlap up to the limit. Request frames are
/// written under a mutex. One reader thread reads every frame: replies go to the waiting call by
/// request id, notices go to the hub.
pub(crate) struct Sock {
  id: u64,
  hub: Arc<Hub>,
  /// Per-request limit, zero for none.
  timeout: Duration,
  writer: Mutex<UnixStream>,
  /// Second handle on the socket. Closes the socket from another thread.
  killer: UnixStream,
  core: Mutex<Core>,
  dead: AtomicBool,
  /// Set when this side ends the connection on purpose.
  closing: AtomicBool,
  /// The server sent Closing, so the end that follows is expected.
  saw_closing: AtomicBool,
  info: OnceLock<Info>,
  reader: Mutex<Option<JoinHandle<()>>>,
  /// Threads that release a handle or watch created by a request that timed out.
  releasers: Mutex<Vec<JoinHandle<()>>>,
}

impl Sock {
  /// Connects and starts the reader. The handshake is separate so a caller can register the
  /// socket for `close` before it blocks.
  fn open(path: &Path, hub: &Arc<Hub>, timeout: Duration) -> Result<Arc<Sock>> {
    let stream = UnixStream::connect(path)?;
    if !timeout.is_zero() {
      stream.set_write_timeout(Some(timeout))?;
    }
    let (rs, killer) = (stream.try_clone()?, stream.try_clone()?);
    let sock = Arc::new(Sock {
      id: CONN_SEQ.fetch_add(1, Ordering::SeqCst),
      hub: hub.clone(),
      timeout,
      writer: Mutex::new(stream),
      killer,
      core: Mutex::new(Core::default()),
      dead: AtomicBool::new(false),
      closing: AtomicBool::new(false),
      saw_closing: AtomicBool::new(false),
      info: OnceLock::new(),
      reader: Mutex::new(None),
      releasers: Mutex::new(Vec::new()),
    });
    let (rsock, guard) = (sock.clone(), hub.thread_guard());
    let h = std::thread::Builder::new()
      .name("mountos-reader".into())
      .spawn(move || {
        let _guard = guard;
        read_loop(rs, &rsock)
      })?;
    *lock(&sock.reader) = Some(h);
    Ok(sock)
  }

  fn handshake(&self, name: &str, max_inflight: usize) -> Result<()> {
    let req = HelloRequest {
      client_name: name.into(),
      min_version: 1,
      max_version: 1,
      capabilities: CLIENT_CAPABILITIES.iter().map(|c| (*c).into()).collect(),
    };
    let (resp, _): (HelloResponse, _) = self.call_msg(method::HELLO, &req, &[])?;
    if resp.version != 1 {
      return Err(Error::Protocol(format!("server chose version {}", resp.version)));
    }
    let mut max_payload = MAX_PAYLOAD_BYTES as usize;
    if resp.max_payload_bytes > 0 {
      max_payload = max_payload.min(resp.max_payload_bytes as usize);
    }
    let pipelined = resp.capabilities.iter().any(|c| c == "pipelining");
    let limit = if pipelined {
      max_inflight.min(resp.max_inflight as usize).max(1)
    } else {
      1
    };
    let instance = resp.instance_id;
    let caps = (!resp.capabilities.is_empty()).then(|| resp.capabilities.into_iter().collect());
    let _ = self.info.set(Info {
      max_payload,
      limit,
      caps,
      instance,
    });
    Ok(())
  }

  pub(crate) fn max_payload(&self) -> usize {
    self.info.get().map_or(MAX_PAYLOAD_BYTES as usize, |i| i.max_payload)
  }

  fn limit(&self) -> usize {
    self.info.get().map_or(1, |i| i.limit)
  }

  fn instance(&self) -> u64 {
    self.info.get().map_or(0, |i| i.instance)
  }

  fn is_dead(&self) -> bool {
    self.dead.load(Ordering::SeqCst)
  }

  /// Ends the connection once: the first reason wins. Every waiting call gets `end`.
  fn fail(&self, end: Error, report: bool) {
    let waiters: Vec<Arc<Waiter>> = {
      let mut core = lock(&self.core);
      if core.end.is_some() {
        return;
      }
      core.end = Some(end.dup());
      self.dead.store(true, Ordering::SeqCst);
      core.abandoned.clear();
      core.pending.drain().map(|(_, w)| w).collect()
    };
    let _ = self.killer.shutdown(Shutdown::Both);
    for w in waiters {
      w.set(Err(end.dup()));
    }
    self.hub.conn_lost(self.id);
    self.hub.ops_lost(self.id, &end);
    let expected = self.closing.load(Ordering::SeqCst) || self.saw_closing.load(Ordering::SeqCst);
    if report && !expected {
      self
        .hub
        .publish(None, Event::local(EventKind::ConnectionLost, 0, Some(end.dup())));
    }
  }

  /// Ends the connection on purpose.
  pub(crate) fn kill(&self) {
    self.closing.store(true, Ordering::SeqCst);
    self.fail(aborted("connection closed"), false);
  }

  /// Ends the connection because of a local fault. Waiting calls get a copy of `e`.
  fn kill_with(&self, e: &Error) {
    self.closing.store(true, Ordering::SeqCst);
    self.fail(e.dup(), false);
  }

  /// Ends the connection and joins its threads.
  fn finish(&self) {
    self.kill();
    let reader = lock(&self.reader).take();
    if let Some(h) = reader {
      join_other(h);
    }
    let releasers = std::mem::take(&mut *lock(&self.releasers));
    for h in releasers {
      join_other(h);
    }
  }

  fn forget(&self, id: u64) {
    lock(&self.core).pending.remove(&id);
  }

  /// Sends one request and waits for its response frame. The sizes and the capability are
  /// checked before anything is sent, so those failures leave the connection intact.
  fn call(
    &self,
    method: u16,
    req: &impl Message,
    payload: &[u8],
    attach: (Option<Arc<Chan>>, Option<Arc<OpState>>),
  ) -> Result<Frame> {
    let (watch, op) = attach;
    if let Some(cap) = required_capability(method) {
      if self
        .info
        .get()
        .and_then(|i| i.caps.as_ref())
        .is_some_and(|c| !c.contains(cap))
      {
        return Err(Error::server(
          "UNSUPPORTED_OPERATION",
          &format!("the server does not report capability {cap}"),
        ));
      }
    }
    let meta = req.encode_to_vec();
    frame::check_limits(meta.len(), payload.len())?;
    let w = Arc::new(Waiter {
      method,
      watch,
      op,
      out: Mutex::new(None),
      cv: Condvar::new(),
    });
    let id = {
      let mut core = lock(&self.core);
      if let Some(e) = &core.end {
        return Err(e.dup());
      }
      core.next_id += 1;
      let id = core.next_id;
      core.pending.insert(id, w.clone());
      id
    };
    let sent = {
      let mut s = lock(&self.writer);
      frame::write_parts(&mut *s, 0, method, id, &meta, payload).map_err(write_timeout)
    };
    if let Err(e) = sent {
      // A failed write may leave half a frame on the wire, so the connection is unusable.
      self.forget(id);
      self.kill_with(&aborted(&format!("send failed: {e}")));
      return Err(e);
    }
    let deadline = (!self.timeout.is_zero()).then(|| Instant::now() + self.timeout);
    let resp = match w.wait(deadline) {
      Some(r) => r?,
      None => self.give_up(id, method, &w)?,
    };
    if resp.flags & FLAG_ERROR != 0 {
      let e = match WireError::decode(&resp.meta[..]) {
        Ok(e) => e,
        Err(e) => {
          let e = Error::Protocol(format!("bad error meta: {e}"));
          self.kill_with(&e);
          return Err(e);
        }
      };
      return Err(Error::Server {
        code: e.code,
        message: e.message,
      });
    }
    Ok(resp)
  }

  /// Handles a call whose wait ran out. With room in the abandoned set the connection stays up
  /// and the late reply is dropped. Otherwise the connection is cut.
  fn give_up(&self, id: u64, method: u16, w: &Waiter) -> Result<Frame> {
    let limit = self.limit();
    let kept = {
      let mut core = lock(&self.core);
      if core.pending.remove(&id).is_none() {
        None
      } else if limit > 1 && core.abandoned.len() < limit * 4 {
        core.abandoned.insert(id, method);
        Some(true)
      } else {
        Some(false)
      }
    };
    match kept {
      // The reply or the failure won the race and is being delivered.
      None => w.wait(None).unwrap_or_else(|| Err(aborted("connection closed"))),
      Some(keep) => {
        if !keep {
          self.kill();
        }
        Err(Error::Timeout(format!(
          "method {method} did not finish within {:?}",
          self.timeout
        )))
      }
    }
  }

  /// Like `call`, decoding the meta as `R` and returning the reply payload.
  pub(crate) fn call_msg<R: Message + Default>(
    &self,
    method: u16,
    req: &impl Message,
    payload: &[u8],
  ) -> Result<(R, Vec<u8>)> {
    self.decode(self.call(method, req, payload, (None, None))?)
  }

  /// Starts a watch. The reader routes the notices of the new watch to `chan` before the call returns.
  pub(crate) fn call_watch(&self, req: &WatchRequest, chan: Arc<Chan>) -> Result<WatchResponse> {
    Ok(self.decode(self.call(method::WATCH, req, &[], (Some(chan), None))?)?.0)
  }

  /// Starts an operation. The reader registers it with the hub before the call returns, so no
  /// notice of the operation can come first. `id_of` reads the op id from the reply.
  pub(crate) fn call_op<R: Message + Default>(
    &self,
    method: u16,
    req: &impl Message,
    st: Arc<OpState>,
    id_of: impl Fn(&R) -> u64,
  ) -> Result<u64> {
    let r: R = self.decode(self.call(method, req, &[], (None, Some(st)))?)?.0;
    Ok(id_of(&r))
  }

  fn decode<R: Message + Default>(&self, resp: Frame) -> Result<(R, Vec<u8>)> {
    match R::decode(&resp.meta[..]) {
      Ok(msg) => Ok((msg, resp.payload)),
      Err(e) => {
        let e = Error::Protocol(format!("bad response meta: {e}"));
        self.kill_with(&e);
        Err(e)
      }
    }
  }

  /// Routes one non-notice frame from the reader. An error is a protocol violation.
  fn deliver(self: &Arc<Self>, hub: &Hub, f: Frame) -> Result<()> {
    let unexpected = |f: &Frame| Error::Protocol(format!("unexpected frame: method {} id {}", f.method, f.request_id));
    if f.flags & FLAG_RESPONSE == 0 {
      return Err(unexpected(&f));
    }
    let waiter = {
      let mut core = lock(&self.core);
      match core.pending.get(&f.request_id).map(|w| w.method) {
        Some(m) if m == f.method => core.pending.remove(&f.request_id),
        Some(_) => return Err(unexpected(&f)),
        None => {
          if core.abandoned.get(&f.request_id) != Some(&f.method) {
            return Err(unexpected(&f));
          }
          core.abandoned.remove(&f.request_id);
          None
        }
      }
    };
    let ok = f.flags & (FLAG_RESPONSE | FLAG_ERROR) == FLAG_RESPONSE;
    match waiter {
      Some(w) => {
        if ok && f.method == method::WATCH {
          if let (Ok(r), Some(chan)) = (WatchResponse::decode(&f.meta[..]), &w.watch) {
            hub.add_watch(r.watch_id, self.id, chan.clone());
          }
        }
        if ok {
          self.register_op(hub, &f, &w);
        }
        w.set(Ok(f));
      }
      None if ok => self.release_late(f),
      None => {}
    }
    Ok(())
  }

  /// Hands the operation a start reply created to the hub. When the connection ended in the
  /// meantime, the operation ends at once, because no notice can reach it.
  fn register_op(&self, hub: &Hub, f: &Frame, w: &Waiter) {
    let Some(st) = &w.op else { return };
    let id = match f.method {
      method::CLONE_DIR => CloneDirResponse::decode(&f.meta[..]).map(|r| r.op_id),
      method::REMOVE_DIR => RemoveDirResponse::decode(&f.meta[..]).map(|r| r.op_id),
      _ => return,
    };
    let Ok(id) = id else { return };
    hub.add_op(id, self.id, st.clone());
    if self.is_dead() {
      hub.ops_lost(self.id, &aborted("connection lost"));
    }
  }

  /// A reply arrived for a request nobody waits for. An Open, Opendir or Watch created a server
  /// resource, so a thread frees it. The reader never writes: it must keep reading.
  fn release_late(self: &Arc<Self>, f: Frame) {
    let job: Box<dyn FnOnce(&Sock) + Send> = match f.method {
      method::OPEN => match OpenResponse::decode(&f.meta[..]) {
        Ok(r) => Box::new(move |s| drop(s.call_msg::<Empty>(method::CLOSE, &CloseRequest { handle: r.handle }, &[]))),
        Err(_) => return,
      },
      method::OPENDIR => match OpendirResponse::decode(&f.meta[..]) {
        Ok(r) => {
          Box::new(move |s| drop(s.call_msg::<Empty>(method::CLOSEDIR, &ClosedirRequest { handle: r.handle }, &[])))
        }
        Err(_) => return,
      },
      method::WATCH => match WatchResponse::decode(&f.meta[..]) {
        Ok(r) => {
          Box::new(move |s| drop(s.call_msg::<Empty>(method::UNWATCH, &UnwatchRequest { watch_id: r.watch_id }, &[])))
        }
        Err(_) => return,
      },
      _ => return,
    };
    if self.is_dead() {
      return;
    }
    let (sock, guard) = (self.clone(), self.hub.thread_guard());
    let spawned = std::thread::Builder::new()
      .name("mountos-release".into())
      .spawn(move || {
        let _guard = guard;
        job(&sock)
      });
    match spawned {
      Ok(h) => {
        let mut rs = lock(&self.releasers);
        rs.retain(|h| !h.is_finished());
        rs.push(h);
      }
      Err(e) => self.hub.log(&format!("could not start release of a late reply: {e}")),
    }
  }
}

/// Reads frames until the stream ends. Replies go to the waiting call, notices to the hub.
fn read_loop(mut s: UnixStream, sock: &Arc<Sock>) {
  let hub = sock.hub.clone();
  let end = loop {
    match frame::read(&mut s) {
      Ok(Some(f)) if f.flags & FLAG_NOTIFY != 0 => on_notice(f, &hub, sock),
      Ok(Some(f)) => {
        if let Err(e) = sock.deliver(&hub, f) {
          break e;
        }
      }
      Ok(None) => break aborted("server closed the connection"),
      Err(e) => break e,
    }
  };
  let _ = s.shutdown(Shutdown::Both);
  sock.fail(end, true);
}

fn on_notice(f: Frame, hub: &Hub, sock: &Sock) {
  if f.method != method::NOTICE {
    hub.log(&format!("ignored notify frame with method {}", f.method));
    return;
  }
  let ev = match Notice::decode(&f.meta[..]) {
    Ok(m) => match Event::from_wire(m) {
      Some(ev) => ev,
      None => {
        hub.log("ignored notice of unknown kind");
        return;
      }
    },
    Err(e) => {
      hub.log(&format!("ignored undecodable notice: {e}"));
      return;
    }
  };
  if ev.kind == EventKind::Closing {
    sock.saw_closing.store(true, Ordering::SeqCst);
  }
  hub.publish(Some(sock.id), ev);
}

struct SlotState {
  sock: Option<Arc<Sock>>,
  /// Counts connects. A file remembers it to detect that its handle died with the old connection.
  epoch: u64,
}

/// Wakes callers that wait for a free connection. Every release bumps the generation, so a
/// waiter that read the generation before it scanned the pool cannot miss a release.
#[derive(Default)]
pub(crate) struct Gate {
  generation: Mutex<u64>,
  cv: Condvar,
  /// Callers that found every connection busy and wait for the first to free.
  waiting: AtomicUsize,
}

/// Marks a caller as waiting for any free connection until dropped.
struct Waiting<'a>(&'a Gate);

impl Drop for Waiting<'_> {
  fn drop(&mut self) {
    self.0.waiting.fetch_sub(1, Ordering::SeqCst);
    self.0.bump();
  }
}

impl Gate {
  fn generation(&self) -> u64 {
    *lock(&self.generation)
  }

  fn bump(&self) {
    *lock(&self.generation) += 1;
    self.cv.notify_all();
  }

  fn wait_change(&self, seen: u64) {
    let mut g = lock(&self.generation);
    while *g == seen {
      g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
    }
  }

  fn wait_for_free(&self) -> Waiting<'_> {
    self.waiting.fetch_add(1, Ordering::SeqCst);
    Waiting(self)
  }

  /// Holds back a caller bound to one connection while others wait for any free one, so a long
  /// operation that goes request after request cannot starve a small call.
  fn let_waiters_pass(&self) {
    loop {
      let seen = self.generation();
      if self.waiting.load(Ordering::SeqCst) == 0 {
        return;
      }
      self.wait_change(seen);
    }
  }
}

/// Holds one of a slot's request places until dropped, then wakes callers that wait for room.
struct Permit<'a>(&'a Slot);

impl Drop for Permit<'_> {
  fn drop(&mut self) {
    self.0.outstanding.fetch_sub(1, Ordering::SeqCst);
    self.0.gate.bump();
  }
}

/// A lazily connected pool entry. Up to `limit` requests run on its connection at once. A
/// request owns a place for one round trip only, never across the chunks of a large operation.
pub(crate) struct Slot {
  path: PathBuf,
  name: String,
  timeout: Duration,
  max_inflight: usize,
  hub: Arc<Hub>,
  gate: Arc<Gate>,
  /// Requests that hold a place.
  outstanding: AtomicUsize,
  /// Places available. One until the Hello reply says more.
  limit: AtomicUsize,
  closed: AtomicBool,
  /// The current connection, reachable without the state lock so `close` can cut a dial or a
  /// call in flight.
  live: Mutex<Option<Arc<Sock>>>,
  state: Mutex<SlotState>,
}

impl Slot {
  fn try_reserve(&self) -> Option<Permit<'_>> {
    let limit = self.limit.load(Ordering::SeqCst);
    let mut cur = self.outstanding.load(Ordering::SeqCst);
    loop {
      if cur >= limit {
        return None;
      }
      match self
        .outstanding
        .compare_exchange_weak(cur, cur + 1, Ordering::SeqCst, Ordering::SeqCst)
      {
        Ok(_) => return Some(Permit(self)),
        Err(c) => cur = c,
      }
    }
  }

  /// Runs `f` on the connection once a place is free. With `want`, fails if the connection is
  /// not the one with that epoch. A broken connection is replaced on the next call.
  pub(crate) fn run<T>(&self, want: Option<u64>, f: impl FnOnce(&Sock) -> Result<T>) -> Result<(T, u64)> {
    self.gate.let_waiters_pass();
    let permit = loop {
      let seen = self.gate.generation();
      if self.closed.load(Ordering::SeqCst) {
        return Err(aborted("client closed"));
      }
      if let Some(p) = self.try_reserve() {
        break p;
      }
      self.gate.wait_change(seen);
    };
    self.exec(permit, want, f)
  }

  /// Runs `f` for a caller that already holds a place and releases it at the end.
  fn exec<T>(&self, _permit: Permit<'_>, want: Option<u64>, f: impl FnOnce(&Sock) -> Result<T>) -> Result<(T, u64)> {
    let (sock, epoch) = self.sock_for(want)?;
    let r = f(&sock);
    if matches!(r, Err(Error::Io(_)) | Err(Error::Protocol(_))) {
      sock.kill();
    }
    r.map(|v| (v, epoch))
  }

  /// The live connection, dialed when there is none.
  fn sock_for(&self, want: Option<u64>) -> Result<(Arc<Sock>, u64)> {
    let mut st = lock(&self.state);
    if self.closed.load(Ordering::SeqCst) {
      return Err(aborted("client closed"));
    }
    let alive = st.sock.as_ref().is_some_and(|s| !s.is_dead());
    if want.is_some_and(|e| !alive || st.epoch != e) {
      return Err(aborted("connection lost, file handle is gone"));
    }
    if !alive {
      if let Some(old) = st.sock.take() {
        old.finish();
      }
      let s = Sock::open(&self.path, &self.hub, self.timeout)?;
      *lock(&self.live) = Some(s.clone());
      let shaken = if self.closed.load(Ordering::SeqCst) {
        Err(aborted("client closed"))
      } else {
        s.handshake(&self.name, self.max_inflight)
      };
      if let Err(e) = shaken {
        s.finish();
        return Err(e);
      }
      self.limit.store(s.limit(), Ordering::SeqCst);
      self.hub.note_instance(s.instance());
      st.sock = Some(s);
      st.epoch += 1;
      self.gate.bump();
    }
    Ok((st.sock.clone().expect("connected above"), st.epoch))
  }

  /// Refuses new calls and cuts the connection, which ends the calls in flight.
  fn close_start(&self) {
    self.closed.store(true, Ordering::SeqCst);
    if let Some(s) = lock(&self.live).as_ref() {
      s.kill();
    }
    self.gate.bump();
  }

  /// Stops the connection's threads.
  fn close_finish(&self) {
    let s = lock(&self.state).sock.take();
    if let Some(s) = s {
      s.finish();
    }
  }
}

pub(crate) struct Inner {
  slots: Vec<Arc<Slot>>,
  gate: Arc<Gate>,
  next: AtomicUsize,
  pub(crate) hub: Arc<Hub>,
  dispatcher: Mutex<Option<JoinHandle<()>>>,
}

impl Inner {
  /// Ends every connection, reader, watch and the dispatcher. Safe to call twice.
  fn shutdown(&self) {
    for s in &self.slots {
      s.close_start();
    }
    for s in &self.slots {
      s.close_finish();
    }
    self.hub.shutdown();
    let d = lock(&self.dispatcher).take();
    if let Some(h) = d {
      join_other(h);
    }
    self.hub.clear_listeners();
  }
}

impl Drop for Inner {
  fn drop(&mut self) {
    self.shutdown();
  }
}

/// Client for one mount. Cheap to clone, clones share the pool, the listeners and the event
/// dispatcher. The last clone to drop closes everything. `close` does it at once for all clones.
#[derive(Clone)]
pub struct Client {
  pub(crate) inner: Arc<Inner>,
}

impl Client {
  /// Connects with default options (4 connections).
  pub fn connect(path: impl AsRef<Path>) -> Result<Client> {
    Client::connect_with(path, Options::default())
  }

  /// Connects with the given options. The first connection and handshake happen now,
  /// so a bad socket or version mismatch fails here.
  pub fn connect_with(path: impl AsRef<Path>, opts: Options) -> Result<Client> {
    let n = opts.pool_size.max(1);
    let hub = Arc::new(Hub::new(opts.logger.clone()));
    let gate = Arc::new(Gate::default());
    let slots: Vec<_> = (0..n)
      .map(|_| {
        Arc::new(Slot {
          path: path.as_ref().to_path_buf(),
          name: opts.client_name.clone(),
          timeout: opts.call_timeout,
          max_inflight: opts.max_inflight.clamp(1, MAX_INFLIGHT_CEILING),
          hub: hub.clone(),
          gate: gate.clone(),
          outstanding: AtomicUsize::new(0),
          limit: AtomicUsize::new(1),
          closed: AtomicBool::new(false),
          live: Mutex::new(None),
          state: Mutex::new(SlotState { sock: None, epoch: 0 }),
        })
      })
      .collect();
    let (h, guard) = (hub.clone(), hub.thread_guard());
    let dispatcher = std::thread::Builder::new()
      .name("mountos-dispatch".into())
      .spawn(move || {
        let _guard = guard;
        h.dispatch_loop()
      })?;
    let inner = Inner {
      slots,
      gate,
      next: AtomicUsize::new(0),
      hub,
      dispatcher: Mutex::new(Some(dispatcher)),
    };
    // On failure, dropping `inner` stops the dispatcher and any reader.
    inner.slots[0].run(None, |_| Ok(()))?;
    Ok(Client { inner: Arc::new(inner) })
  }

  /// Ends all connections, watches and background threads. Later calls fail with an I/O error
  /// of kind `ConnectionAborted`. Dropping the last clone does the same.
  pub fn close(&self) {
    self.inner.shutdown();
  }

  pub(crate) fn pool_size(&self) -> usize {
    self.inner.slots.len()
  }

  /// Runs `f` on the connection with the fewest requests outstanding that has room. The scan
  /// starts at a rotating position so equal choices spread out. When all are full it waits for whichever frees first. Returns the slot
  /// and epoch the call ran on, for callers that bind a handle to it.
  pub(crate) fn run_any<T>(&self, f: impl FnOnce(&Sock) -> Result<T>) -> Result<(T, &Arc<Slot>, u64)> {
    let inner = &*self.inner;
    let n = inner.slots.len();
    let mut waiter = None;
    loop {
      let seen = inner.gate.generation();
      let start = inner.next.fetch_add(1, Ordering::Relaxed) % n;
      let mut best: Option<(&Arc<Slot>, usize)> = None;
      for i in 0..n {
        let slot = &inner.slots[(start + i) % n];
        let out = slot.outstanding.load(Ordering::SeqCst);
        if out >= slot.limit.load(Ordering::SeqCst) {
          continue;
        }
        if best.as_ref().is_none_or(|(_, k)| out < *k) {
          best = Some((slot, out));
        }
      }
      if let Some((slot, _)) = best {
        // Losing the race for the place means another caller took it, so scan again.
        if let Some(permit) = slot.try_reserve() {
          drop(waiter);
          let (v, epoch) = slot.exec(permit, None, f)?;
          return Ok((v, slot, epoch));
        }
        continue;
      }
      if waiter.is_none() {
        // Register, then scan once more: a place may have freed in between.
        waiter = Some(inner.gate.wait_for_free());
        continue;
      }
      inner.gate.wait_change(seen);
    }
  }

  pub(crate) fn unary<R: Message + Default>(&self, m: u16, req: &impl Message) -> Result<R> {
    Ok(self.run_any(|c| c.call_msg::<R>(m, req, &[]))?.0 .0)
  }

  /// Opens a file. `flags` combine `open_flag` values. The file stays bound to the connection that opened it.
  pub fn open(&self, path: &str, flags: u32, mode: u32) -> Result<File> {
    let ((r, _), slot, epoch) = self.run_any(|c| {
      c.call_msg::<OpenResponse>(
        method::OPEN,
        &OpenRequest {
          path: path.into(),
          flags,
          mode,
        },
        &[],
      )
    })?;
    Ok(File::new(slot.clone(), r.handle, epoch))
  }

  /// Sets times in nanoseconds since the epoch. `None` leaves that time unchanged.
  pub fn utimens(&self, path: &str, atime_ns: Option<i64>, mtime_ns: Option<i64>) -> Result<()> {
    self
      .unary::<Empty>(
        method::UTIMENS,
        &UtimensRequest {
          path: path.into(),
          atime_ns: atime_ns.unwrap_or(0),
          mtime_ns: mtime_ns.unwrap_or(0),
          set_atime: atime_ns.is_some(),
          set_mtime: mtime_ns.is_some(),
        },
      )
      .map(drop)
  }

  /// Opens a directory for paged listing. The directory stays bound to the connection that opened it.
  pub fn opendir(&self, path: &str) -> Result<Dir> {
    let ((r, _), slot, epoch) =
      self.run_any(|c| c.call_msg::<OpendirResponse>(method::OPENDIR, &OpendirRequest { path: path.into() }, &[]))?;
    Ok(Dir::new(slot.clone(), r.handle, epoch))
  }

  /// Lists up to `max_entries` entries with their attributes. Pass an empty `cursor` to start,
  /// then the returned cursor to continue. An empty returned cursor means the listing is complete.
  pub fn ls(&self, path: &str, max_entries: u32, cursor: &str) -> Result<(Vec<DirEntry>, String)> {
    let r: LsResponse = self.unary(
      method::LS,
      &LsRequest {
        path: path.into(),
        max_entries,
        cursor: cursor.into(),
      },
    )?;
    Ok((r.entries, r.next_cursor))
  }
}
