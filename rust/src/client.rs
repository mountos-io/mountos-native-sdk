use crate::dir::Dir;
use crate::error::{Error, Result};
use crate::events::{lock, Chan, Event, EventKind, Hub, Logger};
use crate::file::File;
use crate::frame::{self, Frame, FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE, MAX_PAYLOAD_BYTES};
use crate::wire::*;
use prost::Message;
use std::collections::HashSet;
use std::fmt;
use std::io;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Client settings.
#[derive(Clone)]
pub struct Options {
  /// Number of connections. Calls on different files can run in parallel up to this number.
  pub pool_size: usize,
  pub client_name: String,
  /// Receives log lines: events nobody listens for, listener panics and dropped events.
  /// The line has no prefix and no newline. When `None`, lines go to stderr with the prefix
  /// "mountos: ". Pass your own function to route them elsewhere.
  pub logger: Option<Logger>,
  /// Longest a single request may take, from the first byte sent to the reply. `Duration::ZERO`
  /// (the default) means no limit. A call that runs over fails with `Error::Timeout` and its
  /// connection is dropped and dialed again on the next call, so a late reply cannot reach
  /// another call. Files and watches on that connection are gone. A call that sends several
  /// requests (a large read or write) gets the limit for each request.
  pub call_timeout: Duration,
}

/// Alias of `Options`.
pub type ClientOptions = Options;

impl Default for Options {
  fn default() -> Options {
    Options {
      pool_size: 4,
      client_name: concat!("mountos-native-rust/", env!("CARGO_PKG_VERSION")).into(),
      logger: None,
      call_timeout: Duration::ZERO,
    }
  }
}

impl fmt::Debug for Options {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Options")
      .field("pool_size", &self.pool_size)
      .field("client_name", &self.client_name)
      .field("logger", &self.logger.as_ref().map(|_| "<fn>"))
      .field("call_timeout", &self.call_timeout)
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
    _ => None,
  }
}

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

/// State shared between a connection and its reader thread.
pub(crate) struct ConnShared {
  id: u64,
  /// Set when this side ends the connection on purpose.
  closing: AtomicBool,
  /// Set by the reader when the stream has ended.
  dead: AtomicBool,
  /// The server sent Closing, so the end that follows is expected.
  saw_closing: AtomicBool,
  /// Queue for the watch call in flight. The reader registers it before the call returns, so
  /// no change notice that follows the reply is lost.
  pub(crate) pending_watch: Mutex<Option<Arc<Chan>>>,
  /// The only response the reader may accept. Cleared before delivering that response.
  pending_reply: Mutex<Option<(u64, u16)>>,
  /// Second handle on the socket, used to cut it from another thread.
  killer: UnixStream,
}

impl ConnShared {
  fn kill(&self) {
    self.closing.store(true, Ordering::SeqCst);
    let _ = self.killer.shutdown(Shutdown::Both);
  }
}

/// One handshaken connection. The server runs one request at a time on it. A reader thread
/// reads every frame: replies go to the waiting call, notices go to the hub.
pub(crate) struct Conn {
  stream: UnixStream,
  rx: Receiver<Result<Frame>>,
  pub(crate) shared: Arc<ConnShared>,
  reader: Option<JoinHandle<()>>,
  pub(crate) next_id: u64,
  /// Largest payload per frame, from the Hello reply.
  pub(crate) max_payload: usize,
  /// Per-request limit, zero for none.
  timeout: Duration,
  /// Capabilities the server reported. `None` when it reported none (an older server), which
  /// allows every call.
  caps: Option<HashSet<String>>,
}

impl Conn {
  pub(crate) fn connect(path: &Path, name: &str, hub: &Arc<Hub>, timeout: Duration) -> Result<Conn> {
    let stream = UnixStream::connect(path)?;
    if !timeout.is_zero() {
      stream.set_write_timeout(Some(timeout))?;
    }
    let shared = Arc::new(ConnShared {
      id: CONN_SEQ.fetch_add(1, Ordering::SeqCst),
      closing: AtomicBool::new(false),
      dead: AtomicBool::new(false),
      saw_closing: AtomicBool::new(false),
      pending_watch: Mutex::new(None),
      pending_reply: Mutex::new(None),
      killer: stream.try_clone()?,
    });
    let (tx, rx) = sync_channel(1);
    let (rs, rh, rsh) = (stream.try_clone()?, hub.clone(), shared.clone());
    let guard = hub.thread_guard();
    let reader = std::thread::Builder::new()
      .name("mountos-reader".into())
      .spawn(move || {
        let _guard = guard;
        read_loop(rs, tx, &rh, &rsh)
      })?;
    let mut c = Conn {
      stream,
      rx,
      shared,
      reader: Some(reader),
      next_id: 0,
      max_payload: MAX_PAYLOAD_BYTES as usize,
      timeout,
      caps: None,
    };
    let req = HelloRequest {
      client_name: name.into(),
      min_version: 1,
      max_version: 1,
      capabilities: vec![],
    };
    let (resp, _): (HelloResponse, _) = c.call_msg(method::HELLO, &req, &[])?;
    if resp.version != 1 {
      return Err(Error::Protocol(format!("server chose version {}", resp.version)));
    }
    if resp.max_payload_bytes > 0 {
      c.max_payload = c.max_payload.min(resp.max_payload_bytes as usize);
    }
    if !resp.capabilities.is_empty() {
      c.caps = Some(resp.capabilities.into_iter().collect());
    }
    Ok(c)
  }

  fn is_dead(&self) -> bool {
    self.shared.dead.load(Ordering::SeqCst)
  }

  /// Sends one request and waits for its response frame. The sizes and the capability are
  /// checked before anything is sent, so those failures leave the connection intact.
  fn call(&mut self, method: u16, req: &impl Message, payload: &[u8]) -> Result<Frame> {
    if let Some(cap) = required_capability(method) {
      if self.caps.as_ref().is_some_and(|c| !c.contains(cap)) {
        return Err(Error::server(
          "UNSUPPORTED_OPERATION",
          &format!("the server does not report capability {cap}"),
        ));
      }
    }
    let meta = req.encode_to_vec();
    frame::check_limits(meta.len(), payload.len())?;
    self.next_id += 1;
    let id = self.next_id;
    *lock(&self.shared.pending_reply) = Some((id, method));
    frame::write_parts(&mut self.stream, 0, method, id, &meta, payload).map_err(write_timeout)?;
    let resp = if self.timeout.is_zero() {
      self.rx.recv().map_err(|_| aborted("server closed the connection"))??
    } else {
      match self.rx.recv_timeout(self.timeout) {
        Ok(r) => r?,
        Err(RecvTimeoutError::Timeout) => {
          return Err(Error::Timeout(format!(
            "method {method} did not finish within {:?}",
            self.timeout
          )))
        }
        Err(RecvTimeoutError::Disconnected) => return Err(aborted("server closed the connection")),
      }
    };
    if resp.flags & FLAG_RESPONSE == 0 || resp.request_id != id || resp.method != method {
      return Err(Error::Protocol(format!(
        "unexpected frame: method {} id {}",
        resp.method, resp.request_id
      )));
    }
    if resp.flags & FLAG_ERROR != 0 {
      let e = WireError::decode(&resp.meta[..]).map_err(|e| Error::Protocol(format!("bad error meta: {e}")))?;
      return Err(Error::Server {
        code: e.code,
        message: e.message,
      });
    }
    Ok(resp)
  }

  /// Like `call`, decoding the meta as `R` and returning the reply payload.
  pub(crate) fn call_msg<R: Message + Default>(
    &mut self,
    method: u16,
    req: &impl Message,
    payload: &[u8],
  ) -> Result<(R, Vec<u8>)> {
    let resp = self.call(method, req, payload)?;
    let msg = R::decode(&resp.meta[..]).map_err(|e| Error::Protocol(format!("bad response meta: {e}")))?;
    Ok((msg, resp.payload))
  }
}

impl Drop for Conn {
  fn drop(&mut self) {
    if !self.is_dead() {
      self.shared.kill();
    }
    let _ = self.stream.shutdown(Shutdown::Both);
    if let Some(h) = self.reader.take() {
      join_other(h);
    }
  }
}

/// Reads frames until the stream ends. Replies go to the waiting call, notices to the hub.
fn read_loop(mut s: UnixStream, tx: SyncSender<Result<Frame>>, hub: &Hub, cs: &ConnShared) {
  let end = loop {
    match frame::read(&mut s) {
      Ok(Some(f)) if f.flags & FLAG_NOTIFY != 0 => on_notice(f, hub, cs),
      Ok(Some(f)) => {
        let pending = lock(&cs.pending_reply).take();
        if pending != Some((f.request_id, f.method)) || f.flags & FLAG_RESPONSE == 0 {
          break Error::Protocol(format!("unexpected frame: method {} id {}", f.method, f.request_id));
        }
        if f.flags & (FLAG_RESPONSE | FLAG_ERROR) == FLAG_RESPONSE && f.method == method::WATCH {
          if let (Ok(r), Some(chan)) = (WatchResponse::decode(&f.meta[..]), lock(&cs.pending_watch).take()) {
            hub.add_watch(r.watch_id, cs.id, chan);
          }
        }
        if tx.try_send(Ok(f)).is_err() {
          break aborted("connection dropped");
        }
      }
      Ok(None) => break aborted("server closed the connection"),
      Err(e) => break e,
    }
  };
  let _ = s.shutdown(Shutdown::Both);
  cs.dead.store(true, Ordering::SeqCst);
  hub.conn_lost(cs.id);
  let expected = cs.closing.load(Ordering::SeqCst) || cs.saw_closing.load(Ordering::SeqCst);
  if !expected {
    hub.publish(None, Event::local(EventKind::ConnectionLost, 0, Some(end.dup())));
  }
  let _ = tx.try_send(Err(end));
}

fn on_notice(f: Frame, hub: &Hub, cs: &ConnShared) {
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
    cs.saw_closing.store(true, Ordering::SeqCst);
  }
  hub.publish(Some(cs.id), ev);
}

struct SlotState {
  conn: Option<Conn>,
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

/// Fair queue for one connection. Callers are served in arrival order, so a caller that runs
/// request after request cannot starve one that waits. Holds (next ticket, ticket served).
#[derive(Default)]
struct Turnstile {
  tickets: Mutex<(u64, u64)>,
  cv: Condvar,
}

/// Held while a request owns the connection. Dropping it serves the next caller and wakes the
/// callers that wait for any free connection.
struct Turn<'a>(&'a Slot);

impl Drop for Turn<'_> {
  fn drop(&mut self) {
    lock(&self.0.queue.tickets).1 += 1;
    self.0.queue.cv.notify_all();
    self.0.gate.bump();
  }
}

/// A lazily connected pool entry. A request owns it for one round trip only, never across the
/// chunks of a large operation.
pub(crate) struct Slot {
  path: PathBuf,
  name: String,
  timeout: Duration,
  hub: Arc<Hub>,
  gate: Arc<Gate>,
  queue: Turnstile,
  closed: AtomicBool,
  /// The current connection, reachable without the state lock so `close` can cut a call in flight.
  live: Mutex<Option<Arc<ConnShared>>>,
  state: Mutex<SlotState>,
}

impl Slot {
  /// Runs `f` on the connection, waiting for its turn. With `want`, fails if the connection is
  /// not the one with that epoch. A broken connection is dropped so the next call reconnects.
  pub(crate) fn run<T>(&self, want: Option<u64>, f: impl FnOnce(&mut Conn) -> Result<T>) -> Result<(T, u64)> {
    self.gate.let_waiters_pass();
    let mut q = lock(&self.queue.tickets);
    let ticket = q.0;
    q.0 += 1;
    while q.1 != ticket {
      q = self.queue.cv.wait(q).unwrap_or_else(|e| e.into_inner());
    }
    drop(q);
    self.run_entered(want, f)
  }

  /// Takes a turn only when nobody holds or awaits the connection.
  fn try_enter(&self) -> bool {
    let mut q = lock(&self.queue.tickets);
    if q.0 != q.1 {
      return false;
    }
    q.0 += 1;
    true
  }

  /// Runs `f` for a caller that already holds a turn and releases it at the end.
  fn run_entered<T>(&self, want: Option<u64>, f: impl FnOnce(&mut Conn) -> Result<T>) -> Result<(T, u64)> {
    let _turn = Turn(self);
    let mut st = lock(&self.state);
    self.exec(&mut st, want, f)
  }

  fn exec<T>(&self, st: &mut SlotState, want: Option<u64>, f: impl FnOnce(&mut Conn) -> Result<T>) -> Result<(T, u64)> {
    if self.closed.load(Ordering::SeqCst) {
      return Err(aborted("client closed"));
    }
    let alive = st.conn.as_ref().is_some_and(|c| !c.is_dead());
    if want.is_some_and(|e| !alive || st.epoch != e) {
      return Err(aborted("connection lost, file handle is gone"));
    }
    if !alive {
      st.conn = None;
      let c = Conn::connect(&self.path, &self.name, &self.hub, self.timeout)?;
      *lock(&self.live) = Some(c.shared.clone());
      if self.closed.load(Ordering::SeqCst) {
        return Err(aborted("client closed"));
      }
      st.conn = Some(c);
      st.epoch += 1;
    }
    let epoch = st.epoch;
    let r = f(st.conn.as_mut().expect("connected above"));
    if matches!(r, Err(Error::Io(_)) | Err(Error::Protocol(_)) | Err(Error::Timeout(_))) {
      st.conn = None;
    }
    r.map(|v| (v, epoch))
  }

  /// Refuses new calls and cuts the connection, which ends a call in flight.
  fn close_start(&self) {
    self.closed.store(true, Ordering::SeqCst);
    if let Some(cs) = lock(&self.live).as_ref() {
      cs.kill();
    }
    self.gate.bump();
  }

  /// Drops the connection, which stops its reader.
  fn close_finish(&self) {
    lock(&self.state).conn = None;
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
          hub: hub.clone(),
          gate: gate.clone(),
          queue: Turnstile::default(),
          closed: AtomicBool::new(false),
          live: Mutex::new(None),
          state: Mutex::new(SlotState { conn: None, epoch: 0 }),
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

  /// Runs `f` on the first connection that is free, starting the scan at a rotating position so
  /// consecutive calls spread out. When all are busy it waits for whichever frees first. Returns
  /// the slot and epoch the call ran on, for callers that bind a handle to it.
  pub(crate) fn run_any<T>(&self, f: impl FnOnce(&mut Conn) -> Result<T>) -> Result<(T, &Arc<Slot>, u64)> {
    let inner = &*self.inner;
    let n = inner.slots.len();
    let mut waiter = None;
    loop {
      let seen = inner.gate.generation();
      let start = inner.next.fetch_add(1, Ordering::Relaxed) % n;
      for i in 0..n {
        let slot = &inner.slots[(start + i) % n];
        if slot.try_enter() {
          drop(waiter);
          let (v, epoch) = slot.run_entered(None, f)?;
          return Ok((v, slot, epoch));
        }
      }
      if waiter.is_none() {
        // Register, then scan once more: a connection may have freed in between.
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
