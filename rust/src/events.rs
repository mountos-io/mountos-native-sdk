//! Server-pushed notices. Readers decode notify frames into `Event`s and hand them to the hub.
//! The hub settles warm waits and watch queues at once, and queues every event for the dispatcher
//! thread, which calls the listeners in order.

use crate::client::Client;
use crate::error::{Error, Result};
use crate::wire::{self, WatchEventKind};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

/// Capacity of the listener queue.
const LISTENER_QUEUE: usize = 1024;
/// Finished warms kept for `wait_warm`.
const WARM_CACHE: usize = 1024;

/// Receives log lines. The line has no prefix and no newline.
pub type Logger = Arc<dyn Fn(&str) + Send + Sync>;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
  m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Kind of an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
  /// A warm request finished. `ref_id` is the warm id.
  WarmDone,
  /// A warm request failed. `error` says why and `path` names the file.
  WarmFailed,
  /// A watched path changed. `ref_id` is the watch id and `change` says how.
  Change,
  /// Events were dropped. `ref_id` is the watch id, or 0 when the loss is not tied to one watch.
  /// A local queue that overflowed also produces this event.
  Overflow,
  /// The server is about to close the connection, for example on unmount.
  Closing,
  /// A connection ended without a Closing notice or a local close. `error` says why.
  /// This event is local, the server does not send it.
  ConnectionLost,
}

/// How a watched path changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeKind {
  Create,
  Remove,
  Write,
  /// `path` is the new name and `old_path` the old one.
  Rename,
  Attrib,
}

/// One event from the server, or a local one (`Overflow`, `ConnectionLost`).
#[derive(Debug)]
pub struct Event {
  pub kind: EventKind,
  /// Server sequence number. 0 for local events.
  pub sequence: u64,
  pub ref_id: u64,
  pub path: String,
  pub old_path: String,
  /// Set for `Change`.
  pub change: Option<ChangeKind>,
  /// `Error::Server` for `WarmFailed`, an `Error::Io` for `ConnectionLost`.
  pub error: Option<Error>,
  /// When the client received the event.
  pub time: SystemTime,
}

impl Clone for Event {
  fn clone(&self) -> Event {
    Event {
      kind: self.kind,
      sequence: self.sequence,
      ref_id: self.ref_id,
      path: self.path.clone(),
      old_path: self.old_path.clone(),
      change: self.change,
      error: self.error.as_ref().map(Error::dup),
      time: self.time,
    }
  }
}

impl Event {
  pub(crate) fn local(kind: EventKind, ref_id: u64, error: Option<Error>) -> Event {
    Event {
      kind,
      sequence: 0,
      ref_id,
      path: String::new(),
      old_path: String::new(),
      change: None,
      error,
      time: SystemTime::now(),
    }
  }

  /// Decodes a notice. Returns `None` for a kind this client does not know.
  pub(crate) fn from_wire(m: wire::Notice) -> Option<Event> {
    let mut change = None;
    let kind = match wire::NoticeKind::try_from(m.kind).ok()? {
      wire::NoticeKind::WarmDone => EventKind::WarmDone,
      wire::NoticeKind::WarmFailed => EventKind::WarmFailed,
      wire::NoticeKind::Closing => EventKind::Closing,
      wire::NoticeKind::Overflow => EventKind::Overflow,
      wire::NoticeKind::Change => {
        change = match WatchEventKind::try_from(m.change_kind).ok()? {
          WatchEventKind::Create => Some(ChangeKind::Create),
          WatchEventKind::Remove => Some(ChangeKind::Remove),
          WatchEventKind::Write => Some(ChangeKind::Write),
          WatchEventKind::Rename => Some(ChangeKind::Rename),
          WatchEventKind::Attrib => Some(ChangeKind::Attrib),
          WatchEventKind::Overflow => None,
        };
        if change.is_some() {
          EventKind::Change
        } else {
          EventKind::Overflow
        }
      }
    };
    let error = (kind == EventKind::WarmFailed).then_some(Error::Server {
      code: m.code,
      message: m.message,
    });
    Some(Event {
      kind,
      sequence: m.sequence,
      ref_id: m.ref_id,
      path: m.path,
      old_path: m.old_path,
      change,
      error,
      time: SystemTime::now(),
    })
  }

  fn log_line(&self) -> String {
    let err = self
      .error
      .as_ref()
      .map_or_else(|| "none".to_string(), |e| e.to_string());
    format!(
      "event kind={:?} ref_id={} path={:?} error={}",
      self.kind, self.ref_id, self.path, err
    )
  }
}

/// Handle for `Client::remove_listener`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ListenerId(u64);

struct ChanState {
  items: VecDeque<Event>,
  /// Set when the oldest event was dropped. The next pop returns a local Overflow first.
  overflowed: bool,
  closed: bool,
}

/// Bounded event queue. A full queue drops its oldest event and reports one local Overflow.
pub(crate) struct Chan {
  cap: usize,
  /// Stamped on the local Overflow event.
  ref_id: AtomicU64,
  st: Mutex<ChanState>,
  cv: Condvar,
}

impl Chan {
  pub(crate) fn new(cap: usize, ref_id: u64) -> Chan {
    Chan {
      cap,
      ref_id: AtomicU64::new(ref_id),
      st: Mutex::new(ChanState {
        items: VecDeque::new(),
        overflowed: false,
        closed: false,
      }),
      cv: Condvar::new(),
    }
  }

  pub(crate) fn set_ref_id(&self, id: u64) {
    self.ref_id.store(id, Ordering::SeqCst);
  }

  /// Queues `ev`. Returns true when this push started an overflow episode (it dropped the
  /// oldest event and no Overflow was pending yet).
  pub(crate) fn push(&self, ev: Event) -> bool {
    let mut st = lock(&self.st);
    if st.closed {
      return false;
    }
    let mut started = false;
    if st.items.len() >= self.cap {
      st.items.pop_front();
      started = !st.overflowed;
      st.overflowed = true;
    }
    st.items.push_back(ev);
    drop(st);
    self.cv.notify_one();
    started
  }

  /// Ends the queue. With `discard`, queued events are dropped, otherwise they are still delivered.
  pub(crate) fn close(&self, discard: bool) {
    let mut st = lock(&self.st);
    st.closed = true;
    if discard {
      st.items.clear();
      st.overflowed = false;
    }
    drop(st);
    self.cv.notify_all();
  }

  /// Waits for the next event. `None` timeout blocks. Returns `None` on timeout, or when the
  /// queue is closed and empty.
  pub(crate) fn pop(&self, timeout: Option<Duration>) -> Option<Event> {
    let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
    let mut st = lock(&self.st);
    loop {
      if st.overflowed {
        st.overflowed = false;
        return Some(Event::local(
          EventKind::Overflow,
          self.ref_id.load(Ordering::SeqCst),
          None,
        ));
      }
      if let Some(ev) = st.items.pop_front() {
        return Some(ev);
      }
      if st.closed {
        return None;
      }
      st = match deadline {
        None => self.cv.wait(st).unwrap_or_else(|e| e.into_inner()),
        Some(d) => {
          let left = d.saturating_duration_since(Instant::now());
          if left.is_zero() {
            return None;
          }
          self.cv.wait_timeout(st, left).unwrap_or_else(|e| e.into_inner()).0
        }
      };
    }
  }
}

type Listener = Arc<dyn Fn(&Event) + Send + Sync>;

#[derive(Default)]
struct Warms {
  /// Finished warms by id: Ok, or the failure (code, message).
  done: HashMap<u64, std::result::Result<(), (String, String)>>,
  order: VecDeque<u64>,
  closed: bool,
}

struct WatchEntry {
  conn: u64,
  chan: Arc<Chan>,
}

/// Shared by the client, its connection readers and the dispatcher thread.
pub(crate) struct Hub {
  logger: Logger,
  listeners: Mutex<Vec<(ListenerId, Listener)>>,
  next_listener: AtomicU64,
  queue: Chan,
  warms: Mutex<Warms>,
  warm_cv: Condvar,
  watches: Mutex<HashMap<u64, WatchEntry>>,
  /// Counts the background threads of the client that are still running.
  threads: Arc<AtomicUsize>,
}

/// Counts a running background thread until dropped.
pub(crate) struct ThreadGuard(Arc<AtomicUsize>);

impl Drop for ThreadGuard {
  fn drop(&mut self) {
    self.0.fetch_sub(1, Ordering::SeqCst);
  }
}

impl Hub {
  /// Call before spawning a thread and move the guard into it.
  pub(crate) fn thread_guard(&self) -> ThreadGuard {
    self.threads.fetch_add(1, Ordering::SeqCst);
    ThreadGuard(self.threads.clone())
  }

  pub(crate) fn thread_probe(&self) -> Arc<AtomicUsize> {
    self.threads.clone()
  }

  pub(crate) fn new(logger: Option<Logger>) -> Hub {
    Hub {
      logger: logger.unwrap_or_else(|| Arc::new(|line: &str| eprintln!("mountos: {line}"))),
      listeners: Mutex::new(Vec::new()),
      next_listener: AtomicU64::new(1),
      queue: Chan::new(LISTENER_QUEUE, 0),
      warms: Mutex::new(Warms::default()),
      warm_cv: Condvar::new(),
      watches: Mutex::new(HashMap::new()),
      threads: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub(crate) fn log(&self, line: &str) {
    (self.logger)(line);
  }

  pub(crate) fn add_watch(&self, id: u64, conn: u64, chan: Arc<Chan>) {
    chan.set_ref_id(id);
    lock(&self.watches).insert(id, WatchEntry { conn, chan });
  }

  pub(crate) fn remove_watch(&self, id: u64) {
    lock(&self.watches).remove(&id);
  }

  /// Ends the watches of a connection that is gone.
  pub(crate) fn conn_lost(&self, conn: u64) {
    lock(&self.watches).retain(|_, w| {
      if w.conn == conn {
        w.chan.close(false);
        false
      } else {
        true
      }
    });
  }

  /// Handles one event: settles warm waits, feeds the watch queue, queues it for listeners.
  /// `conn` is the connection the event came from, or `None` for a local event. A server
  /// Overflow without a watch id applies to every watch of that connection.
  pub(crate) fn publish(&self, conn: Option<u64>, ev: Event) {
    match ev.kind {
      EventKind::WarmDone | EventKind::WarmFailed => self.record_warm(&ev),
      EventKind::Overflow if ev.ref_id == 0 => {
        if let Some(conn) = conn {
          let chans: Vec<Arc<Chan>> = lock(&self.watches)
            .values()
            .filter(|w| w.conn == conn)
            .map(|w| w.chan.clone())
            .collect();
          for c in chans {
            c.push(ev.clone());
          }
        }
      }
      EventKind::Change | EventKind::Overflow if ev.ref_id != 0 => {
        let chan = lock(&self.watches).get(&ev.ref_id).map(|w| w.chan.clone());
        if let Some(c) = chan {
          if c.push(ev.clone()) {
            self.log(&format!("watch {} queue full, dropped oldest events", ev.ref_id));
          }
        }
      }
      _ => {}
    }
    if self.queue.push(ev) {
      self.log("listener queue full, dropped oldest events");
    }
  }

  fn record_warm(&self, ev: &Event) {
    let r = match &ev.error {
      Some(Error::Server { code, message }) => Err((code.clone(), message.clone())),
      _ => Ok(()),
    };
    let mut w = lock(&self.warms);
    if w.done.insert(ev.ref_id, r).is_none() {
      w.order.push_back(ev.ref_id);
    }
    while w.order.len() > WARM_CACHE {
      if let Some(old) = w.order.pop_front() {
        w.done.remove(&old);
      }
    }
    drop(w);
    self.warm_cv.notify_all();
  }

  pub(crate) fn wait_warm(&self, id: u64, timeout: Duration) -> Result<()> {
    let deadline = Instant::now().checked_add(timeout);
    let mut w = lock(&self.warms);
    loop {
      match w.done.get(&id) {
        Some(Ok(())) => return Ok(()),
        Some(Err((code, message))) => {
          return Err(Error::Server {
            code: code.clone(),
            message: message.clone(),
          })
        }
        None => {}
      }
      if w.closed {
        return Err(Error::Io(io::Error::new(
          io::ErrorKind::ConnectionAborted,
          "client closed",
        )));
      }
      let left = match deadline {
        Some(d) => d.saturating_duration_since(Instant::now()),
        None => Duration::from_secs(3600),
      };
      if left.is_zero() {
        return Err(Error::Io(io::Error::new(
          io::ErrorKind::TimedOut,
          "wait_warm timed out",
        )));
      }
      w = self.warm_cv.wait_timeout(w, left).unwrap_or_else(|e| e.into_inner()).0;
    }
  }

  /// Stops the dispatcher queue, ends all watches and wakes warm waiters.
  pub(crate) fn shutdown(&self) {
    self.queue.close(true);
    for (_, w) in lock(&self.watches).drain() {
      w.chan.close(false);
    }
    lock(&self.warms).closed = true;
    self.warm_cv.notify_all();
  }

  /// Drops all listeners, which releases anything they captured.
  pub(crate) fn clear_listeners(&self) {
    let old = std::mem::take(&mut *lock(&self.listeners));
    drop(old);
  }

  /// Dispatcher thread body. Runs until `shutdown`.
  pub(crate) fn dispatch_loop(&self) {
    while let Some(ev) = self.queue.pop(None) {
      let ls: Vec<Listener> = lock(&self.listeners).iter().map(|(_, l)| l.clone()).collect();
      if ls.is_empty() {
        self.log(&ev.log_line());
        continue;
      }
      for l in ls {
        if catch_unwind(AssertUnwindSafe(|| l(&ev))).is_err() {
          self.log(&format!("listener panicked on {}", ev.log_line()));
        }
      }
    }
  }
}

impl Client {
  /// Adds a listener. Every listener gets every event, in order, on the dispatcher thread of
  /// this client. The call must return quickly. A slow listener makes the queue (1024 events)
  /// drop its oldest events, and every listener then gets one local `Overflow` event.
  /// A panic in a listener is caught and logged. With no listener, events go to the logger.
  /// A listener that holds a `Client` clone keeps the client open until `close` is called.
  pub fn add_listener(&self, f: impl Fn(&Event) + Send + Sync + 'static) -> ListenerId {
    let hub = &self.inner.hub;
    let id = ListenerId(hub.next_listener.fetch_add(1, Ordering::SeqCst));
    lock(&hub.listeners).push((id, Arc::new(f)));
    id
  }

  /// Counter of running background threads (readers and the dispatcher), for tests.
  #[doc(hidden)]
  pub fn thread_probe(&self) -> Arc<AtomicUsize> {
    self.inner.hub.thread_probe()
  }

  /// Removes a listener. An unknown id does nothing.
  pub fn remove_listener(&self, id: ListenerId) {
    lock(&self.inner.hub.listeners).retain(|(i, _)| *i != id);
  }

  /// Waits until warm request `id` is done. A failed warm returns its failure as `Error::Server`.
  /// Waiting longer than `timeout` returns an I/O error of kind `TimedOut`. The client keeps the
  /// last 1024 finished warms, so a warm that ended before this call is not missed. The server
  /// is not polled.
  pub fn wait_warm(&self, id: u64, timeout: Duration) -> Result<()> {
    self.inner.hub.wait_warm(id, timeout)
  }
}
