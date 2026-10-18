//! Change watches. A watch is a plain call on a pooled connection. Its change notices arrive
//! on that connection and are routed to the `Watch` handle by watch id.

use crate::client::{Client, Slot};
use crate::error::{Error, Result};
use crate::events::{lock, Chan, Event, Hub};
use crate::wire::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Events kept per watch before the oldest are dropped.
const WATCH_QUEUE: usize = 256;

/// A live watch. Iterating blocks until the next event. Only `Change` and `Overflow` events of
/// this watch arrive here. When more than 256 events wait, the oldest are dropped and one
/// `Overflow` event is delivered, which tells the caller to list the path again.
///
/// The iterator ends after `close`, when the connection ends, or when the client closes, after
/// the events already queued. All methods take `&self`, so another thread can call `close`
/// while one blocks in `next` (iterate a shared `&Watch` or an `Arc<Watch>`). Dropping the
/// handle closes it. The watch lives on one connection, and the server ends it with that
/// connection.
pub struct Watch {
  id: u64,
  slot: Arc<Slot>,
  epoch: u64,
  chan: Arc<Chan>,
  hub: Arc<Hub>,
  closed: AtomicBool,
}

impl Client {
  /// Starts a watch on `path`. `since` asks for events after that sequence. The server keeps
  /// no history, so an older value yields one Overflow event. Pass 0 for new events only.
  pub fn watch(&self, path: &str, recursive: bool, since: u64) -> Result<Watch> {
    let chan = Arc::new(Chan::new(WATCH_QUEUE, 0));
    let req = WatchRequest {
      path: path.into(),
      recursive,
      since_sequence: since,
    };
    let ((r, _), slot, epoch) = self.run_any(|c| {
      *lock(&c.shared.pending_watch) = Some(chan.clone());
      let r = c.call_msg::<WatchResponse>(method::WATCH, &req, &[]);
      *lock(&c.shared.pending_watch) = None;
      r
    })?;
    Ok(Watch {
      id: r.watch_id,
      slot: slot.clone(),
      epoch,
      chan,
      hub: self.inner.hub.clone(),
      closed: AtomicBool::new(false),
    })
  }
}

impl Watch {
  /// The server's watch id, which is the `ref_id` of its events.
  pub fn id(&self) -> u64 {
    self.id
  }

  /// Waits up to `d` for an event. `None` means the wait timed out or the watch has ended.
  pub fn recv_timeout(&self, d: Duration) -> Option<Event> {
    self.chan.pop(Some(d))
  }

  /// Stops the watch: sends Unwatch on the connection that made the watch and ends a blocked
  /// iterator. A connection that is already gone, or an EBADF reply, is not an error because
  /// the watch ended with the connection. A second call does nothing.
  pub fn close(&self) -> Result<()> {
    if self.closed.swap(true, Ordering::SeqCst) {
      return Ok(());
    }
    self.hub.remove_watch(self.id);
    self.chan.close(false);
    let req = UnwatchRequest { watch_id: self.id };
    match self
      .slot
      .run(Some(self.epoch), |c| c.call_msg::<Empty>(method::UNWATCH, &req, &[]))
    {
      Ok(_) | Err(Error::Io(_)) | Err(Error::Timeout(_)) => Ok(()),
      Err(Error::Server { code, .. }) if code == "EBADF" => Ok(()),
      Err(e) => Err(e),
    }
  }
}

impl Iterator for Watch {
  type Item = Event;

  fn next(&mut self) -> Option<Event> {
    self.chan.pop(None)
  }
}

impl Iterator for &Watch {
  type Item = Event;

  fn next(&mut self) -> Option<Event> {
    self.chan.pop(None)
  }
}

impl Drop for Watch {
  fn drop(&mut self) {
    let _ = self.close();
  }
}
