//! Clone and tree operations. `clone_entry` is one round trip. `clone_dir` and `remove_dir`
//! return an `Operation` at once. The server reports its end as notices on the connection that
//! started it, and the reader routes them here by op id.

use crate::client::{Client, Slot};
use crate::error::{Error, Result};
use crate::events::lock;
use crate::wire::{self, method};
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Most failures an `Operation` keeps. The server sends at most this many per operation.
/// `OpCounts::failed` can be larger.
pub const MAX_OP_FAILURES: usize = 64;

/// Extra properties `clone_entry` and `clone_dir` copy from the source. New kinds can be added.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Preserve {
  /// Keep the source times. Without it the new entry gets the current time.
  Times,
  /// Copy extended attributes.
  Xattr,
  /// Copy access control lists.
  Acl,
}

impl Preserve {
  fn to_wire(self) -> wire::PreserveKind {
    match self {
      Preserve::Times => wire::PreserveKind::Times,
      Preserve::Xattr => wire::PreserveKind::Xattr,
      Preserve::Acl => wire::PreserveKind::Acl,
    }
  }
}

/// Settings for `clone_entry` and `clone_dir`. Start from `CloneOptions::default()` and chain the
/// `with_` methods.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CloneOptions {
  /// Replace the content of a destination that differs. Without it such a destination fails
  /// with `EEXIST`.
  pub overwrite: bool,
  /// Properties to copy besides data, mode and owner.
  pub preserve: Vec<Preserve>,
}

impl CloneOptions {
  /// Sets `overwrite`.
  pub fn with_overwrite(mut self, overwrite: bool) -> CloneOptions {
    self.overwrite = overwrite;
    self
  }

  /// Sets `preserve`.
  pub fn with_preserve(mut self, preserve: Vec<Preserve>) -> CloneOptions {
    self.preserve = preserve;
    self
  }

  fn to_wire(&self) -> wire::CloneOptions {
    wire::CloneOptions {
      overwrite: self.overwrite,
      preserve: self.preserve.iter().map(|p| p.to_wire() as i32).collect(),
    }
  }
}

/// What `clone_entry` did with the destination. New outcomes can be added.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloneOutcome {
  /// The destination did not exist and was created.
  Cloned,
  /// The destination held different content and was replaced.
  Overwritten,
  /// The destination already held the same content. Nothing changed.
  Skipped,
}

/// Totals an operation reports when it ends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpCounts {
  pub cloned: u64,
  pub overwritten: u64,
  pub skipped: u64,
  /// Can exceed the number of failures kept.
  pub failed: u64,
  pub removed: u64,
  /// Entries kept because the mount protects them.
  pub protected: u64,
}

impl OpCounts {
  pub(crate) fn from_notice(m: &wire::Notice) -> OpCounts {
    OpCounts {
      cloned: m.cloned,
      overwritten: m.overwritten,
      skipped: m.skipped,
      failed: m.failed,
      removed: m.removed,
      protected: m.protected,
    }
  }
}

/// One entry that failed inside an operation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpFailure {
  /// Relative to the mount root. Empty when the name is not valid UTF-8.
  pub path: String,
  pub code: String,
  pub message: String,
}

impl OpFailure {
  pub(crate) fn new(path: String, code: String, message: String) -> OpFailure {
    OpFailure { path, code, message }
  }
}

/// How an operation ended.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpResult {
  pub counts: OpCounts,
  /// At most `MAX_OP_FAILURES`, in the order the server reported them.
  pub failures: Vec<OpFailure>,
}

struct Inner {
  failures: Vec<OpFailure>,
  /// Set once: the totals, or why they cannot be known.
  end: Option<std::result::Result<OpCounts, Error>>,
}

/// The shared record of one operation. The reader thread fills it, `Operation::wait` reads it.
pub(crate) struct OpState {
  inner: Mutex<Inner>,
  cv: Condvar,
}

impl OpState {
  pub(crate) fn new() -> Arc<OpState> {
    Arc::new(OpState {
      inner: Mutex::new(Inner {
        failures: Vec::new(),
        end: None,
      }),
      cv: Condvar::new(),
    })
  }

  /// Keeps `f` while the operation runs and fewer than `MAX_OP_FAILURES` are kept.
  pub(crate) fn add_failure(&self, f: OpFailure) {
    let mut g = lock(&self.inner);
    if g.end.is_none() && g.failures.len() < MAX_OP_FAILURES {
      g.failures.push(f);
    }
  }

  /// Ends the operation with its totals. The first ending wins.
  pub(crate) fn finish(&self, counts: OpCounts) {
    self.settle(Ok(counts));
  }

  /// Ends the operation because its end cannot be known. The first ending wins.
  pub(crate) fn fail(&self, e: Error) {
    self.settle(Err(e));
  }

  fn settle(&self, end: std::result::Result<OpCounts, Error>) {
    let mut g = lock(&self.inner);
    if g.end.is_none() {
      g.end = Some(end);
      drop(g);
      self.cv.notify_all();
    }
  }

  fn wait(&self, timeout: Duration) -> Result<OpResult> {
    let deadline = Instant::now().checked_add(timeout);
    let mut g = lock(&self.inner);
    loop {
      match &g.end {
        Some(Ok(counts)) => {
          return Ok(OpResult {
            counts: *counts,
            failures: g.failures.clone(),
          })
        }
        Some(Err(e)) => return Err(e.dup()),
        None => {}
      }
      let left = match deadline {
        Some(d) => d.saturating_duration_since(Instant::now()),
        None => Duration::from_secs(3600),
      };
      if left.is_zero() {
        return Err(Error::Io(io::Error::new(
          io::ErrorKind::TimedOut,
          "operation wait timed out",
        )));
      }
      g = self.cv.wait_timeout(g, left).unwrap_or_else(|e| e.into_inner()).0;
    }
  }
}

/// A running or ended `clone_dir` or `remove_dir`. It lives on the connection that started it.
pub struct Operation {
  id: u64,
  slot: Arc<Slot>,
  epoch: u64,
  state: Arc<OpState>,
}

impl Operation {
  /// The server's operation id, which is the `ref_id` of its events.
  pub fn id(&self) -> u64 {
    self.id
  }

  /// Waits up to `timeout` for the end of the operation. `Ok` means the server reported the end,
  /// and the result has the totals and the failures. `Error::OperationStateUnknown` means notices
  /// were lost. An `Error::Io` of kind `ConnectionAborted` means the connection dropped or the
  /// client closed. A timeout is an `Error::Io` of kind `TimedOut`. Calling again is allowed.
  pub fn wait(&self, timeout: Duration) -> Result<OpResult> {
    self.state.wait(timeout)
  }

  /// Asks the server to stop the operation. It still ends with an `OpDone` event, so call `wait`
  /// for the partial totals. An operation that already ended fails with `ENOENT`.
  pub fn cancel(&self) -> Result<()> {
    let req = wire::OpCancelRequest { op_id: self.id };
    self
      .slot
      .run(Some(self.epoch), |c| {
        c.call_msg::<wire::Empty>(method::OP_CANCEL, &req, &[])
      })
      .map(drop)
  }
}

impl Client {
  /// Copies one entry that is not a directory (a file, a symlink or a special file) and returns
  /// what happened to the destination. Needs the `clone` capability. Without it the call fails
  /// with `UNSUPPORTED_OPERATION`.
  pub fn clone_entry(&self, from: &str, to: &str, opts: &CloneOptions) -> Result<CloneOutcome> {
    let req = wire::CloneRequest {
      from: from.into(),
      to: to.into(),
      options: Some(opts.to_wire()),
    };
    let r: wire::CloneResponse = self.unary(method::CLONE, &req)?;
    match wire::CloneOutcome::try_from(r.outcome) {
      Ok(wire::CloneOutcome::Cloned) => Ok(CloneOutcome::Cloned),
      Ok(wire::CloneOutcome::Overwritten) => Ok(CloneOutcome::Overwritten),
      Ok(wire::CloneOutcome::Skipped) => Ok(CloneOutcome::Skipped),
      Err(_) => Err(Error::Protocol(format!("unknown clone outcome {}", r.outcome))),
    }
  }

  /// Starts copying the tree at `from` to `to` in the background and returns at once. Needs the
  /// `ops` capability. Fails with `EAGAIN` when the server runs too many operations.
  pub fn clone_dir(&self, from: &str, to: &str, opts: &CloneOptions) -> Result<Operation> {
    let req = wire::CloneDirRequest {
      from: from.into(),
      to: to.into(),
      options: Some(opts.to_wire()),
    };
    self.start_op(|c, st| c.call_op::<wire::CloneDirResponse>(method::CLONE_DIR, &req, st, |r| r.op_id))
  }

  /// Starts removing `path` and everything below it in the background and returns at once.
  /// Unlike `remove_all` it keeps going after a failed entry and reports each failure. Needs the
  /// `ops` capability. Fails with `EAGAIN` when the server runs too many operations.
  pub fn remove_dir(&self, path: &str) -> Result<Operation> {
    let req = wire::RemoveDirRequest { path: path.into() };
    self.start_op(|c, st| c.call_op::<wire::RemoveDirResponse>(method::REMOVE_DIR, &req, st, |r| r.op_id))
  }

  fn start_op(&self, f: impl FnOnce(&crate::client::Sock, Arc<OpState>) -> Result<u64>) -> Result<Operation> {
    let state = OpState::new();
    let st = state.clone();
    let (id, slot, epoch) = self.run_any(move |c| f(c, st))?;
    Ok(Operation {
      id,
      slot: slot.clone(),
      epoch,
      state,
    })
  }
}
