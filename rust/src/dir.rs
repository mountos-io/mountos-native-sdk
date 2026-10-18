use crate::client::Slot;
use crate::error::{Error, Result};
use crate::wire::*;
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) struct State {
  pub(crate) handle: u64,
  closed: bool,
}

/// An open remote directory. Bound to the connection that opened it, like `File`.
pub struct Dir {
  slot: Arc<Slot>,
  epoch: u64,
  st: Mutex<State>,
}

impl Dir {
  pub(crate) fn new(slot: Arc<Slot>, handle: u64, epoch: u64) -> Dir {
    Dir {
      slot,
      epoch,
      st: Mutex::new(State { handle, closed: false }),
    }
  }

  pub(crate) fn state(&self) -> Result<MutexGuard<'_, State>> {
    let st = self.st.lock().unwrap_or_else(|e| e.into_inner());
    if st.closed {
      return Err(Error::server("EBADF", "directory is closed"));
    }
    Ok(st)
  }

  pub(crate) fn unary<R: prost::Message + Default>(&self, m: u16, req: &impl prost::Message) -> Result<R> {
    Ok(self.slot.run(Some(self.epoch), |c| c.call_msg::<R>(m, req, &[]))?.0 .0)
  }

  /// Reads up to `max` entries. The flag is true when the directory has no more entries.
  pub fn readdir(&self, max: u32) -> Result<(Vec<DirEntry>, bool)> {
    let st = self.state()?;
    let r: ReaddirResponse = self.unary(
      method::READDIR,
      &ReaddirRequest {
        handle: st.handle,
        max_entries: max,
      },
    )?;
    Ok((r.entries, r.eof))
  }

  /// Closes the handle. A second close returns EBADF.
  pub fn close(&self) -> Result<()> {
    let mut st = self.state()?;
    st.closed = true;
    self
      .unary::<Empty>(method::CLOSEDIR, &ClosedirRequest { handle: st.handle })
      .map(drop)
  }
}

impl Drop for Dir {
  fn drop(&mut self) {
    let (closed, handle) = {
      let st = self.st.get_mut().unwrap_or_else(|e| e.into_inner());
      (st.closed, st.handle)
    };
    if !closed {
      let _ = self.unary::<Empty>(method::CLOSEDIR, &ClosedirRequest { handle });
    }
  }
}
