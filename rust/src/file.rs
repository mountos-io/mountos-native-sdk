use crate::bulk::{batch_errors, item, MAX_BATCH_ITEMS};
use crate::client::{Conn, Slot};
use crate::error::{Error, Result};
use crate::wire::*;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) fn to_i64(v: u64) -> Result<i64> {
  i64::try_from(v).map_err(|_| Error::server("EINVAL", "value out of range"))
}

pub(crate) struct State {
  pub(crate) handle: u64,
  pos: u64,
  closed: bool,
}

/// Reads `len` bytes from `base` in frames within the payload limit. Stops early at end of file.
fn read_on(c: &mut Conn, handle: u64, base: i64, len: usize) -> Result<Vec<u8>> {
  let mut out = Vec::with_capacity(len.min(c.max_payload));
  while out.len() < len {
    let want = (len - out.len()).min(c.max_payload);
    let off = base
      .checked_add(out.len() as i64)
      .ok_or_else(|| Error::server("EINVAL", "offset overflow"))?;
    let req = ReadAtRequest {
      handle,
      offset: off,
      length: want as u32,
    };
    let (r, data) = c.call_msg::<ReadAtResponse>(method::READ_AT, &req, &[])?;
    if data.len() != r.count as usize || data.len() > want {
      return Err(Error::Protocol(format!(
        "read reply count {} payload {} want {want}",
        r.count,
        data.len()
      )));
    }
    let short = data.len() < want;
    out.extend_from_slice(&data);
    if short {
      break;
    }
  }
  Ok(out)
}

/// Writes all of `buf` at `base` in frames within the payload limit.
fn write_on(c: &mut Conn, handle: u64, base: i64, buf: &[u8]) -> Result<usize> {
  let mut done = 0;
  while done < buf.len() {
    let chunk = &buf[done..buf.len().min(done + c.max_payload)];
    let off = base
      .checked_add(done as i64)
      .ok_or_else(|| Error::server("EINVAL", "offset overflow"))?;
    let (r, _) = c.call_msg::<WriteAtResponse>(method::WRITE_AT, &WriteAtRequest { handle, offset: off }, chunk)?;
    let n = r.count as usize;
    if n == 0 || n > chunk.len() {
      return Err(Error::Protocol(format!(
        "write reply count {n} for {} bytes",
        chunk.len()
      )));
    }
    done += n;
  }
  Ok(done)
}

/// One write in `File::write_batch`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteItem {
  pub offset: u64,
  pub data: Vec<u8>,
}

/// An open remote file. Calls on one file are serialized by a mutex. Different files
/// on different pool connections run in parallel.
pub struct File {
  slot: Arc<Slot>,
  epoch: u64,
  st: Mutex<State>,
}

impl File {
  pub(crate) fn new(slot: Arc<Slot>, handle: u64, epoch: u64) -> File {
    File {
      slot,
      epoch,
      st: Mutex::new(State {
        handle,
        pos: 0,
        closed: false,
      }),
    }
  }

  pub(crate) fn state(&self) -> Result<MutexGuard<'_, State>> {
    let st = self.st.lock().unwrap_or_else(|e| e.into_inner());
    if st.closed {
      return Err(Error::server("EBADF", "file is closed"));
    }
    Ok(st)
  }

  pub(crate) fn unary<R: prost::Message + Default>(&self, m: u16, req: &impl prost::Message) -> Result<R> {
    Ok(self.slot.run(Some(self.epoch), |c| c.call_msg::<R>(m, req, &[]))?.0 .0)
  }

  fn do_read(&self, handle: u64, offset: u64, len: usize) -> Result<Vec<u8>> {
    let base = to_i64(offset)?;
    self
      .slot
      .run(Some(self.epoch), |c| read_on(c, handle, base, len))
      .map(|v| v.0)
  }

  fn do_write(&self, handle: u64, offset: u64, buf: &[u8]) -> Result<usize> {
    let base = to_i64(offset)?;
    self
      .slot
      .run(Some(self.epoch), |c| write_on(c, handle, base, buf))
      .map(|v| v.0)
  }

  fn do_fstat(&self, handle: u64) -> Result<FileInfo> {
    let r: StatResponse = self.unary(method::FSTAT, &FstatRequest { handle })?;
    r.info.ok_or_else(|| Error::Protocol("fstat reply has no info".into()))
  }

  /// Reads up to `len` bytes at `offset`. The result is shorter than `len` only at end of file.
  pub fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
    let st = self.state()?;
    self.do_read(st.handle, offset, len)
  }

  /// Writes all of `buf` at `offset`, split into frames within the payload limit.
  pub fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
    let st = self.state()?;
    self.do_write(st.handle, offset, buf)
  }

  /// Reads many ranges in as few requests as the limits allow. The outer error means the call
  /// failed. Each inner result is one range: a range past end of file returns fewer bytes,
  /// a failed range returns its own error.
  pub fn read_batch(&self, ranges: &[Range]) -> Result<Vec<Result<Vec<u8>>>> {
    let st = self.state()?;
    let handle = st.handle;
    self
      .slot
      .run(Some(self.epoch), |c| {
        let mut out = Vec::with_capacity(ranges.len());
        let mut start = 0;
        while start < ranges.len() {
          if ranges[start].length as usize > c.max_payload {
            out.push(item(read_on(
              c,
              handle,
              ranges[start].offset,
              ranges[start].length as usize,
            ))?);
            start += 1;
            continue;
          }
          let (mut end, mut bytes) = (start, 0usize);
          while end < ranges.len()
            && end - start < MAX_BATCH_ITEMS
            && bytes + ranges[end].length as usize <= c.max_payload
          {
            bytes += ranges[end].length as usize;
            end += 1;
          }
          let part = &ranges[start..end];
          let req = ReadBatchRequest {
            handle,
            ranges: part.to_vec(),
          };
          let (resp, data) = c.call_msg::<ReadBatchResponse>(method::READ_BATCH, &req, &[])?;
          if resp.counts.len() != part.len() {
            return Err(Error::Protocol(format!(
              "read batch returned {} counts for {} ranges",
              resp.counts.len(),
              part.len()
            )));
          }
          let mut errs = batch_errors(part.len(), resp.errors)?;
          let mut pos = 0usize;
          for (i, r) in part.iter().enumerate() {
            if let Some(e) = errs[i].take() {
              out.push(Err(e));
              continue;
            }
            let n = resp.counts[i] as usize;
            if n > r.length as usize || pos + n > data.len() {
              return Err(Error::Protocol(format!(
                "read batch count {n} for range {i} is out of bounds"
              )));
            }
            out.push(Ok(data[pos..pos + n].to_vec()));
            pos += n;
          }
          if pos != data.len() {
            return Err(Error::Protocol(format!(
              "read batch payload {} bytes, counts sum to {pos}",
              data.len()
            )));
          }
          start = end;
        }
        Ok(out)
      })
      .map(|v| v.0)
  }

  /// Writes many items in as few requests as the limits allow. Each inner result is the
  /// byte count written for that item or its own error.
  pub fn write_batch(&self, items: &[WriteItem]) -> Result<Vec<Result<usize>>> {
    let st = self.state()?;
    let handle = st.handle;
    self
      .slot
      .run(Some(self.epoch), |c| {
        let mut out = Vec::with_capacity(items.len());
        let mut start = 0;
        while start < items.len() {
          if let Err(e) = to_i64(items[start].offset) {
            out.push(Err(e));
            start += 1;
            continue;
          }
          if items[start].data.len() > c.max_payload {
            let r = to_i64(items[start].offset).and_then(|o| write_on(c, handle, o, &items[start].data));
            out.push(item(r)?);
            start += 1;
            continue;
          }
          let (mut end, mut bytes) = (start, 0usize);
          while end < items.len()
            && end - start < MAX_BATCH_ITEMS
            && items[end].offset <= i64::MAX as u64
            && bytes + items[end].data.len() <= c.max_payload
          {
            bytes += items[end].data.len();
            end += 1;
          }
          let part = &items[start..end];
          let mut ranges = Vec::with_capacity(part.len());
          let mut payload = Vec::with_capacity(bytes);
          for it in part {
            ranges.push(Range {
              offset: it.offset as i64,
              length: it.data.len() as u32,
            });
            payload.extend_from_slice(&it.data);
          }
          let (resp, _) =
            c.call_msg::<WriteBatchResponse>(method::WRITE_BATCH, &WriteBatchRequest { handle, ranges }, &payload)?;
          if resp.counts.len() != part.len() {
            return Err(Error::Protocol(format!(
              "write batch returned {} counts for {} items",
              resp.counts.len(),
              part.len()
            )));
          }
          let mut errs = batch_errors(part.len(), resp.errors)?;
          for (i, it) in part.iter().enumerate() {
            if let Some(e) = errs[i].take() {
              out.push(Err(e));
              continue;
            }
            let n = resp.counts[i] as usize;
            if n > it.data.len() {
              return Err(Error::Protocol(format!(
                "write batch count {n} for item {i} exceeds its length"
              )));
            }
            out.push(Ok(n));
          }
          start = end;
        }
        Ok(out)
      })
      .map(|v| v.0)
  }

  pub fn fsync(&self, data_only: bool) -> Result<()> {
    let st = self.state()?;
    self
      .unary::<Empty>(
        method::FSYNC,
        &FsyncRequest {
          handle: st.handle,
          data_only,
        },
      )
      .map(drop)
  }

  /// Closes the handle. A second close returns EBADF.
  pub fn close(&self) -> Result<()> {
    let mut st = self.state()?;
    st.closed = true;
    self
      .unary::<Empty>(method::CLOSE, &CloseRequest { handle: st.handle })
      .map(drop)
  }
}

impl Drop for File {
  fn drop(&mut self) {
    let (closed, handle) = {
      let st = self.st.get_mut().unwrap_or_else(|e| e.into_inner());
      (st.closed, st.handle)
    };
    if !closed {
      let _ = self.unary::<Empty>(method::CLOSE, &CloseRequest { handle });
    }
  }
}

impl Read for &File {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let mut st = self.state()?;
    let data = self.do_read(st.handle, st.pos, buf.len())?;
    buf[..data.len()].copy_from_slice(&data);
    st.pos += data.len() as u64;
    Ok(data.len())
  }
}

impl Write for &File {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    let mut st = self.state()?;
    let n = self.do_write(st.handle, st.pos, buf)?;
    st.pos += n as u64;
    Ok(n)
  }

  fn flush(&mut self) -> io::Result<()> {
    File::flush(self).map_err(Into::into)
  }
}

impl Seek for &File {
  fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
    let mut st = self.state()?;
    let (base, delta) = match from {
      SeekFrom::Start(p) => (0i128, p as i128),
      SeekFrom::Current(d) => (st.pos as i128, d as i128),
      SeekFrom::End(d) => (self.do_fstat(st.handle)?.size as i128, d as i128),
    };
    let pos = base + delta;
    if pos < 0 || pos > i64::MAX as i128 {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek out of range"));
    }
    st.pos = pos as u64;
    Ok(st.pos)
  }
}

impl Read for File {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    (&*self).read(buf)
  }
}

impl Write for File {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    (&*self).write(buf)
  }

  fn flush(&mut self) -> io::Result<()> {
    File::flush(self).map_err(Into::into)
  }
}

impl Seek for File {
  fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
    (&*self).seek(from)
  }
}
