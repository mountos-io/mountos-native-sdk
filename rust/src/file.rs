use crate::bulk::{batch_errors, item, MAX_BATCH_ITEMS};
use crate::client::{Slot, Sock};
use crate::error::{Error, Result};
use crate::frame::MAX_PAYLOAD_BYTES;
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

/// One ReadAt request of at most one payload from `base`. Returns the data and the length asked.
fn read_chunk(c: &Sock, handle: u64, base: i64, len: usize) -> Result<(Vec<u8>, usize)> {
  let want = len.min(c.max_payload());
  let req = ReadAtRequest {
    handle,
    offset: base,
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
  Ok((data, want))
}

/// One WriteAt request of at most one payload from the start of `rest`. Returns the bytes stored.
fn write_chunk(c: &Sock, handle: u64, base: i64, rest: &[u8]) -> Result<usize> {
  let chunk = &rest[..rest.len().min(c.max_payload())];
  let (r, _) = c.call_msg::<WriteAtResponse>(method::WRITE_AT, &WriteAtRequest { handle, offset: base }, chunk)?;
  let n = r.count as usize;
  if n == 0 || n > chunk.len() {
    return Err(Error::Protocol(format!(
      "write reply count {n} for {} bytes",
      chunk.len()
    )));
  }
  Ok(n)
}

fn offset_at(base: i64, done: usize) -> Result<i64> {
  base
    .checked_add(done as i64)
    .ok_or_else(|| Error::server("EINVAL", "offset overflow"))
}

/// What one pass of a batch call produced. A `Group` holds the results of the leading items and
/// how many items they cover. `Big` means the first item needs more than one payload and goes
/// through the chunked path.
enum Step<T> {
  Group(Vec<Result<T>>, usize),
  Big,
}

/// One write in `File::write_batch`. Build it with `WriteItem::new`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteItem {
  pub offset: u64,
  pub data: Vec<u8>,
}

impl WriteItem {
  /// Writes `data` at `offset`.
  pub fn new(offset: u64, data: Vec<u8>) -> WriteItem {
    WriteItem { offset, data }
  }
}

/// Reads the leading ranges of `ranges` that fit in one ReadBatch request.
fn read_group(c: &Sock, handle: u64, ranges: &[Range]) -> Result<Step<Vec<u8>>> {
  if ranges[0].length as usize > c.max_payload() {
    return Ok(Step::Big);
  }
  let (mut end, mut bytes) = (0, 0usize);
  while end < ranges.len() && end < MAX_BATCH_ITEMS && bytes + ranges[end].length as usize <= c.max_payload() {
    bytes += ranges[end].length as usize;
    end += 1;
  }
  let part = &ranges[..end];
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
  let mut out = Vec::with_capacity(part.len());
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
  Ok(Step::Group(out, end))
}

/// Writes the leading items of `items` that fit in one WriteBatch request. An item whose offset
/// cannot be sent fails alone without a request.
fn write_group(c: &Sock, handle: u64, items: &[WriteItem]) -> Result<Step<usize>> {
  if let Err(e) = to_i64(items[0].offset) {
    return Ok(Step::Group(vec![Err(e)], 1));
  }
  if items[0].data.len() > c.max_payload() {
    return Ok(Step::Big);
  }
  let (mut end, mut bytes) = (0, 0usize);
  while end < items.len()
    && end < MAX_BATCH_ITEMS
    && items[end].offset <= i64::MAX as u64
    && bytes + items[end].data.len() <= c.max_payload()
  {
    bytes += items[end].data.len();
    end += 1;
  }
  let part = &items[..end];
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
  let mut out = Vec::with_capacity(part.len());
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
  Ok(Step::Group(out, end))
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

  /// Reads `len` bytes from `base` one request at a time, so the connection is free between
  /// requests. Stops early at end of file.
  fn read_from(&self, handle: u64, base: i64, len: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(len.min(MAX_PAYLOAD_BYTES as usize));
    while out.len() < len {
      let off = offset_at(base, out.len())?;
      let ((data, want), _) = self
        .slot
        .run(Some(self.epoch), |c| read_chunk(c, handle, off, len - out.len()))?;
      let short = data.len() < want;
      out.extend_from_slice(&data);
      if short {
        break;
      }
    }
    Ok(out)
  }

  /// Writes all of `buf` at `base` one request at a time. On failure after some bytes were
  /// stored, the error carries the count.
  fn write_to(&self, handle: u64, base: i64, buf: &[u8]) -> std::result::Result<usize, (usize, Error)> {
    let mut done = 0;
    while done < buf.len() {
      let off = offset_at(base, done).map_err(|e| (done, e))?;
      let (n, _) = self
        .slot
        .run(Some(self.epoch), |c| write_chunk(c, handle, off, &buf[done..]))
        .map_err(|e| (done, e))?;
      done += n;
    }
    Ok(done)
  }

  fn do_read(&self, handle: u64, offset: u64, len: usize) -> Result<Vec<u8>> {
    self.read_from(handle, to_i64(offset)?, len)
  }

  fn do_write(&self, handle: u64, offset: u64, buf: &[u8]) -> Result<usize> {
    self.write_to(handle, to_i64(offset)?, buf).map_err(|(_, e)| e)
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
  /// a failed range returns its own error. The connection is free between requests.
  pub fn read_batch(&self, ranges: &[Range]) -> Result<Vec<Result<Vec<u8>>>> {
    let st = self.state()?;
    let handle = st.handle;
    let mut out = Vec::with_capacity(ranges.len());
    let mut start = 0;
    while start < ranges.len() {
      let (step, _) = self
        .slot
        .run(Some(self.epoch), |c| read_group(c, handle, &ranges[start..]))?;
      match step {
        Step::Group(items, n) => {
          out.extend(items);
          start += n;
        }
        Step::Big => {
          out.push(item(self.read_from(
            handle,
            ranges[start].offset,
            ranges[start].length as usize,
          ))?);
          start += 1;
        }
      }
    }
    Ok(out)
  }

  /// Writes many items in as few requests as the limits allow. Each inner result is the
  /// byte count written for that item or its own error. An item larger than one payload that
  /// fails after some bytes were stored returns `Error::Partial` with the count.
  pub fn write_batch(&self, items: &[WriteItem]) -> Result<Vec<Result<usize>>> {
    let st = self.state()?;
    let handle = st.handle;
    let mut out = Vec::with_capacity(items.len());
    let mut start = 0;
    while start < items.len() {
      let (step, _) = self
        .slot
        .run(Some(self.epoch), |c| write_group(c, handle, &items[start..]))?;
      match step {
        Step::Group(res, n) => {
          out.extend(res);
          start += n;
        }
        Step::Big => {
          let it = &items[start];
          let r = match self.write_to(handle, it.offset as i64, &it.data) {
            Ok(n) => Ok(Ok(n)),
            Err((0, e)) => item(Err(e)),
            Err((written, e @ Error::Server { .. })) => Ok(Err(Error::Partial {
              written,
              source: Box::new(e),
            })),
            Err((_, e)) => Err(e),
          };
          out.push(r?);
          start += 1;
        }
      }
    }
    Ok(out)
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
