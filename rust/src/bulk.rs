//! Bulk calls on the client: batched stat and whole-file read and write.

use crate::client::Client;
use crate::error::{Error, Result};
use crate::events::lock;
use crate::frame::MAX_META_BYTES;
use crate::wire::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

/// The server rejects more items than this in one batch request.
pub(crate) const MAX_BATCH_ITEMS: usize = 4096;

/// Result of one path in `Client::stat_batch`.
pub type StatResult = Result<FileInfo>;

/// Turns a server error for one item into an inner result. Connection and protocol errors stay outer.
pub(crate) fn item<T>(r: Result<T>) -> Result<Result<T>> {
  match r {
    Err(e @ Error::Server { .. }) => Ok(Err(e)),
    other => other.map(Ok),
  }
}

/// Maps the error list of a batch reply to one slot per item.
pub(crate) fn batch_errors(n: usize, errors: Vec<BatchItemError>) -> Result<Vec<Option<Error>>> {
  let mut out: Vec<Option<Error>> = (0..n).map(|_| None).collect();
  for e in errors {
    let slot = out
      .get_mut(e.index as usize)
      .ok_or_else(|| Error::Protocol(format!("batch error index {} out of {n}", e.index)))?;
    *slot = Some(Error::Server {
      code: e.code,
      message: e.message,
    });
  }
  Ok(out)
}

/// Runs `job(0..n)` on up to `workers` threads and returns the results in index order. After the
/// first failure no new job starts. The first failure in index order is returned.
pub(crate) fn par_map<T: Send>(n: usize, workers: usize, job: impl Fn(usize) -> Result<T> + Sync) -> Result<Vec<T>> {
  if n <= 1 || workers <= 1 {
    return (0..n).map(job).collect();
  }
  let next = AtomicUsize::new(0);
  let failed = AtomicBool::new(false);
  let slots: Mutex<Vec<Option<Result<T>>>> = Mutex::new((0..n).map(|_| None).collect());
  std::thread::scope(|sc| {
    for _ in 0..workers.min(n) {
      sc.spawn(|| loop {
        let i = next.fetch_add(1, Ordering::SeqCst);
        if i >= n || failed.load(Ordering::SeqCst) {
          break;
        }
        let r = job(i);
        if r.is_err() {
          failed.store(true, Ordering::SeqCst);
        }
        lock(&slots)[i] = Some(r);
      });
    }
  });
  let mut out = Vec::with_capacity(n);
  for r in slots
    .into_inner()
    .unwrap_or_else(|e| e.into_inner())
    .into_iter()
    .flatten()
  {
    out.push(r?);
  }
  Ok(out)
}

impl Client {
  /// Returns attributes for many paths. The result has one entry per path in input order.
  /// Large inputs are split into several requests that run in parallel on the pool.
  pub fn stat_batch(&self, paths: &[String]) -> Result<Vec<StatResult>> {
    let meta_budget = MAX_META_BYTES as usize / 2;
    let mut parts = Vec::new();
    let mut start = 0;
    while start < paths.len() {
      let (mut end, mut size) = (start, 0usize);
      while end < paths.len()
        && end - start < MAX_BATCH_ITEMS
        && (end == start || size + paths[end].len() + 8 <= meta_budget)
      {
        size += paths[end].len() + 8;
        end += 1;
      }
      parts.push(&paths[start..end]);
      start = end;
    }
    let chunks = par_map(parts.len(), self.pool_size(), |i| {
      let part = parts[i];
      let r: StatBatchResponse = self.unary(method::STAT_BATCH, &StatBatchRequest { paths: part.to_vec() })?;
      if r.infos.len() != part.len() {
        return Err(Error::Protocol(format!(
          "stat batch returned {} infos for {} paths",
          r.infos.len(),
          part.len()
        )));
      }
      let mut errs = batch_errors(part.len(), r.errors)?;
      Ok(
        r.infos
          .into_iter()
          .enumerate()
          .map(|(i, info)| match errs[i].take() {
            Some(e) => Err(e),
            None => Ok(info),
          })
          .collect::<Vec<_>>(),
      )
    })?;
    Ok(chunks.into_iter().flatten().collect())
  }

  /// Reads a whole file. A file that fits in one payload takes one request. A larger file is
  /// opened and read in chunks until end of file.
  pub fn read_file(&self, path: &str) -> Result<Vec<u8>> {
    let (data, limit) = self
      .run_any(|c| {
        let req = ReadFileRequest {
          path: path.into(),
          max_bytes: c.max_payload() as u64,
        };
        let (r, data) = c.call_msg::<ReadFileResponse>(method::READ_FILE, &req, &[])?;
        if r.count as usize != data.len() {
          return Err(Error::Protocol(format!(
            "read file count {} payload {}",
            r.count,
            data.len()
          )));
        }
        Ok((data, c.max_payload()))
      })?
      .0;
    if data.len() < limit {
      return Ok(data);
    }
    let f = self.open(path, open_flag::READ_ONLY, 0)?;
    let mut out = Vec::new();
    loop {
      let part = f.read_at(out.len() as u64, limit)?;
      let short = part.len() < limit;
      out.extend_from_slice(&part);
      if short {
        return Ok(out);
      }
    }
  }

  /// Writes a whole file, creating it when missing. With `truncate` the old content is dropped
  /// first. Data larger than one payload is written through an opened file in chunks.
  pub fn write_file(&self, path: &str, data: &[u8], truncate: bool) -> Result<()> {
    let sent = self
      .run_any(|c| {
        if data.len() > c.max_payload() {
          return Ok(false);
        }
        let req = WriteFileRequest {
          path: path.into(),
          truncate,
        };
        let (r, _) = c.call_msg::<WriteFileResponse>(method::WRITE_FILE, &req, data)?;
        if r.count as usize != data.len() {
          return Err(Error::Protocol(format!(
            "write file count {} for {} bytes",
            r.count,
            data.len()
          )));
        }
        Ok(true)
      })?
      .0;
    if sent {
      return Ok(());
    }
    let mut flags = open_flag::WRITE_ONLY | open_flag::CREATE;
    if truncate {
      flags |= open_flag::TRUNC;
    }
    let f = self.open(path, flags, 0o644)?;
    f.write_at(0, data)?;
    f.close()
  }
}
