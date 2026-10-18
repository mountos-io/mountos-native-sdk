//! Wire frame codec. Mirrors the Go reference in go/frame/frame.go.
//!
//! Layout: 24-byte big-endian header (magic, version, flags, method, request id,
//! meta length, payload length), then Protobuf meta, then raw payload bytes.

use crate::error::{Error, Result};
use std::io::{self, IoSlice, Read, Write};

pub const MAGIC: [u8; 4] = *b"MFS1";
pub const VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 24;

pub const FLAG_RESPONSE: u8 = 1;
pub const FLAG_ERROR: u8 = 2;
pub const FLAG_STREAM_END: u8 = 4;
/// Server-initiated frame (a notice). The request id is the id of the call it answers.
pub const FLAG_NOTIFY: u8 = 8;

pub const MAX_META_BYTES: u32 = 16 << 20;
pub const MAX_PAYLOAD_BYTES: u32 = 8 << 20;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Frame {
  pub flags: u8,
  pub method: u16,
  pub request_id: u64,
  pub meta: Vec<u8>,
  pub payload: Vec<u8>,
}

/// Rejects sections over the limits. Callers run this before they send a byte, so an oversize
/// request fails alone and leaves the connection usable.
pub fn check_limits(meta_len: usize, payload_len: usize) -> Result<()> {
  if meta_len as u64 > MAX_META_BYTES as u64 {
    return Err(Error::TooLarge(format!("meta {meta_len} > {MAX_META_BYTES}")));
  }
  if payload_len as u64 > MAX_PAYLOAD_BYTES as u64 {
    return Err(Error::TooLarge(format!("payload {payload_len} > {MAX_PAYLOAD_BYTES}")));
  }
  Ok(())
}

/// Writes one frame. Rejects sections over the limits before sending anything.
pub fn write<W: Write>(w: &mut W, f: &Frame) -> Result<()> {
  write_parts(w, f.flags, f.method, f.request_id, &f.meta, &f.payload)
}

/// Writes one frame from borrowed sections with a single vectored write, so the payload is not
/// copied and the header, meta and payload leave in one call.
pub fn write_parts<W: Write>(
  w: &mut W,
  flags: u8,
  method: u16,
  request_id: u64,
  meta: &[u8],
  payload: &[u8],
) -> Result<()> {
  check_limits(meta.len(), payload.len())?;
  let mut hdr = [0u8; HEADER_SIZE];
  hdr[0..4].copy_from_slice(&MAGIC);
  hdr[4] = VERSION;
  hdr[5] = flags;
  hdr[6..8].copy_from_slice(&method.to_be_bytes());
  hdr[8..16].copy_from_slice(&request_id.to_be_bytes());
  hdr[16..20].copy_from_slice(&(meta.len() as u32).to_be_bytes());
  hdr[20..24].copy_from_slice(&(payload.len() as u32).to_be_bytes());
  let mut parts = [IoSlice::new(&hdr), IoSlice::new(meta), IoSlice::new(payload)];
  let mut rest: &mut [IoSlice<'_>] = &mut parts;
  IoSlice::advance_slices(&mut rest, 0);
  while !rest.is_empty() {
    match w.write_vectored(rest) {
      Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
      Ok(n) => IoSlice::advance_slices(&mut rest, n),
      Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
      Err(e) => return Err(e.into()),
    }
  }
  Ok(())
}

/// Step a large section grows by. The buffer follows the bytes that arrived, never the length
/// the header claims, so a peer that claims a large section and stalls holds at most one step.
const READ_STEP: usize = 64 << 10;

fn read_section<R: Read>(r: &mut R, n: usize) -> io::Result<Vec<u8>> {
  if n <= READ_STEP {
    let mut b = vec![0u8; n];
    r.read_exact(&mut b)?;
    return Ok(b);
  }
  let mut buf = Vec::with_capacity(READ_STEP);
  while buf.len() < n {
    let got = buf.len();
    buf.resize(got + (n - got).min(READ_STEP), 0);
    r.read_exact(&mut buf[got..])?;
  }
  Ok(buf)
}

/// Reads one frame. Returns `Ok(None)` only when the stream ends cleanly at a
/// frame boundary. A mid-frame end is an `UnexpectedEof` I/O error.
pub fn read<R: Read>(r: &mut R) -> Result<Option<Frame>> {
  let mut hdr = [0u8; HEADER_SIZE];
  let mut got = 0;
  while got < HEADER_SIZE {
    match r.read(&mut hdr[got..]) {
      Ok(0) if got == 0 => return Ok(None),
      Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
      Ok(n) => got += n,
      Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
      Err(e) => return Err(e.into()),
    }
  }
  if hdr[0..4] != MAGIC {
    return Err(Error::Protocol("bad magic".into()));
  }
  if hdr[4] != VERSION {
    return Err(Error::Protocol(format!("unsupported frame version {}", hdr[4])));
  }
  let meta_len = u32::from_be_bytes(hdr[16..20].try_into().unwrap());
  let payload_len = u32::from_be_bytes(hdr[20..24].try_into().unwrap());
  if meta_len > MAX_META_BYTES {
    return Err(Error::Protocol(format!("meta {meta_len} > {MAX_META_BYTES}")));
  }
  if payload_len > MAX_PAYLOAD_BYTES {
    return Err(Error::Protocol(format!("payload {payload_len} > {MAX_PAYLOAD_BYTES}")));
  }
  let meta = read_section(r, meta_len as usize)?;
  let payload = read_section(r, payload_len as usize)?;
  Ok(Some(Frame {
    flags: hdr[5],
    method: u16::from_be_bytes(hdr[6..8].try_into().unwrap()),
    request_id: u64::from_be_bytes(hdr[8..16].try_into().unwrap()),
    meta,
    payload,
  }))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn roundtrip_and_layout() {
    let f = Frame {
      flags: FLAG_RESPONSE,
      method: 14,
      request_id: 7,
      meta: vec![1, 2],
      payload: vec![9; 5],
    };
    let mut buf = Vec::new();
    write(&mut buf, &f).unwrap();
    assert_eq!(&buf[0..6], b"MFS1\x01\x01");
    assert_eq!(&buf[16..24], &[0, 0, 0, 2, 0, 0, 0, 5]);
    assert_eq!(read(&mut &buf[..]).unwrap(), Some(f));
  }

  #[test]
  fn clean_eof_truncation_and_bad_header() {
    assert_eq!(read(&mut &[][..]).unwrap(), None);
    let mut buf = Vec::new();
    write(
      &mut buf,
      &Frame {
        meta: vec![1; 4],
        ..Default::default()
      },
    )
    .unwrap();
    assert!(matches!(read(&mut &buf[..10]), Err(Error::Io(_))));
    assert!(matches!(read(&mut &buf[..HEADER_SIZE + 2]), Err(Error::Io(_))));
    let mut bad = buf.clone();
    bad[0] = b'X';
    assert!(matches!(read(&mut &bad[..]), Err(Error::Protocol(_))));
    let mut bad = buf.clone();
    bad[4] = 2;
    assert!(matches!(read(&mut &bad[..]), Err(Error::Protocol(_))));
    let mut big = buf.clone();
    big[20..24].copy_from_slice(&(MAX_PAYLOAD_BYTES + 1).to_be_bytes());
    assert!(matches!(read(&mut &big[..]), Err(Error::Protocol(_))));
  }

  #[test]
  fn write_rejects_oversize_payload() {
    let f = Frame {
      payload: vec![0; MAX_PAYLOAD_BYTES as usize + 1],
      ..Default::default()
    };
    assert!(matches!(write(&mut Vec::new(), &f), Err(Error::TooLarge(_))));
  }

  /// Serves a header, then zeros, then ends. Records the largest buffer a read was asked to fill.
  struct Probe {
    head: Vec<u8>,
    zeros: usize,
    max_ask: usize,
  }

  impl Read for Probe {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      if !self.head.is_empty() {
        let n = buf.len().min(self.head.len());
        buf[..n].copy_from_slice(&self.head[..n]);
        self.head.drain(..n);
        return Ok(n);
      }
      self.max_ask = self.max_ask.max(buf.len());
      let n = buf.len().min(self.zeros);
      buf[..n].fill(0);
      self.zeros -= n;
      Ok(n)
    }
  }

  #[test]
  fn large_claim_is_read_in_steps() {
    let mut hdr = Vec::new();
    write(&mut hdr, &Frame::default()).unwrap();
    hdr[20..24].copy_from_slice(&MAX_PAYLOAD_BYTES.to_be_bytes());
    let mut p = Probe {
      head: hdr,
      zeros: 100_000,
      max_ask: 0,
    };
    assert!(matches!(read(&mut p), Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof));
    assert!(p.max_ask <= READ_STEP, "asked for {}", p.max_ask);
  }

  #[test]
  fn write_uses_one_call_per_frame() {
    struct Count(usize, Vec<u8>);
    impl Write for Count {
      fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0 += 1;
        self.1.extend_from_slice(b);
        Ok(b.len())
      }
      fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.0 += 1;
        let mut n = 0;
        for b in bufs {
          self.1.extend_from_slice(b);
          n += b.len();
        }
        Ok(n)
      }
      fn flush(&mut self) -> io::Result<()> {
        Ok(())
      }
    }
    let f = Frame {
      meta: vec![1; 3],
      payload: vec![2; 9],
      ..Default::default()
    };
    let mut c = Count(0, Vec::new());
    write(&mut c, &f).unwrap();
    assert_eq!(c.0, 1);
    assert_eq!(read(&mut &c.1[..]).unwrap(), Some(f));
  }
}
