//! Wire frame codec. Mirrors the Go reference in go/frame/frame.go.
//!
//! Layout: 24-byte big-endian header (magic, version, flags, method, request id,
//! meta length, payload length), then Protobuf meta, then raw payload bytes.

use crate::error::{Error, Result};
use std::io::{self, Read, Write};

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

/// Writes one frame. Rejects sections over the limits before sending anything.
pub fn write<W: Write>(w: &mut W, f: &Frame) -> Result<()> {
  if f.meta.len() as u64 > MAX_META_BYTES as u64 {
    return Err(Error::Protocol(format!("meta {} > {MAX_META_BYTES}", f.meta.len())));
  }
  if f.payload.len() as u64 > MAX_PAYLOAD_BYTES as u64 {
    return Err(Error::Protocol(format!(
      "payload {} > {MAX_PAYLOAD_BYTES}",
      f.payload.len()
    )));
  }
  let mut buf = Vec::with_capacity(HEADER_SIZE + f.meta.len());
  buf.extend_from_slice(&MAGIC);
  buf.push(VERSION);
  buf.push(f.flags);
  buf.extend_from_slice(&f.method.to_be_bytes());
  buf.extend_from_slice(&f.request_id.to_be_bytes());
  buf.extend_from_slice(&(f.meta.len() as u32).to_be_bytes());
  buf.extend_from_slice(&(f.payload.len() as u32).to_be_bytes());
  buf.extend_from_slice(&f.meta);
  w.write_all(&buf)?;
  if !f.payload.is_empty() {
    w.write_all(&f.payload)?;
  }
  Ok(())
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
  let mut meta = vec![0u8; meta_len as usize];
  r.read_exact(&mut meta)?;
  let mut payload = vec![0u8; payload_len as usize];
  r.read_exact(&mut payload)?;
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
    assert!(matches!(write(&mut Vec::new(), &f), Err(Error::Protocol(_))));
  }
}
