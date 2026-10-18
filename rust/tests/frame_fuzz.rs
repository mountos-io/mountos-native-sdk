//! Seeded random tests for the frame reader and writer. A failure replays from SEED and the
//! iteration number. The reader must never panic and must fail only with a protocol or EOF error.

use mountos_native::frame::{self, Frame, HEADER_SIZE, MAX_META_BYTES, MAX_PAYLOAD_BYTES};
use mountos_native::Error;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{self, Cursor, Read};
use std::panic::{catch_unwind, AssertUnwindSafe};

const SEED: u64 = 0x6d66_7331;
const ITERATIONS: usize = 20_000;

thread_local! {
  /// Largest single allocation requested on this thread since the last reset.
  static MAX_ALLOC: Cell<usize> = const { Cell::new(0) };
}

struct Tracking;

unsafe impl GlobalAlloc for Tracking {
  unsafe fn alloc(&self, l: Layout) -> *mut u8 {
    let _ = MAX_ALLOC.try_with(|m| m.set(m.get().max(l.size())));
    System.alloc(l)
  }
  unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
    System.dealloc(p, l)
  }
  unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
    let _ = MAX_ALLOC.try_with(|m| m.set(m.get().max(n)));
    System.realloc(p, l, n)
  }
}

#[global_allocator]
static ALLOC: Tracking = Tracking;

struct Rng(u64);

impl Rng {
  fn next(&mut self) -> u64 {
    self.0 ^= self.0 << 13;
    self.0 ^= self.0 >> 7;
    self.0 ^= self.0 << 17;
    self.0
  }
  fn below(&mut self, n: usize) -> usize {
    (self.next() % n as u64) as usize
  }
  fn bytes(&mut self, n: usize) -> Vec<u8> {
    (0..n).map(|_| self.next() as u8).collect()
  }
  /// Random bytes with a length from `min` up to `min + span - 1`.
  fn some(&mut self, min: usize, span: usize) -> Vec<u8> {
    let n = min + self.below(span);
    self.bytes(n)
  }
}

fn random_frame(r: &mut Rng) -> Frame {
  Frame {
    flags: r.below(16) as u8,
    method: r.below(65536) as u16,
    request_id: r.next(),
    meta: r.some(0, 64),
    payload: r.some(0, 256),
  }
}

fn wire(f: &Frame) -> Vec<u8> {
  let mut v = Vec::new();
  frame::write(&mut v, f).unwrap();
  v
}

/// Returns short reads of random size and occasional Interrupted errors.
struct Chunky<'a> {
  data: &'a [u8],
  rng: Rng,
}

impl Read for Chunky<'_> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    if self.rng.below(8) == 0 {
      return Err(io::ErrorKind::Interrupted.into());
    }
    let n = (1 + self.rng.below(buf.len().max(1))).min(self.data.len()).min(buf.len());
    buf[..n].copy_from_slice(&self.data[..n]);
    self.data = &self.data[n..];
    Ok(n)
  }
}

/// Reads frames until the stream ends or fails. Returns the frames and the failure, if any.
fn drain(rd: &mut impl Read) -> (Vec<Frame>, Option<Error>) {
  let mut out = Vec::new();
  loop {
    match frame::read(rd) {
      Ok(Some(f)) => out.push(f),
      Ok(None) => return (out, None),
      Err(e) => return (out, Some(e)),
    }
  }
}

fn typed(e: &Error) -> bool {
  match e {
    Error::Protocol(_) => true,
    Error::Io(e) => e.kind() == io::ErrorKind::UnexpectedEof,
    _ => false,
  }
}

fn check(frames: &[Frame], fed: usize) {
  let mut bytes = 0;
  for f in frames {
    assert!(f.meta.len() <= MAX_META_BYTES as usize && f.payload.len() <= MAX_PAYLOAD_BYTES as usize);
    bytes += HEADER_SIZE + f.meta.len() + f.payload.len();
  }
  assert!(bytes <= fed, "decoded more bytes than were fed");
}

fn run(name: &str, it: usize, data: &[u8], chunky: Option<u64>) -> Option<Error> {
  let res = catch_unwind(AssertUnwindSafe(|| match chunky {
    Some(seed) => drain(&mut Chunky { data, rng: Rng(seed | 1) }),
    None => drain(&mut Cursor::new(data)),
  }));
  let (frames, err) = res.unwrap_or_else(|_| panic!("{name} iteration {it}: reader panicked on {data:02x?}"));
  if let Some(e) = &err {
    assert!(typed(e), "{name} iteration {it}: untyped error {e:?}");
  }
  check(&frames, data.len());
  err
}

#[test]
fn random_buffers_fail_only_with_typed_errors() {
  let mut r = Rng(SEED);
  let mut rejected = 0;
  for it in 0..ITERATIONS {
    let mut data = r.some(0, 300);
    if r.next().is_multiple_of(2) {
      // Start like a real frame so the reader reaches the length checks.
      let mut head = wire(&random_frame(&mut r));
      head.truncate(HEADER_SIZE);
      let off = 16 + 4 * r.below(2);
      let len = if r.below(3) == 0 { u32::MAX } else { r.below(1 << 25) as u32 };
      head[off..off + 4].copy_from_slice(&len.to_be_bytes());
      head.extend(data);
      data = head;
    }
    let chunky = (it % 2 == 1).then(|| r.next());
    if run("random", it, &data, chunky).is_some() {
      rejected += 1;
    }
  }
  assert!(rejected > ITERATIONS / 4, "the generator must reach the error paths");
}

#[test]
fn mutated_valid_frames_fail_only_with_typed_errors() {
  let mut r = Rng(SEED + 1);
  for it in 0..ITERATIONS {
    let n = 1 + r.below(3);
    let mut data: Vec<u8> = (0..n).flat_map(|_| wire(&random_frame(&mut r))).collect();
    match r.below(4) {
      0 => {
        for _ in 0..1 + r.below(4) {
          let i = r.below(data.len());
          data[i] ^= 1 << r.below(8);
        }
      }
      1 => data.truncate(r.below(data.len())),
      2 => {
        let extra = r.some(1, 40);
        data.extend(extra);
      }
      _ => {
        let i = r.below(HEADER_SIZE);
        data[i] = r.next() as u8;
      }
    }
    let chunky = (it % 2 == 1).then(|| r.next());
    run("mutated", it, &data, chunky);
  }
}

#[test]
fn write_then_read_returns_the_same_frames() {
  let mut r = Rng(SEED + 2);
  for it in 0..ITERATIONS / 4 {
    let want: Vec<Frame> = (0..1 + r.below(5)).map(|_| random_frame(&mut r)).collect();
    let data: Vec<u8> = want.iter().flat_map(wire).collect();
    let chunky = (it % 2 == 1).then(|| r.next());
    let (got, err) = match chunky {
      Some(s) => drain(&mut Chunky { data: &data, rng: Rng(s | 1) }),
      None => drain(&mut Cursor::new(&data)),
    };
    assert!(err.is_none(), "iteration {it}: {err:?}");
    assert_eq!(got, want, "iteration {it}");
  }
}

#[test]
fn every_prefix_of_a_frame_ends_with_a_typed_result() {
  let f = Frame { flags: 1, method: 3, request_id: 5, meta: b"abc".to_vec(), payload: b"defgh".to_vec() };
  let full = wire(&f);
  for n in 0..full.len() {
    let (frames, err) = drain(&mut Cursor::new(&full[..n]));
    assert!(frames.is_empty());
    match (n, err) {
      (0, None) => {}
      (_, Some(Error::Io(e))) if e.kind() == io::ErrorKind::UnexpectedEof => {}
      (n, other) => panic!("prefix {n}: {other:?}"),
    }
  }
}

#[test]
fn a_claim_at_the_limit_then_eof_allocates_one_step() {
  let mut head = wire(&Frame::default());
  head[20..24].copy_from_slice(&MAX_PAYLOAD_BYTES.to_be_bytes());
  head.extend(vec![0u8; 1000]);
  MAX_ALLOC.with(|m| m.set(0));
  let (_, err) = drain(&mut Cursor::new(&head));
  let peak = MAX_ALLOC.with(|m| m.get());
  assert!(matches!(err, Some(Error::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof), "{err:?}");
  assert!(peak <= 2 * (64 << 10), "largest allocation {peak} bytes for a 1000 byte body");
}

#[test]
fn oversize_sections_are_refused_before_any_byte_is_written() {
  let mut sink = Vec::new();
  let f = Frame { payload: vec![0; MAX_PAYLOAD_BYTES as usize + 1], ..Frame::default() };
  assert!(matches!(frame::write(&mut sink, &f), Err(Error::TooLarge(_))));
  assert!(sink.is_empty());
}
