import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { ProtocolError } from "../src/index.js";
import {
  encodeFrame, FLAG_RESPONSE, type Frame, FrameReader, HEADER_SIZE, MAX_META_BYTES, MAX_PAYLOAD_BYTES,
} from "../src/frame.js";

const SEED = 0x6d667331;
const ITERATIONS = 20000;

/** Small deterministic generator, so a failure replays from the seed and iteration number. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const below = (r: () => number, n: number): number => Math.floor(r() * n);

function randomBytes(r: () => number, n: number): Buffer {
  const b = Buffer.alloc(n);
  for (let i = 0; i < n; i++) b[i] = below(r, 256);
  return b;
}

function randomFrame(r: () => number): Frame {
  return {
    flags: below(r, 16),
    method: below(r, 65536),
    requestId: below(r, 2 ** 31),
    meta: randomBytes(r, below(r, 64)),
    payload: randomBytes(r, below(r, 256)),
  };
}

const wire = (f: Frame): Buffer => Buffer.concat(encodeFrame(f));

/** Feeds data in random chunk sizes. Returns the decoded frames, or the error the reader threw. */
function feed(rd: FrameReader, data: Buffer, r: () => number): { frames: Frame[]; err?: unknown } {
  const frames: Frame[] = [];
  try {
    for (let i = 0; i < data.length; ) {
      const n = 1 + below(r, Math.max(1, data.length - i));
      frames.push(...rd.push(data.subarray(i, i + n)));
      i += n;
    }
  } catch (err) {
    return { frames, err };
  }
  return { frames };
}

function checkFrames(frames: Frame[], fed: number): void {
  let bytes = 0;
  for (const f of frames) {
    assert.ok(f.meta.length <= MAX_META_BYTES && f.payload.length <= MAX_PAYLOAD_BYTES);
    assert.ok(Number.isSafeInteger(f.requestId) && f.requestId >= 0);
    assert.ok(f.method >= 0 && f.method <= 0xffff && f.flags >= 0 && f.flags <= 0xff);
    bytes += HEADER_SIZE + f.meta.length + f.payload.length;
  }
  assert.ok(bytes <= fed, "decoded more bytes than were fed");
}

describe("frame reader fuzz", () => {
  it("random buffers fail only with ProtocolError", () => {
    const r = rng(SEED);
    let rejected = 0;
    for (let it = 0; it < ITERATIONS; it++) {
      let data = randomBytes(r, below(r, 300));
      // Half of the inputs start like a real frame, so the decoder reaches the length checks.
      if (r() < 0.5) {
        const head = wire({ flags: below(r, 16), method: below(r, 65536), requestId: below(r, 2 ** 31), meta: Buffer.alloc(0), payload: Buffer.alloc(0) });
        head.writeUInt32BE(r() < 0.3 ? 0xffffffff : below(r, 2 ** 25), 16 + 4 * below(r, 2));
        data = Buffer.concat([head, data]);
      }
      const rd = new FrameReader();
      const { frames, err } = feed(rd, data, r);
      if (err !== undefined) {
        assert.ok(err instanceof ProtocolError, `iteration ${it}: ${(err as Error).message}`);
        rejected++;
      }
      checkFrames(frames, data.length);
    }
    assert.ok(rejected > ITERATIONS / 4, "the generator must reach the error paths");
  });

  it("mutated valid frames fail only with ProtocolError", () => {
    const r = rng(SEED + 1);
    for (let it = 0; it < ITERATIONS; it++) {
      const n = 1 + below(r, 3);
      let data = Buffer.concat(Array.from({ length: n }, () => wire(randomFrame(r))));
      const mode = below(r, 4);
      if (mode === 0) {
        for (let k = 1 + below(r, 4); k > 0; k--) data[below(r, data.length)] ^= 1 << below(r, 8);
      } else if (mode === 1) {
        data = data.subarray(0, below(r, data.length));
      } else if (mode === 2) {
        data = Buffer.concat([data, randomBytes(r, 1 + below(r, 40))]);
      } else {
        data[below(r, HEADER_SIZE)] = below(r, 256);
      }
      const { frames, err } = feed(new FrameReader(), data, r);
      if (err !== undefined) assert.ok(err instanceof ProtocolError, `iteration ${it}: ${(err as Error).message}`);
      checkFrames(frames, data.length);
    }
  });

  it("valid frame streams decode to the same frames under any chunking", () => {
    const r = rng(SEED + 2);
    for (let it = 0; it < ITERATIONS / 4; it++) {
      const want = Array.from({ length: 1 + below(r, 5) }, () => randomFrame(r));
      const data = Buffer.concat(want.map(wire));
      const { frames, err } = feed(new FrameReader(), data, r);
      assert.equal(err, undefined);
      assert.equal(frames.length, want.length);
      want.forEach((w, i) => {
        assert.equal(frames[i].flags, w.flags);
        assert.equal(frames[i].method, w.method);
        assert.equal(frames[i].requestId, w.requestId);
        assert.ok(frames[i].meta.equals(w.meta));
        assert.ok(frames[i].payload.equals(w.payload));
      });
    }
  });

  it("a claim at the limit followed by silence holds only the bytes that arrived", () => {
    const head = wire({ flags: FLAG_RESPONSE, method: 1, requestId: 1, meta: Buffer.alloc(0), payload: Buffer.alloc(0) });
    head.writeUInt32BE(MAX_PAYLOAD_BYTES, 20);
    const rd = new FrameReader();
    assert.deepEqual(rd.push(head), []);
    assert.deepEqual(rd.push(Buffer.alloc(1000)), []);
    const state = rd as unknown as { size: number };
    assert.equal(state.size, HEADER_SIZE + 1000);
  });

  it("rejects limits and ids past the safe range", () => {
    const over = wire({ flags: 1, method: 1, requestId: 1, meta: Buffer.alloc(0), payload: Buffer.alloc(0) });
    over.writeUInt32BE(MAX_META_BYTES + 1, 16);
    assert.throws(() => new FrameReader().push(over), ProtocolError);
    const id = wire({ flags: 1, method: 1, requestId: 1, meta: Buffer.alloc(0), payload: Buffer.alloc(0) });
    id.writeBigUInt64BE(1n << 53n, 8);
    assert.throws(() => new FrameReader().push(id), ProtocolError);
    assert.throws(
      () => encodeFrame({ flags: 0, method: 1, requestId: 1, meta: Buffer.alloc(0), payload: Buffer.alloc(MAX_PAYLOAD_BYTES + 1) }),
      ProtocolError,
    );
  });
});
