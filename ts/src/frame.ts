import { ProtocolError } from "./errors.js";

export const HEADER_SIZE = 24;
export const VERSION = 1;
export const MAX_META_BYTES = 16 * 1024 * 1024;
export const MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;

export const FLAG_RESPONSE = 1;
export const FLAG_ERROR = 2;
export const FLAG_STREAM_END = 4;
/** Server push. The frame answers no request. */
export const FLAG_NOTIFY = 8;

const MAGIC = Buffer.from("MFS1", "ascii");

/**
 * One wire message. requestID is a JS number. The client counts up from 1, so
 * it stays below 2^53 in practice. Larger peer values are a protocol error.
 */
export interface Frame {
  flags: number;
  method: number;
  requestId: number;
  meta: Buffer;
  payload: Buffer;
}

export function encodeFrame(f: Frame): Buffer[] {
  if (f.meta.length > MAX_META_BYTES) {
    throw new ProtocolError(`frame meta ${f.meta.length} exceeds ${MAX_META_BYTES}`);
  }
  if (f.payload.length > MAX_PAYLOAD_BYTES) {
    throw new ProtocolError(`frame payload ${f.payload.length} exceeds ${MAX_PAYLOAD_BYTES}`);
  }
  const h = Buffer.alloc(HEADER_SIZE);
  MAGIC.copy(h, 0);
  h[4] = VERSION;
  h[5] = f.flags;
  h.writeUInt16BE(f.method, 6);
  h.writeBigUInt64BE(BigInt(f.requestId), 8);
  h.writeUInt32BE(f.meta.length, 16);
  h.writeUInt32BE(f.payload.length, 20);
  return [h, f.meta, f.payload];
}

/** Incremental decoder. Feed socket chunks to push(). */
export class FrameReader {
  private chunks: Buffer[] = [];
  private size = 0;

  push(chunk: Buffer): Frame[] {
    this.chunks.push(chunk);
    this.size += chunk.length;
    const out: Frame[] = [];
    while (this.size >= HEADER_SIZE) {
      const head = this.peek(HEADER_SIZE);
      if (!head.subarray(0, 4).equals(MAGIC)) throw new ProtocolError("bad frame magic");
      if (head[4] !== VERSION) throw new ProtocolError(`unsupported frame version ${head[4]}`);
      const metaLen = head.readUInt32BE(16);
      const payloadLen = head.readUInt32BE(20);
      if (metaLen > MAX_META_BYTES) throw new ProtocolError(`frame meta ${metaLen} exceeds limit`);
      if (payloadLen > MAX_PAYLOAD_BYTES) {
        throw new ProtocolError(`frame payload ${payloadLen} exceeds limit`);
      }
      const total = HEADER_SIZE + metaLen + payloadLen;
      if (this.size < total) break;
      const buf = this.take(total, true);
      const id = buf.readBigUInt64BE(8);
      if (id > BigInt(Number.MAX_SAFE_INTEGER)) throw new ProtocolError("request id too large");
      out.push({
        flags: buf[5],
        method: buf.readUInt16BE(6),
        requestId: Number(id),
        meta: buf.subarray(HEADER_SIZE, HEADER_SIZE + metaLen),
        payload: buf.subarray(HEADER_SIZE + metaLen, total),
      });
    }
    return out;
  }

  // Returns the first n bytes without removing them. It joins chunks only as far as needed, so a
  // large frame that arrives in many chunks is copied once, when complete.
  private peek(n: number): Buffer {
    if (this.chunks[0].length >= n) return this.chunks[0].subarray(0, n);
    return this.take(n, false);
  }

  // Returns the first n bytes. consume=true removes them from the queue.
  private take(n: number, consume: boolean): Buffer {
    const buf = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks);
    this.chunks = [buf];
    if (consume) {
      this.size -= n;
      this.chunks = this.size > 0 ? [buf.subarray(n)] : [];
    }
    return buf.subarray(0, n);
  }
}
