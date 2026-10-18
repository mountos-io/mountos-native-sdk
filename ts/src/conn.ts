import net from "node:net";
import { ProtocolError, TransportError } from "./errors.js";
import { encodeFrame, FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE, Frame, FrameReader } from "./frame.js";
import { decWireError } from "./api.js";

interface Pending {
  id: number;
  method: number;
  resolve: (f: Frame) => void;
  reject: (e: Error) => void;
  /** Runs inside the read loop, before any later frame is handled. */
  onReply?: (f: Frame) => void;
}

const noop = (): void => {};

/** One socket. Requests run one at a time in FIFO order (server max_inflight is 1). */
export class Conn {
  private reader = new FrameReader();
  private pending: Pending | undefined;
  private tail: Promise<unknown> = Promise.resolve();
  private nextId = 1;
  private failure: Error | undefined;
  private byClient = false;
  /** Requests queued or running. The pool uses it to pick the least loaded connection. */
  load = 0;
  /**
   * Gets every notify frame in arrival order, with or without a request in flight.
   * It may throw to report a malformed notice, which fails the connection.
   */
  onNotice: (f: Frame) => void = noop;
  /** Runs once when the connection ends. byClient is true after close(). */
  onClosed: (err: Error, byClient: boolean) => void = noop;
  /** Set by the owner when the server announced the close. */
  closingSeen = false;

  private constructor(private socket: net.Socket) {
    socket.on("data", (c) => this.onData(c));
    socket.on("error", (e) => this.fail(new TransportError(`socket error: ${e.message}`, { cause: e })));
    socket.on("close", () => this.fail(new TransportError("connection closed")));
  }

  static connect(path: string): Promise<Conn> {
    return new Promise((resolve, reject) => {
      const s = net.connect(path);
      const onErr = (e: Error): void =>
        reject(new TransportError(`connect ${path}: ${e.message}`, { cause: e }));
      s.once("error", onErr);
      s.once("connect", () => {
        s.off("error", onErr);
        resolve(new Conn(s));
      });
    });
  }

  get alive(): boolean {
    return this.failure === undefined;
  }

  /** onReply sees the response frame in the read loop, before notices that follow it. */
  call(method: number, meta: Buffer, payload: Buffer = Buffer.alloc(0), onReply?: (f: Frame) => void): Promise<Frame> {
    this.load++;
    const run = this.tail.then(() => this.exchange(method, meta, payload, onReply));
    this.tail = run.then(noop, noop);
    return run.finally(() => this.load--);
  }

  close(): void {
    this.byClient = true;
    this.fail(new TransportError("connection closed by client"));
  }

  private exchange(method: number, meta: Buffer, payload: Buffer, onReply?: (f: Frame) => void): Promise<Frame> {
    if (this.failure) return Promise.reject(this.failure);
    const id = this.nextId++;
    let parts: Buffer[];
    try {
      parts = encodeFrame({ flags: 0, method, requestId: id, meta, payload });
    } catch (e) {
      return Promise.reject(e);
    }
    return new Promise<Frame>((resolve, reject) => {
      this.pending = { id, method, resolve, reject, onReply };
      this.socket.cork();
      for (const p of parts) if (p.length > 0) this.socket.write(p);
      this.socket.uncork();
    });
  }

  private onData(chunk: Buffer): void {
    let frames: Frame[];
    try {
      frames = this.reader.push(chunk);
    } catch (e) {
      this.fail(e as Error);
      return;
    }
    for (const f of frames) {
      if (this.failure) return;
      if (f.flags & FLAG_NOTIFY) {
        try {
          this.onNotice(f);
        } catch (e) {
          this.fail(e as Error);
          return;
        }
        continue;
      }
      const p = this.pending;
      if (!p || f.requestId !== p.id || f.method !== p.method || !(f.flags & FLAG_RESPONSE)) {
        this.fail(new ProtocolError(`unexpected frame method=${f.method} id=${f.requestId}`));
        return;
      }
      this.pending = undefined;
      if (f.flags & FLAG_ERROR) {
        try {
          p.reject(decWireError(f.meta));
        } catch (e) {
          p.reject(e as Error);
        }
        continue;
      }
      try {
        p.onReply?.(f);
      } catch (e) {
        p.reject(e as Error);
        continue;
      }
      p.resolve(f);
    }
  }

  private fail(err: Error): void {
    if (this.failure) return;
    this.failure = err;
    this.socket.destroy();
    const p = this.pending;
    this.pending = undefined;
    p?.reject(err);
    this.onClosed(err, this.byClient);
  }
}
