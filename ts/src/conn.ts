import net from "node:net";
import { DeadlineExceededError, ProtocolError, TransportError } from "./errors.js";
import { encodeFrame, FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE, Frame, FrameReader } from "./frame.js";
import { dec, decWireError, enc } from "./api.js";
import * as pb from "./gen/mountos_fs.js";
import { Method } from "./ops_gen.js";

/** One request from call() until it is answered, abandoned, or failed. */
interface Call {
  method: number;
  meta: Buffer;
  payload: Buffer;
  /** Runs inside the read loop, before any later frame is handled. */
  onReply?: (f: Frame) => void;
  id: number;
  sent: boolean;
  done: boolean;
  timer?: NodeJS.Timeout;
  resolve: (f: Frame) => void;
  reject: (e: Error) => void;
}

const noop = (): void => {};

/** Abandoned ids kept per unit of the in-flight limit. Past that the connection is dropped. */
const ABANDONED_PER_SLOT = 4;

/**
 * One socket. Up to `limit` requests are in flight and replies are matched by request id.
 * The limit is 1 until the owner reads the Hello reply and calls setLimit.
 */
export class Conn {
  private reader = new FrameReader();
  private pending = new Map<number, Call>();
  /** Timed-out requests whose reply is still expected, by id, with the method. */
  private abandoned = new Map<number, number>();
  private queue: Call[] = [];
  private inflightLimit = 1;
  private nextId = 1;
  private failure: Error | undefined;
  private byClient = false;
  /** Calls queued or in flight, plus reservations. The pool uses it to pick the least loaded connection. */
  load = 0;
  /** Runs each time load drops. */
  onFree: () => void = noop;
  /**
   * Gets every notify frame in arrival order, with or without a request in flight.
   * It may throw to report a malformed notice, which fails the connection.
   */
  onNotice: (f: Frame) => void = noop;
  /** Runs once when the connection ends. byClient is true after close(). */
  onClosed: (err: Error, byClient: boolean) => void = noop;
  /** Set by the owner when the server announced the close. */
  closingSeen = false;

  private constructor(private socket: net.Socket, private callTimeoutMs: number) {
    socket.on("data", (c) => this.onData(c));
    socket.on("error", (e) => this.fail(new TransportError(`socket error: ${e.message}`, { cause: e })));
    socket.on("close", () => this.fail(new TransportError("connection closed")));
  }

  /** callTimeoutMs bounds every call on this connection. Zero means no limit. */
  static connect(path: string, callTimeoutMs = 0): Promise<Conn> {
    return new Promise((resolve, reject) => {
      const s = net.connect(path);
      const onErr = (e: Error): void =>
        reject(new TransportError(`connect ${path}: ${e.message}`, { cause: e }));
      s.once("error", onErr);
      s.once("connect", () => {
        s.off("error", onErr);
        resolve(new Conn(s, callTimeoutMs));
      });
    });
  }

  get alive(): boolean {
    return this.failure === undefined;
  }

  /** Most requests in flight at once. */
  get limit(): number {
    return this.inflightLimit;
  }

  /** Sets the in-flight limit, at least 1. Queued calls start when the limit rises. */
  setLimit(n: number): void {
    this.inflightLimit = Math.max(1, Math.floor(n));
    this.pump();
  }

  /** Holds the connection for the caller. Pair with unreserve. */
  reserve(): void {
    this.load++;
  }

  unreserve(): void {
    this.load--;
    this.onFree();
  }

  /**
   * onReply sees the response frame in the read loop, before notices that follow it.
   * timeoutMs overrides the connection limit. A call that times out while queued is dropped and
   * nothing is sent. One that times out after its request was sent fails alone and its id is
   * abandoned, so the late reply is discarded. When the limit is 1 or too many ids are abandoned,
   * the connection ends instead.
   */
  call(
    method: number, meta: Buffer, payload: Buffer = Buffer.alloc(0), onReply?: (f: Frame) => void,
    timeoutMs = this.callTimeoutMs,
  ): Promise<Frame> {
    this.load++;
    return new Promise<Frame>((resolve, reject) => {
      const c: Call = { method, meta, payload, onReply, id: 0, sent: false, done: false, resolve, reject };
      if (this.failure) {
        this.settle(c, this.failure);
        return;
      }
      if (timeoutMs > 0) c.timer = setTimeout(() => this.expire(c, timeoutMs), timeoutMs);
      this.queue.push(c);
      this.pump();
    });
  }

  close(): void {
    this.byClient = true;
    this.fail(new TransportError("connection closed by client"));
  }

  // Finishes a call once and releases its load.
  private settle(c: Call, err?: Error, f?: Frame): void {
    if (c.done) return;
    c.done = true;
    clearTimeout(c.timer);
    if (err) c.reject(err);
    else c.resolve(f!);
    this.unreserve();
  }

  private expire(c: Call, timeoutMs: number): void {
    const e = new DeadlineExceededError(`call method ${c.method} exceeded ${timeoutMs} ms`);
    if (c.done) return;
    if (!c.sent) {
      this.queue.splice(this.queue.indexOf(c), 1);
      this.settle(c, e);
      return;
    }
    if (this.inflightLimit > 1) {
      if (this.abandoned.size < this.inflightLimit * ABANDONED_PER_SLOT) {
        this.pending.delete(c.id);
        this.abandoned.set(c.id, c.method);
        this.settle(c, e);
        this.pump();
        return;
      }
      this.settle(c, e);
      this.fail(new TransportError("too many abandoned requests, dropping the connection", { cause: e }));
      return;
    }
    this.settle(c, e);
    this.fail(e);
  }

  // Sends queued calls in order while the in-flight limit has room. Writes happen only here, so
  // frames never interleave.
  private pump(): void {
    while (!this.failure && this.queue.length > 0 && this.pending.size < this.inflightLimit) {
      const c = this.queue.shift()!;
      let parts: Buffer[];
      try {
        parts = encodeFrame({ flags: 0, method: c.method, requestId: this.nextId, meta: c.meta, payload: c.payload });
      } catch (e) {
        this.settle(c, e as Error);
        continue;
      }
      c.id = this.nextId++;
      c.sent = true;
      this.pending.set(c.id, c);
      this.socket.cork();
      for (const p of parts) if (p.length > 0) this.socket.write(p);
      this.socket.uncork();
    }
  }

  // A reply that arrives after its call timed out must not leak the handle or watch it created.
  private releaseLate(method: number, f: Frame): void {
    let m: number;
    let req: Buffer;
    try {
      switch (method) {
        case Method.Open:
          m = Method.Close;
          req = enc(pb.CloseRequest, { handle: dec(pb.OpenResponse, f.meta).handle });
          break;
        case Method.Opendir:
          m = Method.Closedir;
          req = enc(pb.ClosedirRequest, { handle: dec(pb.OpendirResponse, f.meta).handle });
          break;
        case Method.Watch:
          m = Method.Unwatch;
          req = enc(pb.UnwatchRequest, { watchId: dec(pb.WatchResponse, f.meta).watchId });
          break;
        default:
          return;
      }
    } catch {
      return;
    }
    this.call(m, req).then(noop, noop);
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
      const lateMethod = this.abandoned.get(f.requestId);
      if (lateMethod !== undefined && f.method === lateMethod && f.flags & FLAG_RESPONSE) {
        this.abandoned.delete(f.requestId);
        if (!(f.flags & FLAG_ERROR)) this.releaseLate(lateMethod, f);
        continue;
      }
      const p = this.pending.get(f.requestId);
      if (!p || f.method !== p.method || !(f.flags & FLAG_RESPONSE)) {
        this.fail(new ProtocolError(`unexpected frame method=${f.method} id=${f.requestId}`));
        return;
      }
      this.pending.delete(p.id);
      if (f.flags & FLAG_ERROR) {
        try {
          this.settle(p, decWireError(f.meta));
        } catch (e) {
          this.settle(p, e as Error);
        }
      } else {
        try {
          p.onReply?.(f);
          this.settle(p, undefined, f);
        } catch (e) {
          this.settle(p, e as Error);
        }
      }
      this.pump();
    }
  }

  private fail(err: Error): void {
    if (this.failure) return;
    this.failure = err;
    this.socket.destroy();
    const calls = [...this.pending.values(), ...this.queue];
    this.pending.clear();
    this.abandoned.clear();
    this.queue = [];
    for (const c of calls) this.settle(c, err);
    this.onClosed(err, this.byClient);
  }
}
