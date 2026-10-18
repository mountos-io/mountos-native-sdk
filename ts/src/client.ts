import { itemErrors, MAX_BATCH_ITEMS, planPieces, ReadRange, ReadResult, StatResult, WriteItem, WriteResult } from "./bulk.js";
import { Conn } from "./conn.js";
import { DeadlineExceededError, MountosError, ProtocolError, TooManyEntriesError, TransportError } from "./errors.js";
import { Frame, MAX_PAYLOAD_BYTES } from "./frame.js";
import * as pb from "./gen/mountos_fs.js";
import {
  dec, DirEntry, enc, FileInfo, FileType, HelloResponse, OpenFlag,
  decHello, toDirEntry, toFileInfo,
} from "./api.js";
import { ClientOps, FileOps, Method } from "./ops_gen.js";
import { requireCapability } from "./capability.js";
import { ClientEvent, decodeNotice, describeEvent, Logger } from "./events.js";
import { Watch, WatchOptions } from "./watch.js";

export interface ClientOptions {
  /** Number of connections. Default 4. */
  poolSize?: number;
  clientName?: string;
  /**
   * Receives diagnostics: events that arrive while no consumer is registered and exceptions
   * thrown by listeners. Default is the console, which writes to stderr.
   */
  logger?: Logger;
  /**
   * Limit in ms for every request and for the wait for a free connection. Default 0 means none.
   * The call fails with DeadlineExceededError. On a connection that pipelines, only that call
   * fails and a late reply is discarded. Otherwise the connection ends and is dialed again on a
   * later call.
   */
  callTimeout?: number;
  /**
   * Most requests in flight on one connection. Default 32. The server's max_inflight caps it.
   * A server without the pipelining capability gets one request at a time.
   */
  maxInflight?: number;
}

/** Most entries Client.readDirAll holds unless the call passes `maxEntries`. */
export const DEFAULT_MAX_DIR_ENTRIES = 1_000_000;

/** Most directory levels Client.walk descends below its root unless the call passes `maxDepth`. */
export const DEFAULT_MAX_WALK_DEPTH = 1024;

/** Options for Client.readDirAll. */
export interface ReadDirAllOptions {
  maxEntries?: number;
}

/** Options for Client.walk. */
export interface WalkOptions {
  maxDepth?: number;
}

/** One entry found by Client.walk. path is the full path of the entry. */
export interface WalkEntry {
  path: string;
  entry: DirEntry;
}

export interface WaitWarmOptions {
  signal?: AbortSignal;
}

const noop = (): void => {};

/** Default for ClientOptions.maxInflight. */
export const DEFAULT_MAX_INFLIGHT = 32;
const MAX_INFLIGHT_CEILING = 1024;

/** Finished warms kept so a waitWarm that starts late still sees the result. */
const WARM_CACHE_LIMIT = 1024;

/** One pool position. A dead connection is replaced by a fresh dial. */
interface Slot {
  conn: Conn;
  /** Set while a replacement dial runs, so concurrent callers share it. */
  dialing?: Promise<Conn>;
}

/** A call that waits for a free connection. */
interface ConnWaiter {
  resolve: (c: Conn) => void;
  reject: (e: Error) => void;
  timer?: NodeJS.Timeout;
}

interface WarmWaiter {
  resolve: () => void;
  reject: (e: Error) => void;
}

/**
 * Connection pool for the native filesystem API. A connection that fails is replaced by a new
 * dial on a later call. Handles, watches and warm waits tied to the old connection do not carry over.
 */
export class Client extends ClientOps {
  private cursor = 0;
  private closed = false;
  private listeners = new Set<(e: ClientEvent) => void>();
  private watches = new Set<Watch>();
  private warmWaiters = new Map<bigint, Set<WarmWaiter>>();
  /** Finished warms by id, oldest first. The value is the failure, or undefined for success. */
  private warmDone = new Map<bigint, Error | undefined>();

  private slots: Slot[];
  private connWaiters: ConnWaiter[] = [];

  private constructor(
    conns: Conn[],
    private logger: Logger,
    private path: string,
    private clientName: string,
    private hello: HelloResponse,
    private callTimeout: number,
    private maxInflight: number,
  ) {
    super();
    this.slots = conns.map((conn) => ({ conn }));
    for (const c of conns) this.adopt(c);
  }

  private adopt(c: Conn): void {
    c.onNotice = (f) => this.onNotice(c, f);
    c.onClosed = (err, byClient) => this.onConnClosed(c, err, byClient);
    c.onFree = () => this.drain();
  }

  static async connect(socketPath: string, opts: ClientOptions = {}): Promise<Client> {
    const size = opts.poolSize ?? 4;
    if (!Number.isInteger(size) || size < 1) throw new RangeError("poolSize must be >= 1");
    const name = opts.clientName ?? "mountos-ts";
    const timeout = opts.callTimeout ?? 0;
    if (!Number.isFinite(timeout) || timeout < 0) throw new RangeError("callTimeout must be >= 0");
    const inflight = Math.min(opts.maxInflight ?? DEFAULT_MAX_INFLIGHT, MAX_INFLIGHT_CEILING);
    if (!Number.isInteger(inflight) || inflight < 1) throw new RangeError("maxInflight must be >= 1");
    const results = await Promise.allSettled(
      Array.from({ length: size }, async () => {
        const c = await Conn.connect(socketPath, timeout);
        try {
          const hello = await handshake(c, name, inflight);
          return { c, hello };
        } catch (e) {
          c.close();
          throw e;
        }
      }),
    );
    const ok = results.filter((r) => r.status === "fulfilled");
    const bad = results.find((r) => r.status === "rejected");
    if (bad) {
      for (const r of ok) r.value.c.close();
      throw (bad as PromiseRejectedResult).reason;
    }
    const hello = ok[0].value.hello;
    return new Client(ok.map((r) => r.value.c), opts.logger ?? console, socketPath, name, hello, timeout, inflight);
  }

  /** Result of the latest Hello handshake. */
  get server(): HelloResponse {
    return this.hello;
  }

  /** Largest payload per request, from the server's Hello reply. */
  get maxPayload(): number {
    return Math.min(this.server.maxPayloadBytes, MAX_PAYLOAD_BYTES);
  }

  /** Ends all watches, fails pending waitWarm calls, closes the connections, and drops all listeners. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const w of [...this.watches]) w.end();
    for (const sl of this.slots) sl.conn.close();
    const err = new TransportError("client closed");
    for (const set of this.warmWaiters.values()) for (const w of set) w.reject(err);
    this.warmWaiters.clear();
    this.warmDone.clear();
    this.listeners.clear();
    for (const w of this.connWaiters.splice(0)) {
      clearTimeout(w.timer);
      w.reject(err);
    }
  }

  /**
   * Registers a listener for every server notice and connection event, in arrival order.
   * Returns a function that removes it. A listener that throws is logged and does not stop the others.
   */
  addListener(fn: (e: ClientEvent) => void): () => void {
    this.listeners.add(fn);
    return () => {
      this.listeners.delete(fn);
    };
  }

  // Dials and handshakes a replacement for a dead slot. Callers share one dial per slot.
  // A dial that ends after close() is discarded.
  private redial(sl: Slot): Promise<Conn> {
    if (sl.dialing) return sl.dialing;
    const p = (async (): Promise<Conn> => {
      const c = await Conn.connect(this.path, this.callTimeout);
      let hello: HelloResponse;
      try {
        hello = await handshake(c, this.clientName, this.maxInflight);
      } catch (e) {
        c.close();
        throw e;
      }
      if (this.closed) {
        c.close();
        throw new TransportError("client closed");
      }
      this.adopt(c);
      sl.conn = c;
      this.hello = hello;
      return c;
    })();
    sl.dialing = p;
    const done = (): void => {
      if (sl.dialing === p) sl.dialing = undefined;
    };
    p.then(() => {
      done();
      this.drain();
    }, done);
    return p;
  }

  // The live connection with the fewest calls that still has room under its in-flight limit.
  // Ties go to the connection nearest the rotating cursor.
  private pickConn(): Conn | undefined {
    const n = this.slots.length;
    let best: Conn | undefined;
    for (let i = 0; i < n; i++) {
      const c = this.slots[(this.cursor + i) % n].conn;
      if (c.alive && c.load < c.limit && (!best || c.load < best.load)) best = c;
    }
    return best;
  }

  // Hands a connection to the caller with a reservation that the caller releases with unreserve.
  // The least loaded connection with room is used first. When every live connection is full the
  // call waits for the first one that frees up.
  // Dead slots are redialed on the call that finds them. When no connection is live the call
  // waits for the first redial to finish, and a failed dial surfaces as a transport error.
  private async acquire(): Promise<Conn> {
    if (this.closed) throw new TransportError("client closed");
    const n = this.slots.length;
    let live = 0;
    const dead: Slot[] = [];
    for (let i = 0; i < n; i++) {
      const sl = this.slots[(this.cursor + i) % n];
      if (sl.conn.alive) live++;
      else dead.push(sl);
    }
    const free = this.pickConn();
    this.cursor = (this.cursor + 1) % n;
    const dials = dead.map((sl) => this.redial(sl));
    if (live > 0) {
      for (const d of dials) d.catch(noop);
      if (free) {
        free.reserve();
        return free;
      }
      return this.waitFree();
    }
    try {
      const c = await Promise.any(dials);
      c.reserve();
      return c;
    } catch (e) {
      if (e instanceof AggregateError) throw e.errors[0];
      throw e;
    }
  }

  private waitFree(): Promise<Conn> {
    return new Promise<Conn>((resolve, reject) => {
      const w: ConnWaiter = { resolve, reject };
      if (this.callTimeout > 0) {
        w.timer = setTimeout(() => {
          this.connWaiters.splice(this.connWaiters.indexOf(w), 1);
          reject(new DeadlineExceededError(`no free connection within ${this.callTimeout} ms`));
        }, this.callTimeout);
      }
      this.connWaiters.push(w);
    });
  }

  // Serves waiting calls in order with connections that have room.
  private drain(): void {
    while (this.connWaiters.length > 0) {
      const c = this.pickConn();
      if (!c) return;
      const w = this.connWaiters.shift()!;
      clearTimeout(w.timer);
      c.reserve();
      w.resolve(c);
    }
  }

  // When the last live connection ends, waiting calls get a redialed one or the dial error.
  private reviveWaiters(): void {
    if (this.connWaiters.length === 0 || this.slots.some((sl) => sl.conn.alive)) return;
    Promise.any(this.slots.map((sl) => this.redial(sl))).catch((e) => {
      const err = e instanceof AggregateError ? e.errors[0] : e;
      for (const w of this.connWaiters.splice(0)) {
        clearTimeout(w.timer);
        w.reject(err);
      }
    });
  }

  // fn must start its call before its first await. The reservation then turns into that call's load.
  private async lease<T>(fn: (c: Conn) => Promise<T>): Promise<T> {
    const c = await this.acquire();
    let p: Promise<T>;
    try {
      p = fn(c);
    } finally {
      c.unreserve();
    }
    return p;
  }

  // Fails a call locally when the server reported capabilities and this method is not among them.
  private need(method: number): void {
    requireCapability(this.hello.capabilities, method);
  }

  protected async call(method: number, meta: Buffer): Promise<Buffer> {
    this.need(method);
    return this.lease(async (c) => (await c.call(method, meta)).meta);
  }

  private onNotice(conn: Conn, f: Frame): void {
    const e = decodeNotice(f.meta);
    if (!e) {
      this.logger.warn(`mountos: ignoring notice of unknown kind, request id ${f.requestId}`);
      return;
    }
    if (e.kind === "closing") conn.closingSeen = true;
    this.dispatch(conn, e);
  }

  private onConnClosed(conn: Conn, err: Error, byClient: boolean): void {
    if (this.closed || byClient) return;
    this.reviveWaiters();
    if (!conn.closingSeen) {
      const e: ClientEvent = {
        kind: "connectionLost", sequence: 0n, refId: 0n, path: "", oldPath: "", error: err, time: new Date(),
      };
      this.dispatch(conn, e);
    }
    for (const w of [...this.watches]) if (w.ownedBy(conn)) w.end();
  }

  // Delivers one event to warm waiters, watches, and listeners. An event that nobody takes is logged.
  private dispatch(conn: Conn, e: ClientEvent): void {
    let taken = this.listeners.size > 0;
    if (e.kind === "warmDone" || e.kind === "warmFailed") taken = this.finishWarm(e) || taken;
    if (e.kind === "change" || e.kind === "overflow") {
      for (const w of this.watches) {
        if (w.accepts(conn, e)) {
          w.push(e);
          taken = true;
        }
      }
    }
    for (const fn of [...this.listeners]) {
      try {
        fn(e);
      } catch (err) {
        this.logger.warn(`mountos: event listener threw on ${describeEvent(e)}: ${err instanceof Error ? err.stack ?? err.message : String(err)}`);
      }
    }
    if (!taken) this.logger.warn(`mountos: event with no listener ${describeEvent(e)}`);
  }

  // Records a finished warm and wakes its waiters. Returns true when a waiter took it.
  private finishWarm(e: ClientEvent): boolean {
    const result = e.kind === "warmFailed" ? e.error : undefined;
    if (!this.warmDone.has(e.refId) && this.warmDone.size >= WARM_CACHE_LIMIT) {
      this.warmDone.delete(this.warmDone.keys().next().value as bigint);
    }
    this.warmDone.set(e.refId, result);
    const set = this.warmWaiters.get(e.refId);
    if (!set) return false;
    this.warmWaiters.delete(e.refId);
    for (const w of set) {
      if (result) w.reject(result);
      else w.resolve();
    }
    return true;
  }

  /**
   * Stats many paths. The result has one entry per path in request order. A failed path has
   * error set. Inputs over 4096 paths run as several requests.
   */
  async statBatch(paths: string[]): Promise<StatResult[]> {
    const jobs: Promise<StatResult[]>[] = [];
    for (let i = 0; i < paths.length; i += MAX_BATCH_ITEMS) {
      const chunk = paths.slice(i, i + MAX_BATCH_ITEMS);
      jobs.push(this.call(Method.StatBatch, enc(pb.StatBatchRequest, { paths: chunk })).then((meta) => {
        const r = dec(pb.StatBatchResponse, meta);
        if (r.infos.length !== chunk.length) {
          throw new ProtocolError(`StatBatch returned ${r.infos.length} infos for ${chunk.length} paths`);
        }
        const errs = itemErrors(r.errors, chunk.length);
        return chunk.map((_, j): StatResult => {
          const error = errs.get(j);
          return error ? { error } : { info: toFileInfo(r.infos[j]) };
        });
      }));
    }
    return (await Promise.all(jobs)).flat();
  }

  /**
   * Reads a whole file. One request serves files smaller than the payload limit. A file that
   * fills the limit may be longer, so the rest is read from an opened file in payload-sized reads.
   */
  async readFile(path: string): Promise<Buffer> {
    this.need(Method.ReadFile);
    const r = await this.lease((c) => c.call(Method.ReadFile, enc(pb.ReadFileRequest, { path, maxBytes: 0n })));
    const count = dec(pb.ReadFileResponse, r.meta).count;
    if (count !== r.payload.length) {
      throw new ProtocolError(`ReadFile count ${count} does not match payload ${r.payload.length}`);
    }
    if (r.payload.length < this.maxPayload) return r.payload;
    const parts = [r.payload];
    let off = r.payload.length;
    const f = await this.open(path);
    try {
      for (;;) {
        const b = await f.read(off, this.maxPayload);
        if (b.length === 0) break;
        parts.push(b);
        off += b.length;
      }
    } finally {
      await f.close().catch(noop);
    }
    return Buffer.concat(parts);
  }

  /**
   * Writes a whole file and creates it if missing. By default the file is truncated first.
   * Data over the payload limit goes through an opened file in payload-sized writes.
   */
  async writeFile(path: string, data: Uint8Array, opts: { truncate?: boolean } = {}): Promise<void> {
    const truncate = opts.truncate ?? true;
    const buf = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
    let written: number;
    if (buf.length <= this.maxPayload) {
      this.need(Method.WriteFile);
      const r = await this.lease((c) => c.call(Method.WriteFile, enc(pb.WriteFileRequest, { path, truncate }), buf));
      written = dec(pb.WriteFileResponse, r.meta).count;
    } else {
      const f = await this.open(path, OpenFlag.WriteOnly | OpenFlag.Create | (truncate ? OpenFlag.Trunc : 0));
      try {
        written = await f.write(0, buf);
      } finally {
        await f.close().catch(noop);
      }
    }
    if (written !== buf.length) throw new MountosError("EIO", `short write ${written} of ${buf.length} bytes`);
  }

  /**
   * Waits for a warm request to finish. Resolves on warmDone and rejects with the MountosError
   * on warmFailed. A result that arrived before the call is found in a cache of the last 1024
   * warms. Rejects with the signal's reason on abort. It does not poll the server.
   */
  waitWarm(id: number | bigint, opts: WaitWarmOptions = {}): Promise<void> {
    const wid = BigInt(id);
    const { signal } = opts;
    if (this.closed) return Promise.reject(new TransportError("client closed"));
    if (signal?.aborted) return Promise.reject(signal.reason);
    if (this.warmDone.has(wid)) {
      const err = this.warmDone.get(wid);
      return err ? Promise.reject(err) : Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      let set = this.warmWaiters.get(wid);
      if (!set) this.warmWaiters.set(wid, (set = new Set()));
      const onAbort = (): void => {
        set.delete(waiter);
        if (set.size === 0 && this.warmWaiters.get(wid) === set) this.warmWaiters.delete(wid);
        reject(signal!.reason);
      };
      const waiter: WarmWaiter = {
        resolve: () => {
          signal?.removeEventListener("abort", onAbort);
          resolve();
        },
        reject: (e) => {
          signal?.removeEventListener("abort", onAbort);
          reject(e);
        },
      };
      set.add(waiter);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  /**
   * Watches a path. The watch lives on one pooled connection until close() or until that
   * connection ends. Change events reach every listener and the returned Watch.
   */
  async watch(path: string, opts: WatchOptions = {}): Promise<Watch> {
    this.need(Method.Watch);
    const req = enc(pb.WatchRequest, { path, recursive: opts.recursive ?? false, sinceSequence: opts.since ?? 0n });
    let w: Watch | undefined;
    // Registered while the response is read, so a change that follows it in the same chunk is kept.
    await this.lease((conn) => conn.call(Method.Watch, req, undefined, (f) => {
      const id = dec(pb.WatchResponse, f.meta).watchId;
      w = new Watch(id, conn, (x) => this.watches.delete(x));
      this.watches.add(w);
    }));
    if (this.closed) {
      w!.end();
      throw new TransportError("client closed");
    }
    return w!;
  }

  /** Sets times in ns. An omitted time is left unchanged. */
  async utimens(path: string, atimeNs?: bigint, mtimeNs?: bigint): Promise<void> {
    await this.call(Method.Utimens, enc(pb.UtimensRequest, {
      path,
      atimeNs: atimeNs ?? 0n,
      mtimeNs: mtimeNs ?? 0n,
      setAtime: atimeNs !== undefined,
      setMtime: mtimeNs !== undefined,
    }));
  }

  /** Lists one page with metadata. Pass the returned nextCursor for the next page. Empty means done. */
  async ls(path: string, maxEntries = 0, cursor = ""): Promise<{ entries: DirEntry[]; nextCursor: string }> {
    const r = dec(pb.LsResponse, await this.call(Method.Ls, enc(pb.LsRequest, { path, maxEntries, cursor })));
    return { entries: r.entries.map(toDirEntry), nextCursor: r.nextCursor };
  }

  /** Opens a directory. The handle stays bound to the connection that opened it. */
  async opendir(path: string): Promise<Dir> {
    return this.lease(async (conn) => {
      const resp = await conn.call(Method.Opendir, enc(pb.OpendirRequest, { path }));
      return new Dir(conn, dec(pb.OpendirResponse, resp.meta).handle);
    });
  }

  /**
   * Lists a whole directory with attributes, reading every page. Throws TooManyEntriesError when
   * the directory has more than `maxEntries` entries (default DEFAULT_MAX_DIR_ENTRIES).
   */
  async readDirAll(path: string, opts: ReadDirAllOptions = {}): Promise<DirEntry[]> {
    const limit = opts.maxEntries ?? DEFAULT_MAX_DIR_ENTRIES;
    const all: DirEntry[] = [];
    let cursor = "";
    do {
      const page = await this.ls(path, 0, cursor);
      if (all.length + page.entries.length > limit) throw new TooManyEntriesError("TOO_MANY_ENTRIES", path, limit);
      for (const e of page.entries) all.push(e);
      cursor = page.nextCursor;
    } while (cursor !== "");
    return all;
  }

  /**
   * Walks a directory tree depth first, parents before their children. Each entry carries its
   * attributes. Symlinks are reported and not followed. A failed listing throws, so nothing is
   * skipped silently. Leaving the loop early stops the walk. The walk holds one page per level on
   * the path to the current entry and throws TooManyEntriesError (code WALK_TOO_DEEP) when the
   * tree is deeper than `maxDepth` levels below `path` (default DEFAULT_MAX_WALK_DEPTH).
   */
  async *walk(path: string, opts: WalkOptions = {}): AsyncGenerator<WalkEntry, void, undefined> {
    const maxDepth = opts.maxDepth ?? DEFAULT_MAX_WALK_DEPTH;
    interface Level {
      dir: string;
      page: DirEntry[];
      i: number;
      next: string;
    }
    const open = async (dir: string, cursor: string): Promise<Level> => {
      const p = await this.ls(dir, 0, cursor);
      return { dir, page: p.entries, i: 0, next: p.nextCursor };
    };
    const stack: Level[] = [await open(path, "")];
    while (stack.length > 0) {
      const lv = stack[stack.length - 1];
      if (lv.i < lv.page.length) {
        const entry = lv.page[lv.i++];
        const full = lv.dir.endsWith("/") ? lv.dir + entry.name : `${lv.dir}/${entry.name}`;
        yield { path: full, entry };
        if (entry.type === FileType.Directory) {
          if (stack.length > maxDepth) throw new TooManyEntriesError("WALK_TOO_DEEP", full, maxDepth);
          stack.push(await open(full, ""));
        }
      } else if (lv.next !== "") {
        const p = await this.ls(lv.dir, 0, lv.next);
        lv.page = p.entries;
        lv.i = 0;
        lv.next = p.nextCursor;
      } else {
        stack.pop();
      }
    }
  }

  /** Opens a file. The handle stays bound to the connection that opened it. */
  async open(path: string, flags: number = OpenFlag.ReadOnly, mode = 0o644): Promise<FileHandle> {
    if ((flags & 3) === 3) throw new MountosError("EINVAL", "invalid access mode");
    return this.lease(async (conn) => {
      const resp = await conn.call(Method.Open, enc(pb.OpenRequest, { path, flags, mode }));
      return new FileHandle(conn, dec(pb.OpenResponse, resp.meta).handle, this.maxPayload, (m) => this.need(m));
    });
  }
}

// Says hello and sets the connection's in-flight limit from the reply. Without the pipelining
// capability the limit stays 1.
async function handshake(c: Conn, name: string, maxInflight: number): Promise<HelloResponse> {
  const req = enc(pb.HelloRequest, { clientName: name, minVersion: 1, maxVersion: 1, capabilities: ["pipelining"] });
  const h = decHello((await c.call(Method.Hello, req)).meta);
  if (h.version !== 1) throw new ProtocolError(`server chose unsupported version ${h.version}`);
  if (h.maxPayloadBytes === 0) throw new ProtocolError("server max_payload_bytes is zero");
  c.setLimit(h.capabilities.includes("pipelining") ? Math.min(maxInflight, Math.max(1, h.maxInflight)) : 1);
  return h;
}

/** An open file. Calls on one handle run in order. Different handles can run in parallel. */
export class FileHandle extends FileOps {
  private tail: Promise<unknown> = Promise.resolve();
  private closed = false;

  constructor(
    private conn: Conn,
    /** Server handle. Valid only on the connection that opened it. */
    readonly id: bigint,
    private chunk: number,
    private need: (method: number) => void,
  ) {
    super();
  }

  protected call(method: number, meta: Buffer): Promise<Buffer> {
    return this.op(async () => (await this.conn.call(method, meta)).meta);
  }

  private op<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.tail.then(() => {
      if (this.closed) throw new MountosError("EBADF", "handle is closed");
      return fn();
    });
    this.tail = run.then(noop, noop);
    return run;
  }

  /** Reads up to length bytes. Returns fewer at end of file and an empty Buffer at or past it. */
  read(offset: number, length: number): Promise<Buffer> {
    return this.op(async () => {
      const parts: Buffer[] = [];
      let got = 0;
      while (got < length) {
        const want = Math.min(length - got, this.chunk);
        const r = await this.conn.call(Method.ReadAt, enc(pb.ReadAtRequest, { handle: this.id, offset: BigInt(offset + got), length: want }));
        const count = dec(pb.ReadAtResponse, r.meta).count;
        if (count > want || count !== r.payload.length) {
          throw new ProtocolError(`ReadAt count ${count} does not match payload ${r.payload.length}`);
        }
        parts.push(r.payload);
        got += count;
        if (count < want) break;
      }
      return parts.length === 1 ? parts[0] : Buffer.concat(parts);
    });
  }

  /**
   * Reads many ranges in one round trip. The result has one entry per range in request order.
   * A range past the end of file returns fewer bytes. Ranges larger than the payload limit and
   * inputs over the request limits run as several requests.
   */
  readBatch(ranges: ReadRange[]): Promise<ReadResult[]> {
    return this.op(async () => {
      this.need(Method.ReadBatch);
      const groups = planPieces(ranges, this.chunk);
      const st = ranges.map(() => ({ parts: [] as Buffer[], short: false, error: undefined as MountosError | undefined }));
      for (const group of groups) {
        const req = enc(pb.ReadBatchRequest, {
          handle: this.id,
          ranges: group.map((p) => ({ offset: BigInt(p.offset), length: p.length })),
        });
        const r = await this.conn.call(Method.ReadBatch, req);
        const resp = dec(pb.ReadBatchResponse, r.meta);
        if (resp.counts.length !== group.length) {
          throw new ProtocolError(`ReadBatch returned ${resp.counts.length} counts for ${group.length} ranges`);
        }
        const errs = itemErrors(resp.errors, group.length);
        let pos = 0;
        group.forEach((p, k) => {
          const count = resp.counts[k];
          const s = st[p.item];
          const err = errs.get(k);
          if (err) {
            s.error ??= err;
            return;
          }
          if (count > p.length || pos + count > r.payload.length) {
            throw new ProtocolError(`ReadBatch count ${count} does not fit the reply`);
          }
          if (!s.short) s.parts.push(r.payload.subarray(pos, pos + count));
          if (count < p.length) s.short = true;
          pos += count;
        });
        if (pos !== r.payload.length) {
          throw new ProtocolError(`ReadBatch counts sum ${pos} does not match payload ${r.payload.length}`);
        }
      }
      return st.map((s): ReadResult => (s.error ? { error: s.error } : { data: Buffer.concat(s.parts) }));
    });
  }

  /**
   * Writes many items in one round trip. The result has one entry per item in request order.
   * An item that is cut short reports the bytes written before the cut. Items larger than the
   * payload limit and inputs over the request limits run as several requests.
   */
  writeBatch(items: WriteItem[]): Promise<WriteResult[]> {
    return this.op(async () => {
      this.need(Method.WriteBatch);
      const groups = planPieces(items.map((it) => ({ offset: it.offset, length: it.data.byteLength })), this.chunk);
      const st = items.map(() => ({ written: 0, short: false, error: undefined as MountosError | undefined }));
      for (const group of groups) {
        const data = group.map((p) => {
          const d = items[p.item].data;
          return Buffer.from(d.buffer, d.byteOffset + p.src, p.length);
        });
        const req = enc(pb.WriteBatchRequest, {
          handle: this.id,
          ranges: group.map((p) => ({ offset: BigInt(p.offset), length: p.length })),
        });
        const r = await this.conn.call(Method.WriteBatch, req, Buffer.concat(data));
        const resp = dec(pb.WriteBatchResponse, r.meta);
        if (resp.counts.length !== group.length) {
          throw new ProtocolError(`WriteBatch returned ${resp.counts.length} counts for ${group.length} ranges`);
        }
        const errs = itemErrors(resp.errors, group.length);
        group.forEach((p, k) => {
          const count = resp.counts[k];
          const s = st[p.item];
          const err = errs.get(k);
          if (err) {
            s.error ??= err;
            return;
          }
          if (count > p.length) throw new ProtocolError(`WriteBatch count ${count} exceeds request`);
          if (!s.short) s.written += count;
          if (count < p.length) s.short = true;
        });
      }
      return st.map((s): WriteResult => (s.error ? { error: s.error } : { written: s.written }));
    });
  }

  /** Writes data at offset in payload-sized pieces. Returns the bytes written. */
  write(offset: number, data: Uint8Array): Promise<number> {
    const buf = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
    return this.op(async () => {
      let done = 0;
      while (done < buf.length) {
        const piece = buf.subarray(done, done + this.chunk);
        const r = await this.conn.call(Method.WriteAt, enc(pb.WriteAtRequest, { handle: this.id, offset: BigInt(offset + done) }), piece);
        const count = dec(pb.WriteAtResponse, r.meta).count;
        if (count > piece.length) throw new ProtocolError(`WriteAt count ${count} exceeds request`);
        done += count;
        if (count < piece.length) break;
      }
      return done;
    });
  }

  async fsync(dataOnly = false): Promise<void> {
    await this.op(() => this.conn.call(Method.Fsync, enc(pb.FsyncRequest, { handle: this.id, dataOnly })));
  }

  /** Closes the handle. A second close fails with EBADF. */
  async close(): Promise<void> {
    await this.op(async () => {
      await this.conn.call(Method.Close, enc(pb.CloseRequest, { handle: this.id }));
      this.closed = true;
    });
  }
}

/** An open directory. Calls on one Dir run in order. */
export class Dir {
  private tail: Promise<unknown> = Promise.resolve();
  private closed = false;

  constructor(
    private conn: Conn,
    /** Server handle. Valid only on the connection that opened it. */
    readonly id: bigint,
  ) {}

  private op<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.tail.then(() => {
      if (this.closed) throw new MountosError("EBADF", "handle is closed");
      return fn();
    });
    this.tail = run.then(noop, noop);
    return run;
  }

  /** Reads up to max entries. eof is true when the directory has no more entries. */
  readdir(max: number): Promise<{ entries: DirEntry[]; eof: boolean }> {
    return this.op(async () => {
      const req = enc(pb.ReaddirRequest, { handle: this.id, maxEntries: max });
      const r = dec(pb.ReaddirResponse, (await this.conn.call(Method.Readdir, req)).meta);
      return { entries: r.entries.map(toDirEntry), eof: r.eof };
    });
  }

  /** Closes the handle. A second close fails with EBADF. */
  async close(): Promise<void> {
    await this.op(async () => {
      await this.conn.call(Method.Closedir, enc(pb.ClosedirRequest, { handle: this.id }));
      this.closed = true;
    });
  }
}
