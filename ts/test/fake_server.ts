import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { type BinaryReader, type BinaryWriter } from "@bufbuild/protobuf/wire";
import * as pb from "../src/gen/mountos_fs.js";

interface Codec<T> {
  encode(m: T, w?: BinaryWriter): BinaryWriter;
  decode(input: BinaryReader | Uint8Array, length?: number): T;
}
const enc = <T>(c: Codec<T>, m: T): Buffer => Buffer.from(c.encode(m).finish());

/** Offsets at or above this fail per item in batch calls. */
export const BAD_OFFSET = 1e12;

interface Req { method: number; id: number; meta: Buffer; payload: Buffer }

/** In-memory server that speaks the frame protocol. Handles are per connection and start at 1. */
export class FakeServer {
  files = new Map<string, Buffer>();
  symlinks = new Map<string, string>();
  /** Directory path to ordered entry names. */
  dirs = new Map<string, string[]>();
  times = new Map<string, { atime?: bigint; mtime?: bigint }>();
  private dirPos = new Map<string, number>();
  maxPayload = 1000;
  /** Capabilities the Hello reply reports. Empty reports none, like an older server. */
  capabilities: string[] = [];
  readDelayMs = 0;
  helloCount = 0;
  /** instance_id the Hello reply reports. 0n reports none, like an older server. */
  instanceId = 0n;
  readFileCalls = 0;
  writeFileCalls = 0;
  unwatchCount = 0;
  writeSizes: number[] = [];
  statBatchSizes: number[] = [];
  readBatchReqs: { ranges: number; bytes: number }[] = [];
  writeBatchReqs: { ranges: number; bytes: number }[] = [];
  /** Latest event sequence. A watch with an older since_sequence gets one overflow event. */
  sequence = 0n;
  /** Open client sockets. */
  conns = new Set<net.Socket>();
  private watches: { s: net.Socket; id: number; path: string; recursive: boolean }[] = [];
  private nextWatch = 100;
  /** Runs before each request is answered. A test uses it to push notices during a call. */
  beforeReply: ((s: net.Socket, method: number) => void | Promise<void>) | undefined;
  active = 0;
  maxActive = 0;
  /** Warm requests stay pending until finishWarms() while this is set. */
  warmHold = false;
  /** Makes WarmOpen and WarmRead fail with EAGAIN. */
  warmQueueFull = false;
  private warms = new Map<number, { path: string; s: net.Socket; done: boolean }>();
  private nextWarm = 1;
  sockPath: string;
  private dir: string;
  private srv: net.Server;

  constructor() {
    this.dir = fs.mkdtempSync(path.join(os.tmpdir(), "mfs-"));
    this.sockPath = path.join(this.dir, "s.sock");
    this.srv = net.createServer((s) => this.serve(s));
  }

  listen(): Promise<void> {
    return new Promise((r) => this.srv.listen(this.sockPath, r));
  }

  async close(): Promise<void> {
    await new Promise((r) => this.srv.close(r));
    fs.rmSync(this.sockPath, { force: true });
    fs.rmdirSync(this.dir);
  }

  private serve(s: net.Socket): void {
    this.conns.add(s);
    s.on("close", () => {
      this.conns.delete(s);
      this.watches = this.watches.filter((w) => w.s !== s);
    });
    const handles = new Map<number, string>();
    let next = 1;
    let buf = Buffer.alloc(0);
    let greeted = false;
    let busy: Promise<void> = Promise.resolve();
    s.on("error", () => {});
    s.on("data", (c) => {
      buf = Buffer.concat([buf, c]);
      for (;;) {
        if (buf.length < 24) return;
        const total = 24 + buf.readUInt32BE(16) + buf.readUInt32BE(20);
        if (buf.length < total) return;
        const m = buf.readUInt32BE(16);
        const req: Req = {
          method: buf.readUInt16BE(6),
          id: Number(buf.readBigUInt64BE(8)),
          meta: buf.subarray(24, 24 + m),
          payload: buf.subarray(24 + m, total),
        };
        buf = buf.subarray(total);
        busy = busy.then(() => this.handle(s, req, handles, () => next++, () => greeted, () => (greeted = true)));
      }
    });
  }

  /** Sends a change notice to every watch whose path covers the event path. */
  emit(kind: pb.WatchEventKind, path: string, oldPath = ""): void {
    const sequence = ++this.sequence;
    for (const w of this.watches) {
      const covers = path === w.path || path.startsWith(w.path === "/" ? "/" : w.path + "/");
      const direct = path.slice(0, path.lastIndexOf("/")) === w.path || (w.path === "/" && path.lastIndexOf("/") === 0);
      if (!(w.recursive ? covers : direct)) continue;
      this.notice(w.s, { kind: pb.NoticeKind.NOTICE_KIND_CHANGE, sequence, refId: BigInt(w.id), path, oldPath, changeKind: kind });
    }
  }

  /** Pushes a notice frame on any connection at any time. Sequence is filled in when missing. */
  notice(s: net.Socket, n: Partial<pb.Notice>, requestId = 0): void {
    const msg: pb.Notice = {
      kind: 0, sequence: n.sequence ?? ++this.sequence, refId: 0n, path: "", code: "", message: "", changeKind: 0, oldPath: "", ...n,
    };
    this.reply(s, { method: 62, id: requestId, meta: Buffer.alloc(0), payload: Buffer.alloc(0) }, enc(pb.Notice, msg), undefined, false, 8);
  }

  /** Open connections, oldest first. */
  sockets(): net.Socket[] {
    return [...this.conns];
  }

  /** The connection that issued the watch. */
  watchSocket(id: bigint): net.Socket | undefined {
    return this.watches.find((w) => BigInt(w.id) === id)?.s;
  }

  /** Finishes every pending warm. */
  finishWarms(): void {
    for (const [id, w] of this.warms) if (!w.done) this.finishWarm(id);
  }

  private finishWarm(id: number): void {
    const w = this.warms.get(id)!;
    w.done = true;
    if (this.files.has(w.path)) {
      this.notice(w.s, { kind: pb.NoticeKind.NOTICE_KIND_WARM_DONE, refId: BigInt(id), path: w.path }, id);
    } else {
      this.notice(w.s, {
        kind: pb.NoticeKind.NOTICE_KIND_WARM_FAILED, refId: BigInt(id), path: w.path, code: "ENOENT", message: "fake ENOENT",
      }, id);
    }
  }

  private startWarm(s: net.Socket, r: Req, path: string): void {
    if (this.warmQueueFull) return this.fail(s, r, "EAGAIN");
    const id = this.nextWarm++;
    this.warms.set(id, { path, s, done: false });
    this.ok(s, r, pb.WarmOpenResponse, { warmId: BigInt(id) });
    if (!this.warmHold) setTimeout(() => { if (!s.destroyed) this.finishWarm(id); }, 20);
  }

  private reply(s: net.Socket, r: Req, meta: Buffer, payload: Buffer = Buffer.alloc(0), err = false, flags = 0): void {
    const h = Buffer.alloc(24);
    h.write("MFS1", 0, "ascii");
    h[4] = 1;
    h[5] = flags || (err ? 3 : 1);
    h.writeUInt16BE(r.method, 6);
    h.writeBigUInt64BE(BigInt(r.id), 8);
    h.writeUInt32BE(meta.length, 16);
    h.writeUInt32BE(payload.length, 20);
    s.write(Buffer.concat([h, meta, payload]));
  }

  private fail(s: net.Socket, r: Req, code: string): void {
    this.reply(s, r, enc(pb.WireError, { code, message: "fake " + code }), undefined, true);
  }

  private ok<T>(s: net.Socket, r: Req, c: Codec<T>, m: T, payload?: Buffer): void {
    this.reply(s, r, enc(c, m), payload);
  }

  private async handle(
    s: net.Socket, r: Req, handles: Map<number, string>,
    alloc: () => number, greeted: () => boolean, greet: () => void,
  ): Promise<void> {
    const empty = Buffer.alloc(0);
    if (r.method !== 1 && !greeted()) return this.fail(s, r, "EPROTO");
    await this.beforeReply?.(s, r.method);
    switch (r.method) {
      case 1:
        greet();
        this.helloCount++;
        return this.ok(s, r, pb.HelloResponse, {
          version: 1, capabilities: this.capabilities, maxPayloadBytes: this.maxPayload, maxInflight: 1,
          instanceId: this.instanceId,
        });
      case 2:
        return this.reply(s, r, empty);
      case 20: {
        const q = pb.StatRequest.decode(r.meta);
        const file = this.files.get(q.path);
        if (!file) return this.fail(s, r, "ENOENT");
        return this.ok(s, r, pb.StatResponse, { info: this.info(q.path, file.length) });
      }
      case 10: {
        const q = pb.OpenRequest.decode(r.meta);
        const p = q.path;
        if (this.files.has(p) && q.flags & 128) return this.fail(s, r, "EEXIST");
        if (!this.files.has(p)) {
          if (!(q.flags & 64)) return this.fail(s, r, "ENOENT");
          this.files.set(p, Buffer.alloc(0));
        }
        if (q.flags & 512) this.files.set(p, Buffer.alloc(0));
        const h = alloc();
        handles.set(h, p);
        return this.ok(s, r, pb.OpenResponse, { handle: BigInt(h) });
      }
      case 11:
        if (!handles.delete(Number(pb.CloseRequest.decode(r.meta).handle))) return this.fail(s, r, "EBADF");
        return this.reply(s, r, empty);
      case 14: {
        const q = pb.ReadAtRequest.decode(r.meta);
        const p = handles.get(Number(q.handle));
        if (p === undefined) return this.fail(s, r, "EBADF");
        this.maxActive = Math.max(this.maxActive, ++this.active);
        if (this.readDelayMs) await new Promise((x) => setTimeout(x, this.readDelayMs));
        this.active--;
        const file = this.files.get(p)!;
        const off = Number(q.offset);
        const out = file.subarray(Math.min(off, file.length), Math.min(off + q.length, file.length));
        return this.ok(s, r, pb.ReadAtResponse, { count: out.length }, out);
      }
      case 15: {
        const q = pb.WriteAtRequest.decode(r.meta);
        const p = handles.get(Number(q.handle));
        if (p === undefined) return this.fail(s, r, "EBADF");
        this.writeSizes.push(r.payload.length);
        const off = Number(q.offset);
        let file: Buffer = this.files.get(p)!;
        if (file.length < off + r.payload.length) {
          file = Buffer.concat([file, Buffer.alloc(off + r.payload.length - file.length)]);
        }
        r.payload.copy(file, off);
        this.files.set(p, file);
        return this.ok(s, r, pb.WriteAtResponse, { count: r.payload.length });
      }
      case 21: {
        const p = pb.LstatRequest.decode(r.meta).path;
        const t = this.symlinks.get(p);
        if (t !== undefined) return this.ok(s, r, pb.LstatResponse, { info: this.info(p, t.length, 2) });
        const file = this.files.get(p);
        if (!file) return this.fail(s, r, "ENOENT");
        return this.ok(s, r, pb.LstatResponse, { info: this.info(p, file.length) });
      }
      case 27: {
        const q = pb.SymlinkRequest.decode(r.meta);
        if (this.symlinks.has(q.linkPath) || this.files.has(q.linkPath)) return this.fail(s, r, "EEXIST");
        this.symlinks.set(q.linkPath, q.target);
        return this.reply(s, r, empty);
      }
      case 28: {
        const t = this.symlinks.get(pb.ReadlinkRequest.decode(r.meta).path);
        if (t === undefined) return this.fail(s, r, "EINVAL");
        return this.ok(s, r, pb.ReadlinkResponse, { target: t });
      }
      case 29: {
        const q = pb.LinkRequest.decode(r.meta);
        const src = this.files.get(q.existing);
        if (!src) return this.fail(s, r, "ENOENT");
        if (this.files.has(q.newPath)) return this.fail(s, r, "EEXIST");
        this.files.set(q.newPath, src);
        return this.reply(s, r, empty);
      }
      case 31: {
        const q = pb.UtimensRequest.decode(r.meta);
        const rec: { atime?: bigint; mtime?: bigint } = {};
        if (q.setAtime) rec.atime = q.atimeNs;
        if (q.setMtime) rec.mtime = q.mtimeNs;
        this.times.set(q.path, rec);
        return this.reply(s, r, empty);
      }
      case 33:
        return this.ok(s, r, pb.StatfsResponse, {
          blockSize: 4096, blocks: 1n << 40n, blocksFree: 1000n, blocksAvail: 900n,
          files: 5000n, filesFree: 4000n, nameMax: 255,
        });
      case 40: {
        const p = pb.OpendirRequest.decode(r.meta).path;
        if (!this.dirs.has(p)) return this.fail(s, r, "ENOENT");
        const h = alloc();
        handles.set(h, "dir:" + p);
        this.dirPos.set(`${h}`, 0);
        return this.ok(s, r, pb.OpendirResponse, { handle: BigInt(h) });
      }
      case 41: {
        const q = pb.ReaddirRequest.decode(r.meta);
        const h = Number(q.handle);
        const p = handles.get(h);
        if (p === undefined || !p.startsWith("dir:")) return this.fail(s, r, "EBADF");
        const names = this.dirs.get(p.slice(4))!;
        const pos = this.dirPos.get(`${h}`) ?? 0;
        const max = q.maxEntries || names.length;
        const page = names.slice(pos, pos + max);
        this.dirPos.set(`${h}`, pos + page.length);
        return this.ok(s, r, pb.ReaddirResponse, {
          entries: page.map((n) => this.entry(n, false)),
          eof: pos + page.length >= names.length,
        });
      }
      case 42: {
        const h = Number(pb.ClosedirRequest.decode(r.meta).handle);
        if (!handles.get(h)?.startsWith("dir:")) return this.fail(s, r, "EBADF");
        handles.delete(h);
        return this.reply(s, r, empty);
      }
      case 43: {
        const q = pb.LsRequest.decode(r.meta);
        const names = this.dirs.get(q.path);
        if (!names) return this.fail(s, r, "ENOENT");
        const start = q.cursor !== "" ? Number(q.cursor) : 0;
        const max = q.maxEntries || names.length;
        return this.ok(s, r, pb.LsResponse, {
          entries: names.slice(start, start + max).map((n) => {
            const sub = q.path === "/" ? "/" + n : q.path + "/" + n;
            return this.entry(n, true, this.dirs.has(sub) ? 1 : 0);
          }),
          nextCursor: start + max < names.length ? String(start + max) : "",
        });
      }
      case 50: {
        const q = pb.StatBatchRequest.decode(r.meta);
        if (q.paths.length > 4096) return this.fail(s, r, "EINVAL");
        this.statBatchSizes.push(q.paths.length);
        const errors: pb.BatchItemError[] = [];
        const infos = q.paths.map((p, index) => {
          const file = this.files.get(p);
          if (!file) {
            errors.push({ index, code: "ENOENT", message: "fake ENOENT" });
            return this.info("", 0);
          }
          return this.info(p, file.length);
        });
        return this.ok(s, r, pb.StatBatchResponse, { infos, errors });
      }
      case 51: {
        const q = pb.ReadBatchRequest.decode(r.meta);
        const p = handles.get(Number(q.handle));
        if (p === undefined) return this.fail(s, r, "EBADF");
        const total = q.ranges.reduce((a, x) => a + x.length, 0);
        this.readBatchReqs.push({ ranges: q.ranges.length, bytes: total });
        if (q.ranges.length > 4096 || total > this.maxPayload) return this.fail(s, r, "EINVAL");
        const file = this.files.get(p)!;
        const errors: pb.BatchItemError[] = [];
        const outs: Buffer[] = [];
        const counts = q.ranges.map((x, index) => {
          const off = Number(x.offset);
          if (off >= BAD_OFFSET) {
            errors.push({ index, code: "EIO", message: "fake EIO" });
            return 0;
          }
          const out = file.subarray(Math.min(off, file.length), Math.min(off + x.length, file.length));
          outs.push(out);
          return out.length;
        });
        return this.ok(s, r, pb.ReadBatchResponse, { counts, errors }, Buffer.concat(outs));
      }
      case 52: {
        const q = pb.WriteBatchRequest.decode(r.meta);
        const p = handles.get(Number(q.handle));
        if (p === undefined) return this.fail(s, r, "EBADF");
        const total = q.ranges.reduce((a, x) => a + x.length, 0);
        this.writeBatchReqs.push({ ranges: q.ranges.length, bytes: total });
        if (q.ranges.length > 4096 || total > this.maxPayload || total !== r.payload.length) {
          return this.fail(s, r, "EINVAL");
        }
        let file: Buffer = this.files.get(p)!;
        let pos = 0;
        const errors: pb.BatchItemError[] = [];
        const counts = q.ranges.map((x, index) => {
          const data = r.payload.subarray(pos, pos + x.length);
          pos += x.length;
          const off = Number(x.offset);
          if (off >= BAD_OFFSET) {
            errors.push({ index, code: "EIO", message: "fake EIO" });
            return 0;
          }
          if (file.length < off + data.length) file = Buffer.concat([file, Buffer.alloc(off + data.length - file.length)]);
          data.copy(file, off);
          return data.length;
        });
        this.files.set(p, file);
        return this.ok(s, r, pb.WriteBatchResponse, { counts, errors });
      }
      case 53: {
        const q = pb.ReadFileRequest.decode(r.meta);
        const file = this.files.get(q.path);
        if (!file) return this.fail(s, r, "ENOENT");
        const limit = q.maxBytes === 0n ? this.maxPayload : Math.min(Number(q.maxBytes), this.maxPayload);
        const out = file.subarray(0, limit);
        this.readFileCalls++;
        return this.ok(s, r, pb.ReadFileResponse, { count: out.length }, out);
      }
      case 54: {
        const q = pb.WriteFileRequest.decode(r.meta);
        if (r.payload.length > this.maxPayload) return this.fail(s, r, "EINVAL");
        const old = q.truncate ? Buffer.alloc(0) : (this.files.get(q.path) ?? Buffer.alloc(0));
        const next = Buffer.alloc(Math.max(old.length, r.payload.length));
        old.copy(next);
        r.payload.copy(next);
        this.files.set(q.path, next);
        this.writeFileCalls++;
        return this.ok(s, r, pb.WriteFileResponse, { count: r.payload.length });
      }
      case 60: {
        const q = pb.WatchRequest.decode(r.meta);
        const id = this.nextWatch++;
        this.watches.push({ s, id, path: q.path, recursive: q.recursive });
        this.ok(s, r, pb.WatchResponse, { watchId: BigInt(id) });
        if (q.sinceSequence !== 0n && q.sinceSequence < this.sequence) {
          this.notice(s, { kind: pb.NoticeKind.NOTICE_KIND_OVERFLOW, refId: BigInt(id) }, r.id);
        }
        return;
      }
      case 61: {
        const id = Number(pb.UnwatchRequest.decode(r.meta).watchId);
        const w = this.watches.find((x) => x.s === s && x.id === id);
        if (!w) return this.fail(s, r, "EBADF");
        this.watches = this.watches.filter((x) => x !== w);
        this.unwatchCount++;
        return this.reply(s, r, empty);
      }
      case 70:
        return this.startWarm(s, r, pb.WarmOpenRequest.decode(r.meta).path);
      case 71:
        return this.startWarm(s, r, pb.WarmReadRequest.decode(r.meta).path);
      case 55: {
        const p = pb.RemoveAllRequest.decode(r.meta).path;
        let removed = 0;
        const under = (k: string): boolean => k === p || k.startsWith(p + "/");
        for (const k of [...this.files.keys()]) if (under(k)) { this.files.delete(k); removed++; }
        for (const k of [...this.symlinks.keys()]) if (under(k)) { this.symlinks.delete(k); removed++; }
        for (const k of [...this.dirs.keys()]) if (under(k)) { this.dirs.delete(k); removed++; }
        if (removed === 0) return this.fail(s, r, "ENOENT");
        return this.ok(s, r, pb.RemoveAllResponse, { removed: BigInt(removed) });
      }
      default:
        return this.fail(s, r, "ENOSYS");
    }
  }

  private info(name: string, size: number, type = 0, inode = 42n): pb.FileInfo {
    return {
      name, size: BigInt(size), mode: 0o644, type, mtimeNs: 0n, atimeNs: 0n, ctimeNs: 0n,
      uid: 0, gid: 0, nlink: 0, inode,
    };
  }

  private entry(name: string, withInfo: boolean, type = 0): pb.DirEntry {
    return { name, type, info: withInfo ? this.info(name, name.length, type, 7n) : undefined };
  }
}
