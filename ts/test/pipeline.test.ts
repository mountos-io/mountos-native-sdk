import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, it } from "node:test";
import { Client, ClientEvent, DeadlineExceededError, TransportError } from "../src/index.js";
import { FLAG_RESPONSE, Frame, FrameReader } from "../src/frame.js";
import * as pb from "../src/gen/mountos_fs.js";

const quiet = { warn: (): void => {} };
const PING = 2;
const OPEN = 10;
const CLOSE = 11;

function frame(method: number, id: number, meta: Uint8Array = new Uint8Array(0)): Buffer {
  const h = Buffer.alloc(24);
  h.write("MFS1", 0, "ascii");
  h[4] = 1;
  h[5] = FLAG_RESPONSE;
  h.writeUInt16BE(method, 6);
  h.writeBigUInt64BE(BigInt(id), 8);
  h.writeUInt32BE(meta.length, 16);
  return Buffer.concat([h, meta]);
}

/** Replies to a request. Used by a test to answer when and in the order it chooses. */
type Reply = () => void;

interface Req {
  s: net.Socket;
  f: Frame;
  /** Index of the connection, from 0. */
  conn: number;
  /** Answers with an empty success. */
  ok: Reply;
  /** Answers with a chosen meta. */
  send: (meta: Uint8Array) => void;
}

/**
 * Raw frame server. Hello is answered here. Every other request goes to onRequest, which may
 * answer at once, later, out of order, or never.
 */
class PipeServer {
  caps: string[] = ["pipelining"];
  maxInflight = 32;
  conns: net.Socket[] = [];
  seen: Req[] = [];
  /** Requests received and not yet answered, per server, and the highest value reached. */
  outstanding = 0;
  peak = 0;
  onRequest: (r: Req) => void = (r) => r.ok();
  private dir = fs.mkdtempSync(path.join(os.tmpdir(), "mfp-"));
  sockPath = path.join(this.dir, "p.sock");
  private srv = net.createServer((s) => {
    const conn = this.conns.length;
    this.conns.push(s);
    s.on("error", () => {});
    const rd = new FrameReader();
    s.on("data", (c) => {
      for (const f of rd.push(c)) {
        if (f.method === 1) {
          s.write(frame(1, f.requestId, pb.HelloResponse.encode({
            version: 1, capabilities: this.caps, maxPayloadBytes: 1 << 20, maxInflight: this.maxInflight,
          }).finish()));
          continue;
        }
        this.outstanding++;
        this.peak = Math.max(this.peak, this.outstanding);
        const send = (meta: Uint8Array): void => {
          this.outstanding--;
          if (!s.destroyed) s.write(frame(f.method, f.requestId, meta));
        };
        const r: Req = { s, f, conn, ok: () => send(new Uint8Array(0)), send };
        this.seen.push(r);
        this.onRequest(r);
      }
    });
  });

  listen(): Promise<void> {
    return new Promise((r) => this.srv.listen(this.sockPath, r));
  }

  async close(): Promise<void> {
    for (const s of this.conns) s.destroy();
    await new Promise((r) => this.srv.close(r));
    fs.rmSync(this.dir, { recursive: true, force: true });
  }
}

async function until(cond: () => boolean, ms = 3000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await new Promise((r) => setTimeout(r, 5));
  }
}

const openMeta = (handle: number): Uint8Array => pb.OpenResponse.encode({ handle: BigInt(handle) }).finish();

describe("pipelining", () => {
  let srv: PipeServer | undefined;
  let client: Client | undefined;

  async function start(s: PipeServer, opts: Parameters<typeof Client.connect>[1] = {}): Promise<Client> {
    srv = s;
    await s.listen();
    return (client = await Client.connect(s.sockPath, { poolSize: 1, logger: quiet, ...opts }));
  }

  afterEach(async () => {
    client?.close();
    client = undefined;
    await srv?.close();
    srv = undefined;
  });

  it("overlaps calls on one connection", async () => {
    const s = new PipeServer();
    const held: Req[] = [];
    s.onRequest = (r) => {
      held.push(r);
      if (held.length === 6) for (const h of held) h.ok();
    };
    const c = await start(s);
    await Promise.all(Array.from({ length: 6 }, () => c.ping()));
    assert.equal(s.peak, 6);
    assert.equal(s.conns.length, 1);
  });

  it("caps requests at the smaller of the client and server limits", async () => {
    const s = new PipeServer();
    s.maxInflight = 4;
    s.onRequest = (r) => setTimeout(r.ok, 5);
    const c = await start(s, { maxInflight: 3 });
    await Promise.all(Array.from({ length: 20 }, () => c.ping()));
    assert.equal(s.peak, 3);
    srv = undefined;
    client?.close();
    await s.close();

    const s2 = new PipeServer();
    s2.maxInflight = 2;
    s2.onRequest = (r) => setTimeout(r.ok, 5);
    const c2 = await start(s2);
    await Promise.all(Array.from({ length: 20 }, () => c2.ping()));
    assert.equal(s2.peak, 2);
  });

  it("matches out-of-order replies to the right callers", async () => {
    const s = new PipeServer();
    const held: Req[] = [];
    s.onRequest = (r) => {
      held.push(r);
      if (held.length < 5) return;
      for (const h of held.reverse()) {
        h.send(openMeta(Number(pb.OpenRequest.decode(h.f.meta).path.slice(1))));
      }
    };
    const c = await start(s);
    const files = await Promise.all([1, 2, 3, 4, 5].map((n) => c.open(`/${n}`)));
    assert.deepEqual(files.map((f) => Number(f.id)), [1, 2, 3, 4, 5]);
  });

  it("sends one request at a time to a server without the capability", async () => {
    const s = new PipeServer();
    s.caps = [];
    s.onRequest = (r) => setTimeout(r.ok, 5);
    const c = await start(s);
    await Promise.all(Array.from({ length: 8 }, () => c.ping()));
    assert.equal(s.peak, 1);
  });

  it("sends one request at a time when max_inflight is 1", async () => {
    const s = new PipeServer();
    s.maxInflight = 1;
    s.onRequest = (r) => setTimeout(r.ok, 5);
    const c = await start(s);
    await Promise.all(Array.from({ length: 8 }, () => c.ping()));
    assert.equal(s.peak, 1);
  });

  it("picks the connection with the fewest outstanding calls", async () => {
    const s = new PipeServer();
    const held: Req[] = [];
    s.onRequest = (r) => held.push(r);
    const c = await start(s, { poolSize: 3 });
    const calls = Array.from({ length: 9 }, () => c.ping());
    await until(() => held.length === 9);
    for (let i = 0; i < 3; i++) assert.equal(held.filter((h) => h.conn === i).length, 3);
    for (const h of held) h.ok();
    await Promise.all(calls);
  });

  it("fails only the timed-out call and discards its late reply", async () => {
    const s = new PipeServer();
    let first: Req | undefined;
    s.onRequest = (r) => {
      if (!first) first = r;
      else r.ok();
    };
    const events: ClientEvent[] = [];
    const c = await start(s, { callTimeout: 80 });
    c.addListener((e) => events.push(e));
    await assert.rejects(c.ping(), DeadlineExceededError);
    await c.ping();
    first!.ok();
    await new Promise((r) => setTimeout(r, 30));
    await c.ping();
    assert.equal(s.conns.length, 1);
    assert.ok(!s.conns[0].destroyed);
    assert.equal(events.filter((e) => e.kind === "connectionLost").length, 0);
  });

  it("releases the handle of an open that answers after its timeout", async () => {
    const s = new PipeServer();
    let first: Req | undefined;
    s.onRequest = (r) => {
      if (r.f.method === OPEN && !first) first = r;
      else r.ok();
    };
    const c = await start(s, { callTimeout: 80 });
    await assert.rejects(c.open("/slow"), DeadlineExceededError);
    first!.send(openMeta(77));
    await until(() => s.seen.some((r) => r.f.method === CLOSE));
    const close = s.seen.find((r) => r.f.method === CLOSE)!;
    assert.equal(pb.CloseRequest.decode(close.f.meta).handle, 77n);
    await c.ping();
  });

  it("drops and redials the connection when too many ids are abandoned", async () => {
    const s = new PipeServer();
    s.maxInflight = 2;
    s.onRequest = (r) => {
      if (r.conn > 0) r.ok();
    };
    const c = await start(s, { callTimeout: 30 });
    for (let i = 0; i < 9; i++) await assert.rejects(c.ping(), DeadlineExceededError);
    await until(() => s.conns[0].destroyed);
    await c.ping();
    assert.equal(s.conns.length, 2);
  });

  it("fails every outstanding call on a transport error", async () => {
    const s = new PipeServer();
    const held: Req[] = [];
    s.onRequest = (r) => {
      held.push(r);
      if (held.length === 4) r.s.destroy();
    };
    const c = await start(s);
    const calls = Array.from({ length: 4 }, () => c.ping());
    const results = await Promise.allSettled(calls);
    for (const r of results) {
      assert.equal(r.status, "rejected");
      assert.ok((r as PromiseRejectedResult).reason instanceof TransportError);
    }
    s.onRequest = (r) => r.ok();
    await c.ping();
    assert.equal(s.conns.length, 2);
  });

  const timers = (): number => process.getActiveResourcesInfo().filter((n) => n === "Timeout").length;

  it("leaves nothing running after close", async () => {
    const s = new PipeServer();
    s.onRequest = () => {};
    const before = timers();
    const c = await start(s, { callTimeout: 60_000 });
    const calls = Array.from({ length: 5 }, () => c.ping());
    await until(() => s.seen.length === 5);
    assert.ok(timers() >= before + 5);
    c.close();
    for (const r of await Promise.allSettled(calls)) assert.equal(r.status, "rejected");
    assert.equal(timers(), before);
    await until(() => s.conns.every((x) => x.destroyed));
  });
});
