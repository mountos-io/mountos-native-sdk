import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { describe, it } from "node:test";
import { Client, DeadlineExceededError, MountosError, ProtocolError, TransportError } from "../src/index.js";
import { FLAG_ERROR, FLAG_NOTIFY, FLAG_RESPONSE, type Frame, FrameReader } from "../src/frame.js";
import * as pb from "../src/gen/mountos_fs.js";

const quiet = { warn: (): void => {} };

function header(flags: number, method: number, id: number, metaLen: number, payloadLen: number): Buffer {
  const h = Buffer.alloc(24);
  h.write("MFS1", 0, "ascii");
  h[4] = 1;
  h[5] = flags;
  h.writeUInt16BE(method, 6);
  h.writeBigUInt64BE(BigInt(id), 8);
  h.writeUInt32BE(metaLen, 16);
  h.writeUInt32BE(payloadLen, 20);
  return h;
}

const reply = (flags: number, method: number, id: number, meta: Uint8Array = new Uint8Array(0)): Buffer =>
  Buffer.concat([header(flags, method, id, meta.length, 0), meta]);
const good = (f: Frame): Buffer => reply(FLAG_RESPONSE, f.method, f.requestId);
const okMeta = <T>(c: { encode(m: T): { finish(): Uint8Array } }, m: T): Uint8Array => c.encode(m).finish();

/** Misbehaves on the first request only, then answers every request with an empty success. */
const firstOnly = (bad: (s: net.Socket, f: Frame) => void): Act => {
  let used = false;
  return (s, f) => {
    if (used) s.write(good(f));
    else bad(s, f);
    used = true;
    return true;
  };
};

/** Return false to hold the socket open and silent. Return true to keep answering requests. */
type Act = (s: net.Socket, f: Frame) => boolean;

/**
 * Completes Hello on every connection. The first connection then misbehaves through act. Later
 * connections answer every request with an empty success, so a redialed client recovers.
 */
class HostileServer {
  conns = new Set<net.Socket>();
  private n = 0;
  private dir = fs.mkdtempSync(path.join(os.tmpdir(), "mfh-"));
  sockPath = path.join(this.dir, "h.sock");
  private srv: net.Server;

  constructor(act: Act) {
    this.srv = net.createServer((s) => {
      const hostile = this.n++ === 0;
      this.conns.add(s);
      s.on("close", () => this.conns.delete(s));
      s.on("error", () => {});
      const rd = new FrameReader();
      let silent = false;
      s.on("data", (c) => {
        if (silent) return;
        for (const f of rd.push(c)) {
          if (f.method === 1) {
            s.write(reply(FLAG_RESPONSE, 1, f.requestId, okMeta(pb.HelloResponse, {
              version: 1, capabilities: [], maxPayloadBytes: 1 << 20, maxInflight: 1, instanceId: 0n,
            })));
          } else if (!hostile) {
            s.write(good(f));
          } else if (!act(s, f)) {
            silent = true;
            return;
          }
        }
      });
    });
  }

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

const garbage = Buffer.alloc(128, 0x5a);
const bulk = (c: Client): Promise<unknown> => c.statBatch(["/a", "/b"]);

interface Case {
  name: string;
  act: Act;
  call: (c: Client) => Promise<unknown>;
  timeout?: number;
  /** Error classes the call may fail with. */
  want?: (new (...a: never[]) => Error)[];
  /** Calls after the hostile one that may still fail before the client works again. */
  retries?: number;
}

const cases: Case[] = [
  { name: "garbage bytes", call: (c) => c.ping(), want: [ProtocolError],
    act: (s) => (s.write(garbage), false) },
  { name: "bad version", call: (c) => c.ping(), want: [ProtocolError],
    act: (s, f) => {
      const h = header(FLAG_RESPONSE, f.method, f.requestId, 0, 0);
      h[4] = 9;
      s.write(h);
      return false;
    } },
  { name: "claim over the limit", call: (c) => c.ping(), want: [ProtocolError],
    act: (s, f) => (s.write(header(FLAG_RESPONSE, f.method, f.requestId, 0, 8 * 1024 * 1024 + 1)), false) },
  { name: "huge claim then stall", call: (c) => c.ping(), timeout: 300, want: [DeadlineExceededError],
    act: (s, f) => {
      s.write(header(FLAG_RESPONSE, f.method, f.requestId, 0, 8 * 1024 * 1024));
      s.write(Buffer.alloc(1000));
      return false;
    } },
  { name: "huge meta claim then stall", call: (c) => c.ping(), timeout: 300, want: [DeadlineExceededError],
    act: (s, f) => (s.write(header(FLAG_RESPONSE, f.method, f.requestId, 16 * 1024 * 1024, 0)), false) },
  { name: "truncated header then close", call: (c) => c.ping(), want: [TransportError],
    act: (s, f) => (s.end(header(FLAG_RESPONSE, f.method, f.requestId, 0, 0).subarray(0, 11)), false) },
  { name: "truncated body then close", call: (c) => c.ping(), want: [TransportError],
    act: (s, f) => (s.end(Buffer.concat([header(FLAG_RESPONSE, f.method, f.requestId, 40, 0), Buffer.from("short")])), false) },
  { name: "wrong request id", call: (c) => c.ping(), want: [ProtocolError],
    act: (s, f) => (s.write(reply(FLAG_RESPONSE, f.method, f.requestId + 1)), false) },
  { name: "request id too large", call: (c) => c.ping(), want: [ProtocolError],
    act: (s, f) => {
      const h = reply(FLAG_RESPONSE, f.method, 0);
      h.writeBigUInt64BE(1n << 60n, 8);
      s.write(h);
      return false;
    } },
  { name: "reply twice", call: (c) => c.ping(), retries: 2,
    act: (s, f) => (s.write(Buffer.concat([good(f), good(f)])), false) },
  { name: "wrong method id", call: (c) => c.ping(), want: [ProtocolError],
    act: (s, f) => (s.write(reply(FLAG_RESPONSE, f.method + 1, f.requestId)), false) },
  { name: "missing response flag", call: (c) => c.ping(), want: [ProtocolError],
    act: (s, f) => (s.write(reply(0, f.method, f.requestId)), false) },
  { name: "error frame with undecodable meta", call: (c) => c.ping(), want: [ProtocolError],
    act: firstOnly((s, f) => s.write(reply(FLAG_RESPONSE | FLAG_ERROR, f.method, f.requestId, Buffer.from([0xff, 0xff, 0xff, 0xff, 0xff])))) },
  { name: "response with undecodable meta", call: (c) => c.stat("/x"), want: [ProtocolError],
    act: firstOnly((s, f) => s.write(reply(FLAG_RESPONSE, f.method, f.requestId, Buffer.from([0x0a, 0xff, 0xff, 0xff, 0x0f])))) },
  { name: "stat response without info", call: (c) => c.stat("/x"), want: [ProtocolError],
    act: firstOnly((s, f) => s.write(good(f))) },
  { name: "notice with unknown kind", call: (c) => c.ping(),
    act: (s, f) => {
      const n = okMeta(pb.Notice, { kind: 9999 as pb.NoticeKind, sequence: 1n, refId: 0n, path: "", code: "", message: "", changeKind: 0, oldPath: "", cloned: 0n, overwritten: 0n, skipped: 0n, failed: 0n, removed: 0n, protected: 0n });
      s.write(reply(FLAG_NOTIFY, 62, f.requestId, n));
      s.write(good(f));
      return true;
    } },
  { name: "close in StatBatch", call: bulk, want: [TransportError],
    act: (s) => (s.destroy(), false) },
  { name: "close in ReadFile body", call: (c) => c.readFile("/a"), want: [TransportError],
    act: (s, f) => {
      s.write(header(FLAG_RESPONSE, f.method, f.requestId, 0, 1 << 20));
      s.end(Buffer.alloc(100 << 10));
      return false;
    } },
  { name: "close in ReadBatch", want: [TransportError],
    call: async (c) => {
      const f = await c.open("/a");
      return f.readBatch([{ offset: 0, length: 10 }, { offset: 10, length: 10 }]);
    },
    act: (s, f) => {
      if (f.method === 10) {
        s.write(reply(FLAG_RESPONSE, f.method, f.requestId, okMeta(pb.OpenResponse, { handle: 1n })));
        return true;
      }
      s.destroy();
      return false;
    } },
  { name: "never replies", call: (c) => c.ping(), timeout: 200, want: [DeadlineExceededError], act: () => false },
  { name: "never replies to bulk", call: bulk, timeout: 200, want: [DeadlineExceededError], act: () => false },
];

describe("hostile server", () => {
  for (const tc of cases) {
    it(tc.name, async () => {
      const srv = new HostileServer(tc.act);
      await srv.listen();
      const client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet, callTimeout: tc.timeout ?? 0 });
      try {
        const started = Date.now();
        const err = await tc.call(client).then(() => undefined, (e: unknown) => e);
        assert.ok(Date.now() - started < 5000, "call took too long");
        if (tc.want) {
          assert.ok(err instanceof Error, "call succeeded against a hostile server");
          assert.ok(tc.want.some((k) => err instanceof k), `unexpected error ${(err).name}: ${(err).message}`);
          assert.notEqual(err.message, "");
        }
        let recovered = false;
        for (let i = 0; i <= (tc.retries ?? 0) && !recovered; i++) {
          recovered = await client.ping().then(() => true, () => false);
        }
        assert.ok(recovered, "client did not recover on a healthy connection");
      } finally {
        client.close();
        await until(() => srv.conns.size === 0).catch(() => assert.fail("server socket still open after client close"));
        await srv.close();
      }
    });
  }

  it("an error frame with a valid WireError still maps to MountosError", async () => {
    const srv = new HostileServer(firstOnly((s, f) => {
      s.write(reply(FLAG_RESPONSE | FLAG_ERROR, f.method, f.requestId, okMeta(pb.WireError, { code: "ENOENT", message: "gone" })));
    }));
    await srv.listen();
    const client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet, callTimeout: 1000 });
    try {
      await assert.rejects(client.ping(), (e: unknown) => e instanceof MountosError && e.code === "ENOENT");
      await client.ping();
    } finally {
      client.close();
      await srv.close();
    }
  });

  it("logs an unknown notice kind and delivers no event", async () => {
    const logs: string[] = [];
    const srv = new HostileServer(cases.find((c) => c.name === "notice with unknown kind")!.act);
    await srv.listen();
    const client = await Client.connect(srv.sockPath, { poolSize: 1, logger: { warn: (m) => void logs.push(m) } });
    const seen: string[] = [];
    client.addListener((e) => void seen.push(e.kind));
    try {
      await client.ping();
      await new Promise((r) => setTimeout(r, 50));
      assert.deepEqual(seen, []);
      assert.ok(logs.some((l) => /unknown kind/.test(l)), logs.join("|"));
    } finally {
      client.close();
      await srv.close();
    }
  });
});
