import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, ClientEvent, MountosError } from "../src/index.js";
import * as pb from "../src/gen/mountos_fs.js";
import { FakeServer } from "./fake_server.js";

const NK = pb.NoticeKind;

async function until(cond: () => boolean, ms = 2000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe("notices", () => {
  const srv = new FakeServer();
  let client: Client;
  let logs: string[];
  const logger = { warn: (m: string): void => void logs.push(m) };

  before(() => srv.listen());
  after(() => {
    client?.close();
    return srv.close();
  });
  beforeEach(async () => {
    client?.close();
    await until(() => srv.conns.size === 0);
    srv.files.clear();
    srv.warmHold = false;
    srv.warmQueueFull = false;
    srv.beforeReply = undefined;
    srv.unwatchCount = 0;
    logs = [];
    client = await Client.connect(srv.sockPath, { poolSize: 3, logger });
  });

  it("matches responses while notices arrive between and during calls", async () => {
    srv.files.set("/n", Buffer.from("abc"));
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    srv.beforeReply = (s, method) => {
      if (method === 20) {
        srv.notice(s, { kind: NK.NOTICE_KIND_WARM_DONE, refId: 7n, path: "/before" });
        srv.notice(s, { kind: NK.NOTICE_KIND_WARM_DONE, refId: 8n, path: "/during" });
      }
    };
    for (let i = 0; i < 6; i++) {
      assert.equal((await client.stat("/n")).size, 3);
      for (const s of srv.sockets()) srv.notice(s, { kind: NK.NOTICE_KIND_WARM_DONE, refId: 100n + BigInt(i) });
    }
    await until(() => heard.length === 6 * 2 + 6 * 3);
    const seq = heard.map((e) => e.sequence).sort((a, b) => (a < b ? -1 : 1));
    assert.deepEqual(seq, Array.from({ length: 30 }, (_, i) => BigInt(i + 1)));
  });

  it("keeps notice order on one connection", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    const s = srv.sockets()[0];
    for (let i = 1; i <= 50; i++) srv.notice(s, { kind: NK.NOTICE_KIND_WARM_DONE, refId: BigInt(i) });
    await until(() => heard.length === 50);
    assert.deepEqual(heard.map((e) => e.refId), Array.from({ length: 50 }, (_, i) => BigInt(i + 1)));
  });

  it("delivers a notice on an idle connection without any call", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    srv.notice(srv.sockets()[0], { kind: NK.NOTICE_KIND_WARM_DONE, refId: 5n, path: "/idle" });
    await until(() => heard.length === 1);
    assert.equal(heard[0].kind, "warmDone");
    assert.equal(heard[0].refId, 5n);
    assert.equal(heard[0].path, "/idle");
    assert.ok(heard[0].time instanceof Date);
  });

  it("warmDone and warmFailed reach every listener with code, message and path", async () => {
    srv.files.set("/ok", Buffer.from("x"));
    const a: ClientEvent[] = [];
    const b: ClientEvent[] = [];
    client.addListener((e) => a.push(e));
    client.addListener((e) => b.push(e));
    const okId = await client.warmOpen("/ok");
    const badId = await client.warmRead("/missing", 0, 0);
    assert.ok(okId > 0);
    await until(() => a.length === 2 && b.length === 2);
    for (const got of [a, b]) {
      const done = got.find((e) => e.kind === "warmDone")!;
      const failed = got.find((e) => e.kind === "warmFailed")!;
      assert.equal(done.refId, BigInt(okId));
      assert.equal(done.path, "/ok");
      assert.equal(done.error, undefined);
      assert.equal(failed.refId, BigInt(badId));
      assert.equal(failed.path, "/missing");
      assert.ok(failed.error instanceof MountosError);
      assert.equal(failed.error.code, "ENOENT");
      assert.match(failed.error.message, /fake ENOENT/);
    }
    assert.deepEqual(logs, []);
  });

  it("warmOpen returns EAGAIN at once when the queue is full", async () => {
    srv.warmQueueFull = true;
    await assert.rejects(client.warmOpen("/f"), (e: unknown) => e instanceof MountosError && e.code === "EAGAIN");
  });

  it("logs through the supplied logger when no listener is registered", async () => {
    srv.notice(srv.sockets()[0], { kind: NK.NOTICE_KIND_WARM_FAILED, refId: 9n, path: "/gone", code: "ENOENT", message: "gone" });
    await until(() => logs.length === 1);
    assert.match(logs[0], /kind=warmFailed/);
    assert.match(logs[0], /refId=9/);
    assert.match(logs[0], /path="\/gone"/);
    assert.match(logs[0], /ENOENT/);
  });

  it("does not log when a listener is registered, and stops after removal", async () => {
    const heard: ClientEvent[] = [];
    const off = client.addListener((e) => heard.push(e));
    srv.notice(srv.sockets()[0], { kind: NK.NOTICE_KIND_WARM_DONE, refId: 1n });
    await until(() => heard.length === 1);
    assert.deepEqual(logs, []);
    off();
    srv.notice(srv.sockets()[0], { kind: NK.NOTICE_KIND_WARM_DONE, refId: 2n });
    await until(() => logs.length === 1);
    assert.equal(heard.length, 1);
  });

  it("falls back to the console when no logger is given", async () => {
    const c = await Client.connect(srv.sockPath, { poolSize: 1 });
    const lines: string[] = [];
    const orig = console.warn;
    console.warn = (m: string) => void lines.push(String(m));
    try {
      const mine = srv.sockets()[srv.sockets().length - 1];
      srv.notice(mine, { kind: NK.NOTICE_KIND_WARM_DONE, refId: 3n, path: "/c" });
      await until(() => lines.length === 1);
      assert.match(lines[0], /kind=warmDone/);
    } finally {
      console.warn = orig;
      c.close();
    }
  });

  it("a throwing listener is logged and does not stop the others", async () => {
    const heard: ClientEvent[] = [];
    client.addListener(() => {
      throw new Error("boom");
    });
    client.addListener((e) => heard.push(e));
    srv.notice(srv.sockets()[0], { kind: NK.NOTICE_KIND_WARM_DONE, refId: 4n });
    await until(() => heard.length === 1);
    assert.equal(logs.length, 1);
    assert.match(logs[0], /listener threw/);
    assert.match(logs[0], /boom/);
    assert.equal((await client.stat("/missing").catch((e: MountosError) => e.code)), "ENOENT");
  });

  it("emits connectionLost when a pooled connection drops", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    srv.sockets()[0].destroy();
    await until(() => heard.length === 1);
    assert.equal(heard[0].kind, "connectionLost");
    assert.ok(heard[0].error instanceof Error);
    assert.ok(!(heard[0].error instanceof MountosError));
    await client.ping();
  });

  it("emits closing and does not report the following close as lost", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    const s = srv.sockets()[0];
    srv.notice(s, { kind: NK.NOTICE_KIND_CLOSING });
    s.end();
    await until(() => heard.length >= 1 && srv.conns.size === 2);
    await new Promise((r) => setTimeout(r, 50));
    assert.deepEqual(heard.map((e) => e.kind), ["closing"]);
  });

  it("does not report connectionLost for its own close", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    client.close();
    await until(() => srv.conns.size === 0);
    assert.deepEqual(heard, []);
    assert.deepEqual(logs, []);
  });

  it("turns a change notice of kind overflow into an overflow event for the watch", async () => {
    const w = await client.watch("/d");
    const it = w[Symbol.asyncIterator]();
    const next = it.next();
    srv.notice(srv.watchSocket(w.id)!, {
      kind: NK.NOTICE_KIND_CHANGE, refId: w.id, changeKind: pb.WatchEventKind.WATCH_EVENT_KIND_OVERFLOW,
    });
    const ev = await next;
    assert.equal(ev.value.kind, "overflow");
    assert.equal(ev.value.refId, w.id);
    assert.deepEqual(logs, []);
    await w.close();
  });

  describe("waitWarm", () => {
    it("resolves when the warm finishes", async () => {
      srv.files.set("/f", Buffer.from("x"));
      const id = await client.warmOpen("/f");
      await client.waitWarm(id);
    });

    it("rejects with the server error on failure", async () => {
      const id = await client.warmOpen("/missing");
      await assert.rejects(client.waitWarm(id), (e: unknown) =>
        e instanceof MountosError && e.code === "ENOENT" && /fake ENOENT/.test(e.message));
    });

    it("sees a warm that finished before the call", async () => {
      srv.files.set("/f", Buffer.from("x"));
      const seen: ClientEvent[] = [];
      client.addListener((e) => seen.push(e));
      const ok = await client.warmOpen("/f");
      const bad = await client.warmOpen("/missing");
      await until(() => seen.length === 2);
      await client.waitWarm(ok);
      await assert.rejects(client.waitWarm(BigInt(bad)), MountosError);
    });

    it("rejects on abort and leaves no waiter behind", async () => {
      srv.files.set("/f", Buffer.from("x"));
      srv.warmHold = true;
      const id = await client.warmOpen("/f");
      const ac = new AbortController();
      const p = client.waitWarm(id, { signal: ac.signal });
      ac.abort();
      await assert.rejects(p, (e: unknown) => (e as Error).name === "AbortError");
      await assert.rejects(client.waitWarm(id, { signal: ac.signal }), (e: unknown) => (e as Error).name === "AbortError");
      srv.finishWarms();
      await client.waitWarm(id);
    });

    it("keeps only the last 1024 finished warms", async () => {
      srv.files.set("/f", Buffer.from("x"));
      const s = srv.sockets()[0];
      for (let i = 1; i <= 1030; i++) srv.notice(s, { kind: NK.NOTICE_KIND_WARM_DONE, refId: BigInt(i) });
      await until(() => logs.length === 1030);
      await client.waitWarm(1030);
      await client.waitWarm(7);
      const ac = new AbortController();
      const p = client.waitWarm(3, { signal: ac.signal });
      ac.abort();
      await assert.rejects(p);
    });

    it("rejects when the client closes", async () => {
      const p = client.waitWarm(999);
      client.close();
      await assert.rejects(p, /client closed/);
      await assert.rejects(client.waitWarm(1), /client closed/);
    });
  });

  it("close removes listeners and leaves no connection open", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    const w = await client.watch("/d");
    client.close();
    client.close();
    await until(() => srv.conns.size === 0);
    assert.equal((await w[Symbol.asyncIterator]().next()).done, true);
    assert.deepEqual(heard, []);
  });
});
