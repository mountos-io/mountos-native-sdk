import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, TransportError } from "../src/index.js";
import { FakeServer } from "./fake_server.js";

async function until(cond: () => boolean, ms = 2000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await new Promise((r) => setTimeout(r, 5));
  }
}

const quiet = { warn: (): void => {} };
const POOL = 3;

describe("redial", () => {
  const srv = new FakeServer();
  let client: Client;

  before(() => srv.listen());
  after(() => {
    client?.close();
    return srv.close();
  });
  beforeEach(async () => {
    client?.close();
    await until(() => srv.conns.size === 0);
    srv.files.clear();
    srv.beforeReply = undefined;
    srv.files.set("/f", Buffer.from("abc"));
    client = await Client.connect(srv.sockPath, { poolSize: POOL, logger: quiet });
  });

  const killAll = async (): Promise<void> => {
    for (const s of srv.sockets()) s.destroy();
    await until(() => srv.conns.size === 0);
  };

  it("serves the next call on a new connection after the server drops every socket", async () => {
    const before = srv.helloCount;
    await killAll();
    assert.equal((await client.stat("/f")).size, 3);
    assert.ok(srv.helloCount > before);
    assert.ok(srv.conns.size >= 1);
  });

  it("replaces a dead connection while live ones keep serving", async () => {
    srv.sockets()[0].destroy();
    await until(() => srv.conns.size === POOL - 1);
    assert.equal((await client.stat("/f")).size, 3);
    await until(() => srv.conns.size === POOL);
  });

  it("does not redial after close", async () => {
    await killAll();
    const hellos = srv.helloCount;
    client.close();
    await assert.rejects(client.stat("/f"), (e: unknown) => e instanceof TransportError && /closed/.test(e.message));
    await new Promise((r) => setTimeout(r, 50));
    assert.equal(srv.helloCount, hellos);
    assert.equal(srv.conns.size, 0);
  });

  it("closes a dial that finishes after close", async () => {
    await killAll();
    const p = client.stat("/f");
    client.close();
    await assert.rejects(p, TransportError);
    await until(() => srv.conns.size === 0);
  });

  it("opens no more than the pool size during a burst of calls", async () => {
    await killAll();
    const hellos = srv.helloCount;
    const sizes = await Promise.all(Array.from({ length: 30 }, () => client.stat("/f").then((i) => i.size)));
    assert.deepEqual(new Set(sizes), new Set([3]));
    assert.ok(srv.helloCount - hellos <= POOL, `dialed ${srv.helloCount - hellos} times`);
    assert.ok(srv.conns.size <= POOL);
  });

  it("fails the call in flight on the dead socket and recovers on the next call", async () => {
    let hold = true;
    srv.beforeReply = async (s, method) => {
      if (method === 20 && hold) {
        hold = false;
        s.destroy();
        await new Promise(() => {});
      }
    };
    await assert.rejects(client.stat("/f"), TransportError);
    assert.equal((await client.stat("/f")).size, 3);
  });

  it("ends a watch on a lost connection and accepts a new watch", async () => {
    const w = await client.watch("/");
    const it1 = w[Symbol.asyncIterator]();
    const first = it1.next();
    await killAll();
    assert.equal((await first).done, true);
    const w2 = await client.watch("/");
    srv.emit(1, "/x");
    const ev = await w2[Symbol.asyncIterator]().next();
    assert.equal(ev.value.path, "/x");
    await w2.close();
  });

  it("keeps waitWarm waiters pending across a reconnect", async () => {
    srv.warmHold = true;
    const id = await client.warmOpen("/f");
    const waiting = client.waitWarm(id);
    let settled = false;
    waiting.then(() => (settled = true), () => (settled = true));
    await killAll();
    await client.ping();
    assert.equal(settled, false);
    client.close();
    await assert.rejects(waiting, TransportError);
    srv.warmHold = false;
  });

  it("surfaces a transport error when the server is gone", async () => {
    const gone = new FakeServer();
    await gone.listen();
    const c = await Client.connect(gone.sockPath, { poolSize: 2, logger: quiet });
    for (const s of gone.sockets()) s.destroy();
    await until(() => gone.conns.size === 0);
    await gone.close();
    await assert.rejects(c.stat("/f"), TransportError);
    await assert.rejects(c.stat("/f"), TransportError);
    c.close();
  });
});
