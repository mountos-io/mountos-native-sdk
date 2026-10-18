import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, ClientEvent, MountosError, OpenFlag } from "../src/index.js";
import * as pb from "../src/gen/mountos_fs.js";
import { BAD_OFFSET, FakeServer } from "./fake_server.js";

const RW = OpenFlag.ReadWrite | OpenFlag.Create;
const K = pb.WatchEventKind;

async function until(cond: () => boolean, ms = 2000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await new Promise((r) => setTimeout(r, 5));
  }
}

async function take(it: AsyncIterator<ClientEvent>, n: number): Promise<ClientEvent[]> {
  const out: ClientEvent[] = [];
  while (out.length < n) {
    const r = await it.next();
    assert.equal(r.done, false);
    out.push(r.value);
  }
  return out;
}

describe("bulk operations", () => {
  const srv = new FakeServer();
  let client: Client;

  before(() => srv.listen());
  after(() => {
    client?.close();
    return srv.close();
  });
  beforeEach(async () => {
    client?.close();
    srv.files.clear();
    srv.maxPayload = 1000;
    srv.statBatchSizes = [];
    srv.readBatchReqs = [];
    srv.writeBatchReqs = [];
    srv.writeSizes = [];
    srv.readFileCalls = 0;
    srv.writeFileCalls = 0;
    client = await Client.connect(srv.sockPath, { poolSize: 2 });
  });

  it("statBatch reports a failing item in place", async () => {
    srv.files.set("/a", Buffer.from("12345"));
    srv.files.set("/c", Buffer.from("1"));
    const r = await client.statBatch(["/a", "/missing", "/c"]);
    assert.equal(r.length, 3);
    assert.equal(r[0].info?.size, 5);
    assert.equal(r[0].error, undefined);
    assert.ok(r[1].error instanceof MountosError && r[1].error.code === "ENOENT");
    assert.equal(r[1].info, undefined);
    assert.equal(r[2].info?.name, "/c");
    assert.deepEqual(await client.statBatch([]), []);
  });

  it("statBatch splits more than 4096 paths and maps indexes", async () => {
    const n = 10000;
    const paths: string[] = [];
    for (let i = 0; i < n; i++) {
      paths.push(`/f${i}`);
      if (i % 7 !== 0) srv.files.set(`/f${i}`, Buffer.alloc(i % 50));
    }
    const r = await client.statBatch(paths);
    assert.deepEqual([...srv.statBatchSizes].sort(), [1808, 4096, 4096]);
    assert.equal(r.length, n);
    for (let i = 0; i < n; i++) {
      if (i % 7 === 0) {
        assert.equal((r[i].error as MountosError).code, "ENOENT", `index ${i}`);
      } else {
        assert.equal(r[i].info?.name, paths[i], `index ${i}`);
        assert.equal(r[i].info?.size, i % 50);
      }
    }
  });

  it("round trips readBatch and writeBatch across a hole", async () => {
    const f = await client.open("/h", RW);
    const w = await f.writeBatch([
      { offset: 0, data: Buffer.from("abc") },
      { offset: 10, data: Buffer.from("xyz") },
      { offset: BAD_OFFSET, data: Buffer.from("no") },
      { offset: 3, data: Buffer.alloc(0) },
    ]);
    assert.equal(w[0].written, 3);
    assert.equal(w[1].written, 3);
    assert.ok(w[2].error instanceof MountosError && w[2].error.code === "EIO");
    assert.equal(w[2].written, undefined);
    assert.equal(w[3].written, 0);
    const r = await f.readBatch([
      { offset: 0, length: 3 },
      { offset: 3, length: 7 },
      { offset: 10, length: 3 },
      { offset: 20, length: 5 },
      { offset: BAD_OFFSET, length: 4 },
      { offset: 8, length: 10 },
    ]);
    assert.equal(r[0].data?.toString(), "abc");
    assert.deepEqual(r[1].data, Buffer.alloc(7));
    assert.equal(r[2].data?.toString(), "xyz");
    assert.equal(r[3].data?.length, 0);
    assert.equal((r[4].error as MountosError).code, "EIO");
    assert.deepEqual(r[5].data, Buffer.concat([Buffer.alloc(2), Buffer.from("xyz")]));
    assert.deepEqual(await f.readBatch([]), []);
    await f.close();
  });

  it("splits batches by payload size", async () => {
    const f = await client.open("/p", RW);
    const items = Array.from({ length: 5 }, (_, i) => ({ offset: i * 400, data: Buffer.alloc(400, i + 1) }));
    const w = await f.writeBatch(items);
    assert.deepEqual(w.map((x) => x.written), [400, 400, 400, 400, 400]);
    assert.ok(srv.writeBatchReqs.length >= 3);
    for (const q of srv.writeBatchReqs) assert.ok(q.bytes <= 1000);

    const r = await f.readBatch(items.map((it) => ({ offset: it.offset, length: 400 })));
    r.forEach((x, i) => assert.deepEqual(x.data, items[i].data));
    for (const q of srv.readBatchReqs) assert.ok(q.bytes <= 1000);
    assert.ok(srv.readBatchReqs.length >= 3);

    // One range larger than the payload limit is cut into pieces and joined.
    srv.readBatchReqs = [];
    const big = Buffer.alloc(2500);
    for (let i = 0; i < big.length; i++) big[i] = i % 251;
    const g = await client.open("/big", RW);
    const wb = await g.writeBatch([{ offset: 0, data: big }]);
    assert.equal(wb[0].written, 2500);
    assert.deepEqual(srv.files.get("/big"), big);
    const rb = await g.readBatch([{ offset: 0, length: 2500 }, { offset: 2400, length: 500 }]);
    assert.deepEqual(rb[0].data, big);
    assert.deepEqual(rb[1].data, big.subarray(2400));
    for (const q of srv.readBatchReqs) assert.ok(q.bytes <= 1000);
    await g.close();
    await f.close();
  });

  it("splits batches over 4096 ranges", async () => {
    srv.maxPayload = 1 << 20;
    client.close();
    client = await Client.connect(srv.sockPath, { poolSize: 1 });
    const data = Buffer.alloc(5000);
    for (let i = 0; i < data.length; i++) data[i] = (i * 7) % 256;
    srv.files.set("/many", Buffer.from(data));
    const f = await client.open("/many", OpenFlag.ReadWrite);
    const r = await f.readBatch(Array.from({ length: 5000 }, (_, i) => ({ offset: i, length: 1 })));
    assert.deepEqual(srv.readBatchReqs.map((q) => q.ranges), [4096, 904]);
    assert.deepEqual(Buffer.concat(r.map((x) => x.data!)), data);
    const w = await f.writeBatch(Array.from({ length: 5000 }, (_, i) => ({ offset: i, data: Buffer.from([1]) })));
    assert.deepEqual(srv.writeBatchReqs.map((q) => q.ranges), [4096, 904]);
    assert.ok(w.every((x) => x.written === 1));
    await f.close();
  });

  it("rejects invalid ranges before sending", async () => {
    const f = await client.open("/v", RW);
    await assert.rejects(f.readBatch([{ offset: -1, length: 1 }]), RangeError);
    await assert.rejects(f.writeBatch([{ offset: 0.5, data: Buffer.alloc(1) }]), RangeError);
    assert.equal(srv.readBatchReqs.length, 0);
    await f.close();
  });

  it("readFile returns a small file in one request", async () => {
    srv.files.set("/s", Buffer.from("hello"));
    assert.equal((await client.readFile("/s")).toString(), "hello");
    assert.equal(srv.readFileCalls, 1);
    await assert.rejects(client.readFile("/none"), (e) => (e as MountosError).code === "ENOENT");
  });

  it("readFile loops for a file larger than one payload", async () => {
    for (const size of [1000, 1001, 2500, 3000]) {
      const data = Buffer.alloc(size);
      for (let i = 0; i < size; i++) data[i] = (i * 13) % 256;
      srv.files.set("/l", data);
      assert.deepEqual(await client.readFile("/l"), data, `size ${size}`);
    }
  });

  it("writeFile writes small data in one request and creates the file", async () => {
    await client.writeFile("/w", Buffer.from("hello world"));
    assert.equal(srv.writeFileCalls, 1);
    assert.equal(srv.files.get("/w")?.toString(), "hello world");
    await client.writeFile("/w", Buffer.from("HE"), { truncate: false });
    assert.equal(srv.files.get("/w")?.toString(), "HEllo world");
    await client.writeFile("/w", Buffer.from("HE"));
    assert.equal(srv.files.get("/w")?.toString(), "HE");
  });

  it("writeFile falls back to chunked writes over the payload limit", async () => {
    const data = Buffer.alloc(2500, 9);
    srv.files.set("/bw", Buffer.alloc(4000, 1));
    await client.writeFile("/bw", data);
    assert.equal(srv.writeFileCalls, 0);
    assert.deepEqual(srv.writeSizes, [1000, 1000, 500]);
    assert.deepEqual(srv.files.get("/bw"), data);
    srv.files.set("/bw2", Buffer.alloc(4000, 1));
    await client.writeFile("/bw2", data, { truncate: false });
    assert.equal(srv.files.get("/bw2")?.length, 4000);
    assert.deepEqual(srv.files.get("/bw2")!.subarray(0, 2500), data);
  });
});


describe("watch", () => {
  const srv = new FakeServer();
  let client: Client;
  const quiet = { warn: (): void => {} };

  before(() => srv.listen());
  after(() => {
    client?.close();
    return srv.close();
  });
  beforeEach(async () => {
    client?.close();
    await until(() => srv.conns.size === 0);
    srv.files.clear();
    srv.sequence = 0n;
    srv.unwatchCount = 0;
    srv.beforeReply = undefined;
    client = await Client.connect(srv.sockPath, { poolSize: 2, logger: quiet });
  });

  it("returns a watch id and uses no extra connection", async () => {
    const w = await client.watch("/d");
    assert.ok(w.id > 0n);
    assert.equal(srv.conns.size, 2);
    await w.close();
  });

  it("delivers create then rename with oldPath to the iterator and listeners", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    const w = await client.watch("/d", { recursive: true });
    const it = w[Symbol.asyncIterator]();
    srv.emit(K.WATCH_EVENT_KIND_CREATE, "/d/a");
    srv.emit(K.WATCH_EVENT_KIND_RENAME, "/d/sub/b", "/d/a");
    srv.emit(K.WATCH_EVENT_KIND_WRITE, "/other/x");
    srv.emit(K.WATCH_EVENT_KIND_ATTRIB, "/d/sub/b");
    const got = await take(it, 3);
    assert.deepEqual(got.map((e) => [e.kind, e.change, e.path, e.oldPath, e.refId]), [
      ["change", "create", "/d/a", "", w.id],
      ["change", "rename", "/d/sub/b", "/d/a", w.id],
      ["change", "attrib", "/d/sub/b", "", w.id],
    ]);
    assert.equal(got[1].sequence, 2n);
    await until(() => heard.length === 3);
    assert.deepEqual(heard.map((e) => e.change), ["create", "rename", "attrib"]);
    await w.close();
  });

  it("limits a non recursive watch to direct children", async () => {
    const w = await client.watch("/d");
    srv.emit(K.WATCH_EVENT_KIND_CREATE, "/d/deep/x");
    srv.emit(K.WATCH_EVENT_KIND_REMOVE, "/d/y");
    const [ev] = await take(w[Symbol.asyncIterator](), 1);
    assert.equal(ev.change, "remove");
    assert.equal(ev.path, "/d/y");
    await w.close();
  });

  it("keeps two watches apart", async () => {
    const a = await client.watch("/a");
    const b = await client.watch("/b");
    srv.emit(K.WATCH_EVENT_KIND_WRITE, "/b/x");
    srv.emit(K.WATCH_EVENT_KIND_WRITE, "/a/y");
    assert.equal((await take(a[Symbol.asyncIterator](), 1))[0].path, "/a/y");
    assert.equal((await take(b[Symbol.asyncIterator](), 1))[0].path, "/b/x");
    await a.close();
    await b.close();
  });

  it("delivers overflow from an old since and from the server", async () => {
    srv.sequence = 10n;
    const w = await client.watch("/d", { since: 3n });
    const it = w[Symbol.asyncIterator]();
    const first = (await take(it, 1))[0];
    assert.equal(first.kind, "overflow");
    assert.equal(first.refId, w.id);
    srv.notice(srv.watchSocket(w.id)!, { kind: pb.NoticeKind.NOTICE_KIND_OVERFLOW, refId: 0n });
    const second = (await take(it, 1))[0];
    assert.equal(second.kind, "overflow");
    assert.equal(second.refId, 0n);
    await w.close();
  });

  it("a slow consumer loses the oldest events and gets an overflow event", async () => {
    const w = await client.watch("/d");
    for (let i = 0; i < 300; i++) srv.emit(K.WATCH_EVENT_KIND_WRITE, `/d/f${i}`);
    await until(() => srv.sequence === 300n);
    await new Promise((r) => setTimeout(r, 100));
    const it = w[Symbol.asyncIterator]();
    const got: ClientEvent[] = [];
    for (;;) {
      const r = await Promise.race([it.next(), new Promise<null>((x) => setTimeout(() => x(null), 100))]);
      if (!r || r.done) break;
      got.push(r.value);
    }
    assert.ok(got.length <= 256, `queue held ${got.length}`);
    assert.ok(got.some((e) => e.kind === "overflow" && e.refId === w.id));
    assert.equal(got[got.length - 1].path, "/d/f299");
    await w.close();
  });

  it("close sends unwatch on the same connection and ends a pending for await", async () => {
    const w = await client.watch("/d");
    const seen: ClientEvent[] = [];
    const loop = (async () => {
      for await (const ev of w) seen.push(ev);
    })();
    srv.emit(K.WATCH_EVENT_KIND_WRITE, "/d/f");
    await until(() => seen.length === 1);
    await w.close();
    await loop;
    assert.equal(srv.unwatchCount, 1);
    assert.equal(srv.watchSocket(w.id), undefined);
    await w.close();
    assert.equal(srv.unwatchCount, 1);
    assert.equal((await w[Symbol.asyncIterator]().next()).done, true);
  });

  it("unwatch for a watch the connection no longer has is ignored", async () => {
    const w = await client.watch("/d");
    for (const s of srv.sockets()) s.destroy();
    await until(() => srv.conns.size === 0);
    await w.close();
  });

  it("break out of for await closes the watch", async () => {
    const w = await client.watch("/d");
    srv.emit(K.WATCH_EVENT_KIND_CREATE, "/d/one");
    for await (const ev of w) {
      assert.equal(ev.path, "/d/one");
      break;
    }
    await until(() => srv.unwatchCount === 1);
  });

  it("normal calls continue while a watch is open", async () => {
    srv.files.set("/n", Buffer.from("abc"));
    const w = await client.watch("/d");
    const pending = w[Symbol.asyncIterator]().next();
    for (let i = 0; i < 5; i++) assert.equal((await client.stat("/n")).size, 3);
    srv.emit(K.WATCH_EVENT_KIND_WRITE, "/d/z");
    assert.equal((await pending).value.path, "/d/z");
    await w.close();
    assert.equal((await client.stat("/n")).size, 3);
  });

  it("ends the iteration when the owning connection drops", async () => {
    const w = await client.watch("/d");
    const loop = (async () => {
      for await (const ev of w) void ev;
    })();
    srv.watchSocket(w.id)!.destroy();
    await loop;
    await w.close();
  });

  it("client close ends watches and rejects later watch calls", async () => {
    const w = await client.watch("/d");
    const loop = (async () => {
      for await (const ev of w) void ev;
    })();
    client.close();
    await loop;
    await until(() => srv.conns.size === 0);
    await assert.rejects(client.watch("/d"), /client closed/);
  });
});
