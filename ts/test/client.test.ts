import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, FileType, MountosError, OpenFlag, ProtocolError, TransportError } from "../src/index.js";
import { FrameReader } from "../src/frame.js";
import { FakeServer } from "./fake_server.js";

const RW = OpenFlag.ReadWrite | OpenFlag.Create;

describe("client", () => {
  const srv = new FakeServer();
  let client: Client;

  before(() => srv.listen());
  after(() => srv.close());
  beforeEach(async () => {
    client?.close();
    srv.files.clear();
    srv.symlinks.clear();
    srv.dirs.clear();
    srv.times.clear();
    srv.readDelayMs = 0;
    srv.writeSizes = [];
    srv.maxActive = 0;
    srv.helloCount = 0;
    client = await Client.connect(srv.sockPath, { poolSize: 2 });
  });

  it("handshakes every pooled connection", () => {
    assert.equal(srv.helloCount, 2);
    assert.equal(client.server.version, 1);
    assert.equal(client.maxPayload, 1000);
  });

  it("pings", async () => {
    await client.ping();
  });

  it("stats a file", async () => {
    srv.files.set("/a", Buffer.from("hello"));
    const info = await client.stat("/a");
    assert.equal(info.size, 5);
    assert.equal(info.name, "/a");
    assert.equal(info.inode, 42n);
  });

  it("reads zeros in a sparse hole", async () => {
    const f = await client.open("/s", RW);
    assert.equal(await f.write(10, Buffer.from("abcd")), 4);
    const got = await f.read(0, 14);
    assert.deepEqual(got, Buffer.concat([Buffer.alloc(10), Buffer.from("abcd")]));
    await f.close();
  });

  it("follows EOF semantics", async () => {
    srv.files.set("/e", Buffer.from("12345"));
    const f = await client.open("/e");
    assert.equal((await f.read(3, 100)).toString(), "45");
    assert.equal((await f.read(5, 10)).length, 0);
    assert.equal((await f.read(99, 10)).length, 0);
    await f.close();
  });

  it("splits a write over the payload limit", async () => {
    const data = Buffer.alloc(2500, 7);
    const f = await client.open("/big", RW);
    assert.equal(await f.write(0, data), 2500);
    assert.deepEqual(srv.writeSizes, [1000, 1000, 500]);
    assert.deepEqual(srv.files.get("/big"), data);
    assert.deepEqual(await f.read(0, 2500), data);
    await f.close();
  });

  it("maps server errors to MountosError.code", async () => {
    await assert.rejects(client.stat("/missing"), (e) => e instanceof MountosError && e.code === "ENOENT");
    srv.files.set("/x", Buffer.alloc(0));
    await assert.rejects(client.open("/x", RW | OpenFlag.Excl), (e) => (e as MountosError).code === "EEXIST");
    await client.ping();
  });

  it("rejects access mode 3 before sending", async () => {
    await assert.rejects(client.open("/x", 3), (e) => (e as MountosError).code === "EINVAL");
  });

  it("isolates handles per connection", async () => {
    const a = await client.open("/a", RW);
    const b = await client.open("/b", RW);
    assert.equal(a.id, b.id);
    await a.write(0, Buffer.from("AAA"));
    await b.write(0, Buffer.from("BBBB"));
    assert.equal((await a.read(0, 10)).toString(), "AAA");
    assert.equal((await b.read(0, 10)).toString(), "BBBB");
    await a.close();
    assert.equal((await b.read(0, 10)).toString(), "BBBB");
    await b.close();
  });

  it("runs reads on two handles in parallel without corruption", async () => {
    srv.files.set("/p1", Buffer.alloc(3000, 1));
    srv.files.set("/p2", Buffer.alloc(3000, 2));
    srv.readDelayMs = 20;
    const [a, b] = await Promise.all([client.open("/p1"), client.open("/p2")]);
    const [ra, rb] = await Promise.all([a.read(0, 3000), b.read(0, 3000)]);
    assert.deepEqual(ra, Buffer.alloc(3000, 1));
    assert.deepEqual(rb, Buffer.alloc(3000, 2));
    assert.equal(srv.maxActive, 2);
    await Promise.all([a.close(), b.close()]);
  });

  it("serializes calls on one handle", async () => {
    srv.files.set("/q", Buffer.alloc(100, 9));
    srv.readDelayMs = 5;
    const f = await client.open("/q");
    await Promise.all([f.read(0, 10), f.read(10, 10), f.read(20, 10)]);
    assert.equal(srv.maxActive, 1);
    await f.close();
  });

  it("fails on double close with EBADF", async () => {
    const f = await client.open("/d", RW);
    await f.close();
    await assert.rejects(f.close(), (e) => (e as MountosError).code === "EBADF");
  });

  it("lstat does not follow a symlink", async () => {
    srv.files.set("/t", Buffer.from("abc"));
    await client.symlink("/t", "/l");
    const info = await client.lstat("/l");
    assert.equal(info.type, FileType.Symlink);
    assert.equal((await client.lstat("/t")).type, FileType.Regular);
    await assert.rejects(client.lstat("/none"), (e) => (e as MountosError).code === "ENOENT");
  });

  it("reads a link target after symlink", async () => {
    await client.symlink("../target", "/ln");
    assert.equal(await client.readlink("/ln"), "../target");
    await assert.rejects(client.symlink("x", "/ln"), (e) => (e as MountosError).code === "EEXIST");
    await assert.rejects(client.readlink("/plain"), (e) => (e as MountosError).code === "EINVAL");
  });

  it("creates a hard link", async () => {
    srv.files.set("/orig", Buffer.from("data"));
    await client.link("/orig", "/alias");
    assert.equal((await client.stat("/alias")).size, 4);
    await assert.rejects(client.link("/nope", "/x"), (e) => (e as MountosError).code === "ENOENT");
  });

  it("utimens sends only the given times", async () => {
    const big = 1700000000123456789n;
    await client.utimens("/both", 5n, big);
    assert.deepEqual(srv.times.get("/both"), { atime: 5n, mtime: big });
    await client.utimens("/mtime", undefined, -3n);
    assert.deepEqual(srv.times.get("/mtime"), { mtime: -3n });
    await client.utimens("/atime", 9n);
    assert.deepEqual(srv.times.get("/atime"), { atime: 9n });
    await client.utimens("/none");
    assert.deepEqual(srv.times.get("/none"), {});
    await client.utimens("/zero", 0n, 0n);
    assert.deepEqual(srv.times.get("/zero"), { atime: 0n, mtime: 0n });
  });

  it("returns statfs", async () => {
    const st = await client.statfs("/");
    assert.deepEqual(st, {
      blockSize: 4096, blocks: 1n << 40n, blocksFree: 1000n, blocksAvail: 900n,
      files: 5000n, filesFree: 4000n, nameMax: 255,
    });
  });

  it("pages a directory with opendir and readdir", async () => {
    srv.dirs.set("/d", ["a", "b", "c", "d", "e"]);
    const d = await client.opendir("/d");
    const p1 = await d.readdir(2);
    assert.deepEqual(p1.entries.map((e) => e.name), ["a", "b"]);
    assert.equal(p1.eof, false);
    const p2 = await d.readdir(2);
    assert.deepEqual(p2.entries.map((e) => e.name), ["c", "d"]);
    assert.equal(p2.eof, false);
    const p3 = await d.readdir(2);
    assert.deepEqual(p3.entries.map((e) => e.name), ["e"]);
    assert.equal(p3.eof, true);
    await d.close();
    await assert.rejects(d.readdir(1), (e) => (e as MountosError).code === "EBADF");
    await assert.rejects(d.close(), (e) => (e as MountosError).code === "EBADF");
    await assert.rejects(client.opendir("/missing"), (e) => (e as MountosError).code === "ENOENT");
  });

  it("pages ls with a cursor", async () => {
    srv.dirs.set("/l", ["x", "y", "z"]);
    const p1 = await client.ls("/l", 2);
    assert.deepEqual(p1.entries.map((e) => e.name), ["x", "y"]);
    assert.equal(p1.entries[0].info?.inode, 7n);
    assert.notEqual(p1.nextCursor, "");
    const p2 = await client.ls("/l", 2, p1.nextCursor);
    assert.deepEqual(p2.entries.map((e) => e.name), ["z"]);
    assert.equal(p2.nextCursor, "");
  });

  it("removeAll counts removed entries", async () => {
    srv.files.set("/t/a", Buffer.alloc(1));
    srv.files.set("/t/b/c", Buffer.alloc(1));
    srv.files.set("/tx", Buffer.alloc(1));
    srv.dirs.set("/t", []);
    assert.equal(await client.removeAll("/t"), 3);
    assert.ok(srv.files.has("/tx"));
    await assert.rejects(client.removeAll("/t"), (e) => (e as MountosError).code === "ENOENT");
  });

  it("fails calls after the client closes", async () => {
    client.close();
    await assert.rejects(client.ping(), (e) => e instanceof TransportError);
  });
});

describe("frame reader", () => {
  it("rejects bad magic", () => {
    assert.throws(() => new FrameReader().push(Buffer.alloc(24)), ProtocolError);
  });

  it("reassembles a frame split across chunks", () => {
    const h = Buffer.alloc(24);
    h.write("MFS1", 0, "ascii");
    h[4] = 1;
    h[5] = 1;
    h.writeUInt16BE(2, 6);
    h.writeBigUInt64BE(7n, 8);
    h.writeUInt32BE(1, 16);
    h.writeUInt32BE(2, 20);
    const all = Buffer.concat([h, Buffer.from([9]), Buffer.from([1, 2])]);
    const r = new FrameReader();
    let out = r.push(all.subarray(0, 10));
    for (let i = 10; i < all.length; i++) out = out.concat(r.push(all.subarray(i, i + 1)));
    assert.equal(out.length, 1);
    assert.equal(out[0].requestId, 7);
    assert.deepEqual(out[0].payload, Buffer.from([1, 2]));
  });
});
