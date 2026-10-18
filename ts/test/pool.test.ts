import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, DeadlineExceededError, MountosError, OpenFlag } from "../src/index.js";
import { FakeServer } from "./fake_server.js";

const quiet = { warn: (): void => {} };

async function until(cond: () => boolean, ms = 2000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await new Promise((r) => setTimeout(r, 5));
  }
}

function gate(): { wait: Promise<void>; open: () => void } {
  let open!: () => void;
  const wait = new Promise<void>((r) => (open = r));
  return { wait, open };
}

describe("pool", () => {
  const srv = new FakeServer();
  let client: Client | undefined;

  before(() => srv.listen());
  after(() => {
    client?.close();
    return srv.close();
  });
  beforeEach(async () => {
    client?.close();
    client = undefined;
    await until(() => srv.conns.size === 0);
    srv.beforeReply = undefined;
    srv.capabilities = [];
    srv.files.clear();
    srv.dirs.clear();
  });

  it("serves a waiting call on the first connection that frees up", async () => {
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 2, logger: quiet }));
    const gates = [gate(), gate()];
    let seen = 0;
    srv.beforeReply = async (_s, m) => {
      if (m === 2 && seen < 2) await gates[seen++].wait;
    };
    const first = c.ping();
    const second = c.ping();
    await until(() => seen === 2);
    let third = false;
    const z = c.ping().then(() => (third = true));
    gates[1].open();
    await second;
    await z;
    assert.ok(third);
    gates[0].open();
    await first;
  });

  it("fails a call that exceeds callTimeout and redials the connection", async () => {
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 1, callTimeout: 100, logger: quiet }));
    const hold = gate();
    let held = false;
    srv.beforeReply = async (_s, m) => {
      if (m === 2 && !held) {
        held = true;
        await hold.wait;
      }
    };
    const hellos = srv.helloCount;
    await assert.rejects(c.ping(), (e: unknown) => {
      assert.ok(e instanceof DeadlineExceededError);
      assert.equal(e.code, "DEADLINE_EXCEEDED");
      return true;
    });
    await c.ping();
    assert.equal(srv.helloCount, hellos + 1);
    hold.open();
  });

  it("fails an oversize request without dropping the connection", async () => {
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet }));
    const hellos = srv.helloCount;
    await assert.rejects(c.stat("x".repeat(17 * 1024 * 1024)));
    await c.ping();
    assert.equal(srv.helloCount, hellos);
  });

  it("refuses calls the server did not offer", async () => {
    srv.capabilities = ["ls_stat"];
    srv.dirs.set("/d", ["a"]);
    srv.files.set("/f", Buffer.from("x"));
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet }));
    const unsupported = (e: unknown): boolean => e instanceof MountosError && e.code === "UNSUPPORTED_OPERATION";
    await assert.rejects(c.watch("/d"), unsupported);
    await assert.rejects(c.statBatch(["/f"]), unsupported);
    await assert.rejects(c.warmOpen("/f"), unsupported);
    await assert.rejects(c.readFile("/f"), unsupported);
    const f = await c.open("/f", OpenFlag.ReadWrite);
    await assert.rejects(f.readBatch([{ offset: 0, length: 1 }]), unsupported);
    await assert.rejects(f.writeBatch([{ offset: 0, data: Buffer.from("y") }]), unsupported);
    await f.close();
    assert.equal((await c.ls("/d")).entries.length, 1);
  });

  it("walks a tree depth first", async () => {
    srv.dirs.set("/r", ["a", "b", "c"]);
    srv.dirs.set("/r/b", ["x", "y"]);
    srv.dirs.set("/r/b/y", ["z"]);
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 2, logger: quiet }));
    const seen: string[] = [];
    for await (const e of c.walk("/r")) seen.push(e.path);
    assert.deepEqual(seen, ["/r/a", "/r/b", "/r/b/x", "/r/b/y", "/r/b/y/z", "/r/c"]);
    assert.equal((await c.readDirAll("/r")).length, 3);
    const first: string[] = [];
    for await (const e of c.walk("/r")) {
      first.push(e.path);
      break;
    }
    assert.deepEqual(first, ["/r/a"]);
    await assert.rejects(async () => {
      for await (const _ of c.walk("/missing")) void _;
    }, (e: unknown) => e instanceof MountosError && e.code === "ENOENT");
  });
});
