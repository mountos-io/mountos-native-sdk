import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, type ClientEvent, MAX_OP_FAILURES, MountosError, OperationStateUnknownError, TransportError } from "../src/index.js";
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

describe("clone and operations", () => {
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
    srv.capabilities = [];
    srv.opsFull = false;
    srv.cloneError = "";
    srv.cloneOutcome = pb.CloneOutcome.CLONE_OUTCOME_CLONED;
    srv.clones = [];
    srv.cloneDirs = [];
    srv.removeDirs = [];
    srv.cancels = [];
    srv.opRequests = 0;
    srv.eagerOpNotices = [];
    logs = [];
    client = await Client.connect(srv.sockPath, { poolSize: 2, logger });
  });

  it("asks for every capability in Hello", () => {
    for (const c of ["bulk_io", "ls_stat", "remove_all", "watch", "warm", "pipelining", "clone", "ops"]) {
      assert.ok(srv.helloCaps.includes(c), `Hello lacks ${c}`);
    }
  });

  it("clone returns each outcome and sends the options", async () => {
    const outcomes: [pb.CloneOutcome, string][] = [
      [pb.CloneOutcome.CLONE_OUTCOME_CLONED, "cloned"],
      [pb.CloneOutcome.CLONE_OUTCOME_OVERWRITTEN, "overwritten"],
      [pb.CloneOutcome.CLONE_OUTCOME_SKIPPED, "skipped"],
    ];
    for (const [wire, name] of outcomes) {
      srv.cloneOutcome = wire;
      assert.equal(await client.clone("a/src", "b/dst"), name);
    }
    assert.equal(srv.clones[0].options, undefined);
    srv.cloneOutcome = pb.CloneOutcome.CLONE_OUTCOME_OVERWRITTEN;
    await client.clone("a", "b", { overwrite: true, preserve: ["times", "xattr", "acl"] });
    const o = srv.clones[3].options!;
    assert.equal(srv.clones[3].from, "a");
    assert.equal(srv.clones[3].to, "b");
    assert.equal(o.overwrite, true);
    assert.deepEqual(o.preserve, [pb.PreserveKind.PRESERVE_KIND_TIMES, pb.PreserveKind.PRESERVE_KIND_XATTR, pb.PreserveKind.PRESERVE_KIND_ACL]);
  });

  it("clone rejects an unknown preserve kind before sending", async () => {
    await assert.rejects(client.clone("a", "b", { preserve: ["owner" as "acl"] }), RangeError);
    assert.equal(srv.opRequests, 0);
  });

  it("clone errors are MountosError with the server code", async () => {
    for (const code of ["EISDIR", "EEXIST", "EPERM", "EXDEV"]) {
      srv.cloneError = code;
      await assert.rejects(client.clone("a", "b"), (e: unknown) => e instanceof MountosError && e.code === code);
    }
  });

  it("cloneDir waits for the counts and failures", async () => {
    const op = await client.cloneDir("src", "dst", { overwrite: true });
    assert.equal(srv.cloneDirs[0].from, "src");
    assert.equal(srv.cloneDirs[0].options?.overwrite, true);
    srv.opFailed(op.id, "dir/x", "EACCES", "denied");
    srv.opDone(op.id, { cloned: 7n, overwritten: 2n, skipped: 3n, failed: 1n, protected: 4n });
    const r = await op.wait();
    assert.deepEqual(r.counts, { cloned: 7n, overwritten: 2n, skipped: 3n, failed: 1n, removed: 0n, protected: 4n });
    assert.deepEqual(r.failures, [{ path: "dir/x", code: "EACCES", message: "denied" }]);
    assert.deepEqual(await op.wait(), r);
  });

  it("removeDir reports removed and protected counts", async () => {
    const op = await client.removeDir("tree");
    assert.deepEqual(srv.removeDirs, ["tree"]);
    srv.opDone(op.id, { removed: 12n, protected: 1n });
    const r = await op.wait();
    assert.equal(r.counts.removed, 12n);
    assert.equal(r.counts.protected, 1n);
    assert.deepEqual(r.failures, []);
  });

  it("keeps notices that follow the start reply at once", async () => {
    srv.eagerOpNotices = [
      { kind: NK.NOTICE_KIND_OP_FAILED, path: "p", code: "EIO", message: "m" },
      { kind: NK.NOTICE_KIND_OP_DONE, failed: 1n, cloned: 5n },
    ];
    for (let i = 0; i < 30; i++) {
      const op = await client.cloneDir("a", "b");
      const r = await op.wait();
      assert.equal(r.counts.cloned, 5n);
      assert.equal(r.failures.length, 1);
      assert.equal(r.failures[0].code, "EIO");
    }
    assert.deepEqual(logs, []);
  });

  it("caps the failures it keeps and reports the server total", async () => {
    const op = await client.removeDir("tree");
    const total = MAX_OP_FAILURES + 36;
    for (let i = 0; i < total; i++) srv.opFailed(op.id, `f${i}`, "EBUSY", "busy");
    srv.opDone(op.id, { failed: BigInt(total) });
    const r = await op.wait();
    assert.equal(r.failures.length, MAX_OP_FAILURES);
    assert.equal(r.failures[0].path, "f0");
    assert.equal(r.failures[MAX_OP_FAILURES - 1].path, `f${MAX_OP_FAILURES - 1}`);
    assert.equal(r.counts.failed, BigInt(total));
  });

  describe("overflow", () => {
    it("a connection level overflow makes the state unknown", async () => {
      const op = await client.removeDir("tree");
      const p = op.wait();
      srv.notice(srv.opSocket(op.id), { kind: NK.NOTICE_KIND_OVERFLOW });
      await assert.rejects(p, OperationStateUnknownError);
      await assert.rejects(op.wait(), OperationStateUnknownError);
    });

    it("an overflow that names one operation leaves the others alone", async () => {
      client.close();
      await until(() => srv.conns.size === 0);
      client = await Client.connect(srv.sockPath, { poolSize: 1, logger });
      const a = await client.removeDir("a");
      const b = await client.removeDir("b");
      srv.notice(srv.opSocket(a.id), { kind: NK.NOTICE_KIND_OVERFLOW, refId: a.id });
      await assert.rejects(a.wait(), OperationStateUnknownError);
      srv.opDone(b.id, { removed: 1n });
      assert.equal((await b.wait()).counts.removed, 1n);
    });

    for (const [name, eager] of [
      ["connection level", { kind: NK.NOTICE_KIND_OVERFLOW, refId: 0n }],
      ["naming the operation", { kind: NK.NOTICE_KIND_OVERFLOW }],
    ] as const) {
      it(`an overflow ${name} right after the start reply is not lost`, async () => {
        srv.eagerOpNotices = [eager];
        for (let i = 0; i < 30; i++) {
          const op = await client.removeDir("tree");
          await assert.rejects(op.wait(), OperationStateUnknownError);
        }
      });
    }

    it("does not touch an operation on another connection", async () => {
      const ops = [];
      for (let i = 0; i < 8; i++) {
        ops.push(await client.removeDir("tree"));
        if (srv.opSocket(ops[i].id) !== srv.opSocket(ops[0].id)) break;
      }
      const first = ops[0];
      const last = ops[ops.length - 1];
      if (srv.opSocket(first.id) === srv.opSocket(last.id)) return;
      srv.notice(srv.opSocket(first.id), { kind: NK.NOTICE_KIND_OVERFLOW });
      await assert.rejects(first.wait(), OperationStateUnknownError);
      srv.opDone(last.id, { removed: 9n });
      assert.equal((await last.wait()).counts.removed, 9n);
    });

    it("a change overflow of a watch never touches an operation", async () => {
      const op = await client.removeDir("tree");
      srv.notice(srv.opSocket(op.id), {
        kind: NK.NOTICE_KIND_CHANGE, refId: 0n, changeKind: pb.WatchEventKind.WATCH_EVENT_KIND_OVERFLOW,
      });
      await new Promise((r) => setTimeout(r, 50));
      assert.equal(op.settled, false);
      srv.opDone(op.id, { removed: 2n });
      assert.equal((await op.wait()).counts.removed, 2n);
    });

    it("an ended operation keeps its result", async () => {
      const op = await client.removeDir("tree");
      srv.opDone(op.id, { removed: 4n });
      await op.wait();
      srv.notice(srv.opSocket(op.id), { kind: NK.NOTICE_KIND_OVERFLOW });
      await new Promise((r) => setTimeout(r, 30));
      assert.equal((await op.wait()).counts.removed, 4n);
    });
  });

  it("cancel goes to the owning connection and the op still ends with partial counts", async () => {
    const ops = [];
    for (let i = 0; i < 6; i++) ops.push(await client.cloneDir("a", "b"));
    for (const op of ops) await op.cancel();
    assert.deepEqual(srv.cancels, ops.map((o) => o.id));
    const op = ops[0];
    srv.opDone(op.id, { cloned: 2n, skipped: 1n });
    assert.equal((await op.wait()).counts.cloned, 2n);
    await assert.rejects(op.cancel(), (e: unknown) => e instanceof MountosError && e.code === "ENOENT");
  });

  it("wait rejects with the abort reason and can be called again", async () => {
    const op = await client.removeDir("tree");
    const ac = new AbortController();
    const p = op.wait({ signal: ac.signal });
    ac.abort(new Error("stop"));
    await assert.rejects(p, /stop/);
    await assert.rejects(op.wait({ signal: AbortSignal.abort(new Error("early")) }), /early/);
    srv.opDone(op.id, { removed: 1n });
    assert.equal((await op.wait()).counts.removed, 1n);
  });

  it("a lost connection fails every waiter of its operations", async () => {
    const op = await client.removeDir("tree");
    const waits = [op.wait(), op.wait()];
    srv.opSocket(op.id).destroy();
    for (const w of waits) await assert.rejects(w, TransportError);
    await assert.rejects(op.wait(), TransportError);
  });

  it("closing the client fails pending waiters", async () => {
    const op = await client.removeDir("tree");
    const w = op.wait();
    client.close();
    await assert.rejects(w, (e: unknown) => e instanceof TransportError && /client closed/.test(e.message));
  });

  it("the operation limit surfaces as EAGAIN and leaves nothing behind", async () => {
    srv.opsFull = true;
    await assert.rejects(client.cloneDir("a", "b"), (e: unknown) => e instanceof MountosError && e.code === "EAGAIN");
    await assert.rejects(client.removeDir("a"), (e: unknown) => e instanceof MountosError && e.code === "EAGAIN");
  });

  it("fails locally with UNSUPPORTED_OPERATION when the server lacks the capability", async () => {
    client.close();
    await until(() => srv.conns.size === 0);
    srv.capabilities = ["bulk_io", "warm"];
    client = await Client.connect(srv.sockPath, { poolSize: 1, logger });
    const unsupported = (e: unknown): boolean => e instanceof MountosError && e.code === "UNSUPPORTED_OPERATION";
    await assert.rejects(client.clone("a", "b"), unsupported);
    await assert.rejects(client.cloneDir("a", "b"), unsupported);
    await assert.rejects(client.removeDir("a"), unsupported);
    assert.equal(srv.opRequests, 0);
  });

  it("removeAll still works", async () => {
    srv.capabilities = [];
    srv.files.set("/t/a", Buffer.from("x"));
    srv.files.set("/t/b", Buffer.from("y"));
    assert.equal(await client.removeAll("/t"), 2);
  });

  it("delivers opFailed and opDone to listeners without logging", async () => {
    const heard: ClientEvent[] = [];
    client.addListener((e) => heard.push(e));
    const op = await client.removeDir("tree");
    srv.opFailed(op.id, "x", "EIO", "bad");
    srv.opDone(op.id, { removed: 2n, failed: 1n });
    await until(() => heard.length === 2);
    assert.equal(heard[0].kind, "opFailed");
    assert.equal(heard[0].path, "x");
    assert.equal((heard[0].error as MountosError).code, "EIO");
    assert.equal(heard[1].kind, "opDone");
    assert.equal(heard[1].counts?.removed, 2n);
    assert.deepEqual(logs, []);
  });

  it("does not log operation notices when no listener is registered", async () => {
    const op = await client.removeDir("tree");
    srv.opFailed(op.id, "x", "EIO", "bad");
    srv.opDone(op.id, { failed: 1n });
    await op.wait();
    assert.deepEqual(logs, []);
  });

  it("ended and lost operations leave no record", async () => {
    client.close();
    await until(() => srv.conns.size === 0);
    client = await Client.connect(srv.sockPath, { poolSize: 1, logger });
    const pending = (): number => {
      const ops = (client as unknown as { ops: Map<unknown, Map<unknown, unknown>> }).ops;
      return [...ops.values()].reduce((n, m) => n + m.size, 0);
    };
    const a = await client.removeDir("a");
    const b = await client.removeDir("b");
    const d = await client.removeDir("d");
    assert.equal(pending(), 3);
    srv.opDone(a.id, { removed: 1n });
    await a.wait();
    assert.equal(pending(), 2);
    srv.notice(srv.opSocket(b.id), { kind: NK.NOTICE_KIND_OVERFLOW, refId: b.id });
    await assert.rejects(b.wait(), OperationStateUnknownError);
    assert.equal(pending(), 1);
    srv.opSocket(d.id).destroy();
    await assert.rejects(d.wait(), TransportError);
    assert.equal(pending(), 0);
  });
});
