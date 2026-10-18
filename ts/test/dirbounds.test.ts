import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, FileType, TooManyEntriesError } from "../src/index.js";
import type { DirEntry } from "../src/index.js";
import { FakeServer } from "./fake_server.js";

const quiet = { warn: (): void => {} };

function entry(name: string, dir: boolean): DirEntry {
  return { name, type: dir ? FileType.Directory : FileType.Regular };
}

type LsResult = { entries: DirEntry[]; nextCursor: string };

describe("directory helper bounds", () => {
  const srv = new FakeServer();
  let client: Client | undefined;

  before(() => srv.listen());
  after(() => {
    client?.close();
    return srv.close();
  });
  beforeEach(() => {
    client?.close();
    client = undefined;
    srv.dirs.clear();
  });

  it("readDirAll stops at the limit instead of growing", async () => {
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet }));
    let calls = 0;
    // An endless directory: every page has 100 entries and a next cursor.
    c.ls = (): Promise<LsResult> => {
      calls++;
      return Promise.resolve({ entries: Array.from({ length: 100 }, (_, i) => entry(`n${i}`, false)), nextCursor: "more" });
    };
    await assert.rejects(c.readDirAll("/huge", { maxEntries: 250 }), (e: unknown) => {
      return e instanceof TooManyEntriesError && e.code === "TOO_MANY_ENTRIES" && e.limit === 250 && /250/.test(e.message);
    });
    assert.equal(calls, 3);
  });

  it("readDirAll lists a normal directory and honors an exact limit", async () => {
    srv.dirs.set("/d", Array.from({ length: 40 }, (_, i) => `n${i}`));
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet }));
    assert.equal((await c.readDirAll("/d")).length, 40);
    assert.equal((await c.readDirAll("/d", { maxEntries: 40 })).length, 40);
    await assert.rejects(c.readDirAll("/d", { maxEntries: 39 }), TooManyEntriesError);
  });

  it("walk stops at the depth limit", async () => {
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet }));
    // Every directory holds one subdirectory, so the tree never ends.
    c.ls = (): Promise<LsResult> => Promise.resolve({ entries: [entry("d", true)], nextCursor: "" });
    const seen: string[] = [];
    await assert.rejects(
      (async () => {
        for await (const e of c.walk("/", { maxDepth: 3 })) seen.push(e.path);
      })(),
      (e: unknown) => e instanceof TooManyEntriesError && e.code === "WALK_TOO_DEEP" && e.limit === 3,
    );
    assert.equal(seen.length, 4);
  });

  it("walk holds one page of a flat directory", async () => {
    const c = (client = await Client.connect(srv.sockPath, { poolSize: 1, logger: quiet }));
    let calls = 0;
    let biggest = 0;
    c.ls = (_p: string, _m = 0, cursor = ""): Promise<LsResult> => {
      calls++;
      const from = cursor === "" ? 0 : Number(cursor);
      const end = Math.min(from + 100, 450);
      const entries = Array.from({ length: end - from }, (_, i) => entry(`n${from + i}`, false));
      biggest = Math.max(biggest, entries.length);
      return Promise.resolve({ entries, nextCursor: end < 450 ? String(end) : "" });
    };
    let count = 0;
    for await (const e of c.walk("/f", { maxDepth: 1 })) {
      void e;
      count++;
    }
    assert.equal(count, 450);
    assert.equal(biggest, 100);
    assert.equal(calls, 5);
  });
});
