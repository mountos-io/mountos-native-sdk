import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { Client, ServerRestartedError } from "../src/index.js";
import * as pb from "../src/gen/mountos_fs.js";
import { FakeServer } from "./fake_server.js";

async function until(cond: () => boolean, ms = 2000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe("warm results across server instances", () => {
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
    srv.files.set("/f", Buffer.from("x"));
    srv.warmHold = false;
    srv.instanceId = 11n;
    client = await Client.connect(srv.sockPath, { poolSize: 2, logger: { warn: (): void => {} } });
  });

  const restart = async (instanceId: bigint): Promise<void> => {
    srv.instanceId = instanceId;
    for (const s of srv.sockets()) s.destroy();
    await until(() => srv.conns.size === 0);
    await client.stat("/f");
  };

  it("does not accept a finished warm of the replaced server", async () => {
    let warmed = 0;
    client.addListener((e) => { if (e.kind === "warmDone") warmed++; });
    srv.notice(srv.sockets()[0], { kind: pb.NoticeKind.NOTICE_KIND_WARM_DONE, refId: 1n });
    await until(() => warmed === 1);
    await client.waitWarm(1);

    await restart(22n);
    await assert.rejects(
      client.waitWarm(1, { signal: AbortSignal.timeout(100) }),
      (e: unknown) => (e as Error).name === "TimeoutError",
    );
    srv.notice(srv.sockets()[0], { kind: pb.NoticeKind.NOTICE_KIND_WARM_DONE, refId: 1n });
    await client.waitWarm(1);
  });

  it("fails a pending waiter of the replaced server", async () => {
    srv.warmHold = true;
    const id = await client.warmOpen("/f");
    const rejected = assert.rejects(client.waitWarm(id), ServerRestartedError);
    await restart(22n);
    await rejected;
  });

  it("keeps finished warms when the server reports no instance", async () => {
    srv.instanceId = 0n;
    client.close();
    await until(() => srv.conns.size === 0);
    client = await Client.connect(srv.sockPath, { poolSize: 2, logger: { warn: (): void => {} } });
    const seen: unknown[] = [];
    client.addListener((e) => seen.push(e));
    srv.notice(srv.sockets()[0], { kind: pb.NoticeKind.NOTICE_KIND_WARM_DONE, refId: 1n });
    await until(() => seen.length === 1);
    await restart(0n);
    await client.waitWarm(1);
  });
});
