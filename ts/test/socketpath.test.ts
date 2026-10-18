import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, symlinkSync } from "node:fs";
import assert from "node:assert/strict";
import { test } from "node:test";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { socketPath } from "../src/index.js";

const posix = process.platform !== "win32";

function withHome<T>(home: string, f: () => T): T {
  const key = "HOME";
  const saved = process.env[key];
  process.env[key] = home;
  try {
    return f();
  } finally {
    if (saved === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = saved;
    }
  }
}

test("socketPath golden names", { skip: !posix }, () => {
  const cases: Record<string, string> = {
    "/mnt/vol": "mountos_api_9bbffa0eba37045f.sock",
    "/Volumes/data one": "mountos_api_87774d7d94a7fe60.sock",
    "/mnt/ü/x": "mountos_api_bc8db74b980a9567.sock",
  };
  for (const [mount, name] of Object.entries(cases)) {
    const got = withHome("/home/u", () => socketPath(mount));
    assert.equal(got, join("/home/u/.mountOS/sockets", name), mount);
  }
});

test("socketPath cleans the home", { skip: !posix }, () => {
  const got = withHome("/home//u/", () => socketPath("/mnt/vol"));
  assert.equal(got, "/home/u/.mountOS/sockets/mountos_api_9bbffa0eba37045f.sock");
});

test("socketPath long home falls back to /tmp", { skip: !posix }, () => {
  const got = withHome("/home/" + "x".repeat(80), () => socketPath("/mnt/vol"));
  assert.equal(got, `/tmp/mountos-${process.getuid!()}/mountos_api_9bbffa0eba37045f.sock`);
});

test("socketPath limit is inclusive at 103 bytes", { skip: !posix }, () => {
  const name = "mountos_api_9bbffa0eba37045f.sock";
  const fit = "/" + "h".repeat(103 - name.length - "/.mountOS/sockets/".length - 1);
  const at = withHome(fit, () => socketPath("/mnt/vol"));
  assert.equal(at.length, 103);
  assert.ok(at.startsWith(fit));
  const over = withHome(fit + "h", () => socketPath("/mnt/vol"));
  assert.ok(over.startsWith("/tmp/mountos-"));
});

test("socketPath resolves symlinks before hashing", { skip: !posix }, () => {
  const root = mkdtempSync(join(tmpdir(), "mos-sock-"));
  try {
    const target = join(root, "target");
    const link = join(root, "link");
    mkdirSync(target);
    symlinkSync(target, link);
    const want = withHome("/home/u", () => socketPath(target));
    assert.equal(withHome("/home/u", () => socketPath(link)), want);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("socketPath keeps the spelling of a missing path", { skip: !posix }, () => {
  const missing = join(tmpdir(), `mos-sock-missing-${process.pid}`, "vol");
  const sum = createHash("sha256").update(missing, "utf8").digest("hex");
  const got = withHome("/home/u", () => socketPath(missing));
  assert.equal(got, join("/home/u/.mountOS/sockets", `mountos_api_${sum.slice(0, 16)}.sock`));
});

test("socketPath rejects an empty mount path", () => {
  assert.throws(() => socketPath(""));
});
