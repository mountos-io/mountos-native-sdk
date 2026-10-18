# @mountos-io/native-sdk

Node.js client for the [mountOS](https://mountos.io) native filesystem API. mountOS is a cross-platform, forkable, time-traveling distributed filesystem over object storage (S3, Azure Blob, Google Cloud Storage). Each running mountOS mount serves this API on a local Unix socket. Your program talks to the mount directly, without the kernel and without FUSE. The mount answers from its own caches and resolves a whole path in one step, so metadata calls are faster than the same calls through the mount point.

The same operations exist in Go (`github.com/mountos-io/mountos-native-sdk/go`) and Rust (`mountos-native`). All three are generated from one spec.

## Install

```sh
npm install @mountos-io/native-sdk
```

Node.js 20 or later. A mountOS mount must run on the same machine.

## Quick start

```ts
import { Client, socketPath } from "@mountos-io/native-sdk";

const c = await Client.connect(socketPath("/mnt/vol"));
const info = await c.stat("data/report.csv");
const { entries, nextCursor } = await c.ls("data", 1000, "");
await c.writeFile("data/new.txt", Buffer.from("hello"), { truncate: true });
await c.close();
```

Paths are relative to the mount root.

## What you get

- POSIX file operations: open, read, write, seek, fsync, truncate, stat, mkdir, rmdir, unlink, rename, symlink, readlink, link, chmod, utimens, statfs, access, and directory listing.
- Calls that cost one round trip for many items: `ls`, `statBatch`, `readBatch`, `writeBatch`, `readFile`, `writeFile`, and `removeAll`.
- `watch`: change events for a path on the same connection.
- `warmOpen` and `warmRead`: ask the mount to bring a file into its local cache.
- Request pipelining on one connection, per-call timeouts, and one error code set on every operating system.
- `walk` and `readDirAll` with bounds on entries and depth.

## Access control

The socket is private to the mount owner. The mount reads the identity of each connecting process from the operating system and accepts only the owner and root. Every request runs with that identity, so the mount applies its normal permission checks.

## Links

- Website: <https://mountos.io>
- Source and full documentation: <https://github.com/mountos-io/mountos-native-sdk>
- Apache License 2.0
