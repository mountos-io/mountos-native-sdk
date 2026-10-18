# mountos-native

Rust client for the [mountOS](https://mountos.io) native filesystem API. mountOS is a cross-platform, forkable, time-traveling distributed filesystem over object storage (S3, Azure Blob, Google Cloud Storage). Each running mountOS mount serves this API on a local Unix socket. Your program talks to the mount directly, without the kernel and without FUSE.

The same operations exist in Go (`github.com/mountos-io/mountos-native-sdk/go`) and TypeScript (`@mountos-io/native-sdk`). All three are generated from one spec.

## Install

```sh
cargo add mountos-native
```

`protoc` is not needed to build the crate. The crate builds on Unix systems.

## Quick start

```rust
let sock = mountos_native::socket_path("/mnt/vol")?;
let c = mountos_native::Client::connect(sock)?;
let info = c.stat("data/report.csv")?;
```

Paths are relative to the mount root.

## What you get

- POSIX file operations: open, read, write, seek, fsync, truncate, stat, mkdir, rmdir, unlink, rename, symlink, readlink, link, chmod, utimens, statfs, access, and directory listing.
- Calls that cost one round trip for many items: `ls`, `stat_batch`, `read_file`, `write_file`, `remove_all`, and `File::read_batch` and `File::write_batch`.
- `watch` for change events, and `warm_open` and `warm_read` to fill the mount cache.
- Wire types in `mountos_native::wire` are `#[non_exhaustive]`, so code outside the crate cannot build them by struct literal. The client methods take plain arguments.
- Request pipelining on a connection pool, per-call timeouts, and one error code set on every operating system.

## Links

- Website: <https://mountos.io>
- Source and full documentation: <https://github.com/mountos-io/mountos-native-sdk>
- Apache License 2.0
