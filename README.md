# mountOS Native SDK

Clients for the mountOS native filesystem API. Each running mountOS mount serves this API on a local Unix socket. A program talks to the mount directly, without the kernel and without FUSE. The mount answers from its own caches and handles a whole path in one step, so metadata calls are faster than the same calls through the mount point.

The SDK has the same operations in Go, TypeScript, and Rust. All three are generated from one spec, so a call has the same name, arguments, and errors in every language.

## SDKs

| Language | Path | Package |
| --- | --- | --- |
| Go | `go/` | `github.com/mountos-io/mountos-native-sdk/go` |
| TypeScript | `ts/` | `@mountos-io/native-sdk` (Node.js 20 or later) |
| Rust | `rust/` | `mountos-native` |

## What you get

- The POSIX file operations: open, read, write, seek, fsync, truncate, stat, lstat, mkdir, rmdir, unlink, rename, symlink, readlink, link, chmod, utimens, statfs, access, and directory listing. The SDK manages file handles for you.
- Calls that cost one round trip for many items: `Ls` (a listing with attributes), `StatBatch`, `ReadBatch`, `WriteBatch`, `ReadFile`, `WriteFile`, and `RemoveAll`.
- `Watch`: change events for a path, delivered on the same connection.
- `WarmOpen` and `WarmRead`: ask the mount to bring a file into its local cache. The call returns an id at once. The result arrives later as an event.
- The same error code on every operating system. Each error also has a message that names the operation, the path, and the path component that failed.

Snapshot, fork, and version operations are not part of this API. The mount decides every failure, for example EROFS on a read-only mount. The SDK does not check anything first.

## Find the socket

A mount serves one socket. Its path comes from the mount path:

```
<home>/.mountOS/sockets/mountos_api_<hash>.sock
```

`<hash>` is the first 8 bytes of the SHA-256 of the mount path, as 16 hex characters. The mount path is the path the mount was started with. When this path is longer than the Unix socket limit, the socket is in `/tmp/mountos-<uid>/` instead. The directory is private to the user who owns the mount.

## Quick start

Go:

```go
c, err := client.Dial(socketPath, client.Options{})
if err != nil {
    log.Fatal(err)
}
defer c.Close()

info, err := c.Stat("data/report.csv")
entries, next, err := c.Ls("data", 1000, "")
err = c.WriteFile("data/new.txt", []byte("hello"), true)
```

TypeScript:

```ts
import { Client } from "@mountos-io/native-sdk";

const c = await Client.connect(socketPath);
const info = await c.stat("data/report.csv");
await c.close();
```

Rust:

```rust
let c = mountos_native::Client::connect(socket_path)?;
let info = c.stat("data/report.csv")?;
```

Paths are relative to the mount root. A leading slash is accepted and ignored.

## Events

A client delivers every event to all registered listeners. This covers change events, warm results, and mount notices such as a mount that is closing. If no listener takes an event, the SDK writes it to the logger you passed in the options. Without a logger it writes to standard error, or to the console in TypeScript. A warm result has the warm id, an error code, and a message. You never poll for it.

```go
remove := c.AddListener(func(e client.Event) { log.Println(e.Kind, e.Path, e.Err) })
defer remove()

id, err := c.WarmOpen("data/big.bin")
err = c.WaitWarm(ctx, id)
```

Warming needs the local cache. A mount started with `--disable-cache-dir` refuses `WarmOpen` and `WarmRead` at once with `UNSUPPORTED_OPERATION`.

## Access control

The socket is private to the mount owner. The mount reads the user id of each connecting process from the kernel and accepts only the owner and root. Every request runs with the identity of that process, so the mount applies its normal permission checks. A connection limit and limits on open handles and watches protect the mount. When a limit is reached the call fails with `EAGAIN`.

## Versioning

The wire contract is in `api/filesystem.yaml`. The public call layer is in `api/client.yaml`. `api/contract-snapshot.json` records the released contract. A new version can add calls and fields. It cannot remove or change an existing one, and the build fails if a change would break an older client.

## Development

The SDK needs Go, Node.js 20 or later, Rust, and `protoc`.

```sh
make api-gen     # regenerate the proto, wire types, and client stubs for every language
make api-check   # validate the specs, check compatibility, fail on stale generated files
make api-update  # write the contract snapshot after a compatible change
make test        # run the generator tests and every SDK test suite
```

Never edit a generated file. Change the specs and run `make api-gen`. Generated files are `go/wire/*`, `go/client/ops_gen.go`, `ts/src/gen/*`, `ts/src/ops_gen.ts`, `rust/src/ops_gen.rs`, and `api/proto/mountos_fs.proto`.

## Platforms

The mount serves the API on macOS and Linux. Windows support uses the same protocol over AF_UNIX sockets and is not yet tested. The Go and TypeScript SDKs have no Unix-only code. The Rust SDK uses Unix sockets and builds on Unix systems only.

## License

Apache License 2.0. See [LICENSE](LICENSE).
