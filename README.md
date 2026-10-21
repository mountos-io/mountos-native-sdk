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
- `Clone`, `CloneDir`, and `RemoveDir`: copy a file, copy a tree, or remove a tree inside the mount. A clone moves no data. See [Clone and remove trees](#clone-and-remove-trees).
- The same error code on every operating system. Each error also has a message that names the operation, the path, and the path component that failed.

Snapshot, fork, and version operations are not part of this API. The mount decides every failure, for example EROFS on a read-only mount. The SDK does not check anything first.

## Find the socket

A mount serves one socket. Its path comes from the mount path:

```
<home>/.mountOS/sockets/mountos_api_<hash>.sock
```

`<hash>` is the first 8 bytes of the SHA-256 of the mount path, as 16 hex characters. The mount path is the path the mount was started with, resolved through symlinks before hashing. When this path is longer than the Unix socket limit, the socket is in `/tmp/mountos-<uid>/` instead. The directory is private to the user who owns the mount.

Each SDK computes this path for you. The call only computes the path. It does not create or check anything.

```go
sock, err := client.SocketPath("/mnt/vol")   // Go
```

```ts
const sock = socketPath("/mnt/vol");          // TypeScript, exported from the package
```

```rust
let sock = mountos_native::socket_path("/mnt/vol")?;   // Rust
```

Pass the mount path exactly as the mount was started. Paths that differ only by symlinks give the same socket. Other spellings of the same folder give a different socket. The home directory comes from `$HOME`, then the account database. Rust reads `/etc/passwd` for the account entry.

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

## Clone and remove trees

These calls work inside one mount. Paths are relative to the mount root, as in the other path calls. A clone moves no data.

| Operation | Id | Capability | What it does |
| --- | --- | --- | --- |
| `Clone` | 56 | `clone` | Copies one entry that is not a directory (file, symlink, or special file). It replies when done with `cloned`, `overwritten`, or `skipped`. |
| `CloneDir` | 57 | `ops` | Starts copying a tree. It returns an `Operation` at once. |
| `RemoveDir` | 58 | `ops` | Starts removing a tree. It keeps going after a failed entry and reports each failure. It returns an `Operation` at once. |
| `OpCancel` | 59 | `ops` | Asks a running operation to stop. `Operation.Cancel` calls it. |
| `RemoveAll` | 55 | `remove_all` | Deprecated: use `RemoveDir`. The server still serves it. It stops at the first error. |

Options for `Clone` and `CloneDir`: `overwrite` replaces a destination that differs (without it the call fails with `EEXIST`). `preserve` lists extra properties to copy: `times`, `xattr`, `acl`. A source with a trailing slash must be a directory. A destination with a trailing slash names a directory. A destination inside its source fails with `EINVAL`. The names the mount keeps for itself at the root, such as `.mountOS`, fail with `EPERM`.

An operation is not a job. The server keeps no record of it after it ends, and there is no status call and no resume. The start call returns an operation id. The result arrives as events on the connection that started it:

- `OpFailed`: one entry failed. It has the path (relative to the mount root, empty when the name is not valid UTF-8), the code, and a message. The server sends at most 64 of them per operation.
- `OpDone`: the operation ended. It carries the totals: cloned, overwritten, skipped, failed, removed, and protected. `failed` can be larger than the number of `OpFailed` events.

`Operation.Wait` returns the totals and the kept failures (up to 64) once the end arrives. It fails with an "operation state unknown" error when notices were lost (an overflow notice), because the end can no longer be known. It fails with a connection error when the connection drops or the client closes, so it never waits for ever. The server limits the running operations per connection and per mount. At the limit a start call fails with `EAGAIN`. Cancel an operation on the client that started it. An operation that already ended fails with `ENOENT`.

A server that does not report `clone` or `ops` fails these calls at once with `UNSUPPORTED_OPERATION`. Windows mounts do not report them.

```go
out, err := c.Clone("a/file", "b/file", &client.CloneOptions{Overwrite: true})  // Go

op, err := c.CloneDir("src", "dst", nil)
counts, failures, err := op.Wait(ctx)
```

```ts
const out = await c.clone("a/file", "b/file", { overwrite: true, preserve: ["times"] });   // TypeScript

const op = await c.cloneDir("src", "dst");
const { counts, failures } = await op.wait();
```

```rust
let out = c.clone_entry("a/file", "b/file", &CloneOptions::default().with_overwrite(true))?;   // Rust

let op = c.clone_dir("src", "dst", &CloneOptions::default())?;
let result = op.wait(Duration::from_secs(600))?;
```

`clone` is not a Rust method name here because `Client` already has `Clone::clone`.

## Pool, timeouts and helpers

Every language works the same way. A call runs on the pooled connection with the fewest outstanding calls. When the server reports the `pipelining` capability, several calls overlap on one connection and replies match by request id. A timeout fails only the call that ran over, and its late reply is dropped. Without pipelining, or after too many abandoned replies, the connection is dropped and dialed again. When the server reports its capabilities, a call the server does not offer (`watch`, warm calls, `ls`, `remove_all`, `clone`, `ops`, bulk calls) fails at once with `UNSUPPORTED_OPERATION`.

### Go

- **Pipelining:** a connection keeps up to `min(Options.MaxInflight, server max_inflight)` calls in flight (default 32). Calls on different Files overlap. Calls on one File stay in order. With a limit of 1, a connection carries one call at a time.
- **Timeout:** `Options.CallTimeout` limits each request (default 0, no limit). A call that runs over returns `*TimeoutError` (code `DEADLINE_EXCEEDED`, `errors.Is(err, context.DeadlineExceeded)`).
- **Frame limit:** a request larger than the frame limits fails that call only.
- **Batches:** `StatBatch` sends its chunks in parallel. `ReadBatch` and `WriteBatch` run their requests in parallel on several connections when the file can be opened again by path, checked by inode.
- **Walking:** `WalkDir(root, fn)` visits every entry below a path with its attributes, in server order. `fn` can return `fs.SkipDir` for a directory, or any error to stop. `ReadDirAll(path)` lists one directory.
- **Limits:** `ReadDirAll` returns `ErrTooManyEntries` above `DefaultMaxDirEntries` (1,000,000). `WalkDir` returns `ErrWalkTooDeep` below `DefaultMaxWalkDepth` (1024) levels. `WithMaxEntries` and `WithMaxDepth` set them per call.
- **Watch:** a slow listener does not delay `Watch` events.

### Rust

- **Pipelining:** up to `Options::max_inflight` (default 32, capped by the server) calls overlap on one connection. A large read or write sends one request at a time, so other calls use the connection between requests.
- **Timeout:** `Options::call_timeout` limits each request (default `Duration::ZERO`, no limit). A call that runs over returns `Error::Timeout` (code `DEADLINE_EXCEEDED`, `io::ErrorKind::TimedOut`).
- **Frame limit:** a request larger than the frame limits returns `Error::TooLarge` and leaves the connection intact.
- **Batches:** `stat_batch` sends its chunks in parallel. `File::write_batch` returns `Error::Partial` for an item cut short, with the bytes written.
- **Walking:** `walk(path, f)` visits every entry below a path with its attributes. `read_dir_all(path)` lists one directory.
- **Limits:** `read_dir_all` returns `Error::TooManyEntries` above `DEFAULT_MAX_DIR_ENTRIES` (1,000,000). `walk` fails above `DEFAULT_MAX_WALK_PENDING` (65,536) queued directories. `read_dir_all_max` and `walk_max` set them per call.

### TypeScript

- **Pipelining:** a connection keeps up to `min(maxInflight, server max_inflight)` calls in flight (`maxInflight` defaults to 32).
- **Timeout:** pass `callTimeout` (ms, default 0 for none) to `Client.connect`. A request that runs longer fails with `DeadlineExceededError` (code `DEADLINE_EXCEEDED`).
- **Walking:** `for await (const { path, entry } of client.walk("/dir")) {}` lists a tree depth first with attributes. A failed listing throws. `readDirAll(path)` returns one directory as an array.
- **Limits:** both throw `TooManyEntriesError` past `DEFAULT_MAX_DIR_ENTRIES` (1,000,000) entries or `DEFAULT_MAX_WALK_DEPTH` (1024) levels. `{ maxEntries }` and `{ maxDepth }` set them per call.

## Access control

The socket is private to the mount owner. The mount reads the user id of each connecting process from the kernel and accepts only the owner and root. Every request runs with the identity of that process, so the mount applies its normal permission checks. A connection limit and limits on open handles and watches protect the mount. When a limit is reached the call fails with `EAGAIN`.

## Versioning

The wire contract is in `api/filesystem.yaml`. The public call layer is in `api/client.yaml`. `api/contract-snapshot.json` records the released contract. A new version can add calls and fields. It cannot remove or change an existing one, and the build fails if a change would break an older client. Public structs are built by field name only. New fields can be added in a minor version. In Rust, wire types are `#[non_exhaustive]`, so code outside the crate cannot build them by struct literal. The client methods take plain arguments.

## Development

The SDK needs Go, Node.js 20 or later, Rust, and `protoc`.

```sh
make api-gen     # regenerate the proto, wire types, and client stubs for every language
make api-check   # validate the specs, check compatibility, fail on stale generated files
make api-update  # write the contract snapshot after a compatible change
make test        # run the generator tests and every SDK test suite
make vet         # go vet, golangci-lint, cargo fmt --check, clippy, tsc, eslint
make check       # vet, govulncheck, and every drift and coherence check
make audit       # optional dependency audits (npm audit, cargo audit when installed)
```

Never edit a generated file. Change the specs and run `make api-gen`. Generated files are `go/wire/*`, `go/client/ops_gen.go`, `ts/src/gen/*`, `ts/src/ops_gen.ts`, `rust/src/ops_gen.rs`, and `api/proto/mountos_fs.proto`.

## Platforms

The mount serves the API on macOS, Linux, and Windows (AF_UNIX sockets on Windows 10 and later). The Go client has run its live conformance suite against a Windows mount. The TypeScript and Rust clients are not yet run against a Windows mount. The Go and TypeScript SDKs have no Unix-only code. The Rust SDK uses Unix sockets and builds on Unix systems only.

## License

Apache License 2.0. See [LICENSE](LICENSE).
