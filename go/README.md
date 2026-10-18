# mountOS Native SDK for Go

Go client for the [mountOS](https://mountos.io) native filesystem API. mountOS is a cross-platform, forkable, time-traveling distributed filesystem over object storage (S3, Azure Blob, Google Cloud Storage). Each running mountOS mount serves this API on a local Unix socket. Your program talks to the mount directly, without the kernel and without FUSE.

The same operations exist in TypeScript (`@mountos-io/native-sdk`) and Rust (`mountos-native`). All three are generated from one spec.

```sh
go get github.com/mountos-io/mountos-native-sdk/go
```

```go
sock, err := client.SocketPath("/mnt/vol")
c, err := client.Dial(sock, client.Options{})
if err != nil {
    log.Fatal(err)
}
defer c.Close()

info, err := c.Stat("data/report.csv")
entries, next, err := c.Ls("data", 1000, "")
err = c.WriteFile("data/new.txt", []byte("hello"), true)
```

Paths are relative to the mount root. Batch calls (`Ls`, `StatBatch`, `ReadBatch`, `WriteBatch`, `RemoveAll`), `Watch`, warm calls, request pipelining, and per-call timeouts are described in the [full documentation](https://github.com/mountos-io/mountos-native-sdk).

Website: <https://mountos.io>. Apache License 2.0.
