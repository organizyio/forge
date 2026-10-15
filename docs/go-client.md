# Go supervisor / client (`forge/go`)

The Go module is **`github.com/organizyio/forge/go`**, package name **`forge`**:

```go
import forge "github.com/organizyio/forge/go"
```

For wire types and framing details, see [forge-implementation-spec.md](forge-implementation-spec.md) and [protocol/spec.md](../protocol/spec.md).

## Connecting

Use **`Dial`** with a filesystem Unix socket path, or on Windows a named pipe address (`\\.\pipe\Name`, `\\?\pipe\...`, or slash forms like `//./pipe/Name` — normalized the same way as in `Dial`).

Pick **`EncodingMsgpack`** or **`EncodingJSON`**; the worker must use the same encoding on that connection.

```go
conn, err := forge.Dial(ctx, socketPath, forge.EncodingJSON, func(ev *forge.Event) {
    // optional: server-push events (kind event / frame kind 3)
})
if err != nil { /* ... */ }
defer conn.Close()
```

## `Client` (typed control RPCs)

Wrap the connection:

```go
client := forge.NewClient(conn)
_, err := client.Ping(ctx)
st, err := client.JobStatus(ctx, "job-id")
cancelled, err := client.CancelJob(ctx, "job-id")
_, err := client.Shutdown(ctx)
_, err := client.ShutdownWithDelay(ctx, delayMs)
```

For **product methods** (not built into `Client`), use **`client.Conn()`** (a `Caller`) and **`Call`** on the underlying `*Conn`, or call `Conn.Call` directly if you hold a `*Conn`.

## `WorkerProcess` (spawn + supervise)

**`NewWorkerProcess`** runs `BinaryPath` + optional **`Args`**, then Forge flags:

- Socket: `--socket <path> --source-id <id> --log-level <level> --encoding <msgpack|json>`
- Stdio: `--stdio` instead of `--socket` when **`Transport: TransportStdio`**

Also: **`Env`** (append to process env), **`Dir`**, **`MaxRestarts`** / **`InitialBackoff`** (0 → defaults 5 / 500ms).

- **Default socket path:** `filepath.Join(WorkerConfig.SocketDir, fmt.Sprintf("forge-worker-%d.sock", id))`.
- **Override:** set **`WorkerConfig.SocketPath`** to a full Unix socket path or Windows pipe path.
- Socket readiness: **`os.Stat`** / pipe dial poll, then **`Dial`** + **`Ping`**. Stdio: **`Ping`** on the pipe pair.
- **`Stop`**: `Shutdown`, close client, kill child, remove stale Unix socket file when applicable.

Use **`Client()`** only while the worker reports healthy; after a crash the supervisor may restart the child (see implementation spec).

Example interpreted worker: `BinaryPath: "python3", Args: []string{"/path/to/main.py"}`.

## `Pool`

**`NewPool`** holds `[]*WorkerProcess`. **`Start`** / **`Stop`** walk the slice in order. **`PickHealthy`** / **`NextHealthy`** select a healthy worker (first / round-robin). There is no built-in job queue inside Forge.

## Other exports

- **`ChannelEventBus`** — in-process pub/sub for events (`Subscribe`, `Publish` drops when buffer full).
- **`ExtractEmbedded`** — write an embedded worker binary to disk for spawning (see `embed.go`).

## Further reading

- [forge-implementation-spec.md](forge-implementation-spec.md) — authoritative API list (Go supervisor section).
- [README.md](../README.md) — layout and quick examples.
- [python-worker.md](python-worker.md) — Python worker SDK.
- [plans/2026-10-15-multi-language-foundation-plan.md](plans/2026-10-15-multi-language-foundation-plan.md) — multi-language foundation (Done).
