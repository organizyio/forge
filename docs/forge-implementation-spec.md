# Forge — Implementation specification (code-accurate)

This document describes the **Forge worker IPC stack as implemented** in this repository (`forge/go`, `forge/rust/forge-worker-sdk`, `forge/protocol`). It is not a product roadmap: if something is not listed here, it is **not** part of the current SDK unless noted as “reserved / unused”.

**Canonical protocol version:** `1.0` — see [`forge/protocol/VERSION`](../protocol/VERSION).

**Module paths:**

- Go: `github.com/organizyio/forge/go` — single import path; package name **`forge`** (use `import forge "github.com/organizyio/forge/go"` if needed).
- Rust: crate **`forge_worker_sdk`** (package `forge-worker-sdk`) in `forge/rust/forge-worker-sdk`.

---

## 1. Wire framing

| Item | Implementation |
|------|----------------|
| Header | 4-byte **big-endian** `uint32` payload length, then **1 byte** frame kind, then payload bytes. |
| Kind values | `1` = request, `2` = response, `3` = event (Go: `kindRequest` / `kindResponse` / `kindEvent`; Rust: `KIND_REQUEST` / `KIND_RESPONSE` / `KIND_EVENT`). |
| Max payload | **64 MiB** (`frame.MaxPayload` in Go, `MAX_FRAME_PAYLOAD` in Rust). Oversized declared length → decode error (`ErrPayloadTooLarge` / codec error). |
| Payload body | After framing, the body is **one** encoded object (MessagePack or JSON), chosen per connection — see §2. |

Sources: [`forge/go/internal/frame/frame.go`](../go/internal/frame/frame.go), [`forge/rust/forge-worker-sdk/src/framing.rs`](../rust/forge-worker-sdk/src/framing.rs).

---

## 2. Payload encoding (per connection)

| Mode | Go | Rust |
|------|----|------|
| MessagePack (default) | `EncodingMsgpack` → `codec.FormatMsgpack` | `Encoding::Msgpack` |
| JSON | `EncodingJSON` → `codec.FormatJSON` | `Encoding::Json` |

Serialization uses `github.com/vmihailenco/msgpack/v5` (Go) and `rmp-serde` (Rust). **The supervisor and worker must use the same encoding** for a given socket/pipe.

Rust reports the active encoding in the `capabilities` response as `"msgpack"` or `"json"` (`Encoding::wire_name()`).

Sources: [`forge/go/internal/codec/codec.go`](../go/internal/codec/codec.go), [`forge/go/conn.go`](../go/conn.go) (`Encoding`), [`forge/rust/forge-worker-sdk/src/framing.rs`](../rust/forge-worker-sdk/src/framing.rs).

---

## 3. Message types

### 3.1 Request (`WireRequest`)

| Field | JSON/msgpack tag | Notes |
|-------|------------------|--------|
| `id` | `id` | Unique per in-flight RPC on the connection; Go `Conn.Call` generates `req-<n>`. |
| `method` | `method` | RPC name (e.g. `ping`, `start_scan`). |
| `params` | `params,omitempty` | Go: `any`. Omitted when empty. Rust: `Option<serde_json::Value>`. |

### 3.2 Response (`WireResponse`)

| Field | Tag | Notes |
|-------|-----|--------|
| `id` | `id` | Must match request `id`. |
| `ok` | `ok` | `true` = success payload; `false` = error. |
| `error` | `error,omitempty` | Present when `ok` is false: `ErrorPayload`. |
| `payload` | `payload,omitempty` | Go: `*json.RawMessage`. Success body is arbitrary JSON inside this object. |

### 3.3 Error payload (`ErrorPayload`)

| Field | Tag |
|-------|-----|
| `code` | `code` |
| `message` | `message` |
| `detail` | `detail,omitempty` |

Known codes from the Rust dispatcher: `JOB_NOT_FOUND`, `UNKNOWN_METHOD` (others may come from product handlers).

### 3.4 Event (server → client, kind `3`)

Wire shape (Go `wireEvent` / Rust `WireEvent`):

| Field | Tag | Notes |
|-------|-----|--------|
| `type` | `type` | Event type string (product-defined). |
| `job_id` | `job_id` | Job identifier. |
| `payload` | `payload,omitempty` | Nested JSON object; **not** flattened at the top level. |

Go exposes `Event{Type, JobID, RawBody}` where `RawBody` is the raw `payload` bytes.

Sources: [`forge/go/conn.go`](../go/conn.go), [`forge/rust/forge-worker-sdk/src/protocol/envelope.rs`](../rust/forge-worker-sdk/src/protocol/envelope.rs).

---

## 4. Control-plane RPCs (Rust `BaseDispatcher`)

Implemented in [`forge/rust/forge-worker-sdk/src/dispatcher.rs`](../rust/forge-worker-sdk/src/dispatcher.rs). Contract JSON examples live under [`forge/protocol/fixtures/`](../protocol/fixtures/) and are tested in [`protocol_fixtures.rs`](../rust/forge-worker-sdk/tests/protocol_fixtures.rs).

| Method | Request `params` | Success `payload` | Errors |
|--------|------------------|-------------------|--------|
| `ping` | `null` / omitted | `{"pong": true}` | — |
| `health` | `null` / omitted | `status` (`"ok"` or `"busy"` if any job pending/running), `active_jobs` (u32), `uptime_secs`, `pid`, `version` (handler) | — |
| `capabilities` | `null` / omitted | `version`, `protocol_version` (**1**), `features` (string array), `max_concurrent_jobs`, `encoding` (`msgpack`/`json`) | — |
| `shutdown` | `null` or `{"delay_ms": <u64>}` | `{"bye": true}` | Non-test builds spawn sleep then `process::exit(0)` after `delay_ms` clamped to **default 100 ms** if missing, **max 5 minutes**. |
| `cancel_job` | `{"job_id": "<string>"}` | `{"cancelled": true, "job_id": "..."}` | `JOB_NOT_FOUND` if job missing / empty id |
| `job_status` | `{"job_id": "<string>"}` | `JobStatus` JSON: `job_id`, `state`, optional `progress`, `error` if set | `JOB_NOT_FOUND` |

`JobStatus.state` is a lowercase string from Rust `JobState`: `pending`, `running`, `completed`, `failed`, `cancelled`.

Product methods fall through to `WorkerHandler::handle_method`; unknown methods → `UNKNOWN_METHOD`.

---

## 5. Go supervisor API (actual exports)

### 5.1 `Conn`

- **`Dial(ctx, socketPath, encoding, onEvent)`** — On Unix, Unix domain socket. On Windows, if `socketPath` is a named pipe (`\\.\pipe\...`, `\\?\pipe\...`, or slash form after normalization), dials via `go-winio`; otherwise `net.Dial("unix", ...)` for AF_UNIX when available.
- **`Call(ctx, method, params)`** — sends request frame, waits for matching response id or context cancel / connection close.
- **`Close()`** — closes net conn; read loop exits.
- Read loop handles **response** and **event** frames only (request frames from worker are not expected on this client).

### 5.2 `Client` (`NewClient`)

Wraps a `Caller` (typically `*Conn`):

- `Ping`, `Shutdown`, `ShutdownWithDelay`, `CancelJob`, `JobStatus`.
- **`JobStatus`** decodes `job_id`, `state`, optional `progress` (`json.RawMessage`), and `error` from the JSON payload (aligned with Rust `JobStatus`).

### 5.3 `WorkerProcess` (`NewWorkerProcess`)

- Spawns:  
  `binaryPath --socket <path> --source-id <sourceID> --log-level <level> --encoding <msgpack|json>`
- Socket path: if **`WorkerConfig.SocketPath`** is set, that value is used (Unix filesystem socket or Windows named pipe, e.g. `\\.\pipe\...`); otherwise `filepath.Join(SocketDir, fmt.Sprintf("forge-worker-%d.sock", id))`.
- Waits up to **10s** for readiness: on Windows named pipe paths, repeated **test dials** until the pipe accepts; otherwise **`os.Stat`** on the socket path. Then **`Dial`**, then **`Ping`** must succeed.
- On process exit, **`supervise`** attempts up to **5** restarts with exponential backoff starting at **500 ms** (doubles each attempt), unless context is done.
- **`Stop`**: best-effort `Shutdown` on client, `Close`, then **`Kill`** on child process.

### 5.4 `Pool` (`NewPool`)

Holds `[]*WorkerProcess`. **`Start`/`Stop`** forward to each worker in order. **No** built-in load balancing, scheduling, or job queue.

### 5.5 `ExtractEmbedded`

Writes embedded bytes to a temp dir, returns `EmbeddedWorker{Path, Close}`. Caller supplies `fs.ReadFileFS` and path inside it (e.g. product `embed.FS`).

### 5.6 `ChannelEventBus`

`Subscribe(bufSize) <-chan *Event`, `Publish` non-blocking: **drops** when subscriber buffer full.

### 5.7 Not present in `forge/go`

There is **no** `scheduler` package, **no** `metrics` hooks package, and **no** `process.ExtractEmbedded` name under a separate `process` import — embedding is **`forge.ExtractEmbedded`**. The top-level package comment in [`doc.go`](../go/doc.go) mentions scheduling/metrics at a high level; those pieces are **not** implemented as separate modules in this tree.

---

## 6. Rust worker API (`forge_worker_sdk`)

| Symbol | Role |
|--------|------|
| `run_worker(socket_path, handler, encoding)` | Entry point: `BaseDispatcher` + **GC task** every **60 s** (`registry.gc(50)`), then platform listener loop. |
| `WorkerHandler` | `handle_method`, `worker_version`, `features`, optional `max_concurrent_jobs` (default **1**). |
| `BaseDispatcher` | `new`, `dispatch` (async), public `registry: Arc<JobRegistry>`. |
| `JobRegistry` | `register`, `cancel`, `status`, `active_count`, `set_running`, `update_progress`, `emit`, `gc`, etc. |
| `FrameCodec` / `Frame` | Tokio codec; same length + kind + payload as Go. |
| `server` | Unix: `interprocess` local socket; Windows: **named pipe** listener (`serve_windows`). |

Worker CLI is **product-defined**. The reference example [`minimal-worker`](../rust/examples/minimal-worker/src/main.rs) accepts `--socket`, `--source-id` (ignored in the example), `--log-level`, and `--encoding` so it can be spawned by `WorkerProcess` without extra flags.

---

## 7. Transport matrix

| Platform | Rust listener | Go `Dial` in this repo |
|----------|---------------|------------------------|
| Unix | Unix domain socket (`socket_path` as filesystem path) | `unix` dial to same path |
| Windows | Named pipe (`socket_path` as pipe path for `interprocess`) | Named pipe when address matches `\\.\pipe\...` (or `\\?\pipe\...` / `\??\pipe\...`); else AF_UNIX dial |

**Note:** On Windows named pipe paths, `WorkerProcess` uses dial polling for readiness instead of `os.Stat`; Unix socket paths still use `os.Stat`. Stale Unix socket files are removed before start and after stop; pipe paths are not passed to `os.Remove`.

---

## 8. Contract tests and generated docs

- JSON fixtures: [`forge/protocol/fixtures/`](../protocol/fixtures/).
- Human + generated protocol doc: [`forge/protocol/spec.md`](../protocol/spec.md).
- Spec generator: [`forge/protocol/tools/gen_spec.py`](../protocol/tools/gen_spec.py).

---

## 9. Non-goals (not in this implementation)

- Distributed / multi-host orchestration.
- Persistent job queue inside Forge.
- gRPC / HTTP for worker IPC.
- TLS on local sockets.
- Go-side Windows pipe client matching Rust named-pipe server.

---

## 10. Related reading

- [`worker-framework-reference.md`](worker-framework-reference.md) — shorter architecture summary.
- Rust crate docs: `cargo doc -p forge-worker-sdk --open` from `forge/rust`.

---

*This file is generated to reflect the repository state; when behavior changes, update this document and the protocol fixtures together.*
