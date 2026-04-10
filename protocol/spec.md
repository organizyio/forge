# Forge IPC protocol

Canonical protocol version: see [`VERSION`](VERSION) in this directory (currently **1.0**).

## Framing and encoding

Request/response bodies use the wire envelopes defined in the Go SDK (`WireRequest` / `WireResponse`) and the Rust `forge-worker-sdk` crate (`forge_worker_sdk` in code). They are carried inside a length-prefixed frame (4-byte big-endian length, 1-byte kind) over a local socket. Payload encoding is MessagePack or JSON, as negotiated for the connection.

For a concise architecture summary, see [Forge worker framework reference](../docs/worker-framework-reference.md).

## Control-plane RPCs

Built-in methods implemented by `BaseDispatcher` in the Rust SDK include: `ping`, `health`, `capabilities`, `shutdown`, `cancel_job`, `job_status`. Product workers add domain methods (e.g. `start_scan`).

## Fixtures (`fixtures/`)

JSON files under [`fixtures/`](fixtures/) are the **contract** for control-plane request/response shapes. CI runs [`forge/rust/forge-worker-sdk/tests/protocol_fixtures.rs`](../rust/forge-worker-sdk/tests/protocol_fixtures.rs), which loads each fixture, dispatches through [`BaseDispatcher`](../rust/forge-worker-sdk/src/dispatcher.rs), and compares the result to the expected JSON (with `uptime_secs` and `pid` stripped for `health`).

## Maintaining the protocol (fixtures-first)

1. **Change fixtures first** — add or edit JSON under [`fixtures/`](fixtures/) before changing implementation (test-driven).
2. **Regenerate the generated section below** — from the `forge` directory run `make protocol-spec`, or `python3 protocol/tools/gen_spec.py`.
3. **Implement** — update [`dispatcher.rs`](../rust/forge-worker-sdk/src/dispatcher.rs) and Go [`conn.go`](../go/conn.go) / codec until `cargo test -p forge-worker-sdk` and `go test ./...` pass.

**Fixture naming**

| Pattern | Use |
|---------|-----|
| `{method}-request.json` | Request body for an RPC method. |
| `{method}-response.json` | Successful `WireResponse` for that method (example `id` matches the paired request). |
| `{method}-error-<case>.json` | Error `WireResponse`; contract tests pair these with the corresponding `{method}-request.json` (e.g. missing job). |

<!-- PROTOCOL_SPEC_GENERATED_BEGIN -->
## Generated: fixture catalog

Protocol version **1.0** ([`VERSION`](VERSION)). These excerpts are the source of truth for contract tests in [`protocol_fixtures.rs`](../rust/forge-worker-sdk/tests/protocol_fixtures.rs).

The `health` response omits comparison of `uptime_secs` and `pid` in CI (volatile fields).

### `ping`

**Request** (`ping-request.json`):

```json
{
  "id": "req-1",
  "method": "ping",
  "params": null
}
```


**Response (success)** (`ping-response.json`):

```json
{
  "id": "req-1",
  "ok": true,
  "payload": {
    "pong": true
  }
}
```


### `health`

**Request** (`health-request.json`):

```json
{
  "id": "req-2",
  "method": "health",
  "params": null
}
```


**Response (success)** (`health-response.json`):

```json
{
  "id": "req-2",
  "ok": true,
  "payload": {
    "status": "ok",
    "active_jobs": 0,
    "uptime_secs": 123,
    "pid": 1234,
    "version": "0.1.0"
  }
}
```


### `capabilities`

**Request** (`capabilities-request.json`):

```json
{
  "id": "req-3",
  "method": "capabilities",
  "params": null
}
```


**Response (success)** (`capabilities-response.json`):

```json
{
  "id": "req-3",
  "ok": true,
  "payload": {
    "version": "0.1.0",
    "protocol_version": 1,
    "features": [
      "hash.xxh64"
    ],
    "max_concurrent_jobs": 1,
    "encoding": "msgpack"
  }
}
```


### `shutdown`

**Request** (`shutdown-request.json`):

```json
{
  "id": "req-4",
  "method": "shutdown",
  "params": null
}
```


**Response (success)** (`shutdown-response.json`):

```json
{
  "id": "req-4",
  "ok": true,
  "payload": {
    "bye": true
  }
}
```


### `cancel_job`

**Request** (`cancel_job-request.json`):

```json
{
  "id": "req-5",
  "method": "cancel_job",
  "params": {
    "job_id": "job-123"
  }
}
```


**Response (error not found)** (`cancel_job-error-not-found.json`):

```json
{
  "id": "req-5",
  "ok": false,
  "error": {
    "code": "JOB_NOT_FOUND",
    "message": "job job-123 not found"
  }
}
```


**Response (success)** (`cancel_job-response.json`):

```json
{
  "id": "req-5",
  "ok": true,
  "payload": {
    "cancelled": true,
    "job_id": "job-123"
  }
}
```


### `job_status`

**Request** (`job_status-request.json`):

```json
{
  "id": "req-6",
  "method": "job_status",
  "params": {
    "job_id": "job-123"
  }
}
```


**Response (error not found)** (`job_status-error-not-found.json`):

```json
{
  "id": "req-6",
  "ok": false,
  "error": {
    "code": "JOB_NOT_FOUND",
    "message": "job job-123 not found"
  }
}
```


**Response (success)** (`job_status-response.json`):

```json
{
  "id": "req-6",
  "ok": true,
  "payload": {
    "job_id": "job-123",
    "state": "running",
    "progress": {
      "phase": "walk",
      "items_done": 5000
    }
  }
}
```

## Generated: Go wire types (`go doc`)

### `WireRequest`

```
package forge // import "github.com/organizyio/forge/go"

type WireRequest struct {
	ID     string `msgpack:"id"     json:"id"`
	Method string `msgpack:"method" json:"method"`
	Params any    `msgpack:"params" json:"params,omitempty"`
}
    WireRequest is a framed RPC request on the wire.
```

### `WireResponse`

```
package forge // import "github.com/organizyio/forge/go"

type WireResponse struct {
	ID      string           `msgpack:"id"      json:"id"`
	OK      bool             `msgpack:"ok"      json:"ok"`
	Error   *ErrorPayload    `msgpack:"error"   json:"error,omitempty"`
	Payload *json.RawMessage `msgpack:"payload" json:"payload,omitempty"`
}
    WireResponse is a framed RPC response on the wire.
```

### `ErrorPayload`

```
package forge // import "github.com/organizyio/forge/go"

type ErrorPayload struct {
	Code    string `msgpack:"code"    json:"code"`
	Message string `msgpack:"message" json:"message"`
	Detail  string `msgpack:"detail"  json:"detail,omitempty"`
}
    ErrorPayload carries a structured RPC error.

func (e *ErrorPayload) Error() string
```

### `Event`

```
package forge // import "github.com/organizyio/forge/go"

type Event struct {
	Type    string
	JobID   string
	RawBody json.RawMessage
}
    Event is a push notification from a worker connection.
```

## Generated: Rust `forge-worker-sdk` module notes

### `protocol/envelope.rs` (module doc)

Wire envelopes aligned with the Go supervisor (`go/transport`).

**Events** must serialize as three fields: `type`, `job_id`, and nested `payload`
(not a flattened body), so `transport.Conn` can fill `Event.RawBody` correctly.

### `dispatcher.rs` (module doc)

Product-extension point and control-plane dispatcher.

## How it works

1. The product worker implements [`WorkerHandler`] — only the product-specific
RPC methods (`start_scan`, etc.) need to be handled there.
2. [`BaseDispatcher`] wraps the handler and takes care of all Forge control-plane
methods automatically: `ping`, `health`, `capabilities`, `shutdown`,
`cancel_job`, `job_status`.
3. The server calls `BaseDispatcher::dispatch` for every inbound request.

## Implementing a handler

```rust,ignore
struct MyHandler { source_id: String }

impl WorkerHandler for MyHandler {
fn worker_version(&self) -> &str { env!("CARGO_PKG_VERSION") }
fn features(&self) -> Vec<String> { vec!["my.feature".into()] }

fn handle_method(&self, req_id: &str, method: &str, params: Option<Value>,
event_tx: EventSender, registry: Arc<JobRegistry>) -> WireResponse {
match method {
"start_scan" => { /* spawn task, register job … */ todo!() }
_ => forge_worker_sdk::dispatcher::unknown_method(req_id, method),
}
}
}
```

<!-- PROTOCOL_SPEC_GENERATED_END -->
