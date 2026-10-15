# Rust worker (`forge_worker_sdk`)

Crate: **`forge_worker_sdk`** (package **`forge-worker-sdk`**) in `rust/forge-worker-sdk/`. Add it as a path or crates.io dependency from your worker binary.

## Connecting

Use **`run_worker`** with a socket path (Unix) or Windows named pipe name, your [`WorkerHandler`](../rust/forge-worker-sdk/src/dispatcher.rs), and **`Encoding`** (`Msgpack` or `Json` — must match the Go supervisor).

```rust
use forge_worker_sdk::{run_worker, framing::Encoding};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_worker("/tmp/my.sock", MyHandler, Encoding::Json).await
}
```

## Handler

Implement **`forge_worker_sdk::dispatcher::WorkerHandler`**:

- `worker_version` — e.g. `env!("CARGO_PKG_VERSION")`.
- `features` — capability strings surfaced in `capabilities`.
- `handle_method` — domain RPCs; return `unknown_method` for unsupported names.

Built-in control methods are handled by `BaseDispatcher` before your handler.

## Stdio

```rust
run_worker_stdio(MyHandler, Encoding::Json).await?;
```

Use `--stdio` on [`minimal-worker`](../rust/examples/minimal-worker). Keep logs on stderr.

## Long-running jobs

Register a job, return the start RPC promptly, emit kind-3 events, honor cooperative cancel. See [streaming-cancel-spec.md](specs/streaming-cancel-spec.md).

## Sibling SDKs

Python peer: [python-worker.md](python-worker.md) (`forge/python`).

## See also

- Example: [`rust/examples/minimal-worker`](../rust/examples/minimal-worker).
- Contract tests: **`rust/forge-worker-sdk/tests/protocol_fixtures.rs`** (dispatch + compare; volatile keys like health timestamps may be stripped before assert).
- Crate-level docs: `cargo doc -p forge-worker-sdk --open`.
- Foundation plan: [plans/2026-10-15-multi-language-foundation-plan.md](plans/2026-10-15-multi-language-foundation-plan.md).
