# minimal-worker

Tiny binary that uses `forge_worker_sdk::run_worker` with a stub [`WorkerHandler`](../../forge-worker-sdk/src/dispatcher.rs). All control-plane methods (`ping`, `health`, `capabilities`, `cancel_job`, `job_status`) are handled by `BaseDispatcher`; any other RPC returns `UNKNOWN_METHOD`.

## Run

From `forge/rust`:

```bash
cargo run -p minimal-worker -- --socket /tmp/forge-minimal.sock --encoding json
```

Flags:

- `--socket` — Unix socket path (or Windows named-pipe name).
- `--encoding` — `msgpack` (default) or `json` (must match the Go supervisor).
- `--log-level` — tracing filter (default `info`).

Use with a Forge supervisor that dials the same socket and encoding, or for manual IPC experiments.

For smaller examples you can start with `use forge_worker_sdk::prelude::*` (`run_worker`, `WorkerHandler`, `Encoding`, `ErrorPayload`); this sample uses explicit paths for clarity.
