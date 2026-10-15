# Python worker (`forge_worker_sdk`)

Package: **`forge_worker_sdk`** under [`python/forge_worker_sdk/`](../python/forge_worker_sdk/).

Published install: `pip install forge-worker-sdk` or `uv add forge-worker-sdk`. From source: `cd python && pip install -e ".[dev]"`.

## Connecting (Unix socket)

```python
from forge_worker_sdk import Encoding, run_worker

run_worker("/tmp/my.sock", MyHandler(), Encoding.JSON)
```

**Windows named pipes** are not implemented for the Python SDK yet; use Unix or stdio.

## Stdio

```python
from forge_worker_sdk import Encoding, run_worker_stdio

run_worker_stdio(MyHandler(), Encoding.JSON)
```

Pass `--stdio` from the CLI (see `examples/minimal-python-worker`). Logs must go to **stderr** only.

## Handler

Implement methods matching the Rust `WorkerHandler` surface:

- `worker_version`, `features`, optional `max_concurrent_jobs` (default 1)
- `handle_method(req_id, method, params, event_sink, registry)`

Control-plane RPCs are handled by `BaseDispatcher`.

## Long-running jobs and cancel

Register a job, return the start RPC promptly, emit kind-3 events from a background thread, and check `job.is_cancelled()` / `JobRegistry.cancel` between steps. Cancel is **cooperative** — blocking product calls are not preempted.

See [specs/streaming-cancel-spec.md](specs/streaming-cancel-spec.md).

## Contract tests

```bash
cd python && FORGE_WORKER_TEST=1 pytest
```

Fixtures live in [`protocol/fixtures/`](../protocol/fixtures/).

## See also

- Example: [`python/examples/minimal-python-worker`](../python/examples/minimal-python-worker)
- Foundation plan: [plans/2026-10-15-multi-language-foundation-plan.md](plans/2026-10-15-multi-language-foundation-plan.md)
- Sibling Rust guide: [rust-worker.md](rust-worker.md)
