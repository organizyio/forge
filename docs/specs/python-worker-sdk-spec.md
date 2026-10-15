# Python worker SDK (target)

| Field | Value |
|-------|-------|
| Status | Implemented |
| Phase | 1 |
| Plan | [2026-10-15-multi-language-foundation-plan.md](../plans/2026-10-15-multi-language-foundation-plan.md) |
| ADR | [0001-framework-boundary.md](../adr/0001-framework-boundary.md) |

> **Not implementation truth.** Shipped APIs are listed only in [forge-implementation-spec.md](../forge-implementation-spec.md). This document describes the intended Python peer of `forge-worker-sdk`.

## Goal

Ship a Python package that speaks Forge’s wire protocol so Go `WorkerProcess` can supervise a Python worker the same way it supervises Rust binaries today.

## Layout (target)

```
forge/python/
├── forge_worker_sdk/          # import name: forge_worker_sdk
│   ├── __init__.py
│   ├── framing.py             # 4-byte BE length + kind + payload
│   ├── codec.py               # JSON / MessagePack
│   ├── protocol.py            # WireRequest / WireResponse / WireEvent / ErrorPayload
│   ├── dispatcher.py          # BaseDispatcher + WorkerHandler protocol
│   ├── job_registry.py        # states, cancel, progress, emit, gc
│   └── server.py              # run_worker listen loop
└── examples/
    └── minimal-python-worker/ # CLI matching WorkerProcess spawn flags
```

## Behavioral parity with Rust

Mirror [`forge-worker-sdk`](../../rust/forge-worker-sdk/):

| Concern | Requirement |
|---------|-------------|
| Framing | 4-byte big-endian `uint32` length, 1-byte kind (`1` request, `2` response, `3` event), max payload 64 MiB |
| Encoding | Per-connection MessagePack (default) or JSON; advertised in `capabilities` as `msgpack` / `json` |
| Control plane | `ping`, `health`, `capabilities`, `shutdown`, `cancel_job`, `job_status` via `BaseDispatcher` |
| Product methods | `WorkerHandler.handle_method` fall-through; unknown → `UNKNOWN_METHOD` |
| Jobs | Registry with pending/running/completed/failed/cancelled; cancel signal; progress for `job_status`; event emit |
| Entry | `run_worker(socket_path, handler, encoding)` — Unix domain socket first; Windows named pipe when parity is required |
| CLI | `--socket`, `--encoding`, `--source-id`, `--log-level` (same argv as [`WorkerProcess.startLocked`](../../go/worker_process.go)) |

`protocol_version` in `capabilities` remains **1** until a deliberate protocol bump.

## Contract tests

- Load JSON under [`protocol/fixtures/`](../../protocol/fixtures/) and assert dispatcher outputs match expected responses (strip volatile `health` fields the same way Rust does).
- Optional integration: Go `NewWorkerProcess` against `minimal-python-worker` → `Ping` / `capabilities` / `cancel_job` smoke.
- CI: add a job that runs the Python fixture suite when the package exists (sketch only in this docs pass).

## Non-goals

- Importing Hermes, `AIAgent`, or any product runtime inside `forge_worker_sdk`.
- Stdio transport (Phase 4).
- Product turn / tool method names as Forge builtins.

## Acceptance checklist

- [ ] Package importable as `forge_worker_sdk` from `forge/python/`.
- [ ] `minimal-python-worker` accepts the four CLI flags and listens on `--socket`.
- [ ] All current control-plane fixtures pass under Python.
- [ ] Go can spawn the example worker and complete `Ping`.
- [ ] Docs: update implementation-spec §6 (or add § Python), `README` layout, and mark Phase 1 Done in the foundation plan.

## Docs to update when Done

- [forge-implementation-spec.md](../forge-implementation-spec.md)
- [README.md](../../README.md)
- [worker-framework-reference.md](../worker-framework-reference.md)
- [protocol.md](../protocol.md)
- [protocol/spec.md](../../protocol/spec.md) (fixture consumer note)
- Foundation plan phase table
