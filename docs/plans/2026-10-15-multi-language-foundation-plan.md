# Multi-language Forge foundation

| Field | Value |
|-------|-------|
| Status | Done (implementation landed in-tree) |
| Date | 2026-10-15 |
| Canonical? | **No** for historical intent — shipped APIs: [forge-implementation-spec.md](../forge-implementation-spec.md). |

## Goal

Extend Forge so a **Python worker process** can speak the same wire protocol and lifecycle contract as today’s Rust `forge-worker-sdk`, supervised by the existing Go `WorkerProcess` / `Pool`. Product runtimes (for example Hermes `AIAgent`) stay outside Forge and plug in as product handlers.

## Purpose

- Rust workers remain first-class for deterministic local work.
- Python workers become first-class for runtimes that already live in Python.
- Hermes (and similar products) stay **product** consumers: turn ledgers, tool policy, pin/install, and isolation stay out of Forge ([ADR-0001](../adr/0001-framework-boundary.md)).

## Architecture shapes

```text
A) Today (product stdio):  Gateway --stdio NDJSON--> worker/main.py --> AIAgent
B) Forge-shaped:           Gateway --Forge Go--> socket --> python forge worker --> AIAgent
```

## Phases

| Phase | Name | Spec | Depends on | Status |
|-------|------|------|------------|--------|
| 1 | Python worker SDK + boundary | [python-worker-sdk-spec.md](../specs/python-worker-sdk-spec.md), [ADR-0001](../adr/0001-framework-boundary.md) | — | Done |
| 2 | Go `WorkerProcess` spawn/env | [worker-spawn-spec.md](../specs/worker-spawn-spec.md) | Soft: Phase 1 for Python smoke | Done |
| 3 | Streaming + cooperative cancel | [streaming-cancel-spec.md](../specs/streaming-cancel-spec.md) | Phase 1 | Done |
| 4 | Stdio transport | [stdio-transport-spec.md](../specs/stdio-transport-spec.md) | Phase 1 | Done |
| 5 | Pool / ops helpers | [pool-ops-spec.md](../specs/pool-ops-spec.md) | After Phase 1–2 usable | Done |

## Non-goals (Forge)

- Turn / attempt ledger, idempotency, or retry policy.
- Privileged-tool / Tool Router policy inside Forge builtins.
- Hermes pin, venv install, or `HERMES_HOME` semantics (products pass env via `WorkerConfig.Env`).
- Baking product event names (for example `message.delta`) into Forge control-plane methods.

## Sequencing vs product work

Product Tool Router (or equivalent) can ship on current stdio turn protocols. Migrating a Python product worker onto Forge is a **separate product track**.

**Out of repo:** hermes-fleet (or other products) owns mapping product methods/events onto Forge RPCs/events.

## Related

- [ADR-0001: Framework boundary](../adr/0001-framework-boundary.md)
- Phase specs under [`../specs/`](../specs/)
- Python guide: [../python-worker.md](../python-worker.md)
