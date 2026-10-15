# Pool and ops helpers (target)

| Field | Value |
|-------|-------|
| Status | Implemented |
| Phase | 5 |
| Plan | [2026-10-15-multi-language-foundation-plan.md](../plans/2026-10-15-multi-language-foundation-plan.md) |

> **Not implementation truth.** Today [`Pool`](../../go/worker_pool.go) only groups `Start` / `Stop` over `[]*WorkerProcess`.

## Goal

Add small, **domain-agnostic** helpers so products can pick a healthy worker and tune restart visibility without turning Forge into a job scheduler or turn queue.

## Current behavior (as implemented)

- `NewPool` holds workers; `Start` / `Stop` iterate in order.
- Per-worker restart with exponential backoff lives inside `WorkerProcess.supervise` (fixed attempts/backoff).
- No load balancing, no metrics package, no persistent queue ([forge-implementation-spec.md](../forge-implementation-spec.md) §5.4, §5.7, §9).

## Target helpers (sketch)

| Helper | Behavior |
|--------|----------|
| `PickHealthy` / round-robin | Return a `*WorkerProcess` with `IsHealthy() == true`, or error if none |
| Optional backoff knobs | Expose restart attempt count / initial backoff on `WorkerConfig` if products need them (defaults keep current behavior) |
| Optional metrics hooks | Later: callback or interface for spawn/restart/dial failures—only if two+ products need the same hook |

## Non-goals

- Persistent or distributed job queue inside Forge.
- Turn scheduling, retries, or idempotency.
- Stealing work across hosts.

## Acceptance checklist

- [ ] At least one selection helper on `Pool` with tests.
- [ ] Existing Start/Stop behavior unchanged for callers that ignore new helpers.
- [ ] go-client.md and implementation-spec §5.4 updated; Phase 5 marked Done in the foundation plan.

## Docs to update when Done

- [go-client.md](../go-client.md)
- [forge-implementation-spec.md](../forge-implementation-spec.md) §5.4 / §5.7
- Foundation plan phase table
