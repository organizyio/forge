# Stdio transport (target)

| Field | Value |
|-------|-------|
| Status | Implemented |
| Phase | 4 |
| Plan | [2026-10-15-multi-language-foundation-plan.md](../plans/2026-10-15-multi-language-foundation-plan.md) |
| ADR | [0001-framework-boundary.md](../adr/0001-framework-boundary.md) |

> **Not implementation truth.** Forge today uses Unix sockets and Windows named pipes only ([forge-implementation-spec.md](../forge-implementation-spec.md) §7).

## Goal

Optionally carry the **same** length-prefixed frames and codecs over stdin/stdout so a worker can avoid creating a socket file, for constrained environments that prefer a single child process with inherited stdio.

## Why deferred

Shape B (Python Forge worker) does **not** require stdio: sockets/pipes already give multiplexing, concurrent RPCs, and event push. Product stdio NDJSON adapters can remain outside Forge until this phase is justified.

## Target design

| Item | Requirement |
|------|-------------|
| Framing / kinds / codecs | Identical to socket transport |
| Control plane | Unchanged method set and fixture contract |
| Direction | Same as today: supervisor writes requests, reads responses and events on the pipe pair |
| Logging | Product/SDK must not print unstructured logs on the protocol stream (use stderr or a side channel) |
| Go supervisor | New dial/spawn mode: attach to child stdin/stdout instead of `Dial(socketPath)`; readiness cannot use `os.Stat` on a socket—use first successful `Ping` (or a framed hello) with timeout |
| Worker SDK | `run_worker_stdio(handler, encoding)` (name sketch) alongside socket `run_worker` |

## Non-goals

- Replacing sockets as the default.
- Compatible with unrelated NDJSON product protocols (those stay product-owned).
- TLS or network stdio.

## Acceptance checklist

- [ ] Rust and Python SDKs can serve on stdio with fixture-equivalent control-plane behavior.
- [ ] Go can spawn and supervise a stdio worker through a documented config flag/mode.
- [ ] Implementation-spec transport matrix and non-goals updated; Phase 4 marked Done in the foundation plan.

## Docs to update when Done

- [forge-implementation-spec.md](../forge-implementation-spec.md) §7, §9
- [go-client.md](../go-client.md)
- Worker SDK guides
- Foundation plan phase table
