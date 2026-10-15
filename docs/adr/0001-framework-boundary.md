# ADR-0001: Multi-language framework boundary

| Field | Value |
|-------|-------|
| Status | Accepted |
| Date | 2026-10-15 |
| Plan | [2026-10-15-multi-language-foundation-plan.md](../plans/2026-10-15-multi-language-foundation-plan.md) |

## Context

Forge today is a Go supervisor plus a Rust worker SDK over Unix sockets / Windows named pipes. Product runtimes such as Hermes keep intelligence in a Python process and currently talk to their gateway over stdio NDJSON. Extending Forge with a Python worker SDK is the right long-term move for shared lifecycle with Rust workers, but product concerns (turns, tool policy, Hermes install) must not enter the framework.

## Decision

1. **Framework vs product.** Forge owns framing, transport, process lifecycle, control-plane RPCs (`ping`, `health`, `capabilities`, `shutdown`, `cancel_job`, `job_status`), job registry, and worker→supervisor events. Products own domain RPCs, domain event type strings, ledgers, retries, and policy (including privileged-tool allow/deny).

2. **Python host for Hermes intelligence.** Until upstream intelligence moves or is wrapped elsewhere, Hermes-style agents continue to run inside a Python process. Forge does not reimplement `AIAgent` or other product runtimes; a Python Forge worker imports product code inside `handle_method` (or equivalent).

3. **Transport priority.** Shape B uses the existing socket / named-pipe transport first. Stdio framed transport is a later phase (see [stdio-transport-spec.md](../specs/stdio-transport-spec.md)), not a requirement for hosting a Python worker on Forge.

4. **Bidirectional product flows (v1).** Worker→Go uses event frames (kind 3). Go→worker uses concurrent request/response RPCs on the same connection (including `cancel_job` and product methods such as tool decisions). Forge v1 does **not** add a reverse-RPC protocol (worker-initiated requests with supervisor responses).

5. **Contract tests stay language-neutral.** JSON fixtures under `protocol/fixtures/` remain the source of truth. Rust is the current consumer; a Python SDK must pass the same fixtures when Phase 1 lands. CI gains a Python fixture job at that time—fixtures themselves do not encode product schemas.

## Consequences

- Target designs for unfinished work live under `docs/plans/`, `docs/adr/`, and `docs/specs/`. They are **not** implementation truth.
- [`forge-implementation-spec.md`](../forge-implementation-spec.md) continues to describe only what is implemented; readers must not treat planned APIs as shipped.
- Product repositories (for example hermes-fleet) keep turn/tool protocol and migration adapters; Forge docs only point at that ownership boundary.
- Adding stdio later must not change control-plane method semantics—only the byte transport under the same frames and codecs.
