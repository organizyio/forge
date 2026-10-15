# WorkerProcess spawn and env (target)

| Field | Value |
|-------|-------|
| Status | Implemented |
| Phase | 2 |
| Plan | [2026-10-15-multi-language-foundation-plan.md](../plans/2026-10-15-multi-language-foundation-plan.md) |

> **Not implementation truth.** Today’s spawn is documented in [go-client.md](../go-client.md) and [forge-implementation-spec.md](../forge-implementation-spec.md) §5.3.

## Goal

Allow Go `WorkerProcess` to supervise interpreted workers (for example `python3 path/to/worker.py`) and pass process environment without inventing product-specific variables inside Forge.

## Current behavior (as implemented)

```text
binaryPath --socket <path> --source-id <id> --log-level <level> --encoding <msgpack|json>
```

`WorkerConfig` has `BinaryPath`, `SourceID`, `SocketDir` / `SocketPath`, `Encoding`, `LogLevel`, `OnEvent`, `Log`. No extra argv, env, or working directory fields.

## Target `WorkerConfig` fields

| Field | Type (sketch) | Behavior |
|-------|---------------|----------|
| `Args` | `[]string` | Appended after `BinaryPath` and **before** the standard Forge flags, **or** documented as full argv override if a single `Command []string` form is chosen instead. Preferred: `BinaryPath` + `Args` + Forge appends `--socket` … `--encoding`. |
| `Env` | `[]string` | Extra `KEY=value` entries for the child (`os/exec.Cmd.Env` = `os.Environ()` plus these, or replace-all if explicitly documented). Forge does not define `HERMES_HOME`; products set it here. |
| `Dir` | `string` | Optional `Cmd.Dir` working directory. |

Readiness remains: wait for Unix socket / Windows pipe, then `Dial`, then `Ping`. Stdout/stderr of the child continue to be attached for logs (protocol stays on the socket).

## Example spawn shapes

```text
# Rust binary (unchanged)
/path/to/minimal-worker --socket ... --source-id ... --log-level info --encoding msgpack

# Python (Phase 1 example + Phase 2 Args)
python3 /path/to/minimal_python_worker.py --socket ... --source-id ... --log-level info --encoding json
# with WorkerConfig{ BinaryPath: "python3", Args: []string{"/path/to/minimal_python_worker.py"}, Env: []string{"HERMES_HOME=/data/home"}, ... }
```

## Non-goals

- Embedding a Python interpreter in Go.
- Stdio IPC (Phase 4).
- Product-specific env key validation.

## Acceptance checklist

- [ ] `WorkerConfig` exposes `Args`, `Env`, and `Dir` (names may match this table).
- [ ] Existing Rust `minimal-worker` spawn path remains green with empty `Args`/`Env`/`Dir`.
- [ ] Documented example spawns a Python script with Forge flags and custom env.
- [ ] [go-client.md](../go-client.md) and implementation-spec §5.3 updated; Phase 2 marked Done in the foundation plan.

## Docs to update when Done

- [go-client.md](../go-client.md)
- [forge-implementation-spec.md](../forge-implementation-spec.md) §5.3
- [README.md](../../README.md) supervisor example if needed
- Foundation plan phase table
