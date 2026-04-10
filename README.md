# Forge

[![Go Reference](https://pkg.go.dev/badge/github.com/organizyio/forge/go.svg)](https://pkg.go.dev/github.com/organizyio/forge/go)
![Go Version](https://img.shields.io/badge/go-1.26+-blue.svg)
[![crates.io](https://img.shields.io/crates/v/forge-worker-sdk.svg)](https://crates.io/crates/forge-worker-sdk)
[![docs.rs](https://docs.rs/forge-worker-sdk/badge.svg)](https://docs.rs/forge-worker-sdk)
[![CI](https://github.com/organizyio/forge/actions/workflows/ci.yml/badge.svg)](https://github.com/organizyio/forge/actions/workflows/ci.yml)
[![Release](https://github.com/organizyio/forge/actions/workflows/release.yml/badge.svg)](https://github.com/organizyio/forge/actions/workflows/release.yml)

Local **Go ↔ Rust** worker IPC: length-prefixed frames, MessagePack or JSON, control-plane RPCs (`ping`, `health`, `capabilities`, `shutdown`, `cancel_job`, `job_status`), and optional progress **events**.

This directory is the **framework** used by products such as Organizy (`organizy/daemon` + `organizy-worker`). It stays domain-agnostic.

> **Framework boundary:** Forge owns framing, transport, and worker lifecycle — not product schemas, storage, or scheduling.

## Layout

| Path | Contents |
|------|----------|
| `go/` | Go module `github.com/organizyio/forge/go` — package **`forge`**: `Conn`, `Client`, `WorkerProcess`, `Pool`, `ExtractEmbedded`, `ChannelEventBus` |
| `rust/` | Cargo workspace: **`forge-worker-sdk`** crate at `rust/forge-worker-sdk/` (no `crates/` nesting), `examples/minimal-worker` |
| `protocol/` | Version `VERSION`, JSON fixtures, `spec.md`, `tools/gen_spec.py` |
| `docs/` | `forge-implementation-spec.md`, `worker-framework-reference.md`, `go-client.md`, `rust-worker.md` |
| `scripts/` | `release.sh` (pre-tag checks), `gen-fixtures.sh` (regenerate `protocol/spec.md` from fixtures + run contract tests) |

## Installation

```bash
go get github.com/organizyio/forge/go@latest
```

Rust: add `forge-worker-sdk` from this repo (path or git) or from [crates.io](https://crates.io/crates/forge-worker-sdk) once published. Import as `forge_worker_sdk` in code.

## Getting started (real APIs)

### Rust worker

From `forge/rust`:

```bash
cargo run -p minimal-worker -- --socket /tmp/forge-minimal.sock --encoding json
```

Implement `forge_worker_sdk::WorkerHandler`; use `forge_worker_sdk::run_worker` with your handler and `Encoding`. See `forge-worker-sdk` crate docs and `examples/minimal-worker/src/main.rs`.

### Go supervisor

```go
import (
    "context"
    forge "github.com/organizyio/forge/go"
)

conn, err := forge.Dial(ctx, "/tmp/forge-minimal.sock", forge.EncodingJSON, func(ev *forge.Event) {
    // optional: handle push events
})
if err != nil { /* ... */ }
defer conn.Close()

client := forge.NewClient(conn)
if _, err := client.Ping(ctx); err != nil { /* ... */ }

st, err := client.JobStatus(ctx, "job-1") // decodes state, job_id, progress, error
```

**Windows named pipe** (Rust worker listening on a pipe): dial with a pipe address, e.g. `\\.\pipe\YourPipeName` or `//./pipe/YourPipeName`.

**Spawn + supervise** a worker binary (Unix: default socket under `SocketDir`; Windows pipe: set **`SocketPath`** to e.g. `\\.\pipe\YourPipeName` and pass the same value as the worker’s `--socket`):

```go
wp := forge.NewWorkerProcess(1, forge.WorkerConfig{
    BinaryPath: "/path/to/worker",
    SourceID:   "my-product",
    SocketDir: "/tmp",
    // Windows pipe example (optional; when set, replaces SocketDir path):
    // SocketPath: `\\.\pipe\forge-worker-1`,
    Encoding: forge.EncodingMsgpack,
    Log:        slog.Default(),
})
if err := wp.Start(ctx); err != nil { /* ... */ }
defer wp.Stop(ctx)

c := wp.Client()
_, _ = c.Ping(ctx)
```

## Build & test

```bash
# Go (from forge/go)
go test ./...

# Rust (from forge/rust)
cargo test -p forge-worker-sdk
cargo build -p minimal-worker
```

On **Windows**, use the **MSVC** Rust toolchain for `cargo test` if GNU MinGW is incomplete (`rustup default stable-x86_64-pc-windows-msvc` or an override in `forge/rust`). See `rust/README.md`.

## CI

GitHub Actions runs path-scoped jobs for `go/`, `rust/`, `protocol/`, and shared integration tests. The required check to gate merges is the aggregate job **CI** (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

## Releases and versioning

[`release.yml`](.github/workflows/release.yml) can run in three ways:

1. **workflow_dispatch** — Actions → Release → Run workflow; enter a version (e.g. `1.2.0` without `v`).
2. **After CI on `main`/`master`** — when the [**CI**](.github/workflows/ci.yml) workflow completes successfully on a push, the patch version is auto-incremented from the latest `v*` tag (skipped if `HEAD` is already tagged). This runs on every successful CI run on the default branch; disable or adjust the `workflow_run` trigger in [`release.yml`](.github/workflows/release.yml) if you want releases only when you dispatch them manually.
3. **Manual tag** — `git tag v0.1.0 && git push origin v0.1.0` runs GoReleaser only (no changelog commit).

On paths (1) and (2), the workflow verifies the repo, prepends to [`CHANGELOG.md`](CHANGELOG.md), refreshes the Go version badge in this README from `go/go.mod`, bumps [`rust/forge-worker-sdk/Cargo.toml`](rust/forge-worker-sdk/Cargo.toml), commits, pushes the branch, creates the tag, runs **GoReleaser**, and notifies `proxy.golang.org`. Prefer **conventional commits** (`feat:`, `fix:`, `chore:`, …) so changelog sections populate.

### Optional: publish `forge-worker-sdk` to crates.io

Follow [Publishing on crates.io](https://doc.rust-lang.org/cargo/reference/publishing.html): create an API token on crates.io, then add it as a GitHub Actions repository secret named **`CARGO_REGISTRY_TOKEN`**. Cargo reads that token from the environment variable of the same name ([credentials / `CARGO_REGISTRY_TOKEN`](https://doc.rust-lang.org/cargo/reference/config.html#credentials)).

1. Set repository variable **`PUBLISH_TO_CRATES_IO`** to `true`.
2. Add secret **`CARGO_REGISTRY_TOKEN`** (the crates.io API token).
3. The release workflow runs `cargo publish -p forge-worker-sdk --locked` after GoReleaser when the variable is set.

## Specification

- **Wire format & RPC tables:** [`protocol/spec.md`](protocol/spec.md) (includes generated sections from fixtures + `go doc`).
- **Implementation truth:** [`docs/forge-implementation-spec.md`](docs/forge-implementation-spec.md).
