# Forge Go — integration tests

Black-box tests for `github.com/organizyio/forge/go` (real `minimal-worker` subprocess, Unix socket).

- **Tag**: `integration` (and Unix-only for IPC tests).
- **Run**: from repo `forge/go`, after `cargo build -p minimal-worker` in `forge/rust`:

  ```bash
  go test -tags=integration -timeout 120s -v ./tests/integration/...
  ```

  Or: `make -C forge sdk-go-integration`.

- **Binary path**: defaults to `forge/rust/target/debug/minimal-worker` relative to the module; override with **`FORGE_MINIMAL_WORKER`**.

These tests do **not** use Testcontainers; they are process + socket only.
