#!/usr/bin/env bash
# Pre-release verification for Forge (run from the forge directory, or any cwd).
# Mirrors the checks in .github/workflows/release.yml before you tag.
#
# Usage:
#   ./scripts/release.sh
#
# After success, publish a GitHub release (when forge is the repo root):
#   git tag v0.1.0 && git push origin v0.1.0
set -eu
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> Protocol spec (--check)"
python3 protocol/tools/gen_spec.py --check

if command -v golangci-lint >/dev/null 2>&1; then
  echo "==> golangci-lint (go/)"
  (cd go && golangci-lint run --config="${ROOT}/.golangci.yml" ./...)
else
  echo "==> golangci-lint: skipped (not in PATH)"
fi

echo "==> go test (go/)"
(cd go && go test -count=1 ./...)

echo "==> cargo test -p forge-worker-sdk"
(cd rust && cargo test -p forge-worker-sdk)

echo "==> cargo build -p minimal-worker"
(cd rust && cargo build -p minimal-worker)

echo "==> go test -tags=integration (go/tests/integration)"
(cd go && go test -tags=integration -timeout 120s -count=1 ./tests/integration/...)

echo
echo "All checks passed."
echo "Next: use GitHub Actions → Release (workflow_dispatch), wait for post-CI auto patch bump, or: git tag vX.Y.Z && git push origin vX.Y.Z"
