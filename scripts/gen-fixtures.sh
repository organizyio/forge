#!/usr/bin/env bash
# Protocol tooling around JSON fixtures under protocol/fixtures/.
#
# Fixtures are the source of truth for examples in protocol/spec.md; they are
# not auto-generated from code. This script:
#   - Regenerates the generated block in protocol/spec.md from fixtures + go doc + Rust docs, or
#   - With --check, fails if spec.md would change (CI mode).
#   - Runs forge-worker-sdk contract tests so fixtures still match the dispatcher.
#
# Usage (from forge/):
#   ./scripts/gen-fixtures.sh           # write protocol/spec.md
#   ./scripts/gen-fixtures.sh --check   # exit 1 if spec is stale
#
# Fixture naming (per method under protocol/fixtures/):
#   <method>-request.json
#   <method>-response.json
#   <method>-error-<suffix>.json   (optional, e.g. job_status-error-not-found.json)
set -eu
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

case "${1:-}" in
  --check)
    python3 protocol/tools/gen_spec.py --check
    ;;
  --help|-h)
    sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  "")
    python3 protocol/tools/gen_spec.py
    ;;
  *)
    echo "usage: $0 [--check|--help]" >&2
    exit 2
    ;;
esac

echo "==> cargo test -p forge-worker-sdk (protocol fixtures)"
(cd rust && cargo test -p forge-worker-sdk)

echo "Done."
