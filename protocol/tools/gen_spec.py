#!/usr/bin/env python3
"""
Regenerate the generated block in forge/protocol/spec.md from:
  - JSON fixtures in forge/protocol/fixtures/
  - `go doc` for public Go wire types (forge/go)
  - Leading `//!` module docs in Rust forge-worker-sdk protocol sources

Usage (from repo root or forge/; paths are resolved from this file):
  python3 forge/protocol/tools/gen_spec.py           # write spec.md
  python3 forge/protocol/tools/gen_spec.py --check # exit 1 if spec would change
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

MARKER_BEGIN = "<!-- PROTOCOL_SPEC_GENERATED_BEGIN -->"
MARKER_END = "<!-- PROTOCOL_SPEC_GENERATED_END -->"

# Parent of tools/ is protocol/
PROTOCOL_DIR = Path(__file__).resolve().parent.parent
FORGE_DIR = PROTOCOL_DIR.parent
FIXTURES_DIR = PROTOCOL_DIR / "fixtures"
SPEC_PATH = PROTOCOL_DIR / "spec.md"
VERSION_PATH = PROTOCOL_DIR / "VERSION"
GO_DIR = FORGE_DIR / "go"
RUST_PROTOCOL_DIR = FORGE_DIR / "rust" / "forge-worker-sdk" / "src" / "protocol"
RUST_DISPATCHER = FORGE_DIR / "rust" / "forge-worker-sdk" / "src" / "dispatcher.rs"

RPC_ORDER = [
    "ping",
    "health",
    "capabilities",
    "shutdown",
    "cancel_job",
    "job_status",
]


def read_version() -> str:
    return VERSION_PATH.read_text(encoding="utf-8").strip()


def parse_fixture_name(name: str) -> tuple[str, str] | None:
    """Return (method, kind) where kind is request|response|error_<suffix>."""
    base = name.removesuffix(".json")
    if base.endswith("-request"):
        return base[: -len("-request")], "request"
    if "-error-" in base:
        method, _, rest = base.partition("-error-")
        return method, f"error_{rest.replace('-', '_')}"
    if base.endswith("-response"):
        return base[: -len("-response")], "response"
    return None


def load_fixtures_grouped() -> dict[str, dict[str, Path]]:
    grouped: dict[str, dict[str, Path]] = {}
    for p in sorted(FIXTURES_DIR.glob("*.json")):
        parsed = parse_fixture_name(p.name)
        if not parsed:
            continue
        method, kind = parsed
        grouped.setdefault(method, {})[kind] = p
    return grouped


def fence_json(label: str, path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    try:
        pretty = json.dumps(json.loads(raw), indent=2)
    except json.JSONDecodeError:
        pretty = raw
    return f"**{label}** (`{path.name}`):\n\n```json\n{pretty}\n```\n"


def section_fixtures_catalog() -> str:
    version = read_version()
    lines = [
        "## Generated: fixture catalog",
        "",
        f"Protocol version **{version}** ([`VERSION`](VERSION)). "
        "These excerpts are the source of truth for contract tests in "
        "[`protocol_fixtures.rs`](../rust/forge-worker-sdk/tests/protocol_fixtures.rs).",
        "",
        "The `health` response omits comparison of `uptime_secs` and `pid` in CI "
        "(volatile fields).",
        "",
    ]
    grouped = load_fixtures_grouped()
    for method in RPC_ORDER:
        if method not in grouped:
            continue
        files = grouped[method]
        lines.append(f"### `{method}`")
        lines.append("")
        if "request" in files:
            lines.append(fence_json("Request", files["request"]))
            lines.append("")
        for k in sorted(files):
            if k == "request":
                continue
            if k == "response":
                lines.append(fence_json("Response (success)", files[k]))
            elif k.startswith("error_"):
                label = "Response (" + k.replace("_", " ") + ")"
                lines.append(fence_json(label, files[k]))
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


GO_MODULE = "github.com/organizyio/forge/go"


def run_go_doc(symbol: str) -> str:
    """Stable across machines: fully-qualified package path."""
    path = f"{GO_MODULE}.{symbol}"
    try:
        r = subprocess.run(
            ["go", "doc", path],
            cwd=GO_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return f"(go not installed: could not run `go doc {path}`)\n"
    if r.returncode != 0:
        return f"(`go doc {path}` failed: {r.stderr.strip() or r.stdout.strip()})\n"
    return r.stdout.strip() + "\n"


def section_go_docs() -> str:
    lines = ["## Generated: Go wire types (`go doc`)", ""]
    syms = ["WireRequest", "WireResponse", "ErrorPayload", "Event"]
    for sym in syms:
        lines.append(f"### `{sym}`")
        lines.append("")
        lines.append("```")
        lines.append(run_go_doc(sym).rstrip())
        lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def extract_leading_module_doc(rs_path: Path) -> str:
    lines_out: list[str] = []
    for line in rs_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("//!"):
            lines_out.append(line[3:].strip())
        elif line.strip() == "" and lines_out:
            continue
        elif not line.startswith("//!"):
            break
    return "\n".join(lines_out).strip()


def section_rust_docs() -> str:
    lines = ["## Generated: Rust `forge-worker-sdk` module notes", ""]
    env = RUST_PROTOCOL_DIR / "envelope.rs"
    if env.exists():
        lines.append("### `protocol/envelope.rs` (module doc)")
        lines.append("")
        lines.append(extract_leading_module_doc(env) or "(no `//!` block found)")
        lines.append("")
    if RUST_DISPATCHER.exists():
        lines.append("### `dispatcher.rs` (module doc)")
        lines.append("")
        lines.append(extract_leading_module_doc(RUST_DISPATCHER) or "(no `//!` block found)")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_generated() -> str:
    parts = [
        section_fixtures_catalog(),
        section_go_docs(),
        section_rust_docs(),
    ]
    return "\n".join(parts)


def read_spec() -> str:
    return SPEC_PATH.read_text(encoding="utf-8")


def replace_generated(spec_text: str, generated: str) -> str:
    if MARKER_BEGIN not in spec_text or MARKER_END not in spec_text:
        raise SystemExit(
            f"{SPEC_PATH}: missing {MARKER_BEGIN!r} or {MARKER_END!r}"
        )
    before, rest = spec_text.split(MARKER_BEGIN, 1)
    _, after = rest.split(MARKER_END, 1)
    new_middle = f"\n{generated.rstrip()}\n\n"
    return before + MARKER_BEGIN + new_middle + MARKER_END + after


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--check",
        action="store_true",
        help="exit with status 1 if spec.md would change",
    )
    args = ap.parse_args()
    generated = build_generated()
    current = read_spec()
    updated = replace_generated(current, generated)
    if args.check:
        if current != updated:
            print(
                "spec.md is out of date; run: python3 forge/protocol/tools/gen_spec.py",
                file=sys.stderr,
            )
            # Show a small diff hint
            if MARKER_BEGIN in current:
                old_mid = current.split(MARKER_BEGIN, 1)[1].split(MARKER_END, 1)[0]
                new_mid = updated.split(MARKER_BEGIN, 1)[1].split(MARKER_END, 1)[0]
                if old_mid != new_mid:
                    print("--- generated section differs ---", file=sys.stderr)
            sys.exit(1)
        sys.exit(0)
    SPEC_PATH.write_text(updated, encoding="utf-8", newline="\n")
    print(f"Wrote {SPEC_PATH}")


if __name__ == "__main__":
    main()
