#!/usr/bin/env python3
"""Minimal Forge Python worker — control-plane only; unknown methods → UNKNOWN_METHOD."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

# Allow running from a source checkout without install.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from forge_worker_sdk import (  # noqa: E402
    Encoding,
    JobRegistry,
    WireResponse,
    run_worker,
    run_worker_stdio,
    unknown_method,
)


class MinimalHandler:
    def worker_version(self) -> str:
        return "0.1.0"

    def features(self) -> list[str]:
        return ["example.minimal"]

    def max_concurrent_jobs(self) -> int:
        return 1

    def handle_method(
        self,
        req_id: str,
        method: str,
        params: Any,
        event_sink: Any,
        registry: JobRegistry,
    ) -> WireResponse:
        return unknown_method(req_id, method)


def main() -> None:
    p = argparse.ArgumentParser(description="Minimal Forge IPC Python worker")
    p.add_argument("--socket", default="/tmp/forge-minimal-py.sock")
    p.add_argument("--source-id", default="", help="Accepted for WorkerProcess compatibility")
    p.add_argument("--log-level", default="info")
    p.add_argument("--encoding", default="msgpack")
    p.add_argument(
        "--stdio",
        action="store_true",
        help="Serve framed IPC on stdin/stdout instead of --socket",
    )
    args = p.parse_args()
    encoding = Encoding.from_flag(args.encoding)
    handler = MinimalHandler()
    if args.stdio:
        run_worker_stdio(handler, encoding)
    else:
        run_worker(args.socket, handler, encoding)


if __name__ == "__main__":
    main()
