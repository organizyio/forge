"""Generic event fixture encode/decode (kind-3 payload shape)."""

from __future__ import annotations

import json
from pathlib import Path

from forge_worker_sdk.framing import Encoding, marshal, unmarshal
from forge_worker_sdk.protocol import WireEvent

FIXTURES = Path(__file__).resolve().parents[2] / "protocol" / "fixtures"


def test_event_example_roundtrip_json() -> None:
    raw = json.loads((FIXTURES / "event-example.json").read_text(encoding="utf-8"))
    ev = WireEvent.from_dict(raw)
    assert ev.type == "progress"
    assert ev.job_id == "job-123"
    encoded = marshal(Encoding.JSON, ev.to_dict())
    got = WireEvent.from_dict(unmarshal(Encoding.JSON, encoded))
    assert got == ev
