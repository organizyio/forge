"""Contract tests: protocol/fixtures vs BaseDispatcher (mirror Rust protocol_fixtures)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

# Ensure shutdown does not exit the test process.
os.environ["FORGE_WORKER_TEST"] = "1"

from forge_worker_sdk import (  # noqa: E402
    BaseDispatcher,
    Encoding,
    Job,
    WireRequest,
    unknown_method,
)

FIXTURES = Path(__file__).resolve().parents[2] / "protocol" / "fixtures"


class FixtureHandler:
    def worker_version(self) -> str:
        return "0.1.0"

    def features(self) -> list[str]:
        return ["hash.xxh64", "reliable_events_v1"]

    def max_concurrent_jobs(self) -> int:
        return 1

    def handle_method(self, req_id, method, params, event_sink, registry):
        return unknown_method(req_id, method)


def read_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def normalize_health(v: dict) -> dict:
    payload = v.get("payload")
    if isinstance(payload, dict):
        payload = dict(payload)
        payload.pop("uptime_secs", None)
        payload.pop("pid", None)
        v = dict(v)
        v["payload"] = payload
    return v


def dispatch(d: BaseDispatcher, req_dict: dict) -> dict:
    req = WireRequest.from_dict(req_dict)
    resp = d.dispatch(req, event_sink=lambda _ev: None)
    return resp.to_dict()


@pytest.fixture
def dispatcher() -> BaseDispatcher:
    return BaseDispatcher(FixtureHandler(), Encoding.MSGPACK)


def test_ping(dispatcher: BaseDispatcher) -> None:
    actual = dispatch(dispatcher, read_fixture("ping-request.json"))
    assert actual == read_fixture("ping-response.json")


def test_health(dispatcher: BaseDispatcher) -> None:
    actual = normalize_health(dispatch(dispatcher, read_fixture("health-request.json")))
    expected = normalize_health(read_fixture("health-response.json"))
    assert actual == expected


def test_capabilities(dispatcher: BaseDispatcher) -> None:
    actual = dispatch(dispatcher, read_fixture("capabilities-request.json"))
    assert actual == read_fixture("capabilities-response.json")


def test_shutdown(dispatcher: BaseDispatcher) -> None:
    actual = dispatch(dispatcher, read_fixture("shutdown-request.json"))
    assert actual == read_fixture("shutdown-response.json")


def test_cancel_job_ok(dispatcher: BaseDispatcher) -> None:
    job = Job(job_id="job-123")
    dispatcher.registry.register(job)
    actual = dispatch(dispatcher, read_fixture("cancel_job-request.json"))
    assert actual == read_fixture("cancel_job-response.json")


def test_cancel_job_not_found(dispatcher: BaseDispatcher) -> None:
    actual = dispatch(dispatcher, read_fixture("cancel_job-request.json"))
    assert actual == read_fixture("cancel_job-error-not-found.json")


def test_job_status_ok(dispatcher: BaseDispatcher) -> None:
    job = Job(job_id="job-123")
    dispatcher.registry.register(job)
    dispatcher.registry.set_running("job-123")
    dispatcher.registry.update_progress(
        "job-123", {"phase": "walk", "items_done": 5000}
    )
    actual = dispatch(dispatcher, read_fixture("job_status-request.json"))
    assert actual == read_fixture("job_status-response.json")


def test_job_status_not_found(dispatcher: BaseDispatcher) -> None:
    actual = dispatch(dispatcher, read_fixture("job_status-request.json"))
    assert actual == read_fixture("job_status-error-not-found.json")
