"""Long-job pattern: register, emit progress events, cooperative cancel."""

from __future__ import annotations

import os
import threading
import time
from typing import Any

os.environ["FORGE_WORKER_TEST"] = "1"

from forge_worker_sdk import (
    BaseDispatcher,
    Encoding,
    Job,
    JobRegistry,
    WireEvent,
    WireRequest,
    WireResponse,
    ok_response,
    unknown_method,
)


class DemoHandler:
    def worker_version(self) -> str:
        return "0.1.0"

    def features(self) -> list[str]:
        return ["demo.job"]

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
        if method != "start_demo_job":
            return unknown_method(req_id, method)
        job_id = "job-demo"
        if isinstance(params, dict) and params.get("job_id"):
            job_id = str(params["job_id"])
        job = Job(job_id=job_id, event_sink=event_sink)
        registry.register(job)
        registry.set_running(job_id)

        def runner() -> None:
            for n in (1, 2, 3):
                if job.is_cancelled():
                    break
                ev = WireEvent(
                    type="progress",
                    job_id=job_id,
                    payload={"n": n, "phase": "demo"},
                )
                registry.emit(job_id, ev)
                time.sleep(0.05)
            if job.is_cancelled():
                return
            registry.set_completed(job_id, {"n": 3})

        threading.Thread(target=runner, daemon=True).start()
        return ok_response(req_id, {"accepted": True, "job_id": job_id})


def test_demo_job_cancel_stops_progress() -> None:
    d = BaseDispatcher(DemoHandler(), Encoding.JSON)
    events: list[WireEvent] = []

    def sink(ev: WireEvent) -> None:
        events.append(ev)

    start = WireRequest(id="r1", method="start_demo_job", params={"job_id": "job-demo"})
    resp = d.dispatch(start, sink)
    assert resp.ok
    time.sleep(0.02)
    cancel = WireRequest(id="r2", method="cancel_job", params={"job_id": "job-demo"})
    assert d.dispatch(cancel, sink).ok
    time.sleep(0.2)
    st = d.registry.status("job-demo")
    assert st is not None
    assert st.state == "cancelled"
    assert len(events) >= 1
    assert events[0].type == "progress"
