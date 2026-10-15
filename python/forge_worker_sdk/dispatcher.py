"""Control-plane BaseDispatcher and WorkerHandler protocol."""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Protocol, runtime_checkable

from forge_worker_sdk.framing import Encoding
from forge_worker_sdk.job_registry import JobRegistry
from forge_worker_sdk.protocol import ErrorPayload, WireRequest, WireResponse

DEFAULT_SHUTDOWN_DELAY_MS = 100
MAX_SHUTDOWN_DELAY_MS = 5 * 60 * 1000

EventSink = Any  # Callable[[WireEvent], None] — kept loose for handler signature


@runtime_checkable
class WorkerHandler(Protocol):
    def handle_method(
        self,
        req_id: str,
        method: str,
        params: Any,
        event_sink: EventSink,
        registry: JobRegistry,
    ) -> WireResponse: ...

    def worker_version(self) -> str: ...

    def features(self) -> list[str]: ...

    def max_concurrent_jobs(self) -> int:
        return 1


def ok_response(req_id: str, payload: Any) -> WireResponse:
    return WireResponse(id=req_id, ok=True, payload=payload)


def err_response(req_id: str, code: str, message: str) -> WireResponse:
    return WireResponse(
        id=req_id,
        ok=False,
        error=ErrorPayload(code=code, message=message),
    )


def unknown_method(req_id: str, method: str) -> WireResponse:
    return err_response(req_id, "UNKNOWN_METHOD", f"unknown method: {method}")


def _str_param(params: Any, key: str) -> str:
    if not isinstance(params, dict):
        return ""
    v = params.get(key)
    return str(v) if v is not None else ""


def _shutdown_delay_ms(params: Any) -> int:
    if not isinstance(params, dict) or "delay_ms" not in params:
        return DEFAULT_SHUTDOWN_DELAY_MS
    v = params["delay_ms"]
    try:
        n = int(v)
    except (TypeError, ValueError):
        return DEFAULT_SHUTDOWN_DELAY_MS
    if n < 0:
        return DEFAULT_SHUTDOWN_DELAY_MS
    return min(n, MAX_SHUTDOWN_DELAY_MS)


class BaseDispatcher:
    def __init__(self, handler: WorkerHandler, encoding: Encoding) -> None:
        self.handler = handler
        self.registry = JobRegistry()
        self.encoding = encoding
        self._start = time.monotonic()
        self._accepting = True
        self._test_mode = os.environ.get("FORGE_WORKER_TEST", "").strip() in {
            "1",
            "true",
            "yes",
        }

    def dispatch(self, req: WireRequest, event_sink: EventSink) -> WireResponse:
        method = req.method
        params = req.params
        rid = req.id

        if method == "ping":
            return ok_response(rid, {"pong": True})

        if method == "health":
            active = self.registry.active_count()
            return ok_response(
                rid,
                {
                    "status": "busy" if active > 0 else "ok",
                    "active_jobs": active,
                    "uptime_secs": int(time.monotonic() - self._start),
                    "pid": os.getpid(),
                    "version": self.handler.worker_version(),
                },
            )

        if method == "capabilities":
            max_jobs = 1
            if hasattr(self.handler, "max_concurrent_jobs"):
                max_jobs = int(self.handler.max_concurrent_jobs())
            return ok_response(
                rid,
                {
                    "version": self.handler.worker_version(),
                    "protocol_version": 1,
                    "features": list(self.handler.features()),
                    "max_concurrent_jobs": max_jobs,
                    "encoding": self.encoding.wire_name(),
                },
            )

        if method == "shutdown":
            self._accepting = False
            delay_ms = _shutdown_delay_ms(params)
            if not self._test_mode:

                def _exit() -> None:
                    time.sleep(delay_ms / 1000.0)
                    os._exit(0)

                threading.Thread(target=_exit, name="forge-shutdown", daemon=True).start()
            return ok_response(rid, {"bye": True})

        if method == "cancel_job":
            job_id = _str_param(params, "job_id")
            if self.registry.cancel(job_id):
                return ok_response(rid, {"cancelled": True, "job_id": job_id})
            return err_response(rid, "JOB_NOT_FOUND", f"job {job_id} not found")

        if method == "job_status":
            job_id = _str_param(params, "job_id")
            st = self.registry.status(job_id)
            if st is None:
                return err_response(rid, "JOB_NOT_FOUND", f"job {job_id} not found")
            return ok_response(rid, st.to_dict())

        return self.handler.handle_method(
            rid, method, params, event_sink, self.registry
        )
