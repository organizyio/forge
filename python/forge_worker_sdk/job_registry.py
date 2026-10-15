"""Job tracking, cooperative cancel, and event routing."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from forge_worker_sdk.protocol import WireEvent


class JobState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class JobStatus:
    job_id: str
    state: str
    progress: Any = None
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"job_id": self.job_id, "state": self.state}
        if self.progress is not None:
            d["progress"] = self.progress
        if self.error:
            d["error"] = self.error
        return d


EventSink = Callable[[WireEvent], None]


@dataclass
class Job:
    job_id: str
    cancel_event: threading.Event = field(default_factory=threading.Event)
    state: JobState = JobState.PENDING
    started_at: float = field(default_factory=time.monotonic)
    progress: Any = None
    error_msg: str | None = None
    event_sink: EventSink | None = None

    def cancel(self) -> None:
        self.cancel_event.set()
        if self.state in (JobState.PENDING, JobState.RUNNING):
            self.state = JobState.CANCELLED

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def wait_cancelled(self, timeout: float | None = None) -> bool:
        return self.cancel_event.wait(timeout)

    def emit(self, ev: WireEvent) -> None:
        if self.event_sink is not None:
            self.event_sink(ev)


class JobRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self._total_completed = 0

    def register(self, job: Job) -> None:
        with self._lock:
            if job.job_id in self._jobs:
                raise ValueError(f"job {job.job_id} already registered")
            self._jobs[job.job_id] = job

    def set_running(self, job_id: str) -> None:
        with self._lock:
            j = self._jobs.get(job_id)
            if j:
                j.state = JobState.RUNNING

    def set_completed(self, job_id: str, progress: Any = None) -> None:
        with self._lock:
            j = self._jobs.get(job_id)
            if j:
                j.state = JobState.COMPLETED
                if progress is not None:
                    j.progress = progress
                self._total_completed += 1

    def set_failed(self, job_id: str, err: str) -> None:
        with self._lock:
            j = self._jobs.get(job_id)
            if j:
                j.state = JobState.FAILED
                j.error_msg = err
                self._total_completed += 1

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            j = self._jobs.get(job_id)
            if not j:
                return False
            j.cancel()
            return True

    def update_progress(self, job_id: str, progress: Any) -> None:
        with self._lock:
            j = self._jobs.get(job_id)
            if j:
                j.progress = progress

    def emit(self, job_id: str, ev: WireEvent) -> None:
        with self._lock:
            j = self._jobs.get(job_id)
            if j:
                j.emit(ev)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def status(self, job_id: str) -> JobStatus | None:
        with self._lock:
            j = self._jobs.get(job_id)
            if not j:
                return None
            return JobStatus(
                job_id=j.job_id,
                state=j.state.value,
                progress=j.progress,
                error=j.error_msg or "",
            )

    def active_count(self) -> int:
        with self._lock:
            return sum(
                1
                for j in self._jobs.values()
                if j.state in (JobState.PENDING, JobState.RUNNING)
            )

    def gc(self, keep_n: int = 50) -> None:
        with self._lock:
            done = [
                (jid, j.started_at)
                for jid, j in self._jobs.items()
                if j.state not in (JobState.PENDING, JobState.RUNNING)
            ]
            if len(done) <= keep_n:
                return
            done.sort(key=lambda x: x[1], reverse=True)
            for jid, _ in done[keep_n:]:
                self._jobs.pop(jid, None)
