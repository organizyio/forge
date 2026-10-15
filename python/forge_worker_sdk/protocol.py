"""Wire envelopes aligned with Go / Rust Forge SDKs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ErrorPayload:
    code: str
    message: str
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.detail:
            d["detail"] = self.detail
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> ErrorPayload | None:
        if not d:
            return None
        return cls(
            code=str(d.get("code", "")),
            message=str(d.get("message", "")),
            detail=str(d.get("detail", "")),
        )


@dataclass
class WireRequest:
    id: str
    method: str
    params: Any = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"id": self.id, "method": self.method}
        if self.params is not None:
            d["params"] = self.params
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> WireRequest:
        return cls(id=str(d["id"]), method=str(d["method"]), params=d.get("params"))


@dataclass
class WireResponse:
    id: str
    ok: bool
    error: ErrorPayload | None = None
    payload: Any = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"id": self.id, "ok": self.ok}
        if self.error is not None:
            d["error"] = self.error.to_dict()
        if self.payload is not None:
            d["payload"] = self.payload
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> WireResponse:
        return cls(
            id=str(d["id"]),
            ok=bool(d.get("ok")),
            error=ErrorPayload.from_dict(d.get("error")),
            payload=d.get("payload"),
        )


@dataclass
class WireEvent:
    type: str
    job_id: str
    payload: Any = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"type": self.type, "job_id": self.job_id}
        if self.payload is not None:
            d["payload"] = self.payload
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> WireEvent:
        return cls(
            type=str(d.get("type", "")),
            job_id=str(d.get("job_id", "")),
            payload=d.get("payload"),
        )
