"""Length-prefixed framing: `[u32 BE length][u8 kind][payload]`."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

KIND_REQUEST = 1
KIND_RESPONSE = 2
KIND_EVENT = 3
MAX_FRAME_PAYLOAD = 64 * 1024 * 1024


class Encoding(Enum):
    MSGPACK = "msgpack"
    JSON = "json"

    @classmethod
    def from_flag(cls, s: str) -> Encoding:
        if s.lower() == "json":
            return cls.JSON
        return cls.MSGPACK

    def wire_name(self) -> str:
        return self.value


@dataclass
class Frame:
    kind: int
    payload: bytes


def encode_frame(kind: int, payload: bytes) -> bytes:
    if len(payload) > MAX_FRAME_PAYLOAD:
        raise ValueError(f"frame payload too large: {len(payload)}")
    return len(payload).to_bytes(4, "big") + bytes([kind]) + payload


def try_decode_frame(buf: bytearray) -> Frame | None:
    if len(buf) < 5:
        return None
    length = int.from_bytes(buf[0:4], "big")
    kind = buf[4]
    if length > MAX_FRAME_PAYLOAD:
        raise ValueError(f"frame payload too large: {length}")
    if len(buf) < 5 + length:
        return None
    payload = bytes(buf[5 : 5 + length])
    del buf[: 5 + length]
    return Frame(kind=kind, payload=payload)


def marshal(encoding: Encoding, obj: Any) -> bytes:
    if encoding is Encoding.JSON:
        import json

        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    import msgpack

    return msgpack.packb(obj, use_bin_type=True)


def unmarshal(encoding: Encoding, data: bytes) -> Any:
    if encoding is Encoding.JSON:
        import json

        return json.loads(data.decode("utf-8"))
    import msgpack

    return msgpack.unpackb(data, raw=False)
