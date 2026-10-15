"""Unix socket and stdio framed IPC servers."""

from __future__ import annotations

import os
import socket
import sys
import threading
import time

from forge_worker_sdk.dispatcher import BaseDispatcher, WorkerHandler
from forge_worker_sdk.framing import (
    KIND_EVENT,
    KIND_REQUEST,
    KIND_RESPONSE,
    Encoding,
    encode_frame,
    marshal,
    try_decode_frame,
    unmarshal,
)
from forge_worker_sdk.protocol import WireEvent, WireRequest


def run_worker(socket_path: str, handler: WorkerHandler, encoding: Encoding) -> None:
    """Listen on a Unix domain socket until process exit."""
    dispatcher = BaseDispatcher(handler, encoding)
    _start_gc(dispatcher)

    if os.path.exists(socket_path):
        os.remove(socket_path)

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(socket_path)
    srv.listen(16)
    try:
        while True:
            conn, _ = srv.accept()
            threading.Thread(
                target=_handle_socket,
                args=(conn, dispatcher, encoding),
                daemon=True,
            ).start()
    finally:
        srv.close()
        try:
            os.remove(socket_path)
        except OSError:
            pass


def run_worker_stdio(handler: WorkerHandler, encoding: Encoding) -> None:
    """Serve framed IPC on stdin/stdout. Logs must go to stderr only."""
    dispatcher = BaseDispatcher(handler, encoding)
    _start_gc(dispatcher)
    # Use raw OS fds — BufferedReader.read can interact poorly with piped IPC.
    _handle_raw_stdio(dispatcher, encoding)


def _handle_raw_stdio(dispatcher: BaseDispatcher, encoding: Encoding) -> None:
    write_lock = threading.Lock()
    buf = bytearray()
    in_fd = sys.stdin.fileno()
    out_fd = sys.stdout.fileno()

    def send_frame(kind: int, obj: dict) -> None:
        payload = marshal(encoding, obj)
        data = encode_frame(kind, payload)
        with write_lock:
            view = memoryview(data)
            while len(view):
                n = os.write(out_fd, view)
                view = view[n:]

    def event_sink(ev: WireEvent) -> None:
        send_frame(KIND_EVENT, ev.to_dict())

    def dispatch_one(req: WireRequest) -> None:
        resp = dispatcher.dispatch(req, event_sink)
        send_frame(KIND_RESPONSE, resp.to_dict())

    while True:
        try:
            chunk = os.read(in_fd, 65536)
        except OSError:
            break
        if not chunk:
            break
        buf.extend(chunk)
        while True:
            try:
                frame = try_decode_frame(buf)
            except ValueError:
                return
            if frame is None:
                break
            if frame.kind != KIND_REQUEST:
                continue
            raw = unmarshal(encoding, frame.payload)
            req = WireRequest.from_dict(raw)
            threading.Thread(
                target=dispatch_one, args=(req,), daemon=True, name=f"rpc-{req.id}"
            ).start()


def _start_gc(dispatcher: BaseDispatcher) -> None:
    def loop() -> None:
        while True:
            time.sleep(60)
            dispatcher.registry.gc(50)

    threading.Thread(target=loop, name="forge-gc", daemon=True).start()


def _handle_socket(
    conn: socket.socket, dispatcher: BaseDispatcher, encoding: Encoding
) -> None:
    write_lock = threading.Lock()
    buf = bytearray()

    def send_frame(kind: int, obj: dict) -> None:
        payload = marshal(encoding, obj)
        data = encode_frame(kind, payload)
        with write_lock:
            conn.sendall(data)

    def event_sink(ev: WireEvent) -> None:
        send_frame(KIND_EVENT, ev.to_dict())

    def dispatch_one(req: WireRequest) -> None:
        resp = dispatcher.dispatch(req, event_sink)
        send_frame(KIND_RESPONSE, resp.to_dict())

    try:
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            buf.extend(chunk)
            while True:
                try:
                    frame = try_decode_frame(buf)
                except ValueError:
                    return
                if frame is None:
                    break
                if frame.kind != KIND_REQUEST:
                    continue
                raw = unmarshal(encoding, frame.payload)
                req = WireRequest.from_dict(raw)
                threading.Thread(
                    target=dispatch_one,
                    args=(req,),
                    daemon=True,
                    name=f"rpc-{req.id}",
                ).start()
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass