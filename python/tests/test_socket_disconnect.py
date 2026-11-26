"""A worker may finish delivering job events after its gateway disconnects."""

import socket
import threading

from forge_worker_sdk.dispatcher import ok_response
from forge_worker_sdk.framing import Encoding, KIND_REQUEST, encode_frame, marshal
from forge_worker_sdk.protocol import WireEvent
from forge_worker_sdk.server import _handle_socket


def test_late_event_and_response_after_client_disconnect():
    dispatched = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    class Dispatcher:
        def dispatch(self, req, event_sink):
            dispatched.set()
            assert release.wait(3)
            event_sink(WireEvent(type="progress", job_id="j", payload={}))
            completed.set()
            return ok_response(req.id, {})

    server, client = socket.socketpair()
    thread = threading.Thread(target=_handle_socket, args=(server, Dispatcher(), Encoding.JSON))
    thread.start()
    try:
        request = {"id": "r", "method": "demo", "params": {}}
        client.sendall(encode_frame(KIND_REQUEST, marshal(Encoding.JSON, request)))
        assert dispatched.wait(3)
    finally:
        client.close()
        thread.join(3)
        release.set()
    assert not thread.is_alive()
    assert completed.wait(3)
