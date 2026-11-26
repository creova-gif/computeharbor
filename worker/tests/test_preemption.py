from __future__ import annotations

import json
import socket
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from computeharbor_worker import MetadataPreemptionWatcher


class _MockMetadataState:
    def __init__(self) -> None:
        self.status_code: int = 200
        self.body: bytes = b"FALSE"
        self.headers_seen: list[dict[str, str]] = []
        self.lock = threading.Lock()


@contextmanager
def _metadata_server(state: _MockMetadataState) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            with state.lock:
                state.headers_seen.append(dict(self.headers.items()))
                status = state.status_code
                body = state.body

            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address[:2]
        host_str = host.decode("ascii") if isinstance(host, bytes) else host
        yield f"http://{host_str}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def test_gcp_metadata_check_once_and_header() -> None:
    state = _MockMetadataState()
    state.status_code = 200
    state.body = b"FALSE"
    callbacks: list[str] = []

    with _metadata_server(state) as base_url:
        watcher = MetadataPreemptionWatcher(
            provider="gcp",
            metadata_url=f"{base_url}/computeMetadata/v1/instance/preempted",
            on_preempt=lambda: callbacks.append("preempted"),
        )
        assert watcher.check_once() is False
        assert watcher.preempted is False
        assert callbacks == []

        with state.lock:
            state.body = b" true \n"

        assert watcher.check_once() is True
        assert watcher.preempted is True
        assert callbacks == ["preempted"]

        # Subsequent checks remain True but do not re-fire on_preempt.
        assert watcher.check_once() is True
        assert callbacks == ["preempted"]

    assert len(state.headers_seen) == 3
    assert all(h.get("Metadata-Flavor") == "Google" for h in state.headers_seen)


def test_aws_metadata_check_once_404_and_200_json() -> None:
    state = _MockMetadataState()
    state.status_code = 404
    state.body = b"Not Found"
    callbacks: list[str] = []

    with _metadata_server(state) as base_url:
        watcher = MetadataPreemptionWatcher(
            provider="aws",
            metadata_url=f"{base_url}/latest/meta-data/spot/instance-action",
            on_preempt=lambda: callbacks.append("aws-preempt"),
        )
        assert watcher.check_once() is False
        assert watcher.preempted is False
        assert callbacks == []

        # 200 without "action" or invalid JSON should not trigger preemption.
        with state.lock:
            state.status_code = 200
            state.body = b'{"state": "running"}'
        assert watcher.check_once() is False
        assert watcher.preempted is False

        with state.lock:
            state.body = b"not-json"
        assert watcher.check_once() is False
        assert watcher.preempted is False

        # 200 with valid spot instance-action JSON triggers preemption.
        with state.lock:
            state.body = json.dumps(
                {"action": "terminate", "time": "2026-11-26T00:00:00Z"}
            ).encode("utf-8")

        assert watcher.check_once() is True
        assert watcher.preempted is True
        assert callbacks == ["aws-preempt"]


def test_watcher_background_polling_and_wait() -> None:
    state = _MockMetadataState()
    state.status_code = 404
    state.body = b""
    triggered = threading.Event()

    with _metadata_server(state) as base_url:
        watcher = MetadataPreemptionWatcher(
            provider="aws",
            metadata_url=f"{base_url}/latest/meta-data/spot/instance-action",
            poll_interval_sec=0.05,
            timeout_sec=0.5,
            on_preempt=triggered.set,
        )
        watcher.start()
        try:
            assert watcher.wait(timeout=0.1) is False
            assert watcher.preempted is False

            with state.lock:
                state.status_code = 200
                state.body = b'{"action": "stop", "time": "2026-11-26T00:00:00Z"}'

            assert watcher.wait(timeout=2.0) is True
            assert watcher.preempted is True
            assert triggered.wait(timeout=1.0) is True
        finally:
            watcher.stop()


def test_watcher_handles_connection_refused_without_crashing() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        unused_port = sock.getsockname()[1]

    watcher = MetadataPreemptionWatcher(
        provider="gcp",
        metadata_url=f"http://127.0.0.1:{unused_port}/computeMetadata/v1/instance/preempted",
        timeout_sec=0.2,
    )
    assert watcher.check_once() is False
    assert watcher.preempted is False
