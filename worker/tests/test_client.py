from __future__ import annotations

import json
import socket
import threading
import urllib.error
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from computeharbor_worker.client import ControlPlaneClient, ControlPlaneError


class _MockServerState:
    def __init__(self, responses: list[tuple[int, dict[str, Any], dict[str, str]]]) -> None:
        self.responses = list(responses)
        self.calls: int = 0
        self.lock = threading.Lock()


@contextmanager
def _mock_control_plane(state: _MockServerState) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._handle()

        def do_POST(self) -> None:
            content_len = int(self.headers.get("Content-Length", "0"))
            if content_len > 0:
                self.rfile.read(content_len)
            self._handle()

        def _handle(self) -> None:
            with state.lock:
                idx = min(state.calls, len(state.responses) - 1)
                status, body_obj, extra_headers = state.responses[idx]
                state.calls += 1

            raw = json.dumps(body_obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for k, v in extra_headers.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)

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


def test_client_init_validation() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        ControlPlaneClient("http://127.0.0.1:8080", "org-1", max_retries=-1)

    with pytest.raises(ValueError, match="backoff_base_sec"):
        ControlPlaneClient("http://127.0.0.1:8080", "org-1", backoff_base_sec=-0.1)


def test_retries_429_with_retry_after_zero_then_succeeds() -> None:
    state = _MockServerState(
        [
            (429, {"error": "rate limited"}, {"Retry-After": "0"}),
            (429, {"error": "rate limited"}, {"Retry-After": "0"}),
            (200, {"status": "ok"}, {}),
        ]
    )
    with _mock_control_plane(state) as base_url:
        client = ControlPlaneClient(
            base_url=base_url,
            org_id="org-test",
            max_retries=3,
            backoff_base_sec=0.01,
        )
        res = client.healthz()

    assert res == {"status": "ok"}
    assert state.calls == 3


def test_retries_503_then_succeeds() -> None:
    state = _MockServerState(
        [
            (503, {"error": "unavailable"}, {}),
            (503, {"error": "unavailable"}, {}),
            (200, {"job_id": "job-1", "state": "RUNNING"}, {}),
        ]
    )
    with _mock_control_plane(state) as base_url:
        client = ControlPlaneClient(
            base_url=base_url,
            org_id="org-test",
            max_retries=3,
            backoff_base_sec=0.005,
        )
        res = client.get_job("job-1")

    assert res == {"job_id": "job-1", "state": "RUNNING"}
    assert state.calls == 3


@pytest.mark.parametrize("status_code", [400, 403, 404, 409, 422])
def test_4xx_errors_fail_immediately_without_retrying(status_code: int) -> None:
    state = _MockServerState(
        [
            (status_code, {"error": f"client error {status_code}"}, {}),
            (200, {"status": "ok"}, {}),
        ]
    )
    with _mock_control_plane(state) as base_url:
        client = ControlPlaneClient(
            base_url=base_url,
            org_id="org-test",
            max_retries=3,
            backoff_base_sec=0.01,
        )
        with pytest.raises(ControlPlaneError) as exc_info:
            client.healthz()

    assert exc_info.value.status_code == status_code
    assert state.calls == 1


def test_exhausts_retries_on_persistent_503() -> None:
    state = _MockServerState([(503, {"error": "still down"}, {})])
    with _mock_control_plane(state) as base_url:
        client = ControlPlaneClient(
            base_url=base_url,
            org_id="org-test",
            max_retries=2,
            backoff_base_sec=0.001,
        )
        with pytest.raises(ControlPlaneError) as exc_info:
            client.healthz()

    assert exc_info.value.status_code == 503
    assert state.calls == 3


def test_retries_url_error_with_exponential_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr("computeharbor_worker.client.time.sleep", sleep_calls.append)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        unused_port = sock.getsockname()[1]

    client = ControlPlaneClient(
        base_url=f"http://127.0.0.1:{unused_port}",
        org_id="org-test",
        timeout_sec=0.2,
        max_retries=3,
        backoff_base_sec=0.05,
    )
    with pytest.raises(urllib.error.URLError):
        client.healthz()

    assert sleep_calls == [0.05, 0.1, 0.2]
