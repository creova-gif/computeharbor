from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Literal

MetadataProvider = Literal["gcp", "aws"]

GCP_DEFAULT_METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/preempted"
AWS_DEFAULT_METADATA_URL = "http://169.254.169.254/latest/meta-data/spot/instance-action"


class MetadataPreemptionWatcher:
    """Polls GCP or AWS instance metadata endpoints for spot preemption notices."""

    def __init__(
        self,
        provider: MetadataProvider,
        metadata_url: str | None = None,
        poll_interval_sec: float = 0.5,
        timeout_sec: float = 0.5,
        on_preempt: Callable[[], None] | None = None,
    ) -> None:
        if provider not in ("gcp", "aws"):
            raise ValueError(f"Unsupported metadata provider: {provider!r}")
        self.provider: MetadataProvider = provider
        self.metadata_url: str = metadata_url or (
            GCP_DEFAULT_METADATA_URL if provider == "gcp" else AWS_DEFAULT_METADATA_URL
        )
        self.poll_interval_sec = poll_interval_sec
        self.timeout_sec = timeout_sec
        self._on_preempt = on_preempt

        self._preempt_event = threading.Event()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def preempted(self) -> bool:
        """Return True if a preemption notice has been observed."""
        return self._preempt_event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        """Block until preemption is detected or timeout expires."""
        return self._preempt_event.wait(timeout=timeout)

    def check_once(self) -> bool:
        """Poll the metadata endpoint once and return True if preemption is signaled."""
        headers: dict[str, str] = {}
        if self.provider == "gcp":
            headers["Metadata-Flavor"] = "Google"

        req = urllib.request.Request(self.metadata_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                status = getattr(resp, "status", 200)
                if status != 200:
                    return False
                body = resp.read().decode("utf-8", errors="replace")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
            return False

        detected = self._is_preempted_response(body)
        if not detected:
            return False

        should_notify = False
        with self._lock:
            if not self._preempt_event.is_set():
                self._preempt_event.set()
                should_notify = True

        if should_notify and self._on_preempt is not None:
            self._on_preempt()
        return True

    def _is_preempted_response(self, body: str) -> bool:
        if self.provider == "gcp":
            return body.strip().upper() == "TRUE"
        stripped = body.strip()
        if not stripped:
            return False
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return False
        return isinstance(parsed, dict) and "action" in parsed

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            if self.check_once():
                return
            self._stop_event.wait(self.poll_interval_sec)

    def start(self) -> None:
        """Start background polling in a daemon thread."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._poll_loop,
                name=f"metadata-preempt-{self.provider}",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        """Stop background polling and wait for the watcher thread to exit."""
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and threading.current_thread() is not thread:
            thread.join(timeout=max(1.0, self.timeout_sec + self.poll_interval_sec))
