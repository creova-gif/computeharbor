from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

_TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def _is_transient_http_status(status_code: int) -> bool:
    return status_code in _TRANSIENT_STATUS_CODES or 500 <= status_code < 600


def _parse_retry_after(exc: urllib.error.HTTPError) -> float | None:
    if exc.headers is None:
        return None
    raw = exc.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        delay = float(str(raw).strip())
    except ValueError:
        return None
    return max(0.0, delay)


class ControlPlaneError(RuntimeError):
    """Raised when the ComputeHarbor control plane returns an HTTP error."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"Control plane HTTP {status_code}: {message}")


class ControlPlaneClient:
    """Stdlib HTTP/JSON client for the ComputeHarbor Go control plane."""

    def __init__(
        self,
        base_url: str,
        org_id: str,
        timeout_sec: float = 5.0,
        max_retries: int = 3,
        backoff_base_sec: float = 0.05,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if backoff_base_sec < 0:
            raise ValueError("backoff_base_sec must be >= 0")
        self.base_url = base_url.rstrip("/")
        self.org_id = org_id
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_base_sec = backoff_base_sec

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        include_org_header: bool = True,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data: bytes | None = None
        headers: dict[str, str] = {"Accept": "application/json"}
        if include_org_header:
            headers["X-Org-ID"] = self.org_id
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        raw = ""
        for attempt in range(self.max_retries + 1):
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8")
                break
            except urllib.error.HTTPError as exc:
                err_body = exc.read().decode("utf-8", errors="replace")
                try:
                    parsed = json.loads(err_body)
                    msg = (
                        str(parsed.get("error", err_body))
                        if isinstance(parsed, dict)
                        else err_body
                    )
                except json.JSONDecodeError:
                    msg = err_body

                if not _is_transient_http_status(exc.code) or attempt >= self.max_retries:
                    raise ControlPlaneError(exc.code, msg) from exc

                retry_after = _parse_retry_after(exc)
                delay = (
                    retry_after
                    if retry_after is not None
                    else self.backoff_base_sec * (2**attempt)
                )
                if delay > 0:
                    time.sleep(delay)
            except urllib.error.URLError:
                if attempt >= self.max_retries:
                    raise
                delay = self.backoff_base_sec * (2**attempt)
                if delay > 0:
                    time.sleep(delay)

        if not raw:
            return {}
        decoded = json.loads(raw)
        if not isinstance(decoded, dict):
            raise TypeError(
                f"Expected JSON object response from {path}, got {type(decoded).__name__}"
            )
        return decoded

    def healthz(self) -> dict[str, Any]:
        """Check control plane liveness via GET /healthz."""
        return self._request("GET", "/healthz", include_org_header=False)

    def submit_job(
        self,
        job_id: str,
        gpu: str,
        target_steps: int,
        max_preemptions: int,
    ) -> dict[str, Any]:
        """Submit a new training job lease to the control plane."""
        return self._request(
            "POST",
            "/v1/jobs",
            payload={
                "job_id": job_id,
                "org_id": self.org_id,
                "gpu": gpu,
                "target_steps": target_steps,
                "max_preemptions": max_preemptions,
            },
            include_org_header=False,
        )

    def assign_worker(self, job_id: str, worker_node_id: str) -> dict[str, Any]:
        """Assign a worker node to a queued or migrating job."""
        return self._request(
            "POST",
            f"/v1/jobs/{job_id}/assign",
            payload={"worker_node_id": worker_node_id},
        )

    def heartbeat(self, job_id: str, worker_node_id: str) -> dict[str, Any]:
        """Report a worker heartbeat for an active job lease."""
        return self._request(
            "POST",
            f"/v1/jobs/{job_id}/heartbeat",
            payload={"worker_node_id": worker_node_id},
        )

    def notify_sigterm(self, job_id: str) -> dict[str, Any]:
        """Notify the control plane that the active worker received SIGTERM."""
        return self._request("POST", f"/v1/jobs/{job_id}/sigterm")

    def commit_checkpoint(
        self,
        job_id: str,
        step: int,
        epoch: int,
        loss: float,
        safetensors_uri: str,
        manifest_uri: str,
        flush_latency_ms: int | float,
    ) -> dict[str, Any]:
        """Commit a flushed checkpoint reference for a job."""
        flushed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        return self._request(
            "POST",
            f"/v1/jobs/{job_id}/checkpoint",
            payload={
                "step": step,
                "epoch": epoch,
                "loss": loss,
                "safetensors_uri": safetensors_uri,
                "manifest_uri": manifest_uri,
                "flushed_at": flushed_at,
                "flush_latency_ms": int(round(flush_latency_ms)),
            },
        )

    def get_job(self, job_id: str) -> dict[str, Any]:
        """Fetch current lease state for a job."""
        return self._request("GET", f"/v1/jobs/{job_id}")

    def schedule(
        self,
        *,
        min_vram_gb: int,
        gpu_count: int,
        active_hours: float,
        fee_fraction: float = 0.12,
        exclude_gpus: list[str] | None = None,
    ) -> dict[str, Any]:
        """Select the best eligible spot GPU pool via POST /v1/schedule."""
        payload: dict[str, Any] = {
            "min_vram_gb": min_vram_gb,
            "gpu_count": gpu_count,
            "active_hours": active_hours,
            "fee_fraction": fee_fraction,
        }
        if exclude_gpus is not None:
            payload["exclude_gpus"] = exclude_gpus
        return self._request(
            "POST",
            "/v1/schedule",
            payload=payload,
            include_org_header=False,
        )
