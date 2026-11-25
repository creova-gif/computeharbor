from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any


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
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.org_id = org_id
        self.timeout_sec = timeout_sec

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

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(err_body)
                msg = str(parsed.get("error", err_body)) if isinstance(parsed, dict) else err_body
            except json.JSONDecodeError:
                msg = err_body
            raise ControlPlaneError(exc.code, msg) from exc

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
