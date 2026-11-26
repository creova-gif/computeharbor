from __future__ import annotations

import json
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest
from computeharbor_worker.client import ControlPlaneClient, ControlPlaneError
from computeharbor_worker.daemon import run_worker_daemon


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def _wait_for_healthz(
    client: ControlPlaneClient,
    proc: subprocess.Popen[str],
    timeout_sec: float = 10.0,
) -> dict[str, object]:
    deadline = time.perf_counter() + timeout_sec
    last_err: Exception | None = None
    while time.perf_counter() < deadline:
        if proc.poll() is not None:
            stderr = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(
                f"Control plane exited early with code {proc.returncode}: {stderr}"
            )
        try:
            resp = client.healthz()
            if resp.get("status") == "ok":
                return resp
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for control plane /healthz: {last_err}")


def test_auth_and_billing_e2e(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COMPUTEHARBOR_API_KEY", raising=False)

    repo_root = Path(__file__).resolve().parents[2]
    binary_path = tmp_path / "computeharbor"

    subprocess.run(
        ["go", "build", "-o", str(binary_path), "./control-plane/cmd/computeharbor"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )

    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    api_key = "e2e-secret-token"
    org_id = "org-e2e"

    server_proc = subprocess.Popen(
        [str(binary_path), "serve", "-addr", f"127.0.0.1:{port}", "-api-key", api_key],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        unauth_client = ControlPlaneClient(base_url, org_id=org_id)
        health = _wait_for_healthz(unauth_client, server_proc)
        assert health.get("status") == "ok"

        with pytest.raises(ControlPlaneError) as exc_info:
            unauth_client.submit_job(
                job_id="e2e-unauth-job",
                gpu="nvidia-l4-24gb",
                target_steps=10,
                max_preemptions=1,
            )
        assert exc_info.value.status_code == 401

        auth_client = ControlPlaneClient(base_url, org_id=org_id, api_key=api_key)
        job_id = "e2e-auth-billing-job-1"
        target_steps = 20
        submitted = auth_client.submit_job(
            job_id=job_id,
            gpu="nvidia-h100-80gb",
            target_steps=target_steps,
            max_preemptions=2,
        )
        assert submitted["status"] == "QUEUED"

        worker_client = ControlPlaneClient(base_url, org_id=org_id)
        result = run_worker_daemon(
            client=worker_client,
            job_id=job_id,
            worker_node_id="worker-e2e-1",
            checkpoint_dir=tmp_path / "ckpt",
            target_steps=target_steps,
            heartbeat_interval_sec=0.05,
            api_key=api_key,
        )
        assert result.summary.completed_step == target_steps
        assert not result.summary.preempted

        job_state = auth_client.get_job(job_id)
        assert job_state["status"] == "COMPLETED"

        billing_req = urllib.request.Request(
            f"{base_url}/v1/billing/summary",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {api_key}",
                "X-Org-ID": org_id,
            },
            method="GET",
        )
        with urllib.request.urlopen(billing_req, timeout=5.0) as billing_resp:
            assert billing_resp.status == 200
            billing_data = json.loads(billing_resp.read().decode("utf-8"))

        assert billing_data["total_jobs"] == 1
        assert billing_data["completed_jobs"] == 1
        assert billing_data["net_savings_fraction"] >= 0.60

        metrics_req = urllib.request.Request(
            f"{base_url}/metrics",
            headers={"Authorization": f"Bearer {api_key}"},
            method="GET",
        )
        with urllib.request.urlopen(metrics_req, timeout=5.0) as metrics_resp:
            assert metrics_resp.status == 200
            metrics_body = metrics_resp.read().decode("utf-8")

        assert 'computeharbor_jobs_total{status="COMPLETED"} 1' in metrics_body
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
