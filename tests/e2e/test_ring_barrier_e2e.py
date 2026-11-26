from __future__ import annotations

import socket
import subprocess
import time
from pathlib import Path

import pytest
from computeharbor_worker.client import ControlPlaneClient, ControlPlaneError


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def _wait_for_healthz(
    client: ControlPlaneClient,
    proc: subprocess.Popen[str],
    timeout_sec: float = 10.0,
) -> None:
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
                return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for control plane /healthz: {last_err}")


def test_ring_barrier_two_ranks_and_world_size_mismatch_e2e(tmp_path: Path) -> None:
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
    client = ControlPlaneClient(base_url=base_url, org_id="org-ring-e2e")

    server_proc = subprocess.Popen(
        [str(binary_path), "serve", "-addr", f"127.0.0.1:{port}"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_healthz(client, server_proc)

        job_id = "e2e-ring-barrier-job-1"
        submitted = client.submit_job(
            job_id=job_id,
            gpu="nvidia-h100-80gb",
            target_steps=100,
            max_preemptions=3,
        )
        assert submitted["status"] == "QUEUED"

        # Rank 0 commits step 10 -> not all ready yet.
        resp_rank0 = client.barrier(job_id, world_size=2, rank=0, step=10)
        assert resp_rank0["job_id"] == job_id
        assert resp_rank0["world_size"] == 2
        assert resp_rank0["rank"] == 0
        assert resp_rank0["step"] == 10
        assert resp_rank0["all_ready"] is False
        assert resp_rank0["committed_step"] == -1

        # Rank 1 commits step 10 -> all ranks ready, barrier advances to step 10.
        resp_rank1 = client.barrier(job_id, world_size=2, rank=1, step=10)
        assert resp_rank1["job_id"] == job_id
        assert resp_rank1["world_size"] == 2
        assert resp_rank1["rank"] == 1
        assert resp_rank1["step"] == 10
        assert resp_rank1["all_ready"] is True
        assert resp_rank1["committed_step"] == 10

        # Mismatched world_size returns 409 Conflict.
        with pytest.raises(ControlPlaneError) as exc_info:
            client.barrier(job_id, world_size=4, rank=0, step=20)
        assert exc_info.value.status_code == 409
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
