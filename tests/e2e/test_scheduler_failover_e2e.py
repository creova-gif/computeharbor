from __future__ import annotations

import socket
import subprocess
import time
from pathlib import Path

import pytest
from computeharbor_worker.client import ControlPlaneClient, ControlPlaneError
from computeharbor_worker.models import LoRAConfig
from computeharbor_worker.trainer import LoRATrainer


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


def test_scheduler_failover_and_job_completion_e2e(tmp_path: Path) -> None:
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
    client = ControlPlaneClient(base_url=base_url, org_id="org-sched-e2e")

    server_proc = subprocess.Popen(
        [str(binary_path), "serve", "-addr", f"127.0.0.1:{port}"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_healthz(client, server_proc)

        # 1. Initial schedule selects cheapest >=40GB pool (nvidia-a100-40gb) with target savings
        initial = client.schedule(
            min_vram_gb=40,
            gpu_count=4,
            active_hours=24.0,
        )
        selected = initial["selected"]
        assert selected["Quote"]["GPU"] == "nvidia-a100-40gb"
        assert selected["MeetsTargetSavings"] is True

        # 2. Simulate pool exhaustion of nvidia-a100-40gb -> failover selects nvidia-a100-80gb
        failover = client.schedule(
            min_vram_gb=40,
            gpu_count=4,
            active_hours=24.0,
            exclude_gpus=["nvidia-a100-40gb"],
        )
        failover_selected = failover["selected"]
        failover_gpu = failover_selected["Quote"]["GPU"]
        assert failover_gpu == "nvidia-a100-80gb"

        # Submit and complete a job on the failover-selected pool
        job_id = "e2e-sched-failover-job-1"
        target_steps = 10
        submitted = client.submit_job(
            job_id=job_id,
            gpu=failover_gpu,
            target_steps=target_steps,
            max_preemptions=2,
        )
        assert submitted["status"] == "QUEUED"
        assert submitted["gpu"] == "nvidia-a100-80gb"

        assigned = client.assign_worker(job_id, "worker-sched-1")
        assert assigned["status"] == "RUNNING"

        run_result = LoRATrainer(
            job_id=job_id,
            config=LoRAConfig(),
            checkpoint_dir=tmp_path / "ckpt",
        ).train(target_steps=target_steps, step_delay_sec=0.0)

        committed = client.commit_checkpoint(
            job_id=job_id,
            step=run_result.summary.completed_step,
            epoch=1,
            loss=run_result.summary.loss_history[-1],
            safetensors_uri=f"file://{tmp_path / 'ckpt' / 'final.safetensors'}",
            manifest_uri=f"file://{tmp_path / 'ckpt' / 'manifest.json'}",
            flush_latency_ms=10,
        )
        assert committed["status"] == "COMPLETED"

        final_state = client.get_job(job_id)
        assert final_state["status"] == "COMPLETED"
        assert final_state["gpu"] == "nvidia-a100-80gb"

        # 3. Excluding all >=40GB pools returns HTTP 422
        with pytest.raises(ControlPlaneError) as exc_info:
            client.schedule(
                min_vram_gb=40,
                gpu_count=4,
                active_hours=24.0,
                exclude_gpus=[
                    "nvidia-a100-40gb",
                    "nvidia-a100-80gb",
                    "nvidia-h100-80gb",
                ],
            )
        assert exc_info.value.status_code == 422
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
