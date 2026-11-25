from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from computeharbor_worker.client import ControlPlaneClient
from computeharbor_worker.models import LoRAConfig, TrainingRunSummary
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


def _wait_for_file(path: Path, timeout_sec: float = 10.0) -> None:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        if path.is_file():
            return
        time.sleep(0.005)
    raise TimeoutError(f"Timed out waiting for ready file {path}")


def test_control_plane_e2e_sigterm_preemption_and_loss_parity(tmp_path: Path) -> None:
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
    client = ControlPlaneClient(base_url=base_url, org_id="org-e2e-test")

    server_proc = subprocess.Popen(
        [str(binary_path), "serve", "-addr", f"127.0.0.1:{port}"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_healthz(client, server_proc)

        target_steps = 30
        job_id = "e2e-lora-job-1"

        baseline = LoRATrainer(
            job_id="e2e-baseline",
            config=LoRAConfig(),
            checkpoint_dir=tmp_path / "baseline_ckpt",
        ).train(target_steps=target_steps, step_delay_sec=0.0)

        submitted = client.submit_job(
            job_id=job_id,
            gpu="nvidia-l4-24gb",
            target_steps=target_steps,
            max_preemptions=3,
        )
        assert submitted["status"] == "QUEUED"

        assigned_1 = client.assign_worker(job_id, "worker-node-1")
        assert assigned_1["status"] == "RUNNING"
        assert assigned_1["worker_node_id"] == "worker-node-1"

        env = os.environ.copy()
        worker_src = str(repo_root / "worker" / "src")
        existing_py = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{worker_src}:{existing_py}" if existing_py else worker_src

        ckpt_dir = tmp_path / "e2e_ckpt"
        remote_dir = tmp_path / "e2e_remote"
        ready_1 = tmp_path / "ready_1.txt"
        summary_1_path = tmp_path / "summary_1.json"

        worker_1 = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "computeharbor_worker.trainer",
                "--job-id",
                job_id,
                "--checkpoint-dir",
                str(ckpt_dir),
                "--remote-store-dir",
                str(remote_dir),
                "--target-steps",
                str(target_steps),
                "--step-delay-sec",
                "0.02",
                "--ready-file",
                str(ready_1),
                "--summary-out",
                str(summary_1_path),
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        _wait_for_file(ready_1)
        time.sleep(0.08)

        preempt_lease = client.notify_sigterm(job_id)
        assert preempt_lease["status"] == "CHECKPOINTING"
        assert preempt_lease["preemption_count"] == 1

        os.kill(worker_1.pid, signal.SIGTERM)
        _, stderr_1 = worker_1.communicate(timeout=10.0)
        assert worker_1.returncode == 0, f"Worker 1 failed: {stderr_1}"

        summary_1 = TrainingRunSummary.model_validate_json(
            summary_1_path.read_text(encoding="utf-8")
        )
        assert summary_1.preempted is True
        assert 0 < summary_1.completed_step < target_steps

        safetensors_name = str(summary_1.checkpoint_metadata["safetensors_filename"])
        commit_1 = client.commit_checkpoint(
            job_id=job_id,
            step=summary_1.completed_step,
            epoch=summary_1.completed_step // 10,
            loss=summary_1.loss_history[-1],
            safetensors_uri=f"file://{ckpt_dir / safetensors_name}",
            manifest_uri=f"file://{ckpt_dir / 'latest.json'}",
            flush_latency_ms=int(round(summary_1.flush_duration_ms)),
        )
        assert commit_1["status"] == "MIGRATING"
        assert commit_1["current_step"] == summary_1.completed_step

        assigned_2 = client.assign_worker(job_id, "worker-node-2")
        assert assigned_2["status"] == "RUNNING"
        assert assigned_2["worker_node_id"] == "worker-node-2"

        summary_2_path = tmp_path / "summary_2.json"
        worker_2 = subprocess.run(
            [
                sys.executable,
                "-m",
                "computeharbor_worker.trainer",
                "--job-id",
                job_id,
                "--checkpoint-dir",
                str(ckpt_dir),
                "--remote-store-dir",
                str(remote_dir),
                "--target-steps",
                str(target_steps),
                "--step-delay-sec",
                "0.0",
                "--summary-out",
                str(summary_2_path),
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        assert worker_2.returncode == 0

        summary_2 = TrainingRunSummary.model_validate_json(
            summary_2_path.read_text(encoding="utf-8")
        )
        assert summary_2.preempted is False
        assert summary_2.start_step == summary_1.completed_step
        assert summary_2.completed_step == target_steps

        final_safetensors = str(summary_2.checkpoint_metadata["safetensors_filename"])
        commit_2 = client.commit_checkpoint(
            job_id=job_id,
            step=summary_2.completed_step,
            epoch=summary_2.completed_step // 10,
            loss=summary_2.loss_history[-1],
            safetensors_uri=f"file://{ckpt_dir / final_safetensors}",
            manifest_uri=f"file://{ckpt_dir / 'latest.json'}",
            flush_latency_ms=int(round(summary_2.flush_duration_ms)),
        )
        assert commit_2["status"] == "COMPLETED"

        final_lease = client.get_job(job_id)
        assert final_lease["status"] == "COMPLETED"
        assert final_lease["current_step"] == target_steps
        assert final_lease["preemption_count"] == 1

        assert summary_2.loss_history == baseline.summary.loss_history
        assert summary_2.adapter_a_sha256 == baseline.summary.adapter_a_sha256
        assert summary_2.adapter_b_sha256 == baseline.summary.adapter_b_sha256
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
