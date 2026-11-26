from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from computeharbor_worker.client import ControlPlaneClient
from computeharbor_worker.models import CheckpointManifest, LoRAConfig, TrainingRunSummary
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


def test_remote_migration_e2e_across_fresh_node_dirs(tmp_path: Path) -> None:
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
    org_id = "org-remote-migration-e2e"
    client = ControlPlaneClient(base_url=base_url, org_id=org_id)

    server_proc = subprocess.Popen(
        [str(binary_path), "serve", "-addr", f"127.0.0.1:{port}"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_healthz(client, server_proc)

        target_steps = 20
        job_id = "e2e-remote-migration-job-1"

        baseline = LoRATrainer(
            job_id="e2e-remote-baseline",
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

        env = os.environ.copy()
        worker_src = str(repo_root / "worker" / "src")
        existing_py = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{worker_src}:{existing_py}" if existing_py else worker_src

        node1_dir = tmp_path / "node1_ckpt"
        node2_dir = tmp_path / "node2_ckpt"
        mirror_dir = tmp_path / "mirror_store"
        ready_1 = tmp_path / "ready_1.txt"
        summary_1_path = tmp_path / "summary_1.json"

        daemon_1 = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "computeharbor_worker.daemon",
                "--control-plane-url",
                base_url,
                "--org-id",
                org_id,
                "--job-id",
                job_id,
                "--worker-node-id",
                "spot-node-1",
                "--checkpoint-dir",
                str(node1_dir),
                "--remote-store-dir",
                str(mirror_dir),
                "--target-steps",
                str(target_steps),
                "--heartbeat-interval-sec",
                "0.05",
                "--step-delay-sec",
                "0.015",
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
        time.sleep(0.09)

        os.kill(daemon_1.pid, signal.SIGTERM)
        _, stderr_1 = daemon_1.communicate(timeout=10.0)
        assert daemon_1.returncode == 0, f"Node 1 daemon failed: {stderr_1}"

        summary_1 = TrainingRunSummary.model_validate_json(
            summary_1_path.read_text(encoding="utf-8")
        )
        assert summary_1.preempted is True
        assert 0 < summary_1.completed_step < target_steps

        node1_latest = node1_dir / "latest.json"
        assert node1_latest.is_file()
        node1_manifest = CheckpointManifest.model_validate_json(
            node1_latest.read_text(encoding="utf-8")
        )
        assert node1_manifest.step == summary_1.completed_step
        assert (mirror_dir / "latest.json").is_file()
        assert (mirror_dir / job_id / node1_manifest.safetensors_filename).is_file()

        migrating_lease = client.get_job(job_id)
        assert migrating_lease["status"] == "MIGRATING"
        assert migrating_lease["preemption_count"] == 1
        assert migrating_lease["current_step"] == summary_1.completed_step
        assert "last_heartbeat_at" in migrating_lease

        node2_dir.mkdir(parents=True, exist_ok=True)
        assert list(node2_dir.iterdir()) == []

        summary_2_path = tmp_path / "summary_2.json"
        daemon_2 = subprocess.run(
            [
                sys.executable,
                "-m",
                "computeharbor_worker.daemon",
                "--control-plane-url",
                base_url,
                "--org-id",
                org_id,
                "--job-id",
                job_id,
                "--worker-node-id",
                "spot-node-2",
                "--checkpoint-dir",
                str(node2_dir),
                "--remote-store-dir",
                str(mirror_dir),
                "--target-steps",
                str(target_steps),
                "--heartbeat-interval-sec",
                "0.05",
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
        assert daemon_2.returncode == 0

        summary_2 = TrainingRunSummary.model_validate_json(
            summary_2_path.read_text(encoding="utf-8")
        )
        assert summary_2.preempted is False
        assert summary_2.start_step == summary_1.completed_step
        assert summary_2.completed_step == target_steps

        final_lease = client.get_job(job_id)
        assert final_lease["status"] == "COMPLETED"
        assert final_lease["current_step"] == target_steps
        assert final_lease["preemption_count"] == 1
        assert final_lease["worker_node_id"] == "spot-node-2"

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
