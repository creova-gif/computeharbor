from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from computeharbor_worker.batch_inference import BatchCursorManifest, BatchInferenceRunner
from computeharbor_worker.client import ControlPlaneClient


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
        if path.is_file() and path.read_text(encoding="utf-8").strip():
            return
        time.sleep(0.005)
    raise TimeoutError(f"Timed out waiting for ready file {path}")


def test_batch_inference_e2e_sigterm_preemption_and_parity(tmp_path: Path) -> None:
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
    client = ControlPlaneClient(base_url=base_url, org_id="org-batch-e2e")

    server_proc = subprocess.Popen(
        [str(binary_path), "serve", "-addr", f"127.0.0.1:{port}"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_healthz(client, server_proc)

        total_records = 50
        job_id = "e2e-batch-job-1"
        prompts = [{"prompt": f"e2e-batch-prompt-{i:03d}"} for i in range(total_records)]
        prompts_file = tmp_path / "prompts.json"
        prompts_file.write_text(json.dumps(prompts), encoding="utf-8")

        baseline_runner = BatchInferenceRunner(
            job_id="e2e-batch-baseline",
            checkpoint_dir=tmp_path / "baseline_batch",
            seed=42,
            batch_size=8,
        )
        baseline_manifest = baseline_runner.run(prompts)
        baseline_bytes = baseline_runner.output_path.read_bytes()

        submitted = client.submit_job(
            job_id=job_id,
            gpu="nvidia-l4-24gb",
            target_steps=total_records,
            max_preemptions=3,
        )
        assert submitted["status"] == "QUEUED"

        assigned_1 = client.assign_worker(job_id, "batch-worker-1")
        assert assigned_1["status"] == "RUNNING"
        assert assigned_1["worker_node_id"] == "batch-worker-1"

        env = os.environ.copy()
        worker_src = str(repo_root / "worker" / "src")
        existing_py = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{worker_src}:{existing_py}" if existing_py else worker_src

        ckpt_dir = tmp_path / "e2e_batch_ckpt"
        ready_1 = tmp_path / "ready_1.txt"
        manifest_1_path = tmp_path / "manifest_1.json"

        worker_1 = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "computeharbor_worker.batch_inference",
                "--job-id",
                job_id,
                "--checkpoint-dir",
                str(ckpt_dir),
                "--prompts-file",
                str(prompts_file),
                "--seed",
                "42",
                "--batch-size",
                "8",
                "--record-delay-sec",
                "0.03",
                "--ready-file",
                str(ready_1),
                "--manifest-out",
                str(manifest_1_path),
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            _wait_for_file(ready_1)
            time.sleep(0.08)

            preempt_lease = client.notify_sigterm(job_id)
            assert preempt_lease["status"] == "CHECKPOINTING"
            assert preempt_lease["preemption_count"] == 1

            os.kill(worker_1.pid, signal.SIGTERM)
            _, stderr_1 = worker_1.communicate(timeout=15.0)
            assert worker_1.returncode == 0, f"Worker 1 failed: {stderr_1}"
        except Exception:
            worker_1.kill()
            raise

        manifest_1 = BatchCursorManifest.model_validate_json(
            manifest_1_path.read_text(encoding="utf-8")
        )
        assert manifest_1.preempted is True
        assert 0 < manifest_1.processed_count < total_records
        assert manifest_1.elapsed_ms > 0.0
        assert manifest_1.records_per_sec > 0.0
        assert manifest_1.estimated_cost_per_million_records_usd > 0.0

        commit_1 = client.commit_checkpoint(
            job_id=job_id,
            step=manifest_1.processed_count,
            epoch=0,
            loss=0.0,
            safetensors_uri=f"file://{ckpt_dir / 'output.jsonl'}",
            manifest_uri=f"file://{ckpt_dir / 'batch_cursor.manifest.json'}",
            flush_latency_ms=int(round(manifest_1.flush_duration_ms)),
        )
        assert commit_1["status"] == "MIGRATING"
        assert commit_1["current_step"] == manifest_1.processed_count

        assigned_2 = client.assign_worker(job_id, "batch-worker-2")
        assert assigned_2["status"] == "RUNNING"
        assert assigned_2["worker_node_id"] == "batch-worker-2"

        manifest_2_path = tmp_path / "manifest_2.json"
        worker_2 = subprocess.run(
            [
                sys.executable,
                "-m",
                "computeharbor_worker.batch_inference",
                "--job-id",
                job_id,
                "--checkpoint-dir",
                str(ckpt_dir),
                "--prompts-file",
                str(prompts_file),
                "--seed",
                "42",
                "--batch-size",
                "8",
                "--manifest-out",
                str(manifest_2_path),
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=True,
        )
        assert worker_2.returncode == 0

        manifest_2 = BatchCursorManifest.model_validate_json(
            manifest_2_path.read_text(encoding="utf-8")
        )
        assert manifest_2.preempted is False
        assert manifest_2.processed_count == total_records
        assert manifest_2.output_sha256 == baseline_manifest.output_sha256
        assert manifest_2.elapsed_ms > 0.0
        assert manifest_2.records_per_sec > 0.0
        assert manifest_2.estimated_cost_per_million_records_usd > 0.0

        commit_2 = client.commit_checkpoint(
            job_id=job_id,
            step=manifest_2.processed_count,
            epoch=0,
            loss=0.0,
            safetensors_uri=f"file://{ckpt_dir / 'output.jsonl'}",
            manifest_uri=f"file://{ckpt_dir / 'batch_cursor.manifest.json'}",
            flush_latency_ms=int(round(manifest_2.flush_duration_ms)),
        )
        assert commit_2["status"] == "COMPLETED"

        final_lease = client.get_job(job_id)
        assert final_lease["status"] == "COMPLETED"
        assert final_lease["current_step"] == total_records
        assert final_lease["preemption_count"] == 1

        recovered_bytes = (ckpt_dir / "output.jsonl").read_bytes()
        assert recovered_bytes == baseline_bytes
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
