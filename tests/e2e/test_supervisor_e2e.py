from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
from computeharbor_worker.client import ControlPlaneClient
from computeharbor_worker.models import LoRAConfig, TrainingRunSummary
from computeharbor_worker.supervisor import FailoverResult, run_job_with_failover
from computeharbor_worker.trainer import LoRATrainer, RunResult


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


def test_supervisor_e2e_multi_attempt_failover_and_loss_parity(tmp_path: Path) -> None:
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
    org_id = "org-supervisor-e2e"
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

        target_steps = 30
        job_id = "e2e-supervisor-job-1"

        baseline = LoRATrainer(
            job_id="e2e-supervisor-baseline",
            config=LoRAConfig(),
            checkpoint_dir=tmp_path / "baseline_ckpt",
        ).train(target_steps=target_steps, step_delay_sec=0.0)

        env = os.environ.copy()
        worker_src = str(repo_root / "worker" / "src")
        existing_py = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{worker_src}:{existing_py}" if existing_py else worker_src

        ckpt_dir = tmp_path / "supervisor_ckpt"
        remote_dir = tmp_path / "supervisor_remote"

        def run_attempt(worker_node_id: str, attempt: int) -> RunResult:
            should_preempt = attempt <= 2
            ready_file = tmp_path / f"ready_{attempt}.txt"
            summary_path = tmp_path / f"summary_{attempt}.json"
            cmd = [
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
                worker_node_id,
                "--checkpoint-dir",
                str(ckpt_dir),
                "--remote-store-dir",
                str(remote_dir),
                "--target-steps",
                str(target_steps),
                "--heartbeat-interval-sec",
                "0.05",
                "--step-delay-sec",
                "0.02" if should_preempt else "0.0",
                "--summary-out",
                str(summary_path),
            ]
            if should_preempt:
                cmd.extend(["--ready-file", str(ready_file)])
                proc = subprocess.Popen(
                    cmd,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                _wait_for_file(ready_file)
                time.sleep(0.10)
                os.kill(proc.pid, signal.SIGTERM)
                _, stderr = proc.communicate(timeout=10.0)
                assert proc.returncode == 0, f"Attempt {attempt} daemon failed: {stderr}"
            else:
                subprocess.run(
                    cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True,
                )

            summary = TrainingRunSummary.model_validate_json(
                summary_path.read_text(encoding="utf-8")
            )
            empty = np.zeros((0, 0), dtype=np.float64)
            return RunResult(summary=summary, lora_a=empty, lora_b=empty, manifest=None)

        failover_result = run_job_with_failover(
            client=client,
            job_id=job_id,
            checkpoint_dir=ckpt_dir,
            target_steps=target_steps,
            max_preemptions=3,
            worker_prefix="spot-worker",
            worker_runner=run_attempt,
        )

        assert isinstance(failover_result, FailoverResult)
        assert failover_result.job_id == job_id
        assert failover_result.attempts == 3
        assert failover_result.preemptions == 2
        assert failover_result.final_status == "COMPLETED"
        assert failover_result.loss_history == baseline.summary.loss_history
        assert failover_result.adapter_a_sha256 == baseline.summary.adapter_a_sha256
        assert failover_result.adapter_b_sha256 == baseline.summary.adapter_b_sha256
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
