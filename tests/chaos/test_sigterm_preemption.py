from __future__ import annotations

import json
import os
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from computeharbor_worker.models import LoRAConfig, TrainingRunSummary
from computeharbor_worker.trainer import LoRATrainer


def _wait_for_file(path: Path, timeout_sec: float = 5.0) -> None:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        if path.is_file():
            return
        time.sleep(0.002)
    raise TimeoutError(f"Timed out waiting for ready file {path}")


def run_os_sigterm_chaos_trial(
    work_dir: Path,
    *,
    target_steps: int = 30,
    preemptions: int = 3,
    step_delay_sec: float = 0.015,
) -> dict[str, Any]:
    env = os.environ.copy()
    worker_src = str(Path(__file__).resolve().parents[2] / "worker" / "src")
    existing_py = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{worker_src}:{existing_py}" if existing_py else worker_src

    baseline_dir = work_dir / "baseline_ckpt"
    t0_base = time.perf_counter()
    baseline_res = LoRATrainer(
        job_id="chaos-baseline",
        config=LoRAConfig(),
        checkpoint_dir=baseline_dir,
    ).train(target_steps=target_steps, step_delay_sec=0.0)
    baseline_ms = (time.perf_counter() - t0_base) * 1000.0

    chaos_dir = work_dir / "chaos_ckpt"
    recovery_latencies_ms: list[float] = []
    flush_latencies_ms: list[float] = []
    preempted_steps: list[int] = []

    for cycle in range(1, preemptions + 1):
        ready_file = work_dir / f"ready_{cycle}.txt"
        summary_file = work_dir / f"summary_{cycle}.json"
        cmd = [
            sys.executable,
            "-m",
            "computeharbor_worker.trainer",
            "--job-id",
            "chaos-sigterm",
            "--checkpoint-dir",
            str(chaos_dir),
            "--target-steps",
            str(target_steps),
            "--step-delay-sec",
            str(step_delay_sec),
            "--ready-file",
            str(ready_file),
            "--summary-out",
            str(summary_file),
        ]
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        _wait_for_file(ready_file)
        time.sleep(step_delay_sec * 4.5)

        t_sig = time.perf_counter()
        os.kill(proc.pid, signal.SIGTERM)
        _, stderr = proc.communicate(timeout=10.0)
        if proc.returncode != 0:
            raise RuntimeError(f"Cycle {cycle} worker failed ({proc.returncode}): {stderr}")

        summary = TrainingRunSummary.model_validate_json(
            summary_file.read_text(encoding="utf-8")
        )
        if not summary.preempted:
            raise AssertionError(f"Cycle {cycle} completed before SIGTERM arrived")

        probe = LoRATrainer(
            job_id="chaos-sigterm",
            config=LoRAConfig(),
            checkpoint_dir=chaos_dir,
        )
        restored = probe.restore_if_available()
        t_recovered = (time.perf_counter() - t_sig) * 1000.0
        if not restored:
            raise RuntimeError(f"Cycle {cycle} failed to restore checkpoint")

        recovery_latencies_ms.append(round(t_recovered, 3))
        flush_latencies_ms.append(round(summary.flush_duration_ms, 3))
        preempted_steps.append(summary.completed_step)

    final_summary_file = work_dir / "summary_final.json"
    final_cmd = [
        sys.executable,
        "-m",
        "computeharbor_worker.trainer",
        "--job-id",
        "chaos-sigterm",
        "--checkpoint-dir",
        str(chaos_dir),
        "--target-steps",
        str(target_steps),
        "--step-delay-sec",
        "0.0",
        "--summary-out",
        str(final_summary_file),
    ]
    final_proc = subprocess.run(
        final_cmd,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    if final_proc.returncode != 0:
        raise RuntimeError(f"Final resume failed: {final_proc.stderr}")

    final_summary = TrainingRunSummary.model_validate_json(
        final_summary_file.read_text(encoding="utf-8")
    )

    loss_parity = final_summary.loss_history == baseline_res.summary.loss_history
    adapter_a_parity = final_summary.adapter_a_sha256 == baseline_res.summary.adapter_a_sha256
    adapter_b_parity = final_summary.adapter_b_sha256 == baseline_res.summary.adapter_b_sha256

    return {
        "target_steps": target_steps,
        "preemptions_injected": preemptions,
        "preempted_at_steps": preempted_steps,
        "loss_curve_parity_100pct": loss_parity,
        "adapter_a_sha256_parity": adapter_a_parity,
        "adapter_b_sha256_parity": adapter_b_parity,
        "initial_loss": final_summary.loss_history[0],
        "final_loss": final_summary.loss_history[-1],
        "baseline_compute_ms": round(baseline_ms, 3),
        "flush_latencies_ms": flush_latencies_ms,
        "recovery_latencies_ms": recovery_latencies_ms,
        "max_recovery_ms": max(recovery_latencies_ms),
        "meets_30s_sla": max(recovery_latencies_ms) < 30_000.0,
    }


def run_alternating_benchmark(trials: int = 5) -> dict[str, Any]:
    trial_results: list[dict[str, Any]] = []
    for i in range(trials):
        with tempfile.TemporaryDirectory(prefix=f"ch_bench_{i}_") as tmp:
            res = run_os_sigterm_chaos_trial(Path(tmp))
            if not (
                res["loss_curve_parity_100pct"]
                and res["adapter_a_sha256_parity"]
                and res["adapter_b_sha256_parity"]
            ):
                raise AssertionError(f"Parity failure on trial {i}: {res}")
            trial_results.append(res)

    all_recoveries = [ms for r in trial_results for ms in r["recovery_latencies_ms"]]
    all_flushes = [ms for r in trial_results for ms in r["flush_latencies_ms"]]
    return {
        "trials": trials,
        "total_preemptions_tested": len(all_recoveries),
        "parity_failures": 0,
        "flush_ms_median": round(statistics.median(all_flushes), 3),
        "flush_ms_min": round(min(all_flushes), 3),
        "flush_ms_max": round(max(all_flushes), 3),
        "recovery_ms_median": round(statistics.median(all_recoveries), 3),
        "recovery_ms_min": round(min(all_recoveries), 3),
        "recovery_ms_max": round(max(all_recoveries), 3),
        "sla_ceiling_ms": 30_000.0,
        "sample_trial": trial_results[0],
    }


def test_subprocess_three_os_sigterm_preemptions_parity(tmp_path: Path) -> None:
    report = run_os_sigterm_chaos_trial(tmp_path, target_steps=30, preemptions=3)
    assert report["preemptions_injected"] == 3
    assert len(report["preempted_at_steps"]) == 3
    assert report["loss_curve_parity_100pct"] is True
    assert report["adapter_a_sha256_parity"] is True
    assert report["adapter_b_sha256_parity"] is True
    assert report["meets_30s_sla"] is True
    assert report["final_loss"] < report["initial_loss"]


if __name__ == "__main__":
    sys.stdout.write(json.dumps(run_alternating_benchmark(5), indent=2) + "\n")
