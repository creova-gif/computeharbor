from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from computeharbor_worker import BatchCursorManifest, BatchInferenceRunner


def _wait_for_file(path: Path, timeout_sec: float = 10.0) -> None:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        if path.is_file() and path.read_text(encoding="utf-8").strip():
            return
        time.sleep(0.01)
    raise TimeoutError(f"Timed out waiting for {path}")


def test_three_preemptions_exact_jsonl_parity(tmp_path: Path) -> None:
    prompts = [f"synthetic-prompt-{i:03d}" for i in range(50)]

    baseline_dir = tmp_path / "baseline"
    baseline_runner = BatchInferenceRunner(
        job_id="batch-baseline",
        checkpoint_dir=baseline_dir,
        seed=42,
        batch_size=8,
    )
    baseline_manifest = baseline_runner.run(prompts)
    assert isinstance(baseline_manifest, BatchCursorManifest)
    assert baseline_manifest.preempted is False
    assert baseline_manifest.processed_count == 50
    assert baseline_manifest.total_records == 50

    baseline_bytes = baseline_runner.output_path.read_bytes()
    baseline_lines = baseline_bytes.decode("utf-8").splitlines()
    assert len(baseline_lines) == 50

    preempt_dir = tmp_path / "preempted"
    for stop_index in (11, 27, 41):
        runner = BatchInferenceRunner(
            job_id="batch-preempt",
            checkpoint_dir=preempt_dir,
            seed=42,
            batch_size=8,
        )
        partial = runner.run(prompts, simulate_sigterm_at_index=stop_index)
        assert partial.preempted is True
        assert partial.processed_count == stop_index
        assert partial.total_records == 50
        assert partial.flush_duration_ms < 30_000.0

    recovered_runner = BatchInferenceRunner(
        job_id="batch-preempt",
        checkpoint_dir=preempt_dir,
        seed=42,
        batch_size=8,
    )
    recovered_manifest = recovered_runner.run(prompts)
    assert recovered_manifest.preempted is False
    assert recovered_manifest.processed_count == 50
    assert recovered_manifest.output_sha256 == baseline_manifest.output_sha256

    recovered_bytes = recovered_runner.output_path.read_bytes()
    assert recovered_bytes == baseline_bytes

    recovered_lines = recovered_bytes.decode("utf-8").splitlines()
    assert recovered_lines == baseline_lines

    parsed_indices = [json.loads(line)["index"] for line in recovered_lines]
    assert parsed_indices == list(range(50))
    assert len(set(parsed_indices)) == 50


def test_real_sigterm_subprocess_parity(tmp_path: Path) -> None:
    prompts = [{"prompt": f"cli-prompt-{i:03d}"} for i in range(50)]
    prompts_file = tmp_path / "prompts.json"
    prompts_file.write_text(json.dumps(prompts), encoding="utf-8")

    baseline_dir = tmp_path / "baseline_cli"
    baseline_runner = BatchInferenceRunner(
        job_id="cli-baseline",
        checkpoint_dir=baseline_dir,
        seed=42,
        batch_size=8,
    )
    baseline_manifest = baseline_runner.run(prompts)
    baseline_bytes = baseline_runner.output_path.read_bytes()

    preempt_dir = tmp_path / "preempt_cli"
    ready_file = tmp_path / "ready.txt"
    manifest_out_1 = tmp_path / "manifest_1.json"
    manifest_out_2 = tmp_path / "manifest_2.json"

    env = os.environ.copy()
    worker_src = str(Path(__file__).resolve().parents[1] / "src")
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{worker_src}:{existing_pp}" if existing_pp else worker_src

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "computeharbor_worker.batch_inference",
            "--job-id",
            "cli-preempt",
            "--checkpoint-dir",
            str(preempt_dir),
            "--prompts-file",
            str(prompts_file),
            "--seed",
            "42",
            "--batch-size",
            "8",
            "--record-delay-sec",
            "0.03",
            "--ready-file",
            str(ready_file),
            "--manifest-out",
            str(manifest_out_1),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_file(ready_file, timeout_sec=10.0)
        time.sleep(0.08)
        os.kill(proc.pid, signal.SIGTERM)
        stdout_1, stderr_1 = proc.communicate(timeout=15.0)
    except Exception:
        proc.kill()
        raise

    assert proc.returncode == 0, f"Preempted CLI failed: {stderr_1}\n{stdout_1}"
    m1 = BatchCursorManifest.model_validate_json(manifest_out_1.read_text(encoding="utf-8"))
    assert m1.preempted is True
    assert 0 < m1.processed_count < 50

    resumed = subprocess.run(
        [
            sys.executable,
            "-m",
            "computeharbor_worker.batch_inference",
            "--job-id",
            "cli-preempt",
            "--checkpoint-dir",
            str(preempt_dir),
            "--prompts-file",
            str(prompts_file),
            "--seed",
            "42",
            "--batch-size",
            "8",
            "--manifest-out",
            str(manifest_out_2),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=15.0,
        check=True,
    )
    assert resumed.returncode == 0
    m2 = BatchCursorManifest.model_validate_json(manifest_out_2.read_text(encoding="utf-8"))
    assert m2.preempted is False
    assert m2.processed_count == 50
    assert m2.output_sha256 == baseline_manifest.output_sha256

    recovered_bytes = (preempt_dir / "output.jsonl").read_bytes()
    assert recovered_bytes == baseline_bytes
    indices = [json.loads(line)["index"] for line in recovered_bytes.decode("utf-8").splitlines()]
    assert indices == list(range(50))


def test_batch_inference_tamper_detection(tmp_path: Path) -> None:
    runner = BatchInferenceRunner(job_id="tamper-job", checkpoint_dir=tmp_path, seed=42)
    runner.run(["a", "b", "c"], simulate_sigterm_at_index=2)

    raw = bytearray(runner.output_path.read_bytes())
    raw[0] ^= 0x01
    runner.output_path.write_bytes(bytes(raw))

    resumed = BatchInferenceRunner(job_id="tamper-job", checkpoint_dir=tmp_path, seed=42)
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        resumed.restore_if_available()
