from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from computeharbor_worker import supervisor
from computeharbor_worker.client import ControlPlaneClient
from computeharbor_worker.models import TrainingRunSummary
from computeharbor_worker.storage import LocalMirrorObjectStore
from computeharbor_worker.supervisor import FailoverResult, run_job_with_failover
from computeharbor_worker.trainer import RunResult


class _FakeControlPlaneClient(ControlPlaneClient):
    def __init__(self, statuses: list[tuple[str, int]]) -> None:
        super().__init__(base_url="http://127.0.0.1:0", org_id="org-test")
        self._statuses = list(statuses)
        self.submitted_jobs: list[dict[str, Any]] = []
        self._poll_index = 0

    def submit_job(
        self,
        job_id: str,
        gpu: str,
        target_steps: int,
        max_preemptions: int,
    ) -> dict[str, Any]:
        payload = {
            "job_id": job_id,
            "gpu": gpu,
            "target_steps": target_steps,
            "max_preemptions": max_preemptions,
        }
        self.submitted_jobs.append(payload)
        return {"job_id": job_id, "status": "QUEUED"}

    def get_job(self, job_id: str) -> dict[str, Any]:
        status, preemptions = self._statuses[self._poll_index]
        self._poll_index += 1
        return {
            "job_id": job_id,
            "status": status,
            "preemption_count": preemptions,
        }


def _dummy_run_result(job_id: str, attempt: int, preempted: bool) -> RunResult:
    summary = TrainingRunSummary(
        job_id=job_id,
        start_step=(attempt - 1) * 10,
        completed_step=attempt * 10,
        target_steps=30,
        preempted=preempted,
        flush_duration_ms=1.0,
        loss_history=[0.5 / attempt],
        adapter_a_sha256="a" * 64,
        adapter_b_sha256="b" * 64,
    )
    empty = np.zeros((1, 1), dtype=np.float64)
    return RunResult(summary=summary, lora_a=empty, lora_b=empty, manifest=None)


def test_supervisor_failover_succeeds_after_migrating(tmp_path: Path) -> None:
    client = _FakeControlPlaneClient([("MIGRATING", 1), ("MIGRATING", 2), ("COMPLETED", 2)])
    seen_workers: list[str] = []

    def runner(worker_node_id: str, attempt: int) -> RunResult:
        seen_workers.append(worker_node_id)
        return _dummy_run_result("job-1", attempt, preempted=attempt < 3)

    result = run_job_with_failover(
        client=client,
        job_id="job-1",
        checkpoint_dir=tmp_path,
        target_steps=30,
        max_preemptions=3,
        worker_prefix="spot-node",
        worker_runner=runner,
    )

    assert isinstance(result, FailoverResult)
    assert len(client.submitted_jobs) == 1
    assert seen_workers == ["spot-node-1", "spot-node-2", "spot-node-3"]
    assert result.attempts == 3
    assert result.preemptions == 2
    assert result.final_status == "COMPLETED"
    assert result.adapter_a_sha256 == "a" * 64
    assert result.adapter_b_sha256 == "b" * 64


def test_supervisor_raises_on_failed_job_without_submitting(tmp_path: Path) -> None:
    client = _FakeControlPlaneClient([("FAILED", 1)])

    with pytest.raises(RuntimeError, match="failed on attempt 1"):
        run_job_with_failover(
            client=client,
            job_id="job-fail",
            checkpoint_dir=tmp_path,
            target_steps=10,
            max_preemptions=1,
            submit_job=False,
            worker_runner=lambda _w, attempt: _dummy_run_result("job-fail", attempt, True),
        )

    assert client.submitted_jobs == []


def test_supervisor_main_writes_summary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run_job_with_failover(**kwargs: Any) -> FailoverResult:
        captured.update(kwargs)
        return FailoverResult(
            job_id=str(kwargs["job_id"]),
            attempts=2,
            preemptions=1,
            final_status="COMPLETED",
            loss_history=[0.5, 0.25],
            adapter_a_sha256="a" * 64,
            adapter_b_sha256="b" * 64,
        )

    monkeypatch.setattr(supervisor, "run_job_with_failover", fake_run_job_with_failover)

    ckpt_dir = tmp_path / "ckpts"
    remote_dir = tmp_path / "remote"
    summary_path = tmp_path / "summary.json"
    exit_code = supervisor.main(
        [
            "--control-plane-url",
            "http://127.0.0.1:8080",
            "--org-id",
            "org-cli",
            "--job-id",
            "job-cli",
            "--gpu",
            "nvidia-a100-80gb",
            "--checkpoint-dir",
            str(ckpt_dir),
            "--target-steps",
            "50",
            "--max-preemptions",
            "4",
            "--worker-prefix",
            "spot",
            "--heartbeat-interval-sec",
            "0.25",
            "--step-delay-sec",
            "0.05",
            "--remote-store-dir",
            str(remote_dir),
            "--summary-out",
            str(summary_path),
        ]
    )

    assert exit_code == 0
    assert captured["client"].base_url == "http://127.0.0.1:8080"
    assert captured["client"].org_id == "org-cli"
    assert captured["job_id"] == "job-cli"
    assert captured["gpu"] == "nvidia-a100-80gb"
    assert captured["checkpoint_dir"] == ckpt_dir
    assert captured["target_steps"] == 50
    assert captured["max_preemptions"] == 4
    assert captured["worker_prefix"] == "spot"
    assert captured["heartbeat_interval_sec"] == 0.25
    assert captured["step_delay_sec"] == 0.05
    assert isinstance(captured["remote_store"], LocalMirrorObjectStore)

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload == {
        "job_id": "job-cli",
        "attempts": 2,
        "preemptions": 1,
        "final_status": "COMPLETED",
        "loss_history": [0.5, 0.25],
        "adapter_a_sha256": "a" * 64,
        "adapter_b_sha256": "b" * 64,
    }


def test_supervisor_main_prints_stdout_when_no_summary_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run_job_with_failover(**kwargs: Any) -> FailoverResult:
        assert kwargs["remote_store"] is None
        return FailoverResult(
            job_id=str(kwargs["job_id"]),
            attempts=1,
            preemptions=0,
            final_status="COMPLETED",
            loss_history=[0.1],
            adapter_a_sha256="c" * 64,
            adapter_b_sha256="d" * 64,
        )

    monkeypatch.setattr(supervisor, "run_job_with_failover", fake_run_job_with_failover)

    exit_code = supervisor.main(
        [
            "--control-plane-url",
            "http://127.0.0.1:8080",
            "--org-id",
            "org-stdout",
            "--job-id",
            "job-stdout",
            "--checkpoint-dir",
            str(tmp_path / "ckpts"),
        ]
    )

    assert exit_code == 0
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)
    assert payload["job_id"] == "job-stdout"
    assert payload["final_status"] == "COMPLETED"
