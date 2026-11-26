from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest
from computeharbor_worker.client import ControlPlaneClient
from computeharbor_worker.models import TrainingRunSummary
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
