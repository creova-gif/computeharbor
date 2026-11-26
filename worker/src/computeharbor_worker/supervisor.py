from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from computeharbor_worker.client import ControlPlaneClient
from computeharbor_worker.daemon import run_worker_daemon
from computeharbor_worker.models import LoRAConfig
from computeharbor_worker.storage import CheckpointStore
from computeharbor_worker.trainer import RunResult

type WorkerRunner = Callable[[str, int], RunResult]


@dataclass(frozen=True)
class FailoverResult:
    """Outcome of a supervised multi-attempt spot training job."""

    job_id: str
    attempts: int
    preemptions: int
    final_status: str
    loss_history: list[float]
    adapter_a_sha256: str
    adapter_b_sha256: str


def run_job_with_failover(
    *,
    client: ControlPlaneClient,
    job_id: str,
    checkpoint_dir: Path | str,
    target_steps: int,
    max_preemptions: int = 3,
    gpu: str = "nvidia-l4-24gb",
    worker_prefix: str = "worker",
    submit_job: bool = True,
    heartbeat_interval_sec: float = 0.5,
    step_delay_sec: float = 0.0,
    remote_store: CheckpointStore | None = None,
    config: LoRAConfig | None = None,
    worker_runner: WorkerRunner | None = None,
) -> FailoverResult:
    """Run a job across spot preemptions until completion or failure."""
    if max_preemptions < 0:
        raise ValueError("max_preemptions must be >= 0")

    if submit_job:
        client.submit_job(
            job_id=job_id,
            gpu=gpu,
            target_steps=target_steps,
            max_preemptions=max_preemptions,
        )

    ckpt_dir = Path(checkpoint_dir)
    max_attempts = max_preemptions + 1

    for attempt in range(1, max_attempts + 1):
        worker_node_id = f"{worker_prefix}-{attempt}"
        if worker_runner is not None:
            result = worker_runner(worker_node_id, attempt)
        else:
            result = run_worker_daemon(
                client=client,
                job_id=job_id,
                worker_node_id=worker_node_id,
                checkpoint_dir=ckpt_dir,
                target_steps=target_steps,
                heartbeat_interval_sec=heartbeat_interval_sec,
                step_delay_sec=step_delay_sec,
                remote_store=remote_store,
                config=config,
            )

        lease = client.get_job(job_id)
        status = str(lease.get("status", ""))
        preemptions = int(lease.get("preemption_count", 0))

        if status == "COMPLETED":
            return FailoverResult(
                job_id=job_id,
                attempts=attempt,
                preemptions=preemptions,
                final_status=status,
                loss_history=list(result.summary.loss_history),
                adapter_a_sha256=result.summary.adapter_a_sha256,
                adapter_b_sha256=result.summary.adapter_b_sha256,
            )
        if status == "MIGRATING":
            continue
        if status == "FAILED":
            raise RuntimeError(
                f"Job {job_id} failed on attempt {attempt} (preemptions={preemptions})"
            )
        raise RuntimeError(
            f"Job {job_id} ended attempt {attempt} in unexpected status {status!r}"
        )

    raise RuntimeError(
        f"Job {job_id} did not complete within {max_attempts} attempts"
    )
