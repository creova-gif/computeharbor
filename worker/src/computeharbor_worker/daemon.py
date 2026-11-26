from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
from pathlib import Path

from computeharbor_worker.client import ControlPlaneClient, ControlPlaneError
from computeharbor_worker.models import LoRAConfig
from computeharbor_worker.preemption import MetadataPreemptionWatcher, MetadataProvider
from computeharbor_worker.storage import CheckpointStore, LocalMirrorObjectStore
from computeharbor_worker.trainer import LoRATrainer, RunResult


def _heartbeat_loop(
    client: ControlPlaneClient,
    job_id: str,
    worker_node_id: str,
    interval_sec: float,
    stop_event: threading.Event,
) -> None:
    """Send periodic worker heartbeats until stop_event is signaled."""
    while not stop_event.wait(interval_sec):
        try:
            client.heartbeat(job_id, worker_node_id)
        except (ControlPlaneError, OSError):
            if stop_event.is_set():
                return


def run_worker_daemon(
    *,
    client: ControlPlaneClient,
    job_id: str,
    worker_node_id: str,
    checkpoint_dir: Path | str,
    target_steps: int,
    heartbeat_interval_sec: float = 0.5,
    step_delay_sec: float = 0.0,
    ready_file: Path | None = None,
    remote_store: CheckpointStore | None = None,
    config: LoRAConfig | None = None,
    metadata_provider: MetadataProvider | None = None,
    metadata_url: str | None = None,
    metadata_poll_sec: float = 0.5,
    max_checkpoints: int = 5,
    api_key: str | None = None,
) -> RunResult:
    """Assign this worker to a job, stream heartbeats, train, and commit checkpoints."""
    if api_key is not None:
        client.api_key = api_key
    ckpt_dir = Path(checkpoint_dir)
    client.assign_worker(job_id, worker_node_id)

    stop_event = threading.Event()
    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(client, job_id, worker_node_id, heartbeat_interval_sec, stop_event),
        name=f"heartbeat-{job_id}-{worker_node_id}",
        daemon=True,
    )
    hb_thread.start()

    watcher: MetadataPreemptionWatcher | None = None
    if metadata_provider is not None:
        watcher = MetadataPreemptionWatcher(
            provider=metadata_provider,
            metadata_url=metadata_url,
            poll_interval_sec=metadata_poll_sec,
            on_preempt=lambda: os.kill(os.getpid(), signal.SIGTERM),
        )
        watcher.start()

    try:
        trainer = LoRATrainer(
            job_id=job_id,
            config=config or LoRAConfig(),
            checkpoint_dir=ckpt_dir,
            remote_store=remote_store,
            max_checkpoints=max_checkpoints,
        )
        result = trainer.train(
            target_steps=target_steps,
            step_delay_sec=step_delay_sec,
            ready_file=ready_file,
        )
    finally:
        if watcher is not None:
            watcher.stop()
        stop_event.set()
        hb_thread.join(timeout=max(1.0, heartbeat_interval_sec * 2.0))

    if result.summary.preempted:
        client.notify_sigterm(job_id)

    safetensors_name = str(result.summary.checkpoint_metadata.get("safetensors_filename", ""))
    epoch = (
        result.manifest.epoch
        if result.manifest is not None
        else result.summary.completed_step // 10
    )
    loss = result.summary.loss_history[-1] if result.summary.loss_history else 0.0
    client.commit_checkpoint(
        job_id=job_id,
        step=result.summary.completed_step,
        epoch=epoch,
        loss=loss,
        safetensors_uri=f"file://{ckpt_dir / safetensors_name}",
        manifest_uri=f"file://{ckpt_dir / 'latest.json'}",
        flush_latency_ms=int(round(result.summary.flush_duration_ms)),
    )
    return result


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running the ComputeHarbor spot worker daemon."""
    parser = argparse.ArgumentParser(description="ComputeHarbor Spot LoRA Worker Daemon")
    parser.add_argument("--control-plane-url", required=True)
    parser.add_argument("--org-id", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--worker-node-id", required=True)
    parser.add_argument("--checkpoint-dir", required=True)
    parser.add_argument("--target-steps", type=int, default=30)
    parser.add_argument("--heartbeat-interval-sec", type=float, default=0.5)
    parser.add_argument("--step-delay-sec", type=float, default=0.0)
    parser.add_argument("--ready-file", default="")
    parser.add_argument("--summary-out", default="")
    parser.add_argument("--remote-store-dir", default="")
    parser.add_argument("--metadata-provider", choices=["gcp", "aws"], default=None)
    parser.add_argument("--metadata-url", default=None)
    parser.add_argument("--metadata-poll-sec", type=float, default=0.5)
    parser.add_argument("--max-checkpoints", type=int, default=5)
    parser.add_argument("--api-key", default=None)
    args = parser.parse_args(argv)

    remote_store: CheckpointStore | None = (
        LocalMirrorObjectStore(Path(args.remote_store_dir)) if args.remote_store_dir else None
    )
    client = ControlPlaneClient(
        base_url=args.control_plane_url,
        org_id=args.org_id,
        api_key=args.api_key,
    )
    ready_path = Path(args.ready_file) if args.ready_file else None
    result = run_worker_daemon(
        client=client,
        job_id=args.job_id,
        worker_node_id=args.worker_node_id,
        checkpoint_dir=Path(args.checkpoint_dir),
        target_steps=args.target_steps,
        heartbeat_interval_sec=args.heartbeat_interval_sec,
        step_delay_sec=args.step_delay_sec,
        ready_file=ready_path,
        remote_store=remote_store,
        metadata_provider=args.metadata_provider,
        metadata_url=args.metadata_url,
        metadata_poll_sec=args.metadata_poll_sec,
        max_checkpoints=args.max_checkpoints,
    )
    output_json = result.summary.model_dump_json(indent=2)
    if args.summary_out:
        Path(args.summary_out).write_text(output_json, encoding="utf-8")
    else:
        sys.stdout.write(output_json + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
