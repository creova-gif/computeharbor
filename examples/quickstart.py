"""ComputeHarbor typed Python 3.12 quickstart example.

Demonstrates scheduling a spot GPU pool, configuring LoRA training, and running
a supervised job with automatic preemption failover against the control plane.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from computeharbor_worker import (
    ControlPlaneClient,
    FailoverResult,
    LoRAConfig,
    run_job_with_failover,
)


def run_quickstart(
    control_plane_url: str,
    org_id: str,
    job_id: str,
    checkpoint_dir: Path,
    target_steps: int = 30,
    max_preemptions: int = 3,
) -> FailoverResult:
    """Schedule a spot GPU and run a LoRA job with automatic failover."""
    client = ControlPlaneClient(base_url=control_plane_url, org_id=org_id)
    health = client.healthz()
    print(f"Control plane health: {health}")

    placement = client.schedule(min_vram_gb=24, gpu_count=1, active_hours=4.0)
    selected_gpu = str(placement["selected"]["Quote"]["GPU"])
    print(f"Scheduled GPU pool: {selected_gpu}")

    lora_config = LoRAConfig(
        in_features=64,
        out_features=64,
        rank=8,
        alpha=16.0,
        learning_rate=5e-3,
    )

    result = run_job_with_failover(
        client=client,
        job_id=job_id,
        checkpoint_dir=checkpoint_dir,
        target_steps=target_steps,
        max_preemptions=max_preemptions,
        gpu=selected_gpu,
        config=lora_config,
    )
    print(
        f"Completed job={result.job_id} status={result.final_status} "
        f"attempts={result.attempts} preemptions={result.preemptions}"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the ComputeHarbor Python quickstart."""
    parser = argparse.ArgumentParser(description="ComputeHarbor Python Quickstart")
    parser.add_argument("--control-plane-url", default="http://localhost:8080")
    parser.add_argument("--org-id", default="org-quickstart")
    parser.add_argument("--job-id", default="job-quickstart-001")
    parser.add_argument("--checkpoint-dir", default="/tmp/computeharbor-quickstart")
    parser.add_argument("--target-steps", type=int, default=30)
    parser.add_argument("--max-preemptions", type=int, default=3)
    args = parser.parse_args(argv)

    run_quickstart(
        control_plane_url=args.control_plane_url,
        org_id=args.org_id,
        job_id=args.job_id,
        checkpoint_dir=Path(args.checkpoint_dir),
        target_steps=args.target_steps,
        max_preemptions=args.max_preemptions,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
