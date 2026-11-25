# ComputeHarbor

ComputeHarbor orchestrates LoRA fine-tuning and batch inference across preemptible spot GPU pools. When a provider sends `SIGTERM` for a 30-second spot eviction, the worker flushes only the mutable LoRA adapter matrices, AdamW moment buffers, and PRNG state in Safetensors format via an atomic write-and-rename commit.

## Architecture

- [`control-plane/cmd/computeharbor/main.go`](control-plane/cmd/computeharbor/main.go): CLI for spot vs on-demand cost modeling, VRAM-filtered pool ranking, and the `serve` HTTP control plane (`computeharbor serve -addr :8080`).
- [`pkg/server/server.go`](pkg/server/server.go): Stdlib `net/http` JSON API for job submission, worker lease assignment, `SIGTERM` preemption notifications, checkpoint commits, and `X-Org-ID` tenant isolation.
- [`pkg/cost/calculator.go`](pkg/cost/calculator.go): Spot pricing catalog (NVIDIA L4, A10G, A100-40GB, A100-80GB, H100-80GB), preemption recovery overhead accounting, and net customer savings after the platform fee.
- [`pkg/orchestrator/coordinator.go`](pkg/orchestrator/coordinator.go): Thread-safe job lease state machine (`QUEUED` -> `RUNNING` -> `CHECKPOINTING` -> `MIGRATING` -> `COMPLETED`) with idempotent `SIGTERM` handling and step-regression guards.
- [`worker/src/computeharbor_worker/`](worker/src/computeharbor_worker/): Python 3.12+ worker that intercepts `SIGTERM`, syncs CUDA when available, writes LoRA weights and AdamW state to a temporary Safetensors file, calls `os.fsync`, atomically swaps `latest.json`, optionally replicates to an object-store mirror (`--remote-store-dir`), and verifies SHA-256 integrity on restore.
- [`tests/chaos/test_sigterm_preemption.py`](tests/chaos/test_sigterm_preemption.py): Subprocess chaos harness that injects three OS `SIGTERM` signals into a live training run and checks bit-for-bit loss and weight parity against an uninterrupted baseline.
- [`docs/adr/001-lora-streaming-checkpoints.md`](docs/adr/001-lora-streaming-checkpoints.md) and [`docs/adr/002-control-plane-http-api.md`](docs/adr/002-control-plane-http-api.md): Architecture Decision Records for streaming LoRA + RNG checkpoints and the stdlib HTTP control-plane API.

## Measured benchmarks

### Preemption recovery and bit-for-bit loss parity

Run on a 10-core Apple Silicon host (`Darwin arm64`, Python 3.12.12, 5 trials, 3 real `SIGTERM` preemptions per trial, 15 preemptions total):

| Metric | Value | SLA / Gate |
| --- | --- | --- |
| Step-by-step loss curve parity (30 steps, 3 `SIGTERM` kills) | 15 / 15 preemptions (100% bit-for-bit match) | 100% parity |
| Final `lora_a` and `lora_b` SHA-256 match | 5 / 5 trials exact match | 100% match |
| Safetensors + manifest `fsync` flush latency (median) | 0.737 ms (range 0.656 ms to 1.286 ms) | < 30,000 ms |
| End-to-end `SIGTERM` to checkpoint restore latency (median) | 25.147 ms (range 21.643 ms to 67.660 ms) | < 30,000 ms |

Limiter: `os.fsync` syscall latency on the local APFS volume for the Safetensors file and two JSON manifests (`step-NNNNNN.manifest.json` and `latest.json`).

### Spot vs on-demand cost savings (4 GPUs, 24 active hours, 12% savings fee)

| Pool | Provider | On-demand cost | Raw spot cost | Platform fee (12%) | Net customer cost | Gross savings | Net savings |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `nvidia-l4-24gb` | GCP (`us-central1`) | \$81.60 | \$23.05 | \$7.03 | \$30.07 | 71.76% | 63.15% |
| `nvidia-a10g-24gb` | AWS (`us-east-1`) | \$116.16 | \$34.57 | \$9.79 | \$44.36 | 70.24% | 61.81% |
| `nvidia-a100-40gb` | GCP (`us-central1`) | \$352.32 | \$105.64 | \$29.60 | \$135.25 | 70.01% | 61.61% |
| `nvidia-a100-80gb` | AWS (`us-west-2`) | \$470.40 | \$136.40 | \$40.08 | \$176.48 | 71.00% | 62.48% |
| `nvidia-h100-80gb` | GCP (`us-east4`) | \$825.60 | \$247.82 | \$69.33 | \$317.15 | 69.98% | 61.59% |

## Reproduce all checks

```bash
	# Run all linters, type checks, unit tests, and SIGTERM chaos tests
	make lint
	make test
	make chaos

	# Start the Go HTTP control plane
	go run ./control-plane/cmd/computeharbor serve -addr :8080

	# Rank spot GPU pools with >= 24 GB VRAM
	go run ./control-plane/cmd/computeharbor -rank-vram 24 -count 4 -hours 24 -fee 0.12

	# Reproduce the 5-trial SIGTERM benchmark JSON report
	PYTHONPATH=worker/src .venv/bin/python tests/chaos/test_sigterm_preemption.py
```

## Standards

See [`AGENTS.md`](AGENTS.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), [`LICENSE`](LICENSE), [`docs/adr/000-template.md`](docs/adr/000-template.md), [`docs/adr/001-lora-streaming-checkpoints.md`](docs/adr/001-lora-streaming-checkpoints.md), [`docs/adr/002-control-plane-http-api.md`](docs/adr/002-control-plane-http-api.md), and [`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md).
