# ComputeHarbor

ComputeHarbor orchestrates LoRA fine-tuning and batch inference across preemptible spot GPU pools. When a provider sends `SIGTERM` for a 30-second spot eviction, the worker flushes only the mutable LoRA adapter matrices, AdamW moment buffers, and PRNG state in Safetensors format via an atomic write-and-rename commit.

## Architecture

- [`control-plane/cmd/computeharbor/main.go`](control-plane/cmd/computeharbor/main.go): CLI for spot vs on-demand cost modeling, VRAM-filtered pool ranking, and the `serve` HTTP control plane (`computeharbor serve -addr :8080`).
- [`pkg/server/server.go`](pkg/server/server.go): Stdlib `net/http` JSON API for job submission, worker lease assignment, `SIGTERM` preemption notifications, checkpoint commits, and `X-Org-ID` tenant isolation.
- [`pkg/cost/calculator.go`](pkg/cost/calculator.go): Spot pricing catalog (NVIDIA L4, A10G, A100-40GB, A100-80GB, H100-80GB), preemption recovery overhead accounting, and net customer savings after the platform fee.
- [`pkg/orchestrator/coordinator.go`](pkg/orchestrator/coordinator.go): Thread-safe job lease state machine (`QUEUED` -> `RUNNING` -> `CHECKPOINTING` -> `MIGRATING` -> `COMPLETED`) with idempotent `SIGTERM` handling and step-regression guards.
- [`worker/src/computeharbor_worker/`](worker/src/computeharbor_worker/): Python 3.12+ worker that intercepts `SIGTERM`, syncs CUDA when available, writes LoRA weights and AdamW state to a temporary Safetensors file, calls `os.fsync`, atomically swaps `latest.json`, optionally replicates to an object-store mirror (`--remote-store-dir`), and verifies SHA-256 integrity on restore.
- [`tests/chaos/test_sigterm_preemption.py`](tests/chaos/test_sigterm_preemption.py): Subprocess chaos harness that injects three OS `SIGTERM` signals into a live training run and checks bit-for-bit loss and weight parity against an uninterrupted baseline.
- [`docs/adr/001-lora-streaming-checkpoints.md`](docs/adr/001-lora-streaming-checkpoints.md), [`docs/adr/002-control-plane-http-api.md`](docs/adr/002-control-plane-http-api.md), and [`docs/adr/003-prometheus-observability.md`](docs/adr/003-prometheus-observability.md): Architecture Decision Records for streaming LoRA + RNG checkpoints, the stdlib HTTP control-plane API, and zero-dependency Prometheus observability.

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

## HTTP Control Plane API

Start the control plane with `go run ./control-plane/cmd/computeharbor serve -addr :8080 -state-file /tmp/computeharbor-state.json` (supports `-reap-interval`, `-lease-ttl`, and `-state-file` for atomic coordinator JSON snapshot persistence across restarts). Tenant-scoped job endpoints require the `X-Org-ID` request header (except `POST /v1/jobs`, which reads `org_id` from the JSON body).

| Method & Path | Auth / Headers | Request / Query | Response |
| --- | --- | --- | --- |
| `GET /healthz` | None | — | `200 OK` `{"status":"ok"}` |
| `GET /metrics` | None | — | `200 OK` Prometheus `0.0.4` text metrics ([ADR-003](docs/adr/003-prometheus-observability.md)) |
| `POST /v1/jobs` | `Content-Type: application/json` | `job_id`, `org_id`, `gpu`, `target_steps`, `max_preemptions` | `201 Created` `JobLease` |
| `GET /v1/jobs` | `X-Org-ID` | — | `200 OK` array of org-scoped `JobLease` objects |
| `GET /v1/jobs/{id}` | `X-Org-ID` | — | `200 OK` `JobLease` |
| `POST /v1/jobs/{id}/assign` | `X-Org-ID` | `{"worker_node_id": "..."}` | `200 OK` `JobLease` (`RUNNING`) |
| `POST /v1/jobs/{id}/heartbeat` | `X-Org-ID` | `{"worker_node_id": "..."}` | `200 OK` `JobLease` with updated `last_heartbeat_at` |
| `POST /v1/jobs/{id}/sigterm` | `X-Org-ID` | — | `200 OK` `JobLease` (`CHECKPOINTING`, idempotent) |
| `POST /v1/jobs/{id}/checkpoint` | `X-Org-ID` | `step`, `epoch`, `loss`, `safetensors_uri`, `manifest_uri`, `flush_latency_ms` | `200 OK` `JobLease` (`MIGRATING` or `COMPLETED`) |
| `GET /v1/cost/rank` | None | `min_vram`, `count`, `hours`, `fee` | `200 OK` ranked spot GPU pool quotes |

## Spot Worker Daemon

`computeharbor_worker.daemon` assigns a worker lease, streams periodic heartbeats, runs LoRA training with `SIGTERM` checkpoint interception, and commits the final or preempted checkpoint back to the control plane:

```bash
PYTHONPATH=worker/src .venv/bin/python -m computeharbor_worker.daemon \
  --control-plane-url http://127.0.0.1:8080 \
  --org-id org-demo \
  --job-id job-001 \
  --worker-node-id worker-gpu-1 \
  --checkpoint-dir /tmp/computeharbor-ckpt \
  --target-steps 30 \
  --heartbeat-interval-sec 0.5
```

Optional flags:
- `--remote-store-dir`: Replicate committed Safetensors and manifest files to a local object-store mirror directory.
- `--metadata-provider gcp|aws`: Poll GCP (`metadata.google.internal`) or AWS (`169.254.169.254`) spot preemption metadata endpoints and send `SIGTERM` to the worker on notice (with `--metadata-url` and `--metadata-poll-sec` overrides).
- `--step-delay-sec`, `--ready-file`, `--summary-out`: Training cadence, readiness probe path, and JSON summary output path.

Run the end-to-end Go control-plane + Python worker daemon tests (including `SIGTERM` preemption recovery):

```bash
make test-e2e
```

## Container Deployment

Run the control plane and Prometheus locally with Docker Compose:

```bash
docker compose up --build
```

- [`docker-compose.yml`](docker-compose.yml): Starts `control-plane` on `:8080` (with `/healthz` healthcheck) and `prometheus` (`prom/prometheus:v2.55.1`) on `:9090`.
- [`Dockerfile`](Dockerfile): Multi-stage `golang:1.23-alpine` -> `gcr.io/distroless/static-debian12:nonroot` build for the `computeharbor` control plane (`serve -addr :8080`).
- [`Dockerfile.worker`](Dockerfile.worker): `python:3.12-slim` image installing `computeharbor_worker` with entrypoint `python -m computeharbor_worker.daemon`.
- [`deploy/prometheus.yml`](deploy/prometheus.yml): Scrapes `control-plane:8080/metrics` every `5s`.

## Reproduce all checks

```bash
	# Run all linters, type checks, unit tests, and SIGTERM chaos tests
	make lint
	make test
	make chaos

	# Start the Go HTTP control plane with snapshot persistence
	go run ./control-plane/cmd/computeharbor serve -addr :8080 -state-file /tmp/computeharbor-state.json

	# Rank spot GPU pools with >= 24 GB VRAM
	go run ./control-plane/cmd/computeharbor -rank-vram 24 -count 4 -hours 24 -fee 0.12

	# Reproduce the 5-trial SIGTERM benchmark JSON report
	PYTHONPATH=worker/src .venv/bin/python tests/chaos/test_sigterm_preemption.py
```

## Standards

See [`AGENTS.md`](AGENTS.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), [`LICENSE`](LICENSE), [`docs/adr/000-template.md`](docs/adr/000-template.md), [`docs/adr/001-lora-streaming-checkpoints.md`](docs/adr/001-lora-streaming-checkpoints.md), [`docs/adr/002-control-plane-http-api.md`](docs/adr/002-control-plane-http-api.md), [`docs/adr/003-prometheus-observability.md`](docs/adr/003-prometheus-observability.md), and [`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md).
