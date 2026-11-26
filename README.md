# ComputeHarbor

[![CI](https://github.com/creova-gif/computeharbor/actions/workflows/ci.yml/badge.svg)](https://github.com/creova-gif/computeharbor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go 1.23+](https://img.shields.io/badge/Go-1.23+-00ADD8.svg)](go.mod)
[![Python 3.12+](https://img.shields.io/badge/Python-3.12+-3776AB.svg)](pyproject.toml)
[![Release](https://img.shields.io/badge/Release-v0.2.3-green.svg)](CHANGELOG.md)

ComputeHarbor orchestrates LoRA fine-tuning and batch inference across preemptible spot GPU pools. When a provider sends `SIGTERM` for a 30-second spot eviction, the worker flushes only the mutable LoRA adapter matrices, AdamW moment buffers, and PRNG state in Safetensors format via an atomic write-and-rename commit.

## Installation

Requires **Go 1.23+** and **Python 3.12+**.

```bash
# Build the Go control-plane CLI
go build -o bin/computeharbor ./control-plane/cmd/computeharbor

# Create a virtual environment and install the Python worker package + dev tools
uv venv .venv && uv pip install --python .venv/bin/python -e ".[dev]"
# Or with standard venv + pip:
# python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
```

Installing the Python package exposes three console scripts: `computeharbor-worker`, `computeharbor-supervisor`, and `computeharbor-batch`.

## Quickstart

Start the control plane, then run the Python quickstart ([`examples/quickstart.py`](examples/quickstart.py)) or Go client ([`examples/control_plane_client.go`](examples/control_plane_client.go)):

```bash
./bin/computeharbor serve -addr :8080 -state-file /tmp/computeharbor-state.json
.venv/bin/python examples/quickstart.py
```

## CLI & Worker Reference

### Control Plane CLI (`computeharbor`)

| Command | Purpose | Key Flags |
| --- | --- | --- |
| `computeharbor` | Estimate spot vs on-demand cost or rank pools | `-gpu`, `-count`, `-hours`, `-preemptions`, `-recovery-sec`, `-fee`, `-rank-vram` |
| `computeharbor serve` | Start the HTTP control-plane API & stale-lease reaper | `-addr`, `-reap-interval`, `-lease-ttl`, `-state-file`, `-snapshot-interval`, `-rate-limit-rps`, `-rate-limit-burst`, `-log-json`, `-webhook-url`, `-webhook-secret`, `-api-key` |
| `computeharbor status` | Query org jobs or a single job lease as JSON | `-url`, `-org` (required), `-job` |
| `computeharbor savings` | Rank multi-region spot fleet placements | `-url`, `-min-vram`, `-provider`, `-min-availability` |

### Python Worker Entrypoints

- **`computeharbor-worker`** (`computeharbor_worker.daemon`): Assigns a worker lease, streams periodic heartbeats, runs LoRA training with `SIGTERM` checkpoint interception, and commits checkpoints back to the control plane.
  ```bash
  .venv/bin/computeharbor-worker \
    --control-plane-url http://127.0.0.1:8080 \
    --org-id org-demo \
    --job-id job-001 \
    --worker-node-id worker-gpu-1 \
    --checkpoint-dir /tmp/computeharbor-ckpt \
    --target-steps 30 \
    --heartbeat-interval-sec 0.5
  ```
  Optional flags: `--remote-store-dir` (object-store mirror), `--max-checkpoints` (retained local checkpoint pairs, default `5`, `0` for unlimited), `--metadata-provider gcp|aws` (poll cloud preemption metadata and trigger `SIGTERM`, with `--metadata-url` and `--metadata-poll-sec`), `--step-delay-sec`, `--ready-file`, and `--summary-out`.
- **`computeharbor-supervisor`** (`computeharbor_worker.supervisor`): Submits a job and supervises multi-attempt spot failover across preemptions up to `--max-preemptions` (`--control-plane-url`, `--org-id`, `--job-id`, `--gpu`, `--checkpoint-dir`, `--target-steps`, `--worker-prefix`, `--remote-store-dir`, `--summary-out`).
- **`computeharbor-batch`** (`computeharbor_worker.batch_inference`): Runs deterministic, checkpointed batch inference with atomic `SIGTERM` cursor recovery (`--job-id`, `--checkpoint-dir`, `--prompts-file`, `--num-prompts`, `--seed`, `--batch-size`, `--spot-hourly-usd`, `--record-delay-sec`, `--ready-file`, `--manifest-out`).

## HTTP Control Plane API

Full OpenAPI 3.1 specification: [`docs/openapi.yaml`](docs/openapi.yaml). Tenant-scoped job and billing endpoints require the `X-Org-ID` request header (except `POST /v1/jobs`, which reads `org_id` from the JSON body).

| Method & Path | Auth / Headers | Request / Query | Response |
| --- | --- | --- | --- |
| `GET /healthz` | None | — | `200 OK` `{"status":"ok"}` |
| `GET /readyz` | None | — | `200 OK` `{"status":"ready"}` or `503 Service Unavailable` `{"status":"draining"}` |
| `GET /ui` | None | — | `200 OK` HTML status & spot savings web console ([`pkg/server/ui.go`](pkg/server/ui.go)) |
| `GET /metrics` | None | — | `200 OK` Prometheus `0.0.4` text metrics ([ADR-003](docs/adr/003-prometheus-observability.md)) |
| `POST /v1/jobs` | `Content-Type: application/json` | `job_id`, `org_id`, `gpu`, `target_steps`, `max_preemptions` | `201 Created` `JobLease` |
| `GET /v1/jobs` | `X-Org-ID` | — | `200 OK` array of org-scoped `JobLease` objects |
| `GET /v1/jobs/{id}` | `X-Org-ID` | — | `200 OK` `JobLease` |
| `POST /v1/jobs/{id}/assign` | `X-Org-ID` | `{"worker_node_id": "..."}` | `200 OK` `JobLease` (`RUNNING`) |
| `POST /v1/jobs/{id}/heartbeat` | `X-Org-ID` | `{"worker_node_id": "..."}` | `200 OK` `JobLease` with updated `last_heartbeat_at` |
| `POST /v1/jobs/{id}/sigterm` | `X-Org-ID` | — | `200 OK` `JobLease` (`CHECKPOINTING`, idempotent) |
| `POST /v1/jobs/{id}/checkpoint` | `X-Org-ID` | `step`, `epoch`, `loss`, `safetensors_uri`, `manifest_uri`, `flush_latency_ms` | `200 OK` `JobLease` (`MIGRATING` or `COMPLETED`) |
| `POST /v1/jobs/{id}/cancel` | `X-Org-ID` | — | `200 OK` `JobLease` (`FAILED`, idempotent) |
| `POST /v1/jobs/{id}/barrier` | `X-Org-ID` | `world_size`, `rank`, `step` | `200 OK` `BarrierResponse` (`all_ready`, `committed_step`) |
| `GET /v1/cost/rank` | None | `min_vram`, `count`, `hours`, `fee` | `200 OK` ranked spot GPU pool quotes |
| `GET /v1/fleet/rank` | None | `min_vram`, `provider`, `min_availability` | `200 OK` multi-region fleet placements |
| `POST /v1/schedule` | `Content-Type: application/json` | `min_vram_gb`, `gpu_count`, `active_hours`, `fee_fraction`, `exclude_gpus` | `200 OK` selected pool + fallback alternatives |
| `GET /v1/billing/summary` | `X-Org-ID` | `fee` | `200 OK` org-scoped job & preemption billing summary |

### Server Configuration Flags (`computeharbor serve`)

- `-state-file` / `-snapshot-interval`: Path for atomic coordinator JSON snapshot persistence across restarts, and background snapshot cadence when `-state-file` is set (default `5s`).
- `-reap-interval` / `-lease-ttl`: Background stale-lease reaper cadence (default `5s`) and heartbeat TTL (default `30s`).
- `-rate-limit-rps` / `-rate-limit-burst`: Per-org token-bucket rate limiting (`0` disables; default burst `20`).
- `-log-json`: Emit structured JSON HTTP request logs via `slog` to `stderr` (default `true`).
- `-webhook-url` / `-webhook-secret`: Async HTTP endpoint and HMAC-SHA256 secret for signed lifecycle webhook delivery.
- `-api-key`: Optional Bearer API key for HTTP authentication (defaults to `COMPUTEHARBOR_API_KEY`).
- `-org-api-keys`: Optional comma-separated org-scoped Bearer API keys (`org=token,...`, defaults to `COMPUTEHARBOR_ORG_API_KEYS`) that bind tokens to `X-Org-ID` tenants ([`ADR-007`](docs/adr/007-scoped-org-api-keys.md)).

## Deployment & Observability

See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for production SRE operations, incident response, key rotation, and graceful draining procedures.

Run the control plane and Prometheus locally with Docker Compose:

```bash
docker compose up --build
```

- [`docker-compose.yml`](docker-compose.yml): Starts `control-plane` on `:8080` (with `/healthz` healthcheck) and `prometheus` (`prom/prometheus:v2.55.1`) on `:9090`.
- [`Dockerfile`](Dockerfile): Multi-stage `golang:1.23-alpine` -> `gcr.io/distroless/static-debian12:nonroot` build for the `computeharbor` control plane (`serve -addr :8080`).
- [`Dockerfile.worker`](Dockerfile.worker): `python:3.12-slim` image installing `computeharbor_worker` with entrypoint `python -m computeharbor_worker.daemon`.
- [`deploy/prometheus.yml`](deploy/prometheus.yml) & [`deploy/grafana-dashboard.json`](deploy/grafana-dashboard.json): Prometheus scrape config (`5s` interval) and pre-built Grafana dashboard.
- Kubernetes manifests: [`deploy/k8s/control-plane.yaml`](deploy/k8s/control-plane.yaml) and [`deploy/k8s/worker-job.yaml`](deploy/k8s/worker-job.yaml).

## Architecture

- [`control-plane/cmd/computeharbor/main.go`](control-plane/cmd/computeharbor/main.go): CLI for cost modeling, `status`/`savings` queries, and the `serve` HTTP control plane.
- [`pkg/server/server.go`](pkg/server/server.go): Stdlib `net/http` JSON API for job leases, scheduling, billing summaries, Prometheus metrics, per-org rate limiting, and `X-Org-ID` tenant isolation.
- [`pkg/cost/calculator.go`](pkg/cost/calculator.go): Spot pricing catalog (NVIDIA L4, A10G, A100-40GB, A100-80GB, H100-80GB), multi-region fleet ranking, preemption recovery overhead accounting, and net customer savings after the platform fee.
- [`pkg/orchestrator/coordinator.go`](pkg/orchestrator/coordinator.go): Thread-safe job lease state machine (`QUEUED` -> `RUNNING` -> `CHECKPOINTING` -> `MIGRATING` -> `COMPLETED` / `FAILED`) with idempotent `SIGTERM` handling, step-regression guards, and `RingBarrier` synchronization.
- [`worker/src/computeharbor_worker/`](worker/src/computeharbor_worker/): Python 3.12+ worker that intercepts `SIGTERM`, syncs CUDA when available, writes LoRA weights and AdamW state to a temporary Safetensors file, calls `os.fsync`, atomically swaps `latest.json`, optionally replicates to an object-store mirror (`--remote-store-dir`), and verifies SHA-256 integrity on restore.
- [`tests/chaos/test_sigterm_preemption.py`](tests/chaos/test_sigterm_preemption.py): Subprocess chaos harness that injects three OS `SIGTERM` signals into a live training run and checks bit-for-bit loss and weight parity against an uninterrupted baseline.
- Architecture Decision Records: [`ADR-001`](docs/adr/001-lora-streaming-checkpoints.md), [`ADR-002`](docs/adr/002-control-plane-http-api.md), [`ADR-003`](docs/adr/003-prometheus-observability.md), [`ADR-004`](docs/adr/004-rate-limiting-and-observability.md), [`ADR-005`](docs/adr/005-event-webhooks-and-fleet-routing.md), and [`ADR-006`](docs/adr/006-readiness-and-request-correlation.md).

## Measured Benchmarks

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

### Control Plane Throughput (Go Benchmarks)

Measured on Apple M5 (`Darwin arm64`, `make bench`):

| Benchmark | Result |
| --- | --- |
| `BenchmarkCalculate-10` | `38.35 ns/op` |
| `BenchmarkRankFleet-10` | `1383 ns/op` |
| `BenchmarkCoordinatorSubmitAndHeartbeat-10` | `479.3 ns/op` |
| `BenchmarkCoordinatorReapStaleLeases-10` | `9071 ns/op` |

## Reproduce All Checks

```bash
# Run all linters, type checks, unit tests, E2E tests, and SIGTERM chaos tests
make lint
make test
make test-e2e
make chaos

# Reproduce the 5-trial SIGTERM benchmark JSON report
PYTHONPATH=worker/src .venv/bin/python tests/chaos/test_sigterm_preemption.py
```

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup, test/lint requirements, and PR guidelines. Also see [`SECURITY.md`](SECURITY.md), [`docs/RUNBOOK.md`](docs/RUNBOOK.md), [`AGENTS.md`](AGENTS.md), [`LICENSE`](LICENSE), [`docs/adr/000-template.md`](docs/adr/000-template.md), and [`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md).
