# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-11-26

### Added
- **Security Policy (`SECURITY.md`):** Supported versions (`0.2.x`, `0.1.x`), private vulnerability reporting instructions, and summary of authentication, tenant isolation, webhook signing, checkpoint integrity, and CI secret-scanning controls.
- **HTTP Security Hardening (`pkg/server`, `control-plane/`):** Slowloris HTTP server timeouts (`ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`), bounded `OrgRateLimiter` stale-bucket eviction, and constant-time Bearer API-key authentication (`-api-key` / `COMPUTEHARBOR_API_KEY`).
- **Webhook Retry Backoff & Delivery Counters (`pkg/events`):** Bounded exponential retry backoff (`WithRetry`) plus `DeliveredCount`, `FailedCount`, and `DroppedCount` counters on `WebhookDispatcher`.
- **Bounded Checkpoint Retention (`worker/`):** `CheckpointManager` `max_checkpoints` pruning (`--max-checkpoints` on `computeharbor-worker`, default `5`, `0` for unlimited) to bound local disk usage.
- **Prometheus Observability & Grafana Dashboard (`pkg/server`, `deploy/`):** `GET /metrics` endpoint emitting Prometheus `0.0.4` text metrics (`ADR-003`) and pre-built `deploy/grafana-dashboard.json`.
- **Coordinator Snapshot Persistence (`pkg/orchestrator`, `control-plane/`):** `CoordinatorSnapshot` atomic JSON persistence and `-state-file` flag on `computeharbor serve` for crash/restart recovery.
- **Presigned Object Store (`worker/`):** `HTTPPresignedStore` and `store_from_uri` factory supporting `http://`, `https://`, `file://`, `gs://`, and `s3://` checkpoint replication targets.
- **Cloud Metadata Spot Watcher (`worker/`):** `MetadataPreemptionWatcher` with `--metadata-provider gcp|aws` in `computeharbor_worker.daemon` to poll cloud preemption endpoints and trigger `SIGTERM` checkpointing.
- **Container, Compose & Kubernetes Deployment (`Dockerfile`, `Dockerfile.worker`, `docker-compose.yml`, `deploy/`):** Distroless Go control-plane image, Python 3.12 worker image, Docker Compose stack with Prometheus scraping, and Kubernetes manifests (`deploy/k8s/control-plane.yaml`, `deploy/k8s/worker-job.yaml`).
- **Spot Pool Failover & Supervisor (`pkg/server`, `worker/`):** `POST /v1/schedule` endpoint for pool selection with exclusions, bounded exponential backoff retry in `ControlPlaneClient`, `run_job_with_failover` supervisor, and `computeharbor-worker` / `computeharbor-supervisor` CLI entrypoints.
- **Rate Limiting & Structured Logging (`pkg/server`, `control-plane/`):** Per-org `OrgRateLimiter` token-bucket middleware, `slog` JSON HTTP request logging, and `-rate-limit-rps`, `-rate-limit-burst`, `-log-json` flags (`ADR-004`).
- **Job Cancellation & Lifecycle Webhooks (`pkg/orchestrator`, `pkg/server`, `pkg/events`):** Idempotent `CancelJob` (`POST /v1/jobs/{id}/cancel`) and async HMAC-SHA256 webhook event dispatcher wired via `-webhook-url` and `-webhook-secret` (`ADR-005`).
- **Multi-Region Fleet Scoring (`pkg/cost`, `pkg/server`):** `GET /v1/fleet/rank` endpoint scoring spot pools by price and regional availability.
- **Checkpointed Batch Inference (`worker/`):** `BatchInferenceRunner` and `computeharbor-batch` CLI with atomic `SIGTERM` cursor recovery, throughput, and estimated cost-per-million-records telemetry.
- **Distributed Ring Barrier & Org Billing (`pkg/orchestrator`, `pkg/server`):** `RingBarrier` multi-worker synchronization primitive and `BillingSummary` (`GET /v1/billing/summary`).
- **CLI Subcommands & API Docs (`control-plane/`, `docs/`, `examples/`):** `computeharbor status` and `computeharbor savings` subcommands, OpenAPI 3.1 specification (`docs/openapi.yaml`), and Python/Go quickstart examples (`examples/quickstart.py`, `examples/control_plane_client.go`).

## [0.1.0] - 2026-11-25

### Added
- **Go Control Plane (`control-plane/`, `pkg/cost`, `pkg/orchestrator`, `pkg/server`):**
  - Spot-vs-on-demand cost calculator (`pkg/cost`) supporting L4, A100, and H100 pricing comparisons.
  - Thread-safe job lease state machine (`pkg/orchestrator`) with `QUEUED`, `RUNNING`, `CHECKPOINTING`, `MIGRATING`, `COMPLETED`, and `FAILED` states, idempotent `SIGTERM` preemption handling, worker heartbeats, and stale-lease reaping.
  - Stdlib `net/http` JSON API (`pkg/server`) with `X-Org-ID` tenant isolation and `/healthz` liveness probe.
- **Python Spot Worker (`worker/`):**
  - `SIGTERM` preemption handler streaming LoRA adapter weights, AdamW optimizer states, and PRNG/step cursor manifests in deterministic Safetensors format with atomic write-and-rename commits.
  - Local and object-store replication storage backends plus control-plane HTTP client loop.
- **Chaos & E2E Test Suites (`tests/chaos/`, `tests/e2e/`):**
  - Chaos suite verifying bit-for-bit deterministic loss parity across `SIGTERM` interruptions.
  - End-to-end suite exercising the Go control plane HTTP server together with the Python `SIGTERM` worker.
- **Architecture Decision Records (`docs/adr/`):**
  - `ADR-001`: Stream lightweight LoRA adapter and RNG state checkpoints on spot preemption.
  - `ADR-002`: Stdlib `net/http` JSON control plane with `X-Org-ID` tenant isolation.
- **Release & CI Automation (`.github/workflows/`):**
  - Tag-triggered (`v*`) release workflow building `linux/amd64`, `linux/arm64`, and `darwin/arm64` binaries with `SHA256SUMS`.
