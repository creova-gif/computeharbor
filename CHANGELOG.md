# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
