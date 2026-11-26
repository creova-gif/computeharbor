# computeharbor — Execution Checklist

## Wave 1: Day 1–5 Setup
- [x] Set up control-plane/, worker/, and tests/chaos/ directories
- [x] Write Python SIGTERM handler flushing model + optimizer + RNG state to local/GCS storage
- [x] Verify deterministic resume matches uninterrupted training loss bit-for-bit
- [x] Build spot-vs-ondemand cost calculator across L4/A100 spot rates

## Wave 2: MVP & Eval Gate
- [x] Write ADR-001 in `docs/adr/`
- [x] Implement core pipeline + deterministic unit/eval tests
- [x] Verify CI workflow passes (`ruff`/`mypy` or `cargo clippy`/`test` or `go test -race`)

## Wave 3: Launch & Design Partner Validation
- [x] Publish clean README with reproducible benchmark numbers
- [x] Run first external design-partner or open-source release

## Wave 4: Multi-Region Spot Fleet & Event Webhooks
- [ ] Implement HMAC-SHA256 signed webhook event dispatcher (`pkg/events/`) for job lifecycle transitions
- [ ] Add multi-region spot fleet price/availability scoring across GCP, AWS, and OCI pools
- [ ] Add control-plane webhook configuration flags (`-webhook-url`, `-webhook-secret`) and E2E delivery verification

## Wave 5: Batch Inference & Synthetic Data Engine
- [ ] Build checkpointed batch inference & synthetic data worker (`worker/src/computeharbor_worker/batch_inference.py`) with zero-duplicate resume on `SIGTERM`
- [ ] Add `computeharbor-batch` CLI entrypoint and E2E preemption test for batch inference shards
- [ ] Add streaming token-throughput and cost-per-million-tokens telemetry

## Wave 6: Multi-Node Ring Checkpointing & Customer Telemetry
- [ ] Add multi-worker rank coordination (`world_size`, `rank`) and barrier checkpoint commit on `pkg/orchestrator`
- [ ] Build org-scoped usage & savings ledger endpoint (`GET /v1/billing/summary`)
- [ ] Add customer-facing CLI (`computeharbor status`, `computeharbor savings`)
