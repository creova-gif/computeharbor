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
- [ ] Run first external design-partner or open-source release
