# ComputeHarbor: Heterogeneous Spot-GPU & Edge LLM Training Infrastructure

- **Model:** Open-Source CLI/Worker + Managed Spot Orchestrator (10-15% savings fee)
- **Thesis Alignment:** #65 Better LLM Training Infra, #12 Agent-First Infra, #19 Edge Compute
- **Full Google Doc Spec:** [ComputeHarbor: Heterogeneous Spot-GPU & Edge LLM Training Infrastructure](https://docs.google.com/document/d/1s5obcXRUxN7rM31n4T1K9j1__7QQ6P8dPOJBbSZa9rg/edit?usp=drivesdk&ouid=110227624285917376281)
- **Portfolio Strategy:** [00 - Startup & Open-Source Cross-Project Strategy](https://docs.google.com/document/d/12Wpg5mMGnjnl2uhMBTOR0QqRzQj8kwfzTLpvLoRuB5Q/edit?usp=drivesdk&ouid=110227624285917376281)

## 1. Goal & Problem
Cut LLM fine-tuning and batch-inference costs by 60%+ by orchestrating preemptible spot GPUs with automatic <30s SIGTERM checkpoint-and-migrate.

## 2. Target Customer (ICP) & Monetization
AI startups running frequent LoRA fine-tunes, synthetic data generation, or async batch inference.

## 3. Tech Stack
Go control plane + gRPC, Python/PyTorch worker daemon, GCS/S3 streaming Safetensors checkpoints.

## 4. Evaluation & Acceptance Thresholds
< 30s preemption recovery time; 100% deterministic loss-curve parity after 3 forced SIGTERM preemptions; >= 60% cost savings.

## 5. Key Architectural Decision (ADR-001)
Stream lightweight LoRA adapter + RNG state checkpoints asynchronously rather than full 70B base weights.

## 6. Day 1–5 Quickstart
- [x] Set up control-plane/, worker/, and tests/chaos/ directories
- [x] Write Python SIGTERM handler flushing model + optimizer + RNG state to local/GCS storage
- [x] Verify deterministic resume matches uninterrupted training loss bit-for-bit
- [x] Build spot-vs-ondemand cost calculator across L4/A100 spot rates
