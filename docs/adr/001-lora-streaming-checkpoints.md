# ADR-001: Stream Lightweight LoRA Adapter and RNG State Checkpoints on Spot Preemption
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
Cloud providers reclaim spot GPU instances (NVIDIA L4, A100, H100) with a 30-second termination warning delivered via `SIGTERM` or metadata server polling. Saving full 70B-parameter base model weights (140 GB in FP16) over standard network-attached object storage (1 to 2.5 GB/s egress to GCS or S3) takes 56 to 140 seconds. Full-weight checkpointing misses the 30-second eviction deadline, truncates checkpoints, and forces jobs to roll back to stale periodic snapshots.

## Decision
We freeze base model weights as immutable read-only artifacts and stream only the mutable training state on `SIGTERM`:
1. **LoRA Adapter Matrices (`A` and `B`):** Stored in deterministic Safetensors binary format. For rank-16 adapters across attention projections, adapter weights are 40 to 160 MB instead of 140 GB.
2. **Optimizer Moment Buffers (`m` and `v`):** Stored alongside adapter tensors in the same Safetensors payload so AdamW momentum and variance resume without warmup spikes.
3. **Deterministic RNG & Step Cursor Manifest:** A Pydantic-validated JSON manifest (`manifest.json`) written after the tensor payload lands, recording the completed global step, epoch cursor, loss history, and exact PRNG state.
4. **Atomic Write-and-Rename Commit Protocol:** Workers write tensor payloads to `<step>.safetensors.tmp`, `fsync` the file descriptor, rename to `<step>.safetensors`, and atomically swap `latest.json`.

## Alternatives Considered
- **Full-Model Checkpointing:** Rejected. Exceeds the 30-second spot eviction window for models above 7B parameters on standard spot network links.
- **In-Memory Peer Replication (RDMA/TCP):** Rejected for single-node and heterogeneous spot pools where simultaneous zone preemptions drop correlated peers.
- **Framework-Specific Pickle Checkpoints (`torch.save`):** Rejected. Arbitrary code execution risk on untrusted checkpoints and non-atomic partial writes when `SIGKILL` arrives mid-flush.

## Consequences & Failure Modes
- **Recovery Time:** Sub-second local flush and <5s object-storage upload keep total preemption recovery under the 30-second SLA.
- **Bit-for-Bit Parity:** Restoring adapter weights, AdamW moments, dataset cursor, and PRNG state yields identical per-step training loss across arbitrary `SIGTERM` interruptions.
- **Partial Flush on Hard `SIGKILL`:** If a host dies before `latest.json` swaps, the atomic rename protocol leaves the previous step checkpoint intact and discards the `.tmp` file on startup reconciliation.
