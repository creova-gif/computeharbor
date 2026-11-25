# ADR-002: Stdlib `net/http` JSON Control Plane with `X-Org-ID` Tenant Isolation
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
ComputeHarbor's `pkg/orchestrator` package manages spot-GPU training job leases (`QUEUED`, `RUNNING`, `CHECKPOINTING`, `MIGRATING`, `COMPLETED`, `FAILED`) in memory via `Coordinator`. To allow workers, CLI clients, and design-partner schedulers to submit jobs, report `SIGTERM` preemption notices, and commit LoRA checkpoint manifests over the network, the control plane needs an HTTP API that:
1. Enforces strict multi-tenant isolation using organization identifiers (`JobLease.OrgID`) without leaking cross-tenant job metadata.
2. Handles duplicate `SIGTERM` preemption notifications idempotently when metadata pollers and local signal handlers race during the 30-second spot eviction window.
3. Minimizes external runtime dependencies and binary footprint.

## Decision
We expose the control plane as a JSON-over-HTTP service built on Go's standard library `net/http` (`http.ServeMux` with Go 1.22+ method/path routing) wrapping `pkg/orchestrator.Coordinator`:
1. **Stdlib `net/http` JSON Surface:**
   - `POST /v1/jobs` — Submit a job (`SubmitJob`) into `QUEUED` state.
   - `POST /v1/jobs/{id}/assign` — Assign a worker node (`AssignWorker`), transitioning `QUEUED` or `MIGRATING` to `RUNNING` and recording recovery latency.
   - `POST /v1/jobs/{id}/sigterm` — Notify the coordinator of spot preemption (`NotifySIGTERM`).
   - `POST /v1/jobs/{id}/checkpoints` — Commit a flushed Safetensors + `manifest.json` reference (`CommitCheckpoint`), advancing `CurrentStep` and transitioning `CHECKPOINTING` to `MIGRATING` (or `COMPLETED` at `TargetSteps`).
   - `GET /v1/jobs/{id}` — Fetch current lease status and latest checkpoint reference.
   - `GET /healthz` — Liveness probe delegating to `pkg/core`.
2. **`X-Org-ID` Tenant Isolation:**
   - Every `/v1/jobs*` request requires a non-empty `X-Org-ID` header; missing headers return `400 Bad Request`.
   - Reads and state-mutating endpoints verify `lease.OrgID == headerOrgID` before returning data or mutating state. Cross-tenant requests return `404 Not Found` (avoiding existence disclosure) rather than `403 Forbidden`.
3. **Idempotent `SIGTERM` State Transitions:**
   - `Coordinator.NotifySIGTERM` is safe under concurrent or duplicate delivery: if a job is already in `CHECKPOINTING` or `MIGRATING`, the endpoint returns `200 OK` with the existing `JobLease` without incrementing `PreemptionCount` again.
   - If `PreemptionCount >= MaxPreemptions`, the job transitions to `FAILED` and returns `409 Conflict` with `ErrMaxPreemptions`.

## Alternatives Considered
- **Third-Party HTTP Frameworks (Gin, Echo, Chi):** Rejected. Go 1.23 `net/http.ServeMux` supports method-qualified path patterns natively, avoiding third-party dependency churn and CVE surface.
- **gRPC / Protobuf:** Rejected for the initial control-plane boundary. JSON over `net/http` is directly inspectable with `curl` by design partners and matches the CLI's JSON output contract.
- **JWT-Only Auth Without Explicit `X-Org-ID` Boundary:** Deferred. While production deployments will validate signed tokens at the edge, enforcing `X-Org-ID` inside the control-plane handler guarantees tenant isolation in local dev, chaos tests, and internal service-to-service calls.

## Consequences & Failure Modes
- **Zero External Go Dependencies:** The control plane continues to compile with stdlib only, keeping builds fast and reproducible.
- **Tenant Spoofing Without Edge Auth:** `X-Org-ID` alone is an identity claim, not cryptographic authentication; deployments exposed beyond a trusted VPC must terminate mTLS or validate signed bearer tokens at an API gateway that overwrites `X-Org-ID`.
- **In-Memory Lease Volatility:** While `Coordinator` is thread-safe, leases live in memory until a durable backing store is introduced; a control-plane restart loses active lease state even though worker checkpoints remain intact on object storage.
