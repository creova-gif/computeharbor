# ADR-005: Async Bounded-Queue HMAC-SHA256 Webhook Delivery and Multi-Region Spot Fleet Scoring
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
ComputeHarbor's control plane (`pkg/server` and `pkg/orchestrator`) manages spot-GPU job lifecycles (`QUEUED`, `RUNNING`, `PREEMPTED`, `RESUMED`, `COMPLETED`, `FAILED`) across ephemeral compute instances. External orchestrators, customer dashboards, and alerting pipelines need push notifications for these transitions without polling `GET /v1/jobs/{id}`. However, synchronous HTTP webhook delivery on the job state-transition path would couple control-plane latency and the 30,000 ms `SIGTERM` preemption recovery SLA to downstream customer endpoint availability.

Concurrently, relying on a single cloud provider or region exposes workloads to correlated spot-capacity exhaustion and price spikes. ComputeHarbor needs a deterministic fleet-scoring mechanism to rank spot GPU pools across GCP, AWS, and OCI by effective cost and interruption risk before dispatching or migrating jobs.

## Decision
We introduce two decoupled control-plane subsystems:

1. **Async Bounded-Queue HMAC-SHA256 Webhook Dispatcher (`pkg/events/`):**
   - **Non-Blocking Enqueue:** Job lifecycle transitions emit immutable event envelopes into a bounded in-memory channel (`queueSize`, e.g., 1024 events). Enqueue uses a non-blocking `select` with `default` drop-and-count semantics so slow or unreachable webhook receivers never block `POST /v1/jobs/{id}/sigterm` or `POST /v1/jobs/{id}/checkpoint`.
   - **Background Worker & Retries:** A background dispatcher goroutine drains the queue, delivers JSON payloads via HTTP `POST` with a per-attempt request timeout, and retries transient `5xx`/network failures with bounded exponential backoff.
   - **HMAC-SHA256 Signing:** When `-webhook-secret` is configured, each request includes `X-ComputeHarbor-Signature: sha256=<hex>` computed over the exact raw JSON body (or `timestamp.body` if replay-window headers are attached) using `crypto/hmac` and `crypto/sha256`, allowing receivers to verify authenticity in constant time (`hmac.Equal`).
   - **Configuration:** Configured on the control plane via `-webhook-url` and `-webhook-secret` flags (no-op when `-webhook-url` is empty).

2. **Multi-Region Spot Fleet Price & Availability Scoring:**
   - **Normalized Pool Model:** Candidate GPU pools across GCP, AWS, and OCI are represented with `(provider, region, gpu_type, spot_price_usd_hr, ondemand_price_usd_hr, availability_score, interruption_rate)`.
   - **Deterministic Composite Score:** Each pool is scored using a weighted utility function balancing normalized spot discount against historical availability and preemption risk, filtering out pools below a minimum availability threshold or incompatible GPU VRAM/architecture constraints.
   - **Tie-Breaking & Fallback:** Ties break deterministically by lowest spot price, then lexicographically by `(provider, region)` to keep scheduling reproducible in tests and incident postmortems.

## Alternatives Considered
- **Synchronous Webhook Delivery in HTTP Handlers:** Rejected. A slow or timing-out webhook receiver would stall `SIGTERM` preemption acknowledgments and risk breaching the 30s checkpoint window.
- **Unbounded Event Queue or External Broker (Kafka/PubSub) for v1:** Rejected. Unbounded queues risk OOM under sustained receiver outages, while requiring Kafka/PubSub adds heavy operational dependencies for single-binary control-plane deployments.
- **Cheapest-Only Spot Selection:** Rejected. Selecting solely by lowest spot price routes jobs into high-churn pools with frequent `SIGTERM` interruptions, increasing checkpoint overhead and net wall-clock time.

## Consequences & Failure Modes
- **At-Most-Once Under Sustained Outage:** Because the queue is bounded, prolonged webhook receiver downtime fills the buffer and drops oldest/newest events while incrementing Prometheus drop counters (`computeharbor_webhook_dropped_total`).
- **Process-Local Queue:** Pending events in the in-memory channel are lost if the control plane restarts; durable outbox persistence in SQLite/Postgres can be layered on later without changing the `pkg/events` publisher interface.
- **Stale Spot Pricing Signals:** Multi-region fleet scoring depends on periodic price/availability refreshes; stale feed data can temporarily misrank a depleted pool until the next refresh or failed placement triggers fallback to the next-ranked pool.
