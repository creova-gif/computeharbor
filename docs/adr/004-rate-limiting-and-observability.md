# ADR-004: Per-Org Rate Limiting, Structured JSON Access Logging, and Client Retry Backoff
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
ComputeHarbor's Go control plane (`pkg/server`) serves multi-tenant HTTP/JSON traffic from Python spot-GPU workers (`worker/src/computeharbor_worker/client.py`) and external API callers isolated by `X-Org-ID` (ADR-002). Without request throttling, a misbehaving tenant or worker heartbeat storm can starve other organizations' preemption (`POST /v1/jobs/{id}/sigterm`) and checkpoint (`POST /v1/jobs/{id}/checkpoint`) calls, threatening the 30,000 ms preemption recovery SLA. At the same time, operators need machine-parseable access logs to correlate `429` rejections, latency spikes, and tenant activity, and workers need a principled client-side retry strategy when transient `429` or `5xx` responses occur.

## Decision
We standardize three complementary reliability and observability mechanisms across the control plane and worker client:
1. **Per-`X-Org-ID` Token-Bucket Rate Limiter (`pkg/server/middleware.go`):**
   - `OrgRateLimiter` maintains a mutex-protected map of per-organization token buckets (`rps` refill rate, `burst` capacity) keyed by the trimmed `X-Org-ID` header, normalizing missing or empty headers to `"anonymous"`.
   - When an organization's bucket has fewer than `1.0` tokens, `OrgRateLimiter.Middleware` rejects the request with `429 Too Many Requests`, sets `Retry-After: 1`, and returns a JSON error body (`"rate limit exceeded"`).
   - Configured on `pkg/server.Server` via `WithRateLimit(rps, burst)`.
2. **Structured `log/slog` JSON Access Logs (`pkg/server/middleware.go`):**
   - `LoggingMiddleware` wraps `http.ResponseWriter` with a `statusRecorder` (preserving `Unwrap()`) and emits a structured `log/slog` record (`"http request"`) containing `method`, `path`, `status`, `duration_ms`, and `org_id`.
   - In `Server.ServeHTTP`, `LoggingMiddleware` wraps `OrgRateLimiter.Middleware` so `429` rate-limit rejections are logged with their actual HTTP status and `org_id`.
   - Configured on `pkg/server.Server` via `WithLogger(logger)` (using `slog.NewJSONHandler` in production).
3. **Client-Side `429`/`5xx` Exponential Backoff Retry Design (`ControlPlaneClient`):**
   - `ControlPlaneClient` (`worker/src/computeharbor_worker/client.py`) treats `429 Too Many Requests` and `5xx` server errors as transient, while failing fast on non-`429` `4xx` errors (`400`, `403`, `404`, `409`).
   - Retries use bounded exponential backoff (`base * 2^attempt` with jitter, capped at a max delay and bounded total attempts) and honor the server's `Retry-After` header when present on `429` responses so workers do not thundering-herd a saturated control plane during mass spot preemptions.

## Alternatives Considered
- **Third-Party Rate-Limiting & Logging Libraries (e.g., `golang.org/x/time/rate`, `zap`, `zerolog`):** Rejected. Adds external dependencies to `go.mod` when Go 1.23's `log/slog` and a 60-line stdlib `sync.Mutex` token bucket satisfy all requirements with zero transitive dependencies.
- **Global (Non-Tenant) Rate Limit:** Rejected. A single noisy tenant exhausting a global bucket would block `SIGTERM` and checkpoint commits for healthy organizations.
- **Fixed-Interval Client Retries Without Jitter:** Rejected. Synchronized spot-instance preemptions across a fleet would cause workers to retry in lockstep against the control plane.

## Consequences & Failure Modes
- **Noisy-Neighbor Isolation:** One organization's burst traffic is capped at its own token bucket without affecting other `X-Org-ID` tenants.
- **In-Memory Bucket State:** `OrgRateLimiter` stores buckets in process memory; horizontal control-plane replicas enforce per-replica limits rather than a globally shared quota, and unbounded distinct `X-Org-ID` headers would grow the `buckets` map unless periodic eviction or an upstream auth gateway is present.
- **Retry Budget vs. Preemption SLA:** Client retry caps must remain well below the spot preemption grace window so a worker does not exhaust its 30s recovery budget retrying an unreachable control plane.
