# ADR-006: Readiness Probes, Request Correlation, and State/Webhook Hardening
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
As ComputeHarbor moves toward multi-replica Kubernetes deployments and production multi-tenant operation, four operational and security gaps needed to be addressed:

1. **Graceful Drain vs. Liveness:** `/healthz` reports process liveness (`200 OK`) even during shutdown, which prevents Kubernetes or ingress controllers from stopping new traffic before in-flight requests and coordinator snapshots drain.
2. **End-to-End Request Correlation:** Structured `slog` HTTP logs lacked a consistent correlation identifier across client requests, middleware, and control-plane responses.
3. **Snapshot File Permissions:** Coordinator state snapshots written to `-state-file` inherited process `umask` permissions rather than enforcing owner-only access on shared hosts.
4. **Webhook SSRF / Redirect & URL Validation:** `WebhookDispatcher` accepted arbitrary endpoint strings without validating scheme/host/userinfo and followed HTTP `3xx` redirects, allowing a compromised or misconfigured receiver to redirect signed payloads (and `X-ComputeHarbor-Signature` headers) to internal metadata or third-party endpoints.

## Decision
1. **Unauthenticated `/readyz` Readiness Probe with Drain State (`pkg/server`):**
   - Added `GET /readyz` returning `200 OK` (`{"status":"ready"}`) when active and `503 Service Unavailable` (`{"status":"draining"}`) when `Server.SetDraining(true)` is set.
   - `GET /readyz` bypasses Bearer API-key authentication (alongside `GET /healthz` and `GET /ui`) so kubelet probes succeed without credentials while `POST /readyz` remains protected.
2. **`X-Request-ID` Correlation Middleware (`pkg/server/middleware.go`):**
   - `RequestIDMiddleware` inspects incoming `X-Request-ID` headers; values consisting of printable ASCII (`0x20`–`0x7e`) up to 128 bytes are preserved, while missing or invalid values are replaced with a freshly generated 16-byte (`32`-char hex) ID.
   - The resolved ID is set on the `X-Request-ID` response header and stored in `context.Context` (`RequestIDFromContext`) so `LoggingMiddleware` emits `request_id` on every `slog` JSON request record.
3. **Restrictive `0o600` Snapshot Permissions (`pkg/orchestrator/snapshot.go`):**
   - `SaveSnapshot` explicitly calls `tmpFile.Chmod(0o600)` on the temporary file before writing, `Sync()`, and atomic `os.Rename`, ensuring persisted job and org metadata is readable and writable only by the service user.
4. **Webhook URL & Redirect Hardening (`pkg/events/webhook.go`):**
   - `ValidateWebhookURL` enforces non-empty `http` or `https` URLs with a non-empty hostname and rejects embedded userinfo (`user:pass@host`).
   - `WebhookDispatcher`'s default `http.Client` configures `CheckRedirect` to return `http.ErrUseLastResponse`, disabling automatic redirect following on webhook deliveries.

## Alternatives Considered
- **Overloading `/healthz` for Readiness:** Rejected. Failing `/healthz` during graceful drain causes Kubernetes to restart the pod via its liveness probe instead of letting in-flight requests complete.
- **UUIDv4-only `X-Request-ID` Acceptance:** Rejected. Accepting bounded printable ASCII lets upstream gateways (Envoy, Cloud Load Balancing, W3C/custom trace IDs) propagate their existing request IDs without truncation or rejection.
- **Following Same-Origin Redirects for Webhooks:** Rejected. Webhook endpoints should be canonical `POST` targets; disabling redirects entirely eliminates redirect-based SSRF and signature-leak vectors.

## Consequences & Failure Modes
- **Load-Balancer Drain Delay:** Orchestrators must mark the server draining via `SetDraining(true)` before terminating the listener so Kubernetes readiness probes observe `503` and remove the pod from endpoints.
- **Webhook `3xx` Responses Treated as Non-2xx:** Receivers returning `301`/`302`/`307`/`308` will no longer be followed and will be recorded as failed/retried deliveries.
- **Strict Snapshot Ownership:** Backup or sidecar processes reading `-state-file` must run as the same UID as `computeharbor`.
