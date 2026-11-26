# Threat Model

This document describes the security architecture, assets, trust boundaries, mitigations, and residual risks for **ComputeHarbor** (Go control plane + Python spot worker).

## 1. Assets & Data Classification

| Asset | Classification | Storage / Transit | Sensitivity & Impact |
| --- | --- | --- | --- |
| Bearer API key (`COMPUTEHARBOR_API_KEY` / `-api-key`) | **Confidential (Secret)** | Environment / CLI flag; `Authorization: Bearer` header | Compromise grants control-plane API access across protected endpoints. |
| Webhook signing secret (`-webhook-secret` / `COMPUTEHARBOR_WEBHOOK_SECRET`) | **Confidential (Secret)** | Environment / CLI flag; HMAC-SHA256 key | Compromise allows forging `X-ComputeHarbor-Signature` payloads to downstream receivers. |
| Cloud credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, GCP OIDC / Secret Manager) | **Confidential (Secret)** | Workload identity / environment | Compromise exposes object-store buckets or cloud compute quotas. |
| Tenant metadata & job state (`org_id`, `JobLease`, billing summaries) | **Internal / Tenant-Scoped** | In-memory coordinator + atomic JSON state snapshot (`-state-file`) | Cross-tenant exposure leaks training job metadata, spot cost metrics, and billing totals. |
| Checkpoint artifacts (`*.safetensors`, `step-NNNNNN.manifest.json`, `latest.json`) | **Tenant-Scoped Intellectual Property** | Worker local disk + remote object store (`--remote-store-dir` / presigned store) | Tampering or corruption can poison model weights or break deterministic resume; unauthorized read leaks fine-tuned LoRA weights. |
| HTTP access logs & Prometheus metrics (`/metrics`) | **Operational** | `stderr` JSON (`slog`) and `/metrics` exposition | Logs record `method`, `path`, `status`, `duration_ms`, and `org_id` (no authorization headers or secrets). |

## 2. Trust Boundaries

```
  [Client / CLI / Worker]
           |
           |  HTTPS/HTTP + Bearer Auth + X-Org-ID
           v
  [Control Plane: pkg/server]  --->  [Webhook Receiver] (HMAC-SHA256: X-ComputeHarbor-Signature)
           |
           |  JobLease / Checkpoint URIs
           v
  [Python Worker: computeharbor_worker]
           |
           |  _validate_object_key + SHA-256 verification
           v
  [Local Disk / Object Store]
```

### 2.1 Client -> Control Plane
- **Boundary:** External callers, CLI clients (`computeharbor status`, `computeharbor savings`), and worker daemons communicating with `pkg/server.Server` (`ServeHTTP` in `pkg/server/server.go`).
- **Middleware Chain Order:** `SecurityHeadersMiddleware` -> `LoggingMiddleware` -> `BearerAuthMiddleware` -> `OrgRateLimiter.Middleware` -> `s.mux`.
- **Input Validation:** Request bodies are decoded and validated at the HTTP handler and coordinator layers (Go request validation and Pydantic v2 `CheckpointManifest` validation on the worker).

### 2.2 Org Tenant Isolation (`X-Org-ID`)
- **Boundary:** Multi-tenant logical separation across organizations sharing a control-plane instance.
- **Enforcement:**
  - `POST /v1/jobs` binds the created `JobLease` to `org_id` from the request body.
  - Org-scoped endpoints (`GET /v1/jobs`, `GET /v1/jobs/{id}`, `assign`, `heartbeat`, `sigterm`, `checkpoint`, `cancel`, `barrier`, and `GET /v1/billing/summary`) require a non-empty `X-Org-ID` header.
  - `authorizeJob` in `pkg/server/server.go` returns `403 Forbidden` if `X-Org-ID` is missing or if `lease.OrgID != orgID`.

### 2.3 Bearer Auth & Rate Limiting
- **Bearer Authentication (`BearerAuthMiddleware` in `pkg/server/middleware.go`):**
  - When `-api-key` / `COMPUTEHARBOR_API_KEY` is configured, all routes except `GET /healthz` and `GET /ui` require `Authorization: Bearer <token>`.
  - Tokens are compared using `crypto/subtle.ConstantTimeCompare` to prevent timing side-channel attacks; invalid or missing tokens receive `401 Unauthorized`.
- **Per-Org Rate Limiting (`OrgRateLimiter` in `pkg/server/middleware.go`):**
  - Enforces a thread-safe token bucket keyed by `X-Org-ID` (requests without `X-Org-ID` share the `"anonymous"` bucket).
  - Bounded memory via `maxBuckets = 10000` and idle eviction (`bucketIdleTTL = 10 * time.Minute`).
  - Exhausted buckets receive `429 Too Many Requests` with `Retry-After: 1`.

### 2.4 Webhook Dispatch & HMAC-SHA256 Signing
- **Boundary:** Asynchronous outbound HTTP POST requests from `WebhookDispatcher` (`pkg/events/webhook.go`) to operator-configured endpoints (`-webhook-url`).
- **Enforcement:**
  - Payloads are queued in a bounded channel (default buffer `256`) so slow or unreachable receivers cannot block the orchestrator state machine; overflows increment `dropped`.
  - Each payload is signed using HMAC-SHA256 (`signPayload`) with `-webhook-secret` and sent in the `X-ComputeHarbor-Signature` header (`sha256=<hex>`).
  - Receivers can validate authenticity using constant-time `hmac.Equal` (implemented in `events.VerifySignature`).
  - Transient `429` and `5xx` responses are retried with exponential backoff (`defaultMaxRetries = 2`, `defaultBackoff = 50ms`).

### 2.5 Worker -> Object Store Path Traversal & SHA-256 Integrity
- **Boundary:** Checkpoint replication and restore between `computeharbor_worker` and local or remote object stores (`worker/src/computeharbor_worker/storage.py` and `checkpoint.py`).
- **Path Traversal Prevention:**
  - `_validate_object_key` (invoked by `LocalMirrorObjectStore._resolve_key` and `HTTPPresignedStore._object_url`) strips leading `/` and rejects empty keys, absolute paths, and `..` path-traversal segments with `ValueError`.
  - `HTTPPresignedStore` restricts `base_url` schemes to `http` or `https` with a non-empty `netloc`.
- **Atomic Persistence & Integrity Verification:**
  - Writes use `_atomic_write_from_stream` / `_atomic_copy_file` (sibling temp file, `flush()`, `os.fsync()`, and atomic `os.replace()`).
  - `CheckpointManager.load_latest` in `worker/src/computeharbor_worker/checkpoint.py` computes the SHA-256 digest of the restored `.safetensors` file and verifies it against `manifest.payload_sha256` before deserializing tensors, raising `ValueError` on mismatch.

### 2.6 Dashboard CSP & Security Headers
- **Boundary:** Browser rendering of the built-in status and savings console (`GET /ui` in `pkg/server/ui.go`).
- **Enforcement (`SecurityHeadersMiddleware` in `pkg/server/security.go`):**
  - Sets `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Referrer-Policy: no-referrer` on all HTTP responses.
  - Sets `Content-Security-Policy: default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'` on `/ui` to restrict external script/resource loading while supporting the self-contained HTML console.

## 3. Mitigations & CI Secret Scanning

- **Static & Runtime Checks:** Go `gofmt`, `go vet ./...`, `go test -race -v ./...`; Python `ruff check`, `mypy --strict`, `pytest -v`.
- **Secret Scanning:** `.github/workflows/ci.yml` runs a dedicated `secret-scan` job using `gitleaks/gitleaks-action@v2` with `fetch-depth: 0` across full git history on every push and pull request.
- **Credential Hygiene:** `.env` and `.env.*` are gitignored (with `!.env.example` explicitly tracked as a placeholder template); cloud deployments should use workload identity (e.g., GCP Secret Manager + OIDC or AWS IAM roles) rather than static keys.

## 4. Residual Risks & Operational Guidance

| Residual Risk | Context | Recommended Operator Mitigation |
| --- | --- | --- |
| Header-only `X-Org-ID` when `-api-key` is unset | Without `-api-key`, any caller can supply any `X-Org-ID`, and `GET /ui` / `GET /healthz` / `GET /metrics` are unauthenticated or exempt from bearer auth. | Always set `-api-key` (`COMPUTEHARBOR_API_KEY`) in shared environments and place the control plane behind an authenticating reverse proxy / mTLS ingress. |
| Single shared static `-api-key` | `BearerAuthMiddleware` validates a single shared token rather than per-org scoped tokens. | Terminate per-tenant OAuth/OIDC or API gateway auth in front of ComputeHarbor for multi-org production deployments. |
| Plaintext HTTP in local examples | Local quickstart uses `http://127.0.0.1:8080` and `HTTPPresignedStore` permits `http://` for local testing. | Enforce TLS (`https://`) at the ingress/load balancer and for all production webhook and presigned object-store URLs. |
| Inline scripts/styles in `/ui` CSP | `uiContentSecurityPolicy` permits `'unsafe-inline'` for the embedded single-file console. | Keep `/ui` disabled or network-restricted in untrusted environments, or serve a separate static bundle with strict nonces/hashes. |
