# ComputeHarbor SRE Operations & Incident Response Runbook

This runbook covers day-2 operations, alert triage, request correlation, preemption recovery, and state snapshot management for the **ComputeHarbor** control plane (`pkg/server`, `pkg/orchestrator`, `control-plane/cmd/computeharbor`) and spot worker (`worker/src/computeharbor_worker`).

---

## 1. Service Health, Readiness & Graceful Shutdown

### Probes (`/healthz` vs. `/readyz`)

| Endpoint | Auth | Healthy Response | Unhealthy / Draining Response | Kubernetes Probe |
| --- | --- | --- | --- | --- |
| `GET /healthz` | Bypassed (`BearerAuthMiddleware`) | `200 OK` `{"status":"ok"}` | Process unreachable / timeout | `livenessProbe` (`periodSeconds: 10`, `failureThreshold: 3`) |
| `GET /readyz` | Bypassed (`BearerAuthMiddleware`) | `200 OK` `{"status":"ready"}` | `503 Service Unavailable` `{"status":"draining"}` | `readinessProbe` (`periodSeconds: 5`, `failureThreshold: 3`) |

> **Note:** `POST` requests to `/healthz` or `/readyz` are not exempted from `BearerAuthMiddleware` and will return `401 Unauthorized` when `-api-key` / `COMPUTEHARBOR_API_KEY` is configured.

### Graceful Shutdown & Drain Behavior

When the control plane receives `SIGTERM` or `SIGINT` (`ctx.Done()` in `control-plane/cmd/computeharbor/main.go`):

1. `api.SetDraining(true)` is called immediately.
   - `GET /readyz` flips to `503 Service Unavailable` (`{"status":"draining"}`).
   - Prometheus metric `computeharbor_draining` flips from `0` to `1`.
2. `srv.Shutdown(shutdownCtx)` drains in-flight HTTP requests for up to `5s`.
3. Background `StartReaper` and `StartSnapshotter` loops are cancelled and awaited (`<-reaperDone`, `<-snapshotDone`).
4. If `-state-file` is set, a final `coord.SaveSnapshot(cfg.stateFile)` executes before exit.

**Verification during rolling restart:**

```bash
curl -i http://127.0.0.1:8080/readyz
curl -s http://127.0.0.1:8080/metrics | grep computeharbor_draining
```

If a pod stays in `Terminating` or `NotReady` unexpectedly:
- Check `kubectl describe pod <pod>` for readiness probe failures (`503` indicates `SetDraining(true)` is active).
- Inspect `stderr` JSON logs for shutdown errors or slow `-state-file` disk writes.

---

## 2. Prometheus Alert Triage & Response Playbooks

Prometheus scrapes `/metrics` every `5s` (`deploy/prometheus.yml`).

### 2.1 Webhook Delivery Failures & Drops

- **Metrics:** `computeharbor_webhook_failed_total`, `computeharbor_webhook_dropped_total`, `computeharbor_webhook_delivered_total` (emitted when `-webhook-url` is configured).
- **Symptoms:** Downstream lifecycle consumers miss `job.submitted`, `job.preempted`, or completion events.
- **Root Causes:**
  1. Receiver endpoint is unreachable, returning non-`2xx`, or returning a `3xx` redirect (redirects are blocked by `CheckRedirect` returning `http.ErrUseLastResponse` per ADR-006).
  2. Receiver latency exceeds delivery rate, filling the bounded async queue (`256` events in `events.NewWebhookDispatcher`) and incrementing `computeharbor_webhook_dropped_total`.
  3. Invalid `-webhook-url` rejected at startup (`ValidateWebhookURL` requires `http`/`https`, non-empty host, and no embedded `user:pass@` userinfo).
- **Triage & Mitigation:**
  1. Verify receiver health and ensure it returns `2xx` directly without `301`/`302`/`307`/`308` redirects.
  2. Check receiver `X-ComputeHarbor-Signature` verification against `-webhook-secret`.
  3. Scale or decouple the webhook receiver if `computeharbor_webhook_dropped_total` is climbing.

### 2.2 Recovery Latency Spikes (`computeharbor_max_recovery_duration_ms`)

- **Metric:** `computeharbor_max_recovery_duration_ms` (gauge tracking max observed preemption-to-reassignment duration in ms).
- **Symptoms:** Preempted jobs spend excessive time in `CHECKPOINTING` or `MIGRATING` before resuming on a new spot worker.
- **Root Causes:**
  1. Spot capacity shortage for the requested `gpu` SKU.
  2. Large LoRA/checkpoint upload or download latency against `--remote-store-dir` / object store.
  3. `-reap-interval` (default `5s`) or `-lease-ttl` (default `30s`) tuned too conservatively for rapid preemption detection.
- **Triage & Mitigation:**
  1. Query `/v1/jobs` (with `X-Org-ID`) to identify jobs stuck in `CHECKPOINTING` or `MIGRATING`.
  2. Check worker daemon logs for object-store upload/download timeouts or SHA-256 verification retries.
  3. Verify healthy standby workers are polling for the affected GPU type.

### 2.3 Tenant Rate Limiting (`429 Too Many Requests`)

- **Metrics & Headers:** `computeharbor_http_requests_total{status="429"}`; responses include `Retry-After: 1` from `OrgRateLimiter.Middleware`.
- **Symptoms:** Legitimate CLI/worker requests for a specific `X-Org-ID` receive `429 rate limit exceeded`.
- **Root Causes:**
  1. Noisy tenant or tight polling loop exhausting per-org token bucket (`-rate-limit-rps`, `-rate-limit-burst`, default burst `20`).
  2. Missing `X-Org-ID` header on non-exempt routes causing callers to share the `"anonymous"` bucket (`defaultOrgID`).
  3. Bucket table pressure (`maxBuckets = 10000`, `bucketIdleTTL = 10m`) evicting idle orgs under high cardinality.
- **Triage & Mitigation:**
  1. Filter `stderr` JSON logs for `status=429` and inspect `org_id` and `path`.
  2. Confirm clients and workers set `X-Org-ID` consistently.
  3. If traffic is legitimate, increase `-rate-limit-rps` or `-rate-limit-burst` on the control plane deployment.

### 2.4 HTTP `5xx` Spikes

- **Metric:** `computeharbor_http_requests_total{status=~"5.."}` and `computeharbor_http_request_duration_seconds_sum` / `_count`.
- **Triage & Mitigation:**
  1. Note that `GET /readyz` returns `503` intentionally during graceful drain (`computeharbor_draining == 1`); exclude expected rolling-deploy drain windows first.
  2. For unexpected `500`/`502`/`503` responses, grab the `X-Request-ID` from the failing client response and correlate in `stderr` JSON logs (Section 3).

---

## 3. Request Correlation & Log Triage (`X-Request-ID`)

Every HTTP request passes through `RequestIDMiddleware` and `LoggingMiddleware` (`pkg/server/middleware.go`):

- **Inbound `X-Request-ID`:** Printable ASCII (`0x20`–`0x7e`) up to `128` bytes is preserved; missing or invalid headers are replaced with a freshly generated 16-byte (`32`-char hex) ID.
- **Outbound `X-Request-ID`:** Always echoed on the HTTP response header.
- **Structured JSON Logs (`-log-json=true`, default):** Emitted to `stderr` via `slog.NewJSONHandler` with fields `method`, `path`, `status`, `duration_ms`, `org_id`, and `request_id`.

**Log triage commands:**

```bash
# Correlate a single request across Kubernetes control-plane logs
kubectl logs deployment/computeharbor-control-plane | jq 'select(.request_id == "<X-Request-ID>")'

# Find all 5xx or 429 responses for an organization
kubectl logs deployment/computeharbor-control-plane | jq 'select(.org_id == "org-alpha" and (.status >= 500 or .status == 429))'
```

---

## 4. Spot Preemption & Remote Checkpoint Recovery

### Lifecycle State Machine

When a spot instance is preempted or its heartbeat exceeds `-lease-ttl` (`30s` default):

1. `RUNNING -> CHECKPOINTING`: Preemption is triggered (`preemption_count` increments).
2. `CHECKPOINTING -> MIGRATING`: Worker commits its final checkpoint manifest and enters `MIGRATING` while awaiting a replacement worker lease.
3. `MIGRATING -> RUNNING`: A new worker claims the job, downloads the latest checkpoint, and resumes training.
4. `FAILED`: If `preemption_count > max_preemptions` (e.g., `max_preemptions` exhausted), the control plane rejects further preemption/resume with `409 Conflict` and transitions the job to `FAILED`.

### Verifying Remote Checkpoints (`<job_id>/latest.json`)

Workers store job-scoped checkpoints under `<job_id>/latest.json` and `step-NNNNNN.manifest.json` (`worker/src/computeharbor_worker/checkpoint.py`), validating object keys (`_validate_object_key`) and SHA-256 checksums before resuming.

**Recovery checklist:**
1. Confirm `<job_id>/latest.json` exists in the remote store and points to a valid `step-NNNNNN.manifest.json`.
2. Verify all shard/adapter files referenced in the manifest are present and pass SHA-256 verification.
3. If the job transitioned to `FAILED` due to `max_preemptions` exhaustion, resubmit the job with a higher `max_preemptions` budget and point it to the verified checkpoint URI.

---

## 5. Snapshot Backup & Restore (`-state-file`)

When `-state-file` is configured (e.g. `/var/lib/computeharbor/state.json` in `deploy/k8s/control-plane.yaml`), `Coordinator.SaveSnapshot` (`pkg/orchestrator/snapshot.go`) runs on `-snapshot-interval` (`5s` default) and on clean shutdown:

- **Atomic Write:** Marshals deterministic JSON (`CoordinatorSnapshot` v1), writes to `.coordinator-snapshot-*.tmp` in the same directory, calls `tmpFile.Chmod(0o600)`, `Sync()`, `Close()`, and `os.Rename`.
- **Permissions:** Files are strictly `0600` (owner read/write only). Backup/restore tooling must run as the same UID (`fsGroup: 65532` / service user) as the control plane.
- **Startup Behavior:** `LoadCoordinatorFromFile` restores the snapshot if present, starts fresh if the file does not exist, and **fails fast** on corrupt JSON to prevent silent state loss.

### Backup Procedure

```bash
# Because SaveSnapshot uses atomic rename, copying the target path yields a consistent snapshot
cp -p /var/lib/computeharbor/state.json /var/backups/computeharbor/state-$(date -u +%Y%m%dT%H%M%SZ).json
```

### Restore Procedure

1. Scale the control plane to `0` replicas (or stop the process) so no background snapshotter overwrites the file.
2. Verify JSON validity and enforce `0600` permissions before starting:

```bash
jq empty /var/backups/computeharbor/state-backup.json
install -m 0600 /var/backups/computeharbor/state-backup.json /var/lib/computeharbor/state.json
```

3. Start the control plane (`computeharbor serve -state-file /var/lib/computeharbor/state.json`) and verify `/readyz` returns `200` and `/v1/jobs` reflects the restored leases.
