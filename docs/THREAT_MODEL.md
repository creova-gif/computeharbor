# Threat Model
- **Assets:** API credentials, tenant data (`org_id`), checkpoint artifacts (`safetensors` payloads & manifests), execution logs.
- **Trust Boundaries:**
  - Client -> HTTP Control Plane (`/v1/jobs/{id}*`): Enforced via `X-Org-ID` header matching (`authorizeJob` in `pkg/server/server.go` returns `403` if missing or mismatched).
  - Worker -> Object Store (`LocalMirrorObjectStore`): `_resolve_key` rejects empty keys, absolute paths, and `..` path-traversal segments.
  - Object Store -> Checkpoint Restore: `load_latest` in `worker/src/computeharbor_worker/checkpoint.py` verifies the `safetensors` payload SHA-256 against `manifest.payload_sha256` before loading.
- **Mitigations:** Strict schema validation (Pydantic v2 / Go request validation), `org_id` tenant scoping, GCP Secret Manager + OIDC, and `gitleaks` CI secret scanning (`secret-scan` job with `fetch-depth: 0`).
