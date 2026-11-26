# ADR-007: Per-Org Scoped Bearer API Keys
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
Currently, `BearerAuthMiddleware` (`pkg/server/middleware.go`) validates `Authorization: Bearer <token>` using `subtle.ConstantTimeCompare` against a single global `-api-key` (`COMPUTEHARBOR_API_KEY`). Multi-tenant isolation relies on callers self-reporting their organization via the `X-Org-ID` header (`pkg/server/billing.go`, `pkg/server/server.go`, and `OrgRateLimiter.Middleware`).

While a single shared key is sufficient for single-tenant or trusted-gateway deployments, it has two limitations in shared multi-tenant environments:
1. **Cross-Tenant Impersonation Risk:** Any caller possessing the single global Bearer key can set an arbitrary `X-Org-ID` header and read or mutate another organization's jobs, leases, or billing summaries.
2. **Coarse Revocation & Rotation:** Rotating or revoking credentials for one compromised tenant requires rotating the global key and redeploying every client and worker across all organizations.

## Decision
Introduce support for **per-organization scoped Bearer API keys** (`token -> org_id`) alongside the existing optional global admin key:

1. **Scoped Key Mapping (`token -> org_id`):**
   - Configure a mapping of scoped Bearer tokens to canonical `org_id` values (e.g., via `-org-api-keys` / `COMPUTEHARBOR_ORG_API_KEYS` or a structured key file with `0600` permissions).
   - `BearerAuthMiddleware` compares the presented `Authorization: Bearer <token>` against configured scoped tokens (and the optional global admin `-api-key`) using constant-time comparison (`subtle.ConstantTimeCompare`) without leaking which key matched or its length via timing side channels.
2. **`X-Org-ID` Binding & Enforcement:**
   - When a request authenticates with a **scoped org key**, the resolved `org_id` is bound to the request context.
   - If the caller also sends an `X-Org-ID` header that does not match the token's bound `org_id`, the server rejects the request with `403 Forbidden`.
   - If the caller omits `X-Org-ID`, handlers and `OrgRateLimiter` may either require explicit matching `X-Org-ID` or derive the effective `org_id` from the authenticated scoped token consistently across `LoggingMiddleware`, `OrgRateLimiter`, and job/billing handlers.
3. **Optional Global Admin Key:**
   - Retaining `-api-key` (`COMPUTEHARBOR_API_KEY`) as an optional global admin credential preserves backward compatibility for single-tenant deployments, platform operators, and cross-org CLI/automation workflows, where `X-Org-ID` continues to select the target tenant.
   - Probe and console exemptions (`GET /healthz`, `GET /readyz`, `GET /ui`) remain unchanged.

## Alternatives Considered
- **Full JWT / OIDC Verification in the Control Plane:** Rejected for now. Adding JWKS fetching, key rotation caching, and claims parsing increases operational complexity and external identity-provider coupling; static or file-backed scoped Bearer tokens fit ComputeHarbor's lightweight single-binary deployment model and can be fronted by an OIDC-aware API gateway when needed.
- **Replacing the Global Key Entirely:** Rejected. Removing `-api-key` would break existing single-tenant deployments, integration tests, and operator admin tooling.
- **Database-Backed API Key Store:** Rejected. ComputeHarbor intentionally uses an in-memory coordinator with atomic JSON snapshots (`-state-file`) rather than an external RDBMS dependency.

## Consequences & Failure Modes
- **Backward Compatibility:** Deployments using only `-api-key` or running unauthenticated in local dev continue to work without configuration changes.
- **Misconfigured Duplicate Tokens:** Startup validation must reject duplicate tokens mapped to different `org_id` values or empty `org_id` targets to prevent ambiguous authorization.
- **Operational Key Management:** Scoped key files or environment variables must follow the same secret-handling discipline as `COMPUTEHARBOR_API_KEY` (`0600` file permissions, never committed to VCS, and omitted from `slog` access logs).
- **Rate Limiter Alignment:** `OrgRateLimiter.Middleware` runs after `BearerAuthMiddleware`; binding the authenticated `org_id` ensures a tenant cannot bypass per-org rate limits by spoofing `X-Org-ID`.
