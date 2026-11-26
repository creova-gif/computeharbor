# Security Policy

## Supported Versions

| Version | Supported |
| --- | --- |
| `0.2.x` | Yes |
| `0.1.x` | Yes |

## Reporting a Vulnerability

If you discover a security vulnerability in ComputeHarbor, please report it privately rather than opening a public issue:

- **GitHub Security Advisories:** Use the repository's **Security -> Advisories -> Report a vulnerability** workflow (`https://github.com/creova-gif/computeharbor/security/advisories/new`).
- **Private Issue:** If Security Advisories are unavailable, contact the maintainers to open a private security report with reproduction steps, affected versions, and impact assessment.

Please allow up to 48 hours for an initial acknowledgment and coordinate disclosure until a fix is released.

## Security Controls Summary

- **Constant-Time Bearer Authentication (`-api-key` / `COMPUTEHARBOR_API_KEY`):** When configured on `computeharbor serve`, protected HTTP endpoints require `Authorization: Bearer <key>` validated with constant-time comparison (`crypto/subtle.ConstantTimeCompare`).
- **Tenant Scoping (`X-Org-ID`):** Org-scoped job, checkpoint, and billing endpoints require `X-Org-ID` and enforce organization isolation at the coordinator layer.
- **Signed Lifecycle Webhooks (`X-ComputeHarbor-Signature`):** Outbound webhook deliveries are signed with HMAC-SHA256 using `-webhook-secret` and include the `X-ComputeHarbor-Signature` header.
- **Checkpoint Integrity Verification:** Worker restore verifies Safetensors SHA-256 digests against the committed manifest (`latest.json` / `step-NNNNNN.manifest.json`) before loading adapter weights.
- **CI Secret Scanning:** Pull requests and pushes are scanned with `gitleaks` in CI to prevent accidental credential commits.

For threat modeling details, see [`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md).
