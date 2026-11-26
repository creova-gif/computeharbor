# ADR-003: Zero-Dependency Prometheus Text Exposition (`0.0.4`) on `GET /metrics`
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
ComputeHarbor's Go control plane (`pkg/server`) manages spot-GPU job leases, worker heartbeats, stale-lease reaping, `SIGTERM` preemption notices, and Safetensors checkpoint commits. Operators and design partners running Prometheus or Grafana need a standard scrape endpoint to monitor active job states, preemption frequency, checkpoint flush/recovery latency, and control-plane health. At the same time, ComputeHarbor's Go module (`go.mod`) intentionally has zero third-party dependencies to keep builds fast, reproducible, and free of transitive CVE churn.

## Decision
We expose `GET /metrics` on `pkg/server.Server` using only Go's standard library, emitting Prometheus text exposition format `0.0.4` (`Content-Type: text/plain; version=0.0.4; charset=utf-8`):
1. **Stdlib-Only Exposition:** Format `# HELP`, `# TYPE`, and metric sample lines directly with `strings.Builder` / `fmt.Fprintf` and `sync/atomic` or `Coordinator` read-lock snapshots—no `prometheus/client_golang` dependency in `go.mod`.
2. **Low-Cardinality Control-Plane Metrics:** Export bounded-cardinality gauges and counters derived from `Coordinator` state and handler instrumentation (e.g., job lease counts by `status`, total preemptions, checkpoint commits, and stale-lease reaps), avoiding unbounded labels such as `job_id`.
3. **Unauthenticated Scrape Compatibility:** Keep `GET /metrics` free of `X-Org-ID` tenant headers (like `GET /healthz`) so standard Prometheus scrapers can poll it directly inside a VPC or cluster network.

## Alternatives Considered
- **`prometheus/client_golang`:** Rejected. Pulls in a large transitive dependency graph (`protobuf`, `procfs`, `model`, etc.) for a handful of counters and gauges, violating the control plane's zero-dependency Go footprint.
- **OpenTelemetry SDK + OTLP Exporter:** Rejected. Requires external collector infrastructure and substantial Go dependency weight for basic scrape observability.
- **Custom JSON `/stats` Endpoint:** Rejected. Requires custom adapter scripts instead of working out-of-the-box with standard Prometheus scrape configs.

## Consequences & Failure Modes
- **Zero External Go Dependencies Preserved:** `go build` and `go test -race ./...` remain fast and self-contained on Go 1.23 stdlib.
- **Manual Exposition Discipline:** Maintainers must adhere to Prometheus `0.0.4` syntax rules (valid metric names, monotonic counters, deterministic label ordering, and proper escaping of `\` / `"` / `\n` in label values).
- **Cardinality & Info-Leak Guardrails:** Because `GET /metrics` is not tenant-scoped, it must only expose aggregate counts and latencies—never tenant `org_id` values, `job_id`s, or checkpoint URIs.
