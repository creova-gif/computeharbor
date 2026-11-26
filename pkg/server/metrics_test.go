package server_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

func getMetrics(t *testing.T, srv http.Handler) string {
	t.Helper()
	return getMetricsWithAuth(t, srv, "")
}

func getMetricsWithAuth(t *testing.T, srv http.Handler, token string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want %q", got, "text/plain; version=0.0.4; charset=utf-8")
	}
	return rec.Body.String()
}

func assertMetricLine(t *testing.T, body, line string) {
	t.Helper()
	if !strings.Contains(body, line+"\n") {
		t.Errorf("metrics output missing line %q\ngot:\n%s", line, body)
	}
}

func TestMetricsLifecycle(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	coord := orchestrator.NewCoordinator().WithClock(func() time.Time { return now })
	srv := server.New(coord)

	// 1. Before job submission.
	before := getMetrics(t, srv)
	assertMetricLine(t, before, `computeharbor_jobs_total{status="CHECKPOINTING"} 0`)
	assertMetricLine(t, before, `computeharbor_jobs_total{status="COMPLETED"} 0`)
	assertMetricLine(t, before, `computeharbor_jobs_total{status="FAILED"} 0`)
	assertMetricLine(t, before, `computeharbor_jobs_total{status="MIGRATING"} 0`)
	assertMetricLine(t, before, `computeharbor_jobs_total{status="QUEUED"} 0`)
	assertMetricLine(t, before, `computeharbor_jobs_total{status="RUNNING"} 0`)
	assertMetricLine(t, before, `computeharbor_preemptions_total 0`)
	assertMetricLine(t, before, `computeharbor_checkpoints_total 0`)
	assertMetricLine(t, before, `computeharbor_max_recovery_duration_ms 0`)

	// Verify deterministic output.
	if again := getMetrics(t, srv); again != before {
		t.Fatalf("metrics output not deterministic:\nfirst:\n%s\nsecond:\n%s", before, again)
	}

	// 2. After job submission.
	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":          "job-metrics-1",
		"org_id":          "org-alpha",
		"gpu":             string(cost.GPUA100_40G),
		"target_steps":    100,
		"max_preemptions": 2,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit status = %d (%s), want 201", rec.Code, rec.Body.String())
	}

	afterSubmit := getMetrics(t, srv)
	assertMetricLine(t, afterSubmit, `computeharbor_jobs_total{status="QUEUED"} 1`)
	assertMetricLine(t, afterSubmit, `computeharbor_jobs_total{status="RUNNING"} 0`)

	// Assign initial worker.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-metrics-1/assign", "org-alpha", map[string]any{
		"worker_node_id": "node-1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("assign status = %d (%s), want 200", rec.Code, rec.Body.String())
	}

	// 3. After preemption.
	now = now.Add(10 * time.Second)
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-metrics-1/sigterm", "org-alpha", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("sigterm status = %d (%s), want 200", rec.Code, rec.Body.String())
	}

	afterPreempt := getMetrics(t, srv)
	assertMetricLine(t, afterPreempt, `computeharbor_jobs_total{status="CHECKPOINTING"} 1`)
	assertMetricLine(t, afterPreempt, `computeharbor_jobs_total{status="RUNNING"} 0`)
	assertMetricLine(t, afterPreempt, `computeharbor_preemptions_total 1`)

	// 4. After checkpoint commit.
	now = now.Add(50 * time.Millisecond)
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-metrics-1/checkpoint", "org-alpha", map[string]any{
		"step":             42,
		"epoch":            1,
		"loss":             0.25,
		"safetensors_uri":  "s3://bucket/ckpt-42.safetensors",
		"manifest_uri":     "s3://bucket/ckpt-42.json",
		"flush_latency_ms": 12,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("checkpoint status = %d (%s), want 200", rec.Code, rec.Body.String())
	}

	afterCheckpoint := getMetrics(t, srv)
	assertMetricLine(t, afterCheckpoint, `computeharbor_jobs_total{status="MIGRATING"} 1`)
	assertMetricLine(t, afterCheckpoint, `computeharbor_jobs_total{status="CHECKPOINTING"} 0`)
	assertMetricLine(t, afterCheckpoint, `computeharbor_checkpoints_total 1`)

	// 5. After worker reassignment.
	now = now.Add(200 * time.Millisecond)
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-metrics-1/assign", "org-alpha", map[string]any{
		"worker_node_id": "node-2",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("reassign status = %d (%s), want 200", rec.Code, rec.Body.String())
	}

	afterReassign := getMetrics(t, srv)
	assertMetricLine(t, afterReassign, `computeharbor_jobs_total{status="RUNNING"} 1`)
	assertMetricLine(t, afterReassign, `computeharbor_jobs_total{status="MIGRATING"} 0`)
	assertMetricLine(t, afterReassign, `computeharbor_preemptions_total 1`)
	assertMetricLine(t, afterReassign, `computeharbor_checkpoints_total 1`)
	assertMetricLine(t, afterReassign, `computeharbor_max_recovery_duration_ms 250`)
}

type stubWebhookStats struct {
	delivered int64
	failed    int64
	dropped   int64
}

func (s stubWebhookStats) DeliveredCount() int64 { return s.delivered }
func (s stubWebhookStats) FailedCount() int64    { return s.failed }
func (s stubWebhookStats) DroppedCount() int64   { return s.dropped }

func TestMetricsWebhookCounters(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	withoutStats := getMetrics(t, srv)
	if strings.Contains(withoutStats, "computeharbor_webhook_") {
		t.Fatalf("unexpected webhook metrics when WebhookStats is nil:\n%s", withoutStats)
	}

	srv.WithWebhookStats(stubWebhookStats{
		delivered: 42,
		failed:    3,
		dropped:   7,
	})

	withStats := getMetrics(t, srv)
	assertMetricLine(t, withStats, "computeharbor_webhook_delivered_total 42")
	assertMetricLine(t, withStats, "computeharbor_webhook_failed_total 3")
	assertMetricLine(t, withStats, "computeharbor_webhook_dropped_total 7")
}

func TestMetricsDrainingAndHTTPRED(t *testing.T) {
	const token = "secret-token"
	srv := server.New(orchestrator.NewCoordinator()).WithAPIKey(token)

	// Initially not draining and zero HTTP requests recorded.
	initial := getMetricsWithAuth(t, srv, token)
	assertMetricLine(t, initial, "computeharbor_draining 0")
	assertMetricLine(t, initial, "computeharbor_http_request_duration_seconds_count 0")

	// Flip draining state.
	srv.SetDraining(true)
	draining := getMetricsWithAuth(t, srv, token)
	assertMetricLine(t, draining, "computeharbor_draining 1")

	srv.SetDraining(false)
	ready := getMetricsWithAuth(t, srv, token)
	assertMetricLine(t, ready, "computeharbor_draining 0")

	// 200 OK (healthz bypasses bearer auth).
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /healthz status = %d, want 200", rec.Code)
		}
	}

	// 401 Unauthorized (missing bearer token).
	req401 := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	rec401 := httptest.NewRecorder()
	srv.ServeHTTP(rec401, req401)
	if rec401.Code != http.StatusUnauthorized {
		t.Fatalf("GET /v1/jobs without auth status = %d, want 401", rec401.Code)
	}

	// 404 Not Found (with valid bearer token and org header).
	req404 := httptest.NewRequest(http.MethodGet, "/v1/jobs/does-not-exist", nil)
	req404.Header.Set("Authorization", "Bearer "+token)
	req404.Header.Set("X-Org-ID", "org-alpha")
	rec404 := httptest.NewRecorder()
	srv.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("GET /v1/jobs/does-not-exist status = %d, want 404", rec404.Code)
	}

	// Scrape /metrics with valid bearer token.
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	reqMetrics.Header.Set("Authorization", "Bearer secret-token")
	recMetrics := httptest.NewRecorder()
	srv.ServeHTTP(recMetrics, reqMetrics)
	if recMetrics.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", recMetrics.Code)
	}
	body := recMetrics.Body.String()

	assertMetricLine(t, body, `computeharbor_http_requests_total{method="GET",status="200"} 2`)
	assertMetricLine(t, body, `computeharbor_http_requests_total{method="GET",status="401"} 1`)
	assertMetricLine(t, body, `computeharbor_http_requests_total{method="GET",status="404"} 1`)
	assertMetricLine(t, body, "computeharbor_http_request_duration_seconds_count 4")
	if !strings.Contains(body, "computeharbor_http_request_duration_seconds_sum ") {
		t.Fatalf("missing computeharbor_http_request_duration_seconds_sum in:\n%s", body)
	}
}
