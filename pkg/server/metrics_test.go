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
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
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
