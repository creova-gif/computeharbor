package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
)

func TestBillingSummaryEndpoint(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	if _, err := coord.SubmitJob("job-1", "org-acme", cost.GPUH100_80G, 1000, 3); err != nil {
		t.Fatalf("submit job-1: %v", err)
	}
	if _, err := coord.AssignWorker("job-1", "node-1"); err != nil {
		t.Fatalf("assign job-1: %v", err)
	}
	if _, err := coord.CommitCheckpoint("job-1", orchestrator.CheckpointRef{Step: 360}); err != nil {
		t.Fatalf("checkpoint job-1: %v", err)
	}
	if _, err := coord.SubmitJob("job-other", "org-other", cost.GPUL4, 500, 1); err != nil {
		t.Fatalf("submit job-other: %v", err)
	}

	srv := New(coord)

	// Missing X-Org-ID -> 403.
	req := httptest.NewRequest(http.MethodGet, "/v1/billing/summary", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 without X-Org-ID, got %d: %s", rec.Code, rec.Body.String())
	}

	// Non-numeric fee -> 400.
	req = httptest.NewRequest(http.MethodGet, "/v1/billing/summary?fee=abc", nil)
	req.Header.Set("X-Org-ID", "org-acme")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid fee, got %d: %s", rec.Code, rec.Body.String())
	}

	// Out-of-range fee -> 422.
	req = httptest.NewRequest(http.MethodGet, "/v1/billing/summary?fee=0.75", nil)
	req.Header.Set("X-Org-ID", "org-acme")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for out-of-range fee, got %d: %s", rec.Code, rec.Body.String())
	}

	// Valid request with default fee.
	req = httptest.NewRequest(http.MethodGet, "/v1/billing/summary", nil)
	req.Header.Set("X-Org-ID", "org-acme")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var summary orchestrator.BillingSummary
	if err := json.NewDecoder(rec.Body).Decode(&summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.OrgID != "org-acme" || summary.TotalJobs != 1 || summary.TotalSteps != 360 {
		t.Fatalf("unexpected billing summary: %+v", summary)
	}
	if summary.EstimatedOnDemandUSD <= 0 || summary.NetSavingsUSD <= 0 {
		t.Fatalf("expected positive cost and savings, got %+v", summary)
	}

	// Custom fee query parameter overrides default.
	req = httptest.NewRequest(http.MethodGet, "/v1/billing/summary?fee=0.25", nil)
	req.Header.Set("X-Org-ID", "org-acme")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for custom fee, got %d: %s", rec.Code, rec.Body.String())
	}
	var custom orchestrator.BillingSummary
	if err := json.NewDecoder(rec.Body).Decode(&custom); err != nil {
		t.Fatalf("decode custom summary: %v", err)
	}
	if custom.PlatformFeeUSD <= summary.PlatformFeeUSD {
		t.Fatalf("expected higher fee at 0.25 (%v) vs 0.12 (%v)", custom.PlatformFeeUSD, summary.PlatformFeeUSD)
	}
}
