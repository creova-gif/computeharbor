package server_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

type barrierDTO struct {
	JobID         string `json:"job_id"`
	Step          int    `json:"step"`
	Rank          int    `json:"rank"`
	WorldSize     int    `json:"world_size"`
	AllReady      bool   `json:"all_ready"`
	CommittedStep int    `json:"committed_step"`
}

func TestBarrierEndpoint(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	srv := server.New(coord)

	if _, err := coord.SubmitJob("job-ring-1", "org-a", cost.GPUH100_80G, 100, 3); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	// Rank 0 commits step 10 -> not all ready yet.
	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs/job-ring-1/barrier", "org-a", map[string]any{
		"world_size": 2,
		"rank":       0,
		"step":       10,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("rank 0 status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp0 barrierDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &resp0); err != nil {
		t.Fatalf("decode rank 0 response: %v", err)
	}
	if resp0.JobID != "job-ring-1" || resp0.Step != 10 || resp0.Rank != 0 || resp0.WorldSize != 2 {
		t.Fatalf("unexpected rank 0 metadata: %+v", resp0)
	}
	if resp0.AllReady {
		t.Fatalf("rank 0 all_ready = true, want false")
	}
	if resp0.CommittedStep != -1 {
		t.Fatalf("rank 0 committed_step = %d, want -1", resp0.CommittedStep)
	}

	// Rank 1 commits step 10 -> barrier advances to step 10.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-ring-1/barrier", "org-a", map[string]any{
		"world_size": 2,
		"rank":       1,
		"step":       10,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("rank 1 status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp1 barrierDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("decode rank 1 response: %v", err)
	}
	if !resp1.AllReady || resp1.CommittedStep != 10 {
		t.Fatalf("rank 1 response = %+v, want all_ready=true committed_step=10", resp1)
	}

	// Mismatched world_size -> 409 Conflict.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-ring-1/barrier", "org-a", map[string]any{
		"world_size": 4,
		"rank":       0,
		"step":       20,
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("world_size mismatch status = %d, want 409: %s", rec.Code, rec.Body.String())
	}

	// Out-of-range rank -> 422 Unprocessable Entity.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-ring-1/barrier", "org-a", map[string]any{
		"world_size": 2,
		"rank":       2,
		"step":       20,
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("out-of-range rank status = %d, want 422: %s", rec.Code, rec.Body.String())
	}

	// Cross-org isolation -> 403 Forbidden.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-ring-1/barrier", "org-b", map[string]any{
		"world_size": 2,
		"rank":       0,
		"step":       20,
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-org status = %d, want 403: %s", rec.Code, rec.Body.String())
	}

	// Missing X-Org-ID -> 403 Forbidden.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-ring-1/barrier", "", map[string]any{
		"world_size": 2,
		"rank":       0,
		"step":       20,
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("missing org status = %d, want 403: %s", rec.Code, rec.Body.String())
	}
}

func TestBarrierCleanupOnCompletionAndCancel(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	srv := server.New(coord)

	// 1. Completion via final checkpoint removes the barrier entry.
	if _, err := coord.SubmitJob("job-complete", "org-a", cost.GPUH100_80G, 100, 3); err != nil {
		t.Fatalf("SubmitJob complete: %v", err)
	}
	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs/job-complete/assign", "org-a", map[string]string{
		"worker_node_id": "node-1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("assign complete status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-complete/barrier", "org-a", map[string]any{
		"world_size": 2,
		"rank":       0,
		"step":       10,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("barrier complete status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !srv.HasBarrier("job-complete") {
		t.Fatal("expected barrier entry for job-complete before completion")
	}

	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-complete/checkpoint", "org-a", orchestrator.CheckpointRef{
		Step: 100,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("checkpoint complete status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if srv.HasBarrier("job-complete") {
		t.Fatal("expected barrier entry for job-complete to be deleted on completion")
	}

	// 2. Cancellation removes the barrier entry.
	if _, err := coord.SubmitJob("job-cancel", "org-a", cost.GPUH100_80G, 100, 3); err != nil {
		t.Fatalf("SubmitJob cancel: %v", err)
	}
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-cancel/barrier", "org-a", map[string]any{
		"world_size": 2,
		"rank":       0,
		"step":       10,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("barrier cancel status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !srv.HasBarrier("job-cancel") {
		t.Fatal("expected barrier entry for job-cancel before cancellation")
	}

	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-cancel/cancel", "org-a", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if srv.HasBarrier("job-cancel") {
		t.Fatal("expected barrier entry for job-cancel to be deleted on cancellation")
	}
}
