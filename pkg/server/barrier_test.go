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
