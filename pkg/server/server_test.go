package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

type leaseDTO struct {
	JobID             string                      `json:"job_id"`
	OrgID             string                      `json:"org_id"`
	GPU               cost.GPUType                `json:"gpu"`
	WorkerNodeID      string                      `json:"worker_node_id"`
	Status            orchestrator.JobStatus      `json:"status"`
	TargetSteps       int                         `json:"target_steps"`
	CurrentStep       int                         `json:"current_step"`
	PreemptionCount   int                         `json:"preemption_count"`
	MaxPreemptions    int                         `json:"max_preemptions"`
	LatestCheckpoint  *orchestrator.CheckpointRef `json:"latest_checkpoint"`
	LastHeartbeatAt   *time.Time                  `json:"last_heartbeat_at"`
	LastRecoveryDurMs int64                       `json:"last_recovery_dur_ms"`
}

func doJSON(t *testing.T, srv http.Handler, method, path, orgID string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else if s, ok := body.(string); ok {
		reader = bytes.NewReader([]byte(s))
	} else {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if orgID != "" {
		req.Header.Set("X-Org-ID", orgID)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func decodeLease(t *testing.T, rec *httptest.ResponseRecorder) leaseDTO {
	t.Helper()
	var out leaseDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode lease response (%s): %v", rec.Body.String(), err)
	}
	return out
}

func TestHealthz(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())
	rec := doJSON(t, srv, http.MethodGet, "/healthz", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", rec.Code)
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode healthz: %v", err)
	}
	if payload["status"] != "ok" {
		t.Errorf("healthz status = %q, want %q", payload["status"], "ok")
	}
}

func TestThreePreemptionHTTPLifecycle(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	coord := orchestrator.NewCoordinator().WithClock(func() time.Time { return now })
	srv := server.New(coord)

	// 1. Submit job allowing 3 preemptions.
	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":          "job-3p",
		"org_id":          "org-alpha",
		"gpu":             string(cost.GPUA100_40G),
		"target_steps":    100,
		"max_preemptions": 3,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit status = %d (%s), want 201", rec.Code, rec.Body.String())
	}
	lease := decodeLease(t, rec)
	if lease.Status != orchestrator.StatusQueued {
		t.Fatalf("status = %s, want QUEUED", lease.Status)
	}

	// 2. Assign initial worker.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-3p/assign", "org-alpha", map[string]string{
		"worker_node_id": "node-1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("assign status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	lease = decodeLease(t, rec)
	if lease.Status != orchestrator.StatusRunning || lease.WorkerNodeID != "node-1" {
		t.Fatalf("unexpected lease after assign: %+v", lease)
	}

	// 3. Run 3 preemption -> checkpoint -> reassign cycles.
	for i := 1; i <= 3; i++ {
		now = now.Add(10 * time.Second)
		rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-3p/sigterm", "org-alpha", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("sigterm %d status = %d (%s), want 200", i, rec.Code, rec.Body.String())
		}
		lease = decodeLease(t, rec)
		if lease.Status != orchestrator.StatusCheckpointing || lease.PreemptionCount != i {
			t.Fatalf("sigterm %d lease = %+v", i, lease)
		}

		now = now.Add(4 * time.Second)
		step := i * 25
		rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-3p/checkpoint", "org-alpha", orchestrator.CheckpointRef{
			Step:           step,
			Epoch:          i,
			Loss:           1.0 / float64(i),
			SafetensorsURI: "s3://ckpt/model.safetensors",
			ManifestURI:    "s3://ckpt/manifest.json",
			FlushedAt:      now,
			FlushLatencyMs: 250,
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("checkpoint %d status = %d (%s), want 200", i, rec.Code, rec.Body.String())
		}
		lease = decodeLease(t, rec)
		if lease.Status != orchestrator.StatusMigrating || lease.CurrentStep != step {
			t.Fatalf("checkpoint %d lease = %+v", i, lease)
		}

		now = now.Add(8 * time.Second)
		nextWorker := "node-" + string(rune('1'+i))
		rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-3p/assign", "org-alpha", map[string]string{
			"worker_node_id": nextWorker,
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("reassign %d status = %d (%s), want 200", i, rec.Code, rec.Body.String())
		}
		lease = decodeLease(t, rec)
		if lease.Status != orchestrator.StatusRunning || lease.LastRecoveryDurMs != 12000 {
			t.Fatalf("reassign %d lease = %+v", i, lease)
		}
	}

	// 4. Complete job via final checkpoint.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-3p/checkpoint", "org-alpha", orchestrator.CheckpointRef{
		Step:  100,
		Epoch: 4,
		Loss:  0.1,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("final checkpoint status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	lease = decodeLease(t, rec)
	if lease.Status != orchestrator.StatusCompleted || lease.CurrentStep != 100 {
		t.Fatalf("final lease = %+v, want COMPLETED at step 100", lease)
	}

	// 5. Verify GET /v1/jobs/{id} returns completed state.
	rec = doJSON(t, srv, http.MethodGet, "/v1/jobs/job-3p", "org-alpha", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get job status = %d, want 200", rec.Code)
	}
	lease = decodeLease(t, rec)
	if lease.PreemptionCount != 3 || lease.Status != orchestrator.StatusCompleted {
		t.Fatalf("get job lease = %+v", lease)
	}
}

func TestTenantIsolationAndNotFound(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":          "job-iso",
		"org_id":          "org-owner",
		"gpu":             string(cost.GPUL4),
		"target_steps":    10,
		"max_preemptions": 1,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit status = %d, want 201", rec.Code)
	}

	endpoints := []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodGet, "/v1/jobs/job-iso", nil},
		{http.MethodPost, "/v1/jobs/job-iso/assign", map[string]string{"worker_node_id": "w1"}},
		{http.MethodPost, "/v1/jobs/job-iso/heartbeat", map[string]string{"worker_node_id": "w1"}},
		{http.MethodPost, "/v1/jobs/job-iso/sigterm", nil},
		{http.MethodPost, "/v1/jobs/job-iso/checkpoint", map[string]any{"step": 1}},
	}

	for _, ep := range endpoints {
		// Missing X-Org-ID -> 403
		rec = doJSON(t, srv, ep.method, ep.path, "", ep.body)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s %s without X-Org-ID = %d, want 403", ep.method, ep.path, rec.Code)
		}
		// Wrong X-Org-ID -> 403
		rec = doJSON(t, srv, ep.method, ep.path, "org-other", ep.body)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s %s with wrong X-Org-ID = %d, want 403", ep.method, ep.path, rec.Code)
		}
	}

	// Non-existent job -> 404
	rec = doJSON(t, srv, http.MethodGet, "/v1/jobs/does-not-exist", "org-owner", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET missing job = %d, want 404", rec.Code)
	}
}

func TestBoundaryValidationAndErrorMapping(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	// Malformed JSON -> 400
	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs", "", "{bad json")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("malformed json = %d, want 400", rec.Code)
	}

	// Unknown JSON field -> 400
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", `{"job_id":"j1","org_id":"o1","gpu":"nvidia-l4-24gb","target_steps":10,"extra":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("unknown field = %d, want 400", rec.Code)
	}

	// Missing required fields -> 400
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"org_id":       "o1",
		"gpu":          string(cost.GPUL4),
		"target_steps": 10,
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("missing job_id = %d, want 400", rec.Code)
	}

	// Semantic validation: target_steps <= 0 -> 422
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":       "j-bad-steps",
		"org_id":       "o1",
		"gpu":          string(cost.GPUL4),
		"target_steps": 0,
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("target_steps=0 = %d, want 422", rec.Code)
	}

	// Unknown GPU -> 422
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":       "j-bad-gpu",
		"org_id":       "o1",
		"gpu":          "nvidia-unknown",
		"target_steps": 10,
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("unknown gpu = %d, want 422", rec.Code)
	}

	// Valid job submission
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":          "j-conflict",
		"org_id":          "o1",
		"gpu":             string(cost.GPUL4),
		"target_steps":    10,
		"max_preemptions": 0,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit valid job = %d, want 201", rec.Code)
	}

	// Duplicate job_id -> 409
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":       "j-conflict",
		"org_id":       "o1",
		"gpu":          string(cost.GPUL4),
		"target_steps": 10,
	})
	if rec.Code != http.StatusConflict {
		t.Errorf("duplicate job = %d, want 409", rec.Code)
	}

	// Invalid transition: sigterm while QUEUED -> 409
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/j-conflict/sigterm", "o1", nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("sigterm while QUEUED = %d, want 409", rec.Code)
	}

	// Assign empty worker_node_id -> 400
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/j-conflict/assign", "o1", map[string]string{
		"worker_node_id": "  ",
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("empty worker_node_id = %d, want 400", rec.Code)
	}

	// Assign valid worker -> 200
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/j-conflict/assign", "o1", map[string]string{
		"worker_node_id": "w1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("assign = %d, want 200", rec.Code)
	}

	// Commit checkpoint at step 5 -> 200, then regress to step 3 -> 422
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/j-conflict/checkpoint", "o1", map[string]any{"step": 5})
	if rec.Code != http.StatusOK {
		t.Fatalf("checkpoint step 5 = %d, want 200", rec.Code)
	}
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/j-conflict/checkpoint", "o1", map[string]any{"step": 3})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("regressing checkpoint = %d, want 422", rec.Code)
	}

	// Max preemptions exceeded (max_preemptions=0) -> 409 and job transitions to FAILED
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/j-conflict/sigterm", "o1", nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("max preemptions exceeded = %d, want 409", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "maximum preemption") {
		t.Errorf("unexpected error message: %s", rec.Body.String())
	}
}

func TestRankCostEndpoint(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	rec := doJSON(t, srv, http.MethodGet, "/v1/cost/rank?min_vram=24&count=4&hours=12&fee=0.12", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/cost/rank status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var ranked []cost.EstimateResult
	if err := json.Unmarshal(rec.Body.Bytes(), &ranked); err != nil {
		t.Fatalf("decode ranked response: %v", err)
	}
	if len(ranked) != 5 {
		t.Fatalf("len(ranked) = %d, want 5", len(ranked))
	}
	for i := 1; i < len(ranked); i++ {
		if ranked[i].NetCustomerCostUSD < ranked[i-1].NetCustomerCostUSD {
			t.Errorf("results not sorted by NetCustomerCostUSD at index %d", i)
		}
	}

	// Filter by 80GB VRAM
	rec = doJSON(t, srv, http.MethodGet, "/v1/cost/rank?min_vram=80&count=2&hours=5&fee=0.1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/cost/rank 80GB status = %d, want 200", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ranked); err != nil {
		t.Fatalf("decode 80GB ranked: %v", err)
	}
	if len(ranked) != 2 {
		t.Errorf("len(ranked 80GB) = %d, want 2", len(ranked))
	}

	// Bad query parameter type -> 400
	rec = doJSON(t, srv, http.MethodGet, "/v1/cost/rank?min_vram=abc", "", nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("non-int min_vram = %d, want 400", rec.Code)
	}

	// Invalid semantic query parameter -> 422
	rec = doJSON(t, srv, http.MethodGet, "/v1/cost/rank?count=0&hours=10&fee=0.12", "", nil)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("count=0 = %d, want 422", rec.Code)
	}
	rec = doJSON(t, srv, http.MethodGet, "/v1/cost/rank?min_vram=-1", "", nil)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("min_vram=-1 = %d, want 422", rec.Code)
	}
}

func TestHeartbeatEndpoint(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	coord := orchestrator.NewCoordinator().WithClock(func() time.Time { return now })
	srv := server.New(coord)

	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":          "job-hb-http",
		"org_id":          "org-alpha",
		"gpu":             string(cost.GPUL4),
		"target_steps":    10,
		"max_preemptions": 1,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit status = %d, want 201", rec.Code)
	}

	// Heartbeat before assignment -> 409 Conflict (invalid transition).
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-hb-http/heartbeat", "org-alpha", map[string]string{
		"worker_node_id": "node-1",
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("heartbeat while QUEUED = %d, want 409", rec.Code)
	}

	// Assign worker.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-hb-http/assign", "org-alpha", map[string]string{
		"worker_node_id": "node-1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("assign status = %d, want 200", rec.Code)
	}

	// Missing worker_node_id -> 400 Bad Request.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-hb-http/heartbeat", "org-alpha", map[string]string{
		"worker_node_id": "   ",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty worker_node_id = %d, want 400", rec.Code)
	}

	// Mismatched worker_node_id -> 409 Conflict.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-hb-http/heartbeat", "org-alpha", map[string]string{
		"worker_node_id": "node-wrong",
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("mismatched worker_node_id = %d, want 409", rec.Code)
	}

	// Valid heartbeat -> 200 OK with updated last_heartbeat_at.
	now = now.Add(10 * time.Second)
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-hb-http/heartbeat", "org-alpha", map[string]string{
		"worker_node_id": "node-1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("valid heartbeat = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	lease := decodeLease(t, rec)
	if lease.LastHeartbeatAt == nil || !lease.LastHeartbeatAt.Equal(now) {
		t.Fatalf("last_heartbeat_at = %v, want %v", lease.LastHeartbeatAt, now)
	}
}
