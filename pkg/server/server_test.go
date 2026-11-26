package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
		{http.MethodPost, "/v1/jobs/job-iso/cancel", nil},
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

func TestListJobsTenantIsolation(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	// Missing X-Org-ID -> 403
	rec := doJSON(t, srv, http.MethodGet, "/v1/jobs", "", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("GET /v1/jobs without X-Org-ID = %d, want 403", rec.Code)
	}

	// Empty org -> 200 with JSON array []
	rec = doJSON(t, srv, http.MethodGet, "/v1/jobs", "org-empty", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/jobs empty org = %d, want 200", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("GET /v1/jobs empty org body = %q, want []", rec.Body.String())
	}

	jobs := []struct {
		id  string
		org string
	}{
		{"job-2", "org-alpha"},
		{"job-3", "org-beta"},
		{"job-1", "org-alpha"},
	}
	for _, j := range jobs {
		rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
			"job_id":          j.id,
			"org_id":          j.org,
			"gpu":             string(cost.GPUL4),
			"target_steps":    10,
			"max_preemptions": 1,
		})
		if rec.Code != http.StatusCreated {
			t.Fatalf("submit %s = %d, want 201", j.id, rec.Code)
		}
	}

	rec = doJSON(t, srv, http.MethodGet, "/v1/jobs", "org-alpha", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/jobs org-alpha = %d, want 200", rec.Code)
	}
	var alphaJobs []leaseDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &alphaJobs); err != nil {
		t.Fatalf("decode org-alpha jobs: %v", err)
	}
	if len(alphaJobs) != 2 || alphaJobs[0].JobID != "job-1" || alphaJobs[1].JobID != "job-2" {
		t.Fatalf("org-alpha jobs = %+v, want [job-1, job-2]", alphaJobs)
	}

	rec = doJSON(t, srv, http.MethodGet, "/v1/jobs", "org-beta", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/jobs org-beta = %d, want 200", rec.Code)
	}
	var betaJobs []leaseDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &betaJobs); err != nil {
		t.Fatalf("decode org-beta jobs: %v", err)
	}
	if len(betaJobs) != 1 || betaJobs[0].JobID != "job-3" {
		t.Fatalf("org-beta jobs = %+v, want [job-3]", betaJobs)
	}
}

func TestStartReaperAndCancellation(t *testing.T) {
	var clockNano atomic.Int64
	base := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	clockNano.Store(base.UnixNano())

	coord := orchestrator.NewCoordinator().WithClock(func() time.Time {
		return time.Unix(0, clockNano.Load()).UTC()
	})
	srv := server.New(coord)

	// Non-positive interval or ttl closes done immediately.
	select {
	case <-srv.StartReaper(context.Background(), 0, time.Second):
	case <-time.After(time.Second):
		t.Fatal("StartReaper with interval=0 did not close done immediately")
	}
	select {
	case <-srv.StartReaper(context.Background(), time.Second, 0):
	case <-time.After(time.Second):
		t.Fatal("StartReaper with ttl=0 did not close done immediately")
	}

	if _, err := coord.SubmitJob("job-reap", "org-alpha", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-reap", "node-1"); err != nil {
		t.Fatal(err)
	}

	// Advance clock past TTL before starting reaper so first tick reaps deterministically.
	clockNano.Store(base.Add(60 * time.Second).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	done := srv.StartReaper(ctx, 5*time.Millisecond, 30*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		lease, err := coord.GetJob("job-reap")
		if err != nil {
			t.Fatal(err)
		}
		if lease.Status == orchestrator.StatusMigrating {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	lease, err := coord.GetJob("job-reap")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Status != orchestrator.StatusMigrating {
		t.Fatalf("job-reap status = %s, want %s", lease.Status, orchestrator.StatusMigrating)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StartReaper did not exit after context cancellation")
	}
}

func TestCancelJobEndpoint(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	// Submit and assign job.
	rec := doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":          "job-cancel-http",
		"org_id":          "org-alpha",
		"gpu":             string(cost.GPUL4),
		"target_steps":    10,
		"max_preemptions": 1,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit status = %d, want 201", rec.Code)
	}
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-cancel-http/assign", "org-alpha", map[string]string{
		"worker_node_id": "node-1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("assign status = %d, want 200", rec.Code)
	}

	// X-Org-ID enforcement: missing and wrong org return 403.
	for _, org := range []string{"", "org-other"} {
		rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-cancel-http/cancel", org, nil)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("cancel with X-Org-ID=%q status = %d, want 403", org, rec.Code)
		}
	}

	// Valid cancel -> 200, status CANCELLED, worker cleared.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-cancel-http/cancel", "org-alpha", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	lease := decodeLease(t, rec)
	if lease.Status != orchestrator.StatusCancelled || lease.WorkerNodeID != "" {
		t.Fatalf("cancelled lease = %+v, want CANCELLED with empty WorkerNodeID", lease)
	}

	// Idempotent second cancel -> 200.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-cancel-http/cancel", "org-alpha", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("second cancel status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	if got := decodeLease(t, rec); got.Status != orchestrator.StatusCancelled {
		t.Fatalf("second cancel status = %s, want CANCELLED", got.Status)
	}

	// Cancelling a COMPLETED job -> 409 Conflict.
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs", "", map[string]any{
		"job_id":       "job-done-http",
		"org_id":       "org-alpha",
		"gpu":          string(cost.GPUL4),
		"target_steps": 5,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit done job status = %d, want 201", rec.Code)
	}
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-done-http/assign", "org-alpha", map[string]string{
		"worker_node_id": "node-2",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("assign done job status = %d, want 200", rec.Code)
	}
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-done-http/checkpoint", "org-alpha", map[string]any{"step": 5})
	if rec.Code != http.StatusOK {
		t.Fatalf("checkpoint done job status = %d, want 200", rec.Code)
	}
	rec = doJSON(t, srv, http.MethodPost, "/v1/jobs/job-done-http/cancel", "org-alpha", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("cancel completed job status = %d (%s), want 409", rec.Code, rec.Body.String())
	}
}

func TestStartSnapshotterPeriodicAndShutdown(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	srv := server.New(coord).WithLogger(logger)

	// Empty path or non-positive interval closes done immediately.
	select {
	case <-srv.StartSnapshotter(context.Background(), "", time.Second):
	case <-time.After(time.Second):
		t.Fatal("StartSnapshotter with empty path did not close done immediately")
	}
	select {
	case <-srv.StartSnapshotter(context.Background(), filepath.Join(t.TempDir(), "s.json"), 0):
	case <-time.After(time.Second):
		t.Fatal("StartSnapshotter with interval=0 did not close done immediately")
	}

	if _, err := coord.SubmitJob("job-snap-1", "org-alpha", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}

	snapPath := filepath.Join(t.TempDir(), "nested", "state.json")
	ctx, cancel := context.WithCancel(context.Background())
	done := srv.StartSnapshotter(ctx, snapPath, 5*time.Millisecond)

	// Wait for at least one periodic snapshot to be written.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(snapPath); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := os.Stat(snapPath); err != nil {
		cancel()
		<-done
		t.Fatalf("periodic snapshot was not created: %v", err)
	}

	// Mutate coordinator state, then cancel context to trigger final shutdown flush.
	if _, err := coord.SubmitJob("job-snap-2", "org-alpha", cost.GPUL4, 20, 1); err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StartSnapshotter did not exit after context cancellation")
	}

	restored, err := orchestrator.LoadCoordinatorFromFile(snapPath)
	if err != nil {
		t.Fatalf("LoadCoordinatorFromFile: %v", err)
	}
	if _, err := restored.GetJob("job-snap-1"); err != nil {
		t.Fatalf("expected job-snap-1 in final snapshot: %v", err)
	}
	if _, err := restored.GetJob("job-snap-2"); err != nil {
		t.Fatalf("expected job-snap-2 in final shutdown snapshot: %v", err)
	}

	// Verify error logging when snapshot path is unwritable (parent is a regular file).
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	errCtx, errCancel := context.WithCancel(context.Background())
	errDone := srv.StartSnapshotter(errCtx, filepath.Join(blocker, "state.json"), 5*time.Millisecond)
	errCancel()
	select {
	case <-errDone:
	case <-time.After(2 * time.Second):
		t.Fatal("StartSnapshotter error case did not exit")
	}
	if !strings.Contains(logBuf.String(), "coordinator snapshot failed") {
		t.Fatalf("expected error log on snapshot failure, got %q", logBuf.String())
	}
}
