package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// WebhookStats exposes delivery, failure, and drop counters from a webhook dispatcher.
type WebhookStats interface {
	DeliveredCount() int64
	FailedCount() int64
	DroppedCount() int64
}

// Server exposes the ComputeHarbor control plane over HTTP/JSON.
type Server struct {
	coord        *orchestrator.Coordinator
	mux          *http.ServeMux
	logger       *slog.Logger
	limiter      *OrgRateLimiter
	apiKey       string
	orgAPIKeys   map[string]string
	webhookStats WebhookStats
	draining     atomic.Bool
	httpMetrics  *httpMetrics
	barrierMu    sync.Mutex
	barriers     map[string]*orchestrator.RingBarrier
}

// New constructs a Server wrapping the provided Coordinator.
// If coord is nil, a fresh Coordinator is created.
func New(coord *orchestrator.Coordinator) *Server {
	if coord == nil {
		coord = orchestrator.NewCoordinator()
	}
	s := &Server{
		coord:       coord,
		mux:         http.NewServeMux(),
		httpMetrics: newHTTPMetrics(),
		barriers:    make(map[string]*orchestrator.RingBarrier),
	}
	s.registerRoutes()
	return s
}

// WithLogger configures structured HTTP request logging on the Server.
func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.logger = logger
	return s
}

// WithRateLimit configures per-org token-bucket rate limiting on the Server.
func (s *Server) WithRateLimit(rps float64, burst int) *Server {
	s.limiter = NewOrgRateLimiter(rps, burst)
	return s
}

// WithAPIKey configures optional Bearer token authentication on the Server.
func (s *Server) WithAPIKey(token string) *Server {
	s.apiKey = token
	return s
}

// WithOrgAPIKeys configures org-scoped Bearer token authentication on the Server
// using a defensive copy of token -> org_id mappings (ignoring empty keys or orgs).
func (s *Server) WithOrgAPIKeys(keys map[string]string) *Server {
	if len(keys) == 0 {
		s.orgAPIKeys = nil
		return s
	}
	copied := make(map[string]string, len(keys))
	for tok, org := range keys {
		if tok == "" || org == "" {
			continue
		}
		copied[tok] = org
	}
	if len(copied) == 0 {
		s.orgAPIKeys = nil
		return s
	}
	s.orgAPIKeys = copied
	return s
}

// WithWebhookStats configures webhook delivery metrics reporting on the Server.
func (s *Server) WithWebhookStats(stats WebhookStats) *Server {
	s.webhookStats = stats
	return s
}

// SetDraining marks the server as draining (true) or ready (false) for /readyz
// readiness probes.
func (s *Server) SetDraining(v bool) {
	s.draining.Store(v)
}

// IsDraining reports whether the server is currently draining.
func (s *Server) IsDraining() bool {
	return s.draining.Load()
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var handler http.Handler = s.mux
	if s.limiter != nil {
		handler = s.limiter.Middleware(handler)
	}
	if s.apiKey != "" || len(s.orgAPIKeys) > 0 {
		handler = ScopedBearerAuthMiddleware(s.apiKey, s.orgAPIKeys, handler)
	}
	if s.logger != nil {
		handler = LoggingMiddleware(s.logger, handler)
	}
	handler = s.metricsMiddleware(handler)
	handler = SecurityHeadersMiddleware(handler)
	handler = RequestIDMiddleware(handler)
	handler.ServeHTTP(w, r)
}

func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		next.ServeHTTP(rec, r)
		s.httpMetrics.record(r.Method, rec.status, time.Since(start))
	})
}

// StartReaper runs ReapStaleLeases(ttl) on a ticker every interval until ctx is
// cancelled, closing the returned channel once the loop exits.
func (s *Server) StartReaper(ctx context.Context, interval, ttl time.Duration) <-chan struct{} {
	done := make(chan struct{})
	if interval <= 0 || ttl <= 0 {
		close(done)
		return done
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.coord.ReapStaleLeases(ttl)
			}
		}
	}()
	return done
}

// StartSnapshotter periodically persists coordinator state to path every interval
// until ctx is cancelled, then flushes one final snapshot before closing done.
func (s *Server) StartSnapshotter(ctx context.Context, path string, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	if path == "" || interval <= 0 {
		close(done)
		return done
	}

	save := func() {
		if err := s.coord.SaveSnapshot(path); err != nil && s.logger != nil {
			s.logger.Error("coordinator snapshot failed", "path", path, "error", err)
		}
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				save()
				return
			case <-ticker.C:
				save()
			}
		}
	}()
	return done
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /ui", s.handleUI)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /v1/jobs", s.handleListJobs)
	s.mux.HandleFunc("POST /v1/jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /v1/jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("POST /v1/jobs/{id}/assign", s.handleAssignWorker)
	s.mux.HandleFunc("POST /v1/jobs/{id}/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("POST /v1/jobs/{id}/sigterm", s.handleNotifySIGTERM)
	s.mux.HandleFunc("POST /v1/jobs/{id}/checkpoint", s.handleCommitCheckpoint)
	s.mux.HandleFunc("POST /v1/jobs/{id}/barrier", s.handleBarrier)
	s.mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.handleCancelJob)
	s.mux.HandleFunc("GET /v1/cost/rank", s.handleRankCost)
	s.mux.HandleFunc("GET /v1/fleet/rank", s.handleRankFleet)
	s.mux.HandleFunc("POST /v1/schedule", s.handleSchedule)
	s.mux.HandleFunc("GET /v1/billing/summary", s.handleBillingSummary)
}

type errorResponse struct {
	Error string `json:"error"`
}

type submitJobRequest struct {
	JobID          string       `json:"job_id"`
	OrgID          string       `json:"org_id"`
	GPU            cost.GPUType `json:"gpu"`
	TargetSteps    int          `json:"target_steps"`
	MaxPreemptions int          `json:"max_preemptions"`
}

type assignWorkerRequest struct {
	WorkerNodeID string `json:"worker_node_id"`
}

type jobLeaseResponse struct {
	JobID             string                      `json:"job_id"`
	OrgID             string                      `json:"org_id"`
	GPU               cost.GPUType                `json:"gpu"`
	WorkerNodeID      string                      `json:"worker_node_id,omitempty"`
	Status            orchestrator.JobStatus      `json:"status"`
	TargetSteps       int                         `json:"target_steps"`
	CurrentStep       int                         `json:"current_step"`
	PreemptionCount   int                         `json:"preemption_count"`
	MaxPreemptions    int                         `json:"max_preemptions"`
	LatestCheckpoint  *orchestrator.CheckpointRef `json:"latest_checkpoint,omitempty"`
	UpdatedAt         time.Time                   `json:"updated_at"`
	LastHeartbeatAt   *time.Time                  `json:"last_heartbeat_at,omitempty"`
	PreemptedAt       *time.Time                  `json:"preempted_at,omitempty"`
	LastRecoveryDurMs int64                       `json:"last_recovery_dur_ms,omitempty"`
}

func toJobLeaseResponse(lease orchestrator.JobLease) jobLeaseResponse {
	resp := jobLeaseResponse{
		JobID:             lease.JobID,
		OrgID:             lease.OrgID,
		GPU:               lease.GPU,
		WorkerNodeID:      lease.WorkerNodeID,
		Status:            lease.Status,
		TargetSteps:       lease.TargetSteps,
		CurrentStep:       lease.CurrentStep,
		PreemptionCount:   lease.PreemptionCount,
		MaxPreemptions:    lease.MaxPreemptions,
		LatestCheckpoint:  lease.LatestCheckpoint,
		UpdatedAt:         lease.UpdatedAt,
		LastRecoveryDurMs: lease.LastRecoveryDurMs,
	}
	if !lease.LastHeartbeatAt.IsZero() {
		t := lease.LastHeartbeatAt
		resp.LastHeartbeatAt = &t
	}
	if !lease.PreemptedAt.IsZero() {
		t := lease.PreemptedAt
		resp.PreemptedAt = &t
	}
	return resp
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.IsDraining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.JobID) == "" {
		writeError(w, http.StatusBadRequest, "job_id is required")
		return
	}
	if strings.TrimSpace(req.OrgID) == "" {
		writeError(w, http.StatusBadRequest, "org_id is required")
		return
	}
	if strings.TrimSpace(string(req.GPU)) == "" {
		writeError(w, http.StatusBadRequest, "gpu is required")
		return
	}
	if req.TargetSteps <= 0 {
		writeError(w, http.StatusUnprocessableEntity, "target_steps must be positive")
		return
	}
	if req.MaxPreemptions < 0 {
		writeError(w, http.StatusUnprocessableEntity, "max_preemptions cannot be negative")
		return
	}

	lease, err := s.coord.SubmitJob(req.JobID, req.OrgID, req.GPU, req.TargetSteps, req.MaxPreemptions)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toJobLeaseResponse(lease))
}

var validListJobStatuses = map[orchestrator.JobStatus]bool{
	orchestrator.StatusQueued:    true,
	orchestrator.StatusRunning:   true,
	orchestrator.StatusMigrating: true,
	orchestrator.StatusCompleted: true,
	orchestrator.StatusFailed:    true,
	orchestrator.StatusCancelled: true,
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	orgID := strings.TrimSpace(r.Header.Get("X-Org-ID"))
	if orgID == "" {
		writeError(w, http.StatusForbidden, "X-Org-ID header is required")
		return
	}

	q := r.URL.Query()
	statusFilter := strings.TrimSpace(q.Get("status"))
	if statusFilter != "" && !validListJobStatuses[orchestrator.JobStatus(statusFilter)] {
		writeError(w, http.StatusBadRequest, "invalid status filter")
		return
	}

	limit := 0
	if rawLimit := strings.TrimSpace(q.Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		limit = parsed
	}

	offset := 0
	if rawOffset := strings.TrimSpace(q.Get("offset")); rawOffset != "" {
		parsed, err := strconv.Atoi(rawOffset)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		offset = parsed
	}

	leases := s.coord.ListJobs(orgID)
	filtered := leases
	if statusFilter != "" {
		targetStatus := orchestrator.JobStatus(statusFilter)
		filtered = make([]orchestrator.JobLease, 0, len(leases))
		for _, lease := range leases {
			if lease.Status == targetStatus {
				filtered = append(filtered, lease)
			}
		}
	}

	w.Header().Set("X-Total-Count", strconv.Itoa(len(filtered)))

	paged := filtered
	if offset >= len(paged) {
		paged = nil
	} else {
		end := len(paged)
		if limit > 0 {
			end = min(offset+limit, len(paged))
		}
		paged = paged[offset:end]
	}

	resp := make([]jobLeaseResponse, 0, len(paged))
	for _, lease := range paged {
		resp = append(resp, toJobLeaseResponse(lease))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	lease, ok := s.authorizeJob(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, toJobLeaseResponse(lease))
}

func (s *Server) handleAssignWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.authorizeJob(w, r, id); !ok {
		return
	}

	var req assignWorkerRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.WorkerNodeID) == "" {
		writeError(w, http.StatusBadRequest, "worker_node_id is required")
		return
	}

	lease, err := s.coord.AssignWorker(id, req.WorkerNodeID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobLeaseResponse(lease))
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.authorizeJob(w, r, id); !ok {
		return
	}

	var req assignWorkerRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.WorkerNodeID) == "" {
		writeError(w, http.StatusBadRequest, "worker_node_id is required")
		return
	}

	lease, err := s.coord.Heartbeat(id, req.WorkerNodeID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobLeaseResponse(lease))
}

func (s *Server) handleNotifySIGTERM(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.authorizeJob(w, r, id); !ok {
		return
	}

	lease, err := s.coord.NotifySIGTERM(id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobLeaseResponse(lease))
}

func (s *Server) handleCommitCheckpoint(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.authorizeJob(w, r, id); !ok {
		return
	}

	var ref orchestrator.CheckpointRef
	if err := decodeJSONBody(w, r, &ref); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if ref.Step < 0 {
		writeError(w, http.StatusUnprocessableEntity, "step cannot be negative")
		return
	}

	lease, err := s.coord.CommitCheckpoint(id, ref)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	if lease.Status == orchestrator.StatusCompleted || lease.Status == orchestrator.StatusFailed {
		s.deleteBarrier(id)
	}
	writeJSON(w, http.StatusOK, toJobLeaseResponse(lease))
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.authorizeJob(w, r, id); !ok {
		return
	}

	lease, err := s.coord.CancelJob(id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	s.deleteBarrier(id)
	writeJSON(w, http.StatusOK, toJobLeaseResponse(lease))
}

func (s *Server) handleRankCost(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	minVRAM, err := parseIntQuery(q.Get("min_vram"), "min_vram", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if minVRAM < 0 {
		writeError(w, http.StatusUnprocessableEntity, "min_vram cannot be negative")
		return
	}

	count, err := parseIntQuery(q.Get("count"), "count", 1)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	hours, err := parseFloatQuery(q.Get("hours"), "hours", 10.0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	fee, err := parseFloatQuery(q.Get("fee"), "fee", 0.12)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ranked, err := cost.RankPoolsForWorkload(minVRAM, count, hours, fee)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ranked)
}

func (s *Server) authorizeJob(w http.ResponseWriter, r *http.Request, jobID string) (orchestrator.JobLease, bool) {
	orgID := strings.TrimSpace(r.Header.Get("X-Org-ID"))
	if orgID == "" {
		writeError(w, http.StatusForbidden, "X-Org-ID header is required")
		return orchestrator.JobLease{}, false
	}

	lease, err := s.coord.GetJob(jobID)
	if err != nil {
		writeDomainError(w, err)
		return orchestrator.JobLease{}, false
	}

	if lease.OrgID != orgID {
		writeError(w, http.StatusForbidden, "X-Org-ID does not match job organization")
		return orchestrator.JobLease{}, false
	}
	return lease, true
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, orchestrator.ErrJobNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, orchestrator.ErrJobExists),
		errors.Is(err, orchestrator.ErrInvalidTransition),
		errors.Is(err, orchestrator.ErrMaxPreemptions),
		errors.Is(err, orchestrator.ErrWorkerMismatch):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, orchestrator.ErrInvalidStep),
		errors.Is(err, cost.ErrUnknownGPU),
		errors.Is(err, cost.ErrInvalidHours),
		errors.Is(err, cost.ErrInvalidCount),
		errors.Is(err, cost.ErrInvalidFee),
		errors.Is(err, cost.ErrRecoveryWindow),
		errors.Is(err, cost.ErrNegativePreemption),
		errors.Is(err, cost.ErrInvalidVRAM),
		errors.Is(err, cost.ErrInvalidAvailability),
		errors.Is(err, cost.ErrUnknownProvider):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) error {
	if r.Body == nil {
		return errors.New("request body is required")
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

func parseIntQuery(raw, name string, defaultVal int) (int, error) {
	if raw == "" {
		return defaultVal, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid integer query parameter %q", name)
	}
	return v, nil
}

func parseFloatQuery(raw, name string, defaultVal float64) (float64, error) {
	if raw == "" {
		return defaultVal, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric query parameter %q", name)
	}
	return v, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
