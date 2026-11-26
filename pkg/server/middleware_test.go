package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

func TestLoggingMiddlewareJSONOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := server.LoggingMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.Header.Set("X-Org-ID", "org-alpha")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rec.Code)
	}

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log output: %v (raw: %s)", err, buf.String())
	}

	if got := entry["method"]; got != http.MethodPost {
		t.Fatalf("expected method POST, got %v", got)
	}
	if got := entry["path"]; got != "/v1/jobs" {
		t.Fatalf("expected path /v1/jobs, got %v", got)
	}
	if got := entry["status"]; got != float64(http.StatusCreated) {
		t.Fatalf("expected status 201, got %v", got)
	}
	if got := entry["org_id"]; got != "org-alpha" {
		t.Fatalf("expected org_id org-alpha, got %v", got)
	}
	if _, ok := entry["duration_ms"].(float64); !ok {
		t.Fatalf("expected numeric duration_ms, got %T (%v)", entry["duration_ms"], entry["duration_ms"])
	}
}

func TestLoggingMiddlewareDefaultsAnonymousOrg(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := server.LoggingMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log output: %v", err)
	}
	if got := entry["status"]; got != float64(http.StatusOK) {
		t.Fatalf("expected default status 200, got %v", got)
	}
	if got := entry["org_id"]; got != "anonymous" {
		t.Fatalf("expected org_id anonymous, got %v", got)
	}
}

func TestOrgRateLimiterBurstAnd429(t *testing.T) {
	limiter := server.NewOrgRateLimiter(1, 3)
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		req.Header.Set("X-Org-ID", "org-burst")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i+1, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Org-ID", "org-burst")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("expected Retry-After: 1, got %q", got)
	}
}

func TestOrgRateLimiterPerOrgIsolationAndAnonymous(t *testing.T) {
	limiter := server.NewOrgRateLimiter(0.01, 1)
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Exhaust org-a.
	if !limiter.Allow("org-a") {
		t.Fatal("expected first org-a request to be allowed")
	}
	if limiter.Allow("org-a") {
		t.Fatal("expected second org-a request to be rate-limited")
	}

	// org-b has its own independent bucket.
	if !limiter.Allow("org-b") {
		t.Fatal("expected org-b request to be allowed independently of org-a")
	}

	// Requests without X-Org-ID share the "anonymous" bucket.
	req1 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected first anonymous request to succeed, got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second anonymous request to return 429, got %d", rec2.Code)
	}
}

func TestOrgRateLimiterConcurrent(t *testing.T) {
	limiter := server.NewOrgRateLimiter(1000, 10)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			org := "org-1"
			if id%2 == 0 {
				org = "org-2"
			}
			_ = limiter.Allow(org)
		}(i)
	}
	wg.Wait()
}

func TestServerWithLoggerAndRateLimit(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	srv := server.New(orchestrator.NewCoordinator()).
		WithLogger(logger).
		WithRateLimit(0.01, 1)

	req1 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req1.Header.Set("X-Org-ID", "org-srv")
	rec1 := httptest.NewRecorder()
	srv.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected first request 200, got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req2.Header.Set("X-Org-ID", "org-srv")
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request 429, got %d", rec2.Code)
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d (%s)", len(lines), buf.String())
	}

	var second map[string]any
	if err := json.Unmarshal(lines[1], &second); err != nil {
		t.Fatalf("unmarshal second log line: %v", err)
	}
	if got := second["status"]; got != float64(http.StatusTooManyRequests) {
		t.Fatalf("expected logged status 429, got %v", got)
	}
}

func TestBearerAuthMiddleware(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("empty apiKey is a no-op", func(t *testing.T) {
		handler := server.BearerAuthMiddleware("", okHandler)
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 when apiKey is empty, got %d", rec.Code)
		}
	})

	authHandler := server.BearerAuthMiddleware("secret-token-123", okHandler)

	cases := []struct {
		name       string
		method     string
		path       string
		authHeader string
		wantStatus int
	}{
		{
			name:       "missing authorization header",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong bearer token",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer wrong-token",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "non-bearer scheme",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Basic secret-token-123",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "valid bearer token",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer secret-token-123",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GET /healthz bypasses auth",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GET /ui bypasses auth",
			method:     http.MethodGet,
			path:       "/ui",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST /healthz does not bypass auth",
			method:     http.MethodPost,
			path:       "/healthz",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()
			authHandler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.wantStatus, rec.Code, rec.Body.String())
			}
			if tc.wantStatus == http.StatusUnauthorized {
				var body map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("unmarshal 401 body: %v", err)
				}
				if body["error"] != "unauthorized" {
					t.Fatalf("expected error=unauthorized, got %q", body["error"])
				}
			}
		})
	}
}

func TestOrgRateLimiterEviction(t *testing.T) {
	limiter := server.NewOrgRateLimiter(0.0001, 1)

	if !limiter.Allow("org-victim") {
		t.Fatal("expected initial org-victim request to be allowed")
	}
	if limiter.Allow("org-victim") {
		t.Fatal("expected second org-victim request to be rate-limited before eviction")
	}

	// Fill up to maxBuckets (10000) with distinct orgs so org-victim (oldest) is evicted.
	for i := 0; i < 10000; i++ {
		if !limiter.Allow(fmt.Sprintf("org-fill-%d", i)) {
			t.Fatalf("expected org-fill-%d first request to be allowed", i)
		}
	}

	// After eviction, org-victim gets a fresh bucket.
	if !limiter.Allow("org-victim") {
		t.Fatal("expected org-victim to be allowed after bucket eviction")
	}
}
