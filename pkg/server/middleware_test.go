package server_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
			name:       "GET /readyz bypasses auth",
			method:     http.MethodGet,
			path:       "/readyz",
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
		{
			name:       "POST /readyz does not bypass auth",
			method:     http.MethodPost,
			path:       "/readyz",
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

func TestRequestIDMiddleware(t *testing.T) {
	t.Run("generates 16-byte hex ID when absent", func(t *testing.T) {
		var ctxID string
		handler := server.RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctxID = server.RequestIDFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		got := rec.Header().Get("X-Request-ID")
		if len(got) != 32 {
			t.Fatalf("expected 32-char hex X-Request-ID, got %q (len %d)", got, len(got))
		}
		if _, err := hex.DecodeString(got); err != nil {
			t.Fatalf("expected valid hex X-Request-ID, got %q: %v", got, err)
		}
		if ctxID != got {
			t.Fatalf("context request ID %q != response header %q", ctxID, got)
		}
	})

	t.Run("preserves valid incoming X-Request-ID", func(t *testing.T) {
		var ctxID string
		handler := server.RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctxID = server.RequestIDFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		const incoming = "req-abc-123_XYZ"
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		req.Header.Set("X-Request-ID", incoming)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if got := rec.Header().Get("X-Request-ID"); got != incoming {
			t.Fatalf("expected preserved X-Request-ID %q, got %q", incoming, got)
		}
		if ctxID != incoming {
			t.Fatalf("expected context request ID %q, got %q", incoming, ctxID)
		}
	})

	t.Run("rejects invalid incoming X-Request-ID and generates fresh one", func(t *testing.T) {
		invalidInputs := []struct {
			name string
			val  string
		}{
			{name: "whitespace only", val: "   "},
			{name: "newline control char", val: "bad\nid"},
			{name: "null control char", val: "bad\x00id"},
			{name: "del control char", val: "bad\x7fid"},
			{name: "non-ascii", val: "bad-🚀"},
			{name: "oversized >128 chars", val: strings.Repeat("a", 129)},
		}

		for _, tc := range invalidInputs {
			t.Run(tc.name, func(t *testing.T) {
				var ctxID string
				handler := server.RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ctxID = server.RequestIDFromContext(r.Context())
					w.WriteHeader(http.StatusOK)
				}))

				req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
				req.Header.Set("X-Request-ID", tc.val)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				got := rec.Header().Get("X-Request-ID")
				if got == tc.val || len(got) != 32 {
					t.Fatalf("expected fresh 32-char hex ID, got %q", got)
				}
				if _, err := hex.DecodeString(got); err != nil {
					t.Fatalf("expected valid hex ID, got %q: %v", got, err)
				}
				if ctxID != got {
					t.Fatalf("context request ID %q != response header %q", ctxID, got)
				}
			})
		}
	})
}

func TestLoggingMiddlewareIncludesRequestID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := server.RequestIDMiddleware(server.LoggingMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	req.Header.Set("X-Request-ID", "corr-id-42")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log output: %v (raw: %s)", err, buf.String())
	}
	if got := entry["request_id"]; got != "corr-id-42" {
		t.Fatalf("expected request_id corr-id-42, got %v", got)
	}
}

func TestScopedBearerAuthMiddleware(t *testing.T) {
	orgKeys := map[string]string{
		"tok-alpha": "org-alpha",
		"tok-beta":  "org-beta",
	}

	t.Run("empty adminKey and orgKeys is a no-op", func(t *testing.T) {
		handler := server.ScopedBearerAuthMiddleware("", nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	})

	cases := []struct {
		name       string
		method     string
		path       string
		authHeader string
		orgHeader  string
		wantStatus int
		wantOrg    string
		wantErr    string
	}{
		{
			name:       "missing token returns 401",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			wantStatus: http.StatusUnauthorized,
			wantErr:    "unauthorized",
		},
		{
			name:       "invalid token returns 401",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer invalid-tok",
			wantStatus: http.StatusUnauthorized,
			wantErr:    "unauthorized",
		},
		{
			name:       "org token succeeds with matching X-Org-ID",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer tok-alpha",
			orgHeader:  "org-alpha",
			wantStatus: http.StatusOK,
			wantOrg:    "org-alpha",
		},
		{
			name:       "org token injects X-Org-ID when omitted",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer tok-alpha",
			wantStatus: http.StatusOK,
			wantOrg:    "org-alpha",
		},
		{
			name:       "org token injects X-Org-ID when whitespace",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer tok-beta",
			orgHeader:  "   ",
			wantStatus: http.StatusOK,
			wantOrg:    "org-beta",
		},
		{
			name:       "org token returns 403 when X-Org-ID targets another org",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer tok-alpha",
			orgHeader:  "org-beta",
			wantStatus: http.StatusForbidden,
			wantErr:    "org token does not match X-Org-ID",
		},
		{
			name:       "admin apiKey accesses org-alpha",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer admin-secret",
			orgHeader:  "org-alpha",
			wantStatus: http.StatusOK,
			wantOrg:    "org-alpha",
		},
		{
			name:       "admin apiKey accesses org-beta",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			authHeader: "Bearer admin-secret",
			orgHeader:  "org-beta",
			wantStatus: http.StatusOK,
			wantOrg:    "org-beta",
		},
		{
			name:       "GET /healthz bypasses scoped auth",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotOrg string
			handler := server.ScopedBearerAuthMiddleware("admin-secret", orgKeys, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotOrg = r.Header.Get("X-Org-ID")
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			if tc.orgHeader != "" {
				req.Header.Set("X-Org-ID", tc.orgHeader)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.wantStatus, rec.Code, rec.Body.String())
			}
			if tc.wantStatus == http.StatusOK && tc.wantOrg != "" && gotOrg != tc.wantOrg {
				t.Fatalf("expected downstream X-Org-ID %q, got %q", tc.wantOrg, gotOrg)
			}
			if tc.wantErr != "" {
				var body map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("unmarshal error body: %v", err)
				}
				if body["error"] != tc.wantErr {
					t.Fatalf("expected error %q, got %q", tc.wantErr, body["error"])
				}
			}
		})
	}
}

func TestServerWithOrgAPIKeysAndRateLimit(t *testing.T) {
	keys := map[string]string{
		"tok-org1": "org-1",
		"":         "ignored-empty-tok",
		"ignored":  "",
	}
	srv := server.New(orchestrator.NewCoordinator()).
		WithAPIKey("admin-key").
		WithOrgAPIKeys(keys).
		WithRateLimit(0.01, 1)

	// Mutating caller map must not affect server's defensive copy.
	keys["tok-org1"] = "org-hijacked"

	// Request with org-1 token and no X-Org-ID should inject org-1 and succeed.
	req1 := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req1.Header.Set("Authorization", "Bearer tok-org1")
	rec1 := httptest.NewRecorder()
	srv.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 for first org-1 request, got %d (%s)", rec1.Code, rec1.Body.String())
	}

	// Second request with org-1 token should be rate-limited under org-1 bucket.
	req2 := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req2.Header.Set("Authorization", "Bearer tok-org1")
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for second org-1 request, got %d", rec2.Code)
	}

	// Cross-org X-Org-ID returns 403.
	req3 := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req3.Header.Set("Authorization", "Bearer tok-org1")
	req3.Header.Set("X-Org-ID", "org-2")
	rec3 := httptest.NewRecorder()
	srv.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for mismatched X-Org-ID, got %d", rec3.Code)
	}

	// Admin key can access org-2.
	req4 := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req4.Header.Set("Authorization", "Bearer admin-key")
	req4.Header.Set("X-Org-ID", "org-2")
	rec4 := httptest.NewRecorder()
	srv.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Fatalf("expected 200 for admin key on org-2, got %d", rec4.Code)
	}
}
