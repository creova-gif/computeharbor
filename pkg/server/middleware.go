package server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type contextKey string

const (
	defaultOrgID                  = "anonymous"
	maxBuckets                    = 10000
	bucketIdleTTL                 = 10 * time.Minute
	maxRequestIDLength            = 128
	requestIDHeader               = "X-Request-ID"
	requestIDKey       contextKey = "request_id"
)

// statusRecorder wraps http.ResponseWriter to capture the HTTP status code
// written by downstream handlers.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func orgIDFromRequest(r *http.Request) string {
	orgID := strings.TrimSpace(r.Header.Get("X-Org-ID"))
	if orgID == "" {
		return defaultOrgID
	}
	return orgID
}

// RequestIDFromContext returns the correlation request ID stored in ctx by
// RequestIDMiddleware, or an empty string if none is present.
func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

func isValidRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x20 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

func generateRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// RequestIDMiddleware ensures every HTTP request has a valid X-Request-ID
// correlation identifier. Valid incoming X-Request-ID headers (printable ASCII,
// <= 128 bytes) are preserved; otherwise a fresh 16-byte hex ID is generated.
// The ID is set on the response header and attached to the request context.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := strings.TrimSpace(r.Header.Get(requestIDHeader))
		if !isValidRequestID(reqID) {
			reqID = generateRequestID()
		}
		w.Header().Set(requestIDHeader, reqID)
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// LoggingMiddleware logs each HTTP request as a structured slog record with
// method, path, status, duration_ms, org_id, and request_id (when present).
func LoggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		next.ServeHTTP(rec, r)

		attrs := []any{
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			slog.String("org_id", orgIDFromRequest(r)),
		}
		if reqID := RequestIDFromContext(r.Context()); reqID != "" {
			attrs = append(attrs, slog.String("request_id", reqID))
		}
		logger.Info("http request", attrs...)
	})
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// OrgRateLimiter enforces a thread-safe per-organization token bucket rate limit
// keyed by the X-Org-ID request header.
type OrgRateLimiter struct {
	mu      sync.Mutex
	rps     float64
	burst   int
	buckets map[string]*tokenBucket
	now     func() time.Time
}

// NewOrgRateLimiter creates an OrgRateLimiter allowing rps tokens per second
// with a maximum burst capacity per organization.
func NewOrgRateLimiter(rps float64, burst int) *OrgRateLimiter {
	return &OrgRateLimiter{
		rps:     rps,
		burst:   burst,
		buckets: make(map[string]*tokenBucket),
		now:     time.Now,
	}
}

// Allow reports whether a single request for orgID is permitted under its token
// bucket. Empty orgID values are normalized to "anonymous".
func (l *OrgRateLimiter) Allow(orgID string) bool {
	orgID = strings.TrimSpace(orgID)
	if orgID == "" {
		orgID = defaultOrgID
	}
	if l.burst <= 0 {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[orgID]
	if !ok {
		if len(l.buckets) >= maxBuckets {
			l.evict(now)
		}
		b = &tokenBucket{
			tokens: float64(l.burst),
			last:   now,
		}
		l.buckets[orgID] = b
	} else {
		if l.rps > 0 {
			elapsed := now.Sub(b.last).Seconds()
			if elapsed > 0 {
				b.tokens = min(float64(l.burst), b.tokens+elapsed*l.rps)
			}
		}
		b.last = now
	}

	if b.tokens < 1.0 {
		return false
	}
	b.tokens -= 1.0
	return true
}

func (l *OrgRateLimiter) evict(now time.Time) {
	for id, b := range l.buckets {
		if now.Sub(b.last) >= bucketIdleTTL {
			delete(l.buckets, id)
		}
	}
	if len(l.buckets) < maxBuckets {
		return
	}

	var oldestID string
	var oldestTime time.Time
	first := true
	for id, b := range l.buckets {
		if first || b.last.Before(oldestTime) {
			oldestID = id
			oldestTime = b.last
			first = false
		}
	}
	if !first {
		delete(l.buckets, oldestID)
	}
}

type orgKeyEntry struct {
	token []byte
	orgID string
}

// ScopedBearerAuthMiddleware enforces constant-time Bearer token authentication
// with support for a global adminKey and org-scoped tokens (token -> org_id).
// When both adminKey and orgKeys are empty, it is a no-op. GET /healthz,
// GET /readyz, and GET /ui bypass auth for probes and console access.
func ScopedBearerAuthMiddleware(adminKey string, orgKeys map[string]string, next http.Handler) http.Handler {
	if adminKey == "" && len(orgKeys) == 0 {
		return next
	}
	adminBytes := []byte(adminKey)
	entries := make([]orgKeyEntry, 0, len(orgKeys))
	for tok, org := range orgKeys {
		if tok == "" || org == "" {
			continue
		}
		entries = append(entries, orgKeyEntry{
			token: []byte(tok),
			orgID: org,
		})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && (r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/ui") {
			next.ServeHTTP(w, r)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		tokenBytes := []byte(token)
		if adminKey != "" && subtle.ConstantTimeCompare(tokenBytes, adminBytes) == 1 {
			next.ServeHTTP(w, r)
			return
		}
		for _, entry := range entries {
			if subtle.ConstantTimeCompare(tokenBytes, entry.token) == 1 {
				reqOrg := strings.TrimSpace(r.Header.Get("X-Org-ID"))
				if reqOrg != "" && reqOrg != entry.orgID {
					writeError(w, http.StatusForbidden, "org token does not match X-Org-ID")
					return
				}
				cloned := r.Clone(r.Context())
				cloned.Header.Set("X-Org-ID", entry.orgID)
				next.ServeHTTP(w, cloned)
				return
			}
		}
		writeError(w, http.StatusUnauthorized, "unauthorized")
	})
}

// BearerAuthMiddleware enforces constant-time Bearer token authentication when
// apiKey is non-empty, skipping GET /healthz, GET /readyz, and GET /ui for
// probes and console access.
func BearerAuthMiddleware(apiKey string, next http.Handler) http.Handler {
	return ScopedBearerAuthMiddleware(apiKey, nil, next)
}

// Middleware wraps next and rejects requests with 429 Too Many Requests and
// Retry-After: 1 when the caller's organization bucket is exhausted.
func (l *OrgRateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.Allow(orgIDFromRequest(r)) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}
