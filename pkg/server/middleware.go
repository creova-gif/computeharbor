package server

import (
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const defaultOrgID = "anonymous"

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

// LoggingMiddleware logs each HTTP request as a structured slog record with
// method, path, status, duration_ms, and org_id.
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

		logger.Info("http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			slog.String("org_id", orgIDFromRequest(r)),
		)
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
		b = &tokenBucket{
			tokens: float64(l.burst),
			last:   now,
		}
		l.buckets[orgID] = b
	} else if l.rps > 0 {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens = min(float64(l.burst), b.tokens+elapsed*l.rps)
			b.last = now
		}
	}

	if b.tokens < 1.0 {
		return false
	}
	b.tokens -= 1.0
	return true
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
