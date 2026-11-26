package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/orchestrator"
)

func TestUIEndpoint(t *testing.T) {
	srv := New(orchestrator.NewCoordinator())

	req := httptest.NewRequest(http.MethodGet, "/ui", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Fatalf("expected text/html Content-Type, got %q", contentType)
	}

	if cacheControl := rec.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("expected Cache-Control=no-store, got %q", cacheControl)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"ComputeHarbor Status",
		"/healthz",
		"/metrics",
		"/v1/fleet/rank",
		"/v1/jobs",
		"/v1/billing/summary",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected UI HTML to contain %q", want)
		}
	}
}
