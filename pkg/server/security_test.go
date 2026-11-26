package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

func TestSecurityHeadersMiddleware(t *testing.T) {
	cases := []struct {
		name    string
		path    string
		wantCSP bool
	}{
		{name: "ui_route_sets_csp", path: "/ui", wantCSP: true},
		{name: "api_route_no_csp", path: "/v1/jobs", wantCSP: false},
		{name: "healthz_route_no_csp", path: "/healthz", wantCSP: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			h := server.SecurityHeadersMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if !called {
				t.Fatal("next handler was not called")
			}
			if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q, want %q", got, "nosniff")
			}
			if got := rec.Header().Get("X-Frame-Options"); got != "DENY" {
				t.Errorf("X-Frame-Options = %q, want %q", got, "DENY")
			}
			if got := rec.Header().Get("Referrer-Policy"); got != "no-referrer" {
				t.Errorf("Referrer-Policy = %q, want %q", got, "no-referrer")
			}

			gotCSP := rec.Header().Get("Content-Security-Policy")
			wantCSP := ""
			if tc.wantCSP {
				wantCSP = "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'"
			}
			if gotCSP != wantCSP {
				t.Errorf("Content-Security-Policy = %q, want %q", gotCSP, wantCSP)
			}
		})
	}
}

func TestServerServeHTTPSecurityHeaders(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	for _, path := range []string{"/healthz", "/ui"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, rec.Code)
		}
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s X-Content-Type-Options = %q, want nosniff", path, got)
		}
		if got := rec.Header().Get("X-Frame-Options"); got != "DENY" {
			t.Errorf("%s X-Frame-Options = %q, want DENY", path, got)
		}
		if got := rec.Header().Get("Referrer-Policy"); got != "no-referrer" {
			t.Errorf("%s Referrer-Policy = %q, want no-referrer", path, got)
		}
		if path == "/ui" && rec.Header().Get("Content-Security-Policy") == "" {
			t.Errorf("%s expected non-empty Content-Security-Policy", path)
		}
		if path != "/ui" && rec.Header().Get("Content-Security-Policy") != "" {
			t.Errorf("%s expected empty Content-Security-Policy, got %q", path, rec.Header().Get("Content-Security-Policy"))
		}
	}
}
