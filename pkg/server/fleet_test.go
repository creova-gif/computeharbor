package server_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

func TestFleetRankDefaultAndFilters(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	rec := doJSON(t, srv, http.MethodGet, "/v1/fleet/rank", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/fleet/rank status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var all []cost.FleetPlacement
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("decode fleet placements: %v", err)
	}
	if len(all) == 0 {
		t.Fatal("expected non-empty fleet placements")
	}

	// Filter by provider, min_vram, and min_availability.
	rec = doJSON(t, srv, http.MethodGet, "/v1/fleet/rank?provider=oci&min_vram=80&min_availability=0.80", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/fleet/rank filtered status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var filtered []cost.FleetPlacement
	if err := json.Unmarshal(rec.Body.Bytes(), &filtered); err != nil {
		t.Fatalf("decode filtered fleet placements: %v", err)
	}
	if len(filtered) == 0 {
		t.Fatal("expected non-empty filtered fleet placements")
	}
	for _, p := range filtered {
		if p.Pool.Provider != "oci" {
			t.Errorf("got provider %q, want oci", p.Pool.Provider)
		}
		if p.Pool.VRAMGB < 80 {
			t.Errorf("got vram %d, want >= 80", p.Pool.VRAMGB)
		}
		if p.Pool.AvailabilityScore < 0.80 {
			t.Errorf("got availability %v, want >= 0.80", p.Pool.AvailabilityScore)
		}
	}
}

func TestFleetRankErrors(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())

	cases := []struct {
		name     string
		path     string
		wantCode int
	}{
		{name: "invalid min_vram", path: "/v1/fleet/rank?min_vram=abc", wantCode: http.StatusBadRequest},
		{name: "invalid min_availability", path: "/v1/fleet/rank?min_availability=xyz", wantCode: http.StatusBadRequest},
		{name: "negative min_vram", path: "/v1/fleet/rank?min_vram=-5", wantCode: http.StatusUnprocessableEntity},
		{name: "availability out of range", path: "/v1/fleet/rank?min_availability=1.5", wantCode: http.StatusUnprocessableEntity},
		{name: "unknown provider", path: "/v1/fleet/rank?provider=azure", wantCode: http.StatusUnprocessableEntity},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSON(t, srv, http.MethodGet, tc.path, "", nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("GET %s status = %d, want %d: %s", tc.path, rec.Code, tc.wantCode, rec.Body.String())
			}
		})
	}
}
