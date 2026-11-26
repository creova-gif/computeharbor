package server_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

type scheduleResponseDTO struct {
	Selected     cost.EstimateResult   `json:"selected"`
	Alternatives []cost.EstimateResult `json:"alternatives"`
}

func TestScheduleNormalSelection(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())
	rec := doJSON(t, srv, http.MethodPost, "/v1/schedule", "", map[string]any{
		"min_vram_gb":  40,
		"gpu_count":    2,
		"active_hours": 10.0,
		"fee_fraction": 0.12,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/schedule status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var resp scheduleResponseDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode schedule response: %v", err)
	}
	if resp.Selected.Quote.GPU != cost.GPUA100_40G {
		t.Errorf("selected GPU = %q, want %q", resp.Selected.Quote.GPU, cost.GPUA100_40G)
	}
	if len(resp.Alternatives) != 2 {
		t.Fatalf("alternatives len = %d, want 2", len(resp.Alternatives))
	}
	if resp.Alternatives[0].Quote.GPU != cost.GPUA100_80G {
		t.Errorf("alternatives[0] GPU = %q, want %q", resp.Alternatives[0].Quote.GPU, cost.GPUA100_80G)
	}
	if resp.Alternatives[1].Quote.GPU != cost.GPUH100_80G {
		t.Errorf("alternatives[1] GPU = %q, want %q", resp.Alternatives[1].Quote.GPU, cost.GPUH100_80G)
	}
}

func TestScheduleExcludeGPUsFailover(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())
	rec := doJSON(t, srv, http.MethodPost, "/v1/schedule", "", map[string]any{
		"min_vram_gb":  40,
		"gpu_count":    2,
		"active_hours": 10.0,
		"fee_fraction": 0.12,
		"exclude_gpus": []string{string(cost.GPUA100_40G)},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/schedule status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var resp scheduleResponseDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode schedule response: %v", err)
	}
	if resp.Selected.Quote.GPU != cost.GPUA100_80G {
		t.Errorf("selected GPU = %q, want %q", resp.Selected.Quote.GPU, cost.GPUA100_80G)
	}
	if len(resp.Alternatives) != 1 || resp.Alternatives[0].Quote.GPU != cost.GPUH100_80G {
		t.Errorf("alternatives = %+v, want [nvidia-h100-80gb]", resp.Alternatives)
	}
}

func TestScheduleAllExcludedReturns422(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())
	rec := doJSON(t, srv, http.MethodPost, "/v1/schedule", "", map[string]any{
		"min_vram_gb":  40,
		"gpu_count":    1,
		"active_hours": 5.0,
		"fee_fraction": 0.10,
		"exclude_gpus": []string{
			string(cost.GPUA100_40G),
			string(cost.GPUA100_80G),
			string(cost.GPUH100_80G),
		},
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("POST /v1/schedule status = %d, want 422: %s", rec.Code, rec.Body.String())
	}
}

func TestScheduleInvalidJSONReturns400(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())
	rec := doJSON(t, srv, http.MethodPost, "/v1/schedule", "", "{bad json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /v1/schedule status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}

func TestScheduleInvalidInputsReturn422(t *testing.T) {
	srv := server.New(orchestrator.NewCoordinator())
	cases := []struct {
		name string
		body map[string]any
	}{
		{
			name: "negative min_vram_gb",
			body: map[string]any{"min_vram_gb": -1, "gpu_count": 1, "active_hours": 1.0},
		},
		{
			name: "zero gpu_count",
			body: map[string]any{"min_vram_gb": 0, "gpu_count": 0, "active_hours": 1.0},
		},
		{
			name: "non-positive active_hours",
			body: map[string]any{"min_vram_gb": 0, "gpu_count": 1, "active_hours": 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSON(t, srv, http.MethodPost, "/v1/schedule", "", tc.body)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("POST /v1/schedule (%s) status = %d, want 422: %s", tc.name, rec.Code, rec.Body.String())
			}
		})
	}
}
