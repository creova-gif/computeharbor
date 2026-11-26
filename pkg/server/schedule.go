package server

import (
	"math"
	"net/http"
	"strings"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

type scheduleRequest struct {
	MinVRAMGB   int      `json:"min_vram_gb"`
	GPUCount    int      `json:"gpu_count"`
	ActiveHours float64  `json:"active_hours"`
	FeeFraction float64  `json:"fee_fraction"`
	ExcludeGPUs []string `json:"exclude_gpus"`
}

type scheduleResponse struct {
	Selected     cost.EstimateResult   `json:"selected"`
	Alternatives []cost.EstimateResult `json:"alternatives"`
}

func (s *Server) handleSchedule(w http.ResponseWriter, r *http.Request) {
	var req scheduleRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.MinVRAMGB < 0 {
		writeError(w, http.StatusUnprocessableEntity, "min_vram_gb cannot be negative")
		return
	}
	if req.GPUCount < 1 {
		writeError(w, http.StatusUnprocessableEntity, "gpu_count must be at least 1")
		return
	}
	if req.ActiveHours <= 0 || math.IsNaN(req.ActiveHours) || math.IsInf(req.ActiveHours, 0) {
		writeError(w, http.StatusUnprocessableEntity, "active_hours must be positive")
		return
	}

	ranked, err := cost.RankPoolsForWorkload(req.MinVRAMGB, req.GPUCount, req.ActiveHours, req.FeeFraction)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	excluded := make(map[string]struct{}, len(req.ExcludeGPUs))
	for _, gpu := range req.ExcludeGPUs {
		if trimmed := strings.TrimSpace(gpu); trimmed != "" {
			excluded[trimmed] = struct{}{}
		}
	}

	eligible := make([]cost.EstimateResult, 0, len(ranked))
	for _, res := range ranked {
		if _, skip := excluded[string(res.Quote.GPU)]; skip {
			continue
		}
		eligible = append(eligible, res)
	}

	if len(eligible) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "no eligible spot pools remain")
		return
	}

	alternatives := make([]cost.EstimateResult, 0, len(eligible)-1)
	alternatives = append(alternatives, eligible[1:]...)

	writeJSON(w, http.StatusOK, scheduleResponse{
		Selected:     eligible[0],
		Alternatives: alternatives,
	})
}
