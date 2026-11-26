package server

import (
	"net/http"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

func (s *Server) handleRankFleet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	minVRAM, err := parseIntQuery(q.Get("min_vram"), "min_vram", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	minAvailability, err := parseFloatQuery(q.Get("min_availability"), "min_availability", 0.0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	provider := q.Get("provider")

	placements, err := cost.RankFleet(minVRAM, provider, minAvailability)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, placements)
}
