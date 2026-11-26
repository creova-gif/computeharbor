package server

import (
	"fmt"
	"net/http"

	"github.com/creova-gif/computeharbor/pkg/orchestrator"
)

type barrierRequest struct {
	WorldSize int `json:"world_size"`
	Rank      int `json:"rank"`
	Step      int `json:"step"`
}

type barrierResponse struct {
	JobID         string `json:"job_id"`
	Step          int    `json:"step"`
	Rank          int    `json:"rank"`
	WorldSize     int    `json:"world_size"`
	AllReady      bool   `json:"all_ready"`
	CommittedStep int    `json:"committed_step"`
}

func (s *Server) handleBarrier(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.authorizeJob(w, r, id); !ok {
		return
	}

	var req barrierRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	barrier, err := s.getOrCreateBarrier(id, req.WorldSize)
	if err != nil {
		if req.WorldSize < 1 {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	allReady, err := barrier.CommitRank(req.Step, req.Rank)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, barrierResponse{
		JobID:         id,
		Step:          req.Step,
		Rank:          req.Rank,
		WorldSize:     barrier.WorldSize(),
		AllReady:      allReady,
		CommittedStep: barrier.CommittedStep(),
	})
}

func (s *Server) getOrCreateBarrier(jobID string, worldSize int) (*orchestrator.RingBarrier, error) {
	s.barrierMu.Lock()
	defer s.barrierMu.Unlock()

	if existing, ok := s.barriers[jobID]; ok {
		if existing.WorldSize() != worldSize {
			return nil, fmt.Errorf("world_size mismatch: existing=%d, got=%d", existing.WorldSize(), worldSize)
		}
		return existing, nil
	}

	created, err := orchestrator.NewRingBarrier(worldSize)
	if err != nil {
		return nil, err
	}
	s.barriers[jobID] = created
	return created, nil
}

func (s *Server) deleteBarrier(jobID string) {
	s.barrierMu.Lock()
	defer s.barrierMu.Unlock()
	delete(s.barriers, jobID)
}
