package server

// HasBarrier reports whether a barrier entry exists for jobID.
func (s *Server) HasBarrier(jobID string) bool {
	s.barrierMu.Lock()
	defer s.barrierMu.Unlock()
	_, ok := s.barriers[jobID]
	return ok
}
