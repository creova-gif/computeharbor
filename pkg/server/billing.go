package server

import (
	"net/http"
	"strings"
)

const defaultBillingFeeFraction = 0.12

func (s *Server) handleBillingSummary(w http.ResponseWriter, r *http.Request) {
	orgID := strings.TrimSpace(r.Header.Get("X-Org-ID"))
	if orgID == "" {
		writeError(w, http.StatusForbidden, "X-Org-ID header is required")
		return
	}

	fee, err := parseFloatQuery(r.URL.Query().Get("fee"), "fee", defaultBillingFeeFraction)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	summary, err := s.coord.BillingSummary(orgID, fee)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}
