package server

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/creova-gif/computeharbor/pkg/orchestrator"
)

const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	stats := s.coord.Stats()

	statuses := make([]string, 0, len(stats.JobsByStatus))
	for status := range stats.JobsByStatus {
		statuses = append(statuses, string(status))
	}
	sort.Strings(statuses)

	var b strings.Builder
	b.WriteString("# HELP computeharbor_jobs_total Current number of jobs by status.\n")
	b.WriteString("# TYPE computeharbor_jobs_total gauge\n")
	for _, status := range statuses {
		count := stats.JobsByStatus[orchestrator.JobStatus(status)]
		fmt.Fprintf(&b, "computeharbor_jobs_total{status=%q} %d\n", status, count)
	}

	b.WriteString("# HELP computeharbor_preemptions_total Total number of job preemptions.\n")
	b.WriteString("# TYPE computeharbor_preemptions_total counter\n")
	fmt.Fprintf(&b, "computeharbor_preemptions_total %d\n", stats.TotalPreemptions)

	b.WriteString("# HELP computeharbor_checkpoints_total Total number of committed checkpoints.\n")
	b.WriteString("# TYPE computeharbor_checkpoints_total counter\n")
	fmt.Fprintf(&b, "computeharbor_checkpoints_total %d\n", stats.TotalCheckpoints)

	b.WriteString("# HELP computeharbor_max_recovery_duration_ms Maximum observed job recovery duration in milliseconds.\n")
	b.WriteString("# TYPE computeharbor_max_recovery_duration_ms gauge\n")
	fmt.Fprintf(&b, "computeharbor_max_recovery_duration_ms %d\n", stats.MaxRecoveryDurMs)

	w.Header().Set("Content-Type", prometheusContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}
