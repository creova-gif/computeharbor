package server

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/creova-gif/computeharbor/pkg/orchestrator"
)

const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

type httpMetricKey struct {
	method string
	status int
}

type httpMetricEntry struct {
	method string
	status int
	count  int64
}

type httpMetrics struct {
	mu         sync.Mutex
	requests   map[httpMetricKey]int64
	durationNs int64
	totalCount int64
}

func newHTTPMetrics() *httpMetrics {
	return &httpMetrics{
		requests: make(map[httpMetricKey]int64),
	}
}

func (m *httpMetrics) record(method string, status int, dur time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.requests[httpMetricKey{method: method, status: status}]++
	m.durationNs += dur.Nanoseconds()
	m.totalCount++
	m.mu.Unlock()
}

func (m *httpMetrics) snapshot() ([]httpMetricEntry, int64, int64) {
	if m == nil {
		return nil, 0, 0
	}
	m.mu.Lock()
	entries := make([]httpMetricEntry, 0, len(m.requests))
	for k, count := range m.requests {
		entries = append(entries, httpMetricEntry{
			method: k.method,
			status: k.status,
			count:  count,
		})
	}
	durNs := m.durationNs
	total := m.totalCount
	m.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].method != entries[j].method {
			return entries[i].method < entries[j].method
		}
		return entries[i].status < entries[j].status
	})
	return entries, durNs, total
}

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

	if s.webhookStats != nil {
		b.WriteString("# HELP computeharbor_webhook_delivered_total Total number of delivered webhook events.\n")
		b.WriteString("# TYPE computeharbor_webhook_delivered_total counter\n")
		fmt.Fprintf(&b, "computeharbor_webhook_delivered_total %d\n", s.webhookStats.DeliveredCount())

		b.WriteString("# HELP computeharbor_webhook_failed_total Total number of failed webhook deliveries.\n")
		b.WriteString("# TYPE computeharbor_webhook_failed_total counter\n")
		fmt.Fprintf(&b, "computeharbor_webhook_failed_total %d\n", s.webhookStats.FailedCount())

		b.WriteString("# HELP computeharbor_webhook_dropped_total Total number of dropped webhook events.\n")
		b.WriteString("# TYPE computeharbor_webhook_dropped_total counter\n")
		fmt.Fprintf(&b, "computeharbor_webhook_dropped_total %d\n", s.webhookStats.DroppedCount())
	}

	drainingVal := 0
	if s.IsDraining() {
		drainingVal = 1
	}
	b.WriteString("# HELP computeharbor_draining Whether the server is currently draining (1) or ready (0).\n")
	b.WriteString("# TYPE computeharbor_draining gauge\n")
	fmt.Fprintf(&b, "computeharbor_draining %d\n", drainingVal)

	entries, durNs, totalCount := s.httpMetrics.snapshot()
	b.WriteString("# HELP computeharbor_http_requests_total Total number of HTTP requests handled by method and status code.\n")
	b.WriteString("# TYPE computeharbor_http_requests_total counter\n")
	for _, entry := range entries {
		fmt.Fprintf(&b, "computeharbor_http_requests_total{method=%q,status=%q} %d\n", entry.method, strconv.Itoa(entry.status), entry.count)
	}

	b.WriteString("# HELP computeharbor_http_request_duration_seconds_sum Total time spent handling HTTP requests in seconds.\n")
	b.WriteString("# TYPE computeharbor_http_request_duration_seconds_sum counter\n")
	fmt.Fprintf(&b, "computeharbor_http_request_duration_seconds_sum %s\n", strconv.FormatFloat(float64(durNs)/1e9, 'f', -1, 64))

	b.WriteString("# HELP computeharbor_http_request_duration_seconds_count Total number of HTTP requests measured for duration.\n")
	b.WriteString("# TYPE computeharbor_http_request_duration_seconds_count counter\n")
	fmt.Fprintf(&b, "computeharbor_http_request_duration_seconds_count %d\n", totalCount)

	w.Header().Set("Content-Type", prometheusContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}
