package orchestrator

import (
	"fmt"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

var benchLease JobLease
var benchReaped []JobLease

func BenchmarkCoordinatorSubmitAndHeartbeat(b *testing.B) {
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	coord := NewCoordinator().WithClock(func() time.Time { return now })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jobID := fmt.Sprintf("bench-job-%d", i)
		if _, err := coord.SubmitJob(jobID, "org-bench", cost.GPUA100_40G, 100, 3); err != nil {
			b.Fatalf("SubmitJob failed: %v", err)
		}
		if _, err := coord.AssignWorker(jobID, "worker-1"); err != nil {
			b.Fatalf("AssignWorker failed: %v", err)
		}
		lease, err := coord.Heartbeat(jobID, "worker-1")
		if err != nil {
			b.Fatalf("Heartbeat failed: %v", err)
		}
		benchLease = lease
	}
}

func BenchmarkCoordinatorReapStaleLeases(b *testing.B) {
	const totalJobs = 200
	const staleJobs = 100

	baseTime := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	now := baseTime
	coord := NewCoordinator().WithClock(func() time.Time { return now })

	for i := 0; i < totalJobs; i++ {
		jobID := fmt.Sprintf("reap-job-%d", i)
		if _, err := coord.SubmitJob(jobID, "org-bench", cost.GPUL4, 100, 1_000_000_000); err != nil {
			b.Fatalf("SubmitJob failed: %v", err)
		}
		if _, err := coord.AssignWorker(jobID, fmt.Sprintf("worker-%d", i)); err != nil {
			b.Fatalf("AssignWorker failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		now = now.Add(time.Minute)
		for j := staleJobs; j < totalJobs; j++ {
			if _, err := coord.Heartbeat(fmt.Sprintf("reap-job-%d", j), fmt.Sprintf("worker-%d", j)); err != nil {
				b.Fatalf("Heartbeat failed: %v", err)
			}
		}
		b.StartTimer()

		reaped := coord.ReapStaleLeases(30 * time.Second)
		benchReaped = reaped

		b.StopTimer()
		for _, lease := range reaped {
			if _, err := coord.AssignWorker(lease.JobID, lease.WorkerNodeID); err != nil {
				b.Fatalf("AssignWorker restore failed: %v", err)
			}
		}
		b.StartTimer()
	}
}
