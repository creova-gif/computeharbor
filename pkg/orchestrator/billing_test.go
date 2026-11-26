package orchestrator

import (
	"errors"
	"math"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

func TestBillingSummaryOrgScopedAggregation(t *testing.T) {
	coord := NewCoordinator()

	if _, err := coord.SubmitJob("job-a1", "org-alpha", cost.GPUH100_80G, 1000, 3); err != nil {
		t.Fatalf("submit job-a1: %v", err)
	}
	if _, err := coord.AssignWorker("job-a1", "node-1"); err != nil {
		t.Fatalf("assign job-a1: %v", err)
	}
	if _, err := coord.NotifySIGTERM("job-a1"); err != nil {
		t.Fatalf("sigterm job-a1: %v", err)
	}
	if _, err := coord.CommitCheckpoint("job-a1", CheckpointRef{Step: 360}); err != nil {
		t.Fatalf("checkpoint job-a1: %v", err)
	}
	if _, err := coord.AssignWorker("job-a1", "node-2"); err != nil {
		t.Fatalf("reassign job-a1: %v", err)
	}
	if _, err := coord.CommitCheckpoint("job-a1", CheckpointRef{Step: 1000}); err != nil {
		t.Fatalf("complete job-a1: %v", err)
	}

	if _, err := coord.SubmitJob("job-a2", "org-alpha", cost.GPUL4, 500, 2); err != nil {
		t.Fatalf("submit job-a2: %v", err)
	}
	if _, err := coord.AssignWorker("job-a2", "node-3"); err != nil {
		t.Fatalf("assign job-a2: %v", err)
	}
	if _, err := coord.CommitCheckpoint("job-a2", CheckpointRef{Step: 180}); err != nil {
		t.Fatalf("checkpoint job-a2: %v", err)
	}

	// Queued job with 0 steps should still be counted in TotalJobs without error.
	if _, err := coord.SubmitJob("job-a3", "org-alpha", cost.GPUA10G, 200, 1); err != nil {
		t.Fatalf("submit job-a3: %v", err)
	}

	// Different org job must not leak into org-alpha's summary.
	if _, err := coord.SubmitJob("job-b1", "org-beta", cost.GPUH100_80G, 500, 2); err != nil {
		t.Fatalf("submit job-b1: %v", err)
	}
	if _, err := coord.AssignWorker("job-b1", "node-4"); err != nil {
		t.Fatalf("assign job-b1: %v", err)
	}
	if _, err := coord.CommitCheckpoint("job-b1", CheckpointRef{Step: 500}); err != nil {
		t.Fatalf("complete job-b1: %v", err)
	}

	summary, err := coord.BillingSummary("org-alpha", 0.12)
	if err != nil {
		t.Fatalf("billing summary: %v", err)
	}

	if summary.OrgID != "org-alpha" {
		t.Fatalf("expected org_id org-alpha, got %q", summary.OrgID)
	}
	if summary.TotalJobs != 3 {
		t.Fatalf("expected 3 total jobs, got %d", summary.TotalJobs)
	}
	if summary.CompletedJobs != 1 {
		t.Fatalf("expected 1 completed job, got %d", summary.CompletedJobs)
	}
	if summary.TotalSteps != 1180 {
		t.Fatalf("expected 1180 total steps, got %d", summary.TotalSteps)
	}
	if summary.TotalPreemptions != 1 {
		t.Fatalf("expected 1 preemption, got %d", summary.TotalPreemptions)
	}
	if summary.EstimatedOnDemandUSD <= summary.EstimatedSpotUSD {
		t.Fatalf("expected on-demand (%v) > spot (%v)", summary.EstimatedOnDemandUSD, summary.EstimatedSpotUSD)
	}
	if summary.PlatformFeeUSD <= 0 {
		t.Fatalf("expected positive platform fee, got %v", summary.PlatformFeeUSD)
	}
	if summary.NetSavingsUSD <= 0 {
		t.Fatalf("expected positive net savings, got %v", summary.NetSavingsUSD)
	}
	if summary.NetSavingsFraction <= 0.5 || summary.NetSavingsFraction >= 1.0 {
		t.Fatalf("unexpected net savings fraction: %v", summary.NetSavingsFraction)
	}
}

func TestBillingSummaryEmptyOrgAndValidation(t *testing.T) {
	coord := NewCoordinator()

	empty, err := coord.BillingSummary("org-empty", 0.12)
	if err != nil {
		t.Fatalf("expected empty org to succeed, got %v", err)
	}
	if empty.TotalJobs != 0 || empty.EstimatedOnDemandUSD != 0 || empty.NetSavingsFraction != 0 {
		t.Fatalf("expected zeroed summary for empty org, got %+v", empty)
	}

	if _, err := coord.BillingSummary("   ", 0.12); !errors.Is(err, ErrInvalidOrgID) {
		t.Fatalf("expected ErrInvalidOrgID, got %v", err)
	}

	for _, badFee := range []float64{-0.01, 0.51, math.NaN(), math.Inf(1)} {
		if _, err := coord.BillingSummary("org-alpha", badFee); !errors.Is(err, cost.ErrInvalidFee) {
			t.Fatalf("fee=%v: expected ErrInvalidFee, got %v", badFee, err)
		}
	}
}
