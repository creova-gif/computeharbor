package orchestrator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

func TestPreemptionLifecycleThreeEvictions(t *testing.T) {
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	coord := NewCoordinator().WithClock(func() time.Time { return now })

	lease, err := coord.SubmitJob("job-lora-70b", "org-acme", cost.GPUA100_40G, 30, 5)
	if err != nil {
		t.Fatalf("SubmitJob failed: %v", err)
	}
	if lease.Status != StatusQueued {
		t.Fatalf("initial status = %s, want %s", lease.Status, StatusQueued)
	}

	if _, err := coord.AssignWorker("job-lora-70b", "spot-node-1"); err != nil {
		t.Fatalf("AssignWorker 1 failed: %v", err)
	}

	for cycle := 1; cycle <= 3; cycle++ {
		now = now.Add(5 * time.Minute)
		preempted, err := coord.NotifySIGTERM("job-lora-70b")
		if err != nil {
			t.Fatalf("cycle %d NotifySIGTERM failed: %v", cycle, err)
		}
		if preempted.Status != StatusCheckpointing {
			t.Fatalf("cycle %d status = %s, want %s", cycle, preempted.Status, StatusCheckpointing)
		}

		idempotent, err := coord.NotifySIGTERM("job-lora-70b")
		if err != nil || idempotent.PreemptionCount != cycle {
			t.Fatalf("duplicate SIGTERM not idempotent: count=%d err=%v", idempotent.PreemptionCount, err)
		}

		now = now.Add(2 * time.Second)
		step := cycle * 8
		migrating, err := coord.CommitCheckpoint("job-lora-70b", CheckpointRef{
			Step:           step,
			Epoch:          1,
			Loss:           2.5 / float64(cycle),
			SafetensorsURI: fmt.Sprintf("gs://computeharbor-ckpt/job-lora-70b/step-%d.safetensors", step),
			ManifestURI:    fmt.Sprintf("gs://computeharbor-ckpt/job-lora-70b/step-%d.json", step),
			FlushedAt:      now,
			FlushLatencyMs: 1850,
		})
		if err != nil {
			t.Fatalf("cycle %d CommitCheckpoint failed: %v", cycle, err)
		}
		if migrating.Status != StatusMigrating {
			t.Fatalf("cycle %d status after flush = %s, want %s", cycle, migrating.Status, StatusMigrating)
		}

		now = now.Add(9 * time.Second)
		resumed, err := coord.AssignWorker("job-lora-70b", fmt.Sprintf("spot-node-%d", cycle+1))
		if err != nil {
			t.Fatalf("cycle %d AssignWorker resume failed: %v", cycle, err)
		}
		if resumed.Status != StatusRunning {
			t.Fatalf("cycle %d resumed status = %s, want %s", cycle, resumed.Status, StatusRunning)
		}
		if resumed.LastRecoveryDurMs != 11000 {
			t.Errorf("cycle %d recovery duration = %d ms, want 11000 ms (<30000 ms SLA)", cycle, resumed.LastRecoveryDurMs)
		}
	}

	done, err := coord.CommitCheckpoint("job-lora-70b", CheckpointRef{
		Step:           30,
		Epoch:          1,
		Loss:           0.412,
		SafetensorsURI: "gs://computeharbor-ckpt/job-lora-70b/step-30.safetensors",
		ManifestURI:    "gs://computeharbor-ckpt/job-lora-70b/step-30.json",
		FlushedAt:      now.Add(time.Minute),
		FlushLatencyMs: 1500,
	})
	if err != nil {
		t.Fatalf("final CommitCheckpoint failed: %v", err)
	}
	if done.Status != StatusCompleted {
		t.Fatalf("final status = %s, want %s", done.Status, StatusCompleted)
	}
	if done.PreemptionCount != 3 {
		t.Fatalf("PreemptionCount = %d, want 3", done.PreemptionCount)
	}
}

func TestMaxPreemptionsAndStepRegression(t *testing.T) {
	coord := NewCoordinator()
	if _, err := coord.SubmitJob("job-limit", "org-1", cost.GPUL4, 20, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-limit", "node-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CommitCheckpoint("job-limit", CheckpointRef{Step: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CommitCheckpoint("job-limit", CheckpointRef{Step: 5}); !errors.Is(err, ErrInvalidStep) {
		t.Fatalf("expected ErrInvalidStep on regression, got %v", err)
	}
	if _, err := coord.NotifySIGTERM("job-limit"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CommitCheckpoint("job-limit", CheckpointRef{Step: 11}); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-limit", "node-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.NotifySIGTERM("job-limit"); !errors.Is(err, ErrMaxPreemptions) {
		t.Fatalf("expected ErrMaxPreemptions on second preemption, got %v", err)
	}
}

func TestConcurrentCoordinatorAccess(t *testing.T) {
	coord := NewCoordinator()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			jobID := fmt.Sprintf("job-%d", id)
			if _, err := coord.SubmitJob(jobID, "org-concurrent", cost.GPUL4, 10, 3); err != nil {
				t.Errorf("SubmitJob %s: %v", jobID, err)
				return
			}
			if _, err := coord.AssignWorker(jobID, "worker-1"); err != nil {
				t.Errorf("AssignWorker %s: %v", jobID, err)
				return
			}
			if _, err := coord.CommitCheckpoint(jobID, CheckpointRef{Step: 10}); err != nil {
				t.Errorf("CommitCheckpoint %s: %v", jobID, err)
				return
			}
		}(i)
	}
	wg.Wait()
}
