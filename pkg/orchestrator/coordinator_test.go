package orchestrator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/events"
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

func TestHeartbeatAndWorkerMismatch(t *testing.T) {
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	coord := NewCoordinator().WithClock(func() time.Time { return now })

	if _, err := coord.Heartbeat("missing", "w1"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("missing job Heartbeat = %v, want ErrJobNotFound", err)
	}

	if _, err := coord.SubmitJob("job-hb", "org-1", cost.GPUL4, 10, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.Heartbeat("job-hb", "w1"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("queued job Heartbeat = %v, want ErrInvalidTransition", err)
	}

	assigned, err := coord.AssignWorker("job-hb", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if !assigned.LastHeartbeatAt.Equal(now) {
		t.Fatalf("AssignWorker LastHeartbeatAt = %v, want %v", assigned.LastHeartbeatAt, now)
	}

	if _, err := coord.Heartbeat("job-hb", "w2"); !errors.Is(err, ErrWorkerMismatch) {
		t.Fatalf("mismatched worker Heartbeat = %v, want ErrWorkerMismatch", err)
	}

	now = now.Add(15 * time.Second)
	hb, err := coord.Heartbeat("job-hb", "w1")
	if err != nil {
		t.Fatalf("valid Heartbeat failed: %v", err)
	}
	if !hb.LastHeartbeatAt.Equal(now) || !hb.UpdatedAt.Equal(now) {
		t.Fatalf("Heartbeat timestamps not updated: LastHeartbeatAt=%v UpdatedAt=%v want=%v", hb.LastHeartbeatAt, hb.UpdatedAt, now)
	}
	if hb.Status != StatusRunning {
		t.Fatalf("Heartbeat status = %s, want %s", hb.Status, StatusRunning)
	}
}

func TestReapStaleLeasesAndMaxPreemptions(t *testing.T) {
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	coord := NewCoordinator().WithClock(func() time.Time { return now })

	if _, err := coord.SubmitJob("job-fresh", "org-1", cost.GPUL4, 10, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.SubmitJob("job-stale", "org-1", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.SubmitJob("job-queued", "org-1", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}

	if _, err := coord.AssignWorker("job-fresh", "w-fresh"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-stale", "w-stale"); err != nil {
		t.Fatal(err)
	}

	// Non-positive TTL does not reap anything.
	if got := coord.ReapStaleLeases(0); len(got) != 0 {
		t.Fatalf("ReapStaleLeases(0) = %d, want 0", len(got))
	}

	// Advance 20s and refresh only job-fresh.
	now = now.Add(20 * time.Second)
	if _, err := coord.Heartbeat("job-fresh", "w-fresh"); err != nil {
		t.Fatal(err)
	}

	// Advance another 15s: job-stale is 35s old (>30s), job-fresh is 15s old (<=30s).
	now = now.Add(15 * time.Second)
	reaped := coord.ReapStaleLeases(30 * time.Second)
	if len(reaped) != 1 || reaped[0].JobID != "job-stale" {
		t.Fatalf("ReapStaleLeases = %+v, want only job-stale", reaped)
	}
	if reaped[0].Status != StatusMigrating {
		t.Fatalf("reaped status = %s, want %s", reaped[0].Status, StatusMigrating)
	}
	if reaped[0].PreemptionCount != 1 {
		t.Fatalf("reaped PreemptionCount = %d, want 1", reaped[0].PreemptionCount)
	}
	if reaped[0].WorkerNodeID != "" {
		t.Fatalf("reaped WorkerNodeID = %q, want empty", reaped[0].WorkerNodeID)
	}
	if !reaped[0].PreemptedAt.Equal(now) {
		t.Fatalf("reaped PreemptedAt = %v, want %v", reaped[0].PreemptedAt, now)
	}

	// Reassign job-stale, let it go stale again: PreemptionCount (1) >= MaxPreemptions (1) -> StatusFailed.
	now = now.Add(5 * time.Second)
	if _, err := coord.AssignWorker("job-stale", "w-stale-2"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(40 * time.Second)
	if _, err := coord.Heartbeat("job-fresh", "w-fresh"); err != nil {
		t.Fatal(err)
	}
	reaped = coord.ReapStaleLeases(30 * time.Second)
	if len(reaped) != 1 || reaped[0].JobID != "job-stale" {
		t.Fatalf("second ReapStaleLeases = %+v, want only job-stale", reaped)
	}
	if reaped[0].Status != StatusFailed {
		t.Fatalf("max-preemption reaped status = %s, want %s", reaped[0].Status, StatusFailed)
	}
	if reaped[0].PreemptionCount != 2 {
		t.Fatalf("max-preemption PreemptionCount = %d, want 2", reaped[0].PreemptionCount)
	}
	if reaped[0].WorkerNodeID != "" {
		t.Fatalf("max-preemption WorkerNodeID = %q, want empty", reaped[0].WorkerNodeID)
	}
}

func TestListJobs(t *testing.T) {
	coord := NewCoordinator()

	if got := coord.ListJobs("org-empty"); len(got) != 0 {
		t.Fatalf("ListJobs(org-empty) = %d, want 0", len(got))
	}

	if _, err := coord.SubmitJob("job-b", "org-a", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.SubmitJob("job-a", "org-a", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.SubmitJob("job-c", "org-b", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}

	if _, err := coord.AssignWorker("job-a", "w-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CommitCheckpoint("job-a", CheckpointRef{Step: 3}); err != nil {
		t.Fatal(err)
	}

	gotA := coord.ListJobs("org-a")
	if len(gotA) != 2 {
		t.Fatalf("len(ListJobs(org-a)) = %d, want 2", len(gotA))
	}
	if gotA[0].JobID != "job-a" || gotA[1].JobID != "job-b" {
		t.Fatalf("ListJobs(org-a) order = [%s, %s], want [job-a, job-b]", gotA[0].JobID, gotA[1].JobID)
	}

	// Returned leases must be clones isolated from coordinator state.
	gotA[0].LatestCheckpoint.Step = 999
	fresh, err := coord.GetJob("job-a")
	if err != nil {
		t.Fatal(err)
	}
	if fresh.LatestCheckpoint.Step != 3 {
		t.Fatalf("mutating ListJobs result mutated coordinator state: got step %d, want 3", fresh.LatestCheckpoint.Step)
	}

	gotB := coord.ListJobs("org-b")
	if len(gotB) != 1 || gotB[0].JobID != "job-c" {
		t.Fatalf("ListJobs(org-b) = %+v, want [job-c]", gotB)
	}
}

func TestCancelJob(t *testing.T) {
	now := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)
	coord := NewCoordinator().WithClock(func() time.Time { return now })

	if _, err := coord.CancelJob("missing"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("CancelJob(missing) = %v, want ErrJobNotFound", err)
	}

	// Cancel from RUNNING clears WorkerNodeID and updates UpdatedAt.
	if _, err := coord.SubmitJob("job-run", "org-1", cost.GPUL4, 10, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-run", "worker-1"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	cancelled, err := coord.CancelJob("job-run")
	if err != nil {
		t.Fatalf("CancelJob(RUNNING) failed: %v", err)
	}
	if cancelled.Status != StatusCancelled {
		t.Fatalf("status = %s, want %s", cancelled.Status, StatusCancelled)
	}
	if cancelled.WorkerNodeID != "" {
		t.Fatalf("WorkerNodeID = %q, want empty", cancelled.WorkerNodeID)
	}
	if !cancelled.UpdatedAt.Equal(now) {
		t.Fatalf("UpdatedAt = %v, want %v", cancelled.UpdatedAt, now)
	}

	// Idempotent double-cancel does not error or mutate UpdatedAt.
	later := now.Add(time.Minute)
	now = later
	again, err := coord.CancelJob("job-run")
	if err != nil {
		t.Fatalf("idempotent CancelJob failed: %v", err)
	}
	if again.Status != StatusCancelled || !again.UpdatedAt.Equal(cancelled.UpdatedAt) {
		t.Fatalf("idempotent CancelJob = %+v, want unchanged lease", again)
	}

	// COMPLETED cannot be cancelled.
	if _, err := coord.SubmitJob("job-done", "org-1", cost.GPUL4, 5, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-done", "worker-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CommitCheckpoint("job-done", CheckpointRef{Step: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CancelJob("job-done"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("CancelJob(COMPLETED) = %v, want ErrInvalidTransition", err)
	}
}

type recordingSink struct {
	mu     sync.Mutex
	events []events.Event
	onEmit func(events.Event)
}

func (s *recordingSink) Emit(evt events.Event) {
	if s.onEmit != nil {
		s.onEmit(evt)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
}

func TestCoordinatorLifecycleEventEmission(t *testing.T) {
	now := time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC)
	var coord *Coordinator
	sink := &recordingSink{
		onEmit: func(evt events.Event) {
			// Calling GetJob inside Emit verifies c.mu is unlocked before emission.
			if _, err := coord.GetJob(evt.JobID); err != nil {
				t.Errorf("GetJob(%s) inside Emit failed: %v", evt.JobID, err)
			}
		},
	}
	coord = NewCoordinator().WithClock(func() time.Time { return now }).WithEventSink(sink)

	// 1. Submit -> Assign -> SIGTERM -> Checkpoint -> Reap (preempt) -> Assign -> Complete.
	if _, err := coord.SubmitJob("job-1", "org-a", cost.GPUA100_40G, 10, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-1", "node-1"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if _, err := coord.NotifySIGTERM("job-1"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := coord.CommitCheckpoint("job-1", CheckpointRef{Step: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-1", "node-2"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Minute)
	if reaped := coord.ReapStaleLeases(time.Minute); len(reaped) != 1 {
		t.Fatalf("expected 1 reaped lease, got %d", len(reaped))
	}
	if _, err := coord.AssignWorker("job-1", "node-3"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CommitCheckpoint("job-1", CheckpointRef{Step: 10}); err != nil {
		t.Fatal(err)
	}

	// 2. Submit -> Cancel.
	if _, err := coord.SubmitJob("job-2", "org-a", cost.GPUL4, 5, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.CancelJob("job-2"); err != nil {
		t.Fatal(err)
	}

	// 3. Submit -> Assign -> SIGTERM with maxPreemptions=0 -> Failed.
	if _, err := coord.SubmitJob("job-3", "org-a", cost.GPUL4, 5, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.AssignWorker("job-3", "node-4"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.NotifySIGTERM("job-3"); !errors.Is(err, ErrMaxPreemptions) {
		t.Fatalf("expected ErrMaxPreemptions, got %v", err)
	}

	wantTypes := []events.EventType{
		events.EventJobSubmitted,
		events.EventJobAssigned,
		events.EventJobPreempted,
		events.EventJobCheckpointed,
		events.EventJobAssigned,
		events.EventJobPreempted,
		events.EventJobAssigned,
		events.EventJobCompleted,
		events.EventJobSubmitted,
		events.EventJobCancelled,
		events.EventJobSubmitted,
		events.EventJobAssigned,
		events.EventJobFailed,
	}

	if len(sink.events) != len(wantTypes) {
		t.Fatalf("emitted %d events, want %d: %+v", len(sink.events), len(wantTypes), sink.events)
	}
	for i, wantType := range wantTypes {
		if sink.events[i].Type != wantType {
			t.Errorf("event[%d].Type = %s, want %s", i, sink.events[i].Type, wantType)
		}
		if sink.events[i].ID == "" {
			t.Errorf("event[%d].ID is empty", i)
		}
	}
}
