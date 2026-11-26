package orchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

func TestSnapshotRoundTrip(t *testing.T) {
	now := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	c := NewCoordinator().WithClock(func() time.Time {
		now = now.Add(5 * time.Second)
		return now
	})

	if _, err := c.SubmitJob("job-b", "org-1", cost.GPUL4, 100, 3); err != nil {
		t.Fatalf("SubmitJob job-b: %v", err)
	}
	if _, err := c.SubmitJob("job-a", "org-1", cost.GPUA100_40G, 200, 2); err != nil {
		t.Fatalf("SubmitJob job-a: %v", err)
	}
	if _, err := c.AssignWorker("job-a", "worker-1"); err != nil {
		t.Fatalf("AssignWorker job-a: %v", err)
	}
	if _, err := c.NotifySIGTERM("job-a"); err != nil {
		t.Fatalf("NotifySIGTERM job-a: %v", err)
	}
	cpRef := CheckpointRef{
		Step:           42,
		Epoch:          1,
		Loss:           0.123,
		SafetensorsURI: "s3://bucket/job-a/step-42.safetensors",
		ManifestURI:    "s3://bucket/job-a/step-42.json",
		FlushedAt:      now,
		FlushLatencyMs: 850,
	}
	if _, err := c.CommitCheckpoint("job-a", cpRef); err != nil {
		t.Fatalf("CommitCheckpoint job-a: %v", err)
	}

	snapPath := filepath.Join(t.TempDir(), "nested", "coordinator-state.json")
	if err := c.SaveSnapshot(snapPath); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Verify deterministic sorted order in persisted JSON.
	raw, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("ReadFile snapshot: %v", err)
	}
	var parsed CoordinatorSnapshot
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("Unmarshal snapshot: %v", err)
	}
	if parsed.Version != snapshotVersion {
		t.Errorf("snapshot Version = %d, want %d", parsed.Version, snapshotVersion)
	}
	if parsed.TotalCheckpoints != 1 {
		t.Errorf("snapshot TotalCheckpoints = %d, want 1", parsed.TotalCheckpoints)
	}
	if len(parsed.Jobs) != 2 || parsed.Jobs[0].JobID != "job-a" || parsed.Jobs[1].JobID != "job-b" {
		t.Fatalf("expected jobs sorted [job-a, job-b], got %+v", parsed.Jobs)
	}

	restored, err := LoadCoordinatorFromFile(snapPath)
	if err != nil {
		t.Fatalf("LoadCoordinatorFromFile: %v", err)
	}

	jobA, err := restored.GetJob("job-a")
	if err != nil {
		t.Fatalf("restored.GetJob(job-a): %v", err)
	}
	if jobA.Status != StatusMigrating {
		t.Errorf("job-a Status = %s, want %s", jobA.Status, StatusMigrating)
	}
	if jobA.CurrentStep != 42 {
		t.Errorf("job-a CurrentStep = %d, want 42", jobA.CurrentStep)
	}
	if jobA.PreemptionCount != 1 {
		t.Errorf("job-a PreemptionCount = %d, want 1", jobA.PreemptionCount)
	}
	if jobA.LatestCheckpoint == nil || jobA.LatestCheckpoint.SafetensorsURI != cpRef.SafetensorsURI {
		t.Errorf("job-a LatestCheckpoint = %+v, want %+v", jobA.LatestCheckpoint, cpRef)
	}

	jobB, err := restored.GetJob("job-b")
	if err != nil {
		t.Fatalf("restored.GetJob(job-b): %v", err)
	}
	if jobB.Status != StatusQueued {
		t.Errorf("job-b Status = %s, want %s", jobB.Status, StatusQueued)
	}

	stats := restored.Stats()
	if stats.TotalCheckpoints != 1 {
		t.Errorf("restored Stats.TotalCheckpoints = %d, want 1", stats.TotalCheckpoints)
	}
	if stats.TotalPreemptions != 1 {
		t.Errorf("restored Stats.TotalPreemptions = %d, want 1", stats.TotalPreemptions)
	}
}

func TestLoadCoordinatorFromFileMissing(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "does-not-exist.json")
	c, err := LoadCoordinatorFromFile(missingPath)
	if err != nil {
		t.Fatalf("expected nil error for missing snapshot, got %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil Coordinator for missing snapshot")
	}
	if got := len(c.ListJobs("org-1")); got != 0 {
		t.Errorf("expected 0 jobs, got %d", got)
	}
}

func TestLoadCoordinatorFromFileCorrupted(t *testing.T) {
	corruptPath := filepath.Join(t.TempDir(), "corrupt.json")
	if err := os.WriteFile(corruptPath, []byte("{not valid json"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := LoadCoordinatorFromFile(corruptPath); err == nil {
		t.Fatal("expected error for corrupted snapshot JSON, got nil")
	}
}

func TestLoadCoordinatorFromFileUnsupportedVersion(t *testing.T) {
	badVerPath := filepath.Join(t.TempDir(), "bad-version.json")
	if err := os.WriteFile(badVerPath, []byte(`{"version": 99, "jobs": []}`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := LoadCoordinatorFromFile(badVerPath); err == nil {
		t.Fatal("expected error for unsupported snapshot version, got nil")
	}
}

func TestSaveSnapshotCreatesNestedParentDir(t *testing.T) {
	c := NewCoordinator()
	if _, err := c.SubmitJob("job-nested", "org-1", cost.GPUL4, 50, 1); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	nestedDir := filepath.Join(t.TempDir(), "a", "b", "c")
	snapPath := filepath.Join(nestedDir, "coordinator-state.json")
	if err := c.SaveSnapshot(snapPath); err != nil {
		t.Fatalf("SaveSnapshot nested: %v", err)
	}

	entries, err := os.ReadDir(nestedDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "coordinator-state.json" {
		t.Fatalf("unexpected directory entries after SaveSnapshot: %+v", entries)
	}

	restored, err := LoadCoordinatorFromFile(snapPath)
	if err != nil {
		t.Fatalf("LoadCoordinatorFromFile: %v", err)
	}
	if _, err := restored.GetJob("job-nested"); err != nil {
		t.Fatalf("restored.GetJob(job-nested): %v", err)
	}
}

func TestSaveSnapshotFilePermissions(t *testing.T) {
	c := NewCoordinator()
	if _, err := c.SubmitJob("job-perm", "org-secret", cost.GPUL4, 100, 1); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	snapPath := filepath.Join(t.TempDir(), "coordinator-state.json")
	if err := c.SaveSnapshot(snapPath); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	info, err := os.Stat(snapPath)
	if err != nil {
		t.Fatalf("Stat snapshot: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("snapshot file mode = %04o, want 0600", got)
	}
}
