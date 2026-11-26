package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const snapshotVersion = 1

// CoordinatorSnapshot represents a durable point-in-time JSON snapshot of a Coordinator.
type CoordinatorSnapshot struct {
	Version          int        `json:"version"`
	SavedAt          time.Time  `json:"saved_at"`
	TotalCheckpoints int        `json:"total_checkpoints"`
	Jobs             []JobLease `json:"jobs"`
}

// SaveSnapshot atomically persists a deterministic JSON snapshot of the coordinator
// state to path via a temporary file, fsync, and rename.
func (c *Coordinator) SaveSnapshot(path string) error {
	if path == "" {
		return errors.New("snapshot path cannot be empty")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	jobs := make([]JobLease, 0, len(c.jobs))
	for _, job := range c.jobs {
		jobs = append(jobs, cloneLease(job))
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].JobID < jobs[j].JobID
	})

	snap := CoordinatorSnapshot{
		Version:          snapshotVersion,
		SavedAt:          c.now().UTC(),
		TotalCheckpoints: c.totalCheckpoints,
		Jobs:             jobs,
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal coordinator snapshot: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}

	tmpFile, err := os.CreateTemp(dir, ".coordinator-snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp snapshot file: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("write temp snapshot file: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("sync temp snapshot file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp snapshot file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename snapshot file: %w", err)
	}
	cleanup = false
	return nil
}

// LoadCoordinatorFromFile restores a Coordinator from a JSON snapshot file.
// If the file does not exist, it returns a fresh Coordinator with no error.
func LoadCoordinatorFromFile(path string) (*Coordinator, error) {
	if path == "" {
		return nil, errors.New("snapshot path cannot be empty")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewCoordinator(), nil
		}
		return nil, fmt.Errorf("read coordinator snapshot: %w", err)
	}

	var snap CoordinatorSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("decode coordinator snapshot: %w", err)
	}
	if snap.Version != snapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version: %d", snap.Version)
	}

	c := NewCoordinator()
	c.totalCheckpoints = snap.TotalCheckpoints
	for i := range snap.Jobs {
		if snap.Jobs[i].JobID == "" {
			return nil, errors.New("coordinator snapshot contains job with empty job_id")
		}
		cp := cloneLease(&snap.Jobs[i])
		c.jobs[cp.JobID] = &cp
	}
	return c, nil
}
