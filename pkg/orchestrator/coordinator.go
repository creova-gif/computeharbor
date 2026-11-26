package orchestrator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

type JobStatus string

const (
	StatusQueued        JobStatus = "QUEUED"
	StatusRunning       JobStatus = "RUNNING"
	StatusCheckpointing JobStatus = "CHECKPOINTING"
	StatusMigrating     JobStatus = "MIGRATING"
	StatusCompleted     JobStatus = "COMPLETED"
	StatusFailed        JobStatus = "FAILED"
	StatusCancelled     JobStatus = "CANCELLED"
)

var (
	ErrJobNotFound       = errors.New("job not found")
	ErrJobExists         = errors.New("job already exists")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrMaxPreemptions    = errors.New("exceeded maximum preemption retries")
	ErrInvalidStep       = errors.New("checkpoint step cannot regress")
	ErrWorkerMismatch    = errors.New("worker node id does not match active lease")
)

type CheckpointRef struct {
	Step           int       `json:"step"`
	Epoch          int       `json:"epoch"`
	Loss           float64   `json:"loss"`
	SafetensorsURI string    `json:"safetensors_uri"`
	ManifestURI    string    `json:"manifest_uri"`
	FlushedAt      time.Time `json:"flushed_at"`
	FlushLatencyMs int64     `json:"flush_latency_ms"`
}

type JobLease struct {
	JobID             string
	OrgID             string
	GPU               cost.GPUType
	WorkerNodeID      string
	Status            JobStatus
	TargetSteps       int
	CurrentStep       int
	PreemptionCount   int
	MaxPreemptions    int
	LatestCheckpoint  *CheckpointRef
	UpdatedAt         time.Time
	LastHeartbeatAt   time.Time
	PreemptedAt       time.Time
	LastRecoveryDurMs int64
}

type CoordinatorStats struct {
	JobsByStatus     map[JobStatus]int
	TotalPreemptions int
	TotalCheckpoints int
	MaxRecoveryDurMs int64
}

type Coordinator struct {
	mu               sync.RWMutex
	jobs             map[string]*JobLease
	now              func() time.Time
	totalCheckpoints int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		jobs: make(map[string]*JobLease),
		now:  time.Now,
	}
}

func (c *Coordinator) WithClock(clock func() time.Time) *Coordinator {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = clock
	return c
}

func (c *Coordinator) SubmitJob(jobID, orgID string, gpu cost.GPUType, targetSteps, maxPreemptions int) (JobLease, error) {
	if jobID == "" || orgID == "" || targetSteps <= 0 || maxPreemptions < 0 {
		return JobLease{}, fmt.Errorf("%w: invalid job parameters", ErrInvalidTransition)
	}
	if _, err := cost.LookupQuote(gpu); err != nil {
		return JobLease{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.jobs[jobID]; exists {
		return JobLease{}, fmt.Errorf("%w: %s", ErrJobExists, jobID)
	}

	lease := &JobLease{
		JobID:          jobID,
		OrgID:          orgID,
		GPU:            gpu,
		Status:         StatusQueued,
		TargetSteps:    targetSteps,
		MaxPreemptions: maxPreemptions,
		UpdatedAt:      c.now(),
	}
	c.jobs[jobID] = lease
	return cloneLease(lease), nil
}

func (c *Coordinator) AssignWorker(jobID, workerNodeID string) (JobLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, ok := c.jobs[jobID]
	if !ok {
		return JobLease{}, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}
	if job.Status != StatusQueued && job.Status != StatusMigrating {
		return JobLease{}, fmt.Errorf("%w: cannot assign worker in state %s", ErrInvalidTransition, job.Status)
	}

	now := c.now()
	if job.Status == StatusMigrating && !job.PreemptedAt.IsZero() {
		job.LastRecoveryDurMs = now.Sub(job.PreemptedAt).Milliseconds()
	}
	job.WorkerNodeID = workerNodeID
	job.Status = StatusRunning
	job.LastHeartbeatAt = now
	job.UpdatedAt = now
	return cloneLease(job), nil
}

func (c *Coordinator) Heartbeat(jobID, workerNodeID string) (JobLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, ok := c.jobs[jobID]
	if !ok {
		return JobLease{}, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}
	if job.Status != StatusRunning {
		return JobLease{}, fmt.Errorf("%w: cannot heartbeat job in state %s", ErrInvalidTransition, job.Status)
	}
	if job.WorkerNodeID != workerNodeID {
		return JobLease{}, fmt.Errorf("%w: got %q, active %q", ErrWorkerMismatch, workerNodeID, job.WorkerNodeID)
	}

	now := c.now()
	job.LastHeartbeatAt = now
	job.UpdatedAt = now
	return cloneLease(job), nil
}

func (c *Coordinator) ReapStaleLeases(ttl time.Duration) []JobLease {
	if ttl <= 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	var reaped []JobLease
	for _, job := range c.jobs {
		if job.Status != StatusRunning {
			continue
		}
		if now.Sub(job.LastHeartbeatAt) <= ttl {
			continue
		}
		if job.PreemptionCount >= job.MaxPreemptions {
			job.Status = StatusFailed
		} else {
			job.Status = StatusMigrating
		}
		job.PreemptionCount++
		job.PreemptedAt = now
		job.UpdatedAt = now
		job.WorkerNodeID = ""
		reaped = append(reaped, cloneLease(job))
	}
	return reaped
}

func (c *Coordinator) NotifySIGTERM(jobID string) (JobLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, ok := c.jobs[jobID]
	if !ok {
		return JobLease{}, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}
	if job.Status == StatusCheckpointing || job.Status == StatusMigrating {
		return cloneLease(job), nil
	}
	if job.Status != StatusRunning {
		return JobLease{}, fmt.Errorf("%w: cannot preempt job in state %s", ErrInvalidTransition, job.Status)
	}
	if job.PreemptionCount >= job.MaxPreemptions {
		job.Status = StatusFailed
		job.UpdatedAt = c.now()
		return cloneLease(job), ErrMaxPreemptions
	}

	now := c.now()
	job.Status = StatusCheckpointing
	job.PreemptionCount++
	job.PreemptedAt = now
	job.UpdatedAt = now
	return cloneLease(job), nil
}

func (c *Coordinator) CommitCheckpoint(jobID string, ref CheckpointRef) (JobLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, ok := c.jobs[jobID]
	if !ok {
		return JobLease{}, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}
	if ref.Step < job.CurrentStep {
		return JobLease{}, fmt.Errorf("%w: got %d, current %d", ErrInvalidStep, ref.Step, job.CurrentStep)
	}
	if job.Status != StatusRunning && job.Status != StatusCheckpointing {
		return JobLease{}, fmt.Errorf("%w: cannot commit checkpoint in state %s", ErrInvalidTransition, job.Status)
	}

	cp := ref
	job.LatestCheckpoint = &cp
	job.CurrentStep = ref.Step
	job.UpdatedAt = c.now()
	c.totalCheckpoints++

	if ref.Step >= job.TargetSteps {
		job.Status = StatusCompleted
	} else if job.Status == StatusCheckpointing {
		job.Status = StatusMigrating
		job.WorkerNodeID = ""
	}
	return cloneLease(job), nil
}

// CancelJob transitions a non-terminal job to StatusCancelled, clearing its
// active worker assignment. Cancelling an already-cancelled job is idempotent.
func (c *Coordinator) CancelJob(jobID string) (JobLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, ok := c.jobs[jobID]
	if !ok {
		return JobLease{}, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}
	if job.Status == StatusCancelled {
		return cloneLease(job), nil
	}
	if job.Status == StatusCompleted || job.Status == StatusFailed {
		return JobLease{}, fmt.Errorf("%w: cannot cancel job in state %s", ErrInvalidTransition, job.Status)
	}

	job.Status = StatusCancelled
	job.WorkerNodeID = ""
	job.UpdatedAt = c.now()
	return cloneLease(job), nil
}

func (c *Coordinator) GetJob(jobID string) (JobLease, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	job, ok := c.jobs[jobID]
	if !ok {
		return JobLease{}, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}
	return cloneLease(job), nil
}

// ListJobs returns cloned leases belonging to orgID sorted by JobID.
func (c *Coordinator) ListJobs(orgID string) []JobLease {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var out []JobLease
	for _, job := range c.jobs {
		if job.OrgID == orgID {
			out = append(out, cloneLease(job))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].JobID < out[j].JobID
	})
	return out
}

// Stats returns an aggregate snapshot of coordinator job counts, preemptions,
// checkpoints, and maximum recovery duration.
func (c *Coordinator) Stats() CoordinatorStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := CoordinatorStats{
		JobsByStatus: map[JobStatus]int{
			StatusQueued:        0,
			StatusRunning:       0,
			StatusCheckpointing: 0,
			StatusMigrating:     0,
			StatusCompleted:     0,
			StatusFailed:        0,
			StatusCancelled:     0,
		},
		TotalCheckpoints: c.totalCheckpoints,
	}

	for _, job := range c.jobs {
		stats.JobsByStatus[job.Status]++
		stats.TotalPreemptions += job.PreemptionCount
		if job.LastRecoveryDurMs > stats.MaxRecoveryDurMs {
			stats.MaxRecoveryDurMs = job.LastRecoveryDurMs
		}
	}

	return stats
}

func cloneLease(src *JobLease) JobLease {
	out := *src
	if src.LatestCheckpoint != nil {
		cp := *src.LatestCheckpoint
		out.LatestCheckpoint = &cp
	}
	return out
}
