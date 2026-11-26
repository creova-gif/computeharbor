package orchestrator

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidWorldSize is returned when a RingBarrier is created with worldSize < 1.
	ErrInvalidWorldSize = errors.New("world size must be at least 1")
	// ErrInvalidRank is returned when a rank is outside [0, worldSize).
	ErrInvalidRank = errors.New("rank out of range")
	// ErrInvalidBarrierStep is returned when a negative step is committed to a RingBarrier.
	ErrInvalidBarrierStep = errors.New("barrier step cannot be negative")
)

// RingBarrier coordinates checkpoint step commits across worldSize distributed worker ranks.
type RingBarrier struct {
	mu            sync.Mutex
	worldSize     int
	committedStep int
	pending       map[int]map[int]struct{}
}

// NewRingBarrier constructs a RingBarrier for worldSize worker ranks in [0, worldSize).
func NewRingBarrier(worldSize int) (*RingBarrier, error) {
	if worldSize < 1 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidWorldSize, worldSize)
	}
	return &RingBarrier{
		worldSize:     worldSize,
		committedStep: -1,
		pending:       make(map[int]map[int]struct{}),
	}, nil
}

// WorldSize returns the number of worker ranks participating in the barrier.
func (b *RingBarrier) WorldSize() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.worldSize
}

// CommittedStep returns the highest step committed by all ranks, or -1 if none yet.
func (b *RingBarrier) CommittedStep() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committedStep
}

// CommitRank records that rank has reached step. It returns allReady=true once all
// ranks in [0, worldSize) have committed step, advancing CommittedStep() monotonically.
func (b *RingBarrier) CommitRank(step int, rank int) (allReady bool, err error) {
	if rank < 0 || rank >= b.worldSize {
		return false, fmt.Errorf("%w: %d (world_size=%d)", ErrInvalidRank, rank, b.worldSize)
	}
	if step < 0 {
		return false, fmt.Errorf("%w: %d", ErrInvalidBarrierStep, step)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if step <= b.committedStep {
		return true, nil
	}

	ranks, ok := b.pending[step]
	if !ok {
		ranks = make(map[int]struct{}, b.worldSize)
		b.pending[step] = ranks
	}
	ranks[rank] = struct{}{}

	if len(ranks) < b.worldSize {
		return false, nil
	}

	delete(b.pending, step)
	if step > b.committedStep {
		b.committedStep = step
	}
	return true, nil
}
