package orchestrator

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRingBarrierValidation(t *testing.T) {
	if _, err := NewRingBarrier(0); !errors.Is(err, ErrInvalidWorldSize) {
		t.Fatalf("expected ErrInvalidWorldSize for 0, got %v", err)
	}
	if _, err := NewRingBarrier(-2); !errors.Is(err, ErrInvalidWorldSize) {
		t.Fatalf("expected ErrInvalidWorldSize for -2, got %v", err)
	}

	b, err := NewRingBarrier(4)
	if err != nil {
		t.Fatalf("new ring barrier: %v", err)
	}
	if b.WorldSize() != 4 {
		t.Fatalf("expected world size 4, got %d", b.WorldSize())
	}
	if b.CommittedStep() != -1 {
		t.Fatalf("expected initial committed step -1, got %d", b.CommittedStep())
	}

	if _, err := b.CommitRank(10, -1); !errors.Is(err, ErrInvalidRank) {
		t.Fatalf("expected ErrInvalidRank for -1, got %v", err)
	}
	if _, err := b.CommitRank(10, 4); !errors.Is(err, ErrInvalidRank) {
		t.Fatalf("expected ErrInvalidRank for 4, got %v", err)
	}
	if _, err := b.CommitRank(-1, 0); !errors.Is(err, ErrInvalidBarrierStep) {
		t.Fatalf("expected ErrInvalidBarrierStep for -1, got %v", err)
	}
}

func TestRingBarrierSequentialAndMonotonic(t *testing.T) {
	b, err := NewRingBarrier(3)
	if err != nil {
		t.Fatalf("new ring barrier: %v", err)
	}

	// Duplicate commit from rank 0 must not trigger allReady.
	for i := 0; i < 2; i++ {
		ready, err := b.CommitRank(100, 0)
		if err != nil || ready {
			t.Fatalf("step 100 rank 0 iter %d: ready=%v err=%v", i, ready, err)
		}
	}

	ready, err := b.CommitRank(100, 1)
	if err != nil || ready {
		t.Fatalf("step 100 rank 1: ready=%v err=%v", ready, err)
	}
	if b.CommittedStep() != -1 {
		t.Fatalf("expected committed step -1 before all ranks, got %d", b.CommittedStep())
	}

	ready, err = b.CommitRank(100, 2)
	if err != nil || !ready {
		t.Fatalf("step 100 rank 2: expected ready=true, got ready=%v err=%v", ready, err)
	}
	if b.CommittedStep() != 100 {
		t.Fatalf("expected committed step 100, got %d", b.CommittedStep())
	}

	// Committing an already-committed step returns ready=true and does not regress.
	ready, err = b.CommitRank(50, 0)
	if err != nil || !ready {
		t.Fatalf("stale step 50: expected ready=true, got ready=%v err=%v", ready, err)
	}
	if b.CommittedStep() != 100 {
		t.Fatalf("expected committed step to remain 100, got %d", b.CommittedStep())
	}
}

func TestRingBarrierConcurrentRanks(t *testing.T) {
	const worldSize = 8
	const steps = 25

	b, err := NewRingBarrier(worldSize)
	if err != nil {
		t.Fatalf("new ring barrier: %v", err)
	}

	for step := 1; step <= steps; step++ {
		var readyCount atomic.Int32
		var wg sync.WaitGroup
		wg.Add(worldSize)

		for rank := 0; rank < worldSize; rank++ {
			go func(r, s int) {
				defer wg.Done()
				ready, err := b.CommitRank(s, r)
				if err != nil {
					t.Errorf("step %d rank %d: unexpected err %v", s, r, err)
					return
				}
				if ready {
					readyCount.Add(1)
				}
			}(rank, step)
		}
		wg.Wait()

		if readyCount.Load() != 1 {
			t.Fatalf("step %d: expected exactly 1 rank to observe allReady=true, got %d", step, readyCount.Load())
		}
		if b.CommittedStep() != step {
			t.Fatalf("step %d: expected committed step %d, got %d", step, step, b.CommittedStep())
		}
	}
}
