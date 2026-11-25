package cost

import (
	"errors"
	"testing"
)

func TestCalculateSpotSavingsL4AndA100(t *testing.T) {
	cases := []struct {
		name             string
		req              EstimateRequest
		wantMinGrossSave float64
		wantMinNetSave   float64
		wantMeetsTarget  bool
	}{
		{
			name: "L4 24GB 10h fine-tune with 3 preemptions and 12% fee",
			req: EstimateRequest{
				GPU:                GPUL4,
				GPUCount:           4,
				ActiveTrainingHrs:  10.0,
				ExpectedPreempts:   3,
				RecoverySeconds:    12.0,
				SavingsFeeFraction: 0.12,
			},
			wantMinGrossSave: 0.70,
			wantMinNetSave:   0.62,
			wantMeetsTarget:  true,
		},
		{
			name: "A100 40GB 24h fine-tune with 3 preemptions and 10% fee",
			req: EstimateRequest{
				GPU:                GPUA100_40G,
				GPUCount:           8,
				ActiveTrainingHrs:  24.0,
				ExpectedPreempts:   3,
				RecoverySeconds:    18.0,
				SavingsFeeFraction: 0.10,
			},
			wantMinGrossSave: 0.69,
			wantMinNetSave:   0.62,
			wantMeetsTarget:  true,
		},
		{
			name: "A100 80GB 48h training with 6 preemptions and 15% fee",
			req: EstimateRequest{
				GPU:                GPUA100_80G,
				GPUCount:           8,
				ActiveTrainingHrs:  48.0,
				ExpectedPreempts:   6,
				RecoverySeconds:    20.0,
				SavingsFeeFraction: 0.15,
			},
			wantMinGrossSave: 0.70,
			wantMinNetSave:   0.60,
			wantMeetsTarget:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Calculate(tc.req)
			if err != nil {
				t.Fatalf("Calculate(%+v) unexpected error: %v", tc.req, err)
			}
			if res.GrossSavingsFraction < tc.wantMinGrossSave {
				t.Errorf("GrossSavingsFraction = %.4f, want >= %.4f", res.GrossSavingsFraction, tc.wantMinGrossSave)
			}
			if res.NetSavingsFraction < tc.wantMinNetSave {
				t.Errorf("NetSavingsFraction = %.4f, want >= %.4f", res.NetSavingsFraction, tc.wantMinNetSave)
			}
			if res.MeetsTargetSavings != tc.wantMeetsTarget {
				t.Errorf("MeetsTargetSavings = %v, want %v", res.MeetsTargetSavings, tc.wantMeetsTarget)
			}
			if res.TotalWallHours <= tc.req.ActiveTrainingHrs {
				t.Errorf("TotalWallHours = %.4f, expected > %.4f due to preemption recovery", res.TotalWallHours, tc.req.ActiveTrainingHrs)
			}
		})
	}
}

func TestCalculateBoundaryErrors(t *testing.T) {
	valid := EstimateRequest{
		GPU:                GPUL4,
		GPUCount:           1,
		ActiveTrainingHrs:  5.0,
		ExpectedPreempts:   1,
		RecoverySeconds:    15.0,
		SavingsFeeFraction: 0.10,
	}

	t.Run("unknown gpu", func(t *testing.T) {
		req := valid
		req.GPU = "nvidia-t4-unknown"
		if _, err := Calculate(req); !errors.Is(err, ErrUnknownGPU) {
			t.Fatalf("got %v, want %v", err, ErrUnknownGPU)
		}
	})

	t.Run("zero gpu count", func(t *testing.T) {
		req := valid
		req.GPUCount = 0
		if _, err := Calculate(req); !errors.Is(err, ErrInvalidCount) {
			t.Fatalf("got %v, want %v", err, ErrInvalidCount)
		}
	})

	t.Run("non-positive hours", func(t *testing.T) {
		req := valid
		req.ActiveTrainingHrs = 0
		if _, err := Calculate(req); !errors.Is(err, ErrInvalidHours) {
			t.Fatalf("got %v, want %v", err, ErrInvalidHours)
		}
	})

	t.Run("recovery exceeds 30s SLA", func(t *testing.T) {
		req := valid
		req.RecoverySeconds = 31.0
		if _, err := Calculate(req); !errors.Is(err, ErrRecoveryWindow) {
			t.Fatalf("got %v, want %v", err, ErrRecoveryWindow)
		}
	})

	t.Run("fee out of bounds", func(t *testing.T) {
		req := valid
		req.SavingsFeeFraction = 0.65
		if _, err := Calculate(req); !errors.Is(err, ErrInvalidFee) {
			t.Fatalf("got %v, want %v", err, ErrInvalidFee)
		}
	})
}

func TestRankPoolsForWorkload(t *testing.T) {
	ranked, err := RankPoolsForWorkload(40, 4, 20.0, 0.12)
	if err != nil {
		t.Fatalf("RankPoolsForWorkload failed: %v", err)
	}
	if len(ranked) != 3 {
		t.Fatalf("len(ranked) = %d, want 3 pools with >= 40GB VRAM", len(ranked))
	}
	if ranked[0].Quote.GPU != GPUA100_40G {
		t.Errorf("cheapest >=40GB pool = %s, want %s", ranked[0].Quote.GPU, GPUA100_40G)
	}
	for i := 1; i < len(ranked); i++ {
		if ranked[i].NetCustomerCostUSD < ranked[i-1].NetCustomerCostUSD {
			t.Errorf("pools not sorted by net customer cost at index %d", i)
		}
	}
}
