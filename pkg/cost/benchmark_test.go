package cost

import "testing"

var (
	benchEstimateResult  EstimateResult
	benchFleetPlacements []FleetPlacement
)

func BenchmarkCalculate(b *testing.B) {
	req := EstimateRequest{
		GPU:                GPUA100_80G,
		GPUCount:           8,
		ActiveTrainingHrs:  24.0,
		ExpectedPreempts:   3,
		RecoverySeconds:    12.0,
		SavingsFeeFraction: 0.15,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := Calculate(req)
		if err != nil {
			b.Fatalf("Calculate failed: %v", err)
		}
		benchEstimateResult = res
	}
}

func BenchmarkRankFleet(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		placements, err := RankFleet(24, "", 0.80)
		if err != nil {
			b.Fatalf("RankFleet failed: %v", err)
		}
		benchFleetPlacements = placements
	}
}
