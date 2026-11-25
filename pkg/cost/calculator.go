package cost

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

type GPUType string

const (
	GPUL4       GPUType = "nvidia-l4-24gb"
	GPUA10G     GPUType = "nvidia-a10g-24gb"
	GPUA100_40G GPUType = "nvidia-a100-40gb"
	GPUA100_80G GPUType = "nvidia-a100-80gb"
	GPUH100_80G GPUType = "nvidia-h100-80gb"
)

var (
	ErrUnknownGPU         = errors.New("unknown gpu type")
	ErrInvalidHours       = errors.New("active training hours must be positive")
	ErrInvalidCount       = errors.New("gpu count must be at least 1")
	ErrInvalidFee         = errors.New("savings fee rate must be between 0 and 0.5")
	ErrRecoveryWindow     = errors.New("recovery seconds exceeds 30s spot eviction limit")
	ErrNegativePreemption = errors.New("preemptions cannot be negative")
)

type PoolQuote struct {
	GPU                 GPUType
	Provider            string
	Region              string
	VRAMGB              int
	OnDemandHourlyUSD   float64
	SpotHourlyUSD       float64
	MeanPreemptionRateH float64
}

type EstimateRequest struct {
	GPU                GPUType
	GPUCount           int
	ActiveTrainingHrs  float64
	ExpectedPreempts   int
	RecoverySeconds    float64
	SavingsFeeFraction float64
}

type EstimateResult struct {
	Quote                PoolQuote
	TotalWallHours       float64
	RecoveryOverheadHrs  float64
	OnDemandCostUSD      float64
	RawSpotCostUSD       float64
	GrossSavingsUSD      float64
	PlatformFeeUSD       float64
	NetCustomerCostUSD   float64
	GrossSavingsFraction float64
	NetSavingsFraction   float64
	MeetsTargetSavings   bool
}

var defaultCatalog = map[GPUType]PoolQuote{
	GPUL4: {
		GPU:                 GPUL4,
		Provider:            "gcp",
		Region:              "us-central1",
		VRAMGB:              24,
		OnDemandHourlyUSD:   0.85,
		SpotHourlyUSD:       0.24,
		MeanPreemptionRateH: 0.08,
	},
	GPUA10G: {
		GPU:                 GPUA10G,
		Provider:            "aws",
		Region:              "us-east-1",
		VRAMGB:              24,
		OnDemandHourlyUSD:   1.21,
		SpotHourlyUSD:       0.36,
		MeanPreemptionRateH: 0.09,
	},
	GPUA100_40G: {
		GPU:                 GPUA100_40G,
		Provider:            "gcp",
		Region:              "us-central1",
		VRAMGB:              40,
		OnDemandHourlyUSD:   3.67,
		SpotHourlyUSD:       1.10,
		MeanPreemptionRateH: 0.12,
	},
	GPUA100_80G: {
		GPU:                 GPUA100_80G,
		Provider:            "aws",
		Region:              "us-west-2",
		VRAMGB:              80,
		OnDemandHourlyUSD:   4.90,
		SpotHourlyUSD:       1.42,
		MeanPreemptionRateH: 0.14,
	},
	GPUH100_80G: {
		GPU:                 GPUH100_80G,
		Provider:            "gcp",
		Region:              "us-east4",
		VRAMGB:              80,
		OnDemandHourlyUSD:   8.60,
		SpotHourlyUSD:       2.58,
		MeanPreemptionRateH: 0.15,
	},
}

func Catalog() []PoolQuote {
	quotes := make([]PoolQuote, 0, len(defaultCatalog))
	for _, q := range defaultCatalog {
		quotes = append(quotes, q)
	}
	sort.Slice(quotes, func(i, j int) bool {
		return quotes[i].OnDemandHourlyUSD < quotes[j].OnDemandHourlyUSD
	})
	return quotes
}

func LookupQuote(gpu GPUType) (PoolQuote, error) {
	q, ok := defaultCatalog[gpu]
	if !ok {
		return PoolQuote{}, fmt.Errorf("%w: %s", ErrUnknownGPU, gpu)
	}
	return q, nil
}

func Calculate(req EstimateRequest) (EstimateResult, error) {
	quote, err := LookupQuote(req.GPU)
	if err != nil {
		return EstimateResult{}, err
	}
	if req.GPUCount < 1 {
		return EstimateResult{}, ErrInvalidCount
	}
	if req.ActiveTrainingHrs <= 0 || math.IsNaN(req.ActiveTrainingHrs) || math.IsInf(req.ActiveTrainingHrs, 0) {
		return EstimateResult{}, ErrInvalidHours
	}
	if req.ExpectedPreempts < 0 {
		return EstimateResult{}, ErrNegativePreemption
	}
	if req.RecoverySeconds < 0 || req.RecoverySeconds > 30.0 {
		return EstimateResult{}, ErrRecoveryWindow
	}
	if req.SavingsFeeFraction < 0 || req.SavingsFeeFraction > 0.5 {
		return EstimateResult{}, ErrInvalidFee
	}

	recoveryHrs := (float64(req.ExpectedPreempts) * req.RecoverySeconds) / 3600.0
	totalWallHrs := req.ActiveTrainingHrs + recoveryHrs
	count := float64(req.GPUCount)

	onDemandCost := req.ActiveTrainingHrs * count * quote.OnDemandHourlyUSD
	rawSpotCost := totalWallHrs * count * quote.SpotHourlyUSD
	grossSavings := math.Max(0, onDemandCost-rawSpotCost)
	platformFee := grossSavings * req.SavingsFeeFraction
	netCustomerCost := rawSpotCost + platformFee

	grossSavingsFraction := 0.0
	netSavingsFraction := 0.0
	if onDemandCost > 0 {
		grossSavingsFraction = grossSavings / onDemandCost
		netSavingsFraction = (onDemandCost - netCustomerCost) / onDemandCost
	}

	return EstimateResult{
		Quote:                quote,
		TotalWallHours:       round4(totalWallHrs),
		RecoveryOverheadHrs:  round4(recoveryHrs),
		OnDemandCostUSD:      round4(onDemandCost),
		RawSpotCostUSD:       round4(rawSpotCost),
		GrossSavingsUSD:      round4(grossSavings),
		PlatformFeeUSD:       round4(platformFee),
		NetCustomerCostUSD:   round4(netCustomerCost),
		GrossSavingsFraction: round4(grossSavingsFraction),
		NetSavingsFraction:   round4(netSavingsFraction),
		MeetsTargetSavings:   netSavingsFraction >= 0.60,
	}, nil
}

func RankPoolsForWorkload(minVRAMGB int, gpuCount int, activeHours float64, feeFraction float64) ([]EstimateResult, error) {
	results := make([]EstimateResult, 0, len(defaultCatalog))
	for _, quote := range Catalog() {
		if quote.VRAMGB < minVRAMGB {
			continue
		}
		expectedPreempts := int(math.Ceil(activeHours * quote.MeanPreemptionRateH))
		res, err := Calculate(EstimateRequest{
			GPU:                quote.GPU,
			GPUCount:           gpuCount,
			ActiveTrainingHrs:  activeHours,
			ExpectedPreempts:   expectedPreempts,
			RecoverySeconds:    12.0,
			SavingsFeeFraction: feeFraction,
		})
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].NetCustomerCostUSD < results[j].NetCustomerCostUSD
	})
	return results, nil
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
