package cost

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

var (
	ErrInvalidVRAM         = errors.New("min_vram cannot be negative")
	ErrInvalidAvailability = errors.New("min_availability must be between 0 and 1")
	ErrUnknownProvider     = errors.New("unknown provider")
)

const fleetRecoverySeconds = 12.0

// RegionalPool represents a spot GPU capacity pool in a specific cloud provider and region.
type RegionalPool struct {
	GPU               GPUType `json:"gpu"`
	Provider          string  `json:"provider"`
	Region            string  `json:"region"`
	VRAMGB            int     `json:"vram_gb"`
	OnDemandHourlyUSD float64 `json:"on_demand_hourly_usd"`
	SpotHourlyUSD     float64 `json:"spot_hourly_usd"`
	PreemptionRateH   float64 `json:"preemption_rate_h"`
	AvailabilityScore float64 `json:"availability_score"`
}

// FleetPlacement is a scored and ranked regional spot pool candidate.
type FleetPlacement struct {
	Pool               RegionalPool `json:"pool"`
	EffectiveHourlyUSD float64      `json:"effective_hourly_usd"`
	NetSavingsFraction float64      `json:"net_savings_fraction"`
	CompositeScore     float64      `json:"composite_score"`
}

var regionalFleetCatalog = []RegionalPool{
	// GCP us-central1
	{GPU: GPUL4, Provider: "gcp", Region: "us-central1", VRAMGB: 24, OnDemandHourlyUSD: 0.85, SpotHourlyUSD: 0.24, PreemptionRateH: 0.08, AvailabilityScore: 0.91},
	{GPU: GPUA10G, Provider: "gcp", Region: "us-central1", VRAMGB: 24, OnDemandHourlyUSD: 1.24, SpotHourlyUSD: 0.38, PreemptionRateH: 0.09, AvailabilityScore: 0.86},
	{GPU: GPUA100_40G, Provider: "gcp", Region: "us-central1", VRAMGB: 40, OnDemandHourlyUSD: 3.67, SpotHourlyUSD: 1.10, PreemptionRateH: 0.12, AvailabilityScore: 0.84},
	{GPU: GPUH100_80G, Provider: "gcp", Region: "us-central1", VRAMGB: 80, OnDemandHourlyUSD: 8.60, SpotHourlyUSD: 2.58, PreemptionRateH: 0.15, AvailabilityScore: 0.78},

	// GCP europe-west4
	{GPU: GPUL4, Provider: "gcp", Region: "europe-west4", VRAMGB: 24, OnDemandHourlyUSD: 0.90, SpotHourlyUSD: 0.26, PreemptionRateH: 0.07, AvailabilityScore: 0.89},
	{GPU: GPUA10G, Provider: "gcp", Region: "europe-west4", VRAMGB: 24, OnDemandHourlyUSD: 1.29, SpotHourlyUSD: 0.40, PreemptionRateH: 0.08, AvailabilityScore: 0.85},
	{GPU: GPUA100_40G, Provider: "gcp", Region: "europe-west4", VRAMGB: 40, OnDemandHourlyUSD: 3.85, SpotHourlyUSD: 1.18, PreemptionRateH: 0.11, AvailabilityScore: 0.82},
	{GPU: GPUH100_80G, Provider: "gcp", Region: "europe-west4", VRAMGB: 80, OnDemandHourlyUSD: 8.95, SpotHourlyUSD: 2.75, PreemptionRateH: 0.14, AvailabilityScore: 0.74},

	// AWS us-east-1
	{GPU: GPUL4, Provider: "aws", Region: "us-east-1", VRAMGB: 24, OnDemandHourlyUSD: 0.88, SpotHourlyUSD: 0.27, PreemptionRateH: 0.09, AvailabilityScore: 0.88},
	{GPU: GPUA10G, Provider: "aws", Region: "us-east-1", VRAMGB: 24, OnDemandHourlyUSD: 1.21, SpotHourlyUSD: 0.36, PreemptionRateH: 0.09, AvailabilityScore: 0.90},
	{GPU: GPUA100_80G, Provider: "aws", Region: "us-east-1", VRAMGB: 80, OnDemandHourlyUSD: 4.90, SpotHourlyUSD: 1.48, PreemptionRateH: 0.13, AvailabilityScore: 0.81},
	{GPU: GPUH100_80G, Provider: "aws", Region: "us-east-1", VRAMGB: 80, OnDemandHourlyUSD: 8.80, SpotHourlyUSD: 2.68, PreemptionRateH: 0.16, AvailabilityScore: 0.75},

	// AWS us-west-2
	{GPU: GPUL4, Provider: "aws", Region: "us-west-2", VRAMGB: 24, OnDemandHourlyUSD: 0.88, SpotHourlyUSD: 0.25, PreemptionRateH: 0.08, AvailabilityScore: 0.92},
	{GPU: GPUA10G, Provider: "aws", Region: "us-west-2", VRAMGB: 24, OnDemandHourlyUSD: 1.21, SpotHourlyUSD: 0.35, PreemptionRateH: 0.08, AvailabilityScore: 0.88},
	{GPU: GPUA100_80G, Provider: "aws", Region: "us-west-2", VRAMGB: 80, OnDemandHourlyUSD: 4.90, SpotHourlyUSD: 1.42, PreemptionRateH: 0.14, AvailabilityScore: 0.83},
	{GPU: GPUH100_80G, Provider: "aws", Region: "us-west-2", VRAMGB: 80, OnDemandHourlyUSD: 8.80, SpotHourlyUSD: 2.62, PreemptionRateH: 0.15, AvailabilityScore: 0.79},

	// OCI us-phoenix-1
	{GPU: GPUL4, Provider: "oci", Region: "us-phoenix-1", VRAMGB: 24, OnDemandHourlyUSD: 0.82, SpotHourlyUSD: 0.21, PreemptionRateH: 0.06, AvailabilityScore: 0.94},
	{GPU: GPUA10G, Provider: "oci", Region: "us-phoenix-1", VRAMGB: 24, OnDemandHourlyUSD: 1.18, SpotHourlyUSD: 0.32, PreemptionRateH: 0.07, AvailabilityScore: 0.91},
	{GPU: GPUA100_80G, Provider: "oci", Region: "us-phoenix-1", VRAMGB: 80, OnDemandHourlyUSD: 4.65, SpotHourlyUSD: 1.30, PreemptionRateH: 0.10, AvailabilityScore: 0.87},
	{GPU: GPUH100_80G, Provider: "oci", Region: "us-phoenix-1", VRAMGB: 80, OnDemandHourlyUSD: 8.20, SpotHourlyUSD: 2.35, PreemptionRateH: 0.12, AvailabilityScore: 0.82},

	// OCI uk-london-1
	{GPU: GPUL4, Provider: "oci", Region: "uk-london-1", VRAMGB: 24, OnDemandHourlyUSD: 0.86, SpotHourlyUSD: 0.23, PreemptionRateH: 0.07, AvailabilityScore: 0.90},
	{GPU: GPUA10G, Provider: "oci", Region: "uk-london-1", VRAMGB: 24, OnDemandHourlyUSD: 1.22, SpotHourlyUSD: 0.34, PreemptionRateH: 0.08, AvailabilityScore: 0.87},
	{GPU: GPUA100_80G, Provider: "oci", Region: "uk-london-1", VRAMGB: 80, OnDemandHourlyUSD: 4.80, SpotHourlyUSD: 1.38, PreemptionRateH: 0.11, AvailabilityScore: 0.84},
	{GPU: GPUH100_80G, Provider: "oci", Region: "uk-london-1", VRAMGB: 80, OnDemandHourlyUSD: 8.50, SpotHourlyUSD: 2.48, PreemptionRateH: 0.13, AvailabilityScore: 0.77},
}

// RankFleet filters regional spot pools by minimum VRAM, optional provider, and minimum
// availability score, returning placements sorted by highest CompositeScore first.
func RankFleet(minVRAMGB int, providerFilter string, minAvailability float64) ([]FleetPlacement, error) {
	if minVRAMGB < 0 {
		return nil, ErrInvalidVRAM
	}
	if math.IsNaN(minAvailability) || math.IsInf(minAvailability, 0) || minAvailability < 0 || minAvailability > 1 {
		return nil, ErrInvalidAvailability
	}

	provider := strings.ToLower(strings.TrimSpace(providerFilter))
	if provider != "" && provider != "gcp" && provider != "aws" && provider != "oci" {
		return nil, fmt.Errorf("%w: %s", ErrUnknownProvider, providerFilter)
	}

	placements := make([]FleetPlacement, 0, len(regionalFleetCatalog))
	for _, pool := range regionalFleetCatalog {
		if pool.VRAMGB < minVRAMGB {
			continue
		}
		if provider != "" && pool.Provider != provider {
			continue
		}
		if pool.AvailabilityScore < minAvailability {
			continue
		}

		// Account for checkpoint/restart overhead per expected preemption.
		recoveryMultiplier := 1.0 + (pool.PreemptionRateH*fleetRecoverySeconds)/3600.0
		effectiveHourly := pool.SpotHourlyUSD * recoveryMultiplier

		netSavingsFraction := 0.0
		if pool.OnDemandHourlyUSD > 0 {
			netSavingsFraction = math.Max(0, (pool.OnDemandHourlyUSD-effectiveHourly)/pool.OnDemandHourlyUSD)
		}

		// Weight cost savings (60%) and capacity availability (40%) into [0, 1].
		composite := 0.6*netSavingsFraction + 0.4*pool.AvailabilityScore

		placements = append(placements, FleetPlacement{
			Pool:               pool,
			EffectiveHourlyUSD: round4(effectiveHourly),
			NetSavingsFraction: round4(netSavingsFraction),
			CompositeScore:     round4(composite),
		})
	}

	sort.Slice(placements, func(i, j int) bool {
		a, b := placements[i], placements[j]
		if a.CompositeScore != b.CompositeScore {
			return a.CompositeScore > b.CompositeScore
		}
		if a.EffectiveHourlyUSD != b.EffectiveHourlyUSD {
			return a.EffectiveHourlyUSD < b.EffectiveHourlyUSD
		}
		if a.Pool.Provider != b.Pool.Provider {
			return a.Pool.Provider < b.Pool.Provider
		}
		if a.Pool.Region != b.Pool.Region {
			return a.Pool.Region < b.Pool.Region
		}
		return a.Pool.GPU < b.Pool.GPU
	})

	return placements, nil
}
