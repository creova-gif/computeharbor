package orchestrator

import (
	"errors"
	"math"
	"strings"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

const (
	defaultSecondsPerStep  = 10.0
	defaultRecoverySeconds = 12.0
)

// ErrInvalidOrgID is returned when an empty organization identifier is provided.
var ErrInvalidOrgID = errors.New("org_id is required")

// BillingSummary aggregates spot vs on-demand cost estimates and savings for an organization.
type BillingSummary struct {
	OrgID                string  `json:"org_id"`
	TotalJobs            int     `json:"total_jobs"`
	CompletedJobs        int     `json:"completed_jobs"`
	TotalSteps           int     `json:"total_steps"`
	TotalPreemptions     int     `json:"total_preemptions"`
	EstimatedOnDemandUSD float64 `json:"estimated_on_demand_usd"`
	EstimatedSpotUSD     float64 `json:"estimated_spot_usd"`
	PlatformFeeUSD       float64 `json:"platform_fee_usd"`
	NetSavingsUSD        float64 `json:"net_savings_usd"`
	NetSavingsFraction   float64 `json:"net_savings_fraction"`
}

// BillingSummary aggregates estimated on-demand vs spot costs, platform fees,
// and net savings across all jobs belonging to orgID.
func (c *Coordinator) BillingSummary(orgID string, feeFraction float64) (BillingSummary, error) {
	orgID = strings.TrimSpace(orgID)
	if orgID == "" {
		return BillingSummary{}, ErrInvalidOrgID
	}
	if math.IsNaN(feeFraction) || math.IsInf(feeFraction, 0) || feeFraction < 0 || feeFraction > 0.5 {
		return BillingSummary{}, cost.ErrInvalidFee
	}

	jobs := c.ListJobs(orgID)
	summary := BillingSummary{
		OrgID:     orgID,
		TotalJobs: len(jobs),
	}

	var totalOnDemand, totalSpot, totalFee, totalNetSavings float64
	for _, job := range jobs {
		if job.Status == StatusCompleted {
			summary.CompletedJobs++
		}
		summary.TotalSteps += job.CurrentStep
		summary.TotalPreemptions += job.PreemptionCount

		if job.CurrentStep <= 0 {
			continue
		}

		activeHrs := float64(job.CurrentStep) * (defaultSecondsPerStep / 3600.0)
		est, err := cost.Calculate(cost.EstimateRequest{
			GPU:                job.GPU,
			GPUCount:           1,
			ActiveTrainingHrs:  activeHrs,
			ExpectedPreempts:   job.PreemptionCount,
			RecoverySeconds:    defaultRecoverySeconds,
			SavingsFeeFraction: feeFraction,
		})
		if err != nil {
			return BillingSummary{}, err
		}

		totalOnDemand += est.OnDemandCostUSD
		totalSpot += est.RawSpotCostUSD
		totalFee += est.PlatformFeeUSD
		totalNetSavings += est.OnDemandCostUSD - est.NetCustomerCostUSD
	}

	summary.EstimatedOnDemandUSD = round4(totalOnDemand)
	summary.EstimatedSpotUSD = round4(totalSpot)
	summary.PlatformFeeUSD = round4(totalFee)
	summary.NetSavingsUSD = round4(totalNetSavings)
	if totalOnDemand > 0 {
		summary.NetSavingsFraction = round4(totalNetSavings / totalOnDemand)
	}

	return summary, nil
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
