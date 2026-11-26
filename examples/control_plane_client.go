// Command control_plane_client demonstrates interacting with the ComputeHarbor
// control plane over net/http: ranking regional fleet placements, submitting a
// training job, and polling its lease state.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type submitJobRequest struct {
	JobID          string `json:"job_id"`
	OrgID          string `json:"org_id"`
	GPU            string `json:"gpu"`
	TargetSteps    int    `json:"target_steps"`
	MaxPreemptions int    `json:"max_preemptions"`
}

type regionalPool struct {
	GPU               string  `json:"gpu"`
	Provider          string  `json:"provider"`
	Region            string  `json:"region"`
	VRAMGB            int     `json:"vram_gb"`
	SpotHourlyUSD     float64 `json:"spot_hourly_usd"`
	AvailabilityScore float64 `json:"availability_score"`
}

type fleetPlacement struct {
	Pool               regionalPool `json:"pool"`
	EffectiveHourlyUSD float64      `json:"effective_hourly_usd"`
	NetSavingsFraction float64      `json:"net_savings_fraction"`
	CompositeScore     float64      `json:"composite_score"`
}

type jobLease struct {
	JobID           string `json:"job_id"`
	OrgID           string `json:"org_id"`
	GPU             string `json:"gpu"`
	Status          string `json:"status"`
	TargetSteps     int    `json:"target_steps"`
	CurrentStep     int    `json:"current_step"`
	PreemptionCount int    `json:"preemption_count"`
}

type controlPlaneClient struct {
	baseURL    string
	orgID      string
	httpClient *http.Client
}

func (c *controlPlaneClient) rankFleet(ctx context.Context, minVRAM int) ([]fleetPlacement, error) {
	url := fmt.Sprintf("%s/v1/fleet/rank?min_vram=%d", c.baseURL, minVRAM)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	var placements []fleetPlacement
	if err := c.doJSON(req, &placements); err != nil {
		return nil, err
	}
	return placements, nil
}

func (c *controlPlaneClient) submitJob(ctx context.Context, payload submitJobRequest) (jobLease, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return jobLease{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/jobs", bytes.NewReader(body))
	if err != nil {
		return jobLease{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	var lease jobLease
	if err := c.doJSON(req, &lease); err != nil {
		return jobLease{}, err
	}
	return lease, nil
}

func (c *controlPlaneClient) getJob(ctx context.Context, jobID string) (jobLease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/jobs/"+jobID, nil)
	if err != nil {
		return jobLease{}, err
	}

	var lease jobLease
	if err := c.doJSON(req, &lease); err != nil {
		return jobLease{}, err
	}
	return lease, nil
}

func (c *controlPlaneClient) doJSON(req *http.Request, out any) error {
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Org-ID", c.orgID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(raw))
	}
	return json.Unmarshal(raw, out)
}

func main() {
	baseURL := os.Getenv("COMPUTEHARBOR_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	orgID := os.Getenv("COMPUTEHARBOR_ORG_ID")
	if orgID == "" {
		orgID = "org-quickstart"
	}

	client := &controlPlaneClient{
		baseURL:    baseURL,
		orgID:      orgID,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	placements, err := client.rankFleet(ctx, 24)
	if err != nil {
		log.Fatalf("rank fleet: %v", err)
	}
	if len(placements) == 0 {
		log.Fatal("no eligible fleet placements returned")
	}
	best := placements[0]
	fmt.Printf("Top placement: %s (%s/%s) score=%.4f\n",
		best.Pool.GPU, best.Pool.Provider, best.Pool.Region, best.CompositeScore)

	jobID := fmt.Sprintf("job-go-%d", time.Now().Unix())
	submitted, err := client.submitJob(ctx, submitJobRequest{
		JobID:          jobID,
		OrgID:          orgID,
		GPU:            best.Pool.GPU,
		TargetSteps:    100,
		MaxPreemptions: 3,
	})
	if err != nil {
		log.Fatalf("submit job: %v", err)
	}
	fmt.Printf("Submitted job=%s status=%s\n", submitted.JobID, submitted.Status)

	fetched, err := client.getJob(ctx, submitted.JobID)
	if err != nil {
		log.Fatalf("get job: %v", err)
	}
	fmt.Printf("Fetched job=%s status=%s step=%d/%d\n",
		fetched.JobID, fetched.Status, fetched.CurrentStep, fetched.TargetSteps)
}
