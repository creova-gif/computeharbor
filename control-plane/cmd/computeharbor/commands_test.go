package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

func TestRunStatus(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	if _, err := coord.SubmitJob("job-status-1", "org-alpha", cost.GPUL4, 100, 2); err != nil {
		t.Fatalf("submit job 1: %v", err)
	}
	if _, err := coord.SubmitJob("job-status-2", "org-beta", cost.GPUL4, 50, 1); err != nil {
		t.Fatalf("submit job 2: %v", err)
	}

	ts := httptest.NewServer(server.New(coord))
	defer ts.Close()

	t.Run("missing org returns error", func(t *testing.T) {
		var buf bytes.Buffer
		err := runStatus([]string{"-url", ts.URL}, &buf)
		if err == nil {
			t.Fatal("expected error when -org is missing")
		}
	})

	t.Run("single job", func(t *testing.T) {
		var buf bytes.Buffer
		if err := run([]string{"status", "-url", ts.URL, "-org", "org-alpha", "-job", "job-status-1"}, &buf); err != nil {
			t.Fatalf("run status single job failed: %v", err)
		}
		var job struct {
			JobID string `json:"job_id"`
			OrgID string `json:"org_id"`
			State string `json:"state"`
		}
		if err := json.Unmarshal(buf.Bytes(), &job); err != nil {
			t.Fatalf("invalid json output: %v\nbody: %s", err, buf.String())
		}
		if job.JobID != "job-status-1" || job.OrgID != "org-alpha" {
			t.Fatalf("unexpected job payload: %+v", job)
		}
	})

	t.Run("job list", func(t *testing.T) {
		var buf bytes.Buffer
		if err := runStatus([]string{"-url", ts.URL, "-org", "org-alpha"}, &buf); err != nil {
			t.Fatalf("runStatus list failed: %v", err)
		}
		var jobs []struct {
			JobID string `json:"job_id"`
			OrgID string `json:"org_id"`
		}
		if err := json.Unmarshal(buf.Bytes(), &jobs); err != nil {
			t.Fatalf("invalid json list output: %v\nbody: %s", err, buf.String())
		}
		if len(jobs) != 1 || jobs[0].JobID != "job-status-1" || jobs[0].OrgID != "org-alpha" {
			t.Fatalf("unexpected jobs list: %+v", jobs)
		}
	})
}

func TestRunSavings(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	ts := httptest.NewServer(server.New(coord))
	defer ts.Close()

	var buf bytes.Buffer
	if err := run([]string{"savings", "-url", ts.URL, "-min-vram", "24", "-min-availability", "0.5"}, &buf); err != nil {
		t.Fatalf("run savings failed: %v", err)
	}

	var placements []cost.FleetPlacement
	if err := json.Unmarshal(buf.Bytes(), &placements); err != nil {
		t.Fatalf("invalid json output: %v\nbody: %s", err, buf.String())
	}
	if len(placements) == 0 {
		t.Fatal("expected non-empty fleet placements from /v1/fleet/rank")
	}
	for _, p := range placements {
		if p.Pool.VRAMGB < 24 {
			t.Errorf("placement %+v has Pool.VRAMGB < 24", p)
		}
	}
}

func TestRunBilling(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	if _, err := coord.SubmitJob("job-billing-1", "org-billing", cost.GPUL4, 100, 2); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	ts := httptest.NewServer(server.New(coord))
	defer ts.Close()

	t.Run("missing org returns error", func(t *testing.T) {
		var buf bytes.Buffer
		err := runBilling([]string{"-url", ts.URL}, &buf)
		if err == nil {
			t.Fatal("expected error when -org is missing")
		}
	})

	t.Run("success", func(t *testing.T) {
		var buf bytes.Buffer
		if err := run([]string{"billing", "-url", ts.URL, "-org", "org-billing", "-fee", "0.25"}, &buf); err != nil {
			t.Fatalf("run billing failed: %v", err)
		}
		var summary orchestrator.BillingSummary
		if err := json.Unmarshal(buf.Bytes(), &summary); err != nil {
			t.Fatalf("invalid json output: %v\nbody: %s", err, buf.String())
		}
		if summary.OrgID != "org-billing" || summary.TotalJobs != 1 {
			t.Fatalf("unexpected billing summary: %+v", summary)
		}
	})
}

func TestCLIAuthBearerToken(t *testing.T) {
	coord := orchestrator.NewCoordinator()
	if _, err := coord.SubmitJob("job-auth-1", "org-auth", cost.GPUL4, 100, 2); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	ts := httptest.NewServer(server.New(coord).WithAPIKey("secret-token"))
	defer ts.Close()

	t.Setenv("COMPUTEHARBOR_API_KEY", "")

	cases := []struct {
		name string
		args []string
	}{
		{name: "status", args: []string{"status", "-url", ts.URL, "-org", "org-auth"}},
		{name: "savings", args: []string{"savings", "-url", ts.URL}},
		{name: "billing", args: []string{"billing", "-url", ts.URL, "-org", "org-auth"}},
	}

	for _, tc := range cases {
		t.Run(tc.name+" unauthorized without api key", func(t *testing.T) {
			var buf bytes.Buffer
			err := run(tc.args, &buf)
			if err == nil || !strings.Contains(err.Error(), "401") {
				t.Fatalf("expected 401 error without api key, got: %v", err)
			}
		})

		t.Run(tc.name+" succeeds with api key flag", func(t *testing.T) {
			var buf bytes.Buffer
			args := append(append([]string{}, tc.args...), "-api-key", "secret-token")
			if err := run(args, &buf); err != nil {
				t.Fatalf("expected 200 with -api-key, got: %v", err)
			}
		})
	}
}
