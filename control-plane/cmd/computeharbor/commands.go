package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultControlPlaneURL = "http://127.0.0.1:8080"

func runStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("computeharbor status", flag.ContinueOnError)
	baseURL := fs.String("url", defaultControlPlaneURL, "Control plane base URL")
	orgID := fs.String("org", "", "Organization ID for X-Org-ID header (required)")
	jobID := fs.String("job", "", "Optional job ID to inspect a single job")
	apiKey := fs.String("api-key", os.Getenv("COMPUTEHARBOR_API_KEY"), "Optional Bearer API key for HTTP authentication")
	if err := fs.Parse(args); err != nil {
		return err
	}

	org := strings.TrimSpace(*orgID)
	if org == "" {
		return errors.New("-org is required")
	}

	endpoint := strings.TrimRight(*baseURL, "/") + "/v1/jobs"
	if job := strings.TrimSpace(*jobID); job != "" {
		endpoint += "/" + url.PathEscape(job)
	}

	return doJSONGet(&http.Client{Timeout: 10 * time.Second}, endpoint, org, strings.TrimSpace(*apiKey), out)
}

func runSavings(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("computeharbor savings", flag.ContinueOnError)
	baseURL := fs.String("url", defaultControlPlaneURL, "Control plane base URL")
	minVRAM := fs.Int("min-vram", 24, "Minimum GPU VRAM in GB")
	provider := fs.String("provider", "", "Optional cloud provider filter")
	minAvail := fs.Float64("min-availability", 0.0, "Minimum fleet availability score")
	apiKey := fs.String("api-key", os.Getenv("COMPUTEHARBOR_API_KEY"), "Optional Bearer API key for HTTP authentication")
	if err := fs.Parse(args); err != nil {
		return err
	}

	q := url.Values{}
	q.Set("min_vram", strconv.Itoa(*minVRAM))
	q.Set("min_availability", strconv.FormatFloat(*minAvail, 'f', -1, 64))
	if p := strings.TrimSpace(*provider); p != "" {
		q.Set("provider", p)
	}

	endpoint := strings.TrimRight(*baseURL, "/") + "/v1/fleet/rank?" + q.Encode()
	return doJSONGet(&http.Client{Timeout: 10 * time.Second}, endpoint, "", strings.TrimSpace(*apiKey), out)
}

func runBilling(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("computeharbor billing", flag.ContinueOnError)
	baseURL := fs.String("url", defaultControlPlaneURL, "Control plane base URL")
	orgID := fs.String("org", "", "Organization ID for X-Org-ID header (required)")
	fee := fs.Float64("fee", 0.12, "Platform savings fee fraction")
	apiKey := fs.String("api-key", os.Getenv("COMPUTEHARBOR_API_KEY"), "Optional Bearer API key for HTTP authentication")
	if err := fs.Parse(args); err != nil {
		return err
	}

	org := strings.TrimSpace(*orgID)
	if org == "" {
		return errors.New("-org is required")
	}

	q := url.Values{}
	q.Set("fee", strconv.FormatFloat(*fee, 'f', -1, 64))

	endpoint := strings.TrimRight(*baseURL, "/") + "/v1/billing/summary?" + q.Encode()
	return doJSONGet(&http.Client{Timeout: 10 * time.Second}, endpoint, org, strings.TrimSpace(*apiKey), out)
}

func doJSONGet(client *http.Client, endpoint, orgID, apiKey string, out io.Writer) error {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if orgID != "" {
		req.Header.Set("X-Org-ID", orgID)
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("request failed (%s): %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var formatted bytes.Buffer
	if err := json.Indent(&formatted, body, "", "  "); err != nil {
		return fmt.Errorf("invalid JSON response: %w", err)
	}
	formatted.WriteByte('\n')
	_, err = out.Write(formatted.Bytes())
	return err
}
