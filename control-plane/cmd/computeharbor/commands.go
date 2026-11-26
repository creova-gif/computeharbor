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

	return fetchAndPrintJSON(endpoint, org, out)
}

func runSavings(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("computeharbor savings", flag.ContinueOnError)
	baseURL := fs.String("url", defaultControlPlaneURL, "Control plane base URL")
	minVRAM := fs.Int("min-vram", 24, "Minimum GPU VRAM in GB")
	provider := fs.String("provider", "", "Optional cloud provider filter")
	minAvail := fs.Float64("min-availability", 0.0, "Minimum fleet availability score")
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
	return fetchAndPrintJSON(endpoint, "", out)
}

func fetchAndPrintJSON(endpoint, orgID string, out io.Writer) error {
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if orgID != "" {
		req.Header.Set("X-Org-ID", orgID)
	}

	client := &http.Client{Timeout: 10 * time.Second}
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
