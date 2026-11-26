package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/events"
)

func TestCLISingleQuoteAndRank(t *testing.T) {
	var buf bytes.Buffer
	if err := run([]string{"-gpu", "nvidia-a100-40gb", "-count", "8", "-hours", "24", "-preemptions", "3"}, &buf); err != nil {
		t.Fatalf("run single quote failed: %v", err)
	}
	var res cost.EstimateResult
	if err := json.Unmarshal(buf.Bytes(), &res); err != nil {
		t.Fatalf("invalid json output: %v", err)
	}
	if !res.MeetsTargetSavings {
		t.Errorf("expected MeetsTargetSavings=true, got false (net savings %.4f)", res.NetSavingsFraction)
	}

	buf.Reset()
	if err := run([]string{"-rank-vram", "24", "-count", "4", "-hours", "12"}, &buf); err != nil {
		t.Fatalf("run rank-vram failed: %v", err)
	}
	var ranked []cost.EstimateResult
	if err := json.Unmarshal(buf.Bytes(), &ranked); err != nil {
		t.Fatalf("invalid ranked json: %v", err)
	}
	if len(ranked) != 5 {
		t.Errorf("len(ranked) = %d, want 5", len(ranked))
	}
}

func TestServeSubcommandFlagError(t *testing.T) {
	var buf bytes.Buffer
	err := run([]string{"serve", "-unknown-flag"}, &buf)
	if err == nil {
		t.Fatal("expected error for unknown flag in serve subcommand")
	}
}

func TestServeListenerHealthzAndShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- serveListener(ctx, ln, &buf, 5*time.Second, 30*time.Second, "")
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", resp.StatusCode)
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("serveListener returned error: %v", err)
	}
	if !strings.Contains(buf.String(), addr) {
		t.Errorf("expected listen log to contain %q, got %q", addr, buf.String())
	}
}

func TestRunServeReaperFlagsAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	if err := runServe(ctx, []string{"-addr", "127.0.0.1:0", "-reap-interval", "10ms", "-lease-ttl", "50ms"}, &buf); err != nil {
		t.Fatalf("runServe with reaper flags failed: %v", err)
	}
}

func TestServeListenerStateFilePersistence(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")

	// First run: submit a job and shut down gracefully.
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 1: %v", err)
	}
	addr1 := ln1.Addr().String()
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	var buf1 bytes.Buffer
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- serveListener(ctx1, ln1, &buf1, 5*time.Second, 30*time.Second, stateFile)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	payload := `{"job_id":"job-persist-1","org_id":"org-1","gpu":"nvidia-l4-24gb","target_steps":100,"max_preemptions":2}`
	resp, err := client.Post("http://"+addr1+"/v1/jobs", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /v1/jobs: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/jobs status = %d, want 201", resp.StatusCode)
	}

	cancel1()
	if err := <-errCh1; err != nil {
		t.Fatalf("first serveListener returned error: %v", err)
	}

	if _, err := os.Stat(stateFile); err != nil {
		t.Fatalf("expected state file %q to exist after shutdown: %v", stateFile, err)
	}

	// Second run: reload stateFile and verify the job is present.
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 2: %v", err)
	}
	addr2 := ln2.Addr().String()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	var buf2 bytes.Buffer
	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- serveListener(ctx2, ln2, &buf2, 5*time.Second, 30*time.Second, stateFile)
	}()

	resp2Req, err := http.NewRequest(http.MethodGet, "http://"+addr2+"/v1/jobs/job-persist-1", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp2Req.Header.Set("X-Org-ID", "org-1")
	resp2, err := client.Do(resp2Req)
	if err != nil {
		cancel2()
		<-errCh2
		t.Fatalf("GET /v1/jobs/job-persist-1: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		cancel2()
		<-errCh2
		t.Fatalf("GET /v1/jobs/job-persist-1 status = %d, want 200", resp2.StatusCode)
	}

	cancel2()
	if err := <-errCh2; err != nil {
		t.Fatalf("second serveListener returned error: %v", err)
	}
}

func TestRunServeStateFileFlagCorruptError(t *testing.T) {
	corruptFile := filepath.Join(t.TempDir(), "corrupt.json")
	if err := os.WriteFile(corruptFile, []byte("{bad-json"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	err := runServe(ctx, []string{"-addr", "127.0.0.1:0", "-state-file", corruptFile}, &buf)
	if err == nil {
		t.Fatal("expected error when loading corrupt -state-file, got nil")
	}
}

func TestRunServeRateLimitFlags(t *testing.T) {
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		errCh <- runServe(ctx, []string{
			"-addr", "127.0.0.1:0",
			"-rate-limit-rps", "1",
			"-rate-limit-burst", "1",
			"-log-json=true",
		}, pw)
	}()

	scanner := bufio.NewScanner(pr)
	if !scanner.Scan() {
		cancel()
		<-errCh
		t.Fatalf("expected listen line, scan err: %v", scanner.Err())
	}
	line := scanner.Text()
	const prefix = "computeharbor control plane listening on "
	if !strings.HasPrefix(line, prefix) {
		cancel()
		<-errCh
		t.Fatalf("unexpected listen line %q", line)
	}
	addr := strings.TrimSpace(strings.TrimPrefix(line, prefix))

	// Drain any remaining pipe output so pw never blocks.
	go func() {
		_, _ = io.Copy(io.Discard, pr)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	doReq := func(orgID string) int {
		req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/v1/jobs", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("X-Org-ID", orgID)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := doReq("org-rate-test"); got != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", got)
	}
	if got := doReq("org-rate-test"); got != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", got)
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("runServe returned error: %v", err)
	}
}

func TestRunServeWebhookDelivery(t *testing.T) {
	type deliveredWebhook struct {
		body []byte
		sig  string
	}
	receivedCh := make(chan deliveredWebhook, 4)

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		receivedCh <- deliveredWebhook{
			body: body,
			sig:  r.Header.Get(events.SignatureHeader),
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer receiver.Close()

	const webhookSecret = "test-webhook-secret"

	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		errCh <- runServe(ctx, []string{
			"-addr", "127.0.0.1:0",
			"-webhook-url", receiver.URL,
			"-webhook-secret", webhookSecret,
		}, pw)
	}()

	scanner := bufio.NewScanner(pr)
	if !scanner.Scan() {
		cancel()
		<-errCh
		t.Fatalf("expected listen line, scan err: %v", scanner.Err())
	}
	line := scanner.Text()
	const prefix = "computeharbor control plane listening on "
	if !strings.HasPrefix(line, prefix) {
		cancel()
		<-errCh
		t.Fatalf("unexpected listen line %q", line)
	}
	addr := strings.TrimSpace(strings.TrimPrefix(line, prefix))

	go func() {
		_, _ = io.Copy(io.Discard, pr)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	payload := `{"job_id":"job-wh-1","org_id":"org-wh","gpu":"nvidia-l4-24gb","target_steps":10,"max_preemptions":1}`
	resp, err := client.Post("http://"+addr+"/v1/jobs", "application/json", strings.NewReader(payload))
	if err != nil {
		cancel()
		<-errCh
		t.Fatalf("POST /v1/jobs: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		cancel()
		<-errCh
		t.Fatalf("POST /v1/jobs status = %d, want 201", resp.StatusCode)
	}

	var got deliveredWebhook
	select {
	case got = <-receivedCh:
	case <-time.After(3 * time.Second):
		cancel()
		<-errCh
		t.Fatal("timed out waiting for job.submitted webhook delivery")
	}

	if !events.VerifySignature(webhookSecret, got.body, got.sig) {
		t.Fatalf("invalid webhook signature %q for payload %s", got.sig, string(got.body))
	}

	var evt events.Event
	if err := json.Unmarshal(got.body, &evt); err != nil {
		t.Fatalf("unmarshal webhook event: %v", err)
	}
	if evt.Type != events.EventJobSubmitted {
		t.Errorf("evt.Type = %q, want %q", evt.Type, events.EventJobSubmitted)
	}
	if evt.JobID != "job-wh-1" {
		t.Errorf("evt.JobID = %q, want %q", evt.JobID, "job-wh-1")
	}
	if evt.OrgID != "org-wh" {
		t.Errorf("evt.OrgID = %q, want %q", evt.OrgID, "org-wh")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("runServe returned error: %v", err)
	}
}
