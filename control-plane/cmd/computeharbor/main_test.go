package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
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
		errCh <- serveListener(ctx, ln, &buf, 5*time.Second, 30*time.Second)
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
