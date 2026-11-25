package main

import (
	"bytes"
	"encoding/json"
	"testing"

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
