package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/creova-gif/computeharbor/pkg/cost"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "computeharbor: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("computeharbor", flag.ContinueOnError)
	gpu := fs.String("gpu", string(cost.GPUL4), "GPU pool identifier")
	count := fs.Int("count", 4, "Number of GPUs")
	hours := fs.Float64("hours", 10.0, "Active training hours")
	preempts := fs.Int("preemptions", 3, "Expected spot preemptions")
	recoverySec := fs.Float64("recovery-sec", 12.0, "Checkpoint and reschedule time in seconds")
	fee := fs.Float64("fee", 0.12, "Platform savings fee fraction")
	rankVRAM := fs.Int("rank-vram", 0, "If > 0, rank all spot pools meeting minimum VRAM GB")

	if err := fs.Parse(args); err != nil {
		return err
	}

	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")

	if *rankVRAM > 0 {
		ranked, err := cost.RankPoolsForWorkload(*rankVRAM, *count, *hours, *fee)
		if err != nil {
			return err
		}
		return enc.Encode(ranked)
	}

	res, err := cost.Calculate(cost.EstimateRequest{
		GPU:                cost.GPUType(*gpu),
		GPUCount:           *count,
		ActiveTrainingHrs:  *hours,
		ExpectedPreempts:   *preempts,
		RecoverySeconds:    *recoverySec,
		SavingsFeeFraction: *fee,
	})
	if err != nil {
		return err
	}
	return enc.Encode(res)
}
