package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/orchestrator"
	"github.com/creova-gif/computeharbor/pkg/server"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "computeharbor: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) > 0 && args[0] == "serve" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runServe(ctx, args[1:], out)
	}

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

func runServe(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("computeharbor serve", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	reapInterval := fs.Duration("reap-interval", 5*time.Second, "Background stale-lease reaper interval")
	leaseTTL := fs.Duration("lease-ttl", 30*time.Second, "Worker heartbeat lease TTL before preemption")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	return serveListener(ctx, ln, out, *reapInterval, *leaseTTL)
}

func serveListener(ctx context.Context, ln net.Listener, out io.Writer, reapInterval, leaseTTL time.Duration) error {
	api := server.New(orchestrator.NewCoordinator())
	reaperCtx, cancelReaper := context.WithCancel(ctx)
	defer cancelReaper()
	reaperDone := api.StartReaper(reaperCtx, reapInterval, leaseTTL)

	srv := &http.Server{
		Handler:           api,
		ReadHeaderTimeout: 10 * time.Second,
	}

	fmt.Fprintf(out, "computeharbor control plane listening on %s\n", ln.Addr().String())

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case err := <-errCh:
		cancelReaper()
		<-reaperDone
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			cancelReaper()
			<-reaperDone
			return err
		}
		err := <-errCh
		cancelReaper()
		<-reaperDone
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
