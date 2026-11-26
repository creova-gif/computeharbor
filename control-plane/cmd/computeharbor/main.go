package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/creova-gif/computeharbor/pkg/cost"
	"github.com/creova-gif/computeharbor/pkg/events"
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
	if len(args) > 0 {
		switch args[0] {
		case "serve":
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return runServe(ctx, args[1:], out)
		case "status":
			return runStatus(args[1:], out)
		case "savings":
			return runSavings(args[1:], out)
		case "billing":
			return runBilling(args[1:], out)
		}
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

type serveConfig struct {
	reapInterval     time.Duration
	leaseTTL         time.Duration
	stateFile        string
	snapshotInterval time.Duration
	rateLimitRPS     float64
	rateLimitBurst   int
	logJSON          bool
	webhookURL       string
	webhookSecret    string
	apiKey           string
	orgAPIKeys       map[string]string
}

func parseOrgAPIKeys(spec string) (map[string]string, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	out := make(map[string]string)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		org, tok, ok := strings.Cut(part, "=")
		org = strings.TrimSpace(org)
		tok = strings.TrimSpace(tok)
		if !ok || org == "" || tok == "" {
			return nil, fmt.Errorf("invalid -org-api-keys entry %q: expected org_id=token", part)
		}
		if _, exists := out[tok]; exists {
			return nil, fmt.Errorf("duplicate org API token for org %q", org)
		}
		out[tok] = org
	}
	return out, nil
}

func runServe(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("computeharbor serve", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	reapInterval := fs.Duration("reap-interval", 5*time.Second, "Background stale-lease reaper interval")
	leaseTTL := fs.Duration("lease-ttl", 30*time.Second, "Worker heartbeat lease TTL before preemption")
	stateFile := fs.String("state-file", "", "Optional path to persist coordinator JSON snapshot")
	snapshotInterval := fs.Duration("snapshot-interval", 5*time.Second, "Background coordinator snapshot interval when -state-file is set")
	rateLimitRPS := fs.Float64("rate-limit-rps", 0, "Per-org HTTP rate limit in requests per second (<= 0 disables)")
	rateLimitBurst := fs.Int("rate-limit-burst", 20, "Per-org HTTP rate limit burst size")
	logJSON := fs.Bool("log-json", true, "Emit structured JSON HTTP request logs to stderr")
	webhookURL := fs.String("webhook-url", "", "Optional HTTP endpoint to receive signed lifecycle webhook events")
	webhookSecret := fs.String("webhook-secret", "", "HMAC-SHA256 secret used to sign webhook payloads")
	apiKey := fs.String("api-key", os.Getenv("COMPUTEHARBOR_API_KEY"), "Optional Bearer API key for HTTP authentication")
	orgAPIKeysSpec := fs.String("org-api-keys", os.Getenv("COMPUTEHARBOR_ORG_API_KEYS"), "Optional comma-separated org1=tok1,org2=tok2 pairs for org-scoped Bearer authentication")
	if err := fs.Parse(args); err != nil {
		return err
	}
	orgAPIKeys, err := parseOrgAPIKeys(*orgAPIKeysSpec)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	return serveListenerWithConfig(ctx, ln, out, serveConfig{
		reapInterval:     *reapInterval,
		leaseTTL:         *leaseTTL,
		stateFile:        *stateFile,
		snapshotInterval: *snapshotInterval,
		rateLimitRPS:     *rateLimitRPS,
		rateLimitBurst:   *rateLimitBurst,
		logJSON:          *logJSON,
		webhookURL:       *webhookURL,
		webhookSecret:    *webhookSecret,
		apiKey:           *apiKey,
		orgAPIKeys:       orgAPIKeys,
	})
}

func serveListener(ctx context.Context, ln net.Listener, out io.Writer, reapInterval, leaseTTL time.Duration, stateFile string) error {
	return serveListenerWithConfig(ctx, ln, out, serveConfig{
		reapInterval: reapInterval,
		leaseTTL:     leaseTTL,
		stateFile:    stateFile,
	})
}

func serveListenerWithConfig(ctx context.Context, ln net.Listener, out io.Writer, cfg serveConfig) error {
	coord := orchestrator.NewCoordinator()
	if cfg.stateFile != "" {
		var err error
		coord, err = orchestrator.LoadCoordinatorFromFile(cfg.stateFile)
		if err != nil {
			return fmt.Errorf("load coordinator state: %w", err)
		}
	}
	var webhookStats server.WebhookStats
	if cfg.webhookURL != "" {
		dispatcher := events.NewWebhookDispatcher(cfg.webhookURL, cfg.webhookSecret, 256)
		coord.WithEventSink(dispatcher)
		defer dispatcher.Close()
		webhookStats = dispatcher
	}

	api := server.New(coord)
	if webhookStats != nil {
		api.WithWebhookStats(webhookStats)
	}
	if cfg.logJSON {
		api.WithLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	}
	if cfg.rateLimitRPS > 0 {
		api.WithRateLimit(cfg.rateLimitRPS, cfg.rateLimitBurst)
	}
	if cfg.apiKey != "" {
		api.WithAPIKey(cfg.apiKey)
	}
	if len(cfg.orgAPIKeys) > 0 {
		api.WithOrgAPIKeys(cfg.orgAPIKeys)
	}
	reaperCtx, cancelReaper := context.WithCancel(ctx)
	defer cancelReaper()
	reaperDone := api.StartReaper(reaperCtx, cfg.reapInterval, cfg.leaseTTL)
	snapshotDone := api.StartSnapshotter(reaperCtx, cfg.stateFile, cfg.snapshotInterval)

	srv := &http.Server{
		Handler:           api,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
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
		<-snapshotDone
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		api.SetDraining(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			cancelReaper()
			<-reaperDone
			<-snapshotDone
			return err
		}
		err := <-errCh
		cancelReaper()
		<-reaperDone
		<-snapshotDone
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	if cfg.stateFile != "" {
		if err := coord.SaveSnapshot(cfg.stateFile); err != nil {
			return fmt.Errorf("save coordinator state: %w", err)
		}
	}
	return nil
}
