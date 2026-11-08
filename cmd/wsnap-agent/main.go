// Command wsnap-agent snapshots and restores workspaces. Run it as a native
// sidecar ("sidecar"), as a rescue Job ("mover"), as a maintenance Job
// ("maintain"), or as the sidecar's startupProbe ("ready").
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"github.com/chrisyifanjin/wsnap/internal/version"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/agent"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: wsnap-agent sidecar|worker|wait-worker|mover|restorer|maintain|ready|version [flags]")
		os.Exit(2)
	}
	mode := os.Args[1]
	if mode == "version" {
		fmt.Println(version.Version)
		return
	}
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	cfg := agent.Config{}
	fs.StringVar(&cfg.BrokerURL, "broker-url", envOr("WSNAP_BROKER_URL", "http://wsnap-broker.wsnap-system.svc:8080"), "broker URL")
	fs.StringVar(&cfg.TokenFile, "token-file", envOr("WSNAP_TOKEN_FILE", "/var/run/secrets/wsnap/token"), "projected service account token")
	fs.StringVar(&cfg.Dir, "dir", envOr("WSNAP_DIR", "/workspace"), "workspace directory")
	fs.StringVar(&cfg.StateDir, "state-dir", envOr("WSNAP_STATE_DIR", "/var/lib/wsnap"), "private state directory")
	fs.StringVar(&cfg.WorkerPodUID, "pod-uid", "", "workload Pod UID for worker readiness")
	fs.DurationVar(&cfg.WorkloadGrace, "workload-grace", 30*time.Second, "worker: how long the workload's containers may run after its Pod is deleted")
	fs.DurationVar(&cfg.FinalTimeout, "final-timeout", 25*time.Second, "time limit for the final snapshot after SIGTERM")
	fs.DurationVar(&cfg.IntervalOverride, "interval", 0, "override the repository's snapshot interval")
	caFile := fs.String("ca-file", os.Getenv("WSNAP_CA_FILE"), "CA bundle that signs the broker's TLS certificate")
	fs.DurationVar(&cfg.QuiesceTimeout, "quiesce-timeout", 0, "ask the workload to quiesce before each snapshot and wait this long for its ack (0 disables)")
	fs.StringVar(&cfg.Request, "request", "", "WorkspaceSnapshot or WorkspaceRestore served by this Job")
	fs.DurationVar(&cfg.FileRequestSpacing, "file-request-spacing", 30*time.Second, "shortest time between snapshots triggered by request files")
	fs.DurationVar(&cfg.OperationTimeout, "operation-timeout", 30*time.Minute, "time limit for one snapshot or restore; keep it at least 5m below the repository's credentials.ttl")
	_ = fs.Parse(os.Args[2:])
	// A credential request includes the broker's STS round trip.
	cfg.HTTP = &http.Client{Timeout: 60 * time.Second}
	// The agent's root filesystem may be read-only; the engines write
	// temporary files, so give them the private state directory.
	if os.Getenv("TMPDIR") == "" && mode != "ready" && mode != "wait-worker" {
		tmp := filepath.Join(cfg.StateDir, "tmp")
		if err := os.MkdirAll(tmp, 0o700); err == nil {
			_ = os.Setenv("TMPDIR", tmp)
		}
	}
	if *caFile != "" {
		pem, err := os.ReadFile(*caFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			fmt.Fprintln(os.Stderr, "no certificates in", *caFile)
			os.Exit(1)
		}
		cfg.HTTP.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	}
	cfg.Log = slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("mode", mode)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	var err error
	switch mode {
	case "sidecar":
		err = cfg.RunSidecar(ctx)
	case "worker":
		err = cfg.RunWorker(ctx)
	case "wait-worker":
		err = agent.WaitWorker(ctx, cfg.Dir, cfg.WorkerPodUID)
	case "mover":
		err = cfg.RunMover(ctx)
	case "restorer":
		err = cfg.RunRestorer(ctx)
	case "maintain":
		err = cfg.RunMaintenance(ctx)
	case "ready":
		err = agent.StartupReady(cfg.StateDir)
	default:
		err = fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		cfg.Log.Error("exiting", "error", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
