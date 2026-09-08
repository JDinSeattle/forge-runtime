// forge-admin is intentionally a separate operator binary. Never expose its
// database role or token-issuing commands to the public HTTP service.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/localsetup"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	tenant := flag.String("tenant", "demo", "tenant identity")
	principal := flag.String("principal", "operator", "principal identity")
	role := flag.String("role", "admin", "viewer, developer, or admin")
	runner := flag.String("runner", "local", "runner identity")
	endpoint := flag.String("endpoint", "unix://./var/runner.sock", "runner endpoint")
	slots := flag.Int("slots", 4, "runner maximum concurrent workspaces")
	repo := flag.String("repo", ".", "repository containing the checked-in repair fixtures")
	state := flag.String("state", "./var/local", "new private local setup directory")
	age := flag.Duration("age", 168*time.Hour, "minimum terminal run age for event trimming")
	keep := flag.Int("keep", 128, "retained terminal event tail")
	config := flag.String("config", "", "platform config for explicit terminal workspace cleanup")
	runID := flag.String("run", "", "run identity for workspace-gc")
	flag.Parse()
	if flag.NArg() != 1 {
		return fmt.Errorf("usage: forge-admin [flags] migrate|bootstrap|token|register-runner|local-setup")
	}
	url := os.Getenv("FORGE_DATABASE_URL")
	if url == "" {
		return fmt.Errorf("FORGE_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if flag.Arg(0) == "local-setup" {
		return localsetup.Setup(ctx, url, *repo, *state)
	}
	if flag.Arg(0) == "migrate" {
		return db.Migrate(ctx, url)
	}
	s, err := persistence.Open(ctx, url)
	if err != nil {
		return err
	}
	defer s.Close()
	switch flag.Arg(0) {
	case "workspace-gc":
		if *config == "" || *runID == "" {
			return fmt.Errorf("workspace-gc requires -config and -run (and optional -tenant/-age)")
		}
		c, err := configuration.Load(*config)
		if err != nil {
			return err
		}
		signer, err := c.Signer()
		if err != nil {
			return err
		}
		a, err := artifact.NewLocalStore(c.ArtifactRoot, 64<<20)
		if err != nil {
			return err
		}
		defer a.Close()
		r, err := runnerclient.Dial(ctx, c.Runner)
		if err != nil {
			return err
		}
		defer r.Close()
		d := application.Driver{Store: s, Signer: signer, Artifacts: a, Runner: r, RunnerID: c.RunnerID}
		result, err := d.CleanupWorkspace(ctx, domain.ID(*tenant), domain.ID(*runID), string(persistence.NewID("operator")), *age)
		if err == nil {
			fmt.Printf("cleanup=%s phase=%s snapshot=%s\n", result.ID, result.Phase, result.SnapshotRef)
		}
		return err
	case "trim-events":
		if *age < time.Hour {
			return fmt.Errorf("event retention age must be at least one hour")
		}
		n, err := s.TrimEvents(ctx, time.Now().Add(-*age), *keep, 32)
		if err == nil {
			fmt.Printf("trimmed_events=%d\n", n)
		}
		return err
	case "bootstrap":
		return s.BootstrapTenant(ctx, domain.ID(*tenant), domain.ID(*principal), *role)
	case "token":
		token, err := s.IssueToken(ctx, domain.ID(*principal), 30*24*time.Hour)
		if err == nil {
			fmt.Println(token)
		}
		return err
	case "register-runner":
		return s.RegisterRunner(ctx, *runner, *endpoint, *slots)
	default:
		return fmt.Errorf("unknown operator command")
	}
}
