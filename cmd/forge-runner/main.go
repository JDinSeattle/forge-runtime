package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

type configuration struct {
	RootDir          string                     `json:"root_dir"`
	JournalPath      string                     `json:"journal_path"`
	ArtifactRoot     string                     `json:"artifact_root"`
	SigningKeyFile   string                     `json:"signing_key_file"`
	Sources          map[string]string          `json:"sources"`
	Profiles         map[string]sandbox.Profile `json:"profiles"`
	Server           runnerclient.ServerConfig  `json:"server"`
	DockerHost       string                     `json:"docker_host,omitempty"`
	VolumeSlots      []sandbox.VolumeSpec       `json:"volume_slots"`
	DockerBinary     string                     `json:"docker_binary,omitempty"`
	MaxArtifactBytes int64                      `json:"max_artifact_bytes,omitempty"`
	AllowTestBackend bool                       `json:"allow_test_backend,omitempty"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("runner stopped", "error", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	flags := flag.NewFlagSet("forge-runner", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to operator-owned runner JSON configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" || flags.NArg() != 0 {
		return errors.New("usage: forge-runner -config /absolute/path/runner.json")
	}
	file, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer file.Close()
	var c configuration
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&c); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return errors.New("configuration must contain exactly one JSON object")
	}
	keyInfo, err := os.Stat(c.SigningKeyFile)
	if err != nil {
		return err
	}
	if !keyInfo.Mode().IsRegular() || keyInfo.Mode().Perm()&0077 != 0 {
		return errors.New("signing key must be a private regular file (0600 or stricter)")
	}
	key, err := os.ReadFile(c.SigningKeyFile)
	if err != nil {
		return err
	}
	signer, err := runner.NewSigner(key)
	if err != nil {
		return err
	}
	if c.MaxArtifactBytes == 0 {
		c.MaxArtifactBytes = 64 << 20
	}
	store, err := artifact.NewLocalStore(c.ArtifactRoot, c.MaxArtifactBytes)
	if err != nil {
		return err
	}
	defer store.Close()
	var backend sandbox.Backend = &sandbox.Docker{Host: c.DockerHost, Binary: c.DockerBinary, WorkspaceQuota: sandbox.FixedVolumeQuota{Slots: c.VolumeSlots}}
	if c.AllowTestBackend {
		slog.Warn("explicit no-process test backend enabled; no model or container verification claim is supported")
		backend = &sandbox.TestBackend{}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = backend.Check(ctx)
		cancel()
		if err != nil {
			return err
		}
		// A backend-specific quota verifier must be installed by deployment before
		// process admission. The default Docker adapter refuses unbounded volumes.
		slog.Info("process admission requires a verified fixed ext4 volume pool")
	}
	engine, err := runner.Open(runner.Config{RootDir: c.RootDir, JournalPath: c.JournalPath, Artifacts: store, Backend: backend, Signer: signer, Profiles: c.Profiles, Sources: c.Sources, VolumeSlots: c.VolumeSlots})
	if err != nil {
		return err
	}
	defer engine.Close()
	server, err := runnerclient.Serve(c.Server, engine)
	if err != nil {
		return err
	}
	slog.Info("runner listening", "address", server.Address(), "test_backend", c.AllowTestBackend)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.Wait() }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Close(shutdown); err != nil {
			return fmt.Errorf("runner RPC shutdown: %w", err)
		}
		return nil
	case err = <-done:
		return err
	}
}
