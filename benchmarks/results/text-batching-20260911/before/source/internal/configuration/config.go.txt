// Package configuration loads operator-owned deployment policy. Repository
// files and HTTP requests cannot change provider credentials or sandbox policy.
package configuration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
)

type Source struct {
	Path      string `json:"path"`
	Hash      string `json:"hash"`
	ProfileID string `json:"profile_id"`
	HasTarget bool   `json:"has_target"`
}
type Config struct {
	Listen         string                           `json:"listen"`
	ArtifactRoot   string                           `json:"artifact_root"`
	SigningKeyFile string                           `json:"signing_key_file"`
	WorkerID       string                           `json:"worker_id"`
	WorkerSlots    int                              `json:"worker_slots"`
	RunnerID       string                           `json:"runner_id"`
	Runner         runnerclient.ClientConfig        `json:"runner"`
	Sources        map[string]Source                `json:"sources"`
	Configs        map[string]persistence.Config    `json:"configs"`
	Models         map[string]application.ModelSpec `json:"models"`
	Providers      map[string]provider.Registry     `json:"providers"`
	// FakeScripts is an explicit deterministic protocol fixture. Enabling it
	// never constitutes a model-quality evaluation or a paid provider call.
	FakeScripts map[string][]provider.Script `json:"fake_scripts,omitempty"`
}

func Load(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return c, errors.New("configuration must contain exactly one object")
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8097"
	}
	host, _, err := net.SplitHostPort(c.Listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return c, errors.New("HTTP listener must be a numeric loopback address; use a TLS reverse proxy for remote access")
	}
	if !filepath.IsAbs(c.ArtifactRoot) || !filepath.IsAbs(c.SigningKeyFile) || c.WorkerSlots < 1 || c.WorkerSlots > 64 || c.WorkerID == "" || c.RunnerID == "" {
		return c, errors.New("absolute storage/key paths and bounded worker/runner identity are required")
	}
	if len(c.Configs) == 0 || len(c.Sources) == 0 {
		return c, errors.New("at least one source and run config are required")
	}
	for id, s := range c.Sources {
		if id == "" || !filepath.IsAbs(s.Path) || len(s.Hash) != 64 || s.ProfileID == "" {
			return c, fmt.Errorf("invalid source %q", id)
		}
	}
	for id, run := range c.Configs {
		if err := run.Validate(); err != nil {
			return c, fmt.Errorf("config %s: %w", id, err)
		}
		spec, ok := c.Models[run.Provider+"/"+run.Model]
		caps, registered := c.Providers[run.Provider][run.Model]
		if !ok || !registered || !caps.ToolCalling || spec.CredentialGroup == "" || spec.PriceVersion == "" || spec.ContextTokens <= 0 || spec.MaxOutputTokens <= 0 || spec.RequestTimeout <= 0 || spec.MaxOutputTokens > caps.MaxOutputTokens {
			return c, fmt.Errorf("config %s has no valid exact model registration", id)
		}
	}
	return c, nil
}

func (c Config) CheckSources(ctx context.Context) error {
	for id, s := range c.Sources {
		hash, err := runner.ComputeSourceHash(ctx, s.Path, 0, 0)
		if err != nil {
			return fmt.Errorf("source %s: %w", id, err)
		}
		if hash != s.Hash {
			return fmt.Errorf("source %s differs from its registered digest", id)
		}
	}
	return nil
}

func (c Config) Signer() (*runner.Signer, error) {
	info, err := os.Lstat(c.SigningKeyFile)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("signing key must be a private regular file (0600)")
	}
	key, err := os.ReadFile(c.SigningKeyFile)
	if err != nil {
		return nil, err
	}
	return runner.NewSigner(key)
}

func (c Config) BuildProviders() (map[string]provider.Provider, error) {
	result := map[string]provider.Provider{}
	for name, registry := range c.Providers {
		switch name {
		case "openai":
			if strings.TrimSpace(os.Getenv("OPENAI_API_KEY")) == "" {
				return nil, errors.New("OPENAI_API_KEY required for configured provider")
			}
			result[name] = provider.NewOpenAI(registry, provider.Limits{})
		case "anthropic":
			if strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")) == "" {
				return nil, errors.New("ANTHROPIC_API_KEY required for configured provider")
			}
			result[name] = provider.NewAnthropic(registry, provider.Limits{})
		case "fake":
			if len(c.FakeScripts) == 0 {
				return nil, errors.New("fake provider requires explicit fixture scripts")
			}
		default:
			return nil, fmt.Errorf("unsupported provider %q", name)
		}
	}
	return result, nil
}
