// Package localsetup provisions a disposable, explicitly selected development
// database. Credentials are written once to private files, never logged.
package localsetup

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/jackc/pgx/v5"
)

func Setup(ctx context.Context, databaseURL, repoDir, stateDir string) error {
	u, err := url.Parse(databaseURL)
	if err != nil || u.Path != "/forge" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		return errors.New("local-setup requires the explicitly selected loopback /forge development database")
	}
	repoDir, err = filepath.Abs(repoDir)
	if err != nil {
		return err
	}
	stateDir, err = filepath.Abs(stateDir)
	if err != nil {
		return err
	}
	// Repeating setup must not silently rotate credentials or lose operator state.
	if err = os.Mkdir(stateDir, 0700); err != nil {
		return fmt.Errorf("setup needs a new private state directory: %w", err)
	}
	if err = db.Migrate(ctx, databaseURL); err != nil {
		return err
	}
	s, err := persistence.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer s.Close()
	suffix := strings.ToLower(rand.Text()[:10])
	apiName, workerName := "forge_api_"+suffix, "forge_worker_"+suffix
	apiPass, workerPass := rand.Text()+rand.Text(), rand.Text()+rand.Text()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, role := range []struct {
		name, password string
		bypass         bool
	}{{apiName, apiPass, false}, {workerName, workerPass, true}} {
		bypass := "NOBYPASSRLS"
		if role.bypass {
			bypass = "BYPASSRLS"
		}
		name := pgx.Identifier{role.name}.Sanitize()
		// rand.Text contains only base32 characters; no user text is SQL quoted.
		if _, err = tx.Exec(ctx, "CREATE ROLE "+name+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "+bypass+" PASSWORD '"+role.password+"'"); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "GRANT USAGE ON SCHEMA public TO "+name); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO "+name); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "REVOKE ALL ON goose_db_version,api_tokens,memberships,tenants FROM "+name); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "GRANT SELECT ON memberships,tenants TO "+name); err != nil {
			return err
		}
		if !role.bypass {
			if _, err = tx.Exec(ctx, "GRANT SELECT ON api_tokens TO "+name); err != nil {
				return err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	apiURL, workerURL := *u, *u
	apiURL.User = url.UserPassword(apiName, apiPass)
	workerURL.User = url.UserPassword(workerName, workerPass)
	for name, data := range map[string]string{"api.env": "FORGE_DATABASE_URL=" + shellQuote(apiURL.String()) + "\n", "worker.env": "FORGE_DATABASE_URL=" + shellQuote(workerURL.String()) + "\n"} {
		if err = writePrivate(filepath.Join(stateDir, name), []byte(data)); err != nil {
			return err
		}
	}
	if err = s.BootstrapTenant(ctx, "demo", "operator", "admin"); err != nil {
		return err
	}
	token, err := s.IssueToken(ctx, "operator", 30*24*time.Hour)
	if err != nil {
		return err
	}
	if err = writePrivate(filepath.Join(stateDir, "client.env"), []byte("FORGE_API_URL=http://127.0.0.1:8097\nFORGE_TENANT=demo\nFORGE_TOKEN="+token+"\n")); err != nil {
		return err
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return err
	}
	keyPath := filepath.Join(stateDir, "runner.key")
	if err = writePrivate(keyPath, key); err != nil {
		return err
	}
	artifacts := filepath.Join(stateDir, "artifacts")
	socket := filepath.Join(stateDir, "runner.sock")
	c := configuration.Config{Listen: "127.0.0.1:8097", ArtifactRoot: artifacts, SigningKeyFile: keyPath, WorkerID: "local-worker", WorkerSlots: 2, RunnerID: "local", Runner: runnerclient.ClientConfig{UnixSocket: socket}, Sources: map[string]configuration.Source{}, Configs: map[string]persistence.Config{"demo": {Provider: "fake", Model: "fake", MaxModelRounds: 8, MaxToolCalls: 20, MaxCost: 1_000_000, MaxRuntimeSeconds: 300}}, Models: map[string]application.ModelSpec{"fake/fake": {CredentialGroup: "fake", PriceVersion: "fixture-zero-v1", ExactPricing: true, ContextTokens: 32768, MaxOutputTokens: 4096, RequestTimeout: 20 * time.Second}}, Providers: map[string]provider.Registry{"fake": {"fake": {ToolCalling: true, ContextWindow: 65536, MaxOutputTokens: 8192}}}, FakeScripts: map[string][]provider.Script{}}
	fixtures, err := LoadFixtures(ctx, repoDir)
	if err != nil {
		return err
	}
	c.Sources, c.FakeScripts = fixtures.Sources, fixtures.Scripts
	profiles := fixtures.Profiles
	sources := map[string]string{}
	for id, source := range fixtures.Sources {
		sources[id] = source.Path
	}
	if err = writeJSON(filepath.Join(stateDir, "platform.json"), c); err != nil {
		return err
	}
	runnerConfig := map[string]any{"root_dir": filepath.Join(stateDir, "runner"), "journal_path": filepath.Join(stateDir, "runner.db"), "artifact_root": artifacts, "signing_key_file": keyPath, "sources": sources, "profiles": profiles, "server": runnerclient.ServerConfig{UnixSocket: socket}}
	// The fixed slot pool is inserted by the operator after mount-state has been
	// verified. An unconfigured production runner fails closed at admission.
	if err = writeJSON(filepath.Join(stateDir, "runner.json"), runnerConfig); err != nil {
		return err
	}
	if err = s.RegisterRunner(ctx, "local", "unix://"+socket, 4); err != nil {
		return err
	}
	if err = quota.New(s.Pool).Configure(ctx, quota.Config{CredentialGroup: "fake", MaxConcurrent: 4, MaxTokens: 10_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: 10 * time.Second}); err != nil {
		return err
	}
	return nil
}

// Environment files are sourced by the documented shell startup commands.
// Treat URL query separators and substitutions as literal value bytes.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func writePrivate(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writePrivate(path, append(data, '\n'))
}
func tool(id, name, args string) provider.Script {
	return provider.Script{Chunks: []provider.Chunk{{Kind: "tool_start", CallID: id, Name: name}, {Kind: "tool_delta", CallID: id, Delta: args}, {Kind: "tool_end", CallID: id}}, FinishReason: "tool_calls", Usage: provider.Usage{Input: provider.TokenCount{Value: 100, Known: true}, Output: provider.TokenCount{Value: 50, Known: true}, CacheRead: provider.TokenCount{Known: true}, CacheWrite: provider.TokenCount{Known: true}}}
}
