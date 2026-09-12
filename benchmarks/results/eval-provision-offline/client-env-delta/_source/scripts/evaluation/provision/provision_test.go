package main

import (
	"bytes"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func fixtureBundle(t *testing.T) bundle {
	t.Helper()
	scope := filepath.Join(t.TempDir(), "var", "lifecycle-rehearsals", "lr20260912_a")
	dir := filepath.Join(scope, "evaluation", "deepseek-evalv2-offline", "candidate")
	cache := domain.Money(6000)
	spec := application.ModelSpec{CredentialGroup: credentialGroup, PriceVersion: "deepseek-flash-peak-ceiling-20260912", InputPrice: 300000, OutputPrice: 1200000, CacheReadPrice: &cache, ContextTokens: 32768, MaxOutputTokens: 8192, RequestTimeout: time.Minute}
	caps := provider.Capabilities{ToolCalling: true, ContextWindow: 1000000, MaxOutputTokens: 384000}
	r := registration{Provider: "deepseek", Model: modelID, ConfigID: "eval-config", Total: 2000000, Tasks: map[string]domain.Money{}, Rounds: 20, Tools: 64, Runtime: 600, Capabilities: caps, Spec: spec, PriceSources: []string{"https://api-docs.deepseek.com/quick_start/pricing/"}, CapabilitySources: []string{"https://api-docs.deepseek.com/guides/responses_api/"}}
	c := runnerConfig{RootDir: filepath.Join(scope, "runtime", "engine"), JournalPath: filepath.Join(scope, "runtime", "journal.sqlite"), ArtifactRoot: filepath.Join(scope, "runtime", "artifacts"), SigningKeyFile: filepath.Join(scope, "runtime", "runner.key"), Sources: map[string]string{}, Profiles: map[string]sandbox.Profile{}, Server: runnerclient.ServerConfig{UnixSocket: "/tmp/forge-lifecycle-lr20260912_a/runner.sock"}, DockerHost: "unix:///run/user/1000/forge-runtime-docker.sock", VolumeSlots: make([]sandbox.VolumeSpec, 4)}
	p := configuration.Config{Listen: "127.0.0.1:18098", ArtifactRoot: c.ArtifactRoot, SigningKeyFile: c.SigningKeyFile, WorkerID: "eval-worker", WorkerSlots: 1, RunnerID: "application-fault-runner", Runner: runnerclient.ClientConfig{UnixSocket: c.Server.UnixSocket}, Sources: map[string]configuration.Source{}, Configs: map[string]persistence.Config{"eval-config": {Provider: "deepseek", Model: modelID, MaxModelRounds: 20, MaxToolCalls: 64, MaxCost: 500000, MaxRuntimeSeconds: 600}}, Models: map[string]application.ModelSpec{"deepseek/" + modelID: spec}, Providers: map[string]provider.Registry{"deepseek": {modelID: caps}}}
	for _, name := range cases {
		id := "evalv2_" + name
		path := filepath.Join(scope, "evaluation", "source", "scripts", "evaluation", "corpus", name, "source")
		r.Tasks[name] = 500000
		p.Sources[id] = configuration.Source{Path: path, Hash: strings.Repeat("a", 64), ProfileID: id, HasTarget: true}
		c.Sources[id] = path
		c.Profiles[id] = sandbox.Profile{ID: id, Image: "python@sha256:" + strings.Repeat("b", 64), MemoryBytes: 256 << 20, WorkspaceQuotaBytes: 256 << 20, CPUs: 1, PIDs: 64, User: "1000:1000"}
	}
	return bundle{Dir: dir, Out: filepath.Join(filepath.Dir(dir), "private"), Scope: scope, Platform: p, Runner: c, Registration: r}
}
func persistFixture(t *testing.T, b bundle) {
	t.Helper()
	for _, dir := range []string{b.Dir, filepath.Join(b.Scope, "runtime")} {
		if e := os.MkdirAll(dir, 0700); e != nil {
			t.Fatal(e)
		}
	}
	hashes := map[string]string{}
	for name, value := range map[string]any{"platform.json": b.Platform, "runner.json": b.Runner, "registration.json": b.Registration} {
		raw, e := json.Marshal(value)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(b.Dir, name), raw, 0600); e != nil {
			t.Fatal(e)
		}
		hashes[name] = sum(raw)
	}
	source := map[string]string{}
	for _, name := range cases {
		source[name] = b.Platform.Sources["evalv2_"+name].Hash
	}
	v := map[string]any{"candidate_only": true, "corpus_sha256": strings.Repeat("c", 64), "source_hashes": source, "platform_sha256": hashes["platform.json"], "runner_sha256": hashes["runner.json"], "registration_sha256": hashes["registration.json"]}
	if e := writeJSON(filepath.Join(b.Dir, "candidate.json"), v); e != nil {
		t.Fatal(e)
	}
	original := b.Runner
	original.Sources = map[string]string{"legacy": "original"}
	original.Profiles = nil
	if e := writeJSON(filepath.Join(b.Scope, "runtime", "runner.json"), original); e != nil {
		t.Fatal(e)
	}
}
func TestBundleDoesNotReadKeysOrCreateRuntime(t *testing.T) {
	b := fixtureBundle(t)
	persistFixture(t, b)
	t.Setenv("DEEPSEEK_API_KEY", "")
	t.Setenv("FORGE_EVAL_ADMIN_DATABASE_URL", "")
	got, e := loadBundle(b.Dir, b.Out)
	if e != nil {
		t.Fatal(e)
	}
	if got.Registration.Total != 2000000 || got.BatchID != "deepseek-flash-2usd-"+got.Hashes["registration.json"] {
		t.Fatal("fixed registration batch identity changed")
	}
	if e = run([]string{"-candidate", b.Dir, "-output", b.Out}); e == nil {
		t.Fatal("missing explicit admin accepted")
	}
	for _, p := range []string{b.Out, b.Runner.SigningKeyFile, b.Runner.JournalPath, b.Runner.RootDir, b.Runner.ArtifactRoot} {
		if _, e = os.Lstat(p); !os.IsNotExist(e) {
			t.Fatal("preflight created or touched runtime/credential path", p)
		}
	}
}
func TestExactAdminRoute(t *testing.T) {
	good := "postgres://operator:literal%26private@127.0.0.1:32773/forge?sslmode=disable"
	u, e := adminURL(good)
	if e != nil {
		t.Fatal(e)
	}
	scoped := scopedURL(u, "eval_ds_"+strings.Repeat("a", 32), "eval_role", "generated")
	if !strings.Contains(scoped, "search_path=eval_ds_") {
		t.Fatal("missing exact scope")
	}
	cases := []string{"", strings.Replace(good, "127.0.0.1", "localhost", 1), strings.Replace(good, "32773", "5432", 1), strings.Replace(good, "/forge?", "/other?", 1), strings.Replace(good, "operator:literal%26private@", "operator@", 1), strings.Replace(good, "disable", "require", 1), good + "&search_path=public", good + "&options=-csearch_path%3Dpublic", good + "&host=remote", good + "&sslmode=disable", good + "&%73earch_path=public", good + "&service=another", good + "#fragment"}
	for i, v := range cases {
		if _, e := adminURL(v); e == nil {
			t.Errorf("invalid admin route %d accepted", i)
		}
	}
}
func TestFixedPolicyRejectsExpansion(t *testing.T) {
	changes := map[string]func(*bundle){
		"other_model":       func(b *bundle) { b.Registration.Model = "deepseek-chat" },
		"bigger_batch":      func(b *bundle) { b.Registration.Total++ },
		"task_transfer":     func(b *bundle) { b.Registration.Tasks[cases[0]]++; b.Registration.Tasks[cases[1]]-- },
		"missing_task":      func(b *bundle) { delete(b.Registration.Tasks, cases[0]) },
		"more_rounds":       func(b *bundle) { b.Registration.Rounds++ },
		"exact_pricing":     func(b *bundle) { b.Registration.Spec.ExactPricing = true },
		"new_quote":         func(b *bundle) { b.Registration.Spec.InputPrice++ },
		"other_group":       func(b *bundle) { b.Registration.Spec.CredentialGroup = "other" },
		"thinking_contract": func(b *bundle) { b.Registration.Capabilities.NativeCompaction = true },
		"credential_url": func(b *bundle) {
			b.Registration.PriceSources = []string{"https://secret@api-docs.deepseek.com/pricing"}
		},
		"worker_fanout": func(b *bundle) { b.Platform.WorkerSlots = 2 },
		"fallback": func(b *bundle) {
			c := b.Platform.Configs["eval-config"]
			c.Fallback = &persistence.ModelRoute{Provider: "openai", Model: "other"}
			b.Platform.Configs["eval-config"] = c
		},
		"fake":         func(b *bundle) { b.Platform.FakeScripts = map[string][]provider.Script{"fake": {{}}} },
		"old_api":      func(b *bundle) { b.Platform.Listen = "127.0.0.1:8097" },
		"public_api":   func(b *bundle) { b.Platform.Listen = "0.0.0.0:8098" },
		"tcp_runner":   func(b *bundle) { b.Platform.Runner.TCPAddress = "127.0.0.1:9000" },
		"new_key":      func(b *bundle) { b.Platform.SigningKeyFile += ".new" },
		"test_backend": func(b *bundle) { b.Runner.AllowTestBackend = true },
		"no_pool":      func(b *bundle) { b.Runner.VolumeSlots = nil },
		"profile_uid": func(b *bundle) {
			p := b.Runner.Profiles["evalv2_"+cases[0]]
			p.User = "0:0"
			b.Runner.Profiles[p.ID] = p
		},
	}
	for name, mutate := range changes {
		t.Run(name, func(t *testing.T) {
			b := fixtureBundle(t)
			mutate(&b)
			if validatePolicy(b) == nil {
				t.Fatal("policy expansion accepted")
			}
		})
	}
}
func TestAuthorityPreservedAndOutputExclusive(t *testing.T) {
	b := fixtureBundle(t)
	if e := authorityEqual(b.Runner, b.Runner, b.Scope); e != nil {
		t.Fatal(e)
	}
	for _, field := range []string{"journal", "key", "root", "artifact", "slot"} {
		t.Run(field, func(t *testing.T) {
			b := fixtureBundle(t)
			copy := b.Runner
			switch field {
			case "journal":
				copy.JournalPath += ".new"
			case "key":
				copy.SigningKeyFile += ".new"
			case "root":
				copy.RootDir += ".new"
			case "artifact":
				copy.ArtifactRoot += ".new"
			case "slot":
				copy.VolumeSlots = append([]sandbox.VolumeSpec{}, copy.VolumeSlots...)
				copy.VolumeSlots[0].ID = "other"
			}
			if authorityEqual(b.Runner, copy, b.Scope) == nil {
				t.Fatal("authority mutation accepted")
			}
		})
	}
	persistFixture(t, b)
	if e := os.Mkdir(b.Out, 0700); e != nil {
		t.Fatal(e)
	}
	if _, e := loadBundle(b.Dir, b.Out); e == nil {
		t.Fatal("existing output accepted")
	}
	p := filepath.Join(t.TempDir(), "secret.env")
	if e := writePrivate(p, []byte("original")); e != nil {
		t.Fatal(e)
	}
	if e := writePrivate(p, []byte("replacement")); e == nil {
		t.Fatal("credential overwritten")
	}
	raw, _ := os.ReadFile(p)
	st, _ := os.Stat(p)
	if string(raw) != "original" || st.Mode().Perm() != 0600 {
		t.Fatal("private file semantics changed")
	}
}
func TestCandidateHashAndPathBinding(t *testing.T) {
	for _, which := range []string{"tamper", "symlink"} {
		t.Run(which, func(t *testing.T) {
			b := fixtureBundle(t)
			persistFixture(t, b)
			p := filepath.Join(b.Dir, "platform.json")
			if which == "tamper" {
				raw, _ := os.ReadFile(p)
				if e := os.WriteFile(p, append(raw, ' '), 0600); e != nil {
					t.Fatal(e)
				}
			} else {
				target := p + ".original"
				if e := os.Rename(p, target); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(target, p); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := loadBundle(b.Dir, b.Out); e == nil {
				t.Fatal("unbound candidate accepted")
			}
		})
	}
}
func TestPrivateIdentifiersAndPrivilegeOracle(t *testing.T) {
	schema, roles, e := names(strings.Repeat("a", 32))
	if e != nil || !validNames(schema, roles) {
		t.Fatal(e)
	}
	for _, suffix := range []string{"public", strings.Repeat("A", 32), strings.Repeat("a", 31), "a';drop schema public;--"} {
		if _, _, e := names(suffix); e == nil {
			t.Fatal("invalid generated name accepted")
		}
	}
	for _, kind := range []string{"api", "worker", "audit"} {
		for _, table := range []string{"runs", "api_tokens", "memberships", "tenants", "goose_db_version"} {
			read, write := expectedPrivileges(kind, table)
			p := tablePrivileges{Select: read, Insert: write, Update: write, Delete: write}
			if !validTablePrivileges(kind, table, p) {
				t.Fatal("valid privileges rejected")
			}
			for _, change := range []func(*tablePrivileges){func(p *tablePrivileges) { p.Owner = true }, func(p *tablePrivileges) { p.Extra = true }, func(p *tablePrivileges) { p.Select = !p.Select }, func(p *tablePrivileges) { p.Insert = !p.Insert }, func(p *tablePrivileges) { p.Delete = !p.Delete }} {
				bad := p
				change(&bad)
				if validTablePrivileges(kind, table, bad) {
					t.Fatal("unsafe/missing grant accepted")
				}
			}
		}
	}
	if got := quote("abc'$(never)&x"); got != "'abc'\"'\"'$(never)&x'" {
		t.Fatal("literal env quoting changed")
	}
}
func TestDescriptorsHaveNoCredentialFields(t *testing.T) {
	r := provisionReport{SchemaVersion: 1, Purpose: purpose, Phase: "ready", Roles: map[string]string{"api": "role"}, Hashes: map[string]string{"platform.json": strings.Repeat("a", 64)}}
	raw, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	for _, secret := range []string{"password", "worker_dsn", "FORGE_TOKEN", "token_hash", "FORGE_DATABASE_URL"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("credential field in public descriptor")
		}
	}
	var back provisionReport
	if e = json.Unmarshal(raw, &back); e != nil {
		t.Fatal("descriptor decode failed")
	}
	encoded, e := json.Marshal(back)
	if e != nil || !bytes.Equal(raw, encoded) {
		t.Fatal("descriptor JSON roundtrip changed")
	}
	if v, e := latestMigration(); e != nil || v < 10 {
		t.Fatal("membership migration missing", v, e)
	}
}

func TestMinimalAuditDSN(t *testing.T) {
	admin, e := adminURL("postgres://operator:private@127.0.0.1:32773/forge?sslmode=disable&connect_timeout=2")
	if e != nil {
		t.Fatal(e)
	}
	actual, e := url.Parse(auditURL(admin, "eval_role_audit", "generated"))
	if e != nil {
		t.Fatal(e)
	}
	if actual.Host != "127.0.0.1:32773" || actual.Path != "/forge" || actual.User.Username() != "eval_role_audit" || actual.RawQuery != "sslmode=disable" {
		t.Fatal("audit DSN contains unexpected routing/options")
	}
	if actual.Query().Get("search_path") != "" || actual.Query().Get("options") != "" || actual.Query().Get("application_name") != "" {
		t.Fatal("collector cannot accept route overrides")
	}
}

func TestPlainCollectorClientEnvironment(t *testing.T) {
	token := "frg_" + strings.Repeat("A", 52)
	raw, err := clientEnvironment("http://127.0.0.1:18098", "eval_fixture", token)
	if err != nil {
		t.Fatal(err)
	}
	want := "FORGE_API_URL=http://127.0.0.1:18098\nFORGE_TENANT=eval_fixture\nFORGE_TOKEN=" + token + "\n"
	if string(raw) != want {
		t.Fatal("collector requires exact plain assignments, without shell quotes")
	}
	// Independently parse the literal format consumed by the collector. Values
	// must not contain quoting, command substitution or additional assignments.
	entries := map[string]string{}
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || entries[key] != "" || strings.ContainsAny(value, "'\"`$;&\\\r\n") {
			t.Fatal("unsafe or duplicate plain client assignment")
		}
		entries[key] = value
	}
	if len(entries) != 3 || entries["FORGE_TENANT"] != "eval_fixture" || entries["FORGE_TOKEN"] != token {
		t.Fatal("literal collector identity differs")
	}
	for _, tc := range []struct{ name, api, tenant, token string }{
		{"quoted origin", "'http://127.0.0.1:18098'", "eval_fixture", token},
		{"origin path", "http://127.0.0.1:18098/", "eval_fixture", token},
		{"origin query", "http://127.0.0.1:18098?", "eval_fixture", token},
		{"origin userinfo", "http://user@127.0.0.1:18098", "eval_fixture", token},
		{"origin alias", "http://localhost:18098", "eval_fixture", token},
		{"origin port alias", "http://127.0.0.1:018098", "eval_fixture", token},
		{"original stack port", "http://127.0.0.1:8097", "eval_fixture", token},
		{"quoted tenant", "http://127.0.0.1:18098", "'eval_fixture'", token},
		{"tenant newline", "http://127.0.0.1:18098", "eval_fixture\nEVIL=1", token},
		{"tenant shell", "http://127.0.0.1:18098", "$(never)", token},
		{"quoted token", "http://127.0.0.1:18098", "eval_fixture", "'" + token + "'"},
		{"token newline", "http://127.0.0.1:18098", "eval_fixture", token + "\n"},
		{"token shell", "http://127.0.0.1:18098", "eval_fixture", token + ";never"},
		{"token empty", "http://127.0.0.1:18098", "eval_fixture", ""},
		{"token short", "http://127.0.0.1:18098", "eval_fixture", "frg_A"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := clientEnvironment(tc.api, domain.ID(tc.tenant), tc.token)
			if err == nil || len(raw) != 0 {
				t.Fatal("unsafe client assignment accepted")
			}
			if strings.Contains(err.Error(), tc.token) && tc.token != "" {
				t.Fatal("token echoed in validation error")
			}
		})
	}
}
