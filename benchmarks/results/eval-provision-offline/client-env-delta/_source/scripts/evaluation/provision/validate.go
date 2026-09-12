package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

const purpose = "deepseek-eval-v2-provision"
const credentialGroup = "deepseek-flash-evalv2"
const modelID = "deepseek-v4-flash"

var cases = []string{"py-utf8-chunks", "py-latest-records", "go-midpoint", "go-rune-rle"}
var hex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)
var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
var clientID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var clientToken = regexp.MustCompile(`^frg_[A-Z2-7]{52,128}$`)

// The collector reads client.env as three plain literal values, without shell
// parsing. Validate the complete alphabets before writing; DSN env files retain
// their separate shell-quoted contract.
func clientEnvironment(apiURL string, tenant domain.ID, token string) ([]byte, error) {
	u, err := url.Parse(apiURL)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Hostname() != "127.0.0.1" || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("invalid client environment API origin")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1024 || port > 65535 || port == 8097 || apiURL != "http://127.0.0.1:"+strconv.Itoa(port) {
		return nil, errors.New("invalid client environment API origin")
	}
	if !clientID.MatchString(string(tenant)) || !clientToken.MatchString(token) {
		return nil, errors.New("invalid client environment identity or token alphabet")
	}
	return []byte("FORGE_API_URL=" + apiURL + "\nFORGE_TENANT=" + string(tenant) + "\nFORGE_TOKEN=" + token + "\n"), nil
}

type registration struct {
	Provider          string                  `json:"provider"`
	Model             string                  `json:"model_id"`
	ConfigID          string                  `json:"config_id"`
	Total             domain.Money            `json:"total_budget_microusd"`
	Tasks             map[string]domain.Money `json:"task_budgets_microusd"`
	Rounds            int                     `json:"max_model_rounds"`
	Tools             int                     `json:"max_tool_calls"`
	Runtime           int                     `json:"max_runtime_seconds"`
	Capabilities      provider.Capabilities   `json:"capabilities"`
	Spec              application.ModelSpec   `json:"model_spec"`
	PriceSources      []string                `json:"price_sources"`
	CapabilitySources []string                `json:"capability_sources"`
}
type runnerConfig struct {
	Logs             sandbox.LogPolicy          `json:"logs,omitempty"`
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
type bundle struct {
	Dir, Out, Scope string
	Platform        configuration.Config
	Runner          runnerConfig
	Registration    registration
	Hashes          map[string]string
	BatchID         string
}

func regularRead(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("absolute canonical path required")
	}
	actual, e := filepath.EvalSymlinks(path)
	if e != nil || actual != path {
		return nil, errors.New("symlinked/missing input")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() > 2<<20 {
		return nil, errors.New("bounded regular JSON input required")
	}
	b, e := io.ReadAll(io.LimitReader(f, (2<<20)+1))
	if e != nil || len(b) > 2<<20 {
		return nil, errors.New("input exceeds bound")
	}
	return b, nil
}
func decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("one JSON object required")
	}
	return nil
}
func sum(b []byte) string { x := sha256.Sum256(b); return hex.EncodeToString(x[:]) }
func loadBundle(dir, out string) (bundle, error) {
	b := bundle{Dir: dir, Out: out, Hashes: map[string]string{}}
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || out != filepath.Join(filepath.Dir(dir), "private") || filepath.Base(dir) != "candidate" {
		return b, errors.New("candidate and new sibling private directory required")
	}
	if _, e := os.Lstat(out); !os.IsNotExist(e) {
		return b, errors.New("private output already exists or cannot be inspected")
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(out))
	if e != nil || parent != filepath.Dir(out) {
		return b, errors.New("canonical existing output parent required")
	}
	for _, v := range []struct {
		name string
		dest any
	}{{"platform.json", &b.Platform}, {"runner.json", &b.Runner}, {"registration.json", &b.Registration}} {
		raw, e := regularRead(filepath.Join(dir, v.name))
		if e != nil {
			return b, e
		}
		if e = decode(raw, v.dest); e != nil {
			return b, e
		}
		b.Hashes[v.name] = sum(raw)
	}
	// Load validates existing production Config policy without BuildProviders,
	// Signer, CheckSources, Engine.Open, or any credential acquisition.
	b.Platform, e = configuration.Load(filepath.Join(dir, "platform.json"))
	if e != nil {
		return b, e
	}
	var candidate struct {
		Only         bool              `json:"candidate_only"`
		Corpus       string            `json:"corpus_sha256"`
		Sources      map[string]string `json:"source_hashes"`
		Platform     string            `json:"platform_sha256"`
		Runner       string            `json:"runner_sha256"`
		Registration string            `json:"registration_sha256"`
	}
	raw, e := regularRead(filepath.Join(dir, "candidate.json"))
	if e != nil {
		return b, e
	}
	if e = decode(raw, &candidate); e != nil {
		return b, e
	}
	b.Hashes["candidate.json"] = sum(raw)
	if !candidate.Only || candidate.Platform != b.Hashes["platform.json"] || candidate.Runner != b.Hashes["runner.json"] || candidate.Registration != b.Hashes["registration.json"] || !hex64.MatchString(candidate.Corpus) || len(candidate.Sources) != 4 {
		return b, errors.New("prepared candidate hash binding differs")
	}
	if e = validatePolicy(b); e != nil {
		return b, e
	}
	b.Scope = filepath.Dir(filepath.Dir(b.Runner.JournalPath))
	if filepath.Base(b.Scope) != "lr20260912_a" || filepath.Base(filepath.Dir(b.Scope)) != "lifecycle-rehearsals" || filepath.Dir(filepath.Dir(out)) != filepath.Join(b.Scope, "evaluation") {
		return b, errors.New("independent lifecycle evaluation scope required")
	}
	var original runnerConfig
	originalPath := filepath.Join(b.Scope, "runtime", "runner.json")
	raw, e = regularRead(originalPath)
	if e != nil {
		return b, e
	}
	if e = decode(raw, &original); e != nil {
		return b, e
	}
	b.Hashes["authority_runner.json"] = sum(raw)
	if e = authorityEqual(original, b.Runner, b.Scope); e != nil {
		return b, e
	}
	for _, c := range cases {
		if candidate.Sources[c] != b.Platform.Sources["evalv2_"+c].Hash {
			return b, errors.New("candidate source hash differs")
		}
	}
	b.BatchID = "deepseek-flash-2usd-" + b.Hashes["registration.json"]
	return b, nil
}
func validatePolicy(b bundle) error {
	p, r, c := b.Platform, b.Registration, b.Runner
	if r.Provider != "deepseek" || r.Model != modelID || domain.ID(r.ConfigID).Validate() != nil || r.Total != 2000000 || len(r.Tasks) != 4 || r.Rounds != 20 || r.Tools != 64 || r.Runtime != 600 {
		return errors.New("fixed DeepSeek four-task two-dollar registration required")
	}
	for _, name := range cases {
		if r.Tasks[name] != 500000 {
			return errors.New("every fixed task must retain its 50-cent ceiling")
		}
	}
	expectedCaps := provider.Capabilities{ToolCalling: true, ContextWindow: 1000000, MaxOutputTokens: 384000}
	m := r.Spec
	if r.Capabilities != expectedCaps || m.CredentialGroup != credentialGroup || m.PriceVersion != "deepseek-flash-peak-ceiling-20260912" || m.ExactPricing || m.InputPrice != 300000 || m.OutputPrice != 1200000 || m.CacheReadPrice == nil || *m.CacheReadPrice != 6000 || m.CacheWrite5mPrice != nil || m.CacheWrite1hPrice != nil || m.ContextTokens != 32768 || m.MaxOutputTokens != 8192 || m.RequestTimeout != time.Minute {
		return errors.New("reviewed peak quote, native capability and no-thinking contract required")
	}
	for _, sources := range [][]string{r.PriceSources, r.CapabilitySources} {
		if len(sources) == 0 {
			return errors.New("registration source URLs required")
		}
		for _, raw := range sources {
			u, e := url.Parse(raw)
			if e != nil || u.Scheme != "https" || u.Host != "api-docs.deepseek.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return errors.New("credential-free official source URL required")
			}
		}
	}
	if len(p.Configs) != 1 || len(p.Models) != 1 || len(p.Providers) != 1 || len(p.Providers["deepseek"]) != 1 || len(p.Sources) != 4 || len(p.FakeScripts) != 0 || p.WorkerSlots != 1 || p.RunnerID != "application-fault-runner" {
		return errors.New("single worker/native model and four sources required")
	}
	run, ok := p.Configs[r.ConfigID]
	if !ok || run.Provider != "deepseek" || run.Model != modelID || run.Fallback != nil || run.MaxCost != 500000 || run.MaxModelRounds != 20 || run.MaxToolCalls != 64 || run.MaxRuntimeSeconds != 600 || !reflect.DeepEqual(p.Models["deepseek/"+modelID], r.Spec) || p.Providers["deepseek"][modelID] != r.Capabilities {
		return errors.New("platform registration changed or fallback enabled")
	}
	host, port, e := net.SplitHostPort(p.Listen)
	n, ne := strconv.Atoi(port)
	if e != nil || ne != nil || host != "127.0.0.1" || n < 1024 || n > 65535 || n == 8097 {
		return errors.New("fresh explicit numeric-loopback API listener required")
	}
	if p.Runner.UnixSocket != c.Server.UnixSocket || p.Runner.UnixSocket != "/tmp/forge-lifecycle-lr20260912_a/runner.sock" || p.Runner.TCPAddress != "" || c.Server.TCPAddress != "" || p.ArtifactRoot != c.ArtifactRoot || p.SigningKeyFile != c.SigningKeyFile || c.AllowTestBackend || len(c.VolumeSlots) != 4 || len(c.Sources) != 4 || len(c.Profiles) != 4 {
		return errors.New("existing four-slot runner authority required")
	}
	defaults, _ := (sandbox.LogPolicy{}).Normalize()
	logs, e := c.Logs.Normalize()
	if e != nil || logs != defaults {
		return errors.New("default strict logs required")
	}
	for _, name := range cases {
		id := "evalv2_" + name
		s, ok := p.Sources[id]
		profile, exists := c.Profiles[id]
		if !ok || !exists || !hex64.MatchString(s.Hash) || s.ProfileID != id || !s.HasTarget || s.Path != c.Sources[id] || !filepath.IsAbs(s.Path) || !strings.HasSuffix(s.Path, "/scripts/evaluation/corpus/"+name+"/source") || profile.ID != id || profile.MemoryBytes != 256<<20 || profile.PIDs != 64 || profile.CPUs != 1 || profile.User != "1000:1000" || profile.WorkspaceQuotaBytes != 256<<20 || !regexp.MustCompile(`^[a-z0-9][a-z0-9._/:+-]*@sha256:[0-9a-f]{64}$`).MatchString(profile.Image) {
			return errors.New("bounded fixed corpus/source/profile mismatch")
		}
	}
	return nil
}
func authorityEqual(original, candidate runnerConfig, scope string) error {
	original.Sources, original.Profiles = nil, nil
	candidate.Sources, candidate.Profiles = nil, nil
	if !reflect.DeepEqual(original, candidate) || candidate.RootDir != filepath.Join(scope, "runtime", "engine") || candidate.JournalPath != filepath.Join(scope, "runtime", "journal.sqlite") || candidate.ArtifactRoot != filepath.Join(scope, "runtime", "artifacts") || candidate.SigningKeyFile != filepath.Join(scope, "runtime", "runner.key") {
		return errors.New("lifecycle runner authority must be preserved byte-for-value")
	}
	return nil
}
func adminURL(raw string) (*url.URL, error) {
	u, e := url.Parse(raw)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host != "127.0.0.1:32773" || u.Path != "/forge" || u.RawPath != "" || u.User == nil || u.User.Username() == "" || u.Fragment != "" {
		return nil, errors.New("explicit loopback admin URL required")
	}
	pass, ok := u.User.Password()
	if !ok || pass == "" {
		return nil, errors.New("explicit private admin password required")
	}
	q, e := url.ParseQuery(u.RawQuery)
	if e != nil {
		return nil, errors.New("invalid admin options")
	}
	for k, v := range q {
		if len(v) != 1 || (k != "sslmode" && k != "connect_timeout") {
			return nil, errors.New("admin route/search_path overrides forbidden")
		}
	}
	if q.Get("sslmode") != "disable" {
		return nil, errors.New("explicit loopback sslmode=disable required")
	}
	q.Set("connect_timeout", "5")
	q.Set("options", "")
	q.Set("application_name", "forge-eval-provision")
	u.RawQuery = q.Encode()
	return u, nil
}
func scopedURL(admin *url.URL, schema, role, password string) string {
	u := *admin
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	if role != "" {
		u.User = url.UserPassword(role, password)
	}
	return u.String()
}
func validNames(schema string, roles map[string]string) bool {
	if !strings.HasPrefix(schema, "eval_ds_") || !hex32.MatchString(strings.TrimPrefix(schema, "eval_ds_")) || len(roles) != 3 {
		return false
	}
	for _, kind := range []string{"api", "worker", "audit"} {
		if roles[kind] != schema+"_"+kind {
			return false
		}
	}
	return true
}
func names(suffix string) (string, map[string]string, error) {
	schema := "eval_ds_" + suffix
	roles := map[string]string{"api": schema + "_api", "worker": schema + "_worker", "audit": schema + "_audit"}
	if !validNames(schema, roles) {
		return "", nil, fmt.Errorf("invalid private identifiers")
	}
	return schema, roles, nil
}

// The collector supplies a fixed pg_catalog search_path and fully qualifies the
// descriptor schema. Export no per-connection routing/options override here.
func auditURL(admin *url.URL, role, password string) string {
	u := *admin
	u.User = url.UserPassword(role, password)
	u.RawQuery = url.Values{"sslmode": []string{"disable"}}.Encode()
	return u.String()
}
