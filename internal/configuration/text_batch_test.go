package configuration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

func TestLoadOperatorTextBatchPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		batch   application.TextBatchConfig
		want    application.TextBatchConfig
		invalid bool
	}{
		{name: "omitted_defaults", want: application.TextBatchConfig{Interval: 100 * time.Millisecond, MaxBytes: 16 << 10}},
		{name: "explicit", batch: application.TextBatchConfig{Interval: 25 * time.Millisecond, MaxBytes: 4096}, want: application.TextBatchConfig{Interval: 25 * time.Millisecond, MaxBytes: 4096}},
		{name: "interval_rejected", batch: application.TextBatchConfig{Interval: -1}, invalid: true},
		{name: "byte_ceiling_rejected", batch: application.TextBatchConfig{MaxBytes: 32 << 10}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{ArtifactRoot: "/fixture/artifacts", SigningKeyFile: "/fixture/key", WorkerID: "worker", WorkerSlots: 1, RunnerID: "runner", TextBatch: tc.batch,
				Sources:   map[string]Source{"fixture": {Path: "/fixture/source", Hash: strings.Repeat("a", 64), ProfileID: "python"}},
				Configs:   map[string]persistence.Config{"fixture": {Provider: "fake", Model: "fake", MaxModelRounds: 1, MaxToolCalls: 1, MaxRuntimeSeconds: 1}},
				Models:    map[string]application.ModelSpec{"fake/fake": {CredentialGroup: "fake", PriceVersion: "fixture", ContextTokens: 1024, MaxOutputTokens: 128, RequestTimeout: time.Second}},
				Providers: map[string]provider.Registry{"fake": {"fake": {ToolCalling: true, MaxOutputTokens: 128}}}}
			body, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "platform.json")
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			got, err := Load(path)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid policy loaded")
				}
				return
			}
			if err != nil || got.TextBatch != tc.want {
				t.Fatalf("loaded %+v: %v", got.TextBatch, err)
			}
		})
	}
}
