package configuration

import (
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

func TestDeepSeekRequiresOwnCredentialAndRegistry(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "in-memory-openai-sentinel")
	c := Config{Providers: map[string]provider.Registry{"deepseek": {"deepseek-v4-flash": {ToolCalling: true, MaxOutputTokens: 8192}}}}
	if _, err := c.BuildProviders(); err == nil {
		t.Fatal("DeepSeek borrowed OpenAI credential")
	}
	t.Setenv("DEEPSEEK_API_KEY", "in-memory-deepseek-sentinel")
	got, err := c.BuildProviders()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatal("extra provider alias")
	}
	if _, ok := got["deepseek"].(*provider.DeepSeek); !ok {
		t.Fatal("not a separate native provider")
	}
	c.Models = map[string]application.ModelSpec{"deepseek/deepseek-v4-flash": {CredentialGroup: "shared"}, "openai/fixture": {CredentialGroup: "shared"}}
	if _, err = c.BuildProviders(); err == nil {
		t.Fatal("cross-provider quota identity shared")
	}
	c.Models["deepseek/deepseek-v4-flash"] = application.ModelSpec{CredentialGroup: "deepseek-eval"}
	if _, err = c.BuildProviders(); err != nil {
		t.Fatal(err)
	}
}
