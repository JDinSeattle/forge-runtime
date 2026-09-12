package provider

import (
    "context"
    "encoding/json"
    "io"
    "net/http"
    "strings"
    "testing"

    anthropicoption "github.com/anthropics/anthropic-sdk-go/option"
    openaioption "github.com/openai/openai-go/v3/option"
)

// Independent no-network overlay. Each request is served by an in-memory
// RoundTripper, including the public DeepSeek constructor's redirect boundary.
func TestIndependentDeepSeekPublicClientNeverFollowsRedirect(t *testing.T) {
    original := http.DefaultTransport
    t.Cleanup(func(){ http.DefaultTransport = original })
    calls := 0
    http.DefaultTransport = deepSeekTransport(func(r *http.Request)(*http.Response,error){
        calls++
        if r.URL.Host != "api.deepseek.com" || r.Header.Get("Authorization") != "Bearer independent-memory-sentinel" {
            t.Fatal("credential or endpoint escaped production boundary")
        }
        return &http.Response{StatusCode:307, Header:http.Header{"Location":[]string{"https://unrelated.invalid/stolen"}}, Body:io.NopCloser(strings.NewReader("")),Request:r},nil
    })
    p,err:=NewDeepSeek(testRegistry("deepseek-v4-flash"),Limits{},"independent-memory-sentinel")
    if err!=nil {t.Fatal(err)}
    _,err=p.Stream(context.Background(),request("deepseek-v4-flash"),nil)
    if err==nil || calls!=1 {t.Fatalf("redirect/hidden retry accepted: calls=%d err=%v",calls,err)}
}

func TestIndependentResponsesRefactorKeepsOldWireSemantics(t *testing.T) {
    for _,name:=range []string{"openai","anthropic"} {
        t.Run(name,func(t *testing.T){
            calls:=0
            rt:=deepSeekTransport(func(r *http.Request)(*http.Response,error){
                calls++
                var body map[string]any
                if err:=json.NewDecoder(r.Body).Decode(&body);err!=nil{t.Fatal(err)}
                if body["model"]!="test-model" {t.Fatal("requested model changed")}
                if _,ok:=body["reasoning"];ok{t.Fatal("DeepSeek reasoning contract leaked")}
                events:=openAIEvents()
                h:=http.Header{"Content-Type":[]string{"text/event-stream"},"X-Request-Id":[]string{"independent-openai"}}
                if name=="openai" {
                    includes,ok:=body["include"].([]any)
                    if !ok || len(includes)!=1 || includes[0]!="reasoning.encrypted_content" || body["store"]!=false || r.URL.Path!="/responses" {t.Fatal("OpenAI wire defaults changed")}
                } else {
                    events=anthropicEvents()
                    h.Set("Request-Id","independent-anthropic")
                    if r.URL.Path!="/v1/messages" {t.Fatal("Anthropic endpoint changed")}
                }
                return &http.Response{StatusCode:200,Header:h,Body:io.NopCloser(strings.NewReader(deepSeekBody(events))),Request:r},nil
            })
            var p Provider
            if name=="openai" {p=NewOpenAI(testRegistry("test-model"),Limits{},openaioption.WithAPIKey("independent-memory-sentinel"),openaioption.WithBaseURL("https://no-network.invalid/"),openaioption.WithHTTPClient(&http.Client{Transport:rt}))} else {p=NewAnthropic(testRegistry("test-model"),Limits{},anthropicoption.WithAPIKey("independent-memory-sentinel"),anthropicoption.WithBaseURL("https://no-network.invalid/"),anthropicoption.WithHTTPClient(&http.Client{Transport:rt}))}
            turn,err:=p.Stream(context.Background(),request("test-model"),nil)
            if err!=nil {t.Fatal(err)}
            if calls!=1 || len(turn.ToolCalls)!=2 || turn.Text!="Inspecting." || turn.NativeState.Provider!=name || turn.ProviderModel!="" || !turn.Usage.Final {t.Fatal("old semantic turn changed")}
        })
    }
}

func TestIndependentDeepSeekCannotReleaseUnfinishedArguments(t *testing.T){
    events:=deepSeekEvents()
    trimmed:=events[:0]
    for _,e:=range events {if e["type"]!="response.function_call_arguments.done" {trimmed=append(trimmed,e)}}
    p:=deepSeekFixture(t,Limits{},func(r *http.Request)(*http.Response,error){return deepSeekResponse(r,deepSeekBody(trimmed)),nil})
    turn,err:=p.Stream(context.Background(),request("deepseek-v4-flash"),nil)
    if err==nil || len(turn.ToolCalls)!=0 || turn.Usage.Final || turn.NativeState!=nil {t.Fatal("completed outer response released unfinished tool arguments")}
}

func TestIndependentDeepSeekContradictoryFinalToolStatus(t *testing.T){
    for _,status:=range []string{"incomplete","in_progress"} {t.Run(status,func(t *testing.T){
        events:=deepSeekEvents()
        final:=events[len(events)-1]["response"].(map[string]any)
        final["output"].([]any)[0].(map[string]any)["status"]=status
        p:=deepSeekFixture(t,Limits{},func(r *http.Request)(*http.Response,error){return deepSeekResponse(r,deepSeekBody(events)),nil})
        turn,err:=p.Stream(context.Background(),request("deepseek-v4-flash"),nil)
        if err==nil || len(turn.ToolCalls)!=0 || turn.Usage.Final || turn.NativeState!=nil {t.Fatal("contradictory incomplete final tool was accepted as executable")}
    })}
}
