package provider

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
)

// DeepSeekBaseURL is deliberately independent of OPENAI_BASE_URL and credentials.
// The official Responses endpoint is stateless; it is not OpenAI's service.
const DeepSeekBaseURL = "https://api.deepseek.com/"

type DeepSeek struct{ *responsesAdapter }

func NewDeepSeek(registry Registry, limits Limits, apiKey string) (*DeepSeek, error) {
	return newDeepSeek(registry, limits, apiKey, &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	})
}

// The client seam is package-private for no-network HTTP fixtures. Production
// cannot configure arbitrary endpoints or inherit OpenAI custom auth headers.
func newDeepSeek(registry Registry, limits Limits, apiKey string, client *http.Client) (*DeepSeek, error) {
	if strings.TrimSpace(apiKey) == "" || strings.ContainsAny(apiKey, "\r\n") {
		return nil, &Error{Kind: ErrInvalidRequest, Detail: "DEEPSEEK_API_KEY required for configured provider"}
	}
	limits = limits.normalized()
	// NewResponseService, unlike NewClient, does not read OPENAI_* environment.
	service := responses.NewResponseService(option.WithBaseURL(DeepSeekBaseURL), option.WithAPIKey(apiKey), option.WithHTTPClient(client), option.WithMaxRetries(0), option.WithMiddleware(boundResponse(limits.MaxNativeBytes)))
	return &DeepSeek{&responsesAdapter{service: service, name: "deepseek", registry: cloneRegistry(registry), limits: limits}}, nil
}

type deepSeekStream struct {
	started bool
	last    int64
}

func (s *deepSeekStream) check(raw, kind string) error {
	var event struct {
		Type     string `json:"type"`
		Sequence *int64 `json:"sequence_number"`
		Response struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"response"`
		Item struct {
			Type string `json:"type"`
		} `json:"item"`
	}
	if _, err := decodeObject([]byte(raw), 64); err != nil {
		return &Error{Kind: ErrProtocol, Detail: "invalid DeepSeek stream JSON"}
	}
	if json.Unmarshal([]byte(raw), &event) != nil || event.Type != kind || event.Sequence == nil || *event.Sequence < 0 || (s.started && *event.Sequence <= s.last) {
		return &Error{Kind: ErrProtocol, Detail: "missing or non-increasing DeepSeek sequence"}
	}
	if !s.started && kind != "response.created" || s.started && kind == "response.created" {
		return &Error{Kind: ErrProtocol, Detail: "DeepSeek stream must begin with one response.created"}
	}
	if kind == "response.created" && (event.Response.ID == "" || event.Response.Status != "in_progress") {
		return &Error{Kind: ErrProtocol, Detail: "invalid DeepSeek created response"}
	}
	s.started, s.last = true, *event.Sequence
	switch kind {
	case "response.created", "response.in_progress", "response.output_text.delta", "response.output_text.done", "response.content_part.added", "response.content_part.done", "response.reasoning_text.delta", "response.reasoning_text.done", "response.function_call_arguments.delta", "response.function_call_arguments.done", "response.completed", "response.incomplete", "response.failed", "error":
	case "response.output_item.added", "response.output_item.done":
		if event.Item.Type != "function_call" && event.Item.Type != "message" && event.Item.Type != "reasoning" {
			return &Error{Kind: ErrUnsupported, Detail: "unsupported DeepSeek output item"}
		}
	default:
		return &Error{Kind: ErrUnsupported, Detail: "unsupported DeepSeek stream event"}
	}
	return nil
}

// The full completed response is the authority for a usable turn. Do not let
// streamed text, an ignored native tool, or an impossible cache counter silently
// disagree with the final object. Model version remains the server's claim.
func deepseekFinal(raw []byte, text string) error {
	var final struct {
		Model  string `json:"model"`
		Output []struct {
			Type    string          `json:"type"`
			Status  json.RawMessage `json:"status"`
			Role    string          `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	var wire openAIResponse
	if json.Unmarshal(raw, &final) != nil || json.Unmarshal(raw, &wire) != nil || final.Model == "" {
		return &Error{Kind: ErrProtocol, Detail: "missing DeepSeek response model"}
	}
	var complete strings.Builder
	for _, item := range final.Output {
		// Some input-compatible item variants omit this optional field. A
		// supplied status cannot contradict the enclosing completed response.
		if len(item.Status) > 0 {
			var status string
			if json.Unmarshal(item.Status, &status) != nil || status != "completed" {
				return &Error{Kind: ErrProtocol, Detail: "non-completed DeepSeek output item"}
			}
		}
		switch item.Type {
		case "message":
			if item.Role != "assistant" {
				return &Error{Kind: ErrProtocol, Detail: "invalid response message role"}
			}
			for _, c := range item.Content {
				if c.Type != "output_text" {
					return &Error{Kind: ErrUnsupported, Detail: "unsupported DeepSeek message content"}
				}
				complete.WriteString(c.Text)
			}
		case "function_call", "reasoning":
		default:
			return &Error{Kind: ErrUnsupported, Detail: "unsupported DeepSeek completed item"}
		}
	}
	if complete.String() != text {
		return &Error{Kind: ErrProtocol, Detail: "completed DeepSeek text differs from stream"}
	}
	if wire.Usage.Input != nil && wire.Usage.Details.Cache != nil && *wire.Usage.Details.Cache > *wire.Usage.Input {
		return &Error{Kind: ErrProtocol, Detail: "cached tokens exceed input tokens"}
	}
	return nil
}
