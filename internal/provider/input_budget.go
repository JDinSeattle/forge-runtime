package provider

import "encoding/json"

// InputTokenUpperBound is the admission estimate for the text-only adapters.
// One unit per serialized UTF-8 byte deliberately overestimates byte-tokenized
// text (including escaped tool JSON and opaque native history). Framing adds
// 1,024 units plus 64 per message and 256 per tool. This is a conservative gate,
// not measured usage; invoices must still use provider-returned counters. New
// multimodal/tokenizer adapters must provide their own accounting policy.
func InputTokenUpperBound(r ModelRequest) (int64, error) {
	input := struct {
		Messages []Message    `json:"messages"`
		Tools    []Tool       `json:"tools"`
		Native   *NativeState `json:"native,omitempty"`
	}{r.Messages, r.Tools, r.NativeState}
	body, err := json.Marshal(input)
	if err != nil {
		return 0, err
	}
	return int64(len(body)) + 1024 + int64(len(r.Messages))*64 + int64(len(r.Tools))*256, nil
}
