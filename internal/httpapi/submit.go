package httpapi

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func (b *SubmitBody) UnmarshalJSON(raw []byte) error {
	// Missing parent means ordinary submission. An explicitly invalid parent
	// must not silently drop the relationship and create an unrelated run.
	type plain SubmitBody
	var value plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if value.Priority < -2 || value.Priority > 2 {
		return domain.ErrInvalid
	}
	for name, rawValue := range fields {
		if strings.EqualFold(name, "priority") && bytes.Equal(bytes.TrimSpace(rawValue), []byte("null")) {
			return domain.ErrInvalid
		}
		if strings.EqualFold(name, "parent_run_id") {
			if err := value.ParentRunID.Validate(); err != nil {
				return err
			}
		}
	}
	*b = SubmitBody(value)
	return nil
}
