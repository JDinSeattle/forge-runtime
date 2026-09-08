package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// CanonicalJSON defines this protocol's pinned Go JSON encoding: sorted object
// keys, preserved JSON numbers, compact whitespace and standard HTML escaping.
// Hash only its result. Persisted arguments must already be a fixed point of
// this encoding, so ordinary snapshot/journal marshaling cannot change bytes.
func CanonicalJSON(raw []byte) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > 256<<10 || !utf8.Valid(raw) {
		return nil, ErrInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("%w: invalid argument JSON", ErrInvalid)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, ErrInvalid
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, ErrInvalid
	}
	if !boundedJSON(value, 0) {
		return nil, ErrCapacity
	}
	result, err := json.Marshal(value)
	return json.RawMessage(result), err
}
func boundedJSON(value any, depth int) bool {
	if depth > 32 {
		return false
	}
	switch x := value.(type) {
	case map[string]any:
		for _, v := range x {
			if !boundedJSON(v, depth+1) {
				return false
			}
		}
	case []any:
		for _, v := range x {
			if !boundedJSON(v, depth+1) {
				return false
			}
		}
	}
	return true
}
func ValidateCanonicalJSON(raw []byte) error {
	canonical, err := CanonicalJSON(raw)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, raw) {
		return fmt.Errorf("%w: arguments must use protocol canonical JSON before hashing", ErrInvalid)
	}
	return nil
}
