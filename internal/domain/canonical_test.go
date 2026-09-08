package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCanonicalJSONPreservesArgumentHashInputs(t *testing.T) {
	raw := []byte(`{"z":9007199254740993, "content":"x < y & z > w", "a":1}`)
	canonical, err := CanonicalJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(struct {
		Args json.RawMessage `json:"args"`
	}{canonical})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Args json.RawMessage `json:"args"`
	}
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded.Args) != string(canonical) || !strings.Contains(string(canonical), "9007199254740993") {
		t.Fatal("canonical bytes or large integer changed")
	}
	if ValidateCanonicalJSON(raw) == nil {
		t.Fatal("noncanonical raw accepted")
	}
	if ValidateCanonicalJSON(canonical) != nil {
		t.Fatal("canonical fixed point rejected")
	}
}
func TestDuplicateArgumentsCannotBeAdmitted(t *testing.T) {
	if ValidateCanonicalJSON([]byte(`{"a":1,"a":2}`)) == nil {
		t.Fatal("duplicate keys accepted")
	}
}
