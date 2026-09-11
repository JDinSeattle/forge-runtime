package provider

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func FuzzToolArgumentDecoderIsUnambiguousAndBounded(f *testing.F) {
	for _, seed := range []string{`{}`, `{"path":"app.py","offset":9007199254740993}`, `{"a":1,"a":2}`, `{"a":{"a":2}}`, `{"a":[1,{"b":true}]}`, `{"x":`, `{} {}`, `null`, `[]`, `{"x":1e9999}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		value, err := decodeObject(raw, 8)
		if err != nil {
			return
		}
		if !json.Valid(raw) {
			t.Fatal("decoder accepted invalid JSON")
		}
		if _, ok := value.(map[string]any); !ok {
			t.Fatal("tool arguments were not an object")
		}
		canonical, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		again, err := decodeObject(canonical, 8)
		if err != nil || !reflect.DeepEqual(value, again) {
			t.Fatalf("accepted arguments changed in JSON round trip: %v", err)
		}
		if _, err := decodeObject(append(append([]byte{}, canonical...), []byte(" {}")...), 8); err == nil {
			t.Fatal("trailing second value accepted")
		}
		duplicate := append([]byte(`{"duplicate":`), canonical...)
		duplicate = append(duplicate, []byte(`,"duplicate":null}`)...)
		if _, err := decodeObject(duplicate, 8); err == nil {
			t.Fatal("ambiguous duplicate field accepted")
		}
		deep := append(bytes.Repeat([]byte(`{"x":`), 10), canonical...)
		deep = append(deep, bytes.Repeat([]byte(`}`), 10)...)
		if _, err := decodeObject(deep, 8); err == nil {
			t.Fatal("nesting escaped the argument depth budget")
		}
	})
}
