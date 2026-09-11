package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"testing"

	api "github.com/JDinSeattle/forge-runtime/internal/httpcontract"
)

func FuzzEventCursorOnlyAcknowledgesPrintedContiguousFrames(f *testing.F) {
	const one = "id: 1\nevent: text.delta\ndata: {\"run_id\":\"fuzz_run\",\"seq\":1,\"type\":\"text.delta\",\"schema_version\":1,\"payload\":{},\"created_at\":\"2026-09-11T00:00:00Z\"}\n\n"
	f.Add([]byte(one), uint64(0))
	f.Add([]byte(one+one), uint64(0))
	f.Add([]byte(one[:len(one)-1]), uint64(0))
	f.Add([]byte(": heartbeat\n\n"), uint64(8))
	f.Add([]byte("id: -1\ndata: {}\n\n"), uint64(0))
	f.Add([]byte("id: 9223372036854775808\ndata: {}\n\n"), uint64(math.MaxInt64))
	f.Add([]byte("id: 2\ndata: {\"run_id\":\"other_run\",\"seq\":2}\n\n"), uint64(1))
	f.Add([]byte("id: 1\nid: 2\ndata: {}\n\n"), uint64(0))
	f.Fuzz(func(t *testing.T, raw []byte, initial uint64) {
		if len(raw) > 128<<10 {
			t.Skip()
		}
		initial &= math.MaxInt64
		after := initial
		var printed bytes.Buffer
		done, _ := consumeEvents(bytes.NewReader(raw), "fuzz_run", &after, &printed)
		decoder := json.NewDecoder(&printed)
		last := initial
		var lastType string
		for {
			var event api.Event
			err := decoder.Decode(&event)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("acknowledged output is not complete JSON: %v", err)
			}
			if event.RunId != "fuzz_run" || event.SchemaVersion != 1 || event.Seq != last+1 || event.Seq > math.MaxInt64 || event.Type == "" {
				t.Fatalf("printed event violates cursor or identity: %+v after %d", event, last)
			}
			last, lastType = event.Seq, event.Type
		}
		if after != last || after < initial || (done && lastType != "run.finished") {
			t.Fatalf("cursor acknowledged unprinted/noncontiguous input: initial=%d after=%d printed=%d done=%v", initial, after, last, done)
		}
	})
}
