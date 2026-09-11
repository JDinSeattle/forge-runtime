package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
)

func textBatchDatabase(t *testing.T) (*Driver, persistence.Run) {
	t.Helper()
	ctx := context.Background()
	s := testutil.Database(t)
	if err := s.BootstrapTenant(ctx, "tenant", "developer", "developer"); err != nil {
		t.Fatal(err)
	}
	p, err := s.CreateProject(ctx, "tenant", "text delivery fixture", "fixture", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterRunner(ctx, "runner", "recording-only-no-runner", 1); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.Submit(ctx, persistence.SubmitRequest{TenantID: "tenant", PrincipalID: "developer", ProjectID: p.ID, BaseCommit: strings.Repeat("a", 64), Task: "Text delivery control fixture; no external work", Config: persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 3, MaxToolCalls: 3, MaxRuntimeSeconds: 30}}, "batching")
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Claim(ctx, "batch-worker", 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return &Driver{Store: s}, r
}

func saveTextBatchEvidence(t *testing.T, report map[string]any) {
	t.Helper()
	base := os.Getenv("FORGE_TEXT_BATCH_EVIDENCE_DIR")
	if base == "" {
		return
	}
	report["test"] = t.Name()
	report["failed"] = t.Failed()
	report["captured_at"] = time.Now().UTC()
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Error(err)
		return
	}
	f, err := os.OpenFile(filepath.Join(base, strings.ReplaceAll(t.Name(), "/", "-")+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		t.Error(err)
	}
}

func textBatchEvents(t *testing.T, ctx context.Context, d *Driver, r persistence.Run) ([]persistence.Event, []persistence.Event, string) {
	t.Helper()
	events, err := d.Store.Events(ctx, r.TenantID, r.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var text []persistence.Event
	var joined strings.Builder
	for i, e := range events {
		if e.Seq != uint64(i+1) {
			t.Fatal("event sequence gap")
		}
		if e.Type != "text.delta" {
			continue
		}
		var payload textPayload
		encoded, encodingErr := json.Marshal(e.Payload)
		if encodingErr != nil || len(encoded) > 32<<10 || !utf8.Valid(e.Payload) || json.Unmarshal(e.Payload, &payload) != nil || !payload.Provisional || payload.Delta == "" || !utf8.ValidString(payload.Delta) {
			t.Fatalf("invalid text event %d", e.Seq)
		}
		joined.WriteString(payload.Delta)
		text = append(text, e)
	}
	return events, text, joined.String()
}

func TestTextBatchPostgresTimerAndSize(t *testing.T) {
	for _, kind := range []string{"timer", "size_and_encoding"} {
		t.Run(kind, func(t *testing.T) {
			d, r := textBatchDatabase(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			d.TextBatch = TextBatchConfig{Interval: 40 * time.Millisecond, MaxBytes: 16 << 10}
			input := "timer-only 中文🙂"
			if kind == "size_and_encoding" {
				d.TextBatch.Interval = time.Second
				input = strings.Repeat("a", (16<<10)-1) + "你👩🏽‍💻" + strings.Repeat("<>&\u2028\u2029\n\"\\", 4096)
			}
			report := map[string]any{"scope": "real private PG and production stream sink; synthetic attempt identity, no provider/runner/container", "run_id": r.ID, "config": d.TextBatch, "input": input}
			defer saveTextBatchEvidence(t, report)
			s := newSink(ctx, cancel, d, r, persistence.ModelAttempt{ID: "batch_attempt"})
			defer s.close()
			report["emit_started_at"] = time.Now().UTC()
			if err := s.emit(provider.ModelEvent{Type: provider.EventTextDelta, Delta: input}); err != nil {
				t.Fatal(err)
			}
			report["emit_returned_at"] = time.Now().UTC()
			var events, text []persistence.Event
			var joined string
			for {
				events, text, joined = textBatchEvents(t, ctx, d, r)
				if len(text) > 0 {
					break
				}
				if kind != "timer" {
					t.Fatal("size threshold did not synchronously persist text before close")
				}
				select {
				case <-ctx.Done():
					t.Fatal("timer failed to persist pending text")
				case <-time.After(5 * time.Millisecond):
				}
			}
			report["first_observed_at"] = time.Now().UTC()
			report["events_before_close"] = events
			if kind == "timer" && (len(text) != 1 || joined != input) {
				t.Fatal("timer output changed or duplicated")
			}
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			before, _, joined := textBatchEvents(t, ctx, d, r)
			if joined != input {
				t.Fatalf("PG text reconstruction changed: got %d want %d bytes", len(joined), len(input))
			}
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			after, text, _ := textBatchEvents(t, ctx, d, r)
			if len(after) != len(before) {
				t.Fatal("idempotent close added an event")
			}
			report["events_after_close"] = after
			report["text_event_count"] = len(text)
			report["decoded_bytes"] = len(joined)
			report["exact_reconstruction"] = true
		})
	}
}

func TestTextBatchPostgresControlBypassesPausedText(t *testing.T) {
	d, r := textBatchDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.TextBatch = TextBatchConfig{Interval: time.Second}
	s := newSink(ctx, cancel, d, r, persistence.ModelAttempt{ID: "batch_attempt"})
	defer s.close()
	report := map[string]any{"scope": "real Store message/cancel transactions with text sink mutex deliberately held; no HTTP/provider/runner/container", "run_id": r.ID}
	defer saveTextBatchEvidence(t, report)
	// Hold only the text sink, never a DB row/transaction. Controls must not
	// need this lock or force a text flush; this avoids a fragile timer race.
	s.mu.Lock()
	s.pending = "provisional text remains buffered while controls commit"
	func() {
		defer s.mu.Unlock()
		identity := persistence.Identity{TenantID: r.TenantID, PrincipalID: "developer", Role: "developer"}
		report["controls_started_at"] = time.Now().UTC()
		message, replay, err := d.Store.AddMessage(ctx, identity, r.ID, "User instruction independent of text batching", "batch-control")
		if err != nil || replay {
			t.Fatalf("message %v replay %t", err, replay)
		}
		cancelled, err := d.Store.Cancel(ctx, identity, r.ID)
		if err != nil || cancelled.State.Status != domain.StatusCancelRequested {
			t.Fatalf("cancel %s %v", cancelled.State.Status, err)
		}
		report["controls_returned_at"] = time.Now().UTC()
		events, text, _ := textBatchEvents(t, ctx, d, r)
		counts := map[string]int{}
		for _, event := range events {
			counts[event.Type]++
		}
		if len(text) != 0 || counts["run.message_added"] != 1 || counts["run.cancel_requested"] != 1 {
			t.Fatalf("control did not bypass buffered text: %v", counts)
		}
		report["message"] = message
		report["events_while_text_paused"] = events
	}()
	// Cancellation fences provisional text. The original consumer-error path
	// still cancels the model context; it does not pretend buffered text committed.
	if err := s.close(); !errors.Is(err, domain.ErrFenced) || ctx.Err() == nil {
		t.Fatalf("cancel did not fence pending text: %v / %v", err, ctx.Err())
	}
	report["pending_text_fenced"] = true
	report["consumer_cancelled"] = true
}

func TestTextBatchDriverPreservesCompleteModelArtifact(t *testing.T) {
	d, r, model, _ := repairSetup(t)
	d.LeaseDuration = 20 * time.Second
	d.TextBatch = TextBatchConfig{Interval: 20 * time.Millisecond, MaxBytes: 4096}
	input := strings.Repeat("边界🙂<>&\"\\\n", 2048)
	model.scripts[2].Chunks = []provider.Chunk{{Kind: "text", Delta: input[:len(input)/2]}, {Kind: "text", Delta: input[len(input)/2:]}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	report := map[string]any{"scope": "production Driver/FakeProvider/real PG/local artifacts and SQLite/file runner TestBackend; no real model or container", "run_id": r.ID, "config": d.TextBatch, "expected_text": input}
	defer saveTextBatchEvidence(t, report)
	claimed, err := d.Store.Claim(ctx, "batch-driver", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	got, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil || got.State.Status != domain.StatusCompleted {
		t.Fatalf("run: %s %v", got.State.Status, err)
	}
	attempt, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, 3)
	if err != nil || attempt.Status != "completed" || attempt.RawRef == "" {
		t.Fatalf("attempt: %+v %v", attempt, err)
	}
	var turn provider.ModelTurn
	if err := d.load(ctx, r, attempt.RawRef, &turn); err != nil {
		t.Fatal(err)
	}
	events, _, joined := textBatchEvents(t, ctx, d, r)
	if turn.Text != input || joined != input {
		t.Fatal("complete artifact and delivered text differ from exact model result")
	}
	hash := sha256.Sum256([]byte(turn.Text))
	report["model_attempt"] = attempt
	report["full_model_turn"] = turn
	report["full_text_sha256"] = hex.EncodeToString(hash[:])
	report["events"] = events
	report["exact_reconstruction"] = true
}
