package persistence

import (
	"errors"
	"strings"
	"testing"
	"unicode"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

func TestSnapshotHeaderUsesExactVersionWithoutInterpretingFutureFields(t *testing.T) {
	if flow.SnapshotSchemaVersion != 1 {
		t.Fatal("update and review supportedSnapshotSQL for the new reader version")
	}
	for _, raw := range []string{`{"schema_version":1}`, `{"\u0073chema_version": 1 }`, `{"schema_version":1,"future":{"large":9007199254740993,"html":"<>&"}}`} {
		if err := DecodeSnapshotHeader([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{}`, `null`, `[]`, `{"schema_version":null}`, `{"Schema_Version":1}`, `{"schema_version":1.0}`, `{"schema_version":1e0}`, `{"schema_version":"1"}`, `{"schema_version":2}`, `{"schema_version":4294967296}`, `{"schema_version":1,"schema_version":2}`, `{"schema_version":1} {}`, `{"schema_version":1} garbage`} {
		err := DecodeSnapshotHeader([]byte(raw))
		var mismatch *SnapshotCompatibilityError
		if !errors.Is(err, domain.ErrSnapshotMigration) || !errors.As(err, &mismatch) || len(mismatch.SHA256) != 64 || len(mismatch.ObservedSchema) > 64 {
			t.Fatalf("unsafe header accepted %s: %v", raw, err)
		}
	}
}

func TestSnapshotHeaderSQLUnicodeFoldRemainsComplete(t *testing.T) {
	for c := rune(128); c <= unicode.MaxRune; c++ {
		for _, ascii := range "schemavrin_o" {
			if strings.EqualFold(string(c), string(ascii)) && (c != '\u017f' || ascii != 's') {
				t.Fatalf("update supportedSnapshotSQL Unicode fold mapping: %U folds to %q", c, ascii)
			}
		}
	}
}

func TestSnapshotHeaderRejectsAmbiguousMembers(t *testing.T) {
	for _, raw := range []string{
		`{"schema_version":1,"SCHEMA_VERSION":2}`,
		`{"SCHEMA_VERSION":2,"schema_version":1}`,
		`{"schema_version":2,"schema_version":1}`,
		`{"schema_version":1,"schema_version":1}`,
		`{"schema_version":1,"ſchema_version":2}`,
		`{"schema_version":1,"\u017fchema_version":2}`,
	} {
		if err := DecodeSnapshotHeader([]byte(raw)); !errors.Is(err, domain.ErrSnapshotMigration) {
			t.Errorf("ambiguous version accepted %s: %v", raw, err)
		}
	}
}
