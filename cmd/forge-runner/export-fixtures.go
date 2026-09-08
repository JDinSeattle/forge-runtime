//go:build ignore

// Export trusted fixture registration data without reading live configuration,
// credentials, environment files, database state or running candidate code.
// Example: go run ./cmd/forge-runner/export-fixtures.go -repo "$PWD" -source go-ceil-div
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/JDinSeattle/forge-runtime/internal/localsetup"
)

func main() {
	repo := flag.String("repo", ".", "checkout with trusted source/expected fixtures")
	source := flag.String("source", "go-ceil-div", "one registered fixture to export")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected arguments")
		os.Exit(2)
	}
	root, err := filepath.Abs(*repo)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fixtures, err := localsetup.LoadFixtures(context.Background(), root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	selected, ok := fixtures.Sources[*source]
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown trusted fixture")
		os.Exit(2)
	}
	for id := range fixtures.Sources {
		if id != *source {
			delete(fixtures.Sources, id)
			delete(fixtures.Scripts, id)
		}
	}
	for id := range fixtures.Profiles {
		if id != selected.ProfileID {
			delete(fixtures.Profiles, id)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err = enc.Encode(fixtures); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
