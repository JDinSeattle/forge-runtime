// Read-only bridge to the exact production workspace manifest algorithm.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

func main() {
	// Unit-test bridge through the actual production JSON field types/tags.
	// It performs no database, network, worker, runner or model operations.
	if len(os.Args) == 2 && os.Args[1] == "--model-spec-json" {
		var specs []application.ModelSpec
		decoder := json.NewDecoder(os.Stdin)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&specs); err != nil {
			panic(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(specs); err != nil {
			panic(err)
		}
		return
	}
	if len(os.Args) != 2 || !filepath.IsAbs(os.Args[1]) {
		panic("absolute corpus directory required")
	}
	result := map[string]string{}
	for _, id := range []string{"py-utf8-chunks", "py-latest-records", "go-midpoint", "go-rune-rle"} {
		hash, err := runner.ComputeSourceHash(context.Background(), filepath.Join(os.Args[1], id, "source"), 0, 0)
		if err != nil {
			panic(err)
		}
		result[id] = hash
	}
	raw, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(raw))
}
