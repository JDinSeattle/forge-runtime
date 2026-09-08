package application

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

// unifiedPatch uses whole-file hunks, prioritizing reproducible application over
// minimal display size. Hash checks prevent exporting corrupted/binary text.
func unifiedPatch(raw []byte) ([]byte, error) {
	var manifest struct {
		Files []runner.DiffEntry `json:"files"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	seen := map[string]bool{}
	for _, f := range manifest.Files {
		if f.Path == "" || path.IsAbs(f.Path) || path.Clean(f.Path) != f.Path || strings.HasPrefix(f.Path, "../") || strings.ContainsAny(f.Path, "\r\n\t\\\x00") || seen[f.Path] {
			return nil, domain.ErrInvalid
		}
		seen[f.Path] = true
		for _, value := range []struct{ text, hash string }{{f.Before, f.BeforeSHA256}, {f.After, f.AfterSHA256}} {
			if !utf8.ValidString(value.text) || strings.IndexByte(value.text, 0) >= 0 {
				return nil, fmt.Errorf("%w: binary patch export unsupported", domain.ErrInvalid)
			}
			if value.hash != "" {
				sum := sha256.Sum256([]byte(value.text))
				if hex.EncodeToString(sum[:]) != value.hash {
					return nil, domain.ErrUntrusted
				}
			}
		}
		oldMode, newMode := "100644", "100644"
		if f.BeforeExecutable {
			oldMode = "100755"
		}
		if f.AfterExecutable {
			newMode = "100755"
		}
		fmt.Fprintf(&out, "diff --git %q %q\n", "a/"+f.Path, "b/"+f.Path)
		oldName, newName := "a/"+f.Path, "b/"+f.Path
		if f.BeforeSHA256 == "" {
			fmt.Fprintf(&out, "new file mode %s\n", newMode)
			oldName = "/dev/null"
		} else if f.Deleted {
			fmt.Fprintf(&out, "deleted file mode %s\n", oldMode)
			newName = "/dev/null"
		} else if oldMode != newMode {
			fmt.Fprintf(&out, "old mode %s\nnew mode %s\n", oldMode, newMode)
		}
		if f.Before == f.After {
			continue
		}
		label := func(name string) string {
			if name == "/dev/null" {
				return name
			}
			return strconv.Quote(name)
		}
		fmt.Fprintf(&out, "--- %s\n+++ %s\n", label(oldName), label(newName))
		oldLines, newLines := patchLines(f.Before), patchLines(f.After)
		oldStart, newStart := 1, 1
		if len(oldLines) == 0 {
			oldStart = 0
		}
		if len(newLines) == 0 {
			newStart = 0
		}
		fmt.Fprintf(&out, "@@ -%d,%d +%d,%d @@\n", oldStart, len(oldLines), newStart, len(newLines))
		write := func(prefix string, lines []string, text string) {
			for _, line := range lines {
				out.WriteString(prefix)
				out.WriteString(line)
				out.WriteByte('\n')
			}
			if len(lines) > 0 && !strings.HasSuffix(text, "\n") {
				out.WriteString("\\ No newline at end of file\n")
			}
		}
		write("-", oldLines, f.Before)
		write("+", newLines, f.After)
		if out.Len() > 8<<20 {
			return nil, domain.ErrCapacity
		}
	}
	return out.Bytes(), nil
}
func patchLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}
