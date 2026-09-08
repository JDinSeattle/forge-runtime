package runner

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

type fileRecord struct {
	SHA256     string `json:"sha256"`
	Content    []byte `json:"content"`
	Executable bool   `json:"executable"`
}
type tree map[string]fileRecord

// ComputeSourceHash uses exactly the bounded, traversal-resistant manifest
// algorithm used during import, including executable bits. The control plane
// may pin this value when registering an allowed local source snapshot.
func ComputeSourceHash(ctx context.Context, directory string, maxFileBytes, maxWorkspaceBytes int64) (string, error) {
	if maxFileBytes <= 0 {
		maxFileBytes = 1 << 20
	}
	if maxWorkspaceBytes <= 0 {
		maxWorkspaceBytes = 32 << 20
	}
	e := Engine{config: Config{MaxFileBytes: maxFileBytes, MaxWorkspaceBytes: maxWorkspaceBytes}}
	files, err := e.readTree(ctx, directory)
	if err != nil {
		return "", err
	}
	return treeHash(files), nil
}

type FileEdit struct {
	Path           string  `json:"path"`
	ExpectedSHA256 string  `json:"expected_sha256"`
	Content        *string `json:"content,omitempty"`
	Delete         bool    `json:"delete,omitempty"`
	Executable     *bool   `json:"executable,omitempty"`
}
type PatchArgs struct {
	Files []FileEdit `json:"files"`
}
type ReadArgs struct {
	Path      string `json:"path"`
	StartLine int    `json:"start_line,omitempty"`
	EndLine   int    `json:"end_line,omitempty"`
}
type SearchArgs struct {
	Query      string `json:"query"`
	MaxResults int    `json:"max_results,omitempty"`
}
type CommandArgs struct {
	Command []string `json:"command"`
}
type VerifyArgs struct {
	Target bool `json:"target,omitempty"`
}
type DiffEntry struct {
	Path             string `json:"path"`
	BeforeSHA256     string `json:"before_sha256,omitempty"`
	AfterSHA256      string `json:"after_sha256,omitempty"`
	Before           string `json:"before,omitempty"`
	After            string `json:"after,omitempty"`
	Deleted          bool   `json:"deleted,omitempty"`
	BeforeExecutable bool   `json:"before_executable"`
	AfterExecutable  bool   `json:"after_executable"`
}

func decodeArgs(raw []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return fmt.Errorf("%w: tool arguments: %v", domain.ErrInvalid, err)
	}
	return nil
}
func safePath(name string) error {
	if name == "" || name == "." || path.IsAbs(name) || path.Clean(name) != name || strings.Contains(name, "\\") || strings.IndexByte(name, 0) >= 0 {
		return domain.ErrInvalid
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." || part == ".git" || strings.HasPrefix(part, ".forge-") {
			return domain.ErrInvalid
		}
	}
	return nil
}
func hashBytes(value []byte) string { h := sha256.Sum256(value); return hex.EncodeToString(h[:]) }
func treeHash(files tree) string {
	names := sortedPaths(files)
	h := sha256.New()
	for _, name := range names {
		_, _ = fmt.Fprintf(h, "%d:%s:%s:%t\n", len(name), name, files[name].SHA256, files[name].Executable)
	}
	return hex.EncodeToString(h.Sum(nil))
}
func sortedPaths(files tree) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (e *Engine) readTree(ctx context.Context, directory string) (tree, error) {
	r, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	files := tree{}
	var total int64
	err = fs.WalkDir(r.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".venv", "__pycache__", ".pytest_cache":
				return fs.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(entry.Name(), ".forge-") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: snapshot contains nonregular file %q", domain.ErrInvalid, name)
		}
		if info.Size() > e.config.MaxFileBytes || total+info.Size() > e.config.MaxWorkspaceBytes || len(files) >= 10000 {
			return domain.ErrCapacity
		}
		f, err := r.Open(name)
		if err != nil {
			return err
		}
		data, readErr := readBounded(ctx, f, e.config.MaxFileBytes)
		_ = f.Close()
		if readErr != nil {
			return readErr
		}
		total += int64(len(data))
		if total > e.config.MaxWorkspaceBytes {
			return domain.ErrCapacity
		}
		files[name] = fileRecord{SHA256: hashBytes(data), Content: data, Executable: info.Mode().Perm()&0111 != 0}
		return nil
	})
	return files, err
}

func (e *Engine) copyTree(ctx context.Context, files tree, directory string) error {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	r, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, name := range sortedPaths(files) {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = safePath(name); err != nil {
			return err
		}
		if err = r.MkdirAll(path.Dir(name), 0755); err != nil {
			return err
		}
		mode := os.FileMode(0644)
		if files[name].Executable {
			mode = 0755
		}
		f, err := r.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			return err
		}
		_, writeErr := f.Write(files[name].Content)
		if writeErr == nil {
			writeErr = f.Sync()
		}
		closeErr := f.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	f, err := r.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// Rootless container UIDs differ from the control process's host UID. The
// mounted workspace is writable by that nonroot UID; its control-owned parent
// directory remains 0700 and is never mounted into a task container.
func (e *Engine) makeWorkspaceWritable(id domain.ID) error {
	r, err := os.OpenRoot(e.workspacePath(id))
	if err != nil {
		return err
	}
	defer r.Close()
	return fs.WalkDir(r.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		f, err := r.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		if entry.IsDir() {
			return f.Chmod(0777)
		}
		info, err := f.Stat()
		if err != nil {
			return err
		}
		mode := os.FileMode(0666)
		if info.Mode().Perm()&0111 != 0 {
			mode = 0777
		}
		return f.Chmod(mode)
	})
}

func (e *Engine) patchPlan(raw []byte, before tree) (PatchArgs, tree, error) {
	var args PatchArgs
	if err := decodeArgs(raw, &args); err != nil {
		return args, nil, err
	}
	if len(args.Files) == 0 || len(args.Files) > 64 {
		return args, nil, domain.ErrInvalid
	}
	after := tree{}
	for name, f := range before {
		after[name] = f
	}
	seen := map[string]bool{}
	for _, edit := range args.Files {
		if safePath(edit.Path) != nil || seen[edit.Path] || edit.Delete == (edit.Content != nil) {
			return args, nil, domain.ErrInvalid
		}
		seen[edit.Path] = true
		old, exists := before[edit.Path]
		expected := "absent"
		if exists {
			expected = old.SHA256
		}
		if expected != edit.ExpectedSHA256 {
			return args, nil, domain.ErrConflict
		}
		if edit.Delete {
			if !exists {
				return args, nil, domain.ErrConflict
			}
			delete(after, edit.Path)
		} else {
			if int64(len(*edit.Content)) > e.config.MaxFileBytes {
				return args, nil, domain.ErrCapacity
			}
			data := []byte(*edit.Content)
			executable := old.Executable
			if edit.Executable != nil {
				executable = *edit.Executable
			}
			after[edit.Path] = fileRecord{Content: data, SHA256: hashBytes(data), Executable: executable}
		}
	}
	var total int64
	for _, f := range after {
		total += int64(len(f.Content))
	}
	if total > e.config.MaxWorkspaceBytes {
		return args, nil, domain.ErrCapacity
	}
	return args, after, nil
}

func (e *Engine) applyPatch(ctx context.Context, o Operation, before tree) (json.RawMessage, error) {
	args, after, err := e.patchPlan(o.Request.Args, before)
	if err != nil {
		return nil, err
	}
	r, err := os.OpenRoot(e.workspacePath(o.Request.WorkspaceID))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	for i, edit := range args.Files {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if edit.Delete {
			if err = r.Remove(edit.Path); err != nil {
				return nil, err
			}
		} else {
			if err = r.MkdirAll(path.Dir(edit.Path), 0777); err != nil {
				return nil, err
			}
			tmp := path.Join(path.Dir(edit.Path), fmt.Sprintf(".forge-%s-%d", o.Request.OperationID, i))
			f, err := r.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return nil, err
			}
			_, writeErr := f.WriteString(*edit.Content)
			if writeErr == nil {
				mode := os.FileMode(0666)
				if after[edit.Path].Executable {
					mode = 0777
				}
				writeErr = f.Chmod(mode)
			}
			if writeErr == nil {
				writeErr = f.Sync()
			}
			closeErr := f.Close()
			if writeErr != nil {
				return nil, writeErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if err = r.Rename(tmp, edit.Path); err != nil {
				return nil, err
			}
		}
		parent, err := r.Open(path.Dir(edit.Path))
		if err != nil {
			return nil, err
		}
		syncErr := parent.Sync()
		_ = parent.Close()
		if syncErr != nil {
			return nil, syncErr
		}
		if err = e.fault("after_patch_file"); err != nil {
			return nil, err
		}
	}
	return json.Marshal(map[string]any{"changed_files": len(args.Files)})
}

func (e *Engine) fileTool(ctx context.Context, o Operation, before tree) (json.RawMessage, error) {
	switch o.Request.Kind {
	case "list_files":
		return json.Marshal(map[string]any{"files": sortedPaths(before), "workspace_hash": treeHash(before)})
	case "read_file":
		var args ReadArgs
		if err := decodeArgs(o.Request.Args, &args); err != nil {
			return nil, err
		}
		if safePath(args.Path) != nil {
			return nil, domain.ErrInvalid
		}
		f, ok := before[args.Path]
		if !ok {
			return nil, domain.ErrNotFound
		}
		lines := strings.Split(string(f.Content), "\n")
		start, end := args.StartLine, args.EndLine
		if start == 0 {
			start = 1
		}
		if end == 0 {
			end = len(lines)
		}
		if start < 1 || end < start || start > len(lines) || end > len(lines) {
			return nil, domain.ErrInvalid
		}
		return json.Marshal(map[string]any{"path": args.Path, "sha256": f.SHA256, "start_line": start, "content": strings.Join(lines[start-1:end], "\n")})
	case "search_code":
		var args SearchArgs
		if err := decodeArgs(o.Request.Args, &args); err != nil {
			return nil, err
		}
		if args.Query == "" || len(args.Query) > 1024 {
			return nil, domain.ErrInvalid
		}
		limit := args.MaxResults
		if limit <= 0 || limit > 200 {
			limit = 200
		}
		matches := []map[string]any{}
		for _, name := range sortedPaths(before) {
			scan := bufio.NewScanner(bytes.NewReader(before[name].Content))
			scan.Buffer(make([]byte, 4096), int(e.config.MaxFileBytes)+1)
			line := 0
			for scan.Scan() {
				line++
				if strings.Contains(scan.Text(), args.Query) {
					text := scan.Text()
					if len(text) > 4096 {
						text = text[:4096]
					}
					matches = append(matches, map[string]any{"path": name, "line": line, "text": text})
					if len(matches) == limit {
						return json.Marshal(map[string]any{"matches": matches, "truncated": true})
					}
				}
			}
		}
		return json.Marshal(map[string]any{"matches": matches, "truncated": false})
	case "apply_patch":
		return e.applyPatch(ctx, o, before)
	case "get_diff":
		base, err := e.readTree(ctx, e.baselinePath(o.Request.WorkspaceID))
		if err != nil {
			return nil, err
		}
		diff := make([]DiffEntry, 0)
		names := map[string]bool{}
		for n := range base {
			names[n] = true
		}
		for n := range before {
			names[n] = true
		}
		sorted := make([]string, 0, len(names))
		for n := range names {
			sorted = append(sorted, n)
		}
		sort.Strings(sorted)
		for _, name := range sorted {
			old, oldOK := base[name]
			current, newOK := before[name]
			if oldOK && newOK && old.SHA256 == current.SHA256 && old.Executable == current.Executable {
				continue
			}
			diff = append(diff, DiffEntry{Path: name, BeforeSHA256: old.SHA256, AfterSHA256: current.SHA256, Before: string(old.Content), After: string(current.Content), Deleted: !newOK, BeforeExecutable: old.Executable, AfterExecutable: current.Executable})
		}
		return json.Marshal(map[string]any{"files": diff, "base_hash": treeHash(base), "workspace_hash": treeHash(before)})
	default:
		return nil, fmt.Errorf("%w: unknown file tool", domain.ErrInvalid)
	}
}
