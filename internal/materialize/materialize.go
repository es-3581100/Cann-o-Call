package materialize

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"flatten-workspace/internal/workspace"
)

func WriteWorkspace(ws *workspace.Workspace, root string, allowAbsolute bool) ([]string, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve materialization root: %w", err)
	}

	if !allowAbsolute {
		// The caller is expected to enforce a safe base directory.
		// This is a secondary guard against accidental escape.
		if filepath.IsAbs(root) {
			return nil, fmt.Errorf("absolute materialization root requires explicit authority")
		}
	}

	if err := os.MkdirAll(absRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create materialization root: %w", err)
	}

	paths := make([]string, 0, len(ws.Files))
	for p := range ws.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	written := []string{}

	for _, p := range paths {
		f, ok := ws.Files[p]
		if !ok {
			continue
		}

		rel := filepath.FromSlash(p)
		full := filepath.Join(absRoot, rel)

		relCheck, err := filepath.Rel(absRoot, full)
		if err != nil {
			return nil, fmt.Errorf("unsafe path %q: %w", p, err)
		}

		if strings.HasPrefix(relCheck, "..") {
			return nil, fmt.Errorf("path escapes materialization root: %q", p)
		}

		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return nil, fmt.Errorf("create parent directory for %q: %w", p, err)
		}

		if err := os.WriteFile(full, f.Data, 0o644); err != nil {
			return nil, fmt.Errorf("write %q: %w", p, err)
		}

		written = append(written, full)
	}

	return written, nil
}
