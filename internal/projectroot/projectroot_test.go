package projectroot

import (
	"os"
	"path/filepath"
	"testing"

	"flatten-workspace/internal/workspace"
)

func TestVerifyRootMaterialized(t *testing.T) {
	root := t.TempDir()

	if err := os.MkdirAll(filepath.Join(root, "build-ledger"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("# test"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(root, "build-ledger", "manifest.yaml"),
		[]byte("schema: dynamic-build-ledger\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	ws := &workspace.Workspace{
		Files: map[string]*workspace.File{
			"README.md": {
				Path: "README.md",
				Data: []byte("# test"),
			},
			"build-ledger/manifest.yaml": {
				Path: "build-ledger/manifest.yaml",
				Data: []byte("schema: dynamic-build-ledger\n"),
			},
		},
	}

	v := Verify(root, ws, true)

	if !v.Verified {
		t.Fatalf("expected root to verify, got: %+v", v)
	}
}

func TestVerifyRootRejectsNestedGit(t *testing.T) {
	root := t.TempDir()

	if err := os.MkdirAll(filepath.Join(root, "build-ledger", ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	ws := &workspace.Workspace{
		Files: map[string]*workspace.File{},
	}

	v := Verify(root, ws, false)

	if v.NestedGitAbsent {
		t.Fatal("expected nested Git to be detected")
	}

	if v.Verified {
		t.Fatal("expected verification to fail when nested Git is present")
	}
}
