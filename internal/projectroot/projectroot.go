package projectroot

import (
	"os"
	"path/filepath"

	"flatten-workspace/internal/workspace"
)

func Verify(root string, ws *workspace.Workspace, verifyFiles bool) workspace.RootVerification {
	v := workspace.RootVerification{
		Root:            root,
		NestedGitAbsent: true,
		Notes:           []string{},
	}

	if root == "" {
		v.Notes = append(v.Notes, "project root is empty")
		return v
	}

	info, err := os.Stat(root)
	if err != nil {
		v.Exists = false
		v.Notes = append(v.Notes, "project root does not exist or is inaccessible")
		return v
	}

	v.Exists = true
	v.IsDir = info.IsDir()

	if !v.IsDir {
		v.Notes = append(v.Notes, "project root is not a directory")
		return v
	}

	nestedGit := filepath.Join(root, "build-ledger", ".git")
	if _, err := os.Stat(nestedGit); err == nil {
		v.NestedGitAbsent = false
		v.Notes = append(v.Notes, "nested Git directory found under build-ledger/.git")
	}

	rootGit := filepath.Join(root, ".git")
	if _, err := os.Stat(rootGit); err == nil {
		v.GitPresent = true
	}

	manifest := filepath.Join(root, "build-ledger", "manifest.yaml")
	if _, err := os.Stat(manifest); err == nil {
		v.ManifestPresent = true
	}

	readme := filepath.Join(root, "README.md")
	if _, err := os.Stat(readme); err == nil {
		v.ReadmePresent = true
	}

	if verifyFiles && ws != nil {
		v.FileChecks = len(ws.Files)

		missing := []string{}

		for p := range ws.Files {
			full := filepath.Join(root, filepath.FromSlash(p))

			if _, err := os.Stat(full); err != nil {
				missing = append(missing, p)
			}
		}

		v.MissingCount = len(missing)

		if len(missing) > 20 {
			v.MissingFiles = missing[:20]
			v.Notes = append(v.Notes, "more than 20 missing files; showing first 20")
		} else {
			v.MissingFiles = missing
		}
	}

	v.Verified = v.Exists &&
		v.IsDir &&
		v.NestedGitAbsent &&
		(!verifyFiles || v.MissingCount == 0)

	return v
}
