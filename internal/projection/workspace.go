package projection

import (
	"flatten-workspace/internal/workspace"
)

func BuildFromWorkspace(ws *workspace.Workspace) Projection {
	files := map[string][]byte{}

	for p, f := range ws.Files {
		files[p] = f.Data
	}

	return BuildFromFiles(files)
}
