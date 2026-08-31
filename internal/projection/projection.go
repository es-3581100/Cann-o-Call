package projection

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"sort"
	"strings"

	"flatten-workspace/internal/workspace"
)

type FileProjection struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	Language string `json:"language,omitempty"`
}

type Projection struct {
	FileCount      int              `json:"file_count"`
	DirectoryCount int              `json:"directory_count"`
	Files          []FileProjection `json:"files"`
	Directories    []string         `json:"directories"`
	Hash           string           `json:"hash,omitempty"`
}

func BuildFromFiles(files map[string][]byte) Projection {
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	fileProjections := []FileProjection{}
	dirs := map[string]bool{}

	for _, p := range paths {
		data := files[p]

		sum := sha256.Sum256(data)

		fileProjections = append(fileProjections, FileProjection{
			Path:     p,
			Size:     int64(len(data)),
			SHA256:   hex.EncodeToString(sum[:]),
			Language: workspace.LanguageFromPath(p),
		})

		addParents(p, dirs)
	}

	dirList := make([]string, 0, len(dirs))
	for d := range dirs {
		dirList = append(dirList, d)
	}
	sort.Strings(dirList)

	p := Projection{
		FileCount:      len(paths),
		DirectoryCount: len(dirList),
		Files:          fileProjections,
		Directories:    dirList,
	}

	p.Hash = hashProjection(p)

	return p
}

func BuildLedgerFromWorkspace(ws *workspace.Workspace) Projection {
	files := map[string][]byte{}

	for p, f := range ws.Files {
		if strings.HasPrefix(p, "build-ledger/") {
			files[p] = f.Data
		}
	}

	return BuildFromFiles(files)
}

func Equivalent(a, b Projection) bool {
	return a.Hash == b.Hash
}

func addParents(p string, dirs map[string]bool) {
	dir := path.Dir(p)

	for dir != "." && dir != "/" {
		dirs[dir] = true
		dir = path.Dir(dir)
	}
}

func hashProjection(p Projection) string {
	clone := p
	clone.Hash = ""

	b, err := json.Marshal(clone)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
