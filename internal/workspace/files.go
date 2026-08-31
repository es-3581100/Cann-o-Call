package workspace

import (
	"bytes"
	"path"
	"sort"
	"strings"
	"unicode/utf8"
)

func UpsertFile(ws *Workspace, p string, data []byte) (*File, error) {
	if err := IsSafePath(p); err != nil {
		return nil, err
	}

	existing := ws.Files[p]

	encoding := "utf-8"
	if !utf8.Valid(data) {
		encoding = "base64"
	}

	hash := sha256Hex(data)

	f := &File{
		Path:           p,
		Size:           int64(len(data)),
		SHA256:         hash,
		DeclaredSHA256: hash,
		Encoding:       encoding,
		Kind:           "text",
		Language:       LanguageFromPath(p),
		MediaType:      MediaTypeFromPath(p),
		Verified:       true,
		Data:           data,
	}

	if existing != nil {
		if existing.Kind != "" {
			f.Kind = existing.Kind
		}
		if existing.Language != "" {
			f.Language = existing.Language
		}
		if existing.MediaType != "" {
			f.MediaType = existing.MediaType
		}
	}

	ws.Files[p] = f
	RecalcCounts(ws)

	return f, nil
}

func AppendToFile(ws *Workspace, p string, line []byte) (*File, error) {
	existingData := []byte{}

	if f, ok := ws.Files[p]; ok {
		existingData = append(existingData, f.Data...)
	}

	if len(existingData) > 0 && !bytes.HasSuffix(existingData, []byte("\n")) {
		existingData = append(existingData, '\n')
	}

	existingData = append(existingData, line...)

	if !bytes.HasSuffix(existingData, []byte("\n")) {
		existingData = append(existingData, '\n')
	}

	return UpsertFile(ws, p, existingData)
}

func RecalcCounts(ws *Workspace) {
	ws.FileCount = len(ws.Files)

	dirs := map[string]bool{}

	for p := range ws.Files {
		addParents(p, dirs)
	}

	for _, d := range ws.Directories {
		if err := IsSafePath(d); err == nil {
			dirs[d] = true
			addParents(d, dirs)
		}
	}

	list := make([]string, 0, len(dirs))
	for d := range dirs {
		list = append(list, d)
	}
	sort.Strings(list)

	ws.Directories = list
	ws.DirectoryCount = len(list)
}

func addParents(p string, dirs map[string]bool) {
	dir := path.Dir(p)

	for dir != "." && dir != "/" {
		dirs[dir] = true
		dir = path.Dir(dir)
	}
}

func LanguageFromPath(p string) string {
	ext := strings.ToLower(path.Ext(p))

	switch ext {
	case ".yaml", ".yml":
		return "yaml"
	case ".md", ".markdown":
		return "markdown"
	case ".json":
		return "json"
	case ".jsonl":
		return "jsonl"
	case ".go":
		return "go"
	case ".rs":
		return "rust"
	case ".html":
		return "html"
	case ".js":
		return "javascript"
	case ".css":
		return "css"
	default:
		return ""
	}
}

func MediaTypeFromPath(p string) string {
	ext := strings.ToLower(path.Ext(p))

	switch ext {
	case ".yaml", ".yml":
		return "application/yaml"
	case ".md", ".markdown":
		return "text/markdown"
	case ".json":
		return "application/json"
	case ".jsonl":
		return "application/jsonl"
	case ".go":
		return "text/x-go"
	case ".rs":
		return "text/x-rust"
	case ".html":
		return "text/html"
	case ".js":
		return "text/javascript"
	case ".css":
		return "text/css"
	default:
		return "application/octet-stream"
	}
}
