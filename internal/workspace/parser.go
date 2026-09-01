package workspace

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func Parse(data []byte) (*Workspace, error) {
	var env Envelope
	if err := yaml.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse flatten-workspace envelope: %w", err)
	}

	return FromEnvelope(&env)
}

func FromEnvelope(env *Envelope) (*Workspace, error) {
	if env == nil {
		return nil, errors.New("envelope is nil")
	}

	if env.Format != FormatV1 {
		return nil, fmt.Errorf("unsupported format %q", env.Format)
	}

	if env.Mode != ModeZipStructure {
		return nil, fmt.Errorf("unsupported mode %q", env.Mode)
	}

	if len(env.Tree) == 0 {
		return nil, errors.New("tree is empty")
	}

	ws := &Workspace{
		Format:      env.Format,
		Mode:        env.Mode,
		Source:      env.Source,
		Tree:        env.Tree,
		Files:       map[string]*File{},
		Issues:      []string{},
		Quarantined: []string{},
		CreatedAt:   time.Now().UTC(),
	}

	for _, q := range env.QuarantinedEntries {
		ws.Quarantined = append(ws.Quarantined, fmt.Sprint(q))
	}

	fileCount := 0
	dirCount := 0

	var walk func(name string, node any, nodePath string) error

	walk = func(name string, node any, nodePath string) error {
		if err := validateEntryName(name); err != nil {
			return fmt.Errorf("unsafe entry name %q: %w", name, err)
		}

		m, ok := node.(map[string]any)
		if !ok {
			return fmt.Errorf("node %q must be an object", nodePath)
		}

		rawFile, isFile := m["__file__.json"]
		if isFile {
			raw, ok := rawFile.(string)
			if !ok {
				return fmt.Errorf("file node %q: __file__.json must be a string", nodePath)
			}

			var meta FileMeta
			if err := json.Unmarshal([]byte(raw), &meta); err != nil {
				return fmt.Errorf("file node %q: invalid __file__.json: %w", nodePath, err)
			}

			data, err := decodeContent(meta.Content, meta.Encoding)
			if err != nil {
				return fmt.Errorf("file node %q: %w", nodePath, err)
			}

			actualHash := sha256Hex(data)

			f := &File{
				Path:           nodePath,
				Size:           int64(len(data)),
				SHA256:         actualHash,
				DeclaredSHA256: meta.SHA256,
				Encoding:       meta.Encoding,
				Kind:           meta.Kind,
				Language:       meta.Language,
				MediaType:      meta.MediaType,
				Data:           data,
				Verified:       true,
			}

			if meta.Path != "" {
				if err := IsSafePath(meta.Path); err != nil {
					ws.Issues = append(ws.Issues, fmt.Sprintf("%s: declared path rejected: %v", nodePath, err))
					f.Verified = false
				} else {
					if meta.Path != nodePath {
						ws.Issues = append(
							ws.Issues,
							fmt.Sprintf(
								"%s: tree path and declared path differ (%q != %q); using declared path",
								nodePath,
								nodePath,
								meta.Path,
							),
						)
					}
					f.Path = meta.Path
				}
			}

			if meta.Size != int64(len(data)) {
				ws.Issues = append(
					ws.Issues,
					fmt.Sprintf(
						"%s: declared size %d does not match decoded size %d",
						f.Path,
						meta.Size,
						len(data),
					),
				)
				f.Verified = false
			}

			if meta.SHA256 != "" && !strings.EqualFold(meta.SHA256, actualHash) {
				ws.Issues = append(
					ws.Issues,
					fmt.Sprintf(
						"%s: declared sha256 %s does not match computed sha256 %s",
						f.Path,
						meta.SHA256,
						actualHash,
					),
				)
				f.Verified = false
			}

			if _, exists := ws.Files[f.Path]; exists {
				ws.Issues = append(ws.Issues, fmt.Sprintf("%s: duplicate file path %s", nodePath, f.Path))
				f.Verified = false
			} else {
				ws.Files[f.Path] = f
			}

			fileCount++
			return nil
		}

		dirCount++

		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, childName := range keys {
			childNode := m[childName]
			childPath := path.Join(nodePath, childName)
			if err := walk(childName, childNode, childPath); err != nil {
				return err
			}
		}

		return nil
	}

	names := make([]string, 0, len(env.Tree))
	for k := range env.Tree {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		node := env.Tree[name]
		if err := walk(name, node, name); err != nil {
			return nil, err
		}
	}

	ws.FileCount = fileCount
	ws.DirectoryCount = dirCount

	if env.Source.FileCount > 0 && env.Source.FileCount != fileCount {
		ws.Issues = append(
			ws.Issues,
			fmt.Sprintf("source.file_count is %d but tree contains %d files", env.Source.FileCount, fileCount),
		)
	}

	if env.Source.DirectoryCount > 0 && env.Source.DirectoryCount != dirCount {
		ws.Issues = append(
			ws.Issues,
			fmt.Sprintf("source.directory_count is %d but tree contains %d directories", env.Source.DirectoryCount, dirCount),
		)
	}

	if env.Source.UnsafeEntryCount > 0 {
		ws.Issues = append(
			ws.Issues,
			fmt.Sprintf("source.unsafe_entry_count reports %d unsafe entries", env.Source.UnsafeEntryCount),
		)
	}

	if len(env.Manifest) > 0 {
		ws.ManifestChecked = true
		validateManifest(env.Manifest, ws)
	}

	ws.ID = strings.TrimSpace(env.Source.SHA256)
	if ws.ID == "" {
		ws.ID = deriveID(ws)
	}

	return ws, nil
}

func validateManifest(manifest []map[string]any, ws *Workspace) {
	seen := make(map[string]bool, len(manifest))

	for i, item := range manifest {
		raw, err := json.Marshal(item)
		if err != nil {
			ws.Issues = append(ws.Issues, fmt.Sprintf("manifest[%d]: cannot marshal item: %v", i, err))
			continue
		}

		var meta FileMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			ws.Issues = append(ws.Issues, fmt.Sprintf("manifest[%d]: invalid file meta: %v", i, err))
			continue
		}

		if meta.Path == "" {
			ws.Issues = append(ws.Issues, fmt.Sprintf("manifest[%d]: missing path", i))
			continue
		}

		seen[meta.Path] = true

		f, ok := ws.Files[meta.Path]
		if !ok {
			ws.Issues = append(ws.Issues, fmt.Sprintf("manifest[%d]: path %s not found in tree", i, meta.Path))
			continue
		}

		if meta.Size != 0 && meta.Size != f.Size {
			ws.Issues = append(
				ws.Issues,
				fmt.Sprintf(
					"manifest[%d]: %s size mismatch: manifest %d, tree %d",
					i,
					meta.Path,
					meta.Size,
					f.Size,
				),
			)
			f.Verified = false
		}

		if meta.SHA256 != "" && !strings.EqualFold(meta.SHA256, f.SHA256) {
			ws.Issues = append(
				ws.Issues,
				fmt.Sprintf(
					"manifest[%d]: %s sha256 mismatch: manifest %s, computed %s",
					i,
					meta.Path,
					meta.SHA256,
					f.SHA256,
				),
			)
			f.Verified = false
		}
	}

	paths := make([]string, 0, len(ws.Files))
	for p := range ws.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		if !seen[p] {
			ws.Issues = append(ws.Issues, fmt.Sprintf("tree file %s is missing from manifest", p))
		}
	}
}

func decodeContent(content, encoding string) ([]byte, error) {
	enc := strings.ToLower(strings.TrimSpace(encoding))

	switch enc {
	case "", "utf-8", "utf8", "text":
		return []byte(content), nil
	case "base64":
		return base64.StdEncoding.DecodeString(content)
	default:
		return nil, fmt.Errorf("unsupported content encoding %q", encoding)
	}
}

func validateEntryName(name string) error {
	if name == "" {
		return errors.New("empty name")
	}

	if name == "." || name == ".." {
		return errors.New("reserved name")
	}

	if strings.ContainsAny(name, "/\\\x00") {
		return errors.New("contains path separator or null byte")
	}

	return nil
}

func IsSafePath(p string) error {
	if p == "" {
		return errors.New("empty path")
	}

	if strings.HasPrefix(p, "/") {
		return errors.New("absolute path")
	}

	if strings.Contains(p, "\x00") {
		return errors.New("null byte")
	}

	if strings.Contains(p, "..") {
		return errors.New("path traversal")
	}

	cleaned := path.Clean(p)
	if cleaned != p {
		return errors.New("path is not normalized")
	}

	for _, part := range strings.Split(cleaned, "/") {
		if part == "" || part == "." || part == ".." {
			return errors.New("invalid path segment")
		}
	}

	return nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func deriveID(ws *Workspace) string {
	h := sha256.New()

	paths := make([]string, 0, len(ws.Files))
	for p := range ws.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
		h.Write([]byte(ws.Files[p].SHA256))
		h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil))
}
