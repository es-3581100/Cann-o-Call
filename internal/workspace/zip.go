package workspace

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

func FromZipBytes(data []byte, archiveName string) (*Workspace, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	archiveHash := sha256.Sum256(data)

	ws := &Workspace{
		Format: FormatV1,
		Mode:   ModeZipStructure,
		Source: Source{
			Name:   archiveName,
			SHA256: hex.EncodeToString(archiveHash[:]),
		},
		Files:       map[string]*File{},
		Issues:      []string{},
		Quarantined: []string{},
		Directories: []string{},
		CreatedAt:   time.Now().UTC(),
	}

	dirs := map[string]bool{}
	unsafe := 0

	for _, zf := range zr.File {
		rawName := zf.Name
		name := path.Clean(rawName)

		if strings.HasSuffix(rawName, "/") {
			dirName := strings.TrimSuffix(name, "/")
			if dirName == "." || dirName == "" {
				continue
			}

			if err := IsSafePath(dirName); err != nil {
				unsafe++
				ws.Quarantined = append(ws.Quarantined, rawName)
				ws.Issues = append(ws.Issues, fmt.Sprintf("unsafe directory entry %q: %v", rawName, err))
				continue
			}

			dirs[dirName] = true
			continue
		}

		if err := IsSafePath(name); err != nil {
			unsafe++
			ws.Quarantined = append(ws.Quarantined, rawName)
			ws.Issues = append(ws.Issues, fmt.Sprintf("unsafe file entry %q: %v", rawName, err))

			rc, rcErr := zf.Open()
			if rcErr != nil {
				continue
			}
			content, readErr := io.ReadAll(rc)
			rc.Close()
			if readErr != nil {
				continue
			}

			hash := sha256Hex(content)
			blobID := DeterministicQuarantineID(ws.Source.SHA256, rawName, hash)
			safePath := SafeQuarantinePath(rawName, blobID, ws.Files)
			ws.QuarantinedBlobs = append(ws.QuarantinedBlobs, &QuarantinedBlob{
				ID:           blobID,
				OriginalPath: rawName,
				SafePath:     safePath,
				Reason:       err.Error(),
				Status:       "quarantined",
				SHA256:       hash,
				Size:         int64(len(content)),
				Data:         content,
			})
			continue
		}

		rc, err := zf.Open()
		if err != nil {
			ws.Issues = append(ws.Issues, fmt.Sprintf("cannot open zip member %q: %v", rawName, err))
			continue
		}

		content, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			ws.Issues = append(ws.Issues, fmt.Sprintf("cannot read zip member %q: %v", rawName, err))
			continue
		}

		hash := sha256Hex(content)

		encoding := "utf-8"
		if !utf8.Valid(content) {
			encoding = "base64"
		}

		ws.Files[name] = &File{
			Path:           name,
			Size:           int64(len(content)),
			SHA256:         hash,
			DeclaredSHA256: hash,
			Encoding:       encoding,
			Kind:           "text",
			Language:       LanguageFromPath(name),
			MediaType:      MediaTypeFromPath(name),
			Verified:       true,
			Data:           content,
		}

		addParents(name, dirs)
	}

	dirList := make([]string, 0, len(dirs))
	for d := range dirs {
		dirList = append(dirList, d)
	}
	sort.Strings(dirList)

	ws.Directories = dirList
	ws.FileCount = len(ws.Files)
	ws.DirectoryCount = len(dirList)

	ws.Source.FileCount = ws.FileCount
	ws.Source.DirectoryCount = ws.DirectoryCount
	ws.Source.ArchiveMemberCount = len(zr.File)
	ws.Source.UnsafeEntryCount = unsafe

	ws.ID = ws.Source.SHA256

	return ws, nil
}

func (w *Workspace) ToEnvelope() (*Envelope, error) {
	tree := map[string]any{}

	// Create explicit directories first.
	for _, d := range w.Directories {
		parts := strings.Split(d, "/")
		current := tree

		for _, part := range parts {
			next, ok := current[part].(map[string]any)
			if !ok {
				next = map[string]any{}
				current[part] = next
			}
			current = next
		}
	}

	manifest := []map[string]any{}

	paths := make([]string, 0, len(w.Files))
	for p := range w.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		f := w.Files[p]

		content := string(f.Data)
		encoding := "utf-8"

		if !utf8.Valid(f.Data) {
			content = base64.StdEncoding.EncodeToString(f.Data)
			encoding = "base64"
		}

		meta := FileMeta{
			Content:   content,
			Encoding:  encoding,
			Kind:      f.Kind,
			Language:  f.Language,
			MediaType: f.MediaType,
			Path:      f.Path,
			SHA256:    f.SHA256,
			Size:      f.Size,
		}

		rawMeta, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("marshal file meta for %q: %w", p, err)
		}

		parts := strings.Split(p, "/")
		current := tree

		for i, part := range parts {
			if i == len(parts)-1 {
				current[part] = map[string]any{
					"__file__.json": string(rawMeta),
				}
			} else {
				next, ok := current[part].(map[string]any)
				if !ok {
					next = map[string]any{}
					current[part] = next
				}
				current = next
			}
		}

		var manifestItem map[string]any
		if err := json.Unmarshal(rawMeta, &manifestItem); err != nil {
			return nil, fmt.Errorf("unmarshal manifest item for %q: %w", p, err)
		}

		manifest = append(manifest, manifestItem)
	}

	env := &Envelope{
		Format:             FormatV1,
		Mode:               ModeZipStructure,
		Source:             w.Source,
		Tree:               tree,
		Manifest:           manifest,
		QuarantinedEntries: []any{},
	}

	for _, q := range w.Quarantined {
		env.QuarantinedEntries = append(env.QuarantinedEntries, q)
	}

	return env, nil
}
