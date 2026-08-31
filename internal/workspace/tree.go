package workspace

import (
	"path"
	"sort"
	"strings"
)

type TreeNode struct {
	Name     string     `json:"name"`
	Path     string     `json:"path"`
	Type     string     `json:"type"`
	Size     int64      `json:"size,omitempty"`
	SHA256   string     `json:"sha256,omitempty"`
	Language string     `json:"language,omitempty"`
	Children []TreeNode `json:"children,omitempty"`
}

type nodeBuilder struct {
	name  string
	path  string
	dirs  map[string]*nodeBuilder
	files []*File
}

func newNodeBuilder(name, p string) *nodeBuilder {
	return &nodeBuilder{
		name:  name,
		path:  p,
		dirs:  map[string]*nodeBuilder{},
		files: []*File{},
	}
}

func (w *Workspace) TreeNodes() []TreeNode {
	root := newNodeBuilder("", "")

	paths := make([]string, 0, len(w.Files))
	for p := range w.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		parts := strings.Split(p, "/")
		current := root
		currentPath := ""

		for i, part := range parts {
			if i == len(parts)-1 {
				current.files = append(current.files, w.Files[p])
				continue
			}

			currentPath = path.Join(currentPath, part)

			child, ok := current.dirs[part]
			if !ok {
				child = newNodeBuilder(part, currentPath)
				current.dirs[part] = child
			}

			current = child
		}
	}

	return root.children()
}

func (b *nodeBuilder) children() []TreeNode {
	out := []TreeNode{}

	dirNames := make([]string, 0, len(b.dirs))
	for name := range b.dirs {
		dirNames = append(dirNames, name)
	}
	sort.Strings(dirNames)

	for _, name := range dirNames {
		child := b.dirs[name]
		out = append(out, TreeNode{
			Name:     name,
			Path:     child.path,
			Type:     "directory",
			Children: child.children(),
		})
	}

	sort.Slice(b.files, func(i, j int) bool {
		return b.files[i].Path < b.files[j].Path
	})

	for _, f := range b.files {
		out = append(out, TreeNode{
			Name:     path.Base(f.Path),
			Path:     f.Path,
			Type:     "file",
			Size:     f.Size,
			SHA256:   f.SHA256,
			Language: f.Language,
		})
	}

	return out
}
