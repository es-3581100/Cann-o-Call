package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"flatten-workspace/internal/workspace"
)

// Adapter boundary: Inspect -> metadata, Extract -> content, Normalize -> canonical.
type Adapter interface {
	SourceType() string
	Supports(locator string, mediaType string) bool
	Inspect(data []byte, locator string) (Metadata, error)
	Extract(data []byte, locator string) (Content, error)
	Normalize(c Content) (Content, error)
}

// Registry holds adapters in deterministic priority order.
type Registry struct {
	adapters []Adapter
}

func NewRegistry() *Registry {
	r := &Registry{}
	// Priority order determines stable selection.
	r.adapters = []Adapter{
		&MarkdownAdapter{},
		&HTMLAdapter{},
		&JSONAdapter{},
		&YAMLAdapter{},
		&TextAdapter{},
		&GenericAdapter{},
	}
	// Ensure deterministic ordering by SourceType string after priority insertion.
	// Priority already explicit; do not resort arbitrarily.
	return r
}

func (r *Registry) AdapterFor(locator, mediaType string) Adapter {
	loc := strings.ToLower(locator)
	mt := strings.ToLower(mediaType)
	for _, a := range r.adapters {
		if a.Supports(loc, mt) {
			return a
		}
	}
	// fallback
	return &GenericAdapter{}
}

func (r *Registry) List() []Adapter { return append([]Adapter(nil), r.adapters...) }

// Helper: stable metadata map clone.

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// --- Base helpers ---

func mediaFromLocator(locator string) string {
	return workspace.MediaTypeFromPath(locator)
}

func languageFromLocator(locator string) string {
	return workspace.LanguageFromPath(locator)
}

func normalizeText(s string) string {
	// Canonical: trim, normalize CRLF -> LF, strip trailing whitespace per line, collapse multiple blank lines? Keep minimal.
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " \t")
	}
	joined := strings.Join(lines, "\n")
	return strings.Trim(joined, "\n")
}

func contentHashOfText(text string) string {
	return ComputeSHA256([]byte(text))
}

// --- Adapters ---

type TextAdapter struct{}

func (a *TextAdapter) SourceType() string { return SourceTypeText }
func (a *TextAdapter) Supports(locator string, mediaType string) bool {
	ext := strings.ToLower(path.Ext(locator))
	// explicit text family
	if ext == ".txt" || ext == ".text" {
		return true
	}
	if strings.HasPrefix(mediaType, "text/plain") {
		return true
	}
	// catch-all for generic text mime when no more specific adapter matched earlier
	// But priority ensures others matched first; this acts as near-generic text.
	if mediaType == "text/plain" || mediaType == "application/octet-stream" && ext == "" {
		return true
	}
	return false
}
func (a *TextAdapter) Inspect(data []byte, locator string) (Metadata, error) {
	mt := mediaFromLocator(locator)
	lang := languageFromLocator(locator)
	m := Metadata{
		MIMEType:     mt,
		Language:     lang,
		DetectedKind: "text",
		Encoding:     encodingOf(data),
		SourceTool:   "ingest-v1-text",
	}
	if mt == "" {
		m.MIMEType = "text/plain"
	}
	props := map[string]string{
		"locator": locator,
		"size":    fmt.Sprintf("%d", len(data)),
	}
	m.DocumentProperties = props
	return m, nil
}
func (a *TextAdapter) Extract(data []byte, locator string) (Content, error) {
	mt := mediaFromLocator(locator)
	if mt == "" {
		mt = "text/plain"
	}
	text := string(data)
	if !utf8.Valid(data) {
		// treat as binary fallback: keep raw but mark encoding
		text = string(bytes.ToValidUTF8(data, []byte("\uFFFD")))
	}
	norm := normalizeText(text)
	return Content{
		Encoding:       encodingOf(data),
		MediaType:      mt,
		Text:           text,
		NormalizedText: norm,
		Size:           int64(len(data)),
		ContentHash:    ComputeSHA256([]byte(norm)),
	}, nil
}
func (a *TextAdapter) Normalize(c Content) (Content, error) {
	c.NormalizedText = normalizeText(c.Text)
	c.ContentHash = ComputeSHA256([]byte(c.NormalizedText))
	return c, nil
}

type MarkdownAdapter struct{}

func (a *MarkdownAdapter) SourceType() string { return SourceTypeMarkdown }
func (a *MarkdownAdapter) Supports(locator string, mediaType string) bool {
	ext := strings.ToLower(path.Ext(locator))
	return ext == ".md" || ext == ".markdown" || mediaType == "text/markdown"
}
func (a *MarkdownAdapter) Inspect(data []byte, locator string) (Metadata, error) {
	m := Metadata{
		MIMEType:     "text/markdown",
		Language:     "markdown",
		DetectedKind: "markdown",
		Encoding:     encodingOf(data),
		SourceTool:   "ingest-v1-markdown",
	}
	// Keep markdown front-matter style metadata extraction minimal: title from first H1
	title := extractMarkdownTitle(string(data))
	if title != "" {
		m.Title = title
		m.OpenGraph = map[string]string{"title": title}
	}
	m.DocumentProperties = map[string]string{"locator": locator, "size": fmt.Sprintf("%d", len(data))}
	return m, nil
}
func (a *MarkdownAdapter) Extract(data []byte, locator string) (Content, error) {
	text := string(data)
	norm := normalizeText(text)
	return Content{
		Encoding:       encodingOf(data),
		MediaType:      "text/markdown",
		Text:           text,
		NormalizedText: norm,
		Size:           int64(len(data)),
		ContentHash:    ComputeSHA256([]byte(norm)),
	}, nil
}
func (a *MarkdownAdapter) Normalize(c Content) (Content, error) {
	c.NormalizedText = normalizeText(c.Text)
	c.ContentHash = ComputeSHA256([]byte(c.NormalizedText))
	return c, nil
}

type JSONAdapter struct{}

func (a *JSONAdapter) SourceType() string { return SourceTypeJSON }
func (a *JSONAdapter) Supports(locator string, mediaType string) bool {
	ext := strings.ToLower(path.Ext(locator))
	return ext == ".json" || ext == ".jsonl" || mediaType == "application/json" || mediaType == "application/jsonl"
}
func (a *JSONAdapter) Inspect(data []byte, locator string) (Metadata, error) {
	m := Metadata{
		MIMEType:     "application/json",
		Language:     "json",
		DetectedKind: "json",
		Encoding:     encodingOf(data),
		SourceTool:   "ingest-v1-json",
		DocumentProperties: map[string]string{
			"locator": locator,
			"size":    fmt.Sprintf("%d", len(data)),
		},
	}
	// Extract top-level keys as raw properties, not injected into content.
	if data != nil && len(bytes.TrimSpace(data)) > 0 {
		var v any
		if err := json.Unmarshal(data, &v); err == nil {
			if mObj, ok := v.(map[string]any); ok {
				keys := make([]string, 0, len(mObj))
				for k := range mObj {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				props := map[string]string{}
				for _, k := range keys {
					props[k] = fmt.Sprintf("%T", mObj[k])
				}
				m.RawProperties = props
			}
		}
	}
	return m, nil
}
func (a *JSONAdapter) Extract(data []byte, locator string) (Content, error) {
	text := string(data)
	// Canonicalize JSON if valid to ensure deterministic normalized form.
	var v any
	if err := json.Unmarshal(data, &v); err == nil {
		// Use canonical JSON (sorted keys) via json.Marshal with sorted map traversal — but Go's Marshal sorts keys deterministically for maps.
		b, err2 := json.Marshal(v)
		if err2 == nil {
			var buf bytes.Buffer
			if err := json.Indent(&buf, b, "", "  "); err == nil {
				text = buf.String() // pretty but deterministic
			} else {
				text = string(b)
			}
		}
	}
	norm := normalizeText(text)
	return Content{
		Encoding:       encodingOf(data),
		MediaType:      "application/json",
		Text:           string(data),
		NormalizedText: norm,
		Size:           int64(len(data)),
		ContentHash:    ComputeSHA256([]byte(norm)),
	}, nil
}
func (a *JSONAdapter) Normalize(c Content) (Content, error) {
	c.NormalizedText = normalizeText(c.Text)
	c.ContentHash = ComputeSHA256([]byte(c.NormalizedText))
	return c, nil
}

type YAMLAdapter struct{}

func (a *YAMLAdapter) SourceType() string { return SourceTypeYAML }
func (a *YAMLAdapter) Supports(locator string, mediaType string) bool {
	ext := strings.ToLower(path.Ext(locator))
	return ext == ".yaml" || ext == ".yml" || mediaType == "application/yaml"
}
func (a *YAMLAdapter) Inspect(data []byte, locator string) (Metadata, error) {
	m := Metadata{
		MIMEType:     "application/yaml",
		Language:     "yaml",
		DetectedKind: "yaml",
		Encoding:     encodingOf(data),
		SourceTool:   "ingest-v1-yaml",
		DocumentProperties: map[string]string{
			"locator": locator,
			"size":    fmt.Sprintf("%d", len(data)),
		},
	}
	return m, nil
}
func (a *YAMLAdapter) Extract(data []byte, locator string) (Content, error) {
	text := string(data)
	norm := normalizeText(text)
	return Content{
		Encoding:       encodingOf(data),
		MediaType:      "application/yaml",
		Text:           text,
		NormalizedText: norm,
		Size:           int64(len(data)),
		ContentHash:    ComputeSHA256([]byte(norm)),
	}, nil
}
func (a *YAMLAdapter) Normalize(c Content) (Content, error) {
	c.NormalizedText = normalizeText(c.Text)
	c.ContentHash = ComputeSHA256([]byte(c.NormalizedText))
	return c, nil
}

type HTMLAdapter struct{}

func (a *HTMLAdapter) SourceType() string { return SourceTypeHTML }
func (a *HTMLAdapter) Supports(locator string, mediaType string) bool {
	ext := strings.ToLower(path.Ext(locator))
	return ext == ".html" || ext == ".htm" || mediaType == "text/html"
}
func (a *HTMLAdapter) Inspect(data []byte, locator string) (Metadata, error) {
	s := string(data)
	title := extractHTMLTitle(s)
	m := Metadata{
		MIMEType:     "text/html",
		Language:     "html",
		DetectedKind: "html",
		Encoding:     encodingOf(data),
		SourceTool:   "ingest-v1-html",
		DocumentProperties: map[string]string{
			"locator": locator,
			"size":    fmt.Sprintf("%d", len(data)),
		},
	}
	if title != "" {
		m.Title = title
	}
	og := extractOpenGraph(s)
	if len(og) > 0 {
		m.OpenGraph = og
		if title == "" {
			if t, ok := og["og:title"]; ok {
				m.Title = t
			}
		}
	}
	// Do not inject OpenGraph into content keywords — separation preserved.
	return m, nil
}
func (a *HTMLAdapter) Extract(data []byte, locator string) (Content, error) {
	s := string(data)
	// For determinism, strip tags via naive pass to get visible text, but preserve normalized raw as well.
	// Keep Text as raw HTML, NormalizedText as stripped+normalized.
	stripped := stripHTMLTags(s)
	norm := normalizeText(stripped)
	// ContentHash is over normalized stripped text (semantic), not raw bytes, to keep HTML semantics distinct.
	// However keep size as original bytes size.
	return Content{
		Encoding:       encodingOf(data),
		MediaType:      "text/html",
		Text:           s,
		NormalizedText: norm,
		Size:           int64(len(data)),
		ContentHash:    ComputeSHA256([]byte(norm)),
	}, nil
}
func (a *HTMLAdapter) Normalize(c Content) (Content, error) {
	// Re-strip if Text is raw HTML and NormalizedText was raw.
	if c.NormalizedText == "" {
		c.NormalizedText = normalizeText(stripHTMLTags(c.Text))
	} else {
		c.NormalizedText = normalizeText(c.NormalizedText)
	}
	c.ContentHash = ComputeSHA256([]byte(c.NormalizedText))
	return c, nil
}

type GenericAdapter struct{}

func (a *GenericAdapter) SourceType() string                             { return SourceTypeGeneric }
func (a *GenericAdapter) Supports(locator string, mediaType string) bool { return true }
func (a *GenericAdapter) Inspect(data []byte, locator string) (Metadata, error) {
	mt := mediaFromLocator(locator)
	if mt == "" {
		mt = "application/octet-stream"
	}
	return Metadata{
		MIMEType:     mt,
		Language:     languageFromLocator(locator),
		DetectedKind: "generic",
		Encoding:     encodingOf(data),
		SourceTool:   "ingest-v1-generic",
		DocumentProperties: map[string]string{
			"locator": locator,
			"size":    fmt.Sprintf("%d", len(data)),
		},
	}, nil
}
func (a *GenericAdapter) Extract(data []byte, locator string) (Content, error) {
	mt := mediaFromLocator(locator)
	if mt == "" {
		mt = "application/octet-stream"
	}
	text := ""
	if utf8.Valid(data) {
		text = string(data)
	} else {
		// For binary, keep empty semantic text, reference large blob via Ref.
		text = ""
	}
	norm := normalizeText(text)
	hash := ComputeSHA256([]byte(norm))
	if norm == "" && len(data) > 0 {
		// For non-text binary, hash is over raw bytes hex distinction
		hash = ComputeSHA256(data)
	}
	return Content{
		Encoding:       encodingOf(data),
		MediaType:      mt,
		Text:           text,
		NormalizedText: norm,
		Size:           int64(len(data)),
		ContentHash:    hash,
		Ref:            "", // caller may set Ref if size large
	}, nil
}
func (a *GenericAdapter) Normalize(c Content) (Content, error) {
	c.NormalizedText = normalizeText(c.Text)
	if c.NormalizedText == "" && c.Size > 0 {
		// keep existing hash for binary
		return c, nil
	}
	c.ContentHash = ComputeSHA256([]byte(c.NormalizedText))
	return c, nil
}

// --- utilities ---

func encodingOf(data []byte) string {
	if utf8.Valid(data) {
		return "utf-8"
	}
	return "base64"
}

func extractMarkdownTitle(s string) string {
	lines := strings.Split(s, "\n")
	for _, l := range lines {
		trim := strings.TrimSpace(l)
		if strings.HasPrefix(trim, "# ") {
			return strings.TrimSpace(strings.TrimPrefix(trim, "# "))
		}
	}
	return ""
}

func extractHTMLTitle(s string) string {
	lower := strings.ToLower(s)
	start := strings.Index(lower, "<title>")
	if start == -1 {
		return ""
	}
	end := strings.Index(lower[start:], "</title>")
	if end == -1 {
		return ""
	}
	content := s[start+len("<title>") : start+end]
	return strings.TrimSpace(content)
}

func extractOpenGraph(s string) map[string]string {
	out := map[string]string{}
	lower := strings.ToLower(s)
	// naive scan for <meta property="og:*" content="...">
	idx := 0
	for {
		metaIdx := strings.Index(lower[idx:], "<meta")
		if metaIdx == -1 {
			break
		}
		abs := idx + metaIdx
		tagEnd := strings.Index(lower[abs:], ">")
		if tagEnd == -1 {
			break
		}
		tag := s[abs : abs+tagEnd+1]
		lTag := strings.ToLower(tag)
		if strings.Contains(lTag, "property=") && strings.Contains(lTag, "og:") {
			prop := extractAttr(tag, "property")
			content := extractAttr(tag, "content")
			if prop != "" && content != "" {
				out[prop] = content
			}
		}
		if strings.Contains(lTag, `name="description"`) || strings.Contains(lTag, `name='description'`) {
			content := extractAttr(tag, "content")
			if content != "" {
				if _, ok := out["description"]; !ok {
					out["description"] = content
				}
			}
		}
		idx = abs + tagEnd + 1
		if idx >= len(s) {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func extractAttr(tag, attr string) string {
	lower := strings.ToLower(tag)
	attrLower := strings.ToLower(attr) + "="
	idx := strings.Index(lower, attrLower)
	if idx == -1 {
		return ""
	}
	rest := strings.TrimSpace(tag[idx+len(attrLower):])
	if rest == "" {
		return ""
	}
	quote := rest[0]
	if quote != '"' && quote != '\'' {
		end := strings.IndexAny(rest, " \t\n>")
		if end == -1 {
			return rest
		}
		return rest[:end]
	}
	end := strings.IndexByte(rest[1:], quote)
	if end == -1 {
		return ""
	}
	return rest[1 : 1+end]
}

func stripHTMLTags(s string) string {
	var buf strings.Builder
	inTag := false
	for _, r := range s {
		if r == '<' {
			inTag = true
			buf.WriteRune(' ')
			continue
		}
		if r == '>' {
			inTag = false
			continue
		}
		if !inTag {
			buf.WriteRune(r)
		}
	}
	// collapse
	raw := buf.String()
	// normalize whitespace
	fields := strings.Fields(raw)
	return strings.Join(fields, " ")
}

var jsonCanonicalPlaceholder = json.RawMessage{}
