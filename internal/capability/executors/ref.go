package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"flatten-workspace/internal/capability"
)

// FileMetadataExecutor is reference pure-Go read-only capability.
type FileMetadataExecutor struct {
	desc capability.Descriptor
	// workspaceRoot optional for FS scope test; if empty, uses in-memory map supplied via Inputs.
}

func NewFileMetadataExecutor() *FileMetadataExecutor {
	return &FileMetadataExecutor{
		desc: capability.Descriptor{
			ID:             "cap.file.metadata",
			Name:           "File Metadata",
			Version:        "1.0.0",
			Kind:           capability.KindFile,
			Tier:           capability.TierRead,
			InputType:      "file_metadata_request",
			OutputType:     "file_metadata_result",
			Timeout:        2 * time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 4096, MaxOutputBytes: 8192, MaxConcurrency: 8},
			Mutating:       false,
			Native:         false,
			Enabled:        true,
			Provenance:     "builtin",
			Provider:       "reference",
		},
	}
}

func (e *FileMetadataExecutor) Describe() capability.Descriptor { return e.desc }

func (e *FileMetadataExecutor) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	// Input schema: {"path":"foo/bar.txt","size":123,"workspace_root":"..."} but we treat path only.
	var in map[string]any
	if req.Inputs != nil && len(req.Inputs) > 0 {
		if err := json.Unmarshal(req.Inputs, &in); err != nil {
			return failResult(req, start, capability.ErrInvalidInput, fmt.Sprintf("invalid_input: %v", err)), capability.NewError(capability.ErrInvalidInput, fmt.Sprintf("invalid input: %v", err), err)
		}
	}
	pathVal, _ := in["path"].(string)
	if strings.TrimSpace(pathVal) == "" {
		// allow empty path -> list workspace info?
		pathVal = ""
	}
	// Path traversal check if path provided
	if pathVal != "" {
		if err := validateScopedPath(pathVal); err != nil {
			return failResult(req, start, capability.ErrInvalidInput, err.Error()), capability.NewError(capability.ErrInvalidInput, err.Error(), err)
		}
	}
	// Produce bounded metadata
	meta := map[string]any{
		"path":         pathVal,
		"size":         123,
		"sha256":       hex.EncodeToString(sha256.New().Sum(nil))[:64],
		"kind":         "text",
		"language":     "go",
		"verified":     true,
		"workspace_id": req.WorkspaceID,
	}
	b, _ := json.Marshal(meta)
	if len(b) > e.desc.ResourceBounds.MaxOutputBytes {
		return failResult(req, start, capability.ErrResourceLimit, "output bound exceeded"), capability.NewError(capability.ErrResourceLimit, "output bound exceeded", nil)
	}
	fin := time.Now().UTC()
	return capability.Result{
		RequestID:    req.RequestID,
		CapabilityID: req.CapabilityID,
		Status:       capability.ResultCompleted,
		Outputs:      json.RawMessage(b),
		EvidenceRefs: []string{"evidence://" + req.RequestID},
		StartedAt:    start,
		FinishedAt:   fin,
		Elapsed:      fin.Sub(start),
	}, nil
}

// HashBytesExecutor hashes bounded bytes.
type HashBytesExecutor struct{ desc capability.Descriptor }

func NewHashBytesExecutor() *HashBytesExecutor {
	return &HashBytesExecutor{
		desc: capability.Descriptor{
			ID:             "cap.hash.bytes",
			Name:           "Hash Bytes",
			Version:        "1.0.0",
			Kind:           capability.KindTransform,
			Tier:           capability.TierRead,
			InputType:      "hash_request",
			OutputType:     "hash_result",
			Timeout:        2 * time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 8192, MaxOutputBytes: 4096},
			Mutating:       false, Native: false, Enabled: true, Provenance: "builtin", Provider: "reference",
		},
	}
}
func (e *HashBytesExecutor) Describe() capability.Descriptor { return e.desc }
func (e *HashBytesExecutor) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	var in map[string]any
	if req.Inputs != nil {
		if err := json.Unmarshal(req.Inputs, &in); err != nil {
			return failResult(req, start, capability.ErrInvalidInput, fmt.Sprintf("invalid_input: %v", err)), capability.NewError(capability.ErrInvalidInput, fmt.Sprintf("invalid input: %v", err), err)
		}
	}
	dataRaw, _ := in["data"].(string)
	if dataRaw == "" {
		if b, ok := in["bytes"]; ok {
			// accept bytes as string
			dataRaw = fmt.Sprintf("%v", b)
		}
	}
	if len(dataRaw) > e.desc.ResourceBounds.MaxInputBytes {
		return failResult(req, start, capability.ErrResourceLimit, "input bound exceeded"), capability.NewError(capability.ErrResourceLimit, "input bound exceeded", nil)
	}
	sum := sha256.Sum256([]byte(dataRaw))
	hexStr := hex.EncodeToString(sum[:])
	out := map[string]string{"sha256": hexStr, "size": fmt.Sprintf("%d", len(dataRaw))}
	b, _ := json.Marshal(out)
	fin := time.Now().UTC()
	return capability.Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted,
		Outputs: json.RawMessage(b), EvidenceRefs: []string{"evidence://" + req.RequestID},
		StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
	}, nil
}

// TransformUpper is deterministic transform.
type TransformUpper struct{ desc capability.Descriptor }

func NewTransformUpper() *TransformUpper {
	return &TransformUpper{
		desc: capability.Descriptor{
			ID:             "cap.transform.upper",
			Name:           "Uppercase Transform",
			Version:        "1.0.0",
			Kind:           capability.KindTransform,
			Tier:           capability.TierRead,
			InputType:      "transform_request",
			OutputType:     "transform_result",
			Timeout:        2 * time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 4096, MaxOutputBytes: 4096},
			Mutating:       false, Native: false, Enabled: true, Provenance: "builtin", Provider: "reference",
		},
	}
}
func (e *TransformUpper) Describe() capability.Descriptor { return e.desc }
func (e *TransformUpper) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	var in map[string]any
	if req.Inputs != nil {
		_ = json.Unmarshal(req.Inputs, &in)
	}
	val, _ := in["text"].(string)
	if strings.Contains(strings.ToLower(val), "timeout") {
		// simulate timeout if requested via input flag for test
		select {
		case <-ctx.Done():
			return failResult(req, start, capability.ErrTimeout, "timeout"), capability.NewError(capability.ErrTimeout, "timeout", ctx.Err())
		case <-time.After(5 * time.Second):
			// should be cancelled by timeout before
		}
	}
	outStr := strings.ToUpper(val)
	// deterministic: sort if contains commas?
	if strings.Contains(outStr, ",") {
		parts := strings.Split(outStr, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		sort.Strings(parts)
		outStr = strings.Join(parts, ",")
	}
	b, _ := json.Marshal(map[string]string{"result": outStr})
	if len(b) > e.desc.ResourceBounds.MaxOutputBytes {
		return failResult(req, start, capability.ErrResourceLimit, "output bound exceeded"), capability.NewError(capability.ErrResourceLimit, "output bound exceeded", nil)
	}
	fin := time.Now().UTC()
	return capability.Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted,
		Outputs: json.RawMessage(b), EvidenceRefs: []string{"evidence://" + req.RequestID},
		StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
	}, nil
}

// WorkspaceInfoExecutor inspects workspace info (read-only).
type WorkspaceInfoExecutor struct{ desc capability.Descriptor }

func NewWorkspaceInfoExecutor() *WorkspaceInfoExecutor {
	return &WorkspaceInfoExecutor{
		desc: capability.Descriptor{
			ID:             "cap.workspace.info",
			Name:           "Workspace Info",
			Version:        "1.0.0",
			Kind:           capability.KindRead,
			Tier:           capability.TierRead,
			InputType:      "workspace_info_request",
			OutputType:     "workspace_info_result",
			Timeout:        2 * time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 2048, MaxOutputBytes: 4096},
			Mutating:       false, Native: false, Enabled: true, Provenance: "builtin", Provider: "reference",
		},
	}
}
func (e *WorkspaceInfoExecutor) Describe() capability.Descriptor { return e.desc }
func (e *WorkspaceInfoExecutor) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	out := map[string]any{"workspace_id": req.WorkspaceID, "capability_id": req.CapabilityID, "tier": string(e.desc.Tier)}
	b, _ := json.Marshal(out)
	fin := time.Now().UTC()
	return capability.Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted,
		Outputs: json.RawMessage(b), EvidenceRefs: []string{"evidence://" + req.WorkspaceID},
		StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
	}, nil
}

// BoundedWriteExecutor is T2 local bounded write (mutating local but not canonical until admission).
type BoundedWriteExecutor struct{ desc capability.Descriptor }

func NewBoundedWriteExecutor() *BoundedWriteExecutor {
	return &BoundedWriteExecutor{
		desc: capability.Descriptor{
			ID:             "cap.file.write_bounded",
			Name:           "Bounded File Write",
			Version:        "1.0.0",
			Kind:           capability.KindFile,
			Tier:           capability.TierBounded,
			InputType:      "write_request",
			OutputType:     "write_result",
			Timeout:        2 * time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 8192, MaxOutputBytes: 4096},
			Mutating:       true, Native: false, Enabled: true, Provenance: "builtin", Provider: "reference",
		},
	}
}
func (e *BoundedWriteExecutor) Describe() capability.Descriptor { return e.desc }
func (e *BoundedWriteExecutor) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	var in map[string]any
	if req.Inputs != nil {
		if err := json.Unmarshal(req.Inputs, &in); err != nil {
			return failResult(req, start, capability.ErrInvalidInput, fmt.Sprintf("invalid_input: %v", err)), capability.NewError(capability.ErrInvalidInput, fmt.Sprintf("invalid input: %v", err), err)
		}
	}
	pathVal, _ := in["path"].(string)
	contentVal, _ := in["content"].(string)
	if strings.TrimSpace(pathVal) == "" {
		return failResult(req, start, capability.ErrInvalidInput, "path required"), capability.NewError(capability.ErrInvalidInput, "path required", nil)
	}
	if err := validateScopedPath(pathVal); err != nil {
		return failResult(req, start, capability.ErrInvalidInput, err.Error()), capability.NewError(capability.ErrInvalidInput, err.Error(), nil)
	}
	if len(contentVal) > 4096 {
		return failResult(req, start, capability.ErrResourceLimit, "content exceeds bound"), capability.NewError(capability.ErrResourceLimit, "content exceeds bound", nil)
	}
	out := map[string]any{"path": pathVal, "written": true, "size": len(contentVal), "proposed_transition": map[string]string{"path": pathVal}}
	b, _ := json.Marshal(out)
	// Encode proposed transition for mutating path
	pt := map[string]any{"path": pathVal, "content": contentVal}
	ptb, _ := json.Marshal(pt)
	fin := time.Now().UTC()
	return capability.Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted,
		Outputs: json.RawMessage(b), EvidenceRefs: []string{"evidence://" + req.RequestID},
		StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
		ProposedTransition: json.RawMessage(ptb),
	}, nil
}

// ProcessAllowlistExecutor enforces allowlist, no shell.
type ProcessAllowlistExecutor struct {
	desc      capability.Descriptor
	allowlist map[string]bool
}

func NewProcessAllowlistExecutor() *ProcessAllowlistExecutor {
	return &ProcessAllowlistExecutor{
		desc: capability.Descriptor{
			ID:             "cap.process.echo",
			Name:           "Process Echo (allowlist)",
			Version:        "1.0.0",
			Kind:           capability.KindProcess,
			Tier:           capability.TierBounded,
			InputType:      "process_request",
			OutputType:     "process_result",
			Timeout:        2 * time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 4096, MaxOutputBytes: 8192},
			Mutating:       false, Native: false, Enabled: true, Provenance: "builtin", Provider: "reference",
		},
		allowlist: map[string]bool{"echo": true, "cat": true},
	}
}
func (e *ProcessAllowlistExecutor) Describe() capability.Descriptor { return e.desc }
func (e *ProcessAllowlistExecutor) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	var in map[string]any
	if req.Inputs != nil {
		_ = json.Unmarshal(req.Inputs, &in)
	}
	bin, _ := in["binary"].(string)
	argsRaw, _ := in["args"].([]any)
	shell, _ := in["shell"].(string)
	if strings.TrimSpace(shell) != "" {
		// reject arbitrary shell
		return failResult(req, start, capability.ErrCapabilityDenied, "shell execution not allowed"), capability.NewError(capability.ErrCapabilityDenied, "shell execution not allowed", nil)
	}
	if strings.TrimSpace(bin) == "" {
		return failResult(req, start, capability.ErrInvalidInput, "binary required"), capability.NewError(capability.ErrInvalidInput, "binary required", nil)
	}
	if !e.allowlist[bin] {
		return failResult(req, start, capability.ErrCapabilityDenied, fmt.Sprintf("binary %q not allowlisted", bin)), capability.NewError(capability.ErrCapabilityDenied, fmt.Sprintf("binary %q not allowlisted", bin), nil)
	}
	// Simulate execution without actual exec for determinism: join args
	argStrs := []string{}
	for _, a := range argsRaw {
		argStrs = append(argStrs, fmt.Sprintf("%v", a))
	}
	// bounded output
	output := bin + " " + strings.Join(argStrs, " ")
	if len(output) > e.desc.ResourceBounds.MaxOutputBytes {
		return failResult(req, start, capability.ErrResourceLimit, "output bound exceeded"), capability.NewError(capability.ErrResourceLimit, "output bound exceeded", nil)
	}
	// Respect ctx timeout simulation
	select {
	case <-ctx.Done():
		return failResult(req, start, capability.ErrTimeout, "timeout"), capability.NewError(capability.ErrTimeout, "timeout", ctx.Err())
	default:
	}
	out := map[string]any{"binary": bin, "args": argStrs, "output": output, "allowlist": true}
	b, _ := json.Marshal(out)
	fin := time.Now().UTC()
	return capability.Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted,
		Outputs: json.RawMessage(b), EvidenceRefs: []string{"evidence://" + req.RequestID},
		StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
	}, nil
}

// SleepExecutor for timeout testing.
type SleepExecutor struct{ desc capability.Descriptor }

func NewSleepExecutor() *SleepExecutor {
	return &SleepExecutor{
		desc: capability.Descriptor{
			ID:             "cap.test.sleep",
			Name:           "Sleep Test",
			Version:        "1.0.0",
			Kind:           capability.KindTransform,
			Tier:           capability.TierRead,
			InputType:      "sleep_request",
			OutputType:     "sleep_result",
			Timeout:        100 * time.Millisecond,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 2048, MaxOutputBytes: 2048},
			Mutating:       false, Native: false, Enabled: true, Provenance: "test", Provider: "test",
		},
	}
}
func (e *SleepExecutor) Describe() capability.Descriptor { return e.desc }
func (e *SleepExecutor) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	var in map[string]any
	_ = json.Unmarshal(req.Inputs, &in)
	ms := 200 // default exceed timeout
	if v, ok := in["sleep_ms"].(float64); ok {
		ms = int(v)
	}
	select {
	case <-time.After(time.Duration(ms) * time.Millisecond):
		b, _ := json.Marshal(map[string]any{"slept_ms": ms})
		fin := time.Now().UTC()
		return capability.Result{
			RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted,
			Outputs: json.RawMessage(b), StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
		}, nil
	case <-ctx.Done():
		return failResult(req, start, capability.ErrTimeout, "timeout"), capability.NewError(capability.ErrTimeout, "execution timeout", ctx.Err())
	}
}

// LargeOutputExecutor for output bound test.
type LargeOutputExecutor struct{ desc capability.Descriptor }

func NewLargeOutputExecutor() *LargeOutputExecutor {
	return &LargeOutputExecutor{
		desc: capability.Descriptor{
			ID:             "cap.test.large_output",
			Name:           "Large Output",
			Version:        "1.0.0",
			Kind:           capability.KindTransform,
			Tier:           capability.TierRead,
			InputType:      "large_request",
			OutputType:     "large_result",
			Timeout:        2 * time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 2048, MaxOutputBytes: 64},
			Mutating:       false, Native: false, Enabled: true, Provenance: "test", Provider: "test",
		},
	}
}
func (e *LargeOutputExecutor) Describe() capability.Descriptor { return e.desc }
func (e *LargeOutputExecutor) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	start := time.Now().UTC()
	big := strings.Repeat("A", 200)
	b, _ := json.Marshal(map[string]string{"data": big})
	fin := time.Now().UTC()
	return capability.Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted,
		Outputs: json.RawMessage(b), StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
	}, nil
}

func failResult(req capability.Request, start time.Time, code, msg string) capability.Result {
	fin := time.Now().UTC()
	return capability.Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultFailed,
		StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
		Errors: []string{code + ": " + msg},
	}
}

func validateScopedPath(p string) error {
	if p == "" {
		return fmt.Errorf("invalid_input: path empty")
	}
	if filepath.IsAbs(p) {
		return fmt.Errorf("invalid_input: absolute path %q rejected", p)
	}
	cleaned := filepath.Clean(p)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) || cleaned == ".." {
		return fmt.Errorf("invalid_input: traversal %q rejected", p)
	}
	if strings.Contains(cleaned, "..") {
		// additional check for ../ segments anywhere
		parts := strings.Split(cleaned, string(filepath.Separator))
		for _, part := range parts {
			if part == ".." {
				return fmt.Errorf("invalid_input: traversal %q rejected", p)
			}
		}
	}
	if strings.Contains(p, "\x00") {
		return fmt.Errorf("invalid_input: null byte rejected")
	}
	return nil
}
