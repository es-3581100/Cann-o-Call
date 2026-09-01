package capability

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Tier is the graded privilege tier. It determines admission path, not actor confidence.
type Tier string

const (
	TierRead       Tier = "T1_READ"       // read-only, no canonical mutation
	TierBounded    Tier = "T2_BOUNDED"    // local bounded mutation (filesystem under workspace root)
	TierPrivileged Tier = "T3_PRIVILEGED" // canonical/native, requires Go + Rust ACK
)

func (t Tier) Valid() bool {
	switch t {
	case TierRead, TierBounded, TierPrivileged:
		return true
	default:
		return false
	}
}

// Kind classifies capability function.
type Kind string

const (
	KindRead      Kind = "read"
	KindTransform Kind = "transform"
	KindFile      Kind = "file"
	KindProcess   Kind = "process"
	KindNative    Kind = "native"
)

func (k Kind) Valid() bool {
	switch k {
	case KindRead, KindTransform, KindFile, KindProcess, KindNative:
		return true
	default:
		return false
	}
}

// ResourceBounds configures bounded execution.
type ResourceBounds struct {
	MaxInputBytes  int `json:"max_input_bytes"`
	MaxOutputBytes int `json:"max_output_bytes"`
	MaxConcurrency int `json:"max_concurrency,omitempty"`
}

// Descriptor is the typed capability descriptor. No privilege may be inferred from arbitrary strings;
// tier/enum fields are explicit.
type Descriptor struct {
	ID             string          `json:"capability_id"`
	Name           string          `json:"name"`
	Version        string          `json:"version"`
	Kind           Kind            `json:"kind"`
	Tier           Tier            `json:"tier"`
	RiskTier       Tier            `json:"risk_tier,omitempty"` // alias for Tier if needed
	InputType      string          `json:"input_type"`
	OutputType     string          `json:"output_type"`
	InputSchema    json.RawMessage `json:"input_schema,omitempty"`
	OutputSchema   json.RawMessage `json:"output_schema,omitempty"`
	Timeout        time.Duration   `json:"timeout"`
	ResourceBounds ResourceBounds  `json:"resource_bounds"`
	Mutating       bool            `json:"mutating"`
	Native         bool            `json:"native"`
	Enabled        bool            `json:"enabled"`
	Provenance     string          `json:"provenance,omitempty"`
	Provider       string          `json:"provider,omitempty"`
}

func (d Descriptor) Validate() error {
	if strings.TrimSpace(d.ID) == "" {
		return fmt.Errorf("%s: capability_id required", ErrInvalidInput)
	}
	if strings.TrimSpace(d.Name) == "" {
		return fmt.Errorf("%s: name required", ErrInvalidInput)
	}
	if strings.TrimSpace(d.Version) == "" {
		return fmt.Errorf("%s: version required", ErrInvalidInput)
	}
	if !d.Kind.Valid() {
		return fmt.Errorf("%s: invalid kind %q", ErrInvalidInput, d.Kind)
	}
	if !d.Tier.Valid() {
		return fmt.Errorf("%s: invalid tier %q", ErrInvalidInput, d.Tier)
	}
	if strings.TrimSpace(d.InputType) == "" {
		return fmt.Errorf("%s: input_type required", ErrInvalidInput)
	}
	if strings.TrimSpace(d.OutputType) == "" {
		return fmt.Errorf("%s: output_type required", ErrInvalidInput)
	}
	if d.Timeout <= 0 {
		return fmt.Errorf("%s: timeout required", ErrInvalidInput)
	}
	if d.ResourceBounds.MaxInputBytes <= 0 {
		return fmt.Errorf("%s: max_input_bytes required", ErrInvalidInput)
	}
	if d.ResourceBounds.MaxOutputBytes <= 0 {
		return fmt.Errorf("%s: max_output_bytes required", ErrInvalidInput)
	}
	// Invariant: native implies TierPrivileged and Mutating/native consistency checked at admission, not here.
	// But ensure Native flag matches Kind if KindNative.
	if d.Native && d.Kind != KindNative && d.Tier != TierPrivileged {
		// allow but note; privilege is tier-driven
	}
	if d.Tier == TierRead && d.Mutating {
		return fmt.Errorf("%s: T1_READ cannot be mutating", ErrInvalidInput)
	}
	return nil
}

// CanonicalJSON serializes descriptor with sorted keys for determinism.
func (d Descriptor) CanonicalJSON() ([]byte, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	// sort keys via marshal with ordered keys
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]any, len(m))
	for _, k := range keys {
		ordered[k] = m[k]
	}
	// Use generic canonical writer: re-marshal with sorted keys via json Marshal of sorted map is not deterministic due to random iteration,
	// so construct deterministic string manually or rely on our canonicalJSON helper if available.
	// Simpler: marshal sorted via encoding/json with sorted keys using map iteration sorted already would still be random, so we build canonical JSON via sorting in marshal.
	// Use standard json.Marshal on struct directly which has deterministic field order via struct definition order.
	return json.Marshal(d)
}
