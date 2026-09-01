package scoring

import (
	"sort"
)

// Config bounds candidate selection. Zero values are replaced by defaults.
type Config struct {
	Threshold           float64 `json:"threshold"`
	MaxCandidates       int     `json:"max_candidates"`
	MaxActorsPerRequest int     `json:"max_actors_per_request"`
}

// DefaultConfig returns bounded defaults: threshold 1.0, maxCandidates 10, maxActorsPerRequest 3.
func DefaultConfig() Config {
	return Config{
		Threshold:           1.0,
		MaxCandidates:       10,
		MaxActorsPerRequest: 3,
	}
}

// WithDefaults returns a copy with zero values filled by defaults and clamps to sane bounds.
func (c Config) WithDefaults() Config {
	def := DefaultConfig()
	if c.Threshold == 0 {
		c.Threshold = def.Threshold
	}
	if c.MaxCandidates <= 0 {
		c.MaxCandidates = def.MaxCandidates
	}
	if c.MaxActorsPerRequest <= 0 {
		c.MaxActorsPerRequest = def.MaxActorsPerRequest
	}
	// Clamp to prevent unbounded activation even if caller passes large values.
	if c.MaxCandidates > 100 {
		c.MaxCandidates = 100
	}
	if c.MaxActorsPerRequest > 16 {
		c.MaxActorsPerRequest = 16
	}
	return c
}

// SelectCandidates filters and caps scored nodes deterministically.
// Steps: filter ActivationScore >= threshold, sort desc ActivationScore then NodeID asc,
// apply MaxCandidates cap, then MaxActorsPerRequest cap.
// Deterministic and bounded; never wakes unlimited actors.
func SelectCandidates(scored []ScoredNode, cfg Config) []ScoredNode {
	cfg = cfg.WithDefaults()

	// Filter by threshold.
	filtered := make([]ScoredNode, 0, len(scored))
	for _, s := range scored {
		if s.Components.ActivationScore >= cfg.Threshold {
			filtered = append(filtered, s)
		}
	}

	// Deterministic sort: ActivationScore desc, RawScore desc, NodeID asc.
	sort.Slice(filtered, func(i, j int) bool {
		ai := filtered[i].Components.ActivationScore
		aj := filtered[j].Components.ActivationScore
		if ai == aj {
			ri := filtered[i].Components.RawScore
			rj := filtered[j].Components.RawScore
			if ri == rj {
				return filtered[i].Node.NodeID < filtered[j].Node.NodeID
			}
			return ri > rj
		}
		return ai > aj
	})

	// Apply MaxCandidates cap.
	if len(filtered) > cfg.MaxCandidates {
		filtered = filtered[:cfg.MaxCandidates]
	}
	// Apply MaxActorsPerRequest cap.
	if len(filtered) > cfg.MaxActorsPerRequest {
		filtered = filtered[:cfg.MaxActorsPerRequest]
	}

	// Return copy to prevent caller mutation of internal ordering.
	out := make([]ScoredNode, len(filtered))
	copy(out, filtered)
	return out
}
