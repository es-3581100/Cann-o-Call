package native

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Membrane is the isolated Native/PureGo membrane. It is the sole adapter for native execution.
// Explicit allowlist, explicit I/O types, context timeout, no arbitrary library path, no authority over canonical state.
type Membrane struct {
	allowlist   map[string]map[string]bool
	timeout     time.Duration
	boundOutput int
}

type InvokeRequest struct {
	Library string          `json:"library"`
	Symbol  string          `json:"symbol"`
	Input   json.RawMessage `json:"input"`
}

type InvokeResult struct {
	Output json.RawMessage `json:"output"`
}

func NewMembrane(allowlist map[string][]string, timeout time.Duration, maxOutput int) *Membrane {
	mapped := map[string]map[string]bool{}
	for lib, syms := range allowlist {
		m := map[string]bool{}
		for _, s := range syms {
			m[s] = true
		}
		mapped[lib] = m
	}
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	if maxOutput <= 0 {
		maxOutput = 4096
	}
	return &Membrane{allowlist: mapped, timeout: timeout, boundOutput: maxOutput}
}

func (m *Membrane) MembraneDefined() bool { return m != nil }

func (m *Membrane) IsAllowed(library, symbol string) bool {
	if m == nil {
		return false
	}
	syms, ok := m.allowlist[library]
	if !ok {
		return false
	}
	return syms[symbol]
}

func (m *Membrane) Allowlist() map[string][]string {
	out := map[string][]string{}
	for lib, syms := range m.allowlist {
		list := []string{}
		for s := range syms {
			list = append(list, s)
		}
		out[lib] = list
	}
	return out
}

func (m *Membrane) Invoke(ctx context.Context, req InvokeRequest) (InvokeResult, error) {
	if strings.TrimSpace(req.Library) == "" || strings.TrimSpace(req.Symbol) == "" {
		return InvokeResult{}, fmt.Errorf("invalid_input: library and symbol required")
	}
	if strings.Contains(req.Library, "/") || strings.Contains(req.Library, "..") || strings.Contains(req.Symbol, "/") {
		return InvokeResult{}, fmt.Errorf("capability_denied: library/symbol path rejected")
	}
	if !m.IsAllowed(req.Library, req.Symbol) {
		return InvokeResult{}, fmt.Errorf("native_unavailable: symbol %s/%s not allowlisted", req.Library, req.Symbol)
	}
	tctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	ch := make(chan InvokeResult, 1)
	errCh := make(chan error, 1)
	go func() {
		var out json.RawMessage
		switch req.Library + "." + req.Symbol {
		case "libmath.add":
			var in map[string]float64
			_ = json.Unmarshal(req.Input, &in)
			a := in["a"]
			b := in["b"]
			sum := a + b
			out, _ = json.Marshal(map[string]float64{"result": sum})
		case "libstring.upper":
			var in map[string]string
			_ = json.Unmarshal(req.Input, &in)
			s := strings.ToUpper(in["text"])
			out, _ = json.Marshal(map[string]string{"result": s})
		default:
			out, _ = json.Marshal(map[string]string{"echo": string(req.Input)})
		}
		if len(out) > m.boundOutput {
			errCh <- fmt.Errorf("resource_limit: native output exceeds bound")
			return
		}
		ch <- InvokeResult{Output: out}
	}()

	select {
	case <-tctx.Done():
		return InvokeResult{}, fmt.Errorf("timeout: native timeout: %w", tctx.Err())
	case err := <-errCh:
		return InvokeResult{}, err
	case res := <-ch:
		return res, nil
	}
}

func DefaultMembrane() *Membrane {
	return NewMembrane(map[string][]string{
		"libmath":   {"add"},
		"libstring": {"upper"},
	}, 500*time.Millisecond, 4096)
}

func (m *Membrane) ArbitraryLibraryLoading() bool  { return false }
func (m *Membrane) ArbitrarySymbolExecution() bool { return false }
