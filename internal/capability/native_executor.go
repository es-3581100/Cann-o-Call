package capability

import (
	"context"
	"encoding/json"
	"time"

	"flatten-workspace/internal/capability/native"
)

// NativeExecutor wraps the membrane as a Capability Executor.
type NativeExecutor struct {
	desc     Descriptor
	membrane *native.Membrane
}

func NewNativeExecutor(m *native.Membrane) *NativeExecutor {
	if m == nil {
		m = native.DefaultMembrane()
	}
	return &NativeExecutor{
		desc: Descriptor{
			ID:             "cap.native.upper",
			Name:           "Native Upper (membrane)",
			Version:        "1.0.0",
			Kind:           KindNative,
			Tier:           TierPrivileged,
			InputType:      "native_upper_request",
			OutputType:     "native_upper_result",
			Timeout:        2 * time.Second,
			ResourceBounds: ResourceBounds{MaxInputBytes: 4096, MaxOutputBytes: 4096},
			Mutating:       false, Native: true, Enabled: true, Provenance: "native-membrane", Provider: "purego-membrane",
		},
		membrane: m,
	}
}

func (e *NativeExecutor) Describe() Descriptor { return e.desc }

func (e *NativeExecutor) Execute(ctx context.Context, req Request) (Result, error) {
	start := time.Now().UTC()
	// Input expected: {"library":"libstring","symbol":"upper","input":{"text":"hello"}}
	var wrapper map[string]json.RawMessage
	if req.Inputs != nil {
		_ = json.Unmarshal(req.Inputs, &wrapper)
	}
	// If wrapper contains library/symbol, use directly, else default to libstring.upper with text
	lib := "libstring"
	sym := "upper"
	var inner json.RawMessage
	if v, ok := wrapper["library"]; ok {
		var s string
		_ = json.Unmarshal(v, &s)
		if s != "" {
			lib = s
		}
	}
	if v, ok := wrapper["symbol"]; ok {
		var s string
		_ = json.Unmarshal(v, &s)
		if s != "" {
			sym = s
		}
	}
	if v, ok := wrapper["input"]; ok {
		inner = v
	} else {
		inner = req.Inputs
	}
	if inner == nil {
		inner = json.RawMessage(`{}`)
	}
	invokeReq := native.InvokeRequest{Library: lib, Symbol: sym, Input: inner}
	res, err := e.membrane.Invoke(ctx, invokeReq)
	if err != nil {
		fin := time.Now().UTC()
		return Result{
			RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: ResultFailed,
			StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
			Errors: []string{err.Error()},
		}, err
	}
	fin := time.Now().UTC()
	return Result{
		RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: ResultCompleted,
		Outputs: res.Output, EvidenceRefs: []string{"evidence://" + req.RequestID},
		StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
	}, nil
}
