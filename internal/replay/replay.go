package replay

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/projection"
	"flatten-workspace/internal/transition"
)

var mutationActions = map[string]bool{
	"build_ledger.state.updated":        true,
	"build_ledger.event.appended":       true,
	"build_ledger.run.created":          true,
	"build_ledger.receipt.created":      true,
	"build_ledger.verification.created": true,
}

func BuildLedgerProjectionFromEvents(
	events []eventlog.Event,
	workspaceID string,
) (projection.Projection, error) {
	files := map[string][]byte{}

	for _, ev := range events {
		action, details, eventWorkspaceID, ok := legacyTransition(ev)
		if !ok || eventWorkspaceID != workspaceID {
			continue
		}

		switch action {
		case "build_ledger.baseline.recorded":
			if rawFiles, ok := details["files"].(map[string]any); ok {
				for p, raw := range rawFiles {
					b64, ok := raw.(string)
					if !ok {
						continue
					}

					if !strings.HasPrefix(p, "build-ledger/") {
						continue
					}

					data, err := base64.StdEncoding.DecodeString(b64)
					if err != nil {
						continue
					}

					files[p] = data
				}
			}

		default:
			if !mutationActions[action] {
				continue
			}

			p, _ := details["path"].(string)
			b64, _ := details["content_base64"].(string)

			if p == "" || b64 == "" {
				continue
			}

			if !strings.HasPrefix(p, "build-ledger/") {
				continue
			}

			data, err := base64.StdEncoding.DecodeString(b64)
			if err != nil {
				continue
			}

			files[p] = data
		}
	}

	return projection.BuildFromFiles(files), nil
}

// legacyTransition preserves CHUNK-02 event replay while decoding the typed
// authority envelope used by new server mutations. Opaque low-level events
// have neither form and therefore cannot change this projection.
func legacyTransition(ev eventlog.Event) (action string, details map[string]any, workspaceID string, ok bool) {
	if ev.Type != "transition.authority.accepted" {
		if ev.Status != "accepted" {
			return "", nil, "", false
		}
		return ev.Action, ev.Details, ev.WorkspaceID, true
	}
	if ev.Status != "accepted" || ev.Action != "transition.accepted" {
		return "", nil, "", false
	}
	raw, exists := ev.Details["accepted_transition"]
	if !exists {
		return "", nil, "", false
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		return "", nil, "", false
	}
	var accepted transition.AcceptedTransition
	if err := json.Unmarshal(payload, &accepted); err != nil {
		return "", nil, "", false
	}
	var result struct {
		Action  string         `json:"legacy_action"`
		Details map[string]any `json:"legacy_details"`
	}
	if err := json.Unmarshal(accepted.Proposal.ResultData, &result); err != nil || result.Action == "" {
		return "", nil, "", false
	}
	return result.Action, result.Details, accepted.Proposal.Entity, true
}
