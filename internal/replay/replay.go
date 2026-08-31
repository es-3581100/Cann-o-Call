package replay

import (
	"encoding/base64"
	"strings"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/projection"
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
		if ev.WorkspaceID != workspaceID {
			continue
		}

		if ev.Status != "accepted" {
			continue
		}

		switch ev.Action {
		case "build_ledger.baseline.recorded":
			if rawFiles, ok := ev.Details["files"].(map[string]any); ok {
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
			if !mutationActions[ev.Action] {
				continue
			}

			p, _ := ev.Details["path"].(string)
			b64, _ := ev.Details["content_base64"].(string)

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
