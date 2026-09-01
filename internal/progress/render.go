package progress

import (
	"fmt"
	"strings"
	"time"
)

// RenderProgress returns a single-line terminal progress string without Python deps.
// Format: "[operation taskID] phase status  completed/total (pct%) elapsed Xs throughput Y/s ETA Zs | actor a/c/f/s graph n/e ctx sel/coverage"
func RenderProgress(p ProgressPacket) string {
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(string(p.Operation))
	b.WriteString(" ")
	// short task id for display: first 8 chars if hex long
	id := p.TaskID
	if len(id) > 12 {
		id = id[:8]
	}
	b.WriteString(id)
	b.WriteString("] ")
	b.WriteString(string(p.Phase))
	b.WriteString(" ")
	b.WriteString(string(p.Status))

	if p.Completed != nil && p.Total != nil && *p.Total > 0 {
		pct := float64(*p.Completed) / float64(*p.Total) * 100
		fmt.Fprintf(&b, " %d/%d (%.1f%%)", *p.Completed, *p.Total, pct)
	} else if p.Completed != nil {
		fmt.Fprintf(&b, " %d", *p.Completed)
		if p.Total != nil {
			fmt.Fprintf(&b, "/%d", *p.Total)
		}
	} else if p.Total != nil {
		fmt.Fprintf(&b, " 0/%d (0.0%%)", *p.Total)
	}

	elapsed := p.Elapsed().Truncate(time.Millisecond)
	fmt.Fprintf(&b, " elapsed %s", elapsed.String())

	if tp := p.Throughput; tp != nil && *tp > 0 {
		fmt.Fprintf(&b, " throughput %.2f/s", *tp)
	} else if tp2 := p.ComputeThroughput(); tp2 != nil && *tp2 > 0 {
		fmt.Fprintf(&b, " throughput %.2f/s", *tp2)
	}

	if eta := p.ETA; eta != nil {
		fmt.Fprintf(&b, " ETA %s", *eta)
	} else if eta2 := p.ComputeETA(); eta2 != nil {
		fmt.Fprintf(&b, " ETA %s", *eta2)
	}

	// bounded actor summary
	if p.Actor.Active != 0 || p.Actor.Completed != 0 || p.Actor.Failed != 0 || p.Actor.Suppressed != 0 || p.Actor.Rejected != 0 {
		fmt.Fprintf(&b, " actor active=%d completed=%d failed=%d suppressed=%d", p.Actor.Active, p.Actor.Completed, p.Actor.Failed, p.Actor.Suppressed+p.Actor.Rejected)
	}
	if p.Graph.NodeCount != 0 || p.Graph.EdgeCount != 0 {
		fmt.Fprintf(&b, " graph nodes=%d edges=%d", p.Graph.NodeCount, p.Graph.EdgeCount)
	}
	if p.Context.CandidatesConsidered != 0 || p.Context.SelectedCount != 0 {
		fmt.Fprintf(&b, " ctx candidates=%d selected=%d coverage=%.2f", p.Context.CandidatesConsidered, p.Context.SelectedCount, p.Context.Coverage)
	}
	if len(p.Warnings) > 0 {
		fmt.Fprintf(&b, " warnings=%d", len(p.Warnings))
	}
	if len(p.Errors) > 0 {
		fmt.Fprintf(&b, " errors=%d", len(p.Errors))
	}
	return b.String()
}

// RenderReceipt returns single-line receipt summary.
func RenderReceipt(r ReceiptPacket) string {
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(string(r.Operation))
	b.WriteString(" ")
	id := r.TaskID
	if len(id) > 12 {
		id = id[:8]
	}
	b.WriteString(id)
	b.WriteString("] ")
	b.WriteString(string(r.FinalStatus))
	fmt.Fprintf(&b, " elapsed %s units=%d", r.Elapsed, r.UnitsProcessed)
	if r.ActorsCompleted != 0 || r.ActorsFailed != 0 || r.ActorsSuppressed != 0 {
		fmt.Fprintf(&b, " actors completed=%d failed=%d suppressed=%d", r.ActorsCompleted, r.ActorsFailed, r.ActorsSuppressed)
	}
	if len(r.AcceptedTransitionIDs) > 0 {
		fmt.Fprintf(&b, " transitions=%d", len(r.AcceptedTransitionIDs))
		if len(r.DurableRefs) > 0 {
			fmt.Fprintf(&b, " durable_ack=%d", len(r.DurableRefs))
		}
	}
	if r.HeadSequence != 0 {
		fmt.Fprintf(&b, " head %d", r.HeadSequence)
		if r.HeadHash != "" {
			h := r.HeadHash
			if len(h) > 8 {
				h = h[:8]
			}
			fmt.Fprintf(&b, "/%s", h)
		}
	}
	if len(r.Warnings) > 0 {
		fmt.Fprintf(&b, " warnings=%d", len(r.Warnings))
	}
	if len(r.Errors) > 0 {
		fmt.Fprintf(&b, " errors=%d", len(r.Errors))
	}
	return b.String()
}
