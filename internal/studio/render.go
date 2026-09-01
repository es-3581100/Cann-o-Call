package studio

import (
	"fmt"
	"strconv"
	"strings"
)

// RenderOnce produces a stable, plain-text full Studio snapshot.
func RenderOnce(v ViewModel) string {
	return RenderMonitor(v) + "\n\n" + RenderRanger(v, 0)
}

func RenderMonitor(v ViewModel) string {
	var b strings.Builder
	fmt.Fprintln(&b, "STUDIO · MONITOR")
	fmt.Fprintf(&b, "Generated: %s\n", v.GeneratedAt.UTC().Format("2006-01-02T15:04:05Z"))
	fmt.Fprintf(&b, "Runtime health: %s (%s)\n", availability(v.Health.Available), value(v.Health.State))
	fmt.Fprintf(&b, "Actors: descriptors=%d queued=%d active=%d completed=%d failed=%d\n", v.Actors.DormantDescriptors, v.Actors.Queued, v.Actors.Active, v.Actors.Completed, v.Actors.Failed)
	fmt.Fprintf(&b, "Tasks: %d\n", v.Tasks.Count)
	for _, task := range v.Tasks.Packets {
		fmt.Fprintf(&b, "  %s · %s · %s · %s\n", task.TaskID, task.Operation, task.Phase, task.Status)
	}
	fmt.Fprintf(&b, "Graph: nodes=%d edges=%d sources=%d\n", v.Graph.NodeCount, v.Graph.EdgeCount, v.Graph.SourceCount)
	fmt.Fprintf(&b, "Context: considered=%d selected=%d coverage=%s\n", v.Context.CandidatesConsidered, v.Context.CandidatesSelected, strconv.FormatFloat(v.Context.Coverage, 'f', -1, 64))
	fmt.Fprintln(&b, "Scoring: unavailable (no scoring collector is exposed)")
	fmt.Fprintf(&b, "Ledger: events=%d sequence=%d verification=%s replay=%s\n", v.Ledger.AcceptedEventCount, v.Ledger.CurrentSequence, value(v.Ledger.VerificationStatus), value(v.Ledger.ReplayStatus))
	fmt.Fprintf(&b, "Progress receipts: %d\n", v.ProgressReceipts.Count)
	for _, receipt := range v.ProgressReceipts.Packets {
		fmt.Fprintf(&b, "  %s · %s · %s\n", receipt.TaskID, receipt.Operation, receipt.FinalStatus)
	}
	fmt.Fprintf(&b, "Receipts: %d\n", v.Receipts.Count)
	fmt.Fprintln(&b, "Capabilities:")
	if len(v.Capabilities) == 0 {
		fmt.Fprintln(&b, "  unavailable (no capability registry is attached to this server)")
	} else {
		for _, cap := range v.Capabilities {
			fmt.Fprintf(&b, "  %s · %s · %s · enabled=%t\n", cap.ID, cap.Name, cap.Kind, cap.Enabled)
		}
	}
	for _, unavailable := range v.Unavailable {
		fmt.Fprintf(&b, "Unavailable [%s]: %s\n", unavailable.Category, unavailable.Diagnostic)
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// RenderRanger renders category, entries, and inspector columns without
// terminal-width assumptions, so pipes and --once remain testable.
func RenderRanger(v ViewModel, selected int) string {
	var b strings.Builder
	fmt.Fprintln(&b, "STUDIO · RANGER")
	fmt.Fprintln(&b, "CATEGORY | ENTRIES (COUNT) | SELECTED INSPECTOR")
	entries := []RangerEntry{}
	for _, group := range v.Ranger.Groups {
		fmt.Fprintf(&b, "%s | %d (%s) | %s\n", group.Category, len(group.Entries), value(group.State), value(group.Diagnostic))
		entries = append(entries, group.Entries...)
	}
	if len(entries) == 0 {
		return strings.TrimSuffix(b.String(), "\n")
	}
	if selected < 0 || selected >= len(entries) {
		selected = 0
	}
	for i, entry := range entries {
		marker := " "
		if i == selected {
			marker = ">"
		}
		inspector := ""
		if i == selected {
			inspector = rangerInspector(entry)
		}
		fmt.Fprintf(&b, "%s | %s%d: %s (%s) | %s\n", entry.Category, marker, i, entry.Label, entry.Kind, inspector)
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func rangerInspector(entry RangerEntry) string {
	parts := []string{"id=" + entry.ID}
	if entry.WorkspaceID != "" {
		parts = append(parts, "workspace="+entry.WorkspaceID)
	}
	if entry.Metadata.Path != "" {
		parts = append(parts, "path="+entry.Metadata.Path)
	}
	if entry.Metadata.SHA256 != "" {
		parts = append(parts, "sha256="+entry.Metadata.SHA256)
	}
	if entry.Metadata.Size != 0 {
		parts = append(parts, fmt.Sprintf("size=%d", entry.Metadata.Size))
	}
	if entry.Metadata.Sequence != 0 {
		parts = append(parts, fmt.Sprintf("seq=%d", entry.Metadata.Sequence))
	}
	if entry.Metadata.Action != "" {
		parts = append(parts, "action="+entry.Metadata.Action)
	}
	if entry.Metadata.Status != "" {
		parts = append(parts, "status="+entry.Metadata.Status)
	}
	if entry.Metadata.EventType != "" {
		parts = append(parts, "event_type="+entry.Metadata.EventType)
	}
	if entry.Metadata.ReferenceID != "" {
		parts = append(parts, "reference="+entry.Metadata.ReferenceID)
	}
	return strings.Join(parts, " · ")
}

func availability(ok bool) string {
	if ok {
		return "available"
	}
	return "unavailable"
}
func value(s string) string {
	if s == "" {
		return "unavailable"
	}
	return s
}
