package server

import (
	"net/http"
	"sort"
	"time"

	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/observability"
	"flatten-workspace/internal/progress"
	"flatten-workspace/internal/studio"
	"flatten-workspace/internal/workspace"
)

// studioSnapshot projects existing collector state without exercising authority
// or creating a second source of truth.
func (s *Server) studioSnapshot() studio.ViewModel {
	ledger := observability.LedgerMetricsFromService(s.Events, s.DataDir)
	actors := observability.ActorMetricsFromController(s.Actors)
	s.muOrch.RLock()
	graphMetrics := observability.GraphMetricsAggregate(s.graphStores)
	contextMetrics := observability.ContextMetricsAggregate(s.orchestrators)
	s.muOrch.RUnlock()

	packets := make([]progress.ProgressPacket, 0)
	if s.Tasks != nil {
		for _, task := range s.Tasks.List() {
			packets = append(packets, task.Packet)
		}
	}
	progressReceipts := []progress.ReceiptPacket{}
	if s.ProgressReceipts != nil {
		progressReceipts = s.ProgressReceipts.List()
	}
	taskSummaries := studio.TaskSummaries(packets)
	progressReceiptSummaries := studio.ProgressReceiptSummaries(progressReceipts)
	receipts := studio.ReceiptSummaries(nil)
	if s.Receipts != nil {
		receipts = studio.ReceiptSummaries(s.Receipts.List())
	}

	groups := newRangerGroups()
	summaries := s.store.Summaries()
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].ID < summaries[j].ID })
	ranger := studio.Ranger{}
	for _, summary := range summaries {
		addRangerEntry(groups, studio.RangerEntry{
			ID:          "workspace:" + summary.ID,
			Label:       summary.ID,
			Kind:        "workspace",
			Category:    studio.RangerCategoryWorkspaces,
			WorkspaceID: summary.ID,
			Metadata: studio.RangerMetadata{
				Format: summary.Format, FileCount: summary.FileCount, DirectoryCount: summary.DirectoryCount,
			},
		})
		if ws, ok := s.store.Get(summary.ID); ok {
			tree := ws.TreeNodes()
			if ranger.InitialWorkspaceID == "" {
				ranger.InitialWorkspaceID = summary.ID
				ranger.InitialTree = tree
			}
			forEachTreeNode(tree, func(node workspace.TreeNode) {
				addRangerEntry(groups, studio.RangerEntry{
					ID:          "file:" + summary.ID + ":" + node.Path,
					Label:       node.Path,
					Kind:        node.Type,
					Category:    studio.RangerCategoryFiles,
					WorkspaceID: summary.ID,
					Metadata: studio.RangerMetadata{
						Path:   node.Path,
						SHA256: node.SHA256,
						Size:   node.Size,
					},
				})
				if node.Type == "file" {
					// Workspace tree data is provenance metadata only: no file bytes
					// are copied into the shared Ranger projection.
					addRangerEntry(groups, studio.RangerEntry{
						ID:          "evidence:workspace-file:" + summary.ID + ":" + node.Path,
						Label:       node.Path,
						Kind:        "workspace_file_metadata",
						Category:    studio.RangerCategoryEvidenceProvenance,
						WorkspaceID: summary.ID,
						Metadata: studio.RangerMetadata{
							Path:   node.Path,
							SHA256: node.SHA256,
							Size:   node.Size,
						},
					})
				}
			})
		}
	}

	s.muOrch.RLock()
	stores := make(map[string]*graph.Store, len(s.graphStores))
	for workspaceID, store := range s.graphStores {
		stores[workspaceID] = store
	}
	s.muOrch.RUnlock()
	storeIDs := make([]string, 0, len(stores))
	for workspaceID := range stores {
		storeIDs = append(storeIDs, workspaceID)
	}
	sort.Strings(storeIDs)
	for _, workspaceID := range storeIDs {
		store := stores[workspaceID]
		for _, packet := range store.Packets() {
			identity := packet.Identity
			label := identity.SourceRef
			if label == "" {
				label = identity.SourceID
			}
			addRangerEntry(groups, studio.RangerEntry{
				ID:          identity.SourceID,
				Label:       label,
				Kind:        identity.SourceType,
				Category:    studio.RangerCategorySourceNodes,
				WorkspaceID: identity.WorkspaceID,
				Metadata:    studio.RangerMetadata{Path: identity.SourceRef, SHA256: identity.SourceSHA256},
			})
		}
		graphSnapshot := store.Graph()
		nodeIDs := make([]string, 0, len(graphSnapshot.Nodes))
		for nodeID := range graphSnapshot.Nodes {
			nodeIDs = append(nodeIDs, nodeID)
		}
		sort.Strings(nodeIDs)
		for _, nodeID := range nodeIDs {
			node := graphSnapshot.Nodes[nodeID]
			label := node.SourceLocator
			if label == "" {
				label = node.NodeID
			}
			addRangerEntry(groups, studio.RangerEntry{
				ID:          node.NodeID,
				Label:       label,
				Kind:        node.NodeType,
				Category:    studio.RangerCategoryGraphNodes,
				WorkspaceID: node.Provenance.WorkspaceID,
				Metadata:    studio.RangerMetadata{Path: node.SourceLocator, SHA256: node.SourceSHA256, ReferenceID: node.SourceID},
			})
		}
	}

	if s.Actors == nil {
		setRangerUnavailable(groups, studio.RangerCategoryActors, "actor collector is not attached")
	} else {
		for _, actor := range s.Actors.List("") {
			addRangerEntry(groups, studio.RangerEntry{
				ID:          actor.ID,
				Label:       actor.ID,
				Kind:        "actor",
				Category:    studio.RangerCategoryActors,
				WorkspaceID: actor.WorkspaceID,
				Metadata:    studio.RangerMetadata{Status: actor.Status},
			})
		}
	}

	if s.Events == nil {
		setRangerUnavailable(groups, studio.RangerCategoryEvents, "event collector is not attached")
	} else if events, err := s.Events.List(); err != nil {
		// List is explicitly best-effort. Do not invent an event when its source
		// cannot be read.
		setRangerUnavailable(groups, studio.RangerCategoryEvents, "event collector could not list events")
	} else {
		for _, event := range events {
			addRangerEntry(groups, studio.RangerEntry{
				ID:          event.ID,
				Label:       event.ID,
				Kind:        "event",
				Category:    studio.RangerCategoryEvents,
				WorkspaceID: event.WorkspaceID,
				Metadata:    studio.RangerMetadata{Sequence: event.Seq, Action: event.Action, Status: event.Status, EventType: event.Type},
			})
			if event.ReceiptID != "" {
				addRangerEntry(groups, studio.RangerEntry{
					ID:          "evidence:event-receipt:" + event.ID + ":" + event.ReceiptID,
					Label:       event.ReceiptID,
					Kind:        "event_receipt_reference",
					Category:    studio.RangerCategoryEvidenceProvenance,
					WorkspaceID: event.WorkspaceID,
					Metadata:    studio.RangerMetadata{Sequence: event.Seq, ReferenceID: event.ID},
				})
			}
		}
	}

	if s.Receipts == nil {
		setRangerUnavailable(groups, studio.RangerCategoryReceipts, "receipt collector is not attached")
	} else {
		for _, receipt := range receipts {
			addRangerEntry(groups, studio.RangerEntry{
				ID:          "receipt:" + receipt.ID,
				Label:       receipt.ID,
				Kind:        "receipt",
				Category:    studio.RangerCategoryReceipts,
				WorkspaceID: receipt.WorkspaceID,
				Metadata:    studio.RangerMetadata{Sequence: receipt.Seq, Action: receipt.Action, Status: receipt.Status, ReferenceID: receipt.EventID},
			})
			if receipt.EventID != "" {
				addRangerEntry(groups, studio.RangerEntry{
					ID:          "evidence:receipt-event:" + receipt.ID + ":" + receipt.EventID,
					Label:       receipt.EventID,
					Kind:        "receipt_event_reference",
					Category:    studio.RangerCategoryEvidenceProvenance,
					WorkspaceID: receipt.WorkspaceID,
					Metadata:    studio.RangerMetadata{Sequence: receipt.Seq, ReferenceID: receipt.ID},
				})
			}
		}
	}
	if s.ProgressReceipts == nil {
		setRangerUnavailable(groups, studio.RangerCategoryReceipts, "progress receipt collector is not attached")
	} else {
		for _, receipt := range progressReceiptSummaries {
			addRangerEntry(groups, studio.RangerEntry{
				ID:       "progress-receipt:" + receipt.TaskID,
				Label:    receipt.TaskID,
				Kind:     "progress_receipt",
				Category: studio.RangerCategoryReceipts,
				Metadata: studio.RangerMetadata{Sequence: receipt.HeadSequence, Action: string(receipt.Operation), Status: string(receipt.FinalStatus), ReferenceID: receipt.ResultID},
			})
			if receipt.HeadEventID != "" {
				addRangerEntry(groups, studio.RangerEntry{
					ID:       "evidence:progress-receipt-head:" + receipt.TaskID + ":" + receipt.HeadEventID,
					Label:    receipt.HeadEventID,
					Kind:     "progress_receipt_head_reference",
					Category: studio.RangerCategoryEvidenceProvenance,
					Metadata: studio.RangerMetadata{Sequence: receipt.HeadSequence, SHA256: receipt.HeadHash, ReferenceID: receipt.TaskID},
				})
			}
		}
	}

	if s.Tasks == nil {
		setRangerUnavailable(groups, studio.RangerCategoryTasks, "task collector is not attached")
	} else {
		for _, packet := range taskSummaries {
			addRangerEntry(groups, studio.RangerEntry{
				ID:       packet.TaskID,
				Label:    packet.TaskID,
				Kind:     "task",
				Category: studio.RangerCategoryTasks,
				Metadata: studio.RangerMetadata{Status: string(packet.Status)},
			})
		}
	}
	for _, group := range groups {
		ranger.Groups = append(ranger.Groups, *group)
	}

	vm := studio.ViewModel{
		GeneratedAt:      time.Now().UTC(),
		Health:           studio.Health{Available: true, State: "ok"},
		Capabilities:     []studio.Capability{},
		Ledger:           ledger,
		Actors:           actors,
		Graph:            graphMetrics,
		Context:          studio.ContextSummaryFromMetrics(contextMetrics),
		Tasks:            studio.TaskData{Packets: taskSummaries},
		ProgressReceipts: studio.ProgressReceiptData{Packets: progressReceiptSummaries},
		Receipts:         studio.ReceiptData{Packets: receipts},
		Ranger:           ranger,
		Unavailable: []studio.Unavailable{
			{Category: "capabilities", Diagnostic: "no capability registry is attached to this server"},
			{Category: "graph_topology", Diagnostic: "only aggregate graph metrics are exposed"},
			{Category: "scoring", Diagnostic: "no scoring collector is exposed"},
		},
	}
	vm.Normalize()
	return vm
}

func newRangerGroups() map[string]*studio.RangerGroup {
	groups := map[string]*studio.RangerGroup{}
	for _, category := range []string{
		studio.RangerCategoryActors,
		studio.RangerCategoryEvidenceProvenance,
		studio.RangerCategoryEvents,
		studio.RangerCategoryFiles,
		studio.RangerCategoryGraphNodes,
		studio.RangerCategoryReceipts,
		studio.RangerCategorySourceNodes,
		studio.RangerCategoryTasks,
		studio.RangerCategoryWorkspaces,
	} {
		groups[category] = &studio.RangerGroup{Category: category, State: "empty", Diagnostic: "no records are currently available", Entries: []studio.RangerEntry{}}
	}
	return groups
}

func addRangerEntry(groups map[string]*studio.RangerGroup, entry studio.RangerEntry) {
	group := groups[entry.Category]
	if group == nil {
		return
	}
	group.Entries = append(group.Entries, entry)
	group.State = "available"
	group.Diagnostic = ""
}

func setRangerUnavailable(groups map[string]*studio.RangerGroup, category, diagnostic string) {
	group := groups[category]
	if group == nil || len(group.Entries) > 0 {
		return
	}
	group.State = "unavailable"
	group.Diagnostic = diagnostic
}

func forEachTreeNode(nodes []workspace.TreeNode, visit func(workspace.TreeNode)) {
	for _, node := range nodes {
		visit(node)
		forEachTreeNode(node.Children, visit)
	}
}

func (s *Server) handleStudio(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.studioSnapshot())
}
