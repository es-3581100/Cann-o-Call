package studio

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"flatten-workspace/internal/progress"
)

func TestViewModelJSONAndDeterministicRender(t *testing.T) {
	vm := ViewModel{
		GeneratedAt: time.Unix(1700000000, 0).UTC(),
		Health:      Health{Available: true, State: "ok"},
		Tasks:       TaskData{Packets: []TaskSummary{{TaskID: "task-b", Operation: progress.OperationQuery, Phase: progress.PhaseRunning, Status: progress.StatusRunning}, {TaskID: "task-a", Operation: progress.OperationIngest, Phase: progress.PhaseTerminal, Status: progress.StatusComplete}}},
		Ranger:      Ranger{Groups: []RangerGroup{{Category: RangerCategoryWorkspaces, Entries: []RangerEntry{{ID: "workspace-b", Label: "workspace-b", Kind: "workspace", Category: RangerCategoryWorkspaces}, {ID: "workspace-a", Label: "workspace-a", Kind: "workspace", Category: RangerCategoryWorkspaces}}}}},
		Unavailable: []Unavailable{{Category: "scoring", Diagnostic: "no scoring collector is exposed"}},
	}
	vm.Normalize()
	b, err := json.Marshal(vm)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b) {
		t.Fatal("ViewModel JSON is invalid")
	}
	if strings.Contains(string(b), "content") {
		t.Fatalf("snapshot unexpectedly contains content: %s", b)
	}
	first, second := RenderOnce(vm), RenderOnce(vm)
	if first != second {
		t.Fatalf("render is not deterministic:\n%s\n---\n%s", first, second)
	}
	for _, expected := range []string{"STUDIO · MONITOR", "Scoring: unavailable", "STUDIO · RANGER", "workspace-a", "source_nodes", "evidence_provenance"} {
		if !strings.Contains(first, expected) {
			t.Errorf("render missing %q", expected)
		}
	}
}

func TestRangerNormalizeIncludesRequiredCategoriesAndSortsEntries(t *testing.T) {
	vm := ViewModel{Ranger: Ranger{Groups: []RangerGroup{
		{Category: RangerCategoryWorkspaces, Entries: []RangerEntry{{ID: "workspace-z", Label: "z"}, {ID: "workspace-a", Label: "a"}}},
		{Category: RangerCategoryEvents, State: "unavailable", Diagnostic: "event log unavailable"},
	}}}
	vm.Normalize()
	want := []string{
		RangerCategoryActors, RangerCategoryEvents, RangerCategoryEvidenceProvenance,
		RangerCategoryFiles, RangerCategoryGraphNodes, RangerCategoryReceipts,
		RangerCategorySourceNodes, RangerCategoryTasks, RangerCategoryWorkspaces,
	}
	if len(vm.Ranger.Groups) != len(want) {
		t.Fatalf("Ranger groups = %d, want %d", len(vm.Ranger.Groups), len(want))
	}
	for i, category := range want {
		group := vm.Ranger.Groups[i]
		if group.Category != category {
			t.Errorf("group %d = %q, want %q", i, group.Category, category)
		}
		if group.State == "" || (len(group.Entries) == 0 && group.Diagnostic == "") {
			t.Errorf("group %q missing explicit empty/unavailable state: %+v", category, group)
		}
	}
	entries := vm.Ranger.Groups[len(vm.Ranger.Groups)-1].Entries
	if entries[0].ID != "workspace-a" || entries[1].ID != "workspace-z" {
		t.Fatalf("workspace entries not sorted: %+v", entries)
	}
}
