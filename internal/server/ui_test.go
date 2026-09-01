package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"flatten-workspace/internal/studio"
)

func TestUIHomeRendersStudioThemeAndCommandCenter(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("RUST_LEDGER_URL", "")

	server := New()
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ui", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("GET /ui status = %d, want %d", response.Code, http.StatusOK)
	}

	body := response.Body.String()
	for _, expected := range []string{
		"--void:" + studioTheme.Void,
		"ranger-grid",
		"01 · Categories",
		"evidence_provenance",
		"source_nodes",
		"Details · Shared Studio ViewModel",
		"/api/studio",
		"Terminal-first Go Studio",
		"generated_at",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("GET /ui response missing %q", expected)
		}
	}
}

func TestStudioEndpointReturnsSharedViewModel(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("RUST_LEDGER_URL", "")
	s := New()
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/studio", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/studio status = %d", response.Code)
	}
	body := response.Body.String()
	for _, expected := range []string{"\"generated_at\"", "\"health\"", "\"tasks\"", "\"ranger\"", "\"unavailable\""} {
		if !strings.Contains(body, expected) {
			t.Errorf("Studio response missing %s", expected)
		}
	}
	var vm studio.ViewModel
	if err := json.Unmarshal(response.Body.Bytes(), &vm); err != nil {
		t.Fatalf("decode Studio ViewModel: %v", err)
	}
	if !vm.Health.Available || vm.Health.State != "ok" {
		t.Fatalf("unexpected Studio health: %+v", vm.Health)
	}
	wantCategories := []string{
		studio.RangerCategoryActors,
		studio.RangerCategoryEvents,
		studio.RangerCategoryEvidenceProvenance,
		studio.RangerCategoryFiles,
		studio.RangerCategoryGraphNodes,
		studio.RangerCategoryReceipts,
		studio.RangerCategorySourceNodes,
		studio.RangerCategoryTasks,
		studio.RangerCategoryWorkspaces,
	}
	if len(vm.Ranger.Groups) != len(wantCategories) {
		t.Fatalf("Ranger group count = %d, want %d", len(vm.Ranger.Groups), len(wantCategories))
	}
	for i, category := range wantCategories {
		if vm.Ranger.Groups[i].Category != category {
			t.Errorf("Ranger group %d = %q, want %q", i, vm.Ranger.Groups[i].Category, category)
		}
		if vm.Ranger.Groups[i].State == "" {
			t.Errorf("Ranger group %q has no state", category)
		}
	}
}
