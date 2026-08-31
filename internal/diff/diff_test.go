package diff

import (
	"strings"
	"testing"
)

func TestUnifiedDiff(t *testing.T) {
	oldText := "a\nb\nc"
	newText := "a\nx\nc"

	out := Unified(oldText, newText, "test.yaml")

	if !strings.Contains(out, "-b") {
		t.Fatalf("expected removed line, got:\n%s", out)
	}

	if !strings.Contains(out, "+x") {
		t.Fatalf("expected added line, got:\n%s", out)
	}
}
