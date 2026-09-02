package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestHelpFlagsRouteToCLIWithoutServer(t *testing.T) {
	tests := []struct {
		args []string
		want []string
	}{
		{args: []string{"--help"}, want: []string{"Usage: flatten-workspace"}},
		{args: []string{"-h"}, want: []string{"Usage: flatten-workspace"}},
		{args: []string{"studio", "--help"}, want: []string{"Studio is terminal-first.", "read-only GET /api/studio", "--once", "Interactive allowlisted commands"}},
		{args: []string{"studio", "-h"}, want: []string{"Studio is terminal-first.", "read-only GET /api/studio", "--once", "Interactive allowlisted commands"}},
	}

	for _, tt := range tests {
		t.Run(strings.Join(tt.args, "_"), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, "go", append([]string{"run", "."}, tt.args...)...)
			cmd.Env = append(os.Environ(), "ADDR=http://127.0.0.1:1")
			out, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("go run . %s did not terminate: %v\n%s", strings.Join(tt.args, " "), ctx.Err(), out)
			}
			if err != nil {
				t.Fatalf("go run . %s failed: %v\n%s", strings.Join(tt.args, " "), err, out)
			}
			for _, want := range tt.want {
				if !strings.Contains(string(out), want) {
					t.Errorf("go run . %s output missing %q:\n%s", strings.Join(tt.args, " "), want, out)
				}
			}
		})
	}
}
