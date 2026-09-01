package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"flatten-workspace/internal/cli"
	"flatten-workspace/internal/server"
)

var cliCommands = map[string]bool{
	"status":   true,
	"ingest":   true,
	"query":    true,
	"actor":    true,
	"ledger":   true,
	"replay":   true,
	"snapshot": true,
	"task":     true,
	"studio":   true,
	"help":     true,
}

func isCLIArgs(args []string) bool {
	if len(args) == 0 {
		return false
	}
	// Support both binary names: flatten-workspace and cann-o-call; accept subcommands regardless
	cmd := args[0]
	if cliCommands[cmd] {
		return true
	}
	// Also handle --json before command: e.g., --json status
	if strings.HasPrefix(cmd, "--") && len(args) > 1 && cliCommands[args[1]] {
		return true
	}
	return false
}

func main() {
	// If invoked as CLI mode, run CLI; else run server
	// Preserve compatibility: if args[0] is CLI subcommand then run CLI
	if isCLIArgs(os.Args[1:]) {
		// normalize --json position: allow before command
		args := os.Args[1:]
		// if first arg is --json, rotate
		if len(args) >= 2 && args[0] == "--json" && cliCommands[args[1]] {
			args = append([]string{args[1], "--json"}, args[2:]...)
		}
		code := cli.Run(args)
		os.Exit(code)
	}
	// Also support binary name alias cann-o-call: check base name
	base := filepath.Base(os.Args[0])
	if base == "cann-o-call" {
		// If called as cann-o-call with no args, still run server; with CLI args, handle above already
	}

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	s := server.New()

	log.Printf("flatten-workspace listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, s.Handler()))
}
