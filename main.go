package main

import (
	"log"
	"net/http"
	"os"

	"flatten-workspace/internal/server"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	s := server.New()

	log.Printf("flatten-workspace listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, s.Handler()))
}
