.PHONY: tidy run build test

tidy:
	go mod tidy

run:
	go run .

build:
	go build -o bin/flatten-workspace .
	cp bin/flatten-workspace bin/cann-o-call 2>/dev/null || true
	ln -sf flatten-workspace bin/cann-o-call 2>/dev/null || true

test:
	go test ./...
