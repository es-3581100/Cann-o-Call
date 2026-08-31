.PHONY: tidy run build test

tidy:
	go mod tidy

run:
	go run .

build:
	go build -o bin/flatten-workspace .

test:
	go test ./...
