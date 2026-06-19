.PHONY: test-go test-rust test build-go build-rust build

test-go:
	go test ./...

test-rust:
	cargo test -p agent

test: test-go test-rust

build-go:
	go build ./...

build-rust:
	cargo build -p agent

build: build-go build-rust
