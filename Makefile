# Forge — framework-only build pipeline
#
# Targets:
#   make fmt            Apply go fmt + cargo fmt
#   make fmt-check      Fail if Go/Rust formatting differs (CI-style)
#   make lint           golangci-lint (go/) + clippy (rust/)
#   make sdk-go         Build, test, and vet Forge Go SDK
#   make rust-sdk       Test/check forge-worker-sdk + minimal-worker
#   make check          spec + fmt-check + lint + rust-sdk + sdk-go
#   make clean          Remove Rust build artefacts

SHELL := /bin/bash

.PHONY: all fmt fmt-check go-fmt go-fmt-check rust-fmt rust-fmt-check \
	lint go-lint rust-lint sdk-go rust-sdk protocol-spec protocol-spec-check \
	check clean

all: check

## Apply Go and Rust formatters (writes files).
fmt: go-fmt rust-fmt

go-fmt:
	@echo "==> go fmt"
	cd go && go fmt ./...

rust-fmt:
	@echo "==> cargo fmt"
	cd rust && cargo fmt --all

## Fail if any Go or Rust file needs formatting.
fmt-check: go-fmt-check rust-fmt-check

go-fmt-check:
	@echo "==> gofmt (check)"
	@cd go && files=$$(gofmt -l .); if [ -n "$$files" ]; then \
		echo "These files need go fmt (run: make go-fmt):"; echo "$$files"; exit 1; fi

rust-fmt-check:
	@echo "==> cargo fmt --check"
	cd rust && cargo fmt --all --check

## golangci-lint + clippy (requires golangci-lint in PATH for Go).
lint: go-lint rust-lint

go-lint:
	@echo "==> golangci-lint"
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not in PATH; install: https://golangci-lint.run/"; exit 1; }
	cd go && golangci-lint run --config=../.golangci.yml ./...

rust-lint:
	@echo "==> cargo clippy"
	cd rust && cargo clippy --workspace --all-targets -- -D warnings

## Build, test, and vet Forge Go SDK.
sdk-go:
	@echo "==> Building Forge Go SDK"
	cd go && go build ./...
	@echo "==> Testing Forge Go SDK"
	cd go && go test ./...
	@echo "==> Vetting Forge Go SDK"
	cd go && go vet ./...

## Regenerate protocol/spec.md generated block from fixtures + go doc + Rust module docs.
protocol-spec:
	@echo "==> Regenerating forge/protocol/spec.md"
	python3 protocol/tools/gen_spec.py

## Fail if spec.md is out of date (for CI).
protocol-spec-check:
	@echo "==> Checking forge/protocol/spec.md"
	python3 protocol/tools/gen_spec.py --check

## Test forge-worker-sdk integration tests; compile-check SDK and minimal-worker example.
rust-sdk:
	@echo "==> Testing forge-worker-sdk"
	cd rust && cargo test -p forge-worker-sdk
	@echo "==> Checking forge-worker-sdk"
	cd rust && cargo check -p forge-worker-sdk
	@echo "==> Checking minimal-worker example"
	cd rust && cargo check -p minimal-worker

## Full local verification: spec, formatting, linters, Rust tests, Go build/test/vet.
check: protocol-spec-check fmt-check lint rust-sdk sdk-go

clean:
	cd rust && cargo clean
