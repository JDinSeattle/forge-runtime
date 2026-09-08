SHELL := /bin/bash
GO ?= go

.PHONY: test race vet build generate check-generated db-up db-down
test:
	$(GO) test ./...
race:
	$(GO) test -race ./...
vet:
	$(GO) vet ./...
build:
	$(GO) build -o bin/ ./cmd/...
generate:
	sh api/generate.sh
	sh db/generate.sh
	$(GO) generate ./proto/runner/v1
check-generated:
	sh api/check-generated.sh
	sh db/check-generated.sh
	sh proto/check-generated.sh
db-up:
	docker compose -p forge-runtime -f deploy/compose/compose.yaml up -d postgres
db-down:
	docker compose -p forge-runtime -f deploy/compose/compose.yaml down
