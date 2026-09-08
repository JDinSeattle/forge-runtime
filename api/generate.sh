#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
go tool oapi-codegen --config api/oapi-codegen.yaml -o internal/httpcontract/client.gen.go api/openapi.yaml
