#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
generated_tmp=$(mktemp)
trap 'rm -f "$generated_tmp"' EXIT HUP INT TERM
go tool oapi-codegen --config api/oapi-codegen.yaml -o "$generated_tmp" api/openapi.yaml
if ! cmp -s internal/httpcontract/client.gen.go "$generated_tmp"; then
  echo 'Generated HTTP contract differs; run sh api/generate.sh.' >&2
  exit 1
fi
