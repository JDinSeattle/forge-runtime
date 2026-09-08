#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
project_dir=$(pwd)
generated_dir=$(mktemp -d)
trap 'rm -rf "$generated_dir"' EXIT HUP INT TERM
cd proto/runner/v1
FORGE_PROTO_OUTPUT="$generated_dir" go run generate.go
for binding in runner.pb.go runner_grpc.pb.go; do
  diff -u "$project_dir/proto/runner/v1/$binding" "$generated_dir/proto/runner/v1/$binding"
done
