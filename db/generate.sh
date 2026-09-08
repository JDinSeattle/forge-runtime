#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
go tool sqlc generate -f sqlc.yaml
