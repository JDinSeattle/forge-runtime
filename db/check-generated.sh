#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
# sqlc resolves all configured paths relative to its configuration, including
# paths that appear absolute. Mirror the small inputs under a private directory
# so checking never overwrites the tracked output. Quoting supports repo spaces.
mkdir -p "$tmp/db" "$tmp/internal/persistence"
cp -R db/migrations db/queries "$tmp/db/"
cp sqlc.yaml "$tmp/sqlc.yaml"
go tool sqlc generate -f "$tmp/sqlc.yaml"
diff -ru internal/persistence/sqlgen "$tmp/internal/persistence/sqlgen"
