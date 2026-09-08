#!/bin/sh
# Trusted parent: candidate code is built and executed only as a child process.
# Fixed profile argv supplies the suite; repository files cannot alter cases.
set -eu
case "${1:-}" in target|regression) suite=$1 ;; *) exit 2 ;; esac
test "$#" -eq 1
umask 077
work=$(mktemp -d /tmp/forge-go-check.XXXXXX)
trap 'rm -rf -- "$work"' EXIT HUP INT TERM
mkdir "$work/cache" "$work/build"
export GOCACHE="$work/cache" GOTMPDIR="$work/build" HOME="$work"
export GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOWORK=off GOENV=off
export CGO_ENABLED=0 GOMAXPROCS=1 GOMEMLIMIT=160MiB GOGC=50 GOTELEMETRY=off
export GOFLAGS=
# The read-only candidate checkout supplies no external modules or build hooks.
# -mod=readonly refuses module changes; GOTOOLCHAIN=local forbids auto-download.
timeout 35 /usr/local/go/bin/go build -p=1 -trimpath -buildvcs=false -mod=readonly -o "$work/candidate" .
passed=0
check_case() {
 expected_status=$1; expected_output=$2; shift 2
 status=0
 # The child may be hostile. Bound time and output files, and compare from this
 # independent parent. Exit 0 without the exact result cannot pass a case.
 (ulimit -f 64; timeout 3 "$work/candidate" "$@") >"$work/out" 2>"$work/err" || status=$?
 actual=$(head -c 4096 "$work/out")
 output_bytes=$(wc -c <"$work/out")
 expected_bytes=$((${#expected_output} + 1))
 if [ "$status" -ne "$expected_status" ] || [ "$actual" != "$expected_output" ] || [ "$output_bytes" -ne "$expected_bytes" ]; then
  printf '{"fixture":"go-ceil-div","suite":"%s","case":%s,"passed":false,"exit_code":%s}\n' "$suite" "$passed" "$status"
  exit 1
 fi
 passed=$((passed + 1))
}
if [ "$suite" = target ]; then
 check_case 0 0 0 5
 check_case 0 2 6 3
 check_case 0 9223372036854775807 9223372036854775807 1
else
 check_case 0 3 7 3
 check_case 0 1 1 9223372036854775807
 check_case 0 4611686018427387904 9223372036854775807 2
 check_case 2 error -1 3
 check_case 2 error 3 0
 check_case 2 error 3 -1
 check_case 2 error invalid 2
 check_case 2 error 3
fi
printf '{"fixture":"go-ceil-div","suite":"%s","passed":%s}\n' "$suite" "$passed"
