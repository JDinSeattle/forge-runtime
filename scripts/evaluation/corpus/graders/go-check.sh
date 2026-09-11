#!/bin/sh
# Trusted parent. No expected values or grader code are copied into /workspace.
set -eu
test "$#" -eq 2
fixture=$1; suite=$2
case "$fixture/$suite" in go-midpoint/target|go-midpoint/regression|go-rune-rle/target|go-rune-rle/regression) ;; *) exit 2 ;; esac
umask 077
work=$(mktemp -d /tmp/forge-eval-go.XXXXXX)
trap 'rm -rf -- "$work"' EXIT HUP INT TERM
mkdir "$work/cache" "$work/build"
export GOCACHE="$work/cache" GOTMPDIR="$work/build"
export GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOWORK=off GOENV=off
export CGO_ENABLED=0 GOMAXPROCS=1 GOMEMLIMIT=160MiB GOGC=50 GOTELEMETRY=off
export GOFLAGS=
timeout 35 /usr/local/go/bin/go build -p=1 -trimpath -buildvcs=false -mod=readonly -o "$work/candidate" .
passed=0
check_case() {
 expected_status=$1; expected_output=$2; shift 2
 status=0
 (ulimit -f 64; timeout 3 "$work/candidate" "$@") >"$work/out" 2>"$work/err" || status=$?
 actual=$(head -c 4096 "$work/out")
 output_bytes=$(wc -c <"$work/out")
 expected_bytes=$((${#expected_output} + 1))
 if [ "$status" -ne "$expected_status" ] || [ "$actual" != "$expected_output" ] || [ "$output_bytes" -ne "$expected_bytes" ]; then
  printf '{"fixture":"%s","suite":"%s","case":%s,"passed":false,"exit_code":%s}\n' "$fixture" "$suite" "$passed" "$status"
  exit 1
 fi
 passed=$((passed + 1))
}
case "$fixture/$suite" in
go-midpoint/target)
 check_case 0 -1 -2 1
 check_case 0 9223372036854775806 9223372036854775806 9223372036854775807
 check_case 0 -9223372036854775808 -9223372036854775808 -9223372036854775807
 check_case 0 -1 -9223372036854775808 9223372036854775807
 ;;
go-midpoint/regression)
 check_case 0 4 2 7
 check_case 0 -4 -6 -2
 check_case 0 0 0 0
 check_case 2 error 4 3
 check_case 2 error invalid 3
 check_case 2 error 3
 ;;
go-rune-rle/target)
 check_case 0 '00E9:2,1F642:1' 'éé🙂'
 check_case 0 '6C49:2,5B57:1' '汉汉字'
 check_case 0 '0065:1,0301:1,0065:1,0301:1' 'éé'
 check_case 0 '1F642:3' '🙂🙂🙂'
 ;;
go-rune-rle/regression)
 check_case 0 '0061:2,0062:2,0063:1' 'aabbc'
 check_case 0 '' ''
 check_case 0 '0041:1' 'A'
 check_case 0 '0061:1,0062:1,0061:1' 'aba'
 check_case 2 error
 ;;
esac
printf '{"fixture":"%s","suite":"%s","passed":%s}\n' "$fixture" "$suite" "$passed"
