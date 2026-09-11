#!/usr/bin/env bash
set -euo pipefail
# This wrapper neither starts/stops production services nor mounts any volume.
# The operator must quiesce them first and invoke inside the deployed UID map.
: "${FORGE_RUNNER_FAULT_CONFIG:?absolute existing real runner JSON required}"
: "${FORGE_RUNNER_FAULT_BINARY:?absolute newly built forge-runner required}"
: "${FORGE_RUNNER_FAULT_EVIDENCE:?absolute private evidence directory required}"
: "${FORGE_RUNNER_FAULT_TEST_BINARY:?absolute compiled runnerclient test binary required}"
for input in "$FORGE_RUNNER_FAULT_CONFIG" "$FORGE_RUNNER_FAULT_BINARY" "$FORGE_RUNNER_FAULT_TEST_BINARY"; do
  [[ "$input" = /* && -f "$input" ]] || { echo 'expected absolute existing input file' >&2; exit 2; }
done
[[ "$FORGE_RUNNER_FAULT_EVIDENCE" = /* ]] || { echo 'absolute evidence directory required' >&2; exit 2; }
exec "$FORGE_RUNNER_FAULT_TEST_BINARY" -test.v -test.run '^TestRealRunnerCrashMatrix$' -test.timeout=8m
