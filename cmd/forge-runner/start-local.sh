#!/bin/sh
set -eu
# Run the trusted runner as namespace UID 0 (host UID 1000), sharing the full
# subordinate UID range with the dedicated rootless Docker daemon. Repository
# processes still use the nonroot UID from their constrained Docker profile.
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
runner_runtime_dir=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}
test -x "$project_dir/bin/forge-runner"
test -f "$project_dir/var/local/runner.json"
exec systemd-run --user --unit=forge-runtime-runner --property=Delegate=yes --collect \
 /usr/bin/rootlesskit --propagation=rslave \
 --state-dir="$runner_runtime_dir/forge-runtime-runnerkit" \
 "$project_dir/bin/forge-runner" -config "$project_dir/var/local/runner.json"
