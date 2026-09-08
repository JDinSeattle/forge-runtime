#!/bin/sh
set -eu
# This dedicated transient user service never selects or changes Docker's
# default context. Delegate=yes is necessary for real rootless cgroup limits.
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
config="$project_dir/var/rootless-docker.json"
test -f "$config"
runtime_dir=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}
test -d "$runtime_dir"
exec systemd-run --user --unit=forge-runtime-docker --property=Delegate=yes --collect \
  --setenv="DOCKERD_ROOTLESS_ROOTLESSKIT_STATE_DIR=$runtime_dir/forge-runtime-rootlesskit" \
  /usr/bin/dockerd-rootless.sh --config-file "$config"
