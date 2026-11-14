#!/usr/bin/env bash
# Ramp orgs up through the given steps and run every scenario at each step.
# Usage: ramp.sh <label> <step>...   e.g. ramp.sh baseline 100 350 1000
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
label="$1"; shift
cur=$("$HERE/orgs.sh" count)
for target in "$@"; do
  if (( target > cur )); then
    t0=$(date +%s)
    "$HERE/orgs.sh" create $((cur + 1)) "$target" >/dev/null
    echo "RESULT label=$label scenario=create orgs=$target createSeconds=$(( $(date +%s) - t0 ))"
    cur=$target
    sleep "${SETTLE:-30}"
  fi
  for s in latency newauth delete connect; do
    "$HERE/measure.sh" "$s" 2>&1 | sed "s/^RESULT /RESULT label=$label /"
  done
done
