#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && -n "$1" ]] || { echo 'usage: pull-image.sh IMAGE' >&2; exit 2; }
output="$(mktemp)"
trap 'rm -f "$output"' EXIT
for attempt in 1 2 3; do
  if docker pull "$1" >"$output" 2>&1; then
    cat "$output"
    exit 0
  else
    status=$?
  fi
  cat "$output" >&2
  if (( attempt == 3 )) || ! grep -Eiq '502|bad gateway' "$output"; then
    exit "$status"
  fi
  sleep 5
done
