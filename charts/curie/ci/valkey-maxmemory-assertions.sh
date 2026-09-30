#!/usr/bin/env bash
#
# Render-assertion test for issue #3350. The in-chart Valkey must carry an
# internal `maxmemory` ceiling below its container memory limit, with the
# `noeviction` policy, so a full broker refuses writes with an OOM error the
# API and worker can surface instead of being OOMKilled (which loses the
# writes since the last RDB save). `noeviction` is required both because
# evicting Curie stream or lock keys would be worse than refusing a write and
# because Langfuse's BullMQ queues, which share this Valkey, require it.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TPL=templates/valkey.yaml

fail() { echo "FAIL: $*" >&2; exit 1; }

cmd() {
  helm template rel "$CHART" --show-only "$TPL" "$@" \
    | python3 -c '
import sys, yaml
for d in yaml.safe_load_all(sys.stdin):
    if d and d.get("kind") == "StatefulSet":
        print(" ".join(d["spec"]["template"]["spec"]["containers"][0]["command"]))
'
}

expect() {
  local name="$1" want="$2"; shift 2
  local got; got="$(cmd "$@")"
  [[ "$got" == *"--maxmemory-policy noeviction"* ]] || fail "$name: no noeviction policy in: $got"
  [[ "$got" == *"--maxmemory $want "* || "$got" == *"--maxmemory $want" ]] \
    || fail "$name: expected --maxmemory $want in: $got"
  echo "  ok: $name -> --maxmemory $want"
}

# Default 256Mi limit: 75% = 201326592 bytes.
expect "default 256Mi limit" 201326592
# The ceiling follows a raised limit rather than staying pinned.
expect "1Gi limit" 805306368 --set valkey.resources.limits.memory=1Gi
# Decimal suffix is parsed too (512M -> 384000000).
expect "512M limit" 384000000 --set valkey.resources.limits.memory=512M
# An explicit operator value wins verbatim.
expect "explicit override" 100mb --set valkey.maxmemory=100mb

# An override is spliced into `sh -c`, so anything but a plain size must fail
# the render: a trailing flag could replace noeviction, a metacharacter could
# run shell.
for bad in "100mb --maxmemory-policy allkeys-lru" '100mb;id' '1gib'; do
  if helm template rel "$CHART" --show-only "$TPL" --set-string "valkey.maxmemory=$bad" >/dev/null 2>&1; then
    fail "invalid valkey.maxmemory '$bad' rendered instead of failing"
  fi
done
echo "  ok: non-size valkey.maxmemory values fail the render"

# No memory limit: nothing to stay under, so no ceiling (Valkey default 0).
got="$(cmd --set valkey.resources.limits.memory=null)"
[[ "$got" != *"--maxmemory "* ]] || fail "no limit: unexpected --maxmemory in: $got"
[[ "$got" == *"--maxmemory-policy noeviction"* ]] || fail "no limit: policy missing in: $got"
echo "  ok: no memory limit -> no ceiling, noeviction kept"

echo
echo "PASS: Valkey renders a maxmemory ceiling at 75% of its memory limit with noeviction (#3350)."
