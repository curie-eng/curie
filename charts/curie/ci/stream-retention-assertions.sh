#!/usr/bin/env bash
#
# Render assertions for worker.streamRetention.minAgeSeconds (ADR 0184, #1523).
#
# The worker trims settled stream entries older than this window. The value
# reaches the worker as CURIE_STREAM_RETENTION_MIN_AGE_S, and the worker's own
# config refuses anything outside 3600 to 31536000 at boot. The schema refuses
# the same range at render time, so a bad value fails the install instead of
# CrashLoopBackOff on the worker.
#
#   (a) the default renders 86400;
#   (b) an override inside the range reaches the worker unchanged, at both ends;
#   (c) below the minimum, above the maximum, and a fractional value fail render;
#   (d) a release whose retained values carry no streamRetention block, or a null
#       one (an upgrade from a chart before this knob), still renders the default.
#
# Negatives check only that helm failed and named the knob; helm's validator
# wording differs across versions (see worker-ttl-bounds-assertions.sh).
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "FAIL [$1] $2" >&2; exit 1; }
render() { helm template curie "$CHART" "$@" 2>&1; }

IFS= read -r -d '' WORKER_ENV_PY <<'PY' || true
import sys, yaml
name, expected = sys.argv[2], sys.argv[3]
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
deploys = [
    d for d in docs
    if d.get("kind") == "Deployment"
    and (d["metadata"].get("labels") or {}).get("app.kubernetes.io/component") == "worker"
]
if len(deploys) != 1:
    raise SystemExit(f"expected exactly one worker Deployment, rendered {len(deploys)}")
values = [
    e.get("value")
    for c in deploys[0]["spec"]["template"]["spec"]["containers"]
    for e in c.get("env", [])
    if e["name"] == name
]
if values != [expected]:
    raise SystemExit(f"{name} rendered {values!r}, expected exactly [{expected!r}]")
PY

expect_env() {
  local case="$1" expected="$2"; shift 2
  local out file
  file="$(mktemp)"
  if ! out="$(render "$@")"; then
    rm -f "$file"
    fail "$case" "render failed: $out"
  fi
  printf '%s\n' "$out" > "$file"
  python3 -c "$WORKER_ENV_PY" "$file" CURIE_STREAM_RETENTION_MIN_AGE_S "$expected" \
    || { rm -f "$file"; fail "$case" "worker env mismatch"; }
  rm -f "$file"
  echo "ok [$case]"
}

expect_refused() {
  local case="$1"; shift
  local out
  if out="$(render "$@")"; then
    fail "$case" "render succeeded, expected the schema to refuse it"
  fi
  grep -q "minAgeSeconds" <<<"$out" || fail "$case" "refusal did not name minAgeSeconds: $out"
  echo "ok [$case]"
}

expect_env a-default 86400
expect_env b-override 7200 --set worker.streamRetention.minAgeSeconds=7200
expect_env b-at-min 3600 --set worker.streamRetention.minAgeSeconds=3600
expect_env b-at-max 31536000 --set worker.streamRetention.minAgeSeconds=31536000
expect_refused c-below-min --set worker.streamRetention.minAgeSeconds=3599
expect_refused c-zero --set worker.streamRetention.minAgeSeconds=0
expect_refused c-above-max --set worker.streamRetention.minAgeSeconds=31536001
expect_refused c-fractional --set worker.streamRetention.minAgeSeconds=3600.5
expect_env d-null-block 86400 --set worker.streamRetention=null
expect_env d-null-knob 86400 --set worker.streamRetention.minAgeSeconds=null

echo "stream retention render assertions passed"
