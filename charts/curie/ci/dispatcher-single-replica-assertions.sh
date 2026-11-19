#!/usr/bin/env bash
#
# Render assertions for the dispatcher single-replica guard (#2944). Every
# dispatcher replica is another Slack Socket Mode client on the same app token,
# and Slack delivers an event to only one connected client, so the chart refuses
# dispatcher.replicas above 1 while the dispatcher is deployed.
#
#   1. replicas=1 (the default) renders a dispatcher Deployment with 1 replica.
#   2. replicas=2 fails the render with a message naming dispatcher.replicas.
#   3. With dispatcher.deploy=false there is no dispatcher, so the value is not
#      checked and the render succeeds.
#
# Fails loudly, naming the assertion. Runnable locally and from CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

# The dispatcher renders only when the default Slack identity has both tokens.
TOKENS=(
  --set dispatcher.slack.appToken=xapp-assert
  --set dispatcher.slack.botToken=xoxb-assert
)

mkdir -p "$TMP/one"
helm template curie "$CHART" --output-dir "$TMP/one" "${TOKENS[@]}" \
  --set dispatcher.replicas=1 >/dev/null 2>"$TMP/one.err" \
  || fail "replicas=1 must render: $(cat "$TMP/one.err")"
manifest="$TMP/one/curie/templates/dispatcher.yaml"
[ -f "$manifest" ] || fail "replicas=1: dispatcher.yaml did not render"
grep -qE '^  replicas: 1$' "$manifest" || fail "replicas=1: Deployment does not render replicas: 1"

if helm template curie "$CHART" --output-dir "$TMP/two" "${TOKENS[@]}" \
  --set dispatcher.replicas=2 >/dev/null 2>"$TMP/two.err"; then
  fail "replicas=2 rendered; the chart must refuse more than one dispatcher replica"
fi
grep -q "dispatcher.replicas is 2" "$TMP/two.err" \
  || fail "replicas=2 failed without naming dispatcher.replicas: $(cat "$TMP/two.err")"
grep -q "single replica" "$TMP/two.err" \
  || fail "replicas=2 failure does not explain the single-replica rule: $(cat "$TMP/two.err")"

mkdir -p "$TMP/off"
helm template curie "$CHART" --output-dir "$TMP/off" "${TOKENS[@]}" \
  --set dispatcher.deploy=false --set dispatcher.replicas=2 >/dev/null 2>"$TMP/off.err" \
  || fail "dispatcher.deploy=false must not check replicas: $(cat "$TMP/off.err")"
[ ! -f "$TMP/off/curie/templates/dispatcher.yaml" ] || fail "dispatcher.deploy=false still rendered a dispatcher"

echo "dispatcher single-replica assertions passed"
