#!/usr/bin/env bash
#
# Render-assertion test for the remediation switch (AUTOMATED-REMEDIATION-1;
# automated remediation plan task 3).
#
# One chart value, `remediation.enabled` (default off), renders the one setting
# CURIE_REMEDIATION_ENABLED into both the API and the worker. Enabling it
# requires `actionExecutor.enabled`: a render with remediation on and the
# executor off fails.
#
# Assertions:
#   (a) Default render: the API and the worker carry the same value for
#       CURIE_REMEDIATION_ENABLED, and it is off (absent or "false").
#   (b) `--set remediation.enabled=true --set actionExecutor.enabled=true`
#       renders "true" into both; `remediation.enabled=false` renders the same
#       pair as the default.
#   (c) values.yaml declares `remediation.enabled` with default false, and the
#       values schema types it boolean, so a quoted "true" cannot turn it on.
#   (d) `--set remediation.enabled=true` with the executor off (default, and
#       explicitly false) fails to render, naming the requirement.
#   (e) The executor on and remediation off renders remediation off in both:
#       the executor alone never turns remediation on.
#
# Runnable locally (from anywhere) and from CI. Render-only: no cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
NS="curie-remediation-assert"

fail() {
  echo "FAIL [$1] $2" >&2
  exit 1
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# $@ = extra --set flags.
render() {
  helm template curie "$CHART" -n "$NS" \
    -f "$CHART/values-dev.yaml" \
    --set agentSandbox.controller.deploy=false \
    --set worker.publication.enabled=false \
    "$@"
}

# Print "<api>|<worker>" where each side is the literal env value, or <absent>.
# The manifest goes through a file, never argv (ARG_MAX on Linux runners).
flag_pair() {
  local manifest; manifest=$(mktemp "$WORK/manifest.XXXXXX"); printf '%s' "$1" >"$manifest"
  python3 - "$manifest" <<'PY'
import sys, yaml

FLAG = "CURIE_REMEDIATION_ENABLED"
docs = [d for d in yaml.safe_load_all(open(sys.argv[1]).read()) if d]


def deployment(component):
    found = [
        d for d in docs
        if d.get("kind") == "Deployment"
        and d["metadata"].get("labels", {}).get("app.kubernetes.io/component") == component
    ]
    if len(found) != 1:
        print(f"expected one {component} Deployment, got {len(found)}", file=sys.stderr)
        sys.exit(2)
    return found[0]


def value(component):
    seen = []
    for c in deployment(component)["spec"]["template"]["spec"]["containers"]:
        for e in c.get("env") or []:
            if e.get("name") == FLAG:
                if "value" not in e:
                    print(f"{component} renders {FLAG} without a literal value: {e!r}", file=sys.stderr)
                    sys.exit(2)
                seen.append(str(e["value"]))
    if len(set(seen)) > 1:
        print(f"{component} renders {FLAG} with conflicting values {seen!r}", file=sys.stderr)
        sys.exit(2)
    return seen[0] if seen else "<absent>"


print(f"{value('api')}|{value('worker')}")
PY
}

# (a) Default: off, and the same in both.
DEFAULT_PAIR="$(flag_pair "$(render)")"
api="${DEFAULT_PAIR%%|*}"
worker="${DEFAULT_PAIR##*|}"
[[ "$api" == "$worker" ]] ||
  fail a "default render disagrees: api=$api worker=$worker"
[[ "$api" == "<absent>" || "$api" == "false" ]] ||
  fail a "default render turns remediation on: api=$api worker=$worker"

# (b) One value turns both on (the executor on, as required).
ENABLED_PAIR="$(flag_pair "$(render --set remediation.enabled=true --set actionExecutor.enabled=true)")"
api="${ENABLED_PAIR%%|*}"
worker="${ENABLED_PAIR##*|}"
[[ "$api" == "true" ]] ||
  fail b "remediation.enabled=true did not render CURIE_REMEDIATION_ENABLED=\"true\" into the API (got $api)"
[[ "$worker" == "true" ]] ||
  fail b "remediation.enabled=true did not render CURIE_REMEDIATION_ENABLED=\"true\" into the worker (got $worker)"

FALSE_PAIR="$(flag_pair "$(render --set remediation.enabled=false)")"
[[ "$FALSE_PAIR" == "$DEFAULT_PAIR" ]] ||
  fail b "remediation.enabled=false renders $FALSE_PAIR, default renders $DEFAULT_PAIR"

# (c) The key is declared with default false and typed boolean.
python3 - "$CHART/values.yaml" "$CHART/values.schema.json" <<'PY' || exit 1
import json, sys, yaml
values = yaml.safe_load(open(sys.argv[1]))
section = values.get("remediation")
if not isinstance(section, dict) or "enabled" not in section:
    print("FAIL [c] values.yaml does not declare remediation.enabled", file=sys.stderr)
    sys.exit(1)
if section["enabled"] is not False:
    print(f"FAIL [c] remediation.enabled defaults to {section['enabled']!r}, expected false", file=sys.stderr)
    sys.exit(1)
schema = json.load(open(sys.argv[2]))
enabled = schema.get("properties", {}).get("remediation", {}).get("properties", {}).get("enabled", {})
if enabled.get("type") != "boolean":
    print(f"FAIL [c] values.schema.json types remediation.enabled as {enabled!r}, expected boolean", file=sys.stderr)
    sys.exit(1)
PY

if render --set-string remediation.enabled=true --set actionExecutor.enabled=true >/dev/null 2>"$WORK/quoted.err"; then
  fail c "a quoted remediation.enabled=\"true\" rendered; the schema must refuse a string"
fi

# (d) Remediation on with the executor off fails, by default and explicitly.
for executor in default false; do
  if [[ "$executor" == default ]]; then
    args=(--set remediation.enabled=true)
  else
    args=(--set remediation.enabled=true --set actionExecutor.enabled=false)
  fi
  if render "${args[@]}" >/dev/null 2>"$WORK/refused.err"; then
    fail d "remediation.enabled=true rendered with the executor $executor; it must fail"
  fi
  grep -q "actionExecutor.enabled" "$WORK/refused.err" ||
    fail d "the refusal with the executor $executor does not name actionExecutor.enabled: $(cat "$WORK/refused.err")"
done

# (e) The executor alone does not turn remediation on.
EXEC_ONLY_PAIR="$(flag_pair "$(render --set actionExecutor.enabled=true)")"
[[ "$EXEC_ONLY_PAIR" == "$DEFAULT_PAIR" ]] ||
  fail e "actionExecutor.enabled=true alone renders remediation $EXEC_ONLY_PAIR, default $DEFAULT_PAIR"

echo "remediation-config-assertions: all assertions passed"
