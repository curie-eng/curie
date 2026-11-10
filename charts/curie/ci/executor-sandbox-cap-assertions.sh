#!/usr/bin/env bash
#
# Render-assertion test for the installation-wide executor sandbox cap
# (AUTOMATED-REMEDIATION-12, executor amendment E9; automated remediation plan
# task 7).
#
# One chart value, `actionExecutor.maxConcurrentSandboxes` (default 2, at
# least 1), renders the one setting CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES
# into both the API (whose claim route enforces it across worker replicas) and
# the worker (whose executor loop may run up to that many at once), so the two
# can never disagree.
#
# Assertions:
#   (a) Default render: the API and the worker both carry the literal "2".
#   (b) `--set actionExecutor.maxConcurrentSandboxes=3` renders "3" into both,
#       with the executor off and on.
#   (c) values.yaml declares `actionExecutor.maxConcurrentSandboxes` with
#       default 2, and the values schema types it integer with minimum 1, so 0
#       and a quoted "3" do not render.
#   (d) The name is chart-owned for both workloads in files/reserved-env.yaml,
#       so extraEnv cannot set a second, disagreeing value.
#
# Runnable locally (from anywhere) and from CI. Render-only: no cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
NS="curie-sandbox-cap-assert"
FLAG="CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES"

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
  python3 - "$manifest" "$FLAG" <<'PY'
import sys, yaml

FLAG = sys.argv[2]
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
    if len(seen) > 1:
        print(f"{component} renders {FLAG} {len(seen)} times: {seen!r}", file=sys.stderr)
        sys.exit(2)
    return seen[0] if seen else "<absent>"


print(f"{value('api')}|{value('worker')}")
PY
}

# (a) Default: "2" in both.
DEFAULT_PAIR="$(flag_pair "$(render)")"
[[ "$DEFAULT_PAIR" == "2|2" ]] ||
  fail a "default render is api|worker=$DEFAULT_PAIR, expected 2|2"
EXEC_PAIR="$(flag_pair "$(render --set actionExecutor.enabled=true)")"
[[ "$EXEC_PAIR" == "2|2" ]] ||
  fail a "executor-on render is api|worker=$EXEC_PAIR, expected 2|2"

# (b) One value moves both, executor off and on.
for executor in false true; do
  PAIR="$(flag_pair "$(render --set actionExecutor.enabled=$executor --set actionExecutor.maxConcurrentSandboxes=3)")"
  [[ "$PAIR" == "3|3" ]] ||
    fail b "maxConcurrentSandboxes=3 (executor $executor) renders api|worker=$PAIR, expected 3|3"
done

# (c) Declared with default 2, typed integer with minimum 1.
python3 - "$CHART/values.yaml" "$CHART/values.schema.json" <<'PY' || exit 1
import json, sys, yaml
values = yaml.safe_load(open(sys.argv[1]))
section = values.get("actionExecutor")
if not isinstance(section, dict) or "maxConcurrentSandboxes" not in section:
    print("FAIL [c] values.yaml does not declare actionExecutor.maxConcurrentSandboxes", file=sys.stderr)
    sys.exit(1)
if section["maxConcurrentSandboxes"] != 2 or isinstance(section["maxConcurrentSandboxes"], bool):
    print(f"FAIL [c] actionExecutor.maxConcurrentSandboxes defaults to {section['maxConcurrentSandboxes']!r}, expected 2", file=sys.stderr)
    sys.exit(1)
schema = json.load(open(sys.argv[2]))
cap = schema.get("properties", {}).get("actionExecutor", {}).get("properties", {}).get("maxConcurrentSandboxes", {})
if cap.get("type") != "integer" or cap.get("minimum") != 1:
    print(f"FAIL [c] values.schema.json types actionExecutor.maxConcurrentSandboxes as {cap!r}, expected integer minimum 1", file=sys.stderr)
    sys.exit(1)
PY

if render --set actionExecutor.maxConcurrentSandboxes=0 >/dev/null 2>"$WORK/zero.err"; then
  fail c "actionExecutor.maxConcurrentSandboxes=0 rendered; the schema must refuse it"
fi
if render --set-string actionExecutor.maxConcurrentSandboxes=3 >/dev/null 2>"$WORK/quoted.err"; then
  fail c "a quoted actionExecutor.maxConcurrentSandboxes=\"3\" rendered; the schema must refuse a string"
fi

# (d) Chart-owned for both workloads.
python3 - "$CHART/files/reserved-env.yaml" "$FLAG" <<'PY' || exit 1
import sys, yaml
reserved = yaml.safe_load(open(sys.argv[1]))
for workload in ("api", "worker"):
    owner = (reserved.get(workload) or {}).get(sys.argv[2])
    if owner != "actionExecutor.maxConcurrentSandboxes":
        print(f"FAIL [d] reserved-env.yaml {workload}.{sys.argv[2]} is {owner!r}, expected actionExecutor.maxConcurrentSandboxes", file=sys.stderr)
        sys.exit(1)
PY

echo "executor-sandbox-cap-assertions: all assertions passed"
