#!/usr/bin/env bash
#
# Render-assertion test for the action executor switch (ACTION-EXECUTOR-1, -12,
# -14; connector action executor plan task 5).
#
# One chart value, `actionExecutor.enabled` (default off), renders the one
# setting CURIE_ACTION_EXECUTOR_ENABLED into both the API and the worker. Two
# processes reading two values is the failure this pins: an API that creates
# executions the worker never claims, or a worker that claims work the API was
# told to refuse. The worker also gains a single-object `get` on Deployments so
# the digest recorder (AE-12) and the pinned-digest check (AE-14) can read the
# connector's owned Deployment by name -- and nothing broader.
#
# Assertions:
#   (a) Default render: the API and the worker carry the same value for
#       CURIE_ACTION_EXECUTOR_ENABLED, and it is off (absent or "false").
#   (b) `--set actionExecutor.enabled=true` renders "true" into both, and the two
#       values match.
#   (c) values.yaml declares `actionExecutor.enabled` with default false, so the
#       switch is a documented key rather than an undeclared `--set`.
#   (d) With the executor enabled (and the connector reconciler enabled, which
#       is what renders the Deployments rule), the worker Role grants exactly
#       {create,list,patch,delete,get} on apps/deployments; services, secrets
#       and networkpolicies keep exactly {create,list,patch,delete} (no `get`);
#       no `watch`/`update`/`*` anywhere on the connector kinds; the executor
#       adds no other rule; and nothing is cluster-scoped.
#   (e) With the executor off (reconciler on), Deployments carry no `get`: the
#       grant follows the executor switch.
#
# Runnable locally (from anywhere) and from CI. Render-only: no cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
NS="curie-action-executor-assert"

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
flag_pair() {
  local manifest; manifest=$(mktemp "$WORK/manifest.XXXXXX"); printf '%s' "$1" >"$manifest"
  python3 - "$manifest" <<'PY'
import sys, yaml

FLAG = "CURIE_ACTION_EXECUTOR_ENABLED"
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

worker_role_rules() {
  local manifest; manifest=$(mktemp "$WORK/manifest.XXXXXX"); printf '%s' "$1" >"$manifest"
  python3 - "$manifest" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1]).read()) if d]
roles = [
    d for d in docs
    if d.get("kind") == "Role" and d["metadata"]["name"].endswith("-worker")
]
if len(roles) != 1:
    print(f"expected exactly one worker Role, got {len(roles)}", file=sys.stderr)
    sys.exit(2)
print(yaml.safe_dump(roles[0].get("rules", [])))
PY
}

# (a) Default: off, and the same in both.
DEFAULT_PAIR="$(flag_pair "$(render)")"
api="${DEFAULT_PAIR%%|*}"
worker="${DEFAULT_PAIR##*|}"
[[ "$api" == "$worker" ]] ||
  fail a "default render disagrees: api=$api worker=$worker"
[[ "$api" == "<absent>" || "$api" == "false" ]] ||
  fail a "default render turns the executor on: api=$api worker=$worker"

# (b) One value turns both on.
ENABLED_PAIR="$(flag_pair "$(render --set actionExecutor.enabled=true)")"
api="${ENABLED_PAIR%%|*}"
worker="${ENABLED_PAIR##*|}"
[[ "$api" == "true" ]] ||
  fail b "actionExecutor.enabled=true did not render CURIE_ACTION_EXECUTOR_ENABLED=\"true\" into the API (got $api)"
[[ "$worker" == "true" ]] ||
  fail b "actionExecutor.enabled=true did not render CURIE_ACTION_EXECUTOR_ENABLED=\"true\" into the worker (got $worker)"

# ...and setting it false explicitly yields the same pair as the default.
FALSE_PAIR="$(flag_pair "$(render --set actionExecutor.enabled=false)")"
[[ "$FALSE_PAIR" == "$DEFAULT_PAIR" ]] ||
  fail b "actionExecutor.enabled=false renders $FALSE_PAIR, default renders $DEFAULT_PAIR"

# (c) The key is declared with default false.
python3 - "$CHART/values.yaml" <<'PY' || exit 1
import sys, yaml
values = yaml.safe_load(open(sys.argv[1]))
section = values.get("actionExecutor")
if not isinstance(section, dict) or "enabled" not in section:
    print("FAIL [c] values.yaml does not declare actionExecutor.enabled", file=sys.stderr)
    sys.exit(1)
if section["enabled"] is not False:
    print(f"FAIL [c] actionExecutor.enabled defaults to {section['enabled']!r}, expected false", file=sys.stderr)
    sys.exit(1)
PY

# (d) Enabled: exactly one new verb, `get`, on exactly apps/deployments.
EXEC_ON="$(render -s templates/worker.yaml --set worker.connectorReconciler.enabled=true --set actionExecutor.enabled=true)"
EXEC_OFF="$(render -s templates/worker.yaml --set worker.connectorReconciler.enabled=true --set actionExecutor.enabled=false)"
ON_RULES="$(worker_role_rules "$EXEC_ON")"
OFF_RULES="$(worker_role_rules "$EXEC_OFF")"

if grep -qE '^kind: ClusterRole' <<<"$EXEC_ON"; then
  fail d "the worker template rendered a ClusterRole with the executor enabled"
fi

python3 - "$ON_RULES" "$OFF_RULES" <<'PY' || exit 1
import sys, yaml

on = yaml.safe_load(sys.argv[1])
off = yaml.safe_load(sys.argv[2])

connector = {
    ("apps", "deployments"),
    ("", "services"),
    ("", "secrets"),
    ("networking.k8s.io", "networkpolicies"),
}
base = {"create", "list", "patch", "delete"}
want = {key: set(base) for key in connector}
want[("apps", "deployments")] = base | {"get"}


def union(rules):
    seen = {}
    for rule in rules:
        for group in rule["apiGroups"]:
            for resource in rule["resources"]:
                seen.setdefault((group, resource), set()).update(rule["verbs"])
    return seen


seen_on = union(on)
seen_off = union(off)

for key, verbs in sorted(want.items()):
    got = seen_on.get(key, set())
    # secrets also carry the per-claim token `create` rule; it is inside `base`.
    if got != verbs:
        print(f"FAIL [d] enabled render grants {sorted(got)} on {key}, expected {sorted(verbs)}", file=sys.stderr)
        sys.exit(1)

# The executor adds nothing but deployments/get: every (group, resource) other
# than apps/deployments has the same verbs on and off.
for key in sorted(set(seen_on) | set(seen_off)):
    if key == ("apps", "deployments"):
        continue
    if seen_on.get(key, set()) != seen_off.get(key, set()):
        print(
            f"FAIL [d] the executor switch changes {key}: off={sorted(seen_off.get(key, set()))} "
            f"on={sorted(seen_on.get(key, set()))}; only apps/deployments may gain `get`",
            file=sys.stderr,
        )
        sys.exit(1)
added = seen_on[("apps", "deployments")] - seen_off.get(("apps", "deployments"), set())
if added != {"get"}:
    print(f"FAIL [d] the executor adds {sorted(added)} on apps/deployments, expected exactly ['get']", file=sys.stderr)
    sys.exit(1)

# The `get` must ride a rule naming only apps/deployments, so it cannot spill
# onto another kind listed in the same rule.
for rule in on:
    if "get" in rule["verbs"] and "deployments" in rule["resources"]:
        if rule["apiGroups"] != ["apps"] or rule["resources"] != ["deployments"]:
            print(f"FAIL [d] deployments `get` shares a rule with other kinds: {rule!r}", file=sys.stderr)
            sys.exit(1)
    for verb in ("watch", "update", "*"):
        if verb in rule["verbs"] and any(r in rule["resources"] for r in ("deployments", "services", "secrets", "networkpolicies")):
            print(f"FAIL [d] unexpected verb '{verb}' on {rule['resources']}", file=sys.stderr)
            sys.exit(1)

# (e) Off: deployments carry no `get`.
if "get" in seen_off.get(("apps", "deployments"), set()):
    print("FAIL [e] apps/deployments grants `get` with the executor disabled", file=sys.stderr)
    sys.exit(1)
PY

echo "action-executor-config-assertions: all assertions passed"
