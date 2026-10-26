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
#   (f) #4800: `actionExecutor.connectorReadsWithoutReconciler` is declared with
#       default false, and the default render carries no
#       CURIE_ACTION_EXECUTOR_CONNECTOR_READS and no Deployments rule at all.
#   (g) With the executor on, the reconciler off and that setting on, the worker
#       Role differs from the executor-on, reconciler-off Role by exactly one
#       rule, `get` on apps/deployments alone; it carries none of the
#       reconciler's verbs on the connector kinds; the worker renders
#       CURIE_ACTION_EXECUTOR_CONNECTOR_READS="true" and CURIE_CONNECTOR_APP_NAME,
#       and no CURIE_CONNECTOR_RECONCILE.
#   (h) The setting is refused at render with the executor off, and with the
#       reconciler on.
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

# (f) The executor's own connector reads (#4800): declared, default off, and the
# default render is unchanged by them.
python3 - "$CHART/values.yaml" <<'PY' || exit 1
import sys, yaml
section = yaml.safe_load(open(sys.argv[1])).get("actionExecutor") or {}
if section.get("connectorReadsWithoutReconciler", "<absent>") is not False:
    print(
        "FAIL [f] values.yaml must declare actionExecutor.connectorReadsWithoutReconciler: false, "
        f"got {section.get('connectorReadsWithoutReconciler', '<absent>')!r}",
        file=sys.stderr,
    )
    sys.exit(1)
PY

worker_env() {
  local manifest; manifest=$(mktemp "$WORK/manifest.XXXXXX"); printf '%s' "$1" >"$manifest"
  python3 - "$manifest" <<'PY'
import json, sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1]).read()) if d]
workers = [
    d for d in docs
    if d.get("kind") == "Deployment"
    and d["metadata"].get("labels", {}).get("app.kubernetes.io/component") == "worker"
]
if len(workers) != 1:
    print(f"expected one worker Deployment, got {len(workers)}", file=sys.stderr)
    sys.exit(2)
env = {}
for c in workers[0]["spec"]["template"]["spec"]["containers"]:
    for e in c.get("env") or []:
        env.setdefault(e["name"], []).append(e.get("value", "<valueFrom>"))
print(json.dumps(env))
PY
}

DEFAULT_WORKER="$(render -s templates/worker.yaml)"
python3 - "$(worker_role_rules "$DEFAULT_WORKER")" "$(worker_env "$DEFAULT_WORKER")" <<'PY' || exit 1
import json, sys, yaml
rules = yaml.safe_load(sys.argv[1])
env = json.loads(sys.argv[2])
if "CURIE_ACTION_EXECUTOR_CONNECTOR_READS" in env:
    print("FAIL [f] the default render sets CURIE_ACTION_EXECUTOR_CONNECTOR_READS", file=sys.stderr)
    sys.exit(1)
if any("deployments" in r["resources"] for r in rules):
    print(f"FAIL [f] the default worker Role grants on deployments: {rules!r}", file=sys.stderr)
    sys.exit(1)
PY

# (g) On: exactly one extra rule, `get` on apps/deployments alone.
READS_BASE="$(render -s templates/worker.yaml --set actionExecutor.enabled=true)"
READS_ON="$(render -s templates/worker.yaml --set actionExecutor.enabled=true --set actionExecutor.connectorReadsWithoutReconciler=true)"
if grep -qE '^kind: ClusterRole' <<<"$READS_ON"; then
  fail g "the worker template rendered a ClusterRole with the executor's own connector reads on"
fi
python3 - "$(worker_role_rules "$READS_BASE")" "$(worker_role_rules "$READS_ON")" "$(worker_env "$READS_ON")" <<'PY' || exit 1
import json, sys, yaml

base = yaml.safe_load(sys.argv[1])
on = yaml.safe_load(sys.argv[2])
env = json.loads(sys.argv[3])

extra = [r for r in on if r not in base]
missing = [r for r in base if r not in on]
want = {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}
if missing or extra != [want]:
    print(f"FAIL [g] the reads add {extra!r} and drop {missing!r}; expected exactly [{want!r}]", file=sys.stderr)
    sys.exit(1)
for rule in on:
    for kind in ("deployments", "services", "networkpolicies"):
        if kind in rule["resources"] and set(rule["verbs"]) - {"get"}:
            print(f"FAIL [g] a reconciler verb on {kind} rendered without the reconciler: {rule!r}", file=sys.stderr)
            sys.exit(1)
if env.get("CURIE_ACTION_EXECUTOR_CONNECTOR_READS") != ["true"]:
    print(f"FAIL [g] worker CURIE_ACTION_EXECUTOR_CONNECTOR_READS is {env.get('CURIE_ACTION_EXECUTOR_CONNECTOR_READS')!r}", file=sys.stderr)
    sys.exit(1)
app = env.get("CURIE_CONNECTOR_APP_NAME") or []
if len(app) != 1 or not app[0]:
    print(f"FAIL [g] worker CURIE_CONNECTOR_APP_NAME is {app!r}, expected one non-empty value", file=sys.stderr)
    sys.exit(1)
if "CURIE_CONNECTOR_RECONCILE" in env:
    print("FAIL [g] the executor's own connector reads turned the reconcile loop on", file=sys.stderr)
    sys.exit(1)
PY

# (h) Refused combinations fail the render, each with its own message.
if out="$(render -s templates/worker.yaml --set actionExecutor.connectorReadsWithoutReconciler=true 2>&1)"; then
  fail h "the executor's own connector reads rendered with the executor off"
fi
grep -q "requires actionExecutor.enabled=true" <<<"$out" ||
  fail h "executor-off refusal names the wrong cause: $out"
if out="$(render -s templates/worker.yaml --set actionExecutor.enabled=true --set worker.connectorReconciler.enabled=true --set actionExecutor.connectorReadsWithoutReconciler=true 2>&1)"; then
  fail h "the executor's own connector reads rendered with the reconciler on"
fi
grep -q "is for worker.connectorReconciler.enabled=false" <<<"$out" ||
  fail h "reconciler-on refusal names the wrong cause: $out"

echo "action-executor-config-assertions: all assertions passed"
