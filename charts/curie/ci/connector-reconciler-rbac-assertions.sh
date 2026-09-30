#!/usr/bin/env bash
#
# Render-assertion test for the connector reconciler's RBAC (ADR-0090, #1184).
#
# The worker's Role is the authority that lets a background loop DELETE
# Deployments, Services, Secrets and NetworkPolicies. Two ways that goes wrong,
# and this pins both:
#
#   - Too little. A server-side apply of an object that does not exist yet is
#     authorized as a CREATE, so a Role with only `patch` lets the reconciler
#     update existing connectors while failing to create any -- the first-run
#     path. Rendering alone cannot catch that; it was found by running
#     `kubectl apply --server-side --as=<the worker SA>` and reading the 403.
#   - Too much. The grant must stay namespaced, must not reach pods, and must
#     not exist at all when the reconciler is switched off. A component that is
#     not running should not hold delete-on-Secrets.
#
# Eight assertions:
#   (a) With the reconciler DISABLED (the default), the worker Role grants
#       nothing on the four connector kinds.
#   (b) Disabled render still grants the two agent-sandbox CRD rules, so the
#       gate did not swallow the pre-existing rules.
#   (c) Enabled render grants exactly {create,list,patch,delete} on each of the
#       four kinds -- create present, and no extra verb sneaking in.
#   (d) No `get` and no `watch`: nothing in the client reads a single object or
#       opens a watch, and an unused verb is an unexplained one.
#   (e) The four kinds match CONNECTOR_KINDS in the worker source. The Python
#       module's docstring claims this file enforces that; this is the claim.
#   (f) Nothing the reconciler grants is cluster-scoped, and it never mentions
#       pods.
#   (g) The RBAC and connector specific worker env are switched by the same
#       flag. Shared API env remains present in both renders for the worker's
#       runs and eval lanes.
#   (h) The reconciler is pointed at the namespace and release the worker
#       already tells the runner about, so the Service it creates is the one the
#       agent dials.
#
# Runnable locally (from anywhere) and from CI. Fails loudly, naming the
# assertion.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
SOURCE="$REPO_ROOT/apps/worker/src/curie_worker/connector_k8s.py"
NS="curie-rbac-assert"

fail() {
  echo "FAIL [$1] $2" >&2
  exit 1
}

render() {
  helm template curie "$CHART" -n "$NS" \
    -f "$CHART/values-dev.yaml" \
    --set "worker.connectorReconciler.enabled=$1" \
    --set worker.publication.enabled=false \
    -s templates/worker.yaml
}

worker_role_rules() {
  # Only the Role document; the Deployment in the same template mentions none
  # of these strings, but scoping keeps the greps honest.
  python3 - "$1" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(sys.argv[1]) if d]
roles = [d for d in docs if d.get("kind") == "Role"]
if len(roles) != 1:
    print(f"expected exactly one Role, got {len(roles)}", file=sys.stderr)
    sys.exit(2)
print(yaml.safe_dump(roles[0].get("rules", [])))
PY
}

DISABLED="$(render false)"
ENABLED="$(render true)"
DISABLED_RULES="$(worker_role_rules "$DISABLED")"
ENABLED_RULES="$(worker_role_rules "$ENABLED")"

# (a) Off by default means no grant at all.
for resource in deployments services secrets networkpolicies; do
  if grep -q "$resource" <<<"$DISABLED_RULES"; then
    fail a "worker Role grants $resource with the reconciler disabled; the RBAC gate is not working"
  fi
done

# (b) ...without having eaten the rules that were already there.
grep -q "sandboxclaims" <<<"$DISABLED_RULES" || fail b "the disabled render lost the sandboxclaims rule"
grep -q "sandboxes" <<<"$DISABLED_RULES" || fail b "the disabled render lost the sandboxes rule"

# (c) Enabled: exactly the four verbs on each of the four kinds.
python3 - "$ENABLED_RULES" <<'PY' || exit 1
import sys, yaml
rules = yaml.safe_load(sys.argv[1])
want = {
    ("apps", "deployments"),
    ("", "services"),
    ("", "secrets"),
    ("networking.k8s.io", "networkpolicies"),
}
verbs = {"create", "list", "patch", "delete"}
seen = {}
for rule in rules:
    for group in rule["apiGroups"]:
        for resource in rule["resources"]:
            if (group, resource) in want:
                seen[(group, resource)] = set(rule["verbs"])
missing = want - set(seen)
if missing:
    print(f"FAIL [c] no rule grants {sorted(missing)}", file=sys.stderr)
    sys.exit(1)
for key, got in sorted(seen.items()):
    if got != verbs:
        print(
            f"FAIL [c] {key} grants {sorted(got)}, expected {sorted(verbs)}. "
            "`create` is required: a server-side apply of a missing object is "
            "authorized as a create.",
            file=sys.stderr,
        )
        sys.exit(1)
PY

# (d) No verb the client does not call.
python3 - "$ENABLED_RULES" <<'PY' || exit 1
import sys, yaml
for rule in yaml.safe_load(sys.argv[1]):
    if "networkpolicies" in rule["resources"] or "secrets" in rule["resources"]:
        for verb in ("get", "watch", "update", "*"):
            if verb in rule["verbs"]:
                print(f"FAIL [d] unexpected verb '{verb}' on {rule['resources']}", file=sys.stderr)
                sys.exit(1)
PY

# (e) The chart and the client agree on which kinds a connector is made of.
python3 - "$SOURCE" "$ENABLED_RULES" <<'PY' || exit 1
import re, sys, yaml

source = open(sys.argv[1]).read()
kinds = set(re.findall(r'^\s{4}"(\w+)": \(k8s_client', source, re.M))
if not kinds:
    print("FAIL [e] could not read the kind table out of connector_k8s.py", file=sys.stderr)
    sys.exit(1)

plural = {
    "Deployment": "deployments",
    "Service": "services",
    "Secret": "secrets",
    "NetworkPolicy": "networkpolicies",
}
granted = {r for rule in yaml.safe_load(sys.argv[2]) for r in rule["resources"]}
for kind in sorted(kinds):
    if kind not in plural:
        print(f"FAIL [e] {kind} is a new connector kind with no plural mapped here", file=sys.stderr)
        sys.exit(1)
    if plural[kind] not in granted:
        print(
            f"FAIL [e] connector_k8s.py handles {kind} but the Role does not grant "
            f"{plural[kind]}; the reconciler would fail to prune it",
            file=sys.stderr,
        )
        sys.exit(1)
PY

# (f) Namespaced only, and never pods.
if grep -qE '^kind: ClusterRole' <<<"$ENABLED"; then
  fail f "the worker template rendered a ClusterRole; connector RBAC must stay namespaced"
fi
# The base Role's exact-pod read (#3169) is not the connector's grant: check
# only the lines the reconciler flag adds.
CONNECTOR_RULES="$(comm -13 <(sort <<<"$DISABLED_RULES") <(sort <<<"$ENABLED_RULES"))"
if grep -q "pods" <<<"$CONNECTOR_RULES"; then
  fail f "the connector Role mentions pods; it manages objects, never pods directly"
fi

# (g) The RBAC and connector specific env are switched by the same flag. Half
#     of the pair is the worst outcome: the grant without the env is unused
#     authority, and the env without the grant is a loop that 403s every pass.
for required in CURIE_CONNECTOR_RECONCILE CURIE_CONNECTOR_APP_NAME; do
  grep -q "$required" <<<"$ENABLED" || fail g "enabling the reconciler did not render $required"
  grep -q "$required" <<<"$DISABLED" &&
    fail g "$required renders with the reconciler disabled; the env gate is not working"
done
for required in CURIE_API_URL CURIE_API_KEY; do
  grep -q "$required" <<<"$ENABLED" || fail g "enabled render is missing $required"
  grep -q "$required" <<<"$DISABLED" || fail g "disabled render is missing $required"
done

# Shared API wiring is not connector specific. The worker needs it in both
# renders so its runs and eval lanes can call the platform API.
for required in CURIE_API_URL CURIE_API_KEY; do
  grep -q "$required" <<<"$ENABLED" || fail g "the enabled render is missing shared API env $required"
  grep -q "$required" <<<"$DISABLED" || fail g "the disabled render is missing shared API env $required"
done

# (h) The worker reconciles the namespace and release it already tells the
#     runner about. Two settings could disagree, and the symptom would be a
#     connector that exists and an agent that cannot reach it.
grep -q "CURIE_NAMESPACE" <<<"$ENABLED" || fail h "CURIE_NAMESPACE is missing from the worker env"
grep -q "CURIE_RELEASE" <<<"$ENABLED" || fail h "CURIE_RELEASE is missing from the worker env"

echo "connector-reconciler-rbac-assertions: all eight assertions passed"
