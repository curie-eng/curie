#!/usr/bin/env bash
#
# Render-assertion test for personal Claude enrollment custody RBAC (#5377).
#
# `personalClaude.grantNamespace` is off by default. When set, the API gets
# Secret create/get/list/delete and the worker Secret get, each through a Role
# in the grant namespace bound to the release's ServiceAccount.
#
# Assertions:
#   (a) Unset (default): no personal-grants Role or RoleBinding renders and
#       neither Deployment carries CURIE_PERSONAL_GRANT_NAMESPACE.
#   (b) Set: the API Role and the worker Role live in the grant namespace with
#       exactly one rule each, on `secrets` only, with exactly the verbs
#       {create,get,list,delete} and {get}.
#   (c) Set: each RoleBinding is in the grant namespace, names its Role, and has
#       exactly one subject, the matching ServiceAccount in the RELEASE
#       namespace.
#   (d) Set: both Deployments carry CURIE_PERSONAL_GRANT_NAMESPACE with the
#       value, exactly once.
#   (e) A value equal to the release namespace fails the render.
#   (f) A values set with the key deleted entirely (a `--reuse-values` upgrade
#       from an older release) renders like the default.
#
# Runnable locally (from anywhere) and from CI. Fails loudly, naming the
# assertion.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
NS="curie-pg-assert"
GRANT="acme-personal-grants"
# Test hook: a different expected verb list proves the script can fail.
API_VERBS="${PERSONAL_GRANT_ASSERT_API_VERBS:-create,delete,get,list}"

fail() {
  echo "FAIL [$1] $2" >&2
  exit 1
}

render() {
  helm template curie "$CHART" -n "$NS" -f "$CHART/values-dev.yaml" "$@"
}

check() {
  printf '%s\n' "$2" | python3 -c "$CHECK_PY" "$1"
}

CHECK_PY="$(cat <<'PY'
import sys, yaml
mode, text = sys.argv[1], sys.stdin.read()
docs = [d for d in yaml.safe_load_all(text) if d]
ns, grant = "curie-pg-assert", "acme-personal-grants"
import os
api_verbs = os.environ.get("PERSONAL_GRANT_ASSERT_API_VERBS", "create,delete,get,list").split(",")

def pg(kind):
    return {d["metadata"]["name"]: d for d in docs
            if d.get("kind") == kind and d["metadata"]["name"].endswith("-personal-grants")}

def envs(name_suffix):
    out = []
    for d in docs:
        if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith(name_suffix):
            for c in d["spec"]["template"]["spec"]["containers"]:
                out += [e for e in c.get("env", []) if e["name"] == "CURIE_PERSONAL_GRANT_NAMESPACE"]
    return out

def die(tag, msg):
    print(f"FAIL [{tag}] {msg}", file=sys.stderr); sys.exit(1)

if mode == "off":
    if pg("Role") or pg("RoleBinding"):
        die("a", "personal-grants RBAC rendered with the value unset")
    for sfx in ("-api", "-worker"):
        if envs(sfx):
            die("a", f"CURIE_PERSONAL_GRANT_NAMESPACE rendered on {sfx} with the value unset")
    sys.exit(0)

roles, bindings = pg("Role"), pg("RoleBinding")
want = {"curie-api-personal-grants": (api_verbs, "curie-api"),
        "curie-worker-personal-grants": (["get"], "curie-worker")}
if set(roles) != set(want) or set(bindings) != set(want):
    die("b", f"expected Roles/RoleBindings {sorted(want)}, got {sorted(roles)} / {sorted(bindings)}")
for name, (verbs, sa) in want.items():
    r, b = roles[name], bindings[name]
    if r["metadata"].get("namespace") != grant:
        die("b", f"{name} Role namespace is {r['metadata'].get('namespace')!r}")
    if len(r["rules"]) != 1:
        die("b", f"{name} has {len(r['rules'])} rules, want 1")
    rule = r["rules"][0]
    if rule.get("apiGroups") != [""] or rule.get("resources") != ["secrets"]:
        die("b", f"{name} rule is not core/secrets only: {rule}")
    if sorted(rule["verbs"]) != sorted(verbs):
        die("b", f"{name} verbs {sorted(rule['verbs'])}, want {sorted(verbs)}")
    if b["metadata"].get("namespace") != grant:
        die("c", f"{name} RoleBinding namespace is {b['metadata'].get('namespace')!r}")
    if b["roleRef"]["kind"] != "Role" or b["roleRef"]["name"] != name:
        die("c", f"{name} RoleBinding roleRef is {b['roleRef']}")
    if b["subjects"] != [{"kind": "ServiceAccount", "name": sa, "namespace": ns}]:
        die("c", f"{name} subjects are {b['subjects']}")
for sfx in ("-api", "-worker"):
    e = envs(sfx)
    if len(e) != 1 or e[0].get("value") != grant:
        die("d", f"{sfx} Deployment env is {e}")
PY
)"


export PERSONAL_GRANT_ASSERT_API_VERBS="$API_VERBS"

# (a) default off.
check off "$(render)" || fail a "default render"
# (a) explicit empty.
check off "$(render --set personalClaude.grantNamespace=)" || fail a "empty render"
# (b)-(d) on.
check on "$(render --set "personalClaude.grantNamespace=$GRANT")" || fail b-d "enabled render"

# (e) equal to the release namespace.
if out="$(render --set "personalClaude.grantNamespace=$NS" 2>&1)"; then
  fail e "render succeeded with grantNamespace equal to the release namespace"
fi
grep -q "must differ from the release namespace" <<<"$out" \
  || fail e "refusal does not name the cause: $out"

# (f) key absent from the values (older release under --reuse-values): a null
# deletes the chart default, so the template sees no personalClaude map at all.
check off "$(render --set personalClaude=null)" || fail f "render without the personalClaude key"

echo "OK personal grant custody RBAC assertions"
