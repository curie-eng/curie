#!/usr/bin/env bash
#
# Render-assertion test for the end to end connector's identity on a test
# cluster (ADR 0176 decision 4, #3243). The live denials are proven by
# charts/curie/ci/runtime/e2e-connector-identity-runtime.sh; this pins the
# rendered grant so a later edit cannot widen it without failing here.
#
#   (a) Off by default: a stock render contains none of it.
#   (b) Enabled: exactly one ClusterRoleBinding, and it binds the cluster role,
#       never the per namespace worker role.
#   (c) The cluster role holds no cluster scoped write beyond namespace create
#       and delete, no namespace update or patch, no wildcard, and `bind` only
#       on the worker role by name.
#   (d) The worker role is referenced by no ClusterRoleBinding (only by the
#       RoleBindings the connector creates inside its own namespaces).
#   (e) The admission policy fails closed, denies, matches this service account
#       and the service accounts inside its prefixed namespaces, requires a
#       Pod Security level on every namespace it creates, confines RoleBinding
#       subjects to the binding's namespace, and carries the configured prefix
#       and label.
#   (f) An invalid prefix, or one covering kube-system or the release's own
#       namespace, is refused at render time.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
NS="curie-e2e-identity-assert"

fail() {
  echo "FAIL [$1] $2" >&2
  exit 1
}

render() {
  helm template curie "$CHART" -n "$NS" -f "$CHART/values-dev.yaml" "$@"
}

stock="$(render)"
if grep -q "e2e-connector" <<<"$stock"; then
  fail a "a stock render contains e2e connector objects"
fi

enabled="$(render --set e2eConnectorIdentity.enabled=true \
  --set e2eConnectorIdentity.namespacePrefix=probe-ns- \
  --set e2eConnectorIdentity.ownerLabel.value=probe-owner \
  -s templates/e2e-connector-identity.yaml)"

python3 - "$enabled" <<'PY' || exit 1
import sys, yaml

docs = [d for d in yaml.safe_load_all(sys.argv[1]) if d]
by_kind = {}
for d in docs:
    by_kind.setdefault(d["kind"], []).append(d)

def fail(tag, msg):
    print(f"FAIL [{tag}] {msg}", file=sys.stderr)
    sys.exit(1)

sa = "curie-e2e-connector"
cluster_role = "curie-e2e-connector-cluster"
worker_role = "curie-e2e-connector-namespace"

crbs = by_kind.get("ClusterRoleBinding", [])
if len(crbs) != 1 or crbs[0]["roleRef"]["name"] != cluster_role:
    fail("b", f"expected one ClusterRoleBinding to {cluster_role}, got {crbs}")
if crbs[0]["subjects"] != [{"kind": "ServiceAccount", "name": sa, "namespace": "curie-e2e-identity-assert"}]:
    fail("b", f"ClusterRoleBinding subjects are {crbs[0]['subjects']}")
if any(r.get("kind") == "RoleBinding" for r in docs):
    fail("d", "the chart must not pre-bind the worker role anywhere")

roles = {d["metadata"]["name"]: d for d in by_kind.get("ClusterRole", [])}
if set(roles) != {cluster_role, worker_role}:
    fail("c", f"unexpected ClusterRoles {sorted(roles)}")

allowed = {
    ("", "namespaces"): {"get", "list", "watch", "create", "delete"},
    ("rbac.authorization.k8s.io", "rolebindings"): {"create"},
    ("rbac.authorization.k8s.io", "clusterroles"): {"bind"},
}
for rule in roles[cluster_role]["rules"]:
    for group in rule["apiGroups"]:
        for resource in rule["resources"]:
            verbs = set(rule["verbs"])
            if "*" in (group, resource) or "*" in verbs:
                fail("c", f"wildcard in cluster role rule {rule}")
            if (group, resource) not in allowed or not verbs <= allowed[(group, resource)]:
                fail("c", f"cluster role grants {sorted(verbs)} on {group}/{resource}")
            if resource == "clusterroles" and rule.get("resourceNames") != [worker_role]:
                fail("c", f"bind must be scoped to {worker_role}, got {rule.get('resourceNames')}")
            if resource == "namespaces" and verbs & {"update", "patch", "deletecollection"}:
                fail("c", "namespace update, patch or deletecollection granted")

for binding in crbs:
    if binding["roleRef"]["name"] == worker_role:
        fail("d", "worker role bound cluster wide")

policies = by_kind.get("ValidatingAdmissionPolicy", [])
bindings = by_kind.get("ValidatingAdmissionPolicyBinding", [])
if len(policies) != 1 or len(bindings) != 1:
    fail("e", "expected one policy and one binding")
spec = policies[0]["spec"]
if spec["failurePolicy"] != "Fail":
    fail("e", "policy must fail closed")
if bindings[0]["spec"]["validationActions"] != ["Deny"]:
    fail("e", "binding must deny")
if bindings[0]["spec"]["policyName"] != policies[0]["metadata"]["name"]:
    fail("e", "binding names another policy")
match = [c["expression"] for c in spec["matchConditions"]]
if match != [f"request.userInfo.username == 'system:serviceaccount:curie-e2e-identity-assert:{sa}' || request.userInfo.username.startsWith('system:serviceaccount:probe-ns-')"]:
    fail("e", f"policy matches {match}")
ops = spec["matchConstraints"]["resourceRules"][0]["operations"]
if set(ops) != {"CREATE", "UPDATE", "DELETE", "CONNECT"}:
    fail("e", f"policy covers operations {ops}")
text = " ".join(v["expression"] for v in spec["validations"])
for needle in (
    "startsWith('probe-ns-')",
    "['curietech.ai/e2e-owner'] == 'probe-owner'",
    "['pod-security.kubernetes.io/enforce'] in ['baseline', 'restricted']",
    "object.subjects.all(s, s.kind == 'ServiceAccount'",
):
    if needle not in text:
        fail("e", f"policy lacks {needle}")
print("ok: e2e connector identity grant")
PY

if render --set e2eConnectorIdentity.enabled=true \
  --set e2eConnectorIdentity.namespacePrefix="x')||true||('" \
  -s templates/e2e-connector-identity.yaml >/dev/null 2>&1; then
  fail f "an unsafe prefix rendered"
fi

for bad in kube- "${NS%-*}-"; do
  if render --set e2eConnectorIdentity.enabled=true \
    --set e2eConnectorIdentity.namespacePrefix="$bad" \
    -s templates/e2e-connector-identity.yaml >/dev/null 2>&1; then
    fail f "prefix $bad covering a system or the release namespace rendered"
  fi
done

echo "PASS e2e-connector-identity-assertions"
