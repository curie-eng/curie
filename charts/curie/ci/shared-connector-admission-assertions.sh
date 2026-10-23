#!/usr/bin/env bash
#
# Render assertions for the worker's Secret admission with shared connector
# applications (ADR 0239, cp5, AC4).
#
# The worker creates exactly three Secret shapes in the release namespace: the
# per-claim -tokens Secret (#3842), and, for each shared application, one key
# Secret (shared-key-<uuid>, data key private-keys) and one sealed DSN Secret
# (shared-dsn-<uuid>, data key sealed-dsn). The <fullname>-worker-secrets
# ValidatingAdmissionPolicy admits only those shapes.
#
# Asserts:
#
#   1. The policy has exactly one validation, whose expression is the OR of
#      variables.tokens, variables.sharedKey and variables.sharedDsn and of
#      nothing else.
#   2. Each of those variables carries its whole shape:
#        tokens:    the claim label, the -tokens suffix, Opaque, and a refusal
#                   of both shared labels;
#        sharedKey: no claim label, the canonical UUID pattern on
#                   curietech.ai/shared-application, role encryption-key,
#                   component shared-connector-application, the name
#                   'shared-key-' + the application label, Opaque, exactly one
#                   data key named private-keys, no stringData;
#        sharedDsn: the same with database-dsn, 'shared-dsn-' and sealed-dsn.
#      A shared variable never names the other shape's prefix, role or key,
#      nor -tokens.
#   3. The message keeps the -tokens wording as its first clause and names both
#      shared prefixes. The matchCondition (worker SA), failurePolicy Fail,
#      CREATE of v1 secrets, the release namespaceSelector and the Deny binding
#      are unchanged.
#   4. The API identity holds no Secret access: no Role or ClusterRole bound to
#      <fullname>-api has a rule on secrets (or "*").
#   5. The worker Role's verbs are unchanged: secrets create only without the
#      reconciler; with it, secrets/services/deployments/networkpolicies are
#      exactly create, list, patch, delete (no get, watch or update on secrets).
#   6. Without agentSandbox.deploy the policy does not render (pre-existing,
#      plan edge case 10).
#
# Negative controls (in process, on mutated copies of the real render): drop
# the sharedKey disjunct; drop each required needle from each shared variable
# one at a time; swap the two shared prefixes; add a secrets rule to the API
# Role; add get to the worker's secrets verbs. Every mutation must make the
# checker report a failure, or this script fails.
#
# This pins the policy TEXT. No CEL evaluator ships in the uv environment, so
# the executable proof that each shape is admitted or refused is the kind
# runtime (ci/runtime/shared-connector-kind-runtime.sh), which sends every
# allowed and denied shape as the worker identity with --dry-run=server.
#
# Render to a directory and read the written files; a piped `helm template`
# has been observed to truncate silently while still exiting 0.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT

RELEASE=sca
NAMESPACE=sca-ns
FULLNAME=sca-curie

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

command -v helm >/dev/null || fail "helm is required"
python3 -c 'import yaml' 2>/dev/null || fail "python3 with PyYAML is required"

render_dir() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
    --output-dir "$out" "$@" >/dev/null
  printf '%s\n' "$out"
}

SANDBOX_OUT="$(render_dir sandbox --set agentSandbox.deploy=true)" \
  || fail "agentSandbox.deploy=true must render"
RECONCILER_OUT="$(render_dir reconciler --set agentSandbox.deploy=true \
  --set worker.connectorReconciler.enabled=true)" \
  || fail "agentSandbox.deploy=true with the connector reconciler must render"
NOSANDBOX_OUT="$(render_dir nosandbox --set agentSandbox.deploy=false)" \
  || fail "agentSandbox.deploy=false must render"

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import copy
import pathlib
import re
import sys

import yaml

CLAIM = "curietech.ai/sandbox-claim"
APP_LABEL = "curietech.ai/shared-application"
ROLE_LABEL = "curietech.ai/shared-secret"
COMPONENT = "shared-connector-application"
UUID_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
TOKENS_MESSAGE = (
    "the worker may create only Opaque per-claim -tokens Secrets labelled "
    "curietech.ai/sandbox-claim"
)
VARIABLES = ("tokens", "sharedKey", "sharedDsn")

SHAPES = {
    "sharedKey": {
        "need": ("'shared-key-'", "'encryption-key'", "'private-keys'"),
        "forbid": ("shared-dsn-", "database-dsn", "sealed-dsn", "-tokens"),
    },
    "sharedDsn": {
        "need": ("'shared-dsn-'", "'database-dsn'", "'sealed-dsn'"),
        "forbid": ("shared-key-", "encryption-key", "private-keys", "-tokens"),
    },
}
SHARED_COMMON = (
    CLAIM,
    APP_LABEL,
    ROLE_LABEL,
    f"'{COMPONENT}'",
    "app.kubernetes.io/component",
    "'Opaque'",
    "object.type",
    "object.data",
    "size() == 1",
    "stringData",
    UUID_PATTERN,
    "object.metadata.name",
)
TOKENS_NEED = (CLAIM, "'-tokens'", "'Opaque'", APP_LABEL, ROLE_LABEL, "object.metadata.name")


def needle_regex(needle):
    """Match a needle ignoring whitespace and the CEL quote style."""

    parts = []
    for char in needle:
        if char.isspace():
            continue
        parts.append("['\"]" if char in "'\"" else re.escape(char))
    return re.compile(r"\s*".join(parts))


def has(text, needle):
    return needle_regex(needle).search(text) is not None


def load(root):
    docs = []
    for path in sorted(pathlib.Path(root).rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if isinstance(doc, dict):
                docs.append(doc)
    if not docs:
        raise SystemExit(f"{root}: Helm wrote no YAML documents")
    return docs


def find(docs, kind, name):
    return [
        d for d in docs
        if d.get("kind") == kind and (d.get("metadata") or {}).get("name") == name
    ]


def expanded(variables, name, seen=()):
    """A variable's expression with every variables.<x> reference inlined."""

    if name in seen:
        return ""
    text = variables.get(name, "")
    for other in variables:
        if other != name:
            text = re.sub(
                rf"\bvariables\.{re.escape(other)}\b",
                lambda _m, o=other: "(" + expanded(variables, o, (*seen, name)) + ")",
                text,
            )
    return text


def check_policy(docs, fullname, namespace):
    errors = []
    name = f"{fullname}-worker-secrets"
    found = find(docs, "ValidatingAdmissionPolicy", name)
    if len(found) != 1:
        return [f"expected one ValidatingAdmissionPolicy {name}, found {len(found)}"]
    spec = found[0].get("spec") or {}
    validations = spec.get("validations") or []
    if len(validations) != 1:
        return [f"{name}: expected exactly one validation, found {len(validations)}"]
    expression = validations[0].get("expression") or ""
    terms = [t for t in re.sub(r"[()\s]", "", expression).split("||") if t]
    if sorted(terms) != sorted(f"variables.{v}" for v in VARIABLES):
        errors.append(
            f"{name}: the validation must be exactly variables.tokens || variables.sharedKey "
            f"|| variables.sharedDsn, got {expression!r}"
        )
    variables = {
        v.get("name"): v.get("expression") or "" for v in spec.get("variables") or []
    }
    for wanted in VARIABLES:
        if wanted not in variables:
            errors.append(f"{name}: no variable named {wanted}")
    if errors:
        return errors

    tokens = expanded(variables, "tokens")
    missing = [n for n in TOKENS_NEED if not has(tokens, n)]
    if missing:
        errors.append(f"{name}: variables.tokens lacks {missing}: {tokens!r}")
    for shape, want in SHAPES.items():
        text = expanded(variables, shape)
        missing = [n for n in (*SHARED_COMMON, *want["need"]) if not has(text, n)]
        if missing:
            errors.append(f"{name}: variables.{shape} lacks {missing}: {text!r}")
        stray = [n for n in want["forbid"] if has(text, n)]
        if stray:
            errors.append(f"{name}: variables.{shape} names another shape's {stray}: {text!r}")

    message = validations[0].get("message") or ""
    if not message.startswith(TOKENS_MESSAGE):
        errors.append(f"{name}: the message must start with the -tokens clause, got {message!r}")
    for needle in ("shared-key-", "shared-dsn-"):
        if needle not in message:
            errors.append(f"{name}: the message does not name {needle}: {message!r}")

    if spec.get("failurePolicy") != "Fail":
        errors.append(f"{name}: failurePolicy is {spec.get('failurePolicy')!r}, want Fail")
    conditions = spec.get("matchConditions") or []
    want_user = f"system:serviceaccount:{namespace}:{fullname}-worker"
    if len(conditions) != 1 or want_user not in (conditions[0].get("expression") or ""):
        errors.append(f"{name}: matchConditions do not pin {want_user}: {conditions!r}")
    constraints = spec.get("matchConstraints") or {}
    rules = constraints.get("resourceRules") or []
    shape = [
        (r.get("apiGroups"), r.get("apiVersions"), r.get("resources"), r.get("operations"))
        for r in rules
    ]
    if shape != [([""], ["v1"], ["secrets"], ["CREATE"])]:
        errors.append(f"{name}: resourceRules are {shape}, want CREATE of v1 secrets only")
    selector = (constraints.get("namespaceSelector") or {}).get("matchLabels") or {}
    if selector != {"kubernetes.io/metadata.name": namespace}:
        errors.append(f"{name}: namespaceSelector is {selector!r}, want the release namespace")
    bindings = [
        d for d in docs
        if d.get("kind") == "ValidatingAdmissionPolicyBinding"
        and (d.get("spec") or {}).get("policyName") == name
    ]
    if len(bindings) != 1 or (bindings[0].get("spec") or {}).get("validationActions") != ["Deny"]:
        errors.append(f"{name}: expected one Deny binding, found {bindings!r}")
    return errors


def bound_roles(docs, namespace, account):
    """Every (kind, name) of a Role or ClusterRole bound to the ServiceAccount."""

    roles = set()
    for doc in docs:
        if doc.get("kind") not in ("RoleBinding", "ClusterRoleBinding"):
            continue
        for subject in doc.get("subjects") or []:
            if (
                subject.get("kind") == "ServiceAccount"
                and subject.get("name") == account
                and subject.get("namespace", namespace) == namespace
            ):
                ref = doc.get("roleRef") or {}
                roles.add((ref.get("kind"), ref.get("name")))
    return roles


def check_api_rbac(docs, fullname, namespace):
    errors = []
    account = f"{fullname}-api"
    roles = bound_roles(docs, namespace, account)
    if not roles:
        return [f"no Role or ClusterRole is bound to {account}; the render changed shape"]
    for kind, name in sorted(roles):
        for doc in find(docs, kind, name):
            for rule in doc.get("rules") or []:
                resources = rule.get("resources") or []
                if "secrets" in resources or "*" in resources:
                    errors.append(f"{kind} {name} bound to {account} grants {rule!r}")
    return errors


def verbs_for(docs, fullname, resource, group):
    roles = find(docs, "Role", f"{fullname}-worker")
    if len(roles) != 1:
        return None
    verbs = set()
    for rule in roles[0].get("rules") or []:
        if resource in (rule.get("resources") or []) and group in (rule.get("apiGroups") or []):
            verbs.update(rule.get("verbs") or [])
    return verbs


def check_worker_rbac(docs, fullname, reconciler):
    errors = []
    full = {"create", "list", "patch", "delete"}
    want = {
        ("secrets", ""): full if reconciler else {"create"},
        ("services", ""): full if reconciler else set(),
        ("deployments", "apps"): full if reconciler else set(),
        ("networkpolicies", "networking.k8s.io"): full if reconciler else set(),
    }
    for (resource, group), verbs in want.items():
        got = verbs_for(docs, fullname, resource, group)
        if got is None:
            return [f"expected one Role {fullname}-worker"]
        if got != verbs:
            errors.append(
                f"worker Role verbs on {group or 'core'}/{resource} are {sorted(got)}, "
                f"want {sorted(verbs)}"
            )
    return errors


def mutate_policy(docs, fullname, change):
    out = copy.deepcopy(docs)
    policy = find(out, "ValidatingAdmissionPolicy", f"{fullname}-worker-secrets")[0]
    change(policy["spec"])
    return out


def set_variable(spec, name, transform):
    for variable in spec.get("variables") or []:
        if variable.get("name") == name:
            variable["expression"] = transform(variable.get("expression") or "")


def negative_controls(docs, fullname, namespace):
    """Each mutation must be caught; returns the labels of mutations that were not."""

    escaped = []

    def expect_caught(label, mutated, checker):
        if not checker(mutated):
            escaped.append(label)

    policy_check = lambda d: check_policy(d, fullname, namespace)  # noqa: E731

    def drop_shared_key(spec):
        spec["validations"][0]["expression"] = re.sub(
            r"\|\|\s*variables\.sharedKey|variables\.sharedKey\s*\|\|", "",
            spec["validations"][0]["expression"],
        )

    expect_caught("validation without the sharedKey disjunct",
                  mutate_policy(docs, fullname, drop_shared_key), policy_check)

    def add_disjunct(spec):
        spec["validations"][0]["expression"] += " || true"

    expect_caught("validation with an extra always-true disjunct",
                  mutate_policy(docs, fullname, add_disjunct), policy_check)

    for shape, want in SHAPES.items():
        for needle in (*SHARED_COMMON, *want["need"]):
            def strip(spec, shape=shape, needle=needle):
                # Strip from the variable itself and from every helper variable
                # it references, so a needle factored into a helper is removed
                # too.
                targets = {shape} | referenced(spec, shape)
                for variable in spec.get("variables") or []:
                    if variable.get("name") in targets:
                        variable["expression"] = needle_regex(needle).sub(
                            "", variable.get("expression") or ""
                        )

            expect_caught(f"variables.{shape} without {needle}",
                          mutate_policy(docs, fullname, strip), policy_check)

    def swap_prefixes(spec):
        set_variable(spec, "sharedKey", lambda e: e.replace("'shared-key-'", "'shared-dsn-'"))

    expect_caught("variables.sharedKey naming the DSN prefix",
                  mutate_policy(docs, fullname, swap_prefixes), policy_check)

    def tokens_admits_shared(spec):
        set_variable(spec, "tokens", lambda e: e.replace(APP_LABEL, "curietech.ai/unrelated"))

    expect_caught("variables.tokens no longer refusing the shared application label",
                  mutate_policy(docs, fullname, tokens_admits_shared), policy_check)

    api = copy.deepcopy(docs)
    for kind, name in bound_roles(api, namespace, f"{fullname}-api"):
        for doc in find(api, kind, name):
            doc.setdefault("rules", []).append(
                {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}
            )
    expect_caught("API Role with a secrets rule", api,
                  lambda d: check_api_rbac(d, fullname, namespace))

    worker = copy.deepcopy(docs)
    find(worker, "Role", f"{fullname}-worker")[0]["rules"].append(
        {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}
    )
    expect_caught("worker Role with get on secrets", worker,
                  lambda d: check_worker_rbac(d, fullname, True))
    return escaped


def referenced(spec, shape):
    """Every helper variable a shape's variable references, transitively."""

    variables = {v.get("name"): v.get("expression") or "" for v in spec.get("variables") or []}
    found, frontier = set(), [shape]
    while frontier:
        current = frontier.pop()
        for other in variables:
            if (
                other not in found
                and other != shape
                and re.search(rf"\bvariables\.{re.escape(other)}\b", variables.get(current, ""))
            ):
                found.add(other)
                frontier.append(other)
    return found


def main():
    mode, root, fullname, namespace = sys.argv[1:5]
    docs = load(root)
    if mode == "policy":
        errors = check_policy(docs, fullname, namespace)
    elif mode == "api-rbac":
        errors = check_api_rbac(docs, fullname, namespace)
    elif mode == "worker-rbac":
        errors = check_worker_rbac(docs, fullname, sys.argv[5] == "reconciler")
    elif mode == "absent":
        name = f"{fullname}-worker-secrets"
        errors = (
            [f"{name} rendered without agentSandbox.deploy"]
            if find(docs, "ValidatingAdmissionPolicy", name) else []
        )
    elif mode == "controls":
        live = check_policy(docs, fullname, namespace)
        if live:
            errors = ["negative controls need a passing render first: " + "; ".join(live)]
        else:
            escaped = negative_controls(docs, fullname, namespace)
            errors = [f"negative control not caught: {label}" for label in escaped]
    else:
        raise SystemExit(f"unknown mode {mode!r}")
    for error in errors:
        print(f"  {error}", file=sys.stderr)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
PY

check() {
  python3 "$CHECKER" "$1" "$2" "$FULLNAME" "$NAMESPACE" "${@:3}"
}

echo "=== Assertion 1-3: worker-secrets admits exactly the -tokens, key and DSN shapes ==="
check policy "$SANDBOX_OUT" || fail "assertions 1-3 (agentSandbox.deploy=true)"
check policy "$RECONCILER_OUT" || fail "assertions 1-3 (with the connector reconciler)"
echo "  ok: one validation ORs tokens, sharedKey and sharedDsn, each carrying its whole shape"

echo "=== Assertion 4: the API identity holds no Secret access ==="
check api-rbac "$SANDBOX_OUT" || fail "assertion 4 (agentSandbox.deploy=true)"
check api-rbac "$RECONCILER_OUT" || fail "assertion 4 (with the connector reconciler)"
echo "  ok: no Role or ClusterRole bound to ${FULLNAME}-api grants secrets"

echo "=== Assertion 5: the worker Role's verbs are unchanged ==="
check worker-rbac "$SANDBOX_OUT" sandbox || fail "assertion 5 (without the reconciler)"
check worker-rbac "$RECONCILER_OUT" reconciler || fail "assertion 5 (with the reconciler)"
echo "  ok: secrets create only without the reconciler; create, list, patch, delete with it"

echo "=== Assertion 6: no worker-secrets policy without agentSandbox.deploy ==="
check absent "$NOSANDBOX_OUT" || fail "assertion 6"
echo "  ok: the policy renders only with agentSandbox.deploy (Secret creates are then RBAC-bounded)"

echo "=== Negative controls: every mutation of the real render is caught ==="
check controls "$RECONCILER_OUT" || fail "negative controls"
echo "  ok: dropping a disjunct, any shape needle, a prefix, or adding Secret RBAC fails the checks"

echo
echo "PASS: the worker may create only the -tokens, shared key and shared DSN Secret shapes; the API holds no Secret access; the worker Role is unchanged"
