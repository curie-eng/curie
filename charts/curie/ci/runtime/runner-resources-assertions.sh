#!/usr/bin/env bash
#
# Render assertions for the per-agent runner resource override (#3209).
#
# The API compares one override block to resourceQuota.hard because this chart's
# sandbox pod has one regular container, every init container carries that same
# resources block, and the RuntimeClass this chart renders has no overhead.
# Those three facts are what make the direct comparison true. The quota env on
# the API deployment is that hard ceiling, set together, and only when the
# quota object itself renders. There is no container-count env.
#
# ValidatingAdmissionPolicy in admissionregistration.k8s.io/v1 is GA at
# Kubernetes 1.30. The group/version alone has existed since 1.16 for webhook
# configuration, so the render must see the policy kind via --api-versions.
# Passing --kube-version 1.29.0 with no --api-versions fails the render before
# the new template RBAC is installed. Passing --kube-version 1.30.0 and
# --api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy
# renders the policy.
#
# The RuntimeClass object is rendered only when
# security.gvisor.installRuntimeClass is true. The successful render sets that
# so the overhead assertion has the object this chart owns. An external
# RuntimeClass is outside this render.
#
# Render to a directory and read the written files. A piped `helm template`
# has been observed to truncate silently while still exiting 0.
#
# Fails loudly, naming the assertion. Runnable locally and from CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/../.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

# Render to $TMP/$1. Callers walk that directory. helm's stdout is discarded;
# a piped template has truncated silently while still exiting 0.
render_dir() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template curie "$CHART" --output-dir "$out" "$@" >/dev/null
}

PYCHECK="$TMP/check.py"
cat > "$PYCHECK" <<'PY'
"""Structural assertions over one rendered chart directory.

argv: enabled-render-root disabled-quota-render-root values.yaml
Exits 0 on success. Exits 1 with a message on failure.
"""
import pathlib
import re
import sys

import yaml

QUOTA_ENV = (
    "CURIE_SANDBOX_QUOTA_REQUESTS_CPU",
    "CURIE_SANDBOX_QUOTA_REQUESTS_MEMORY",
    "CURIE_SANDBOX_QUOTA_LIMITS_CPU",
    "CURIE_SANDBOX_QUOTA_LIMITS_MEMORY",
)
CONTAINER_COUNT_ENV = "CURIE_SANDBOX_RUNNER_RESOURCE_CONTAINERS"
TEMPLATE_RESOURCES = ("sandboxtemplates", "sandboxwarmpools")
TEMPLATE_VERBS = {"get", "create", "patch"}
TEMPLATE_GROUP = "extensions.agents.x-k8s.io"


def load_docs(root):
    docs = []
    for path in sorted(pathlib.Path(root).rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if isinstance(doc, dict):
                docs.append(doc)
    return docs


def has_key(value, wanted):
    if isinstance(value, dict):
        if wanted in value:
            return True
        return any(has_key(item, wanted) for item in value.values())
    if isinstance(value, list):
        return any(has_key(item, wanted) for item in value)
    return False


def labeled(docs, kind, component):
    return [
        doc
        for doc in docs
        if doc.get("kind") == kind
        and (doc.get("metadata") or {}).get("labels", {}).get("app.kubernetes.io/component")
        == component
    ]


def sandbox_resources(docs):
    found = []
    for doc in docs:
        if doc.get("kind") != "SandboxTemplate":
            continue
        spec = (((doc.get("spec") or {}).get("podTemplate") or {}).get("spec")) or {}
        found.append(((doc.get("metadata") or {}).get("name"), spec))
    if not found:
        sys.exit("no SandboxTemplate rendered")
    for name, spec in found:
        containers = spec.get("containers") or []
        if len(containers) != 1:
            sys.exit(
                f"SandboxTemplate {name}: expected exactly one regular container, "
                f"found {len(containers)}"
            )
        resources = containers[0].get("resources")
        inits = spec.get("initContainers") or []
        if not inits:
            sys.exit(f"SandboxTemplate {name}: no init container rendered")
        for init in inits:
            if init.get("resources") != resources:
                sys.exit(
                    f"SandboxTemplate {name}: init container {init.get('name')!r} "
                    f"resources {init.get('resources')!r} != runner resources {resources!r}"
                )
    return len(found)


def runtime_class_overhead(docs):
    classes = [doc for doc in docs if doc.get("kind") == "RuntimeClass"]
    if not classes:
        sys.exit(
            "no RuntimeClass rendered; security.gvisor.installRuntimeClass must "
            "render the object this chart owns"
        )
    for doc in classes:
        if has_key(doc, "overhead"):
            name = (doc.get("metadata") or {}).get("name")
            sys.exit(f"RuntimeClass {name} has an overhead key: {doc}")
    return len(classes)


def deployment_env(docs, component, container_name):
    deployments = labeled(docs, "Deployment", component)
    if len(deployments) != 1:
        sys.exit(f"expected one {component} Deployment, found {len(deployments)}")
    pod = (((deployments[0].get("spec") or {}).get("template") or {}).get("spec")) or {}
    containers = (pod.get("initContainers") or []) + (pod.get("containers") or [])
    named = [container for container in containers if container.get("name") == container_name]
    if len(named) != 1:
        sys.exit(f"expected one {container_name} container, found {len(named)}")
    values = {}
    names = set()
    for container in containers:
        for entry in container.get("env") or []:
            env_name = entry.get("name")
            names.add(env_name)
            if container is named[0] and "value" in entry:
                values[env_name] = str(entry["value"])
    return values, names


def quota_hard(values_path):
    hard = yaml.safe_load(pathlib.Path(values_path).read_text())["resourceQuota"]["hard"]
    return {
        "CURIE_SANDBOX_QUOTA_REQUESTS_CPU": str(hard["requestsCpu"]),
        "CURIE_SANDBOX_QUOTA_REQUESTS_MEMORY": str(hard["requestsMemory"]),
        "CURIE_SANDBOX_QUOTA_LIMITS_CPU": str(hard["limitsCpu"]),
        "CURIE_SANDBOX_QUOTA_LIMITS_MEMORY": str(hard["limitsMemory"]),
    }


def assert_quota_env(docs, values_path):
    expected = quota_hard(values_path)
    values, names = deployment_env(docs, "api", "api")
    for env_name, want in expected.items():
        got = values.get(env_name)
        if got != want:
            sys.exit(f"{env_name} is {got!r}, resourceQuota.hard expects {want!r}")
    if CONTAINER_COUNT_ENV in names:
        sys.exit(f"{CONTAINER_COUNT_ENV} is set; quota usage is the one override block")


def assert_quota_env_absent(docs):
    _values, names = deployment_env(docs, "api", "api")
    present = [name for name in (*QUOTA_ENV, CONTAINER_COUNT_ENV) if name in names]
    if present:
        sys.exit(f"resourceQuota.enabled=false still renders {present}")


def worker_sa_name(docs):
    accounts = labeled(docs, "ServiceAccount", "worker")
    if len(accounts) != 1:
        sys.exit(f"expected one worker ServiceAccount, found {len(accounts)}")
    name = (accounts[0].get("metadata") or {}).get("name")
    if not name or not str(name).endswith("-worker"):
        sys.exit(f"worker ServiceAccount name is {name!r}")
    return str(name)


def assert_policy(docs, sa_name):
    policies = [doc for doc in docs if doc.get("kind") == "ValidatingAdmissionPolicy"]
    if not policies:
        sys.exit("no ValidatingAdmissionPolicy rendered")
    text = "\n".join(yaml.safe_dump(doc) for doc in policies)
    if not re.search(r"-resources(?!-pool)", text):
        sys.exit("ValidatingAdmissionPolicy does not mention the -resources template suffix")
    if "-resources-pool" not in text:
        sys.exit("ValidatingAdmissionPolicy does not mention the -resources-pool suffix")
    if sa_name not in text:
        sys.exit(f"ValidatingAdmissionPolicy does not mention worker ServiceAccount {sa_name}")


def assert_worker_role(docs):
    roles = labeled(docs, "Role", "worker")
    if len(roles) != 1:
        sys.exit(f"expected one worker Role, found {len(roles)}")
    for resource in TEMPLATE_RESOURCES:
        matched = [
            rule
            for rule in (roles[0].get("rules") or [])
            if resource in (rule.get("resources") or [])
        ]
        if not matched:
            sys.exit(f"worker Role does not grant {resource}")
        verbs = set()
        for rule in matched:
            groups = rule.get("apiGroups") or []
            if groups != [TEMPLATE_GROUP]:
                sys.exit(f"{resource} apiGroups are {groups!r}, expected [{TEMPLATE_GROUP!r}]")
            verbs.update(rule.get("verbs") or [])
        if verbs != TEMPLATE_VERBS:
            sys.exit(
                f"{resource} verbs are {sorted(verbs)}, expected {sorted(TEMPLATE_VERBS)} "
                "(get, create, patch only; list and delete are not granted)"
            )


enabled, disabled, values_path = sys.argv[1:]
enabled_docs = load_docs(enabled)
disabled_docs = load_docs(disabled)
sandbox_resources(enabled_docs)
runtime_class_overhead(enabled_docs)
assert_quota_env(enabled_docs, values_path)
assert_quota_env_absent(disabled_docs)
sa_name = worker_sa_name(enabled_docs)
assert_policy(enabled_docs, sa_name)
assert_worker_role(enabled_docs)
print(f"  ok: worker ServiceAccount {sa_name}")
PY

echo "=== kube 1.29 without the policy kind fails the render ==="
mkdir -p "$TMP/kube129"
if helm template curie "$CHART" \
  --kube-version 1.29.0 \
  --output-dir "$TMP/kube129" \
  >"$TMP/kube129.out" 2>"$TMP/kube129.err"; then
  fail "helm template --kube-version 1.29.0 with no --api-versions exited 0"
fi
echo "  ok: exited non-zero"

POLICY_ARGS=(
  --kube-version 1.30.0
  --api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy
)

echo "=== kube 1.30 with the policy kind renders the sandbox, quota env, and admission rule ==="
render_dir enabled \
  "${POLICY_ARGS[@]}" \
  --set security.gvisor.installRuntimeClass=true

echo "=== resourceQuota.enabled=false omits the four quota env vars ==="
render_dir disabled \
  "${POLICY_ARGS[@]}" \
  --set resourceQuota.enabled=false

python3 "$PYCHECK" "$TMP/enabled" "$TMP/disabled" "$CHART/values.yaml" \
  || fail "runner resource render"

echo
echo "All runner resource render assertions passed."
