#!/usr/bin/env bash
# Render contract for managed workspaces and approval-gated publication.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RENDERED="$TMP/rendered.yaml"
SEALED="$TMP/sealed.yaml"
helm template remote-dev "$CHART" -f "$CHART/values-dev.yaml" \
  --set agentSandbox.deploy=true \
  --set agentSandbox.controller.deploy=false > "$RENDERED"
helm template remote-dev "$CHART" --show-only templates/secrets.yaml > "$SEALED"

# The chart's install-time schema is the first rejection point for this API
# setting. Keep it no broader than the API's canonical owner/repository policy:
# in particular, a repository name cannot end in a dot.
for invalid_allowlist_entry in \
  "acme-corp/acme-bot." \
  "acme-corp/.acme-bot" \
  "acme-corp/acme-bot/" \
  "acme-corp/*/"; do
  if helm template remote-dev "$CHART" \
    --set-json "api.githubRepoAllowlist=[\"${invalid_allowlist_entry}\"]" \
    >/dev/null 2>&1; then
    echo "api.githubRepoAllowlist accepted invalid API value: ${invalid_allowlist_entry}" >&2
    exit 1
  fi
done

python3 - "$RENDERED" "$SEALED" "$CHART/values.yaml" <<'PY'
import sys
from pathlib import Path

import yaml

rendered_path, sealed_path, values_path = map(Path, sys.argv[1:])
docs = [doc for doc in yaml.safe_load_all(rendered_path.read_text()) if doc]
sealed_docs = [doc for doc in yaml.safe_load_all(sealed_path.read_text()) if doc]
values = yaml.safe_load(values_path.read_text())


def fail(message):
    raise AssertionError(message)


def one(kind, *, component=None, name_suffix=None):
    matches = []
    for doc in docs:
        if doc.get("kind") != kind:
            continue
        metadata = doc.get("metadata") or {}
        labels = metadata.get("labels") or {}
        if component is not None and labels.get("app.kubernetes.io/component") != component:
            continue
        if name_suffix is not None and not str(metadata.get("name", "")).endswith(name_suffix):
            continue
        matches.append(doc)
    if len(matches) != 1:
        fail(f"expected one {kind} component={component!r} suffix={name_suffix!r}, got {len(matches)}")
    return matches[0]


def containers(workload):
    if workload["kind"] == "Deployment":
        return workload["spec"]["template"]["spec"]["containers"]
    return workload["spec"]["podTemplate"]["spec"]["containers"]


def env_map(container):
    return {entry["name"]: entry for entry in container.get("env", [])}


# Dedicated worker auth is generated independently and mounted only into the
# API and worker. It is neither the platform CLI key nor sandbox state.
secret = one("Secret", name_suffix="-secrets")
string_data = secret.get("stringData") or {}
worker_auth = string_data.get("internalWorkerToken")
api_key = string_data.get("apiKey")
if not worker_auth:
    fail("development Secret must carry internalWorkerToken")
if worker_auth == api_key:
    fail("internalWorkerToken must not equal the platform apiKey")
sealed_secret = next(doc for doc in sealed_docs if doc.get("kind") == "Secret")
sealed_worker_auth = (sealed_secret.get("stringData") or {}).get("internalWorkerToken")
if not sealed_worker_auth or len(sealed_worker_auth) < 32:
    fail("sealed internalWorkerToken must be a generated strong value")
if sealed_worker_auth == (sealed_secret.get("stringData") or {}).get("apiKey"):
    fail("sealed internalWorkerToken must not equal the platform apiKey")

api = one("Deployment", component="api")
worker = one("Deployment", component="worker")
api_env = env_map(containers(api)[0])
if api_env.get("GITHUB_REPO_ALLOWLIST", {}).get("value") != "[]":
    fail("runtime repository allowlist must render explicitly and default deny-all")
for workload in (api, worker):
    env = env_map(containers(workload)[0])
    ref = env.get("CURIE_INTERNAL_WORKER_TOKEN", {}).get("valueFrom", {}).get("secretKeyRef", {})
    if ref.get("key") != "internalWorkerToken":
        fail(f"{workload['metadata']['name']} must mount internalWorkerToken")

for workload in [doc for doc in docs if doc.get("kind") == "Deployment" and doc not in (api, worker)]:
    for container in containers(workload):
        if "CURIE_INTERNAL_WORKER_TOKEN" in env_map(container):
            fail(f"internal worker auth leaked into {workload['metadata']['name']}")


# Publication authority lives only in the dedicated namespace Role. The main
# worker Role remains free of publication resources.
worker_role = one("Role", component="worker")
worker_resources = {
    resource
    for rule in worker_role.get("rules") or []
    for resource in rule.get("resources") or []
}
if worker_resources & {"jobs", "configmaps", "secrets", "pods/log"}:
    fail(f"main worker Role carries publication authority: {sorted(worker_resources)}")
# The one pod grant is the exact-name read an unschedulable claim needs
# (#3169); publication's pod authority is list plus pods/log.
pod_verbs = {
    verb
    for rule in worker_role.get("rules") or []
    if "pods" in (rule.get("resources") or [])
    for verb in rule.get("verbs") or []
}
if pod_verbs - {"get"}:
    fail(f"main worker Role grants pods beyond get: {sorted(pod_verbs)}")

role = one("Role", component="publication-worker")
rules = role.get("rules") or []
resources_to_verbs = {}
for rule in rules:
    for resource in rule.get("resources") or []:
        resources_to_verbs.setdefault(resource, set()).update(rule.get("verbs") or [])
for resource in ("jobs", "configmaps", "secrets"):
    if not {"create", "get", "delete"} <= resources_to_verbs.get(resource, set()):
        fail(f"worker Role is missing publication lifecycle verbs for {resource}")
if "get" not in resources_to_verbs.get("pods/log", set()):
    fail("worker Role cannot read the publication Job URL marker")
if "list" not in resources_to_verbs.get("pods", set()):
    fail("worker Role cannot discover the publication Job pod")
if "pods/exec" in resources_to_verbs:
    fail("worker Role must never grant pods/exec")


# Publication Jobs use a no-RBAC, tokenless identity. No RoleBinding may name
# it; the Job itself is built dynamically and separately unit-tested.
publication_sa = one("ServiceAccount", component="publication")
if publication_sa.get("automountServiceAccountToken") is not False:
    fail("publication ServiceAccount must disable token automount")
publication_sa_name = publication_sa["metadata"]["name"]
publication_namespace = publication_sa["metadata"].get("namespace")
if not publication_namespace or publication_namespace == worker["metadata"].get("namespace", "default"):
    fail("publication resources must use a dedicated namespace")
for binding in [doc for doc in docs if doc.get("kind") in ("RoleBinding", "ClusterRoleBinding")]:
    if any(subject.get("name") == publication_sa_name for subject in binding.get("subjects") or []):
        fail("publication ServiceAccount must have no RBAC binding")
owner = one("ConfigMap", component="publication-owner")
if owner["metadata"].get("namespace") != publication_namespace:
    fail("publication owner is outside the publication namespace")
publication_binding = one("RoleBinding", component="publication-worker")
subjects = publication_binding.get("subjects") or []
worker_service_account = worker["spec"]["template"]["spec"].get("serviceAccountName")
release_fullname = worker["metadata"]["name"].removesuffix("-worker")
release_namespace = publication_namespace.removesuffix(
    f"-{release_fullname}-publication"
)
if not any(
    subject.get("name") == worker_service_account
    and subject.get("namespace") == release_namespace
    for subject in subjects
):
    fail("publication RoleBinding must bind the release worker ServiceAccount")
for secret_doc in [doc for doc in docs if doc.get("kind") == "Secret"]:
    if secret_doc.get("metadata", {}).get("namespace") == publication_namespace:
        fail("operator credential Secret was copied into the publication namespace")
publication_policy = one("NetworkPolicy", component="publication")
publication_selector = publication_policy["spec"]["podSelector"]["matchLabels"]
if publication_selector != {"curietech.ai/component": "publication"}:
    fail(f"publication NetworkPolicy selector drifted: {publication_selector!r}")
if set(publication_policy["spec"].get("policyTypes") or []) != {"Ingress", "Egress"}:
    fail("publication NetworkPolicy must deny ingress and restrict egress")
if publication_policy["spec"].get("ingress") != []:
    fail("publication NetworkPolicy ingress must be an explicit deny-all list")
egress = publication_policy["spec"].get("egress") or []
if len(egress) != 2 or any(not rule.get("to") for rule in egress):
    fail("publication NetworkPolicy must render two non-empty egress destinations")
dns_rules = [
    rule for rule in egress
    if {port.get("port") for port in rule.get("ports") or []} == {53}
]
github_rules = [
    rule for rule in egress
    if rule.get("ports") == [{"protocol": "TCP", "port": 443}]
]
if len(dns_rules) != 1 or len(github_rules) != 1:
    fail("publication egress must contain one DNS rule and one GitHub HTTPS rule")
rendered_cidrs = {
    target.get("ipBlock", {}).get("cidr")
    for target in github_rules[0]["to"]
}
expected_cidrs = set(values["worker"]["publication"]["githubHttpsCidrs"])
if rendered_cidrs != expected_cidrs:
    fail(f"publication GitHub CIDR egress drifted: {sorted(rendered_cidrs)!r}")


# Worker scratch is private and bounded; worker resources include explicit
# memory/CPU/ephemeral limits for clone/archive work.
worker_pod = worker["spec"]["template"]["spec"]
if worker_pod.get("securityContext") != {
    "fsGroup": 1000,
    "fsGroupChangePolicy": "OnRootMismatch",
}:
    fail("worker pod must give the non-root clone process fsGroup ownership")
clone_volume = next((v for v in worker_pod.get("volumes", []) if v.get("name") == "workspace-clone"), None)
if clone_volume is None or clone_volume.get("emptyDir", {}).get("sizeLimit") != "4Gi":
    fail("worker workspace-clone emptyDir must be bounded to 4Gi")
worker_container = containers(worker)[0]
if not any(m.get("name") == "workspace-clone" for m in worker_container.get("volumeMounts", [])):
    fail("worker must mount the workspace-clone volume")
worker_env = env_map(worker_container)
if worker_env.get("CURIE_WORKSPACE_SCRATCH_ROOT", {}).get("value") != "/var/lib/curie/workspaces/worker":
    fail("worker scratch must use an owned child below the fsGroup-owned mount root")
for name, expected in {
    "CURIE_WORKSPACE_MAX_CHECKOUT_BYTES": "536870912",
    "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES": "268435456",
}.items():
    rendered = worker_env.get(name, {}).get("value")
    if rendered != expected:
        fail(f"{name} must render as a decimal integer, got {rendered!r}")
expected_worker_resources = {
    "requests": {"cpu": "500m", "memory": "512Mi", "ephemeral-storage": "2Gi"},
    "limits": {"cpu": "2", "memory": "1Gi", "ephemeral-storage": "8Gi"},
}
if worker_container.get("resources") != expected_worker_resources:
    fail(f"worker clone resources differ: {worker_container.get('resources')!r}")


# Sandbox workspace consumers receive only claim-scoped workspace facts. The
# workspace init stage and runner share a dedicated 1Gi /workspace; no
# workspace/GitHub/internal-worker identity reaches those consumers and
# /workspace is not a general writable-root path. The established bundle-fetch
# S3 identity is covered independently by object-store-web-identity-assertions.
sandbox = one("SandboxTemplate", component="agent-sandbox")
pod = sandbox["spec"]["podTemplate"]["spec"]
workspace_volume = next((v for v in pod.get("volumes", []) if v.get("name") == "workspace"), None)
if workspace_volume is None or workspace_volume.get("emptyDir", {}).get("sizeLimit") != "1Gi":
    fail("sandbox workspace emptyDir must be bounded to 1Gi")
init_containers = list(pod.get("initContainers") or [])
runner = next((c for c in pod.get("containers") or [] if c.get("name") == "runner"), None)
if runner is None:
    fail("sandbox is missing runner")
workspace_init = next(
    (container for container in init_containers if container.get("name") == "workspace-init"),
    None,
)
if workspace_init is None:
    fail("sandbox must render one workspace-init download-and-extract stage")
if any(container.get("name") in {"workspace-fetch", "workspace-extract"} for container in init_containers):
    fail("workspace download and extraction must not cross an init-container handoff")
if not any(
    mount.get("name") == "workspace" and mount.get("mountPath") == "/workspace"
    for mount in workspace_init.get("volumeMounts", [])
):
    fail("workspace-init must share /workspace")
if not any(m.get("name") == "workspace" and m.get("mountPath") == "/workspace"
           for m in runner.get("volumeMounts", [])):
    fail("runner must share workspace at /workspace")

signed_workspace_facts = {"CURIE_WORKSPACE_REF", "CURIE_WORKSPACE_SHA256"}
fetch_env = set(env_map(workspace_init))
if fetch_env != signed_workspace_facts:
    fail(
        "workspace-init must carry only the signed exact-object reference and digest; "
        f"rendered env was {sorted(fetch_env)}"
    )

names = set(env_map(workspace_init))
forbidden = {
    "S3_ACCESS_KEY", "S3_SECRET_KEY", "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "GH_TOKEN", "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY", "CURIE_INTERNAL_WORKER_TOKEN", "GIT_CONFIG_COUNT",
}
leaked = names & forbidden
leaked.update(
    name for name in names
    if name.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))
)
if leaked:
    fail(
        "workspace consumer workspace-init receives credential env "
        f"{sorted(leaked)}"
    )

runner_env = env_map(runner)
forbidden_runner = {
    "S3_ACCESS_KEY", "S3_SECRET_KEY", "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "GH_TOKEN", "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY", "CURIE_INTERNAL_WORKER_TOKEN",
}
if forbidden_runner & set(runner_env):
    fail("runner receives repository or worker credentials")
safe_directory = {
    name: runner_env.get(name, {}).get("value")
    for name in ("GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0")
}
if safe_directory != {
    "GIT_CONFIG_COUNT": "1",
    "GIT_CONFIG_KEY_0": "safe.directory",
    "GIT_CONFIG_VALUE_0": "/workspace",
}:
    fail(f"runner safe.directory wiring drifted: {safe_directory!r}")
if any(
    name.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))
    and name not in {"GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"}
    for name in runner_env
):
    fail("runner receives unexpected process-scoped git configuration")

writable = values["agentSandbox"]["runner"]["hardening"]["writablePaths"]
paths = [item["path"] if isinstance(item, dict) else item for item in writable]
if "/workspace" in paths:
    fail("/workspace must be a dedicated bounded volume, not hardening.writablePaths")


# Every runner NetworkPolicy selects only runner-sandbox labels; the dynamic
# publication component therefore remains outside the fail-closed sandbox
# selector and needs no GitHub widening on sandbox pods.
policies = [doc for doc in docs if doc.get("kind") == "NetworkPolicy" and "runner" in doc["metadata"]["name"]]
if not policies:
    fail("runner fail-closed NetworkPolicies are absent")
for policy in policies:
    labels = policy.get("spec", {}).get("podSelector", {}).get("matchLabels", {})
    expected_runner_labels = {
        "app.kubernetes.io/name": "curie",
        "app.kubernetes.io/instance": "remote-dev",
        "app.kubernetes.io/component": "runner-sandbox",
    }
    if labels != expected_runner_labels:
        fail(f"runner policy selector widened: {policy['metadata']['name']}")

print("remote-dev capability render assertions passed")
PY
