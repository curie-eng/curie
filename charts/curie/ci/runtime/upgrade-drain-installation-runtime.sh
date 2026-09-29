#!/usr/bin/env bash
# Exercise the installation-identity lifecycle against a task-owned kind cluster.
# Every workload replica is zero and every Helm operation uses --no-hooks: this
# script proves Secret lookup/reuse and retained-Secret adoption only. It never
# runs a product image and cannot prove the candidate drain command's guard.
set -euo pipefail

: "${KUBECONFIG:?set a private kubeconfig for the owned kind cluster}"
: "${CURIE_UPGRADE_DRAIN_KIND_CONTEXT:?set the explicit task-owned kind context}"

context="$CURIE_UPGRADE_DRAIN_KIND_CONTEXT"
[[ "$context" == kind-* ]] || {
  echo "CURIE_UPGRADE_DRAIN_KIND_CONTEXT must name an owned kind-* context" >&2
  exit 2
}
[[ "$(kubectl config get-contexts "$context" -o name)" == "$context" ]] || {
  echo "the requested owned kind context is not present in KUBECONFIG" >&2
  exit 2
}

CHART="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"
namespace="acme-upgrade-drain-${RANDOM}-${RANDOM}"
release="acme"
secret_name="acme-curie-secrets"
created=false

cleanup() {
  local rc=$? cleanup_failed=false
  trap - EXIT
  set +e
  if [[ "$created" == true ]]; then
    helm uninstall "$release" \
      --kube-context "$context" \
      --namespace "$namespace" \
      --no-hooks >/dev/null 2>&1 || true
    kubectl --context "$context" delete namespace "$namespace" \
      --wait=true --timeout=90s >/dev/null 2>&1 || true
    if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
      echo "FAIL: task-owned namespace still exists after cleanup" >&2
      cleanup_failed=true
    fi
  fi
  rm -rf "$TMP"
  if [[ "$cleanup_failed" == true && "$rc" -eq 0 ]]; then
    rc=1
  fi
  exit "$rc"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  fail "random task namespace already exists"
fi
kubectl --context "$context" create namespace "$namespace" >/dev/null
created=true

umask 077
cat > "$TMP/values.yaml" <<'YAML'
security:
  allowDevDefaults: false
  checkDefaultCredentials: false
  gvisor:
    enabled: false
  networkPolicy:
    enabled: false
preflights:
  avxCheck:
    enabled: false
  networkPolicyProbe:
    enabled: false
  controllerReady:
    enabled: false
postgres:
  deploy: false
  host: postgres.example.com
valkey:
  deploy: false
  host: valkey.example.com
clickhouse:
  deploy: false
  host: clickhouse.example.com
rustfs:
  deploy: false
  host: object-store.example.com
  egress:
    - cidr: 192.0.2.0/24
      port: 443
langfuse:
  deploy: false
  host: traces.example.com
  modelPricing:
    enabled: false
otelCollector:
  deploy: false
  telemetryDisabled: true
api:
  deploy: false
dispatcher:
  deploy: false
mailAdapter:
  deploy: false
worker:
  deploy: true
  replicas: 0
  serviceAccount:
    create: false
  publication:
    enabled: false
  upgradeDrain:
    enabled: true
ui:
  deploy: false
inference:
  deploy: false
grafanaConnector:
  deploy: false
agentSandbox:
  deploy: false
  controller:
    deploy: false
priorityClasses:
  platform:
    create: false
    name: ""
  sandbox:
    create: false
    name: ""
YAML

install_chart() {
  local label="$1"
  shift
  if ! helm install "$release" "$CHART" \
    --kube-context "$context" \
    --namespace "$namespace" \
    --skip-crds \
    --no-hooks \
    -f "$TMP/values.yaml" \
    "$@" > "$TMP/${label}.log" 2>&1; then
    fail "$label Helm install failed; output withheld because it may contain Secret data"
  fi
}

upgrade_chart() {
  local label="$1"
  shift
  if ! helm upgrade "$release" "$CHART" \
    --kube-context "$context" \
    --namespace "$namespace" \
    --skip-crds \
    --no-hooks \
    -f "$TMP/values.yaml" \
    "$@" > "$TMP/${label}.log" 2>&1; then
    fail "$label Helm upgrade failed; output withheld because it may contain Secret data"
  fi
}

snapshot_secret() {
  local destination="$1"
  kubectl --context "$context" get secret "$secret_name" \
    --namespace "$namespace" -o json > "$destination"
}

snapshot_hooks() {
  local destination="$1"
  helm get hooks "$release" \
    --kube-context "$context" \
    --namespace "$namespace" > "$destination"
}

assert_no_product_pods() {
  local count
  count="$(
    kubectl --context "$context" get pods --namespace "$namespace" -o json |
      python3 -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))'
  )"
  [[ "$count" == 0 ]] || fail "identity-only fixture created a Pod"
}

assert_secret_transition() {
  local before="$1" after="$2" mode="$3"
  python3 - "$before" "$after" "$mode" <<'PY'
import base64
import json
import sys

before_path, after_path, mode = sys.argv[1:4]
before = json.load(open(before_path))
after = json.load(open(after_path))
credential_keys = ("valkeyPassword", "postgresPassword", "apiKey")


def decoded(secret, key):
    encoded = (secret.get("data") or {}).get(key)
    assert isinstance(encoded, str) and encoded, f"managed Secret is missing {key}"
    value = base64.b64decode(encoded, validate=True).decode()
    assert value.strip(), f"managed Secret has blank {key}"
    return value


for key in credential_keys:
    assert before["data"][key] == after["data"][key], (
        f"generated {key} changed across the identity lifecycle"
    )

before_id = decoded(before, "installationId")
after_id = decoded(after, "installationId")
assert before["metadata"]["uid"] == after["metadata"]["uid"], (
    "managed Secret object was replaced instead of updated"
)

if mode == "uid-fallback":
    assert after_id == before["metadata"]["uid"], (
        "first legacy upgrade did not adopt the live Secret UID"
    )
elif mode == "reuse":
    assert after_id == before_id, "subsequent upgrade changed installation identity"
elif mode == "fresh-install":
    assert after_id != before_id, "fresh install reused the prior installation identity"
    assert after_id != after["metadata"]["uid"], (
        "fresh install used the retained Secret UID instead of a new identity"
    )
else:
    raise AssertionError(f"unknown transition mode {mode}")
PY
}

assert_stored_hook_identity() {
  local secret_json="$1" hooks_yaml="$2" expected_legacy="$3" expected_observed="$4"
  python3 - "$secret_json" "$hooks_yaml" "$expected_legacy" "$expected_observed" <<'PY'
import base64
import json
import sys

import yaml

secret_path, hooks_path, expected_legacy, expected_observed = sys.argv[1:5]
secret = json.load(open(secret_path))
installation_id = base64.b64decode(
    secret["data"]["installationId"], validate=True
).decode()
assert installation_id.strip(), "stored installation identity is blank"

components = {
    "upgrade-drain": "drain",
    "upgrade-drain-release": "release",
    "upgrade-drain-attest": "attest",
}
jobs = {}
for doc in yaml.safe_load_all(open(hooks_path)):
    if not doc or doc.get("kind") != "Job":
        continue
    component = ((doc.get("metadata") or {}).get("labels") or {}).get(
        "app.kubernetes.io/component"
    )
    if component in components:
        assert component not in jobs, f"duplicate stored hook {component}"
        jobs[component] = doc
assert set(jobs) == set(components), "release does not store the upgrade-drain hooks"

identities = []
revisions = []
legacy_values = []
for component, mode in components.items():
    containers = jobs[component]["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1, f"{component} has more than one container"
    container = containers[0]
    assert container["command"] == [
        "python",
        "-m",
        "curie_worker.upgrade_drain",
        "--mode",
        mode,
        f"--installation-id-observed={expected_observed}",
    ], f"{component} stored the wrong observed-identity argument"
    env = {entry["name"]: entry for entry in container.get("env") or []}
    identity = env["CURIE_INSTALLATION_ID"]
    revision = env["CURIE_UPGRADE_REVISION"]
    legacy = env["CURIE_UPGRADE_LEGACY_QUIESCE"]
    assert set(identity) == {"name", "value"}, (
        f"{component} identity is not a stored literal"
    )
    assert set(revision) == {"name", "value"}, (
        f"{component} revision is not a stored literal"
    )
    assert set(legacy) == {"name", "value"}, (
        f"{component} legacy bit is not a stored literal"
    )
    assert isinstance(revision["value"], str) and revision["value"].isdecimal(), (
        f"{component} revision is not a decimal integer string"
    )
    assert int(revision["value"]) > 0, f"{component} revision is not positive"
    identities.append(identity["value"])
    revisions.append(revision["value"])
    legacy_values.append(legacy["value"])

assert identities == [installation_id, installation_id, installation_id], (
    "stored hook identities do not match the decoded managed Secret value"
)
assert len(set(revisions)) == 1, "stored hooks disagree on Helm revision"
assert legacy_values == [expected_legacy, expected_legacy, expected_legacy], (
    "stored hooks disagree on legacy compatibility"
)
PY
}

# Helm's lookup contract says client-side template rendering returns an empty
# value while a server-connected install/upgrade can read the live Secret:
# https://helm.sh/docs/chart_template_guide/functions_and_pipelines/#using-the-lookup-function
install_chart initial
assert_no_product_pods
snapshot_secret "$TMP/initial-secret.json"
snapshot_hooks "$TMP/initial-hooks.yaml"
python3 - "$TMP/initial-secret.json" <<'PY'
import base64
import json
import sys

secret = json.load(open(sys.argv[1]))
encoded = secret.get("data", {}).get("installationId")
assert isinstance(encoded, str) and encoded, "fresh install omitted installationId"
installation_id = base64.b64decode(encoded, validate=True).decode()
assert installation_id.strip(), "fresh install rendered a blank installationId"
assert installation_id != secret["metadata"]["uid"], (
    "fresh install used the Secret UID instead of a new identity"
)
for key in ("valkeyPassword", "postgresPassword", "apiKey"):
    assert secret["data"].get(key), f"fresh install omitted generated {key}"
PY
assert_stored_hook_identity "$TMP/initial-secret.json" "$TMP/initial-hooks.yaml" false true

# A pre-fix Secret has no installationId. The first fix-bearing upgrade must
# use the UID already visible to Helm, while preserving generated credentials.
kubectl --context "$context" patch secret "$secret_name" \
  --namespace "$namespace" --type=json \
  -p='[{"op":"remove","path":"/data/installationId"}]' >/dev/null
upgrade_chart legacy-upgrade
assert_no_product_pods
snapshot_secret "$TMP/legacy-upgrade-secret.json"
snapshot_hooks "$TMP/legacy-upgrade-hooks.yaml"
assert_secret_transition \
  "$TMP/initial-secret.json" "$TMP/legacy-upgrade-secret.json" uid-fallback
assert_stored_hook_identity \
  "$TMP/legacy-upgrade-secret.json" "$TMP/legacy-upgrade-hooks.yaml" true true

# Once installationId exists, every later upgrade decodes and reuses it. The
# compatibility bridge is then off in both stored hooks.
upgrade_chart subsequent-upgrade
assert_no_product_pods
snapshot_secret "$TMP/subsequent-secret.json"
snapshot_hooks "$TMP/subsequent-hooks.yaml"
assert_secret_transition \
  "$TMP/legacy-upgrade-secret.json" "$TMP/subsequent-secret.json" reuse
assert_stored_hook_identity \
  "$TMP/subsequent-secret.json" "$TMP/subsequent-hooks.yaml" false true

# A live annotation is insufficient: Helm uninstall filters the stored release
# manifest before kube.Client.rdelete deletes the remaining objects. Record the
# task-only keep policy through a post-renderer on a zero-replica/no-hooks
# upgrade, then verify it is in the stored manifest.
# Sources:
# https://raw.githubusercontent.com/helm/helm/v3.17.3/pkg/action/uninstall.go
# https://raw.githubusercontent.com/helm/helm/v3.17.3/pkg/kube/client.go
cat > "$TMP/keep-secret.py" <<'PY'
#!/usr/bin/env python3
import os
import sys

import yaml

target = os.environ["TARGET_SECRET_NAME"]
documents = list(yaml.safe_load_all(sys.stdin))
matched = 0
for document in documents:
    if not document or document.get("kind") != "Secret":
        continue
    metadata = document.setdefault("metadata", {})
    if metadata.get("name") != target:
        continue
    metadata.setdefault("annotations", {})["helm.sh/resource-policy"] = "keep"
    matched += 1
assert matched == 1, f"expected one managed Secret named {target}, found {matched}"
yaml.safe_dump_all(documents, sys.stdout, sort_keys=False)
PY
chmod 700 "$TMP/keep-secret.py"
export TARGET_SECRET_NAME="$secret_name"
upgrade_chart record-keep --post-renderer "$TMP/keep-secret.py"
unset TARGET_SECRET_NAME
assert_no_product_pods
snapshot_secret "$TMP/keep-secret.json"
assert_secret_transition "$TMP/subsequent-secret.json" "$TMP/keep-secret.json" reuse
helm get manifest "$release" \
  --kube-context "$context" \
  --namespace "$namespace" > "$TMP/keep-manifest.yaml"
python3 - "$TMP/keep-manifest.yaml" "$secret_name" <<'PY'
import sys

import yaml

path, expected_name = sys.argv[1:3]
secrets = [
    doc
    for doc in yaml.safe_load_all(open(path))
    if doc and doc.get("kind") == "Secret" and doc["metadata"]["name"] == expected_name
]
assert len(secrets) == 1, "stored release manifest has no unique managed Secret"
annotations = secrets[0]["metadata"].get("annotations") or {}
assert annotations.get("helm.sh/resource-policy") == "keep", (
    "keep policy was not recorded in the stored release manifest"
)
PY

if ! helm uninstall "$release" \
  --kube-context "$context" \
  --namespace "$namespace" \
  --no-hooks > "$TMP/uninstall.log" 2>&1; then
  fail "Helm uninstall failed; output withheld because it may contain Secret data"
fi
snapshot_secret "$TMP/retained-secret.json"
assert_secret_transition "$TMP/keep-secret.json" "$TMP/retained-secret.json" reuse

# Helm permits same-release adoption when existingResourceConflict and
# checkOwnership find the Helm managed-by label plus matching release-name and
# release-namespace annotations. No ownership override is used here:
# https://raw.githubusercontent.com/helm/helm/v3.16.4/pkg/action/validate.go
kubectl --context "$context" annotate secret "$secret_name" \
  --namespace "$namespace" helm.sh/resource-policy- >/dev/null
snapshot_secret "$TMP/adoptable-secret.json"
python3 - "$TMP/adoptable-secret.json" "$release" "$namespace" <<'PY'
import json
import sys

path, release, namespace = sys.argv[1:4]
secret = json.load(open(path))
labels = secret["metadata"].get("labels") or {}
annotations = secret["metadata"].get("annotations") or {}
assert labels.get("app.kubernetes.io/managed-by") == "Helm", (
    "retained Secret lost Helm managed-by ownership"
)
assert annotations.get("meta.helm.sh/release-name") == release, (
    "retained Secret has the wrong Helm release-name owner"
)
assert annotations.get("meta.helm.sh/release-namespace") == namespace, (
    "retained Secret has the wrong Helm release-namespace owner"
)
assert "helm.sh/resource-policy" not in annotations, (
    "temporary keep policy was not removed before reinstall"
)
PY

# Reinstall the same name/namespace with ordinary matching-ownership adoption.
# .Release.IsInstall must still mint a new ID even though lookup sees the
# retained Secret and its UID; generated credentials remain byte-identical.
install_chart reinstall
assert_no_product_pods
snapshot_secret "$TMP/reinstalled-secret.json"
snapshot_hooks "$TMP/reinstalled-hooks.yaml"
assert_secret_transition \
  "$TMP/adoptable-secret.json" "$TMP/reinstalled-secret.json" fresh-install
assert_stored_hook_identity \
  "$TMP/reinstalled-secret.json" "$TMP/reinstalled-hooks.yaml" false true

# The offline observed=false argument is pinned by upgrade-drain-assertions.sh.
# Executing that Job against the candidate image is driver-owned manual E2E;
# substituting a registry image here would not prove the candidate guard.
echo "OK: live UID fallback, decoded ID reuse, hook agreement, generated credential stability, and retained-Secret fresh-install identity reset passed"
