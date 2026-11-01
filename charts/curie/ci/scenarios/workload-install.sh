#!/usr/bin/env bash
# One caller-owned install for the sequential workload scenarios.
set -euo pipefail
umask 077

[[ $# == 1 && ( "$1" == up || "$1" == down ) ]] || {
    echo "FAIL: usage: workload-install.sh up|down" >&2
    exit 2
}
ACTION="$1"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }
for variable in CURIE_BIN CURIE_SCENARIO_CONTEXT CURIE_SCENARIO_RUN_ID \
    CURIE_SCENARIO_API_IMAGE CURIE_SCENARIO_WORKER_IMAGE CURIE_SCENARIO_RUNNER_IMAGE \
    CURIE_SCENARIO_NAMESPACE CURIE_SCENARIO_RELEASE; do
    [[ -n "${!variable:-}" ]] || fail "$variable is required"
done
[[ -f "$CURIE_BIN" && -x "$CURIE_BIN" ]] || fail "CURIE_BIN must name an executable curie binary"
command -v python3 >/dev/null || fail "required executable is unavailable: python3"
# Validate all inputs, including context existence, before invoking any
# infrastructure tool. Read the same kubeconfig files the caller's tools use.
python3 - <<'PY'
import os
import pathlib
import re
import sys


def fail(variable, reason):
    print(f"FAIL: {variable} {reason}", file=sys.stderr)
    raise SystemExit(1)


context = os.environ["CURIE_SCENARIO_CONTEXT"]
if re.search(r"[\s\x00-\x1f\x7f]", context) or context.startswith("-"):
    fail("CURIE_SCENARIO_CONTEXT", "must name one kubeconfig context")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,24}", os.environ["CURIE_SCENARIO_RUN_ID"]):
    fail("CURIE_SCENARIO_RUN_ID", "must match ^[a-z0-9][a-z0-9-]{0,24}$")
for variable, maximum in (("CURIE_SCENARIO_NAMESPACE", 63), ("CURIE_SCENARIO_RELEASE", 53)):
    value = os.environ[variable]
    if len(value) > maximum or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", value):
        fail(variable, f"must be a DNS label of at most {maximum} characters")
component = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
host_component = r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
host = rf"(?:{host_component}(?:\.{host_component})*|\[[A-Fa-f0-9:]+\])(?::[0-9]+)?"
repository = rf"(?:{host}/)?{component}(?:/{component})*"
for variable in ("CURIE_SCENARIO_API_IMAGE", "CURIE_SCENARIO_WORKER_IMAGE", "CURIE_SCENARIO_RUNNER_IMAGE"):
    image = os.environ[variable]
    name, separator, tag = image.rpartition(":")
    if not separator or not re.fullmatch(repository, name) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        fail(variable, "must be a repository:tag image reference")
try:
    import yaml
except ImportError:
    fail("CURIE_SCENARIO_CONTEXT", "validation requires Python 3 with PyYAML")
kubeconfig = os.environ.get("KUBECONFIG") or str(pathlib.Path.home() / ".kube/config")
contexts = {}
for filename in dict.fromkeys(path for path in kubeconfig.split(os.pathsep) if path):
    path = pathlib.Path(filename)
    if not path.exists():
        continue
    try:
        config = yaml.safe_load(path.read_text()) or {}
        entries = config.get("contexts", [])
        if not isinstance(entries, list):
            fail("CURIE_SCENARIO_CONTEXT", "cannot read the caller's kubeconfig contexts")
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
                fail("CURIE_SCENARIO_CONTEXT", "cannot read the caller's kubeconfig contexts")
            contexts.setdefault(entry["name"], entry.get("context"))
    except (OSError, ValueError, AttributeError, yaml.YAMLError):
        fail("CURIE_SCENARIO_CONTEXT", "cannot read the caller's kubeconfig")
if not isinstance(contexts.get(context), dict):
    fail("CURIE_SCENARIO_CONTEXT", "is absent from the caller's kubeconfig")
PY
for command in kubectl helm; do
    command -v "$command" >/dev/null || fail "required executable is unavailable: $command"
done
CONTEXT="$CURIE_SCENARIO_CONTEXT"
RUN_ID="$CURIE_SCENARIO_RUN_ID"
NAMESPACE="$CURIE_SCENARIO_NAMESPACE"
RELEASE="$CURIE_SCENARIO_RELEASE"
[[ "$NAMESPACE" == "test-workload-$RUN_ID" ]] || fail "CURIE_SCENARIO_NAMESPACE must be test-workload-<run ID>"
[[ "$RELEASE" == "workload-$RUN_ID" ]] || fail "CURIE_SCENARIO_RELEASE must be workload-<run ID>"
FULLNAME="$RELEASE-curie"
PUBLICATION_NAMESPACE="${NAMESPACE}-${FULLNAME}-publication"
PUBLICATION_NAMESPACE="${PUBLICATION_NAMESPACE:0:63}"
PUBLICATION_NAMESPACE="${PUBLICATION_NAMESPACE%-}"
CLUSTER_OBJECTS=(
    namespace/agent-sandbox-system
    clusterrole/agent-sandbox-controller
    clusterrole/agent-sandbox-controller-extensions
    clusterrole/agent-sandbox-controller-networkpolicies-read
    clusterrolebinding/agent-sandbox-controller
    clusterrolebinding/agent-sandbox-controller-extensions
    clusterrolebinding/agent-sandbox-controller-networkpolicies-read
    priorityclass/curie-platform
    priorityclass/curie-sandbox
    "namespace/$PUBLICATION_NAMESPACE"
    "validatingadmissionpolicy/$FULLNAME-runner-resources"
    "validatingadmissionpolicybinding/$FULLNAME-runner-resources"
)
CRDS=(
    sandboxes.agents.x-k8s.io
    sandboxclaims.extensions.agents.x-k8s.io
    sandboxtemplates.extensions.agents.x-k8s.io
    sandboxwarmpools.extensions.agents.x-k8s.io
)
ABSENT_CRDS=()
NAMESPACE_OWNED=0
WORKDIR="$(python3 -c 'import tempfile; print(tempfile.mkdtemp(prefix="curie-workload-install-"))')"
export CURIE_CONFIG_DIR="$WORKDIR/curie-config"
unset CURIE_CREDENTIALS ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN ANTHROPIC_BASE_URL
unset CURIE_GITHUB_TOKEN GITHUB_TOKEN GH_TOKEN SLACK_BOT_TOKEN SLACK_APP_TOKEN
unset CURIE_API_URL CURIE_API_KEY CURIE_VALKEY_PASSWORD CURIE_NAMESPACE CURIE_STREAM
unset CURIE_MODEL_BASE_URL CURIE_FAKE_MODEL CURIE_FAKE_SCRIPT CURIE_FAKE_SCRIPT_FILE
kc() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }

teardown() {
    local cleanup_status=0 object remaining crd resource
    for resource in sandboxclaims sandboxes; do
        if [[ "$resource" == sandboxclaims ]]; then
            crd=sandboxclaims.extensions.agents.x-k8s.io
        else
            crd=sandboxes.agents.x-k8s.io
        fi
        if ! remaining="$(kc get "crd/$crd" --ignore-not-found -o name 2>"$WORKDIR/inventory-crd.log")"; then
            echo "FAIL: could not inventory scenario resources before teardown" >&2
            cleanup_status=1
        elif [[ -n "$remaining" ]]; then
            kc delete "$resource" --all --ignore-not-found --wait=true --timeout=90s \
                >"$WORKDIR/delete-$resource.log" 2>&1 \
                || { echo "FAIL: could not drain scenario $resource" >&2; cleanup_status=1; }
        fi
    done
    helm --kube-context "$CONTEXT" uninstall "$RELEASE" -n "$NAMESPACE" \
        --ignore-not-found --wait --timeout 60s >"$WORKDIR/uninstall.log" 2>&1 \
        || { echo "FAIL: could not uninstall the owned release" >&2; cleanup_status=1; }
    kc delete pods --all --ignore-not-found --wait=true --timeout=90s >"$WORKDIR/delete-pods.log" 2>&1 \
        || { echo "FAIL: could not drain pods in the owned namespace" >&2; cleanup_status=1; }
    for object in "${CLUSTER_OBJECTS[@]}"; do
        if ! kc get "$object" --ignore-not-found -o json >"$WORKDIR/object.json" 2>"$WORKDIR/inventory-object.log"; then
            echo "FAIL: could not inventory owned object $object during teardown" >&2
            cleanup_status=1
            continue
        fi
        [[ -s "$WORKDIR/object.json" ]] || continue
        if ! python3 - "$WORKDIR/object.json" "$RELEASE" "$NAMESPACE" <<'PY'
import json, pathlib, sys
try:
    metadata = json.loads(pathlib.Path(sys.argv[1]).read_text())["metadata"]
    annotations = metadata.get("annotations", {})
    assert annotations.get("meta.helm.sh/release-name") == sys.argv[2]
    assert annotations.get("meta.helm.sh/release-namespace") == sys.argv[3]
except (OSError, ValueError, KeyError, TypeError, AssertionError):
    raise SystemExit(1) from None
PY
        then
            echo "FAIL: refusing to delete $object without this release's Helm ownership" >&2
            cleanup_status=1
            continue
        fi
        if [[ "$object" == namespace/* ]]; then
            kc -n "${object#namespace/}" delete pods --all --ignore-not-found --wait=true --timeout=90s \
                >"$WORKDIR/delete-controller-pods.log" 2>&1 \
                || { echo "FAIL: could not drain pods in an owned auxiliary namespace" >&2; cleanup_status=1; }
        fi
        kc delete "$object" --ignore-not-found --wait=false >"$WORKDIR/delete-object.log" 2>&1 \
            || { echo "FAIL: could not delete owned object $object" >&2; cleanup_status=1; }
    done
    for crd in "${ABSENT_CRDS[@]}"; do
        kc delete "crd/$crd" --ignore-not-found --wait=false >"$WORKDIR/delete-crd.log" 2>&1 \
            || { echo "FAIL: could not delete owned CRD $crd" >&2; cleanup_status=1; }
        kc wait --for=delete "crd/$crd" --timeout=60s >"$WORKDIR/wait-crd.log" 2>&1 \
            || { echo "FAIL: owned CRD $crd deletion did not complete" >&2; cleanup_status=1; }
    done
    for object in "${CLUSTER_OBJECTS[@]}"; do
        kc wait --for=delete "$object" --timeout=180s >"$WORKDIR/wait-object.log" 2>&1 \
            || { echo "FAIL: owned object $object deletion did not complete" >&2; cleanup_status=1; }
    done
    if ! remaining="$(helm --kube-context "$CONTEXT" list -n "$NAMESPACE" --all --short \
        --filter "^$RELEASE$" 2>"$WORKDIR/verify-release.log")"; then
        echo "FAIL: could not verify release teardown" >&2
        cleanup_status=1
    elif [[ -n "$remaining" ]]; then
        echo "FAIL: owned release remains" >&2
        cleanup_status=1
    fi
    for object in "${CLUSTER_OBJECTS[@]}" "${ABSENT_CRDS[@]/#/crd/}"; do
        if ! remaining="$(kc get "$object" --ignore-not-found -o name 2>"$WORKDIR/verify-object.log")"; then
            echo "FAIL: could not verify teardown of $object" >&2
            cleanup_status=1
        elif [[ -n "$remaining" ]]; then
            echo "FAIL: owned object $object remains" >&2
            cleanup_status=1
        fi
    done
    kc delete namespace "$NAMESPACE" --ignore-not-found --wait=false >"$WORKDIR/delete-namespace.log" 2>&1 \
        || { echo "FAIL: could not delete the owned namespace" >&2; cleanup_status=1; }
    kc wait --for=delete "namespace/$NAMESPACE" --timeout=180s >"$WORKDIR/wait-namespace.log" 2>&1 \
        || { echo "FAIL: owned namespace deletion did not complete" >&2; cleanup_status=1; }
    if ! remaining="$(kc get "namespace/$NAMESPACE" --ignore-not-found -o name 2>"$WORKDIR/verify-namespace.log")"; then
        echo "FAIL: could not verify owned namespace teardown" >&2
        cleanup_status=1
    elif [[ -n "$remaining" ]]; then
        echo "FAIL: owned namespace remains" >&2
        cleanup_status=1
    fi
    (( cleanup_status == 0 )) || return 1
    NAMESPACE_OWNED=0
    echo "OK: owned release, namespaces, controller resources, priority classes, and new CRDs are gone"
}
cleanup() {
    local status=$? cleanup_status=0
    trap - EXIT INT TERM
    set +e
    if [[ "$ACTION" == up && "$status" != 0 ]] && (( NAMESPACE_OWNED )); then
        teardown || cleanup_status=1
    fi
    rm -rf -- "$WORKDIR" || cleanup_status=1
    (( cleanup_status == 0 )) || exit 1
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$ACTION" == down ]]; then
    kc get "namespace/$NAMESPACE" --ignore-not-found -o json >"$WORKDIR/namespace.json" \
        || fail "could not inventory workload namespace ownership"
    [[ -s "$WORKDIR/namespace.json" ]] || exit 0
    if python3 - "$WORKDIR/namespace.json" "$RUN_ID" "$RELEASE" "$WORKDIR/created-crds" <<'PY'
import json, pathlib, sys
try:
    namespace = json.loads(pathlib.Path(sys.argv[1]).read_text())
    metadata = namespace["metadata"]
    labels = metadata.get("labels", {})
    annotations = metadata.get("annotations", {})
    if (labels.get("curie.dev/scenario") != "workload"
            or labels.get("curie.dev/scenario-run") != sys.argv[2]
            or annotations.get("curie.dev/scenario-release") != sys.argv[3]):
        raise SystemExit(3)
    known = {"sandboxes.agents.x-k8s.io", "sandboxclaims.extensions.agents.x-k8s.io",
             "sandboxtemplates.extensions.agents.x-k8s.io", "sandboxwarmpools.extensions.agents.x-k8s.io"}
    raw = annotations["curie.dev/scenario-created-crds"]
    assert isinstance(raw, str)
    names = raw.split(",") if raw else []
    assert len(names) == len(set(names)) and set(names) <= known
    pathlib.Path(sys.argv[4]).write_text("".join(name + "\n" for name in names))
except (OSError, ValueError, KeyError, TypeError, AssertionError):
    raise SystemExit(1) from None
PY
    then
        mapfile -t ABSENT_CRDS <"$WORKDIR/created-crds"
    else
        ownership_status=$?
        (( ownership_status == 3 )) && exit 0
        fail "workload namespace has malformed ownership inventory"
    fi
    NAMESPACE_OWNED=1
    teardown || fail "owned workload teardown failed"
    exit 0
fi

for object in "namespace/$NAMESPACE" "${CLUSTER_OBJECTS[@]}"; do
    existing="$(kc get "$object" --ignore-not-found -o name)" || fail "could not inventory $object"
    [[ -z "$existing" ]] || fail "refusing to install while $object exists"
done
for crd in "${CRDS[@]}"; do
    existing="$(kc get "crd/$crd" --ignore-not-found -o name)" || fail "could not inventory CRD $crd"
    [[ -n "$existing" ]] || ABSENT_CRDS+=("$crd")
done
created_crds="$(IFS=,; echo "${ABSENT_CRDS[*]}")"
python3 - "$WORKDIR/namespace.json" "$NAMESPACE" "$RUN_ID" "$RELEASE" "$created_crds" <<'PY'
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "apiVersion": "v1", "kind": "Namespace", "metadata": {
        "name": sys.argv[2],
        "labels": {"curie.dev/scenario": "workload", "curie.dev/scenario-run": sys.argv[3],
                   "curietech.ai/created-by": sys.argv[4], "curietech.ai/created-in": sys.argv[2]},
        "annotations": {"curie.dev/scenario-release": sys.argv[4], "curie.dev/scenario-created-crds": sys.argv[5]},
    },
}))
PY
kc create -f "$WORKDIR/namespace.json" >"$WORKDIR/create-namespace.log" 2>&1 \
    || fail "could not create the owned workload namespace"
NAMESPACE_OWNED=1
python3 - "$WORKDIR/images.yaml" <<'PY'
import os, pathlib, sys, yaml
images = {component: os.environ[f"CURIE_SCENARIO_{component.upper()}_IMAGE"].rsplit(":", 1)
          for component in ("api", "worker", "runner")}
values = {component: {"image": {"repository": images[component][0], "tag": images[component][1]}}
          for component in ("api", "worker")}
values["agentSandbox"] = {"runner": {"image": images["runner"][0], "tag": images["runner"][1]}}
pathlib.Path(sys.argv[1]).write_text(yaml.safe_dump(values))
PY
sets=(
    --set "fullnameOverride=$FULLNAME"
    --set api.image.pullPolicy=Never --set worker.image.pullPolicy=Never
    --set agentSandbox.runner.imagePullPolicy=Never --set agentSandbox.runner.prewarm.imagePullPolicy=Never
    --set agentSandbox.deploy=true --set agentSandbox.controller.deploy=true
    --set agentSandbox.platformPool.replicas=0 --set agentSandbox.runner.fakeModel=true
    --set worker.replicas=1 --set agentSandbox.runner.workspace.enabled=true
    --set worker.claimTimeoutSeconds=90 --set worker.routeTtlSeconds=3600
    --set resourceQuota.hard.sandboxPodCount=2
    --set api.readinessProbe.failureThreshold=120 --set api.livenessProbe.failureThreshold=60
    --set worker.connectorReconciler.enabled=true --set worker.connectorReconciler.intervalSeconds=1
    --set agentSandbox.agentResourceProfiles.acme-fp=factory
    --set security.gvisor.mode=off
    --set langfuse.deploy=false --set langfuse.host=langfuse.example.com
    --set clickhouse.deploy=false --set otelCollector.deploy=false --set otelCollector.telemetryDisabled=true
    --set ui.deploy=false --set dispatcher.deploy=false --set mailAdapter.deploy=false
)
"$CURIE_BIN" cluster --context "$CONTEXT" up --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" --values-file "$WORKDIR/images.yaml" \
    --dev --fake-model --no-expose "${sets[@]}" \
    >"$WORKDIR/install.log" 2>&1 || fail "candidate cluster install failed"
for component in api worker; do
    image_variable="CURIE_SCENARIO_${component^^}_IMAGE"
    kc rollout status "deployment/$FULLNAME-$component" --timeout=300s >/dev/null
    [[ "$(kc get "deployment/$FULLNAME-$component" -o jsonpath='{.spec.template.spec.containers[0].image}')" == "${!image_variable}" ]] \
        || fail "the $component Deployment is not using the candidate image"
done
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_CLAIM_TIMEOUT_SECONDS")].value}')" == 90 ]] \
    || fail "worker claim timeout must retain its normal 90-second value"
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_ROUTE_TTL_SECONDS")].value}')" == 3600 ]] \
    || fail "worker route retention must retain its normal 3600-second value"
echo "OK: shared candidate workload release is ready"
